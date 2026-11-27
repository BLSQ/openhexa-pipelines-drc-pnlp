import json
import os
from datetime import UTC, datetime, timedelta
from pathlib import Path

from openhexa.sdk import current_run, parameter, pipeline, workspace
from utils import (
    OpenHexaApi,
    evaluate_flow,
    evaluate_step,
    fingerprint,
    parse_datetime,
    worst_level,
)

# JSON files are not included in the pipeline bundle by `openhexa pipelines push`, so the configuration lives in the
# workspace filesystem. The repository copy is used as a fallback for local runs.
WORKSPACE_CONFIG_PATH = "pipelines/pnlp_integrations_status/config/flows_config.json"
LOCAL_CONFIG_PATH = Path(__file__).parent / "config" / "flows_config.json"
LATEST_FILENAME = "status_latest.json"


@pipeline("pnlp_integrations_status")
@parameter(
    "force",
    name="Force",
    type=bool,
    default=False,
    required=False,
    help="Collect and write a new status file even if nothing changed since the last one.",
)
def pnlp_integrations_status(force: bool) -> None:
    """Collect the status of every data flow pushing to the PNLP NMDR DHIS2 into a JSON file for the webapp."""
    config = load_config(Path(workspace.files_path) / WORKSPACE_CONFIG_PATH, LOCAL_CONFIG_PATH)
    workspace_slug = os.environ.get("HEXA_WORKSPACE", config["workspace_slug"])
    api = OpenHexaApi.from_environment(workspace_slug)
    output_dir = Path(workspace.files_path) / config["output_dir"]
    now = datetime.now(UTC)

    codes = [step["code"] for flow in config["flows"] for step in flow["steps"]]
    dataset_slugs = sorted(
        {d["slug"] for flow in config["flows"] for d in flow.get("datasets", [])}
        | {c["dataset"] for flow in config["flows"] for c in flow.get("checks", []) if "dataset" in c}
    )

    current_fingerprint = check_changes(api, codes, dataset_slugs)
    previous = read_previous_status(output_dir / LATEST_FILENAME)
    if not force and is_unchanged(previous, current_fingerprint, now, config["max_snapshot_age_hours"]):
        previous["checked_at"] = now.isoformat()
        write_json(output_dir / LATEST_FILENAME, previous, compact=True)
        current_run.log_info(f"Aucun changement depuis {previous['generated_at']}. Collecte ignorée.")
        return

    status = collect_status(api, config, codes, dataset_slugs, now)
    status["fingerprint"] = current_fingerprint
    write_status(output_dir, status, now)


def load_config(workspace_path: Path, local_path: Path) -> dict:
    """Load the flows configuration from the workspace, falling back to the local copy.

    Args:
        workspace_path: Path of the configuration file in the workspace filesystem.
        local_path: Path of the configuration file next to the pipeline code (local runs).

    Returns:
        The configuration as a dictionary.
    """
    path = workspace_path if workspace_path.exists() else local_path
    if not path.exists():
        raise FileNotFoundError(f"Configuration file not found: {workspace_path}")
    current_run.log_info(f"Configuration loaded from {path}.")
    with path.open(encoding="utf-8") as f:
        return json.load(f)


def check_changes(api: OpenHexaApi, codes: list[str], dataset_slugs: list[str]) -> str:
    """Compute the fingerprint of the latest runs and dataset versions (light query, no messages).

    Args:
        api: OpenHEXA API client.
        codes: Tracked pipeline codes.
        dataset_slugs: Tracked dataset slugs.

    Returns:
        The current fingerprint.
    """
    pipelines = api.pipelines_runs(codes, per_page=1, with_messages=False)
    datasets = api.datasets_latest(dataset_slugs)
    return fingerprint(pipelines, datasets)


def read_previous_status(path: Path) -> dict | None:
    """Read the previous status file, if any.

    Args:
        path: Path of `status_latest.json`.

    Returns:
        The previous status, or None when missing or unreadable.
    """
    if not path.exists():
        return None
    try:
        with path.open(encoding="utf-8") as f:
            return json.load(f)
    except (OSError, ValueError) as e:
        current_run.log_warning(f"Previous status file unreadable, a new one will be written: {e}")
        return None


def is_unchanged(previous: dict | None, current_fingerprint: str, now: datetime, max_age_hours: int) -> bool:
    """Decide whether the collection can be skipped.

    Args:
        previous: Previous status (or None).
        current_fingerprint: Fingerprint of the current runs and dataset versions.
        now: Reference time.
        max_age_hours: Maximum age of the previous full collection before forcing a new one.

    Returns:
        True when nothing changed and the previous collection is recent enough.
    """
    if not previous or previous.get("fingerprint") != current_fingerprint:
        return False
    generated_at = parse_datetime(previous.get("generated_at"))
    return generated_at is not None and now - generated_at < timedelta(hours=max_age_hours)


def read_state_files(api: OpenHexaApi, paths: list[str], files_path: Path) -> dict[str, dict | None]:
    """Read the pipelines' state/config JSON files from the workspace filesystem, or through the API when absent.

    Inside OpenHEXA the workspace is mounted at `files_path` (the run token cannot use `readFileContent`).
    Locally the files are not mounted, so they are read through the API with the local token.

    Args:
        api: OpenHEXA API client.
        paths: File paths relative to the workspace root.
        files_path: Root of the workspace filesystem.

    Returns:
        Mapping of path to parsed JSON content (None if missing or invalid).
    """
    states, remote = {}, []
    for path in paths:
        local = files_path / path
        if not local.exists():
            remote.append(path)
            continue
        try:
            with local.open(encoding="utf-8") as f:
                states[path] = json.load(f)
        except (OSError, ValueError) as e:
            current_run.log_warning(f"Cannot read {path}: {e}")
            states[path] = None
    if remote:
        states.update(api.read_json_files(remote))
    for path, content in states.items():
        if content is None:
            current_run.log_warning(f"State file unreadable or missing: {path}")
    return states


def list_output_files(api: OpenHexaApi, checks: list[dict], files_path: Path) -> dict[str, list[str] | None]:
    """List the output files used by `latest_period_file` checks, from the filesystem or through the API.

    Args:
        api: OpenHEXA API client.
        checks: Checks with `prefix` (folder relative to the workspace root) and `pattern` (glob under the prefix).
        files_path: Root of the workspace filesystem.

    Returns:
        Mapping of `prefix` + `pattern` to matching paths relative to the workspace root (None if listing failed).
    """
    listings = {}
    for check in checks:
        key = check["prefix"] + check["pattern"]
        folder = files_path / check["prefix"]
        try:
            if folder.exists():
                listings[key] = [str(p.relative_to(files_path)) for p in folder.glob(check["pattern"])]
            else:
                listings[key] = api.glob_files(check["prefix"], check["pattern"])
        except Exception as e:
            current_run.log_warning(f"Cannot list files in {check['prefix']}: {e}")
            listings[key] = None
    return listings


def collect_status(api: OpenHexaApi, config: dict, codes: list[str], dataset_slugs: list[str], now: datetime) -> dict:
    """Collect runs, messages, datasets and state files, then evaluate every flow.

    Args:
        api: OpenHEXA API client.
        config: Flows configuration.
        codes: Tracked pipeline codes.
        dataset_slugs: Tracked dataset slugs.
        now: Reference time.

    Returns:
        The full status document.
    """
    current_run.log_info(f"Collecting runs of {len(codes)} pipelines and {len(dataset_slugs)} datasets.")
    pipelines = api.pipelines_runs(codes, per_page=config["runs_to_scan"], with_messages=True)
    datasets = api.datasets_latest(dataset_slugs)
    paths = sorted({c["path"] for flow in config["flows"] for c in flow.get("checks", []) if "path" in c})
    states = read_state_files(api, paths, Path(workspace.files_path))
    file_checks = [c for flow in config["flows"] for c in flow.get("checks", []) if c["type"] == "latest_period_file"]
    listings = list_output_files(api, file_checks, Path(workspace.files_path))

    app_url = config["app_url"]
    steps_config = {step["code"]: step for flow in config["flows"] for step in flow["steps"]}
    steps = {
        code: evaluate_step(step, pipelines.get(code), now, app_url, api.workspace_slug)
        for code, step in steps_config.items()
    }
    flows = [
        evaluate_flow(flow, steps, datasets, states, listings, now, app_url, api.workspace_slug)
        for flow in config["flows"]
    ]
    for flow in flows:
        current_run.log_info(f"{flow['label']}: {flow['status']} ({len(flow['flags'])} alert(s)).")

    run_id = os.environ.get("HEXA_RUN_ID")
    return {
        "generated_at": now.isoformat(),
        "checked_at": now.isoformat(),
        "fingerprint": None,
        "collector_run_url": (
            f"{app_url}/workspaces/{api.workspace_slug}/pipelines/pnlp-integrations-status/runs/{run_id}"
            if run_id
            else None
        ),
        "target": config["target"],
        "status": worst_level([f["status"] for f in flows]),
        "flows": flows,
    }


def write_status(output_dir: Path, status: dict, now: datetime) -> None:
    """Write the timestamped status file and refresh `status_latest.json`.

    Args:
        output_dir: Output folder in the workspace.
        status: Status document.
        now: Reference time, used in the timestamped file name.
    """
    timestamped = output_dir / f"status_{now:%Y%m%d_%H%M%S}.json"
    write_json(timestamped, status)
    write_json(output_dir / LATEST_FILENAME, status, compact=True)
    current_run.log_info(f"Status written to {timestamped.name} and {LATEST_FILENAME}.")
    current_run.add_file_output(str(timestamped))


def write_json(path: Path, data: dict, compact: bool = False) -> None:
    """Write a JSON file, creating parent folders.

    Args:
        path: Destination path.
        data: Content to serialize.
        compact: Write on a single line (read by the webapp, avoids line-based read limits).
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=None if compact else 2)


if __name__ == "__main__":
    pnlp_integrations_status()
