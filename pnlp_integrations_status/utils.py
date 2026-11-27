"""Helpers for the PNLP integrations status collector: API access, run message parsing and status rules."""

import ast
import hashlib
import json
import os
import re
from datetime import UTC, date, datetime, timedelta

import requests

LEVELS = ["ok", "info", "unknown", "warning", "error"]
ACTIVE_STATUSES = {"running", "queued", "terminating"}
FAILED_STATUSES = {"failed", "stopped"}

RUN_FIELDS = "id status executionDate duration triggerMode"
MESSAGE_FIELDS = "messages { priority message timestamp }"

# Run message patterns shared by the PNLP push pipelines.
RE_SUMMARY = re.compile(r"(?:Push|Final|export|delete) summary:\s*(\{.*\})", re.IGNORECASE)
RE_PROGRESS = re.compile(r"^\s*\d+\s*/\s*\d+\s+data points")
RE_SECTION = re.compile(r"^(.+?) (?:data )?push started", re.IGNORECASE)
RE_PERIODS = re.compile(r"Period\(s\):\s*(.+?)\.?$")
RE_NOTHING = re.compile(r"Nothing to save for period (\S+?)\.?$")
RE_RETRIEVING = re.compile(r"Retrieving indicators extract for period\s*:\s*(\S+)")
RE_NO_NEW = re.compile(
    r"No updates found|No new .* to push|All data points already pushed|Update skipped", re.IGNORECASE
)
RE_NEW_VERSION = re.compile(r"New data version detected", re.IGNORECASE)
RE_LAST_UPDATE = re.compile(r"Last run timestamp updated to:\s*(\S+)")
RE_AVAILABLE = re.compile(r"Last (.+?) pushed date (\S+) - .*?(?:available|until)\s*(?:to|until)?\s*:\s*(\S+)")
RE_DRY_RUN = re.compile(r"dry[ _]run:\s*(True|False)", re.IGNORECASE)
RE_EWARS_FORM = re.compile(r"Extracting the ewars form \d+ for the date (\d{4}-\d{2}-\d{2})")
RE_EWARS_EMPTY = re.compile(r"ewars form \d+ is empty for the date (\d{4}-\d{2}-\d{2})")
RE_TABLE_WEEK = re.compile(r"Last data in dataset (\S+) from year:\s*(\d+) week:\s*(\d+)")
RE_VERSION_TS = re.compile(r"(\d{8}_\d{4})")


class OpenHexaApi:
    """Minimal GraphQL client for the OpenHEXA API."""

    def __init__(self, server_url: str, token: str, workspace_slug: str):
        """Store the API settings.

        Args:
            server_url: Base URL of the OpenHEXA API (without `/graphql/`).
            token: Bearer token used to authenticate.
            workspace_slug: Slug of the workspace holding the tracked pipelines.
        """
        self.url = f"{server_url.rstrip('/')}/graphql/"
        self.token = token
        self.workspace_slug = workspace_slug

    @classmethod
    def from_environment(cls, workspace_slug: str) -> "OpenHexaApi":
        """Build a client from the environment.

        Inside an OpenHEXA run the SDK variables `HEXA_SERVER_URL` / `HEXA_TOKEN` are used. For local runs, set
        `PNLP_STATUS_API_URL` / `PNLP_STATUS_API_TOKEN` instead, so the SDK does not try to report to a run.

        Args:
            workspace_slug: Slug of the workspace holding the tracked pipelines.

        Returns:
            A configured API client.
        """
        url = os.environ.get("HEXA_SERVER_URL") or os.environ.get("PNLP_STATUS_API_URL")
        token = os.environ.get("HEXA_TOKEN") or os.environ.get("PNLP_STATUS_API_TOKEN")
        if not url or not token:
            raise RuntimeError("OpenHEXA API credentials not found (HEXA_* or PNLP_STATUS_API_* variables).")
        return cls(server_url=url, token=token, workspace_slug=workspace_slug)

    def query(self, query: str, variables: dict | None = None) -> dict:
        """Execute a GraphQL query.

        Args:
            query: GraphQL query string.
            variables: Query variables.

        Returns:
            The `data` part of the response.
        """
        response = requests.post(
            self.url,
            json={"query": query, "variables": variables or {}},
            headers={"Authorization": f"Bearer {self.token}"},
            timeout=120,
        )
        response.raise_for_status()
        body = response.json()
        if body.get("errors"):
            raise RuntimeError(f"GraphQL errors: {body['errors']}")
        return body["data"]

    def pipelines_runs(self, codes: list[str], per_page: int, with_messages: bool) -> dict[str, dict | None]:
        """Fetch pipeline metadata and latest runs for several pipelines in one request.

        Args:
            codes: Pipeline codes.
            per_page: Number of latest runs to fetch per pipeline.
            with_messages: Include run messages (heavier payload).

        Returns:
            Mapping of pipeline code to pipeline data (None if the pipeline is not found).
        """
        run_fields = RUN_FIELDS + (" " + MESSAGE_FIELDS if with_messages else "")
        parts = [
            f'p{i}: pipelineByCode(workspaceSlug: $ws, code: "{code}") '
            f"{{ id code name schedule runs(page: 1, perPage: {per_page}) {{ items {{ {run_fields} }} }} }}"
            for i, code in enumerate(codes)
        ]
        data = self.query("query($ws: String!) {" + " ".join(parts) + "}", {"ws": self.workspace_slug})
        return {code: data.get(f"p{i}") for i, code in enumerate(codes)}

    def datasets_latest(self, slugs: list[str]) -> dict[str, dict | None]:
        """Fetch the latest version of several datasets (own or linked) in one request.

        Args:
            slugs: Dataset slugs as visible from the workspace.

        Returns:
            Mapping of dataset slug to dataset data (None if not found).
        """
        if not slugs:
            return {}
        parts = [
            f'd{i}: datasetLinkBySlug(workspaceSlug: $ws, datasetSlug: "{slug}") '
            "{ dataset { slug name workspace { slug name } latestVersion { id name createdAt } } }"
            for i, slug in enumerate(slugs)
        ]
        data = self.query("query($ws: String!) {" + " ".join(parts) + "}", {"ws": self.workspace_slug})
        return {slug: (data.get(f"d{i}") or {}).get("dataset") for i, slug in enumerate(slugs)}

    def read_json_files(self, paths: list[str]) -> dict[str, dict | None]:
        """Read several JSON files from the workspace filesystem in one request.

        Args:
            paths: File paths relative to the workspace root.

        Returns:
            Mapping of path to parsed JSON content (None if missing or invalid).
        """
        if not paths:
            return {}
        parts = [
            f'f{i}: readFileContent(workspaceSlug: $ws, filePath: "{path}") {{ success content }}'
            for i, path in enumerate(paths)
        ]
        data = self.query("query($ws: String!) {" + " ".join(parts) + "}", {"ws": self.workspace_slug})
        result = {}
        for i, path in enumerate(paths):
            item = data.get(f"f{i}") or {}
            try:
                result[path] = json.loads(item["content"]) if item.get("success") else None
            except (TypeError, ValueError):
                result[path] = None
        return result

    def glob_files(self, prefix: str, pattern: str) -> list[str]:
        """List workspace files matching a glob pattern under a prefix.

        Args:
            prefix: Folder relative to the workspace root (narrows the search, required on large buckets).
            pattern: Glob pattern relative to `prefix` (e.g. `*/data_*.parquet`).

        Returns:
            Matching file paths relative to the workspace root.
        """
        data = self.query(
            "query($ws: String!, $pattern: String!, $prefix: String!) {"
            " globFiles(workspaceSlug: $ws, pattern: $pattern, prefix: $prefix) { success errors paths } }",
            {"ws": self.workspace_slug, "pattern": f"{prefix.rstrip('/')}/{pattern}", "prefix": prefix},
        )
        result = data["globFiles"]
        if not result["success"]:
            raise RuntimeError(f"globFiles failed: {result['errors']}")
        return result["paths"] or []


def parse_datetime(value: str | None) -> datetime | None:
    """Parse an ISO datetime returned by the API into an aware UTC datetime.

    Args:
        value: ISO formatted string (with or without timezone).

    Returns:
        The parsed datetime, or None.
    """
    if not value:
        return None
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def parse_version_timestamp(value: str | None) -> datetime | None:
    """Extract a `YYYYMMDD_HHMM` timestamp from a dataset version name or state value.

    Args:
        value: Text containing the timestamp.

    Returns:
        The timestamp as an aware UTC datetime, or None.
    """
    match = RE_VERSION_TS.search(value or "")
    if not match:
        return None
    return datetime.strptime(match.group(1), "%Y%m%d_%H%M").replace(tzinfo=UTC)


def worst_level(levels: list[str]) -> str:
    """Return the most severe status level.

    Args:
        levels: Status levels (see `LEVELS`).

    Returns:
        The most severe level, `ok` when the list is empty.
    """
    return max(levels, key=LEVELS.index, default="ok")


def fingerprint(pipelines: dict[str, dict | None], datasets: dict[str, dict | None]) -> str:
    """Compute a fingerprint of the latest runs and dataset versions, used to skip unchanged collections.

    Args:
        pipelines: Pipelines with their latest run (see `OpenHexaApi.pipelines_runs`).
        datasets: Datasets with their latest version (see `OpenHexaApi.datasets_latest`).

    Returns:
        A short hash of the tracked state.
    """
    items = []
    for code, pipeline in sorted(pipelines.items()):
        runs = ((pipeline or {}).get("runs") or {}).get("items") or []
        items.append(f"{code}:{runs[0]['id']}:{runs[0]['status']}" if runs else f"{code}:-")
    for slug, dataset in sorted(datasets.items()):
        version = (dataset or {}).get("latestVersion") or {}
        items.append(f"{slug}:{version.get('id', '-')}")
    return hashlib.sha256("|".join(items).encode()).hexdigest()[:16]


def parse_run_messages(messages: list[dict]) -> dict:
    """Extract push evidence from the messages of one run.

    Args:
        messages: Run messages (`priority`, `message`, `timestamp`).

    Returns:
        Parsed evidence: summaries, totals, periods, availability, dry run, errors, EWARS forms and DSE tables.
    """
    parsed = {
        "summaries": [],
        "periods": [],
        "retrieved_periods": [],
        "empty_periods": [],
        "availability": [],
        "tables": [],
        "no_new_data": False,
        "new_version": False,
        "last_update_set": None,
        "dry_run": None,
        "error_count": 0,
        "warning_count": 0,
        "errors": [],
        "ewars_dates": [],
        "ewars_empty_dates": [],
    }
    section = None
    for item in messages:
        text = item["message"].strip()
        priority = item["priority"].upper()
        if priority in {"ERROR", "CRITICAL"}:
            parsed["error_count"] += 1
            if len(parsed["errors"]) < 5:
                parsed["errors"].append(text[:300])
        elif priority == "WARNING":
            parsed["warning_count"] += 1

        if match := RE_SECTION.search(text):
            section = match.group(1).strip()
        if (match := RE_SUMMARY.search(text)) and not RE_PROGRESS.search(text):
            try:
                counts = ast.literal_eval(match.group(1))
                parsed["summaries"].append({"section": section, **{k: int(v) for k, v in counts.items()}})
            except (ValueError, SyntaxError):
                pass
        if match := RE_PERIODS.search(text):
            parsed["periods"].extend(p.strip() for p in match.group(1).split(",") if p.strip())
        if match := RE_RETRIEVING.search(text):
            parsed["retrieved_periods"].append(match.group(1))
        if match := RE_NOTHING.search(text):
            parsed["empty_periods"].append(match.group(1))
        if RE_NO_NEW.search(text):
            parsed["no_new_data"] = True
        if RE_NEW_VERSION.search(text):
            parsed["new_version"] = True
        if match := RE_LAST_UPDATE.search(text):
            parsed["last_update_set"] = match.group(1)
        if match := RE_AVAILABLE.search(text):
            parsed["availability"].append(
                {"label": match.group(1), "pushed": match.group(2), "available": match.group(3)}
            )
        if match := RE_DRY_RUN.search(text):
            parsed["dry_run"] = bool(parsed["dry_run"]) or match.group(1).lower() == "true"
        if match := RE_EWARS_FORM.search(text):
            parsed["ewars_dates"].append(match.group(1))
        if match := RE_EWARS_EMPTY.search(text):
            parsed["ewars_empty_dates"].append(match.group(1))
        if match := RE_TABLE_WEEK.search(text):
            parsed["tables"].append({"table": match.group(1), "year": int(match.group(2)), "week": int(match.group(3))})

    parsed["periods"] = sorted(set(parsed["periods"]))
    parsed["totals"] = {
        key: sum(s.get(key, 0) for s in parsed["summaries"]) for key in ("imported", "updated", "ignored", "deleted")
    }
    return parsed


def run_url(app_url: str, workspace_slug: str, code: str, run_id: str) -> str:
    """Build the OpenHEXA URL of a pipeline run.

    Args:
        app_url: OpenHEXA web app base URL.
        workspace_slug: Workspace slug.
        code: Pipeline code.
        run_id: Run identifier.

    Returns:
        The run URL.
    """
    return f"{app_url}/workspaces/{workspace_slug}/pipelines/{code}/runs/{run_id}"


def summarize_run(run: dict, app_url: str, workspace_slug: str, code: str) -> dict:
    """Keep the run fields displayed by the dashboard.

    Args:
        run: Run data from the API.
        app_url: OpenHEXA web app base URL.
        workspace_slug: Workspace slug.
        code: Pipeline code.

    Returns:
        Compact run description.
    """
    return {
        "id": run["id"],
        "status": run["status"],
        "date": run["executionDate"],
        "duration": run.get("duration"),
        "trigger": run.get("triggerMode"),
        "url": run_url(app_url, workspace_slug, code, run["id"]),
    }


def evaluate_step(step_config: dict, pipeline: dict | None, now: datetime, app_url: str, workspace_slug: str) -> dict:
    """Build the status of one pipeline step from its latest runs.

    Args:
        step_config: Step entry of the flow configuration (`code`, `role`, `interval_days`).
        pipeline: Pipeline data with runs and messages (None if not found).
        now: Reference time.
        app_url: OpenHEXA web app base URL.
        workspace_slug: Workspace slug.

    Returns:
        Step status with last run, last success, last push evidence and flags.
    """
    code = step_config["code"]
    step = {
        "code": code,
        "role": step_config["role"],
        "name": code,
        "url": f"{app_url}/workspaces/{workspace_slug}/pipelines/{code}",
        "schedule": None,
        "interval_days": step_config["interval_days"],
        "last_run": None,
        "last_finished_run": None,
        "last_success": None,
        "last_push": None,
        "evidence": None,
        "flags": [],
        "status": "unknown",
    }
    if pipeline is None:
        step["flags"].append({"level": "error", "message": f"Pipeline {code} introuvable dans le workspace."})
        step["status"] = "error"
        return step

    step["name"] = pipeline["name"]
    step["schedule"] = pipeline.get("schedule")
    runs = (pipeline.get("runs") or {}).get("items") or []
    if not runs:
        step["flags"].append({"level": "warning", "message": "Aucune exécution trouvée."})
        step["status"] = "warning"
        return step

    step["last_run"] = summarize_run(runs[0], app_url, workspace_slug, code)
    finished = [r for r in runs if r["status"] not in ACTIVE_STATUSES]
    success = next((r for r in runs if r["status"] == "success"), None)
    if success:
        step["last_success"] = success["executionDate"]

    if finished:
        last = finished[0]
        step["last_finished_run"] = summarize_run(last, app_url, workspace_slug, code)
        evidence = parse_run_messages(last.get("messages") or [])
        step["evidence"] = evidence
        if last["status"] in FAILED_STATUSES:
            step["flags"].append({"level": "error", "message": f"Dernière exécution en échec ({last['status']})."})
        elif evidence["error_count"]:
            step["flags"].append(
                {
                    "level": "warning",
                    "message": f"{evidence['error_count']} message(s) d'erreur dans la dernière exécution.",
                }
            )
        if evidence["dry_run"]:
            step["flags"].append({"level": "warning", "message": "Push exécuté en mode test (dry run)."})

    for run in finished:
        evidence = parse_run_messages(run.get("messages") or [])
        if evidence["summaries"]:
            step["last_push"] = {
                "run": summarize_run(run, app_url, workspace_slug, code),
                "totals": evidence["totals"],
                "summaries": evidence["summaries"],
                "periods": evidence["periods"],
            }
            break

    interval = timedelta(days=step_config["interval_days"])
    last_date = parse_datetime(runs[0]["executionDate"])
    if last_date and now - last_date > 2 * interval:
        step["flags"].append({"level": "error", "message": f"Aucune exécution depuis {(now - last_date).days} jours."})
    elif success is None or now - parse_datetime(success["executionDate"]) > 2 * interval:
        step["flags"].append({"level": "error", "message": "Aucune exécution réussie récente."})

    step["status"] = worst_level([f["level"] for f in step["flags"]])
    return step


def check_state_dates(check: dict, states: dict, steps: dict, now: datetime) -> tuple[list[dict], list[dict]]:
    """Compare the dates stored in a push state file with today and with the data available at the source.

    Args:
        check: Check configuration (`path`, `step`, optional `source_step`, and `keys` with `label`,
            `message_label`, optional `source_table` and `max_age_days`).
        states: Parsed state files by path.
        steps: Evaluated steps by pipeline code.
        now: Reference time.

    Returns:
        Flags and freshness items.
    """
    flags, freshness = [], []
    state = states.get(check["path"])
    if state is None:
        return [{"level": "warning", "message": f"Fichier d'état illisible : {check['path']}"}], []

    evidence = (steps.get(check["step"]) or {}).get("evidence") or {}
    available = {a["label"].lower(): a["available"] for a in evidence.get("availability", [])}
    source_evidence = (steps.get(check.get("source_step")) or {}).get("evidence") or {}
    table_weeks = {t["table"]: f"{t['year']}W{t['week']:02d}" for t in source_evidence.get("tables", [])}
    for key, spec in check["keys"].items():
        value = state.get(key)
        pushed = date.fromisoformat(value) if value else None
        source = table_weeks.get(spec.get("source_table")) or available.get(spec.get("message_label", "").lower())
        item = {"label": spec["label"], "source": source, "pushed": value}
        if pushed and spec.get("source_table"):
            iso = pushed.isocalendar()
            item["pushed_week"] = f"{iso.year}W{iso.week:02d}"
        freshness.append(item)
        if pushed is None:
            flags.append({"level": "warning", "message": f"{spec['label']} : aucune date de push enregistrée."})
        elif pushed > now.date():
            flags.append(
                {
                    "level": "error",
                    "message": f"{spec['label']} : date d'état dans le futur ({value}) — les pushs suivants sont "
                    "ignorés. Corriger le fichier d'état.",
                }
            )
        elif (now.date() - pushed).days > spec["max_age_days"]:
            flags.append(
                {
                    "level": "warning",
                    "message": f"{spec['label']} : dernière donnée poussée le {value} "
                    f"({(now.date() - pushed).days} jours).",
                }
            )
    return flags, freshness


def check_dataset_vs_last_update(
    check: dict, states: dict, datasets: dict, now: datetime
) -> tuple[list[dict], list[dict]]:
    """Compare the latest source dataset version with the version last pushed by the pipeline.

    Args:
        check: Check configuration (`dataset`, `path`, `grace_days`).
        states: Parsed state files by path.
        datasets: Datasets with latest version by slug.
        now: Reference time.

    Returns:
        Flags and freshness items.
    """
    dataset = datasets.get(check["dataset"]) or {}
    version = dataset.get("latestVersion") or {}
    state = states.get(check["path"]) or {}
    last_update = state.get("LAST_UPDATE")
    freshness = [{"label": dataset.get("name", check["dataset"]), "source": version.get("name"), "pushed": last_update}]
    if not version:
        return [{"level": "warning", "message": f"Dataset source {check['dataset']} introuvable."}], freshness
    if not last_update:
        return [{"level": "warning", "message": "Aucune version poussée enregistrée (LAST_UPDATE vide)."}], freshness

    source_ts = parse_version_timestamp(version["name"])
    pushed_ts = parse_version_timestamp(last_update)
    if source_ts and pushed_ts and source_ts > pushed_ts:
        age = (now - parse_datetime(version["createdAt"])).days
        level = "warning" if age > check["grace_days"] else "info"
        return [
            {"level": level, "message": f"Nouvelle version source {version['name']} non poussée ({age} jours)."}
        ], freshness
    return [], freshness


def check_config_dry_run(check: dict, states: dict) -> tuple[list[dict], list[dict]]:
    """Flag a pipeline whose configuration file enables dry run.

    Args:
        check: Check configuration (`path`, `key`).
        states: Parsed configuration files by path.

    Returns:
        Flags and freshness items.
    """
    config = states.get(check["path"])
    if config is None:
        return [{"level": "warning", "message": f"Configuration illisible : {check['path']}"}], []
    if config.get(check["key"]) is True:
        return [
            {
                "level": "warning",
                "message": f"{check['key']}=true dans {check['path']} : les données ne sont pas écrites dans NMDR.",
            }
        ], []
    return [], []


def check_ewars_forms(check: dict, steps: dict, now: datetime) -> tuple[list[dict], list[dict]]:
    """Find the latest EWARS form date with data and flag it when too old.

    Args:
        check: Check configuration (`step`, `max_age_days`).
        steps: Evaluated steps by pipeline code.
        now: Reference time.

    Returns:
        Flags and freshness items.
    """
    step = steps.get(check["step"]) or {}
    evidence = step.get("evidence") or {}
    with_data = sorted(set(evidence.get("ewars_dates", [])) - set(evidence.get("ewars_empty_dates", [])))
    latest = with_data[-1] if with_data else None
    pushed = ((step.get("last_push") or {}).get("run") or {}).get("date")
    freshness = [{"label": "Formulaires EWARS", "source": latest, "pushed": pushed[:10] if pushed else None}]
    if latest is None:
        return [{"level": "warning", "message": "Aucun formulaire EWARS avec des données dans la fenêtre."}], freshness
    age = (now.date() - date.fromisoformat(latest)).days
    if age > check["max_age_days"]:
        return [
            {"level": "warning", "message": f"Dernier formulaire EWARS avec données : {latest} ({age} jours)."}
        ], freshness
    return [], freshness


def check_source_periods(check: dict, steps: dict) -> tuple[list[dict], list[dict]]:
    """Flag an extraction where every requested period came back empty from the source.

    Args:
        check: Check configuration (`step`).
        steps: Evaluated steps by pipeline code.

    Returns:
        Flags and freshness items.
    """
    step = steps.get(check["step"]) or {}
    evidence = step.get("evidence") or {}
    retrieved = sorted(set(evidence.get("retrieved_periods", [])))
    empty = set(evidence.get("empty_periods", []))
    with_data = [p for p in retrieved if p not in empty]
    if retrieved and not with_data:
        run_date = ((step.get("last_finished_run") or {}).get("date") or "")[:10]
        return [
            {
                "level": "warning",
                "message": f"Extraction du {run_date} : les périodes demandées ({retrieved[0]} à {retrieved[-1]}) "
                "n'ont renvoyé aucune donnée depuis la source.",
            }
        ], []
    return [], []


def check_latest_period_file(check: dict, listings: dict, steps: dict, now: datetime) -> tuple[list[dict], list[dict]]:
    """Find the most recent monthly period among output files and flag it when it lags behind the current month.

    Args:
        check: Check configuration (`prefix`, `pattern`, `period_regex`, `max_lag_months`, `label`, `step`).
        listings: File paths found for each check, keyed by `prefix` + `pattern`.
        steps: Evaluated steps by pipeline code.
        now: Reference time.

    Returns:
        Flags and freshness items.
    """
    paths = listings.get(check["prefix"] + check["pattern"])
    step = steps.get(check.get("step")) or {}
    pushed = ((step.get("last_push") or {}).get("run") or {}).get("date")
    if paths is None:
        return [{"level": "warning", "message": f"Impossible de lister les fichiers de {check['prefix']}."}], []

    regex = re.compile(check["period_regex"])
    periods = sorted({m.group(1) for p in paths if (m := regex.search(p.rsplit("/", 1)[-1]))})
    latest = periods[-1] if periods else None
    freshness = [{"label": check["label"], "source": latest, "pushed": pushed[:10] if pushed else None}]
    if latest is None:
        return [{"level": "warning", "message": f"{check['label']} : aucun fichier trouvé."}], freshness

    year, month = int(latest[:4]), int(latest[4:6])
    lag = (now.year * 12 + now.month) - (year * 12 + month)
    if lag > check["max_lag_months"]:
        return [
            {
                "level": "warning",
                "message": f"{check['label']} : dernière donnée disponible {year}-{month:02d} ({lag} mois de retard).",
            }
        ], freshness
    return [], freshness


def evaluate_flow(
    flow_config: dict,
    steps: dict,
    datasets: dict,
    states: dict,
    listings: dict,
    now: datetime,
    app_url: str,
    workspace_slug: str,
) -> dict:
    """Build the status of one data flow from its steps, datasets, state files and checks.

    Args:
        flow_config: Flow entry of the configuration.
        steps: Evaluated steps by pipeline code.
        datasets: Datasets with latest version by slug.
        states: Parsed state/config files by path.
        listings: Output file paths by check key (`prefix` + `pattern`).
        now: Reference time.
        app_url: OpenHEXA web app base URL.
        workspace_slug: Workspace slug.

    Returns:
        Flow status ready to be serialized.
    """
    flags = [dict(f) for f in flow_config.get("flags", [])]
    freshness = []
    for check in flow_config.get("checks", []):
        kind = check["type"]
        if kind == "state_dates":
            new_flags, new_fresh = check_state_dates(check, states, steps, now)
        elif kind == "dataset_vs_last_update":
            new_flags, new_fresh = check_dataset_vs_last_update(check, states, datasets, now)
        elif kind == "config_dry_run":
            new_flags, new_fresh = check_config_dry_run(check, states)
        elif kind == "ewars_forms":
            new_flags, new_fresh = check_ewars_forms(check, steps, now)
        elif kind == "source_periods":
            new_flags, new_fresh = check_source_periods(check, steps)
        elif kind == "latest_period_file":
            new_flags, new_fresh = check_latest_period_file(check, listings, steps, now)
        else:
            new_flags, new_fresh = [{"level": "warning", "message": f"Type de contrôle inconnu : {kind}"}], []
        flags.extend(new_flags)
        freshness.extend(new_fresh)

    flow_steps = [steps[s["code"]] for s in flow_config["steps"]]
    flow_datasets = []
    for entry in flow_config.get("datasets", []):
        dataset = datasets.get(entry["slug"]) or {}
        version = dataset.get("latestVersion") or {}
        source_ws = (dataset.get("workspace") or {}).get("slug", workspace_slug)
        flow_datasets.append(
            {
                "slug": entry["slug"],
                "name": dataset.get("name", entry["slug"]),
                "workspace": (dataset.get("workspace") or {}).get("name"),
                "produced_by": entry.get("produced_by"),
                "latest_version": version.get("name"),
                "latest_version_date": version.get("createdAt"),
                "url": f"{app_url}/workspaces/{workspace_slug}/datasets/{entry['slug']}/from/{source_ws}/",
            }
        )

    push_steps = [s for s in flow_steps if s["role"] == "push" and s.get("last_push")]
    last_push = max((s["last_push"] for s in push_steps), key=lambda p: p["run"]["date"], default=None)
    status = worst_level([f["level"] for f in flags] + [s["status"] for s in flow_steps])
    return {
        "id": flow_config["id"],
        "label": flow_config["label"],
        "source": flow_config["source"],
        "status": status,
        "flags": flags,
        "freshness": freshness,
        "last_push": last_push,
        "datasets": flow_datasets,
        "connections": flow_config.get("connections", []),
        "steps": [s | {"evidence": compact_evidence(s)} for s in flow_steps],
    }


def compact_evidence(step: dict) -> dict | None:
    """Keep the parsed evidence fields worth displaying (drops raw date lists).

    Args:
        step: Evaluated step.

    Returns:
        Compact evidence, or None when the step has no finished run.
    """
    evidence = step.get("evidence")
    if not evidence:
        return None
    keep = ("no_new_data", "new_version", "last_update_set", "dry_run", "error_count", "warning_count", "errors")
    compact = {k: evidence[k] for k in keep}
    compact["availability"] = evidence["availability"]
    compact["tables"] = evidence["tables"]
    compact["periods"] = evidence["periods"]
    compact["empty_periods"] = sorted(set(evidence["empty_periods"]))
    return compact
