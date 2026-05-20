from datetime import datetime
from pathlib import Path

import pandas as pd
from d2d_development.push import DHIS2Pusher
from openhexa.sdk import current_run, parameter, pipeline, workspace
from utils import (
    configure_logging,
    connect_to_dhis2,
    get_dataset_version_timestamp,
    get_file_from_dataset,
    load_configuration,
    read_json_file,
    save_logs,
)


@pipeline("dhis2_snis_sentinel_pnlp_sync_v2")
@parameter(
    "push_analytics",
    name="Push data elements",
    type=bool,
    default=True,
    help="Run the data elements push task.",
)
@parameter(
    "force_run",
    name="Force run",
    help="Force the pipeline to run even if no new data is detected.",
    type=bool,
    default=False,
    required=False,
)
def dhis2_snis_sentinel_pnlp_sync_v2(push_analytics: bool, force_run: bool) -> None:
    """Pipeline to synchronizes sentinel org units data elements from SNIS to NMDR DHIS2 instance."""
    pipeline_path = Path(workspace.files_path) / "pipelines" / "dhis2_snis_sentinel_pnlp_sync_v2"

    # check updated data in dataset
    sentinel_dataset_id = "snis-sentinel-dataset"
    to_update = should_push_data(pipeline_path=pipeline_path, dataset_id=sentinel_dataset_id)

    if to_update or force_run:
        try:
            push_extracts(
                pipeline_path=pipeline_path,
                dataset_id=sentinel_dataset_id,
                run_task=push_analytics,
            )

        except Exception as e:
            current_run.log_error(f"An error occurred: {e}")
            raise
    else:
        current_run.log_info("No updates found. Pipeline execution skipped.")

    current_run.log_info("Pipeline execution completed.")


def should_push_data(pipeline_path: Path, dataset_id: str) -> bool:
    """Check if new data is available by comparing the latest dataset version timestamp.

    Returns:
        bool: True if an update is needed, False if data is up to date or on error.
    """
    try:
        new_version_dt = get_dataset_version_timestamp(dataset_id=dataset_id)
    except Exception as e:
        current_run.log_error(f"{e}")
        return False

    # read last run timestamp from file
    try:
        last_update = read_json_file(pipeline_path / "config" / "last_update.json")
        last_update_str = last_update.get("LAST_UPDATE", "")
        last_update_dt = datetime.strptime(last_update_str, "%Y%m%d_%H%M") if last_update_str else None
    except Exception as e:
        current_run.log_error(f"Error reading last update timestamp: Running update by default. Error: {e}")
        return True  # If we can't read the last update, assume we need to update

    if not last_update_dt or new_version_dt > last_update_dt:
        current_run.log_info("New data version detected. Update needed.")
        return True

    current_run.log_info("Data is up to date. No update needed.")
    return False


def push_extracts(pipeline_path: Path, dataset_id: str, run_task: bool = True) -> None:
    """Pushes data points to the target DHIS2 instance."""
    if not run_task:
        current_run.log_info("Data push task skipped.")
        return

    current_run.log_info("Starting data push.")

    logger, logs_file = configure_logging(logs_path=Path("/home/jovyan/tmp/logs"), task_name="push_data")
    # logger, logs_file = configure_logging(logs_path=pipeline_path / "logs", task_name="push_data")

    # load configuration
    config_push = load_configuration(config_path=pipeline_path / "config" / "push_config.json")
    dhis2_client_target = connect_to_dhis2(connection_str=config_push["SETTINGS"]["DHIS2_CONNECTION"])
    current_run.log_info(f"Loading data from dataset: {dataset_id}")

    # Parameters for the api call
    import_strategy = config_push["SETTINGS"].get("IMPORT_STRATEGY", "CREATE_AND_UPDATE")
    dry_run = config_push["SETTINGS"].get("DRY_RUN", True)
    max_post = config_push["SETTINGS"].get("MAX_POST", 500)

    # log parameters
    current_run.log_info(f"Import strategy: {import_strategy} - Dry Run: {dry_run} - Max post: {max_post}")

    pusher = DHIS2Pusher(
        dhis2_client=dhis2_client_target,
        import_strategy=import_strategy,
        dry_run=dry_run,
        max_post=max_post,
        logger=logger,
    )

    # Get files to push from updates_collector.json file in the dataset.
    files_to_push = get_file_from_dataset(dataset_id=dataset_id, filename="updates_collector.json")
    for node, file_list in files_to_push.items():
        current_run.log_info(f"Extract: {node} - Files to push: {file_list}")
        logger.info(f"Extract: {node} - Files to push: {file_list}")
        extract_config = find_extract_by_uid(
            extracts=config_push.get("DATA_ELEMENTS", {}).get("EXTRACTS", []), extract_uid=node
        )

        if extract_config is None:
            current_run.log_warning(f"No extract configuration found for node {node}, skipping.")
            continue

        for filename in file_list:
            current_run.log_info(f"Processing analytics file: {filename}")
            try:
                # Load data from dataset (extracted from DSNIS workspace pipeline: dhis2-snis-sentinel-extract)
                df_data = get_file_from_dataset(dataset_id=dataset_id, filename=filename)
                df_mapped = apply_data_element_mappings(
                    df=df_data,
                    extract=extract_config,
                )

                # Sort the dataframe by org_unit to reduce import time (hopefully)
                df_mapped = df_mapped.sort_values(by=["org_unit"], ascending=True)
                df_mapped["value"] = df_mapped["value"].replace("None", pd.NA)  # Ensure string "None" is treated as NA
                pusher.push_data(df_data=df_mapped)
                current_run.log_info(f"Data elements data push finished for extract: {filename}.")
            except Exception as e:
                raise Exception(f"Error for extract {filename}), stopping push process. Error: {e}") from e
            finally:
                save_logs(logs_file, output_dir=pipeline_path / "logs" / "push_data_elements")

    current_run.log_info("Data elements push completed.")


def find_extract_by_uid(extracts: list, extract_uid: str) -> dict | None:
    """Finds the extract configuration by its UID in the provided configuration.

    Returns:
        dict: The extract configuration if found, otherwise None.
    """
    for extract in extracts:
        if extract.get("EXTRACT_UID") == extract_uid:
            return extract

    return None


def apply_data_element_mappings(
    df: pd.DataFrame,
    extract: dict,
) -> pd.DataFrame:
    """Applies indicator mappings to the extracted data.

    NOTE: It also FILTERS! indicator based on category option combo (COC) if specified in the extract configuration.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame containing the extracted data.
    extract : dict
        This is a dictionary containing the extract mappings.

    Returns
    -------
    pd.DataFrame
        DataFrame with mapped indicator.
    """
    if len(extract) == 0:
        current_run.log_warning("No extract details provided, skipping indicator mappings.")
        return df

    extract_mappings = extract.get("MAPPINGS", {})
    if len(extract_mappings) == 0:
        current_run.log_warning("No extract mappings provided, skipping indicator mappings.")
        return df

    # Loop over the configured indicator mappings to filter by COC/AOC if provided
    chunks = []
    uid_mappings = {}
    for uid, mapping in extract_mappings.items():
        uid_mapping = mapping.get("UID")
        coc_mappings = mapping.get("CATEGORY_OPTION_COMBO", {})
        aoc_mappings = mapping.get("ATTRIBUTE_OPTION_COMBO", {})

        # Build a mask selection
        df_uid = df[df["dx"] == uid].copy()
        if coc_mappings:
            coc_mappings_clean = clean_mapping(coc_mappings)
            df_uid = df_uid[df_uid["category_option_combo"].isin(coc_mappings_clean.keys())]
            df_uid["category_option_combo"] = df_uid.loc[:, "category_option_combo"].replace(coc_mappings_clean)

        if aoc_mappings:
            aoc_mappings_clean = clean_mapping(aoc_mappings)
            df_uid = df_uid[df_uid["attribute_option_combo"].isin(aoc_mappings_clean.keys())]
            df_uid["attribute_option_combo"] = df_uid.loc[:, "attribute_option_combo"].replace(aoc_mappings_clean)

        if uid_mapping:
            uid_mappings[uid] = uid_mapping

        chunks.append(df_uid)

    if len(chunks) == 0:
        current_run.log_warning("No data elements matched the provided mappings, returning empty dataframe.")
        return pd.DataFrame(columns=df.columns)

    df_filtered = pd.concat(chunks, ignore_index=True)

    # set defaults
    df_filtered["category_option_combo"] = df_filtered["category_option_combo"].fillna("HllvX50cXC0")
    df_filtered["attribute_option_combo"] = df_filtered["attribute_option_combo"].fillna("HllvX50cXC0")

    if uid_mappings:
        uid_mappings = {k: v for k, v in uid_mappings.items() if v is not None}  # Do not replace with None
        uid_mappings_clean = {str(k).strip(): str(v).strip() for k, v in uid_mappings.items()}
        df_filtered["dx"] = df_filtered.loc[:, "dx"].replace(uid_mappings_clean)

    return df_filtered


def clean_mapping(mapping: dict) -> dict:
    """Cleans the provided mapping by removing entries with None values and stripping whitespace.

    Returns:
        dict: Cleaned mapping dictionary.
    """
    return {str(k).strip(): str(v).strip() for k, v in mapping.items() if v is not None}


if __name__ == "__main__":
    dhis2_snis_sentinel_pnlp_sync_v2()
