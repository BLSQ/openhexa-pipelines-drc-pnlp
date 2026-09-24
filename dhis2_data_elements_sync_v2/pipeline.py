from datetime import datetime
from pathlib import Path

import pandas as pd
import polars as pl
from d2d_development.push import DHIS2Pusher
from openhexa.sdk import current_run, parameter, pipeline, workspace
from org_units_aligner.org_units_aligner import DHIS2PyramidAligner
from utils import (
    configure_logging,
    connect_to_dhis2,
    get_dataset_version_timestamp,
    get_file_from_dataset,
    get_matching_filenames_from_dataset,
    load_configuration,
    read_json_file,
    save_json_file,
    save_logs,
)


@pipeline("dhis2_data_elements_sync_v2", timeout=28800)
@parameter(
    "ou_sync",
    name="Run org units sync",
    type=bool,
    default=True,
    help="Run organisation units sync.",
)
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
def dhis2_data_elements_sync_v2(ou_sync: bool, push_analytics: bool, force_run: bool = False) -> None:
    """Pipeline to synchronizes data elements from SNIS to NMDR DHIS2 instance."""
    pipeline_path = Path(workspace.files_path) / "pipelines" / "dhis2_data_elements_sync_v2"

    # check updated data in dataset (shared from DRC DSNIS)
    data_elements_dataset_id = "snis-data-elements-extracts"
    to_update = should_push_data(
        dataset_id=data_elements_dataset_id, timestamp_path=pipeline_path / "config" / "last_update.json"
    )

    if to_update or force_run:
        current_run.log_info("New data version detected. Starting pipeline execution...")
        try:
            sync_organisation_units(
                pipeline_path=pipeline_path,
                dataset_id=data_elements_dataset_id,
                run_task=ou_sync,
            )

            push_extracts(
                pipeline_path=pipeline_path,
                dataset_id=data_elements_dataset_id,
                run_task=push_analytics,
            )

            update_last_run_timestamp(
                timestamp_filename=pipeline_path / "config" / "last_update.json",
                dataset_id=data_elements_dataset_id,
            )

        except Exception as e:
            current_run.log_error(f"An error occurred: {e}")
            raise
    else:
        current_run.log_info("No updates found. Pipeline execution skipped.")


def should_push_data(dataset_id: str, timestamp_path: Path) -> bool:
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
        last_update = read_json_file(timestamp_path)
        last_update_str = last_update.get("LAST_UPDATE", "")
        last_update_dt = datetime.strptime(last_update_str, "%Y%m%d_%H%M") if last_update_str else None
    except Exception as e:
        current_run.log_warning(f"Error reading last update timestamp: Running update by default. Error: {e}")
        return True  # If we can't read the last update, assume we need to update

    return not last_update_dt or new_version_dt > last_update_dt


def sync_organisation_units(pipeline_path: Path, dataset_id: str, run_task: bool) -> None:
    """Synchronizes organisation units from SNIS to NMDR DHIS2 instance."""
    if not run_task:
        current_run.log_info("Organisation units sync skipped.")
        return

    current_run.log_info("Starting organisation units synchronization...")
    logger, logs_file = configure_logging(logs_path=Path("/home/jovyan/tmp/logs"), task_name="sync_orgunits")
    # logger, logs_file = configure_logging(logs_path=pipeline_path / "logs", task_name="sync_orgunits")

    # load configuration
    config_push = load_configuration(config_path=pipeline_path / "config" / "push_config.json")

    # connect to DHIS2 instances
    dhis2_client_target = connect_to_dhis2(connection_str=config_push["SETTINGS"]["DHIS2_CONNECTION"])

    # Load pyramid from dataset (extracted from DSNIS workspace pipeline: dhis2_snis_data_elements_extract)
    dhis2_pyramid_source = get_file_from_dataset(dataset_id=dataset_id, filename="snis_pyramid.parquet")

    try:
        DHIS2PyramidAligner(logger=logger, logging_interval=5000).align_to(
            target_dhis2=dhis2_client_target,
            source_pyramid=dhis2_pyramid_source,
        )
    finally:
        save_logs(logs_file, output_dir=pipeline_path / "logs" / "push_orgunits")

    current_run.log_info("Organisation units synchronization completed.")


def push_extracts(pipeline_path: Path, dataset_id: str, run_task: bool) -> None:
    """Pushes data elements from SNIS to NMDR DHIS2 instance."""
    if not run_task:
        current_run.log_info("Data elements push skipped.")
        return

    current_run.log_info("Starting data elements push...")
    logger, logs_file = configure_logging(logs_path=Path("/home/jovyan/tmp/logs"), task_name="push_data_elements")
    # logger, logs_file = configure_logging(logs_path=pipeline_path / "logs", task_name="push_data_elements")  # local

    # load configuration
    config_push = load_configuration(config_path=pipeline_path / "config" / "push_config.json")

    # connect to DHIS2 instances
    dhis2_client_target = connect_to_dhis2(connection_str=config_push["SETTINGS"]["DHIS2_CONNECTION"])

    # Load data elements from dataset (extracted from DSNIS workspace pipeline: dhis2_snis_data_elements_extract)
    current_run.log_info(f"Loading data from dataset: {dataset_id}")
    extract_filenames = get_matching_filenames_from_dataset(dataset_id=dataset_id, pattern="data_*.parquet")

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
        cache_path=pipeline_path / "cache",
    )

    for filename in extract_filenames:
        current_run.log_info(f"Processing analytics file: {filename}")
        logger.info(f"Processing analytics file: {filename}")  # should be included in the Push class
        try:
            df_data = get_file_from_dataset(dataset_id=dataset_id, filename=filename)
            df_mapped = apply_data_element_mappings(
                df=df_data,
                mappings=config_push.get("DATAELEMENT_MAPPINGS", {}),
                coc_default=config_push.get("CAT_OPTION_COMBO_DEFAULT", {}).get("default", {}),
                aoc_default=config_push.get("ATTR_OPTION_COMBO_DEFAULT", {}).get("default", {}),
            )
        except Exception as e:
            raise Exception(
                f"Error applying data element mappings for extract {filename}, stopping push process. Error: {e}"
            ) from e

        # Sort the dataframe by org_unit to reduce import time (hopefully)
        df_mapped = df_mapped.sort(by="org_unit", descending=False)
        df_mapped = df_mapped.with_columns(
            pl.when(pl.col("value") == "None").then(None).otherwise(pl.col("value")).alias("value")
        )
        try:
            pusher.push_data(df_data=df_mapped)
            current_run.log_info(f"Data elements data push finished for extract: {filename}.")
        except Exception as e:
            raise Exception(f"Error for extract {filename}), stopping push process. Error: {e}") from e
        finally:
            save_logs(logs_file, output_dir=pipeline_path / "logs" / "push_data_elements")

    current_run.log_info("Data elements push completed.")


def apply_data_element_mappings(df: pd.DataFrame, mappings: dict, coc_default: str, aoc_default: str) -> pl.DataFrame:
    """All matching ids will be replaced. Is user responsability to provide the correct UIDS.

    This is a filter, not an in-place update: the returned DataFrame contains only rows with
    data_type == "DATA_ELEMENT" whose dx is a key in `mappings` and whose category_option_combo
    is one of that mapping's CAT_OPTION_COMBO keys. Every other row (non-DATA_ELEMENT rows, or
    DATA_ELEMENT rows not covered by `mappings`) is dropped from the result.

    Returns:
        pl.DataFrame: DataFrame with the mapped COC and AOC values.
    """
    df = pl.from_pandas(df)
    df = df.filter(df["data_type"] == "DATA_ELEMENT")  # DE mapppings

    # Loop over the DataElement COC mappings
    chunks = []
    for uid, mapping in mappings.items():
        uid_str = str(uid).strip()
        df_de = df.filter(df["dx"] == uid_str)

        if df_de.is_empty():
            current_run.log_warning(f"No matching data for data element: {uid}.")
            continue

        coc_uids_map = {str(k).strip(): str(v).strip() for k, v in mapping.get("CAT_OPTION_COMBO", {}).items()}
        df_de = df_de.filter(df_de["category_option_combo"].is_in(list(coc_uids_map.keys())))

        if df_de.is_empty():
            current_run.log_warning(f"No matching category option combo for data element: {uid}.")
            continue

        df_de = df_de.with_columns(pl.col("category_option_combo").replace(coc_uids_map))

        # for AOC there are no mappings (we could repeat the logic avobe if there was something to filter)
        chunks.append(df_de)

    if len(chunks) == 0:
        current_run.log_warning("No data elements matched the provided mappings.")
        df_coc_mapped = df.clear()  # empty df, same schema as df
    else:
        df_coc_mapped = pl.concat(chunks)

    # set all AOC None to Default values
    if aoc_default:
        df_coc_mapped = df_coc_mapped.with_columns(pl.col("attribute_option_combo").fill_null(pl.lit(aoc_default)))

    return df_coc_mapped


def update_last_run_timestamp(timestamp_filename: Path, dataset_id: str) -> None:
    """Updates the last run timestamp in the JSON file."""
    timestamp = get_dataset_version_timestamp(dataset_id=dataset_id)
    try:
        save_json_file(
            file_path=timestamp_filename,
            contents={"LAST_UPDATE": timestamp.strftime("%Y%m%d_%H%M")},
        )
        current_run.log_info(f"Last run timestamp updated to: {timestamp.strftime('%Y%m%d_%H%M')}")
    except Exception as e:
        current_run.log_error(f"Error updating last run timestamp: {e}")


if __name__ == "__main__":
    dhis2_data_elements_sync_v2()
