import re
from datetime import datetime
from pathlib import Path

import pandas as pd
from d2d_development.push import DHIS2Pusher
from openhexa.sdk import current_run, parameter, pipeline, workspace
from openhexa.toolbox.dhis2 import DHIS2
from org_units_aligner.org_units_aligner import DHIS2PyramidAligner
from utils import (
    configure_logging,
    connect_to_dhis2,
    get_file_from_dataset,
    get_matching_filenames_from_dataset,
    load_configuration,
    read_json_file,
    save_json_file,
    save_logs,
)


@pipeline("dhis2_pnlp_push_v2", timeout=36000)
@parameter(
    "push_orgunits",
    name="Push Organisation Units",
    help="",
    type=bool,
    default=True,
    required=False,
)
@parameter(
    "push_pop",
    name="Push population",
    help="",
    type=bool,
    default=True,
    required=False,
)
@parameter(
    "push_analytics_task",
    name="Push analytics",
    help="",
    type=bool,
    default=True,
    required=False,
)
@parameter(
    "force_run",
    name="Force run",
    help="Force the pipeline to run even if no new data is detected.",
    type=bool,
    default=False,
    required=False,
)
def dhis2_pnlp_push_v2(push_orgunits: bool, push_pop: bool, push_analytics_task: bool, force_run: bool = False) -> None:
    """Push the extracted data from DRC DSNIS ws shared via dataset to PNLP DHIS2 (NMDR)."""
    pipeline_path = Path(workspace.files_path) / "pipelines" / "dhis2_pnlp_push_v2"
    pipeline_path.mkdir(parents=True, exist_ok=True)

    try:
        config = load_configuration(pipeline_path / "config" / "pnlp_push_config.json")
        dhis2_client = connect_to_dhis2(connection_str=config["SETTINGS"].get("DHIS2_CONNECTION"))

        # check updated data in dataset
        to_update = should_push_data(
            dataset_id=config["SETTINGS"].get("OPENHEXA_DATASET_ID"),
            timestamp_path=pipeline_path / "config" / "last_update.json",
        )

        if to_update or force_run:
            current_run.log_info("New data version detected. Starting pipeline execution...")
            push_organisation_units(
                pipeline_path=pipeline_path,
                dhis2_client_target=dhis2_client,
                config=config,
                run_task=push_orgunits,
            )

            push_population(
                pipeline_path=pipeline_path,
                dhis2_client_target=dhis2_client,
                config=config,
                run_task=push_pop,
            )

            push_analytics(
                pipeline_path=pipeline_path,
                dhis2_client_target=dhis2_client,
                config=config,
                run_task=push_analytics_task,
            )

            update_last_run_timestamp(
                timestamp_filename=pipeline_path / "config" / "last_update.json",
                dataset_id=config["SETTINGS"].get("OPENHEXA_DATASET_ID"),
            )
        else:
            current_run.log_info("No updates found. Pipeline execution skipped.")

    except Exception as e:
        current_run.log_error(f"An error occurred: {e}")


def push_organisation_units(pipeline_path: str, dhis2_client_target: DHIS2, config: dict, run_task: bool) -> None:
    """Pushes organisation units to the target DHIS2 instance.

    Args:
        pipeline_path (str): The root path for the pipeline.
        dhis2_client_target (DHIS2): The DHIS2 client for the target instance.
        config (dict): The configuration dictionary containing necessary settings.
        run_task (bool): Whether to execute the task or just prepare it.
    """
    if not run_task:
        current_run.log_info("Organisation units push task is disabled. Skipping.")
        return

    current_run.log_info("Starting organisation units push.")
    logger, logs_file = configure_logging(logs_path=Path("/home/jovyan/tmp/logs"), task_name="push_orgunits")
    # logger, logs_file = configure_logging(logs_path=pipeline_path / "logs", task_name="push_orgunits")  # local

    # Load from dataset
    dhis2_pyramid_source = get_file_from_dataset(config["SETTINGS"].get("OPENHEXA_DATASET_ID"), "snis_pyramid.parquet")

    try:
        DHIS2PyramidAligner(logger=logger, logging_interval=5000).align_to(
            target_dhis2=dhis2_client_target,
            source_pyramid=dhis2_pyramid_source,
        )
    finally:
        save_logs(logs_file, output_dir=pipeline_path / "logs" / "push_orgunits")


def extract_timestamp_from_version_name(version_name: str) -> datetime:
    """Parses the timestamp from a version name.

    Returns:
        datetime: The extracted timestamp as a datetime object.
    """
    match = re.search(r"(\d{8}_\d{4})", version_name)
    if match:
        timestamp_str = match.group(1)
        try:
            return datetime.strptime(timestamp_str, "%Y%m%d_%H%M")
        except ValueError as e:
            raise ValueError(f"Invalid timestamp format in version name: {version_name}") from e
    else:
        raise ValueError(f"No timestamp found in version name: {version_name}")


def get_dataset_version_timestamp(dataset_id: str) -> datetime:
    """Fetch the latest dataset version and extract the timestamp from its name.

    Returns:
        datetime: The extracted timestamp as a datetime object.
    """
    try:
        dataset = workspace.get_dataset(dataset_id)
        version_name = dataset.latest_version.name
        if not version_name:
            raise ValueError("Dataset version name is missing.")
        return extract_timestamp_from_version_name(version_name)
    except Exception as e:
        raise Exception(f"An error occurred while fetching the dataset or extracting the timestamp: {e}") from e


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


def push_population(pipeline_path: str, dhis2_client_target: DHIS2, config: dict, run_task: bool) -> None:
    """Pushes population data to the target DHIS2 instance.

    Args:
        pipeline_path (str): The root path for the pipeline.
        dhis2_client_target (DHIS2): The DHIS2 client for the target instance.
        config (dict): The configuration dictionary containing necessary settings.
        run_task (bool): Whether to execute the task or just prepare it.
    """
    if not run_task:
        current_run.log_info("Population push task is disabled. Skipping.")
        return

    current_run.log_info("Starting population push.")
    logger, logs_file = configure_logging(logs_path=Path("/home/jovyan/tmp/logs"), task_name="push_population")
    # logger, logs_file = configure_logging(logs_path=pipeline_path / "logs", task_name="push_population")  # local

    # Load data from dataset
    pop_filenames = get_matching_filenames_from_dataset(
        dataset_id=config["SETTINGS"].get("OPENHEXA_DATASET_ID"), pattern="snis_population_*.parquet"
    )
    # Parameters for the api call
    import_strategy = config["SETTINGS"].get("IMPORT_STRATEGY", "CREATE_AND_UPDATE")
    dry_run = config["SETTINGS"].get("DRY_RUN", True)
    max_post = config["SETTINGS"].get("MAX_POST", 500)

    # log parameters
    current_run.log_info(f"Import strategy: {import_strategy} - Dry Run: {dry_run} - Max post: {max_post}")

    pusher = DHIS2Pusher(
        dhis2_client=dhis2_client_target,
        import_strategy=import_strategy,
        dry_run=dry_run,
        max_post=max_post,
        logger=logger,
    )

    # logic changed to loop over available files to push.
    for pop_filename in pop_filenames:
        current_run.log_info(f"Processing population file: {pop_filename}")
        try:
            pop_data = get_file_from_dataset(config["SETTINGS"].get("OPENHEXA_DATASET_ID"), pop_filename)

            pop_data = pop_data.sort_values(by=["org_unit"], ascending=True)
            pop_data_mapped = apply_population_mappings(df=pop_data, mappings=config.get("POPULATION_MAPPING"))

            # push data
            pusher.push_data(df_data=pop_data_mapped)
            current_run.log_info(f"Population data push finished for extract: {pop_filename}.")
        except Exception as e:
            raise Exception(f"Error for extract {pop_filename}), stopping push process. Error: {e!s}") from e
        finally:
            save_logs(logs_file, output_dir=pipeline_path / "logs" / "push_population")


def apply_population_mappings(df: pd.DataFrame, mappings: dict) -> pd.DataFrame:
    """Applies the mappings for population data to the given DataFrame.

    Returns:
        pd.DataFrame: The DataFrame with the applied mappings.
    """
    df = df.copy()

    if mappings is None:
        current_run.log_error("No POPULATION_MAPPING found in the configuration file.")
        raise ValueError

    # get values from mappings
    coc_mappings = mappings.get("CATEGORY_OPTION_COMBO", {}).get("DEFAULT", "HllvX50cXC0")
    aoc_mappings = mappings.get("ATTRIBUTE_OPTION_COMBO", {}).get("DEFAULT", "HllvX50cXC0")

    # Complete with defaults
    df["category_option_combo"] = df.loc[:, "category_option_combo"].replace({None: coc_mappings})
    df["attribute_option_combo"] = df.loc[:, "attribute_option_combo"].replace({None: aoc_mappings})

    return df


def push_analytics(pipeline_path: str, dhis2_client_target: DHIS2, config: dict, run_task: bool) -> None:
    """Pushes analytics data to the target DHIS2 instance.

    Args:
        pipeline_path (str): The root path for the pipeline.
        dhis2_client_target (DHIS2): The DHIS2 client for the target instance.
        config (dict): The configuration dictionary containing necessary settings.
        run_task (bool): Whether to execute the task or just prepare it.
    """
    if not run_task:
        current_run.log_info("Analytics push task is disabled. Skipping.")
        return

    current_run.log_info("Starting analytics push.")
    logger, logs_file = configure_logging(logs_path=Path("/home/jovyan/tmp/logs"), task_name="push_analytics")
    # logger, logs_file = configure_logging(logs_path=pipeline_path / "logs", task_name="push_analytics")  # local

    # Load data from dataset
    analytics_filenames = get_matching_filenames_from_dataset(
        dataset_id=config["SETTINGS"].get("OPENHEXA_DATASET_ID"), pattern="snis_data_*.parquet"
    )
    # Parameters for the api call
    import_strategy = config["SETTINGS"].get("IMPORT_STRATEGY", "CREATE_AND_UPDATE")
    dry_run = config["SETTINGS"].get("DRY_RUN", True)
    max_post = config["SETTINGS"].get("MAX_POST", 500)

    # log parameters
    current_run.log_info(f"Import strategy: {import_strategy} - Dry Run: {dry_run} - Max post: {max_post}")

    pusher = DHIS2Pusher(
        dhis2_client=dhis2_client_target,
        import_strategy=import_strategy,
        dry_run=dry_run,
        max_post=max_post,
        logger=logger,
    )
    for analytics_filename in analytics_filenames:
        current_run.log_info(f"Processing analytics file: {analytics_filename}")
        try:
            analytics_data = get_file_from_dataset(config["SETTINGS"].get("OPENHEXA_DATASET_ID"), analytics_filename)
            # The number of data points will be less after mapping, as some of them will be filtered out
            # according to the mapping rules (e.g. COC mappings for data elements)
            df_de_mapped = apply_dataelement_mappings(
                df=analytics_data[analytics_data["data_type"] == "DATA_ELEMENT"],
                mappings=config.get("DATAELEMENT_MAPPING"),
            )
            df_rr_mapped = apply_reporting_rate_mappings(
                df=analytics_data[analytics_data["data_type"] == "REPORTING_RATE"], mappings=config.get("RATE_MAPPING")
            )
            df_ind_mapped = apply_acm_mappings(
                df=analytics_data[analytics_data["data_type"] == "INDICATOR"],
                mappings=config.get("ACM_INDICATOR_MAPPING"),
            )

            df_mapped = pd.concat([df_de_mapped, df_rr_mapped, df_ind_mapped], ignore_index=True)
            df_mapped = df_mapped.sort_values(by=["org_unit"], ascending=True)
            df_mapped["value"] = df_mapped["value"].replace("None", pd.NA)  # Ensure string "None" is treated as NA

            # push data
            pusher.push_data(df_data=df_mapped)
            current_run.log_info(f"Analytics data push finished for extract: {analytics_filename}.")
        except Exception as e:
            raise Exception(f"Error for extract {analytics_filename}), stopping push process. Error: {e}") from e
        finally:
            save_logs(logs_file, output_dir=pipeline_path / "logs" / "push_analytics")


def apply_dataelement_mappings(df: pd.DataFrame, mappings: dict) -> pd.DataFrame:
    """Applies the mappings for data elements to the given DataFrame.

    Returns:
        pd.DataFrame: The DataFrame with the applied mappings.
    """
    if mappings is None:
        current_run.log_error("No DATAELEMENT_MAPPING found in the configuration file.")
        raise ValueError
    df = df.copy()

    # change in SNIS COC mappings after 2025.
    # To avoid dupplicates we map and filter the DEs to be pushed
    # by the expected COC depending on the period (>=202501)
    if df.period.iloc[0] >= "202501":
        df = apply_dataelement_coc_mappings_2025(
            df=df, mappings_coc_2025=mappings.get("CAT_OPTION_COMBO", {}).get("MAPPINGS_2025")
        )
    else:
        df = apply_dataelement_coc_mappings_2024(
            df=df, mappings_coc_2024=mappings("CAT_OPTION_COMBO", {}).get("MAPPINGS_2024")
        )

    # map attribute option combo default
    aoc_default = mappings.get("ATTR_OPTION_COMBO", {}).get("DEFAULT")
    if aoc_default:
        df["attribute_option_combo"] = df["attribute_option_combo"].fillna(aoc_default)

    return df


def apply_dataelement_coc_mappings_2025(df: pd.DataFrame, mappings_coc_2025: dict) -> pd.DataFrame:
    """Applies the mappings for data elements to the given DataFrame according to the 2025 COC mapping rules.

    Any data element uid mapped in the configuration will be filtered to only keep rows with the category option combos
    (COC) specified in the mapping for that uid.
    Then, the selected COC values will be mapped following the configuration.
    Default COC and AOC mappings specified in the configuration are applied to all None values.

    Returns:
        pd.DataFrame: The DataFrame with the applied mappings.
    """
    if mappings_coc_2025 is None:
        raise ValueError("No MAPPINGS_2025 found in the configuration file.")

    df = df.copy()
    current_run.log_debug("Running 2025 COC mappings.")

    # map category option combo default
    coc_default = mappings_coc_2025.get("DEFAULT")
    if coc_default:
        df["attribute_option_combo"] = df["attribute_option_combo"].fillna(coc_default)

    # Loop over the DataElement COC mappings:
    # For each uid, remove rows where the COC is not in the allowed mapping,
    # then replace the remaining COC values with the mapped ones
    for uid, coc_uids in mappings_coc_2025["UIDS"].items():
        uid_clean = uid.strip()
        coc_mapping = {k.strip(): v.strip() for k, v in coc_uids.items()}
        allowed_cocs = set(coc_mapping.keys())

        # Step 1: Remove rows where the COC is not in the allowed mapping for this uid
        is_target_uid = df["dx"] == uid_clean
        is_invalid_coc = is_target_uid & ~df["category_option_combo"].isin(allowed_cocs)
        df = df[~is_invalid_coc].copy()

        # Step 2: Replace COC values using the mapping
        is_target_uid = df["dx"] == uid_clean  # reindex after filtering
        df.loc[is_target_uid, "category_option_combo"] = df.loc[is_target_uid, "category_option_combo"].replace(
            coc_mapping
        )

    # Apply COC General mappings (to all other COC in the extract)
    gen_mappings = mappings_coc_2025.get("GENERAL_MAPPINGS")
    gen_mappings_clean = {k.strip(): v.strip() for k, v in gen_mappings.items()}
    if gen_mappings_clean:
        df.loc[:, "category_option_combo"] = df.loc[:, "category_option_combo"].replace(gen_mappings_clean)

    return df


def apply_dataelement_coc_mappings_2024(df: pd.DataFrame, mappings_coc_2024: dict) -> pd.DataFrame:
    """Legacy code from old version.

    Returns:
        pd.DataFrame: The DataFrame with the applied mappings.
    """
    if mappings_coc_2024 is None:
        raise ValueError("No MAPPINGS_2024 found in the configuration file.")

    df = df.copy()
    current_run.log_debug("Running 2024 COC mappings.")

    # map category option combo default
    coc_default = mappings_coc_2024.get("DEFAULT")
    if coc_default:
        # current_run.log_info(f"Using {coc_default} default COC id.")
        df["category_option_combo"] = df["category_option_combo"].fillna(coc_default)

    # Replace COC values using the provided mappings
    coc_mappings = mappings_coc_2024.get("MAPPINGS", {})
    coc_to_replace = set(df["category_option_combo"]).intersection(coc_mappings.keys())  # just for logging
    if coc_to_replace:
        current_run.log_info(f"{len(coc_to_replace)} COC values will be replaced using mappings.")
        df["category_option_combo"] = df["category_option_combo"].replace(coc_mappings)

    # Remove rows with COC values in the ignore list
    coc_to_remove = mappings_coc_2024.get("IGNORE_MAPPINGS", [])
    if coc_to_remove:
        current_run.log_info(f"{len(coc_to_remove)} COC values will be ignored and removed.")
        df = df[~df["category_option_combo"].isin(coc_to_remove)]

    return df


def apply_reporting_rate_mappings(df: pd.DataFrame, mappings: dict) -> pd.DataFrame:
    """Applies the mappings for reporting rates to the given DataFrame.

    Returns:
        pd.DataFrame: The DataFrame with the applied mappings.
    """
    if mappings is None:
        raise ValueError("No RATE_MAPPING found in the configuration file.")

    df = df.copy()

    if df.period.iloc[0] >= "202501":
        mappings_year = mappings.get("2025")
    else:
        mappings_year = mappings.get("2024")

    for uid, metrics in mappings_year.items():
        for metric_name, new_uid in metrics.items():
            mask = (df["dx"] == uid) & (df["rate_metric"] == metric_name)
            if not mask.any():
                current_run.log_warning(f"No rows found for uid={uid}, metric={metric_name} — skipping.")
                continue
            df.loc[mask, "dx"] = new_uid

    # NOTE: This COC and AOC are applied by default to all rates (!)
    coc_default = mappings.get("CAT_OPTION_COMBO", {}).get("DEFAULT", "HllvX50cXC0")
    aoc_default = mappings.get("ATTR_OPTION_COMBO", {}).get("DEFAULT", "HllvX50cXC0")
    df["category_option_combo"] = df["category_option_combo"].fillna(coc_default)
    df["attribute_option_combo"] = df["attribute_option_combo"].fillna(aoc_default)

    return df


def apply_acm_mappings(df: pd.DataFrame, mappings: dict) -> pd.DataFrame:
    """Applies the mappings for ACM indicators to the given DataFrame.

    Returns:
        pd.DataFrame: The DataFrame with the applied mappings.
    """
    if mappings is None:
        raise ValueError("No ACM_INDICATOR_MAPPING found in the configuration file.")
    df = df.copy()

    uids_acm = mappings.get("UIDS")
    if uids_acm:
        df.loc[:, "dx"] = df.loc[:, "dx"].replace(uids_acm)

    # map category option combo default
    coc_default = mappings.get("CAT_OPTION_COMBO", {}).get("DEFAULT")
    if coc_default:
        current_run.log_info(f"Using {coc_default} default COC id mapping on ACM.")
        df["category_option_combo"] = df["category_option_combo"].fillna(coc_default)

    # map attribute option combo default
    aoc_default = mappings.get("ATTR_OPTION_COMBO", {}).get("DEFAULT")
    if aoc_default:
        current_run.log_info(f"Using {aoc_default} default AOC id on ACM.")
        df["attribute_option_combo"] = df["attribute_option_combo"].fillna(aoc_default)

    # Set values of 'INDICATOR' to INT format (no decimal), otherwise is ignored by DHIS2
    df["value"] = df["value"].apply(lambda x: str(int(float(x))))

    return df


def update_last_run_timestamp(timestamp_filename: Path, dataset_id: str) -> None:
    """Updates the last run timestamp in the JSON file."""
    timestamp = get_dataset_version_timestamp(dataset_id=dataset_id)
    try:
        save_json_file(
            file_path=timestamp_filename,
            contents={"LAST_UPDATE": timestamp.strftime("%Y%m%d_%H%M")},
        )
    except Exception as e:
        current_run.log_error(f"Error updating last run timestamp: {e}")
    current_run.log_info(f"Last run timestamp updated to: {timestamp.strftime('%Y%m%d_%H%M')}")


if __name__ == "__main__":
    dhis2_pnlp_push_v2()
