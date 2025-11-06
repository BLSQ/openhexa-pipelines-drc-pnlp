import logging
import shutil
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
import polars as pl

from d2d_library.db_queue import Queue
from d2d_library.dhis2.extract import DHIS2Extractor
from d2d_library.dhis2.push import DHIS2Pusher
from dateutil.relativedelta import relativedelta
from openhexa.sdk import current_run, parameter, pipeline, workspace
from openhexa.toolbox.dhis2 import DHIS2
from openhexa.toolbox.dhis2.dataframe import get_datasets
from openhexa.toolbox.dhis2.periods import period_from_string
from d2d_library.utils import (
    configure_logging_flush,
    connect_to_dhis2,
    load_configuration,
    read_parquet_extract,
    save_to_parquet,
    select_descendants,
)

# Ticket(s) related to this pipeline:
#   -https://bluesquare.atlassian.net/browse/COMACEP-3
# github repo:
#   -https://github.com/BLSQ/openhexa-pipelines-drc-pnlp


@pipeline("dhis2_snis_sentinel_pnlp_sync")
@parameter(
    code="run_extract_data",
    name="Extract data",
    type=bool,
    default=True,
    help="Extract data elements from source DHIS2.",
)
@parameter(
    code="run_push_data",
    name="Push data",
    type=bool,
    default=True,
    help="Push data to target DHIS2.",
)
def dhis2_snis_sentinel_pnlp_sync(run_extract_data: bool, run_push_data: bool):
    """Pipeline to sync sentinelles sites data from SNIS DHIS2 to PNLP DHIS2.

     The pipeline extracts data elements from the SNIS DHIS2 instance for sentinel
     sites and pushes them to the PNLP DHIS2 instance.

    Parameters
    ----------
    run_extract_data : bool, optional
        If True, runs the data extraction task (default is True).
    run_push_data : bool, optional
        If True, runs the data push task (default is True).

    Raises
    ------
    Exception
        If an error occurs during the pipeline execution.
    """
    # NOTE: in this pipeline, we do not align pyramids between source and target DHIS2 instances.
    # The pyramid is already aligned by the dhis2_snis_extract pipeline
    pipeline_path = Path(workspace.files_path) / "pipelines" / "dhis2_snis_sentinel_pnlp_sync"

    try:
        extract_data(
            pipeline_path=pipeline_path,
            run_task=run_extract_data,
        )

        push_data(
            pipeline_path=pipeline_path,
            run_task=run_push_data,
        )

    except Exception as e:
        current_run.log_error(f"An error occurred: {e}")
        raise


@dhis2_snis_sentinel_pnlp_sync.task
def extract_data(
    pipeline_path: Path,
    run_task: bool = True,
):
    """Extracts indicators (data elements) from the source DHIS2 instance and saves them in parquet format."""
    if not run_task:
        current_run.log_info("indicators extraction task skipped.")
        return

    current_run.log_info("indicators extraction task started.")

    # load configuration
    extract_config = load_configuration(config_path=pipeline_path / "configuration" / "extract_config.json")
    sync_config = load_configuration(config_path=pipeline_path / "configuration" / "sync_config.json")
    extract_settings = extract_config.get("SETTINGS", {})
    sync_settings = sync_config.get("ORG_UNITS", {})

    # connect to source DHIS2 instance (No cache for data extraction)
    dhis2_client = connect_to_dhis2(connection_str=extract_settings.get("SOURCE_DHIS2_CONNECTION"), cache_dir=None)

    # download source pyramid
    extract_pyramid(
        dhis2_client=dhis2_client,
        limit_level=sync_settings["SELECTION"].get("LIMIT_LEVEL", None),
        org_units_selection=sync_settings["SELECTION"].get("UIDS", []),
        include_children=sync_settings["SELECTION"].get("INCLUDE_CHILDREN", True),
        output_dir=pipeline_path / "data" / "pyramid",
        filename="pyramid_data.parquet",
    )

    # initialize queue
    db_path = pipeline_path / "configuration" / ".queue.db"
    push_queue = Queue(db_path)

    download_settings = extract_settings.get("MODE", None)
    if download_settings is None:
        download_settings = "DOWNLOAD_REPLACE"
        current_run.log_warning(f"No 'MODE' found in extraction settings. Set default: {download_settings}")

    current_run.log_info(f"Download MODE: {extract_settings['MODE']}")

    # limits
    dhis2_client.data_value_sets.MAX_DATA_ELEMENTS = 100
    dhis2_client.data_value_sets.MAX_ORG_UNITS = 100

    # Setup extractor
    # See docs about return_existing_file impact.
    dhis2_extractor = DHIS2Extractor(
        dhis2_client=dhis2_client, download_mode=download_settings, return_existing_file=False
    )

    # NOTE: These are data elements but we retrieve them aggregated from analytics (aire de sante),
    # that's why we use the indicators extractor.
    handle_data_element_extracts(
        pipeline_path=pipeline_path,
        dhis2_extractor=dhis2_extractor,
        data_element_extracts=extract_config["DATA_ELEMENTS"].get("EXTRACTS", []),
        source_pyramid=read_parquet_extract(pipeline_path / "data" / "pyramid" / "pyramid_data.parquet"),
        push_queue=push_queue,
    )


def extract_pyramid(
    dhis2_client: DHIS2,
    limit_level: int,
    org_units_selection: list[str],
    include_children: bool,
    output_dir: Path,
    filename: str,
) -> None:
    """Extracts the source DHIS2 pyramid data and saves it as a Parquet file."""
    current_run.log_info("Retrieving source DHIS2 pyramid data")

    try:
        output_dir.mkdir(parents=True, exist_ok=True)
        org_units = dhis2_client.meta.organisation_units(
            fields="id,name,shortName,openingDate,closedDate,parent,level,path,geometry"
        )
        org_units = pd.DataFrame(org_units)
        current_run.log_info(f"Organisation units extracted: {len(org_units.id.unique())}")

        if limit_level is None:
            current_run.log_info("OU limit level not set, selecting entire pyramid")
        else:
            org_units = org_units[org_units.level <= limit_level]  # filter by limit_level

        if len(org_units_selection) > 0:
            # Add parent_id column for easier filtering
            org_units["parent_id"] = org_units["parent"].apply(lambda x: x.get("id") if isinstance(x, dict) else None)
            org_units = select_descendants(org_units, org_units_selection)
            org_units = org_units.drop(columns=["parent_id"])
            if not include_children:
                org_units = org_units[org_units.id.isin(org_units_selection)]

        current_run.log_info(f"Selected organisation units: {len(org_units.id.unique())}.")

        # Save as Parquet
        pyramid_fname = output_dir / filename
        save_to_parquet(data=org_units, filename=pyramid_fname)
        current_run.log_info(f"DHIS2 pyramid data saved: {pyramid_fname}")

    except Exception as e:
        raise Exception(f"Error while extracting DHIS2 Pyramid: {e}") from e


def handle_data_element_extracts(
    pipeline_path: Path,
    dhis2_extractor: DHIS2Extractor,
    data_element_extracts: list,
    source_pyramid: pd.DataFrame,
    push_queue: Queue,
):
    """Handles dataelement (data elements) extracts based on the configuration."""
    if len(data_element_extracts) == 0:
        current_run.log_info("No data element to extract.")
        return

    logger, logs_file = configure_logging_flush(logs_path=Path("/home/jovyan/tmp/logs"), task_name="extract_sentinel")
    current_run.log_info("Starting data element extracts.")
    try:
        source_datasets = get_datasets(dhis2_extractor.dhis2_client)

        # loop over the available extract configurations
        for idx, extract in enumerate(data_element_extracts):
            extract_id = extract.get("EXTRACT_UID")
            dataset_id = extract.get("DATASET_UID", None)
            org_units_level = extract.get("ORG_UNITS_LEVEL", None)
            period_window = extract.get("PERIOD_WINDOW", 3)
            period_freq = extract.get("PERIOD_FREQUENCY", None)
            period_start = extract.get("PERIOD_START")
            period_end = extract.get("PERIOD_END")
            data_element_uids = extract.get("UIDS", [])

            if extract_id is None:
                current_run.log_warning(
                    f"No 'EXTRACT_UID' defined at position: {idx}. This is required, extract skipped."
                )
                continue

            if dataset_id is None and org_units_level is None:
                current_run.log_warning(
                    f"No 'DATASET_UID' or 'ORG_UNITS_LEVEL' defined for extract: {extract_id}, extract skipped."
                )
                continue

            if len(data_element_uids) == 0:
                current_run.log_warning(f"No data elements defined for extract: {extract_id}, extract skipped.")
                continue

            # Select org units from pyramid or from dataset OU definition
            # We prefer dataset OUs if dataset_id is provided
            if dataset_id:
                source_dataset = source_datasets.filter(pl.col("id").is_in([dataset_id]))
                source_ds_ou = source_dataset["organisation_units"][0]
                # Validate OUs against aligned pyramid (should always be the case)
                valid_ous = set(source_pyramid.id)
                org_units = [ou for ou in source_ds_ou if ou in valid_ous]
                current_run.log_info(
                    f"Starting data elements extract ID: '{extract_id}' ({idx + 1}) "
                    f"with {len(data_element_uids)} data elements across {len(org_units)} org units from dataset "
                    f"'{source_dataset['name'][0]}' ({dataset_id})."
                )
            else:
                org_units = source_pyramid[source_pyramid["level"] == org_units_level]["id"].to_list()
                current_run.log_info(
                    f"Starting data elements extract ID: '{extract_id}' ({idx + 1}) "
                    f"with {len(data_element_uids)} data elements across {len(org_units)} org units "
                    f"(level {org_units_level})."
                )

            try:
                # get extract periods
                extract_periods = get_extract_periods(
                    start=period_start, end=period_end, period_window=period_window, frequency=period_freq
                )
                current_run.log_info(f"Download extract period from: {extract_periods[0]} to {extract_periods[-1]}")
                logger.error(f"Download extract period from: {extract_periods[0]} to {extract_periods[-1]}")
            except Exception as e:
                current_run.log_warning(
                    f"Failed to compute extract periods for extract: {extract_id}, skipping extract."
                )
                logger.error(f"Extract {extract_id} - period computation error: {e}")
                continue

            # run extraction per period
            for period in extract_periods:
                try:
                    extract_path = dhis2_extractor.data_elements.download_period(
                        data_elements=data_element_uids,
                        org_units=org_units,
                        period=period,
                        output_dir=pipeline_path / "data" / "extracts" / "data_elements" / f"extract_{extract_id}",
                    )
                    if extract_path is not None:
                        push_queue.enqueue(f"{extract_id}|{extract_path}")

                except Exception as e:
                    current_run.log_warning(
                        f"Extract {extract_id} download failed for period {period}, skipping to next extract."
                    )
                    logger.error(f"Extract {extract_id} - period {period} error: {e}")
                    break  # skip to next extract

            current_run.log_info(f"Extract {extract_id} finished.")

    finally:
        push_queue.enqueue("FINISH")
        save_logs(logs_file, output_dir=pipeline_path / "logs" / "extract")


def save_logs(logs_file: Path, output_dir: Path) -> None:
    """Moves all .log files from logs_path to output_dir."""
    output_dir.mkdir(parents=True, exist_ok=True)
    if logs_file.is_file():
        dest_file = output_dir / logs_file.name
        shutil.copy(logs_file.as_posix(), dest_file.as_posix())


def apply_indicators_extract_config(
    df: pd.DataFrame, extract: dict, logger: logging.Logger | None = None
) -> pd.DataFrame:
    """Handles the mappings of indicators."""
    raise NotImplementedError("Indicator mappings is not yet implemented.")


def apply_reporting_rates_extract_config(
    df: pd.DataFrame,
    extract: dict,
    logger: logging.Logger | None = None,
) -> pd.DataFrame:
    """Handles the mappings of reporting rates."""
    raise NotImplementedError("Reporting rates mappings is not yet implemented.")


def apply_data_element_extract_config(
    df: pd.DataFrame,
    extract: dict,
    logger: logging.Logger | None = None,
) -> pd.DataFrame:
    """Applies indicator mappings to the extracted data.

    NOTE: It also FILTERS! indicator based on category option combo (COC) if specified in the extract configuration.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame containing the extracted data.
    extract : dict
        This is a dictionary containing the extract mappings.
    logger : logging.Logger, optional
        Logger instance for logging (default is None).

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
        df_uid = df[df["DX_UID"] == uid].copy()
        if coc_mappings:
            coc_mappings = {k: v for k, v in coc_mappings.items() if v is not None}  # Do not replace with None
            coc_mappings_clean = {str(k).strip(): str(v).strip() for k, v in coc_mappings.items()}
            df_uid = df_uid[df_uid["CATEGORY_OPTION_COMBO"].isin(coc_mappings_clean.keys())]
            df_uid["CATEGORY_OPTION_COMBO"] = df_uid.loc[:, "CATEGORY_OPTION_COMBO"].replace(coc_mappings_clean)

        if aoc_mappings:
            aoc_mappings = {k: v for k, v in aoc_mappings.items() if v is not None}  # Do not replace with None
            aoc_mappings_clean = {str(k).strip(): str(v).strip() for k, v in aoc_mappings.items()}
            df_uid = df_uid[df_uid["ATTRIBUTE_OPTION_COMBO"].isin(aoc_mappings_clean.keys())]
            df_uid["ATTRIBUTE_OPTION_COMBO"] = df_uid.loc[:, "ATTRIBUTE_OPTION_COMBO"].replace(aoc_mappings_clean)

        if uid_mapping:
            uid_mappings[uid] = uid_mapping

        chunks.append(df_uid)

    if len(chunks) == 0:
        current_run.log_warning("No data elements matched the provided mappings, returning empty dataframe.")
        logger.warning("No data elements matched the provided mappings, returning empty dataframe.")
        return pd.DataFrame(columns=df.columns)

    df_filtered = pd.concat(chunks, ignore_index=True)

    # set defaults
    df_filtered["CATEGORY_OPTION_COMBO"] = df_filtered["CATEGORY_OPTION_COMBO"].fillna("HllvX50cXC0")
    df_filtered["ATTRIBUTE_OPTION_COMBO"] = df_filtered["ATTRIBUTE_OPTION_COMBO"].fillna("HllvX50cXC0")

    if uid_mappings:
        uid_mappings = {k: v for k, v in uid_mappings.items() if v is not None}  # Do not replace with None
        uid_mappings_clean = {str(k).strip(): str(v).strip() for k, v in uid_mappings.items()}
        df_filtered["DX_UID"] = df_filtered.loc[:, "DX_UID"].replace(uid_mappings_clean)

    return df_filtered


@dhis2_snis_sentinel_pnlp_sync.task
def push_data(
    pipeline_path: Path,
    run_task: bool = True,
):
    """Pushes data points to the target DHIS2 instance."""
    if not run_task:
        current_run.log_info("Data push task skipped.")
        return

    current_run.log_info("Starting data push.")

    # setup
    logger, logs_file = configure_logging_flush(logs_path=Path("/home/jovyan/tmp/logs"), task_name="push_sentinel")
    push_config = load_configuration(config_path=pipeline_path / "configuration" / "push_config.json")
    settings = push_config.get("SETTINGS", {})
    target_dhis2 = connect_to_dhis2(connection_str=settings["TARGET_DHIS2_CONNECTION"], cache_dir=None)
    db_path = pipeline_path / "configuration" / ".queue.db"
    push_queue = Queue(db_path)

    # log parameters
    msg = (
        f"Import strategy: {settings.get('IMPORT_STRATEGY', 'CREATE_AND_UPDATE')} - "
        f"Dry Run: {settings.get('DRY_RUN', True)} - "
        f"Max Post elements: {settings.get('MAX_POST', 500)}"
    )
    logger.info(msg)
    current_run.log_info(msg)

    # Set up DHIS2 pusher
    pusher = DHIS2Pusher(
        dhis2_client=target_dhis2,
        import_strategy=settings.get("IMPORT_STRATEGY", "CREATE_AND_UPDATE"),
        dry_run=settings.get("DRY_RUN", True),
        max_post=settings.get("MAX_POST", 500),
        logger=logger,
    )

    # Map data types to their respective mapping functions
    dispatch_map = {
        "DATA_ELEMENT": (push_config["DATA_ELEMENTS"]["EXTRACTS"], apply_data_element_extract_config),
        "REPORTING_RATE": (push_config["REPORTING_RATES"]["EXTRACTS"], apply_reporting_rates_extract_config),
        "INDICATOR": (push_config["INDICATORS"]["EXTRACTS"], apply_indicators_extract_config),
    }

    conflict_list = []
    push_wait = settings.get("PUSH_WAIT_MINUTES", 5)
    # loop over the queue
    while True:
        next_extract = push_queue.peek()
        if next_extract == "FINISH":
            push_queue.dequeue()  # remove marker if present
            break

        if not next_extract:
            current_run.log_info("Push data process: waiting for updates")
            time.sleep(60 * int(push_wait))
            continue

        try:
            # Read extract
            extract_id, extract_file_path = split_on_pipe(next_extract)
            extract_path = Path(extract_file_path)
            extract_data = read_parquet_extract(parquet_file=extract_path)
        except Exception as e:
            current_run.log_error(f"Failed to read extract from queue item: {next_extract}.")
            logger.info(f"Failed to read extract from queue item: {next_extract}. Error: {e}")
            push_queue.dequeue()  # remove problematic item
            continue

        try:
            # Determine data type
            data_type = extract_data["DATA_TYPE"].unique()[0]

            current_run.log_info(f"Pushing data for extract {extract_id}: {extract_path.name}.")
            logger.info(f"Pushing data for extract {extract_id}: {extract_path.name}.")
            if data_type not in dispatch_map:
                current_run.log_warning(f"Unknown DATA_TYPE '{data_type}' in extract: {extract_path.name}. Skipping.")
                push_queue.dequeue()  # remove unknown item
                continue

            # Get config and mapping function
            cfg_list, mapper_func = dispatch_map[data_type]
            extract_config = next((e for e in cfg_list if e["EXTRACT_UID"] == extract_id), {})

            # Apply mapping and push data
            df_mapped: pd.DataFrame = mapper_func(df=extract_data, extract=extract_config)
            # df_mapped[[""]].drop_duplicates().head()
            df_mapped = df_mapped.sort_values(by="ORG_UNIT")  # speed up DHIS2 processing
            pusher.push_data(df_data=df_mapped)
            conflict_list.extend(pusher.summary["ERRORS"])

            # Success → dequeue
            push_queue.dequeue()

        except Exception as e:
            current_run.log_error(
                f"Fatal error for extract {extract_id} ({extract_path.name}), stopping push process. Error: {e!s}"
            )
            raise  # crash on error

        finally:
            log_invalid_org_units(conflict_list, logger)
            save_logs(logs_file, output_dir=pipeline_path / "logs" / "push")

    current_run.log_info("Push process finished.")


def log_invalid_org_units(
    error_list: list,
    logger: logging.Logger,
) -> None:
    logger.info("Check for invalid org units in push summary.")
    invalid_org_units = set()
    for error in error_list:
        if not isinstance(error, dict):
            continue
        # search for conflicts with orgUnit issues
        if (
            "Organisation unit not found" in str(error.get("value", ""))
            or "Invalid organisation unit" in str(error.get("value", ""))
            or error.get("property") == "orgUnit"
            or error.get("errorCode") == "E7612"  # DHIS2 invalid orgUnit code
        ):
            orgunit_id = error.get("objects", {}).get("organisationUnit") or error.get("object")
            if orgunit_id:
                invalid_org_units.add(str(orgunit_id))

    if invalid_org_units:
        formatted = ", ".join(sorted(invalid_org_units))
        logger.warning(f"Found {len(invalid_org_units)} invalid org units: {formatted}")
    else:
        logger.info("No invalid org units found in the summary.")


def split_on_pipe(s: str) -> tuple[str, str | None]:
    """Splits a string on the first pipe character and returns a tuple.

    Parameters
    ----------
    s : str
        The string to split.

    Returns
    -------
    tuple[str, str | None]
        A tuple containing the part before the pipe and the part after the pipe (or None if no pipe is found).
    """
    parts = s.split("|", 1)
    if len(parts) == 2:
        return parts[0], parts[1]
    return None, parts[0]


def get_formatted_period(period_window: int, frequency: str) -> str:
    if frequency == "WEEKLY":
        return (datetime.now() - relativedelta(weeks=period_window)).strftime("%YW%W")
    if frequency == "MONTHLY":
        return (datetime.now() - relativedelta(months=period_window)).strftime("%Y%m")
    if frequency == "QUARTERLY":
        q_date = datetime.now() - relativedelta(months=period_window * 3)
        quarter = (q_date.month - 1) // 3 + 1
        return f"{q_date.year}Q{quarter}"
    if frequency == "YEARLY":
        return (datetime.now() - relativedelta(years=period_window)).strftime("%Y")
    raise ValueError(f"Unsupported frequency: {frequency}")


def get_extract_periods(start: str, end: str, period_window: int, frequency: str) -> list[str]:
    """Compute the list of DHIS2 periods based on pipeline configuration.

    TODO: Make this function to returns different types of periods
    (weekly, monthly, quarterly, yearly, etc.) and add it to the utils library.

    Args:
        settings (dict): Config dictionary containing a 'SETTINGS' section
            with optional STARTDATE, ENDDATE, and NUMBER_MONTHS_WINDOW.

    Returns:
        list[str]: List of DHIS2 period strings (e.g., ['202407', '202408', '202409']).
    """
    try:
        start_formatted = start or get_formatted_period(period_window, frequency)
        end_formatted = end or get_formatted_period(0, frequency)

        # Convert to period objects
        start_period = period_from_string(start_formatted)
        end_period = period_from_string(end_formatted)

        # Generate list of periods
        if str(start_period) < str(end_period):
            return [str(p) for p in start_period.get_range(end_period)]
        return [str(start_period)]

    except Exception as e:
        raise Exception(f"Error computing extract periods: {e}") from e


if __name__ == "__main__":
    dhis2_snis_sentinel_pnlp_sync()
