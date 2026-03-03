import json
import logging
from datetime import datetime
from pathlib import Path
import time
import shutil
import pandas as pd

# import polars as pl
import requests
from dateutil.relativedelta import relativedelta
from openhexa.sdk import current_run, pipeline, workspace, parameter
from openhexa.toolbox.dhis2 import DHIS2
from openhexa.toolbox.dhis2.periods import period_from_string
from utils import (
    DataPoint,
    OrgUnitObj,
    Queue,
    build_id_indexes,
    connect_to_dhis2,
    load_configuration,
    read_parquet_extract,
    retrieve_ou_list,
    save_to_parquet,
    split_list,
    configure_logging_flush,
)


@pipeline("dhis2_data_elements_sync", timeout=28800)  # 8 hrs
@parameter(
    "ou_sync",
    name="Run org units sync",
    type=bool,
    default=True,
    help="Run organisation units sync.",
)
@parameter(
    "extract_analytics",
    name="Extract data elements",
    type=bool,
    default=True,
    help="Run the analytics extraction task.",
)
@parameter(
    "push_analytics",
    name="Push data elements",
    type=bool,
    default=True,
    help="Run the analytics push task.",
)
def dhis2_data_elements_sync(ou_sync: bool, extract_analytics: bool, push_analytics: bool):
    """Write your pipeline orchestration here.

    Pipeline functions should only call tasks and should never perform IO operations or expensive computations.
    """
    pipeline_path = Path(workspace.files_path) / "pipelines" / "dhis2_data_elements_sync"

    try:
        configure_login(logs_path=pipeline_path / "logs", task_name="data_push")

        pyramid_ready = sync_organisation_units(
            pipeline_path=pipeline_path,
            run_task=ou_sync,
        )

        extract_data(
            pipeline_path=pipeline_path,
            run_task=extract_analytics,
            wait=pyramid_ready,
        )

        push_extracts(
            pipeline_path=pipeline_path,
            run_task=push_analytics,
            wait=pyramid_ready,
        )

    except Exception as e:
        current_run.log_error(f"An error occurred: {e}")
        raise


@dhis2_data_elements_sync.task
def sync_organisation_units(
    pipeline_path: Path,
    run_task: bool,
) -> bool:
    """Pyramid extraction task.

    extracts and saves a pyramid dataframe for all levels (could be set via config in the future)
    """
    if not run_task:
        current_run.log_info("Organisation units sync task skipped.")
        return

    try:
        # load configuration
        config_extract = load_configuration(config_path=pipeline_path / "config" / "extract_config.json")
        config_push = load_configuration(config_path=pipeline_path / "config" / "push_config.json")

        dry_run = config_push["SETTINGS"].get("DRY_RUN", True)  # Default to True

        # connect to source DHIS2 instance
        dhis2_client_source = connect_to_dhis2(
            connection_str=config_extract["SETTINGS"]["DHIS2_CONNECTION"], cache_dir=pipeline_path / "data" / "cache"
        )

        # connect to target DHIS2 instance
        dhis2_client_target = connect_to_dhis2(
            connection_str=config_push["SETTINGS"]["DHIS2_CONNECTION"], cache_dir=pipeline_path / "data" / "cache"
        )

        extract_pyramid(dhis2_client=dhis2_client_source, output_dir=pipeline_path / "data" / "pyramid")

        # sync pyramid data to target
        sync_pyramid_with(pipeline_path=pipeline_path, dhis2_client=dhis2_client_target, dry_run=dry_run)

    except Exception as e:
        raise Exception(f"Error during pyramid update: {e}") from e
    return True


@dhis2_data_elements_sync.task
def extract_data(pipeline_path: Path, run_task: bool, wait: bool):
    """Extracts data elements from the source DHIS2 instance and saves them in Parquet format."""
    if not run_task:
        current_run.log_info("Data elements extraction task skipped.")
        return

    current_run.log_info("Data elements extraction task started.")

    # load configuration
    config = load_configuration(config_path=pipeline_path / "config" / "extract_config.json")

    # connect to source DHIS2 instance
    dhis2_client_source = connect_to_dhis2(
        connection_str=config["SETTINGS"]["DHIS2_CONNECTION"], cache_dir=pipeline_path / "data" / "cache"
    )

    # initialize queue
    db_path = pipeline_path / "config" / ".queue.db"
    push_queue = Queue(db_path)

    try:
        months_lag = config["SETTINGS"].get("NUMBER_MONTHS_WINDOW", 6)  # default 6 months window
        if config["SETTINGS"]["STARTDATE"] == "":
            start = (datetime.now() - relativedelta(months=months_lag)).strftime("%Y%m")
        else:
            start = config["SETTINGS"]["STARTDATE"]
        if config["SETTINGS"]["ENDDATE"] == "":
            end = (datetime.now() - relativedelta(months=1)).strftime("%Y%m")  # go back 1 month.
        else:
            end = config["SETTINGS"]["ENDDATE"]

        download_settings = config["SETTINGS"].get("MODE", None)
        if download_settings is None:
            download_settings = "DOWNLOAD_REPLACE"
            current_run.log_warning(f"No 'MODE' found in extraction settings. Set default: {download_settings}")

        # Data Elements
        data_elements = config["DATA_ELEMENTS"].get("UIDS", [])
        if len(data_elements) == 0:
            current_run.log_info("No data elements to extract.")
            return

        # limits TEST
        # dhis2_client_source.analytics.MAX_DX = 100
        # dhis2_client_source.analytics.MAX_ORG_UNITS = 100
        dhis2_client_source.data_value_sets.MAX_DATA_ELEMENTS = 100
        dhis2_client_source.data_value_sets.MAX_ORG_UNITS = 100

        # Get periods
        start_period = period_from_string(start)
        end_period = period_from_string(end)
        extract_periods = (
            [str(p) for p in start_period.get_range(end_period)]
            if str(start_period) < str(end_period)
            else [str(start_period)]
        )
        # retrieve FOSA ids from SNIS
        fosa_list = retrieve_ou_list(dhis2_client=dhis2_client_source, ou_level=5)
        current_run.log_info(f"Download MODE: {config['SETTINGS']['MODE']} from: {start} to {end}")

        # retrieve data
        extract_finished = False
        for period in extract_periods:
            current_run.log_info(f"Retrieving data extract for period : {period}")
            handle_extract_for_period(
                pipeline_path=pipeline_path,
                dhis2_client=dhis2_client_source,
                period=period,
                org_unit_list=fosa_list,
                download_settings=download_settings,
                data_elements=data_elements,
                queue=push_queue,
            )
        extract_finished = True
        current_run.log_info("Extract process finished.")
    except Exception as e:
        raise Exception(f"Extract task error : {e}") from e
    finally:
        if extract_finished:
            push_queue.enqueue("FINISH")


@dhis2_data_elements_sync.task
def push_extracts(
    pipeline_path: Path,
    run_task: bool,
    wait: bool,
):
    """Pushes data elements to the target DHIS2 instance."""
    if not run_task:
        current_run.log_info("Data elements push task skipped.")
        return

    current_run.log_info("Data elements push task started.")

    # load configuration
    logger, logs_file = configure_logging_flush(logs_path=Path("/home/jovyan/tmp/logs"), task_name="push_des")
    config = load_configuration(config_path=pipeline_path / "config" / "push_config.json")

    # connect to target DHIS2 instance
    dhis2_client_target = connect_to_dhis2(
        connection_str=config["SETTINGS"]["DHIS2_CONNECTION"], cache_dir=pipeline_path / "data" / "cache"
    )

    current_run.log_info("Starting data elements push.")
    db_path = pipeline_path / "config" / ".queue.db"
    push_queue = Queue(db_path)

    #  Import parameters
    import_strategy = config["SETTINGS"].get("IMPORT_STRATEGY")
    if import_strategy is None:
        import_strategy = "CREATE_AND_UPDATE"  # CREATE, UPDATE, CREATE_AND_UPDATE

    dry_run = config["SETTINGS"].get("DRY_RUN", True)  # Default to True
    max_post = config["SETTINGS"].get("MAX_POST", 500)  # Default to 500
    push_wait = config["SETTINGS"].get("PUSH_WAIT", 5)  # Default to 5 minutes

    # log parameters
    logger.info(f"Import strategy: {import_strategy} - Dry Run: {dry_run} - Max Post elements: {max_post}")
    current_run.log_info(
        f"Pushing data elements with parameters import_strategy: {import_strategy}, dry_run: {dry_run}, max_post: {max_post}"
    )

    try:
        dataelement_mappings = config.get("DATAELEMENT_MAPPINGS", [])
        if dataelement_mappings is None:
            current_run.log_error("No DATAELEMENT_MAPPINGS found in the configuration file.")
            raise ValueError

        while True:
            next_period = push_queue.peek()  # I dont remove yet, just take a look at the next period
            if next_period == "FINISH":
                push_queue.dequeue()  # remove the FINISH marker
                break

            if not next_period:
                current_run.log_info("Push data process: waiting for updates")
                time.sleep(60 * int(push_wait))  # wait for "push_wait" min if no next period is available
                continue

            try:
                extract_data = read_parquet_extract(
                    parquet_file=pipeline_path / "data" / "extracts" / f"data_{next_period}.parquet"
                )
                current_run.log_info(f"Push extract period: {next_period}.")
            except Exception as e:
                current_run.log_warning(
                    f"Error while reading the extracts file: data_{next_period}.parquet - error: {e}"
                )
                continue

            # Use dictionary mappings to replace UIDS, COC and AOC..
            coc_default = config.get("CAT_OPTION_COMBO_DEFAULT", {})
            aoc_default = config.get("ATTR_OPTION_COMBO_DEFAULT", {})
            df = apply_dataelement_mappings(
                datapoints_df=extract_data,
                mappings=dataelement_mappings,
                coc_default=coc_default,
                aoc_default=aoc_default,
            )

            # mandatory fields in the input dataset
            mandatory_fields = [
                "dx_uid",
                "period",
                "org_unit",
                "category_option_combo",
                "attribute_option_combo",
                "value",
            ]
            df = df[mandatory_fields]

            # Sort the dataframe by org_unit to reduce the time (hopefully)
            df = df.sort_values(by=["org_unit"], ascending=True)

            # convert the datapoints to json and check if they are valid
            # check the implementation of DataPoint class for the valid fields (mandatory)
            datapoints_valid, datapoints_not_valid, datapoints_to_na = select_transform_to_json(data_values=df)

            # log not valid datapoints
            log_ignored_or_na(report_path=pipeline_path / "logs", datapoint_list=datapoints_not_valid, logger=logger)

            # datapoints set to NA
            if len(datapoints_to_na) > 0:
                log_ignored_or_na(
                    report_path=pipeline_path / "logs",
                    datapoint_list=datapoints_to_na,
                    logger=logger,
                    data_type="extract",
                    is_na=True,
                )
                summary_na = push_data_elements(
                    dhis2_client=dhis2_client_target,
                    data_elements_list=datapoints_to_na,  # different json format for deletion see: DataPoint class to_delete_json()
                    strategy=import_strategy,
                    dry_run=dry_run,
                    max_post=max_post,
                )

                # log info
                msg = f"Data elements delete summary:  {summary_na['import_counts']}"
                current_run.log_info(msg)
                logger.info(msg)
                log_summary_errors(summary_na, logger=logger)

            current_run.log_info(f"Pushing {len(datapoints_valid)} valid data elements for period {next_period}.")
            # push data
            summary = push_data_elements(
                dhis2_client=dhis2_client_target,
                data_elements_list=datapoints_valid,
                strategy=import_strategy,
                dry_run=dry_run,
                max_post=max_post,
            )

            # The process is correct, so we remove the period from the queue
            _ = push_queue.dequeue()
            current_run.log_info(f"Extracts pushed for period {next_period}.")

            # log info
            msg = f"Analytics extracts summary for period {next_period}: {summary['import_counts']}"
            current_run.log_info(msg)
            logger.info(msg)
            log_summary_errors(summary, logger=logger)

        current_run.log_info("No more extracts to push.")

    except Exception as e:
        raise Exception(f"Analytic extracts task error: {e}")
    finally:
        push_queue.enqueue("FINISH")
        save_logs(logs_file, output_dir=pipeline_path / "logs" / "push")


def save_logs(logs_file: Path, output_dir: Path) -> None:
    """Moves all .log files from logs_path to output_dir."""
    output_dir.mkdir(parents=True, exist_ok=True)
    if logs_file.is_file():
        dest_file = output_dir / logs_file.name
        shutil.copy(logs_file.as_posix(), dest_file.as_posix())


def extract_pyramid(dhis2_client: DHIS2, output_dir: Path) -> None:
    """Extracts the SNIS DHIS2 pyramid data and saves it as a Parquet file."""
    current_run.log_info("Retrieving SNIS DHIS2 pyramid data")

    try:
        output_dir.mkdir(parents=True, exist_ok=True)
        # Retrieve all available levels..
        # levels = pl.DataFrame(dhis2_client.meta.organisation_unit_levels())
        # org_levels = levels.select("level").unique().sort(by="level").to_series().to_list()
        org_units = dhis2_client.meta.organisation_units(
            fields="id,name,shortName,openingDate,closedDate,parent,level,path,geometry"
        )
        org_units = pd.DataFrame(org_units)
        org_units = org_units[org_units.level <= 5]  # Select level 5

        current_run.log_info(
            f"Extracted {len(org_units[org_units.level == 5].id.unique())} units at organisation unit level 5"
        )
        pyramid_fname = output_dir / "pyramid_data.parquet"

        # Save as Parquet
        save_to_parquet(data=org_units, filename=pyramid_fname)
        current_run.log_info(f"SNIS DHIS2 pyramid data saved: {pyramid_fname}")

    except Exception as e:
        raise Exception(f"Error while extracting SNIS DHIS2 Pyramid: {e}") from e


def sync_pyramid_with(pipeline_path: Path, dhis2_client: DHIS2, dry_run: bool) -> None:
    current_run.log_info("Starting organisation units sync.")

    # set logger
    logger, logs_file = configure_logging_flush(logs_path=Path("/home/jovyan/tmp/logs"), task_name="org_units_sync")
    # Load pyramid extract
    orgUnit_source = read_parquet_extract(pipeline_path / "data" / "pyramid" / "pyramid_data.parquet")
    current_run.log_debug(f"Shape source pyramid: {orgUnit_source.shape}")

    if orgUnit_source.shape[0] > 0:
        # Retrieve the target (PNLP) orgUnits to compare
        current_run.log_info(f"Retrieving organisation units from target DHIS2 instance {dhis2_client.api.url}")
        current_run.log_info(f"Run org units sync with dry_run: {dry_run}")
        orgUnit_target = dhis2_client.meta.organisation_units(
            fields="id,name,shortName,openingDate,closedDate,parent,level,path,geometry"
        )
        orgUnit_target = pd.DataFrame(orgUnit_target)
        current_run.log_debug(f"Shape target pyramid: {orgUnit_target.shape}")

        # Get list of ids for creation and update
        ou_new = list(set(orgUnit_source.id) - set(orgUnit_target.id))
        ou_matching = list(set(orgUnit_source.id).intersection(set(orgUnit_target.id)))
        dhsi2_version = dhis2_client.meta.system_info().get("version")

        # Create orgUnits
        try:
            if len(ou_new) > 0:
                current_run.log_info(f"Creating {len(ou_new)} organisation units.")
                ou_to_create = orgUnit_source[orgUnit_source.id.isin(ou_new)]
                # NOTE: Geometry is valid for versions > 2.32
                if dhsi2_version <= "2.32":
                    ou_to_create["geometry"] = None
                    current_run.log_warning("DHIS2 version not compatible with geometry. Geometry will be ignored.")
                push_orgunits_create(
                    ou_df=ou_to_create,
                    dhis2_client_target=dhis2_client,
                    dry_run=dry_run,
                    logger=logger,
                )
        except Exception as e:
            raise Exception(f"Unexpected error occurred while creating organisation units. Error: {e}") from e
        finally:
            save_logs(logs_file, output_dir=pipeline_path / "logs" / "org_units_sync")

        # Update orgUnits
        try:
            if len(ou_matching) > 0:
                current_run.log_info(f"Checking for updates in {len(ou_matching)} organisation units")
                # NOTE: Geometry is valid for versions > 2.32
                if dhsi2_version <= "2.32":
                    orgUnit_source["geometry"] = None
                    orgUnit_target["geometry"] = None
                    current_run.log_warning("DHIS2 version not compatible with geometry. Geometry will be ignored.")
                push_orgunits_update(
                    orgUnit_source=orgUnit_source,
                    orgUnit_target=orgUnit_target,
                    matching_ou_ids=ou_matching,
                    dhis2_client_target=dhis2_client,
                    dry_run=dry_run,
                    logger=logger,
                )
                current_run.log_info("Organisation units push finished.")
        except Exception as e:
            raise Exception(f"Unexpected error occurred while updating organisation units. Error: {e}") from e
        finally:
            save_logs(logs_file, output_dir=pipeline_path / "logs" / "org_units_sync")

    else:
        current_run.log_warning("No data found in the pyramid file. Organisation units task skipped.")


def push_orgunits_create(ou_df: pd.DataFrame, dhis2_client_target: DHIS2, dry_run: bool, logger: logging.Logger):
    errors_count = 0
    for _, row in ou_df.iterrows():
        ou = OrgUnitObj(row)
        if ou.is_valid():
            response = push_orgunit(
                dhis2_client=dhis2_client_target,
                orgunit=ou,
                strategy="CREATE",
                dry_run=dry_run,  # dry_run=False -> Apply changes in the DHIS2
            )
            if response["status"] == "ERROR":
                errors_count = errors_count + 1
                logger.info(str(response))
            else:
                current_run.log_info(f"New organisation unit created: {ou}")
        else:
            logger.info(
                str(
                    {
                        "action": "CREATE",
                        "statusCode": None,
                        "status": "NOTVALID",
                        "response": None,
                        "ou_id": row.get("id"),
                    }
                )
            )

    if errors_count > 0:
        current_run.log_info(
            f"{errors_count} errors occurred during creation. Please check the latest execution reports"
        )


def push_orgunits_update(
    orgUnit_source: pd.DataFrame,
    orgUnit_target: pd.DataFrame,
    matching_ou_ids: list,
    dhis2_client_target: DHIS2,
    dry_run: bool,
    logger: logging.Logger,
):
    """
    Update org units based matching id list
    """

    # Use these columns to compare (check for Updates)
    comparison_cols = [
        "name",
        "shortName",
        "openingDate",
        "closedDate",
        "parent",
        "geometry",
    ]

    # build id dictionary (faster) and compare on selected columns
    index_dictionary = build_id_indexes(orgUnit_source, orgUnit_target, matching_ou_ids)
    orgUnit_source_f = orgUnit_source[comparison_cols]
    orgUnit_target_f = orgUnit_target[comparison_cols]

    errors_count = 0
    updates_count = 0
    progress_count = 0
    for id, indices in index_dictionary.items():
        progress_count = progress_count + 1
        source = orgUnit_source_f.iloc[indices["source"]]
        target = orgUnit_target_f.iloc[indices["target"]]
        # get cols with differences
        diff_fields = source[~((source == target) | (source.isna() & target.isna()))]

        # If there are differences update!
        if not diff_fields.empty:
            # add the ID for update
            source["id"] = id
            ou_update = OrgUnitObj(source)
            response = push_orgunit(
                dhis2_client=dhis2_client_target,
                orgunit=ou_update,
                strategy="UPDATE",
                dry_run=dry_run,  # dry_run=False -> Apply changes in the DHIS2
            )
            if response["status"] == "ERROR":
                errors_count = errors_count + 1
            else:
                updates_count = updates_count + 1
            logger.info(str(response))

        if progress_count % 5000 == 0:
            current_run.log_info(f"Organisation units checked: {progress_count}/{len(matching_ou_ids)}")

    current_run.log_info(f"Organisation units updated: {updates_count}")
    if errors_count > 0:
        current_run.log_info(
            f"{errors_count} errors occurred during OU update. Please check the latest execution reports."
        )


def push_orgunit(dhis2_client: DHIS2, orgunit: OrgUnitObj, strategy: str = "CREATE", dry_run: bool = True):
    if strategy == "CREATE":
        endpoint = "organisationUnits"
        payload = orgunit.to_json()

    if strategy == "UPDATE":
        endpoint = "metadata"
        payload = {"organisationUnits": [orgunit.to_json()]}

    r = dhis2_client.api.session.post(
        f"{dhis2_client.api.url}/{endpoint}",
        json=payload,
        params={"dryRun": dry_run, "importStrategy": f"{strategy}"},
    )

    return build_formatted_response(response=r, strategy=strategy, ou_id=orgunit.id)


def build_formatted_response(response: requests.Response, strategy: str, ou_id: str) -> dict:
    resp = {
        "action": strategy,
        "statusCode": response.status_code,
        "status": response.json().get("status"),
        "response": response.json().get("response"),
        "ou_id": ou_id,
    }
    return resp


def handle_extract_for_period(
    pipeline_path: Path,
    dhis2_client: DHIS2,
    period: str,
    org_unit_list: list,
    download_settings: str,
    data_elements: dict,
    queue: Queue,
) -> None:
    """
    This function retrieves and processes data from the DHIS2 system for a given period
    and mode of extraction, saving the results in a Parquet file.

    Parameters:
    ----------
    root : str
        The pipeline directory
    dhis2_client : DHIS2
        An instance of the DHIS2 client used for API calls to retrieve data.
    period : str
        The period for data extraction in the "YYYYMM" format (e.g., "202409" for September 2024 -> MONTHLY for SNIS)
    org_unit_list : list
        A list of organizational unit IDs for which data needs to be extracted.
    config : dict
        A dictionary containing the extraction configuration.
        Expected keys include:
            - "MODE": Specifies the mode of extraction ("DOWNLOAD_REPLACE" or "DOWNLOAD_NEW").
                DOWNLOAD_REPLACE: Full download of the period and replace the corresponding extract file
                DOWNLOAD_NEW: download only new

    Returns:
    --------
    None
    """
    # fname period extract
    output_dir = pipeline_path / "data" / "extracts"
    output_dir.mkdir(parents=True, exist_ok=True)
    extract_fname = output_dir / f"data_{period}.parquet"

    if download_settings == "DOWNLOAD_REPLACE":
        raw_data = retrieve_data_elements(
            dhis2_client=dhis2_client,
            period=period,
            org_unit_list=org_unit_list,
            data_elements=data_elements,
        )
        if raw_data is not None:
            if extract_fname.exists():
                current_run.log_info(f"Replacing extract for period {period}.")
            save_to_parquet(raw_data, extract_fname)
            queue.enqueue(period)
        else:
            current_run.log_info(f"Nothing to save for period {period}.")

    elif download_settings == "DOWNLOAD_NEW":
        if extract_fname.exists():
            current_run.log_info(f"Extract for period {period} already exist, download skipped.")
        else:
            raw_data = retrieve_data_elements(
                dhis2_client=dhis2_client,
                period=period,
                org_unit_list=org_unit_list,
                data_elements=data_elements,
            )
            if raw_data is not None:
                save_to_parquet(raw_data, extract_fname)
                queue.enqueue(period)
            else:
                current_run.log_info(f"Nothing to save for period {period}..")
    else:
        raise ValueError("Incorrect 'MODE' configuration.")


def retrieve_data_elements(
    dhis2_client: DHIS2,
    period: str,
    org_unit_list: list,
    data_elements: list,
    lastUpdated: str = None,
):
    response = dhis2_client.data_value_sets.get(
        data_elements=data_elements,
        periods=[period],
        org_units=org_unit_list,
        last_updated=lastUpdated,
    )
    return map_to_dhis2_format(pd.DataFrame(response), data_type="DATAELEMENT")


def map_to_dhis2_format(
    dhis_data: pd.DataFrame,
    data_type: str = "DATAELEMENT",
    domain_type: str = "AGGREGATED",
) -> pd.DataFrame:
    """
    Maps DHIS2 data to a standardized data extraction table.

    Parameters
    ----------
    dhis_data : pd.DataFrame
        Input DataFrame containing DHIS2 data. Must include columns like `period`, `orgUnit`,
        `categoryOptionCombo(DATAELEMENT)`, `attributeOptionCombo(DATAELEMENT)`, `dataElement`
        and `value` based on the data type.
    data_type : str
        The type of data being mapped. Supported values are:
        - "DATAELEMENT": Includes `categoryOptionCombo` and maps `dataElement` to `dx_uid`.
        - "DATASET": Maps `dx` to `dx_uid` and `rate_type` by split the string by `.`.
        - "INDICATOR": Maps `dx` to `dx_uid`.
        - "POPULATION": Maps `dx` to `dx_uid` and the rest of DHIS2 raw columns
        Default is "DATAELEMENT".
    domain_type : str, optional
        The domain of the data if its per period (Agg ex: monthly) or datapoint (Tracker ex: per day):
        - "AGGREGATED": For aggregated data (default).
        - "TRACKER": For tracker data.

    Returns
    -------
    pd.DataFrame
        A DataFrame formatted to SNIS standards, with the following columns:
        - "data_type": The type of data (DATAELEMENT, DATASET, or INDICATOR).
        - "dx_uid": Data element, dataset, or indicator UID.
        - "period": Reporting period.
        - "orgUnit": Organization unit.
        - "categoryOptionCombo": (Only for DATAELEMENT) Category option combo UID.
        - "rate_type": (Only for DATASET) Rate type.
        - "domain_type": Data domain (AGGREGATED or TRACKER).
        - "value": Data value.
    """
    if dhis_data.empty:
        return None

    if data_type not in ["DATAELEMENT", "DATASET", "INDICATOR", "POPULATION"]:
        raise ValueError("Incorrect 'data_type' configuration ('DATAELEMENT', 'DATASET', 'INDICATOR', 'POPULATION')")

    try:
        snis_format = pd.DataFrame(
            columns=[
                "data_type",
                "dx_uid",
                "period",
                "org_unit",
                "category_option_combo",
                "attribute_option_combo",
                "rate_type",
                "domain_type",
                "value",
            ]
        )
        snis_format["period"] = dhis_data.period
        snis_format["org_unit"] = dhis_data.orgUnit
        snis_format["domain_type"] = domain_type
        snis_format["value"] = dhis_data.value
        snis_format["data_type"] = data_type
        if data_type in ["DATAELEMENT", "POPULATION"]:
            snis_format["dx_uid"] = dhis_data.dataElement
            snis_format["category_option_combo"] = dhis_data.categoryOptionCombo
            snis_format["attribute_option_combo"] = dhis_data.attributeOptionCombo
        if data_type == "DATASET":
            snis_format[["dx_uid", "rate_type"]] = dhis_data.dx.str.split(".", expand=True)
        if data_type == "INDICATOR":
            snis_format["dx_uid"] = dhis_data.dx
        return snis_format

    except AttributeError as e:
        raise AttributeError(f"Routine data is missing a required attribute: {e}")
    except Exception as e:
        raise Exception(f"Unexpected Error while creating routine format table: {e}")


def configure_login(logs_path: Path, task_name: str):
    # Configure logging
    logs_path.mkdir(parents=True, exist_ok=True)
    now = datetime.now().strftime("%Y-%m-%d-%H_%M")
    logging.basicConfig(
        filename=logs_path / f"{task_name}_{now}.log",
        level=logging.INFO,
        format="%(asctime)s - %(message)s",
    )


def apply_dataelement_mappings(
    datapoints_df: pd.DataFrame, mappings: dict, coc_default: dict, aoc_default: dict
) -> pd.DataFrame:
    """
    All matching ids will be replaced.
    Is user responsability to provide the correct UIDS.
    """
    dataelement_mask = datapoints_df["data_type"] == "DATAELEMENT"

    # set all COC None to Default values
    if coc_default:
        coc_default_value = next(iter(coc_default.values()))
        current_run.log_info(f"Using {coc_default_value} default COC for data elements.")
        datapoints_df.loc[dataelement_mask, "category_option_combo"] = datapoints_df.loc[
            dataelement_mask, "category_option_combo"
        ].replace({None: coc_default_value})

    # set all AOC None to Default values
    if aoc_default:
        aoc_default_value = next(iter(aoc_default.values()))
        current_run.log_info(f"Using {aoc_default_value} default AOC for data elements.")
        datapoints_df.loc[dataelement_mask, "attribute_option_combo"] = datapoints_df.loc[
            dataelement_mask, "attribute_option_combo"
        ].replace({None: aoc_default_value})

    # Loop over the DataElement COC mappings
    for uid, mapping in mappings.items():
        # Set COC variable mappings
        coc_uids_map = mapping.get("CAT_OPTION_COMBO", {})
        coc_uids_map.update(coc_default)
        coc_uids_map = {k.strip(): v.strip() for k, v in coc_uids_map.items()}
        allowed_cocs = list(coc_uids_map.keys())

        # Set AOC variable mappings
        aoc_uids_map = mapping.get("ATTR_OPTION_COMBO", {})
        aoc_uids_map.update(aoc_default)
        aoc_uids_map = {k.strip(): v.strip() for k, v in aoc_uids_map.items()}
        allowed_aocs = list(aoc_uids_map.keys())

        # Step 1: Remove rows where the dx_uid matches, but the COC is not in the allowed list
        mask_uid = (datapoints_df["data_type"] == "DATAELEMENT") & (datapoints_df["dx_uid"] == uid.strip())

        # Remove rows where COC is not in the allowed list
        if allowed_cocs:
            coc_mask_to_remove = mask_uid & ~datapoints_df["category_option_combo"].isin(allowed_cocs)
            datapoints_df = datapoints_df[~coc_mask_to_remove].copy()

        # Remove rows where AOC is not in the allowed list
        if allowed_aocs:
            aoc_mask_to_remove = mask_uid & ~datapoints_df["attribute_option_combo"].isin(allowed_aocs)
            datapoints_df = datapoints_df[~aoc_mask_to_remove].copy()

        # Step 2: Replace remaining COC/AOC values using the provided mapping
        dataelement_mask = datapoints_df["data_type"] == "DATAELEMENT"  # reindexing
        mask_uid = dataelement_mask & (datapoints_df["dx_uid"] == uid.strip())  # reindexing
        datapoints_df.loc[mask_uid, "category_option_combo"] = datapoints_df.loc[
            mask_uid, "category_option_combo"
        ].replace(coc_uids_map)
        datapoints_df.loc[mask_uid, "attribute_option_combo"] = datapoints_df.loc[
            mask_uid, "attribute_option_combo"
        ].replace(aoc_uids_map)

    return datapoints_df


def select_transform_to_json(data_values: pd.DataFrame):
    if data_values is None:
        return [], []
    valid = []
    not_valid = []
    to_delete = []
    for _, row in data_values.iterrows():
        dpoint = DataPoint(row)
        if dpoint.is_valid():
            valid.append(dpoint.to_json())
        elif dpoint.is_to_delete():
            to_delete.append(dpoint.to_delete_json())
        else:
            not_valid.append(row)  # row is the original data, not the json
    return valid, not_valid, to_delete


def log_ignored_or_na(report_path, datapoint_list, logger: logging.Logger, data_type="population", is_na=False):
    if len(datapoint_list) > 0:
        current_run.log_info(
            f"{len(datapoint_list)} datapoints will be  {'updated to NA' if is_na else 'ignored'}. Please check the report for details {report_path}"
        )
        logger.warning(f"{len(datapoint_list)} {data_type} datapoints to be ignored: ")
        for i, ignored in enumerate(datapoint_list, start=1):
            logger.warning(f"{i} DataElement {'NA' if is_na else ''} ignored: {ignored}")


def push_data_elements(
    dhis2_client: DHIS2,
    data_elements_list: list,
    strategy: str = "CREATE_AND_UPDATE",
    dry_run: bool = True,
    max_post: int = 1000,
):
    """
    dry_run: This parameter can be set to true to get an import summary without actually importing data (DHIS2).
    """

    # max_post instead of MAX_POST_DATA_VALUES
    summary = {
        "import_counts": {"imported": 0, "updated": 0, "ignored": 0, "deleted": 0},
        "import_options": {},
        "ERRORS": [],
    }

    total_datapoints = len(data_elements_list)
    count = 0

    for chunk in split_list(data_elements_list, max_post):
        count = count + 1
        try:
            # chunk_period = list(set(c.get("period") for c in chunk))[0]

            r = dhis2_client.api.session.post(
                f"{dhis2_client.api.url}/dataValueSets",
                json={"dataValues": chunk},
                params={
                    "dryRun": dry_run,
                    "importStrategy": strategy,
                    "preheatCache": True,
                    "skipAudit": True,
                },  # speed!
            )
            r.raise_for_status()

            try:
                response_json = r.json()
                status = response_json.get("httpStatus")
                response = response_json.get("response")
            except json.JSONDecodeError as e:
                summary["ERRORS"].append(f"Response JSON decoding failed: {e}")  # period: {chunk_period}")
                response_json = None
                status = None
                response = None

            if status != "OK" and response:
                summary["ERRORS"].append(response)

            if response:
                for key in ["imported", "updated", "ignored", "deleted"]:
                    summary["import_counts"][key] += response.get("importCount", {}).get(key, 0)

        except requests.exceptions.RequestException as e:
            try:
                response = r.json().get("response")
            except (ValueError, AttributeError):
                response = None

            if response:
                for key in ["imported", "updated", "ignored", "deleted"]:
                    summary["import_counts"][key] += response["importCount"][key]

            error_response = get_response_value_errors(response, chunk=chunk)
            summary["ERRORS"].append({"error": e, "response": error_response})

        if (count * max_post) % 100000 == 0:
            current_run.log_info(
                f"{count * max_post} / {total_datapoints} data points pushed summary: {summary['import_counts']}"
            )

    return summary


def get_response_value_errors(response, chunk):
    """
    Collect relevant data for error logs
    """
    if response is None:
        return None

    if chunk is None:
        return None

    try:
        out = {}
        for k in ["responseType", "status", "description", "importCount", "dataSetComplete"]:
            out[k] = response.get(k)
        if "conflicts" in response and response["conflicts"]:
            out["rejected_datapoints"] = []
            for i in response["rejectedIndexes"]:
                out["rejected_datapoints"].append(chunk[i])
            out["conflicts"] = {}
            for conflict in response["conflicts"]:
                out["conflicts"]["object"] = conflict.get("object")
                out["conflicts"]["objects"] = conflict.get("objects")
                out["conflicts"]["value"] = conflict.get("value")
                out["conflicts"]["errorCode"] = conflict.get("errorCode")
        return out
    except AttributeError:
        return None


def log_summary_errors(summary: dict, logger: logging.Logger) -> None:
    """
    Logs all the errors in the summary dictionary using the configured logging.

    Args:
        summary (dict): The dictionary containing import counts and errors.
    """
    errors = summary.get("ERRORS", [])
    if not errors:
        logger.info("No errors found in the summary.")
    else:
        logger.error(f"Logging {len(errors)} error(s) from export summary.")
        # for i, error in enumerate(errors, start=1):
        #     logging.error(f"Error {i}: {error}")
        #         logging.error(f"Logging {len(errors)} error(s) from export summary.")
        for i_e, error in enumerate(errors, start=1):
            logger.error(f"Error {i_e} : HTTP request failed : {error.get('error', None)}")
            error_response = error.get("response", None)
            if error_response:
                rejected_list = error_response.pop("rejected_datapoints", [])
                logger.error(f"Error response : {error_response}")
                for i_r, rejected in enumerate(rejected_list, start=1):
                    logger.error(f"Rejected data point {i_r}: {rejected}")


if __name__ == "__main__":
    dhis2_data_elements_sync()
