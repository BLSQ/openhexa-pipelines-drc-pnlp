import os
import pandas as pd
import polars as pl
import json
from datetime import datetime
from dateutil.relativedelta import relativedelta
from itertools import product
# from typing import List
# from types import MethodType

from utils import (
    get_periods_yyyymm,
    merge_dataframes,
    initialize_queue,
    enqueue,
    save_to_parquet,
    first_day_of_future_month,
)

from openhexa.sdk import current_run, pipeline, workspace, parameter
from openhexa.toolbox.dhis2 import DHIS2

from openhexa.toolbox.dhis2.api import DHIS2Error


@pipeline("dhis2-snis-extract", name="dhis2_snis_extract")
@parameter(
    "extract_orgunits",
    name="Extract Organisation Units",
    help="",
    type=bool,
    default=True,
    required=False,
)
@parameter(
    "extract_pop",
    name="Extrat population",
    help="",
    type=bool,
    default=True,
    required=False,
)
@parameter(
    "extract_analytics",
    name="Extract analytics",
    help="",
    type=bool,
    default=True,
    required=False,
)
def dhis2_snis_extract(extract_orgunits: bool, extract_pop: bool, extract_analytics: bool):
    """
    Simple pipeline to retrieve the a monthly PNLP related data extract from DHIS2 SNIS instance.
    """

    # setup variables
    PIPELINE_ROOT = os.path.join(f"{workspace.files_path}", "pipelines", "dhis2_snis_extract")

    try:
        # load config
        config = load_configuration(pipeline_path=PIPELINE_ROOT)

        # connect to DHIS2 SNIS
        dhis2_client = connect_to_dhis2(config=config, cache_dir=None)

        # retrieve pyramid (for alignment)
        ready = extract_snis_pyramid(
            pipeline_path=PIPELINE_ROOT, dhis2_snis_client=dhis2_client, run_pipeline=extract_orgunits
        )

        # retrieve population
        pop_ready = extract_snis_population(
            pipeline_path=PIPELINE_ROOT,
            dhis2_snis_client=dhis2_client,
            config=config,
            run_pipeline=extract_pop,
            wait=ready,
        )

        # retrieve data extracts
        extract_snis_data(
            pipeline_path=PIPELINE_ROOT,
            config=config,
            dhis2_snis_client=dhis2_client,
            run_pipeline=extract_analytics,
            wait=pop_ready,
        )

    except Exception as e:
        current_run.log_error(f"Error occurred: {e}")


@dhis2_snis_extract.task
def load_configuration(pipeline_path: str) -> dict:
    """
    Reads a JSON file configuration and returns its contents as a dictionary.

    Args:
        pipeline_path (str): Root path of the pipeline to find the file.

    Returns:
        dict: Dictionary containing the JSON data.
    """
    try:
        file_path = os.path.join(pipeline_path, "config", "snis_extraction_config.json")
        with open(file_path, "r") as file:
            data = json.load(file)

        current_run.log_info("Configuration loaded.")
        return data
    except FileNotFoundError as e:
        raise Exception(f"The file '{file_path}' was not found {e}")
    except json.JSONDecodeError as e:
        raise Exception(f"Error decoding JSON: {e}")
    except Exception:
        raise


@dhis2_snis_extract.task
def connect_to_dhis2(config: dict, cache_dir: str):
    try:
        connection = workspace.dhis2_connection(config["EXTRACT_SETTINGS"]["DHIS2_CONNECTION"])
        dhis2_client = DHIS2(connection=connection, cache_dir=cache_dir)

        current_run.log_info(f"Connected to DHIS2 connection: {config['EXTRACT_SETTINGS']['DHIS2_CONNECTION']}")
        return dhis2_client
    except Exception as e:
        raise Exception(f"Error while connecting to DHIS2 SNIS: {e}")


@dhis2_snis_extract.task
def extract_snis_pyramid(pipeline_path: str, dhis2_snis_client: DHIS2, run_pipeline: bool) -> int:
    """Pyramid extraction task.

    extracts and saves a pyramid dataframe for all levels (could be set via config in the future)
    """
    if not run_pipeline:
        return True

    current_run.log_info("Retrieving SNIS DHIS2 pyramid data")

    try:
        # Retrieve all available levels..
        levels = pl.DataFrame(dhis2_snis_client.meta.organisation_unit_levels())
        org_levels = levels.select("level").unique().sort(by="level").to_series().to_list()
        org_units = dhis2_snis_client.meta.organisation_units(
            fields="id,name,shortName,openingDate,closedDate,parent,level,path,geometry"
        )
        org_units = pd.DataFrame(org_units)

        current_run.log_info(
            f"Extracted {len(org_units[org_units.level == org_levels[-1]].id.unique())} units at organisation unit level {org_levels[-1]}"
        )

        # pyramid_table = build_pyramid_table(org_units, org_levels)
        pyramid_fname = os.path.join(pipeline_path, "data", "raw", "pyramid", "snis_pyramid.parquet")

        # Save as Parquet
        save_to_parquet(data=org_units, filename=pyramid_fname)
        current_run.log_info(f"SNIS DHIS2 pyramid data saved: {pyramid_fname}")
        return True

    except Exception as e:
        raise Exception(f"Error while extracting SNIS DHIS2 Pyramid: {e}")


@dhis2_snis_extract.task
def extract_snis_population(pipeline_path: str, dhis2_snis_client: str, config: dict, run_pipeline: bool, wait: bool):
    if not run_pipeline:
        return True

    try:
        current_run.log_info("Retrieving SNIS DHIS2 population data")

        # retrieve AIRE de sante list from SNIS
        aires_list = retrieve_ou_list(dhis2_client=dhis2_snis_client, ou_level=4)

        # output file name for population
        pop_fname = os.path.join(pipeline_path, "data", "raw", "population", "snis_population.parquet")

        # start year
        if config["EXTRACT_SETTINGS"]["STARTDATE"] == "":
            start = "201601"
        else:
            start = config["EXTRACT_SETTINGS"]["STARTDATE"]

        # end year
        if config["EXTRACT_SETTINGS"]["ENDDATE"] == "":
            end = datetime.now().strftime("%Y%m")
        else:
            end = config["EXTRACT_SETTINGS"]["ENDDATE"]

        # Get periods
        extract_periods = [p[:4] for p in get_periods_yyyymm(start, end)]
        extract_periods = list(set(extract_periods))
        extract_periods.sort()

        # retrieve
        raw_pop_data = dhis2_snis_client.data_value_sets.get(
            data_elements=config["POPULATION_UIDS"],
            org_units=aires_list,
            periods=extract_periods,
        )

        # save raw dhis2 data
        population_table = pd.DataFrame(raw_pop_data)
        # format and save
        population_table_formatted = map_to_snis_format(dhis_data=population_table, data_type="POPULATION")
        save_to_parquet(population_table_formatted, filename=pop_fname)
        current_run.log_info(f"SNIS DHIS2 population from {start} to {end} saved: {pop_fname}")
        return True
    except Exception as e:
        raise Exception(f"Population task error : {e}")


@dhis2_snis_extract.task
def extract_snis_data(pipeline_path: str, config: dict, dhis2_snis_client: DHIS2, run_pipeline: bool, wait: bool):
    """Data extraction task."""

    if not run_pipeline:
        return True

    # NOTE: We add this function to the class to remind me to update the library!
    # dhis2_snis_client.data_value_sets.snis_get = MethodType(snis_get, dhis2_snis_client.data_value_sets)
    try:
        # Set start and end periods ---> with a window of 6 months we are safe: 6 months DEFAULT
        months_lag = config["EXTRACT_SETTINGS"].get("NUMBER_MONTHS_WINDOW", 6)  # default 6 months window

        # STARTDATE overwrites month window
        if config["EXTRACT_SETTINGS"]["STARTDATE"] == "":
            start = (datetime.now() - relativedelta(months=months_lag)).strftime("%Y%m")
        else:
            start = config["EXTRACT_SETTINGS"]["STARTDATE"]

        if config["EXTRACT_SETTINGS"]["ENDDATE"] == "":
            end = (datetime.now() - relativedelta(months=1)).strftime("%Y%m")  # go back 1 month.
        else:
            end = config["EXTRACT_SETTINGS"]["ENDDATE"]

        # Periods to download (monthly strings YYYYMM)
        extract_periods = get_periods_yyyymm(start, end)

        # retrieve FOSA ids from SNIS
        fosa_list = retrieve_ou_list(dhis2_client=dhis2_snis_client, ou_level=5)
        current_run.log_info(f"Download MODE: {config['EXTRACT_SETTINGS']['MODE']} from: {start} to {end}")

        # retrieve data
        for period in extract_periods:
            current_run.log_info(f"Retrieving SNIS data extract for period : {period}")
            handle_extract_for_period(dhis2_snis_client, period, fosa_list, config, pipeline_path)

        current_run.log_info("Process finished.")

    except Exception as e:
        # current_run.log_error(f"Extract task error : {e}")
        raise Exception(f"Extract task error : {e}")


def retrieve_ou_list(dhis2_client: DHIS2, ou_level: int) -> list:
    try:
        # Retrieve organisational units and filter by ou_level
        ous = pd.DataFrame(dhis2_client.meta.organisation_units())
        ou_list = ous.loc[ous.level == ou_level].id.to_list()

        # Log the result based on the OU level
        if ou_level == 5:
            current_run.log_info(f"Retrieved SNIS DHIS2 FOSA id list {len(ou_list)}")
        elif ou_level == 4:
            current_run.log_info(f"Retrieved SNIS DHIS2 Aires de Sante id list {len(ou_list)}")
        else:
            current_run.log_info(f"Retrieved SNIS DHIS2 OU level {ou_level} id list {len(ou_list)}")

        return ou_list

    except Exception as e:
        raise Exception(f"Error while retrieving OU id list for level {ou_level}: {e}")


def handle_extract_for_period(dhis2_client: DHIS2, period: str, org_unit_list: list, config: dict, root: str):
    """
    This function retrieves and processes data from the DHIS2 system for a given period
    and mode of extraction, saving the results in a Parquet file.

    Parameters:
    ----------
    dhis2_client : DHIS2
        An instance of the DHIS2 client used for API calls to retrieve data.
    period : str
        The period for data extraction in the "YYYYMM" format (e.g., "202409" for September 2024 -> MONTHLY for SNIS)
    org_unit_list : list
        A list of organizational unit IDs for which data needs to be extracted.
    config : dict
        A dictionary containing the extraction configuration.
        Expected keys include:
            - "MODE": Specifies the mode of extraction ("DOWNLOAD_REPLACE" or "DOWNLOAD_UPDATE").
                DOWNLOAD_REPLACE: Full download of the period and replace the corresponding extract file
                DOWNLOAD_NEW: Full download of the period if the extract does not exist. If exist is skipped.
                DOWNLOAD_UPDATE: Retrieval with "lastUpdated" and update the corresponding extract file
            - "LASTUPDATE_WINDOW" (used in "DOWNLOAD_UPDATE"): starting point from where to obtain last updates in Number of months after.
                ATTENTION: THIS PARAMETER IS ONLY WORKING IN THE DATAVALUESET endpoint.
                examples:
                    period: 202409 and LASTUPDATE_WINDOW: 1 --> lastUpdated = "20241001".
                    period: 202409 and LASTUPDATE_WINDOW: 2 --> lastUpdated = "20241101").
    root : str
        The root directory or the pipeline

    Returns:
    --------
    None
    """

    # fname period extract (We can use the config["DHIS2_CONNECTION"] as reference in the file)
    extract_fname = os.path.join(root, "data", "raw", "extracts", f"snis_data_{period}.parquet")

    # initialize update queue
    queue_db_path = os.path.join(root, "config", ".queue.db")
    initialize_queue(queue_db_path)

    # limits
    dhis2_client.analytics.MAX_DX = 10
    dhis2_client.analytics.MAX_OU = 10

    download_settings = config["EXTRACT_SETTINGS"].get("MODE", None)
    if download_settings is None:
        raise ValueError("No 'MODE' configuration found in the extraction settings.")

    # a bit of code replication... but readable (i guess)
    # Download and replace the extract (create functions for each mode?)
    if download_settings == "DOWNLOAD_REPLACE":
        raw_routine_data = retrieve_snis_routine_extract(
            dhis2_client,
            period,
            org_unit_list,
            config["ROUTINE_DATA_ELEMENT_UIDS"],
            lastUpdated=None,
        )
        raw_rates_data = retrieve_snis_rates_extract(dhis2_client, period, org_unit_list, config["RATE_UIDS"])
        raw_acm_data = retrieve_snis_acm_extract(dhis2_client, period, org_unit_list, config["ACM_INDICATOR_UIDS"])
        raw_data = merge_dataframes([raw_routine_data, raw_rates_data, raw_acm_data])
        if raw_data is not None:
            if os.path.exists(extract_fname):
                current_run.log_info(f"Replacing extract for period {period}.")
            save_to_parquet(raw_data, extract_fname)
            enqueue(period, queue_db_path)
        else:
            current_run.log_info(f"Nothing to save for period {period}..")

    # Download new data if the extract does not exist
    elif download_settings == "DOWNLOAD_NEW":
        if os.path.exists(extract_fname):
            current_run.log_info(f"Extract for period {period} already exist, download skipped.")
        else:
            raw_routine_data = retrieve_snis_routine_extract(
                dhis2_client,
                period,
                org_unit_list,
                config["ROUTINE_DATA_ELEMENT_UIDS"],
                lastUpdated=None,
            )
            raw_rates_data = retrieve_snis_rates_extract(dhis2_client, period, org_unit_list, config["RATE_UIDS"])
            raw_acm_data = retrieve_snis_acm_extract(dhis2_client, period, org_unit_list, config["ACM_INDICATOR_UIDS"])
            raw_data = merge_dataframes([raw_routine_data, raw_rates_data, raw_acm_data])
            if raw_data is not None:
                save_to_parquet(raw_data, extract_fname)
                enqueue(period, queue_db_path)
            else:
                current_run.log_info(f"Nothing to save for period {period}..")

    # NOTE: It is possible that this query takes so long that is not worth checking..
    # therefore we can just download the data. (the bottle neck is in DHIS2, not in the data transfer apparently)
    # In that case we can simply remove this code :'(
    # Download and update the extract if it exists
    elif download_settings == "DOWNLOAD_UPDATE":
        if os.path.exists(extract_fname):
            lastUpdated = first_day_of_future_month(period, config["EXTRACT_SETTINGS"]["LASTUPDATE_WINDOW"])
            raw_routine_data = retrieve_snis_routine_extract(
                dhis2_client,
                period,
                org_unit_list,
                config["ROUTINE_DATA_ELEMENT_UIDS"],
                lastUpdated=lastUpdated,  # Check for new data after lastUpdated
            )
            if raw_routine_data is not None:
                current_run.log_info(f"Updating routine data for extract period {period} (lastUpdated={lastUpdated})")
                raw_rates_data = retrieve_snis_rates_extract(dhis2_client, period, org_unit_list, config["RATE_UIDS"])
                raw_acm_data = retrieve_snis_acm_extract(
                    dhis2_client, period, org_unit_list, config["ACM_INDICATOR_UIDS"]
                )
                raw_data = merge_dataframes([raw_routine_data, raw_rates_data, raw_acm_data])
                # Update the extract
                raw_data_updated = update_snis_extract(
                    new_data_df=raw_data,
                    target_df=pd.read_parquet(extract_fname),
                )
                save_to_parquet(raw_data_updated, extract_fname)
                enqueue(period, queue_db_path)

        else:
            raw_routine_data = retrieve_snis_routine_extract(
                dhis2_client,
                period,
                org_unit_list,
                config["ROUTINE_DATA_ELEMENT_UIDS"],
                lastUpdated=None,
            )
            raw_rates_data = retrieve_snis_rates_extract(dhis2_client, period, org_unit_list, config["RATE_UIDS"])
            raw_acm_data = retrieve_snis_acm_extract(dhis2_client, period, org_unit_list, config["ACM_INDICATOR_UIDS"])
            raw_data = merge_dataframes([raw_routine_data, raw_rates_data, raw_acm_data])

        # save
        if raw_data is not None:
            save_to_parquet(raw_data, extract_fname)
            enqueue(period, queue_db_path)
        else:
            current_run.log_info(f"Nothing to save for period {period}..")
    else:
        raise ValueError("Incorrect 'MODE' configuration.")


def retrieve_snis_routine_extract(
    dhis2_snis_client: DHIS2,
    period: str,
    org_unit_list: list,
    routine_ids: list,
    lastUpdated=None,
):
    # response = dhis2_snis_client.data_value_sets.snis_get(
    #     data_elements=routine_ids,
    #     periods=[period],
    #     org_units=org_unit_list,
    #     last_updated=lastUpdated,
    # )
    response = dhis2_snis_client.data_value_sets.get(
        data_elements=routine_ids,
        periods=[period],
        org_units=org_unit_list,
        last_updated=lastUpdated,
    )
    return map_to_snis_format(pd.DataFrame(response), data_type="DATAELEMENT")


def retrieve_snis_rates_extract(dhis2_snis_client: DHIS2, period: str, org_unit_list: list, rate_ids: dict):
    reporting_datasets = rate_ids["REPORTING_DATASETS"]
    reporting_des = [f"{ds}.{metric}" for ds, metric in product(reporting_datasets, rate_ids["METRICS"])]
    try:
        response = dhis2_snis_client.analytics.get(
            data_elements=reporting_des,
            periods=[period],
            org_units=org_unit_list,
            include_cocs=False,
        )
    except DHIS2Error as e:
        current_run.log_error(f"Error while retrieving rates data: {e}")

    raw_data_formatted = pd.DataFrame(response).rename(columns={"pe": "period", "ou": "orgUnit"})
    return map_to_snis_format(raw_data_formatted, data_type="DATASET")


def retrieve_snis_acm_extract(dhis2_snis_client: DHIS2, period: str, org_unit_list: list, acm_indicator_ids: list):
    try:
        response = dhis2_snis_client.analytics.get(
            indicators=acm_indicator_ids,
            periods=[period],
            org_units=org_unit_list,
            include_cocs=False,
        )
    except DHIS2Error as e:
        current_run.log_error(f"Error while retrieving ACM data: {e}")
        raise

    raw_data_formatted = pd.DataFrame(response).rename(columns={"pe": "period", "ou": "orgUnit"})
    return map_to_snis_format(raw_data_formatted, data_type="INDICATOR")


def map_to_snis_format(
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


# Update dataframe with new data
def update_snis_extract(new_data_df: pd.DataFrame, target_df: pd.DataFrame) -> pd.DataFrame:
    """
    Updates the values in target_df with matching values from update_df,
    and appends new rows from update_df that do not exist in target_df.

    Args:
        target_df (pd.DataFrame): The DataFrame to update.
        new_data_df (pd.DataFrame): The DataFrame containing updates and new rows.

    Returns:
        pd.DataFrame: Updated DataFrame.
    """

    # Merge DataFrames on key columns
    updated_df = pd.merge(
        target_df,
        new_data_df,
        on=[
            "data_type",
            "dx_uid",
            "period",
            "org_unit",
            "category_option_combo",
            "attribute_option_combo",
            "rate_type",
            "domain_type",
        ],
        how="outer",
        suffixes=("_old", ""),
    )

    # If there's a new value in update_df, replace the old value
    updated_df["value"] = updated_df["value"].fillna(updated_df["value_old"])
    updated_df = updated_df.drop(columns=["value_old"])

    return updated_df


if __name__ == "__main__":
    dhis2_snis_extract()
