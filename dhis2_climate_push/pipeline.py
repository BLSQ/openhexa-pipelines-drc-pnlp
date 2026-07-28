import ast
import json
import logging
from collections.abc import Generator
from datetime import datetime
from pathlib import Path

import geopandas as gpd
import pandas as pd
from d2d_development.push import DHIS2Pusher
from openhexa.sdk import current_run, pipeline, workspace
from openhexa.toolbox.dhis2 import DHIS2
from org_units_aligner.org_units_aligner import DHIS2PyramidAligner
from shapely.geometry import mapping
from sqlalchemy import create_engine
from utils import configure_logging, connect_to_dhis2, read_json_file, save_json_file, save_logs


@pipeline("dhis2_climate_push")
def dhis2_climate_push():
    """Pipeline to push climate data to DHIS2."""
    current_run.log_info("Starting climate pipeline...")
    pipeline_path = Path(workspace.files_path) / "pipelines" / "dhis2_climate_push"

    config = read_json_file(pipeline_path / "config" / "pnlp_climate_push_config.json")
    dhis2_client = connect_to_dhis2(connection_str=config["CLIMATE_PUSH_SETTINGS"]["DHIS2_CONNECTION_TARGET"])

    # TODO:
    # THIS PIPELINE SHOULD BE A SLAVE OF THE ERA5 PIPELINES EXECUTED FROM THERE
    # THE ERA5 PIPELINES WILL DOWNLOAD AND UPDATE THE SHAPES TABLE
    # THIS PIPELINE WILL MAKE THE ALIGNMENT USING THAT UPDATED TABLE
    try:
        # NOTE: we could implement a check at the begining to execute only when there is new data..
        # push_organisation_units(
        #     pipeline_path=pipeline_path,
        #     dhis2_client_target=dhis2_client,
        #     config=config,
        #     run_task=True,
        # )

        # Run precipitation task
        # precipitation_push(pipeline_path=pipeline_path, dhis2_client_target=dhis2_client, config=config)

        # Run temperature min task
        # tempareture_min_push(pipeline_path=pipeline_path, dhis2_client_target=dhis2_client, config=config)

        # Run temperature task
        tempareture_max_push(pipeline_path=pipeline_path, dhis2_client_target=dhis2_client, config=config)

        # Run humidity task
        # relative_humidity_push(pipeline_path=pipeline_path, dhis2_client_target=dhis2_client, config=config)

    except Exception as e:
        current_run.log_error(f"An error occurred: {e}")
        raise


def push_organisation_units(pipeline_path: Path, dhis2_client_target: DHIS2, config: dict, run_task: bool) -> bool:
    """Task to handle creation and updates of organisation units in the target DHIS2 (incremental approach only).

    We use the previously extracted pyramid (full) stored as dataframe as input.
    The format of the pyramid contains the expected columns. A dataframe that doesn't contain the
    mandatory columns will be skipped (not valid).

    Returns:
        bool: True if the task was executed successfully, False otherwise.
    """
    if not run_task:
        return True

    current_run.log_info("Starting organisation units push.")
    # logger, logs_file = configure_logging(logs_path=Path("/home/jovyan/tmp/logs"), task_name="push_orgunits")
    logger, logs_file = configure_logging(logs_path=pipeline_path / "logs", task_name="push_orgunits")  ## local

    # WE ALIGN ONLY THE ZONES DE SANTE USED FOR CLIMATE METRICS.
    # Load pyramid from boundaries DB table (cod_iaso_zone_de_sante)
    # (this table is updated by era5_precipitation pipeline Zones de sante level only)
    dbengine = create_engine(workspace.database_url)
    cod_zs_boundaries_table = gpd.read_postgis(
        config["CLIMATE_PUSH_SETTINGS"]["BOUNDARIES_TABLE"], con=dbengine, geom_col="geometry"
    )

    # Use 'mapping' to convert geometry to GeoJSON-like dictionary
    cod_zs_boundaries_table["geometry_json"] = cod_zs_boundaries_table["geometry"].apply(
        lambda x: json.dumps(mapping(x))
    )
    orgunit_source = pd.DataFrame(cod_zs_boundaries_table.drop(columns=["geometry", "parent"]))
    orgunit_source = orgunit_source.rename(columns={"ref": "id", "ou_parent": "parent", "geometry_json": "geometry"})
    orgunit_source = orgunit_source[
        ["id", "name", "shortName", "openingDate", "closedDate", "parent", "geometry"]
    ]  # format

    # convert that column to dictionary if possible
    orgunit_source["parent"] = orgunit_source["parent"].apply(safe_eval)

    current_run.log_info("Starting organisation units push.")

    try:
        DHIS2PyramidAligner(logger=logger, logging_interval=100).align_to(
            target_dhis2=dhis2_client_target,
            source_pyramid=orgunit_source,
        )
    finally:
        save_logs(logs_file, output_dir=pipeline_path / "logs" / "push_orgunits")


# convert str to dict
def safe_eval(val: dict) -> dict | None:
    """Evaluate a string as a Python literal safely.

    Returns:
        dict | None: The evaluated dictionary if successful, None otherwise.
    """
    try:
        return ast.literal_eval(val)
    except (ValueError, SyntaxError):
        return None


def precipitation_push(pipeline_path: Path, dhis2_client_target: DHIS2, config: dict) -> None:
    """Put some data processing code here.

    Args:
        pipeline_path (Path): Path to the pipeline directory.
        dhis2_client_target (DHIS2): DHIS2 client for the target instance.
        config (dict): Configuration dictionary.
    """
    current_run.log_info("Precipitation data push started...")
    logger, logs_file = configure_logging(logs_path=Path("/home/jovyan/tmp/logs"), task_name="precipitation_push")
    # logger, logs_file = configure_logging(logs_path=pipeline_path / "logs", task_name="precipitation_push")  ## local

    # Parameters for the import
    import_strategy = config["CLIMATE_PUSH_SETTINGS"].get("IMPORT_STRATEGY", "CREATE_AND_UPDATE")
    dry_run = config["CLIMATE_PUSH_SETTINGS"].get("DRY_RUN", True)
    max_post = config["CLIMATE_PUSH_SETTINGS"].get("MAX_POST", 500)

    # Get last date pushed for precipitation
    try:
        last_pushed_date = get_last_pushed_date(pipeline_path / "config", "precipitation")
        if last_pushed_date is None:
            current_run.log_warning("The last precipitation date was not set. Falling back to default : 2017-01-01")
            last_pushed_date = "2017-01-01"
    except Exception as e:
        current_run.log_warning(f"The last precipitation date was not found: {e}. Falling back to default : 2017-01-01")
        last_pushed_date = "2017-01-01"  # Default

    # Load precipitation data from DB table
    table_name = config["CLIMATE_PUSH_SETTINGS"].get("PRECIPITATION_TABLE", None)
    if table_name is None:
        current_run.log_error("Precipitation table name is not provided.")
        raise ValueError("Precipitation table name is not provided.")

    try:
        current_run.log_info(f"Loading precipitation data from database table: {table_name}")
        precipitation_data = load_climate_data(table_name=table_name)
    except Exception as e:
        current_run.log_error(f"Failed to load precipitation data: {e}")

    # Check for new data.
    precip_date_max = precipitation_data.start_date.max()
    current_run.log_info(f"Last precipitation pushed date {last_pushed_date} - data available to : {precip_date_max}")
    if precip_date_max <= last_pushed_date:
        current_run.log_info("No new precipitation data to push.")
        return

    # Filter data using last_pushed_date
    current_run.log_info(f"Pushing new precipitation data from : {last_pushed_date}")
    precipitation_data = precipitation_data[precipitation_data.start_date >= last_pushed_date]

    # map data to DHIS2 format
    precip_dx_uid = config["PRECIPITATION_MAPPING"].get("UID", None)
    if precip_dx_uid is None:
        current_run.log_error("Precipitation UID is not provided.")
        raise ValueError

    # Format climate data to DHIS2 format
    precipitation_data = to_dhis2_format_precipitation(
        climate_data=precipitation_data, dx_uid=precip_dx_uid, logger=logger
    )

    # Apply mappings (if any..)
    precipitation_map = apply_mappings_for_climate_data(precipitation_data, config["PRECIPITATION_MAPPING"])

    msg = (
        f"Pushing precipitation data with parameters "
        f"import_strategy: {import_strategy}, dry_run: {dry_run}, max_post: {max_post}"
    )
    current_run.log_info(msg)
    logger.info(msg)

    pusher = DHIS2Pusher(
        dhis2_client=dhis2_client_target,
        import_strategy=import_strategy,
        dry_run=dry_run,
        max_post=max_post,
        logger=logger,
    )

    try:
        pusher.push_data(df_data=precipitation_map)
        update_last_available_date_log(pipeline_path / "config", "precipitation", precip_date_max)
    finally:
        save_logs(logs_file, output_dir=pipeline_path / "logs" / "push_orgunits")


def tempareture_min_push(pipeline_path: Path, dhis2_client_target: DHIS2, config: dict) -> None:
    """Put some data processing code here."""
    current_run.log_info("Temperature min data push started...")
    logger, logs_file = configure_logging(logs_path=Path("/home/jovyan/tmp/logs"), task_name="temp_min_push")
    # logger, logs_file = configure_logging(logs_path=pipeline_path / "logs", task_name="temp_min_push")  ## local

    # Parameters for the import
    import_strategy = config["CLIMATE_PUSH_SETTINGS"].get("IMPORT_STRATEGY", "CREATE_AND_UPDATE")
    dry_run = config["CLIMATE_PUSH_SETTINGS"].get("DRY_RUN", True)
    max_post = config["CLIMATE_PUSH_SETTINGS"].get("MAX_POST", 500)

    # Get last date pushed for Temperature
    try:
        last_pushed_date = get_last_pushed_date(pipeline_path / "config", "temperature_min")
        if last_pushed_date is None:
            current_run.log_warning("The last temperature_min date was not set. Falling back to default : 2017-01-01")
            last_pushed_date = "2017-01-01"
    except Exception as e:
        current_run.log_warning(
            f"The last temperature_min date was not found: {e}. Falling back to default : 2017-01-01"
        )
        last_pushed_date = "2017-01-01"  # Default

    # Load Temperature data from DB table
    table_name = config["CLIMATE_PUSH_SETTINGS"].get("TEMP_MIN_TABLE", None)
    if table_name is None:
        current_run.log_error("Temperature min table name is not provided.")
        raise ValueError

    try:
        current_run.log_info(f"Loading temperature min data from database table: {table_name}")
        temperature_min_data = load_climate_data(table_name=table_name)
    except Exception as e:
        current_run.log_error(f"Failed to load temperature min data: {e}")

    # Check for new data.
    temp_min_date_max = temperature_min_data.start.max().strftime("%Y-%m-%d")
    current_run.log_info(
        f"Last temperature min pushed date {last_pushed_date} - data available to : {temp_min_date_max}"
    )
    if temp_min_date_max <= last_pushed_date:
        current_run.log_info("No new temperature min data to push.")
        return

    # Filter data using last_pushed_date
    current_run.log_info(f"Pushing new temperature min data from : {last_pushed_date}")
    temperature_min_data = temperature_min_data[temperature_min_data.start >= last_pushed_date]

    # get uids list
    uids = config["TEMPERATURE_MAPPING_MIN"].get("UIDS", [])
    if len(uids) == 0:
        current_run.log_error("Temperature min UIDs are not provided.")
        raise ValueError

    # map uids to DHIS2 format
    dx_uid_min = uids.get("TEMP_MIN", None)
    dx_uid_max = uids.get("TEMP_MAX", None)
    dx_uid_mean = uids.get("TEMP_MEAN", None)
    if dx_uid_min is None or dx_uid_max is None or dx_uid_mean is None:
        current_run.log_error("Temperature max, min, max and mean UIDs are not provided.")
        raise ValueError

    # Format climate data to DHIS2 format
    temperature_min_data_formatted = to_dhis2_format_temperature(
        climate_data=temperature_min_data,
        dx_uid_min=dx_uid_min,
        dx_uid_max=dx_uid_max,
        dx_uid_mean=dx_uid_mean,
        data_type="TEMPERATURE_MIN",
        logger=logger,
    )

    # Apply mappings (if any..)
    temperature_min_data_formatted = apply_mappings_for_climate_data(
        temperature_min_data_formatted, config["TEMPERATURE_MAPPING_MIN"]
    )

    # push data
    msg = (
        f"Pushing Temperature min data with parameters "
        f"import_strategy: {import_strategy}, dry_run: {dry_run}, max_post: {max_post}"
    )
    current_run.log_info(msg)
    logger.info(msg)
    pusher = DHIS2Pusher(
        dhis2_client=dhis2_client_target,
        import_strategy=import_strategy,
        dry_run=dry_run,
        max_post=max_post,
        logger=logger,
    )
    try:
        pusher.push_data(df_data=temperature_min_data_formatted)
        update_last_available_date_log(pipeline_path / "config", "temperature_min", temp_min_date_max)
    finally:
        save_logs(logs_file, output_dir=pipeline_path / "logs" / "temp_min_push")


def get_last_pushed_date(json_folder: Path, node: str, json_name: str = "last_pushed_date.json") -> str:
    """Reads the value of a specific node from a JSON file.

    Args:
        json_folder (Path): Path to the JSON file.
        node (str): The name of the data type (precipitation, temperature_max, temperature_min).
        json_name (str) : The name of the JSON file (default is "last_pushed_date.json").

    Returns:
        str: The date value of the node in the file.
    """
    # Check if the file exists
    file_path = json_folder / json_name
    if not file_path.exists():
        raise FileNotFoundError(f"The file {file_path} does not exist.")

    data = read_json_file(file_path)

    # Check if the node exists
    if node not in data:
        raise KeyError(f"The node '{node}' does not exist in the JSON file.")

    return data.get(node)


def load_climate_data(table_name: str) -> pd.DataFrame:
    """Load climate data from database.

    Returns:
        pd.DataFrame: DataFrame containing the climate data.
    """
    current_run.log_info(f"Loading data from {table_name}")
    dbengine = create_engine(workspace.database_url)
    return pd.read_sql_table(table_name, con=dbengine)


def to_dhis2_format_precipitation(
    climate_data: pd.DataFrame,
    dx_uid: str,
    logger: logging,
    data_type: str = "PRECIPITATION",
    coc_default: str = "HllvX50cXC0",
    aoc_default: str = "HllvX50cXC0",
    domain_type: str = "AGGREGATED",
) -> pd.DataFrame:
    """Maps Climate data to a standardized DHIS2 data table.

    Parameters
    ----------
    climate_data : pd.DataFrame
        DataFrame containing climate data with columns: "period", "ref", "sum".
    dx_uid : str
        The UID for the data element in DHIS2.
    logger : logging
        Logger for logging messages.
    data_type : str, optional
        The type of data (default is "PRECIPITATION").
    coc_default : str, optional
        Default category option combo UID (default is "HllvX50cXC0").
    aoc_default : str, optional
        Default attribute option combo UID (default is "HllvX50cXC0").
    domain_type : str, optional
        Data domain (default is "AGGREGATED").

    Returns
    -------
    pd.DataFrame
        A DataFrame formatted to DHIS2 with the following columns:
        - "data_type": The type of data (PRECIPITATION, TEMPERATURE_MAX, TEMPERATURE_MIN).
        - "dx_uid": UID.
        - "period": Reporting period.
        - "orgUnit": Organization unit.
        - "categoryOptionCombo": Category option combo UID.
        - "rate_type": Rate type.
        - "domain_type": Data domain (AGGREGATED or TRACKER).
        - "value": Data value.
    """
    if climate_data.empty:
        return None

    accepted_types = ["PRECIPITATION", "TEMPERATURE_MAX", "TEMPERATURE_MIN"]
    if data_type not in accepted_types:
        raise ValueError(f"Incorrect 'data_type' configuration {accepted_types}")

    dhis2_format = pd.DataFrame(index=climate_data.index)
    dhis2_format["data_type"] = data_type
    dhis2_format["dx"] = dx_uid
    dhis2_format["period"] = climate_data["period"]
    dhis2_format["org_unit"] = climate_data["ref"]
    dhis2_format["category_option_combo"] = coc_default
    dhis2_format["attribute_option_combo"] = aoc_default
    dhis2_format["rate_type"] = None
    dhis2_format["domain_type"] = domain_type
    dhis2_format["value"] = climate_data["sum"]

    # Ensure all values in the column are numeric
    dhis2_format["value"] = pd.to_numeric(dhis2_format["value"], errors="coerce")

    # Apply the condition safely
    rows_to_change = dhis2_format[
        (dhis2_format["value"].notna())  # Ensure the value is not NaN
        & (abs(dhis2_format["value"]) < 0.0001)  # Value's absolute is less than 0.0001
        & (dhis2_format["value"] != 0)  # Value is not 0
    ]

    # Log the rows to be changed
    if not rows_to_change.empty:
        current_run.log_warning(
            f"{len(rows_to_change)} data points will have their 'value' replaced to 0.0001. "
            f"Please check the report for details"
        )
        for _, row in rows_to_change.iterrows():
            # current_run.log_info(
            # f'UID: {row["dx_uid"]} period: {row["period"]} ou: {row["org_unit"]} value: {row["value"]}')
            logger.info(f"UID: {row['dx']} period: {row['period']} ou: {row['org_unit']} value: {row['value']}")

    # Set the absolute values under 0.0001 to 0.0001 (rounding up)
    dhis2_format["value"] = dhis2_format["value"].apply(
        lambda x: 0.0001 if pd.notna(x) and abs(x) < 0.0001 and x != 0 else x
    )

    return dhis2_format


def apply_mappings_for_climate_data(datapoints_df: pd.DataFrame, mappings: dict) -> pd.DataFrame:
    """All matching ids will be replaced.

    Is user responsability to provide the correct IDS, ORG_UNITS, COC and AOC

    Returns:
        pd.DataFrame: DataFrame with updated org_unit, category_option_combo, and
            attribute_option_combo based on the provided mappings.
    """
    # Fields ou, coc and aoc will throw an error while pushing if wrong..
    orunits_to_replace = list(set(datapoints_df.org_unit).intersection(set(mappings.get("ORG_UNITS", {}).keys())))
    current_run.log_info(f"Number of Org units to be replaced using mappings: {len(orunits_to_replace)}.")
    datapoints_df.loc[:, "org_unit"] = datapoints_df["org_unit"].replace(mappings.get("ORG_UNITS", {}))

    coc_default = mappings["CAT_OPTION_COMBO"].get("DEFAULT")
    if coc_default:
        datapoints_df.loc[:, "category_option_combo"] = datapoints_df["category_option_combo"].replace(
            {None: coc_default}
        )
    datapoints_df.loc[:, "category_option_combo"] = datapoints_df["category_option_combo"].replace(
        mappings.get("CAT_OPTION_COMBO", {})
    )

    aoc_default = mappings["ATTR_OPTION_COMBO"].get("DEFAULT")
    if aoc_default:
        datapoints_df.loc[:, "attribute_option_combo"] = datapoints_df["attribute_option_combo"].replace(
            {None: aoc_default}
        )
    datapoints_df.loc[:, "attribute_option_combo"] = datapoints_df["attribute_option_combo"].replace(
        mappings.get("ATTR_OPTION_COMBO", {})
    )

    return datapoints_df


def update_last_available_date_log(
    json_folder: Path, node: str, iso_date: str, json_name: str = "last_pushed_date.json"
) -> None:
    """Updates the last available date for a given data source in a JSON file.

    Args:
        json_folder (str): Path to the folder containing the JSON file.
        node (str): The name of the data source (e.g., "precipitation", "temperature_max", "temperature_min").
        iso_date (str): The new last available date in ISO 8601 format.
        json_name (str, optional): The name of the JSON file. Defaults to "last_pushed_date.json".
    """
    # Validate the ISO date format
    try:
        datetime.fromisoformat(iso_date)
    except Exception as e:
        raise Exception("Invalid ISO date format. Provide a valid ISO 8601 string.") from e

    # Load existing data or create a new dictionary
    file_path = json_folder / json_name
    if file_path.exists():
        data = read_json_file(file_path)
    else:
        data = {}

    # Update the node with the new date
    data[node] = iso_date

    # Write the updated data back to the file
    save_json_file(file_path, data)
    current_run.log_info(f"Updated {node} with date {iso_date} in {file_path}.")


def to_dhis2_format_temperature(
    climate_data: pd.DataFrame,
    dx_uid_min: str,
    dx_uid_max: str,
    dx_uid_mean: str,
    logger: logging,
    data_type: str = "TEMPERATURE_MIN",
    coc_default: str = "HllvX50cXC0",
    aoc_default: str = "HllvX50cXC0",
    domain_type: str = "AGGREGATED",
) -> pd.DataFrame:
    """Maps Climate data to a standardized DHIS2 data table.

    Args:
        climate_data (pd.DataFrame): DataFrame containing climate data with columns: "epiweek", "uid", "tmin_min",
         "tmin_max", "tmin_mean" or "tmax_min", "tmax_max", "tmax_mean".
        dx_uid_min (str): The UID for the minimum temperature data element in DHIS2.
        dx_uid_max (str): The UID for the maximum temperature data element in DHIS2.
        dx_uid_mean (str): The UID for the mean temperature data element in DHIS2.
        logger (logging): Logger for logging messages.
        data_type (str, optional): The type of data (default is "TEMPERATURE_MIN").
        coc_default (str, optional): Default category option combo UID (default is "HllvX50cXC0").
        aoc_default (str, optional): Default attribute option combo UID (default is "HllvX50cXC0").
        domain_type (str, optional): Data domain (default is "AGGREGATED").

    Returns:
        pd.DataFrame
            A DataFrame formatted to DHIS2 with the following columns:
            - "data_type": The type of data (PRECIPITATION, TEMPERATURE_MAX, TEMPERATURE_MIN).
            - "dx_uid": UID.
            - "period": Reporting period.
            - "orgUnit": Organization unit.
            - "categoryOptionCombo": Category option combo UID.
            - "rate_type": Rate type.
            - "domain_type": Data domain (AGGREGATED or TRACKER).
            - "value": Data value.
    """
    if climate_data.empty:
        return None

    accepted_types = ["TEMPERATURE_MAX", "TEMPERATURE_MIN"]
    if data_type not in accepted_types:
        raise ValueError(f"Incorrect 'data_type' configuration {accepted_types}")

    uids = {"min": dx_uid_min, "max": dx_uid_max, "mean": dx_uid_mean}
    if all([uid is None and pd.isna(uid) for uid in uids.values()]):
        raise ValueError(f"Incorrect 'dx' provided for {data_type}: {uids}")

    if data_type == "TEMPERATURE_MIN":
        col_var = "tmin"
    else:
        col_var = "tmax"

    temp_table = []
    for key, value in uids.items():
        dhis2_format_sub = pd.DataFrame(index=climate_data.index)
        dhis2_format_sub["data_type"] = f"{data_type}_{key.upper()}"
        dhis2_format_sub["dx"] = value
        dhis2_format_sub["period"] = climate_data["epiweek"].str.replace(r"W0(\d)", r"W\1", regex=True)
        dhis2_format_sub["org_unit"] = climate_data["uid"]
        dhis2_format_sub["category_option_combo"] = coc_default
        dhis2_format_sub["attribute_option_combo"] = aoc_default
        dhis2_format_sub["rate_type"] = None
        dhis2_format_sub["domain_type"] = domain_type
        dhis2_format_sub["value"] = climate_data[f"{col_var}_{key}"]
        temp_table.append(dhis2_format_sub)

    dhis2_format = pd.concat(temp_table, ignore_index=True)
    # Ensure all values in the column are numeric
    dhis2_format["value"] = pd.to_numeric(dhis2_format["value"], errors="coerce")

    # Apply the condition safely
    rows_to_change = dhis2_format[
        (dhis2_format["value"].notna())  # Ensure the value is not NaN
        & (abs(dhis2_format["value"]) < 0.0001)  # Value's absolute is less than 0.0001
        & (dhis2_format["value"] != 0)  # Value is not 0
    ]

    # Log the rows to be changed
    if not rows_to_change.empty:
        current_run.log_warning(
            f"{len(rows_to_change)} data points in {data_type} will have their 'value' replaced to 0.0001. "
            f"Please check the report for details."
        )
        for _, row in rows_to_change.iterrows():
            logger.info(f"UID: {row['dx']} period: {row['period']} ou: {row['org_unit']} value: {row['value']}")

    # Set the absolute values under 0.0001 to 0.0001
    dhis2_format["value"] = dhis2_format["value"].apply(
        lambda x: 0.0001 if pd.notna(x) and abs(x) < 0.0001 and x != 0 else x
    )

    # sorting might improve speed
    return dhis2_format.sort_values(by=["org_unit", "period"], ascending=True)


if __name__ == "__main__":
    dhis2_climate_push()
