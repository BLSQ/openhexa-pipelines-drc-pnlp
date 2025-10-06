import ast
import json
import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Generator

import geopandas as gpd
import pandas as pd
import requests
from openhexa.sdk import current_run, pipeline, workspace
from openhexa.toolbox.dhis2 import DHIS2
from shapely.geometry import mapping
from sqlalchemy import create_engine


@pipeline("dhis2_climate_push")
def dhis2_climate_push():
    """
    This pipeline pushes climate data to DHIS2.

    """
    current_run.log_info("Starting climate pipeline...")
    root_path = Path(workspace.files_path) / "pipelines" / "dhis2_climate_push"

    try:
        # Load the pipeline configuration
        pipeline_config = load_climate_config(pipeline_path=root_path)

        # connect to DHIS2
        dhis2_client = connect_to_dhis2_target(config=pipeline_config, cache_dir=None)

        # TODO:
        # THIS PIPELINE SHOULD BE A SLAVE OF THE ERA5 PIPELINES EXECUTED FROM THERE
        # THE ERA5 PIPELINES WILL DOWNLOAD AND UPDATE THE SHAPES TABLE
        # THIS PIPELINE WILL MAKE THE ALIGNMENT USING THAT UPDATED TABLE

        # NOTE: we could implement a check at the begining to execute only when there is new data..
        # Align pyramid
        push_organisation_units(
            root=root_path,
            dhis2_client_target=dhis2_client,
            config=pipeline_config,
            run_task=True,
        )

        # Run precipitation task
        precipitation_push(
            pipeline_path=root_path,
            dhis2_client_target=dhis2_client,
            config=pipeline_config,
        )

        # Run temperature min task
        tempareture_min_push(pipeline_path=root_path, dhis2_client_target=dhis2_client, config=pipeline_config)

        # Run temperature task
        tempareture_max_push(pipeline_path=root_path, dhis2_client_target=dhis2_client, config=pipeline_config)

        # Run humidity task
        relative_humidity_push(pipeline_path=root_path, dhis2_client_target=dhis2_client, config=pipeline_config)

    except Exception as e:
        current_run.log_error(f"An error occurred: {e}")


def load_climate_config(pipeline_path: Path) -> dict:
    """Load the pipeline configuration."""

    config_path = pipeline_path / "config" / "pnlp_climate_push_config.json"
    current_run.log_info(f"Loading pipeline configuration from {config_path}")

    try:
        with open(config_path, "r") as f:
            config = json.load(f)
        return config
    except FileNotFoundError as e:
        raise FileNotFoundError(f"Configuration file not found {e}.") from e
    except json.JSONDecodeError as e:
        raise json.JSONDecodeError(f"Configuration file is not a valid JSON file {e}.") from e
    except Exception as e:
        raise Exception(f"An error occurred while loading the configuration: {e}") from e


def connect_to_dhis2_target(config: dict, cache_dir: str):
    try:
        conn_id = config["CLIMATE_PUSH_SETTINGS"].get("DHIS2_CONNECTION_TARGET", None)
        if conn_id is None:
            current_run.log_error("DHIS2 connection is not provided.")
            raise ValueError

        connection = workspace.dhis2_connection(conn_id)
        dhis2_client = DHIS2(connection=connection, cache_dir=cache_dir)
        current_run.log_info(f"Connected to DHIS2 connection: {conn_id}")

        return dhis2_client

    except Exception as e:
        raise Exception(f"Error while connecting to DHIS2: {e}")


def push_organisation_units(root: Path, dhis2_client_target: DHIS2, config: dict, run_task: bool):
    """
    This task handles creation and updates of organisation units in the target DHIS2 (incremental approach only).

    We use the previously extracted pyramid (full) stored as dataframe as input.
    The format of the pyramid contains the expected columns. A dataframe that doesn't contain the
    mandatory columns will be skipped (not valid).
    """
    if not run_task:
        return True

    current_run.log_info("Starting organisation units push.")
    report_path = root / "logs" / "org_units"
    configure_logging(logs_path=report_path, task_name="climate_data_org_units")

    # WE ALIGN ONLY THE ZONES DE SANTE USED FOR CLIMATE METRICS.
    # Load pyramid from boundaries DB table (cod_iaso_zone_de_sante)
    # (this table is updated by era5_precipitation pipeline Zones de sante level only)
    dbengine = create_engine(os.environ["WORKSPACE_DATABASE_URL"])
    cod_zs_boundaries_table = gpd.read_postgis(
        config["CLIMATE_PUSH_SETTINGS"]["BOUNDARIES_TABLE"], con=dbengine, geom_col="geometry"
    )

    # Use 'mapping' to convert geometry to GeoJSON-like dictionary
    cod_zs_boundaries_table["geometry_json"] = cod_zs_boundaries_table["geometry"].apply(
        lambda x: json.dumps(mapping(x))
    )
    orgUnit_source = pd.DataFrame(cod_zs_boundaries_table.drop(columns=["geometry", "parent"]))
    orgUnit_source = orgUnit_source.rename(columns={"ref": "id", "ou_parent": "parent", "geometry_json": "geometry"})
    orgUnit_source = orgUnit_source[
        ["id", "name", "shortName", "openingDate", "closedDate", "parent", "geometry"]
    ]  # format

    # convert that column to dictionary if possible
    orgUnit_source["parent"] = orgUnit_source["parent"].apply(safe_eval)

    if orgUnit_source.shape[0] > 0:
        # Retrieve the target (NMDR/PNLP) orgUnits to compare
        current_run.log_info(f"Retrieving organisation units from target DHIS2 instance {dhis2_client_target.api.url}")
        orgUnit_target = dhis2_client_target.meta.organisation_units(
            fields="id,name,shortName,openingDate,closedDate,parent,level,path,geometry"
        )
        orgUnit_target = pd.DataFrame(orgUnit_target)

        # Get list of ids for creation and update (Zones de sante (source) - Entire pyramid (target) = new updates)
        ou_new = list(set(orgUnit_source.id) - set(orgUnit_target.id))
        ou_matching = list(set(orgUnit_source.id).intersection(set(orgUnit_target.id)))  # Check differences
        dhsi2_version = dhis2_client_target.meta.system_info().get("version")

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
                    dhis2_client_target=dhis2_client_target,
                    report_path=report_path,
                )
        except Exception as e:
            raise Exception(f"Unexpected error occurred while creating organisation units. Error: {e}")

        # Update orgUnits
        try:
            if len(ou_matching) > 0:
                current_run.log_info(f"Checking for updates in {len(ou_matching)} organisation units")
                # NOTE: Geometry is valid for versions > 2.32
                if dhsi2_version <= "2.32":
                    current_run.log_warning("DHIS2 version not compatible with geometry. Geometry will be ignored.")
                    orgUnit_source["geometry"] = None
                    orgUnit_target["geometry"] = None

                push_orgunits_update(
                    orgUnit_source=orgUnit_source,
                    orgUnit_target=orgUnit_target,
                    matching_ou_ids=ou_matching,
                    dhis2_client_target=dhis2_client_target,
                    report_path=report_path,
                )
                current_run.log_info("Organisation units push finished.")
        except Exception as e:
            raise Exception(f"Unexpected error occurred while updating organisation units. Error: {e}")

    else:
        current_run.log_warning("No data found in the pyramid file. Organisation units task skipped.")


# convert str to dict
def safe_eval(val):
    try:
        return ast.literal_eval(val)
    except (ValueError, SyntaxError):
        return None


def precipitation_push(pipeline_path: Path, dhis2_client_target: DHIS2, config: dict) -> bool:
    """Put some data processing code here."""

    current_run.log_info("Precipitation data push started...")
    report_path = pipeline_path / "logs" / "precipitation"
    configure_logging(logs_path=report_path, task_name="push_data")

    # Parameters for the import
    import_strategy = config["CLIMATE_PUSH_SETTINGS"].get("IMPORT_STRATEGY", None)
    if import_strategy is None:
        import_strategy = "CREATE_AND_UPDATE"  # CREATE, UPDATE, CREATE_AND_UPDATE

    dry_run = config["CLIMATE_PUSH_SETTINGS"].get("DRY_RUN", None)
    if dry_run is None:
        dry_run = True  # True or False

    max_post = config["CLIMATE_PUSH_SETTINGS"].get("MAX_POST", None)
    if max_post is None:
        max_post = 500  # number of datapoints without a time-out limit (?)

    # Get last date pushed for precipitation
    try:
        last_pushed_date = get_last_pushed_date(pipeline_path / "config", "precipitation")
        if last_pushed_date is None:
            current_run.log_warning("The last precipitation date was not set. Falling back to default : 2017-01-01")
            last_pushed_date = "2017-01-01"
    except Exception as e:
        current_run.log_warning(f"The last precipitation date was not found: {e}. Falling back to default : 2017-01-01")
        last_pushed_date = "2017-01-01"  # Default

    try:
        # Load precipitation data from DB table
        table_name = config["CLIMATE_PUSH_SETTINGS"].get("PRECIPITATION_TABLE", None)
        if table_name is None:
            current_run.log_error("Precipitation table name is not provided.")
            raise ValueError

        current_run.log_info(f"Loading precipitation data from database table: {table_name}")
        precipitation_data = load_climate_data(table_name=table_name)

        # Check for new data.
        precip_date_max = precipitation_data.start_date.max()
        current_run.log_info(
            f"Last precipitation pushed date {last_pushed_date} - data available to : {precip_date_max}"
        )
        if precip_date_max <= last_pushed_date:
            current_run.log_info("No new precipitation data to push.")
            return True  # No new data to push EXIT!

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
            climate_data=precipitation_data, dx_uid=precip_dx_uid, report_path=report_path
        )

        # Apply mappings (if any..)
        precipitation_data = apply_mappings_for_climate_data(precipitation_data, config["PRECIPITATION_MAPPING"])

        # convert the datapoints to DHIS2 json format and check if they are valid
        # we are sending datapoints in batch to DHIS2 ..
        # NOTE: We are not going to delete, we only populate new data into DHIS2
        datapoints_valid, datapoints_not_valid, datapoints_na = select_transform_to_json(data_values=precipitation_data)

        # log not valid datapoints
        log_ignored(report_path=report_path, datapoint_list=datapoints_not_valid, data_type="PRECIPITATION")
        log_ignored(report_path=report_path, datapoint_list=datapoints_na, data_type="PRECIPITATION", is_na=True)

        # push data
        current_run.log_info(
            f"Pushing precipitation data with parameters import_strategy: {import_strategy}, dry_run: {dry_run}, max_post: {max_post}"
        )
        logging.info(
            f"Pushing precipitation data with parameters import_strategy: {import_strategy}, dry_run: {dry_run}, max_post: {max_post}"
        )
        summary = push_data_elements(
            dhis2_client=dhis2_client_target,
            data_elements_list=datapoints_valid,
            strategy=import_strategy,
            dry_run=dry_run,
            max_post=max_post,
        )
        # log info
        msg = f"Precipitation export summary:  {summary['import_counts']}"
        current_run.log_info(msg)
        logging.info(msg)
        log_summary_errors(summary)

        # Check if all data points were correctly imported by DHIS2
        dp_ignored = summary["import_counts"]["ignored"]
        if dp_ignored > 0:
            current_run.log_warning(
                f"{dp_ignored} datapoints not imported. precipitation last push date is not updated: {last_pushed_date}"
            )
        else:
            # Save the last date pushed for Temperature min
            total_dp = summary["import_counts"]["imported"] + summary["import_counts"]["updated"]
            current_run.log_info(f"{total_dp} datapoints correctly processed.")
            # current_run.log_info(f"All {len(datapoints_valid)} datapoints correctly imported.")
            # Save the last date pushed for precipitation
            update_last_available_date_log(pipeline_path / "config", "precipitation", precip_date_max)

        return True

    except Exception as e:
        raise Exception(f"An error occurred while pushing precipitation data: {e}") from e


def tempareture_min_push(pipeline_path: Path, dhis2_client_target: DHIS2, config: dict) -> bool:
    """Put some data processing code here."""

    current_run.log_info("Temperature min data push started...")
    report_path = pipeline_path / "logs" / "temperature_min"
    configure_logging(logs_path=report_path, task_name="push_temp_min")

    # Parameters for the import
    import_strategy = config["CLIMATE_PUSH_SETTINGS"].get("IMPORT_STRATEGY", None)
    if import_strategy is None:
        import_strategy = "CREATE_AND_UPDATE"  # CREATE, UPDATE, CREATE_AND_UPDATE

    dry_run = config["CLIMATE_PUSH_SETTINGS"].get("DRY_RUN", None)
    if dry_run is None:
        dry_run = True  # True or False

    max_post = config["CLIMATE_PUSH_SETTINGS"].get("MAX_POST", None)
    if max_post is None:
        max_post = 500  # number of datapoints without a time-out limit (?)

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

    try:
        # Load Temperature data from DB table
        table_name = config["CLIMATE_PUSH_SETTINGS"].get("TEMP_MIN_TABLE", None)
        if table_name is None:
            current_run.log_error("Temperature min table name is not provided.")
            raise ValueError

        current_run.log_info(f"Loading temperature min data from database table: {table_name}")
        temperature_min_data = load_climate_data(table_name=table_name)

        # Check for new data.
        temp_min_date_max = temperature_min_data.start.max().strftime("%Y-%m-%d")
        current_run.log_info(
            f"Last temperature min pushed date {last_pushed_date} - data available to : {temp_min_date_max}"
        )
        if temp_min_date_max <= last_pushed_date:
            current_run.log_info("No new temperature min data to push.")
            return True  # No new data to push EXIT!

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
            report_path=report_path,
        )

        # Apply mappings (if any..)
        temperature_min_data_formatted = apply_mappings_for_climate_data(
            temperature_min_data_formatted, config["TEMPERATURE_MAPPING_MIN"]
        )
        datapoints_valid, datapoints_not_valid, datapoints_na = select_transform_to_json(
            data_values=temperature_min_data_formatted
        )

        # log not valid datapoints
        log_ignored(report_path=report_path, datapoint_list=datapoints_not_valid, data_type="TEMPERATURE_MIN")
        log_ignored(report_path=report_path, datapoint_list=datapoints_na, data_type="TEMPERATURE_MIN", is_na=True)

        # push data
        current_run.log_info(
            f"Pushing Temperature min data with parameters import_strategy: {import_strategy}, dry_run: {dry_run}, max_post: {max_post}"
        )
        logging.info(
            f"Pushing Temperature min data with parameters import_strategy: {import_strategy}, dry_run: {dry_run}, max_post: {max_post}"
        )
        summary = push_data_elements(
            dhis2_client=dhis2_client_target,
            data_elements_list=datapoints_valid,
            strategy=import_strategy,
            dry_run=dry_run,
            max_post=max_post,
        )

        # log info
        msg = f"Temperature min export summary:  {summary['import_counts']}"
        current_run.log_info(msg)
        logging.info(msg)
        log_summary_errors(summary)

        # Check if all data points were correctly imported by DHIS2
        dp_ignored = summary["import_counts"]["ignored"]
        if dp_ignored > 0:
            current_run.log_warning(
                f"{dp_ignored} datapoints not imported. tempearture_min last push date is not updated: {last_pushed_date}"
            )
        else:
            # Save the last date pushed for Temperature min
            total_dp = summary["import_counts"]["imported"] + summary["import_counts"]["updated"]
            current_run.log_info(f"{total_dp} datapoints correctly processed.")
            # current_run.log_info(f"All {len(datapoints_valid)} datapoints correctly imported.")
            # Save the last date pushed for Temperature min
            update_last_available_date_log(pipeline_path / "config", "temperature_min", temp_min_date_max)

        return True

    except Exception as e:
        raise Exception(f"An error occurred while pushing temperature min data: {e}") from e


# Helper class definition to store/create the correct OU JSON format for creation/update
class OrgUnitObj:
    def __init__(self, orgUnit_row: pd.Series):
        """Create a new org unit instance.

        Parameters
        ----------
        orgUnit_row : pandas series
            Expects columns with names :
                ['id', 'name', 'shortName', 'openingDate', 'closedDate', 'parent','level', 'path', 'geometry']
        """
        self.initialize_from(orgUnit_row.squeeze(axis=0))

    def initialize_from(self, row: pd.Series):
        # let's keep names consistent
        self.id = row.get("id")
        self.name = row.get("name")
        self.shortName = row.get("shortName")
        self.openingDate = row.get("openingDate")
        self.closedDate = row.get("closedDate")
        self.parent = row.get("parent")
        geometry = row.get("geometry")
        self.geometry = json.loads(geometry) if isinstance(geometry, str) else geometry

    def to_json(self) -> dict:
        json_dict = {
            "id": self.id,
            "name": self.name,
            "shortName": self.shortName,
            "openingDate": self.openingDate,
            "closedDate": self.closedDate,
            "parent": {"id": self.parent.get("id")} if self.parent else None,
        }
        if self.geometry:
            geometry = json.loads(self.geometry) if isinstance(self.geometry, str) else self.geometry
            json_dict["geometry"] = {
                "type": geometry["type"],
                "coordinates": geometry["coordinates"],
            }
        return {k: v for k, v in json_dict.items() if v is not None}

    def is_valid(self):
        if self.id is None:
            return False
        if self.name is None:
            return False
        if self.shortName is None:
            return False
        if self.openingDate is None:
            return False
        if self.parent is None:
            return False

        return True

    def __str__(self):
        return f"OrgUnitObj({self.id}, {self.name})"


def push_orgunits_create(ou_df: pd.DataFrame, dhis2_client_target: DHIS2, report_path: Path):
    errors_count = 0
    for _, row in ou_df.iterrows():
        ou = OrgUnitObj(row)
        if ou.is_valid():
            response = push_orgunit(
                dhis2_client=dhis2_client_target,
                orgunit=ou,
                strategy="CREATE",
                dry_run=False,  # dry_run=False -> Apply changes in the DHIS2
            )
            if response["status"] == "ERROR":
                errors_count = errors_count + 1
                logging.info(str(response))
            else:
                current_run.log_info(f"New organisation unit created: {ou}")
        else:
            logging.info(
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
            f"{errors_count} errors occurred during creation. Please check the latest execution report under {report_path}."
        )
    else:
        current_run.log_info("No new organisation units found.")


def push_orgunits_update(
    orgUnit_source: pd.DataFrame,
    orgUnit_target: pd.DataFrame,
    matching_ou_ids: list,
    dhis2_client_target: DHIS2,
    report_path: str,
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
    # orgUnit_source_f = orgUnit_source[comparison_cols]
    # orgUnit_target_f = orgUnit_target[comparison_cols]
    orgUnit_source_f = orgUnit_source.loc[:, comparison_cols].copy()
    orgUnit_target_f = orgUnit_target.loc[:, comparison_cols].copy()

    errors_count = 0
    updates_count = 0
    progress_count = 0
    for id, indices in index_dictionary.items():
        progress_count = progress_count + 1
        source = orgUnit_source_f.iloc[indices["source"]].copy()
        target = orgUnit_target_f.iloc[indices["target"]].copy()
        # get cols with differences
        diff_fields = source[~((source == target) | (source.isna() & target.isna()))]

        # If there are differences then update!
        if not diff_fields.empty:
            # add the ID for update
            source["id"] = id
            ou_update = OrgUnitObj(source)
            response = push_orgunit(
                dhis2_client=dhis2_client_target,
                orgunit=ou_update,
                strategy="UPDATE",
                dry_run=False,  # dry_run=False -> Apply changes in the DHIS2
            )
            if response["status"] == "ERROR":
                errors_count = errors_count + 1
            else:
                updates_count = updates_count + 1
            logging.info(str(response))

        if progress_count % 5000 == 0:
            current_run.log_info(f"Organisation units checked: {progress_count}/{len(matching_ou_ids)}")

    current_run.log_info(f"Organisation units updated: {updates_count}")
    if errors_count > 0:
        current_run.log_info(
            f"{errors_count} errors occurred during OU update. Please check the latest execution report under {report_path}."
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


def build_id_indexes(ou_source, ou_target, ou_matching_ids):
    # Set "id" as the index for faster lookup
    df1_lookup = {val: idx for idx, val in enumerate(ou_source["id"])}
    df2_lookup = {val: idx for idx, val in enumerate(ou_target["id"])}

    # Build the dictionary using prebuilt lookups
    index_dict = {
        match_id: {"source": df1_lookup[match_id], "target": df2_lookup[match_id]}
        for match_id in ou_matching_ids
        if match_id in df1_lookup and match_id in df2_lookup
    }
    return index_dict


def tempareture_max_push(pipeline_path: Path, dhis2_client_target: DHIS2, config: dict) -> bool:
    """Put some data processing code here."""

    current_run.log_info("Temperature max data push started...")
    report_path = pipeline_path / "logs" / "temperature_max"
    configure_logging(logs_path=report_path, task_name="push_temp_max")

    # Parameters for the import
    import_strategy = config["CLIMATE_PUSH_SETTINGS"].get("IMPORT_STRATEGY", None)
    if import_strategy is None:
        import_strategy = "CREATE_AND_UPDATE"  # CREATE, UPDATE, CREATE_AND_UPDATE

    dry_run = config["CLIMATE_PUSH_SETTINGS"].get("DRY_RUN", None)
    if dry_run is None:
        dry_run = True  # True or False

    max_post = config["CLIMATE_PUSH_SETTINGS"].get("MAX_POST", None)
    if max_post is None:
        max_post = 500  # number of datapoints without a time-out limit (?)

    # Get last date pushed for Temperature max
    try:
        last_pushed_date = get_last_pushed_date(pipeline_path / "config", "temperature_max")
        if last_pushed_date is None:
            current_run.log_warning("The last temperature_max date was not set. Falling back to default : 2017-01-01")
            last_pushed_date = "2017-01-01"
    except Exception as e:
        current_run.log_warning(
            f"The last temperature_max date was not found: {e}. Falling back to default : 2017-01-01"
        )
        last_pushed_date = "2017-01-01"  # Default

    try:
        # Load temperature max data from DB table
        table_name = config["CLIMATE_PUSH_SETTINGS"].get("TEMP_MAX_TABLE", None)
        if table_name is None:
            current_run.log_error("Temperature max table name is not provided.")
            raise ValueError

        current_run.log_info(f"Loading temperature max data from database table: {table_name}")
        temperature_max_data = load_climate_data(table_name=table_name)

        # Check for new data.
        temp_max_date_max = temperature_max_data.start.max().strftime("%Y-%m-%d")
        current_run.log_info(
            f"Last temperature max pushed date {last_pushed_date} - data available to : {temp_max_date_max}"
        )
        if temp_max_date_max <= last_pushed_date:
            current_run.log_info("No new temperature max data to push.")
            return True  # No new data to push EXIT!

        # Select new data using temp_max_date_max
        current_run.log_info(f"Pushing new temperature max data from : {last_pushed_date}")
        temperature_max_data = temperature_max_data[temperature_max_data.start >= last_pushed_date]

        # get uids list
        uids = config["TEMPERATURE_MAPPING_MAX"].get("UIDS", [])
        if len(uids) == 0:
            current_run.log_error("Temperature max UIDs are not provided.")
            raise ValueError

        # map uids to DHIS2 format
        dx_uid_min = uids.get("TEMP_MIN", None)
        dx_uid_max = uids.get("TEMP_MAX", None)
        dx_uid_mean = uids.get("TEMP_MEAN", None)
        if dx_uid_min is None or dx_uid_max is None or dx_uid_mean is None:
            current_run.log_error("Temperature max, min, max and mean UIDs are not provided.")
            raise ValueError

        # format the climate data to DHIS2 format
        temperature_max_data_formatted = to_dhis2_format_temperature(
            climate_data=temperature_max_data,
            dx_uid_min=dx_uid_min,
            dx_uid_max=dx_uid_max,
            dx_uid_mean=dx_uid_mean,
            data_type="TEMPERATURE_MAX",
            report_path=report_path,
        )

        # Apply mappings (if any..)
        temperature_max_data_formatted = apply_mappings_for_climate_data(
            temperature_max_data_formatted, config["TEMPERATURE_MAPPING_MAX"]
        )
        datapoints_valid, datapoints_not_valid, datapoints_na = select_transform_to_json(
            data_values=temperature_max_data_formatted
        )

        # log not valid datapoints
        log_ignored(report_path=report_path, datapoint_list=datapoints_not_valid, data_type="TEMPERATURE_MAX")
        log_ignored(report_path=report_path, datapoint_list=datapoints_na, data_type="TEMPERATURE_MAX", is_na=True)

        # push data
        current_run.log_info(
            f"Pushing Temperature max data with parameters import_strategy: {import_strategy}, dry_run: {dry_run}, max_post: {max_post}"
        )
        logging.info(
            f"Pushing Temperature max data with parameters import_strategy: {import_strategy}, dry_run: {dry_run}, max_post: {max_post}"
        )
        summary = push_data_elements(
            dhis2_client=dhis2_client_target,
            data_elements_list=datapoints_valid,
            strategy=import_strategy,
            dry_run=dry_run,
            max_post=max_post,
        )

        # log info
        msg = f"Temperature max export summary:  {summary['import_counts']}"
        current_run.log_info(msg)
        logging.info(msg)
        log_summary_errors(summary)

        # Check if all data points were correctly imported by DHIS2
        dp_ignored = summary["import_counts"]["ignored"]
        if dp_ignored > 0:
            current_run.log_warning(
                f"{dp_ignored} datapoints not imported. tempearture_max last push date is not updated: {last_pushed_date}"
            )
        else:
            # Save the last date pushed for Temperature max
            total_dp = summary["import_counts"]["imported"] + summary["import_counts"]["updated"]
            current_run.log_info(f"{total_dp} datapoints correctly processed.")
            # current_run.log_info(f"All {len(datapoints_valid)} datapoints correctly imported.")
            update_last_available_date_log(os.path.join(pipeline_path, "config"), "temperature_max", temp_max_date_max)

        return True

    except Exception as e:
        raise Exception(f"An error occurred while pushing temperature max data: {e}") from e


def relative_humidity_push(pipeline_path: Path, dhis2_client_target: DHIS2, config: dict) -> bool:
    """Put some data processing code here."""

    current_run.log_info("Relative humidity data push started...")
    report_path = pipeline_path / "logs" / "relative_humidity"
    configure_logging(logs_path=report_path, task_name="push_humidity")

    # Parameters for the import
    import_strategy = config["CLIMATE_PUSH_SETTINGS"].get("IMPORT_STRATEGY", None)
    if import_strategy is None:
        import_strategy = "CREATE_AND_UPDATE"  # CREATE, UPDATE, CREATE_AND_UPDATE

    dry_run = config["CLIMATE_PUSH_SETTINGS"].get("DRY_RUN", None)
    if dry_run is None:
        dry_run = True  # True or False

    max_post = config["CLIMATE_PUSH_SETTINGS"].get("MAX_POST", None)
    if max_post is None:
        max_post = 500  # number of datapoints without a time-out limit (?)

    # Get last date pushed for Temperature max
    try:
        last_pushed_date = get_last_pushed_date(json_folder=pipeline_path / "config", node="relative_humidity")
        if last_pushed_date is None:
            current_run.log_warning("The last relative_humidity date was not set. Falling back to default : 2017-01-01")
            last_pushed_date = "2017-01-01"
    except Exception as e:
        current_run.log_warning(
            f"The last relative_humidity date was not found: {e}. Falling back to default : 2017-01-01"
        )
        last_pushed_date = "2017-01-01"  # Default

    try:
        # Load temperature max data from DB table
        table_name = config["CLIMATE_PUSH_SETTINGS"].get("RELATIVE_HUMIDITY_TABLE")
        if table_name is None:
            current_run.log_error("Relative humidity table name is not provided.")
            raise ValueError

        current_run.log_info(f"Loading relative humidity data from database table: {table_name}")
        relative_humidity_data = load_climate_data(table_name=table_name)

        # Check for new data.
        relative_humidity_date_max = relative_humidity_data.period.max()
        current_run.log_info(
            f"Last relative humidity pushed date {last_pushed_date} - data available to : {relative_humidity_date_max}"
        )
        if relative_humidity_date_max <= last_pushed_date:
            current_run.log_info("No new relative humidity data to push.")
            return True  # No new data to push EXIT!

        # Select new data using temp_max_date_max
        current_run.log_info(f"Pushing new relative humidity date from : {last_pushed_date}")
        relative_humidity_data = relative_humidity_data[relative_humidity_data.period >= last_pushed_date]

        # get uids list
        uids = config["RELATIVE_HUMIDITY_MAPPING"].get("UIDS", [])
        if len(uids) == 0:
            current_run.log_error("Relative humidity UIDs are not provided.")
            raise ValueError

        # map uids to DHIS2 format
        dx_uid_min = uids.get("HUMIDITY_MIN", None)
        dx_uid_max = uids.get("HUMIDITY_MAX", None)
        dx_uid_mean = uids.get("HUMIDITY_MEAN", None)
        if dx_uid_min is None or dx_uid_max is None or dx_uid_mean is None:
            current_run.log_error("Relative humidity, min, max and mean UIDs are not provided.")
            raise ValueError

        # format the climate data to DHIS2 format
        relative_humidity_data_formatted = to_dhis2_format_humidity(
            climate_data=relative_humidity_data,
            dx_uid_min=dx_uid_min,
            dx_uid_max=dx_uid_max,
            dx_uid_mean=dx_uid_mean,
            data_type="RELATIVE_HUMIDITY",
            report_path=report_path,
        )

        # Apply mappings (if any..)
        relative_humidity_data_formatted = apply_mappings_for_climate_data(
            relative_humidity_data_formatted, config["RELATIVE_HUMIDITY_MAPPING"]
        )
        datapoints_valid, datapoints_not_valid, datapoints_na = select_transform_to_json(
            data_values=relative_humidity_data_formatted
        )

        # log not valid datapoints
        log_ignored(report_path=report_path, datapoint_list=datapoints_not_valid, data_type="RELATIVE_HUMIDITY")
        log_ignored(report_path=report_path, datapoint_list=datapoints_na, data_type="RELATIVE_HUMIDITY", is_na=True)

        # push data
        current_run.log_info(
            f"Pushing relative humidity data with parameters import_strategy: {import_strategy}, dry_run: {dry_run}, max_post: {max_post}"
        )
        logging.info(
            f"Pushing relative humidity data with parameters import_strategy: {import_strategy}, dry_run: {dry_run}, max_post: {max_post}"
        )
        summary = push_data_elements(
            dhis2_client=dhis2_client_target,
            data_elements_list=datapoints_valid,
            strategy=import_strategy,
            dry_run=dry_run,
            max_post=max_post,
        )

        # log info
        msg = f"Relative humidity export summary: {summary['import_counts']}"
        current_run.log_info(msg)
        logging.info(msg)
        log_summary_errors(summary)

        # Check if all data points were correctly imported by DHIS2
        dp_ignored = summary["import_counts"]["ignored"]
        if dp_ignored > 0:
            current_run.log_warning(
                f"{dp_ignored} datapoints not imported. relative_humity last push date is not updated: {last_pushed_date}"
            )
        else:
            # Save the last date pushed for Temperature max
            total_dp = summary["import_counts"]["imported"] + summary["import_counts"]["updated"]
            current_run.log_info(f"{total_dp} datapoints correctly processed.")
            # current_run.log_info(f"All {len(datapoints_valid)} datapoints correctly imported.")
            update_last_available_date_log(pipeline_path / "config", "relative_humidity", relative_humidity_date_max)

    except Exception as e:
        raise Exception(f"An error occurred while pushing relative humidity max data: {e}") from e


def load_climate_data(table_name: str) -> pd.DataFrame:
    """Load climate data from database."""
    current_run.log_info(f"Loading data from {table_name}")
    dbengine = create_engine(os.environ["WORKSPACE_DATABASE_URL"])
    data = pd.read_sql_table(table_name, con=dbengine)
    return data


def to_dhis2_format_precipitation(
    climate_data: pd.DataFrame,
    dx_uid: str,
    data_type: str = "PRECIPITATION",
    coc_default: str = "HllvX50cXC0",
    aoc_default: str = "HllvX50cXC0",
    domain_type: str = "AGGREGATED",
    report_path: str = "",
) -> pd.DataFrame:
    """
    Maps Climate data to a standardized DHIS2 data table.

    Parameters
    ----------
    climate_data : pd.DataFrame
        Input DataFrame containing climate data from ERA5.
    data_type : str
        The type of data being mapped. Default is "PRECIPITATION".
    domain_type : str, optional
        The domain of the data if its per period (Agg ex: monthly) or datapoint (Tracker ex: per day):
        - "AGGREGATED": For aggregated data (default).
        - "TRACKER": For tracker data.

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

    try:
        dhis2_format = pd.DataFrame(index=climate_data.index)
        dhis2_format["data_type"] = data_type
        dhis2_format["dx_uid"] = dx_uid
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
                f"{len(rows_to_change)} data points will have their 'value' replaced to 0.0001. Please check the report for details {report_path}"
            )
            for _, row in rows_to_change.iterrows():
                # current_run.log_info(
                # f'UID: {row["dx_uid"]} period: {row["period"]} ou: {row["org_unit"]} value: {row["value"]}')
                logging.info(
                    f"UID: {row['dx_uid']} period: {row['period']} ou: {row['org_unit']} value: {row['value']}"
                )

        # Set the absolute values under 0.0001 to 0.0001 (rounding up)
        dhis2_format["value"] = dhis2_format["value"].apply(
            lambda x: 0.0001 if pd.notna(x) and abs(x) < 0.0001 and x != 0 else x
        )

        return dhis2_format

    except AttributeError as e:
        raise AttributeError(f"Climate data is missing a required attribute: {e}")
    except Exception as e:
        raise Exception(f"Unexpected Error while creating routine format table: {e}")


def to_dhis2_format_temperature(
    climate_data: pd.DataFrame,
    dx_uid_min: str,
    dx_uid_max: str,
    dx_uid_mean: str,
    data_type: str = "TEMPERATURE_MIN",  # TEMPERATURE_MAXN
    coc_default: str = "HllvX50cXC0",
    aoc_default: str = "HllvX50cXC0",
    domain_type: str = "AGGREGATED",
    report_path: str = "",
) -> pd.DataFrame:
    """
    Maps Climate data to a standardized DHIS2 data table.

    Parameters
    ----------
    climate_data : pd.DataFrame
        Input DataFrame containing climate data from ERA5.
    data_type : str
        The type of data being mapped. Supported values are: TEMPERATURE_MAX, TEMPERATURE_MIN.
        Default is "TEMPERATURE_MIN".
    domain_type : str, optional
        The domain of the data if its per period (Agg ex: monthly) or datapoint (Tracker ex: per day):
        - "AGGREGATED": For aggregated data (default).
        - "TRACKER": For tracker data.

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

    accepted_types = ["TEMPERATURE_MAX", "TEMPERATURE_MIN"]
    if data_type not in accepted_types:
        raise ValueError(f"Incorrect 'data_type' configuration {accepted_types}")

    uids = {"min": dx_uid_min, "max": dx_uid_max, "mean": dx_uid_mean}
    if all([uid is None and pd.isna(uid) for uid in uids.values()]):
        raise ValueError(f"Incorrect 'dx_uid_*' provided for {data_type}: {uids}")

    if data_type == "TEMPERATURE_MIN":
        col_var = "tmin"
    else:
        col_var = "tmax"

    try:
        temp_table = []
        for key, value in uids.items():
            dhis2_format_sub = pd.DataFrame(index=climate_data.index)
            dhis2_format_sub["data_type"] = f"{data_type}_{key.upper()}"
            dhis2_format_sub["dx_uid"] = value
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
                f"{len(rows_to_change)} data points in {data_type} will have their 'value' replaced to 0.0001. Please check the report for details {report_path}"
            )
            for _, row in rows_to_change.iterrows():
                # current_run.log_info(
                # f'UID: {row["dx_uid"]} period: {row["period"]} ou: {row["org_unit"]} value: {row["value"]}')
                logging.info(
                    f"UID: {row['dx_uid']} period: {row['period']} ou: {row['org_unit']} value: {row['value']}"
                )

        # Set the absolute values under 0.0001 to 0.0001
        dhis2_format["value"] = dhis2_format["value"].apply(
            lambda x: 0.0001 if pd.notna(x) and abs(x) < 0.0001 and x != 0 else x
        )

        # sorting might improve speed
        dhis2_format = dhis2_format.sort_values(by=["org_unit", "period"], ascending=True)
        return dhis2_format

    except AttributeError as e:
        raise AttributeError(f"Climate data is missing a required attribute: {e}")
    except Exception as e:
        raise Exception(f"Unexpected Error while creating routine format table: {e}")


def to_dhis2_format_humidity(
    climate_data: pd.DataFrame,
    dx_uid_min: str,
    dx_uid_max: str,
    dx_uid_mean: str,
    data_type: str = "RELATIVE_HUMIDITY",
    coc_default: str = "HllvX50cXC0",
    aoc_default: str = "HllvX50cXC0",
    domain_type: str = "AGGREGATED",
    report_path: str = "",
) -> pd.DataFrame:
    """
    Maps Climate data to a standardized DHIS2 data table.

    Parameters
    ----------
    climate_data : pd.DataFrame
        Input DataFrame containing climate data from ERA5.
    data_type : str
        The type of data being mapped. Supported value : RELATIVE_HUMIDITY
    domain_type : str, optional
        The domain of the data if its per period (Agg ex: monthly) or datapoint (Tracker ex: per day):
        - "AGGREGATED": For aggregated data (default).
        - "TRACKER": For tracker data.

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

    uids = {"min": dx_uid_min, "max": dx_uid_max, "mean": dx_uid_mean}
    if all([uid is None and pd.isna(uid) for uid in uids.values()]):
        raise ValueError(f"Incorrect 'dx_uid_*' provided for {data_type}: {uids}")

    try:
        humidity_table = []
        for key, value in uids.items():
            dhis2_format_sub = pd.DataFrame(index=climate_data.index)
            dhis2_format_sub["data_type"] = data_type
            dhis2_format_sub["dx_uid"] = value
            dhis2_format_sub["period"] = climate_data["period"]
            dhis2_format_sub["org_unit"] = climate_data["ref"]
            dhis2_format_sub["category_option_combo"] = coc_default
            dhis2_format_sub["attribute_option_combo"] = aoc_default
            dhis2_format_sub["rate_type"] = None
            dhis2_format_sub["domain_type"] = domain_type
            dhis2_format_sub["value"] = climate_data[key]
            humidity_table.append(dhis2_format_sub)

        dhis2_format = pd.concat(humidity_table, ignore_index=True)
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
                f"{len(rows_to_change)} data points in {data_type} will have their 'value' replaced to 0.0001. Please check the report for details {report_path}"
            )
            for _, row in rows_to_change.iterrows():
                # current_run.log_info(
                # f'UID: {row["dx_uid"]} period: {row["period"]} ou: {row["org_unit"]} value: {row["value"]}')
                logging.info(
                    f"UID: {row['dx_uid']} period: {row['period']} ou: {row['org_unit']} value: {row['value']}"
                )

            # Set the absolute values under 0.0001 to 0.0001
            dhis2_format["value"] = dhis2_format["value"].apply(
                lambda x: 0.0001 if pd.notna(x) and abs(x) < 0.0001 and x != 0 else x
            )

        # sorting might improve speed
        dhis2_format = dhis2_format.sort_values(by=["org_unit", "period"], ascending=True)
        return dhis2_format

    except AttributeError as e:
        raise AttributeError(f"Climate data is missing a required attribute: {e}")
    except Exception as e:
        raise Exception(f"Unexpected Error while creating routine format table: {e}")


def apply_mappings_for_climate_data(datapoints_df: pd.DataFrame, mappings: dict) -> pd.DataFrame:
    """
    All matching ids will be replaced.
    Is user responsability to provide the correct IDS.
    ORG_UNITS, COC and AOC
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


def select_transform_to_json(data_values: pd.DataFrame):
    if data_values is None:
        return [], []
    valid = []
    not_valid = []
    to_delete = []
    for _, row in data_values.iterrows():
        dpoint = ClimateDataPoint(row)
        if dpoint.is_valid():
            valid.append(dpoint.to_json())
        elif dpoint.is_to_delete():
            to_delete.append(dpoint.is_to_delete())
        else:
            not_valid.append(row)  # row is the original data, not the json
    return valid, not_valid, to_delete


def push_data_elements(
    dhis2_client: DHIS2,
    data_elements_list: list,
    strategy: str = "CREATE_AND_UPDATE",
    dry_run: bool = True,
    max_post: int = 1000,
) -> dict:
    """
    dry_run: This parameter can be set to true to get an import summary without actually importing data (DHIS2).
    """

    summary = {
        "import_counts": {"imported": 0, "updated": 0, "ignored": 0, "deleted": 0},  # , "period": ""},
        "import_options": {},
        "ERRORS": [],
    }

    total_datapoints = len(data_elements_list)
    count = 0

    for chunk in split_list(data_elements_list, max_post):
        count = count + 1
        try:
            # chunk_period = min(list(set(c.get("period") for c in chunk)))
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

            # this might not be necessary, but just in case r.raise_for_status() is not enough.
            if status != "OK" and response:
                summary["ERRORS"].append(response)

            if response:
                for key in ["imported", "updated", "ignored", "deleted"]:
                    summary["import_counts"][key] += response.get("importCount", {}).get(key, 0)

        except requests.exceptions.RequestException as e:
            try:
                # response = r.json().get("response")
                response = r.json().get("response") if r else None
            except (ValueError, AttributeError) as un_err:
                response = None
                raise Exception(f"Unexpected error. Pipeline halted: {un_err} ")

            if response:
                for key in ["imported", "updated", "ignored", "deleted"]:
                    summary["import_counts"][key] += response["importCount"][key]

            error_response = get_response_value_errors(response, chunk=chunk)
            summary["ERRORS"].append({"error": e, "response": error_response})

            # Stop the pipeline if the we have a server error.
            if 500 <= response["httpStatusCode"] < 600:
                raise Exception(
                    f"Server error pipeline halted: {e} - {error_response} summary: {summary['import_counts']}"
                )

        if (count * max_post) % 20000 == 0:
            current_run.log_info(
                f"{count * max_post} / {total_datapoints} data points pushed summary: {summary['import_counts']}"
            )

    return summary


def split_list(src_list: list, length: int) -> Generator[list, None, None]:
    """Split list into chunks."""
    for i in range(0, len(src_list), length):
        yield src_list[i : i + length]


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


def log_summary_errors(summary: dict):
    """
    Logs all the errors in the summary dictionary using the configured logging.

    Args:
        summary (dict): The dictionary containing import counts and errors.
    """
    errors = summary.get("ERRORS", [])
    if not errors:
        logging.info("No errors found in the summary.")
    else:
        logging.error(f"Logging {len(errors)} error(s) from export summary.")
        for i_e, error in enumerate(errors, start=1):
            logging.error(f"Error {i_e} : HTTP request failed : {error.get('error', None)}")
            error_response = error.get("response", None)
            if error_response:
                rejected_list = error_response.pop("rejected_datapoints", [])
                logging.error(f"Error response : {error_response}")
                for i_r, rejected in enumerate(rejected_list, start=1):
                    logging.error(f"Rejected data point {i_r}: {rejected}")


# helper class to handle the data points format to push
class ClimateDataPoint:
    def __init__(self, series_row: pd.Series):
        """Create a new data element

        Parameters
        ----------
        series_row : pandas series
            Expects columns with names :
                ['data_type', 'dx_uid', 'period', 'org_unit', 'category_option_combo',
                 'attribute_option_combo', 'rate_type', 'domain_type', 'value']
        """
        row = series_row.squeeze(axis=0)
        self.dataType = row.get("data_type")
        self.dataElement = row.get("dx_uid")
        self.period = row.get("period")
        self.orgUnit = row.get("org_unit")
        self.categoryOptionCombo = row.get("category_option_combo")
        self.attributeOptionCombo = row.get("attribute_option_combo")
        self.value = row.get("value")

    def to_json(self) -> dict:
        json_dict = {
            "dataElement": self.dataElement,
            "period": self.period,
            "orgUnit": self.orgUnit,
            "categoryOptionCombo": self.categoryOptionCombo,
            "attributeOptionCombo": self.attributeOptionCombo,
            "value": self.value,
        }
        # return {k: v for k, v in json_dict.items() if v is not None}
        return json_dict

    def _check_attributes(self, exclude_value=False):
        # List of attributes to check, optionally excluding the 'value' attribute
        attributes = [self.dataElement, self.period, self.orgUnit, self.categoryOptionCombo, self.attributeOptionCombo]
        if not exclude_value:
            attributes.append(self.value)

        # Return True if all attributes are not None
        return all(attr is not None for attr in attributes)

    def is_valid(self):
        # Check if all attributes are valid (None check)
        return self._check_attributes(exclude_value=False)

    def is_to_delete(self):
        # Check if all attributes except 'value' are not None and 'value' is None
        return self._check_attributes(exclude_value=True) and self.value is None

    def __str__(self):
        return f"DataPoint({self.dataType} id:{self.dataElement} pe:{self.period} ou:{self.orgUnit} value:{self.value})"


def get_last_pushed_date(json_folder: Path, node: str, json_name: str = "last_pushed_date.json") -> str:
    """
    Reads the value of a specific node from a JSON file.

    Parameters:
    - file_path (str): Path to the JSON file.
    - node (str): The name of the data type (precipitation, temperature_max, temperature_min).

    Returns:
    - str: The date value of the node in the file.

    Raises:
    - FileNotFoundError: If the JSON file does not exist.
    - KeyError: If the node is not found in the file.
    """
    # Check if the file exists
    file_path = json_folder / json_name
    if not file_path.exists():
        raise FileNotFoundError(f"The file {file_path} does not exist.")

    # Load the JSON file
    with open(file_path, "r") as f:
        data = json.load(f)

    # Check if the node exists
    if node not in data:
        raise KeyError(f"The node '{node}' does not exist in the JSON file.")

    value = data.get(node)
    if not value:
        value = None

    return value


def update_last_available_date_log(
    json_folder: str, node: str, iso_date: str, json_name: str = "last_pushed_date.json"
) -> None:
    """
    Updates the last available date for a given data source in a JSON file.

    Parameters:
    - file_path (str): Path to the JSON file.
    - node (str): The name of the data source ("precipitation", "temperature_max", "temperature_min").
    - iso_date (str): The new ISO 8601 date to set for the node.

    Returns:
    None
    """
    # Validate the ISO date format
    try:
        datetime.fromisoformat(iso_date)
    except ValueError:
        raise ValueError("Invalid ISO date format. Provide a valid ISO 8601 string.")

    # Load existing data or create a new dictionary
    file_path = os.path.join(json_folder, json_name)
    if os.path.exists(file_path):
        with open(file_path, "r") as f:
            data = json.load(f)
    else:
        data = {}

    # Update the node with the new date
    data[node] = iso_date

    # Write the updated data back to the file
    with open(file_path, "w") as f:
        json.dump(data, f, indent=4)

    current_run.log_info(f"Updated {node} with date {iso_date} in {file_path}.")


# log ignored datapoints in the report
def log_ignored(report_path, datapoint_list, data_type="precipitation", is_na=False):
    if len(datapoint_list) > 0:
        current_run.log_info(
            f"{len(datapoint_list)} datapoints will be ignored. Please check the report for details {report_path}"
        )
        logging.warning(f"{len(datapoint_list)} {data_type} datapoints to be ignored: ")
        for i, error in enumerate(datapoint_list, start=1):
            logging.warning(f"{i} DataElement {'NA' if is_na else ''} ignored: {error}")


def configure_logging(logs_path: Path, task_name: str):
    """Configure logging for the pipeline.

    Parameters
    ----------
    logs_path : Path
        Directory path where log files will be stored.
    task_name : str
        Name of the task to include in the log filename.

    This function creates the log directory if it does not exist and sets up logging to a file.
    """
    # Configure logging
    logs_path.mkdir(parents=True, exist_ok=True)
    now = datetime.now().strftime("%Y-%m-%d-%H_%M")
    logging.basicConfig(
        filename=logs_path / f"{task_name}_{now}.log",
        level=logging.INFO,
        format="%(asctime)s - %(message)s",
    )


if __name__ == "__main__":
    dhis2_climate_push()
