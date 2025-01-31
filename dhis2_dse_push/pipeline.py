from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import logging
import os
import json
from typing import Generator

# from typing import Generator
import pandas as pd
import requests
from sqlalchemy import create_engine
from openhexa.sdk import current_run, pipeline, workspace
from openhexa.toolbox.dhis2 import DHIS2

import numpy as np


@pipeline("dhis2-dse-push", name="dhis2_dse_push")
def dhis2_dse_push():
    current_run.log_info("Starting DSE data pipeline...")
    root_path = os.path.join(workspace.files_path, "pipelines", "dhis2_dse_push")

    try:
        # Load the pipeline configuration
        pipeline_config = load_dse_config(pipeline_path=root_path)

        # connect to DHIS2
        dhis2_client = connect_to_dhis2(config=pipeline_config, cache_dir=None)

        # Download current pyramid from SNIS and use it for alignment
        # pyramid_extract_ready = extract_organisation_for_zs() # Copy the code from precipitation era5

        # Align pyramid
        pyramid_ready = organisation_units_alignment(
            root=root_path,
            dhis2_client_target=dhis2_client,
            config=pipeline_config,
            run_task=True,
            # success=pyramid_extract_ready,
        )

        # Run dse data push task PNLP TOTAL
        dse_db_success = pnlp_dse_database_push(
            pipeline_path=root_path,
            dhis2_client_target=dhis2_client,
            config=pipeline_config,
            success=pyramid_ready,
        )

        # Run completude push task
        pnlp_completude_push(
            pipeline_path=root_path, dhis2_client_target=dhis2_client, config=pipeline_config, success=dse_db_success
        )

    except Exception as e:
        current_run.log_error(f"An error occurred: {e}")


@dhis2_dse_push.task
def load_dse_config(pipeline_path: str) -> dict:
    """Load the pipeline configuration."""

    config_path = os.path.join(pipeline_path, "config", "pnlp_dse_push_config.json")
    current_run.log_info(f"Loading pipeline configuration from {config_path}")

    try:
        with open(config_path, "r") as f:
            config = json.load(f)
        return config
    except FileNotFoundError:
        current_run.log_error("Configuration file not found.")
        raise
    except json.JSONDecodeError:
        current_run.log_error("Configuration file is not a valid JSON file.")
        raise
    except Exception as e:
        current_run.log_error(f"An error occurred while loading the configuration: {e}")
        raise


@dhis2_dse_push.task
def connect_to_dhis2(config: dict, cache_dir: str):
    try:
        conn_id = config["DSE_PUSH_SETTINGS"].get("DHIS2_CONNECTION_TARGET", None)
        if conn_id is None:
            current_run.log_error("DHIS2 connection is not provided.")
            raise ValueError

        connection = workspace.dhis2_connection(conn_id)
        dhis2_client = DHIS2(connection=connection, cache_dir=cache_dir)
        current_run.log_info(f"Connected to DHIS2 connection: {conn_id}")

        return dhis2_client

    except Exception as e:
        raise Exception(f"Error while connecting to DHIS2: {e}")


@dhis2_dse_push.task
def organisation_units_alignment(root: str, dhis2_client_target: DHIS2, config: dict, run_task: bool):
    """
    This task handles creation and updates of organisation units in the target DHIS2 (incremental approach only).

    The format of the pyramid contains the expected columns.
    A datapoint that doesn't contain the mandatory columns will be skipped (not valid).
    """

    if not run_task:
        return True

    current_run.log_info("Starting organisation units alignment.")

    # Check DSE vs SNIS pyramid first!
    try:
        # Load pyramids from SNIS
        connection_string = config["DSE_PUSH_SETTINGS"]["DHIS2_CONNECTION_SOURCE"]
        source_pyramid_provinces = get_pyramid_for_level(connection_str=connection_string, pyramid_lvl=2)  # provinces
        source_pyramid_zs = get_pyramid_for_level(connection_str=connection_string, pyramid_lvl=3)  # zs

        # Load DSE Database and completude to check the pyramids agains the SNIS pyramid (they should match).
        # We run the DSE process considering the SNIS pyramid in first place, so it would be rare that we might
        # have differences. But if that happens, we can handle those differences using mappings.
        dse_database_data = load_db_table_data(table_name=config["DSE_PUSH_SETTINGS"]["PNLP_DSE_PALU_TABLE"])
        dse_completude_data = load_db_table_data(table_name=config["DSE_PUSH_SETTINGS"]["PNLP_DSE_COMPLETUDE_TABLE"])
        dse_database_data_map = apply_source_mappings_for_dse_data(
            dse_database_data, config["SOURCE_MAPPINGS"]["PNLP_DSE_PALU_MAPPING"], dataset="dse_database"
        )
        dse_completude_data_map = apply_source_mappings_for_dse_data(
            dse_completude_data, config["SOURCE_MAPPINGS"]["PNLP_DSE_COMPLETUDE_MAPPING"], dataset="dse_completude"
        )

        # CHECK THE DSE Zones de sante
        source_zs_id = set(source_pyramid_zs.id)
        dse_palu_zs_id = set(dse_database_data_map.zone_id_DHIS2)
        # Differences: ZS IDS in DSE that do not match with Source (SNIS)
        zs_diffs = dse_palu_zs_id.difference(source_zs_id)
        if len(zs_diffs) > 0:
            for d in zs_diffs:
                matching_rows = dse_database_data_map[dse_database_data_map.zone_id_DHIS2 == d]
                current_run.log_error(
                    f"The Zone de Santé {matching_rows.iloc[0].Nom} ({matching_rows.iloc[0].zone_id_DHIS2}) can not be found in the source pyramid {connection_string}."
                )
            current_run.log_info(
                "Add organisation unit UID mappings to the configuration file SOURCE_MAPPINGS > PNLP_DSE_PALU_MAPPING > ZONE_DE_SANTE_UID list"
            )
            current_run.log_info(f"Configuration path: {os.path.join(root, 'config', 'pnlp_dse_push_config.json')}")
            raise ValueError
        current_run.log_info(f"DSE organisation unit (Zones de Santé) match against {connection_string} pyramid.")

        # provinces (COMPLETUDE requires check by name)
        source_province_names = set(source_pyramid_provinces.name)
        dse_comp_province_ref_names = set(dse_completude_data_map.province_ref)
        # differences: dse provinces that are not in Source (SNIS)
        province_diffs = dse_comp_province_ref_names.difference(source_province_names)
        if len(province_diffs) > 0:
            for d in province_diffs:
                matching_rows = dse_completude_data[dse_completude_data.province_ref == d]
                current_run.log_error(
                    f"The Province name: '{matching_rows.iloc[0].province_ref}' can not be found in the source pyramid."
                )
            current_run.log_info(
                "Add province name mappings to the configuration SOURCE_MAPPINGS > PNLP_DSE_COMPLETUDE_MAPPING > PROVINCE_NAMES list"
            )
            current_run.log_info(f"Configuration path: {os.path.join(root, 'config', 'pnlp_dse_push_config.json')}")
            raise ValueError
        current_run.log_info(f"DSE organisation unit (Provinces) match against {connection_string} pyramid.")

    except ValueError:
        raise
    except Exception as e:
        raise Exception(
            f"An error occurred while checking org units alignment against source {connection_string}. Error: {e}"
        )

    # -------------Start alignment-------------
    orgUnit_source = pd.concat([source_pyramid_provinces, source_pyramid_zs])

    # logs
    report_path = os.path.join(root, "logs", "organisation_units")
    os.makedirs(report_path, exist_ok=True)
    configure_login(logs_path=report_path, task_name="organisation_units")

    if orgUnit_source.shape[0] > 0:
        # Retrieve the target (PNLP) orgUnits to compare
        current_run.log_info(f"Retrieving organisation units from target DHIS2 instance {dhis2_client_target.api.url}")
        orgUnit_target = dhis2_client_target.meta.organisation_units(
            fields="id,name,shortName,openingDate,closedDate,parent,level,path,geometry"
        )
        orgUnit_target = pd.DataFrame(orgUnit_target)

        # Get list of ids for creation and update
        ou_new = list(set(orgUnit_source.id) - set(orgUnit_target.id))
        ou_matching = list(set(orgUnit_source.id).intersection(set(orgUnit_target.id)))
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
                    config=config,
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
                    config=config,
                )
                current_run.log_info("Organisation units push finished.")
        except Exception as e:
            raise Exception(f"Unexpected error occurred while updating organisation units. Error: {e}")

    else:
        current_run.log_warning("No data found in the pyramid file. Organisation units task skipped.")


@dhis2_dse_push.task
def pnlp_dse_database_push(pipeline_path: str, dhis2_client_target: DHIS2, config: dict, success: bool) -> bool:
    """Put some data processing code here."""

    current_run.log_info("DSE database data push started...")
    report_path = os.path.join(pipeline_path, "logs", "dse_database")
    os.makedirs(report_path, exist_ok=True)
    configure_login(logs_path=report_path, task_name="dse_database")

    # Parameters for the import
    import_strategy = config["DSE_PUSH_SETTINGS"].get("IMPORT_STRATEGY", None)
    if import_strategy is None:
        import_strategy = "CREATE_AND_UPDATE"  # CREATE, UPDATE, CREATE_AND_UPDATE

    dry_run = config["DSE_PUSH_SETTINGS"].get("DRY_RUN", None)
    if dry_run is None:
        dry_run = True  # True or False

    max_post = config["DSE_PUSH_SETTINGS"].get("MAX_POST", None)
    if max_post is None:
        max_post = 500  # number of datapoints without a time-out limit (?)

    months_overlap = config["DSE_PUSH_SETTINGS"].get("MONTHS_DATA_OVERLAP", None)
    if months_overlap is None:
        months_overlap = 0  # number of months to push from the previous push (based on last_pushed_date)

    try:
        # get last date pushed
        last_pushed_date = get_last_pushed_date(os.path.join(pipeline_path, "config"), "dse_database")

        # table name
        table_name = config["DSE_PUSH_SETTINGS"].get("PNLP_DSE_PALU_TABLE", None)
        if table_name is None:
            current_run.log_error("PNLP_DSE_PALU_TABLE table name is not provided.")
            raise ValueError

        # Load DSE database PALU_TOT data from DB table
        dse_database_data = load_db_table_data(table_name=table_name)

        # Add date columns and use combination of year+NUMSEM for period weeks
        dse_database_data["year"] = dse_database_data["year"].apply(lambda x: str(int(float(x))))
        dse_database_data["period"] = (
            dse_database_data["year"].astype(str) + "W" + dse_database_data["NUMSEM"].astype(str)
        )
        dse_database_data["period_date"] = dse_database_data["period"].apply(lambda x: week_to_date(x))

        # Get max period_date available
        dse_date_max = dse_database_data.period_date.max()
        current_run.log_info(
            f"Last DSE database pushed date {last_pushed_date} - DSE table data available until : {dse_date_max}"
        )
        if dse_date_max <= last_pushed_date:
            current_run.log_info("No new DSE data to push.")
            return True  # Exit!

        # SELECT data to PUSH using last_pushed_date
        last_pushed_date = subtract_months(last_pushed_date, months_overlap)  # APPLY LAG
        current_run.log_info(
            f"Pushing DSE database data ({table_name}) from : {last_pushed_date} (overlap: -{months_overlap})"
        )
        dse_database_data = dse_database_data[dse_database_data.Date >= last_pushed_date]

        # we should use the same source (snis) ID mappings for pushing.
        # At this point (after OU alignment) the names should be correct.
        # This is why is not necessary to apply ORG_UNIT mappings using TARGET_MAPPINGS config.
        # This is only to keep the mapping between SNIS <> DSE.
        dse_database_data_map = apply_source_mappings_for_dse_data(
            dse_database_data, config["SOURCE_MAPPINGS"]["PNLP_DSE_PALU_MAPPING"], dataset="dse_database"
        )

        # reformat data to DHIS2 push format...
        dse_database_data_formatted = to_dhis2_format_dse_db(dse_data=dse_database_data_map, config=config)

        # We don't apply further mappings for NMDR
        # Also not necessary if the alignment was successful...

        # transform in a list of json dataElements (NA values were filtered in Cas and Deces in to_dhis2_format_dse_db())
        datapoints_valid, datapoints_not_valid, datapoints_to_na = select_transform_to_json(
            data_values=dse_database_data_formatted
        )

        # log not valid datapoints (if any)
        log_ignored_or_na(report_path=report_path, datapoint_list=datapoints_not_valid, data_type="dse_database")

        # push data..
        current_run.log_info(
            f"Pushing DSE database data with parameters import_strategy: {import_strategy}, dry_run: {dry_run}, max_post: {max_post}"
        )
        # datapoints set to NA
        if len(datapoints_to_na) > 0:
            log_ignored_or_na(
                report_path=report_path, datapoint_list=datapoints_to_na, data_type="dse_database", is_na=True
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
            logging.info(msg)
            log_summary_errors(summary_na)
        #

        current_run.log_info(f"Start pushing {len(datapoints_valid)} valid data values.")
        # Push valid data
        summary = push_data_elements(
            dhis2_client=dhis2_client_target,
            data_elements_list=datapoints_valid,
            strategy=import_strategy,
            dry_run=dry_run,
            max_post=max_post,
        )

        # log info
        msg = f"DSE database export summary:  {summary['import_counts']}"
        current_run.log_info(msg)
        logging.info(msg)
        log_summary_errors(summary)

        # Check if all data points were correctly imported by DHIS2
        dp_ignored = summary["import_counts"]["ignored"]
        if dp_ignored > 0:
            current_run.log_warning(
                f"{dp_ignored} datapoints not imported. dse_database last push date is not updated: {last_pushed_date}"
            )
        else:
            # Save the last date pushed for Temperature max
            total_dp = summary["import_counts"]["imported"] + summary["import_counts"]["updated"]
            current_run.log_info(f"{total_dp} datapoints correctly processed.")
            # current_run.log_info(f"All {len(datapoints_valid)} datapoints correctly imported.")
            # Save the last date pushed for DSE db
            update_last_available_date_log(os.path.join(pipeline_path, "config"), "dse_database", dse_date_max)
        return True

    except Exception as e:
        current_run.log_error(f"An error occurred while pushing dse database data: {e}")
        raise


@dhis2_dse_push.task
def pnlp_completude_push(pipeline_path: str, dhis2_client_target: DHIS2, config: dict, success: bool) -> bool:
    """Push DSE_completude table to NMDR DHIS2."""

    current_run.log_info("DSE completude data push started...")
    report_path = os.path.join(pipeline_path, "logs", "dse_completude")
    os.makedirs(report_path, exist_ok=True)
    configure_login(logs_path=report_path, task_name="dse_completude")

    # Parameters for the import
    import_strategy = config["DSE_PUSH_SETTINGS"].get("IMPORT_STRATEGY", None)
    if import_strategy is None:
        import_strategy = "CREATE_AND_UPDATE"  # CREATE, UPDATE, CREATE_AND_UPDATE

    dry_run = config["DSE_PUSH_SETTINGS"].get("DRY_RUN", None)
    if dry_run is None:
        dry_run = True  # True or False

    max_post = config["DSE_PUSH_SETTINGS"].get("MAX_POST", None)
    if max_post is None:
        max_post = 500  # number of datapoints without a time-out limit (?)

    months_overlap = config["DSE_PUSH_SETTINGS"].get("MONTHS_DATA_OVERLAP", None)
    if months_overlap is None:
        months_overlap = 0  # number of months to push from the previous push (based on last_pushed_date)

    try:
        # get last date pushed
        last_date_pushed = get_last_pushed_date(os.path.join(pipeline_path, "config"), "dse_completude")

        # table name
        table_name = config["DSE_PUSH_SETTINGS"].get("PNLP_DSE_COMPLETUDE_TABLE", None)
        if table_name is None:
            current_run.log_error("PNLP_DSE_COMPLETUDE_TABLE table name is not provided.")
            raise ValueError

        # Load DSE completude data from DB table
        dse_completude_data = load_db_table_data(table_name=table_name)

        # Add columns period and period_date
        dse_completude_data["period"] = (
            dse_completude_data["year"].astype(str) + "W" + dse_completude_data["NUMSEM"].astype(str)
        )
        dse_completude_data["period_date"] = dse_completude_data["period"].apply(lambda x: week_to_date(x))

        # Get max period_date available
        dse_date_max = dse_completude_data.period_date.max()
        current_run.log_info(
            f"Last DSE completude pushed date {last_date_pushed} - DSE completude data available until : {dse_date_max}"
        )
        if dse_date_max <= last_date_pushed:
            current_run.log_info("No new DSE completude data to push.")
            return True  # Exit!

        # SELECT data to PUSH using last_pushed_date
        last_date_pushed = subtract_months(last_date_pushed, months_overlap)  # APPLY LAG?
        current_run.log_info(f"Pushing new DSE completude data from : {last_date_pushed} (overlap: -{months_overlap})")
        dse_completude_data_f = dse_completude_data[dse_completude_data.period_date >= last_date_pushed].copy()

        # ADD province OU ids (merge new column)
        current_run.log_info("Adding Province ids to completude table.")
        source_pyramid_provinces = get_pyramid_for_level(
            connection_str=config["DSE_PUSH_SETTINGS"]["DHIS2_CONNECTION_SOURCE"], pyramid_lvl=2
        )  # provinces
        dse_completude_data_m = dse_completude_data_f.merge(
            source_pyramid_provinces[["name", "id"]], left_on="province_ref", right_on="name", how="left"
        )
        dse_completude_data_m = dse_completude_data_m.drop(columns=["name"])

        # we should use the same source (snis) ID mappings for pushing.
        dse_database_data_map = apply_source_mappings_for_dse_data(
            dse_completude_data_m, config["SOURCE_MAPPINGS"]["PNLP_DSE_COMPLETUDE_MAPPING"], dataset="dse_completude"
        )

        # reformat data to DHIS2 push format...
        dse_completude_data_formatted = to_dhis2_format_dse_completude(
            dse_completude_data=dse_database_data_map, config=config
        )

        # We don't apply further mappings for NMDR
        # Also not necessary if the alignment was successful...

        # transform in a list of json dataElements (NA values were filtered in Cas and Deces in to_dhis2_format_dse_db())
        datapoints_valid, datapoints_not_valid, datapoints_to_na = select_transform_to_json(
            data_values=dse_completude_data_formatted
        )

        # log not valid datapoints (if any)
        log_ignored_or_na(report_path=report_path, datapoint_list=datapoints_not_valid, data_type="dse_completude")

        # push data
        current_run.log_info(
            f"Pushing DSE completude data with parameters import_strategy: {import_strategy}, dry_run: {dry_run}, max_post: {max_post}"
        )

        # datapoints set to NA
        if len(datapoints_to_na) > 0:
            log_ignored_or_na(
                report_path=report_path, datapoint_list=datapoints_to_na, data_type="dse_completude", is_na=True
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
            logging.info(msg)
            log_summary_errors(summary_na)
        #

        current_run.log_info(f"Start pushing {len(datapoints_valid)} valid data values.")
        # push valid datapoints
        summary = push_data_elements(
            dhis2_client=dhis2_client_target,
            data_elements_list=datapoints_valid,
            strategy=import_strategy,
            dry_run=dry_run,
            max_post=max_post,
        )

        # log info
        msg = f"DSE completude export summary:  {summary['import_counts']}"
        current_run.log_info(msg)
        logging.info(msg)
        log_summary_errors(summary)

        # Check if all data points were correctly imported by DHIS2
        dp_ignored = summary["import_counts"]["ignored"]
        if dp_ignored > 0:
            current_run.log_warning(
                f"{dp_ignored} datapoints not imported. dse_completude last push date is not updated: {last_date_pushed}"
            )
        else:
            # Save the last date pushed for Temperature max
            total_dp = summary["import_counts"]["imported"] + summary["import_counts"]["updated"]
            current_run.log_info(f"{total_dp} datapoints correctly processed.")
            # current_run.log_info(f"All {len(datapoints_valid)} datapoints correctly imported.")
            # Save the last date pushed for DSE db
            update_last_available_date_log(os.path.join(pipeline_path, "config"), "dse_completude", dse_date_max)

        return True
    except Exception as e:
        current_run.log_error(f"Error occurred during completude push {e}")
        raise


def configure_login(logs_path: str, task_name: str):
    # Configure logging
    now = datetime.now().strftime("%Y-%m-%d-%H_%M")
    logging.basicConfig(
        filename=os.path.join(logs_path, f"{task_name}_{now}.log"),
        level=logging.INFO,
        format="%(asctime)s - %(message)s",
    )


# helper
def subtract_months(date_str: str, lag_months: int) -> str:
    # Parse the input date string into a datetime object
    date = datetime.strptime(date_str, "%Y-%m-%d")

    # Subtract the specified number of months
    new_date = date - relativedelta(months=lag_months)

    # Return the resulting date as a string
    return new_date.strftime("%Y-%m-%d")


def push_orgunits_create(ou_df: pd.DataFrame, dhis2_client_target: DHIS2, report_path: str, config: dict):
    # Parameters for the api call
    dry_run = config["DSE_PUSH_SETTINGS"].get("DRY_RUN", None)
    if dry_run is None:
        dry_run = True  # True or False

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


class DataPoint:
    def __init__(self, series_row: pd.Series):
        """Create a new org unit instance.

        Parameters
        ----------
        series_row : pandas series
            Expects columns with names :
                ['data_type',
                'dx_uid',
                'period',
                'org_unit',
                'category_option_combo',
                'attribute_option_combo',
                'rate_type',
                'domain_type',
                'value']
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
        return json_dict

    def to_delete_json(self) -> dict:
        json_dict = {
            "dataElement": self.dataElement,
            "period": self.period,
            "orgUnit": self.orgUnit,
            "categoryOptionCombo": self.categoryOptionCombo,
            "attributeOptionCombo": self.attributeOptionCombo,
            "value": "",
            "comment": "deleted value",
        }
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


def get_pyramid_for_level(connection_str: str, pyramid_lvl: int):
    try:
        # Set parameters
        connection = workspace.dhis2_connection(connection_str)
        dhis2_source_client = DHIS2(connection=connection, cache_dir=None)
        # Retrieve province pyramid
        pyramid_lvl_selection = dhis2_source_client.meta.organisation_units(
            filter=f"level:le:{pyramid_lvl}",
            fields="id,name,shortName,openingDate,closedDate,parent,level,geometry",  # fixed columns for SNIS push pipelines
        )
        pyramid = pd.DataFrame(pyramid_lvl_selection)

        # Pyramid at level (pyramid_lvl)
        return pyramid[pyramid.level == pyramid_lvl]

    except Exception as e:
        raise Exception(f"Error occurred while loading OU ids for completude {e}")


# format for completude DHIS2 data
def to_dhis2_format_dse_completude(
    dse_completude_data: pd.DataFrame,
    config: dict,
) -> pd.DataFrame:
    """
    Maps DSE data to a standardized DHIS2 data table.
    """

    if dse_completude_data.empty:
        return None

    uid_mappings = config["TARGET_MAPPINGS"]["PNLP_DSE_COMPLETUDE_MAPPING"].get("UIDS", {})
    coc_default = config["TARGET_MAPPINGS"]["PNLP_DSE_PALU_MAPPING"]["ATTR_OPTION_COMBO"].get("DEFAULT", None)
    aoc_default = config["TARGET_MAPPINGS"]["PNLP_DSE_PALU_MAPPING"]["ATTR_OPTION_COMBO"].get("DEFAULT", None)
    if coc_default is None:
        coc_default = "HllvX50cXC0"
    if aoc_default is None:
        aoc_default = "HllvX50cXC0"

    # Check for None values in UIDS
    if any(value is None for value in uid_mappings.values()):
        missing_keys = [key for key, value in uid_mappings.items() if value is None]
        raise ValueError(f"The PNLP_DSE_COMPLETUDE_MAPPING UIDS have None values: {', '.join(missing_keys)}")

    # SET NA VALUES TO 0
    for c in uid_mappings.keys():
        if dse_completude_data[c].isna().any():
            # current_run.log_info(f"NaN values found DSE Completude in column: {c} replacing to 0.")
            dse_completude_data[c] = dse_completude_data[c].fillna(0)

    try:
        temp_table = []
        for uid_key, uid_value in uid_mappings.items():
            dhis2_format_sub = pd.DataFrame(index=dse_completude_data.index)
            dhis2_format_sub["data_type"] = "DSE_COMPLETUDE"
            dhis2_format_sub["dx_uid"] = uid_value
            dhis2_format_sub["period"] = dse_completude_data["period"]
            dhis2_format_sub["org_unit"] = dse_completude_data["id"]
            dhis2_format_sub["category_option_combo"] = coc_default
            dhis2_format_sub["attribute_option_combo"] = aoc_default
            dhis2_format_sub["rate_type"] = None
            dhis2_format_sub["domain_type"] = "AGGREGATED"
            dhis2_format_sub["value"] = dse_completude_data[uid_key]  # the columns names and keys should always match!
            temp_table.append(dhis2_format_sub)

        dhis2_format = pd.concat(temp_table, ignore_index=True)
        # Ensure all values are numeric or set to None
        dhis2_format["value"] = pd.to_numeric(dhis2_format["value"], errors="coerce")
        dhis2_format["value"] = dhis2_format["value"].replace({np.nan: None})
        # sorting might improve speed
        dhis2_format = dhis2_format.sort_values(by=["org_unit"], ascending=True)
        return dhis2_format

    except AttributeError as e:
        raise AttributeError(f"DSE completude data is missing a required attribute: {e}")
    except Exception as e:
        raise Exception(f"Unexpected Error while creating DSE completude format table: {e}")


def push_orgunits_update(
    orgUnit_source: pd.DataFrame,
    orgUnit_target: pd.DataFrame,
    matching_ou_ids: list,
    dhis2_client_target: DHIS2,
    report_path: str,
    config: dict,
):
    """
    Update org units based matching id list
    """
    # Parameters for the api call
    dry_run = config["DSE_PUSH_SETTINGS"].get("DRY_RUN", None)
    if dry_run is None:
        dry_run = True  # True or False

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
            logging.info(str(response))

        if progress_count % 500 == 0:
            current_run.log_info(f"Organisation units checked: {progress_count}/{len(matching_ou_ids)}")

    current_run.log_info(f"Organisation units updated: {updates_count}")
    logging.info(f"Organisation units updated: {updates_count}")
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
        params={"dryRun": dry_run, "importStrategy": "CREATE_AND_UPDATE"},  # f"{strategy}"}, #fixed
    )

    return build_formatted_response(response=r, strategy=strategy, ou_id=orgunit.id)


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
                response = r.json().get("response") if r else None
            except (ValueError, AttributeError):
                response = None

            if response:
                for key in ["imported", "updated", "ignored", "deleted"]:
                    summary["import_counts"][key] += response.get("importCount", {}).get(key, 0)

            error_response = get_response_value_errors(response, chunk=chunk)
            summary["ERRORS"].append({"error": e, "response": error_response})

            # Stop the pipeline if the we have a server error.
            if r and 500 <= r.status_code < 600:
                raise Exception(
                    f"Server error pipeline halted: {e} - {error_response} summary: {summary['import_counts']}"
                )

        if ((count * max_post) % 20000) == 0:
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


def split_list(src_list: list, length: int) -> Generator[list, None, None]:
    """Split list into chunks."""
    for i in range(0, len(src_list), length):
        yield src_list[i : i + length]


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


def read_parquet_extract(parquet_file: str) -> pd.DataFrame:
    ou_source = pd.DataFrame()
    try:
        ou_source = pd.read_parquet(parquet_file)
    except FileNotFoundError:
        raise FileNotFoundError(f"Error while loading the extract: File was not found {parquet_file}.")
    except pd.errors.EmptyDataError:
        pd.errors.EmptyDataError(f"Error while loading the extract: File is empty {parquet_file}.")
    except Exception as e:
        Exception(f"Error while loading the extract: {parquet_file}. Error: {e}")

    return ou_source


def get_last_pushed_date(json_folder: str, node: str, json_name: str = "last_pushed_DSE_date.json") -> str:
    # Check if the file exists
    file_path = os.path.join(json_folder, json_name)
    if not os.path.exists(file_path):
        current_run.log_warning(f"The file {file_path} does not exist.")
        return "2010-01-01"  # Default

    # Load the JSON file
    with open(file_path, "r") as f:
        data = json.load(f)

    # Check if the node exists
    if node not in data:
        current_run.log_warning(f"The node '{node}' does not exist in the JSON file {file_path}")
        return "2010-01-01"  # Default

    # Return the node's value
    return data[node]


def update_last_available_date_log(
    json_folder: str, node: str, iso_date: str, json_name: str = "last_pushed_DSE_date.json"
):
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

    current_run.log_info(f"Updated {node} with start of week date {iso_date} in {file_path}.")


def to_dhis2_format_dse_db(
    dse_data: pd.DataFrame,
    config: dict,
) -> pd.DataFrame:
    """
    Maps DSE data to a standardized DHIS2 data table.
    """

    if dse_data.empty:
        return None

    uid_mappings = {
        "Cas": config["TARGET_MAPPINGS"]["PNLP_DSE_PALU_MAPPING"]["UIDS"].get("CAS", None),
        "Deces": config["TARGET_MAPPINGS"]["PNLP_DSE_PALU_MAPPING"]["UIDS"].get("DECES", None),
    }
    # Prefix = C for "Cas" and D for "Deces"..
    coc_mappings = {
        "011MOIS": config["TARGET_MAPPINGS"]["PNLP_DSE_PALU_MAPPING"]["CAT_OPTION_COMBO"].get("C011MOIS", None),
        "1259MOIS": config["TARGET_MAPPINGS"]["PNLP_DSE_PALU_MAPPING"]["CAT_OPTION_COMBO"].get("C1259MOIS", None),
        "5ANSP": config["TARGET_MAPPINGS"]["PNLP_DSE_PALU_MAPPING"]["CAT_OPTION_COMBO"].get("C5ANSP", None),
    }

    aoc_default = config["TARGET_MAPPINGS"]["PNLP_DSE_PALU_MAPPING"]["ATTR_OPTION_COMBO"].get("DEFAULT", None)
    if aoc_default is None:
        aoc_default = "HllvX50cXC0"

    # Check for None values in UIDS
    if any(value is None for value in uid_mappings.values()):
        missing_keys = [key for key, value in uid_mappings.items() if value is None]
        current_run.log_error(f"The PNLP_DSE_PALU_MAPPING UIDS have None values: {', '.join(missing_keys)}")
        raise ValueError

    # Check for None values in COC
    if any(value is None for value in coc_mappings.values()):
        missing_keys = [key for key, value in coc_mappings.items() if value is None]
        current_run.log_error(f"The CAT_OPTION_COMBO UIDS have None values: {', '.join(missing_keys)}")
        raise ValueError

    # dse_data_f = dse_data[dse_data["Cas"].notna() & dse_data["Deces"].notna()] # we push NAs also
    dse_data_f = dse_data[dse_data["MALADIE"] == "PALUDISME"]  # make sure is the correct disease

    try:
        temp_table = []
        for uid_key, uid_value in uid_mappings.items():
            for coc_key, coc_value in coc_mappings.items():
                # select the correct COC sub group by Cas or Deces.
                if uid_key == "Cas":
                    df_data_coc = dse_data_f[dse_data_f["Cas_Age"] == f"C{coc_key}"]
                else:
                    df_data_coc = dse_data_f[dse_data_f["Deces_Age"] == f"D{coc_key}"]
                dhis2_format_sub = pd.DataFrame(index=df_data_coc.index)
                dhis2_format_sub["data_type"] = f"DSE_PALU_{uid_key.upper()}"
                dhis2_format_sub["dx_uid"] = uid_value
                dhis2_format_sub["period"] = df_data_coc["period"]
                dhis2_format_sub["org_unit"] = df_data_coc["zone_id_DHIS2"]
                dhis2_format_sub["category_option_combo"] = coc_value
                dhis2_format_sub["attribute_option_combo"] = aoc_default
                dhis2_format_sub["rate_type"] = None
                dhis2_format_sub["domain_type"] = "AGGREGATED"
                dhis2_format_sub["value"] = df_data_coc[uid_key]
                temp_table.append(dhis2_format_sub)

        dhis2_format = pd.concat(temp_table, ignore_index=True)
        # Ensure all values are numeric or set to None
        dhis2_format["value"] = pd.to_numeric(dhis2_format["value"], errors="coerce")
        dhis2_format["value"] = dhis2_format["value"].replace({np.nan: None})
        # sorting might improve speed
        dhis2_format = dhis2_format.sort_values(by=["org_unit"], ascending=True)
        return dhis2_format

    except AttributeError as e:
        raise AttributeError(f"DSE db data is missing a required attribute: {e}")
    except Exception as e:
        raise Exception(f"Unexpected Error while creating DSE format table: {e}")


def load_db_table_data(table_name: str) -> pd.DataFrame:
    """Load db data from database table."""
    current_run.log_info(f"Loading data from {table_name}")
    dbengine = create_engine(os.environ["WORKSPACE_DATABASE_URL"])
    data = pd.read_sql_table(table_name, con=dbengine)
    return data


def build_formatted_response(response: requests.Response, strategy: str, ou_id: str) -> dict:
    resp = {
        "action": strategy,
        "statusCode": response.status_code,
        "status": response.json().get("status"),
        "response": response.json().get("response"),
        "ou_id": ou_id,
    }
    return resp


def week_to_date(week_str: str) -> str:
    """
    Converts a week string in the format 'YYYYWw' to the corresponding start date of the week.
    Args:
        week_str (str): A string in the format 'YYYYWw', where YYYY is the year and w is the week number.
    Returns:
        str: The start date of the week in 'YYYY-MM-DD' format.
    """
    # Parse the input string to extract year and week number
    year = int(week_str[:4])
    week = int(week_str[5:])

    # Calculate the first day of the first week of the year
    first_day_of_year = datetime(year, 1, 1)

    # Adjust for the first Thursday rule
    first_week_start = first_day_of_year - timedelta(days=first_day_of_year.weekday())  # Monday of the first ISO week
    if first_day_of_year.weekday() > 3:
        first_week_start += timedelta(weeks=1)

    # Calculate the start of the given week
    start_of_week = first_week_start + timedelta(weeks=week - 1)

    # Return the date as a string in 'YYYY-MM-DD' format
    return start_of_week.strftime("%Y-%m-%d")


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


# log ignored datapoints in the report
def log_ignored_or_na(report_path, datapoint_list, data_type="dse_database", is_na=False):
    if len(datapoint_list) > 0:
        current_run.log_info(
            f"{len(datapoint_list)} datapoints will be {'set to NA' if is_na else 'ignored'}. Please check the report for details {report_path}"
        )
        logging.warning(
            f"{len(datapoint_list)} {data_type} datapoints {'will be set to NA' if is_na else 'to be ignored'}: "
        )
        for i, dp in enumerate(datapoint_list, start=1):
            logging.warning(f"{i} DataElement {'NA' if is_na else 'ignored'} : {dp}")


def apply_source_mappings_for_dse_data(dataset_df: pd.DataFrame, mappings: dict, dataset: str) -> pd.DataFrame:
    """
    All matching ids will be replaced.
    Is user responsability to provide the correct IDS.
    id_column = "zone_id_DHIS2" or "province_ref"
    """
    if dataset == "dse_database":
        dataset_msg = "(ZS) in DSE database"
        id_column = "zone_id_DHIS2"
        map_var = "ZONE_DE_SANTE_UID"

    elif dataset == "dse_completude":
        dataset_msg = "(Province names) in DSE Completude"
        id_column = "province_ref"
        map_var = "PROVINCE_NAMES"
    else:
        raise ValueError("Dataset must be: dse_database or dse_completude")

    orunits_to_replace = list(set(dataset_df[id_column]).intersection(set(mappings.get(map_var, {}).keys())))
    if len(orunits_to_replace) > 0:
        current_run.log_info(
            f"Number of org units {dataset_msg} to be replaced for source mapping: {len(orunits_to_replace)}."
        )
        dataset_df.loc[:, id_column] = dataset_df[id_column].replace(mappings.get(map_var, {}))

    return dataset_df


if __name__ == "__main__":
    dhis2_dse_push()
