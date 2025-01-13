import os
import pandas as pd
import json
from datetime import datetime
import requests
import logging

from utils import (
    # get_periods_yyyymm,
    # merge_dataframes,
    # initialize_queue,
    enqueue,
    dequeue,
    # save_to_parquet,
    # first_day_of_future_month,
    read_parquet_extract,
    split_list,
)

from openhexa.sdk import current_run, pipeline, workspace, parameter
from openhexa.toolbox.dhis2 import DHIS2


@pipeline("dhis2-pnlp-push", name="dhis2_pnlp_push", timeout=28800)
@parameter(
    "push_orgunits",
    name="Export Organisation Units",
    help="",
    type=bool,
    default=True,
    required=False,
)
@parameter(
    "push_pop",
    name="Export population",
    help="",
    type=bool,
    default=True,
    required=False,
)
@parameter(
    "push_analytics",
    name="Export analytics",
    help="",
    type=bool,
    default=True,
    required=False,
)
def dhis2_pnlp_push(push_orgunits: bool, push_pop: bool, push_analytics: bool):
    """
    This pipeline push the extracted data from SNIS DHIS2 to the PNLP DHIS2.
    Most of the tasks and functions are specific to this project.

    """

    # set paths
    PIPELINE_ROOT = os.path.join(f"{workspace.files_path}", "pipelines", "dhis2_pnlp_push")
    EXTRACT_PIPELINE_ROOT = os.path.join(f"{workspace.files_path}", "pipelines", "dhis2_snis_extract")

    try:
        # load config
        config = load_configuration(pipeline_path=PIPELINE_ROOT)

        # connect to DHIS2
        dhis2_client = connect_to_dhis2(config=config, cache_dir=None)

        pyramid_ready = push_organisation_units(
            root=PIPELINE_ROOT,
            extract_pipeline_path=EXTRACT_PIPELINE_ROOT,
            dhis2_client_target=dhis2_client,
            run_task=push_orgunits,
        )

        pop_ready = push_population(
            root=PIPELINE_ROOT,
            extract_pipeline_path=EXTRACT_PIPELINE_ROOT,
            dhis2_client_target=dhis2_client,
            config=config,
            run_task=push_pop,
            wait=pyramid_ready,
        )

        push_extracts(
            root=PIPELINE_ROOT,
            extract_pipeline_path=EXTRACT_PIPELINE_ROOT,
            dhis2_client_target=dhis2_client,
            config=config,
            run_task=push_analytics,
            wait=pop_ready,
        )

    except Exception as e:
        current_run.log_error(f"An error occurred: {e}")


@dhis2_pnlp_push.task
def load_configuration(pipeline_path: str) -> dict:
    """
    Reads a JSON file configuration and returns its contents as a dictionary.

    Args:
        pipeline_path (str): Root path of the pipeline to find the file.

    Returns:
        dict: Dictionary containing the JSON data.
    """
    try:
        file_path = os.path.join(pipeline_path, "config", "pnlp_push_config.json")
        with open(file_path, "r") as file:
            data = json.load(file)

        current_run.log_info("Configuration loaded.")
        return data
    except FileNotFoundError as e:
        raise Exception(f"The file '{file_path}' was not found {e}")
    except json.JSONDecodeError as e:
        raise Exception(f"Error decoding JSON: {e}")
    except Exception as e:
        raise Exception(f"Unexpected error while loading configuration '{file_path}' {e}")


@dhis2_pnlp_push.task
def connect_to_dhis2(config: dict, cache_dir: str):
    try:
        connection = workspace.dhis2_connection(config["PUSH_SETTINGS"]["DHIS2_CONNECTION"])
        dhis2_client = DHIS2(connection=connection, cache_dir=cache_dir)
        current_run.log_info(f'Connected to DHIS2 connection: {config["PUSH_SETTINGS"]["DHIS2_CONNECTION"]}')
        return dhis2_client
    except Exception as e:
        raise Exception(f"Error while connecting to DHIS2 {config['PUSH_SETTINGS']['DHIS2_CONNECTION']}: {e}")


@dhis2_pnlp_push.task
def push_organisation_units(root: str, extract_pipeline_path: str, dhis2_client_target: DHIS2, run_task: bool):
    """
    This task handles creation and updates of organisation units in the target DHIS2 (incremental approach only).

    We use the previously extracted pyramid (full) stored as dataframe as input.
    The format of the pyramid contains the expected columns. A dataframe that doesn't contain the
    mandatory columns will be skipped (not valid).
    """

    if not run_task:
        return True

    current_run.log_info("Starting organisation units push.")
    report_path = os.path.join(root, "logs", "organisationUnits")
    configure_login(logs_path=report_path, task_name="organisation_units")

    # Load pyramid extract
    ou_parquet = os.path.join(extract_pipeline_path, "data", "raw", "pyramid", "snis_pyramid.parquet")
    orgUnit_source = read_parquet_extract(ou_parquet)

    if orgUnit_source.shape[0] > 0:
        # Retrieve the target (PNLP) orgUnits to compare
        current_run.log_info(f"Retrieving organisation units from target DHIS2 instance {dhis2_client_target.api.url}")
        # orgUnit_target = dhis2_client_target.meta.organisation_units_extra_fields()
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
                )
        except Exception as e:
            raise Exception(f"Unexpected error occurred while creating organisation units. Error: {e}")

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
                    dhis2_client_target=dhis2_client_target,
                    report_path=report_path,
                )
                current_run.log_info("Organisation units push finished.")
        except Exception as e:
            raise Exception(f"Unexpected error occurred while updating organisation units. Error: {e}")

    else:
        current_run.log_warning("No data found in the pyramid file. Organisation units task skipped.")


@dhis2_pnlp_push.task
def push_population(
    root: str, extract_pipeline_path: str, dhis2_client_target: DHIS2, config: dict, run_task: bool, wait: bool
) -> bool:
    """in this task we push the population extracted data.

    NOTE: These correspond to regular data elements, so they could be treated as such. But let's keep them separated for now.
    """
    if not run_task:
        return True

    current_run.log_info("Starting population data push.")
    report_path = os.path.join(root, "logs", "population")
    configure_login(logs_path=report_path, task_name="population")

    # Parameters for the api call
    import_strategy = config["PUSH_SETTINGS"].get("IMPORT_STRATEGY", None)
    if import_strategy is None:
        import_strategy = "CREATE_AND_UPDATE"  # CREATE, UPDATE, CREATE_AND_UPDATE

    dry_run = config["PUSH_SETTINGS"].get("DRY_RUN", None)
    if dry_run is None:
        dry_run = True  # True or False

    max_post = config["PUSH_SETTINGS"].get("MAX_POST", None)
    if max_post is None:
        max_post = 500  # number of datapoints without a time-out limit (?)

    # log parameters
    logging.info(f"Import strategy: {import_strategy} - Dry Run: {dry_run} - Max Post elements: {max_post}")

    try:
        pop_mappings = config.get("POPULATION_MAPPING", None)
        if pop_mappings is None:
            current_run.log_error("No POPULATION_MAPPING found in the configuration file.")
            raise ValueError

        # Load population extract
        pop_fname = os.path.join(
            extract_pipeline_path,
            "data",
            "raw",
            "population",
            "snis_population.parquet",
        )
        pop_source = read_parquet_extract(pop_fname)

        if pop_source.shape[0] > 0:
            # mandatory fields in the input dataset (pop_source)
            mandatory_fields = [
                "dx_uid",
                "period",
                "org_unit",
                "category_option_combo",
                "attribute_option_combo",
                "value",
            ]
            df = pop_source[mandatory_fields]

            # Sort the dataframe by org_unit to reduce the time (hopefully)
            df = df.sort_values(by=["org_unit"], ascending=True)

            # Use dictionary mappings to replace UIDS, OrgUnits, COC and AOC..
            df = apply_dataelement_mappings(datapoints_df=df, mappings=pop_mappings)

            # convert the datapoints to json and check if thei are valid
            # check the implementation of DataPoint class for the valid fields (mandatory)
            datapoints_valid, datapoints_not_valid, datapoints_to_na = select_transform_to_json(data_values=df)

            # log not valid datapoints
            log_ignored_or_na(report_path=report_path, datapoint_list=datapoints_not_valid)

            # Datapoints set value to NA
            if len(datapoints_to_na) > 0:
                log_ignored_or_na(report_path=report_path, datapoint_list=datapoints_to_na, is_na=True)
                summary_na = push_data_elements(
                    dhis2_client=dhis2_client_target,
                    data_elements_list=datapoints_to_na,  # different json format for deletion see: DataPoint class > to_delete_json()
                    strategy=import_strategy,
                    dry_run=dry_run,
                    max_post=max_post,
                )
                # log info
                msg = f"Population data elements set to NA summary:  {summary_na['import_counts']}"
                current_run.log_info(msg)
                logging.info(msg)
                log_summary_errors(summary_na)

            # push data
            summary = push_data_elements(
                dhis2_client=dhis2_client_target,
                data_elements_list=datapoints_valid,
                strategy=import_strategy,
                dry_run=dry_run,
                max_post=max_post,
            )

            # log info
            msg = f"Population export summary:  {summary['import_counts']}"
            current_run.log_info(msg)
            logging.info(msg)
            log_summary_errors(summary)

        else:
            current_run.log_warning("No population data found. Process finished.")

        return True
    except ValueError as e:
        raise ValueError(f"Population mapping error: {e}")
    except Exception as e:
        raise Exception(f"Population task error: {e}")


@dhis2_pnlp_push.task
def push_extracts(
    root: str,
    extract_pipeline_path: str,
    dhis2_client_target: DHIS2,
    config: dict,
    run_task: bool,
    wait: bool,
    enqueue_failed: bool = True,
):
    """Put some data processing code here."""

    if not run_task:
        return True

    current_run.log_info("Starting analytic extracts push.")
    report_path = os.path.join(root, "logs", "extracts")
    configure_login(logs_path=report_path, task_name="extracts")
    db_path = os.path.join(extract_pipeline_path, "config", ".queue.db")
    period_skipped = []

    # Parameters for the import
    import_strategy = config["PUSH_SETTINGS"].get("IMPORT_STRATEGY", None)
    if import_strategy is None:
        import_strategy = "CREATE_AND_UPDATE"  # CREATE, UPDATE, CREATE_AND_UPDATE

    dry_run = config["PUSH_SETTINGS"].get("DRY_RUN", None)
    if dry_run is None:
        dry_run = True  # True or False

    max_post = config["PUSH_SETTINGS"].get("MAX_POST", None)
    if max_post is None:
        max_post = 500  # number of datapoints without a time-out limit (?)

    # log parameters
    logging.info(f"Import strategy: {import_strategy} - Dry Run: {dry_run} - Max Post elements: {max_post}")
    current_run.log_info(f"Import strategy: {import_strategy} - Dry Run: {dry_run} - Max Post elements: {max_post}")

    try:
        data_mappings = config.get("DATAELEMENT_MAPPING", None)
        if data_mappings is None:
            current_run.log_error("No DATAELEMENT_MAPPING found in the configuration file.")
            raise ValueError
        rate_mappings = config.get("RATE_MAPPING", None)
        if rate_mappings is None:
            current_run.log_error("No RATE_MAPPING found in the configuration file.")
            raise ValueError
        acm_mappings = config.get("ACM_INDICATOR_MAPPING", None)
        if acm_mappings is None:
            current_run.log_error("No ACM_INDICATOR_MAPPING found in the configuration file.")
            raise ValueError

        while True:
            next_period = dequeue(db_path)
            if not next_period:
                break

            try:
                extract_data = read_parquet_extract(
                    parquet_file=os.path.join(
                        extract_pipeline_path, "data", "raw", "extracts", f"snis_data_{next_period}.parquet"
                    )
                )
                current_run.log_info(f"Push extract period: {next_period}.")
            except Exception as e:
                period_skipped.append(next_period)
                current_run.log_warning(
                    f"Error while reading the extracts file: snis_data_{next_period}.parquet - error: {e}"
                )
                continue

            # mandatory fields in the input dataset
            mandatory_fields = [
                "dx_uid",
                "period",
                "org_unit",
                "category_option_combo",
                "attribute_option_combo",
                "value",
            ]
            df = extract_data[mandatory_fields]
            # NOTE: FILTER: DO NOT PUSH THESE RATES, NOT USED!
            df = df[~(df.rate_type.isin(["ACTUAL_REPORTS", "EXPECTED_REPORTS", "ACTUAL_REPORTS_ON_TIME"]))]

            # Use dictionary mappings to replace UIDS, OrgUnits, COC and AOC..
            df = apply_dataelement_mappings(datapoints_df=df, mappings=data_mappings)
            df = apply_rate_mappings(datapoints_df=df, mappings=rate_mappings)
            df = apply_acm_mappings(datapoints_df=df, mappings=acm_mappings)

            # Sort the dataframe by org_unit to reduce the time (hopefully)
            df = df.sort_values(by=["org_unit"], ascending=True)

            # convert the datapoints to json and check if they are valid
            # check the implementation of DataPoint class for the valid fields (mandatory)
            datapoints_valid, datapoints_not_valid, datapoints_to_na = select_transform_to_json(data_values=df)

            # log not valid datapoints
            log_ignored_or_na(report_path=report_path, datapoint_list=datapoints_not_valid)

            # datapoints set to NA
            if len(datapoints_to_na) > 0:
                log_ignored_or_na(
                    report_path=report_path, datapoint_list=datapoints_to_na, data_type="extract", is_na=True
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

            # push data
            summary = push_data_elements(
                dhis2_client=dhis2_client_target,
                data_elements_list=datapoints_valid,
                strategy=import_strategy,
                dry_run=dry_run,
                max_post=max_post,
            )

            # log info
            msg = f"Analytics extracts summary for period {next_period}: {summary['import_counts']}"
            current_run.log_info(msg)
            logging.info(msg)
            log_summary_errors(summary)

        current_run.log_info("No more extracts to push.")

        # Enqueue the fail periods to re-try in the next run?
        # Sure, if the period exists in the queue, it will not be added again.
        if enqueue_failed:
            for p in period_skipped:
                enqueue(p, db_path)

    except Exception as e:
        raise Exception(f"Analytic extracts task error: {e}")


# def configure_login(logs_path: str, task_name: str):
#     # Configure logging
#     now = datetime.now().strftime("%Y-%m-%d-%H_%M")
#     logging.basicConfig(
#         filename=os.path.join(logs_path, f"{task_name}_{now}.log"),
#         level=logging.INFO,
#         format="%(asctime)s - %(message)s",
#     )


def configure_login(logs_path: str, task_name: str):
    """
    Configure logging to write logs to a file with real-time flushing.

    Args:
        logs_path (str): The directory path where logs should be stored.
        task_name (str): The task name to include in the log filename.
    """
    now = datetime.now().strftime("%Y-%m-%d-%H_%M")
    log_file = os.path.join(logs_path, f"{task_name}_{now}.log")

    # Set up logging with a FileHandler
    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(logging.Formatter("%(asctime)s - %(message)s"))

    # Get the root logger and add the handler
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    logger.addHandler(file_handler)

    # Ensure logs are flushed immediately
    file_handler.flush = lambda: file_handler.stream.flush()


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
        # for i, error in enumerate(errors, start=1):
        #     logging.error(f"Error {i}: {error}")
        #         logging.error(f"Logging {len(errors)} error(s) from export summary.")
        for i_e, error in enumerate(errors, start=1):
            logging.error(f"Error {i_e} : HTTP request failed : {error.get('error',None)}")
            error_response = error.get("response", None)
            if error_response:
                rejected_list = error_response.pop("rejected_datapoints", [])
                logging.error(f"Error response : {error_response}")
                for i_r, rejected in enumerate(rejected_list, start=1):
                    logging.error(f"Rejected data point {i_r}: {rejected}")


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

    def copy(self):
        attributes = self.to_json()
        new_instance = OrgUnitObj(pd.Series(attributes))
        return new_instance


# Helper class definition to store/create the correct DataElement JSON format
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
        # return {k: v for k, v in json_dict.items() if v is not None}
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


def push_orgunits_create(ou_df: pd.DataFrame, dhis2_client_target: DHIS2, report_path: str):
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

        if (count * max_post) % 20000 == 0:
            current_run.log_info(
                f'{count * max_post} / {total_datapoints} data points pushed summary: {summary["import_counts"]}'
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


def apply_dataelement_mappings(datapoints_df: pd.DataFrame, mappings: dict) -> pd.DataFrame:
    """
    All matching ids will be replaced.
    Is user responsability to provide the correct UIDS.
    """
    uids_to_replace = set(datapoints_df["dx_uid"]).intersection(mappings.get("UIDS", {}).keys())
    if len(uids_to_replace) > 0:
        current_run.log_info(f"{len(uids_to_replace)} OU UIDS to be replaced using mappings.")
        datapoints_df.loc[:, "dx_uid"] = datapoints_df["dx_uid"].replace(mappings.get("UIDS", {}))

    # Fields ou, coc and aoc will throw an error while pushing if wrong..
    orunits_to_replace = list(set(datapoints_df.org_unit).intersection(set(mappings.get("ORG_UNITS", {}).keys())))
    if len(orunits_to_replace) > 0:
        current_run.log_info(f"{len(orunits_to_replace)} Org units will be replaced using mappings.")
        datapoints_df.loc[:, "org_unit"] = datapoints_df["org_unit"].replace(mappings.get("ORG_UNITS", {}))

    # map category option combo default
    coc_default = mappings["CAT_OPTION_COMBO"].get("DEFAULT", None)
    if coc_default:
        current_run.log_info(f"Using {coc_default} default COC id.")
        datapoints_df.loc[:, "category_option_combo"] = datapoints_df["category_option_combo"].replace(
            {None: coc_default}
        )
    # map category option combo values
    coc_to_replace = set(datapoints_df["category_option_combo"]).intersection(
        mappings.get("CAT_OPTION_COMBO", {}).keys()
    )
    if len(coc_to_replace) > 0:
        current_run.log_info(f"{len(coc_to_replace)} of COC will be replaced using mappings.")
        datapoints_df.loc[:, "category_option_combo"] = datapoints_df["category_option_combo"].replace(
            mappings.get("CAT_OPTION_COMBO", {})
        )

    # map attribute option combo
    aoc_default = mappings["ATTR_OPTION_COMBO"].get("DEFAULT", None)
    if aoc_default:
        current_run.log_info(f"Using {aoc_default} default AOC id.")
        datapoints_df.loc[:, "attribute_option_combo"] = datapoints_df["attribute_option_combo"].replace(
            {None: aoc_default}
        )
    aoc_to_replace = set(datapoints_df["attribute_option_combo"]).intersection(
        mappings.get("ATTR_OPTION_COMBO", {}).keys()
    )
    if len(aoc_to_replace) > 0:
        current_run.log_info(f"{len(aoc_to_replace)} of AOC will be replaced using mappings.")
        datapoints_df.loc[:, "attribute_option_combo"] = datapoints_df["attribute_option_combo"].replace(
            mappings.get("ATTR_OPTION_COMBO", {})
        )

    return datapoints_df


def apply_rate_mappings(datapoints_df: pd.DataFrame, mappings: dict) -> pd.DataFrame:
    """
    this is a specific code to map the rate mapings

    All matching ids (keys) will be replaced.
    Is user responsability to provide the correct UIDS.
    """
    datapoints_df = datapoints_df.copy()
    for key, value in mappings.items():
        datapoints_df.loc[((datapoints_df.dx_uid == key) & (datapoints_df.rate_type == "REPORTING_RATE")), "dx_uid"] = (
            value["REPORTING_RATE"]
        )
        datapoints_df.loc[
            ((datapoints_df.dx_uid == key) & (datapoints_df.rate_type == "REPORTING_RATE_ON_TIME")), "dx_uid"
        ] = value["REPORTING_RATE_ON_TIME"]

    return datapoints_df


def apply_acm_mappings(datapoints_df: pd.DataFrame, mappings: dict) -> pd.DataFrame:
    """
    All matching ids will be replaced.
    Is user responsability to provide the correct UIDS.
    """
    datapoints_df = datapoints_df.copy()
    uids_acm_to_replace = set(datapoints_df["dx_uid"]).intersection(mappings.get("UIDS", {}).keys())
    if len(uids_acm_to_replace) > 0:
        current_run.log_info(f"{len(uids_acm_to_replace)} ACM indicator(s) to be replaced using mappings.")
        datapoints_df.loc[:, "dx_uid"] = datapoints_df["dx_uid"].replace(mappings.get("UIDS", {}))

    # map category option combo default
    coc_default = mappings["CAT_OPTION_COMBO"].get("DEFAULT", None)
    if coc_default and len(uids_acm_to_replace) > 0:
        current_run.log_info(f"Using {coc_default} default COC id mapping on ACM.")
        print(f"Using {coc_default} default COC id mapping on ACM.")
        datapoints_df.loc[datapoints_df.dx_uid.isin(mappings.get("UIDS", {}).values()), "category_option_combo"] = (
            coc_default
        )

    return datapoints_df


# log ignored datapoints in the report
def log_ignored_or_na(report_path, datapoint_list, data_type="population", is_na=False):
    if len(datapoint_list) > 0:
        current_run.log_info(
            f"{len(datapoint_list)} datapoints will be  {'updated to NA' if is_na else 'ignored'}. Please check the report for details {report_path}"
        )
        logging.warning(f"{len(datapoint_list)} {data_type} datapoints to be ignored: ")
        for i, ignored in enumerate(datapoint_list, start=1):
            logging.warning(f"{i} DataElement {'NA' if is_na else ''} ignored: {ignored}")


if __name__ == "__main__":
    dhis2_pnlp_push()
