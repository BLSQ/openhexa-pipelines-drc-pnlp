import os
import papermill as pm
from datetime import datetime

from openhexa.sdk import current_run, pipeline, parameter, workspace


@pipeline(code="sentinel-update-extract", name="Sentinel update extract")
@parameter(
    "get_year",
    name="Year",
    help="Year for which to process data",
    type=int,
    default=2024,
    required=True,
)
# @parameter(
#     "update_legacy",
#     name="Update legacy",
#     help="Update legacy OpenHexa database",
#     type=bool,
#     default=False,
#     required=False,
# )
@parameter(
    "update_pnlp",
    name="Update pnlp",
    help="Update pnlp OpenHexa database",
    type=bool,
    default=True,
    required=False,
)
def sentinel_update_extract(get_year, update_pnlp):
    """
    In this pipeline we call a notebook that executes the sentinel extract update

    """

    # Setup variables
    notebook_name = "sentinel_data_from_routine_extracts_pipeline"
    notebook_path = f"{workspace.files_path}/sentinel-sites/code/"
    out_notebook_path = f"{workspace.files_path}/sentinel-sites/papermill_outputs"

    # Set parameters
    parameters = {
        "ANNEE_A_ANALYSER": get_year,
        # 'UPLOAD_LEGACY' : update_legacy,
        "UPLOAD_PNLP": update_pnlp,
    }

    # execute OU sentinel uid list (NOTE: see sentinel-sites/sentinel_org_units_groups.ipynb)
    succes = run_sentinel_oug_update(parameters=parameters)

    # Run update notebook for PNLP tables
    run_update_with(
        nb_name=notebook_name,
        nb_path=notebook_path,
        out_nb_path=out_notebook_path,
        parameters=parameters,
        succes=succes,
    )


@sentinel_update_extract.task
def run_update_with(nb_name: str, nb_path: str, out_nb_path: str, parameters: dict, succes: bool):
    """
    Update a tables using the latest dataset version

    """
    nb_full_path = os.path.join(nb_path, f"{nb_name}.ipynb")

    current_run.log_info(f"Executing sentinel update notebook: {nb_full_path}")
    current_run.log_info(f"Running report for YEAR: {parameters['ANNEE_A_ANALYSER']}")
    current_run.log_info(f"Database update PNLP: {parameters['UPLOAD_PNLP']}")

    # out_nb_fname = os.path.basename(in_nb_dir.replace('.ipynb', ''))
    execution_timestamp = datetime.utcnow().strftime("%Y-%m-%d_%H_%M_%S")
    out_nb_fname = f"{nb_name}_OUTPUT_{execution_timestamp}.ipynb"
    out_nb_full_path = os.path.join(out_nb_path, out_nb_fname)

    try:
        pm.execute_notebook(input_path=nb_full_path, output_path=out_nb_full_path, parameters=parameters)
    except Exception as e:
        current_run.log_info(f"Caught error {type(e)}: e")

    current_run.log_info("Sentinel table updated")


@sentinel_update_extract.task
def run_sentinel_oug_update(parameters: dict):
    """
    We update the organisation unit groups file for the current year that
    should contain the list of organisation units for which we compute the
    sentinel indicators.
    """
    execution_timestamp = datetime.utcnow().strftime("%Y-%m-%d_%H_%M_%S")
    try:
        pm.execute_notebook(
            input_path=f"{workspace.files_path}/sentinel-sites/code/sentinel_org_units_groups.ipynb",
            output_path=f"{workspace.files_path}/sentinel-sites/papermill_outputs/sentinel_org_units_groups_OUTPUT_{execution_timestamp}.ipynb",
            parameters=parameters,
        )
    except Exception as e:
        current_run.log_info(f"Sentinel OU groups update error {type(e)}: e")

    # current_run.log_info(f"Sentinel organisation unit groups file updated for year {parameters['ANNEE_A_ANALYSER']}.")

    return True


if __name__ == "__main__":
    sentinel_update_extract()
