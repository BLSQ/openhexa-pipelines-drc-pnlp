import os

import papermill as pm

from datetime import date, datetime
from openhexa.sdk import current_run, parameter, pipeline, workspace


@pipeline("drc-pnlp-malaria-bulletin", name="DRC PNLP malaria bulletin")
@parameter(
    "week_to_use", 
    name="week",
    type=int,
    default=5,  
    required=True
)
@parameter(
    "year_to_use", 
    name="year",
    type=int,
    default=2024, 
    required=True
)

def pnlp_malaria_bulletin(week_to_use, year_to_use):
    """
    In this pipeline we call a notebook launcher that executes the report generation
    
    """
    # Setup paths
    NB_LAUNCHER_VERSION = "bulletin_epidemio_mapepi_launcher_v1"
    INPUT_NB_FOLDER = f"{workspace.files_path}/pipelines/pnlp_malaria_bulletin/bulletin_code/"
    OUTPUT_NB_FOLDER = f'{workspace.files_path}/pipelines/pnlp_malaria_bulletin/bulletin_epidemiologique/nb_outputs/'

    parameters = {
        'week_to_use': week_to_use,
        'year_to_use': year_to_use,  
    }

    # Run task
    run_papermill_script(NB_LAUNCHER_VERSION, INPUT_NB_FOLDER, OUTPUT_NB_FOLDER, parameters) 


@pnlp_malaria_bulletin.task
def run_papermill_script(nb_version, in_nb_folder, out_nb_folder, params):
    
    current_run.log_info(f"Running report for: {params['week_to_use']} and {params['year_to_use']}")
    
    in_nb_path = f"{in_nb_folder}{nb_version}.ipynb".replace('/',os.sep)
        
    current_run.log_info(f"Input launcher: {in_nb_path}")

    # out_nb_fname = os.path.basename(in_nb_dir.replace('.ipynb', ''))
    execution_timestamp = datetime.utcnow().strftime("%Y-%m-%d_%H_%M_%S")   
    out_nb_fname = f"{nb_version}_OUTPUT_{execution_timestamp}.ipynb" 
    out_nb_path = f"{out_nb_folder}{out_nb_fname}".replace('/',os.sep)

    current_run.log_info(f"Output launcher: {out_nb_path}")

    pm.execute_notebook(input_path = in_nb_path,
                        output_path = out_nb_path,
                        parameters = params) 

 

if __name__ == "__main__":
    pnlp_malaria_bulletin()