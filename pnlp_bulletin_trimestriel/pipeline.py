import os
from pathlib import Path
import papermill as pm
from datetime import date, datetime
from openhexa.sdk import current_run, parameter, pipeline, workspace

@pipeline(name="drc_pnlp_bulletin_trimestriel")
@parameter(
    "get_ref_year",
    name="Year",
    help="",
    type=int,
    default=2025,
    required=True,
)
@parameter(
    "get_ref_quarter",
    name="Quarter number",
    help="",
    type=int,
    choices=[1, 2, 3, 4],
    default=1,
    required=True,
)
def pnlp_quarterly_bulletin(
    get_ref_year, 
    get_ref_quarter
):
    
    # paths
    root_path = workspace.files_path
    project_path = Path(root_path, "pipelines", "pnlp_bulletin_trimestriel")
    code_path = Path(project_path, "code")
    launcher_nb = f"LAUNCHER_PNLP_bulletin_trimestriel.ipynb"
    papermill_outputs_path = Path(project_path, "papermill_outputs")
    output_path = Path(project_path, "outputs")

    os.chdir(code_path)

    make_report(
        path_to_in_nb=code_path,
        in_nb_name=launcher_nb, 
        out_nb_dir=papermill_outputs_path, 
        doc_path=output_path,
        doc_prefix="PNLP_bulletin_trimestriel",
        doc_yr=get_ref_year,
        doc_qtr=get_ref_quarter)

@pnlp_quarterly_bulletin.task
def make_report(path_to_in_nb, in_nb_name, out_nb_dir, doc_path, doc_prefix:str, doc_yr:int, doc_qtr:int, *args, **kwargs):

    params = {
        'ref_year': doc_yr, 
        'ref_quarter': doc_qtr
    }
    current_run.log_info(f"Running notebok {in_nb_name} from {path_to_in_nb} and saving outputs to {out_nb_dir}.")
    
    in_nb = Path(path_to_in_nb, in_nb_name)
    in_stem = in_nb.stem
    out_nb = Path(out_nb_dir, f"output_{in_stem}.ipynb")

    pm.execute_notebook(in_nb, out_nb, params)

    output_file_name = f"{doc_prefix}_{doc_yr}_T{doc_qtr}.docx"

    output_file_path = Path(doc_path, output_file_name)

    current_run.log_info(f"Output document saved in {output_file_path}")

    current_run.add_file_output(str(output_file_path))

    return

if __name__ == "__main__":
    pnlp_quarterly_bulletin()