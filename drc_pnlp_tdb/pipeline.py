import json
import typing
import tempfile
import io
import os

import pandas as pd
import geopandas as gpd
import papermill as pm

from datetime import date, datetime
from shapely.geometry import shape
from itertools import product

from openhexa.sdk import current_run, parameter, pipeline, workspace

from openhexa.toolbox.dhis2 import DHIS2


@pipeline("drc-pnlp-tdb", name="DRC PNLP TdB")
@parameter(
    "get_year",
    name="Year",
    help="Year for which to extract and process data",
    type=int,
    default=2023,
    required=False,
)
@parameter(
    "get_download",
    name="Download new data?",
    help="Download new data or reprocess existing extracts",
    type=bool,
    default=True,
    required=False,
)
@parameter(
    "get_upload",
    name="Upload?",
    help="Whether or not to push the processed results to the dashboard DB",
    type=bool,
    default=False,
    required=False,
)
def pnlp_extract_process(get_year, get_upload, get_download, *args, **kwargs):
    """
    """

    # setup variables
    PROJ_ROOT = f'{workspace.files_path}/pnlp-tdb-pipeline/'
    DATA_DIR = f'{PROJ_ROOT}data/'
    RAW_DATA_DIR = f'{DATA_DIR}raw/'

    INPUT_NB = f'{PROJ_ROOT}LAUNCHER.ipynb'
    OUTPUT_NB_DIR = f'{PROJ_ROOT}papermill-outputs/'

    if get_download:
        # extract data from DHIS
        routine_download_complete = extract_dhis_data(
            RAW_DATA_DIR, 
            get_year, 
            'routine'
        )
        
        all_cause_mortality_download_complete = extract_dhis_data(
            RAW_DATA_DIR, 
            get_year, 
            'acm',
            routine_download_complete
        )

        reporting_download_complete = extract_dhis_data(
            RAW_DATA_DIR, 
            get_year, 
            'reporting-rates',
            all_cause_mortality_download_complete
        )

    # run processing code in notebook
    params = {
        'ANNEES': [get_year], 
        'UPLOAD': get_upload
    }

    # if get_download:
    #     ppml = run_papermill_script(
    #         INPUT_NB, 
    #         OUTPUT_NB_DIR, 
    #         params, 
    #         all_cause_mortality_download_complete,
    #         reporting_download_complete
    # )
    # else:
    #     ppml = run_papermill_script(INPUT_NB, OUTPUT_NB_DIR, params)

@pnlp_extract_process.task
def extract_dhis_data(output_dir, year, mode, *args, **kwargs):
    
    is_routine = (mode == 'routine')

    extract_periods = get_dhis_month_period(
        year, 
        routine = is_routine
    )

    for period in extract_periods:
        current_run.log_info(f"Extracting analytics data for {mode} ({period})")
        dhis2_download_analytics(
            output_dir, 
            period, 
            mode
        )

    return 0       

@pnlp_extract_process.task
def run_papermill_script(in_nb, out_nb_dir, parameters, *args, **kwargs):
    current_run.log_info(f"Running code in {in_nb}")

    execution_timestamp = datetime.utcnow().strftime("%Y-%m-%d_%H:%M:%S")
    out_nb = f"{out_nb_dir}{os.path.basename(in_nb)}_OUTPUT_{execution_timestamp}.ipynb"

    pm.execute_notebook(in_nb, out_nb, parameters)
    return        

#### helper functions ####
def dhis2_download_analytics(output_dir, extract_period, mode, *args, **kwargs):
    """
    Extracts DHIS2 data for dashboard.

    Mode: routine (malaria data elements) or all cause mortality (indicator)
    extract_period: list of periods to extract in DHIS period format (ex 202204)

    """
    
    # establish DHIS2 connection
    connection = workspace.dhis2_connection('drc-snis')
    dhis2 = DHIS2(connection=connection, cache_dir = None) # f'{workspace.files_path}/temp/')

    # define data dimension 
    routine = False
    reporting_rates = False

    if mode == 'routine':
        routine = True

        # data elements for extract ex: ["aZwnLALknnj", "D3h3Qvl0332"]
        DATA_DIR = f'{workspace.files_path}/pnlp-tdb-pipeline/data/' # Todo: make global
        metadata_file_path = f'{DATA_DIR}metadata/data_elements_for_routine_extract.csv'
        monitored_des = pd.read_csv(metadata_file_path).dx_uid.to_list() 
        
        output_directory_name = 'routine'
    elif mode == 'reporting-rates':
        reporting_rates = True

        METRICS = {
            "REPORTING_RATE": float,
            "REPORTING_RATE_ON_TIME": float,
            "ACTUAL_REPORTS": int,
            "ACTUAL_REPORTS_ON_TIME": int,
            "EXPECTED_REPORTS": int,
        }

        reporting_datasets = ['pMbC0FJPkcm', 'maDtHIFrSHx', 'OeWrFwkFMvf']
        output_directory_name = 'reporting-rates'
        reporting_des = [f"{ds}.{metric}" for ds, metric in product(reporting_datasets, METRICS)]
    else:
        acm_indicator_id = ["fvlFcxuGRng"]
        output_directory_name = 'all-cause-mortality'

    # run extraction requests for analytics data

    # testing limits
    dhis2.analytics.MAX_DX = 25
    dhis2.analytics.MAX_OU = 25

    if mode == 'routine':
        raw_data = dhis2.analytics.get(
            data_elements = monitored_des,
            periods = extract_period,
            org_unit_levels = [5]
        )
    elif mode == 'reporting-rates':
        raw_data = dhis2.analytics.get(
            data_elements = reporting_des,
            periods = extract_period,
            org_unit_levels = [5],
            include_cocs = False
        )
    else:
        raw_data = dhis2.analytics.get(
            indicators = acm_indicator_id,
            periods = extract_period,
            org_unit_levels = [5],
            include_cocs = False
        )

    df = pd.DataFrame(raw_data)

    # extract metadata
    current_run.log_info("Extracting + merging instance metadata")
    df = dhis2.meta.add_org_unit_name_column(dataframe=df)
    df = dhis2.meta.add_org_unit_parent_columns(dataframe=df)

    if reporting_rates:
        # default dx values are in the format "<dataset_uid>.<reporting_rate_metric>"
        # we want to move dataset uid and metric into two new columns
        df[['ds', 'metric']] = df['dx'].str.split('.', expand=True)

        # we don't need the dx column anymore
        df.drop(columns=['dx'], inplace=True)

    else:
        # TODO: add dataset names to reporting rate 
        df = dhis2.meta.add_dx_name_column(dataframe=df)

    # add COCs to routine data elements
    if routine:
        df = dhis2.meta.add_coc_name_column(dataframe=df)


    # define output path and save file
    current_run.log_info("Creating analytics.csv")

    if routine:
        output_subdir_name = f'/{month_to_quarter(int(extract_period[0]))}/'
    else:
        output_subdir_name = ''

    year = int(extract_period[0]) // 100

    output_dir = f'{output_dir}/{output_directory_name}/{year}{output_subdir_name}'
    os.makedirs(output_dir, exist_ok=True)

    out_path = f'{output_dir}/analytics.csv'
    df.to_csv(out_path)

    return out_path

def get_dhis_month_period(year, routine=False):
    """ 
    Returns a list of lists of DHIS2 months (YYYYMM -- 202003) based on the year
    specified to the function. For the current year, all months up to N-1 are 
    included in the list. For previous years, the list contains all months.

    Routine data is broken down by quarters to reduce the total size of the extract.
    """

    current_date = date.today()

    period_start_month = 1
    period_end_month = 12
    
    # current year : up to last month
    # previous years: all months 
    if year == current_date.year:
        period_end_month = current_date.month - 1

        # routine data: only extract back to start of quarter
        if routine:
            period_start_month = first_month_of_quarter(period_end_month)
    else:
        if routine:
            raise NotImplementedError('Extract of routine data for historical periods not yet developed')
        
    month_list = dhis_period_range(year, period_start_month, period_end_month)

    # first month of quarter (routine), extract previous quarter as well
    if routine and period_start_month in [4, 7, 10]:
        return month_list = [ 
            dhis_period_range(
                year, 
                period_start_month - 3, 
                period_end_month - 1
            ),
            month_list
        ]
    
    return [month_list]

def dhis_period_range(year, start, end):
    r = [
        f'{year}{str(x).zfill(2)}' for x in 
        range(start, end + 1)
    ]

    return r

def first_month_of_quarter(month):
    """ 
    Returns the number first month of the quarter 
    for the number month passed (1 - 12)
    """

    if month not in range(1, 13):
        raise ValueError("Not a valid month number (1-12)")

    return (month - 1) // 3 * 3 + 1

def month_to_quarter(num):
    """
    Input:
    - num (int) : a given month in DHIS format (e.g. 201808)
    Returns: (str) the quarter corresponding to the given month (e.g. Q3)
    """
    y = num // 100
    m = num % 100
    return "Q" + str((m - 1) // 3 + 1)

if __name__ == "__main__":
    pnlp_extract_process.run()
