from datetime import date, datetime
from pathlib import Path

import pandas as pd
import papermill as pm
import polars as pl
from openhexa.sdk import current_run, parameter, pipeline, workspace
from openhexa.toolbox.dhis2 import DHIS2
from openhexa.toolbox.dhis2.dataframe import InvalidParameterError, get_organisation_unit_levels
from utils import get_file_from_dataset, get_matching_filenames_from_dataset

# NOTE: The idea of this pipeline is to run closely after the data has been extracted and shared via dataset from:
# Workspace: DRC DSNIS
# Pipeline: DHIS2 SNIS extract (dhis2-snis-extract)

# ticket:
# - https://bluesquare.atlassian.net/browse/PATHEOC-404


@pipeline("drc-pnlp-tdb", timeout=14400)  # (4 * 60 * 60))
@parameter(
    "get_year",
    name="Year",
    help="Year for which to extract and process data",
    type=int,
    default=2026,
    required=True,
)
@parameter(
    "get_run_notebooks",
    name="Process data",
    help="Whether or not to push the processed results to the dashboard DB",
    type=bool,
    default=False,
    required=True,
)
def pnlp_extract_process(get_year: int, get_run_notebooks: bool):
    """Main pipeline code."""
    # setup variables
    pipeline_path = Path(workspace.files_path) / "pnlp-tdb-pipeline"
    intput_nb = "LAUNCHER.ipynb"

    # extract data from DHIS
    extract_dhis_data(pipeline_path=pipeline_path, input_data_path=pipeline_path / "data", year=get_year)

    # run processing code in notebook
    if get_run_notebooks:
        run_papermill_script(
            in_nb=pipeline_path / intput_nb,
            out_nb_dir=pipeline_path / "papermill-outputs",
            parameters={"ANNEE": get_year, "UPLOAD": True},
        )


def extract_dhis_data(pipeline_path: Path, input_data_path: Path, year: int) -> None:
    """Extracts DHIS2 data for the specified year and processes it."""
    current_run.log_info("Connecting to DHIS2 instance and extracting metadata")
    # Connect and get DHIS2 metadata
    dhis2_client = DHIS2(connection=workspace.dhis2_connection("drc-snis"), cache_dir=None)
    org_units = get_organisation_units(dhis2=dhis2_client)
    org_units_lvl5 = org_units.filter(pl.col("level") == 5).to_pandas()  # fosa levels

    refresh_snis_extracts_from_dataset(pipeline_path=pipeline_path, dataset_id="snis-extracts")

    extract_periods_routine = get_quarters_until_now(year)
    for period in extract_periods_routine:
        retrieve_routine_data(
            pipeline_path=pipeline_path,
            input_data_path=input_data_path / "snis_extracts",
            period=period,
            dhis2_client=dhis2_client,
            org_units=org_units_lvl5,
        )

    extract_periods = get_dhis_month_period(year, routine=False)
    retrieve_acm_data(
        pipeline_path=pipeline_path,
        input_data_path=input_data_path / "snis_extracts",
        period=extract_periods[0],
        dhis2_client=dhis2_client,
        org_units=org_units_lvl5,
    )

    retrieve_reporting_data(
        pipeline_path=pipeline_path,
        input_data_path=input_data_path / "snis_extracts",
        period=extract_periods[0],
        org_units=org_units_lvl5,
    )


def refresh_snis_extracts_from_dataset(pipeline_path: Path, dataset_id: str) -> None:
    """Refreshes the SNIS extracts from the specified dataset."""
    snis_extracts_path = pipeline_path / "data"

    # Load pyramid
    current_run.log_info(f"Downloading SNIS pyramid from dataset {dataset_id} file: snis_pyramid.parquet")
    snis_pyramid = get_file_from_dataset(dataset_id, "snis_pyramid.parquet")
    pyramid_path = pipeline_path / "data" / "snis_pyramid"
    pyramid_path.mkdir(parents=True, exist_ok=True)
    snis_pyramid.to_parquet(pyramid_path / "snis_pyramid.parquet")

    # load population
    try:
        pop_filenames = get_matching_filenames_from_dataset(dataset_id=dataset_id, pattern="snis_population_*.parquet")
    except Exception as e:
        current_run.log_warning(f"Error while fetching SNIS population files from dataset {dataset_id}: {e}")
        return

    pop_path = pipeline_path / "data" / "snis_population"
    pop_path.mkdir(parents=True, exist_ok=True)

    for pop_filename in pop_filenames:
        current_run.log_info(f"Downloading SNIS population data from dataset {dataset_id} file: {pop_filename}")
        population = get_file_from_dataset(dataset_id, pop_filename)
        population.to_parquet(pop_path / pop_filename)

    # Load extracts
    try:
        analytics_filenames = get_matching_filenames_from_dataset(dataset_id=dataset_id, pattern="snis_data_*.parquet")
    except Exception as e:
        current_run.log_warning(f"Error while fetching SNIS extract files from dataset {dataset_id}: {e}")
        return

    extracts_path = snis_extracts_path / "snis_extracts"
    extracts_path.mkdir(parents=True, exist_ok=True)

    for filename in analytics_filenames:
        current_run.log_info(f"Downloading SNIS extract from dataset {dataset_id} file: {filename}")
        extract = get_file_from_dataset(dataset_id, filename)
        extract.to_parquet(extracts_path / filename)


def run_papermill_script(in_nb: Path, out_nb_dir: Path, parameters: dict) -> None:
    """Runs the specified notebook with papermill, passing the given parameters."""
    current_run.log_info(f"Running code in {in_nb}")
    execution_timestamp = datetime.utcnow().strftime("%Y-%m-%d_%H%M%S")
    out_nb = out_nb_dir / f"{in_nb.stem}_OUTPUT_{execution_timestamp}.ipynb"

    pm.execute_notebook(in_nb, out_nb, parameters)


def retrieve_routine_data(
    pipeline_path: Path, input_data_path: Path, period: list, dhis2_client: DHIS2, org_units: pd.DataFrame
):
    """Retrieves routine data from the DHIS2 extracts."""
    metadata_file_path = pipeline_path / "data" / "metadata" / "data_elements_for_routine_extract.csv"

    # Get the list of monitored data elements (snis config file list?)
    monitored_des = pd.read_csv(metadata_file_path).dx_uid.to_list()

    current_run.log_info(f"Extracting routine data for : {period}")
    loaded_extracts = []
    for p in period:
        extract_fname = input_data_path / f"snis_data_{p}.parquet"
        if extract_fname.exists():
            current_run.log_info(f"Loading extract file: {extract_fname.name}")
            extract = pd.read_parquet(extract_fname)
            # check if all the data elements are present
            not_found = set(monitored_des) - set(extract["dx"].unique().tolist())
            if not_found:
                current_run.log_warning(f"Data elements not found in period {p} : {not_found}.")
            loaded_extracts.append(extract)
    if not loaded_extracts:
        current_run.log_info(f"No extracts found for the period: {period}.")
        return
    raw_routine_data = pd.concat(loaded_extracts, ignore_index=True)

    # filter data elements
    raw_routine_data = raw_routine_data[raw_routine_data["dx"].isin(monitored_des)]

    # Theres an additional step here to handle the mappings for COC before and after Jan-2025
    if int(period[0]) >= 202501:
        # For 2025 onwards, we filter out the old COC and map the new DEs
        current_run.log_info("Mapping COC for 2025.")
        raw_routine_data = map_rountine_coc_2025(df=raw_routine_data)
    else:
        # For 2024 and before, we filter out the new COC
        current_run.log_info("Filter COC added in 2025 for data from 2024.")
        raw_routine_data = raw_routine_data[
            ~(raw_routine_data.category_option_combo.isin(["xxMINnPGqUg", "xCV9NGB897u", "r5lWfJh2t2l"]))
        ]

    # add metadata to the dataframe
    current_run.log_info("Adding metadata to the dataframe")
    df = dhis2_client.meta.add_dx_name_column(dataframe=raw_routine_data, dx_id_column="dx")  # dx_uid
    df = dhis2_client.meta.add_coc_name_column(dataframe=df, coc_column="category_option_combo")
    df = df.merge(
        org_units,
        left_on="org_unit",
        right_on="level_5_id",
        how="left",
        suffixes=("", ""),
    )

    # Column renaming/selection and output directory
    column_names = {
        "dx": "dx",
        "category_option_combo": "co",
        "org_unit": "ou",
        "period": "pe",
        "value": "value",
        "level_5_name": "ou_name",
        "level_1_id": "parent_level_1_id",
        "level_1_name": "parent_level_1_name",
        "level_2_id": "parent_level_2_id",
        "level_2_name": "parent_level_2_name",
        "level_3_id": "parent_level_3_id",
        "level_3_name": "parent_level_3_name",
        "level_4_id": "parent_level_4_id",
        "level_4_name": "parent_level_4_name",
    }
    col_selection = list(column_names.keys()) + ["dx_name", "co_name"]
    df = df[col_selection]
    df = df.rename(columns=column_names)

    # Output file
    output_subdir_name = f"{month_to_quarter(int(period[0]))}"
    year = int(period[0]) // 100
    output_dir = pipeline_path / "data" / "raw" / "routine" / f"{year}" / f"{output_subdir_name}"
    output_dir.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_dir / "analytics.csv")


def retrieve_acm_data(
    pipeline_path: Path,
    input_data_path: Path,
    period: list,
    dhis2_client: DHIS2,
    org_units: pd.DataFrame,
):
    """Retrieves ACM data from the DHIS2 extracts. The ACM indicator is the one with dx 'fvlFcxuGRng'."""
    acm_indicator_id = ["fvlFcxuGRng"]

    current_run.log_info(f"Retrieving ACM data for : {period}")
    loaded_extracts = []
    for p in period:
        extract_fname = input_data_path / f"snis_data_{p}.parquet"
        if extract_fname.exists():
            current_run.log_info(f"Loading extract file: {extract_fname.name}")
            extract = pd.read_parquet(extract_fname)
            # check if all the data elements are present
            not_found = set(acm_indicator_id) - set(extract["dx"].unique().tolist())
            if not_found:
                current_run.log_warning(f"ACM not found in period {p} : {acm_indicator_id}.")
            loaded_extracts.append(extract)

    if not loaded_extracts:
        current_run.log_info(f"No extracts found for the period: {period}.")
        return
    raw_routine_data = pd.concat(loaded_extracts, ignore_index=True)

    # filter data elements
    raw_routine_data = raw_routine_data[raw_routine_data["dx"].isin(acm_indicator_id)]

    # add metadata to the dataframe
    current_run.log_info("Adding metadata to the dataframe")
    df = dhis2_client.meta.add_dx_name_column(dataframe=raw_routine_data, dx_id_column="dx")
    df = df.merge(
        org_units,
        left_on="org_unit",
        right_on="level_5_id",
        how="left",
    )

    # Column renaming/selection and output directory
    column_names = {
        "dx": "dx",
        "org_unit": "ou",
        "period": "pe",
        "value": "value",
        "level_5_name": "ou_name",
        "level_1_id": "parent_level_1_id",
        "level_1_name": "parent_level_1_name",
        "level_2_id": "parent_level_2_id",
        "level_2_name": "parent_level_2_name",
        "level_3_id": "parent_level_3_id",
        "level_3_name": "parent_level_3_name",
        "level_4_id": "parent_level_4_id",
        "level_4_name": "parent_level_4_name",
    }
    col_selection = list(column_names.keys()) + ["dx_name"]
    df = df[col_selection]
    df = df.rename(columns=column_names)

    # output dataframe
    year = int(period[0]) // 100
    output_dir = pipeline_path / "data" / "raw" / "all-cause-mortality" / f"{year}"
    output_dir.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_dir / "analytics.csv")


def retrieve_reporting_data(
    pipeline_path: Path,
    input_data_path: Path,
    period: list,
    org_units: pd.DataFrame,
):
    """Retrieves reporting rates data from the DHIS2 extracts."""
    current_run.log_info(f"Retrieving reporting data for : {period}")

    year = int(period[0]) // 100

    # handle the different DE as from Jan-2025
    reporting_mappings = {"ahT7ysZZ913": "pMbC0FJPkcm", "E4BX1ea2iDJ": "maDtHIFrSHx", "CfCNNEwbTSH": "OeWrFwkFMvf"}
    if year >= 2025:
        # ahT7ysZZ913, E4BX1ea2iDJ, CfCNNEwbTSH
        reporting_datasets = list(reporting_mappings.keys())
    else:
        # pMbC0FJPkcm, maDtHIFrSHx, OeWrFwkFMvf
        reporting_datasets = list(reporting_mappings.values())

    loaded_extracts = []
    for p in period:
        extract_fname = input_data_path / f"snis_data_{p}.parquet"
        if extract_fname.exists():
            current_run.log_info(f"Loading extract file: {extract_fname.name}")
            extract = pd.read_parquet(extract_fname)
            # check if all the data elements are present
            not_found = set(reporting_datasets) - set(extract["dx"].unique().tolist())
            if not_found:
                current_run.log_warning(f"Reporting rates not found in period {p} : {not_found}.")
            loaded_extracts.append(extract)

    if not loaded_extracts:
        current_run.log_info(f"No extracts found for the period: {period}.")
        return
    raw_reporting_data = pd.concat(loaded_extracts, ignore_index=True)

    # filter data elements
    raw_reporting_data = raw_reporting_data[raw_reporting_data["dx"].isin(reporting_datasets)]
    raw_reporting_data["dx"] = raw_reporting_data["dx"].replace(reporting_mappings)
    if raw_reporting_data.shape[0] == 0:
        current_run.log_info(f"No reporting rates data found for the period: {period}.")
        return

    # add metadata to the dataframe
    current_run.log_info("Adding metadata to the dataframe")
    df = raw_reporting_data.merge(
        org_units,
        left_on="org_unit",
        right_on="level_5_id",
        how="left",
    )
    # Column renaming/selection and output directory
    column_names = {
        "dx": "ds",  # dx_uid: ds (dataset)
        "org_unit": "ou",
        "period": "pe",
        "value": "value",
        "rate_metric": "metric",
        "level_1_id": "parent_level_1_id",
        "level_1_name": "parent_level_1_name",
        "level_2_id": "parent_level_2_id",
        "level_2_name": "parent_level_2_name",
        "level_3_id": "parent_level_3_id",
        "level_3_name": "parent_level_3_name",
        "level_4_id": "parent_level_4_id",
        "level_4_name": "parent_level_4_name",
        "level_5_id": "parent_level_5_id",
        "level_5_name": "parent_level_5_name",
    }
    df = df[list(column_names.keys())]
    df = df.rename(columns=column_names)
    df["ou_name"] = df["parent_level_5_name"]

    # output dataframe
    output_dir = pipeline_path / "data" / "raw" / "reporting-rates" / f"{year}"
    output_dir.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_dir / "analytics.csv")


def map_rountine_coc_2025(df: pd.DataFrame) -> pd.DataFrame:
    """Maps the COC for the routine data elements for 2025 onwards, where some of the data elements have changed COC.

    Returns:
    - df (pd.DataFrame) : the dataframe with the COC mapped for the relevant data elements,
    and aggregated at dx_uid, period, org_unit, category_option_combo, attribute_option_combo level.
    """
    coc_mappings = {
        "xCV9NGB897u": "yI0WfOFcgSc",  # < 2 ans
        "xxMINnPGqUg": "yI0WfOFcgSc",  # < 5 ans
        "r5lWfJh2t2l": "brxxCYkQqcd",  # >= 5 ans
    }
    changed_des = [
        "aZwnLALknnj",
        "AxJhIi7tUam",
        "CGZbvJchfjk",
        "aK0QXqm8Zxn",
        "rfeqp2kdOGi",
        "nRm30I4w9En",
        "SpmQSLRPMl4",
        "CIzQAR8IWH1",
        "wfmDVt6RVm2",
        "e6qMP9fVtG9",
        "sRbXNrdKvyl",
    ]
    # select only the IDS to map
    df_des = df[df["dx"].isin(changed_des)].copy()
    df_rest = df[~(df["dx"].isin(changed_des))].copy()

    # Filter and map the COC for the changed data elements
    df_filtered = df_des[df_des["category_option_combo"].isin(list(coc_mappings.keys()))].copy()
    df_filtered["category_option_combo"] = df_filtered["category_option_combo"].replace(coc_mappings)

    # group by dx and category_option_combo, summing the values
    df_agg = (
        df_filtered.assign(value_numeric=lambda df: pd.to_numeric(df_filtered["value"], errors="coerce"))
        .groupby(["dx", "period", "org_unit", "category_option_combo", "attribute_option_combo"])
        .agg({"value_numeric": "sum"})
        .rename(columns={"value_numeric": "value"})
        .reset_index()
    )
    df_agg["value"] = df_agg["value"].astype(str)

    return pd.concat([df_rest, df_agg], ignore_index=True).sort_values(by=["period"]).reset_index(drop=True)


def get_dhis_month_period(year: str, routine: bool = False) -> list[list[str]]:
    """Returns a list of lists of month periods in DHIS2 format (e.g. 202401, 202402, etc.) for the given year.

    Returns:
    - list of lists of month periods in DHIS2 format (e.g. 202401, 202402, etc.) for the given year.
    If routine is True, it returns the periods grouped by quarter, otherwise it returns a single list with
    all the months periods.
    """
    # LEGACY FUNCTION
    current_date = date.today()
    # "Hacky solution" to run the pipeline for a specific period
    # current_date = datetime.strptime('2024-04-01', '%Y-%m-%d').date() # Run for quarter Q1
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
            month_quarter_pairs = [(1, 3), (4, 6), (7, 9), (10, 12)]
            return list(map(lambda x: dhis_period_range(year, x[0], x[1]), month_quarter_pairs))

    month_list = dhis_period_range(year, period_start_month, period_end_month)

    # first month of quarter (routine), extract previous quarter as well
    if routine and period_end_month in [4, 7, 10]:
        return [
            dhis_period_range(year, period_start_month - 3, period_end_month - 1),
            month_list,
        ]

    return [month_list]


def get_quarters_until_now(year: int) -> list[list[str]]:
    """Returns a list of lists of month periods in DHIS2 format.

    Example (e.g. 202401, 202402, etc.) for the given year, grouped by quarter, until the last complete month.

    Returns:
    - list of lists of month periods in DHIS2 format (e.g. 202401, 202402, etc.) for the given year, grouped by quarter.
    """
    now = datetime.now()
    months = []

    if year < now.year:
        # Full year: all 12 months
        months = [f"{year}{month:02}" for month in range(1, 13)]
    elif year == now.year:
        # Only until last complete month
        last_month = now.month - 1
        if last_month < 1:
            return []  # No full months yet in the year
        months = [f"{year}{month:02}" for month in range(1, last_month + 1)]
    else:
        # Future year
        return []

    # Group by quarters (chunks of 3 months)
    return [months[i : i + 3] for i in range(0, len(months), 3)]


def dhis_period_range(year: int, start: int, end: int) -> list[str]:
    """Returns a list of month periods in DHIS2 format (e.g. 202401, 202402, etc.) for the given year and month range.

    Returns:
    - list of month periods in DHIS2 format (e.g. 202401, 202402, etc.) for the given year and month range.
    """
    return [f"{year}{str(x).zfill(2)}" for x in range(start, end + 1)]


def first_month_of_quarter(month: int) -> int:
    """Returns the number first month of the quarter for the number month passed (1 - 12).

    Returns:
    - int : the number of the first month of the quarter (1, 4, 7, 10) corresponding to the given month.
    """
    if month not in range(1, 13):
        raise ValueError("Not a valid month number (1-12)")

    return (month - 1) // 3 * 3 + 1


def month_to_quarter(num: int) -> str:
    """Returns the quarter corresponding to the given month in DHIS format (e.g. 201808).

    Returns:
     -(str) the quarter corresponding to the given month (e.g. Q3)
    """
    # y = num // 100
    m = num % 100
    return "Q" + str((m - 1) // 3 + 1)


def get_organisation_units(
    dhis2: DHIS2, max_level: int | None = None, filters: list[str] | None = None
) -> pl.DataFrame:
    """Extract organisation units metadata.

    Parameters
    ----------
    dhis2 : DHIS2
        DHIS2 instance.
    max_level : int, optional
        Maximum level of organisation units to extract. If None, all levels are extracted.
    filters : list[str], optional
        DHIS2 query filter expressions.

    Returns
    -------
    pl.DataFrame
        Dataframe containing organisation units metadata with the following columns: id, name,
        level, level_{level}_id, level_{level}_name, geometry.

    Raises
    ------
    InvalidParameter
        If max_level is greater than the maximum level of the organisation units.
    """
    levels = get_organisation_unit_levels(dhis2)
    if max_level:
        if max_level > levels["level"].max():
            msg = f"max_level cannot be greater than {levels['level'].max()}"
            current_run.log_error(msg)
            raise InvalidParameterError(msg)
        level_filter = f"level:le:{max_level}"
        if filters:
            filters = [*filters, max_level]
        else:
            filters = [level_filter]

    # meta = dhis2.meta.organisation_units(fields="id,name,level,path,openingDate,closedDate,geometry", filters=filters)
    meta = dhis2.meta.organisation_units()

    schema = {
        "id": str,
        "name": str,
        "level": int,
        "path": str,
        "openingDate": str,
        "geometry": str,
    }
    df = pl.DataFrame(data=meta, schema=schema)

    for row in levels.iter_rows(named=True):
        lvl = row["level"]
        if max_level:
            if lvl > max_level:
                continue

        df = df.with_columns(
            pl.col("path").str.split("/").list.slice(1).list.get(lvl - 1, null_on_oob=True).alias(f"level_{lvl}_id")
        )

        df = df.join(
            other=df.select("id", pl.col("name").alias(f"level_{lvl}_name")),
            left_on=f"level_{lvl}_id",
            right_on="id",
            how="left",
        )

    df = df.select(
        "id",
        "name",
        "level",
        pl.col("openingDate").str.to_datetime("%Y-%m-%dT%H:%M:%S.%3f").alias("opening_date"),
        *[col for col in df.columns if col.startswith("level_")],
        "geometry",
    )

    return df.sort(by=["level", "name"], descending=False)


if __name__ == "__main__":
    pnlp_extract_process()
