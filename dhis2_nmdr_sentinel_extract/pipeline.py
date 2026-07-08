import tempfile
from pathlib import Path

import polars as pl
from d2d_development.extract import DHIS2Extractor
from openhexa.sdk import current_run, parameter, pipeline, workspace
from openhexa.toolbox.dhis2 import DHIS2
from openhexa.toolbox.dhis2.dataframe import get_organisation_unit_groups, get_organisation_units
from utils import (
    connect_to_dhis2,
    get_extract_periods,
    load_configuration,
    resolve_dates_and_validate,
    save_to_parquet,
)

# Sentinelle org unit groups: maps group ID -> display name
SENTINELLE_OU_GROUPS = {
    "qTZ3L6pLMTi": "CS Site Sentinelle",
    "wldxDI2Ey5c": "HGR Site Sentinelle",
}


@pipeline("dhis2_nmdr_sentinel_extract", timeout=21600)  # 6 hours
@parameter(
    code="start_date",
    name="Start date (format: YYYYMM)",
    default="202605",
    type=str,
    required=False,
    help=(
        "Start date for data extraction in YYYYMM format. "
        "If not set, it will default to current date minus NUMBER_MONTHS_WINDOW."
    ),
)
@parameter(
    code="end_date",
    name="End date (format: YYYYMM)",
    default="202605",
    type=str,
    required=False,
    help=("End date for data extraction in YYYYMM format. If not set, it will default to current date minus 1."),
)
@parameter(
    code="run_extract_data",
    name="Extract data",
    type=bool,
    default=True,
    help="Extract data elements from NMDR.",
)
def dhis2_nmdr_sentinel_extract(start_date: str, end_date: str, run_extract_data: bool):
    """Orchestrates the NMDR Sentinel monthly extraction and compilation into parquet files.

    Args:
        start_date (str): Start date for data extraction in YYYYMM format.
        end_date (str): End date for data extraction in YYYYMM format.
        run_extract_data (bool): Whether to run the DHIS2 data extraction step.
    """
    pipelines_root = Path(workspace.files_path) / "pipelines"
    pipeline_path = pipelines_root / "dhis2_nmdr_sentinel_extract"

    # Load configuration and connect to DHIS2
    config = load_configuration(pipeline_path / "config" / "extract_config.json")
    dhis2_client = connect_to_dhis2(connection_str=config["SETTINGS"]["DHIS2_CONNECTION"])

    # get dates and validate
    start, end = resolve_dates_and_validate(start_date, end_date, config)
    extract_periods = get_extract_periods(start, end)

    # Retrieve org units belonging to the two sentinelle groups
    sentinelle_ou_list, sentinelle_ou_groups = get_sentinelle_org_units(dhis2_client)
    current_run.log_info(f"Found {len(sentinelle_ou_list)} sentinelle facilities across {len(SENTINELLE_OU_GROUPS)} groups.")

    try:
        extract_pyramid_metadata(
            pipeline_path=pipeline_path,
            dhis2_nmdr_client=dhis2_client,
            run_task=run_extract_data,
        )
        data_by_period = extract_data(
            extract_periods=extract_periods,
            config=config,
            dhis2_nmdr_client=dhis2_client,
            sentinelle_ou_list=sentinelle_ou_list,
            sentinelle_ou_groups=sentinelle_ou_groups,
            run_task=run_extract_data,
        )

        current_run.log_info("Data extracted successfully.")

    except Exception as e:
        current_run.log_error(f"An error occurred: {e}")
        raise

    try:
        compile_nmdr_extracts(
            extract_periods=extract_periods,
            data_by_period=data_by_period,
            nmdr_extracts_path=pipelines_root / "dhis2_nmdr_sentinel_extract" / "data",
            output_path=pipeline_path / "data" / "nmdr_extracts",
            config_path=pipeline_path / "config",
        )
    except Exception as e:
        current_run.log_error(f"An error while compiling data: {e}")
        raise

    ## add a new task to transform the data try function -> except rise error and log to say data has been transformed/extracted,
    # add parameter to sometimes only run extract  or only run transform, or both


def get_sentinelle_org_units(dhis2_client: DHIS2) -> tuple[list[str], dict[str, str]]:
    """Retrieves org unit IDs and group name mapping for all sentinelle facilities.

    Queries the two sentinelle org unit groups (CS and HGR) and builds:
    - a flat list of all org unit IDs belonging to either group
    - a dict mapping each org unit ID to its group display name

    Args:
        dhis2_client (DHIS2): Connected DHIS2 client.

    Returns:
        tuple[list[str], dict[str, str]]:
            - List of sentinelle org unit IDs.
            - Dict mapping org unit ID -> group display name.
    """
    try:
        ou_groups_df = get_organisation_unit_groups(dhis2_client)
    except Exception as e:
        raise Exception(f"Error retrieving organisation unit groups: {e}") from e

    sentinelle_groups = ou_groups_df.filter(pl.col("id").is_in(list(SENTINELLE_OU_GROUPS.keys())))

    ou_to_group: dict[str, str] = {}
    for row in sentinelle_groups.iter_rows(named=True):
        group_name = SENTINELLE_OU_GROUPS[row["id"]]
        for ou_id in row["organisation_units"]:
            ou_to_group[ou_id] = group_name

    return list(ou_to_group.keys()), ou_to_group


def extract_pyramid_metadata(pipeline_path: str, dhis2_nmdr_client: DHIS2, run_task: bool) -> None:
    """Extracts and saves the pyramid metadata at level 5.

    Args:
        pipeline_path (str): Root path of the pipeline used to resolve the output data folder.
        dhis2_nmdr_client (DHIS2): Connected DHIS2 client used to retrieve the pyramid.
        run_task (bool): Whether to run this extraction step.
    """
    if not run_task:
        current_run.log_info("Skipping pyramid metadata extraction as run_task is set to False.")
        return

    current_run.log_info("Retrieving NMDR DHIS2 pyramid metadata")

    try:
        # retrieve full pyramid
        org_units = get_organisation_units(dhis2_nmdr_client).drop("geometry")
        org_units = org_units.filter(pl.col("level") == 5)
        current_run.log_info(f"{len(org_units['id'].unique())} units at organisation unit level: 5")
    except Exception as e:
        raise Exception(f"Error while extracting NMDR DHIS2 Pyramid: {e}") from e

    # Save as Parquet
    pyramid_path = pipeline_path / "data" / "pyramid_metadata"
    save_to_parquet(data=org_units, filename=pyramid_path / "nmdr_pyramid_metadata.parquet")
    current_run.log_info(f"NMDR DHIS2 pyramid metadata saved: {pyramid_path / 'nmdr_pyramid_metadata.parquet'}")


def extract_data(
    extract_periods: list[str],
    config: dict,
    dhis2_nmdr_client: DHIS2,
    sentinelle_ou_list: list[str],
    sentinelle_ou_groups: dict[str, str],
    run_task: bool,
) -> dict[str, pl.DataFrame]:
    """Retrieves DHIS2 analytics data elements and reporting rates for the given periods.

    Args:
        extract_periods (list[str]): Periods to extract, in YYYYMM format.
        config (dict): Extraction configuration loaded from extract_config.json.
        dhis2_nmdr_client (DHIS2): Connected DHIS2 client used to retrieve the data.
        sentinelle_ou_list (list[str]): Sentinelle org unit IDs to extract data for.
        sentinelle_ou_groups (dict[str, str]): Mapping of org unit ID -> group display name,
            used to add the organisationUnitGroup column to extracted data.
        run_task (bool): Whether to run this extraction step.

    Returns:
        dict[str, pl.DataFrame]: Extracted data keyed by period (YYYYMM). Empty if run_task is False.
    """
    if not run_task:
        current_run.log_info("Skipping data extraction as run_task is set to False.")
        return {}

    current_run.log_info("Retrieving DHIS2 analytics data")

    fosa_list = sentinelle_ou_list
    current_run.log_info(f"Download MODE: {config['SETTINGS']['MODE']} for periods: {extract_periods}")

    # limits
    dhis2_nmdr_client.analytics.MAX_DX = 100
    dhis2_nmdr_client.analytics.MAX_ORG_UNITS = 100
    dhis2_nmdr_client.data_value_sets.MAX_DATA_ELEMENTS = 100
    dhis2_nmdr_client.data_value_sets.MAX_ORG_UNITS = 100

    data_by_period = _extract_data_elements_for_periods(
        dhis2_client=dhis2_nmdr_client,
        periods=extract_periods,
        org_unit_list=fosa_list,
        sentinelle_ou_groups=sentinelle_ou_groups,
        config=config,
    )
    current_run.log_info("Data elements extract finished.")
    return data_by_period


def _get_ou_list(pyramid_fname: Path, ou_level: int) -> list:
    """Retrieves a list of organizational unit IDs from the pyramid Parquet file based on the specified OU level.

    Args:
        pyramid_fname (Path): Path to the pyramid metadata Parquet file.
        ou_level (int): Organisation unit level to filter by.

    Returns:
        list: Organisation unit IDs corresponding to the specified OU level.
    """
    try:
        ous = pl.read_parquet(pyramid_fname)
        ou_list = ous.filter(pl.col("level") == ou_level)["id"].to_list()
    except Exception as e:
        raise Exception(f"Error loading pyramid file: {e}") from e

    current_run.log_info(f"DHIS2 org units id list {len(ou_list)} at level {ou_level}")
    return ou_list


def _extract_data_elements_for_periods(
    dhis2_client: DHIS2,
    periods: list[str],
    org_unit_list: list[str],
    sentinelle_ou_groups: dict[str, str],
    config: dict,
) -> dict[str, pl.DataFrame]:
    """Downloads data elements for each period and returns them as in-memory DataFrames.

    Args:
        dhis2_client (DHIS2): Connected DHIS2 client used to retrieve the data.
        periods (list[str]): Periods to extract, in YYYYMM format.
        org_unit_list (list[str]): Organisation unit IDs to extract data for.
        sentinelle_ou_groups (dict[str, str]): Mapping of org unit ID -> group display name,
            used to add the organisationUnitGroup column to each downloaded parquet.
        config (dict): Extraction configuration loaded from extract_config.json.

    Returns:
        dict[str, pl.DataFrame]: Extracted DataFrames keyed by period (YYYYMM).
    """
    dhis2_extractor = DHIS2Extractor(dhis2_client=dhis2_client, download_mode=config["SETTINGS"]["MODE"])
    data_by_period: dict[str, pl.DataFrame] = {}

    try:
        with tempfile.TemporaryDirectory() as tmp_dir:
            for period in periods:
                raw_data_path = dhis2_extractor.data_elements.download_period(
                    data_elements=config["DATA_ELEMENTS"]["UIDS"],
                    org_units=org_unit_list,
                    period=period,
                    output_dir=Path(tmp_dir),
                    filename=f"data_{period}.parquet",
                )
                if not raw_data_path:
                    current_run.log_info(f"No data elements data for period {period}.")
                    continue

                # Read into memory and add organisationUnitGroup column, then let temp file be cleaned up
                df = pl.read_parquet(raw_data_path)
                df = df.with_columns(
                    pl.col("org_unit")
                    .map_elements(lambda x: sentinelle_ou_groups.get(x), return_dtype=pl.String)
                    .alias("organisationUnitGroup")
                )
                data_by_period[period] = df

    except Exception as e:
        raise Exception(f"Extract data elements error : {e}") from e

    return data_by_period


def compile_nmdr_extracts(
    extract_periods: list[str],
    data_by_period: dict[str, pl.DataFrame],
    nmdr_extracts_path: Path,
    output_path: Path,
    config_path: Path,
) -> list[Path]:
    """Collects and creates extracts based on the new extracts and searches for required data in nmdr extracts.

    Args:
        extract_periods (list[str]): Periods to compile, in YYYYMM format.
        data_by_period (dict[str, pl.DataFrame]): In-memory extracted DataFrames keyed by period.
        nmdr_extracts_path (Path): Path to the dhis2_nmdr_sentinel_extract pipeline's data.
        output_path (Path): Path where the compiled nmdr extracts are saved.
        config_path (Path): Path to the folder containing required_nmdr_ids.py.

    Returns:
        list[Path]: Paths of the compiled nmdr extracts, including the pyramid metadata and population data.
    """
    current_run.log_info("Compiling nmdr extracts..")
    output_path.mkdir(parents=True, exist_ok=True)

    nmdr_extracts = []
    nmdr_extracts.append(nmdr_extracts_path / "pyramid_metadata" / "nmdr_pyramid_metadata.parquet")
    req_de = load_required_dhis2_uids(config_path / "required_nmdr_ids.py")

    extract_path = collect_data_for_periods(
        periods=extract_periods,
        data_by_period=data_by_period,
        nmdr_extracts_path=nmdr_extracts_path,
        output_path=output_path,
        required_data_elements=req_de,
    )
    nmdr_extracts.extend(extract_path)

    pop_paths = collect_population_data_for_periods(
        extract_periods=extract_periods,
        nmdr_extracts_path=nmdr_extracts_path,
    )
    nmdr_extracts.extend(pop_paths)

    return nmdr_extracts


def load_required_dhis2_uids(identifiers_fname: Path) -> list[str]:
    """Loads the required DHIS2 data element UIDs from a Python config file.

    Args:
        identifiers_fname (Path): Path to the Python file defining the required UID lists.

    Returns:
        list[str]: Required data element UIDs.
    """
    namespace = {}
    exec(identifiers_fname.read_text(encoding="utf-8"), namespace)
    return namespace["required_data_elements"]


def collect_data_for_periods(
    periods: list[str],
    data_by_period: dict[str, pl.DataFrame],
    nmdr_extracts_path: Path,
    output_path: Path,
    required_data_elements: list,
) -> list[Path]:
    """Collects and creates extracts based on the new extracts and searches for additional data in nmdr extracts.

    Args:
        periods (list[str]): Periods to compile, in YYYYMM format.
        data_by_period (dict[str, pl.DataFrame]): In-memory extracted DataFrames keyed by period.
        nmdr_extracts_path (Path): Path to the dhis2_nmdr_sentinel_extract pipeline's data.
        output_path (Path): Path where the compiled nmdr extracts are saved.
        required_data_elements (list): Data element UIDs to include from the NMDR extracts.

    Returns:
        list[Path]: Paths of the compiled nmdr extracts.
    """
    current_run.log_info(f"Compiling nmdr extract for period: {periods}..")

    extract_schema = {
        "data_type": pl.String,
        "dx": pl.String,
        "period": pl.String,
        "org_unit": pl.String,
        "category_option_combo": pl.String,
        "attribute_option_combo": pl.String,
        "rate_metric": pl.String,
        "domain_type": pl.String,
        "value": pl.String,
    }

    nmdr_extracts = []
    for period in periods:
        data_elements_nmdr_file = next((nmdr_extracts_path / "nmdr_extracts").glob(f"nmdr_data_{period}.parquet"), None)
        nmdr_df = pl.read_parquet(data_elements_nmdr_file) if data_elements_nmdr_file else pl.DataFrame()

        data_elements_df = _collect_data_elements_for_period(
            period=period,
            source_df=data_by_period.get(period),
            nmdr_extract=nmdr_df,
            nmdr_required_de=required_data_elements,
            schema=extract_schema,
        )

        nmdr_extract_df = data_elements_df
        if nmdr_extract_df.is_empty():
            current_run.log_info(f"No data found for period {period}. Skipping extract.")
            continue

        save_to_parquet(data=nmdr_extract_df, filename=output_path / f"nmdr_extract_{period}.parquet")
        current_run.log_info(
            f"NMDR extract for period {period} saved at {output_path / f'nmdr_extract_{period}.parquet'}"
        )
        nmdr_extracts.append(output_path / f"nmdr_extract_{period}.parquet")

    return nmdr_extracts


def _collect_data_elements_for_period(
    period: str,
    source_df: pl.DataFrame | None,
    nmdr_extract: pl.DataFrame,
    nmdr_required_de: list,
    schema: dict,
) -> pl.DataFrame:
    """Collects data elements for a given period from the in-memory DataFrame and appends them to the result.

    Also searches for additional data elements in the NMDR extracts and appends them to the DataFrame.

    Args:
        period (str): Period to collect, in YYYYMM format.
        source_df (pl.DataFrame | None): In-memory DataFrame for this period, or None if not available.
        nmdr_extract (pl.DataFrame): nmdr extract data to search for additional data elements.
        nmdr_required_de (list): Data element UIDs to include from the nmdr extract.
        schema (dict): Polars schema used to cast the collected data.

    Returns:
        pl.DataFrame: Collected data elements for the specified period.
    """
    data_elements_df = pl.DataFrame(schema=schema)
    if source_df is not None:
        data_elements_df = source_df.cast(schema)

    # Search for the additional data elements in the nmdr folder
    if not nmdr_extract.is_empty():
        nmdr_de_df = nmdr_extract.filter(
            (pl.col("data_type") == "DATA_ELEMENT") & pl.col("dx").is_in(nmdr_required_de)
        ).cast(schema)
        data_elements_df = pl.concat([data_elements_df, nmdr_de_df])

    return data_elements_df


def collect_population_data_for_periods(extract_periods: list[str], nmdr_extracts_path: Path) -> list[Path]:
    """Collects population data for the specified periods from the nmdr extracts.

    Args:
        extract_periods (list[str]): Periods to collect, in YYYYMM format.
        nmdr_extracts_path (Path): Path to the dhis2_nmdr_extract pipeline's data.

    Returns:
        list[Path]: Paths of the population data extracts found for each period.
    """
    pop_paths = []
    year_periods = sorted(set([p[0:4] for p in extract_periods]))
    for period in year_periods:
        pop_file = next((nmdr_extracts_path / "population").glob(f"nmdr_population_{period}.parquet"), None)
        if pop_file:
            pop_paths.append(pop_file)
        else:
            current_run.log_info(f"No population data found for period {period}.")
    return pop_paths


if __name__ == "__main__":
    dhis2_nmdr_sentinel_extract()