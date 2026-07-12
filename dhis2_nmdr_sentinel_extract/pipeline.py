import json
import tempfile
from pathlib import Path
import polars as pl
from d2d_development.extract import DHIS2Extractor
from openhexa.sdk import current_run, parameter, pipeline, workspace
from openhexa.toolbox.dhis2 import DHIS2
from openhexa.toolbox.dhis2.dataframe import get_organisation_unit_groups, get_organisation_units
from utils import connect_to_dhis2, get_extract_periods, load_configuration, resolve_dates_and_validate, save_to_parquet,  read_parquet_extract

GEO_SCHEMA = {
    "org_unit": pl.String,
    "fosa": pl.String,
    "province": pl.String,
    "zone_de_sante": pl.String,
    "aire_de_sante": pl.String,
}

@pipeline("dhis2_nmdr_sentinel_extract", timeout=21600)  # 6 hours
@parameter(
    code="start_date",
    name="Start date (format: YYYYMM)",
    default="202604",
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
    default="202604",
    type=str,
    required=False,
    help="End date for data extraction in YYYYMM format. If not set, it will default to current date minus 1.",
)
@parameter(
    code="extract",
    name="Run data extract",
    type=bool,
    default=True,
    help=(
        "If set to TRUE the extraction from DHIS2 wiil take place."
    ),
)
@parameter(
    code="transform_load",
    name="Run data transformation and laod to the database",
    type=bool,
    default=True,
    help=(
        "If set to TRUE it will rin the transformation step and load form the data previously extracted."
    ),
)
def dhis2_nmdr_sentinel_extract(start_date: str, end_date: str, extract: bool, transform_load: bool):
    """Orchestrates the NMDR Sentinel monthly extraction and transformation into files and a database table.

    Runs extraction and/or transformation and load, depending on parameters.

    Args:
        start_date (str): Start date for data extraction/transformation in YYYYMM format.
        end_date (str): End date for data extraction/transformation in YYYYMM format.
        extract (bool): Whether to run the extract step from DHIS2.
        transform_load (bool): Whether to run the extract and load steps from DHIS2.
    """
    pipelines_root = Path(workspace.files_path) / "pipelines"
    pipeline_path = pipelines_root / "dhis2_nmdr_sentinel_extract"
    pyramid_path = pipeline_path / "data" / "pyramid_metadata"
    org_units_group_path = pipeline_path / "data" / "orgUnits_groups"

    config = load_configuration(pipeline_path / "config" / "extract_config.json")
    start, end = resolve_dates_and_validate(start_date, end_date, config)
    periods = get_extract_periods(start, end)

    sentinelle_ou_groups = {
    "qTZ3L6pLMTi": "CS Site Sentinelle",
    "wldxDI2Ey5c": "HGR Site Sentinelle",
    }

    # --- EXTRACT ---
    if extract:

        dhis2_nmdr_client = connect_to_dhis2(connection_str=config["SETTINGS"]["DHIS2_CONNECTION"])
        try:
            extract_step(
                pipeline_path=pipeline_path,
                dhis2_client=dhis2_nmdr_client,
                extract_periods=periods,
                config=config,
                pyramid_path=pyramid_path,
                org_units_group_path=org_units_group_path,
                sentinelle_ou_groups=sentinelle_ou_groups,
            )
            current_run.log_info("Data extracted successfully.")
        except Exception as e:
            current_run.log_error(f"An error occurred during extraction: {e}")
            raise

 
# =============================================================================
# EXTRACT FUNCTIONS
# =============================================================================

def extract_step(
    pipeline_path: Path,
    dhis2_client: DHIS2,
    extract_periods: list[str],
    config: dict,
    pyramid_path: Path,
    org_units_group_path: Path,
    sentinelle_ou_groups: dict[str, str],
) -> None:
    """Extracts pyramid metadata, org Unit groups meadata and DHIS2 analytics data.

    Args:
        pipeline_path (Path): Root path of the pipeline.
        dhis2_client (DHIS2): Connected DHIS2 client.
        extract_periods (list[str]): Periods to extract (YYYYMM format).
        config (dict): Extraction configuration.
        pyramid_path (Path): Root path for the pyramid metadata.
        org_units_group_path (Path): Root path for the Org Units per group metadata.
        sentinelle_ou_groups (dict[str, str]): Mapping of org unit ID -> group name.
    """
    try:
        extract_pyramid_metadata(
            dhis2_client=dhis2_client,
            pyramid_path=pyramid_path,
        )
        current_run.log_info("Pyramid metada extracted successfully.")
    except Exception as e:
        current_run.log_error(f"An error occurred during pyramid extraction: {e}")
        raise
    
    try:
        extract_sentinelle_org_units(
            org_units_group_path=org_units_group_path,
            dhis2_client=dhis2_client,
            sentinelle_ou_groups=sentinelle_ou_groups,
        )
        current_run.log_info("Org unit groups metada extracted successfully.")
    except Exception as e:
        current_run.log_error(f"An error occurred during sentinelle org units extraction: {e}")
        raise

    current_run.log_info(f"Extracting for periods: {extract_periods}")

    dhis2_client.analytics.MAX_DX = 100
    dhis2_client.analytics.MAX_ORG_UNITS = 100
    dhis2_client.data_value_sets.MAX_DATA_ELEMENTS = 100
    dhis2_client.data_value_sets.MAX_ORG_UNITS = 100

    org_units_df = pl.read_parquet(org_units_group_path / "orgUnits_group.parquet")
    org_unit_list = org_units_df["org_unit_id"].to_list()

    try:
        extract_data_elements_for_periods(
            extract_periods=extract_periods,
            config=config,
            dhis2_nmdr_client=dhis2_client,
            pipeline_path=pipeline_path,
            org_unit_list=org_unit_list,
        )
        current_run.log_info("DHIS2 indicators data extracted successfully.")
    except Exception as e:
        current_run.log_error(f"An error occurred during DHIS2 indicators extraction: {e}")
        raise


def extract_pyramid_metadata(
    dhis2_client: DHIS2, 
    pyramid_path: Path,
) -> None:
    """Extracts and saves the pyramid metadata at level 5.

    Args:
        dhis2_client (DHIS2): Connected DHIS2 client used to retrieve the pyramid.
        pyramid_path (Path): Root path for the pyramid metadata.
    """
    current_run.log_info("Retrieving NMDR DHIS2 pyramid metadata")

    try:
        org_units = get_organisation_units(dhis2_client).drop("geometry")
        org_units = org_units.filter(pl.col("level") == 5)
        current_run.log_info(f"{len(org_units['id'].unique())} units at organisation unit level: 5")
    except Exception as e:
        raise Exception(f"Error while extracting NMDR DHIS2 Pyramid: {e}") from e

    save_to_parquet(data=org_units, filename=pyramid_path / "nmdr_pyramid_metadata.parquet")
    current_run.log_info(f"NMDR DHIS2 pyramid metadata saved: {pyramid_path / 'nmdr_pyramid_metadata.parquet'}")


def extract_sentinelle_org_units(
    org_units_group_path: Path, 
    dhis2_client: DHIS2, 
    sentinelle_ou_groups: dict[str, str],
    ) -> None:
    """Extracts and saves sentinelle organisation unit IDs and their group mapping.

    Queries the sentinelle org unit groups (CS and HGR) and saves a parquet file with:
    - org_unit_id: organisation unit ID
    - orgUnitGroup: group display name

    Args:
        org_units_group_path (Path): Path where orgUnits_group.parquet will be saved.
        dhis2_client (DHIS2): Connected DHIS2 client.
        sentinelle_ou_groups (dict[str, str]): Mapping of group IDs to group display names.
    """
    current_run.log_info("Retrieving sentinelle organisation units")

    try:
        ou_groups_df = get_organisation_unit_groups(dhis2_client)
    except Exception as e:
        raise Exception(f"Error retrieving organisation unit groups: {e}") from e

    sentinelle_groups = ou_groups_df.filter(pl.col("id").is_in(list(sentinelle_ou_groups.keys())))

    ou_group_pairs = []
    for row in sentinelle_groups.iter_rows(named=True):
        group_name = sentinelle_ou_groups[row["id"]]
        for ou_id in row["organisation_units"]:
            ou_group_pairs.append({"org_unit_id": ou_id, "orgUnitGroup": group_name})

    df = pl.DataFrame(ou_group_pairs)
    
    current_run.log_info(f"Found {len(df)} sentinelle facilities across {len(sentinelle_ou_groups)} groups")
    
    save_to_parquet(data=df, filename=org_units_group_path / "orgUnits_group.parquet")
    current_run.log_info(f"Sentinelle org units saved: {org_units_group_path / 'orgUnits_group.parquet'}")


def extract_data_elements_for_periods(
    extract_periods: list[str],
    config: dict,
    dhis2_nmdr_client: DHIS2,
    pipeline_path: Path,
    org_unit_list: list[str],
) -> None:
    """Downloads data elements for each period.

    Args:
        extract_periods (list[str]): Periods to extract, in YYYYMM format.
        config (dict): Extraction configuration loaded from extract_config.json.
        dhis2_nmdr_client (DHIS2): Connected DHIS2 client used to retrieve the data.
        pipeline_path (Path): Root path of the pipeline.
        org_unit_list (list[str]): Organisation unit IDs to extract data for.
    """
    dhis2_extractor = DHIS2Extractor(dhis2_client=dhis2_nmdr_client, download_mode=config["SETTINGS"]["MODE"])
    try:
        for period in extract_periods:
            raw_data_path = dhis2_extractor.data_elements.download_period(
                data_elements=config["DATA_ELEMENTS"]["UIDS"],
                org_units=org_unit_list,
                period=period,
                output_dir=pipeline_path / "data" / "nmdr_extracts",
                filename=f"nmdr_extract_{period}.parquet",
            )
            if not raw_data_path:
                current_run.log_info(f"No data elements data for period {period}.")
    except Exception as e:
        raise Exception(f"Extract data elements error : {e}") from e 


if __name__ == "__main__":
    dhis2_nmdr_sentinel_extract()