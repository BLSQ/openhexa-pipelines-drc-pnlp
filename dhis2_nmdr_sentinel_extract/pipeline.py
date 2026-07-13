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
    default=False,
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
    extract_path = pipeline_path / "data" / "nmdr_extracts"
    transform_path = pipeline_path / "data" / "nmdr_transforms"

    config = load_configuration(pipeline_path / "config" / "extract_config.json")
    mapping = load_configuration(pipeline_path / "config" / "indicator_mapping.json")

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

    # --- TRASFORM ---
    if transform_load:

        try:
            transform_step(
                pipeline_path=pipeline_path,
                pyramid_path=pyramid_path,
                org_units_group_path=org_units_group_path,
                extract_path=extract_path,
                transform_path=transform_path,
                mapping=mapping,
            )
            current_run.log_info("Data transformed successfully.")
        except Exception as e:
            current_run.log_error(f"An error occurred during transformation: {e}")
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
    
# =============================================================================
# TRANSFORM FUNCTIONS
# =============================================================================


def transform_step(
    pipeline_path: Path,
    pyramid_path: Path,
    org_units_group_path: Path,
    extract_path: Path,
    transform_path: Path,
    mapping: dict,
) -> None:
    """Transforms the extracted data, computes num and den values for each indicator.
    
    Args:
        pipeline_path (Path): Root path of the pipeline.
        pyramid_path (Path): Root path for the pyramid metadata.
        org_units_group_path (Path): Root path for the Org Units per group metadata.
        extract_path (Path): Root path for the extracted files to be transformed.
        transform_path (Path): Root path where to save the transformed files.
        mapping (dict): Mappings between indicators to be calculated and num and den components.
    """
    try:
        fosa_names = load_fosa_names(pyramid_path)

        periods = sorted(
        file.stem[-6:]
        for file in extract_path.glob("nmdr_extract_*.parquet")
        )

        for period in periods:
            transform_period(
                period=period,
                extracts_path=extract_path,
                indicator_mapping=mapping,
                fosa_names=fosa_names,
                org_units_group_path=org_units_group_path,
                transform_path=transform_path,
            )

        current_run.log_info("DHIS2 indicators data transformed successfully.")
    except Exception as e:
        current_run.log_error(f"An error occurred during DHIS2 indicators transformation: {e}")
        raise


def load_fosa_names(pyramid_path: Path) -> pl.DataFrame:
    """Loads the org_unit id -> name/geo-hierarchy mapping from the pyramid metadata parquet.

    Args:
        pyramid_path (Path): Path to nmdr_pyramid_metadata.parquet.

    Returns:
        pl.DataFrame: Columns ['org_unit', 'fosa', 'province', 'zone_de_sante', 'aire_de_sante'].
            Empty if the pyramid file is not found or missing the expected level_*_name columns.
    """
    if not pyramid_path.exists():
        current_run.log_info(
            f"No pyramid metadata found at {pyramid_path}. 'fosa' will fall back to org_unit id, "
            "and 'province'/'zone_de_sante'/'aire_de_sante' will be null."
        )
        return pl.DataFrame(schema=GEO_SCHEMA)

    pyramid = pl.read_parquet(pyramid_path)

    missing_cols = [c for c in ("level_2_name", "level_3_name", "level_4_name") if c not in pyramid.columns]
    if missing_cols:
        current_run.log_info(
            f"Pyramid metadata at {pyramid_path} is missing expected column(s) {missing_cols}. "
            "'province'/'zone_de_sante'/'aire_de_sante' will be null for rows they can't be resolved for. "
            f"Available columns: {pyramid.columns}"
        )

    return pyramid.select(
        pl.col("id").alias("org_unit"),
        pl.col("name").alias("fosa"),
        (pl.col("level_2_name") if "level_2_name" in pyramid.columns else pl.lit(None, dtype=pl.String)).alias(
            "province"
        ),
        (pl.col("level_3_name") if "level_3_name" in pyramid.columns else pl.lit(None, dtype=pl.String)).alias(
            "zone_de_sante"
        ),
        (pl.col("level_4_name") if "level_4_name" in pyramid.columns else pl.lit(None, dtype=pl.String)).alias(
            "aire_de_sante"
        ),
    )


def transform_period(
    period: str,
    extracts_path: Path,
    indicator_mapping: list[dict],
    fosa_names: pl.DataFrame,
    org_units_group_path: Path,
    transform_path: Path,
) -> None:
    """Transforms a single period's NMDR extract into indicator numerator/denominator rows.

    Args:
        period (str): Period to transform, in YYYYMM format.
        extracts_path (Path): Path to the folder containing nmdr_extract_<period>.parquet files.
        indicator_mapping (list[dict]): Indicator definitions (name, num_dx, den_dx).
        fosa_names (pl.DataFrame): org_unit id -> fosa name lookup table.
        org_units_group_path (Path): Folder containing orgUnits_group.parquet, mapping org_unit_id -> orgUnitGroup.
        transform_path (Path): Folder where nmdr_transform_<period>.parquet is written.
    """
    extract_file = extracts_path / f"nmdr_extract_{period}.parquet"
    if not extract_file.exists():
        current_run.log_info(f"No extract found for period {period} at {extract_file}. Skipping.")
        return

    df = pl.read_parquet(extract_file).with_columns(pl.col("value").cast(pl.Float64, strict=False))

    org_units_groups = (
        pl.read_parquet(org_units_group_path / "orgUnits_group.parquet")
        .rename({"org_unit_id": "org_unit", "orgUnitGroup": "organisationUnitGroup"})
    )

    all_org_units = df.select("org_unit").unique().join(org_units_groups, on="org_unit", how="left")
    indicator_names = pl.DataFrame({"indicateur_name": [ind["indicateur_name"] for ind in indicator_mapping]})
    base = all_org_units.join(indicator_names, how="cross")

    num_frames = []
    den_frames = []
    for indicator in indicator_mapping:
        num_frames.append(
            df.filter(pl.col("dx").is_in(indicator["num_dx"]))
            .group_by("org_unit")
            .agg(pl.col("value").sum().alias("indicateur_num"))
            .with_columns(pl.lit(indicator["indicateur_name"]).alias("indicateur_name"))
        )
        den_frames.append(
            df.filter(pl.col("dx").is_in(indicator["den_dx"]))
            .group_by("org_unit")
            .agg(pl.col("value").sum().alias("indicateur_den"))
            .with_columns(pl.lit(indicator["indicateur_name"]).alias("indicateur_name"))
        )

    num_df = pl.concat(num_frames) if num_frames else pl.DataFrame(schema={"org_unit": pl.String, "indicateur_name": pl.String, "indicateur_num": pl.Float64})
    den_df = pl.concat(den_frames) if den_frames else pl.DataFrame(schema={"org_unit": pl.String, "indicateur_name": pl.String, "indicateur_den": pl.Float64})

    result = (
        base.join(num_df, on=["org_unit", "indicateur_name"], how="left")
        .join(den_df, on=["org_unit", "indicateur_name"], how="left")
        .join(fosa_names, on="org_unit", how="left")
    )

    result = result.with_columns(
        pl.lit(f"{period[0:4]}-{period[4:6]}").alias("period"),
        pl.lit(period[0:4]).alias("annee"),
        pl.coalesce(pl.col("fosa"), pl.col("org_unit")).alias("fosa"),
    ).select(
        "period",
        "annee",
        "indicateur_name",
        "province",
        "zone_de_sante",
        "aire_de_sante",
        "fosa",
        "organisationUnitGroup",
        "indicateur_num",
        "indicateur_den",
    )

    output_file = transform_path / f"nmdr_transform_{period}.parquet"
    save_to_parquet(data=result, filename=output_file)
    current_run.log_info(f"NMDR transform for period {period} saved at {output_file}")


if __name__ == "__main__":
    dhis2_nmdr_sentinel_extract()