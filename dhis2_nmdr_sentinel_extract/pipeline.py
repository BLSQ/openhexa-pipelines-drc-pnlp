import os
from pathlib import Path

import polars as pl
from d2d_development.extract import DHIS2Extractor
from openhexa.sdk import current_run, parameter, pipeline, workspace
from openhexa.toolbox.dhis2 import DHIS2
from openhexa.toolbox.dhis2.dataframe import get_organisation_unit_groups, get_organisation_units
from sqlalchemy import create_engine, text
from utils import (
    connect_to_dhis2,
    get_extract_periods,
    load_configuration,
    resolve_dates_and_validate,
    save_to_parquet,
)

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
    default="202605",
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
        "If set to TRUE the extraction from DHIS2 will take place."
    ),
)
@parameter(
    code="transform_load",
    name="Run data transformation and load to the database",
    type=bool,
    default=True,
    help=(
        "If set to TRUE it will run the transformation step and load from the data previously extracted."
    ),
)
def dhis2_nmdr_sentinel_extract(start_date: str, end_date: str, extract: bool, transform_load: bool):
    """Orchestrates the NMDR Sentinel monthly extraction and transformation into files and a database table.

    Runs extraction and/or transformation and load, depending on parameters.

    Args:
        start_date (str): Start date for data extraction in YYYYMM format (extract step only).
        end_date (str): End date for data extraction in YYYYMM format (extract step only).
        extract (bool): Whether to run the extract step from DHIS2.
        transform_load (bool): Whether to run the transform and load steps. Note: the transform
            step processes every extract file found on disk, regardless of start/end dates.
    """
    pipeline_path = Path(workspace.files_path) / "pipelines" / "dhis2_nmdr_sentinel_extract"
    pyramid_path = pipeline_path / "data" / "pyramid_metadata"
    org_units_group_path = pipeline_path / "data" / "orgUnits_groups"
    extract_path = pipeline_path / "data" / "nmdr_extracts"
    transform_path = pipeline_path / "data" / "nmdr_transforms"

    config = load_configuration(pipeline_path / "config" / "extract_config.json")
    mapping = load_configuration(pipeline_path / "config" / "indicator_mapping.json")

    start, end = resolve_dates_and_validate(start_date, end_date, config)
    periods = get_extract_periods(start, end)

    sentinelle_ou_groups = config["SENTINELLE_OU_GROUPS"]

    # --- EXTRACT ---
    if extract:
        dhis2_nmdr_client = connect_to_dhis2(connection_str=config["SETTINGS"]["DHIS2_CONNECTION"])
        try:
            extract_step(
                extract_path=extract_path,
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

    # --- TRANSFORM ---
    if transform_load:
        try:
            transform_step(
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


    # --- LOAD ---
    if transform_load:
        try:
            load_step(
                transform_path=transform_path,
            )
            current_run.log_info("Data loaded successfully.")
        except Exception as e:
            current_run.log_error(f"An error occurred during loading: {e}")
            raise


# =============================================================================
# EXTRACT FUNCTIONS
# =============================================================================

def extract_step(
    extract_path: Path,
    dhis2_client: DHIS2,
    extract_periods: list[str],
    config: dict,
    pyramid_path: Path,
    org_units_group_path: Path,
    sentinelle_ou_groups: dict[str, str],
) -> None:
    """Extracts pyramid metadata, org unit groups metadata and DHIS2 analytics data.

    Args:
        extract_path (Path): Root path where the extracted files are saved.
        dhis2_client (DHIS2): Connected DHIS2 client.
        extract_periods (list[str]): Periods to extract (YYYYMM format).
        config (dict): Extraction configuration.
        pyramid_path (Path): Root path for the pyramid metadata.
        org_units_group_path (Path): Root path for the Org Units per group metadata.
        sentinelle_ou_groups (dict[str, str]): Mapping of org unit ID -> group name.
    """
    extract_pyramid_metadata(
        dhis2_client=dhis2_client,
        pyramid_path=pyramid_path,
    )
    current_run.log_info("Pyramid metadata extracted successfully.")

    extract_sentinelle_org_units(
        org_units_group_path=org_units_group_path,
        dhis2_client=dhis2_client,
        sentinelle_ou_groups=sentinelle_ou_groups,
    )
    current_run.log_info("Org unit groups metadata extracted successfully.")

    current_run.log_info(f"Extracting for periods: {extract_periods}")

    dhis2_client.analytics.MAX_DX = 100
    dhis2_client.analytics.MAX_ORG_UNITS = 100
    dhis2_client.data_value_sets.MAX_DATA_ELEMENTS = 100
    dhis2_client.data_value_sets.MAX_ORG_UNITS = 100

    org_units_df = pl.read_parquet(org_units_group_path / "orgUnits_group.parquet")
    org_unit_list = org_units_df["org_unit_id"].to_list()

    extract_data_elements_for_periods(
        extract_periods=extract_periods,
        config=config,
        dhis2_nmdr_client=dhis2_client,
        extract_path=extract_path,
        org_unit_list=org_unit_list,
    )
    current_run.log_info("DHIS2 indicators data extracted successfully.")


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
    extract_path: Path,
    org_unit_list: list[str],
) -> None:
    """Downloads data elements for each period.

    Args:
        extract_periods (list[str]): Periods to extract, in YYYYMM format.
        config (dict): Extraction configuration loaded from extract_config.json.
        dhis2_nmdr_client (DHIS2): Connected DHIS2 client used to retrieve the data.
        extract_path (Path): Root path where the extracted files are saved.
        org_unit_list (list[str]): Organisation unit IDs to extract data for.
    """
    dhis2_extractor = DHIS2Extractor(dhis2_client=dhis2_nmdr_client, download_mode=config["SETTINGS"]["MODE"])
    try:
        for period in extract_periods:
            raw_data_path = dhis2_extractor.data_elements.download_period(
                data_elements=config["DATA_ELEMENTS"]["UIDS"],
                org_units=org_unit_list,
                period=period,
                output_dir=extract_path,
                filename=f"nmdr_extract_{period}.parquet",
            )
            if not raw_data_path:
                current_run.log_info(f"No data elements data for period {period}.")
    except Exception as e:
        raise Exception(f"Extract data elements error : {e}") from e  # let it crash!

# =============================================================================
# TRANSFORM FUNCTIONS
# =============================================================================


def transform_step(
    pyramid_path: Path,
    org_units_group_path: Path,
    extract_path: Path,
    transform_path: Path,
    mapping: dict,
) -> None:
    """Transforms the extracted data, computes num and den values for each indicator.

    Processes every file matching nmdr_extract_YYYYMM.parquet found in extract_path,
    regardless of the pipeline's start/end date parameters.

    Args:
        pyramid_path (Path): Root path for the pyramid metadata.
        org_units_group_path (Path): Root path for the Org Units per group metadata.
        extract_path (Path): Root path for the extracted files to be transformed.
        transform_path (Path): Root path where to save the transformed files.
        mapping (dict): Mappings between indicators to be calculated and num and den components.
    """
    fosa_names = load_fosa_names(pyramid_path / "nmdr_pyramid_metadata.parquet")

    org_units_groups = (
        pl.read_parquet(org_units_group_path / "orgUnits_group.parquet")
        .rename({"org_unit_id": "org_unit", "orgUnitGroup": "organisationUnitGroup"})
    )

    extract_files = sorted(extract_path.glob("nmdr_extract_*.parquet"))

    if not extract_files:
        current_run.log_info(f"No extract files found in {extract_path}. Nothing to transform.")
        return

    for extract_file in extract_files:
        transform_period(
            extract_file=extract_file,
            period=extract_file.stem[-6:],
            indicator_mapping=mapping,
            fosa_names=fosa_names,
            org_units_groups=org_units_groups,
            transform_path=transform_path,
        )

    current_run.log_info("DHIS2 indicators data transformed successfully.")


def load_fosa_names(pyramid_file: Path) -> pl.DataFrame:
    """Loads the org_unit id -> name/geo-hierarchy mapping from the pyramid metadata parquet.

    Args:
        pyramid_file (Path): Path to nmdr_pyramid_metadata.parquet.

    Returns:
        pl.DataFrame: Columns ['org_unit', 'fosa', 'province', 'zone_de_sante', 'aire_de_sante'].

    Raises:
        Exception: If the pyramid file is not found or is missing expected columns.
    """
    if not pyramid_file.is_file():
        raise Exception(
            f"Pyramid metadata not found at {pyramid_file}. Run the extract step first."
        )

    pyramid = pl.read_parquet(pyramid_file)

    expected_cols = ("id", "name", "level_2_name", "level_3_name", "level_4_name")
    missing_cols = [c for c in expected_cols if c not in pyramid.columns]
    if missing_cols:
        raise Exception(
            f"Pyramid metadata at {pyramid_file} is missing expected column(s) {missing_cols}. "
            f"Available columns: {pyramid.columns}"
        )

    return pyramid.select(
        pl.col("id").alias("org_unit"),
        pl.col("name").alias("fosa"),
        pl.col("level_2_name").alias("province"),
        pl.col("level_3_name").alias("zone_de_sante"),
        pl.col("level_4_name").alias("aire_de_sante"),
    )


def transform_period(
    extract_file: Path,
    period: str,
    indicator_mapping: list[dict],
    fosa_names: pl.DataFrame,
    org_units_groups: pl.DataFrame,
    transform_path: Path,
) -> None:
    """Transforms a single period's NMDR extract into indicator numerator/denominator rows.

    Args:
        extract_file (Path): Path to the nmdr_extract_<period>.parquet file to transform.
        period (str): Period being transformed, in YYYYMM format.
        indicator_mapping (list[dict]): Indicator definitions (name, num_dx, den_dx).
        fosa_names (pl.DataFrame): org_unit id -> fosa name lookup table.
        org_units_groups (pl.DataFrame): org_unit -> organisationUnitGroup lookup table.
        transform_path (Path): Folder where nmdr_transform_<period>.parquet is written.
    """
    df = pl.read_parquet(extract_file).with_columns(pl.col("value").cast(pl.Float64, strict=False))

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

# =============================================================================
# LOAD FUNCTIONS
# =============================================================================


def load_step(
    transform_path: Path,
) -> None:
    """Loads the transformed NMDR data into the public.nmdr_sentinelles_test table.

    Truncates the target table and appends every nmdr_transform_YYYYMM.parquet file
    found in transform_path. If no transform files exist, the table is left untouched.

    Args:
        transform_path (Path): Root path containing the transformed parquet files to load.
    """
    table_name = "nmdr_sentinelles_test"

    expected_columns = [
        "period", "annee", "indicateur_name", "province",
        "zone_de_sante", "aire_de_sante", "fosa", "organisationUnitGroup",
        "indicateur_num", "indicateur_den",
    ]

    transform_files = sorted(transform_path.glob("nmdr_transform_*.parquet"))

    if not transform_files:
        current_run.log_info(f"No transform files found in {transform_path}. Nothing to load.")
        return

    engine = create_engine(os.environ["WORKSPACE_DATABASE_URL"])

    with engine.begin() as conn:
        conn.execute(text(f"TRUNCATE TABLE public.{table_name};"))
    current_run.log_info(f"Table public.{table_name} truncated.")

    total_rows = 0
    for transform_file in transform_files:
        df = pl.read_parquet(transform_file).to_pandas()

        missing = set(expected_columns) - set(df.columns)
        if missing:
            raise ValueError(f"{transform_file.name} is missing expected columns: {missing}")
        df = df[expected_columns]

        df.to_sql(
            table_name,
            engine,
            schema="public",
            if_exists="append",
            index=False,
            chunksize=10_000,
            method="multi",
        )
        total_rows += len(df)
        current_run.log_info(f"Loaded {len(df):,} rows from {transform_file.name}")

    current_run.log_info(
        f"DHIS2 indicators data loaded successfully. "
        f"{total_rows:,} rows loaded into public.{table_name}."
    )


if __name__ == "__main__":
    dhis2_nmdr_sentinel_extract()