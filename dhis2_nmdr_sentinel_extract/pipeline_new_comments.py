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

# Sentinelle org unit groups: maps group ID -> display name
SENTINELLE_OU_GROUPS = {
    "qTZ3L6pLMTi": "CS Site Sentinelle",
    "wldxDI2Ey5c": "HGR Site Sentinelle",
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
    code="mode",
    name="Mode",
    type=str,
    required=False,
    default=None,
    choices=["extract", "transform"],
    help=(
        "Controls which steps to run. "
        "'extract' runs only the DHIS2 extraction. "
        "'transform' runs only the indicator transformation. "
        "Leave empty to run both steps (default)."
    ),
)
@parameter(
    code="fill_missing_with_zero",
    name="Fill missing num/den with 0",
    type=bool,
    default=True,
    help=(
        "If a facility has no reported value at all for an indicator's numerator or denominator "
        "data elements in a period, fill it with 0 instead of leaving it null."
    ),
)
def dhis2_nmdr_sentinel_extract(start_date: str, end_date: str, mode: str | None, fill_missing_with_zero: bool):
    """Orchestrates the NMDR Sentinel monthly extraction and transformation into indicator parquet files.

    Runs extraction, transformation, or both depending on the `mode` parameter.

    Args:
        start_date (str): Start date for data extraction/transformation in YYYYMM format.
        end_date (str): End date for data extraction/transformation in YYYYMM format.
        mode (str | None): 'extract' to only extract, 'transform' to only transform,
            or None (default) to run both steps in sequence.
        fill_missing_with_zero (bool): Whether to fill missing num/den sums with 0 instead of null.
    """
    run_extract = mode in (None, "extract")
    run_transform = mode in (None, "transform")

    pipelines_root = Path(workspace.files_path) / "pipelines"
    pipeline_path = pipelines_root / "dhis2_nmdr_sentinel_extract"

    # Defined once here so both extract and transform reference the same path
    pyramid_path = pipeline_path / "data" / "pyramid_metadata" / "nmdr_pyramid_metadata.parquet"
    # todo: add org unit_path  and pass it to the new function load_org_units_groups somethign
    # groups_path = pipeline_path / "data" / "pyramid_metadata" / "nmdr_pyramid_metadata.parquet"

    config = load_configuration(pipeline_path / "config" / "extract_config.json")
    start, end = resolve_dates_and_validate(start_date, end_date, config)
    periods = get_extract_periods(start, end)

    # --- EXTRACT ---
    if run_extract:
        dhis2_client = connect_to_dhis2(connection_str=config["SETTINGS"]["DHIS2_CONNECTION"])

        sentinelle_ou_groups = get_sentinelle_org_units(dhis2_client)
        #TODO update to get the correct list of facilities or for exmaple x facilities per group
        current_run.log_info(
            f"Found {len(sentinelle_ou_list)} sentinelle facilities across {len(SENTINELLE_OU_GROUPS)} groups."
        )

        #TODO create just one biiiiiig function for extract with all mini functiones inside 
        try:
            extract_pyramid_metadata(
                pipeline_path=pipeline_path,
                dhis2_nmdr_client=dhis2_client,
            )
            extract_data(
                extract_periods=periods,
                config=config,
                dhis2_nmdr_client=dhis2_client,
                pipeline_path=pipeline_path,
                # TODO update parameters because we might just need one with the lsit of fosas and group
                sentinelle_ou_list=sentinelle_ou_list,
                sentinelle_ou_groups=sentinelle_ou_groups,
            )
            current_run.log_info("Data extracted successfully.")
        except Exception as e:
            current_run.log_error(f"An error occurred during extraction: {e}")
            raise

    # --- TRANSFORM ---
    if run_transform:

        #TODO create just one biiiiiig function for transform with all mini functiones inside 
        extracts_path = pipeline_path / "data" / "nmdr_extracts"
        output_path = pipeline_path / "data" / "nmdr_transforms"
        mapping_path = pipeline_path / "config" / "indicator_mapping.json"

        current_run.log_info(f"Transforming NMDR sentinel data for periods: {periods}")

        # TODO wrap each read parquet with try /exeception to 
        try:
            indicator_mapping = load_indicator_mapping(mapping_path)
            # TODO replace it for pyramid = read_parquet_extract(pyramid_path) note that now the columns are no longer renamed, to do later
            pyramid = read_parquet_extract(pyramid_path) # renamed from fosa_names
            # TODO load org unit group and fosa ids -> give the path also use utils function read from parquet read_parquet_extract 
        except Exception as e:
            current_run.log_error(f"An error occurred during reading parquet: {e}")
            raise

        output_path.mkdir(parents=True, exist_ok=True)
        transformed_files = []

        try:
            # todo remove for period loop because now I ll be reading everything that s in the folder, use a data_path.glob("*.parquet")
            for period in periods:
                transformed_path = transform_period(
                    period=period,
                    extracts_path=extracts_path,
                    indicator_mapping=indicator_mapping,
                    fosa_names=pyramid,
                    output_path=output_path,
                    fill_missing_with_zero=fill_missing_with_zero,
                )
                if transformed_path:
                    transformed_files.append(transformed_path)

            current_run.log_info(f"Transform finished. Files created: {transformed_files}")
        except Exception as e:
            current_run.log_error(f"An error occurred during transformation: {e}")
            raise

        # TODO add try/exception for compileFiles (new function to be created) it shoudl read all the transform files in the folder, 
        # truncate my databse table and load everything to it
        # Extract / Transform / Load


# =============================================================================
# EXTRACT FUNCTIONS
# =============================================================================

def get_sentinelle_org_units(dhis2_client: DHIS2) -> pl.DataFrame:
    """Retrieves org unit IDs and group name mapping for all sentinelle facilities.

    Queries the two sentinelle org unit groups (CS and HGR) and builds:
    - a flat list of all org unit IDs belonging to either group
   

    Args:
        dhis2_client (DHIS2): Connected DHIS2 client.

    Returns:
        tuple[list[str], dict[str, str]]:
            - List of sentinelle org unit IDs.
            - Dict mapping org unit ID -> group display name.
    """
    #TODO: update discription and make it save it somewhere to be sued in case by the transform only. add a parameter pipeline_path -> save as parquet like int he pyrmad extractioin
    try:
        ou_groups_df = get_organisation_unit_groups(dhis2_client)
    except Exception as e:
        raise Exception(f"Error retrieving organisation unit groups: {e}") from e
    ## TODO: adjust further down to use this new format for the pyramid selection
    return ou_groups_df.filter(pl.col("id").is_in(list(SENTINELLE_OU_GROUPS.keys())))


def extract_pyramid_metadata(pipeline_path: Path, dhis2_nmdr_client: DHIS2) -> None:
    """Extracts and saves the pyramid metadata at level 5.

    Args:
        pipeline_path (Path): Root path of the pipeline used to resolve the output data folder.
        dhis2_nmdr_client (DHIS2): Connected DHIS2 client used to retrieve the pyramid.
    """
    current_run.log_info("Retrieving NMDR DHIS2 pyramid metadata")

    try:
        org_units = get_organisation_units(dhis2_nmdr_client).drop("geometry")
        org_units = org_units.filter(pl.col("level") == 5)
        current_run.log_info(f"{len(org_units['id'].unique())} units at organisation unit level: 5")
    except Exception as e:
        raise Exception(f"Error while extracting NMDR DHIS2 Pyramid: {e}") from e

    pyramid_path = pipeline_path / "data" / "pyramid_metadata"
    save_to_parquet(data=org_units, filename=pyramid_path / "nmdr_pyramid_metadata.parquet")
    current_run.log_info(f"NMDR DHIS2 pyramid metadata saved: {pyramid_path / 'nmdr_pyramid_metadata.parquet'}")


def extract_data(
    extract_periods: list[str],
    config: dict,
    dhis2_nmdr_client: DHIS2,
    pipeline_path: Path,
    sentinelle_ou_list: list[str],
    sentinelle_ou_groups: dict[str, str],
) -> dict[str, pl.DataFrame]:
    """Retrieves DHIS2 analytics data elements for the given periods.

    Args:
        extract_periods (list[str]): Periods to extract, in YYYYMM format.
        config (dict): Extraction configuration loaded from extract_config.json.
        dhis2_nmdr_client (DHIS2): Connected DHIS2 client used to retrieve the data.
        sentinelle_ou_list (list[str]): Sentinelle org unit IDs to extract data for.
        sentinelle_ou_groups (dict[str, str]): Mapping of org unit ID -> group display name,
            used to add the organisationUnitGroup column to extracted data.

    Returns:
        dict[str, pl.DataFrame]: Extracted data keyed by period (YYYYMM).
    """
    current_run.log_info("Retrieving DHIS2 analytics data")
    current_run.log_info(f"Download MODE: {config['SETTINGS']['MODE']} for periods: {extract_periods}")

    dhis2_nmdr_client.analytics.MAX_DX = 100
    dhis2_nmdr_client.analytics.MAX_ORG_UNITS = 100
    dhis2_nmdr_client.data_value_sets.MAX_DATA_ELEMENTS = 100
    dhis2_nmdr_client.data_value_sets.MAX_ORG_UNITS = 100

    data_by_period = _extract_data_elements_for_periods(
        dhis2_client=dhis2_nmdr_client,
        periods=extract_periods,
        pipeline_path=pipeline_path,
        ## TODO update sentinelles
        org_unit_list=sentinelle_ou_list,
        sentinelle_ou_groups=sentinelle_ou_groups,
        config=config,
    )
    current_run.log_info("Data elements extract finished.")
    return data_by_period


def _extract_data_elements_for_periods(
    dhis2_client: DHIS2,
    periods: list[str],
    pipeline_path: Path,
    org_unit_list: list[str],
    sentinelle_ou_groups: dict[str, str],
    config: dict,
) -> None:
    """Downloads data elements for each period.

    Args:
        dhis2_client (DHIS2): Connected DHIS2 client used to retrieve the data.
        periods (list[str]): Periods to extract, in YYYYMM format.
        org_unit_list (list[str]): Organisation unit IDs to extract data for.
       ## TODO pl dataframe being returned update how it works -> sentinelle_ou_groups (dict[str, str]): Mapping of org unit ID -> group display name,
            used to add the organisationUnitGroup column to each downloaded parquet.
        config (dict): Extraction configuration loaded from extract_config.json.

    """
    dhis2_extractor = DHIS2Extractor(dhis2_client=dhis2_client, download_mode=config["SETTINGS"]["MODE"])

    ## delete temporary
    try:
        for period in periods:
            raw_data_path = dhis2_extractor.data_elements.download_period(
                data_elements=config["DATA_ELEMENTS"]["UIDS"],
                org_units=org_unit_list,
                period=period,
                output_dir=pipeline_path / "data" / "nmdr_extracts",
                filename=f"nmdr_extract_{period}.parquet",
            )
            if not raw_data_path:
                current_run.log_info(f"No data elements data for period {period}.")
                continue

            ## TODO move transformation aprt of asigning orgunitgroup to the transform part        
            ## df = pl.read_parquet(raw_data_path)
            ## df = df.with_columns(
            ##    pl.col("org_unit")
            ##    .map_elements(lambda x: sentinelle_ou_groups.get(x), return_dtype=pl.String)
            ##    .alias("organisationUnitGroup")
            ## )
            ## data_by_period[period] = df

    except Exception as e:
        raise Exception(f"Extract data elements error : {e}") from e
    

# =============================================================================
# TRANSFORM FUNCTIONS
# =============================================================================


def load_indicator_mapping(mapping_path: Path) -> list[dict]:
    """Loads the indicator numerator/denominator dx mapping from a JSON config file.

    Args:
        mapping_path (Path): Path to indicator_mapping.json.

    Returns:
        list[dict]: Each dict has keys 'indicateur_name', 'num_dx' (list[str]), 'den_dx' (list[str]).
    """
    if not mapping_path.exists():
        sibling_files = (
            sorted(p.name for p in mapping_path.parent.iterdir())
            if mapping_path.parent.exists()
            else ["<parent folder does not exist>"]
        )
        raise FileNotFoundError(
            f"indicator_mapping.json not found at {mapping_path}. "
            f"Files actually present in {mapping_path.parent}: {sibling_files}. "
            "If it's missing here, make sure the file exists on local disk at this exact path "
            "(not only in the OpenHEXA workspace file browser)."
        )

    with open(mapping_path, encoding="utf-8") as f:
        mapping = json.load(f)
    current_run.log_info(f"Loaded {len(mapping)} indicator definitions from {mapping_path}")
    return mapping


# TODO replace by utils.read_parquet_extract at this point only load it, no need to rename columns
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

# TODO instead of taking the periods from parameters, it should transform everything in the extract folder, 
# it also should not return anything, just recreate the files in the transform folder
# also add here the renaming of columns
def transform_period(
    period: str,
    extracts_path: Path,
    indicator_mapping: list[dict],
    fosa_names: pl.DataFrame,
    output_path: Path,
    fill_missing_with_zero: bool,
) -> Path | None:
    """Transforms a single period's NMDR extract into indicator numerator/denominator rows.

    Args:
        period (str): Period to transform, in YYYYMM format.
        extracts_path (Path): Path to the folder containing nmdr_extract_<period>.parquet files.
        indicator_mapping (list[dict]): Indicator definitions (name, num_dx, den_dx).
        fosa_names (pl.DataFrame): org_unit id -> fosa name lookup table.
        output_path (Path): Folder where nmdr_transform_<period>.parquet is written.
        fill_missing_with_zero (bool): Whether to fill missing num/den sums with 0.

    Returns:
        Path | None: Path to the written parquet file, or None if there was no extract for the period.
    """
    extract_file = extracts_path / f"nmdr_extract_{period}.parquet"
    if not extract_file.exists():
        current_run.log_info(f"No extract found for period {period} at {extract_file}. Skipping.")
        return None

    df = pl.read_parquet(extract_file).with_columns(pl.col("value").cast(pl.Float64, strict=False))

    all_org_units = df.select(["org_unit", "organisationUnitGroup"]).unique(subset=["org_unit"])
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

    if fill_missing_with_zero:
        result = result.with_columns(
            pl.col("indicateur_num").fill_null(0.0),
            pl.col("indicateur_den").fill_null(0.0),
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

    output_file = output_path / f"nmdr_transform_{period}.parquet"
    save_to_parquet(data=result, filename=output_file)
    current_run.log_info(f"NMDR transform for period {period} saved at {output_file}")
    return output_file


if __name__ == "__main__":
    dhis2_nmdr_sentinel_extract()