import json
from pathlib import Path

import polars as pl
from openhexa.sdk import current_run, parameter, pipeline, workspace

# Reused from the dhis2_nmdr_sentinel_extract pipeline's utils module, since both
# pipelines live under the same workspace and share date-handling helpers.
# If `utils` is not importable from here, copy get_extract_periods / resolve_dates_and_validate
# into this pipeline's own utils, or adjust the import path below.
from utils import get_extract_periods, resolve_dates_and_validate, save_to_parquet


@pipeline("dhis2_nmdr_sentinel_transform", timeout=3600)  # 1 hour
@parameter(
    code="start_period",
    name="Start period (format: YYYYMM)",
    default="202605",
    type=str,
    required=False,
    help="First period to transform, in YYYYMM format.",
)
@parameter(
    code="end_period",
    name="End period (format: YYYYMM)",
    default="202605",
    type=str,
    required=False,
    help="Last period to transform, in YYYYMM format. If equal to start_period, only one period is processed.",
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
def dhis2_nmdr_sentinel_transform(start_period: str, end_period: str, fill_missing_with_zero: bool):
    """Transforms NMDR sentinel DHIS2 extracts into indicator-level numerator/denominator values.

    Reads the nmdr_extract_<period>.parquet file(s) produced by the dhis2_nmdr_sentinel_extract
    pipeline, computes the numerator and denominator for each indicator defined in
    config/indicator_mapping.json, and writes one nmdr_transform_<period>.parquet file per period.

    Args:
        start_period (str): First period to transform, in YYYYMM format.
        end_period (str): Last period to transform, in YYYYMM format.
        fill_missing_with_zero (bool): Whether to fill missing num/den sums with 0 instead of null.
    """
    # For now, everything (config + data) lives under the dhis2_nmdr_sentinel_extract
    # pipeline folder, same as the extract pipeline itself. If these two pipelines are
    # ever split apart, only this block needs to change.
    pipelines_root = Path(workspace.files_path) / "pipelines"
    pipeline_path = pipelines_root / "dhis2_nmdr_sentinel_extract"

    extracts_path = pipeline_path / "data" / "nmdr_extracts"
    pyramid_path = pipeline_path / "data" / "pyramid_metadata" / "nmdr_pyramid_metadata.parquet"
    output_path = pipeline_path / "data" / "nmdr_transforms"
    mapping_path = pipeline_path / "config" / "indicator_mapping.json"

    try:
        start, end = resolve_dates_and_validate(start_period, end_period, config=None)
        periods = get_extract_periods(start, end)
    except Exception:
        # Fallback if resolve_dates_and_validate strictly requires a config dict.
        periods = sorted({start_period, end_period}) if start_period != end_period else [start_period]

    current_run.log_info(f"Transforming NMDR sentinel data for periods: {periods}")

    indicator_mapping = load_indicator_mapping(mapping_path)
    fosa_names = load_fosa_names(pyramid_path)

    output_path.mkdir(parents=True, exist_ok=True)
    transformed_files = []

    for period in periods:
        try:
            transformed_path = transform_period(
                period=period,
                extracts_path=extracts_path,
                indicator_mapping=indicator_mapping,
                fosa_names=fosa_names,
                output_path=output_path,
                fill_missing_with_zero=fill_missing_with_zero,
            )
            if transformed_path:
                transformed_files.append(transformed_path)
        except Exception as e:
            current_run.log_error(f"An error occurred while transforming period {period}: {e}")
            raise

    current_run.log_info(f"Transform finished. Files created: {transformed_files}")


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


GEO_SCHEMA = {
    "org_unit": pl.String,
    "fosa": pl.String,
    "province": pl.String,
    "zone_de_sante": pl.String,
    "aire_de_sante": pl.String,
}


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

    all_org_units = df.select(pl.col("org_unit").unique())
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
        "indicateur_num",
        "indicateur_den",
    )

    output_file = output_path / f"nmdr_transform_{period}.parquet"
    save_to_parquet(data=result, filename=output_file)
    current_run.log_info(f"NMDR transform for period {period} saved at {output_file}")
    return output_file


if __name__ == "__main__":
    dhis2_nmdr_sentinel_transform()