import os
from datetime import datetime, timedelta
import shutil

import polars as pl
from openhexa.sdk import current_run, parameter, pipeline, workspace
from openhexa.toolbox.dhis2 import DHIS2


@pipeline("dhis2-extract-metadata", name="DHIS2 Metadata")
@parameter(
    "get_org_units",
    name="Organisation units",
    help="Extract organisation units metadata",
    type=bool,
    default=False,
    required=False,
)
@parameter(
    "get_org_unit_levels",
    name="Organisation unit levels",
    help="Extract organisation unit levels metadata",
    type=bool,
    default=False,
    required=False,
)
@parameter(
    "get_org_unit_groups",
    name="Organisation unit groups",
    help="Extract organisation unit groups metadata",
    type=bool,
    default=False,
    required=False,
)
@parameter(
    "get_datasets",
    name="Datasets",
    help="Extract datasets metadata",
    type=bool,
    default=False,
    required=False,
)
@parameter(
    "get_data_elements",
    name="Data elements",
    help="Extract data elements metadata",
    type=bool,
    default=False,
    required=False,
)
@parameter(
    "get_data_element_groups",
    name="Data element groups",
    help="Extract data element groups metadata",
    type=bool,
    default=False,
    required=False,
)
@parameter(
    "get_indicators",
    name="Indicators",
    help="Extract indicators metadata",
    type=bool,
    default=False,
    required=False,
)
@parameter(
    "get_indicator_groups",
    name="Indicator groups",
    help="Extract indicator groups metadata",
    type=bool,
    default=False,
    required=False,
)
@parameter(
    "get_coc",
    name="Category option combos",
    help="Extract category option combos",
    type=bool,
    default=False,
    required=False,
)
@parameter(
    "use_cache",
    name="Use cache",
    help="Use cache if possible (NB: data might be outdated)",
    type=bool,
    default=True,
    required=False,
)
@parameter(
    "output_dir",
    name="Output directory",
    help="Output directory where metadata files are saved",
    type=str,
    required=False,
)
def dhis2_extract_metadata(
    output_dir: str = None,
    get_org_units: bool = False,
    get_org_unit_levels: bool = False,
    get_org_unit_groups: bool = False,
    get_datasets: bool = False,
    get_data_elements: bool = False,
    get_data_element_groups: bool = False,
    get_indicators: bool = False,
    get_indicator_groups: bool = False,
    get_coc: bool = False,
    use_cache: bool = True,
):
    get_metadata(
        output_dir,
        get_org_units,
        get_org_unit_levels,
        get_org_unit_groups,
        get_datasets,
        get_data_elements,
        get_data_element_groups,
        get_indicators,
        get_indicator_groups,
        get_coc,
        use_cache,
    )


def join_lists(df: pl.DataFrame) -> pl.DataFrame:
    """Transform list values into comma-separated strings for compatibility with CSV."""
    for col in df.columns:
        if df[col].dtype == pl.List():
            df = df.with_columns(
                pl.when(pl.col(col).list.lengths() > 0)
                .then(pl.col(col).list.join(", ").alias(col))
                .otherwise(pl.lit(None))
            )
    return df


def clean_default_output_dir(output_dir: str):
    """Delete directories older than 1 month."""
    for d in os.listdir(output_dir):
        try:
            date = datetime.strptime(d, "%Y-%m-%d_%H:%M:%f")
        except ValueError:
            continue
        if datetime.now() - date > timedelta(days=31):
            shutil.rmtree(os.path.join(output_dir, d))


@dhis2_extract_metadata.task
def get_metadata(
    output_dir: str,
    get_org_units: bool = False,
    get_org_unit_levels: bool = False,
    get_org_unit_groups: bool = False,
    get_datasets: bool = False,
    get_data_elements: bool = False,
    get_data_element_groups: bool = False,
    get_indicators: bool = False,
    get_indicator_groups: bool = False,
    get_coc: bool = False,
    use_cache: bool = True,
):
    con = workspace.dhis2_connection("drc-snis")

    if use_cache:
        cache_dir = os.path.join(workspace.files_path, ".cache")
    else:
        cache_dir = None

    dhis = DHIS2(con, cache_dir=cache_dir)
    current_run.log_info(f"Connected to {con.url}")

    if output_dir:
        os.makedirs(output_dir, exist_ok=True)
    else:
        default_basedir = os.path.join(
            workspace.files_path,
            "pipelines",
            "dhis2-extract-metadata",
        )
        output_dir = os.path.join(
            default_basedir, datetime.now().strftime("%Y-%m-%d_%H:%M:%f")
        )
        os.makedirs(output_dir, exist_ok=True)
        clean_default_output_dir(default_basedir)

    if get_org_units:
        df = pl.DataFrame(dhis.meta.organisation_units())
        df = dhis.meta.add_org_unit_parent_columns(df, org_unit_id_column="id")
        df = df.select(
            ["id", "name", "level"]
            + [col for col in df.columns if col.startswith("parent")]
            + ["geometry"]
        )
        fp = os.path.join(output_dir, "organisation_units.csv")
        df.write_csv(fp)
        current_run.add_file_output(fp)
        current_run.log_info(f"Extracted metadata for {len(df)} organisation units")

    if get_org_unit_levels:
        df = pl.DataFrame(dhis.meta.organisation_unit_levels())
        fp = os.path.join(output_dir, "organisation_unit_levels.csv")
        df.write_csv(fp)
        current_run.add_file_output(fp)
        current_run.log_info(
            f"Extracted metadata for {len(df)} organisation unit levels"
        )

    if get_org_unit_groups:
        df = pl.DataFrame(dhis.meta.organisation_unit_groups())
        df = join_lists(df)
        fp = os.path.join(output_dir, "organisation_unit_groups.csv")
        df.write_csv(fp)
        current_run.add_file_output(fp)
        current_run.log_info(
            f"Extracted metadata for {len(df)} organisation unit groups"
        )

    if get_datasets:
        df = pl.DataFrame(dhis.meta.datasets())
        df = join_lists(df)
        fp = os.path.join(output_dir, "datasets.csv")
        df.write_csv(fp)
        current_run.add_file_output(fp)
        current_run.log_info(f"Extracted metadata for {len(df)} datasets")

    if get_data_elements:
        df = pl.DataFrame(dhis.meta.data_elements())
        fp = os.path.join(output_dir, "data_elements.csv")
        df.write_csv(fp)
        current_run.add_file_output(fp)
        current_run.log_info(f"Extracted metadata for {len(df)} data elements")

    if get_data_element_groups:
        df = pl.DataFrame(dhis.meta.data_element_groups())
        df = join_lists(df)
        fp = os.path.join(output_dir, "data_element_groups.csv")
        df.write_csv(fp)
        current_run.add_file_output(fp)
        current_run.log_info(f"Extracted metadata for {len(df)} data element groups")

    if get_indicators:
        df = pl.DataFrame(dhis.meta.indicators())
        fp = os.path.join(output_dir, "indicators.csv")
        df.write_csv(fp)
        current_run.add_file_output(fp)
        current_run.log_info(f"Extracted metadata for {len(df)} indicators")

    if get_indicator_groups:
        df = pl.DataFrame(dhis.meta.indicator_groups())
        df = join_lists(df)
        fp = os.path.join(output_dir, "indicator_groups.csv")
        df.write_csv(fp)
        current_run.add_file_output(fp)
        current_run.log_info(f"Extracted metadata for {len(df)} indicator groups")

    if get_coc:
        df = pl.DataFrame(dhis.meta.category_option_combos())
        df = join_lists(df)
        fp = os.path.join(output_dir, "category_option_combos.csv")
        df.write_csv(fp)
        current_run.add_file_output(fp)
        current_run.log_info(f"Extracted metadata for {len(df)} category option combos")

    return


if __name__ == "__main__":
    dhis2_extract_metadata()
