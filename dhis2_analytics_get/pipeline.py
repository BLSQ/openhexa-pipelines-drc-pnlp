import os
import shutil
from datetime import datetime, timedelta

import polars as pl
from openhexa.sdk import current_run, parameter, pipeline, workspace
from openhexa.toolbox.dhis2 import DHIS2


@pipeline("dhis2-analytics-get", name="DHIS2 Analytics")
@parameter(
    "output_dir",
    name="Output directory",
    help="Output directory where data extract will be saved",
    type=str,
    required=False,
)
@parameter(
    "data_elements",
    name="Data elements",
    help="UIDs of data elements",
    type=str,
    multiple=True,
    required=False,
)
@parameter(
    "data_element_groups",
    name="Data element groups",
    help="UIDs of data element groups",
    type=str,
    multiple=True,
    required=False,
)
@parameter(
    "indicators",
    name="Indicators",
    help="UIDs of indicators",
    type=str,
    multiple=True,
    required=False,
)
@parameter(
    "indicator_groups",
    name="Indicator groups",
    help="UIDs of indicator groups",
    type=str,
    multiple=True,
    required=False,
)
@parameter(
    "periods",
    name="Periods",
    help="DHIS2 periods",
    type=str,
    multiple=True,
    required=False,
)
@parameter(
    "start",
    name="Period (start)",
    help="Start of DHIS2 period range",
    type=str,
    required=False,
)
@parameter(
    "end",
    name="Period (end)",
    help="End of DHIS2 period range",
    type=str,
    required=False,
)
@parameter(
    "org_units",
    name="Organisation units",
    help="UIDs of organisation units",
    type=str,
    multiple=True,
    required=False,
)
@parameter(
    "org_unit_groups",
    name="Organisation unit groups",
    help="UIDs of organisation unit groups",
    type=str,
    multiple=True,
    required=False,
)
@parameter(
    "org_unit_levels",
    name="Organisation unit levels",
    help="Organisation unit levels",
    type=str,
    multiple=True,
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
def dhis2_analytics_get(
    output_dir=None,
    data_elements=None,
    data_element_groups=None,
    indicators=None,
    indicator_groups=None,
    periods=None,
    start=None,
    end=None,
    org_units=None,
    org_unit_groups=None,
    org_unit_levels=None,
    use_cache=True,
):
    if org_unit_levels:
        org_unit_levels = [int(level) for level in org_unit_levels]
    get(
        output_dir=output_dir,
        data_elements=data_elements,
        data_element_groups=data_element_groups,
        indicators=indicators,
        indicator_groups=indicator_groups,
        periods=periods,
        start=start,
        end=end,
        org_units=org_units,
        org_unit_groups=org_unit_groups,
        org_unit_levels=org_unit_levels,
        use_cache=use_cache,
    )


def clean_default_output_dir(output_dir: str):
    """Delete directories older than 1 month."""
    for d in os.listdir(output_dir):
        try:
            date = datetime.strptime(d, "%Y-%m-%d_%H:%M:%f")
        except ValueError:
            continue
        if datetime.now() - date > timedelta(days=31):
            shutil.rmtree(os.path.join(output_dir, d))


@dhis2_analytics_get.task
def get(
    output_dir=None,
    data_elements=None,
    data_element_groups=None,
    indicators=None,
    indicator_groups=None,
    periods=None,
    start=None,
    end=None,
    org_units=None,
    org_unit_groups=None,
    org_unit_levels=None,
    use_cache=True,
):
    con = workspace.dhis2_connection("drc-snis")

    if use_cache:
        cache_dir = os.path.join(workspace.files_path, ".cache")
    else:
        cache_dir = None

    dhis = DHIS2(con, cache_dir=cache_dir)
    current_run.log_info(f"Connected to {con.url}")

    if output_dir:
        output_dir = os.path.join(workspace.files_path, output_dir)
        os.makedirs(output_dir, exist_ok=True)
    else:
        default_basedir = os.path.join(
            workspace.files_path,
            "pipelines",
            "dhis2-analytics",
        )
        output_dir = os.path.join(
            default_basedir, datetime.now().strftime("%Y-%m-%d_%H:%M:%f")
        )
        os.makedirs(output_dir, exist_ok=True)
        clean_default_output_dir(default_basedir)

    # max elements per request
    dhis.analytics.MAX_DX = 50
    dhis.analytics.MAX_ORG_UNITS = 50
    dhis.analytics.MAX_PERIODS = 1

    data_values = dhis.analytics.get(
        data_elements=data_elements,
        data_element_groups=data_element_groups,
        indicators=indicators,
        indicator_groups=indicator_groups,
        periods=periods,
        org_units=org_units,
        org_unit_groups=org_unit_groups,
        org_unit_levels=org_unit_levels,
    )
    current_run.log_info(f"Extracted {len(data_values)} data values")

    df = pl.DataFrame(data_values)
    df = dhis.meta.add_dx_name_column(df)
    df = dhis.meta.add_coc_name_column(df)
    df = dhis.meta.add_org_unit_name_column(df)
    df = dhis.meta.add_org_unit_parent_columns(df)

    fp = os.path.join(output_dir, "analytics.csv")
    df.write_csv(fp)
    current_run.add_file_output(fp)

    return


if __name__ == "__main__":
    dhis2_analytics_get()
