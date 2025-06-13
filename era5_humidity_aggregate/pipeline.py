import os
import tempfile
import zipfile
from io import BytesIO
from pathlib import Path
from datetime import datetime

from epiweeks import Week
import geopandas as gpd
import polars as pl
import xarray as xr
import pandas as pd
import numpy as np
from openhexa.sdk import Dataset, current_run, parameter, pipeline, workspace
from openhexa.sdk.datasets import DatasetFile
from sqlalchemy import create_engine
from openhexa.toolbox.era5.aggregate import (
    build_masks,
    get_transform,
)


@pipeline("era5_relative_humidity_aggregate")
@parameter(
    "output_dir",
    type=str,
    name="Output directory",
    help="Output directory for the aggregated data",
    default="pipelines/era5_relative_humidity_aggregate/aggregate",
)
def era5_relative_humidity_aggregate(
    output_dir: str,
):
    # fixed input directory for obvious reasons
    current_run.log_info("Starting ERA5 relative humidity aggregation pipeline")
    input_dir = Path(workspace.files_path) / "pipelines" / "era5_relative_humidity_extract" / "data" / "raw"
    output_dir = Path(workspace.files_path, output_dir)

    # subdirs containing raw data are named after variable names
    subdirs = [d for d in input_dir.iterdir() if d.is_dir()]
    variables = [d.name for d in subdirs if d.name in ["relative_humidity"]]

    if not variables:
        msg = "No variables found in input directory"
        current_run.log_error(msg)
        raise FileNotFoundError(msg)

    # load boundaries
    # boundaries = load_boundaries(db_table="cod_iaso_zone_de_sante")
    boundaries = read_boundaries(workspace.get_dataset("zones-de-sante-boundaries"), "zs_boundaries.gpkg")

    df_monthly = aggregate_monthly_humidity_data(
        input_dir=input_dir,
        boundaries=boundaries,
        dst_file=output_dir / "relative_humidity_monthly.parquet",  # ).as_posix(),
    )

    # upload to table
    upload_data_to_table(df=df_monthly, targetTable="cod_relative_humidity_monthly_auto")

    # update dataset (if required: create DS and update the DS identifier in this function).
    # update_humidity_dataset(df=df_monthly)

    current_run.log_info("Monthly humidity data table updated")


def load_boundaries(db_table: dict) -> gpd.GeoDataFrame:
    """Load boundaries from database."""
    current_run.log_info(f"Loading boundaries from {db_table}")
    dbengine = create_engine(os.environ["WORKSPACE_DATABASE_URL"])
    boundaries = gpd.read_postgis(f"SELECT * FROM {db_table}", con=dbengine, geom_col="geometry")
    return boundaries


def aggregate_monthly_humidity_data(
    input_dir: Path,
    boundaries: gpd.GeoDataFrame,
    dst_file: Path,
) -> pd.DataFrame:
    """Aggregate monthly relative humidity data."""
    current_run.log_info("Aggregating monthly relative humidity data")

    # merge all available files
    ds_merged = get_merged_dataset(input_dir / "relative_humidity")

    # monthly aggregation
    df_monthly = spatial_aggregation(
        ds=ds_merged,
        dst_file=dst_file,
        boundaries=boundaries,
    )

    return df_monthly


def upload_data_to_table(df: pd.DataFrame, targetTable: str):
    """Upload the processed precipitation data stats to target table."""

    current_run.log_info(f"Updating table : {targetTable}")

    # Create engine
    dbengine = create_engine(os.environ["WORKSPACE_DATABASE_URL"])

    # Create table
    df.to_pandas().to_sql(targetTable, dbengine, index=False, if_exists="replace", chunksize=4096)

    del dbengine


def update_humidity_dataset(df: pd.DataFrame):
    """Update the humidity dataset to be shared."""

    current_run.log_info("Updating humidity dataset")

    # Get the dataset
    dataset = workspace.get_dataset("")  # SET THE ID #
    date_version = f"ds_{datetime.now().strftime('%Y_%m_%d_%H%M')}"
    added_new = False

    try:
        with tempfile.NamedTemporaryFile(suffix=".parquet") as tmp:
            if not added_new:
                # Create new DS version
                version = dataset.create_version(date_version)
                current_run.log_info(f"New dataset version {date_version} created")
                added_new = True
            # Add Precipitation .parquet to DS
            df.to_parquet(tmp.name)
            version.add_file(tmp.name, filename=f"Precipitation_{date_version}.parquet")
    except Exception as e:
        current_run.log_error(f"Dataset file cannot be saved - ERROR: {e}")
        raise

    current_run.log_info(f"New dataset version {date_version} created")


def read_boundaries(boundaries_dataset: Dataset, filename: str | None = None) -> gpd.GeoDataFrame:
    """Read boundaries geographic file from input dataset.

    Parameters
    ----------
    boundaries_dataset : Dataset
        Input dataset containing a "*district*.parquet" geoparquet file
    filename : str
        Filename of the boundaries file to read if there are several.
        If set to None, the 1st parquet file found will be loaded.

    Return
    ------
    gpd.GeoDataFrame
        Geopandas GeoDataFrame containing boundaries geometries

    Raises
    ------
    FileNotFoundError
        If the boundaries file is not found
    """
    ds = boundaries_dataset.latest_version

    ds_file: DatasetFile | None = None
    for f in ds.files:
        if f.filename == filename:
            if f.filename.endswith(".parquet"):
                ds_file = f
            if f.filename.endswith(".geojson") or f.filename.endswith(".gpkg"):
                ds_file = f

    if ds_file is None:
        msg = f"File {filename} not found in dataset {ds.name}"
        current_run.log_error(msg)
        raise FileNotFoundError(msg)

    if ds_file.filename.endswith(".parquet"):
        return gpd.read_parquet(BytesIO(ds_file.read()))

    return gpd.read_file(BytesIO(ds_file.read()))


def get_merged_dataset(input_dir: Path) -> pl.DataFrame:
    datasets = []

    for file in input_dir.glob("*.grib"):
        if zipfile.is_zipfile(file):
            with zipfile.ZipFile(file, "r") as zipf:
                data = zipf.read("data.grib")
                with tempfile.NamedTemporaryFile(suffix=".grib") as tmp:
                    tmp.write(data)
                    tmp.flush()
                    ds = xr.open_dataset(tmp.name, engine="cfgrib")
                    datasets.append(ds.load())
        else:
            ds = xr.open_dataset(file, engine="cfgrib")
            datasets.append(ds.load())

    humidity_concat = xr.concat(datasets, dim="time")
    humidity_concat["r"] = xr.where(humidity_concat.r > 100, 100, humidity_concat.r)  # cap humidity at 100%

    return humidity_concat


def spatial_aggregation(
    ds: xr.Dataset,
    dst_file: Path,
    boundaries: gpd.GeoDataFrame,
) -> pd.DataFrame:
    """Apply spatial aggregation on dataset based on a set of boundaries.

    Final value for each boundary is equal to the mean of all cells
    intersecting the shape.

    Parameters
    ----------
    ds : xr.Dataset
        Input dataset of shape (height, width, n_time_steps)
    dst_file : Path
        Path to output file
    boundaries : gpd.GeoDataFrame
        Input boundaries

    Return
    ------
    dataframe
        Mean value as a dataframe of length (n_boundaries * n_time_steps)
    """
    current_run.log_info("Running spatial aggregation ...")

    df = _spatial_aggregation(
        ds=ds,
        boundaries=boundaries,
    )

    df = pnlp_formatting(df, boundaries)

    current_run.log_info(f"Applied spatial aggregation for {len(boundaries)} boundaries")

    # fs = filesystem(dst_file)
    # with tempfile.NamedTemporaryFile(suffix=".parquet") as tmp:
    df.write_parquet(dst_file)
    # fs.put(tmp.name, dst_file)
    current_run.add_file_output(dst_file.as_posix())

    return df


def pnlp_formatting(df: pl.DataFrame, boundaries: gpd.GeoDataFrame) -> pl.DataFrame:
    """Format the dataframe to match the expected format for the PNLp platform.

    Parameters
    ----------
    df : pl.DataFrame
        Input dataframe with aggregated values
    boundaries : gpd.GeoDataFrame
        Input boundaries
    Return
    ------
    pd.DataFrame
        Formatted dataframe with boundaries information and aggregated values
    """
    # boundaries parent_ref , parent_name
    df = df.select(["boundary_id", "date", "mean", "min", "max", "month", "epi_week"])

    df = df.rename({"epi_week": "epi_week_source"})
    df = df.with_columns(
        [
            pl.col("epi_week_source").str.split("W").list.get(0).cast(pl.Int32).alias("epi_year"),
            pl.col("epi_week_source").str.split("W").list.get(1).cast(pl.Int32).alias("epi_week"),
            pl.col("date").cast(pl.Utf8).alias("period"),
        ]
    )
    df = df.drop(["date", "epi_week_source"])

    # add extra name columns
    df_parent = pl.from_pandas(
        boundaries[
            [
                "ref",
                "name",
                "parent_ref",
                "parent",
            ]
        ]
    )
    df = df.join(df_parent, left_on="boundary_id", right_on="ref", how="left")

    return df.rename({"boundary_id": "ref"})


def _spatial_aggregation(
    ds: xr.Dataset,
    boundaries: gpd.GeoDataFrame,
) -> pd.DataFrame:
    """Apply spatial aggregation on dataset based on a set of boundaries.

    Final value for each boundary is equal to the mean of all cells
    intersecting the shape.

    Parameters
    ----------
    ds : xr.Dataset
        Input dataset of shape (height, width, n_time_steps)
    boundaries : gpd.GeoDataFrame
        Input boundaries

    Return
    ------
    dataframe
        Mean value as a dataframe of length (n_boundaries * n_time_steps)
    """

    ncols = len(ds.longitude)
    nrows = len(ds.latitude)
    transform = get_transform(ds)

    # build binary raster masks for each boundary geometry for spatial aggregation
    areas = build_masks(boundaries, nrows, ncols, transform)

    monthly_df = aggregate_humidity(ds=ds, var="r", masks=areas, boundaries_id=boundaries["ref"])

    # var = [v for v in ds.data_vars][0]

    # records = []
    # days = [day for day in ds.time.values]

    # for day in days:
    #     measurements = ds.sel(time=day)
    #     measurements = measurements[var].values

    #     for i, (_, row) in enumerate(boundaries.iterrows()):
    #         count_val = np.sum(areas[i, :, :])
    #         sum_val = np.nansum(measurements[(measurements >= 0) & (areas[i, :, :])])  #
    #         new_stats = pd.Series([str(day)[:10], count_val, sum_val], index=["period", "count", "sum"])
    #         records.append(pd.concat([row, new_stats]))

    # records = pd.DataFrame(records)
    # if "geometry" in records.columns:
    #     records = records.drop(columns=["geometry"])

    return monthly_df


def aggregate_humidity(ds: xr.Dataset, var: str, masks: np.ndarray, boundaries_id: list[str]) -> pl.DataFrame:
    rows = []
    for month in ds.time.values:
        da = ds[var].sel(time=month)

        if "step" in da.dims:
            da_mean = da.mean(dim="step").values
            da_min = da.min(dim="step").values
            da_max = da.max(dim="step").values
        else:
            da_mean = da.values
            da_min = da.values
            da_max = da.values

        for i, uid in enumerate(boundaries_id):
            v_mean = np.nanmean(da_mean[masks[i, :, :]])
            v_min = np.nanmin(da_min[masks[i, :, :]])
            v_max = np.nanmax(da_max[masks[i, :, :]])

            rows.append(
                {
                    "boundary_id": uid,
                    "date": _np_to_datetime(month).date(),
                    "mean": v_mean,
                    "min": v_min,
                    "max": v_max,
                }
            )

    SCHEMA = {
        "boundary_id": pl.String,
        "date": pl.Date,
        "mean": pl.Float64,
        "min": pl.Float64,
        "max": pl.Float64,
    }

    df = pl.DataFrame(data=rows, schema=SCHEMA)

    # add week, month, and epi_week period columns
    df = df.with_columns(
        pl.col("date").map_elements(_week, str).alias("week"),
        pl.col("date").map_elements(_month, str).alias("month"),
        pl.col("date").map_elements(_epi_week, str).alias("epi_week"),
    )

    return df


def _np_to_datetime(dt64: np.datetime64) -> datetime:
    epoch = np.datetime64(0, "s")
    one_second = np.timedelta64(1, "s")
    seconds_since_epoch = (dt64 - epoch) / one_second
    return datetime.fromtimestamp(seconds_since_epoch)


def _week(date: datetime) -> str:
    year = date.isocalendar()[0]
    week = date.isocalendar()[1]
    return f"{year}W{week}"


def _epi_week(date: datetime) -> str:
    epiweek = Week.fromdate(date)
    year = epiweek.year
    week = epiweek.week
    return f"{year}W{week}"


def _month(date: datetime) -> str:
    return date.strftime("%Y%m")


if __name__ == "__main__":
    era5_relative_humidity_aggregate()
