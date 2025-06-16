import os
import tempfile
import zipfile
from io import BytesIO
from pathlib import Path
from shutil import copyfile
from datetime import datetime

import geopandas as gpd
import polars as pl
import xarray as xr
import pandas as pd
import numpy as np
from openhexa.sdk import Dataset, current_run, parameter, pipeline, workspace
from openhexa.sdk.datasets import DatasetFile
from sqlalchemy import create_engine
from epiweek import EpiWeek
import fsspec
from openhexa.toolbox.era5.aggregate import (
    build_masks,
    get_transform,
    merge,
)
from openhexa.toolbox.era5.cds import VARIABLES


@pipeline("ERA5_precipitation_aggregate")
@parameter(
    "input_dir",
    type=str,
    name="Input directory",
    help="Input directory with raw ERA5 extracts",
    default="data/era5/raw",
)
@parameter(
    "output_dir",
    type=str,
    name="Output directory",
    help="Output directory for the aggregated data",
    default="data/era5/aggregate",
)
def era5_aggregate(
    input_dir: str,
    output_dir: str,
):
    input_dir = Path(workspace.files_path, input_dir)
    output_dir = Path(workspace.files_path, output_dir)

    # subdirs containing raw data are named after variable names
    subdirs = [d for d in input_dir.iterdir() if d.is_dir()]
    variables = [d.name for d in subdirs if d.name in VARIABLES.keys()]

    if not variables:
        msg = "No variables found in input directory"
        current_run.log_error(msg)
        raise FileNotFoundError(msg)

    # load boundaries
    # boundaries = load_boundaries(db_table="cod_iaso_zone_de_sante")
    boundaries = read_boundaries(workspace.get_dataset("zones-de-sante-boundaries"), "zs_boundaries.gpkg")
    # boundaries = gpd.read_file(Path(workspace.files_path) / "zs_boundaries.gpkg")  # local tests

    df_daily = aggregate_ERA5_data_daily(
        input_dir=input_dir,
        boundaries=boundaries,
        output_dir=output_dir,
    )

    df_weekly = weekly(
        df=df_daily,
        dst_file=(output_dir / "total_precipitation_weekly.parquet").as_posix(),
    )

    df_monthly = monthly(
        df=df_daily,
        dst_file=(output_dir / "total_precipitation_monthly.parquet").as_posix(),
    )

    # upload to table
    upload_data_to_table(df=df_weekly, targetTable="cod_precipitation_weekly_auto")
    # upload_data_to_table(df=df_monthly, targetTable="cod_precipitation_monthly_auto")

    # update dataset -- we only do this for the weekly data.
    update_precipitation_dataset(df=df_weekly)

    current_run.log_info("Precipitation data table updated")


def load_boundaries(db_table: dict) -> gpd.GeoDataFrame:
    """Load boundaries from database."""
    current_run.log_info(f"Loading boundaries from {db_table}")
    dbengine = create_engine(os.environ["WORKSPACE_DATABASE_URL"])
    boundaries = gpd.read_postgis(db_table, con=dbengine, geom_col="geometry")
    return boundaries


def aggregate_ERA5_data_daily(
    input_dir: Path,
    boundaries: gpd.GeoDataFrame,
    output_dir: Path,
    variable: str = "total_precipitation",
) -> pl.DataFrame:
    # merge all available files
    ds_merged = get_merged_dataset(input_dir / variable)

    ds_merged = ds_merged * 1000  # convert m to mm

    # daily aggregation
    df_daily = spatial_aggregation(
        ds=ds_merged,
        dst_file=Path(output_dir).joinpath(f"{variable}_daily.parquet").as_posix(),
        boundaries=boundaries,
    )

    return df_daily


def upload_data_to_table(df: pd.DataFrame, targetTable: str):
    """Upload the processed precipitation data stats to target table."""

    current_run.log_info(f"Updating table : {targetTable}")

    # Create engine
    dbengine = create_engine(os.environ["WORKSPACE_DATABASE_URL"])

    # Create table
    df.to_sql(targetTable, dbengine, index=False, if_exists="replace", chunksize=4096)

    del dbengine


def update_precipitation_dataset(df: pd.DataFrame):
    """Update the precipitation dataset to be shared."""

    current_run.log_info("Updating precipitation dataset")

    # Get the dataset
    dataset = workspace.get_dataset("climate-dataset-precipi-6349a3")
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
    # build xarray dataset by merging all available grib files across the time dimension
    with tempfile.TemporaryDirectory() as tmpdir:
        for file in input_dir.glob("*.grib"):
            # if the .grib file is actually a zip file, extract the data.grib file
            # and copy its content instead with same filename
            if zipfile.is_zipfile(file):
                with zipfile.ZipFile(file.as_posix(), "r") as zip:
                    bytes = zip.read("data.grib")
                with open(Path(tmpdir, file.name), "wb") as f:
                    f.write(bytes)

            # if it's not a zip file, copy the grib file directly
            else:
                copyfile(src=file.as_posix(), dst=Path(tmpdir, file.name).as_posix())

        ds = merge(Path(tmpdir))

        # remove all non-precipitation variables.. merge is selecting maximum value
        # ds = ds.dropna(dim="time", subset=["tp"])  # for temperature variable is called "t2m"

        # remove duplicated (if any) dates and sort
        ds = ds.drop_duplicates(dim="time")
        ds = ds.sortby("time")

    return ds


def spatial_aggregation(
    ds: xr.Dataset,
    dst_file: str,
    boundaries: gpd.GeoDataFrame,
) -> pd.DataFrame:
    """Apply spatial aggregation on dataset based on a set of boundaries.

    Final value for each boundary is equal to the mean of all cells
    intersecting the shape.

    Parameters
    ----------
    ds : xr.Dataset
        Input dataset of shape (height, width, n_time_steps)
    dst_file : str
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

    current_run.log_info(f"Applied spatial aggregation for {len(boundaries)} boundaries")

    fs = filesystem(dst_file)
    with tempfile.NamedTemporaryFile(suffix=".parquet") as tmp:
        df.to_parquet(tmp.name)
        fs.put(tmp.name, dst_file)
    current_run.add_file_output(dst_file)

    return df


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

    var = [v for v in ds.data_vars][0]

    records = []
    days = [day for day in ds.time.values]

    for day in days:
        measurements = ds.sel(time=day)
        measurements = measurements[var].values

        for i, (_, row) in enumerate(boundaries.iterrows()):
            count_val = np.sum(areas[i, :, :])
            sum_val = np.nansum(measurements[(measurements >= 0) & (areas[i, :, :])])  #
            new_stats = pd.Series([str(day)[:10], count_val, sum_val], index=["period", "count", "sum"])
            records.append(pd.concat([row, new_stats]))

    records = pd.DataFrame(records)
    if "geometry" in records.columns:
        records = records.drop(columns=["geometry"])

    return records


def filesystem(target_path: str, cache_dir: str = None) -> fsspec.AbstractFileSystem:
    """Guess filesystem based on path.

    Parameters
    ----------
    target_path : str
        Target file path starting with protocol.
    cache_dir : bool, optional
        Cache remote files locally

    Return
    ------
    AbstractFileSystem
        Local, S3, or GCS filesystem. WholeFileCacheFileSystem if
        cache=True.
    """
    if "://" in target_path:
        target_protocol = target_path.split("://")[0]
    else:
        target_protocol = "file"

    if target_protocol not in ("file", "s3", "gcs"):
        raise ValueError(f"Protocol {target_protocol} not supported.")

    client_kwargs = {}
    if target_protocol == "s3":
        client_kwargs = {"endpoint_url": os.environ.get("AWS_S3_ENDPOINT")}

    if cache_dir:
        return fsspec.filesystem(
            protocol="filecache",
            target_protocol=target_protocol,
            target_options={"client_kwargs": client_kwargs},
            cache_storage=cache_dir,
        )
    else:
        return fsspec.filesystem(protocol=target_protocol, client_kwargs=client_kwargs)


def weekly(df: pd.DataFrame, dst_file: str) -> pd.DataFrame:
    """Get weekly precipation from daily dataset."""
    df_weekly = get_weekly_aggregates(df)
    current_run.log_info(f"Applied weekly aggregation ({len(df_weekly)} measurements)")
    fs = filesystem(dst_file)
    with tempfile.NamedTemporaryFile(suffix=".parquet") as tmp:
        df_weekly.to_parquet(tmp.name)
        fs.put(tmp.name, dst_file)
    current_run.add_file_output(dst_file)
    return df_weekly


def monthly(df: pd.DataFrame, dst_file: str) -> pd.DataFrame:
    """Get monthly precipation from daily dataset."""
    df_monthly = get_monthly_aggregates(df)
    current_run.log_info(f"Applied montly aggregation ({len(df_monthly)} measurements)")
    fs = filesystem(dst_file)
    with tempfile.NamedTemporaryFile(suffix=".parquet") as tmp:
        df_monthly.to_parquet(tmp.name)
        fs.put(tmp.name, dst_file)
    current_run.add_file_output(dst_file)
    return df_monthly


def get_weekly_aggregates(df: pd.DataFrame) -> pd.DataFrame:
    """Apply weekly aggregation of input daily dataframe.

    Uses epidemiological weeks and assumes at least 4 columns in input
    dataframe: ref, period, count and sum.

    Parameters
    ----------
    df : dataframe
        Input dataframe

    Return
    ------
    dataframe
        Weekly dataframe of length (n_features * n_weeks)
    """

    df_ = df.copy()
    df_["epi_year"] = df_["period"].apply(lambda day: EpiWeek(datetime.strptime(day, "%Y-%m-%d")).year).astype(int)
    df_["epi_week"] = df_["period"].apply(lambda day: EpiWeek(datetime.strptime(day, "%Y-%m-%d")).week).astype(int)

    # Epiweek start end dates
    df_["start_date"] = df_["period"].apply(lambda day: EpiWeek(datetime.strptime(day, "%Y-%m-%d")).start).astype(str)
    df_["end_date"] = df_["period"].apply(lambda day: EpiWeek(datetime.strptime(day, "%Y-%m-%d")).end).astype(str)
    df_["mid_date"] = df_["period"].apply(lambda day: _compute_mid_date(EpiWeek(datetime.strptime(day, "%Y-%m-%d"))))

    # epiweek aggregation sum
    sums = df_.groupby(by=["ref", "epi_year", "epi_week"])[["sum"]].sum().reset_index()
    data_left = df_.copy()
    data_left["period"] = data_left["period"].apply(lambda day: str(EpiWeek(datetime.strptime(day, "%Y-%m-%d"))))
    data_left = data_left.drop(columns=["sum"])
    data_left = data_left.drop_duplicates(subset=data_left.columns)  # unique rows

    # merge
    merged_df = pd.merge(data_left, sums, on=["ref", "epi_year", "epi_week"], how="left")
    merged_df["mid_date"] = pd.to_datetime(merged_df["mid_date"])

    # dummy columns to keep the format
    merged_df["group_refs"] = None
    merged_df["group_names"] = None
    merged_df["uuid"] = None

    # fix format
    merged_df = merged_df[
        [
            "name_short",
            "ref",
            "parent_short",
            "parent_ref",
            "group_refs",
            "group_names",
            "uuid",
            "name",
            "parent",
            "period",
            "count",
            "epi_year",
            "epi_week",
            "start_date",
            "end_date",
            "mid_date",
            "sum",
        ]
    ]

    return merged_df


def get_monthly_aggregates(df: pd.DataFrame) -> pd.DataFrame:
    """Apply montly aggregation of input daily dataframe.

    Parameters
    ----------
    df : dataframe
        Input dataframe

    Return
    ------
    dataframe
        Monthly dataframe of length (n_features * n_months)
    """
    df_ = df.copy()
    df_["period"] = pd.to_datetime(df_["period"])
    df_["year"] = df_["period"].dt.year
    df_["month"] = df_["period"].dt.month

    # Start and end dates
    df_["start_date"] = df_["period"].values.astype("datetime64[M]")
    df_["end_date"] = df_["start_date"] + pd.offsets.MonthEnd(0)
    df_["mid_date"] = df_["start_date"] + pd.Timedelta(days=14)

    # monthly aggregation sum
    sums = df_.groupby(by=["ref", "year", "month"])[["sum"]].sum().reset_index()
    data_left = df_.copy()
    data_left["period"] = pd.to_datetime(data_left["period"]).dt.strftime("%Y-%m")
    data_left = data_left.drop(columns=["sum"])
    data_left = data_left.drop_duplicates(subset=data_left.columns)  # unique rows

    # merge
    merged_df = pd.merge(data_left, sums, on=["ref", "year", "month"], how="left")

    # dummy columns to keep the format
    merged_df["group_refs"] = None
    merged_df["group_names"] = None
    merged_df["uuid"] = None

    # fix format
    merged_df = merged_df[
        [
            "name_short",
            "ref",
            "parent_short",
            "parent_ref",
            "group_refs",
            "group_names",
            "uuid",
            "name",
            "parent",
            "period",
            "count",
            "year",
            "month",
            "start_date",
            "end_date",
            "mid_date",
            "sum",
        ]
    ]

    return merged_df


def _compute_mid_date(epiweek: EpiWeek) -> datetime:
    return epiweek.start + (epiweek.end - epiweek.start) / 2


if __name__ == "__main__":
    era5_aggregate()
