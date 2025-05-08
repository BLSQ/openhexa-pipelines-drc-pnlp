import os
import tempfile
import zipfile
from io import BytesIO
from pathlib import Path
from shutil import copyfile
from datetime import datetime
import warnings

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
)
from openhexa.toolbox.era5.cds import VARIABLES

local = True
warnings.simplefilter(action="ignore", category=FutureWarning)


@pipeline("__pipeline_id__", name="ERA5_temperature_aggregate")
@parameter(
    "input_dir",
    type=str,
    name="Input directory",
    help="Input directory with raw ERA5 extracts",
    default="data/era5/full_raw",
)
@parameter(
    "output_dir",
    type=str,
    name="Output directory",
    help="Output directory for the aggregated data",
    default="data/era5/full_aggregate",
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
    boundaries = load_boundaries(db_table="cod_iaso_zone_de_sante")

    calculate_aggregations(boundaries, input_dir, output_dir)


@era5_aggregate.task
def calculate_aggregations(boundaries, input_dir, output_dir):
    """
    Calculate the daily, weekly and montly temperature aggregations.
    """
    # Initialize the variables
    list_daily = []
    list_weekly = []
    list_monthly = []
    full_input_dir = input_dir / "2m_temperature"
    dict_weekly = {}

    # Calculate the aggregations for the necessary funcions.
    for agg_func in ["min", "max"]:  # We want both the maximum and minimum temperature.
        ds_merged_part = merge_dataset(full_input_dir, agg_func)
        df_daily_part = spatial_aggregation(ds=ds_merged_part, boundaries=boundaries, agg_func=agg_func)
        list_daily.append(df_daily_part)

        df_weekly_part = get_weekly_aggregates(
            df=df_daily_part,
            agg_func=agg_func,
        )
        list_weekly.append(df_weekly_part)
        dict_weekly[agg_func] = df_weekly_part

        df_monthly_part = get_monthly_aggregates(
            df=df_daily_part,
            agg_func=agg_func,
        )
        list_monthly.append(df_monthly_part)

    # Calculate and save the daily aggregations
    df_daily_full = concatenate_list_dfs(list_daily)
    path_daily = Path(output_dir).joinpath("2m_temperature_daily.parquet").as_posix()
    save_df(df_daily_full, path_daily)

    # Calculate and save the weekly aggregations
    df_weekly_full = concatenate_list_dfs(list_weekly)
    df_weekly_full = format_df_weekly(df_weekly_full)
    path_weekly = Path(output_dir).joinpath("2m_temperature_weekly.parquet").as_posix()
    save_df(df_weekly_full, path_weekly)

    # Calculate and save the monthly aggregations
    df_monthly_full = concatenate_list_dfs(list_monthly)
    path_monthly = Path(output_dir).joinpath("2m_temperature_monthly.parquet").as_posix()
    save_df(df_monthly_full, path_monthly)

    # upload to table
    names_of_tables = {"min": "cod_temperature_tmin_weekly_test", "max": "cod_temperature_tmax_weekly_test"}
    for agg_func, target_table in names_of_tables.items():
        df_weekly = dict_weekly[agg_func]
        upload_data_to_table(df=df_weekly, targetTable=target_table)

    # update dataset -- we only do this for the weekly data
    if not local:
        update_temperature_dataset(dict=dict_weekly)

    current_run.log_info("Temperature data table updated")


@era5_aggregate.task
def load_boundaries(db_table: dict) -> gpd.GeoDataFrame:
    """Load boundaries from database."""
    current_run.log_info(f"Loading boundaries from {db_table}")
    if local:
        postgres_connection = workspace.postgresql_connection(db_table)
        dbengine = create_engine(postgres_connection.url)
    else:
        dbengine = create_engine(os.environ["WORKSPACE_DATABASE_URL"])
    boundaries = gpd.read_postgis(db_table, con=dbengine, geom_col="geometry")
    return boundaries


def format_df_weekly(df: pd.DataFrame) -> pd.DataFrame:
    """Format the weekly dataframe to match the expected output format."""
    dict_rename_first = {
        "ref": "uid",
        "start_date": "start",
        "end_date": "end",
        "epi_year": "year",
        "epi_week": "week",
    }
    dict_second_rename = {"period": "epiweek"}
    list_output = [
        "uid",
        "name",
        "epiweek",
        "tmin_min",
        "tmin_max",
        "tmin_mean",
        "tmax_min",
        "tmax_max",
        "tmax_mean",
        "start",
        "end",
        "year",
        "week",
    ]
    df = df.rename(columns=dict_rename_first)
    df = df.rename(columns=dict_second_rename)
    df = df[list_output]
    df["start"] = pd.to_datetime(df["start"])
    df["end"] = pd.to_datetime(df["end"])
    return df


def upload_data_to_table(df: pd.DataFrame, targetTable: str):
    """Upload the processed temperature data stats to target table."""

    current_run.log_info(f"Updating table : {targetTable}")

    # Create engine
    if local:
        postgres_connection = workspace.postgresql_connection(targetTable)
        dbengine = create_engine(postgres_connection.url)
    else:
        dbengine = create_engine(os.environ["WORKSPACE_DATABASE_URL"])

    # Create table
    df.to_sql(targetTable, dbengine, index=False, if_exists="replace", chunksize=4096)

    del dbengine


def update_temperature_dataset(dict: dict):
    """Update the temperature dataset to be shared."""

    current_run.log_info("Updating temperature dataset")

    # Get the dataset
    dataset = workspace.get_dataset("climate-dataset-tempera-39e1f8")
    date_version = f"ds_{datetime.now().strftime('%Y_%m_%d_%H%M')}"
    added_new = False

    for agg_func, df in dict.items():
        try:
            with tempfile.NamedTemporaryFile(suffix=".parquet") as tmp:
                # If we have not created the new version yet, we create it.
                if not added_new:
                    version = dataset.create_version(date_version)
                    current_run.log_info(f"New dataset version {date_version} created")
                    added_new = True
                # Add temperature .parquet to DS
                df.to_parquet(tmp.name)
                file_name = f"Temperature_t{agg_func}_{date_version}.parquet"
                version.add_file(tmp.name, filename=file_name)
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


def concatenate_list_dfs(list_df):
    """Concatenate a list of dataframes on common columns."""
    common_cols = set()
    df_merged = pd.DataFrame()
    for df in list_df:
        set_cols = set(df.columns)
        if not common_cols:
            common_cols = set_cols
        else:
            common_cols = common_cols.intersection(set_cols)

    for df in list_df:
        if df_merged.empty:
            df_merged = df
        else:
            df_merged = pd.merge(df_merged, df, on=list(common_cols))

    return df_merged


def save_df(df, dst_file):
    """Save dataframe to parquet file."""
    fs = filesystem(dst_file)
    with tempfile.NamedTemporaryFile(suffix=".parquet") as tmp:
        df.to_parquet(tmp.name)
        fs.put(tmp.name, dst_file)
    current_run.add_file_output(dst_file)


def merge_dataset(input_dir: Path, agg: str) -> pl.DataFrame:
    """Merge the grib files in the input directory and return a xarray dataset."""
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

        ds = daily_aggregations(Path(tmpdir), agg)

        # remove duplicated (if any) dates and sort
        # ds = ds.drop_duplicates(dim="time")
        # ds = ds.sortby("time")

    ds = ds.drop_duplicates(dim="time", keep="first")
    ds = ds.where(~np.isnan(ds["t2m"]), drop=True)  # After the aggregation, we we rid of the NaNs in the temperature.
    ds = ds - 273.15  # convert degrees K to degrees C

    return ds


def daily_aggregations(data_dir, agg: str):
    """
    Merge the hourly datasets. We want the ability to select the maximum and minimum values.
    """
    datasets = []

    if isinstance(data_dir, str):
        data_dir = Path(data_dir)

    list_files = data_dir.glob("*.grib")

    for datafile in list_files:
        ds = xr.open_dataset(datafile)

        if "valid_time" in ds.variables:
            ds = ds.stack(record=["time", "step"])
            ds = ds.set_index(record="valid_time")
            ds = ds.sortby("record")

        if agg == "mean":
            ds["t2m"] = ds["t2m"].resample(record="1D").mean()
        elif agg == "sum":
            ds["t2m"] = ds["t2m"].resample(record="1D").sum()
        elif agg == "min":
            ds["t2m"] = ds["t2m"].resample(record="1D").min()
        elif agg == "max":
            ds["t2m"] = ds["t2m"].resample(record="1D").max()
        else:
            raise ValueError(f"{agg} is not a recognized aggregation method")

        ds = ds.drop_vars(["time", "step"], errors="ignore").rename({"record": "time"})
        datasets.append(ds)

    ds = xr.concat(datasets, dim="time")

    n = len(ds.longitude) * len(ds.latitude) * len(ds.time)
    current_run.log_info(f"Merged {len(datasets)} hourly datasets ({n} measurements)")

    return ds


def spatial_aggregation(ds: xr.Dataset, boundaries: gpd.GeoDataFrame, agg_func: str) -> pd.DataFrame:
    """Apply spatial aggregation on dataset based on a set of boundaries.

    Final value for each boundary is equal to the mean of all cells
    intersecting the shape.

    Parameters
    ----------
    ds : xr.Dataset
        Input dataset of shape (height, width, n_time_steps)
    boundaries : gpd.GeoDataFrame
        Input boundaries
    agg_func : str
        Aggregation function


    Return
    ------
    dataframe
        Mean value as a dataframe of length (n_boundaries * n_time_steps)
    """
    current_run.log_info("Running spatial aggregation ...")
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
            relevant_area = measurements[(measurements >= 0) & (areas[i, :, :])]
            if relevant_area.size > 0:
                mean_val = np.nanmean(relevant_area)
            else:
                mean_val = np.nan
            # For the spatial dimension, we calculate the mean.
            # For the time dimension, we calculate the max/min.
            new_stats = pd.Series([str(day)[:10], count_val, mean_val], index=["period", "count", "mean"])
            records.append(pd.concat([row, new_stats]))

    records = pd.DataFrame(records)
    if "geometry" in records.columns:
        records = records.drop(columns=["geometry"])

    records = records.rename(columns={"mean": f"t{agg_func}"})

    current_run.log_info(f"Applied spatial aggregation for {len(boundaries)} boundaries")

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


def get_weekly_aggregates(df: pd.DataFrame, agg_func: str) -> pd.DataFrame:
    """Apply weekly aggregation of input daily dataframe.

    Uses epidemiological weeks and assumes at least 4 columns in input
    dataframe: ref, period, count and sum.

    Parameters
    ----------
    df : dataframe
        Input dataframe
    agg_func : str
        Aggregation function

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
    df_["middle_date"] = (
        df_["period"]
        .apply(lambda day: _compute_mid_date(EpiWeek(datetime.strptime(day, "%Y-%m-%d"))))
        .dt.date.astype(str)
    )

    # epiweek aggregation with the agg
    aggregations = (
        df_.groupby(by=["ref", "epi_year", "epi_week"])[[f"t{agg_func}"]].agg(["min", "max", "mean"]).reset_index()
    )
    aggregations.columns = ["_".join(col).strip("_") for col in aggregations.columns.values]
    aggregations = aggregations.rename(
        columns={
            f"t{agg_func}_min": f"t{agg_func}_min",
            f"t{agg_func}_max": f"t{agg_func}_max",
            f"t{agg_func}_mean": f"t{agg_func}_mean",
        }
    )
    data_left = df_.copy()
    data_left["period"] = data_left["period"].apply(lambda day: str(EpiWeek(datetime.strptime(day, "%Y-%m-%d"))))
    data_left = data_left.drop(columns=[f"t{agg_func}"])
    data_left = data_left.drop_duplicates(subset=data_left.columns)  # unique rows

    # merge
    merged_df = pd.merge(data_left, aggregations, on=["ref", "epi_year", "epi_week"], how="left")
    merged_df["middle_date"] = pd.to_datetime(merged_df["middle_date"])

    # fix format
    merged_df = merged_df[
        [
            "name_short",
            "name",
            "ref",
            "parent_short",
            "parent",
            "parent_ref",
            "group_names",
            "group_refs",
            "uuid",
            "period",
            "epi_year",
            "epi_week",
            "start_date",
            "middle_date",
            "end_date",
            "count",
            f"t{agg_func}_min",
            f"t{agg_func}_max",
            f"t{agg_func}_mean",
        ]
    ]

    current_run.log_info(f"Applied {agg_func} temperature aggregation ({len(merged_df)} measurements)")
    return merged_df


def get_monthly_aggregates(df: pd.DataFrame, agg_func: str) -> pd.DataFrame:
    """Apply montly aggregation of input daily dataframe.

    Parameters
    ----------
    df : dataframe
        Input dataframe

    agg_func : str
        Aggregation function

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
    df_["middle_date"] = df_["start_date"] + pd.Timedelta(days=14)

    # monthly aggregation calculation
    aggregations = df_.groupby(by=["ref", "year", "month"])[[f"t{agg_func}"]].agg(["min", "max", "mean"]).reset_index()
    aggregations.columns = ["_".join(col).strip("_") for col in aggregations.columns.values]
    aggregations = aggregations.rename(
        columns={
            f"t{agg_func}_min": f"t{agg_func}_min",
            f"t{agg_func}_max": f"t{agg_func}_max",
            f"t{agg_func}_mean": f"t{agg_func}_mean",
        }
    )

    data_left = df_.copy()
    data_left["period"] = pd.to_datetime(data_left["period"]).dt.strftime("%Y-%m")
    data_left = data_left.drop(columns=[f"t{agg_func}"])
    data_left = data_left.drop_duplicates(subset=data_left.columns)  # unique rows

    # merge
    merged_df = pd.merge(data_left, aggregations, on=["ref", "year", "month"], how="left")

    # fix format
    merged_df = merged_df[
        [
            "name_short",
            "name",
            "ref",
            "parent_short",
            "parent",
            "parent_ref",
            "group_names",
            "group_refs",
            "uuid",
            "period",
            "year",
            "month",
            "start_date",
            "middle_date",
            "end_date",
            "count",
            f"t{agg_func}_min",
            f"t{agg_func}_max",
            f"t{agg_func}_mean",
        ]
    ]
    return merged_df


def _compute_mid_date(epiweek: EpiWeek) -> datetime:
    return epiweek.start + (epiweek.end - epiweek.start) / 2


if __name__ == "__main__":
    era5_aggregate()
