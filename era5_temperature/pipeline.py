import datetime
import json
import logging
import os
import tempfile
from calendar import monthrange
from typing import List, Tuple

import geopandas as gpd
import numpy as np
import pandas as pd
import rasterio
import xarray as xr
from api import Era5
from dateutil.relativedelta import relativedelta
from epiweek import EpiWeek
from openhexa.sdk import current_run, pipeline, workspace
from rasterio.features import rasterize
from utils import filesystem
from sqlalchemy import create_engine
import string

logger = logging.getLogger(__name__)

class ERA5Error(Exception):
    pass

class ERA5MissingData(ERA5Error):
    pass


@pipeline("era5-temperature", name="ERA5 Temperature")
def era5_temperature():
    """Download and aggregate temperature data from climate data store."""

    # parse configuration
    with open(f"{workspace.files_path}/pipelines/era5_temperature/config_DRC.json") as f:
        config = json.load(f)

    # last day of previous month
    dt = datetime.datetime.now()
    dt = dt - relativedelta(months=1)
    end_date = datetime.datetime(dt.year, dt.month, monthrange(dt.year, dt.month)[1])

    # Create engine
    dbengine = create_engine(os.environ["WORKSPACE_DATABASE_URL"]) 

    # load boundaries
    boundaries=gpd.read_postgis(_safe_from_injection(config['boundaries_table']), con=dbengine, geom_col='geometry')
    del dbengine

    api = Era5()
    api.init_cdsapi()
    current_run.log_info("Connected to Climate Data Store API")
    datafiles = download(
        api=api,
        cds_variable=config["cds_variable"],
        bounds=boundaries.total_bounds,
        start_date=datetime.datetime.strptime(config["start_date"], "%Y-%m-%d"),
        end_date=end_date,
        hours=config["hours"],
        data_dir=os.path.join(workspace.files_path, config["download_dir"]),
    )
    api.close()

    meta = get_raster_metadata(datafiles)

    # Loop over the two tables we need to produce for DRC PNLP
    for agg in ["min", "max"]: ## we want weekly tmax and tmin tables
        # merge data
        ds = merge(
            src_files=datafiles,            
            dst_file=os.path.join(
                workspace.files_path, 
                config["output_dir"],
                f"{config['cds_variable']}_t{agg}.nc",
            ),
            agg=agg,
        )

        # reformat to daily data
        df_daily = spatial_aggregation(
            ds=ds,
            dst_file=os.path.join(
                workspace.files_path,                 
                config["output_dir"],
                f"{config['cds_variable']}_t{agg}_daily.parquet",
            ),
            boundaries=boundaries,
            meta=meta,
            column_uid=config["column_uid"],  #### DOUBLE CHECK that we're selecting the correct id (ref)
            column_name=config["column_name"],
        )

        # weekly values reformat for DRC 
        df_weekly = weekly_for_DRC(
            df=df_daily,
            dst_file=os.path.join(
                workspace.files_path, 
                config["output_dir"],
                f"{config['cds_variable']}_t{agg}_weekly.parquet",
            ),
            agg=agg,
        )

        # Push the data to the DB table
        upload_data_to_table(df=df_weekly, targetTable=config[f't{agg}_table']) 
        # Passing the dbengine as parameter throw a pickel/dill exception (see logs of fail runs)

 

@era5_temperature.task
def download(
    api: Era5,
    cds_variable: str,
    bounds: Tuple[float],
    start_date: datetime.datetime,
    end_date: datetime.datetime,
    hours: List[str],
    data_dir: str,
) -> List[str]:
    """Download data products to cover the area of interest."""
    xmin, ymin, xmax, ymax = bounds

    # add a buffer around the bounds and rearrange order for
    # compatbility with climate data store API
    bounds = (
        round(ymin, 1) - 0.1,
        round(xmin, 1) - 0.1,
        round(ymax, 1) + 0.1,
        round(xmax, 1) + 0.1,
    )

    datafiles = download_monthly_products(
        api=api,
        cds_variable=cds_variable,
        bounds=bounds,
        start_date=start_date,
        end_date=end_date,
        hours=hours,
        output_dir=data_dir,
        overwrite=False,
    )

    return datafiles


@era5_temperature.task
def get_raster_metadata(datafiles: List[str]) -> dict:
    """Get raster metadata from 1st downloaded product."""
    with rasterio.open(datafiles[0]) as src:
        meta = src.meta
    return meta


@era5_temperature.task
def merge(src_files: List[str], dst_file: str, agg: str) -> xr.Dataset:
    """Merge hourly datasets into a single daily one.

    Parameters
    ----------
    src_files : list of str
        Input data files
    dst_file : str
        Path to output file
    agg : str
        Mean, sum, min or max

    Return
    ------
    dataset
        Output merged dataset
    """
    ds = merge_datasets(src_files, agg=agg) 
    
    # convert degrees K to degrees C
    ds = ds - 273.15

    fs = filesystem(dst_file)
    with tempfile.NamedTemporaryFile(suffix=".nc") as tmp:
        ds.to_netcdf(tmp.name)
        fs.put(tmp.name, dst_file)

    return ds


@era5_temperature.task
def spatial_aggregation(
    ds: xr.Dataset,
    dst_file: str,
    boundaries: gpd.GeoDataFrame,
    meta: dict,
    column_uid: str,
    column_name: str,
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
    meta : dict
        Raster metadata
    column_uid : str
        Column in boundaries geodataframe with feature UID
    column_name : str
        Column in boundaries geodataframe with feature name

    Return
    ------
    dataframe
        Mean value as a dataframe of length (n_boundaries * n_time_steps)
    """
    df = _spatial_aggregation(
        ds=ds,
        boundaries=boundaries,
        height=meta["height"],
        width=meta["width"],
        transform=meta["transform"],
        nodata=meta["nodata"],
        column_uid=column_uid,
        column_name=column_name,
    )

    current_run.log_info(
        f"Applied spatial aggregation for {len(boundaries)} boundaries"
    )

    fs = filesystem(dst_file)
    with tempfile.NamedTemporaryFile(suffix=".parquet") as tmp:
        df.to_parquet(tmp.name)
        fs.put(tmp.name, dst_file)
    current_run.add_file_output(dst_file)

    return df


@era5_temperature.task
def weekly_for_DRC(df: pd.DataFrame, dst_file: str, agg:str) -> pd.DataFrame:
    """Get weekly temperature stats from daily dataset."""
    df_weekly = get_weekly_aggregates_for_DRC(df, agg)
    current_run.log_info(f"Applied weekly aggregation ({len(df_weekly)} measurements)")
    fs = filesystem(dst_file)
    with tempfile.NamedTemporaryFile(suffix=".parquet") as tmp:
        df_weekly.to_parquet(tmp.name)
        fs.put(tmp.name, dst_file)
    current_run.add_file_output(dst_file)
    
    return df_weekly


@era5_temperature.task
def upload_data_to_table(df: pd.DataFrame, targetTable:str, create=True):
    """Upload the processed weekly data temperature stats to target table."""

    targetTable_safe = _safe_from_injection(targetTable)

    # Create engine
    dbengine = create_engine(os.environ["WORKSPACE_DATABASE_URL"]) 

    # Create table
    df.to_sql(targetTable_safe, dbengine, index=False, if_exists="replace", chunksize=4096)
    current_run.log_info(f"Updating weekly table : {targetTable_safe}")
    del dbengine
    

def download_monthly_products(
    api: Era5,
    cds_variable: str,
    bounds: Tuple[float],
    start_date: datetime,
    end_date: datetime,
    hours: list,
    output_dir: str,
    overwrite: bool = False,
) -> List[str]:
    """Download all available products for the provided dates, bounds
    and CDS variable.

    By default, existing files are not overwritten and downloads will be skipped.
    Monthly products with incomplete dates (i.e. with only a fraction of days)
    will also be skipped.

    Parameters
    ----------
    api : Era5
        Authenticated ERA5 API object
    cds_variable : str
        CDS variable name. See documentation for a list of available
        variables <https://confluence.ecmwf.int/display/CKB/ERA5-Land>.
    bounds : tuple of float
        Bounding box of interest as a tuple of float (lon_min, lat_min,
        lon_max, lat_max)
    start_date : date
        Start date
    end_date : date
        End date
    hours : list of str
        List of hours in the day for which measurements will be downloaded
    dst_file : str
        Path to output file

    Return
    ------
    list of str
        Downloaded files
    """
    dst_files = []
    with tempfile.TemporaryDirectory() as tmp_dir:
        date_ = start_date

        while date_ <= end_date:
            fname = f"{cds_variable}_{date_.year:04}{date_.month:02}.nc"
            dst_file = os.path.join(output_dir, fname)
            fs = filesystem(dst_file)
            fs.makedirs(os.path.dirname(dst_file), exist_ok=True)

            if fs.exists(dst_file) and not overwrite:
                msg = f"{fname} already exists, skipping"
                logger.info(msg)
                current_run.log_info(msg)
                date_ = date_ + relativedelta(months=1)
                dst_files.append(dst_file)
                continue

            datafile = api.download(
                variable=cds_variable,
                bounds=bounds,
                year=date_.year,
                month=date_.month,
                hours=hours,
                dst_file=os.path.join(tmp_dir, fname),
            )

            if datafile:
                fs.put(datafile, dst_file)
                dst_files.append(dst_file)
                msg = f"Downloaded {fname}"
                logger.info(msg)
                current_run.log_info(msg)
            else:
                msg = (
                    f"Missing data for period {date_.year:04}{date_.month:02}, skipping"
                )
                logger.info(msg)
                current_run.log_info(msg)
                return dst_files

            date_ = date_ + relativedelta(months=1)

    return dst_files


def fix_geometries(geodf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Try to fix invalid geometries in a geodataframe.

    Parameters
    ----------
    geodf : geodataframe
        Input geodataframe with a geometry column.

    Return
    ------
    geodf : geodataframe
        Updated geodataframe with valid geometries.
    """
    geodf_ = geodf.copy()
    n_features_orig = len(geodf_)
    for i, row in geodf_.iterrows():
        if not row.geometry.is_valid:
            geodf_.at[i, "geometry"] = row.geometry.buffer(0)
    geodf_ = geodf_[geodf_.is_simple]
    geodf_ = geodf_[geodf_.is_valid]
    n_features = len(geodf_)
    if n_features < n_features_orig:
        msg = f"{n_features_orig - n_features} features are invalid and were excluded"
        logger.warn(msg)
        current_run.log_warning(msg)
    return geodf_


def merge_datasets(datafiles: List[str], agg: str = "mean") -> xr.Dataset:
    """Merge hourly data files into a single daily dataset.

    Parameters
    ----------
    datafiles : list of str
        List of dataset paths
    agg : str, optional
        Temporal aggregation method (mean, sum)

    Return
    ------
    xarray dataset
        Merged dataset of shape (height, width, n_days).
    """
    datasets = [] 
    for datafile in datafiles:
        ds = xr.open_dataset(datafile)
        if agg == "mean":
            ds = ds.resample(time="1D").mean()
        elif agg == "sum":
            ds = ds.resample(time="1D").sum()
        elif agg == "min":
            ds = ds.resample(time="1D").min()
        elif agg == "max":
            ds = ds.resample(time="1D").max()
        else:
            raise ValueError(f"{agg} is not a recognized aggregation method")
        datasets.append(ds)
    ds = xr.concat(datasets, dim="time")

    # when both ERA5 and ERA5RT (real time) data are in the dataset, an `expver`
    # dimension is added. Measurements are either ERA5 or ERA5RT, so we just
    # take the max. value across the dimension.
    if "expver" in ds.dims:
        ds = ds.max("expver")

    n = len(ds.longitude) * len(ds.latitude) * len(ds.time)
    current_run.log_info(f"Merged {len(datasets)} hourly datasets ({n} measurements)")
    return ds


def generate_boundaries_raster(
    boundaries: gpd.GeoDataFrame, height: int, width: int, transform: rasterio.Affine
):
    """Generate a binary raster mask for each boundary.

    Parameters
    ----------
    boundaries : gpd.GeoDataFrame
        Boundaries to rasterize
    height : int
        Raster height
    width : int
        Raster width
    transform : affine
        Raster affine transform

    Return
    ------
    areas : ndarray
        A binary raster of shape (n_boundaries, height, width).
    """
    areas = np.empty(shape=(len(boundaries), height, width), dtype=np.bool_)
    for i, geom in enumerate(boundaries.geometry):
        area = rasterize(
            [geom],
            out_shape=(height, width),
            fill=0,
            default_value=1,
            transform=transform,
            all_touched=True,
            dtype="uint8",
        )
        if np.count_nonzero(area) == 0:
            logger.warn(f"No cell covered by input geometry {i}")
        areas[i, :, :] = area == 1
    return areas


def _spatial_aggregation(
    ds: xr.Dataset,
    boundaries: gpd.GeoDataFrame,
    height: int,
    width: int,
    transform: rasterio.Affine,
    nodata: int,
    column_uid: str,
    column_name: str,
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
    height : int
        Raster grid height
    width : int
        Raster grid width
    transform : affine
        Raster affine transform
    nodata : int
        Raster nodata value
    column_uid : str
        Column in boundaries geodataframe with feature UID
    column_name : str
        Column in boundaries geodataframe with feature name

    Return
    ------
    dataframe
        Mean value as a dataframe of length (n_boundaries * n_time_steps)
    """
    areas = generate_boundaries_raster(
        boundaries=boundaries, height=height, width=width, transform=transform
    )

    var = [v for v in ds.data_vars][0]

    records = []
    days = [day for day in ds.time.values]

    for day in days:
        measurements = ds.sel(time=day)
        measurements = measurements[var].values

        for i, (_, row) in enumerate(boundaries.iterrows()):
            value = np.mean(
                measurements[
                    (measurements >= 0) & (measurements != nodata) & (areas[i, :, :])
                ]
            )
            records.append(
                {
                    "uid": row[column_uid],
                    "name": row[column_name],
                    "period": str(day)[:10],
                    "value": value,
                }
            )

    return pd.DataFrame(records)


def get_weekly_aggregates_for_DRC(df: pd.DataFrame, agg:str) -> pd.DataFrame:
    """Apply weekly aggregation of input daily dataframe.

    This method creates a formatted table with the expected structure
    by the DRC malaria dashboards.

    Uses epidemiological weeks and assumes 4 columns in input
    dataframe: uid, name, period and value.
    
    Parameters
    ----------
    df : dataframe
        Input dataframe

    agg : Aggregation function

    Return
    ------
    dataframe
        Weekly dataframe of length (n_locations * n_weeks)
    """
    df_ = df.copy()
    df_["epiweek"] = df_["period"].apply(lambda day: str(EpiWeek(datetime.datetime.strptime(day, "%Y-%m-%d"))))

    # get min/max date by epiweek
    df_sd = df_.groupby(by=["uid", "name", "epiweek"]).min().reset_index().drop(columns='value')
    df_ed = df_.groupby(by=["uid", "name", "epiweek"]).max().reset_index().drop(columns='value')    
    
    # Stats
    df_g = df_.groupby(by=["uid", "name", "epiweek"])['value'].agg(['min', 'max', 'mean']).reset_index()
    
    # Merge start and end dates
    df_g = df_g.merge(df_sd, on=["uid", "name", "epiweek"], how='left')
    df_ = df_g.merge(df_ed, on=["uid", "name", "epiweek"], how='left')
    
    # Rename columns
    df_.columns = ['uid', 'name', 'epiweek', f't{agg}_min', f't{agg}_max', f't{agg}_mean', 'start', 'end']

    # Add extra columns as int
    df_[['year', 'week']] = df_['epiweek'].str.split('W', expand=True).astype(int)    
    df_.drop(columns='uid', inplace=True)

    # format temps
    for column in df_.columns:
        if df_[column].dtype == "float32":
            df_[column] = df_[column].astype(float).round(1)
    
    # format dates
    df_['start'] = pd.to_datetime(df_['start'])
    df_['end'] = pd.to_datetime(df_['end']) 

    return df_


def _safe_from_injection(db_table: str) -> str:
    """Remove potential SQL injection."""
    return "".join(
        [c for c in db_table if c in string.ascii_letters + string.digits + "_"]
    )


if __name__ == "__main__":
    era5_temperature()
