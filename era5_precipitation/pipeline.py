from datetime import datetime, timedelta
import json
import logging
import os
import tempfile
from typing import List, Tuple

import geopandas as gpd
import numpy as np
import pandas as pd
import rasterio
from rasterio.windows import Window
import xarray as xr
from api import Era5
from dateutil.relativedelta import relativedelta
from epiweek import EpiWeek, epiweek_range
from openhexa.sdk import current_run, pipeline, workspace
from utils import filesystem
from sqlalchemy import create_engine
import string
from rasterio.features import rasterize

logger = logging.getLogger(__name__)


class ERA5Error(Exception):
    pass


class ERA5MissingData(ERA5Error):
    pass


@pipeline("era5-precipitation", name="ERA5 Precipitation")
def era5_precipitation():
    """Download and aggregate total precipitation data from climate data store."""

    # parse configuration
    with open(f"{workspace.files_path}/pipelines/era5_precipitation/config_DRC.json") as f:    
        config = json.load(f)
 
    # Download data from a Epiweek that finished at least 5 days ago (ERA5 updates freq)    
    # Get last complete Epiweek (ERA5 returns precip cumsum of day 01 at 00:00 of day 02)
    # Function download_epiWeek_products() uses  epiweek start and epiweek end+1      
    dt = datetime.now() - relativedelta(days=5)     
    end_date = EpiWeek(dt - relativedelta(weeks=1)).end 
    
    # keep this date fix (no reason to change this)!
    # Blame ERA5 API (start_date is used as filter in _adjust_ERA5_dataset())
    start_date = datetime.strptime(config["start_date"], "%Y-%m-%d")
                    
    # load boundaries
    dbengine = create_engine(os.environ["WORKSPACE_DATABASE_URL"]) 
    boundaries=gpd.read_postgis(_safe_from_injection(config['boundaries_table']), con=dbengine, geom_col='geometry')
    
    api = Era5()
    api.init_cdsapi()
    current_run.log_info("Connected to Climate Data Store API")    
    datafiles = download(
        api=api,
        cds_variable=config["cds_variable"],
        bounds=boundaries.total_bounds,
        start_date=start_date,
        end_date=end_date,
        hours=config["hours"],
        data_dir=os.path.join(workspace.files_path, config["download_dir"])        
    )
    # api.close() 

    meta = get_raster_metadata(datafiles)

    # merge all available files   
    ds = merge_ERA5(
        src_files=datafiles,
        dst_file=os.path.join( 
            workspace.files_path, config["output_dir"], f"{config['cds_variable']}.nc"
        ),    
        start_date=start_date    
    ) 
        
    df_daily = spatial_aggregation(
        ds=ds,
        dst_file=os.path.join(
            workspace.files_path,
            config["output_dir"],
            f"{config['cds_variable']}_daily.parquet",
        ),
        boundaries=boundaries,
        meta=meta,
        column_uid=config["column_uid"],
        column_name=config["column_name"],
    )

    df_weekly = weekly(
        df=df_daily,
        dst_file=os.path.join(
            workspace.files_path,
            config["output_dir"],
            f"{config['cds_variable']}_weekly.parquet",
        ),
    )
    
    # upload to table
    upload_data_to_table(df=df_weekly, targetTable=config['update_table']) 

    # update dataset
    update_precipitation_dataset(df=df_weekly) 


@era5_precipitation.task
def download(
    api: Era5,
    cds_variable: str,
    bounds: Tuple[float],
    start_date: datetime,
    end_date: datetime,
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

    datafiles = download_epiWeek_products(
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


@era5_precipitation.task
def get_raster_metadata(datafiles: List[str]) -> dict:
    """Get raster metadata from 1st downloaded product."""
    with rasterio.open(datafiles[0]) as src:
        meta = src.meta
    return meta


@era5_precipitation.task
def merge_ERA5(src_files: List[str], dst_file: str, start_date: datetime) -> xr.Dataset:
    """Merge hourly datasets into a single daily one.

    Parameters
    ----------
    src_files : list of str
        Input data files
    dst_file : str
        Path to output file

    Return
    ------
    dataset
        Output merged dataset
    """
    ds = merge_ERA5_datasets(src_files, agg="raw")
    
    # filter and shift ERA5 dates
    ds = _adjust_ERA5_dataset(ds, start_date)
    
    # m to mm
    ds = ds * 1000

    fs = filesystem(dst_file)
    with tempfile.NamedTemporaryFile(suffix=".nc") as tmp: 
        ds.to_netcdf(tmp.name)
        fs.put(tmp.name, dst_file)

    return ds

 
@era5_precipitation.task
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
    current_run.log_info(
        f"Running spatial aggregation ..."
    )

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


@era5_precipitation.task
def weekly(df: pd.DataFrame, dst_file: str) -> pd.DataFrame:
    """Get weekly temperature from daily dataset."""
    df_weekly = get_weekly_aggregates(df)
    current_run.log_info(f"Applied weekly aggregation ({len(df_weekly)} measurements)")
    fs = filesystem(dst_file)
    with tempfile.NamedTemporaryFile(suffix=".parquet") as tmp: 
        df_weekly.to_parquet(tmp.name)
        fs.put(tmp.name, dst_file)
    current_run.add_file_output(dst_file)
    return df_weekly


@era5_precipitation.task
def upload_data_to_table(df: pd.DataFrame, targetTable:str):
    """Upload the processed weekly data temperature stats to target table."""

    targetTable_safe = _safe_from_injection(targetTable)
    current_run.log_info(f"Updating weekly table : {targetTable_safe}")

    # Create engine
    dbengine = create_engine(os.environ["WORKSPACE_DATABASE_URL"]) 

    # Create table
    df.to_sql(targetTable_safe, dbengine, index=False, if_exists="replace", chunksize=4096)
    
    del dbengine


@era5_precipitation.task
def update_precipitation_dataset(df: pd.DataFrame):
    """Update the precipitation dataset to be shared."""
    
    current_run.log_info(f"Updating precipitation dataset")

    # Get the dataset 
    dataset = workspace.get_dataset("climate-dataset-precipi-6349a3")
    date_version = f"ds_{datetime.now().strftime('%Y_%m_%d_%H%M')}"

    try:
        # Create new DS version
        version = dataset.create_version(date_version) 
    except Exception as e:
        print(f"The dataset version already exists - ERROR: {e}")
        raise
            
    try:
        # Add Precipitation .parquet to DS
        with tempfile.NamedTemporaryFile(suffix=".parquet") as tmp:
            df.to_parquet(tmp.name)
            version.add_file(tmp.name, filename=f"Precipitation_{date_version}.parquet")                
    except Exception as e:
        print(f"Dataset file cannot be saved - ERROR: {e}")
        raise

    current_run.log_info(f"New dataset version {date_version} created")
    
    

def download_epiWeek_products(
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

    # epiweek range
    epiweeks = epiweek_range(start_date, end_date)    
    with tempfile.TemporaryDirectory() as tmp_dir: 
        for week in epiweeks:        
            epiweek_start = week.start 
            epiweek_end = week.end + timedelta(days=1) # add one extra day for "TP" (handle duplicates when merging)
            fname = f"{cds_variable}_{epiweek_end.year:04}{epiweek_end.month:02}_epiweek{week.week:02}.nc"            
            dst_file = os.path.join(output_dir, fname)
            fs = filesystem(dst_file)
            fs.makedirs(os.path.dirname(dst_file), exist_ok=True)

            if fs.exists(dst_file) and not overwrite:
                msg = f"{fname} already exists, skipping"
                logger.info(msg)
                current_run.log_info(msg)                
                dst_files.append(dst_file) 
                continue
             
            datafile = api.download_range(
                variable=cds_variable,
                bounds=bounds,
                start_date=epiweek_start,
                end_date=epiweek_end,
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
                msg = f"Missing data for period {week}, skipping"
                logger.info(msg)
                current_run.log_info(msg)
                return dst_files            

    return dst_files


def merge_ERA5_datasets(datafiles: List[str], agg: str = "mean") -> xr.Dataset:
    """Merge hourly data files into a single daily dataset.
    
    This method handles the duplicated dates

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
        elif agg == "raw":
           pass  # no aggregation performed
        else:
            raise ValueError(f"{agg} is not a recognized aggregation method")
        datasets.append(ds)
    ds = xr.concat(datasets, dim="time")

    # remove duplicated dates and sort (handle extra end+1 day)
    ds = ds.drop_duplicates(dim='time') 
    ds = ds.sortby('time')  

    # when both ERA5 and ERA5RT (real time) data are in the dataset, an `expver`
    # dimension is added. Measurements are either ERA5 or ERA5RT, so we just
    # take the max. value across the dimension.
    if "expver" in ds.dims:
        ds = ds.max("expver")

    n = len(ds.longitude) * len(ds.latitude) * len(ds.time)
    current_run.log_info(f"Merged {len(datasets)} datasets ({n} measurements)")
    return ds


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

    # get polygon shapes     
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

            count_val = np.sum(areas[i, :, :])
            sum_val = np.nansum(measurements[(measurements >= 0) & (measurements != nodata) & (areas[i, :, :])])
            new_stats = pd.Series([str(day)[:10], count_val, sum_val], index=['period', 'count', 'sum'])            
            records.append(pd.concat([row, new_stats]))
            
    records = pd.DataFrame(records)
    records.drop(columns=["geometry"], inplace=True)

    return records


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
    df_['epi_year'] = df_["period"].apply(lambda day: EpiWeek(datetime.strptime(day, "%Y-%m-%d")).year).astype(int) 
    df_['epi_week'] = df_["period"].apply(lambda day: EpiWeek(datetime.strptime(day, "%Y-%m-%d")).week).astype(int) 

    # Epiweek start end dates
    df_['start_date'] = df_["period"].apply(lambda day: EpiWeek(datetime.strptime(day, "%Y-%m-%d")).start).astype(str) 
    df_['end_date'] = df_["period"].apply(lambda day: EpiWeek(datetime.strptime(day, "%Y-%m-%d")).end).astype(str) 
    df_['mid_date'] = df_["period"].apply(lambda day: _compute_mid_date(EpiWeek(datetime.strptime(day, "%Y-%m-%d"))))

    # epiweek aggregation sum
    sums = df_.groupby(by=["ref", "epi_year", "epi_week"])[["sum"]].sum().reset_index()
    data_left = df_.copy()
    data_left['period'] = data_left["period"].apply(lambda day: str(EpiWeek(datetime.strptime(day, "%Y-%m-%d"))))
    data_left = data_left.drop(columns=["sum"])
    data_left = data_left.drop_duplicates(subset=data_left.columns) # unique rows
    
    # merge 
    merged_df = pd.merge(data_left, sums, on=["ref", "epi_year", "epi_week"], how='left')
    merged_df['mid_date'] = pd.to_datetime(merged_df['mid_date'])

    return merged_df


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


def _compute_mid_date(epiweek: EpiWeek) -> datetime:        
    return epiweek.start + (epiweek.end - epiweek.start)/2


def _safe_from_injection(db_table: str) -> str:
    """Remove potential SQL injection."""
    return "".join(
        [c for c in db_table if c in string.ascii_letters + string.digits + "_"]
    )


def _adjust_ERA5_dataset(ds:xr.Dataset, start: datetime) -> xr.Dataset:
    # Select valid dates within the range of EPIWeeksZ
    ds_filtered = ds.sel(time=(ds['time']>=pd.Timestamp(EpiWeek(start).start)))
 
    # Shift values one day backwards
    selected_ds_values = ds_filtered.isel(time=slice(1, None))
    selected_ds_values['time'] = ds_filtered.isel(time=slice(None, -1)).time.values
     
    return  selected_ds_values



if __name__ == "__main__":
    era5_precipitation()
