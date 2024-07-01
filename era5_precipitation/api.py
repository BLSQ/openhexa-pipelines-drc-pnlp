import logging
import os
import warnings
from calendar import monthrange
from datetime import datetime, timedelta
from typing import List, Tuple

import cdsapi
import pandas as pd
import xarray as xr
from openhexa.sdk.workspaces import workspace

import numpy as np

logger = logging.getLogger(__name__)


class Era5:
    def __init__(self, cache_dir: str = None):
        """Copernicus climate data store client.

        Parameters
        ----------
        cache_dir : str, optional
            Directory to cache downloaded files.
        """
        self.cache_dir = cache_dir
        self.cds_api_url = "https://cds.climate.copernicus.eu/api/v2"

    def init_cdsapi(self):
        """Create a .cdsapirc in the HOME directory.

        The API key must have been generated in CDS web application
        beforehand.
        """
        connection = workspace.custom_connection("climate-data-store")
        # cdsapirc = os.path.join(os.getenv("HOME"), ".cdsapirc")
        # with open(cdsapirc, "w") as f:
        #     f.write(f"url: {self.cds_api_url}\n")
        #     f.write(f"key: {connection.api_uid}:{connection.api_key}\n")
        #     f.write("verify: 0")
        # logger.info(f"Created .cdsapirc at {cdsapirc}")
        self.api = cdsapi.Client(url=self.cds_api_url, key=f"{connection.api_uid}:{connection.api_key}")

    # def close(self):
    #     """Remove .cdsapirc from HOME directory."""
    #     cdsapirc = os.path.join(os.getenv("HOME"), ".cdsapirc")
    #     os.remove(cdsapirc)
    #     logger.info(f"Removed .cdsapirc at {cdsapirc}")

    def download(
        self,
        variable: str,
        bounds: Tuple[float],
        year: int,
        month: int,
        hours: List[str],
        dst_file: str,
    ) -> str:
        """Download product for a given date.

        Parameters
        ----------
        variable : str
            CDS variable name. See documentation for a list of available
            variables <https://confluence.ecmwf.int/display/CKB/ERA5-Land>.
        bounds : tuple of float
            Bounding box of interest as a tuple of float (lon_min, lat_min,
            lon_max, lat_max)
        year : int
            Year of interest
        month : int
            Month of interest
        hours : list of str
            List of hours in the day for which measurements will be extracted
        dst_file : str
            Path to output file

        Return
        ------
        dst_file
            Path to output file
        """
        request = {
            "format": "netcdf",
            "variable": variable,
            "year": year,
            "month": month,
            "day": [f"{d:02}" for d in range(1, 32)],
            "time": hours,
            "area": list(bounds),
        }

        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            self.api.retrieve("reanalysis-era5-land", request, dst_file)
            logger.info(f"Downloaded product into {dst_file}")

        # dataset should have data until last day of the month
        ds = xr.open_dataset(dst_file)
        n_days = monthrange(year, month)[1]
        if not ds.time.values.max() >= pd.to_datetime(datetime(year, month, n_days)):
            logger.info(f"Data for {year:04}{month:02} is incomplete")
            return None

        return dst_file


    def download_range(
        self,
        variable: str,
        bounds: Tuple[float],
        start_date: datetime,
        end_date: datetime,        
        hours: List[str],
        dst_file: str,
    ) -> str:
        """Download product for a given range of days.   
        [WARNING] This function will download a file containing all the years, months 
        and days in the request (filtering is required).

        Parameters
        ----------
        variable : str
            CDS variable name. See documentation for a list of available
            variables <https://confluence.ecmwf.int/display/CKB/ERA5-Land>.
        bounds : tuple of float
            Bounding box of interest as a tuple of float (lon_min, lat_min,
            lon_max, lat_max)
        start_date : date
            Start of the range
        end_date : date
            End of the range
        hours : list of str
            List of hours in the day for which measurements will be extracted
        dst_file : str
            Path to output file

        Return
        ------
        dst_file
            Path to output file
        """        

        request = self._build_request(variable, bounds, start_date, end_date, hours)        

        with warnings.catch_warnings():
            warnings.simplefilter("ignore")            
            self.api.retrieve("reanalysis-era5-land", request, dst_file)  
            logger.info(f"Downloaded product into {dst_file}")
            
        # Check the data has all requested days in the dates range        
        if not self._is_valid_dataset(dst_file, start_date, end_date, hours): 
            logger.info(f"Data for range {start_date} - {end_date} is incomplete")            
            return None
       
        return dst_file
    

    def _build_request(self, variable:str, bounds:Tuple[float], start:datetime, end:datetime, hours:List[str])->dict:
            """ 
            API request string builder 
            
            """    

            date_range = [start + timedelta(days=x) for x in range((end - start).days + 1)]                                         
            request = {
                "format": "netcdf",
                "variable": variable,
                "year": sorted(list({f"{d.year:02}" for d in date_range})),
                "month": sorted(list({f"{d.month:02}" for d in date_range})),
                "day": sorted(list({f"{d.day:02}" for d in date_range})),
                "time": hours,
                "area": list(bounds),
            }
            return request


    def _is_valid_dataset(self, ds_filename:str, start:datetime, end:datetime, hours:List[str])->bool:
        """         
        Check if the file contains all requested data [days, hours]

        """        
        if hours == ['ALL']:                     
            hours_f = [f"{d:02}:00" for d in range(0, 24)] 
        else:
            hours_f = hours

        # Create a range of timestamps hours + hour
        valid_range = [[pd.Timestamp(datetime.strptime(f"{(start + timedelta(days=d)).strftime('%Y-%m-%d')}T{h}:00", "%Y-%m-%dT%H:%M:%S")) for d in range((end - start).days + 1)] for h in hours_f]
        valid_range = np.array(valid_range).flatten()

        with xr.open_dataset(ds_filename) as ds:                            
            ds_filtered = ds.sel(time=((ds['time']>=valid_range.min()) & (ds['time']<=valid_range.max())))   
        
        if len(ds_filtered.time.values) == len(valid_range):
            logger.info(f"Data validated")          
            return True
        
        return False

