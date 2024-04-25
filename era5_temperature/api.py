import logging
import os
import warnings
from calendar import monthrange
from datetime import datetime
from typing import List, Tuple

import cdsapi
import pandas as pd
import xarray as xr
from openhexa.sdk.workspaces import workspace

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
        connection = workspace.custom_connection("CLIMATE-DATA-STORE")
        cdsapirc = os.path.join(os.getenv("HOME"), ".cdsapirc")
        with open(cdsapirc, "w") as f:
            f.write(f"url: {self.cds_api_url}\n")
            f.write(f"key: {connection.api_uid}:{connection.api_key}\n")
            f.write("verify: 0")
        logger.info(f"Created .cdsapirc at {cdsapirc}")
        self.api = cdsapi.Client()

    def close(self):
        """Remove .cdsapirc from HOME directory."""
        cdsapirc = os.path.join(os.getenv("HOME"), ".cdsapirc")
        os.remove(cdsapirc)
        logger.info(f"Removed .cdsapirc at {cdsapirc}")

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
