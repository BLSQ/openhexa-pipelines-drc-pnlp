from openhexa.sdk import (
    CustomConnection,
    Dataset,
    current_run,
    parameter,
    pipeline,
    workspace,
)
from io import BytesIO
from math import ceil
import geopandas as gpd
from datetime import datetime, timezone
from openhexa.toolbox.dhis2.periods import period_from_string
from datapi import ApiClient
from dateutil.relativedelta import relativedelta
from pathlib import Path
from openhexa.sdk.datasets import DatasetFile


@pipeline("era5_humidity_extract")
@parameter(
    "start_date",
    type=str,
    name="Start date",
    help="Start date of extraction period.",
    default="2018-01-01",
)
@parameter(
    "end_date",
    type=str,
    name="End date",
    help="End date of extraction period. Latest available by default.",
    required=False,
)
@parameter(
    "cds_connection",
    name="Climate data store",
    type=CustomConnection,
    help="Credentials for connection to the Copernicus Climate Data Store",
    required=True,
)
@parameter(
    "boundaries_dataset",
    name="Boundaries dataset",
    type=Dataset,
    help="Input dataset containing boundaries geometries",
    required=True,
)
@parameter(
    "boundaries_file",
    name="Boundaries filename in dataset",
    type=str,
    help="Filename of the boundaries file to use in the boundaries dataset",
    required=False,
    default="district.parquet",
)
@parameter(
    "output_dir",
    name="Output directory",
    type=str,
    help="Output directory for the extracted data",
    required=True,
    default="pipelines/era5_relative_humidity_extract/data/raw",
)
def era5_extract(
    start_date: str,
    end_date: str,
    cds_connection: CustomConnection,
    boundaries_dataset: Dataset,
    output_dir: str,
    boundaries_file: str | None = None,
) -> None:
    """Download ERA5 products from the Climate Data Store."""
    # cds = CDS(key=cds_connection.key)
    cds_client = ApiClient(key=cds_connection.key, url="https://cds.climate.copernicus.eu/api")
    dataset = "reanalysis-era5-pressure-levels-monthly-means"
    variable = "relative_humidity"
    current_run.log_info("Successfully connected to the Climate Data Store")

    boundaries = read_boundaries(boundaries_dataset, filename=boundaries_file)
    bounds = get_bounds(boundaries)
    current_run.log_info(f"Using area of interest: {bounds}")

    if not end_date:
        # Let's set the end date to the last day of the previous month
        end_date = (datetime.now().astimezone(timezone.utc).replace(day=1) - relativedelta(days=1)).strftime("%Y-%m-%d")
        current_run.log_info(f"End date set to {end_date}")

    output_dir = Path(workspace.files_path, output_dir, variable)
    output_dir.mkdir(parents=True, exist_ok=True)

    start = period_from_string(datetime.strptime(start_date, "%Y-%m-%d").strftime("%Y%m"))
    end = period_from_string(datetime.strptime(end_date, "%Y-%m-%d").strftime("%Y%m"))
    periods = start.get_range(end)

    try:
        download_data(
            client=cds_client, periods=periods, variable=variable, dataset=dataset, output_dir=output_dir, bounds=bounds
        )

    except Exception as e:
        current_run.log_error(f"Error downloading humidity data: {e}")
        raise ValueError(f"Invalid date format: {e}")


def download_data(
    client: ApiClient,
    periods: list,
    variable: str,
    dataset: str,
    output_dir: Path,
    bounds: tuple[int],
) -> None:
    """Download ERA5 humidity data for specified periods."""
    for period in periods:
        current_run.log_info(f"Downloading for period year: {period.year} - {period.month}")
        dst_file = output_dir / f"data_{period.year}_{period.month:02}.grib"
        if dst_file.exists():
            current_run.log_info(f"Data for {period} already exists, skipping download.")
            continue

        request = {
            "product_type": ["monthly_averaged_reanalysis"],
            "variable": [variable],
            "pressure_level": ["1000"],
            "year": [period.year],
            "month": [period.month],
            "time": ["00:00"],
            "data_format": "grib",
            "download_format": "unarchived",
            "area": list(bounds),
        }

        try:
            client.retrieve(collection_id=dataset, target=dst_file, request=request)
        except Exception as e:
            current_run.log_warning(f"Failed to retrieve data for period: {period} - {e}")


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


def get_bounds(boundaries: gpd.GeoDataFrame) -> tuple[int]:
    """Extract bounding box coordinates of the input geodataframe.

    Parameters
    ----------
    boundaries : gpd.GeoDataFrame
        Geopandas GeoDataFrame containing boundaries geometries

    Return
    ------
    tuple[int]
        Bounding box coordinates in the order (ymax, xmin, ymin, xmax)
    """
    xmin, ymin, xmax, ymax = boundaries.total_bounds
    xmin = ceil(xmin - 0.5)
    ymin = ceil(ymin - 0.5)
    xmax = ceil(xmax + 0.5)
    ymax = ceil(ymax + 0.5)
    return ymax, xmin, ymin, xmax


if __name__ == "__main__":
    era5_extract()
