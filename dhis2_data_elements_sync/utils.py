import os
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta
import sqlite3
from pathlib import Path
import threading

# from typing import List
import json
from openhexa.sdk import current_run, workspace
from openhexa.toolbox.dhis2 import DHIS2


def connect_to_dhis2(connection_str: str, cache_dir: str):
    try:
        connection = workspace.dhis2_connection(connection_str)
        dhis2_client = DHIS2(connection=connection, cache_dir=cache_dir)
        current_run.log_info(f"Connected to DHIS2 connection: {connection.url}")
        return dhis2_client
    except Exception as e:
        raise Exception(f"Error while connecting to DHIS2 {connection_str}: {e}")


def load_configuration(config_path: Path) -> dict:
    """
    Reads a JSON file configuration and returns its contents as a dictionary.

    Args:
        pipeline_path (str): Root path of the pipeline to find the file.

    Returns:
        dict: Dictionary containing the JSON data.
    """
    try:
        with Path.open(config_path, "r") as file:
            data = json.load(file)

        current_run.log_info(f"Configuration loaded from {config_path}.")
        return data
    except FileNotFoundError as e:
        raise Exception(f"The file '{config_path}' was not found {e}")
    except json.JSONDecodeError as e:
        raise Exception(f"Error decoding JSON: {e}")
    except Exception as e:
        raise Exception(f"Unexpected error while loading configuration '{config_path}' {e}")


def retrieve_ou_list(dhis2_client: DHIS2, ou_level: int) -> list:
    try:
        # Retrieve organisational units and filter by ou_level
        ous = pd.DataFrame(dhis2_client.meta.organisation_units())
        ou_list = ous.loc[ous.level == ou_level].id.to_list()

        # Log the result based on the OU level
        if ou_level == 5:
            current_run.log_info(f"Retrieved SNIS DHIS2 FOSA id list {len(ou_list)}")
        elif ou_level == 4:
            current_run.log_info(f"Retrieved SNIS DHIS2 Aires de Sante id list {len(ou_list)}")
        else:
            current_run.log_info(f"Retrieved SNIS DHIS2 OU level {ou_level} id list {len(ou_list)}")

        return ou_list

    except Exception as e:
        raise Exception(f"Error while retrieving OU id list for level {ou_level}: {e}")


def merge_dataframes(dataframes: list[pd.DataFrame]) -> pd.DataFrame:
    """
    Merge a list of dataframes, excluding None values.
    Assume they shared the same columns.

    Args:
        dataframes (list[pd.DataFrame]): A list of dataframes to merge.

    Returns:
        pd.DataFrame: Concatenated dataframe, or None if all inputs are None.
    """
    # Filter out None values from the list
    not_none_df = [df for df in dataframes if df is not None]

    # Check if all columns match
    if len(not_none_df) > 1:
        first_columns = set(not_none_df[0].columns)
        for df in not_none_df[1:]:
            if set(df.columns) != first_columns:
                raise ValueError("DataFrames have mismatched columns and cannot be concatenated.")

    # Concatenate if there are valid dataframes, else return None
    return pd.concat(not_none_df) if not_none_df else None


def first_day_of_future_month(date: str, months_to_add: int) -> str:
    """
    Compute the first day of the month after adding a given number of months.

    Args:
        date_str (str): A date in the "YYYYMM" format.
        months_to_add (int): Number of months to add.

    Returns:
        str: The resulting date in "YYYY-MM-DD" format.
    """
    # Parse the input date string
    input_date = datetime.strptime(date, "%Y%m")
    target_date = input_date + relativedelta(months=months_to_add)

    return target_date.strftime("%Y-%m-01")


def save_to_parquet(data: pd.DataFrame, filename: str):
    """
    Safely saves a DataFrame to a Parquet file.

    Args:
        data (pd.DataFrame): The DataFrame to save.
        file_path (str): The path where the Parquet file will be saved.

    Returns:
        None
    """
    try:
        # Ensure the data is a DataFrame
        if not isinstance(data, pd.DataFrame):
            raise TypeError("The 'data' parameter must be a pandas DataFrame.")

        # Save the DataFrame as a Parquet file
        data.to_parquet(
            os.path.join(filename),
            engine="pyarrow",
            index=False,
        )

    except Exception as e:
        raise Exception(f"An unexpected error occurred while saving the parquet file: {e}")


def read_parquet_extract(parquet_file: Path) -> pd.DataFrame:
    try:
        ou_source = pd.read_parquet(parquet_file)
    except FileNotFoundError:
        raise FileNotFoundError(f"Error while loading the extract: File was not found {parquet_file}.")
    except pd.errors.EmptyDataError:
        pd.errors.EmptyDataError(f"Error while loading the extract: File is empty {parquet_file}.")
    except Exception as e:
        Exception(f"Error while loading the extract: {parquet_file}. Error: {e}")

    return ou_source


def read_json_file(file_path):
    """
    Reads a JSON file and handles potential errors.

    Parameters:
        file_path (str): The path to the JSON file.

    Returns:
        dict or list: Parsed JSON data if successful.
        None: If an error occurs.
    """
    try:
        with open(file_path, "r") as file:
            data = json.load(file)
        return data
    except FileNotFoundError:
        raise FileNotFoundError(f"Error: The file '{file_path}' was not found.")
    except json.JSONDecodeError as e:
        raise json.JSONDecodeError(f"Error: Failed to decode JSON in the file '{file_path}'. Details: {e}")
    except Exception as e:
        raise Exception(f"Unexpected error while reading the file '{file_path}': {e}")


def split_list(src_list: list, length: int):
    """Split list into chunks."""
    for i in range(0, len(src_list), length):
        yield src_list[i : i + length]


# my queue class
class Queue:
    def __init__(self, db_path: str):
        """
        Initialize the queue with the given SQLite database path.
        """
        self.db_path = db_path
        self._lock = threading.Lock()
        self._initialize_queue()

    def _initialize_queue(self):
        """
        Create the queue table if it does not exist.
        """
        with self._lock:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS queue (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        period TEXT NOT NULL
                    )
                """)
                conn.commit()

    def enqueue(self, period: str):
        """
        Add a new period to the queue only if it does not already exist.
        """
        with self._lock:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT COUNT(*) FROM queue WHERE period = ?", (period,))
                exists = cursor.fetchone()[0]

                if not exists:  # Insert only if the period does not exist
                    cursor.execute("INSERT INTO queue (period) VALUES (?)", (period,))
                    conn.commit()

    def dequeue(self):
        """
        Remove and return the first period in the queue.
        """
        with self._lock:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT id, period FROM queue ORDER BY id LIMIT 1")
                item = cursor.fetchone()
                if item:
                    cursor.execute("DELETE FROM queue WHERE id = ?", (item[0],))
                    conn.commit()
                    return item[1]
            return None

    def peek(self):
        """
        Return the first period in the queue without removing it.
        """
        with self._lock:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT period FROM queue ORDER BY id LIMIT 1")
                item = cursor.fetchone()
                return item[0] if item else None

    def count(self) -> int:
        """
        Return the number of items in the queue.
        """
        with self._lock:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT COUNT(*) FROM queue")
                return cursor.fetchone()[0]

    def reset(self):
        """
        Clear all contents from the queue and reset the indexing.
        """
        with self._lock:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("DROP TABLE IF EXISTS queue;")  # Drop table to reset indexing
                conn.commit()
                self._initialize_queue()  # Recreate table

    def view_queue(self):
        """
        View all elements in the queue without removing them.
        """
        with self._lock:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT id, period FROM queue ORDER BY id")
                items = cursor.fetchall()
                if items:
                    print("Queue contents:")
                    for item in items:
                        print(f"ID: {item[0]}, Period: {item[1]}")
                else:
                    print("The queue is empty.")

    def count_queue_items(self) -> int:
        """
        Count the number of items in the queue.
        """
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM queue")
            count = cursor.fetchone()[0]
        return count


def build_id_indexes(ou_source, ou_target, ou_matching_ids):
    # Set "id" as the index for faster lookup
    df1_lookup = {val: idx for idx, val in enumerate(ou_source["id"])}
    df2_lookup = {val: idx for idx, val in enumerate(ou_target["id"])}

    # Build the dictionary using prebuilt lookups
    index_dict = {
        match_id: {"source": df1_lookup[match_id], "target": df2_lookup[match_id]}
        for match_id in ou_matching_ids
        if match_id in df1_lookup and match_id in df2_lookup
    }
    return index_dict


class OrgUnitObj:
    """Helper class definition to store/create the correct OrgUnit JSON format"""

    def __init__(self, orgUnit_row: pd.Series):
        """Create a new org unit instance.

        Parameters
        ----------
        orgUnit_row : pandas series
            Expects columns with names :
                ['id', 'name', 'shortName', 'openingDate', 'closedDate', 'parent','level', 'path', 'geometry']
        """
        self.initialize_from(orgUnit_row.squeeze(axis=0))

    def initialize_from(self, row: pd.Series):
        # let's keep names consistent
        self.id = row.get("id")
        self.name = row.get("name")
        self.shortName = row.get("shortName")
        self.openingDate = row.get("openingDate")
        self.closedDate = row.get("closedDate")
        self.parent = row.get("parent")
        geometry = row.get("geometry")
        self.geometry = json.loads(geometry) if isinstance(geometry, str) else geometry

    def to_json(self) -> dict:
        json_dict = {
            "id": self.id,
            "name": self.name,
            "shortName": self.shortName,
            "openingDate": self.openingDate,
            "closedDate": self.closedDate,
            "parent": {"id": self.parent.get("id")} if self.parent else None,
        }
        if self.geometry:
            geometry = json.loads(self.geometry) if isinstance(self.geometry, str) else self.geometry
            json_dict["geometry"] = {
                "type": geometry["type"],
                "coordinates": geometry["coordinates"],
            }
        return {k: v for k, v in json_dict.items() if v is not None}

    def is_valid(self):
        if self.id is None:
            return False
        if self.name is None:
            return False
        if self.shortName is None:
            return False
        if self.openingDate is None:
            return False
        if self.parent is None:
            return False

        return True

    def __str__(self):
        return f"OrgUnitObj({self.id}, {self.name})"

    def copy(self):
        attributes = self.to_json()
        new_instance = OrgUnitObj(pd.Series(attributes))
        return new_instance


class DataPoint:
    """Helper class definition to store/create the correct DataElement JSON format"""

    def __init__(self, series_row: pd.Series):
        """Create a new org unit instance.

        Parameters
        ----------
        series_row : pandas series
            Expects columns with names :
                ['data_type',
                'dx_uid',
                'period',
                'org_unit',
                'category_option_combo',
                'attribute_option_combo',
                'rate_type',
                'domain_type',
                'value']
        """
        row = series_row.squeeze(axis=0)
        self.dataType = row.get("data_type")
        self.dataElement = row.get("dx_uid")
        self.period = row.get("period")
        self.orgUnit = row.get("org_unit")
        self.categoryOptionCombo = row.get("category_option_combo")
        self.attributeOptionCombo = row.get("attribute_option_combo")
        self.value = row.get("value")

    def to_json(self) -> dict:
        json_dict = {
            "dataElement": self.dataElement,
            "period": self.period,
            "orgUnit": self.orgUnit,
            "categoryOptionCombo": self.categoryOptionCombo,
            "attributeOptionCombo": self.attributeOptionCombo,
            "value": self.value,
        }
        # return {k: v for k, v in json_dict.items() if v is not None}
        return json_dict

    def to_delete_json(self) -> dict:
        json_dict = {
            "dataElement": self.dataElement,
            "period": self.period,
            "orgUnit": self.orgUnit,
            "categoryOptionCombo": self.categoryOptionCombo,
            "attributeOptionCombo": self.attributeOptionCombo,
            "value": "",
            "comment": "deleted value",
        }
        return json_dict

    def _check_attributes(self, exclude_value=False):
        # List of attributes to check, optionally excluding the 'value' attribute
        attributes = [self.dataElement, self.period, self.orgUnit, self.categoryOptionCombo, self.attributeOptionCombo]
        if not exclude_value:
            attributes.append(self.value)

        # Return True if all attributes are not None
        return all(attr is not None for attr in attributes)

    def is_valid(self):
        # Check if all attributes are valid (None check)
        return self._check_attributes(exclude_value=False)

    def is_to_delete(self):
        # Check if all attributes except 'value' are not None and 'value' is None
        return self._check_attributes(exclude_value=True) and self.value is None

    def __str__(self):
        return f"DataPoint({self.dataType} id:{self.dataElement} pe:{self.period} ou:{self.orgUnit} value:{self.value})"
