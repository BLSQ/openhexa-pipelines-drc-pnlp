import os
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta
import sqlite3
from typing import List
import json


def get_periods_yyyymm(start_period: str, end_period: str) -> list:
    """
    Generate all monthly periods between start_period and end_period (inclusive).

    Args:
        start_period (str): The start period in YYYYMM format.
        end_period (str): The end period in YYYYMM format.

    Returns:
        list: A list of monthly periods in YYYYMM format.
    """
    # Convert input strings to datetime objects
    start_date = datetime.strptime(start_period, "%Y%m")
    end_date = datetime.strptime(end_period, "%Y%m")

    # Check that start_date is not after end_date
    if start_date > end_date:
        raise ValueError("start_period must not be after end_period")

    # Generate periods
    periods = []
    current_date = start_date
    while current_date <= end_date:
        periods.append(current_date.strftime("%Y%m"))
        next_month = current_date.month % 12 + 1
        next_year = current_date.year + (current_date.month // 12)
        current_date = current_date.replace(year=next_year, month=next_month)

    return periods


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


def initialize_queue(db_path: str):
    """
    Initialize the items as TEXT
    #"""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("""
                CREATE TABLE IF NOT EXISTS queue (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    period TEXT NOT NULL
                )
            """)
    conn.commit()


def enqueue(period: str, db_path: str):
    """
    Add a new period to the queue only if it does not already exist.
    """
    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM queue WHERE period = ?", (period,))
        exists = cursor.fetchone()[0]

        if not exists:  # Insert only if the period does not exist
            cursor.execute("INSERT INTO queue (period) VALUES (?)", (period,))
            conn.commit()


def dequeue(db_path: str):
    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT id, period FROM queue ORDER BY id LIMIT 1")
        item = cursor.fetchone()
        if item:
            cursor.execute("DELETE FROM queue WHERE id = ?", (item[0],))
            conn.commit()
            return item[1]
        return None


def reset_queue(db_path: str):
    """
    Clear all contents from the queue.
    """
    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        cursor.execute("DELETE FROM queue;")
        conn.commit()
        cursor.execute("VACUUM")
        conn.commit()


def count_queue_items(db_path: str) -> int:
    """
    Count the number of items in the queue.
    """
    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM queue")
        count = cursor.fetchone()[0]
    return count


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


def read_parquet_extract(parquet_file: str) -> pd.DataFrame:
    ou_source = pd.DataFrame()
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


def split_list(src_list: list, length: int) -> List[list]:
    """Split list into chunks."""
    for i in range(0, len(src_list), length):
        yield src_list[i : i + length]
