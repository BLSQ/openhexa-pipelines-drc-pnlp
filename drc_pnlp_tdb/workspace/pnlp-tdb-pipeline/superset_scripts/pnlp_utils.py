import time

import pandas as pd
import polars as pl
from openhexa.sdk import current_run
from sqlalchemy import text
from sqlalchemy.engine import Engine


def push_dataframe_to_db(
    df: pd.DataFrame | pl.DataFrame,
    table_name: str,
    engine: Engine,
    schema: str = "public",
    push_behaviour: str = "replace",
    chunksize: int = 10000,
) -> None:
    """Push a dataframe to a database table using SQLAlchemy engine."""
    if not isinstance(df, pd.DataFrame):
        df = df.to_pandas()

    start = time.time()
    print(f"Pushing data ({df.shape[0]} rows) to '{table_name}' (if_exists: {push_behaviour})..")
    # Push data in chunks
    df.to_sql(
        table_name,
        con=engine,
        schema="public",
        if_exists=push_behaviour,  # create table if it doesn't exist with "replace"
        index=False,
        chunksize=chunksize,  # rows at the time
    )

    end = time.time()
    minutes, seconds = divmod(end - start, 60)
    print(f"Import finished, elapsed time: {int(minutes)} min {seconds:.2f} sec")

         
def check_partition_exist(engine: Engine, table: str, year: int) -> bool:
    """Checks if a partition for a given year exists in the specified table.

    Returns
    -------
    bool
        True if the partition exists, False otherwise.
    """
    with engine.begin() as conn:
        exists = conn.execute(
            text(f"""SELECT 1 FROM pg_class c 
                JOIN pg_inherits i ON c.oid = i.inhrelid 
                JOIN pg_class p ON p.oid = i.inhparent
                WHERE p.relname = '{table}' AND c.relname = '{table}_{year}';""")
        ).scalar()
    return bool(exists)


def log_msg(msg: str, level: str = "info") -> None:
    """Logs a message to the OpenHexa current run and prints it to the console."""
    if "current_run" in globals() and current_run:
        if level == "info":
            current_run.log_info(msg)
        if level == "warning":
            current_run.log_warning(msg)
        if level == "error":
            current_run.log_error(msg)
    print(f"{level} : {msg}")


def upsert_partition(engine: Engine, target_table: str, tmp_table: str, year_target: int) -> None:
    """Creates the parent partitioned table if it doesn't exist.

    Then either updates or creates the yearly partition and inserts data from tmp_table.
    """
    # create parent table if it doesn't exist
    with engine.begin() as conn:
        conn.execute(
            text(f"""
            CREATE TABLE IF NOT EXISTS public."{target_table}" (
                LIKE public."{tmp_table}" INCLUDING ALL
            ) PARTITION BY LIST ("annee");
        """)
        )

    # handle partition update
    if check_partition_exist(engine, target_table, year_target):
        with engine.begin() as conn:
            try:
                log_msg(f"Partition for year {year_target} already exists, updating (truncate/insert) data..")
                conn.execute(text(f'TRUNCATE TABLE public."{target_table}_{year_target}";'))
                conn.execute(text(f'INSERT INTO public."{target_table}" SELECT * FROM public."{tmp_table}";'))
            except Exception as e:
                print(f"Error while updating the partition '{target_table}'. {e}")
                raise
            finally:
                conn.execute(text(f'DROP TABLE IF EXISTS public."{tmp_table}";'))
                print(f"Table '{tmp_table}' deleted.")
    else:
        with engine.begin() as conn:
            try:
                log_msg(f"Creating new partition '{target_table}_{year_target}'..")
                conn.execute(
                    text(
                        f'CREATE TABLE public."{target_table}_{year_target}"'
                        f' PARTITION OF public."{target_table}"'
                        f" FOR VALUES IN ({year_target});"
                    )
                )
                conn.execute(text(f'INSERT INTO public."{target_table}" SELECT * FROM public."{tmp_table}";'))
            except Exception as e:
                print(f"Error while creating the partition '{target_table}'. {e}")
                raise
            finally:
                conn.execute(text(f'DROP TABLE IF EXISTS public."{tmp_table}";'))
                print(f"Table '{tmp_table}' deleted.")


def create_partition_indexes(engine: Engine, target_table: str, year_target: int, index_columns: list[str]) -> None:
    """Creates indexes on the yearly partition table if they don't already exist."""
    with engine.begin() as conn:
        table_name = f"{target_table}_{year_target}"
        index_name = f"idx_{target_table}_{year_target}_filters"
        columns = ", ".join(index_columns)
        log_msg(f"Creating index '{index_name}' on '{table_name}'..")
        conn.execute(
            text(f"""
            CREATE INDEX IF NOT EXISTS "{index_name}"
            ON public."{table_name}"
            ({columns});
        """)
        )
