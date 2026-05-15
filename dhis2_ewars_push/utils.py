from collections.abc import Iterator
import polars as pl

from openhexa.sdk import current_run
from openhexa.toolbox.dhis2.dataframe import get_organisation_unit_levels, InvalidParameterError
from openhexa.toolbox.dhis2 import DHIS2


def split_list(src_list: list, length: int) -> Iterator[list]:
    """Split list into chunks.

    Args:
        src_list (list): The list to split.
        length (int): The length of each chunk.

    Yields:
        list: Chunks of the original list of the specified length.
    """
    for i in range(0, len(src_list), length):
        yield src_list[i : i + length]


def get_response_value_errors(response, chunk) -> dict:
    """Collect relevant data for error logs.

    Returns
    -------
        dict: A dictionary containing the response type, status, description, import count, and conflicts.
    """
    if response is None:
        return None

    if chunk is None:
        return None

    try:
        out = {}
        for k in ["responseType", "status", "description", "importCount", "dataSetComplete"]:
            out[k] = response.get(k)
        if response.get("conflicts"):
            out["rejected_datapoints"] = []
            for i in response["rejectedIndexes"]:
                out["rejected_datapoints"].append(chunk[i])
            out["conflicts"] = {}
            for conflict in response["conflicts"]:
                out["conflicts"]["object"] = conflict.get("object")
                out["conflicts"]["objects"] = conflict.get("objects")
                out["conflicts"]["value"] = conflict.get("value")
                out["conflicts"]["errorCode"] = conflict.get("errorCode")
        return out
    except AttributeError:
        return None


def get_organisation_units(
    dhis2: DHIS2, max_level: int | None = None, filters: list[str] | None = None
) -> pl.DataFrame:
    """Extract organisation units metadata.

    Parameters
    ----------
    dhis2 : DHIS2
        DHIS2 instance.
    max_level : int, optional
        Maximum level of organisation units to extract. If None, all levels are extracted.
    filters : list[str], optional
        DHIS2 query filter expressions.

    Returns
    -------
    pl.DataFrame
        Dataframe containing organisation units metadata with the following columns: id, name,
        level, level_{level}_id, level_{level}_name, geometry.

    Raises
    ------
    InvalidParameter
        If max_level is greater than the maximum level of the organisation units.
    """
    levels = get_organisation_unit_levels(dhis2)
    if max_level:
        if max_level > levels["level"].max():
            msg = f"max_level cannot be greater than {levels['level'].max()}"
            current_run.log_error(msg)
            raise InvalidParameterError(msg)
        level_filter = f"level:le:{max_level}"
        if filters:
            filters = [*filters, max_level]
        else:
            filters = [level_filter]

    meta = dhis2.meta.organisation_units(fields="id,name,level,path,openingDate,geometry", filters=filters)

    schema = {
        "id": str,
        "name": str,
        "level": int,
        "path": str,
        "openingDate": str,
        "geometry": str,
    }
    df = pl.DataFrame(data=meta, schema=schema)

    for row in levels.iter_rows(named=True):
        lvl = row["level"]
        if max_level:
            if lvl > max_level:
                continue

        df = df.with_columns(
            pl.col("path").str.split("/").list.slice(1).list.get(lvl - 1, null_on_oob=True).alias(f"level_{lvl}_id")
        )

        df = df.join(
            other=df.select("id", pl.col("name").alias(f"level_{lvl}_name")),
            left_on=f"level_{lvl}_id",
            right_on="id",
            how="left",
        )

    df = df.select(
        "id",
        "name",
        "level",
        pl.col("openingDate").str.to_datetime("%Y-%m-%dT%H:%M:%S.%3f").alias("opening_date"),
        *[col for col in df.columns if col.startswith("level_")],
        "geometry",
    )

    return df.sort(by=["level", "name"], descending=False)
