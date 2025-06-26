import pandas as pd
import polars as pl
import os
from datetime import datetime, date, timedelta
from unidecode import unidecode
from rapidfuzz import fuzz, process
import json
import requests

from openhexa.sdk import pipeline, workspace, current_run, parameter
from openhexa.toolbox.dhis2 import DHIS2
from openhexa.toolbox.dhis2.dataframe import get_organisation_units

import config
from ewars_client import EWARSClient

from utils import split_list, get_response_value_errors


@pipeline("dhis2-ewars-push")
@parameter("extract_pyramids", name="Extract the pyramids", type=bool, default=True)
@parameter(
    "extract_all_ewars",
    name="Extract all of the EWARS form",
    help="Even if they are already present",
    type=bool,
    default=False,
)
@parameter(
    "date_start",
    name="Start date for the extraction",
    help="Format DDMMYYYY. Included to the largest epi-week.",
    type=int,
    required=True,
    default=20241001,
)
@parameter(
    "date_end",
    name="End date for the extraction",
    help="Format DDMMYYYY. Included to the largest epi-week.",
    type=int,
    required=True,
    default=20241031,
)
def dhis2_ewars_push(extract_pyramids, extract_all_ewars, date_start, date_end):
    """
    ADD SUMMARY OF PIPELINE HERE.
    """
    ewars = get_ewars()
    dhis2_snis = get_dhis2("drc-snis")
    dhis_nmdr = get_dhis2("dhis2-nmdr-drc")
    ewars_pyramid = get_ewars_pyramid(extract_pyramids, ewars)
    dhis2_pyramid = get_dhis2_pyramid(extract_pyramids, dhis2_snis)
    full_pyramid = match_pyramid(df_ewars=ewars_pyramid, df_dhis2=dhis2_pyramid, extract_pyramids=extract_pyramids)
    check_pyramid(full_pyramid)
    list_dates = get_list_dates(date_start, date_end)
    ewars_extract_list = extract_ewars_forms(list_dates, ewars, extract_all_ewars)
    ewars_extract_concat = concat_ewars_forms(ewars_extract_list)
    ewars_formated = format_ewars_extract(ewars_extract_concat, full_pyramid)
    ewars_dhis2 = put_dhis2_format(ewars_formated)
    summary = push_data_elements(dhis_nmdr, ewars_dhis2)


@dhis2_ewars_push.task
def put_dhis2_format(ewars_extract: pd.DataFrame):
    """
    Adapt the Ewars data to a format we can push to DHIS2.

    Parameters
    ----------
    ewars_extract : pd.DataFrame
        The ewars extract. It contains all of the Ewars data that we want to push to DHIS2.

    Returns
    -------
    List[dict]
        The ewars extract in the DHIS2 format.
    """
    current_run.log_info("Adapting the ewars extract to the DHIS2 format...")
    values_to_post = []
    for index, row in ewars_extract.iterrows():
        service = row["variable"]
        dataElement = config.dict_dE_CoC[service][0]
        categoryOptionCombo = config.dict_dE_CoC[service][1]
        orgUnit = row["dhis2_level_4_id"]
        value = str(row["value"])
        period = row["epi_week"].replace("-W", "W")
        values_to_post.append(
            {
                "dataElement": dataElement,
                "categoryOptionCombo": categoryOptionCombo,
                "orgUnit": orgUnit,
                "value": value,
                "period": period,
            }
        )

    return values_to_post


@dhis2_ewars_push.task
def get_ewars_pyramid(extract_pyramids: bool, ewars: EWARSClient):
    """
    Get the ewars pyramid, either from a file or by extracting it from the API.

    Parameters
    ----------
    extract_pyramids : bool
        If True, we will extract the pyramid from the API. If False, we will use the already extracted pyramid.
        (if it exists)

    Returns
    -------
    pd.DataFrame
        The cleaned ewars pyramid.
    """
    path_pyramid = f"{workspace.files_path}/pipelines/dhis2_ewars_push/raw/pyramids/ewars_pyramid.parquet"
    if extract_pyramids or not os.path.exists(path_pyramid):
        ewars_pyramid = extract_ewars_pyramid(ewars)
        path_pyramid = f"{workspace.files_path}/pipelines/dhis2_ewars_push/raw/pyramids/ewars_pyramid.parquet"
        ewars_pyramid.to_parquet(path_pyramid)
    else:
        ewars_pyramid = pd.read_parquet(path_pyramid)
        current_run.log_info("Using the already extracted ewars pyramid")

    return ewars_pyramid


@dhis2_ewars_push.task
def get_dhis2_pyramid(extract_pyramids: bool, dhis2: DHIS2):
    """
    Get the DHIS2 pyramid, either from a file or by extracting it from the API.

    Parameters
    ----------
    extract_pyramids : bool
        If True, we will extract the pyramid from the API. If False, we will use the already extracted pyramid.
        (if it exists)
    dhis2 : DHIS2
        The DHIS2 client.

    Returns
    -------
    pd.DataFrame
        The cleaned DHIS2 pyramid.
    """
    path_pyramid = f"{workspace.files_path}/pipelines/dhis2_ewars_push/raw/pyramids/dhis2_pyramid.parquet"
    if extract_pyramids or not os.path.exists(path_pyramid):
        dhis2_pyramid = extract_dhis2_pyramid(dhis2)
        path_pyramid = f"{workspace.files_path}/pipelines/dhis2_ewars_push/raw/pyramids/dhis2_pyramid.parquet"
        dhis2_pyramid.to_parquet(path_pyramid)
    else:
        dhis2_pyramid = pd.read_parquet(path_pyramid)
        current_run.log_info("Using the already extracted DHIS2 pyramid")

    return dhis2_pyramid


@dhis2_ewars_push.task
def match_pyramid(df_ewars: pd.DataFrame, df_dhis2: pd.DataFrame, extract_pyramids: bool):
    """
    Match the ewars and dhis2 pyramids.

    We do it iteratively per level.
        - We start my matching the first level.
        - Then, for each of the ewars second level names, we try to match them with the corresponding dhis second level names.
        (Where corresponding means that they have the same first level).
        - We do this iteratively for all of the levels, always demanding that the previous levels are the same before doing the match.

    We do it iteratively per threshold.
        - We try to do the match with a very high threshold first.
        - If we failed at matching some names, we try to do the match with a lower threshold.
        - We do this iteratively until we reach the lowest threshold.

    Parameters
    ----------
    df_ewars : pd.DataFrame
        The ewars dataframe. It contains all of the Ewars data that we want to match.
    df_dhis2 : pd.DataFrame
        The dhis2 dataframe. It contains all of the DHIS2 data that we can use to do the match.
    extract_pyramids : bool
        If True, we will extract the pyramids from the API. If False, we will use the already extracted pyramids.

    Returns
    -------
    pd.DataFrame
        The matched dataframe. It contains all of the matched names and ids.
    """
    path_full_pyramid = f"{workspace.files_path}/pipelines/dhis2_ewars_push/raw/pyramids/full_pyramid.parquet"
    if extract_pyramids or not os.path.exists(path_full_pyramid):
        current_run.log_info("Creating the full pyramid by matching ewars and dhis2 pyramids")
    else:
        full_pyramid = pd.read_parquet(path_full_pyramid)
        current_run.log_info("Using the already extracted full pyramid")
        return full_pyramid

    df_ewars_to_be_matched = df_ewars.copy()
    list_match = []

    for threshold in config.thresholds_match:
        levels_already_matched = []
        list_no_match_threshold = []
        df_match_threshold = pd.DataFrame()

        for level in config.list_levels:
            current_run.log_info(f"Doing the matching for the level {level} and threshold {threshold}")
            df_match_threshold, df_no_match_level = match_level(
                df_match_threshold, df_ewars_to_be_matched, df_dhis2, level, levels_already_matched, threshold
            )
            levels_already_matched.append(level)
            if df_no_match_level.empty:
                current_run.log_info(f"All names matched for level {level} with threshold {threshold}")
            else:
                list_no_match_threshold.append(df_no_match_level)

        list_match.append(df_match_threshold)

        if len(list_no_match_threshold) > 0:
            df_ewars_to_be_matched = pd.concat(list_no_match_threshold, ignore_index=True)
        else:
            current_run.log_info(f"All names matched with maximum threshold {threshold}")
            break

    else:
        current_run.log_info(
            f"Some ewars names could not be matched with the maximum threshold {threshold}. I have saved them in a CSV file."
        )
        dt = datetime.now().strftime("%Y%m%d_%H%M%S")
        path_df_no_match = f"{workspace.files_path}/pipelines/dhis2_ewars_push/processed/not_matched/ewars_data_not_matched_{dt}.parquet"
        df_ewars_to_be_matched.to_parquet(path_df_no_match)

    df_match = pd.concat(list_match, ignore_index=True)
    path_df_match = f"{workspace.files_path}/pipelines/dhis2_ewars_push/raw/pyramids/full_pyramid.parquet"
    df_match.to_parquet(path_df_match)

    return df_match


@dhis2_ewars_push.task
def check_pyramid(pyramid: pd.DataFrame):
    """
    Some checks on the pyramid.
    """
    current_run.log_info("Checking the created pyramid for incoherences...")
    repeated_dhis2_id = pyramid[pyramid["dhis2_level_4_id"].duplicated(keep=False)]
    # If the ewars name is also repeated, we do not care -- it is just that the same ewars name has two ids.
    unique_pairs = repeated_dhis2_id[
        ~repeated_dhis2_id.duplicated(subset=["dhis2_level_4_id", "ewars_level_4_name_cleaned"], keep=False)
    ]
    repeated_ewars_id = pyramid[pyramid["ewars_level_4_id"].duplicated(keep=False)]
    if not unique_pairs.empty:
        current_run.log_error(
            f"Some DHIS2 level 4 ids are duplicated in the pyramid. {len(unique_pairs)} duplicates found."
        )
        dt = datetime.now().strftime("%Y%m%d_%H%M%S")
        path_dhis2_repeated = (
            f"{workspace.files_path}/pipelines/dhis2_ewars_push/processed/repeated_values/dhis2_repeated_{dt}.parquet"
        )
        unique_pairs.to_parquet(path_dhis2_repeated)
    if not repeated_ewars_id.empty:
        current_run.log_error(
            f"Some EWARS level 4 ids are duplicated in the pyramid. {len(repeated_ewars_id)} duplicates found."
        )
        dt = datetime.now().strftime("%Y%m%d_%H%M%S")
        path_ewars_repeated = (
            f"{workspace.files_path}/pipelines/dhis2_ewars_push/processed/repeated_values/ewars_repeated_{dt}.parquet"
        )
        repeated_ewars_id.to_parquet(path_ewars_repeated)


@dhis2_ewars_push.task
def get_list_dates(date_start: int, date_end: int):
    """
    Get the list of dates. For the start, we use the start of the epi-week of the date_start.
    For the end, we use the end of the epi-week of the date_end.

    Parameters
    ----------
    date_start : int
        The start date in the format DDMMYYYY.
    date_end : int
        The end date in the format DDMMYYYY.

    Returns
    -------
    list
        The list of dates between date_start and date_end.
    """
    start_date = datetime.strptime(str(date_start), "%Y%m%d")
    end_date = datetime.strptime(str(date_end), "%Y%m%d")

    start_date_epi = get_beginning_of_epi_week(start_date)
    end_date_epi = get_beginning_of_epi_week(end_date) + timedelta(days=6)

    list_dates = pd.date_range(start=start_date_epi, end=end_date_epi).to_list()
    current_run.log_info(f"I am going to extract the ewars forms from {start_date_epi} to {end_date_epi}")
    return list_dates


@dhis2_ewars_push.task
def extract_ewars_forms(list_dates: list, ewars: EWARSClient, extract_all_ewars: bool):
    list_ewars_forms = []
    for date in list_dates:
        date_str = date.strftime("%Y-%m-%d")
        current_run.log_info(f"Extracting the ewars form {config.form_id} for the date {date_str}")
        file_name = f"form-{config.form_id}_date-{date_str}.parquet"
        path_file = f"{workspace.files_path}/pipelines/dhis2_ewars_push/raw/ewars_forms/{file_name}"

        if os.path.exists(path_file) and not extract_all_ewars:
            current_run.log_info(f"File {file_name} already exists. Skipping the extraction.")
            ewars_form = pd.read_parquet(path_file)
            list_ewars_forms.append(ewars_form)

        else:
            ewars_form = ewars.get_reports_for_date(config.form_id, date_str)
            if not ewars_form.empty:
                ewars_form = ewars_form.drop(columns=["history"])
                ewars_form.to_parquet(path_file)
                list_ewars_forms.append(ewars_form)
            else:
                current_run.log_info(f"The ewars form {config.form_id} is empty for the date {date_str}")

    return list_ewars_forms


@dhis2_ewars_push.task
def concat_ewars_forms(list_ewars_forms: list):
    """
    Concatenate the ewars forms.

    Parameters
    ----------
    list_ewars_forms : list
        The list of ewars forms.

    Returns
    -------
    pd.DataFrame
        The concatenated ewars forms.
    """
    ewars = pd.concat(list_ewars_forms, ignore_index=True)
    ewars = ewars[config.relevant_ewars_forms_cols]

    return ewars


@dhis2_ewars_push.task
def format_ewars_extract(ewars_not_melted: pd.DataFrame, full_pyramid: pd.DataFrame):
    """
    Format the ewars extract.
    - Melt the malaria columns.
    - Get the DHIS2 OU ids for the pyramid.
    - Add the relevant date columns.

    Parameters
    ----------
    ewars_not_melted : pd.DataFrame
        The ewars extract.
    full_pyramid : pd.DataFrame
        The full pyramid, containing the matched ewars and dhis2 ids.

    Returns
    -------
    pd.DataFrame
        The formatted ewars extract.
    """
    pl_ewars_not_melted = pl.from_pandas(ewars_not_melted)
    pl_full_pyramid = pl.from_pandas(full_pyramid)

    pl_ewars_not_melted = pl_ewars_not_melted.join(
        pl_full_pyramid, left_on="location_id", right_on="ewars_level_4_id", how="left"
    )
    current_run.log_info("I have merged the ewars extract with the full pyramid.")

    pl_ewars_not_melted = remove_unwanted_locations(pl_ewars_not_melted)

    pl_ewars_extract = pl_ewars_not_melted.melt(
        id_vars=config.relevant_info_cols + config.relevant_level_cols,
        variable_name="variable",
        value_name="value",
    )
    current_run.log_info("I have melted the ewars extract and summed the values.")

    pl_ewars_extract = pl_ewars_extract.with_columns(pl.col("date").str.to_date())
    pl_ewars_extract = pl_ewars_extract.with_columns(
        pl.col("date").map_elements(lambda date: get_epi_week(date), return_dtype=pl.Utf8).alias("epi_week"),
    ).drop("date")
    current_run.log_info("I have added the epi_week column to the ewars extract.")
    pl_ewars_extract = look_at_value_col(pl_ewars_extract)

    pl_ewars_extract = pl_ewars_extract.group_by(
        ["location_id", "variable", "epi_week"] + config.relevant_level_cols
    ).agg(pl.col("value").sum().alias("value"))
    current_run.log_info("I have summed the values for each location and variable.")

    ewars_extract = pl_ewars_extract.to_pandas()

    # ewars_extract = ewars_extract[config.ewars_formated_cols]

    dt = datetime.now().strftime("%Y%m%d_%H%M%S")
    path_ewars_extract = (
        f"{workspace.files_path}/pipelines/dhis2_ewars_push/processed/ewars_forms/ewars_extract_{dt}.parquet"
    )
    ewars_extract.to_parquet(path_ewars_extract)

    return ewars_extract


def remove_unwanted_locations(ewars_extract: pl.DataFrame):
    """
    There are some locations that have been badly matched. We will remove them from the push
    (We would not like to push incorrrect data).

    Parameters
    ----------
    ewars_extract : pl.DataFrame
        The ewars extract.

    Returns
    -------
    pl.DataFrame
        The ewars extract without the unwanted locations.
    """
    ewars_extract_mod = ewars_extract.filter(~pl.col("ewars_level_4_name_cleaned").is_in(config.list_ewars_to_remove))
    # With this line you also drop the NULLs.
    removed_repeated = ewars_extract.filter(pl.col("ewars_level_4_name_cleaned").is_in(config.list_ewars_to_remove))
    if not removed_repeated.is_empty:
        current_run.log_info(
            f"I have removed {removed_repeated.height} locations that were not correctly matched from the ewars extract."
        )
        path = (
            f"{workspace.files_path}/pipelines/dhis2_ewars_push/processed/repeated_values/repeated_removed_{dt}.parquet"
        )
        removed_repeated.write_parquet(path)

    return ewars_extract_mod


def look_at_value_col(ewars_extract: pl.DataFrame):
    """
    Look at the values extracted from EWARS to detect the unusual ones.

    Parameters
    ----------
    ewars_extract : pl.DataFrame
        The ewars extract.

    Returns
    -------
    pl.DataFrame
        The ewars extract with the values formatted.
    """
    current_run.log_info("Looking at the values extracted from EWARS to detect the unusual ones...")
    ewars_numeric = ewars_extract.clone()

    ewars_numeric = ewars_numeric.with_columns(pl.col("value").cast(pl.Float64, strict=False))

    ser_non_numeric = ewars_extract.select(pl.col("value").is_null().alias("non_numeric"))["non_numeric"]
    ser_multiple_zeros = ewars_extract.select(
        pl.col("value").is_in(["0.0", ".0", "00", "000"]).alias("multiple_zeros")
    )["multiple_zeros"]
    ser_non_integers = ewars_numeric.select(
        ((pl.col("value") % 1 != 0) & pl.col("value").is_not_null()).alias("non_integers")
    )["non_integers"]
    ser_big_values = ewars_numeric.select((pl.col("value") > 1000).alias("big_values"))["big_values"]
    ser_seems_okey = ~ser_non_numeric & ~ser_multiple_zeros & ~ser_non_integers & ~ser_big_values

    ewars_non_numeric = ewars_numeric.filter(ser_non_numeric)
    ewars_multiple_zeros = ewars_numeric.filter(ser_multiple_zeros)
    ewars_non_integers = ewars_numeric.filter(ser_non_integers)
    ewars_big_values = ewars_numeric.filter(ser_big_values)
    ewars_seems_okey = ewars_numeric.filter(ser_seems_okey)

    if not ewars_non_numeric.height > 0:
        current_run.log_info(
            "There are non numeric values in the ewars_extract. I have saved all of the strange values in a csv."
        )
    if not ewars_multiple_zeros.height > 0:
        current_run.log_info(
            "There are weirdly formatted zeros in the ewars_extract. I have saved all of the strange values in a csv."
        )
    if not ewars_non_integers.height > 0:
        current_run.log_info(
            "There are non-integer values in the ewars_extract. I have saved all of the strange values in a csv."
        )
    if not ewars_big_values.height > 0:
        current_run.log_info(
            "There are very big values in the ewars_extract. I have saved all of the strange values in a csv."
        )

    ewars_weird = pl.concat([ewars_non_numeric, ewars_multiple_zeros, ewars_non_integers, ewars_big_values])
    if ewars_weird.height > 0:
        dt = datetime.now().strftime("%Y%m%d_%H%M%S")
        path = (
            f"{workspace.files_path}/pipelines/dhis2_ewars_push/processed/strange_ewars_values/strange_ewars_{dt}.csv"
        )
        ewars_weird.write_csv(path)

    return ewars_numeric


def get_epi_week(date_object: date):
    """Get epidemiological week info from date object."""
    week_day = date_object.isoweekday()
    week_day = 0 if week_day == 7 else week_day  # Week : Sun = 0 ; Sat = 6

    # Start the weekday on Sunday (CDC)
    start = date_object - timedelta(days=week_day)
    # End the weekday on Saturday (CDC)
    end = start + timedelta(days=6)

    week_start_year_day = start.timetuple().tm_yday
    week_end_year_day = end.timetuple().tm_yday

    if week_end_year_day in range(4, 11):
        week = 1
    else:
        week = ((week_start_year_day + 2) // 7) + 1

    if week_end_year_day in range(4, 11):
        year = end.year
    else:
        year = start.year

    return f"{year}-W{week:02d}"


def get_beginning_of_epi_week(date_object: date):
    """
    Get the beginning of the epidemiological week for a given date.

    Parameters
    ----------
    date_object : date
        The date for which we want to get the beginning of the epidemiological week.

    Returns
    -------
    date
        The beginning of the epidemiological week.
    """
    week_day = (date_object - timedelta(days=0)).isoweekday()
    week_day = 0 if week_day == 7 else week_day  # Week : Sun = 0 ; Sat = 6
    return date_object - timedelta(days=week_day)


def extract_dhis2_pyramid(dhis2: DHIS2):
    """
    Extract the DHIS2 pyramid from the API. Clean it.

    Parameters
    ----------
    dhis2 : DHIS2
        The DHIS2 client.

    Returns
    -------
    pd.DataFrame
        The cleaned DHIS2 pyramid.
    """
    current_run.log_info("Extracting the DHIS2 pyramid from the API")
    raw_pyramid = get_organisation_units(dhis2).to_pandas()
    pyramid = clean_dhis2_pyramid(raw_pyramid)
    return pyramid


def clean_dhis2_pyramid(raw_pyramid: pd.DataFrame):
    """
    Clean the DHIS2 pyramid dataframe.

    Parameters
    ----------
    df : pd.DataFrame
        The DHIS2 pyramid dataframe.

    Returns
    -------
    pd.DataFrame
        The cleaned DHIS2 pyramid dataframe.
    """
    current_run.log_info("Cleaning the DHIS2 pyramid dataframe...")
    pyramid = raw_pyramid[raw_pyramid["level"] == 4][
        [
            "level_1_name",
            "level_1_id",
            "level_2_name",
            "level_2_id",
            "level_3_name",
            "level_3_id",
            "level_4_name",
            "level_4_id",
        ]
    ]

    pyramid["level_4_name_cleaned"] = pyramid["level_4_name"].apply(
        lambda x: clean_ous_names(x, start_chars_to_remove=2, endings_to_remove=["aire de sante"])
    )
    pyramid["level_3_name_cleaned"] = pyramid["level_3_name"].apply(
        lambda x: clean_ous_names(x, start_chars_to_remove=2, endings_to_remove=["zone de sante"])
    )
    pyramid["level_2_name_cleaned"] = pyramid["level_2_name"].apply(
        lambda x: clean_ous_names(x, start_chars_to_remove=2, endings_to_remove=["province"])
    )
    pyramid["level_1_name_cleaned"] = pyramid["level_1_name"].apply(lambda x: clean_ous_names(x))

    return pyramid


def clean_ewars_pyramid(raw_pyramid: pd.DataFrame):
    """
    Clean the ewars pyramid dataframe.

    Parameters
    ----------
    raw_pyramid : pd.DataFrame
        The ewars pyramid dataframe.

    Returns
    -------
    pd.DataFrame
        The cleaned ewars pyramid dataframe.
    """
    current_run.log_info("Cleaning the ewars pyramid dataframe...")
    nan_fr = (raw_pyramid["name_fr"].isna()) | (raw_pyramid["name_fr"] == "")
    nan_en = (raw_pyramid["name_en"].isna()) | (raw_pyramid["name_en"] == "")
    nan_fr_nonan_en = nan_fr & (~nan_en)
    raw_pyramid.loc[nan_fr_nonan_en, "name_fr"] = raw_pyramid.loc[nan_fr_nonan_en, "name_en"]
    # all_levels = raw_pyramid[
    #    (raw_pyramid["status"] == "ACTIVE") & (raw_pyramid["name_fr"].notna()) & (raw_pyramid["name_fr"] != "")
    # ][["uuid", "name_fr", "parent_id", "location_type_id"]]
    # We are still interested in some disabled locations, so I will not filter on status.
    all_levels = raw_pyramid[(raw_pyramid["name_fr"].notna()) & (raw_pyramid["name_fr"] != "")][
        ["uuid", "name_fr", "parent_id", "location_type_id"]
    ]
    current_run.log_info(f"I have dropped {len(raw_pyramid) - len(all_levels)} rows from the ewars pyramid.")
    level_1 = all_levels[all_levels["location_type_id"] == 11]
    level_2 = all_levels[all_levels["location_type_id"] == 5]
    level_3 = all_levels[all_levels["location_type_id"] == 28]
    level_4 = all_levels[all_levels["location_type_id"].isin([30, 29])]
    pyramid = (
        level_4.merge(
            level_3,
            how="left",
            left_on="parent_id",
            right_on="uuid",
            suffixes=("_level_4", "_level_3"),
        )
        .merge(
            level_2,
            how="left",
            left_on="parent_id_level_3",
            right_on="uuid",
            suffixes=("", "_level_2"),
        )
        .merge(
            level_1,
            how="left",
            left_on="parent_id",
            right_on="uuid",
            suffixes=("", "_level_1"),
        )[
            [
                "uuid_level_4",
                "name_fr_level_4",
                "uuid_level_3",
                "name_fr_level_3",
                "uuid",
                "name_fr",
                "uuid_level_1",
                "name_fr_level_1",
            ]
        ]
    )
    pyramid = pyramid.rename(
        columns={
            "uuid_level_4": "level_4_id",
            "name_fr_level_4": "level_4_name",
            "uuid_level_3": "level_3_id",
            "name_fr_level_3": "level_3_name",
            "uuid": "level_2_id",
            "name_fr": "level_2_name",
            "uuid_level_1": "level_1_id",
            "name_fr_level_1": "level_1_name",
        }
    )
    # Clean EWARS names
    pyramid["level_4_name_cleaned"] = pyramid["level_4_name"].apply(
        lambda x: clean_ous_names(
            x, start_chars_to_remove=0, endings_to_remove=["Aire de santé"], begginings_to_remove=["CS ", "KN "]
        )
    )
    pyramid["level_3_name_cleaned"] = pyramid["level_3_name"].apply(
        lambda x: clean_ous_names(x, begginings_to_remove=["KN ", "NK ", "TP ", "KC ", "KL ", "LM "])
    )
    pyramid["level_2_name_cleaned"] = pyramid["level_2_name"].apply(lambda x: clean_ous_names(x))
    pyramid["level_1_name_cleaned"] = pyramid["level_1_name"].apply(lambda x: clean_ous_names(x))

    # Replace roman numbers by arabic numbers
    for roman, arabic in config.numeric_replacements.items():
        pyramid["level_4_name_cleaned"] = pyramid["level_4_name_cleaned"].str.replace(roman, arabic, regex=True)
        pyramid["level_3_name_cleaned"] = pyramid["level_3_name_cleaned"].str.replace(roman, arabic, regex=True)

    # ad-hoc changes
    for level2, level3_change in config.ewars_level3_replacements.items():
        relevant_level2 = pyramid["level_2_name_cleaned"] == level2
        for value_in, value_out in level3_change.items():
            pyramid.loc[relevant_level2 & (pyramid["level_3_name_cleaned"] == value_in), "level_3_name_cleaned"] = (
                value_out
            )

    for level2, [level3_change, relevant_level4] in config.ewars_level3_replacements_somelevel4.items():
        relevant_level2 = pyramid["level_2_name_cleaned"] == level2
        for value_in, value_out in level3_change.items():
            pyramid.loc[
                (
                    relevant_level2
                    & (pyramid["level_3_name_cleaned"] == value_in)
                    & pyramid["level_4_name_cleaned"].isin(relevant_level4)
                ),
                "level_3_name_cleaned",
            ] = value_out

    for level2, level3_change in config.ewars_level4_replacements.items():
        relevant_level2 = pyramid["level_2_name_cleaned"] == level2
        for level3, level4_change in level3_change.items():
            relevant_level3 = relevant_level2 & (pyramid["level_3_name_cleaned"] == level3)
            for value_in, value_out in level4_change.items():
                pyramid.loc[relevant_level3 & (pyramid["level_4_name_cleaned"] == value_in), "level_4_name_cleaned"] = (
                    value_out
                )
    pyramid = pyramid[~pyramid["level_3_name_cleaned"].isna()]
    pyramid = pyramid.drop_duplicates()
    return pyramid


def clean_ous_names(
    name: str, start_chars_to_remove: int = 0, endings_to_remove: list = [], begginings_to_remove: list = []
):
    """
    Clean the names of the organizational units.
    - Remove the first start_chars_to_remove characters
    - Remove the string ending_to_remove if it is present at the end of the name
    - Normalize the string (remove accents, remove leading and trailing spaces, convert to uppercase)

    Parameters
    ----------
    name : str
        The name of the organizational unit.
    start_chars_to_remove : int, default=0
        The number of characters to remove from the start of the name.
    ending_to_remove : list, default=[]
        The list of strings to remove from the end of the name.
    begginings_to_remove : list, default=[]
        The list of strings to remove from the start of the name.

    Returns
    -------
    str
        The cleaned name of the organizational unit.
    """
    if pd.isna(name):
        return name

    # Remove start characters
    cleaned = name[start_chars_to_remove:] if start_chars_to_remove > 0 else name

    # Normalize
    cleaned_norm = unidecode(cleaned).casefold().strip()

    # Remove the endings if they match (case-insensitive, accent-insensitive)
    for ending in endings_to_remove:
        ending_norm = unidecode(ending).casefold()
        if ending_norm != "" and cleaned_norm.endswith(ending_norm):
            # Remove the actual suffix
            cleaned = cleaned[: -(len(ending))]

    # Remove the begginings if they match (case-insensitive, accent-insensitive)
    for beggining in begginings_to_remove:
        beggining_norm = unidecode(beggining).casefold()
        if beggining_norm != "" and cleaned_norm.startswith(beggining_norm):
            # Remove the actual prefix
            cleaned = cleaned[len(beggining) :]

    # Final cleanup
    return unidecode(cleaned).casefold().upper().strip()


@dhis2_ewars_push.task
def get_dhis2(con_name: str = "drc"):
    """
    Get the DHIS2 instance from the API.
    """
    con_dhis = workspace.dhis2_connection(con_name)
    current_run.log_info("Connected to the DHIS2 instance.")
    return DHIS2(con_dhis)


@dhis2_ewars_push.task
def push_data_elements(
    dhis2_client: DHIS2,
    data_elements_list: list,
    strategy: str = "CREATE_AND_UPDATE",
    dry_run: bool = True,
    max_post: int = 1000,
) -> dict:
    """dry_run: Set to true to get an import summary without actually importing data (DHIS2).

    Returns
    -------
        dict: A summary dictionary containing import counts and errors.
    """
    # max_post instead of MAX_POST_DATA_VALUES
    summary = {
        "import_counts": {"imported": 0, "updated": 0, "ignored": 0, "deleted": 0},
        "import_options": {},
        "ERRORS": [],
    }

    total_datapoints = len(data_elements_list)
    count = 0

    for chunk in split_list(data_elements_list, max_post):
        count = count + 1
        try:
            r = dhis2_client.api.session.post(
                f"{dhis2_client.api.url}/dataValueSets",
                json={"dataValues": chunk},
                params={
                    "dryRun": dry_run,
                    "importStrategy": strategy,
                    "preheatCache": True,
                    "skipAudit": True,
                },  # speed!
            )
            r.raise_for_status()

            try:
                response_json = r.json()
                status = response_json.get("httpStatus")
                response = response_json.get("response")
            except json.JSONDecodeError as e:
                summary["ERRORS"].append(f"Response JSON decoding failed: {e}")  # period: {chunk_period}")
                response_json = None
                status = None
                response = None

            if status != "OK" and response:
                summary["ERRORS"].append(response)

            if response:
                for key in ["imported", "updated", "ignored", "deleted"]:
                    summary["import_counts"][key] += response.get("importCount", {}).get(key, 0)

        except requests.exceptions.RequestException as e:
            try:
                response = r.json().get("response")
            except (ValueError, AttributeError):
                response = None

            if response:
                for key in ["imported", "updated", "ignored", "deleted"]:
                    summary["import_counts"][key] += response["importCount"][key]

            error_response = get_response_value_errors(response, chunk=chunk)
            summary["ERRORS"].append({"error": e, "response": error_response})

        if (count * max_post) % 10000 == 0:
            current_run.log_info(
                f"{count * max_post} / {total_datapoints} data points pushed summary: {summary['import_counts']}"
            )

    current_run.log_info(f"Push completed. Final summary: {summary['import_counts']}")

    return summary


@dhis2_ewars_push.task
def get_ewars(con_name: str = "ewars"):
    """
    Get the EWARS instance from the API.
    """
    ewars_conn = workspace.custom_connection(con_name)
    ewars_client = EWARSClient(
        user=ewars_conn.user, password=ewars_conn.password, aid=ewars_conn.aid, client=ewars_conn.client
    )
    current_run.log_info("Connected to the EWARS instance.")
    return ewars_client


def extract_ewars_pyramid(ewars: EWARSClient):
    """
    Extract the ewars pyramid from the API.

    Parameters
    ----------
    ewars : EWARSClient
        The ewars client.

    Returns
    -------
    pd.DataFrame
        The ewars pyramid.
    """
    current_run.log_info("Extracting the EWARS pyramid from the API")

    """try:
        raw_pyramid = ewars.get_locations()
    except:
        # The API sometimes fails because of some connection issues.
        current_run.log_error("Error while extracting the ewars pyramid from the API. Using the saved one instead.")
        path_pyramid = f"{workspace.files_path}/pipelines/dhis2_ewars_push/raw/pyramids/ewars_locations.parquet"
        raw_pyramid = pd.read_parquet(path_pyramid)"""

    path_pyramid = f"{workspace.files_path}/pipelines/dhis2_ewars_push/raw/pyramids/locations_new.parquet"
    raw_pyramid = pd.read_parquet(path_pyramid)
    pyramid = clean_ewars_pyramid(raw_pyramid)
    return pyramid


def match_level(
    df_previous_match: pd.DataFrame,
    df_ewars: pd.DataFrame,
    df_dhis2: pd.DataFrame,
    target_level: str,
    levels_already_matched: list,
    threshold: int,
):
    """
    We do the matching for a specific level.

    Parameters
    ----------
    df_previous_match : pd.DataFrame
        The previous match dataframe. It contains all of the matched names and ids for the previous levels.
    df_ewars : pd.DataFrame
        The ewars dataframe. It contains all of the Ewars data that we want to match.
    df_dhis2 : pd.DataFrame
        The dhis2 dataframe. It contains all of the DHIS2 data that we can use to do the match.
    target_level : str
        The level of the pyramid that we want to match.
    levels_already_matched : list
        The list of levels that have already been matched.
    threshold : int
        The threshold for the matching.

    Returns
    -------
    df_match_level : pd.DataFrame
        The matched dataframe. It contains all of the matched names and ids for the current level and the ones before
    df_no_match_level : pd.DataFrame
        The not matched dataframe. It contains all of the ewars data for the names we could not match.
    """
    if df_previous_match.empty:
        df_match_level, list_no_match_level = match_name(
            df_ewars, df_dhis2, target_level + "_name_cleaned", target_level + "_id", target_level, threshold
        )
        if len(list_no_match_level) > 0:
            df_no_match_level = construct_not_matched(list_no_match_level, target_level, df_ewars)
        else:
            df_no_match_level = pd.DataFrame()
    else:
        list_match_level = []
        list_no_match_level = []

        for i, row in df_previous_match.iterrows():
            # Select the relevant EWARS and DHIS2 names to match
            relevant_ewars = df_ewars.copy()
            relevant_dhis2 = df_dhis2.copy()
            for level in levels_already_matched:
                relevant_ewars = relevant_ewars[
                    relevant_ewars[level + "_name_cleaned"] == row["ewars_" + level + "_name_cleaned"]
                ]
                relevant_dhis2 = relevant_dhis2[
                    relevant_dhis2[level + "_name_cleaned"] == row["dhis2_" + level + "_name_cleaned"]
                ]

            # Do the matching
            df_match_row, list_no_match_row = match_name(
                df_ewars=relevant_ewars,
                df_dhis2=relevant_dhis2,
                col_name=target_level + "_name_cleaned",
                col_id=target_level + "_id",
                level=target_level,
                threshold=threshold,
            )

            for col in row.index:
                df_match_row[col] = row[col]

            list_match_level.append(df_match_row)
            if len(list_no_match_row) > 0:
                list_no_match_level.append(construct_not_matched(list_no_match_row, target_level, relevant_ewars))

        df_match_level = pd.concat(list_match_level, ignore_index=True)
        if len(list_no_match_level) > 0:
            df_no_match_level = pd.concat(list_no_match_level, ignore_index=True)
        else:
            df_no_match_level = pd.DataFrame()

    return df_match_level, df_no_match_level


def match_name(df_ewars: pd.DataFrame, df_dhis2: pd.DataFrame, col_name: str, col_id: str, level: str, threshold: int):
    """
    Match the names of the ewars and dhis2 dataframes.


    Parameters
    ----------
    df_ewars : pd.DataFrame
        The ewars dataframe. It contains all of the Ewars data that we want to match.
    df_dhis2 : pd.DataFrame
        The dhis2 dataframe. It contains all of the DHIS2 data that we can use to do the match.
    col_name : str
        The name of the column containing the names of the Organizational Units that we want to match.
        It is the same for both dataframes.
    col_id : str
        The name of the column containing the ids of the Organizational Units that we want to match.
        It is the same for both dataframes.
    level : str
        The level of the pyramid that we want to match.
    threshold : int
        The threshold for the matching.

    Returns
    -------
    df_matches : pd.DataFrame
        The matched dataframe. It contains all of the matched names and ids.

    list_no_matches : list
        The list of names that could not be matched. It contains all of the ewars names that could not be matched.
    """
    col_ewars_name = "ewars_" + col_name
    col_dhis2_name = "dhis2_" + col_name
    col_ewars_id = "ewars_" + col_id
    col_dhis2_id = "dhis2_" + col_id
    col_score = "score_" + level

    list_ewars = df_ewars[[col_name, col_id]].drop_duplicates().itertuples(index=False)
    dict_dhis2 = dict(df_dhis2[[col_name, col_id]].drop_duplicates().values)
    list_dhis2_names = list(dict_dhis2.keys())
    list_matches = []
    list_no_matches = []

    for ewars_name, ewars_id in list_ewars:
        best_match = process.extractOne(ewars_name, list_dhis2_names, scorer=fuzz.ratio)
        if best_match[1] >= threshold:
            dhis2_name = best_match[0]
            dhis2_id = dict_dhis2[dhis2_name]
            list_matches.append([ewars_name, dhis2_name, best_match[1], ewars_id, dhis2_id])
        else:
            list_no_matches.append(ewars_name)

    if len(list_matches) > 0:
        df_matches = pd.DataFrame(
            list_matches, columns=[col_ewars_name, col_dhis2_name, col_score, col_ewars_id, col_dhis2_id]
        )
    else:
        df_matches = pd.DataFrame(columns=[col_ewars_name, col_dhis2_name, col_score, col_ewars_id, col_dhis2_id])

    return df_matches, list_no_matches


def construct_not_matched(no_matches: list, level: str, df_ewars: pd.DataFrame):
    """
    Construct the not matched dataframe from the list of names that could not be matched.

    Parameters
    ----------
    no_matches : list
        The list of names that could not be matched. It contains all of the ewars names that could not be matched.
    level : str
        The level of the pyramid of the names in the list.
    df_ewars : pd.DataFrame
        The ewars dataframe.

    Returns
    -------
    pd.DataFrame
        The not matched dataframe. It contains all of the ewars data for the names we could not match.
    """
    return df_ewars[df_ewars[level + "_name_cleaned"].isin(no_matches)]


if __name__ == "__main__":
    dhis2_ewars_push()
