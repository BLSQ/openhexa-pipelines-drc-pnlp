import polars as pl


def format_time_period_mensuel(df: pl.DataFrame, data_type: str) -> pl.DataFrame:
    """Formats a DHIS2 dataframe to monthly aggregation.

    Replicates R's pivot_wider/pivot_longer behavior: the set of
    Agg_period x ID_UID combinations is determined globally (only
    combinations that exist anywhere in the data), then every
    (Annee, Filtre_time, [grouping cols], LEVEL_5_UID) row gets all
    of those combinations, with null where no real data exists.

    data_type: 'DE' (data elements), 'IND' (indicators), 'CP' (completeness/promptitude)

    Returns:
        pl.DataFrame with columns Agg_period, Annee, Filtre_time, [grouping cols], LEVEL_5_UID, and value.
    """
    if df.is_empty():
        return pl.DataFrame()

    # derive Annee from PERIOD if YEAR column doesn't exist
    if "YEAR" in df.columns:
        annee_expr = pl.col("YEAR").cast(pl.Int32).alias("Annee")
    else:
        annee_expr = pl.col("PERIOD").cast(pl.Utf8).str.slice(0, 4).cast(pl.Int32).alias("Annee")

    df = df.with_columns(
        [
            pl.col("PERIOD").cast(pl.Utf8).str.strptime(pl.Date, "%Y%m").dt.strftime("%b %Y").alias("Agg_period"),
            pl.lit("Mensuel").alias("Filtre_time"),
            annee_expr,
        ]
    )

    if data_type == "DE":
        id_col = "DE_UID"
        id_group_cols = ["Annee", "Filtre_time", "COC_NAME", "LEVEL_5_UID"]
        metadata_cols = [
            "OU_UID",
            "OU_NAME",
            "LEVEL_5_NAME",
            "LEVEL_4_UID",
            "LEVEL_4_NAME",
            "LEVEL_3_UID",
            "LEVEL_3_NAME",
            "LEVEL_2_UID",
            "LEVEL_2_NAME",
            "LEVEL_1_UID",
            "LEVEL_1_NAME",
        ]
        full_group_cols = id_group_cols + ["Agg_period", id_col]
    elif data_type == "IND":  # mortality
        df = df.with_columns([pl.lit(None).cast(pl.Utf8).alias("COC_NAME"), pl.col("IND_UID").alias("DE_UID")])
        id_col = "DE_UID"
        id_group_cols = ["Annee", "Filtre_time", "COC_NAME", "LEVEL_5_UID"]
        metadata_cols = [
            "OU_UID",
            "OU_NAME",
            "LEVEL_5_NAME",
            "LEVEL_4_UID",
            "LEVEL_4_NAME",
            "LEVEL_3_UID",
            "LEVEL_3_NAME",
            "LEVEL_2_UID",
            "LEVEL_2_NAME",
            "LEVEL_1_UID",
            "LEVEL_1_NAME",
        ]
        full_group_cols = id_group_cols + ["Agg_period", id_col]
    elif data_type == "CP":
        id_col = "DS_UID"
        id_group_cols = ["Annee", "REPORTING_TYPE", "Filtre_time", "LEVEL_5_UID"]
        metadata_cols = [
            "OU_UID",
            "OU_NAME",
            "LEVEL_5_NAME",
            "LEVEL_4_UID",
            "LEVEL_4_NAME",
            "LEVEL_3_UID",
            "LEVEL_3_NAME",
            "LEVEL_2_UID",
            "LEVEL_2_NAME",
            "LEVEL_1_UID",
            "LEVEL_1_NAME",
        ]
        full_group_cols = id_group_cols + ["Agg_period", id_col]
    else:
        raise ValueError(f"data_type must be 'DE', 'IND' or 'CP', got '{data_type}'")

    # aggregate actual values first (this is the agg_helper equivalent)
    agg = df.group_by(full_group_cols).agg(pl.col("VALUE").sum().alias("value"))

    # global set of (Agg_period, id_col) combinations that exist anywhere
    period_id_combos = df.select(["Agg_period", id_col]).unique()

    # every id_group_cols row gets crossed with every period_id_combos
    id_groups = df.select(id_group_cols).unique()
    full_grid = id_groups.join(period_id_combos, how="cross")

    # left join the aggregated values onto the full grid -> NA where missing
    result = full_grid.join(agg, on=full_group_cols, how="left")

    # bring back metadata columns via a join on LEVEL_5_UID
    metadata = df.select(["LEVEL_5_UID"] + metadata_cols).unique()
    return result.join(metadata, on="LEVEL_5_UID", how="left")


def calculer_indicateur_lvl5(
    df: pl.DataFrame,
    indicateur_name: str,
    numerator: list[str],
    denominator: list[str],
) -> pl.DataFrame:
    """Calculates an indicator from numerator and denominator DE_UIDs.

    Aggregates at LEVEL_5.

    indicateur_name: str, name of the indicator
    numerator: list of DE_UIDs for the numerator
    denominator: list of DE_UIDs for the denominator

    Returns:
        pl.DataFrame with columns Agg_period, Annee, Filtre_time, Indicateur_name,
        Indicateur_num, Indicateur_den, and LEVEL_1-5 NAME/UID columns.
    """
    empty_schema = {
        "Agg_period": pl.Utf8,
        "Annee": pl.Int32,
        "Filtre_time": pl.Utf8,
        "Indicateur_name": pl.Utf8,
        "Indicateur_num": pl.Float64,
        "Indicateur_den": pl.Float64,
        "LEVEL_1_NAME": pl.Utf8,
        "LEVEL_1_UID": pl.Utf8,
        "LEVEL_2_NAME": pl.Utf8,
        "LEVEL_2_UID": pl.Utf8,
        "LEVEL_3_NAME": pl.Utf8,
        "LEVEL_3_UID": pl.Utf8,
        "LEVEL_4_NAME": pl.Utf8,
        "LEVEL_4_UID": pl.Utf8,
        "LEVEL_5_NAME": pl.Utf8,
        "LEVEL_5_UID": pl.Utf8,
    }

    if df.is_empty():
        print(f"Warning: '{indicateur_name}' — input dataframe is empty.")
        return pl.DataFrame(schema=empty_schema)

    geo_cols = [
        "LEVEL_5_NAME",
        "LEVEL_5_UID",
        "LEVEL_4_NAME",
        "LEVEL_4_UID",
        "LEVEL_3_NAME",
        "LEVEL_3_UID",
        "LEVEL_2_NAME",
        "LEVEL_2_UID",
        "LEVEL_1_NAME",
        "LEVEL_1_UID",
    ]
    time_cols = ["Annee", "Agg_period", "Filtre_time"]
    all_de_uids = numerator + denominator

    # filter to DE_UIDs of interest
    df = df.filter(pl.col("DE_UID").is_in(all_de_uids))
    group_cols = geo_cols + ["COC_NAME"] + time_cols

    # select relevant columns
    df = df.select(group_cols + ["DE_UID", "value"])

    # pivot wide: one column per DE_UID
    df = df.pivot(values="value", index=group_cols, on="DE_UID", aggregate_function=None)

    # sum numerator and denominator columns
    num_cols = [c for c in df.columns if c in numerator]
    den_cols = [c for c in df.columns if c in denominator]

    if not num_cols and not den_cols:
        print(f"Warning: '{indicateur_name}' — no numerator nor denominator DE_UIDs found in data.")
        num_expr = pl.lit(None).cast(pl.Float64).alias("num_sum")
        den_expr = pl.lit(None).cast(pl.Float64).alias("den_sum")
    elif not num_cols:
        print(f"Warning: '{indicateur_name}' — no numerator DE_UIDs found in data.")
        num_expr = pl.lit(None).cast(pl.Float64).alias("num_sum")
        den_expr = pl.sum_horizontal([pl.col(c) for c in den_cols]).alias("den_sum")
    elif not den_cols:
        print(f"Warning: '{indicateur_name}' — no denominator DE_UIDs found in data.")
        num_expr = pl.sum_horizontal([pl.col(c) for c in num_cols]).alias("num_sum")
        den_expr = pl.lit(None).cast(pl.Float64).alias("den_sum")
    else:
        num_expr = pl.sum_horizontal([pl.col(c) for c in num_cols]).alias("num_sum")
        den_expr = pl.sum_horizontal([pl.col(c) for c in den_cols]).alias("den_sum")

    df = df.with_columns([num_expr, den_expr])

    # aggregate at LEVEL_5
    agg_group_cols = geo_cols + time_cols

    df = df.group_by(agg_group_cols).agg(
        [
            pl.col("num_sum").sum(),
            pl.col("den_sum").sum(),
        ]
    )

    # replace zeros with null
    df = df.with_columns(
        [
            pl.when(pl.col("num_sum") == 0)
            .then(None)
            .otherwise(pl.col("num_sum"))
            .cast(pl.Float64)
            .alias("Indicateur_num"),
            pl.when(pl.col("den_sum") == 0)
            .then(None)
            .otherwise(pl.col("den_sum"))
            .cast(pl.Float64)
            .alias("Indicateur_den"),
            pl.lit(indicateur_name).alias("Indicateur_name"),
        ]
    )

    # format output
    return df.select(
        [
            "Agg_period",
            "Annee",
            "Filtre_time",
            "Indicateur_name",
            "Indicateur_num",
            "Indicateur_den",
            "LEVEL_1_NAME",
            "LEVEL_1_UID",
            "LEVEL_2_NAME",
            "LEVEL_2_UID",
            "LEVEL_3_NAME",
            "LEVEL_3_UID",
            "LEVEL_4_NAME",
            "LEVEL_4_UID",
            "LEVEL_5_NAME",
            "LEVEL_5_UID",
        ]
    )


def aggreger_reporting_lvl5(
    df: pl.DataFrame,
    reporting_type: str,
    reporting_uid: str,
    indicateur_name: str,
    groups: pl.DataFrame,
) -> pl.DataFrame:
    """Aggregates reporting data for a given indicator at LEVEL_5.

    df: polars dataframe (cp mensuel)
    reporting_type: str, e.g. 'REPORTING_RATE'
    reporting_uid: str, DS_UID to filter on
    indicateur_name: str, name of the indicator
    groups: polars dataframe of organisation unit groups (organisation_unit_groups.csv)

    Returns:
        pl.DataFrame with columns Agg_period, Annee, Filtre_time, Indicateur_name,
        Indicateur_num, Indicateur_den, and LEVEL_1-5 NAME/UID columns.
    """
    empty_schema = {
        "Agg_period": pl.Utf8,
        "Annee": pl.Int32,
        "Filtre_time": pl.Utf8,
        "Indicateur_name": pl.Utf8,
        "Indicateur_num": pl.Float64,
        "Indicateur_den": pl.Float64,
        "LEVEL_1_NAME": pl.Utf8,
        "LEVEL_1_UID": pl.Utf8,
        "LEVEL_2_NAME": pl.Utf8,
        "LEVEL_2_UID": pl.Utf8,
        "LEVEL_3_NAME": pl.Utf8,
        "LEVEL_3_UID": pl.Utf8,
        "LEVEL_4_NAME": pl.Utf8,
        "LEVEL_4_UID": pl.Utf8,
        "LEVEL_5_NAME": pl.Utf8,
        "LEVEL_5_UID": pl.Utf8,
    }

    if df.is_empty():
        print(f"Warning: '{indicateur_name}' — input dataframe is empty.")
        return pl.DataFrame(schema=empty_schema)

    # load org unit group depending on indicator
    if "hosp" in indicateur_name.lower():
        ou_group_name = "D - Service Hôpital"
    elif "second" in indicateur_name.lower():
        ou_group_name = "B - Services Secondaires"
    else:
        ou_group_name = "A - Services de Base"

    # get list of org units for the group
    ou_list_str = groups.filter(pl.col("oug_shortname") == ou_group_name)["org_units"][0]
    ou_list = ou_list_str.split(";")

    # replace NAs with 0
    df = df.with_columns(pl.col("value").fill_null(0))

    # filter to data of interest
    df = df.filter(
        (pl.col("DS_UID") == reporting_uid)
        & (pl.col("REPORTING_TYPE") == reporting_type)
        & (pl.col("LEVEL_5_UID").is_in(ou_list))
    ).select(
        [
            "LEVEL_5_NAME",
            "LEVEL_5_UID",
            "LEVEL_4_NAME",
            "LEVEL_4_UID",
            "LEVEL_3_NAME",
            "LEVEL_3_UID",
            "LEVEL_2_NAME",
            "LEVEL_2_UID",
            "LEVEL_1_NAME",
            "LEVEL_1_UID",
            "DS_UID",
            "value",
            "Annee",
            "Agg_period",
            "Filtre_time",
        ]
    )

    if df.is_empty():
        print(f"Warning: '{indicateur_name}' — no data found after filtering.")
        return pl.DataFrame(schema=empty_schema)

    # aggregate at LEVEL_5
    agg_group_cols = [
        "LEVEL_5_NAME",
        "LEVEL_5_UID",
        "LEVEL_4_NAME",
        "LEVEL_4_UID",
        "LEVEL_3_NAME",
        "LEVEL_3_UID",
        "LEVEL_2_NAME",
        "LEVEL_2_UID",
        "LEVEL_1_NAME",
        "LEVEL_1_UID",
        "Annee",
        "Agg_period",
        "Filtre_time",
    ]

    df = df.group_by(agg_group_cols).agg(
        [
            pl.col("value").sum().alias("num_sum"),
            (pl.lit(100) * pl.len()).alias("den_sum"),
        ]
    )

    # add indicator name
    df = df.with_columns(
        [
            pl.col("num_sum").cast(pl.Float64).alias("Indicateur_num"),
            pl.col("den_sum").cast(pl.Float64).alias("Indicateur_den"),
            pl.lit(indicateur_name).alias("Indicateur_name"),
        ]
    )

    # format output
    return df.select(
        [
            "Agg_period",
            "Annee",
            "Filtre_time",
            "Indicateur_name",
            "Indicateur_num",
            "Indicateur_den",
            "LEVEL_1_NAME",
            "LEVEL_1_UID",
            "LEVEL_2_NAME",
            "LEVEL_2_UID",
            "LEVEL_3_NAME",
            "LEVEL_3_UID",
            "LEVEL_4_NAME",
            "LEVEL_4_UID",
            "LEVEL_5_NAME",
            "LEVEL_5_UID",
        ]
    )


def sommer_indicateur_lvl5(
    df: pl.DataFrame,
    indicateur_name: str,
    element_ids: list[str],
) -> pl.DataFrame:
    """Sums a single indicator from a list of DE_UIDs.

    Aggregates at LEVEL_5.

    indicateur_name: str, name of the indicator
    element_ids: list of DE_UIDs to sum

    Returns:
        pl.DataFrame with columns Agg_period, Annee, Filtre_time, Indicateur_name,
        Indicateur_num, Indicateur_den, and LEVEL_1-5 NAME/UID columns.
    """
    empty_schema = {
        "Agg_period": pl.Utf8,
        "Annee": pl.Int32,
        "Filtre_time": pl.Utf8,
        "Indicateur_name": pl.Utf8,
        "Indicateur_num": pl.Float64,
        "Indicateur_den": pl.Float64,
        "LEVEL_1_NAME": pl.Utf8,
        "LEVEL_1_UID": pl.Utf8,
        "LEVEL_2_NAME": pl.Utf8,
        "LEVEL_2_UID": pl.Utf8,
        "LEVEL_3_NAME": pl.Utf8,
        "LEVEL_3_UID": pl.Utf8,
        "LEVEL_4_NAME": pl.Utf8,
        "LEVEL_4_UID": pl.Utf8,
        "LEVEL_5_NAME": pl.Utf8,
        "LEVEL_5_UID": pl.Utf8,
    }

    if df.is_empty():
        print(f"Warning: '{indicateur_name}' — input dataframe is empty.")
        return pl.DataFrame(schema=empty_schema)

    geo_cols = [
        "LEVEL_5_NAME",
        "LEVEL_5_UID",
        "LEVEL_4_NAME",
        "LEVEL_4_UID",
        "LEVEL_3_NAME",
        "LEVEL_3_UID",
        "LEVEL_2_NAME",
        "LEVEL_2_UID",
        "LEVEL_1_NAME",
        "LEVEL_1_UID",
    ]
    time_cols = ["Annee", "Agg_period", "Filtre_time"]

    # filter to DE_UIDs of interest
    df = df.filter(pl.col("DE_UID").is_in(element_ids))

    if df.is_empty():
        print(f"Warning: '{indicateur_name}' — no data found after filtering.")
        return pl.DataFrame(schema=empty_schema)

    group_cols = geo_cols + ["COC_NAME"] + time_cols

    # select relevant columns
    df = df.select(group_cols + ["DE_UID", "value"])

    # pivot wide: one column per DE_UID
    df = df.pivot(values="value", index=group_cols, on="DE_UID", aggregate_function=None)

    # sum element columns
    element_cols = [c for c in df.columns if c in element_ids]

    if not element_cols:
        print(f"Warning: '{indicateur_name}' — no element DE_UIDs found in data.")
        ind_expr = pl.lit(None).cast(pl.Float64).alias("ind_sum")
    else:
        ind_expr = pl.sum_horizontal([pl.col(c) for c in element_cols]).alias("ind_sum")

    df = df.with_columns([ind_expr])

    # aggregate at LEVEL_5
    agg_group_cols = geo_cols + time_cols

    df = df.group_by(agg_group_cols).agg(
        [
            pl.col("ind_sum").sum().alias("num_sum"),
        ]
    )

    # add indicator name and constant denominator
    df = df.with_columns(
        [
            pl.col("num_sum").cast(pl.Float64).alias("Indicateur_num"),
            pl.lit(1.0, dtype=pl.Float64).alias("Indicateur_den"),
            pl.lit(indicateur_name).alias("Indicateur_name"),
        ]
    )

    # format output
    return df.select(
        [
            "Agg_period",
            "Annee",
            "Filtre_time",
            "Indicateur_name",
            "Indicateur_num",
            "Indicateur_den",
            "LEVEL_1_NAME",
            "LEVEL_1_UID",
            "LEVEL_2_NAME",
            "LEVEL_2_UID",
            "LEVEL_3_NAME",
            "LEVEL_3_UID",
            "LEVEL_4_NAME",
            "LEVEL_4_UID",
            "LEVEL_5_NAME",
            "LEVEL_5_UID",
        ]
    )


def run_indicators_nd_aggregations_lvl5(df_year: pl.DataFrame, group_mappings: dict) -> pl.DataFrame:
    """Perform aggregations for all indicators at LEVEL_5 (Fosa) and for all temporal levels.

    Returns:
        pl.DataFrame with columns: LEVEL_1_NAME, LEVEL_2_NAME, LEVEL_3_NAME, LEVEL_4_NAME, LEVEL_5_NAME, LEVEL_5_UID,
        Indicateur_name, Annee, Indicateur_num, Indicateur_den, Absolute_indicateur_value, Taux_indicateur_value,
        Aggregation_temporelle, Period, Indicateur_group
    """
    if df_year.is_empty():
        print("Warning: input dataframe is empty.")
        return pl.DataFrame()

    # Spatial & temporal hierarchies
    spatial_name = "Fosa"
    spatial_cols = ["LEVEL_1_NAME", "LEVEL_2_NAME", "LEVEL_3_NAME", "LEVEL_4_NAME", "LEVEL_5_NAME", "LEVEL_5_UID"]

    temporal_levels = {
        "Année": "Year_period",
        "Semestre": "Semester_period",
        "Trimestre": "Quarter_period",
        "Mois": "Month_period",
    }

    standard_cols = [
        "LEVEL_1_NAME",
        "LEVEL_2_NAME",
        "LEVEL_3_NAME",
        "LEVEL_4_NAME",
        "LEVEL_5_NAME",
        "LEVEL_5_UID",
        "Indicateur_name",
        "Annee",
        "Indicateur_num",
        "Indicateur_den",
        "Absolute_indicateur_value",
        "Taux_indicateur_value",
        "Aggregation_temporelle",
        "Period",
        "Indicateur_group",
    ]

    # Add year and period columns
    print("Creating period columns..")
    df_pl = df_year.with_columns(pl.col("Agg_period").str.strptime(pl.Date, "%b %Y").alias("Agg_date"))
    df_pl = df_pl.with_columns(
        pl.col("Annee").cast(pl.Int32),
        pl.col("Agg_date").dt.year().cast(pl.Utf8).alias("Year_period"),
        (
            pl.col("Agg_date").dt.year().cast(pl.Utf8)
            + " S"
            + (((pl.col("Agg_date").dt.month() - 1) // 6 + 1).cast(pl.Utf8))
        ).alias("Semester_period"),
        (
            pl.col("Agg_date").dt.year().cast(pl.Utf8)
            + " T"
            + (((pl.col("Agg_date").dt.month() - 1) // 3 + 1).cast(pl.Utf8))
        ).alias("Quarter_period"),
        pl.col("Agg_date").dt.strftime("%Y-%m").alias("Month_period"),
    )

    agg_list_pl = []
    print(f"Processing → Spatial: {spatial_name}")
    for temporal_name, temporal_col in temporal_levels.items():
        print(f"Processing → Temporal: {temporal_name}")

        group_cols = spatial_cols + ["Indicateur_name", temporal_col, "Annee"]

        # Polars groupby + aggregation
        agg_df = df_pl.group_by(group_cols, maintain_order=True).agg(
            [pl.sum("Indicateur_num").alias("Indicateur_num"), pl.sum("Indicateur_den").alias("Indicateur_den")]
        )

        # Compute absolute value and rate
        agg_df = agg_df.with_columns(
            [
                pl.col("Indicateur_num").alias("Absolute_indicateur_value"),
                (
                    pl.when(pl.col("Indicateur_den") != 0)
                    .then(pl.col("Indicateur_num") / pl.col("Indicateur_den"))
                    .otherwise(None)
                ).alias("Taux_indicateur_value"),
            ]
        )

        # Ensure all spatial columns exist with correct type (Utf8)
        for col in ["LEVEL_1_NAME", "LEVEL_2_NAME", "LEVEL_3_NAME", "LEVEL_4_NAME", "LEVEL_5_NAME", "LEVEL_5_UID"]:
            if col not in agg_df.columns:
                agg_df = agg_df.with_columns(pl.lit(None, dtype=pl.Utf8).alias(col))

        agg_df = agg_df.with_columns(
            pl.col("Indicateur_name").replace(group_mappings).cast(pl.Utf8).alias("Indicateur_group")
        )

        # Metadata
        agg_df = agg_df.with_columns(
            [
                pl.lit(spatial_name).alias("Aggregation_spatiale"),
                pl.lit(temporal_name).alias("Aggregation_temporelle"),
                pl.col(temporal_col).alias("Period"),
                pl.col("Indicateur_name")
                .str.replace_all("_", " ")  # replace all underscores
                .str.to_lowercase()
                .str.to_titlecase()
                .alias("Indicateur_name"),
            ]
        )

        # Drop the temporal column
        agg_df = agg_df.drop(temporal_col)

        # Reorder agg_df and append
        agg_df = agg_df.select([col for col in standard_cols if col in agg_df.columns])
        agg_list_pl.append(agg_df)

    # Concatenate all results
    df_year_agg_pl = pl.concat(agg_list_pl)
    df_year_agg_pl = df_year_agg_pl.rename(
        {
            "LEVEL_2_NAME": "Province",
            "LEVEL_3_NAME": "Zone_de_sante",
            "LEVEL_4_NAME": "Aire_de_sante",
            "LEVEL_5_NAME": "Fosa",
            "LEVEL_5_UID": "Fosa_dhis2_id",
        }
    )
    df_year_agg_pl = df_year_agg_pl.drop("LEVEL_1_NAME")  # drop

    return df_year_agg_pl.rename({c: c.lower() for c in df_year_agg_pl.columns})  # cols to lower
