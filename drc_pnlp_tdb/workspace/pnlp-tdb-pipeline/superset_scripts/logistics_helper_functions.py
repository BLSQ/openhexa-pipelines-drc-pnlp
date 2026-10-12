import polars as pl

_FR_MONTHS = {
    1: "janv.",
    2: "févr.",
    3: "mars",
    4: "avr.",
    5: "mai",
    6: "juin",
    7: "juil.",
    8: "août",
    9: "sept.",
    10: "oct.",
    11: "nov.",
    12: "déc.",
}


def format_time_period(df: pl.DataFrame, month_locale: str = "en") -> pl.DataFrame:
    """Format the time period in the dataframe to include additional columns for analysis.

    Returns
    -------
    pl.DataFrame
        A new dataframe with additional columns for year, month, and formatted period.
    """
    df = df.clone()

    # 1. Full OU x PERIOD x DE grid (expand.grid equivalent)
    ou_ids = df.select(pl.col("OU_UID").unique())
    periods = df.select(pl.col("PERIOD").unique())
    de_ids = df.select(pl.col("DE_UID").unique())
    ou_period_de = ou_ids.join(periods, how="cross").join(de_ids, how="cross")

    # 2. COC_UIDs that actually exist for each DE_UID
    de_coc_combos = df.select(["DE_UID", "COC_UID"]).unique()

    # 3. Expand: one row per OU x PERIOD x DE x COC
    rect = ou_period_de.join(de_coc_combos, on="DE_UID", how="inner")

    # 4. Attach actual values (left join -> null where no data)
    rect = rect.join(
        df.select(["OU_UID", "PERIOD", "DE_UID", "COC_UID", "VALUE"]),
        on=["OU_UID", "PERIOD", "DE_UID", "COC_UID"],
        how="left",
    )

    # 5. Metadata
    de_coc_meta = df.select(["DE_UID", "DE_NAME", "COC_UID", "COC_NAME"]).unique()
    ou_meta = df.select(
        [
            "OU_UID",
            "OU_NAME",
            "LEVEL_1_UID",
            "LEVEL_1_NAME",
            "LEVEL_2_UID",
            "LEVEL_2_NAME",
            "LEVEL_3_UID",
            "LEVEL_3_NAME",
            "LEVEL_4_UID",
            "LEVEL_4_NAME",
            "LEVEL_5_UID",
            "LEVEL_5_NAME",
        ]
    ).unique()

    rect = rect.join(de_coc_meta, on=["DE_UID", "COC_UID"], how="inner")
    rect = rect.join(ou_meta, on="OU_UID", how="inner")

    # 6. Derived time columns
    rect = rect.with_columns(pl.col("PERIOD").cast(pl.Utf8))

    rect = rect.with_columns(
        (pl.col("PERIOD").str.slice(0, 4) + "-" + pl.col("PERIOD").str.slice(4, 2) + "-01")
        .str.strptime(pl.Date, "%Y-%m-%d", strict=False)
        .alias("_period_date"),
        pl.col("PERIOD").str.slice(0, 4).cast(pl.Int64, strict=False).alias("Annee"),
        pl.lit("Mensuel").alias("Filtre_time"),
    )

    if month_locale == "fr":
        rect = (
            rect.with_columns(
                pl.col("_period_date").dt.month().replace_strict(_FR_MONTHS, return_dtype=pl.Utf8).alias("_mon"),
                pl.col("_period_date").dt.year().alias("_yr"),
            )
            .with_columns((pl.col("_mon") + " " + pl.col("_yr").cast(pl.Utf8)).alias("Agg_period"))
            .drop(["_mon", "_yr"])
        )
    else:
        rect = rect.with_columns(pl.col("_period_date").dt.strftime("%b %Y").alias("Agg_period"))

    rect = rect.drop("_period_date")

    # 7. Final column selection + rename VALUE -> value
    return rect.select(
        [
            "OU_NAME",
            "Annee",
            "Filtre_time",
            "COC_NAME",
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
            "Agg_period",
            "DE_UID",
            "DE_NAME",
            pl.col("VALUE").alias("value"),
        ]
    )


def pct_sans_rupture_fosa(df: pl.DataFrame, indicateur_name: str) -> pl.DataFrame:
    """Compute the numerator and denominator for the Pct_FOSA_Sans_Rupture indicator, at FOSA (LEVEL_5) granularity.

    Returns
    -------
    pl.DataFrame
        A new dataframe with columns: Agg_period, Annee, Filtre_time, Indicateur_name, DE_NAME,
        Indicateur_num, Indicateur_den, LEVEL_1_NAME, LEVEL_1_UID, LEVEL_2_NAME, LEVEL_2_UID, LEVEL_3_NAME,
        LEVEL_3_UID, LEVEL_4_NAME, LEVEL_4_UID, LEVEL_5_NAME, LEVEL_5_UID
    """
    # 1. Subset to the stock-out-days DEs, flagged via the DE_NAME hack
    df = df.filter(
        (pl.col("COC_NAME") == "Nbre de jours de rupture de stock")
        & pl.col("DE_NAME").str.contains("Zero or Positive Int")
    )

    # 2. Flag "no rupture": value == 0 or missing both count as no stock-out
    df = df.with_columns(((pl.col("value") == 0) | pl.col("value").is_null()).alias("no_rupture"))

    group_cols_fosa = [
        "Agg_period",
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
        "Filtre_time",
    ]

    if indicateur_name == "Pct_FOSA_Sans_Rupture_ACT":
        # 3a. Restrict to ACT products (Artemether / Artesunate)
        df = df.filter(pl.col("DE_NAME").str.contains("Artemether|sunate"))

        # A FOSA counts as "no rupture" for the period only if ALL its ACT
        # products were rupture-free that period
        df = (
            df.group_by(group_cols_fosa)
            .agg((pl.col("no_rupture").sum() == pl.len()).alias("no_rupture"))
            .with_columns(pl.lit("Groupe ACT").alias("DE_NAME"))
        )

    elif indicateur_name == "Pct_FOSA_Sans_Rupture_Tous_Produits":
        # Same logic, across every tracer product
        df = (
            df.group_by(group_cols_fosa)
            .agg((pl.col("no_rupture").sum() == pl.len()).alias("no_rupture"))
            .with_columns(pl.lit("Tous les produits traceurs").alias("DE_NAME"))
        )
    # else: leave df at its per-product, per-FOSA granularity as-is
    # (mirrors the empty `else {}` branch in the R original)

    # 4. Aggregate at FOSA (LEVEL_5) level -- no roll-up beyond this
    group_cols_l5 = [
        "Agg_period",
        "DE_NAME",
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
        "Filtre_time",
    ]

    agg_combined = df.group_by(group_cols_l5).agg(
        pl.col("no_rupture").sum().alias("Indicateur_num"),
        pl.len().alias("Indicateur_den"),
    )

    # NB: in the R source, both branches of the if/(indicateur_name %in% ...)/else
    # set Indicateur_name to the exact same value -- that branch is dead code,
    # so it's applied unconditionally here.
    agg_combined = agg_combined.with_columns(pl.lit("Pct_FOSA_Sans_Rupture").alias("Indicateur_name"))

    # 5. Final column order
    return agg_combined.select(
        [
            "Agg_period",
            "Annee",
            "Filtre_time",
            "Indicateur_name",
            "DE_NAME",
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


def run_logistics_nd_aggregations_lvl5(df_year: pl.DataFrame) -> pl.DataFrame:
    """Perform aggregations for all logistics indicators at LEVEL_5 (Fosa) and for all temporal levels.

    Returns:
        pl.DataFrame with columns: LEVEL_1_NAME, LEVEL_2_NAME, LEVEL_3_NAME, LEVEL_4_NAME, LEVEL_5_NAME, LEVEL_5_UID,
        Indicateur_name, Annee, Indicateur_num, Indicateur_den, Absolute_indicateor_value, Taux_indicateur_value,
        Aggregation_temporelle, Period, DE_NAME
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
        "Aggregation_temporelle",
        "LEVEL_1_NAME",
        "LEVEL_2_NAME",
        "LEVEL_3_NAME",
        "LEVEL_4_NAME",
        "LEVEL_5_NAME",
        "LEVEL_5_UID",
        "Indicateur_name",
        "Period",
        "Annee",
        "Indicateur_num",
        "Indicateur_den",
        "Absolute_indicateur_value",
        "Taux_indicateur_value",
        "DE_NAME",
    ]

    # Add year and period columns
    print("Creating period columns..")
    df_pl = df_year.with_columns(pl.col("Agg_period").str.strptime(pl.Date, "%b %Y").alias("Agg_date"))
    df_pl = df_pl.with_columns(
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
        pl.col("DE_NAME").str.replace(r"\s(?:Number|Zero or Positive Int)$", "", literal=False),
    )

    agg_list_pl = []
    print(f"Processing → Spatial: {spatial_name}")
    for temporal_name, temporal_col in temporal_levels.items():
        print(f"Processing → Temporal: {temporal_name}")
        group_cols = spatial_cols + ["Indicateur_name", "DE_NAME", temporal_col, "Annee"]

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
        for col in ["LEVEL_1_NAME", "LEVEL_2_NAME", "LEVEL_3_NAME", "LEVEL_4_NAME", "LEVEL_5_NAME"]:
            if col not in agg_df.columns:
                agg_df = agg_df.with_columns(pl.lit(None, dtype=pl.Utf8).alias(col))

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


def calculer_indicateur_lvl5(df: pl.DataFrame, indicateur_name: str) -> pl.DataFrame:
    """Compute num/den at fosa level"""
    # 1. Subset to the relevant DEs/COCs
    if indicateur_name == "Stock_Disp":
        df = df.filter(
            (pl.col("COC_NAME") == "Stock Disponible Utilisable-SDU")
            & pl.col("DE_NAME").str.contains("Zero or Positive Int")
        )
 
    elif indicateur_name == "ACT_consommé":
        df = df.filter(
            pl.col("DE_NAME").str.contains("Artemether|sunate")
            & (pl.col("COC_NAME") == "Quantité consommée")
        )
        df = df.with_columns(pl.lit("Groupe ACT").alias("DE_NAME"))
 
    elif indicateur_name == "Mois_stock":
        df = df.filter(
            (pl.col("COC_NAME") == "MSD")
            & pl.col("DE_NAME").str.contains("Number")
        )
 
    elif indicateur_name == "CMM":
        df = df.filter(
            (pl.col("COC_NAME") == "CMM")
            & pl.col("DE_NAME").str.contains("Zero or Positive Int")
        )
 
    else:
        raise ValueError(f"Indicator selection not supported: {indicateur_name}")
 
    # 2. Aggregate at FOSA (LEVEL_5) level
    group_cols = [
        "Agg_period", "DE_NAME",
        "LEVEL_5_NAME", "LEVEL_5_UID",
        "LEVEL_4_NAME", "LEVEL_4_UID",
        "LEVEL_3_NAME", "LEVEL_3_UID",
        "LEVEL_2_NAME", "LEVEL_2_UID",
        "LEVEL_1_NAME", "LEVEL_1_UID",
        "Annee", "Filtre_time",
    ]
 
    agg_combined = df.group_by(group_cols).agg(
        pl.col("value").sum().alias("Indicateur_num"),
    )
 
    agg_combined = agg_combined.with_columns(
        pl.lit(1).alias("Indicateur_den"),
        pl.lit(indicateur_name).alias("Indicateur_name"),
    )
 
    # 3. Final column order
    agg_combined = agg_combined.select([
        "Agg_period", "Annee", "Filtre_time",
        "Indicateur_name", "DE_NAME",
        "Indicateur_num", "Indicateur_den",
        "LEVEL_1_NAME", "LEVEL_1_UID",
        "LEVEL_2_NAME", "LEVEL_2_UID",
        "LEVEL_3_NAME", "LEVEL_3_UID",
        "LEVEL_4_NAME", "LEVEL_4_UID",
        "LEVEL_5_NAME", "LEVEL_5_UID",
    ])
 
    return agg_combined
 