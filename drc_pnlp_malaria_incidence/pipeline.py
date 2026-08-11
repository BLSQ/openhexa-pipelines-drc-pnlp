from pathlib import Path

from openhexa.sdk import current_run, pipeline, workspace
from sqlalchemy import create_engine,  text
import polars as pl

@pipeline("drc-pnlp-malaria-incidence")
def drc_pnlp_malaria_incidence():
    """Orchestrates the population extraction from stored files and joins it with malaria cases
    data, aggregating them per year and health zone level."""
    data_path = Path(workspace.files_path) / "pnlp-tdb-pipeline" / "data"
    population_path = data_path / "snis_population"
    pyramid_path = data_path / "snis_pyramid"

    engine = create_engine(workspace.database_url)

    # --- READ ---#
    try:
        population_df, pyramid_df, cases_df = read_step(
            population_path=population_path,
            pyramid_path=pyramid_path,
            engine=engine,
        )
        current_run.log_info("Data extracted successfully")
    except Exception as e:
        current_run.log_error(f"An error occurred during extraction: {e}")
        raise

    # --- TRANSFORM ---#
    try:
        result = transform_step(
            population=population_df,
            pyramid=pyramid_df,
            cases=cases_df,
        )
        current_run.log_info("Data transformed successfully")
    except Exception as e:
        current_run.log_error(f"An error occurred during transformation: {e}")
        raise

    # --- LOAD ---#
    try:
        load_step(
            engine=engine,
            data=result,
            table_name="pnlp_malaria_incidence",
        )
        current_run.log_info("Data loaded successfully")
    except Exception as e:
        current_run.log_error(f"An error occurred during load: {e}")
        raise

# =============================================================================
# READ FUNCTIONS
# =============================================================================

def read_step(
    population_path: Path,
    pyramid_path: Path,
    engine,
) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    """Reads the population files, the pyramid metadata and the cases table.

    Args:
        population_path (Path): Root path for the population.
        pyramid_path (Path): Root path for the pyramid metadata.
        engine: SQLAlchemy engine connected to WORKSPACE_DATABASE_URL.

    Returns:
        tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
            (population, pyramid, cases) tables.
    """
    population_files = sorted(population_path.glob("snis_population_*.parquet"))

    if not population_files:
        current_run.log_info(
            f"No population files found in {population_path}. Nothing to transform."
        )
        return pl.DataFrame(), pl.DataFrame(), pl.DataFrame()

    population_df = pl.read_parquet(
    population_files,
    columns=["org_unit", "period", "value"],
    )

    pyramid_df = pl.read_parquet(pyramid_path / "snis_pyramid.parquet")

    cases_df = read_cases(engine)

    return population_df, pyramid_df, cases_df


def read_cases(engine) -> pl.DataFrame:
    """Reads malaria cases aggregated per year, province and health zone from
    the DSE_palu_for_PNLP_tot warehouse table.
 
    Args:
        engine: SQLAlchemy engine connected to WORKSPACE_DATABASE_URL.
 
    Returns:
        pl.DataFrame: Columns year, PROVINCE, Nom, zone_id_DHIS2, Cases.
    """

    query = text(
        'SELECT "year", "PROVINCE", "Nom", "zone_id_DHIS2", sum("Cas") AS "cases" '
        'FROM public."DSE_palu_for_PNLP_tot" '
        "GROUP BY 1, 2, 3, 4"
    )
 
    with engine.connect() as conn:
        cases_df = pl.read_database(query=query, connection=conn)
 
    current_run.log_info(
        f"Read {cases_df.height} rows of cases from DSE_palu_for_PNLP_tot."
    )
    return cases_df

# =============================================================================
# TRANSFORM FUNCTIONS
# =============================================================================


def transform_step(
    population: pl.DataFrame,
    pyramid: pl.DataFrame,
    cases: pl.DataFrame,
) -> pl.DataFrame:
    """Builds the health-zone lookup, aggregates population per year and health
    zone, joins the malaria cases, and computes incidence per health zone.

    Args:
        population (pl.DataFrame): Concatenated population data (health-area level).
        pyramid (pl.DataFrame): Full org-unit pyramid.
        cases (pl.DataFrame): Cases pulled from DSE_palu_for_PNLP_tot.

    Returns:
        pl.DataFrame: Population, cases and incidence per year and health zone.
    """
    zone_lookup = build_zone_lookup(pyramid)

    pop_by_zone = aggregate_population(population, zone_lookup)

    pop_cases_by_zone = join_cases(pop_by_zone, cases)

    incidence_by_zone = compute_incidence(pop_cases_by_zone)

    return incidence_by_zone


def build_zone_lookup(pyramid: pl.DataFrame) -> pl.DataFrame:
    """Maps each health area (level 4) to its parent health zone (level 3).

    Args:
        pyramid (pl.DataFrame): Full org-unit pyramid (long format).

    Returns:
        pl.DataFrame: One row per health area with columns
            health_area_id, health_area, health_zone_id, health_zone.
    """
    health_zone_id = pl.col("parent").struct.field("id")

    areas = pyramid.filter(pl.col("level") == 4).select(
        pl.col("id").alias("health_area_id"),
        health_zone_id.alias("health_zone_id"),
    )

    zones = pyramid.filter(pl.col("level") == 3).select(
        pl.col("id").alias("health_zone_id"),
        pl.col("name").alias("health_zone"),
    )

    return areas.join(zones, on="health_zone_id", how="left")


def aggregate_population(
    population: pl.DataFrame,
    zone_lookup: pl.DataFrame,
) -> pl.DataFrame:
    """Joins population (health-area level) to the zone lookup and sums
    population per year and health zone.

    Args:
        population (pl.DataFrame): Population data at health-area level.
        zone_lookup (pl.DataFrame): area -> zone mapping from build_zone_lookup.

    Returns:
        pl.DataFrame: Columns year, health_zone, health_zone_id, population.
    """
    return (
        population.join(
            zone_lookup,
            left_on="org_unit",
            right_on="health_area_id",
            how="left",
        )
        .with_columns(
            pl.col("period").cast(pl.Int64).alias("year"),
            pl.col("value").cast(pl.Int64).alias("population"),  # value is str on disk
        )
        .group_by("year", "health_zone_id", "health_zone")
        .agg(pl.col("population").sum())
        .select("year", "health_zone", "health_zone_id", "population")
    )


def join_cases(
    pop_by_zone: pl.DataFrame,
    cases: pl.DataFrame,
) -> pl.DataFrame:
    """Adds the malaria "cases" and "province" columns onto the
    population-per-zone table.

    Args:
        pop_by_zone (pl.DataFrame): Population per year and health zone.
        cases (pl.DataFrame): Cases from read_cases (year, PROVINCE, Nom,
            zone_id_DHIS2, Cases).

    Returns:
        pl.DataFrame: pop_by_zone with "Cases" and "province" columns joined on
            year x health zone.
    """

    return pop_by_zone.join(
        cases.select(
            pl.col("year").cast(pl.Int64), 
            "zone_id_DHIS2",
            pl.col("PROVINCE").alias("province"),
            "cases",
        ),
        left_on=["year", "health_zone_id"],
        right_on=["year", "zone_id_DHIS2"],
        how="left",
    )


def compute_incidence(pop_cases_by_zone: pl.DataFrame) -> pl.DataFrame:
    """Adds the incidence per 1,000 inhabitants per health zone.

    Args:
        pop_cases_by_zone (pl.DataFrame): Population and cases per year and
            health zone.

    Returns:
        pl.DataFrame: Same table with an added "incidence_zone" column.
    """
    return pop_cases_by_zone.with_columns(
        pl.when(pl.col("population") > 0)
        .then(pl.col("cases") / pl.col("population") * 1000)
        .otherwise(None)
        .alias("incidence_zone")
    )

# =============================================================================
# LOAD FUNCTIONS
# =============================================================================

def load_step(engine, data: pl.DataFrame, table_name: str) -> None:
    """Atomically replaces the target table with the result set.

    Truncates and appends in a single transaction so the table is never left
    empty if the write fails. Skips the whole operation when there is nothing
    to load, to avoid wiping the table on an empty result.

    Args:
        engine: SQLAlchemy engine connected to WORKSPACE_DATABASE_URL.
        data (pl.DataFrame): Final table to persist.
        table_name (str): Target table name in the public schema.
    """
    if data.is_empty():
        current_run.log_warning(
            f"Result is empty — skipping load, public.{table_name} left unchanged."
        )
        return

    with engine.begin() as conn:
        conn.execute(text(f'TRUNCATE TABLE public."{table_name}";'))
        data.write_database(
            table_name=f"public.{table_name}",
            connection=conn,
            if_table_exists="append",
        )

    current_run.log_info(
        f"Replaced public.{table_name} with {data.height} rows."
    )

if __name__ == "__main__":
    drc_pnlp_malaria_incidence()