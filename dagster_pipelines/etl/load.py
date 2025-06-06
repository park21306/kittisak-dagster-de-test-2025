from dagster import get_dagster_logger
import duckdb
import pandas as pd

logger = get_dagster_logger()

def load_to_duckdb(df: pd.DataFrame, table_name: str) -> None:
    try:
        with duckdb.connect("/opt/dagster/app/dagster_pipelines/db/plan.db") as con:
            logger.info("Connected to DuckDB successfully.")
            
            con.sql("CREATE SCHEMA IF NOT EXISTS plan;")
            con.register('df_view', df)

            con.sql(f"CREATE OR REPLACE TABLE plan.plan.{table_name} AS SELECT * FROM df_view;")
            result = con.sql(f"SELECT * FROM plan.plan.{table_name} LIMIT 1").fetchone()
            logger.info(f"Sample record from {table_name}: {result}")
            logger.info(f"Data successfully inserted into table '{table_name}'.")
    except Exception as e:
        logger.error(f"Error loading data into DuckDB: {e}")
        raise

def load_to_duckdb_with_join(table_name: str) -> None:
    try:
        with duckdb.connect("/opt/dagster/app/dagster_pipelines/db/plan.db") as con:
            logger.info("Connected to DuckDB successfully.")
            con.sql(f"CREATE SCHEMA IF NOT EXISTS plan;")
            con.sql(f"""
                CREATE OR REPLACE TABLE plan.plan.{table_name} AS
                SELECT
                    k.*,
                    m.Center_Name,
                    CURRENT_TIMESTAMP AS updated_at
                FROM  plan.plan.kpi_fy k
                JOIN  plan.plan.m_center m ON k.Center_ID = m.Center_ID;
            """)
            result = con.sql(f"SELECT * FROM plan.plan.{table_name} LIMIT 1").fetchone()
            logger.info(f"Sample record from plan.plan.{result}")
            logger.info(f"Data successfully inserted into table '{table_name}'.")
    except Exception as e:
        logger.error(f"Error loading data into DuckDB: {e}")
        raise