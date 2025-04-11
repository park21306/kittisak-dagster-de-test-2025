import dagster as dg
from dagster_pipelines.etl.extract import read_excel, read_csv
from dagster_pipelines.etl.transform import pivot_data
from dagster_pipelines.etl.load import load_to_duckdb,load_to_duckdb_with_join

# 2.3.1.1 Load pivoted KPI_FY.xlsm into KPI_FY
@dg.asset(compute_kind="duckdb", group_name="plan")
def kpi_fy(context: dg.AssetExecutionContext) :
    df = read_excel()
    context.log.info("✅ Successfully read the Excel file.")
    df2 = pivot_data(df)
    context.log.info(f"header of the DataFrame:{df2.columns}")
    context.log.info("✅ Successfully pivoted the DataFrame.")
    load_to_duckdb(df2, "KPI_FY")
    context.log.info("✅ Successfully loaded the DataFrame into DuckDB.")
    return df2

# 2.3.1.2 Load M_Center.csv into M_Center
@dg.asset(compute_kind="duckdb", group_name="plan")
def m_center(context: dg.AssetExecutionContext) :
    df = read_csv()
    context.log.info("✅ Successfully read the CSV file.")
    load_to_duckdb(df, "M_Center")
    context.log.info("✅ Successfully loaded the DataFrame into DuckDB.")
    return df
    

# 2.3.2 Create asset kpi_fy_final_asset()

@dg.asset(deps=[kpi_fy,m_center], compute_kind="duckdb", group_name="plan")
def kpi_fy_final_asset(context: dg.AssetExecutionContext) -> None:
    load_to_duckdb_with_join("KPI_FY_Final")
    context.log.info("✅ Successfully loaded the DataFrame into DuckDB.")


defs = dg.Definitions(
    assets=[kpi_fy, m_center, kpi_fy_final_asset]
)

