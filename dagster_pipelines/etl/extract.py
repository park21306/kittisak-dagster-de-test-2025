import pandas as pd

# 2.1.1 Read KPI evaluation data from the "Data to DB" sheet in the "KPI_FY.xlsm" Excel file
def read_excel() -> pd.DataFrame:
    path = "dagster_pipelines/data/KPI_FY.xlsm"
    sheet_name = "Data to DB"
    try:
        df = pd.read_excel(path, sheet_name=sheet_name)
        df = df.rename(columns={'Kpi Number': 'Kpi_Number'})
        return df
    except Exception as e:
        context.log.error(f"Error reading Excel file: {e}")

# 2.1.2 Read center master data from the "M_Center.csv" CSV file
def read_csv() -> pd.DataFrame:
    path = "dagster_pipelines/data/M_Center.csv"
    try:
        df = pd.read_csv(path)
        return df
    except Exception as e:
        context.log.error(f"Error reading CSV file: {e}")
