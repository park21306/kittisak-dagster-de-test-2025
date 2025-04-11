import pandas as pd

# 2.2.1 Pivot data in the "KPI_FY.xlsm" file
def pivot_data(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        raise ValueError("Input DataFrame is None or empty.")
        
    amount_name = []
    defalut_columns = []
    add_data = []
    for i in df.columns:
        if i.startswith("Plan") or i.startswith("Actual"):
            amount_name.append(i)
        else:
            defalut_columns.append(i)

    df_pivot = df.drop(columns=defalut_columns)
    df_copy = df.drop(columns=amount_name)
    
    for amount in amount_name:
        data  = df_copy.copy()
        data['Amount_Name'] = amount
        data['Amount'] = df_pivot[amount]
        data['Amount_Type'] = amount.split('_', 1)[0]
        add_data.append(data)
    pivot_df = pd.concat(add_data, ignore_index=True)
    return pivot_df