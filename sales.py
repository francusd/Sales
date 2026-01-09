import pandas as pd 
from pathlib import Path

"""

file=pd.read_parquet(r"C:\DevProjects\DATASETS\VENTAS_POR_MES\VENTAS_ICG_2025-01.parquet")
print(f'{file}')
df_file=file.groupby(["FECHA"]).agg({
       'VENTA': 'sum',
       'COSTO': 'sum',
       'UNIDADES':'sum'
}).reset_index()

print(f'SALES DAILY \n{df_file}')
"""

data_dir = Path('C:\DevProjects\DATASETS\VENTAS_POR_MES')
full_df = pd.concat(
    pd.read_parquet(parquet_file)
    for parquet_file in data_dir.glob('*.parquet')
)
full_df.to_csv('C:\DevProjects\DATASETS\VENTAS_POR_MES\VENTAS_ICG_DAILY.csv')
full_df.to_parquet('C:\DevProjects\DATASETS\VENTAS_POR_MES\VENTAS_ICG_DAILY.parquet')
