import pandas as pd
import oracledb
from sqlalchemy import create_engine

oracledb.init_oracle_client(lib_dir=r"D:\oracle\instantclient")

cols = ['CUENTA', 'NOMBRE', 'DOCUMENTO', 'MOVIL', 'No FACTVENC', 'DIAS DE MORA']

engineOracle = create_engine("oracle://#######################################")

tabla = pd.read_csv(filepath_or_buffer = r"D:\REP_20231001.csv",
                             sep=";",
                             dtype=str,
                             encoding = "ISO-8859-1",
                             usecols=cols)

tabla.insert(0, 'PERIODO', '202310')

print(tabla)

tabla.to_sql(name='DGB_MORA_CARTERA',
             con=engineOracle,
             if_exists="append",
             chunksize=50000,
             index=False)
