import oracledb
import pandas as pd
import os
from sqlalchemy import create_engine, text

def oracle_connect():
    user = os.getenv('ORACLE_USER')
    password = os.getenv('ORACLE_PASS')
    host = os.getenv('ORACLE_HOST')
    port = os.getenv('ORACLE_PORT')
    service = os.getenv('ORACLE_SERVICE')

    oracleEngine = create_engine(f"oracle+oracledb://{user}:{password}@{host}:{port}/{service}")
    oracledb.init_oracle_client(lib_dir=r"D:\oracle\instantclient")

    return oracleEngine
    
def sqlserver_connect():
    host = os.getenv('SQLSERVER_HOST')
    database = os.getenv('SQLSERVER_DB')

    engine = create_engine(f"mssql+pyodbc://{host}/{database}?trusted_connection=yes&encrypt=no&driver=ODBC+Driver+18+for+SQL+Server")

    return engine

def write_table(engine, data, table_name):
    try:
        data.to_sql(table_name, engine, if_exists="replace", index=False)
    except Exception as error:
        print(f"Error subiendo la tabla {table_name}: {error}")
    finally:
        engine.dispose()

def read_table(engine, table_name):
    try:
        oracle_data = pd.read_sql(f"SELECT * FROM {table_name}", engine)
    except Exception as error:
        print(f"Error leyendo la tabla {table_name}: {error}")
    finally:
        engine.dispose()

    return oracle_data

def main():
    ORACLE_TABLE = "<ORACLE TABLE>"
    SQL_SERVER_TABLE = "<SQL SERVER TABLE>"

    oracle_engine = oracle_connect()
    data = read_table(oracle_engine, ORACLE_TABLE)
    sqlserver_engine = sqlserver_connect()
    write_table(sqlserver_engine, data, SQL_SERVER_TABLE)

if __name__ == '__main__':
    main()
