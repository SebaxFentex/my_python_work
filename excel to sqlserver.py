import connectorx as cx
import pandas as pd
import os
from sqlalchemy import create_engine



ss_engine = "mssql+pyodbc://<SERVER>/<DB>?trusted_connection=yes&encrypt=no&driver=ODBC+Driver+18+for+SQL+Server"

files = [
    (
        "Excel File.xlsx",
        "Sheet Name",
        "DESTINATION TABLE NAME"
    )
]

def read_table(file):
    filename, sheetname, _ = file
    filepath = fr"{os.path.dirname(__file__)}\{filename}"

    data = pd.read_excel(io = filepath,
                         sheet_name = sheetname)
    
    data = data.astype('string')
    
    return data

def upload_table(data, tablename):

    data.to_sql(name = tablename,
                    con = ss_engine,
                    if_exists = "replace",
                    chunksize = 50000,
                    index = False)
    

def main():
    for file in files:
        try:
            data = read_table(file)
            _, _, tablename = file
            upload_table(data, tablename)
        except Exception as e:
            print(f"Exception while loading {tablename}: {e}")
        finally:
            pass
        
    
if __name__ == '__main__':
    main()
