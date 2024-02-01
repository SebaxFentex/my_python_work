import connectorx as cx
import time
import querys

# PERIODO A CARGAR EN AWS
periodo = '202312'

# CREDENCIALES SQL SERVER
HOST = '###############'
DATABASE = '###########'
connection_string = f"mssql://{HOST}/{DATABASE}?trusted_connection=true"

# CREDENCIALES AWS
ACCESS_KEY_ID = '################'
ACCESS_SECRET_KEY = '######################################'
BUCKET_NAME = '#################'

def main():
    tables = [
        (
            "pricing_vmax",
            querys.query_pricing_vmax(periodo),
            f"###########{periodo}.txt"
        )
        ,
        (
            "premium",
            querys.query_premium(periodo),
            f"###########{periodo}.txt"
        )
        ,
        (
            "convergencia",
            querys.query_convergencia(periodo),
            f"###########{periodo}.txt"
        )
    ]

    for table in tables:
        process_table(table)


def process_table(table):
    table_name, query, path = table
    print(f"Procesando {table_name}")
    try:
        data = readTable(query)
        uploadToS3(data, path)
        print(f"Terminado {table_name}")
    except Exception as e:
        print(f"Error en {table_name}.")
    

def readTable(query):
    print("    Leyendo tabla", end="\r")
    data = cx.read_sql(connection_string, query)
    print(u"    Leyendo tabla \u2714")
    return data

def uploadToS3(data, path):
    print("    Subiendo archivo a S3 ", end="\r")
    key = path

    data.to_csv(
        f"s3://{BUCKET_NAME}/{key}",
        index=False,
        encoding='utf-8',
        sep=';',
        storage_options={
            "key": ACCESS_KEY_ID,
            "secret": ACCESS_SECRET_KEY
        }
    )
    print(u"    Subiendo archivo a S3 \u2714")

if __name__ == '__main__':
    start = time.time()
    main()
    end = time.time()
    execution_time = time.strftime("%M:%S", time.gmtime(end - start))
    print(f"Tiempo de ejecución: {execution_time}.")