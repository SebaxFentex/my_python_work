import oracledb, datetime
import connectorx as cx
import time
import boto3
import gc
import os

# Libreria oracle para conectar en Thick Mode
oracledb.init_oracle_client(lib_dir=r"D:\oracle\instantclient")

# Orden de las columnas
columns = ['FCT_DT', 'LLAVE', 'TECNOLOGIA', 'LINEA', 'CONTRATO', 'CLIENTE_ID', 'DEPARTAMENTO', 'MUNICIPIO', 'ESTRATO', 'FUENTE', 'DIRECCION', 'NODO', 'IDENTIFICADOR', 'NOMBRE_CLIENTE', 'REGIONAL', 'PORTAFOLIO2', 'N_TO', 'N_TV', 'N_BA', 'N_TOTAL', 'PLAN_COMERCIAL', 'ANTIGUEDAD', 'MAC', 'VEL_HOMOLOGADA', 'RGU', 'VALOR_FACTURA', 'PRICING']

# Credenciales para la conexión de la base de datos de Oracle
user = os.getenv('ORACLE_USER')
password = os.getenv('ORACLE_PASS')
host = os.getenv('ORACLE_HOST')
port = os.getenv('ORACLE_PORT')
service = os.getenv('ORACLE_SERVICE')
conn = f"oracle://{user}:{password}@{host}:{port}/{service}"

# Credenciales AWS
ACCESS_KEY_ID = 'AWS_ID'
ACCESS_SECRET_KEY = 'AWS_KEY'
BUCKET_NAME = 'AWS_BUCKET'

def main():
    days_to_upload = get_list_days_to_upload()

    for day in days_to_upload:
        start = time.time()

        # Dia de String a Date para darle los formatos requeridos
        day_date = datetime.datetime.strptime(day, "%Y%m%d")

        sql_day = day_date.strftime("%d/%m/%y") # Formato para el query (dd/mm/yyyy)
        str_day = day_date.strftime("%Y%m%d") # Formato para el nombre del archivo (yyyymmdd)

        print(f"Cargando el dia {sql_day}.")

        data = read_data_oracle(sql_day) # Leer datos de oracle
        upload_data_AWS(data, str_day)   # Cargar datos a AWS

        del data
        gc.collect()

        # Tiempo de carga por día
        end = time.time()
        execution_time = time.strftime("%M:%S", time.gmtime(end - start))
        print(f"  Cargados {len(data)} registros en {execution_time}.")
        
def get_days_in_aws(): ## Obtener la lista de dias que ya están cargados en AWS
    s3_client = boto3.client('s3',
                            aws_access_key_id = ACCESS_KEY_ID, 
                            aws_secret_access_key = ACCESS_SECRET_KEY
                            )

    objects = s3_client.list_objects_v2(Bucket=BUCKET_NAME, Prefix="bi-home/TAB_PANINI_HOME_ACTIVOS/")
    days_in_aws = [file['Key'][56:65] for file in objects['Contents']]
    return days_in_aws
    
def get_days_in_oracle(): ## Obtener la lista de dias disponibles en ORACLE
    query = """
        SELECT *
        FROM(
            SELECT PARTITION_NAME as FECHA_ACTIVOS
            FROM ALL_TAB_PARTITIONS
            WHERE TABLE_NAME = 'TBL_ACTIVO_PANINI' AND MAX_SIZE IS NOT NULL
            ORDER BY PARTITION_NAME DESC
        )
        WHERE ROWNUM < 60"""

    data = cx.read_sql(conn, query)

    days_in_oracle = data["FECHA_ACTIVOS"].to_list()
    days_in_oracle.pop(0)

    return days_in_oracle

def get_list_days_to_upload(): ## Obtener la lista de dias disponibles que no se han cargado
    days_aws = get_days_in_aws()
    days_oracle = get_days_in_oracle()

    days_to_upload = [day for day in days_oracle if day not in days_aws]
    days_to_upload = [day[1:9] for day in days_to_upload]

    return days_to_upload

def read_data_oracle(sql_day):
    print("  Leyendo tabla de Oracle ", end="\r")
    query = f"SELECT * FROM TBL_ACTIVO_PANINI WHERE fct_dt = '{sql_day}'"
    data = cx.read_sql(conn, query)
    data = data[columns]
    print(u"  Leyendo tabla de Oracle \u2714")
    return data

def upload_data_AWS(data, str_day):
    print("  Subiendo archivo a S3 ", end="\r")
    key = f"bi-home/TAB_PANINI_HOME_ACTIVOS/TAB_PANINI_HOME_ACTIVOS_P{str_day}.txt"
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
    print(u"  Subiendo archivo a S3 \u2714")

if __name__ == '__main__':
    main()