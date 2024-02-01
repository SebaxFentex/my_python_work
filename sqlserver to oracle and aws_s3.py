import connectorx as cx
import oracledb
from datetime import datetime

# Notación:
# ss -> Sql Server
# oc -> Oracle
# dmy -> Fechas en formato dd/mm/yyyy
# ymd -> Fechas en formato yyyy-mm-dd

# Libreria oracle para conectar en Thick Mode
oracledb.init_oracle_client(lib_dir=r"D:\oracle\instantclient")

# Columnas en orden que se quieren subir
columns = ['FCT_DT', 'APERTURA', 'LLAVE', 'CLIENTE_ID', 'MOTIVO_FUENTE', 'MOTIVO_RETIRO_V1', 'MOTIVO_RETIRO_V2', 'CANAL', 'CANAL_DETALLADO', 'FECHA']

# Credenciales AWS
ACCESS_KEY_ID = 'AWS_ID'
ACCESS_SECRET_KEY = 'AWS_KEY'
BUCKET_NAME = 'AWS_BUCKET'

ss_conn = "mssql://<SERVER>/<DATABASE>?trusted_connection=true"
oc_conn = "oracle://#######################################"

def main():
    days_ss = get_ss_days()
    days_ss_dmy = [elem.strftime('%d/%m/%y') for elem in days_ss]
    days_ss_ymd = [elem.strftime('%Y-%m-%d') for elem in days_ss]

    days_oc = get_days_oc(days_ss_dmy)
    days_oc_ymd = [elem.strftime('%Y-%m-%d') for elem in days_oc]

    diff = list(set(days_ss_ymd) - set(days_oc_ymd))
    diff.sort()
    diff.pop()

    for date in diff:
        print(f"Procesando dia {date}")
        data = get_data_from_ss(date)
        upload_to_oracle(data)
        upload_to_s3(data, date)


def get_ss_days():
    query_days_ss = "SELECT DISTINCT TOP 100 APERTURA FROM TBL_EFECTIVIDAD_RETENCION ORDER BY APERTURA DESC"
    table_ss = cx.read_sql(conn=ss_conn, query=query_days_ss)
    days_ss = table_ss["APERTURA"].to_list()
    return days_ss

def get_days_oc(days_ss_dmy):
    query_days_oc = F"SELECT DISTINCT FCT_DT FROM TBL_EFECTIVIDAD_PANINI WHERE FCT_DT IN {tuple(days_ss_dmy)}"
    table_oc = cx.read_sql(conn=oc_conn, query=query_days_oc)
    days_oc = table_oc["FCT_DT"].to_list()
    return days_oc

def get_data_from_ss(date):
    print("    Leyendo tabla de SqlServer", end="\r")
    query = f"""SELECT '{date}' AS FCT_DT, APERTURA, _Llave AS LLAVE, CLIENTE_ID, MOTIVO_FUENTE, MOTIVO_RETIRO_V1, MOTIVO_RETIRO_V2, CANAL, CANAL_DETALLADO, null as FECHA
                FROM [BODEGA_HOGARES].[dbo].[TBL_EFECTIVIDAD_RETENCION]
                WHERE APERTURA > DATEADD(DAY, -63, '{date}') AND APERTURA <= '{date}'
            """
    data = cx.read_sql(conn=ss_conn, query=query)
    data = data[columns]
    data["FCT_DT"] = data["FCT_DT"].map(lambda value: datetime.strptime(value, "%Y-%m-%d"))
    print(u"    Leyendo tabla de SqlServer \u2714")
    return data

def upload_to_oracle(data):
    print("    Subiendo tabla a oracle", end="\r")
    data.to_sql(name = "tbl_efectividad_panini",
                con = oc_conn,
                if_exists = "append",
                index = False,
                chunksize = 50000)
    print(u"    Subiendo tabla a oracle \u2714")

def upload_to_s3(data, date):
    print("    Subiendo archivo a S3 ", end="\r")
    date_txt = date.replace("-", "")
    key = f"bi-home/TAB_PANINI_HOME_EFECTIVIDAD/TAB_PANINI_HOME_EFECTIVIDAD_P{date_txt}.txt"
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
    
def upload_custom_date(date):
    pass

if __name__ == '__main__':
    main()