import connectorx as cx
import oracledb
import gc
import os

oracledb.init_oracle_client(lib_dir=r"D:\oracle\instantclient")

# Credenciales para la conexión de la base de datos de Oracle
user = os.getenv('ORACLE_USER')
password = os.getenv('ORACLE_PASS')
host = os.getenv('ORACLE_HOST')
port = os.getenv('ORACLE_PORT')
service = os.getenv('ORACLE_SERVICE')

ss_dsn = "mssql://############/############?trusted_connection=true"
oc_dsn = f"oracle://{user}:{password}@{host}:{port}/{service}"

query_contactos = f"select * FROM [GI].[dbo].[TEL_CONTACTOS_BCC]"
query_correos = f"select * FROM [GI].[dbo].[CORREOS_BBC]"


oracle_connection = oracledb.connect(host = host,
                          user = user,
                          password = password,
                          service_name = service,
                          port = port)

oracle_cursor = oracle_connection.cursor()

try:
    oracle_cursor.execute("DROP TABLE DGB_CORREOS_BBC_2")
    oracle_cursor.execute("DROP TABLE DGB_TEL_CONTACTOS_BCC_2")
    
    oracle_connection.commit()

except oracledb.DatabaseError as e:
    print(f"Error al borrar la tabla '{e}'.")

finally:
    oracle_cursor.close()
    oracle_connection.close()


print("Leyendo contactos")
contactos = cx.read_sql(ss_dsn, query_contactos)
print("cargando contactos")
contactos.to_sql(name = "DGB_TEL_CONTACTOS_BCC_2",
                 con = oc_dsn, if_exists = "replace",
                 index = False,
                 chunksize = 50000)

del contactos
gc.collect()

print("Leyendo correos")
correos = cx.read_sql(ss_dsn, query_correos)
print("cargando correos")
correos.to_sql(name = "DGB_CORREOS_BBC_2",
               con = oc_dsn,
               if_exists = "replace",
               index = False,
               chunksize = 50000)

del correos
gc.collect()