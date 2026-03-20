# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "145bdfbe-52c9-4c4c-8220-b834c7938e3c",
# META       "default_lakehouse_name": "lk_bronze_guillermo",
# META       "default_lakehouse_workspace_id": "ed3943de-ba3c-46d9-9f6b-6947697f0090",
# META       "known_lakehouses": [
# META         {
# META           "id": "145bdfbe-52c9-4c4c-8220-b834c7938e3c"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

#importar librerias
from pyspark.sql.types import StructType, StringType, DoubleType, TimestampType
import pyspark.sql.functions as F

#definir rutas de archivo

ruta_origen = "abfss://WS2603_Guillermo@onelake.dfs.fabric.microsoft.com/lk_bronze_guillermo.Lakehouse/Files/sensor_simulado"
ruta_chequeo = "abfss://WS2603_Guillermo@onelake.dfs.fabric.microsoft.com/lk_bronze_guillermo.Lakehouse/Files/sensor_simulado"
nombre_tabla = "dbo.sensores_simulados"

esquema_archivo = StructType()\
    .add("id" ,        StringType())\
    .add("temp" ,      DoubleType())\
    .add("humedad" ,   DoubleType() )\
    .add("viento" ,    DoubleType())\
    .add("timestamp" , TimestampType())

spark.sql(
f"""
CREATE TABLE IF NOT EXISTS {nombre_tabla}(
    id  STRING ,
    temperatura DOUBLE,
    humedad DOUBLE,
    viento DOUBLE,
    ts_registro TIMESTAMP, 
    ts_process TIMESTAMP
) USING DELTA

"""
)
 



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# DELTA STREAMING o Microbatching

archivo_raiz = spark.readStream\
    .schema(esquema_archivo)\
    .option("maxFilesPerTrigger" , 1)\
    .json(ruta_origen)

#agrego al dataframe el campo "ts_process"
archivo_df = archivo_raiz.withColumn("ts_process", F.current_timestamp() )

deltastream = archivo_df\
    .writeStream\
    .format("delta")\
    .outputMode("append")\
    .option("mergeSchema", True)\
    .option("checkpointLocation", ruta_chequeo)\
    .toTable(nombre_tabla)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
