# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "f220f095-d229-4d02-8053-67e2b86d0827",
# META       "default_lakehouse_name": "LK_Silver_Guillermo",
# META       "default_lakehouse_workspace_id": "ed3943de-ba3c-46d9-9f6b-6947697f0090",
# META       "known_lakehouses": [
# META         {
# META           "id": "f220f095-d229-4d02-8053-67e2b86d0827"
# META         },
# META         {
# META           "id": "eb17aee6-64ac-4752-904f-fe766e2795d6"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Preparar df para entrenar

df_para_entrenar = spark.read.table("dbo.ft_vientos")
df_para_entrenar.show(10)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Celda 2 - Agregación diaria de viento promedio por estación
from pyspark.sql import functions as F

df_agregado = (
    df_para_entrenar
    .withColumn("fecha", F.to_date("time"))
    .withColumn("anio",  F.year("time"))
    .withColumn("mes",   F.month("time"))
    .withColumn("dia",   F.dayofmonth("time"))
    .groupBy("CodigoNacional", "anio", "mes", "dia", "fecha")
    .agg(
        F.round(F.avg("ff_valor"), 2).alias("vviento_promedio")
    )
    .orderBy("CodigoNacional", "anio", "mes", "dia")
)

df_agregado.show(10)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#guardar en Gold

df_agregado.write.format("delta").mode("overwrite")\
.saveAsTable("LK_Gold_Guillermo.dbo.ml_train_data_viento_promedio")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
