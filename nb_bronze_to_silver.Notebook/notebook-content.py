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
# META         },
# META         {
# META           "id": "f220f095-d229-4d02-8053-67e2b86d0827"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

df_bronze = spark.sql("SELECT * FROM lk_bronze_guillermo.dbo.humanresources_department LIMIT 1000")
display(df_bronze)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_ft_viento = spark.sql(
"""
SELECT
time, dd_valor, ff_valor, VRB_Valor, CodigoNacional
FROM lk_bronze_guillermo.dbo.viento_dmc
WHERE dd_Valor is not NULL OR ff_Valor is not NULL or VRB_Valor is not NULL
"""

)

df_dim_estaciones = spark.sql(

"""
SELECT 
distinct (CodigoNacional) ,
latitud, 
longitud,
nombreEstacion
FROM lk_bronze_guillermo.dbo.viento_dmc

"""
)




display(df_ft_viento)
display(df_dim_estaciones)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_dim_estaciones.write.format("delta").mode("overwrite").saveAsTable("LK_Silver_Guillermo.dbo.dim_estaciones")
df_ft_viento.write.format("delta").mode("overwrite").saveAsTable("LK_Silver_Guillermo.dbo.ft_vientos")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
