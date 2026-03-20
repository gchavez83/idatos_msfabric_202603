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
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Paso 1: Leer desde el origen y camino largo
df_original = spark.sql(
"""
SELECT * from dbo.ft_vientos

"""
)

display (df_original)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
