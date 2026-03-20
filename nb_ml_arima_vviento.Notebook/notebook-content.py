# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "eb17aee6-64ac-4752-904f-fe766e2795d6",
# META       "default_lakehouse_name": "LK_Gold_Guillermo",
# META       "default_lakehouse_workspace_id": "ed3943de-ba3c-46d9-9f6b-6947697f0090",
# META       "known_lakehouses": [
# META         {
# META           "id": "eb17aee6-64ac-4752-904f-fe766e2795d6"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

#importar librerias
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

# Librerías para análisis temporal
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime, timedelta

# Librerías para ARIMA
from statsmodels.tsa.arima.model import ARIMA
from statsmodels.tsa.stattools import adfuller, acf, pacf
from statsmodels.graphics.tsaplots import plot_acf, plot_pacf
from statsmodels.tsa.seasonal import seasonal_decompose

# Métricas
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score

import warnings
warnings.filterwarnings('ignore')

print("✓ Librerías importadas correctamente")
 



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_base= spark.read.table("dbo.ml_train_data_viento_promedio")

#muestra informacion basica de lo importado
print(f"Total de registros: {df_base.count():,} ")
print(f"Total de colummnas: {df_base.columns} ")
df_base.printSchema()

#muestra vista previa de los datos

df_base.show(5)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Estadísticas descriptivas
print("=" * 80)
print("ESTADÍSTICAS DESCRIPTIVAS")
print("=" * 80)
display(df_base.select("vviento_promedio").describe())

# Verificar valores nulos
print("\n" + "=" * 80)
print("VALORES NULOS")
print("=" * 80)
null_counts = df_base.select(
    [count(when(col(c).isNull(), c)).alias(c) for c in df_base.columns]
)
display(null_counts)

# Distribución por estación
print("\n" + "=" * 80)
print("DISTRIBUCIÓN POR ESTACIÓN")
print("=" * 80)
display(df_base.groupBy("CodigoNacional").count().orderBy(desc("count")))
 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Transformamos de Spark a Pandas y aseguramos tipos correctos
df_pandas = df_base.toPandas()

# Forzar tipo datetime desde el origen
df_pandas['fecha'] = pd.to_datetime(df_pandas['fecha'])
df_pandas['CodigoNacional'] = df_pandas['CodigoNacional'].astype(int)

print(f"Tipo de columna fecha : {df_pandas['fecha'].dtype}")
print(f"Rango de fechas       : {df_pandas['fecha'].min()} → {df_pandas['fecha'].max()}")
print(f"\nPrimeros registros:")
display(df_pandas.head(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Filtrar por una estación
df_estacion = df_pandas[df_pandas['CodigoNacional'] == 400009].copy()
df_estacion = df_estacion.set_index('fecha')
df_estacion = df_estacion.sort_index()
df_estacion = df_estacion[['vviento_promedio']]
df_estacion = df_estacion[~df_estacion.index.duplicated(keep='first')]

print(f"Rango  : {df_estacion.index.min()} → {df_estacion.index.max()}")
print(f"Filas  : {df_estacion.shape[0]}")
print(f"Nulos  : {df_estacion['vviento_promedio'].isna().sum()}")
print(df_estacion.head(10).to_string())  # ← reemplaza display()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
