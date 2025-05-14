import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions, Window

import pandas as pd

from pyspark.sql.functions import concat_ws, col, row_number, when

#mysql-connector-j-9.3.0.jar

# Crear la sesión Spark
spark = SparkSession.builder \
                    .config("spark.sql.warehouse.dir", "hdfs://localhost:9000/user/hive/warehouse") \
                    .appName("TFG NBA") \
                    .enableHiveSupport() \
                    .getOrCreate()
 
               

path = "file:///home/tfg/Escritorio/TFG-NBA/ETLs/Datos/"

# Sacar los datos del csv
df = spark.read.option("delimiter", ",") \
                .option("header", True) \
                .csv(path + "draft_combine_stats.csv")

aux1_df = spark.read.option("delimiter", ",") \
                .option("header", True) \
                .csv(path + "common_player_info.csv")

# Sacamos los datos necesarios
df = df.select("position")
df = df.dropDuplicates(["position"])

aux1_df = aux1_df.select("position")
aux1_df = aux1_df.dropDuplicates(["position"])

# Unimos los datos en un solo dataset
df = df.union(aux1_df)

# Pasamos el dataset a pandas y lo convertimos a csv
pandas_df = df.toPandas()
pandas_df.to_csv('posiciones_nba.csv', index=False, encoding='utf-8')