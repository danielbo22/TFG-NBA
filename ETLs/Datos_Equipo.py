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
aux3_df = spark.read.option("delimiter", ",") \
                .option("header", True) \
                .csv(path + "game.csv")

# Unimos todos los datos de los equipos locales o visitantes de los partidos
aux3_df = aux3_df.selectExpr("stack(2, team_name_home, team_name_away) as nombre")
aux3_df = aux3_df.dropDuplicates(["nombre"])

# Pasamos el dataset a pandas y lo convertimos a csv
pandas_df = aux3_df.toPandas()
pandas_df.to_csv('nombre_equipos_nba.csv', index=False, encoding='utf-8')