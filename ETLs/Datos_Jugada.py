import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions, Window, udf

import pandas as pd
import re

from pyspark.sql.functions import concat_ws, col, row_number, when
from pyspark.sql.types import IntegerType, StringType

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
                .csv(path + "play_by_play.csv", columns=["player1_name", "player2_name", "player3_name", "homedescription", "visitordescription"])


# Reducimos las particiones par mejorar el rendimiento
df = df.repartition(8)

# Seleccionar los datos necesarios para la tabla y poner el nombre de las columnas especificos
df1 = df.select("player1_name") \
       .withColumnRenamed("player1_name", "nombre") 

df2 = df.select("player2_name") \
       .withColumnRenamed("player2_name", "nombre") 

df3 = df.select("player3_name") \
       .withColumnRenamed("player3_name", "nombre") 

# Unimos todos los nombres de los jugadores
aux1_df = df1.union(df2).union(df3).dropna()

# Eliminamos nombres repetidos
aux1_df = aux1_df.dropDuplicates(["nombre"])

# Obtenemos todas las descripciones 
df4 = df.select("homedescription") \
        .withColumnRenamed("homedescription", "descripcion") 

df5 = df.select("visitordescription") \
        .withColumnRenamed("visitordescription", "descripcion") 

# Unimos todas las descripciones
aux2_df = df4.union(df5)

# Eliminamos los nombres de los jugadores de las descripciones
# Separamos el nombre en partes y lo guardamos
nombres = aux1_df.rdd.map(lambda row: row["nombre"]).collect()
nombres_fragmentos = set()
for nombre in nombres:
    for parte in nombre.split():
        nombres_fragmentos.add(parte)

# Función para limpiar cada jugada de los nombres
def eliminar_nombres(texto):
    if texto is None:
        return ""
    limpio = texto
    for nombre in nombres_fragmentos:
        limpio = re.sub(rf'\b{nombre}\b', '', limpio)
    limpio = re.sub(r'\s+', ' ', limpio).strip()
    return limpio

eliminar_nombres_udf = functions.udf(eliminar_nombres, StringType())

# Aplicar la funcion al dataset
df = aux2_df.withColumn("descripcion_limpia", eliminar_nombres_udf(functions.col("descripcion")))

# Pasamos el dataset a pandas y lo convertimos a csv
pandas_df = df.toPandas()
pandas_df.to_csv('jugadas_nba.csv', index=False, encoding='utf-8')