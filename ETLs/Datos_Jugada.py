import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions, Window, udf

import pandas as pd
import re

from pyspark.sql.functions import concat_ws, col, row_number, when
from pyspark.sql.types import IntegerType, StringType, BooleanType

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
                .csv(path + "play_by_play.csv") \
                .select("player1_name", "player2_name", "player3_name", "player1_team_nickname", "player2_team_nickname", "player3_team_nickname", "homedescription", "visitordescription")


# Reducimos las particiones par mejorar el rendimiento
# df = df.repartition(8)

# Seleccionar los datos necesarios para la tabla y poner el nombre de las columnas especificos
df1 = df.select("player1_name") \
       .withColumnRenamed("player1_name", "nombre") \
       .dropDuplicates(["nombre"])

df2 = df.select("player2_name") \
       .withColumnRenamed("player2_name", "nombre") \
       .dropDuplicates(["nombre"])

df3 = df.select("player3_name") \
       .withColumnRenamed("player3_name", "nombre") \
       .dropDuplicates(["nombre"])

# Seleccionamos los nombres de equipos posibles
df4 = df.select("player1_team_nickname") \
       .withColumnRenamed("player1_team_nickname", "equipo") \
       .dropDuplicates(["equipo"])

df5 = df.select("player2_team_nickname") \
       .withColumnRenamed("player2_team_nickname", "equipo") \
       .dropDuplicates(["equipo"])

df6 = df.select("player3_team_nickname") \
       .withColumnRenamed("player3_team_nickname", "equipo") \
       .dropDuplicates(["equipo"])

# Unimos todos los nombres de los jugadores y equipos
aux1_df = df1.union(df2).union(df3).dropna()
aux2_df = df4.union(df5).union(df6).dropna()

# Eliminamos nombres y equipos repetidos
aux1_df = aux1_df.dropDuplicates(["nombre"])
aux2_df = aux2_df.dropDuplicates(["equipo"])


# Obtenemos todas las descripciones 
df7 = df.select("homedescription", "player1_name") \
        .withColumnRenamed("homedescription", "descripcion") 

df8 = df.select("visitordescription", "player1_name") \
        .withColumnRenamed("visitordescription", "descripcion") 

# Unimos todas las descripciones
aux3_df = df7.union(df8)

# Eliminamos las decripciones que no tengan un jugador asignado
aux3_df = aux3_df.filter(col("player1_name").isNotNull())

# Nos quedamos solo con las descripciones
aux3_df = aux3_df.select("descripcion")

# Eliminamos los nombres de los jugadores de las descripciones
# Separamos el nombre en partes y lo guardamos
# nombres = aux1_df.rdd.map(lambda row: row["nombre"]).collect()
nombres = aux1_df.rdd.map(lambda row: row["nombre"]).filter(lambda x: x is not None).collect()
equipos = aux2_df.select("equipo") .rdd .map(lambda row: row["equipo"]) .filter(lambda x: x is not None) .collect()

# Pasamos los nombres de equipos a mayusculas
equipos_mayus = [equipo.upper() for equipo in equipos]

# Generamos el regex a aplicar en las descripciones
fragmentos = {re.escape(parte) for nombre in nombres for parte in nombre.split()}
nombres_regex = r'\b(?:' + '|'.join(fragmentos) + r')\b|\([^)]*\)'
parentesis_regex = r'\([^)]*\)'
equipos_mayus_regex = r'\b(?:' + '|'.join(map(re.escape, equipos_mayus)) + r')\b'
equipos_regex = r'\b(?:' + '|'.join(map(re.escape, equipos)) + r')\b'
extras_regex = r"\b\d{1,3}'|\b[A-Z]\."
abreviaturas_regex = r"\b[A-Za-z]{2,5}\."
multiples_regex = r"\b\d+ of \d+\b"
regex_total = f"{nombres_regex}|{parentesis_regex}|{equipos_regex}|{equipos_mayus_regex}|{extras_regex}|{abreviaturas_regex}|{multiples_regex}"


# Compilamos una sola vez el patrón
patron = re.compile(regex_total)

# nombres_fragmentos = set()
# for nombre in nombres:
#     for parte in nombre.split():
#         nombres_fragmentos.add(parte)

# Función para limpiar cada jugada de los nombres
def eliminar_nombres(texto):
    if texto is None:
        return ""
    limpio = patron.sub( '', texto)
    limpio = re.sub(r'[",]+', '', limpio)
    limpio = re.sub(r'\s*[-–—]+\s*', ' ', limpio) 
    limpio = re.sub(r'^\W+|\W+$', '', limpio) 
    limpio = re.sub(r"\b\d+' ?", "", limpio)
    limpio = re.sub(r"\s*[\.,:;]+\s*", " ", limpio)
    limpio = re.sub(r'\s+', ' ', limpio).strip()
    return limpio

eliminar_nombres_udf = functions.udf(eliminar_nombres, StringType())

# Aplicar la funcion al dataset
aux3_df = aux3_df.withColumn("descripcion_limpia", eliminar_nombres_udf(functions.col("descripcion")))

# Eliminamos descripciones duplicadas
aux3_df = aux3_df.dropDuplicates(["descripcion_limpia"])

# Crear conjunto de fragmentos de nombres para comparación
nombres_fragmentos = set()
for nombre in nombres:
    if nombre:
        for parte in nombre.split():
            nombres_fragmentos.add(parte.lower())

# UDF para verificar si al menos un fragmento está en la descripción original
def contiene_nombre(desc):
    if desc is None:
        return False
    palabras = set(re.findall(r'\w+', desc.lower()))
    return any(palabra in palabras for palabra in nombres_fragmentos)

contiene_nombre_udf = functions.udf(contiene_nombre, BooleanType())

# Filtramos las descripciones que no contengan al menos un nombre de jugador
aux3_df = aux3_df.filter(contiene_nombre_udf(col("descripcion")))

# Eliminamos descripciones duplicadas
df = aux3_df.dropDuplicates(["descripcion_limpia"])

# Generamos una id
df = df.withColumn("idJugada", functions.monotonically_increasing_id())

# Seleccionamos los datos que buscamos
df = df.select("idJugada", "descripcion_limpia", "descripcion") 


# Pasamos el dataset a pandas y lo convertimos a csv
pandas_df = df.toPandas()
pandas_df.to_csv('jugadas_nba.csv', index=False, encoding='utf-8')