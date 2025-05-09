import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions, Window, when

from pyspark.sql.functions import concat_ws, col, row_number

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
                .csv(path + "team.csv")

aux1_df = spark.read.option("delimiter", ",") \
                .option("header", True) \
                .csv(path + "team_details.csv")


# Seleccionar los datos necesarios para la tabla y poner el nombre de las columnas especificos
df = df.select("id","city","state") \
       .withColumnRenamed("city", "ciudad") \
       .withColumnRenamed("state", "estado")
aux1_df = aux1_df.select("team_id","arena", "arenacapacity") \
       .withColumnRenamed("arenacapacity", "capacidad")

# Hacemos Join de las diferentes tablas de datos
df = df.join(aux1_df, on="team_id", how="left")

# Categorizamos la capacidad de la arena en diferentes tamaños

# Eliminar duplicados y generar la id de la tabla
df = df.dropDuplicates(["id"]) \
    .withColumn("idUbicacion", functions.monotonically_increasing_id())

# Eliminar columnas innecesarias
df = df.drop("team_id","id")

# Se le da el valor Desconocido a los datos no existentes de los equipos
df = df.fillna('Desconocido')

# Reorganizar columnas
df = df.select("idUbicacion", "nombre")

# Mostramos la tabla final
df.show()

# Almacenamos el resultado en Hive
df.write.mode("overwrite").insertInto("mydb.ubicacion")