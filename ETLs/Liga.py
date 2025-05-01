import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions
from pyspark.sql.functions import concat_ws, col

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
                .csv(path + "game.csv")

# Seleccionar los datos necesarios para la tabla y poner el nombre de las columnas especificos
df = df.select("season_type")


# Eliminar duplicados y generar la id de la tabla
df = df.dropDuplicates(["season_type"]) \
    .withColumn("idLiga", functions.monotonically_increasing_id())

# Reorganizar columnas
df = df.select("idLiga", "nombre")

# Mostramos la tabla final
df.show()

# Almacenamos el resultado en Hive
df.write.mode("overwrite").insertInto("mydb.liga")