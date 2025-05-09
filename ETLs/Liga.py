import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions
from pyspark.sql.functions import concat_ws, col, when

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
df = df.select("season_type") \
       .withColumnRenamed("season_type", "nombre") 

# Estandarizar el caso especifico All Star y All-Star
df = df.withColumn(
    "nombre",
    when(col("nombre") == "All Star", "All-Star").otherwise(col("nombre"))
)

# Eliminar duplicados y generar la id de la tabla
df = df.dropDuplicates(["nombre"]) \
    .withColumn("idLiga", functions.monotonically_increasing_id())

# Reorganizar columnas
df = df.select("idLiga", "nombre")

# Mostramos la tabla final
df.show()

# Almacenamos el resultado en Hive
df.write.mode("overwrite").insertInto("mydb.liga")