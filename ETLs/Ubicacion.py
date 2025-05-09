import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions, Window

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
                .csv(path + "team.csv")

aux1_df = spark.read.option("delimiter", ",") \
                .option("header", True) \
                .csv(path + "team_details.csv")

aux2_df = spark.read.option("delimiter", ",") \
                .option("header", True) \
                .csv(path + "Datos_equipos.csv")

# Seleccionar los datos necesarios para la tabla y poner el nombre de las columnas especificos
df = df.select("id","city","state") \
       .withColumnRenamed("id", "team_id") \
       .withColumnRenamed("city", "ciudad") \
       .withColumnRenamed("state", "estado")
aux1_df = aux1_df.select("team_id","arena", "arenacapacity") \
       .withColumnRenamed("arenacapacity", "capacidad_numero")
aux2_df = aux2_df.select("Ciudad", "Estado", "Arena", "Capacidad") \
       .withColumnRenamed("Capacidad", "capacidad_numero") \
       .withColumnRenamed("Ciudad", "ciudad") \
       .withColumnRenamed("Arena", "arena") \
       .withColumnRenamed("Estado", "estado")


# Hacemos Join de las diferentes tablas de datos
df = df.join(aux1_df, on="team_id", how="right")

# Eliminamos las columnas innecesarias para permitir la union
df = df.drop("team_id","id")

# Reorganizar columnas para permitir la union
df = df.select("arena", "ciudad", "estado", "capacidad_numero")
aux2_df = aux2_df.select("arena", "ciudad", "estado", "capacidad_numero")

# Hacemos union de la tabla total y la tabla obtenida en la busqueda
df = df.union(aux2_df)

# Categorizamos la capacidad de la arena en diferentes tamaños
df = df.withColumn(
    "capacidad",
    when(col("capacidad_numero") < 10000, "pequeno")
    .when((col("capacidad_numero") >= 10000) & (col("capacidad_numero") < 20000), "mediano")
    .otherwise("grande")
)

# Eliminar duplicados y generar la id de la tabla
df = df.dropDuplicates(["arena", "ciudad", "estado"]) \
    .withColumn("idUbicacion", functions.monotonically_increasing_id())

# Se le da el valor Desconocido a los datos no existentes de los equipos
df = df.fillna('Desconocido')

# Eliminamos las columnas innecesarias para permitir la union
df = df.drop("capacidad_numero")

# Mostramos la tabla final
df.show()

# Cast explícito de todas las columnas según los tipos esperados en Hive
df = df.select(
    col("idUbicacion").cast(IntegerType()).alias("idUbicacion"),
    col("estado").cast(StringType()).alias("estado"),
    col("arena").cast(StringType()).alias("arena"),
    col("ciudad").cast(StringType()).alias("ciudad"),
    col("capacidad").cast(StringType()).alias("capacidad")
)

# Almacenamos el resultado en Hive
df.write.mode("overwrite").insertInto("mydb.ubicacion")