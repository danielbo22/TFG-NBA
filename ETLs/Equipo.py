import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions, Window

from pyspark.sql.functions import concat_ws, col, row_number, when, split, expr, trim
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
                .csv(path + "team_history.csv")

aux3_df = spark.read.option("delimiter", ",") \
                .option("header", True) \
                .csv(path + "Datos_equipos.csv")

# Seleccionar los datos necesarios para la tabla y poner el nombre de las columnas especificos
df = df.select("id","nickname","city","state") \
       .withColumnRenamed("id", "team_id") \
       .withColumnRenamed("nickname", "sobrenombre") \
       .withColumnRenamed("city", "ciudad") \
       .withColumnRenamed("state", "estado")
aux1_df = aux1_df.select("team_id","arena")
aux2_df = aux2_df.select("team_id","year_active_till")
aux3_df = aux3_df.select("Equipo", "Ciudad", "Estado", "Arena", "Activo") \
       .withColumnRenamed("Equipo", "nombre") \
       .withColumnRenamed("Ciudad", "ciudad") \
       .withColumnRenamed("Arena", "arena") \
       .withColumnRenamed("Activo", "estado_actual") \
       .withColumnRenamed("Estado", "estado") \

# Seleccionamos solo la tabla del historial con el año activo mas tardio
window_spec = Window.partitionBy("team_id").orderBy(col("year_active_till").desc())
team_year = aux2_df.withColumn("row_num", row_number().over(window_spec))
latest_team_year = team_year.filter(col("row_num") == 1).drop("row_num")

# Hacemos Join de las diferentes tablas de datos
df = df.join(aux1_df, on="team_id", how="left")
df = df.join(aux2_df, on="team_id", how="left")

# Generamos el nombre en las tablas donde esta por separado
df = df.withColumn("nombre", concat_ws(" ", col("ciudad"), col("sobrenombre")))

#Se genera el valor del estado a partir de su ultimo año activo
df = df.withColumn("year_active_till", col("year_active_till").cast("int"))
df = df.withColumn(
    "estado_actual",
    when(col("year_active_till") == 2019, 1).otherwise(0)
)

# Eliminar columnas innecesarias para hacer la union
df = df.drop("team_id","year_active_till", "sobrenombre")

# Se organizan las tablas para permitir la union
df = df.select("nombre", "ciudad", "estado", "arena", "estado_actual")
aux3_df = aux3_df.select("nombre", "ciudad", "estado", "arena", "estado_actual")

# Hacemos union de la tabla total y la tabla obtenida en la busqueda
df = df.union(aux3_df)

# Eliminar duplicados y generar la id de la tabla
df = df.dropDuplicates(["nombre", "ciudad"]) \
    .withColumn("idEquipo", functions.monotonically_increasing_id())

# Se le da el valor Desconocido a los datos no existentes de los equipos
df = df.fillna('Desconocido', subset=["nombre", "ciudad", "estado", "arena"])

# Mostramos la tabla final
df.show()

# Cast explícito de todas las columnas según los tipos esperados en Hive
df = df.select(
    col("idEquipo").cast(IntegerType()).alias("idEquipo"),
    col("estado_actual").cast(IntegerType()).alias("estado_actual"),
    col("nombre").cast(StringType()).alias("nombre"),
    col("arena").cast(StringType()).alias("arena"),
    col("ciudad").cast(StringType()).alias("ciudad"),
    col("estado").cast(StringType()).alias("estado")
)

# Almacenamos el resultado en Hive
df.write.mode("overwrite").insertInto("mydb.equipo")
