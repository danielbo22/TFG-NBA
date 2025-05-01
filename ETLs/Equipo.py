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

aux2_df = spark.read.option("delimiter", ",") \
                .option("header", True) \
                .csv(path + "team_history.csv")


# Seleccionar los datos necesarios para la tabla y poner el nombre de las columnas especificos
df = df.select("id","nickname","city","state") \
       .withColumnRenamed("nickname", "nombre") \
       .withColumnRenamed("city", "ciudad") \
       .withColumnRenamed("state", "estado")
aux1_df = aux1_df.select("team_id","arena")
aux2_df = aux2_df.select("team_id","year_active_till")

# Seleccionamos solo la tabla del historial con el año activo mas tardio
window_spec = Window.partitionBy("team_id").orderBy(col("year_active_till").desc())
team_year = aux2_df.withColumn("row_num", row_number().over(window_spec))
latest_team_year = team_year.filter(col("row_num") == 1).drop("row_num")

# Hacemos Join de las diferentes tablas de datos
df = df.join(aux1_df, on="team_id", how="left")
df = df.join(aux2_df, on="team_id", how="left")

#Se genera el valor del estado a partir de su ultimo año activo
df = df.withColumn("year_active_till", col("year_active_till").cast("int"))
df = df.withColumn(
    "estado_actual",
    when(col("year_active_till") == 2019, 1).otherwise(0)
)

# Eliminar duplicados y generar la id de la tabla
df = df.dropDuplicates(["id"]) \
    .withColumn("idEquipo", functions.monotonically_increasing_id())

# Eliminar columnas innecesarias
df = df.drop("team_id","id","year_active_till")

# Se le da el valor Desconocido a los datos no existentes de los equipos
df = df.fillna('Desconocido')

# Reorganizar columnas
df = df.select("idEquipo", "nombre")

# Mostramos la tabla final
df.show()

# Almacenamos el resultado en Hive
df.write.mode("overwrite").insertInto("mydb.equipo")