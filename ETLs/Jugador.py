import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions, Window, when

from pyspark.sql.functions import concat_ws, col, row_number, split

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
                .csv(path + "player.csv")

aux1_df = spark.read.option("delimiter", ",") \
                .option("header", True) \
                .csv(path + "common_player_info.csv")

aux2_df = spark.read.option("delimiter", ",") \
                .option("header", True) \
                .csv(path + "inactive_players.csv")


# Seleccionar los datos necesarios para la tabla y poner el nombre de las columnas especificos
df = df.select("id","full_name","is_active") \
       .withColumnRenamed("full_name", "nombre")
# seguir por aqui 
aux1_df = aux1_df.select("team_id","arena", "arenacapacity") \
       .withColumnRenamed("arenacapacity", "capacidad")
aux2_df = aux2_df.select("player_id","first_name", "last_name","team_name") \
       .withColumnRenamed("team_name", "equipo")

# Generamos el nombre en las tablas donde esta por separado
aux2_df = aux2_df.withColumn("nombre", concat_ws(" ", col("first_name"), col("last_name")))

# Hacemos Join de las diferentes tablas de datoss
df = df.join(aux1_df, on="team_id", how="left")

# Eliminar duplicados y generar la id de la tabla
df = df.dropDuplicates(["id"]) \
    .withColumn("idJugador", functions.monotonically_increasing_id())

# Eliminar columnas innecesarias
df = df.drop("id", "player_id", "first_name","last_name")

# Se le da el valor Desconocido a los datos no existentes de los equipos
df = df.fillna('Desconocido')

# Reorganizar columnas
df = df.select("idJugador")

# Mostramos la tabla final
df.show()

# Almacenamos el resultado en Hive
df.write.mode("overwrite").insertInto("mydb.jugador")