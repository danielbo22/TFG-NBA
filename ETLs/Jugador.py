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
                .csv(path + "play_by_play.csv")

aux0_df = spark.read.option("delimiter", ",") \
                .option("header", True) \
                .csv(path + "player.csv")

aux1_df = spark.read.option("delimiter", ",") \
                .option("header", True) \
                .csv(path + "common_player_info.csv")

aux2_df = spark.read.option("delimiter", ",") \
                .option("header", True) \
                .csv(path + "inactive_players.csv")

aux3_df = spark.read.option("delimiter", ",") \
                .option("header", True) \
                .csv(path + "draft_combine_stats.csv")


# Seleccionar los datos necesarios para la tabla y poner el nombre de las columnas especificos
df1 = df.select("player1_id", "player1_name", "player1_team_city", "player1_team_nickname") \
       .withColumnRenamed("player1_id", "player_id") \
       .withColumnRenamed("player1_name", "nombre") 

df2 = df.select("player2_id", "player2_name", "player2_team_city", "player2_team_nickname") \
       .withColumnRenamed("player2_id", "player_id") \
       .withColumnRenamed("player2_name", "nombre") 

df3 = df.select("player3_id", "player3_name", "player3_team_city", "player3_team_nickname") \
       .withColumnRenamed("player3_id", "player_id") \
       .withColumnRenamed("player3_name", "nombre") 

aux0_df = aux0_df.select("id","full_name","is_active") \
       .withColumnRenamed("id", "player_id") \
       .withColumnRenamed("full_name", "nombre")

aux1_df = aux1_df.select("person_id", "display_first_last", "school", "country", "height", "weight", "position", "rosterstatus", "team_name", "team_city", "from_year", "to_year") \
       .withColumnRenamed("person_id", "player_id") \
       .withColumnRenamed("display_first_last", "nombre") \
       .withColumnRenamed("school", "escuela") \
       .withColumnRenamed("country", "pais") 

aux2_df = aux2_df.select("player_id","first_name", "last_name", "team_city", "team_name")

aux3_df = aux3_df.select("player_id","player_name", "position", "height_wo_shoes", "wingspan", "weight", "standing_reach") \
       .withColumnRenamed("player_name", "nombre") \

# Generamos el nombre del equipo en cada df
df1 = df1.withColumn("equipo", concat_ws(" ", col("player1_team_city"), col("player1_team_nickname")))
df1 = df1.withColumn("equipo", concat_ws(" ", col("player2_team_city"), col("player2_team_nickname")))
df1 = df1.withColumn("equipo", concat_ws(" ", col("player3_team_city"), col("player3_team_nickname")))


# Le damos el atributo de activo negativo a todos los jugadores del csv inactive_players
aux2_df = aux2_df.withColumn("is_active", 0)

# Generamos el nombre en las tablas donde esta por separado
aux2_df = aux2_df.withColumn("nombre", concat_ws(" ", col("first_name"), col("last_name")))

# Quitamos las columnas innecesarias 
aux2_df = aux2_df.drop("first_name", "last_name")
df1 = df1.drop("player1_team_city", "player1_team_nickname")
df2 = df2.drop("player2_team_city", "player2_team_nickname")
df3 = df3.drop("player3_team_city", "player3_team_nickname")

#generamos la tabla principal de datos
df = df1.union(df2).union(df3)

# Hacemos Join de las tablas principales de datos de jugadores
df = df.join(aux0_df, on="player_id", how="left")
df = df.join(aux2_df, on="player_id", how="left")
df = df.join(aux1_df, on="player_id", how="left")
df = df.join(aux3_df, on="player_id", how="left")

# Paso de valores de unidades de medidas estadounidenses a estandar
df = df.withColumn("peso", col("weight") * 0.45359237) \
       .withColumn("altura", col("height_wo_shoes") * 0.0254) \
       .withColumn("envergadura", col("wingspan") * 0.0254) \
       .withColumn("alcance_pie", col("standing_reach") * 0.0254)

# Obtención de valores de carrera_profesional
df = df.withColumn("carrera_profesional", col("to_year") - col("from_year"))

# Categorizacion de peso


# Categorizacion de altura 


# Categorizacion de envergadura


# Categorizacion de alcance_pie


# Categorizacion carrera_profesional


# Dar valor al estado_actual


# Dar valor a la posicion(quitar valor abreviado por uno normal)


# Eliminar duplicados y generar la id de la tabla
df = df.dropDuplicates(["player_id"]) \
    .withColumn("idJugador", functions.monotonically_increasing_id())

# Eliminar columnas innecesarias
df = df.drop("player_id", "first_name","last_name")

# Se le da el valor Desconocido a los datos no existentes de los equipos
df = df.fillna('Desconocido')

# Reorganizar columnas
df = df.select("idJugador", "nombre", "equipo", "escuela", "pais", )

# Mostramos la tabla final
df.show()

# Almacenamos el resultado en Hive
df.write.mode("overwrite").saveAsTable("mydb.jugador")