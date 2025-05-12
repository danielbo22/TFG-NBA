import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions, Window

from pyspark.sql.functions import concat_ws, col, row_number, split, when

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
       .withColumnRenamed("full_name", "nombre") \
       .withColumnRenamed("is_active", "activo")

aux1_df = aux1_df.select("person_id", "display_first_last", "school", "country", "height", "weight", "position", "rosterstatus", "team_name", "team_city", "from_year", "to_year") \
       .withColumnRenamed("person_id", "player_id") \
       .withColumnRenamed("display_first_last", "nombre") \
       .withColumnRenamed("position", "posicion") \
       .withColumnRenamed("school", "escuela") \
       .withColumnRenamed("country", "pais") 

aux2_df = aux2_df.select("player_id","first_name", "last_name", "team_city", "team_name")

aux3_df = aux3_df.select("player_id","player_name", "position", "height_wo_shoes", "wingspan", "weight", "standing_reach") \
       .withColumnRenamed("player_name", "nombre") \
       .withColumnRenamed("position", "posicion") 

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

# Elimina duplicados de la tabla principal
df = df.dropDuplicates(["player_id"])

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
df = df.withColumn(
    "peso",
    when(col("peso") <= 74.99 & (col("peso") > 0), "Muy liviano")
    .when((col("peso") >= 75) & (col("peso") <= 94.99), "Liviano")
    .when((col("peso") >= 95) & (col("peso") <= 119.99), "Medio")
    .when((col("peso") >= 120) & (col("peso") <= 139.99), "Pesado")
    .when(col("peso") >= 140, "Muy Pesado")
    .otherwise("Desconocido")
)

# Categorizacion de altura 
df = df.withColumn(
    "altura",
    when((col("altura") > 0) & (col("altura") <= 1.79), "Muy bajo")
    .when((col("altura") >= 1.80) & (col("altura") <= 1.89), "Bajo")
    .when((col("altura") >= 1.90) & (col("altura") <= 2.04), "Medio")
    .when((col("altura") >= 2.05) & (col("altura") <= 2.14), "Alto")
    .when(col("altura") >= 2.15, "Muy alto")
    .otherwise("Desconocido")
)

# Categorizacion de envergadura
df = df.withColumn(
    "envergadura",
    when((col("envergadura") > 0) & (col("envergadura") <= 1.89), "Muy corta")
    .when((col("envergadura") >= 1.90) & (col("envergadura") <= 2.04), "Corta")
    .when((col("envergadura") >= 2.05) & (col("envergadura") <= 2.19), "Media")
    .when((col("envergadura") >= 2.20) & (col("envergadura") <= 2.34), "Larga")
    .when(col("envergadura") >= 2.35, "Muy larga")
    .otherwise("Desconocido")
)

# Categorizacion de alcance_pie
df = df.withColumn(
    "alcance_pie",
    when((col("alcance_pie") > 0) & (col("alcance_pie") <= 2.34), "Muy bajo")
    .when((col("alcance_pie") >= 2.35) & (col("alcance_pie") <= 2.54), "Bajo")
    .when((col("alcance_pie") >= 2.55) & (col("alcance_pie") <= 2.84), "Medio")
    .when((col("alcance_pie") >= 2.85) & (col("alcance_pie") <= 2.99), "Alto")
    .when(col("alcance_pie") >= 3.0, "Muy alto")
    .otherwise("Desconocido")
)

# Categorizacion carrera_profesional
df = df.withColumn(
    "carrera_profesional",
    when((col("carrera_profesional") >= 0) & (col("carrera_profesional") <= 9), "Novato")
    .when((col("carrera_profesional") >= 10) & (col("carrera_profesional") <= 19), "Consolidado")
    .when((col("carrera_profesional") >= 20) & (col("carrera_profesional") <= 29), "Veterano")
    .when(col("carrera_profesional") >= 30, "Longevo")
    .otherwise("Desconocido")
)

# Dar valor al estado_actual (de momento no)


# Dar valor a la posicion(quitar valor abreviado por uno normal)


# Eliminar duplicados(redundante) y generar la id de la tabla
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