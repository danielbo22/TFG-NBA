import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions, Window

from pyspark.sql.functions import concat_ws, col, row_number, split, when, lit, coalesce, to_timestamp, dayofmonth, month, year

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
                .csv(path + "game.csv")

aux1_df = spark.read.option("delimiter", ",") \
                .option("header", True) \
                .csv(path + "officials.csv")

# Seleccionar los datos de jugadas con los jugadores y equipos que las hacen
df1 = df.select("game_id", "player1_name", "player1_team_city", "player1_team_nickname") \
       .withColumnRenamed("player1_name", "nombre") \
       .withColumnRenamed("player1_team_city", "equipo_ciudad") \
       .withColumnRenamed("player1_team_nickname", "equipo_sobrenombre")  

df2 = df.select("game_id", "player2_name", "player2_team_city", "player2_team_nickname") \
       .withColumnRenamed("player2_name", "nombre") \
       .withColumnRenamed("player2_team_city", "equipo_ciudad") \
       .withColumnRenamed("player2_team_nickname", "equipo_sobrenombre")  

df3 = df.select("game_id", "player3_name", "player3_team_city", "player3_team_nickname") \
       .withColumnRenamed("player3_name", "nombre") \
       .withColumnRenamed("player3_team_city", "equipo_ciudad") \
       .withColumnRenamed("player3_team_nickname", "equipo_sobrenombre")        

# Generamos el nombre del equipo a partir de la ciudad y el sobrenombre
df1 = df1.withColumn("nombre_equipo", concat_ws(" ", col("equipo_ciudad"), col("equipo_sobrenombre")))
df2 = df2.withColumn("nombre_equipo", concat_ws(" ", col("equipo_ciudad"), col("equipo_sobrenombre")))
df3 = df3.withColumn("nombre_equipo", concat_ws(" ", col("equipo_ciudad"), col("equipo_sobrenombre")))

# Eliminamos datos innecesarios
df1 = df1.drop("equipo_ciudad", "equipo_sobrenombre")

# Unimos todos los datos en un dataset
df_jugadas = df1.union(df2).union(df3)

# Seleccionamos los datos requeridos del dataset de partidos
df4 = aux0_df.select("game_id", "team_name_home", "team_name_away", "game_date", "season_type")

# Eliminamos partidos duplicados
df4 = df4.dropDuplicates("game_id")

# Seleccionar los datos de los arbitros
df5 = aux1_df.select("game_id","first_name","last_name")
 
# Generamos el nombre completo del arbitro
df5 = df5.withcolumn("nombre_arbitro", concat_ws(" ", col("first_name"), col("last_name")))

# Eliminamos datos innecesarios
df5 = df5.drop("first_name","last_name")

# Juntamos los datos del partido y el arbitro de dicho partido
df_partidos = df4.join(df5, on="game_id", how="left")

# Obtenemos las tablas del almacen de datos
arbitro_df = spark.table("mydb.arbitro").select("idArbitro", "nombre")
liga_df = spark.table("mydb.liga").select("idLiga", "nombre_mal")
fecha_df = spark.table("mydb.fecha").select("idFecha", "dia", "mes", "ano")
equipo_df = spark.table("mydb.equipo").select("idEquipo", "nombre", "arena", "ciudad", "estado")
ubicacion_df = spark.table("mydb.ubicacion").select("idUbicacion", "arena", "ciudad", "estado")
jugador_df = spark.table("mydb.jugador")

# Parte Arbitro #
# Obtenemos la id del arbitro
df_partidos = df_partidos.join(
    arbitro_df,
    df_partidos.nombre_arbitro == arbitro_df.nombre,
    how="left"
).drop(df_partidos.nombre, df_partidos.nombre_arbitro) 


#Parte Liga #
# normalizamos el caso all-stars y all stars
liga_df = liga_df.withColumn(
    "nombre",
    when(col("nombre_mal") == "All Star", "All-Star").otherwise(col("nombre_mal")))

# Obtenemos la id de la liga
df_partidos = df_partidos.join(
    liga_df,
    df_partidos.season_type == liga_df.nombre,
    how="left"
).drop(df_partidos.nombre, df_partidos.nombre_arbitro, df_partidos.nombre_mal) 

# Parte Fecha #
# Generamos el dia, mes y ano de cada fecha
df_partidos = df_partidos.withColumn("game_date", to_timestamp("game_date", "yyyy-MM-dd HH:mm:ss"))
df_partidos = df_partidos.withColumn("dia", dayofmonth("game_date")) \
                         .withColumn("mes", month("game_date")) \
                         .withColumn("ano", year("game_date"))

# Obtenemos la id de la fecha
df_partidos = df_partidos.join(
    fecha_df,
    on=[
        df_partidos.dia == fecha_df.dia,
        df_partidos.mes == fecha_df.mes,
        df_partidos.ano == fecha_df.ano
    ],
    how="left"
).drop(fecha_df.dia, fecha_df.mes, fecha_df.ano, df_partidos.game_date, df_partidos.dia, df_partidos.mes, df_partidos.ano) 

# Parte Equipo Visitante #
df_partidos = df_partidos.join(
    equipo_df,
    df_partidos.team_name_away == equipo_df.nombre,
    how="left"
).drop(equipo_df.nombre, equipo_df.arena, equipo_df.ciudad, equipo_df.estado, df_partidos.team_name_away) \
.withColumnRenamed("idEquipo", "idEquipo_Visitante")  

# Parte Equipo Local #
df_partidos = df_partidos.join(
    equipo_df,
    df_partidos.team_name_home == equipo_df.nombre,
    how="left"
).drop(equipo_df.nombre, "team_name_home") \
.withColumnRenamed("idEquipo", "idEquipo_Local") 

# Parte Ubicacion #
df_partidos = df_partidos.join(
    equipo_df,
    on=[
        df_partidos.arena == ubicacion_df.arena,
        df_partidos.ciudad == ubicacion_df.ciudad,
        df_partidos.estado == ubicacion_df.estado
    ],
    how="left"
).drop(df_partidos.arena, df_partidos.ciudad, df_partidos.estado, ubicacion_df.arena, ubicacion_df.ciudad, ubicacion_df.estado)

# Parte Jugador y jugadas #













