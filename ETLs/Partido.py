import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions, Window

from pyspark.sql.functions import concat_ws, col, row_number, split, when, lit, coalesce, to_timestamp, dayofmonth, month, year, lower, explode, array, sum, round, trim, length

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
       .withColumnRenamed("player1_name", "jugador") \
       .withColumnRenamed("player1_team_city", "equipo_ciudad") \
       .withColumnRenamed("player1_team_nickname", "equipo_sobrenombre")  

df2 = df.select("game_id", "player2_name", "player2_team_city", "player2_team_nickname") \
       .withColumnRenamed("player2_name", "jugador") \
       .withColumnRenamed("player2_team_city", "equipo_ciudad") \
       .withColumnRenamed("player2_team_nickname", "equipo_sobrenombre")  

df3 = df.select("game_id", "player3_name", "player3_team_city", "player3_team_nickname") \
       .withColumnRenamed("player3_name", "jugador") \
       .withColumnRenamed("player3_team_city", "equipo_ciudad") \
       .withColumnRenamed("player3_team_nickname", "equipo_sobrenombre")        

# Generamos el nombre del equipo a partir de la ciudad y el sobrenombre
df1 = df1.withColumn("nombre_equipo", concat_ws(" ", col("equipo_ciudad"), col("equipo_sobrenombre")))
df2 = df2.withColumn("nombre_equipo", concat_ws(" ", col("equipo_ciudad"), col("equipo_sobrenombre")))
df3 = df3.withColumn("nombre_equipo", concat_ws(" ", col("equipo_ciudad"), col("equipo_sobrenombre")))

# Eliminamos datos innecesarios
df1 = df1.drop("equipo_ciudad", "equipo_sobrenombre")
df2 = df2.drop("equipo_ciudad", "equipo_sobrenombre")
df3 = df3.drop("equipo_ciudad", "equipo_sobrenombre")

# Unimos todos los datos en un dataset
df_jugadores = df1.union(df2).union(df3)

# Eliminamos nombres de jugadores repretidos en el mismo partido
df_jugadores = df_jugadores.dropDuplicates(["game_id", "jugador"])

# Normalizamos los nombres de los jugadores
df_jugadores = df_jugadores.withColumn("jugador", lower(trim(col("jugador"))))

# Seleccionamos los datos requeridos del dataset de partidos
df4 = aux0_df.select("game_id", "team_name_home", "team_name_away", "game_date", "season_type")

# Eliminamos partidos duplicados
df4 = df4.dropDuplicates(["game_id"])

# Seleccionar los datos de los arbitros
df5 = aux1_df.select("game_id","first_name","last_name")
 
# Generamos el nombre completo del arbitro
df5 = df5.withColumn("nombre_arbitro", concat_ws(" ", col("first_name"), col("last_name")))

# Eliminamos datos innecesarios
df5 = df5.drop("first_name","last_name")

# Juntamos los datos del partido y el arbitro de dicho partido
df_partidos = df4.join(df5, on="game_id", how="left")

# Obtenemos las tablas del almacen de datos
arbitro_df = spark.table("mydb.arbitro").select("idArbitro", "nombre")
liga_df = spark.table("mydb.liga").select("idLiga", "nombre")\
       .withColumnRenamed("nombre", "nombre_mal") 
fecha_df = spark.table("mydb.fecha").select("idFecha", "dia", "mes", "ano")
equipo_df = spark.table("mydb.equipo").select("idEquipo", "nombre", "arena", "ciudad", "estado")
ubicacion_df = spark.table("mydb.ubicacion").select("idUbicacion", "arena", "ciudad", "estado")
jugador_df = spark.table("mydb.jugador").select("idJugador", "nombre")

# Normalizamos los nombres de los jugadores
jugador_df = jugador_df.withColumn("nombre", lower(trim(col("nombre"))))

# Parte Arbitro #
# Obtenemos la id del arbitro
df_partidos = df_partidos.join(
    arbitro_df,
    df_partidos.nombre_arbitro == arbitro_df.nombre,
    how="left"
).drop(arbitro_df.nombre, "nombre_arbitro") 


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
).drop(liga_df.nombre, "nombre_arbitro", liga_df.nombre_mal) 

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
).drop(fecha_df.dia, fecha_df.mes, fecha_df.ano, "game_date", "dia", "mes", "ano") 

# Parte Equipo Visitante #
equipo_visitante_df = equipo_df.alias("visitante")
df_partidos = df_partidos.join(
    equipo_visitante_df,
    df_partidos.team_name_away == equipo_visitante_df.nombre,
    how="left"
).drop(equipo_df.nombre, equipo_df.arena, equipo_df.ciudad, equipo_df.estado, "team_name_away") \
.withColumnRenamed("idEquipo", "idEquipo_Visitante")  

# Parte Equipo Local #
equipo_local_df = equipo_df.alias("local")
df_partidos = df_partidos.join(
    equipo_local_df,
    df_partidos.team_name_home == equipo_local_df.nombre,
    how="left"
).drop(equipo_df.nombre, "team_name_home") \
.withColumnRenamed("idEquipo", "idEquipo_Local") 

# Parte Ubicacion #
df_partidos = df_partidos.join(
    ubicacion_df,
    on=[
        df_partidos.arena == ubicacion_df.arena,
        df_partidos.ciudad == ubicacion_df.ciudad,
        df_partidos.estado == ubicacion_df.estado
    ],
    how="left"
).drop("arena", "ciudad", "estado", ubicacion_df.arena, ubicacion_df.ciudad, ubicacion_df.estado)

# Parte Jugador #
# Unimos los datos de los jugadores con los del partido
df_partidos = df_partidos.join(df_jugadores, on="game_id", how="inner")

# Obtenemos la id a partir del nomvre de los jugadores en cada jugada
df_partidos = df_partidos.join(
    jugador_df,
    df_partidos.jugador == jugador_df.nombre,
    how="left"
).drop(jugador_df.nombre)

# Quitamos partidos sin jugadores asignados
df_partidos = df_partidos.filter("idJugador IS NOT NULL")

# Parte jugadas #
# Obtenemos los datos de las jugadas
df6 = df.select("game_id", "homedescription", "player1_name", "player2_name", "player3_name") \
        .withColumnRenamed("homedescription", "descripcion") \

df7 = df.select("game_id", "visitordescription", "player1_name", "player2_name", "player3_name") \
        .withColumnRenamed("visitordescription", "descripcion") 

# Unimos todas las descripciones
df_jugadas = df6.union(df7)

# Normalizamos las descripciones a minuscula 
df_jugadas = df_jugadas.withColumn("descripcion", lower(col("descripcion")))

# Eliminamos jugadas vacias
df_jugadas = df_jugadas.dropna(subset=["descripcion"])

# Normalizamos los nombres de los jugadores
df_jugadas = df_jugadas.withColumn("player1_name", lower(trim(col("player1_name")))) \
    .withColumn("player2_name", lower(trim(col("player2_name")))) \
    .withColumn("player3_name", lower(trim(col("player3_name"))))

# Caso especifico del saque #
# Obtenemos los saques existentes en las jugadas
descripcion_saques = df_jugadas.filter(lower(col("descripcion")).contains("jump ball"))

# Procesamos los datos de las jugadas
saque_jugador1 = descripcion_saques.select(
    col("game_id"),
    col("player1_name").alias("jugador"),
    lit(1).alias("Saques_Exitosos"),
    lit(1).alias("cantidad_ataques_exitosos"),
    lit(0).alias("Saques_Fallidos"),
    lit(0).alias("cantidad_ataques_fallidos"),
)

saque_jugador2 = descripcion_saques.select(
    col("game_id"),
    col("player2_name").alias("jugador"),
    lit(0).alias("Saques_Exitosos"),
    lit(0).alias("cantidad_ataques_exitosos"),
    lit(1).alias("Saques_Fallidos"),
    lit(1).alias("cantidad_ataques_fallidos"),
)

saque_jugador3 = descripcion_saques.select(
    col("game_id"),
    col("player3_name").alias("jugador"),
    lit(1).alias("Saques_Exitosos"),
    lit(1).alias("cantidad_ataques_exitosos"),
    lit(0).alias("Saques_Fallidos"),
    lit(0).alias("cantidad_ataques_fallidos"),
)

# Unimos las 3 tablas para tener los saques totales
saques_df = saque_jugador1.unionByName(saque_jugador2).unionByName(saque_jugador3)

# Obtenemos las ids de los jugadores
saques_df = saques_df.join(jugador_df, saques_df.jugador == jugador_df.nombre, "left") \
                         .drop("nombre", "jugador") 

# Caso especifico del bloqueo #
# Filtramos jugadas con bloqueos
descripcion_bloqueo = df_jugadas.filter(lower(col("descripcion")).contains("block"))

# Jugador 3 es quien realiza el bloqueo
bloqueos_df = descripcion_bloqueo.select(
    col("game_id"),
    col("player3_name").alias("jugador"),
    lit(1).alias("Tiros_Bloqueados")
)

# Obtenemos las ids de los jugadores
bloqueos_df = bloqueos_df.join(jugador_df, bloqueos_df.jugador == jugador_df.nombre, "left") \
                         .drop("nombre", "jugador") 

# Caso especifico del robo #
# Filtramos jugadas con robos
descripcion_robos = df_jugadas.filter(lower(col("descripcion")).contains("steal"))

# Jugador 2 es quien realiza el robo
robos_df = descripcion_robos.select(
    col("game_id"),
    col("player2_name").alias("jugador"),
    lit(1).alias("Robos_Exitosos")
)

# Obtenemos las ids de los jugadores
robos_df = robos_df.join(jugador_df, robos_df.jugador == jugador_df.nombre, "left") \
                         .drop("nombre", "jugador") 

# Caso general #
# Aplanamos los datos de las jugadas para cada jugador
df_jugadas = df_jugadas.withColumn(
    "jugador",
    explode(
        array(
            coalesce(col("player1_name"), lit("")),
            coalesce(col("player2_name"), lit("")),
            coalesce(col("player3_name"), lit(""))
        )
    )
).filter(col("jugador") != "") \
.select("game_id", "descripcion", "jugador")


# Obtenemos las ids de los jugadores
df_jugadas = df_jugadas.join(jugador_df, df_jugadas.jugador == jugador_df.nombre, "left") \
                         .drop("nombre", "jugador") 

# Generacion de estadisticas generales(solo realizadas por un jugador) por cada jugada segun sus datos
df_jugadas = df_jugadas \
    .withColumn("tiro_doble_exitoso", when(
        (lower(col("descripcion")).rlike("layup|dunk|shot|roll")) &
        (~lower(col("descripcion")).contains("3pt")) &
        (~lower(col("descripcion")).contains("turnover")) &
        (~lower(col("descripcion")).contains("miss")), 1).otherwise(0)) \
    .withColumn("tiro_doble_fallido", when(
        (lower(col("descripcion")).rlike("layup|dunk|shot|roll")) &
        (~lower(col("descripcion")).contains("3pt")) &
        (~lower(col("descripcion")).contains("turnover")) &
        (lower(col("descripcion")).contains("miss")), 1).otherwise(0)) \
    .withColumn("tiro_triple_exitoso", when(
        col("descripcion").contains("3pt") & ~col("descripcion").contains("miss"), 1).otherwise(0)) \
    .withColumn("tiro_triple_fallido", when(
        col("descripcion").contains("3pt") & col("descripcion").contains("miss"), 1).otherwise(0)) \
    .withColumn("tiro_libre_exitoso", when(
        col("descripcion").contains("free throw") & ~col("descripcion").contains("miss"), 1).otherwise(0)) \
    .withColumn("tiro_libre_fallido", when(
        col("descripcion").contains("free throw") & col("descripcion").contains("miss"), 1).otherwise(0)) \
    .withColumn("rebotes_obtenidos", when(
        col("descripcion").contains("rebound"), 1).otherwise(0)) \
    .withColumn("rebotes_defensivos_obtenidos", when(
        col("descripcion").contains("rebound") & col("descripcion").contains("defensive"), 1).otherwise(0)) \
    .withColumn("rebotes_ofensivos_obtenidos", when(
        col("descripcion").contains("rebound") & col("descripcion").contains("offensive"), 1).otherwise(0)) \
    .withColumn("perdidas_balon", when(
        col("descripcion").contains("turnover") & ~col("descripcion").contains("foul"), 1).otherwise(0)) \
    .withColumn("cambios_jugadores", when(
        col("descripcion").contains("sub") & col("descripcion").contains("for"), 1).otherwise(0)) \
    .withColumn("tiempos_muertos", when(
        col("descripcion").contains("timeout"), 1).otherwise(0)) \
    .withColumn("faltas_personales", when(
        (col("descripcion").contains("p.foul")) |
        (col("descripcion").contains("foul") & col("descripcion").contains("personal")), 1).otherwise(0)) \
    .withColumn("faltas_balon_suelto", when(
        col("descripcion").contains("l.b.foul"), 1).otherwise(0)) \
    .withColumn("faltas_disparo", when(
        col("descripcion").contains("s.foul"), 1).otherwise(0)) \
    .withColumn("faltas_rebote", when(
        col("descripcion").contains("foul turnover"), 1).otherwise(0)) \
    .withColumn("faltas_ofensivas", when(
        col("descripcion").contains("offensive") & col("descripcion").contains("foul"), 1).otherwise(0)) \
    .withColumn("cantidad_ataques_fallidos", when(
        col("descripcion").contains("miss"), 1).otherwise(0)) \
    .withColumn("jugada_total", when(
        col("descripcion").isNotNull() & (length(trim(col("descripcion"))) > 0), 1).otherwise(0))

# Agrupamos los datos #
# Agrupamos datos generales
df_estadisticas_finales = df_jugadas.groupBy("game_id", "idJugador").agg(
    sum("tiro_doble_exitoso").alias("Tiros_Dobles_Exitosos"),
    sum("tiro_doble_fallido").alias("Tiros_Dobles_Fallidos"),
    sum("tiro_triple_exitoso").alias("Tiros_Triples_Exitosos"),
    sum("tiro_triple_fallido").alias("Tiros_Triples_Fallidos"),
    sum("tiro_libre_exitoso").alias("Tiros_Libres_Exitosos"),
    sum("tiro_libre_fallido").alias("Tiros_Libres_Fallidos"),
    sum("rebotes_obtenidos").alias("Rebotes_Obtenidos"),
    sum("rebotes_defensivos_obtenidos").alias("Rebotes_Defensivos_Obtenidos"),
    sum("rebotes_ofensivos_obtenidos").alias("Rebotes_Ofensivos_Obtenidos"),
    sum("perdidas_balon").alias("Perdidas_Balon"),
    sum("cambios_jugadores").alias("Cambios_Jugadores"),
    sum("tiempos_muertos").alias("Tiempos_Muertos"),
    sum("faltas_personales").alias("Faltas_Personales"),
    sum("faltas_balon_suelto").alias("Faltas_Balon_Suelto"),
    sum("faltas_disparo").alias("Faltas_Disparo"),
    sum("faltas_rebote").alias("Faltas_Rebote"),
    sum("faltas_ofensivas").alias("Faltas_Ofensivas"),
    sum("jugada_total").alias("Numero_Jugadas_Totales"),
    sum("cantidad_ataques_fallidos").alias("Cantidad_Ataques_Fallidos_General")
)

# Agrupamos los datos de saques/bloqueos/robos
saques_df = saques_df.groupBy("game_id", "idJugador").agg(
    sum("Saques_Exitosos").alias("Saques_Exitosos"),
    sum("cantidad_ataques_exitosos").alias("cantidad_ataques_exitosos"),
    sum("Saques_Fallidos").alias("Saques_Fallidos"),
    sum("cantidad_ataques_fallidos").alias("cantidad_ataques_fallidos"),
)
bloqueos_df = bloqueos_df.groupBy("game_id", "idJugador").agg(sum("Tiros_Bloqueados").alias("Tiros_Bloqueados"))
robos_df = robos_df.groupBy("game_id", "idJugador").agg(sum("Robos_Exitosos").alias("Robos_Exitosos"))

# Juntar los datos de saques y generales #
# Realizamos un join entre la tabla general y las especificas
df_final = df_estadisticas_finales.join(
    saques_df,
    on=["game_id", "idJugador"],
    how="outer"
)

df_final = df_final.join(
    bloqueos_df,
    on=["game_id", "idJugador"],
    how="outer"
)

df_final = df_final.join(
    robos_df,
    on=["game_id", "idJugador"],
    how="outer"
)

# Reemplazo de posibles nulos antes de calculos
df_final = df_final.fillna(0, subset=[
    "Tiros_Triples_Exitosos", "Tiros_Dobles_Exitosos", "Tiros_Libres_Exitosos",
    "cantidad_ataques_exitosos", "Numero_Jugadas_Totales", "Cantidad_Ataques_Fallidos_General", "cantidad_ataques_fallidos"
])

# Sumamos los valores de la columna de ataques fallidos
df_final = df_final.withColumn(
    "Cantidad_Ataques_Fallidos",
    coalesce(col("Cantidad_Ataques_Fallidos_General"), lit(0)) + coalesce(col("cantidad_ataques_fallidos"), lit(0))
)

# Calculo de ataques exitosos #
df_final = df_final.withColumn(
    "Cantidad_Ataques_Exitosos",
    col("Tiros_Triples_Exitosos") + col("Tiros_Dobles_Exitosos") + col("Tiros_Libres_Exitosos") + col("cantidad_ataques_exitosos")
)

# Calculo probabilidades y puntos #
df_final = df_final \
.withColumn(
    "Proporcion_Exito_Ataques",
    when(col("Numero_Jugadas_Totales") > 0,
     round(col("Cantidad_Ataques_Exitosos") / col("Numero_Jugadas_Totales"), 3)
    ).otherwise(lit(0.0))
).withColumn(
    "Puntos_Totales",
    col("Tiros_Triples_Exitosos")*3 + col("Tiros_Dobles_Exitosos")*2 + col("Tiros_Libres_Exitosos")
)

# Union de las estadisticas y las claves #
# Unimos los datasets por la id del jugador y la game_id
df_partido_completo = df_final.join(
    df_partidos,
    on=["game_id", "IdJugador"],
    how="inner"
)

# Generar id_partido #
# Crear un DataFrame con los game_id únicos y asignarles un idPartido incremental
window_spec = Window.orderBy("game_id")

df_ids_partido = df_partido_completo.select("game_id").distinct() \
    .withColumn("idPartido", row_number().over(window_spec))

# Unir la tabla total con los ids
df_partido_completo = df_partido_completo.join(df_ids_partido, on="game_id", how="left")

# Reorganizar la tabla #
df_partido_completo = df_partido_completo.select(
    "idPartido",
    "idEquipo_Local",
    "idEquipo_Visitante",
    "idArbitro",
    "idLiga",
    "idFecha",
    "idUbicacion",
    "idJugador",
    "Puntos_Totales",
    "Saques_Exitosos",
    "Saques_Fallidos",
    "Tiros_Dobles_Exitosos",
    "Tiros_Dobles_Fallidos",
    "Tiros_Triples_Exitosos",
    "Tiros_Triples_Fallidos",
    "Tiros_Libres_Exitosos",
    "Tiros_Libres_Fallidos",
    "Robos_Exitosos",
    "Rebotes_Defensivos_Obtenidos",
    "Rebotes_Ofensivos_Obtenidos",
    "Rebotes_Obtenidos",
    "Tiros_Bloqueados",
    "Perdidas_Balon",
    "Cambios_Jugadores",
    "Tiempos_Muertos",
    "Faltas_Personales",
    "Faltas_Balon_Suelto",
    "Faltas_Disparo",
    "Faltas_Rebote",
    "Faltas_Ofensivas",
    "Numero_Jugadas_Totales",
    "Cantidad_Ataques_Exitosos",
    "Cantidad_Ataques_Fallidos",
    "Proporcion_Exito_Ataques"
)

# Rellenar estadisticas vacias con 0
df_partido_completo = df_partido_completo.fillna(0, subset=
    [
        "Puntos_Totales",
        "Saques_Exitosos",
        "Saques_Fallidos",
        "Tiros_Dobles_Exitosos",
        "Tiros_Dobles_Fallidos",
        "Tiros_Triples_Exitosos",
        "Tiros_Triples_Fallidos",
        "Tiros_Libres_Exitosos",
        "Tiros_Libres_Fallidos",
        "Robos_Exitosos",
        "Rebotes_Defensivos_Obtenidos",
        "Rebotes_Ofensivos_Obtenidos",
        "Rebotes_Obtenidos",
        "Tiros_Bloqueados",
        "Perdidas_Balon",
        "Cambios_Jugadores",
        "Tiempos_Muertos",
        "Faltas_Personales",
        "Faltas_Balon_Suelto",
        "Faltas_Disparo",
        "Faltas_Rebote",
        "Faltas_Ofensivas",
        "Numero_Jugadas_Totales",
        "Cantidad_Ataques_Exitosos",
        "Cantidad_Ataques_Fallidos",
        "Proporcion_Exito_Ataques"
    ])

# Mostramos la tabla final #
df_partido_completo.show()

# Almacenamos el resultado en Hive #
df_partido_completo.write.mode("overwrite").saveAsTable("mydb.partido")

