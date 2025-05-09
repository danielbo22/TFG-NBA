-- HiveQL: Crear base de datos (opcional)
CREATE DATABASE IF NOT EXISTS mydb;
USE mydb;

-- Tabla Equipo
CREATE TABLE IF NOT EXISTS Equipo (
  idEquipo BIGINT,
  estado_actual INT,
  nombre STRING,
  arena STRING,
  ciudad STRING,
  estado STRING
)
STORED AS PARQUET
LOCATION 'hdfs://localhost:9000/opt/hive/apache-hive-3.1.3-bin/warehouse/mydb.db/equipo';

-- Tabla Arbitro
CREATE TABLE IF NOT EXISTS Arbitro (
  idArbitro BIGINT,
  nombre STRING
)
STORED AS PARQUET
LOCATION 'hdfs://localhost:9000/opt/hive/apache-hive-3.1.3-bin/warehouse/mydb.db/arbitro';

-- Tabla Liga
CREATE TABLE IF NOT EXISTS Liga (
  idLiga BIGINT,
  nombre STRING
)
STORED AS PARQUET
LOCATION 'hdfs://localhost:9000/opt/hive/apache-hive-3.1.3-bin/warehouse/mydb.db/liga';

-- Tabla Fecha
CREATE TABLE IF NOT EXISTS Fecha (
  idFecha BIGINT,
  temporada STRING,
  dia INT,
  mes INT,
  ano INT
)
STORED AS PARQUET
LOCATION 'hdfs://localhost:9000/opt/hive/apache-hive-3.1.3-bin/warehouse/mydb.db/fecha';

-- Tabla Ubicacion
CREATE TABLE IF NOT EXISTS Ubicacion (
  idUbicacion BIGINT,
  capacidad STRING,
  arena STRING,
  ciudad STRING,
  estado STRING
)
STORED AS PARQUET
LOCATION 'hdfs://localhost:9000/opt/hive/apache-hive-3.1.3-bin/warehouse/mydb.db/ubicacion';

-- Tabla Jugador
CREATE TABLE IF NOT EXISTS Jugador (
  idJugador BIGINT,
  altura STRING,
  peso STRING,
  nombre STRING,
  envergadura STRING,
  alcance_pie STRING,
  posicion STRING,
  escuela STRING,
  pais STRING,
  equipo STRING,
  estado_actual INT,
  carrera_profesional STRING
)
STORED AS PARQUET
LOCATION 'hdfs://localhost:9000/opt/hive/apache-hive-3.1.3-bin/warehouse/mydb.db/jugador';

-- Tabla Partido
CREATE TABLE IF NOT EXISTS Partido (
  idPartido BIGINT,
  idEquipo_Local BIGINT,
  idEquipo_Visitante BIGINT,
  idArbitro BIGINT,
  idLiga BIGINT,
  idFecha BIGINT,
  idUbicacion BIGINT,
  idJugador BIGINT,
  Puntos_Totales INT,
  Saques_Exitosos INT,
  Saques_Fallidos INT,
  Tiros_Dobles_Exitosos INT,
  Tiros_Dobles_Fallidos INT,
  Tiros_Triples_Exitosos INT,
  Tiros_Triples_Fallidos INT,
  Tiros_Libres_Exitosos INT,
  Tiros_LIbres_Fallidos INT,
  Robos_Exitosos INT,
  Robos_Fallidos INT,
  Rebotes_Defensivos_Obtenidos INT,
  Rebotes_Ofensivos_Obtenidos INT,
  Rebotes_Obtenidos INT,
  Tiros_Bloqueados INT,
  Perdidas_Balon INT,
  Cambios_Jugadores INT,
  Tiempos_Muertos INT,
  Faltas_Personales INT,
  Faltas_Balon_Suelto INT,
  Faltas_Disparo INT,
  Faltas_Rebote INT,
  Faltas_Ofensivas INT,
  Numero_Jugadas_Totales INT,
  Cantidad_Ataques_Exitosos INT,
  Cantidad_Ataques_Fallidos INT,
  Proporcion_Exito_Ataques FLOAT
)
STORED AS PARQUET
LOCATION 'hdfs://localhost:9000/opt/hive/apache-hive-3.1.3-bin/warehouse/mydb.db/partido';