-- HiveQL: Crear base de datos (opcional)
CREATE DATABASE IF NOT EXISTS mydb;
USE mydb;

-- Tabla Equipo
CREATE TABLE IF NOT EXISTS Equipo (
  idEquipo INT,
  estado_actual INT,
  nombre STRING,
  arena STRING,
  ciudad STRING,
  estado STRING
)
STORED AS PARQUET;

-- Tabla Arbitro
CREATE TABLE IF NOT EXISTS Arbitro (
  idArbitro INT,
  nombre STRING
)
STORED AS PARQUET;

-- Tabla Liga
CREATE TABLE IF NOT EXISTS Liga (
  idLiga INT,
  nombre STRING
)
STORED AS PARQUET;

-- Tabla Fecha
CREATE TABLE IF NOT EXISTS Fecha (
  idFecha INT,
  temporada STRING,
  dia INT,
  mes INT,
  ano INT
)
STORED AS PARQUET;

-- Tabla Ubicacion
CREATE TABLE IF NOT EXISTS Ubicacion (
  idUbicacion INT,
  capacidad STRING,
  arena STRING,
  ciudad STRING,
  estado STRING
)
STORED AS PARQUET;

-- Tabla Jugador
CREATE TABLE IF NOT EXISTS Jugador (
  idJugador INT,
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
STORED AS PARQUET;

-- Tabla Partido
CREATE TABLE IF NOT EXISTS Partido (
  idPartido INT,
  idEquipo_Local INT,
  idEquipo_Visitante INT,
  idArbitro INT,
  idLiga INT,
  idFecha INT,
  idUbicacion INT,
  idJugador INT,
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
STORED AS PARQUET;