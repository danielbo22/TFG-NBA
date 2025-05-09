import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions, Window
from datetime import datetime

from pyspark.sql.functions import col, expr, to_date, year, month, dayofmonth, concat_ws, when
from pyspark.sql.types import IntegerType, StringType

#mysql-connector-j-9.3.0.jar

# Crear la sesión Spark
spark = SparkSession.builder \
                    .config("spark.sql.warehouse.dir", "hdfs://localhost:9000/user/hive/warehouse") \
                    .appName("TFG NBA") \
                    .enableHiveSupport() \
                    .getOrCreate()
 
# Generamos la fecha inicial y final de la NBA (segun los datos usados)
fecha_inicial = "1946-01-01"
fecha_final = "2019-12-31"

# Calculamos el numero total de dias
inicio = datetime.strptime(fecha_inicial, "%Y-%m-%d")
fin = datetime.strptime(fecha_final, "%Y-%m-%d")
dias = (fin - inicio).days + 1

# Creamos un DataFrame con un rango de días
df = spark.range(0, dias).withColumn(
    "fecha", expr(f"date_add('{fecha_inicial}', CAST(id AS INT))")
)

# Obtenemos los datos requeridos del dataframe creado
df = df.select(
    col("fecha"),
    year("fecha").cast(IntegerType()).alias("ano"),
    month("fecha").cast(IntegerType()).alias("mes"),
    dayofmonth("fecha").cast(IntegerType()).alias("dia")
)

# Ordenamos las fechas antes de añadir la id
df = df.orderBy("fecha", ascending=True)

# Generamos la id de cada fecha
df = df.withColumn("idFecha", functions.monotonically_increasing_id())

# Calculamos la temporada segun la fecha especifica
df = df.withColumn(
    "ano_inicio_temporada",
    when(col("mes") >= 10, col("ano")).otherwise(col("ano") - 1)
)

df = df.withColumn(
    "temporada",
    concat_ws("-", col("ano_inicio_temporada"), (col("ano_inicio_temporada") + 1))
)

# Eliminar columnas innecesarias
df = df.drop("fecha", "ano_inicio_temporada")

# Mostramos la tabla final
df.show()

# Cast explícito de todas las columnas según los tipos esperados en Hive
df = df.select(
    col("idFecha").cast(IntegerType()).alias("idFecha"),
    col("dia").cast(IntegerType()).alias("dia"),
    col("mes").cast(IntegerType()).alias("mes"),
    col("ano").cast(IntegerType()).alias("ano"),
    col("temporada").cast(StringType()).alias("temporada")
)

# Almacenamos el resultado en Hive
df.write.mode("overwrite").saveAsTable("mydb.fecha")