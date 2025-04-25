import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions
from pyspark.sql.functions import concat_ws, col

#mysql-connector-j-9.3.0.jar

# Crear la sesión Spark con la dependencia JDBC para MySQL
spark = SparkSession.builder \
                    .config("spark.sql.warehouse.dir", "hdfs://localhost:9000/user/hive/warehouse") \
                    .appName("TFG NBA") \
                    .enableHiveSupport() \
                    .getOrCreate()
 
               

path = "file:///home/tfg/Escritorio/TFG-NBA/ETLs/Datos/"


# Mostrar bases de datos
print("Bases de datos:")
spark.sql("SHOW DATABASES").show()

# Seleccionar una base de datos que sabes que tiene tablas (por ejemplo, "mydb")
spark.sql("USE mydb")

# Mostrar tablas
print("Tablas en mydb:")
spark.sql("SHOW TABLES").show()

spark.sql("USE mydb")
spark.sql("SELECT COUNT(*) FROM arbitro").show()
#df = spark.read.text("hdfs://localhost:9000/user/hive/warehouse")
#df.show()