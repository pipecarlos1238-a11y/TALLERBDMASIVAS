from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# 1. Iniciar Spark configurando los conectores de Kafka y MySQL
spark = SparkSession.builder \
    .appName("Kafka_SparkSQL_MySQL") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,mysql:mysql-connector-java:8.0.33") \
    .getOrCreate()

# Reducir los logs para ver la consola limpia
spark.sparkContext.setLogLevel("WARN")

# 2. Definir la estructura (esquema) de los datos que vienen del JSON
esquema = StructType([
    StructField("origen", StringType(), True),
    StructField("valor", IntegerType(), True)
])

# 3. Conectarse a Kafka y leer el stream
df_kafka = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "topic_datos") \
    .option("startingOffsets", "latest") \
    .load()

# 4. Usar Spark SQL para procesar el JSON
df_datos = df_kafka.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), esquema).alias("data")) \
    .select("data.*")

# 5. Función que procesa los LOTES y hace el INSERT en MySQL
def procesar_lote_mysql(df_lote, epoch_id):
    # Escribir el DataFrame del lote en MySQL
    df_lote.write \
        .format("jdbc") \
        .option("url", "jdbc:mysql://localhost:3306/tu_base_de_datos") \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .option("dbtable", "tabla_datos") \
        .option("user", "root") \
        .option("password", "tu_contraseña") \
        .mode("append") \
        .save()
    pass

# 6. Ejecutar el consumidor en modo Lotes (foreachBatch)
query = df_datos.writeStream \
    .foreachBatch(procesar_lote_mysql) \
    .start()

query.awaitTermination()