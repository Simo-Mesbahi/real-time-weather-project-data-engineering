from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_extract, avg

# Charger la configuration
import configparser

config = configparser.ConfigParser()
config.read("spark_config.ini")

LOG_FILE = config["Spark"]["log_file"]
OUTPUT_MODE = config["Spark"]["output_mode"]

# Créer une session Spark
spark = SparkSession.builder \
    .appName("Real-Time Weather Processing") \
    .getOrCreate()

# Lire les logs météo depuis un fichier texte
weather_logs = spark.readStream \
    .format("text") \
    .option("path", LOG_FILE) \
    .load()

# Extraire les informations clés
weather_data = weather_logs.select(
    regexp_extract(col("value"), r"Temperature: (\d+\.\d+|\d+)°C", 1).cast("float").alias("temperature"),
    regexp_extract(col("value"), r"Wind Speed: (\d+\.\d+) km/h", 1).cast("float").alias("wind_speed"),
    regexp_extract(col("value"), r"Condition: (\d+)", 1).alias("condition_code")
)

# Calculer la température moyenne en temps réel
average_temperature = weather_data.groupBy().agg(avg("temperature").alias("avg_temperature"))

# Afficher les résultats dans la console
query = average_temperature.writeStream \
    .outputMode(OUTPUT_MODE) \
    .format("console") \
    .start()

query.awaitTermination()
