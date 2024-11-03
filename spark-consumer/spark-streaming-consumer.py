import os 
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col

schema = StructType([
    StructField("id", StringType(), True),
    StructField("brand", StringType(), True),
    StructField("screen_size", StringType(), True),
    StructField("ram", StringType(), True),
    StructField("rom", StringType(), True),
    StructField("sim_type", StringType(), True),
    StructField("battery", StringType(), True),
    StructField("price", StringType(), True)
])

# Get the current directory (where your spark script is running)
current_dir = os.path.dirname(os.path.abspath(__file__))

# Create checkpoint path relative to current directory
checkpoint_location = os.path.join(current_dir, "checkpoint_spark")

# Initialize spark session
spark = SparkSession \
    .builder \
    .appName("Spark Streaming Consumer") \
    .getOrCreate()

# Set the log level to reduce verbosity
spark.sparkContext.setLogLevel("WARN")

# Read from kafka 
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "test") \
    .option("startingOffsets", "earliest") \
    .load()

# Parse the kakfa messages as json
json_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")
# Cast the necessary columns to double
json_df = json_df \
    .withColumn("screen_size", col("screen_size").cast("double")) \
    .withColumn("ram", col("ram").cast("double")) \
    .withColumn("rom", col("rom").cast("double")) \
    .withColumn("battery", col("battery").cast("double")) \
    .withColumn("price", col("price").cast("double"))

json_df.printSchema()