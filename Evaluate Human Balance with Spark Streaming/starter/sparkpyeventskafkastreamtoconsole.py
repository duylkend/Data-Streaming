from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructField, StructType, StringType, FloatType, DateType

# Initialize Spark session with an appropriate application name and log level
spark = SparkSession.builder.appName("KafkaEventsProcessing").getOrCreate()
spark.sparkContext.setLogLevel('WARN')

# Define the schema for parsing JSON data
eventSchema = StructType([
    StructField("customer", StringType(), nullable=True),
    StructField("score", FloatType(), nullable=True),
    StructField("riskDate", DateType(), nullable=True)
])

# Read streaming data from the Kafka topic 'stedi-events'
kafkaDF = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "stedi-events") \
    .option("startingOffsets", "earliest") \
    .load() \
    .selectExpr("CAST(value AS STRING) AS jsonData")

# Parse the JSON data and select relevant fields
parsedDF = kafkaDF.select(from_json(col("jsonData"), eventSchema).alias("data")) \
    .select("data.customer", "data.score", "data.riskDate")

# Register the DataFrame as a temporary view for SQL queries
parsedDF.createOrReplaceTempView("CustomerRisk")

# Execute SQL query to extract required fields
customerRiskDF = spark.sql("SELECT customer, score FROM CustomerRisk")

# Write the results to the console in append mode
customerRiskDF.writeStream \
    .outputMode("append") \
    .format("console") \
    .start() \
    .awaitTermination()
