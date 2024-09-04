from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, unbase64, col, split
from pyspark.sql.types import StructField, StructType, StringType, BooleanType, ArrayType, DateType

# Define schema for Kafka Redis messages
redisSchema = StructType([
    StructField("key", StringType()),
    StructField("existType", StringType()),
    StructField("Ch", BooleanType()),
    StructField("Incr", BooleanType()),
    StructField("zSetEntries", ArrayType(
        StructType([
            StructField("element", StringType()),
            StructField("Score", StringType())
        ])
    ))
])

# Define schema for JSON Customer data
customerJsonSchema = StructType([
    StructField("customer", StringType()),
    StructField("score", StringType()),
    StructField("email", StringType()),
    StructField("birthYear", StringType())
])

# Define schema for Kafka stedi-events
eventsSchema = StructType([
    StructField("customer", StringType()),
    StructField("score", FloatType()),
    StructField("riskDate", DateType())
])

# Initialize Spark session and set log level
spark = SparkSession.builder.appName("KafkaRedisProcessing").getOrCreate()
spark.sparkContext.setLogLevel('WARN')

# Read streaming data from Kafka topic 'redis-server'
kafkaRedisDF = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "redis-server") \
    .option("startingOffsets", "earliest") \
    .load() \
    .selectExpr("CAST(value AS STRING) AS value")

# Parse the JSON data and create temporary view
redisDF = kafkaRedisDF \
    .withColumn("value", from_json(col("value"), redisSchema)) \
    .select(col("value.key"), col("value.existType"), col("value.Ch"), col("value.Incr"), col("value.zSetEntries")) \
    .createOrReplaceTempView("RedisSortedSet")

# Extract the first element from the zSetEntries array and decode
zSetEntriesDF = spark.sql("SELECT zSetEntries[0].element AS encodedCustomer FROM RedisSortedSet")
decodedEntriesDF = zSetEntriesDF.withColumn("customer", unbase64(col("encodedCustomer")).cast("string"))

# Parse the decoded customer JSON and create a temporary view
customerSchema = StructType([
    StructField("customerName", StringType()),
    StructField("email", StringType()),
    StructField("phone", StringType()),
    StructField("birthDay", StringType())
])

customerRecordsDF = decodedEntriesDF \
    .withColumn("customer", from_json(col("customer"), customerSchema)) \
    .select(col("customer.*")) \
    .createOrReplaceTempView("CustomerRecords")

# Filter and transform the customer data
emailAndBirthDayDF = spark.sql("SELECT * FROM CustomerRecords WHERE email IS NOT NULL AND birthDay IS NOT NULL")
emailAndBirthYearDF = emailAndBirthDayDF \
    .withColumn("birthYear", split(col("birthDay"), "-").getItem(0)) \
    .select(col("email"), col("birthYear"))

# Write the resulting DataFrame to the console
emailAndBirthYearDF.writeStream \
    .outputMode("append") \
    .format("console") \
    .start() \
    .awaitTermination()

# Run the Python script with the following command:
# /home/workspace/submit-redis-kafka-streaming.sh
# Verify the data looks correct
