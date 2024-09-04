from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_json, col, unbase64, base64, split, expr
from pyspark.sql.types import StructField, StructType, StringType, BooleanType, ArrayType, FloatType, StructField

# Create a Spark Session
spark = SparkSession.builder \
    .appName("STEDI") \
    .getOrCreate()

# Set log level to WARN
spark.sparkContext.setLogLevel("WARN")

# Define the schema for Redis server topic
redis_schema = StructType([
    StructField("key", StringType(), True),
    StructField("existType", StringType(), True),
    StructField("Ch", BooleanType(), True),
    StructField("Incr", BooleanType(), True),
    StructField("zSetEntries", ArrayType(StructType([
        StructField("element", StringType(), True),
        StructField("Score", StringType(), True)
    ])), True)
])

# Define the schema for Customer JSON
customer_schema = StructType([
    StructField("customer", StringType(), True),
    StructField("score", StringType(), True),
    StructField("email", StringType(), True),
    StructField("birthYear", StringType(), True)
])

# Define the schema for STEDI events topic
stedi_schema = StructType([
    StructField("customer", StringType(), True),
    StructField("score", FloatType(), True),
    StructField("riskDate", DateType(), True)
])

# Read from Kafka redis-server topic
redis_df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "redis-server") \
    .option("startingOffsets", "earliest") \
    .load()

# Cast the value column as STRING
redis_df = redis_df.selectExpr("CAST(value AS STRING)")

# Parse the JSON value
parsed_redis_df = redis_df.withColumn("value", from_json("value", redis_schema)) \
    .select(col('value.existType'), col('value.Ch'),\
                    col('value.Incr'), col('value.zSetEntries'))

# Create a temporary view
parsed_redis_df.createOrReplaceTempView("RedisSortedSet")

# Extract the encodedCustomer from zSetEntries
encoded_customer_df = spark.sql("""
    SELECT zSetEntries[0].element AS encodedCustomer
    FROM RedisSortedSet
""")

# Decode the base64 encoded customer data
decoded_customer_df = encoded_customer_df.withColumn("customer", unbase64(col("encodedCustomer")).cast("string"))

# Parse the decoded JSON customer data
cus_Schema = StructType (
    [
        StructField("customerName", StringType()),
        StructField("email", StringType()),
        StructField("phone", StringType()),
        StructField("birthDay", StringType()),
    ]
)
customer_df = decoded_customer_df.withColumn("customer", from_json(col("customer"), cus_Schema)) \
    .select(col("customer.*"))

# Create a temporary view for customer records
customer_df.createOrReplaceTempView("CustomerRecords")

# Select non-null fields as email and birth year
email_and_birth_day_df = customer_df.select("email", "birthDay").filter(col("birthDay").isNotNull())

# Split the birth year from the birthday
email_and_birth_year_df = email_and_birth_day_df.withColumn("birthYear", split(col("birthDay"), "-").getItem(0)) \
    .select("email", "birthYear")

# Read from Kafka stedi-events topic
stedi_df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "stedi-events") \
    .option("startingOffsets", "earliest") \
    .load()

# Cast the value column as STRING
stedi_df = stedi_df.selectExpr("CAST(value AS STRING)")

# Parse the JSON value
parsed_stedi_df = stedi_df.withColumn("value", from_json("value", stedi_schema)) \
    .select(col("value.*"))

# Create a temporary view for STEDI events
parsed_stedi_df.createOrReplaceTempView("CustomerRisk")

# Select customer and score from the temporary view
customer_risk_df = spark.sql("""
    SELECT customer, score
    FROM CustomerRisk
""")

# Join the two dataframes on email to get risk score and birth year
joined_df = customer_risk_df.join(email_and_birth_year_df, customer_risk_df.customer == email_and_birth_year_df.email)

# Sink the joined dataframes to a new Kafka topic
joined_df.selectExpr("CAST(customer AS STRING) AS key", "to_json(struct(*)) AS value") \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "stedi-cus_core") \
    .option("checkpointLocation", "/tmp/checkpoints") \
    .start() \
    .awaitTermination()
