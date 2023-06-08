import re 
import findspark
from pyspark.ml import PipelineModel
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, udf
from pyspark.sql.types import StructType, StructField, StringType, ArrayType
from datetime import datetime, timedelta

three_years_ago = datetime.now() - timedelta(days=365 * 3)
starting_timestamp = int(three_years_ago.timestamp() * 1000)

# Schema for the incoming data
schema = StructType([StructField("message", StringType())])

# Read the data from kafka
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "Opdivo, Keytruda, Tagrisso, Alecensa, Tecentriq, Bevacizumab, Stelara, Zytiga, Lynparza, Lixiana") \
    .option("startingOffsets", starting_timestamp) \
    .option("header", "true") \
    .load() \
    .selectExpr("CAST(value AS STRING) as message")

df = df.withColumn("value", from_json("message", schema)) #new value column with json of message

# Pre-processing the data
pre_process = udf(
    lambda x: re.sub(r'[^A-Za-z\n ]|(http\S+)|(www.\S+)', '', x.lower().strip()).split(), ArrayType(StringType())
)

df = df.withColumn("cleaned_data", pre_process(df.message)).dropna()

# Load the pre-trained model
pipeline_model = PipelineModel.load(path_to_model)
# Make predictions
prediction = pipeline_model.transform(df)
# Select the columns of interest
prediction = prediction.select(prediction.message, prediction.prediction)

# Print prediction in console
prediction \
    .writeStream \
    .format("console") \
    .outputMode("update") \
    .start() \
    .awaitTermination()


if __name__ == "__main__":
    findspark.init()

    # Path to the pre-trained model
    path_to_model = r''

    # Config
    spark = SparkSession \
        .builder \
        .master("local[*]") \
        .appName("TwiterPharmaBrandsSentimentAnalysis") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2") \
        .getOrCreate()