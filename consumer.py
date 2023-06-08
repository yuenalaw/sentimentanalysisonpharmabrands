import re 
import findspark
from pyspark.ml import PipelineModel
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, udf
from pyspark.sql.types import StructType, StructField, StringType, ArrayType
from datetime import datetime, timedelta

three_years_ago = datetime.now() - timedelta(days=365 * 3)
starting_timestamp = int(three_years_ago.timestamp() * 1000)

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
        .selectExpr("CAST(value AS STRING) as message", "CAST(topic AS STRING) as topic") #keep track of message AND topic

    df = df.withColumn("value", from_json("message", schema)) #new value column with json format of message

    # Pre-processing the data, udf = user defined function
    pre_process = udf(
        lambda x: re.sub(r'[^A-Za-z\n ]|(http\S+)|(www.\S+)', '', x.lower().strip()).split(), ArrayType(StringType())
    )

    df = df.withColumn("cleaned_data", pre_process(df.message)).dropna()

    # Load the pre-trained model
    pipeline_model = PipelineModel.load(path_to_model)
    # Make predictions, resulting df will contain additional columns like "prediction"
    prediction = pipeline_model.transform(df)
    # Select the columns of interest - original message, and also the prediction
    prediction = prediction.select(prediction.topic, prediction.message, prediction.prediction)

    # Print prediction in console
    prediction \
        .writeStream \
        .format("console") \
        .outputMode("update") \
        .start() \
        .awaitTermination()