import os
from kafka import KafkaConsumer
from json import loads
from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.streaming import StreamingContext
from pyspark.sql.functions import from_json, col, concat
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, IntegerType, ArrayType, BooleanType

# Download spark sql kakfa package from Maven repository and submit to PySpark at runtime. 
# os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.3 streaming_consumer.py pyspark-shell'
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.3 pyspark-shell'
# specify the topic we want to stream data from.
kafka_topic_name = 'PinterestPipelineTopic'
# Specify your Kafka server to read data from.
kafka_bootstrap_servers = 'localhost:9092'

# Define structure of the json file
schema = StructType([
        StructField('category', StringType()),
        StructField('index', StringType()),
        StructField('unique_id', StringType()),
        StructField('title', StringType()),
        StructField('description', StringType()),
        StructField('follower_count', StringType()),
        StructField('tag_list', StringType()),
        StructField('is_image_or_video', StringType()),
        StructField('image_src', StringType()),
        StructField('downloaded', StringType()),
        StructField('save_location', StringType())
    ])


def consume_data_to_spark_streaming():
    session = SparkSession.builder.config(
        conf=SparkConf()
        .setMaster('local[*]')
        .setAppName('KafkaStreaming')
    ).getOrCreate()

    # Only display Error messages in the console.
    session.sparkContext.setLogLevel("ERROR")

    # Construct a streaming DataFrame that reads from topic
    stream_df = (session
            .readStream
            .format('kafka')
            .option('kafka.bootstrap.servers', kafka_bootstrap_servers)
            .option('subscribe', kafka_topic_name)
            .option('startingOffsets', 'earliest')
            .load()
        )

    # # Select the value part of the kafka message and cast it to a string.
    stream_df = stream_df.selectExpr('CAST(value as STRING)')
    # express the value column as a structure
    stream_df = stream_df.withColumn('value',from_json(col('value'),schema))
    # expanded the structure into the separate columns
    exploded_df = stream_df.select(col('value.*'))
    
    # outputting the messages to the console 
    (exploded_df
        .writeStream
        .format('console')
        .outputMode('append')
        .start()
        .awaitTermination()
    )

def consume_streaming_messages():
    streaming_consumer = KafkaConsumer(
        kafka_topic_name,
        bootstrap_servers=kafka_bootstrap_servers,
        value_deserializer=lambda message: loads(message), #.decode('utf-8')
        auto_offset_reset="earliest" # This value ensures the messages are read from the beginning 
    )

    for message in streaming_consumer:
        print(message.value)

if __name__ == '__main__':
    print('Running Streaming Consumer')
    # consume_streaming_messages()
    consume_data_to_spark_streaming()

