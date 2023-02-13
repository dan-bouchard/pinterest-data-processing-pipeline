import os
from kafka import KafkaConsumer
from json import loads
from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.streaming import StreamingContext
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, IntegerType, ArrayType, BooleanType, ShortType, ByteType

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
    stream_df = stream_df.selectExpr('timestamp', 'CAST(value as STRING)')
    # stream_df = stream_df.select(F.col('timestamp'),F.col('value'))
    # express the value column as a structure
    stream_df = stream_df.withColumn('value',F.from_json(F.col('value'),schema))
    # expanded the structure into the separate columns
    exploded_df = stream_df.select(F.col('timestamp'), F.col('value.*'))

    cleaned_df = clean_dataframe(exploded_df)

    windowed_counts = (cleaned_df
        # .withWatermark("timestamp", "10 minutes")
        .groupBy(F.window(F.col('timestamp'), '5 minutes'))
        .agg({'follower_count': 'avg', 'is_image': 'avg', 'index': 'count'})
        .withColumnRenamed('avg(follower_count)', 'avg_follower_count')
        .withColumnRenamed('avg(is_image)', 'percentage_of_images')
        .withColumnRenamed('count(index)', 'total_items')
        .withColumn('avg_follower_count', F.round(F.col('avg_follower_count'), 0))
        .withColumn('percentage_of_images', F.round(F.col('percentage_of_images') * 100.0, 2) )
        .orderBy(F.col('window.start'))
    )
    #outputting the messages to the console 
    (windowed_counts
        .writeStream
        .format('console')
        .outputMode('complete')
        .option('truncate', 'false')
        .start()
        .awaitTermination()
    )

    # (cleaned_df
    #     .writeStream
    #     .format('console')
    #     .outputMode('append')
    #     .start()
    #     .awaitTermination()
    # )

def clean_dataframe(df):
    df1 = (df
        .replace('multi-video(story page format)', 'video', 'is_image_or_video') # changes all fields to image/video
        .replace('No Title Data Available', None, 'title') # removes null fields
        .replace('No description available Story format', None, 'description') # removes null fields
        .replace('N,o, ,T,a,g,s, ,A,v,a,i,l,a,b,l,e', None, 'tag_list') # removes null fields
        .replace('Image src error.', None, 'image_src') # removes null fields
        .replace('User Info Error', None, 'follower_count') # removes null fields
        .withColumn('downloaded', df.downloaded.cast(BooleanType())) # casts downloaded to a boolean
        .withColumn('index', df.index.cast(ShortType())) # casts index to an int
        .withColumn('tag_list', F.split(F.col('tag_list'), ',')) # splits up tag_list into an array
        .withColumn('follower_count', F.regexp_replace('follower_count', 'k', '000')) # replace 1000's suffix
        .withColumn('follower_count', F.regexp_replace('follower_count', 'M', '000000')) # replace 1,000,000's suffix
    )
    
    df2 = (df1
            .withColumn('follower_count', df1.follower_count.cast(IntegerType())) # casts follower_count to an int
            .withColumn('is_image', df1.is_image_or_video.contains('image').cast(ByteType()))
        )
    return df2

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

