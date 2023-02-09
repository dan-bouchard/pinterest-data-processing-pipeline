import os
import json
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
from pyspark.sql.types import BooleanType, ShortType, IntegerType
from pyspark.sql import functions as F

# Adding the packages required to get data from S3  
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.amazonaws:aws-java-sdk-s3:1.12.196,org.apache.hadoop:hadoop-aws:3.3.1 pyspark-shell'

def setup_spark_aws_connection():
    # Creating our Spark configuration
    cfg = SparkConf().setAppName('S3toSpark').setMaster('local[*]')

    sc=SparkContext(conf=cfg)

    # Create our Spark session
    spark=SparkSession(sc).builder.appName('S3App').getOrCreate()

    # Configure the setting to read from the S3 bucket
    with open('aws_access_key.json') as f:
        accesskeys = json.load(f)

    accessKeyId = accesskeys['accessKeyId']
    secretAccessKey = accesskeys['secretAccessKey']

    hadoopConf = sc._jsc.hadoopConfiguration()
    hadoopConf.set('fs.s3a.access.key', accessKeyId)
    hadoopConf.set('fs.s3a.secret.key', secretAccessKey)
    hadoopConf.set('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider')
    # Allows the package to authenticate with AWS

    return spark

def read_from_s3_bucket(spark_obj, filename):
    # Read from the S3 bucket
    df = spark_obj.read.json(f's3a://pinterest-data-b8070821-f9b8-4a20-872f-212f94d56365/{filename}.json')
    df.show()
    return df

def clean_dataframe_in_spark(df):
    df1 = (df
        .replace('multi-video(story page format)', 'video', 'is_image_or_video')
        .replace('No Title Data Available', None, 'title')
        .replace('No description available Story format', None, 'description')
        .replace('N,o, ,T,a,g,s, ,A,v,a,i,l,a,b,l,e', None, 'tag_list')
        .replace('Image src error.', None, 'image_src')
        .replace('User Info Error', None, 'follower_count')
        .withColumn('downloaded', df.downloaded.cast(BooleanType()))
        .withColumn('index', df.index.cast(ShortType()))
        .withColumn('tag_list', F.split(F.col('tag_list'), ','))
        .withColumn('follower_count', F.regexp_replace('follower_count', 'k', '000'))
        .withColumn('follower_count', F.regexp_replace('follower_count', 'M', '000000'))
    )

    df2 = (df1
        .withColumn('follower_count', df1.follower_count.cast(IntegerType()))
    )
    df2.show()
    return df2

if __name__ == '__main__':
    print('Setting up Spark connection')
    spark = setup_spark_aws_connection()
    print('Displaying a Spark dataframe')
    filename = 'PinterestData_0'
    df = read_from_s3_bucket(spark, filename)
    print('Displaying cleaned Spark dataframe')
    cleaned_df = clean_dataframe_in_spark(df)