import os
import json
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf

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


if __name__ == '__main__':
    print('Setting up Spark connection')
    spark = setup_spark_aws_connection()
    print('Displaying one file in a Spark dataframe')
    read_from_s3_bucket(spark, 'PinterestData_0')