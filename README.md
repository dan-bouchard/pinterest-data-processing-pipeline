# Pinterest Data Processing Pipeline
> A project to build a data processing pipeline for Pinterest data. One that is extensible, can process and store large amounts of data and the ability to compute accurate metrics using both historical and recent data. Built using kafka and Spark to build two pipelines: a batch processing pipeline and a streaming processing pipeline.

## Kafka

Once a Kafka Cluster has been started by running the Zookeeper executable:

`bin/zookeeper-server-start.sh config/zookeeper.properties` 

and a Kafka Broker has also been started:

`bin/kafka-server-start.sh config/server.properties` 

a new kafka topic can be created with the line in the terminal:

`bin/kafka-topics.sh --create --topic PinterestPipelineTopic --partitions 1 --replication-factor 1 --bootstrap-server localhost:9092`

This topic can be described with the line in the terminal:

`bin/kafka-topics.sh --describe --topic PinterestPipelineTopic --bootstrap-server localhost:9092`

### Kafka Producer and Consumer on fastAPI

The Pinterest data can be created via the fastAPI from the file `project_pin_API`. Once data is sent via the POST method /pin/ Get Db Row, the data can be received by a consumer.

A consumer can be setup with the line in the terminal:

`bin/kafka-console-consumer.sh --topic PinterestPipelineTopic --from-beginning --bootstrap-server localhost:9092`

> A user posting emulation script can be run which posts to the fastAPI /pin/ method. 

### Kafka Producer and Consumer via kafka-python

Setting up the scripts `batch_consumer` and `streaming_consumer`, the emulation script can show that the data is being produced via the API and consumed in both a batch and streaming manner at the same time.

## Batch processing

The `batch_consumer` stores a batch of data and will periodically upload it to a S3 bucket data lake for long-term persistent storage. The data is saved as JSON files. This data can be processed later down the line when retrieved from storage.

The `spark_batch_processing` sets up a SparkSession and loads the JSON file(s) from the S3 bucket into a Spark dataframe. From this, the data can be cleaned and processed accordingly, all within Spark.

This batch processing will be run once daily using an Airflow DAG (`dag_spark`) to orchestrate the job.

## Streaming Pipeline

The `streaming_consumer` has been modified to consume the Kafka messages with a Spark Streaming session, then performing some data cleaning and aggregation metrics on the streamed data. Finally, this data is saved to a local postgreSQL database for long term storage.