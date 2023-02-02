from kafka import KafkaConsumer
from json import loads
import json
import boto3

batch_consumer = KafkaConsumer(
    'PinterestPipelineTopic',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda message: loads(message), #.decode('utf-8')
    # auto_offset_reset="earliest" # This value ensures the messages are read from the beginning 
)

s3_client = boto3.client('s3')

def consume_batch_messages(batch_size=300):
    batch_messages = []
    batch = 0
    print('Batch 0:')
    #for loop for each message in consumer
    for message in batch_consumer:
        # add message to the batch
        batch_messages.append(message.value)

        # print debug of how many messages are being consumed
        if len(batch_messages) % 10 == 0:
            print(f'{len(batch_messages)} messages in the batch')           
        
        # print(type(message))
        # print(message.value)
        # print(message.topic)
        # print(message.timestamp)
        
        # if batch is big enough send to s3 bucket
        if len(batch_messages) >= batch_size:
            print(f'Sending Batch_{batch} data to s3 bucket')
            
            s3_client.put_object(
                Body=json.dumps(batch_messages),
                Bucket='pinterest-data-b8070821-f9b8-4a20-872f-212f94d56365',
                Key=f'PinterestData_{batch}.json'
            )
            print('Upload complete')
            batch += 1
            batch_messages = []
            print(f'Batch {batch}:')

if __name__ == '__main__':
    print('Running Batch Consumer')
    consume_batch_messages(batch_size=100)