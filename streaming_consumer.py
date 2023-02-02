from kafka import KafkaConsumer
from json import loads

streaming_consumer = KafkaConsumer(
    'PinterestPipelineTopic',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda message: loads(message), #.decode('utf-8')
    auto_offset_reset="earliest" # This value ensures the messages are read from the beginning 
)

def consume_streaming_messages():
    for message in streaming_consumer:
        print(message.value)

if __name__ == '__main__':
    print('Running Streaming Consumer')
    consume_streaming_messages()
