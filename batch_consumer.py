from kafka import KafkaConsumer
from json import loads

batch_consumer = KafkaConsumer(
    'PinterestPipelineTopic',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda message: loads(message), #.decode('utf-8')
    auto_offset_reset="earliest" # This value ensures the messages are read from the beginning 
)

def consume_batch_messages(batch_size=300):
    batch_messages = []
    #for loop for each message in consumer
    for message in batch_consumer:
        # add message to the batch
        batch_messages.append(message.value)
        if len(batch_messages) == 1:
            print('1 message in the batch')
        else:    
            print(f'{len(batch_messages)} messages in the batch')
        
        # print(type(message))
        # print(message.value)
        # print(message.topic)
        # print(message.timestamp)
        
        # if batch is big enough break
        if len(batch_messages) > batch_size:
            print(batch_messages)
            break

if __name__ == '__main__':
    print('Running Batch Consumer')
    consume_batch_messages(batch_size=3)