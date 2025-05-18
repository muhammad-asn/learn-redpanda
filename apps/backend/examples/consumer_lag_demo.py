from kafka import KafkaProducer, KafkaConsumer, TopicPartition
import json
import time
import threading
from datetime import datetime

# Configuration
BOOTSTRAP_SERVERS = 'localhost:9094'
TOPIC_NAME = 'lag-demo-topic'
PARTITION = 0

def create_producer():
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )

def create_consumer(group_id):
    return KafkaConsumer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id=group_id,
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        enable_auto_commit=True,
        auto_commit_interval_ms=1000
    )

def fast_producer():
    producer = create_producer()
    message_count = 0
    
    while True:
        message = {
            'timestamp': datetime.now().isoformat(),
            'message_id': message_count
        }
        producer.send(TOPIC_NAME, value=message)
        message_count += 1
        
        # Produce 100 messages per second
        time.sleep(0.01)

def slow_consumer():
    consumer = create_consumer('slow-consumer-group')
    consumer.subscribe([TOPIC_NAME])
    
    for message in consumer:
        # Simulate slow processing (process 1 message per second)
        print(f"Processing message: {message.value}")
        time.sleep(0.01)

def monitor_lag():
    consumer = create_consumer('monitor-group')
    tp = TopicPartition(TOPIC_NAME, PARTITION)
    
    while True:
        # Get the last committed offset
        consumer.assign([tp])
        committed = consumer.committed(tp)
        
        # Get the end offset
        end_offset = consumer.end_offsets([tp])[tp]
        
        # Calculate lag
        if committed is not None:
            lag = end_offset - committed
            print(f"Consumer Lag: {lag} messages")
        else:
            print("No committed offset yet")
        
        time.sleep(5)

if __name__ == "__main__":
    # Start producer in a separate thread
    producer_thread = threading.Thread(target=fast_producer)
    producer_thread.daemon = True
    producer_thread.start()
    
    # Start consumer in a separate thread
    consumer_thread = threading.Thread(target=slow_consumer)
    consumer_thread.daemon = True
    consumer_thread.start()
    
    # Start monitoring in the main thread
    monitor_lag()