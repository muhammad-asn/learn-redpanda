from kafka import KafkaConsumer, TopicPartition
import json
import time
import threading
from datetime import datetime

# Configuration
BOOTSTRAP_SERVERS = 'localhost:9094'
TOPIC_NAME = 'lag-demo-topic'
PARTITION = 0

def create_consumer(group_id):
    return KafkaConsumer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id=group_id,
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        enable_auto_commit=False,
        fetch_max_bytes=52428800,  # 50MB
        max_poll_records=500,
        # Add timeout configurations
        session_timeout_ms=10000,
        request_timeout_ms=15000,
        # Add auto offset reset configuration
        auto_offset_reset='earliest'
    )

def catch_up_consumer():
    consumer = create_consumer('catchup-consumer-group')
    tp = TopicPartition(TOPIC_NAME, PARTITION)
    
    try:
        # Assign and seek to beginning if needed
        consumer.assign([tp])
        
        # Get the beginning offset
        beginning_offset = consumer.beginning_offsets([tp])[tp]
        consumer.seek(tp, beginning_offset)
        
        max_retries = 3
        retry_count = 0
        
        while True:
            try:
                messages = consumer.poll(timeout_ms=1000, max_records=500)
                
                if not messages:
                    retry_count += 1
                    if retry_count >= max_retries:
                        # If no messages after max retries, check if we're caught up
                        current_offset = consumer.position(tp)
                        end_offset = consumer.end_offsets([tp])[tp]
                        
                        if current_offset >= end_offset:
                            break
                        retry_count = 0
                    continue
                
                retry_count = 0  # Reset retry count when messages are received
                
                # Process messages in batch
                for partition_data in messages.values():
                    for record in partition_data:
                        # Process the message (implement your processing logic here)
                        pass
                
                # Commit offsets after processing the batch
                consumer.commit()
                
            except Exception as e:
                consumer.close()
                raise Exception(f"Error processing messages: {str(e)}")
            
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()

def monitor_lag():
    consumer = create_consumer('monitor-catchup-group')
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
    # Start catch-up consumer in a separate thread
    consumer_thread = threading.Thread(target=catch_up_consumer)
    consumer_thread.daemon = True
    consumer_thread.start()
    
    # Start monitoring in the main thread
    monitor_lag()
