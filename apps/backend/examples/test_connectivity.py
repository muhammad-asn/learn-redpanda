from kafka import KafkaAdminClient, KafkaProducer, KafkaConsumer
from kafka.admin import NewTopic
from kafka.errors import KafkaError
import json
import time

def test_redpanda_connectivity():
    # Broker configuration
    bootstrap_servers = ['localhost:9094']
    test_topic = 'connectivity_test_topic'

    try:
        # Test admin connection
        print("Testing admin connection...")
        admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
        
        # Create a test topic
        print("Creating test topic...")
        topic = NewTopic(name=test_topic, 
                        num_partitions=1, 
                        replication_factor=1)
        admin_client.create_topics([topic])
        
        # Test producer connection
        print("Testing producer connection...")
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        
        # Send test message
        test_message = {"message": "Hello Redpanda!"}
        future = producer.send(test_topic, test_message)
        result = future.get(timeout=10)
        print(f"Message sent successfully to partition {result.partition}")
        
        # Test consumer connection
        print("Testing consumer connection...")
        consumer = KafkaConsumer(
            test_topic,
            bootstrap_servers=bootstrap_servers,
            auto_offset_reset='earliest',
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            consumer_timeout_ms=10000  # Stop consuming after 10 seconds
        )
        
        # Read test message
        for message in consumer:
            print(f"Received message: {message.value}")
            break
            
        # Clean up
        print("Cleaning up...")
        admin_client.delete_topics([test_topic])
        
        print("All connectivity tests passed successfully!")
        return True

    except KafkaError as e:
        print(f"Failed to connect to Redpanda: {str(e)}")
        return False
    finally:
        # Close all connections
        try:
            admin_client.close()
            producer.close()
            consumer.close()
        except:
            pass

if __name__ == "__main__":
    test_redpanda_connectivity()
