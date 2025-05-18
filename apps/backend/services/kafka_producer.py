from kafka import KafkaProducer
import json
from config.settings import KafkaSettings
from models.message import Message

class MessageProducer:
    def __init__(self):
        self.producer = KafkaProducer(
            bootstrap_servers=KafkaSettings.BOOTSTRAP_SERVERS,
            value_serializer=lambda x: json.dumps(x).encode('utf-8')
        )
        self.topic = KafkaSettings.TOPIC_NAME
        self.num_partitions = KafkaSettings.NUM_PARTITIONS

    def send_message(self, message: Message):
        # If partition is specified and valid, use it
        partition = (
            message.partition 
            if message.partition is not None and 0 <= message.partition < self.num_partitions 
            else self._get_partition(message.message)
        )
        
        future = self.producer.send(
            topic=self.topic,
            partition=partition,
            value={"message": message.message, "partition": partition}
        )
        return future.get(timeout=KafkaSettings.PRODUCER_TIMEOUT)

    def _get_partition(self, message: str) -> int:
        # Simple partitioning strategy based on message content
        # You can implement more sophisticated strategies here
        return hash(message) % self.num_partitions 