from kafka import KafkaConsumer, TopicPartition
import json
from typing import List, Dict, Any
from config.settings import KafkaSettings

class MessageConsumer:
    def __init__(self):
        self.consumer = KafkaConsumer(
            bootstrap_servers=KafkaSettings.BOOTSTRAP_SERVERS,
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        self.topic = KafkaSettings.TOPIC_NAME
        self.num_partitions = KafkaSettings.NUM_PARTITIONS

    def get_messages(self, partition: int = None) -> List[Dict[str, Any]]:
        partitions = (
            [TopicPartition(self.topic, partition)]
            if partition is not None and 0 <= partition < self.num_partitions
            else [TopicPartition(self.topic, p) for p in range(self.num_partitions)]
        )

        self.consumer.assign(partitions)
        
        # Seek to beginning for all assigned partitions
        for partition in partitions:
            self.consumer.seek_to_beginning(partition)

        messages = []
        message_pack = self.consumer.poll(timeout_ms=KafkaSettings.TIMEOUT_MS)

        for tp, msgs in message_pack.items():
            for msg in msgs:
                messages.append(msg.value)

        return messages 