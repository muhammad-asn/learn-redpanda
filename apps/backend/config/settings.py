from typing import List

class KafkaSettings:
    BOOTSTRAP_SERVERS: List[str] = ['localhost:9094']
    TOPIC_NAME: str = 'messages'
    TIMEOUT_MS: int = 1000
    PRODUCER_TIMEOUT: int = 10
    NUM_PARTITIONS: int = 1  # Configure number of partitions

class ServerSettings:
    HOST: str = "0.0.0.0"
    PORT: int = 8000
