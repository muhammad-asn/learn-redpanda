# Simple Event Driven Communication

This is a simple e-commerce backend that uses Redpanda as a pub/sub system for event-driven communication between services.

## Setup

1. Install dependencies:
   ```
   pip install -r requirements.txt
   ```

2. Make sure your Redpanda broker is running and accessible.

3. Edit the `KAFKA_BOOTSTRAP_SERVERS` variable in each file to match your Redpanda broker address.

## Running the Services

You can run all services together using:


## How To Test
```
# Send message without specifying partition (will use hash-based partitioning)
curl -X POST http://localhost:8000/send-message -H "Content-Type: application/json" -d '{"message": "Hello World"}'

# Send message to specific partition
curl -X POST http://localhost:8000/send-message -H "Content-Type: application/json" -d '{"message": "Hello World", "partition": 1}'

# Get all messages from all partitions
curl http://localhost:8000/messages

# Get messages from specific partition
curl http://localhost:8000/messages?partition=1
```