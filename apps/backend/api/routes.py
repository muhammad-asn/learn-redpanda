from fastapi import APIRouter, HTTPException
from models.message import Message
from services.kafka_producer import MessageProducer
from services.kafka_consumer import MessageConsumer
from typing import Optional

router = APIRouter()
producer = MessageProducer()
consumer = MessageConsumer()

@router.post("/send-message")
async def send_message(message: Message):
    try:
        producer.send_message(message)
        return {"status": "Message sent successfully", "partition": message.partition}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/messages")
async def get_messages(partition: Optional[int] = None):
    try:
        return consumer.get_messages(partition)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
