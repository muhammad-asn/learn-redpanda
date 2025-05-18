from pydantic import BaseModel
from typing import Optional

class Message(BaseModel):
    message: str
    partition: Optional[int] = None  # Optional partition specification 
