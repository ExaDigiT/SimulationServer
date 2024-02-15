from fastapi import APIRouter, Depends
from ..models.base import BaseModel

router = APIRouter(prefix="/example", tags=['example'])

class ExampleMessage(BaseModel):
    message: str

@router.get("", response_model=ExampleMessage)
def index():
    return {
        "message": "Hello World"
    }
