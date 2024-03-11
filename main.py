from fastapi import FastAPI
from pymongo import MongoClient
from pydantic import BaseModel
import datetime

app = FastAPI()
client = MongoClient("mongodb://localhost:27017/")
db = client["mydatabase"]

# Schema
class Transaction(BaseModel):
    transaction_id: int
    merchant_id: int
    user_id: int
    card_number: str
    transaction_date: datetime.datetime
    transaction_amount: float
    device_id: int
    has_cbk: bool

@app.get("/")
async def root():
    return {"message": "Hello World"}