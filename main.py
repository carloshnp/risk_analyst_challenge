from fastapi import FastAPI
from pymongo import MongoClient
from motor.motor_asyncio import AsyncIOMotorClient
from pydantic import BaseModel
from typing import Union
import datetime

app = FastAPI()
client = AsyncIOMotorClient("mongodb://db:27017/")
db = client['risk_analysis_db']

# Access your collections
chargebacks = db['chargebacks']
transactions = db['transactions']
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

async def analyze_transaction(transaction: Transaction):
    # Checks if transaction has chargeback
    print(transaction)
    if transaction.has_cbk:
        result["recommendation"] = "Deny"
        result["reason"] = "Transaction flagged as fraudulent (has chargeback)"
    else:
        chargeback_history = await db.chargebacks.find_one({"user_id": transaction.user_id})
        if chargeback_history:
            result["recommendation"] = "Deny"
            result["reason"] = "Previous chargeback history"
    
    # Record the transaction in the "transactions" collection
    await db.transactions.insert_one(transaction.model_dump())
    
    result = {"recommendation": "Approve"}
    return transaction, result

@app.post("/analyze_transaction/")
async def analyze_single(transaction: Transaction):
    return await analyze_transaction(transaction)

@app.get("/")
async def root():
    return {"message": "Hello World"}