import pandas as pd
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
    result = {"recommendation": "Approve"}
    
    # Checks if transaction is already in the database
    if await db.transactions.find_one({"transaction_id": transaction.transaction_id}):
        return {"error": "Transaction already in database"}
    
    # Checks if transaction has chargeback
    if transaction.has_cbk:
        result["recommendation"] = "Deny"
        result["reason"] = "Transaction flagged as fraudulent (has chargeback)"
    else:
        chargeback_history = await db.chargebacks.find_one({"user_id": transaction.user_id})
        if chargeback_history:
            result["recommendation"] = "Deny"
            result["reason"] = "Previous chargeback history"
    
    # Too many transactions in a row
    # Fetch recent transactions for the user from the database
    recent_transactions = await db.transactions.find({
        "user_id": transaction.user_id
        }).sort("transaction_date", -1).to_list(length=None) 
    
    # If recent transactions exist, perform analysis
    if recent_transactions:
        df = pd.DataFrame(recent_transactions) 
        df['transaction_date'] = pd.to_datetime(df['transaction_date'])
        df_sorted = df.sort_values(['user_id', 'transaction_date'])
        df_sorted['time_diff'] = df_sorted.groupby('user_id')['transaction_date'].diff()
        transactions_within_2min = df_sorted[df_sorted['time_diff'] < pd.Timedelta(minutes=2)]

        # If there are transactions within 2 minutes, trigger the rule
        if not transactions_within_2min.empty: 
            result["recommendation"] = "Deny"
            result["reason"] = "Too many transactions in a short period"
    
    # Record the transaction in the "transactions" collection
    await db.transactions.insert_one(transaction.model_dump())
    
    # If transaction was fraudulent, add it to the "chargebacks" collection
    if result["recommendation"] == "Deny":
        await db.chargebacks.insert_one(transaction.model_dump())
    
    return transaction, result

@app.post("/analyze_transaction/")
async def analyze_single(transaction: Transaction):
    return await analyze_transaction(transaction)

@app.get("/")
async def root():
    return {"message": "Hello World"}