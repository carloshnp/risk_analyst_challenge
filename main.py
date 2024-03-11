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
    transaction_result = {"recommendation": "Approve"}
    
    ### SERVICES
    ## Checks if transaction is already in the database
    if await db.transactions.find_one({"transaction_id": transaction.transaction_id}):
        return {"error": "Transaction already in database"}
    
    # Checks if transaction has chargeback
    if transaction.has_cbk:
        transaction_result["recommendation"] = "Deny"
        transaction_result["reason"] = "Transaction flagged as fraudulent (has chargeback)"
    else:
        chargeback_history = await db.chargebacks.find_one({"user_id": transaction.user_id})
        if chargeback_history:
            transaction_result["recommendation"] = "Deny"
            transaction_result["reason"] = "Previous chargeback history"
    
    ## Too many transactions in a row
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
            transaction_result["recommendation"] = "Deny"
            transaction_result["reason"] = "Too many transactions in a short period"
            
    ## Checks for a transaction amount threshold
    AMOUNT_THRESHOLD_PER_DAY = 4000
    # Fetch recent transactions for the user from the database
    recent_transactions = await db.transactions.find({
        "user_id": transaction.user_id
        }).sort("transaction_date", -1).to_list(length=None)
    
    # If recent transactions exist, perform analysis
    if recent_transactions:
        df = pd.DataFrame(recent_transactions) 
        df['transaction_date'] = pd.to_datetime(df['transaction_date'])

        # Calculate a week-long interval centered around the transaction date
        transaction_date = transaction.transaction_date
        week_start = transaction_date - datetime.timedelta(days=3)
        week_end = transaction_date + datetime.timedelta(days=3)

        # Filter transactions within the one-week interval
        transactions_in_window = df[
            (df['transaction_date'] >= week_start) & (df['transaction_date'] <= week_end)
        ]

        # Check for high-amount transactions within the window
        count_high_amount_transactions = (transactions_in_window['transaction_amount'] >= AMOUNT_THRESHOLD_PER_DAY).sum()

        # Adapt threshold logic based on fetched data
        if count_high_amount_transactions >= 2:
            transaction_result["recommendation"] = "Deny"
            transaction_result["reason"] = "Multiple high-amount transactions detected in a one-week window"
        else:
            # Daily limit check
            today = transaction.transaction_date.date()  
            transactions_today = df[df['transaction_date'].dt.date == today]
            total_amount_today = transactions_today['transaction_amount'].sum()

            if total_amount_today + transaction.transaction_amount >= AMOUNT_THRESHOLD_PER_DAY:
                transaction_result["recommendation"] = "Deny"
                transaction_result["reason"] = "Exceeds daily transaction limit"
                
    ## Missing Device ID
    if transaction.device_id == -1:
        transaction_result["recommendation"] = "Deny"
        transaction_result["reason"] = "Missing device ID (potential fraud)"

    
    ### REPOSITORY
    # Record the transaction in the "transactions" collection
    await db.transactions.insert_one(transaction.model_dump())
    
    # If transaction was fraudulent, add it to the "chargebacks" collection
    if transaction_result["recommendation"] == "Deny":
        await db.chargebacks.insert_one(transaction.model_dump())
    
    return transaction, transaction_result

@app.post("/analyze_transaction/")
async def analyze_single(transaction: Transaction):
    return await analyze_transaction(transaction)

@app.get("/")
async def root():
    return {"message": "Hello World"}