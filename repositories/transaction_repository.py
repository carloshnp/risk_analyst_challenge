from motor.motor_asyncio import AsyncIOMotorClient

client = AsyncIOMotorClient("mongodb://db:27017/")

db = client['risk_analysis_db']
chargebacks = db['chargebacks']
transactions = db['transactions']

class TransactionRepository:
    async def get_transaction(self, transaction_id: int):
        result = await db.transactions.find_one({"transaction_id": transaction_id})
        return result  # Return the document directly

    async def get_transactions_by_user(self, user_id: int):
        cursor = db.transactions.find({"user_id": user_id})
        return await cursor.to_list(length=None)  # Convert cursor to a list 

    async def get_chargeback_by_user(self, user_id: int):
        result = await db.chargebacks.find_one({"user_id": user_id})
        return result  # Return the document directly

    async def insert_transaction(self, transaction, transaction_result: dict):
        if transaction_result["recommendation"] == "Deny":
            await db.chargebacks.insert_one(transaction.model_dump())
        await db.transactions.insert_one(transaction.model_dump())
        return "Transaction recorded successfully"
