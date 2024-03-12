from fastapi import FastAPI, HTTPException, Depends
from services.transaction_service import TransactionService
from repositories.transaction_repository import TransactionRepository
from models.transaction_schema import Transaction

app = FastAPI()

transaction_repository = TransactionRepository()
transaction_service = TransactionService(transaction_repository)

@app.post("/analyze_transaction/")
async def analyze(transaction: Transaction, service: TransactionService = Depends(lambda: transaction_service)):
    try:
        result = await service.analyze_transaction(transaction)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail="Error analyzing transaction")

@app.get("/")
async def root():
    return {"message": "Hello World"}
