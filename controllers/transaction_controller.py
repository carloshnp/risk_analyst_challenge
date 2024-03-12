from fastapi import APIRouter, HTTPException, Depends
from services.transaction_service import TransactionService
from repositories.transaction_repository import TransactionRepository
from models.transaction_schema import Transaction

router = APIRouter(prefix="/api/transactions", tags=["transactions"])  # Create APIRouter

transaction_repository = TransactionRepository()
transaction_service = TransactionService(transaction_repository)

@router.post("/analyze_transaction/")
async def analyze(transaction: Transaction, service: TransactionService = Depends(lambda: transaction_service)):
  try:
    result = await service.analyze_transaction(transaction) 
    return result
  except Exception as e:
    raise HTTPException(status_code=500, detail="Error analyzing transaction")
