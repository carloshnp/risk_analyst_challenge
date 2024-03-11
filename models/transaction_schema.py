from pydantic import BaseModel
import datetime

# Transaction schema
class Transaction(BaseModel):
    transaction_id: int
    merchant_id: int
    user_id: int
    card_number: str
    transaction_date: datetime.datetime
    transaction_amount: float
    device_id: int
    has_cbk: bool