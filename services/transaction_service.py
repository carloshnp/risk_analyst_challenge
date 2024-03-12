from repositories.transaction_repository import TransactionRepository
from models.transaction_schema import Transaction
import pandas as pd
import datetime

class TransactionService:
    def __init__(self, transaction_repository: TransactionRepository):
        self.transaction_repository = transaction_repository
        
    async def analyze_transaction(self, transaction: Transaction):
        transaction_result = {"recommendation": "Approve"}
        
        ### SERVICES
        ## Checks if transaction is already in the database
        transaction_id = transaction.transaction_id
        has_transaction = await TransactionRepository().get_transaction(transaction_id)
        print("transaction:", has_transaction)
        if has_transaction:
            return {"error": "Transaction already in database"}
        
        # Checks if transaction has chargeback
        if transaction.has_cbk:
            transaction_result["recommendation"] = "Deny"
            transaction_result["reason"] = "Transaction flagged as fraudulent (has chargeback)"
        else:
            user_id = transaction.user_id
            chargeback_history = await TransactionRepository().get_chargeback_by_user(user_id)
            if chargeback_history:
                transaction_result["recommendation"] = "Deny"
                transaction_result["reason"] = "Previous chargeback history"
        
        ## Too many transactions in a row
        # Fetch recent transactions for the user from the database
        user_id = transaction.user_id
        recent_transactions = await TransactionRepository().get_transactions_by_user(user_id) 
        
        # If recent transactions exist, perform analysis
        if recent_transactions:
            df = pd.DataFrame(recent_transactions)
            df['transaction_date'] = pd.to_datetime(df['transaction_date'])

            df_sorted = df.sort_values(by=['user_id', 'transaction_date'], ascending=False)

            df_sorted['time_diff'] = df_sorted.groupby('user_id')['transaction_date'].diff()
            transactions_within_2min = df_sorted[df_sorted['time_diff'] < pd.Timedelta(minutes=2)]

            # If there are transactions within 2 minutes, trigger the rule
            if not transactions_within_2min.empty:
                transaction_result["recommendation"] = "Deny"
                transaction_result["reason"] = "Too many transactions in a short period"
                
        ## Checks for a transaction amount threshold
        AMOUNT_THRESHOLD_PER_DAY = 4000
        # Fetch recent transactions for the user from the database
        user_id = transaction.user_id
        recent_transactions = await TransactionRepository().get_transactions_by_user(user_id) 
        # If recent transactions exist, perform analysis
        if recent_transactions:
            df = pd.DataFrame(recent_transactions) 
            df['transaction_date'] = pd.to_datetime(df['transaction_date'])

            # Sort before subsequent operations (Corrected)
            df = df.sort_values(by=['transaction_date'], ascending=False)  

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
            
        ## Card Testing (small time and amount differences from transactions)
        recent_transactions = await TransactionRepository().get_transactions_by_user(user_id)

        if recent_transactions:
            df = pd.DataFrame(recent_transactions)
            df['transaction_date'] = pd.to_datetime(df['transaction_date'])

            # Sort before subsequent operations (Corrected)
            df = df.sort_values(by=['user_id', 'transaction_date', 'transaction_amount']) 

            # Calculates the time and amount differences between transactions
            df_sorted = df # No need for a new variable, the data is already sorted
            df_sorted['time_diff'] = df_sorted.groupby('user_id')['transaction_date'].diff()
            df_sorted['amount_diff'] = df_sorted.groupby('user_id')['transaction_amount'].diff()

            transactions_within_5min_diff_10 = df_sorted[
            (df_sorted['time_diff'] <= pd.Timedelta(minutes=10)) &
            (df_sorted['amount_diff'].abs() < 10)
            ]

            if not transactions_within_5min_diff_10.empty:
                transaction_result["recommendation"] = "Deny"
                transaction_result["reason"] = "Potential card testing (small time and amount differences)"

        await TransactionRepository().insert_transaction(transaction, transaction_result)
        
        return transaction, transaction_result
