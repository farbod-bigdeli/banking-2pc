import grpc
from concurrent import futures
import banking_pb2
import banking_pb2_grpc
import threading
import time
from datetime import datetime
import uuid

# In-memory storage for demo (in production, this would connect to database service)
transactions_db = {}
transaction_counter = 1

# Shared account database - in a real system, this would be a shared database
# For demo purposes, we'll use a simple approach

accounts_db = {}

# Account service connection
ACCOUNT_SERVICE_URL = "account-service:5001"

def get_account_stub():
    channel = grpc.insecure_channel(ACCOUNT_SERVICE_URL)
    return banking_pb2_grpc.AccountServiceStub(channel)

def sync_accounts_from_service():
    """Sync account data from the account service"""
    try:
        account_stub = get_account_stub()
        request = banking_pb2.ListAccountsRequest()
        response = account_stub.ListAccounts(request)
        
        if response.success:
            for account in response.accounts:
                accounts_db[account.account_id] = {
                    "account_id": account.account_id,
                    "name": account.name,
                    "email": account.email,
                    "balance": account.balance,
                    "created_at": account.created_at
                }
            return True
    except Exception as e:
        print(f"Failed to sync accounts: {e}")
    return False

class TransactionService(banking_pb2_grpc.TransactionServiceServicer):
    
    def TransferMoney(self, request, context):
        global transaction_counter
        
        # Sync accounts from account service first
        sync_accounts_from_service()
        
        # Check if both accounts exist
        if request.from_account_id not in accounts_db:
            return banking_pb2.TransferResponse(
                success=False,
                message="Source account not found"
            )
        
        if request.to_account_id not in accounts_db:
            return banking_pb2.TransferResponse(
                success=False,
                message="Destination account not found"
            )
        
        # Check sufficient funds
        if accounts_db[request.from_account_id]["balance"] < request.amount:
            return banking_pb2.TransferResponse(
                success=False,
                message="Insufficient funds"
            )
        
        # Perform transfer
        accounts_db[request.from_account_id]["balance"] -= request.amount
        accounts_db[request.to_account_id]["balance"] += request.amount
        
        # Record transactions
        transaction_id = str(transaction_counter)
        transaction_counter += 1
        
        timestamp = datetime.now().isoformat()
        
        # Debit transaction
        debit_transaction = {
            "transaction_id": transaction_id,
            "account_id": request.from_account_id,
            "type": "debit",
            "amount": request.amount,
            "description": f"Transfer to {request.to_account_id}",
            "timestamp": timestamp
        }
        
        # Credit transaction
        credit_transaction = {
            "transaction_id": str(transaction_counter),
            "account_id": request.to_account_id,
            "type": "credit",
            "amount": request.amount,
            "description": f"Transfer from {request.from_account_id}",
            "timestamp": timestamp
        }
        
        transaction_counter += 1
        
        transactions_db[transaction_id] = debit_transaction
        transactions_db[str(transaction_counter - 1)] = credit_transaction
        
        return banking_pb2.TransferResponse(
            success=True,
            message="Transfer successful",
            transaction_id=transaction_id,
            from_balance=accounts_db[request.from_account_id]["balance"],
            to_balance=accounts_db[request.to_account_id]["balance"]
        )
    
    def DepositMoney(self, request, context):
        global transaction_counter
        
        # Sync accounts from account service first
        sync_accounts_from_service()
        
        if request.account_id not in accounts_db:
            return banking_pb2.DepositResponse(
                success=False,
                message="Account not found"
            )
        
        # Update balance
        accounts_db[request.account_id]["balance"] += request.amount
        
        # Record transaction
        transaction_id = str(transaction_counter)
        transaction_counter += 1
        
        new_transaction = {
            "transaction_id": transaction_id,
            "account_id": request.account_id,
            "type": "credit",
            "amount": request.amount,
            "description": request.description or "Deposit",
            "timestamp": datetime.now().isoformat()
        }
        
        transactions_db[transaction_id] = new_transaction
        
        return banking_pb2.DepositResponse(
            success=True,
            message="Deposit successful",
            transaction_id=transaction_id,
            new_balance=accounts_db[request.account_id]["balance"]
        )
    
    def WithdrawMoney(self, request, context):
        global transaction_counter
        
        # Sync accounts from account service first
        sync_accounts_from_service()
        
        if request.account_id not in accounts_db:
            return banking_pb2.WithdrawResponse(
                success=False,
                message="Account not found"
            )
        
        # Check sufficient funds
        if accounts_db[request.account_id]["balance"] < request.amount:
            return banking_pb2.WithdrawResponse(
                success=False,
                message="Insufficient funds"
            )
        
        # Update balance
        accounts_db[request.account_id]["balance"] -= request.amount
        
        # Record transaction
        transaction_id = str(transaction_counter)
        transaction_counter += 1
        
        new_transaction = {
            "transaction_id": transaction_id,
            "account_id": request.account_id,
            "type": "debit",
            "amount": request.amount,
            "description": request.description or "Withdrawal",
            "timestamp": datetime.now().isoformat()
        }
        
        transactions_db[transaction_id] = new_transaction
        
        return banking_pb2.WithdrawResponse(
            success=True,
            message="Withdrawal successful",
            transaction_id=transaction_id,
            new_balance=accounts_db[request.account_id]["balance"]
        )
    
    def GetTransactions(self, request, context):
        # Sync accounts from account service first
        sync_accounts_from_service()
        
        if request.account_id not in accounts_db:
            return banking_pb2.GetTransactionsResponse(
                success=False,
                message="Account not found"
            )
        
        # Get transactions for this account
        account_transactions = [
            transaction for transaction in transactions_db.values()
            if transaction["account_id"] == request.account_id
        ]
        
        transactions = []
        for transaction in account_transactions:
            transactions.append(banking_pb2.Transaction(
                transaction_id=transaction["transaction_id"],
                account_id=transaction["account_id"],
                type=transaction["type"],
                amount=transaction["amount"],
                description=transaction["description"],
                timestamp=transaction["timestamp"]
            ))
        
        return banking_pb2.GetTransactionsResponse(
            transactions=transactions,
            success=True,
            message="Transactions retrieved successfully"
        )

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    banking_pb2_grpc.add_TransactionServiceServicer_to_server(TransactionService(), server)
    server.add_insecure_port('[::]:5002')
    server.start()
    print("Transaction Service started on port 5002")
    
    try:
        while True:
            time.sleep(86400)
    except KeyboardInterrupt:
        server.stop(0)

if __name__ == "__main__":
    serve()

