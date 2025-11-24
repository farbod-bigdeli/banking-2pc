import os
import account_2pc_pb2
import account_2pc_pb2_grpc
from datetime import datetime
import grpc
from concurrent import futures
import time
from datetime import datetime

accounts_db = {}        # account_id -> account dict
pending_accounts = {}   # transaction_id -> account dict
account_counter = 1

class TwoPCParticipantServicer(account_2pc_pb2_grpc.TwoPCParticipantServicer):
    def __init__(self, node_id):
        self.node_id = node_id

    def PrepareCreateAccount(self, request, context):
        global account_counter, accounts_db, pending_accounts

        accounts_db["test"] = {
            "account_id": "test",
            "name": "test",
            "email": "amin@mail.com",
            "balance": "123",
            "created_at": datetime.now().isoformat()
        }

        tx_id = request.transaction_id

        print(f"Phase voting of Node {self.node_id} receives RPC PrepareCreateAccount "
              f"for transaction {tx_id}")

        # 1. Check if this transaction is already prepared
        if tx_id in pending_accounts:
            # Already prepared → idempotent OK
            return account_2pc_pb2.PrepareCreateAccountResponse(
                vote_commit=True,
                reason="Already prepared"
            )

        # 2. Check if email already exists in committed accounts
        for acc in accounts_db.values():
            if acc["email"] == request.email:
                print(f"Email {acc['email']} Already exist on node {self.node_id} for transaction {tx_id}")
                return account_2pc_pb2.PrepareCreateAccountResponse(
                    vote_commit=False,
                    reason="Email already exists (committed)"
                )

        # 3. Check if email already exists in pending accounts (other tx)
        for pending in pending_accounts.values():
            if pending["email"] == request.email:
                return account_2pc_pb2.PrepareCreateAccountResponse(
                    vote_commit=False,
                    reason="Email already pending in another transaction"
                )

        # 4. Reserve an account_id but don't commit yet
        account_id = str(account_counter)
        account_counter += 1

        pending_accounts[tx_id] = {
            "account_id": account_id,
            "name": request.name,
            "email": request.email,
            "balance": request.initial_balance,
            "created_at": datetime.now().isoformat()
        }

        print(f"Phase voting of Node {self.node_id} votes COMMIT for tx {tx_id}, "
              f"account_id={account_id}")

        return account_2pc_pb2.PrepareCreateAccountResponse(
            vote_commit=True,
            reason="Prepared OK"
        )

    def CommitCreateAccount(self, request, context):
        global accounts_db, pending_accounts

        tx_id = request.transaction_id

        print(f"Phase decision of Node {self.node_id} receives RPC CommitCreateAccount "
              f"for transaction {tx_id}")

        account = pending_accounts.pop(tx_id, None)
        if account is not None:
            # Move from pending → committed
            accounts_db[account["account_id"]] = account
            print(f"Node {self.node_id} COMMITTED tx {tx_id}, account_id={account['account_id']}")
        else:
            print(f"Node {self.node_id} had nothing to commit for tx {tx_id} (maybe already done)")

        return account_2pc_pb2.Empty()

    def AbortCreateAccount(self, request, context):
        global pending_accounts

        tx_id = request.transaction_id

        print(f"Phase decision of Node {self.node_id} receives RPC AbortCreateAccount "
              f"for transaction {tx_id}")

        if tx_id in pending_accounts:
            pending_accounts.pop(tx_id)
            print(f"Node {self.node_id} ABORTED tx {tx_id}")
        else:
            print(f"Node {self.node_id} had nothing to abort for tx {tx_id}")

        return account_2pc_pb2.Empty()



def serve():
    port = os.getenv("PORT", "default-node")
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    account_2pc_pb2_grpc.add_TwoPCParticipantServicer_to_server(TwoPCParticipantServicer(os.getenv("NODE_ID", "default-node")), server)
    server.add_insecure_port(f"[::]:{port}")
    server.start()
    print(f"Account Service started on port {port}")
    
    try:
        while True:
            time.sleep(86400)
    except KeyboardInterrupt:
        server.stop(0)

if __name__ == "__main__":
    print("test")
    serve()