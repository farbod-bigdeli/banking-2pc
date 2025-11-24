import grpc
from concurrent import futures
import banking_pb2
import banking_pb2_grpc
import threading
import time
from datetime import datetime
import uuid
import account_2pc_pb2_grpc
import account_2pc_pb2


# In-memory storage for demo (in production, this would connect to database service)
accounts_db = {}
account_counter = 1

class AccountService(banking_pb2_grpc.AccountServiceServicer):

    def make_participant_stubs(self):
        stubs = []
        for host, port in [("account-service-1", 5004),
                        ("account-service-2", 5005)]:
            channel = grpc.insecure_channel(f"{host}:{port}")
            stub = account_2pc_pb2_grpc.TwoPCParticipantStub(channel)
            stubs.append(stub)
        return stubs

    
    def CreateAccount(self, request, context):
        # 1. New transaction id
        tx_id = str(uuid.uuid4())
        coordinator_id = 1

        print(f"Phase voting of Node {coordinator_id} starts 2PC tx {tx_id} "
              f"for email={request.email}")

        # 2. VOTING PHASE: send PrepareCreateAccount to all participants
        participants = self.make_participant_stubs()
        votes = []
        for idx, stub in enumerate(participants):
            node_id = f"participant-{idx+1}"
            print(f"Phase voting of Node {coordinator_id} sends RPC PrepareCreateAccount "
                  f"to Phase voting of Node {node_id}")

            try:
                resp = stub.PrepareCreateAccount(
                    account_2pc_pb2.PrepareCreateAccountRequest(
                        transaction_id=tx_id,
                        name=request.name,
                        email=request.email,
                        initial_balance=request.initial_balance
                    ),
                    timeout=2.0
                )
                votes.append((node_id, resp.vote_commit, resp.reason))
            except grpc.RpcError as e:
                # treat RPC failure as vote_abort
                votes.append((node_id, False, f"RPC error: {e}"))

        # 3. Decide commit or abort
        all_commit = all(vote for (_, vote, _) in votes)

        if not all_commit:
            print(f"Coordinator {coordinator_id}: decision = ABORT for tx {tx_id}")
            # DECISION PHASE: AbortCreateAccount
            for idx, stub in enumerate(participants):
                node_id = f"participant-{idx+1}"
                print(f"Phase decision of Node {coordinator_id} sends RPC AbortCreateAccount "
                      f"to Phase decision of Node {node_id}")
                try:
                    stub.AbortCreateAccount(
                        account_2pc_pb2.AbortCreateAccountRequest(transaction_id=tx_id),
                        timeout=2.0
                    )
                except grpc.RpcError:
                    pass  # best-effort abort
            return banking_pb2.CreateAccountResponse(
                success=False,
                message="2PC abort: " + "; ".join(
                    f"{nid} -> {reason}" for (nid, vote, reason) in votes if not vote
                )
            )

        # If everyone voted commit → send CommitCreateAccount
        print(f"Coordinator {coordinator_id}: decision = COMMIT for tx {tx_id}")

        committed_account = None

        for idx, stub in enumerate(participants):
            node_id = f"participant-{idx+1}"
            print(f"Phase decision of Node {coordinator_id} sends RPC CommitCreateAccount "
                  f"to Phase decision of Node {node_id}")
            try:
                stub.CommitCreateAccount(
                    account_2pc_pb2.CommitCreateAccountRequest(transaction_id=tx_id),
                    timeout=2.0
                )
            except grpc.RpcError:
                # in real world you'd need recovery; for assignment we can ignore or log
                print(f"CommitCreateAccount RPC to {node_id} failed")

        # For the response we can either:
        # - Ask one participant for the committed account info, or
        # - Locally reconstruct it (since we know the input and transaction_id).
        # Here we'll just echo back the original request as "committed".
        return banking_pb2.CreateAccountResponse(
            account_id="(coordinator-does-not-store-id-here)",
            name=request.name,
            email=request.email,
            balance=request.initial_balance,
            created_at=datetime.now().isoformat(),
            success=True,
            message="Account created via 2PC successfully"
        )
    
    def GetAccount(self, request, context):
        if request.account_id not in accounts_db:
            return banking_pb2.GetAccountResponse(
                success=False,
                message="Account not found"
            )
        
        account = accounts_db[request.account_id]
        return banking_pb2.GetAccountResponse(
            account_id=account["account_id"],
            name=account["name"],
            email=account["email"],
            balance=account["balance"],
            created_at=account["created_at"],
            success=True,
            message="Account retrieved successfully"
        )
    
    def ListAccounts(self, request, context):
        accounts = []
        for account in accounts_db.values():
            accounts.append(banking_pb2.Account(
                account_id=account["account_id"],
                name=account["name"],
                email=account["email"],
                balance=account["balance"],
                created_at=account["created_at"]
            ))
        
        return banking_pb2.ListAccountsResponse(
            accounts=accounts,
            success=True,
            message="Accounts retrieved successfully"
        )
    
    def UpdateAccount(self, request, context):
        if request.account_id not in accounts_db:
            return banking_pb2.UpdateAccountResponse(
                success=False,
                message="Account not found"
            )
        
        # Update account information
        if request.name:
            accounts_db[request.account_id]["name"] = request.name
        if request.email:
            accounts_db[request.account_id]["email"] = request.email
        if request.balance is not None:
            accounts_db[request.account_id]["balance"] = request.balance
        
        return banking_pb2.UpdateAccountResponse(
            success=True,
            message="Account updated successfully"
        )

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    banking_pb2_grpc.add_AccountServiceServicer_to_server(AccountService(), server)
    server.add_insecure_port('[::]:5001')
    server.start()
    print("Account Service started on port 5001")
    
    try:
        while True:
            time.sleep(86400)
    except KeyboardInterrupt:
        server.stop(0)

if __name__ == "__main__":
    serve()

