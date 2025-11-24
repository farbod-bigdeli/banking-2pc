FROM python:3.11-slim

WORKDIR /app

# Copy proto file and generate gRPC code
COPY banking.proto .
RUN pip install grpcio-tools
RUN python -m grpc_tools.protoc --proto_path=. --python_out=. --grpc_python_out=. banking.proto

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 5001

CMD ["python", "-u", "app.py"]

