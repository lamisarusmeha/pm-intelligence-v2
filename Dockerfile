FROM python:3.12-slim

# Install build dependencies for pydantic/other C extensions
RUN apt-get update && apt-get install -y gcc cargo && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD cd backend && uvicorn main:app --host 0.0.0.0 --port ${PORT:-8000}
