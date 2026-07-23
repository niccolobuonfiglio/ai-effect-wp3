FROM python:3.11-slim

WORKDIR /app

# Copy requirements and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy orchestrator source code
COPY src/ ./src/

# Copy tests 
COPY tests/ ./tests/

WORKDIR /app/src

EXPOSE 8000

CMD ["python", "main.py"]
