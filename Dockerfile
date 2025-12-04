FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 8080

# Print startup message and run
CMD ["sh", "-c", "echo 'Container starting...' && python -c 'print(\"Testing imports...\"); import streamlit; import pandas; import web3; print(\"Imports OK\")' && echo 'Starting Streamlit on port ${PORT:-8080}' && streamlit run app.py --server.port=${PORT:-8080} --server.address=0.0.0.0 --server.headless=true --server.enableCORS=false --server.enableXsrfProtection=false"]
