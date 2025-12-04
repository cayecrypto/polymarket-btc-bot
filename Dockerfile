FROM python:3.11-slim

WORKDIR /app

# Install dependencies first for better caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy app files
COPY . .

# Railway sets PORT env var dynamically
ENV PORT=8080

# Expose the port
EXPOSE ${PORT}

# Use exec form with shell wrapper to properly expand PORT
CMD ["sh", "-c", "streamlit run app.py --server.port=$PORT --server.address=0.0.0.0 --server.headless=true --browser.gatherUsageStats=false"]
