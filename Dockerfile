FROM python:3.11-slim

WORKDIR /app

# Install dependencies first for better caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy app files
COPY . .

# Make startup script executable
RUN chmod +x start.sh

# Railway injects PORT at runtime - start.sh reads it
CMD ["./start.sh"]
