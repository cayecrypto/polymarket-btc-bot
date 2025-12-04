#!/bin/bash
# Railway startup script - reads PORT env var injected by Railway
set -e
PORT="${PORT:-8501}"
echo "Starting Container"
echo "Starting Streamlit on port $PORT"
exec python -m streamlit run app.py --server.port=$PORT --server.address=0.0.0.0 --server.headless=true --browser.gatherUsageStats=false 2>&1
