#!/bin/bash
# Railway startup script - reads PORT env var injected by Railway
PORT="${PORT:-8501}"
echo "Starting Streamlit on port $PORT"
exec streamlit run app.py --server.port=$PORT --server.address=0.0.0.0 --server.headless=true --browser.gatherUsageStats=false
