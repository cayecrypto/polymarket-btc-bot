#!/bin/bash
# Railway startup script
PORT="${PORT:-8080}"
echo "Starting Streamlit on port $PORT"
exec python -m streamlit run app.py --server.port=$PORT --server.address=0.0.0.0
