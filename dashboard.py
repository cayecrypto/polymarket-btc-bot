"""
================================================================================
POLYMARKET ENGINE MONITOR - READ-ONLY DASHBOARD
================================================================================

A single-screen, zero-scroll hedge fund terminal for monitoring the Polymarket
arbitrage engine. Reads from Postgres only - NO trading logic.

Data Sources:
    - engine_state table (last_tick, last_trade, etc.)
    - trade_logs table (all trade history)

gabagool style - Dec 2025 - 4X PRINTING SEASON with the bros
================================================================================
"""

import os
import json
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Any, List, Tuple

import streamlit as st
import time
import pandas as pd
import plotly.graph_objects as go
import pytz

# Database
try:
    import psycopg2
    from psycopg2 import OperationalError
    HAS_PSYCOPG2 = True
except ImportError:
    HAS_PSYCOPG2 = False

from dotenv import load_dotenv
load_dotenv()

# =============================================================================
# PAGE CONFIGURATION - FULLSCREEN GOD MODE
# =============================================================================

st.set_page_config(
    page_title="ENGINE MONITOR",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# =============================================================================
# DARK TERMINAL CSS - HEDGE FUND AESTHETIC (from app.py)
# =============================================================================

TERMINAL_CSS = """
<style>
    @import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;500;600;700;800&family=Inter:wght@400;500;600;700;800;900&display=swap');

    /* GLOBAL RESET */
    * {
        margin: 0;
        padding: 0;
        box-sizing: border-box;
    }

    .stApp {
        background: linear-gradient(180deg, #0a0f0d 0%, #0d1210 50%, #0a0e0c 100%) !important;
        color: #e0e0e0;
        font-family: 'Inter', -apple-system, sans-serif;
        min-height: 100vh;
    }

    /* HIDE STREAMLIT CHROME */
    #MainMenu, footer {visibility: hidden !important;}
    .stDeployButton, [data-testid="stToolbar"] {display: none !important;}
    .block-container {padding: 1rem !important; max-width: 100% !important;}

    /* DISABLE the dimming/loading overlay on rerun */
    [data-testid="stAppViewBlockContainer"] > div:first-child > div[data-stale="true"] {
        opacity: 1 !important;
    }
    .stSpinner, [data-testid="stStatusWidget"] {
        display: none !important;
    }
    div[data-stale="true"] {
        opacity: 1 !important;
    }

    /* Keep header minimal */
    [data-testid="stHeader"] {
        background: #0a0f0d !important;
        height: auto !important;
        min-height: 2.5rem !important;
    }

    .main .block-container {
        padding-top: 1rem !important;
        padding-bottom: 3rem !important;
    }

    /* TOP STATS BAR - THIN DARK GREEN */
    .top-bar {
        background: linear-gradient(90deg, #0d2818 0%, #0f3020 50%, #0d2818 100%);
        border-bottom: 1px solid #1a5c35;
        border-radius: 8px;
        padding: 10px 24px;
        display: flex;
        justify-content: space-between;
        align-items: center;
        margin-bottom: 16px;
        box-shadow: 0 2px 20px rgba(0, 80, 40, 0.3);
    }

    .top-bar-left {
        display: flex;
        align-items: center;
        gap: 32px;
    }

    .top-bar-title {
        font-family: 'JetBrains Mono', monospace;
        font-weight: 800;
        font-size: 14px;
        letter-spacing: 2px;
        color: #00ff6a;
        text-shadow: 0 0 20px rgba(0, 255, 106, 0.5);
    }

    .top-bar-stat {
        display: flex;
        align-items: center;
        gap: 8px;
    }

    .top-bar-stat-label {
        font-size: 10px;
        color: #5a8a6a;
        text-transform: uppercase;
        letter-spacing: 1px;
        font-weight: 600;
    }

    .top-bar-stat-value {
        font-family: 'JetBrains Mono', monospace;
        font-size: 14px;
        font-weight: 700;
        color: #00ff6a;
    }

    .top-bar-stat-value.positive {
        color: #00ff6a;
        text-shadow: 0 0 10px rgba(0, 255, 106, 0.4);
    }

    .top-bar-stat-value.neutral {
        color: #e0e0e0;
    }

    .top-bar-stat-value.warning {
        color: #ffd93d;
    }

    .top-bar-stat-value.danger {
        color: #ff4d4d;
    }

    /* MARKET CARD - COMPACT */
    .market-card {
        background: linear-gradient(145deg, #111916 0%, #0f1512 100%);
        border: 1px solid #1a3025;
        border-radius: 8px;
        padding: 12px 16px;
        transition: all 0.2s ease;
        margin-bottom: 8px;
    }

    .market-card:hover {
        border-color: #2a5035;
        box-shadow: 0 0 30px rgba(0, 255, 106, 0.1);
    }

    .market-card.live {
        border-color: #00ff6a;
        box-shadow: 0 0 20px rgba(0, 255, 106, 0.2);
    }

    .coin-badge {
        display: flex;
        align-items: center;
        gap: 10px;
    }

    .coin-symbol {
        font-family: 'JetBrains Mono', monospace;
        font-weight: 800;
        font-size: 18px;
    }

    .coin-btc { color: #f7931a; }
    .coin-eth { color: #627eea; }
    .coin-sol { color: #00ffa3; }
    .coin-xrp { color: #c0c0c0; }

    .pair-row {
        display: flex;
        justify-content: space-between;
        align-items: center;
        margin: 8px 0;
        padding: 8px 12px;
        background: rgba(0, 50, 30, 0.3);
        border-radius: 6px;
    }

    .pair-label {
        font-size: 11px;
        color: #5a8a6a;
        text-transform: uppercase;
        letter-spacing: 1px;
    }

    .pair-cost {
        font-family: 'JetBrains Mono', monospace;
        font-size: 20px;
        font-weight: 800;
    }

    .pair-cost.good { color: #00ff6a; text-shadow: 0 0 15px rgba(0, 255, 106, 0.5); }
    .pair-cost.marginal { color: #ffd93d; }
    .pair-cost.bad { color: #ff4d4d; }

    /* PANEL STYLES */
    .panel {
        background: linear-gradient(145deg, #111916 0%, #0f1512 100%);
        border: 1px solid #1a3025;
        border-radius: 8px;
        padding: 12px 16px;
    }

    .panel-header {
        display: flex;
        justify-content: space-between;
        align-items: center;
        margin-bottom: 8px;
    }

    .panel-title {
        font-family: 'JetBrains Mono', monospace;
        font-size: 11px;
        font-weight: 700;
        color: #5a8a6a;
        text-transform: uppercase;
        letter-spacing: 2px;
    }

    .panel-value {
        font-family: 'JetBrains Mono', monospace;
        font-size: 16px;
        font-weight: 800;
        color: #00ff6a;
    }

    /* STATUS BADGES */
    .status-badge {
        display: inline-block;
        padding: 4px 12px;
        border-radius: 4px;
        font-family: 'JetBrains Mono', monospace;
        font-size: 11px;
        font-weight: 700;
        text-transform: uppercase;
        letter-spacing: 1px;
    }

    .status-online {
        background: rgba(0, 255, 106, 0.15);
        border: 1px solid #00ff6a;
        color: #00ff6a;
    }

    .status-offline {
        background: rgba(255, 77, 77, 0.15);
        border: 1px solid #ff4d4d;
        color: #ff4d4d;
    }

    .status-dryrun {
        background: rgba(255, 217, 61, 0.15);
        border: 1px solid #ffd93d;
        color: #ffd93d;
    }

    .status-live {
        background: rgba(0, 255, 106, 0.15);
        border: 1px solid #00ff6a;
        color: #00ff6a;
    }

    /* TRADE TABLE */
    .trade-row {
        display: flex;
        justify-content: space-between;
        align-items: center;
        padding: 6px 0;
        border-bottom: 1px solid rgba(26, 48, 37, 0.5);
        font-size: 12px;
    }

    .trade-row:last-child {
        border-bottom: none;
    }

    .trade-time {
        font-family: 'JetBrains Mono', monospace;
        color: #5a8a6a;
        font-size: 11px;
    }

    .trade-coin {
        font-weight: 700;
        color: #e0e0e0;
    }

    .trade-profit {
        font-family: 'JetBrains Mono', monospace;
        color: #00ff6a;
        font-weight: 600;
    }

    .trade-dryrun {
        color: #ffd93d;
    }

    .trade-live {
        color: #00ff6a;
        font-weight: 700;
    }

    /* METRIC BOX */
    .metric-box {
        background: rgba(0, 50, 30, 0.3);
        border-radius: 6px;
        padding: 12px;
        text-align: center;
    }

    .metric-value {
        font-family: 'JetBrains Mono', monospace;
        font-size: 24px;
        font-weight: 800;
        color: #00ff6a;
    }

    .metric-label {
        font-size: 10px;
        color: #5a8a6a;
        text-transform: uppercase;
        letter-spacing: 1px;
        margin-top: 4px;
    }

    /* PLOTLY CHART CONTAINER */
    .stPlotlyChart {
        flex: 1;
    }

    /* RESPONSIVE */
    @media (max-width: 1200px) {
        .top-bar { padding: 8px 12px; }
        .top-bar-stat { gap: 4px; }
        .top-bar-stat-value { font-size: 12px; }
    }

    /* STREAMLIT DATAFRAME - DARK TERMINAL THEME */
    [data-testid="stDataFrame"] {
        background: transparent !important;
    }

    [data-testid="stDataFrame"] > div {
        background: linear-gradient(145deg, #111916 0%, #0f1512 100%) !important;
        border: 1px solid #1a3025 !important;
        border-radius: 8px !important;
    }

    /* DataFrame container */
    [data-testid="stDataFrame"] iframe {
        background: transparent !important;
    }

    /* Glide Data Grid - the actual table component */
    .dvn-scroller {
        background: #0d1210 !important;
    }

    /* Table header */
    [data-testid="stDataFrame"] [role="columnheader"],
    .dvn-header-cell {
        background: linear-gradient(180deg, #0d2818 0%, #0a1f14 100%) !important;
        color: #5a8a6a !important;
        font-family: 'JetBrains Mono', monospace !important;
        font-size: 11px !important;
        font-weight: 700 !important;
        text-transform: uppercase !important;
        letter-spacing: 1px !important;
        border-bottom: 1px solid #1a5c35 !important;
    }

    /* Table cells */
    [data-testid="stDataFrame"] [role="gridcell"],
    .dvn-cell {
        background: #0d1210 !important;
        color: #e0e0e0 !important;
        font-family: 'JetBrains Mono', monospace !important;
        font-size: 12px !important;
        border-bottom: 1px solid rgba(26, 48, 37, 0.3) !important;
    }

    /* Alternating row colors */
    [data-testid="stDataFrame"] [role="row"]:nth-child(even) [role="gridcell"] {
        background: rgba(13, 40, 24, 0.2) !important;
    }

    /* Hover effect on rows */
    [data-testid="stDataFrame"] [role="row"]:hover [role="gridcell"] {
        background: rgba(0, 255, 106, 0.08) !important;
    }

    /* Scrollbar styling */
    [data-testid="stDataFrame"] ::-webkit-scrollbar {
        width: 8px;
        height: 8px;
    }

    [data-testid="stDataFrame"] ::-webkit-scrollbar-track {
        background: #0a0f0d;
    }

    [data-testid="stDataFrame"] ::-webkit-scrollbar-thumb {
        background: #1a3025;
        border-radius: 4px;
    }

    [data-testid="stDataFrame"] ::-webkit-scrollbar-thumb:hover {
        background: #2a5035;
    }

    /* Remove default borders */
    [data-testid="stDataFrame"] [role="grid"] {
        border: none !important;
    }

    /* Custom HTML table styling (fallback) */
    .trades-table {
        width: 100%;
        border-collapse: collapse;
        font-family: 'JetBrains Mono', monospace;
        font-size: 12px;
    }

    .trades-table thead {
        background: linear-gradient(180deg, #0d2818 0%, #0a1f14 100%);
        position: sticky;
        top: 0;
        z-index: 10;
    }

    .trades-table th {
        color: #5a8a6a;
        font-weight: 700;
        text-transform: uppercase;
        letter-spacing: 1px;
        font-size: 10px;
        padding: 10px 12px;
        text-align: left;
        border-bottom: 1px solid #1a5c35;
    }

    .trades-table td {
        padding: 8px 12px;
        color: #e0e0e0;
        border-bottom: 1px solid rgba(26, 48, 37, 0.3);
    }

    .trades-table tbody tr:nth-child(even) {
        background: rgba(13, 40, 24, 0.2);
    }

    .trades-table tbody tr:hover {
        background: rgba(0, 255, 106, 0.08);
    }

    .trades-table .mode-dry {
        color: #ffd93d;
        font-weight: 600;
    }

    .trades-table .mode-live {
        color: #00ff6a;
        font-weight: 700;
    }

    .trades-table .status-ok {
        color: #00ff6a;
    }

    .trades-table .status-fail {
        color: #ff4d4d;
    }

    .trades-table .profit-positive {
        color: #00ff6a;
    }

    .trades-table .profit-negative {
        color: #ff4d4d;
    }

    .trades-table .pair-marginal {
        color: #ffd93d;
    }

    .trades-table .coin-btc { color: #f7931a; font-weight: 700; }
    .trades-table .coin-eth { color: #627eea; font-weight: 700; }
    .trades-table .coin-sol { color: #00ffa3; font-weight: 700; }
    .trades-table .coin-xrp { color: #c0c0c0; font-weight: 700; }

    /* Table container with scroll */
    .trades-table-container {
        background: linear-gradient(145deg, #111916 0%, #0f1512 100%);
        border: 1px solid #1a3025;
        border-radius: 8px;
        max-height: 400px;
        overflow-y: auto;
    }

    .trades-table-container::-webkit-scrollbar {
        width: 8px;
    }

    .trades-table-container::-webkit-scrollbar-track {
        background: #0a0f0d;
    }

    .trades-table-container::-webkit-scrollbar-thumb {
        background: #1a3025;
        border-radius: 4px;
    }

    .trades-table-container::-webkit-scrollbar-thumb:hover {
        background: #2a5035;
    }

    /* BUTTON STYLING - Dark Theme */
    .stButton > button, .stDownloadButton > button {
        background: linear-gradient(135deg, #0d2818 0%, #1a3025 100%) !important;
        border: 1px solid #1a5c35 !important;
        color: #00ff6a !important;
        font-family: 'JetBrains Mono', monospace !important;
        font-size: 11px !important;
        font-weight: 600 !important;
        padding: 8px 16px !important;
        border-radius: 6px !important;
        transition: all 0.2s ease !important;
    }

    .stButton > button:hover, .stDownloadButton > button:hover {
        background: linear-gradient(135deg, #1a3025 0%, #2a5035 100%) !important;
        border-color: #00ff6a !important;
        box-shadow: 0 0 15px rgba(0, 255, 106, 0.3) !important;
    }

    .stButton > button:disabled, .stDownloadButton > button:disabled {
        background: #0a1510 !important;
        border-color: #1a3025 !important;
        color: #3a5a4a !important;
        opacity: 0.6 !important;
    }

    /* EXPANDER STYLING - Dark Theme (comprehensive selectors) */
    .streamlit-expanderHeader,
    [data-testid="stExpander"] summary,
    [data-testid="stExpander"] > details > summary,
    .st-emotion-cache-1clstc5,
    div[data-testid="stExpander"] summary {
        background: linear-gradient(90deg, #0d2818 0%, #0f3020 50%, #0d2818 100%) !important;
        border: 1px solid #1a5c35 !important;
        border-radius: 8px !important;
        color: #00ff6a !important;
        font-family: 'JetBrains Mono', monospace !important;
        font-size: 12px !important;
        font-weight: 600 !important;
        letter-spacing: 1px !important;
    }

    /* Expander header text/span inside */
    [data-testid="stExpander"] summary span,
    [data-testid="stExpander"] summary p,
    .streamlit-expanderHeader span,
    .streamlit-expanderHeader p {
        color: #00ff6a !important;
    }

    /* Expander arrow/icon */
    [data-testid="stExpander"] summary svg,
    .streamlit-expanderHeader svg {
        fill: #00ff6a !important;
        color: #00ff6a !important;
    }

    .streamlit-expanderContent,
    [data-testid="stExpander"] > details > div[data-testid="stExpanderDetails"],
    [data-testid="stExpanderDetails"] {
        background: rgba(13, 40, 24, 0.3) !important;
        border: 1px solid #1a3025 !important;
        border-top: none !important;
        border-radius: 0 0 8px 8px !important;
    }

    /* st.error styling to match theme */
    .stAlert {
        background: rgba(80, 20, 20, 0.3) !important;
        border: 1px solid #5c1a1a !important;
        border-radius: 6px !important;
    }
</style>
"""

st.markdown(TERMINAL_CSS, unsafe_allow_html=True)

ET = pytz.timezone("US/Eastern")

# =============================================================================
# DATABASE CONNECTION (READ-ONLY)
# =============================================================================

_db_connection = None

def get_db_connection():
    """Get or create database connection."""
    global _db_connection

    if not HAS_PSYCOPG2:
        return None

    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        return None

    try:
        if _db_connection is None or _db_connection.closed:
            _db_connection = psycopg2.connect(database_url)
            _db_connection.autocommit = True
        return _db_connection
    except Exception as e:
        st.error(f"Database connection failed: {e}")
        return None


def db_query(query: str, params: tuple = None) -> Optional[List[Dict]]:
    """Execute a read query and return results as list of dicts."""
    conn = get_db_connection()
    if not conn:
        return None

    try:
        cur = conn.cursor()
        cur.execute(query, params or ())

        # Safely handle cursor description
        if cur.description is None:
            cur.close()
            return []

        # Safely extract column names with proper error handling
        columns = []
        for desc in cur.description:
            if desc and len(desc) > 0:
                columns.append(desc[0])
            else:
                columns.append(f"col_{len(columns)}")

        rows = cur.fetchall()
        cur.close()
        return [dict(zip(columns, row)) for row in rows]
    except Exception as e:
        st.error(f"Query failed: {e}")
        return None


# =============================================================================
# CACHED DATA FETCHERS
# =============================================================================

@st.cache_data(ttl=1)
def fetch_engine_state() -> Dict[str, Any]:
    """Fetch all engine_state entries."""
    rows = db_query("SELECT key, value, updated_at FROM engine_state")
    if not rows:
        return {}
    return {row["key"]: {"value": row["value"], "updated_at": row["updated_at"]} for row in rows}


@st.cache_data(ttl=1)
def fetch_trade_stats() -> Dict[str, Any]:
    """Fetch aggregate trade statistics."""
    stats = {
        "total_trades": 0,
        "live_trades": 0,
        "dryrun_trades": 0,
        "total_locked_profit": 0.0,
        "total_amount_usd": 0.0,
    }

    # Total counts
    result = db_query("""
        SELECT
            COUNT(*) as total,
            COUNT(*) FILTER (WHERE dry_run = FALSE) as live,
            COUNT(*) FILTER (WHERE dry_run = TRUE) as dryrun,
            COALESCE(SUM(locked_profit) FILTER (WHERE dry_run = FALSE), 0) as locked_profit,
            COALESCE(SUM(amount_usd), 0) as total_usd
        FROM trade_logs
    """)

    if result and len(result) > 0:
        row = result[0]
        stats["total_trades"] = row.get("total", 0) or 0
        stats["live_trades"] = row.get("live", 0) or 0
        stats["dryrun_trades"] = row.get("dryrun", 0) or 0
        stats["total_locked_profit"] = float(row.get("locked_profit", 0) or 0)
        stats["total_amount_usd"] = float(row.get("total_usd", 0) or 0)

    return stats


@st.cache_data(ttl=1)
def fetch_recent_trades(limit: int = 50) -> List[Dict]:
    """Fetch recent trades."""
    result = db_query("""
        SELECT
            id, timestamp, market, side, amount_usd, shares, price,
            pair_cost, locked_profit, dry_run, success, error, tx_hash
        FROM trade_logs
        ORDER BY timestamp DESC
        LIMIT %s
    """, (limit,))
    return result or []


@st.cache_data(ttl=1)
def fetch_coin_stats() -> Dict[str, Dict]:
    """Fetch per-coin statistics."""
    result = db_query("""
        SELECT
            UPPER(SUBSTRING(market FROM 1 FOR 3)) as coin,
            COUNT(*) as trade_count,
            COUNT(*) FILTER (WHERE dry_run = FALSE) as live_count,
            COALESCE(AVG(pair_cost), 0) as avg_pair_cost,
            COALESCE(AVG(pair_cost) FILTER (WHERE dry_run = FALSE), 0) as avg_pair_cost_live,
            COALESCE(SUM(locked_profit) FILTER (WHERE dry_run = FALSE), 0) as total_profit,
            MAX(timestamp) as last_trade
        FROM trade_logs
        WHERE market IS NOT NULL
        GROUP BY UPPER(SUBSTRING(market FROM 1 FOR 3))
    """)

    stats = {}
    if result:
        for row in result:
            coin = row.get("coin", "").upper()
            if coin in ["BTC", "ETH", "SOL", "XRP"]:
                stats[coin] = row

    return stats


@st.cache_data(ttl=1)
def fetch_last_trade_per_coin() -> Dict[str, Dict]:
    """Fetch the most recent trade for each coin."""
    result = db_query("""
        SELECT DISTINCT ON (UPPER(SUBSTRING(market FROM 1 FOR 3)))
            market, side, amount_usd, pair_cost, timestamp, dry_run
        FROM trade_logs
        WHERE market IS NOT NULL
        ORDER BY UPPER(SUBSTRING(market FROM 1 FOR 3)), timestamp DESC
    """)

    trades = {}
    if result:
        for row in result:
            market = row.get("market", "") or ""
            coin = market[:3].upper() if market else ""
            if coin in ["BTC", "ETH", "SOL", "XRP"]:
                trades[coin] = row

    return trades


@st.cache_data(ttl=1)
def fetch_equity_curve() -> List[Dict]:
    """Fetch live trades for equity curve."""
    result = db_query("""
        SELECT timestamp, locked_profit, amount_usd, market
        FROM trade_logs
        WHERE dry_run = FALSE AND success = TRUE
        ORDER BY timestamp ASC
    """)
    return result or []


# =============================================================================
# 12-HOUR BACKTEST WORKFLOW - SQL HELPERS
# =============================================================================

@st.cache_data(ttl=5)
def get_trades_last_12h() -> List[Dict]:
    """Fetch all trades from the last 12 hours for export."""
    result = db_query("""
        SELECT *
        FROM trade_logs
        WHERE timestamp >= NOW() - INTERVAL '12 hours'
        ORDER BY timestamp DESC
    """)
    return result or []


@st.cache_data(ttl=5)
def get_pair_cost_series() -> List[Dict]:
    """Fetch pair_cost time series data for charting."""
    # Use market column to derive coin - works with existing schema
    # Handle edge cases where market might be NULL or short
    result = db_query("""
        SELECT timestamp, pair_cost,
               CASE
                   WHEN market IS NULL OR LENGTH(market) < 3 THEN 'UNK'
                   ELSE UPPER(SUBSTRING(market FROM 1 FOR 3))
               END as coin,
               side, dry_run
        FROM trade_logs
        WHERE pair_cost IS NOT NULL AND pair_cost > 0
        ORDER BY timestamp ASC
    """)
    return result or []


@st.cache_data(ttl=5)
def get_locked_profit_series() -> List[Dict]:
    """Fetch cumulative locked profit over time for charting."""
    # Use market column to derive coin - works with existing schema
    # Handle edge cases where market might be NULL or short
    result = db_query("""
        SELECT timestamp,
               COALESCE(locked_profit, 0) as locked_profit,
               SUM(COALESCE(locked_profit, 0)) OVER (ORDER BY timestamp) as cumulative_profit,
               CASE
                   WHEN market IS NULL OR LENGTH(market) < 3 THEN 'UNK'
                   ELSE UPPER(SUBSTRING(market FROM 1 FOR 3))
               END as coin,
               dry_run
        FROM trade_logs
        ORDER BY timestamp ASC
    """)
    return result or []


@st.cache_data(ttl=5)
def get_window_summary() -> List[Dict]:
    """Fetch 15-minute window PnL summary for the last 12 hours."""
    # Use date_trunc to 15-minute windows with simpler syntax
    result = db_query("""
        SELECT
            to_timestamp(floor(extract(epoch from timestamp) / 900) * 900) as window_start,
            COUNT(*) as trade_count,
            COALESCE(SUM(amount_usd), 0) as total_volume,
            COALESCE(AVG(CASE WHEN pair_cost > 0 THEN pair_cost ELSE NULL END), 0) as avg_pair_cost,
            COALESCE(SUM(locked_profit), 0) as total_locked_profit,
            SUM(CASE WHEN dry_run = FALSE THEN 1 ELSE 0 END) as live_count,
            SUM(CASE WHEN dry_run = TRUE THEN 1 ELSE 0 END) as dry_count,
            0 as pairs_completed
        FROM trade_logs
        WHERE timestamp >= NOW() - INTERVAL '12 hours'
        GROUP BY floor(extract(epoch from timestamp) / 900)
        ORDER BY window_start DESC
    """)
    return result or []


def export_trades_to_csv(trades: List[Dict]) -> str:
    """Convert trades to CSV string for download."""
    if not trades:
        return ""
    df = pd.DataFrame(trades)
    return df.to_csv(index=False)


def clear_trade_history(hours: int = 0):
    """
    Clear trade history from database.

    Args:
        hours: 0 = truncate all, >0 = delete older than X hours
    """
    conn = get_db_connection()
    if not conn:
        return False

    try:
        cur = conn.cursor()
        if hours == 0:
            cur.execute("TRUNCATE trade_logs")
        else:
            cur.execute(
                "DELETE FROM trade_logs WHERE timestamp < NOW() - INTERVAL '%s hours'",
                (hours,)
            )
        cur.close()
        return True
    except Exception as e:
        st.error(f"Failed to clear trade history: {e}")
        return False


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def get_engine_status(engine_state: Dict) -> Tuple[str, int]:
    """Determine engine status from last_tick age."""
    last_tick = engine_state.get("last_tick", {})
    updated_at = last_tick.get("updated_at")

    if not updated_at:
        return "OFFLINE", -1

    # Handle timezone-aware datetime
    if updated_at.tzinfo is None:
        updated_at = updated_at.replace(tzinfo=timezone.utc)

    now = datetime.now(timezone.utc)
    age_seconds = (now - updated_at).total_seconds()

    if age_seconds <= 10:
        return "ONLINE", int(age_seconds)
    else:
        return "OFFLINE", int(age_seconds)


def get_trading_mode(trade_stats: Dict, engine_state: Dict) -> str:
    """Determine if we're in DRY_RUN or LIVE mode."""
    # Check recent trades
    if trade_stats.get("live_trades", 0) > 0:
        return "LIVE"

    # Check environment
    dry_run_env = os.environ.get("DRY_RUN", "true").lower()
    if dry_run_env == "false":
        return "LIVE"

    return "DRY_RUN"


def format_time_ago(dt) -> str:
    """Format datetime as relative time."""
    if not dt:
        return "N/A"

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)

    now = datetime.now(timezone.utc)
    diff = now - dt

    if diff.total_seconds() < 60:
        return f"{int(diff.total_seconds())}s ago"
    elif diff.total_seconds() < 3600:
        return f"{int(diff.total_seconds() / 60)}m ago"
    elif diff.total_seconds() < 86400:
        return f"{int(diff.total_seconds() / 3600)}h ago"
    else:
        return f"{int(diff.total_seconds() / 86400)}d ago"


def format_pair_cost(cost: float) -> Tuple[str, str]:
    """Format pair cost with color class."""
    if cost <= 0.982:
        return f"${cost:.4f}", "good"
    elif cost <= 0.995:
        return f"${cost:.4f}", "marginal"
    else:
        return f"${cost:.4f}", "bad"


# =============================================================================
# UI COMPONENTS
# =============================================================================

def render_top_bar(trade_stats: Dict, engine_state: Dict, refresh_count: int):
    """Render the top stats bar."""
    status, age = get_engine_status(engine_state)
    mode = get_trading_mode(trade_stats, engine_state)

    status_class = "positive" if status == "ONLINE" else "danger"
    mode_class = "warning" if mode == "DRY_RUN" else "positive"

    # Get last tick info
    last_tick_data = engine_state.get("last_tick", {}).get("value", {})
    if isinstance(last_tick_data, str):
        try:
            last_tick_data = json.loads(last_tick_data)
        except:
            last_tick_data = {}

    markets_found = last_tick_data.get("markets_found", 0)
    wallet_usdc = last_tick_data.get("wallet_usdc")
    wallet_str = f"${wallet_usdc:.2f}" if wallet_usdc is not None else "N/A"

    # Current time for live clock
    now = datetime.now(ET)
    time_str = now.strftime("%H:%M:%S")

    st.markdown(f"""
    <div class="top-bar">
        <div class="top-bar-left">
            <div class="top-bar-title">POLYMARKET ENGINE MONITOR</div>
            <div class="top-bar-stat">
                <span class="top-bar-stat-label">Engine</span>
                <span class="top-bar-stat-value {status_class}">{status}</span>
            </div>
            <div class="top-bar-stat">
                <span class="top-bar-stat-label">Mode</span>
                <span class="top-bar-stat-value {mode_class}">{mode}</span>
            </div>
            <div class="top-bar-stat">
                <span class="top-bar-stat-label">Wallet</span>
                <span class="top-bar-stat-value positive">{wallet_str}</span>
            </div>
            <div class="top-bar-stat">
                <span class="top-bar-stat-label">Markets</span>
                <span class="top-bar-stat-value neutral">{markets_found}</span>
            </div>
        </div>
        <div class="top-bar-left">
            <div class="top-bar-stat">
                <span class="top-bar-stat-label">Total Trades</span>
                <span class="top-bar-stat-value neutral">{trade_stats.get('total_trades', 0)}</span>
            </div>
            <div class="top-bar-stat">
                <span class="top-bar-stat-label">Live</span>
                <span class="top-bar-stat-value positive">{trade_stats.get('live_trades', 0)}</span>
            </div>
            <div class="top-bar-stat">
                <span class="top-bar-stat-label">DRY_RUN</span>
                <span class="top-bar-stat-value warning">{trade_stats.get('dryrun_trades', 0)}</span>
            </div>
            <div class="top-bar-stat">
                <span class="top-bar-stat-label">Locked Profit</span>
                <span class="top-bar-stat-value positive">${trade_stats.get('total_locked_profit', 0):.2f}</span>
            </div>
            <div class="top-bar-stat">
                <span class="top-bar-stat-label">Clock</span>
                <span class="top-bar-stat-value neutral">{time_str}</span>
            </div>
            <div class="top-bar-stat">
                <span class="top-bar-stat-label">Refresh</span>
                <span class="top-bar-stat-value neutral">#{refresh_count}</span>
            </div>
        </div>
    </div>
    """, unsafe_allow_html=True)


def render_coin_card(coin: str, stats: Dict, last_trade: Dict, live_data: Dict = None):
    """
    Render a single coin card.

    Args:
        coin: Coin symbol (BTC, ETH, SOL, XRP)
        stats: Aggregate trade stats for this coin from DB
        last_trade: Most recent trade for this coin from DB
        live_data: Live data from engine_state containing:
            - binance_prices: {coin: {price, change}}
            - latest_pairs: {coin: {pair_cost, up_price, down_price}}
    """
    coin_class = f"coin-{coin.lower()}"

    # Get stats from DB
    trade_count = stats.get("trade_count", 0) if stats else 0
    live_count = stats.get("live_count", 0) if stats else 0
    avg_pair = stats.get("avg_pair_cost", 0) if stats else 0
    total_profit = stats.get("total_profit", 0) if stats else 0

    # Get last trade info from DB
    last_side = last_trade.get("side", "N/A") if last_trade else "N/A"
    last_amount = last_trade.get("amount_usd", 0) if last_trade else 0
    last_time = last_trade.get("timestamp") if last_trade else None
    is_dryrun = last_trade.get("dry_run", True) if last_trade else True

    time_str = format_time_ago(last_time) if last_time else "No trades"
    mode_badge = "DRY" if is_dryrun else "LIVE"
    mode_class = "trade-dryrun" if is_dryrun else "trade-live"

    # Get live data from engine_state
    binance_prices = live_data.get("binance_prices", {}) if live_data else {}
    latest_pairs = live_data.get("latest_pairs", {}) if live_data else {}

    # Binance spot price
    coin_binance = binance_prices.get(coin, {})
    spot_price = coin_binance.get("price", 0)
    spot_change = coin_binance.get("change", 0)
    if spot_price > 1000:
        spot_str = f"${spot_price:,.0f}"
    elif spot_price > 1:
        spot_str = f"${spot_price:.2f}"
    else:
        spot_str = f"${spot_price:.4f}" if spot_price else "N/A"
    change_class = "positive" if spot_change >= 0 else "danger"
    change_str = f"+{spot_change:.1f}%" if spot_change >= 0 else f"{spot_change:.1f}%"

    # Live pair costs from engine
    coin_pair = latest_pairs.get(coin, {})
    mid_pair_cost = coin_pair.get("pair_cost")  # Midpoint-based (fair value)
    edge_pair_cost = coin_pair.get("edge_pair_cost")  # Ask-based (actual cost)
    up_price = coin_pair.get("up_price")
    down_price = coin_pair.get("down_price")

    # Market metadata for transparency
    market_valid = coin_pair.get("valid", True)  # Default to True for backwards compat
    condition_id = coin_pair.get("condition_id")
    seconds_remaining = coin_pair.get("seconds_remaining")
    validation_error = coin_pair.get("validation_error")

    # Format condition_id and expiry for display
    if condition_id:
        cid_str = f"cid:{condition_id}..."
    else:
        cid_str = "No market"

    if seconds_remaining is not None and seconds_remaining != 999:
        minutes = seconds_remaining // 60
        secs = seconds_remaining % 60
        if seconds_remaining <= 60:
            expiry_str = f"<span style='color: #ff4444;'>{secs}s</span>"
        elif seconds_remaining <= 300:
            expiry_str = f"<span style='color: #ffd93d;'>{minutes}m{secs:02d}s</span>"
        else:
            expiry_str = f"{minutes}m{secs:02d}s"
    else:
        expiry_str = "N/A"

    # Format mid pair cost
    if mid_pair_cost is not None:
        mid_pair_str = f"{mid_pair_cost:.4f}"
        mid_pair_class = "neutral"
    else:
        mid_pair_str = "N/A"
        mid_pair_class = "neutral"

    # Format edge pair cost with highlight for arb opportunity
    TARGET_PAIR_COST = 0.982
    if edge_pair_cost is not None:
        edge_pair_str = f"{edge_pair_cost:.4f}"
        if edge_pair_cost <= TARGET_PAIR_COST:
            edge_pair_class = "positive"
            arb_badge = '<span style="background: #00ff6a; color: #000; padding: 1px 4px; border-radius: 3px; font-size: 9px; margin-left: 4px;">ARB</span>'
        elif edge_pair_cost < 1.0:
            edge_pair_class = "warning"
            arb_badge = ""
        else:
            edge_pair_class = "neutral"
            arb_badge = ""
    else:
        edge_pair_str = "N/A"
        edge_pair_class = "neutral"
        arb_badge = ""

    # Format up/down prices
    if up_price is not None and down_price is not None:
        updown_str = f"UP: {up_price:.2f} / DN: {down_price:.2f}"
    else:
        updown_str = "Waiting..."

    card_class = "market-card live" if not is_dryrun and last_trade else "market-card"

    # Show inactive/invalid market warning
    if not market_valid or validation_error:
        market_status_html = f'<div style="text-align: center; font-size: 9px; color: #ff4444; padding: 2px; margin-bottom: 4px; background: rgba(255,68,68,0.1); border-radius: 3px;">No active 15m market detected</div>'
    else:
        market_status_html = f'<div style="display: flex; justify-content: space-between; font-size: 9px; color: #5a8a6a; margin-bottom: 4px;"><span>{cid_str}</span><span>Exp: {expiry_str}</span></div>'

    st.markdown(f"""
    <div class="{card_class}">
        <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 4px;">
            <span class="coin-symbol {coin_class}">{coin}</span>
            <span class="{mode_class}">{mode_badge}</span>
        </div>
        {market_status_html}
        <div style="display: flex; justify-content: space-between; align-items: center; font-size: 11px; color: #7a9a8a; margin-bottom: 4px;">
            <span>Spot: <strong style="color: #e0e0e0;">{spot_str}</strong></span>
            <span class="top-bar-stat-value {change_class}" style="font-size: 11px;">{change_str}</span>
        </div>
        <div style="display: flex; justify-content: space-between; font-size: 11px; color: #7a9a8a; margin-bottom: 4px;">
            <span>Mid: <strong style="color: #e0e0e0;">{mid_pair_str}</strong></span>
            <span>Edge: <strong class="top-bar-stat-value {edge_pair_class}">{edge_pair_str}</strong>{arb_badge}</span>
        </div>
        <div style="text-align: center; font-size: 10px; color: #5a8a6a; margin-bottom: 8px;">
            {updown_str}
        </div>
        <div style="display: flex; justify-content: space-between; font-size: 11px; color: #7a9a8a; margin-top: 4px;">
            <span>Last: <strong style="color: #e0e0e0;">{last_side.upper() if last_side != 'N/A' else 'N/A'}</strong></span>
            <span>Amount: <strong style="color: #00ff6a;">${last_amount:.2f}</strong></span>
        </div>
        <div style="display: flex; justify-content: space-between; font-size: 11px; color: #7a9a8a; margin-top: 4px;">
            <span>Trades: <strong style="color: #e0e0e0;">{trade_count}</strong> ({live_count} live)</span>
            <span>{time_str}</span>
        </div>
        <div style="display: flex; justify-content: space-between; font-size: 11px; color: #7a9a8a; margin-top: 4px;">
            <span>Avg Pair: <strong style="color: #ffd93d;">{avg_pair:.4f}</strong></span>
            <span>Profit: <strong style="color: #00ff6a;">${total_profit:.2f}</strong></span>
        </div>
    </div>
    """, unsafe_allow_html=True)


def render_trades_table(trades: List[Dict]):
    """Render the recent trades table with custom HTML styling."""
    if not trades:
        st.markdown("""
        <div class="panel" style="text-align: center; padding: 40px;">
            <span style="color: #5a8a6a;">No trades recorded yet</span>
        </div>
        """, unsafe_allow_html=True)
        return

    # Build HTML table rows
    rows_html = ""
    for trade in trades:
        # Extract and format data
        timestamp = trade.get("timestamp")
        time_str = timestamp.strftime("%H:%M:%S") if timestamp else ""

        market = (trade.get("market") or "").upper()
        # Extract coin from market slug (e.g., "XRP-UPDOWN-15M-1764912600" -> "XRP")
        coin = market.split("-")[0] if "-" in market else market
        coin_class = f"coin-{coin.lower()}" if coin in ["BTC", "ETH", "SOL", "XRP"] else ""
        # Display just the coin name, not the full slug
        market_display = coin

        side = (trade.get("side") or "").upper()
        side_class = "positive" if side == "UP" else "danger" if side == "DOWN" else ""

        amount = trade.get("amount_usd") or 0
        amount_str = f"${amount:.2f}"

        pair_cost = trade.get("pair_cost")
        pair_str = f"{pair_cost:.4f}" if pair_cost else "N/A"
        # Color pair cost based on profitability
        # Green if < 0.982 (profitable), yellow if 0.982-0.999, red if >= 1.0
        if pair_cost and pair_cost < 0.982:
            pair_class = "profit-positive"
        elif pair_cost and pair_cost < 1.0:
            pair_class = "pair-marginal"  # Yellow for marginal
        else:
            pair_class = "profit-negative"

        profit = trade.get("locked_profit")
        if profit is not None:
            profit_str = f"${profit:.2f}"
            profit_class = "profit-positive" if profit > 0 else "profit-negative" if profit < 0 else ""
        else:
            profit_str = "-"
            profit_class = ""

        dry_run = trade.get("dry_run", True)
        mode_str = "DRY" if dry_run else "LIVE"
        mode_class = "mode-dry" if dry_run else "mode-live"

        success = trade.get("success")
        if success is True:
            status_str = "OK"
            status_class = "status-ok"
        elif success is False:
            status_str = "FAIL"
            status_class = "status-fail"
        else:
            status_str = "-"
            status_class = ""

        # Build row with live trade highlight
        if not dry_run:
            row_html = f'<tr style="background: rgba(0, 255, 106, 0.08);">'
        else:
            row_html = '<tr>'

        rows_html += f"""{row_html}<td style="color: #5a8a6a;">{time_str}</td><td class="{coin_class}">{market_display}</td><td class="top-bar-stat-value {side_class}">{side}</td><td style="color: #00ff6a;">{amount_str}</td><td class="{pair_class}">{pair_str}</td><td class="{profit_class}">{profit_str}</td><td class="{mode_class}">{mode_str}</td><td class="{status_class}">{status_str}</td></tr>"""

    # Render the complete table
    table_html = f"""<div class="trades-table-container"><table class="trades-table"><thead><tr><th>Time</th><th>Coin</th><th>Side</th><th>Amount</th><th>Pair Cost</th><th>Profit</th><th>Mode</th><th>Status</th></tr></thead><tbody>{rows_html}</tbody></table></div>"""
    st.markdown(table_html, unsafe_allow_html=True)


def render_equity_chart(trades: List[Dict]):
    """Render cumulative equity chart."""
    if not trades:
        st.markdown("""
        <div class="panel" style="text-align: center; padding: 60px;">
            <div style="color: #5a8a6a; font-size: 14px;">Waiting for live trades...</div>
            <div style="color: #3a5a4a; font-size: 11px; margin-top: 8px;">
                Equity curve will appear after DRY_RUN=false trades execute
            </div>
        </div>
        """, unsafe_allow_html=True)
        return

    # Build cumulative equity
    df = pd.DataFrame(trades)
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df = df.sort_values("timestamp")
    df["cumulative_profit"] = df["locked_profit"].cumsum()

    fig = go.Figure()

    fig.add_trace(go.Scatter(
        x=df["timestamp"],
        y=df["cumulative_profit"],
        mode="lines+markers",
        name="Cumulative Profit",
        line=dict(color="#00ff6a", width=2),
        marker=dict(size=6, color="#00ff6a"),
        fill="tozeroy",
        fillcolor="rgba(0, 255, 106, 0.1)"
    ))

    fig.update_layout(
        template="plotly_dark",
        paper_bgcolor="rgba(17, 25, 22, 0)",
        plot_bgcolor="rgba(17, 25, 22, 0.8)",
        margin=dict(l=40, r=20, t=20, b=40),
        xaxis=dict(
            showgrid=True,
            gridcolor="rgba(26, 48, 37, 0.5)",
            tickfont=dict(family="JetBrains Mono", size=10, color="#5a8a6a")
        ),
        yaxis=dict(
            showgrid=True,
            gridcolor="rgba(26, 48, 37, 0.5)",
            tickprefix="$",
            tickfont=dict(family="JetBrains Mono", size=10, color="#5a8a6a")
        ),
        showlegend=False,
        height=250
    )

    st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})


# =============================================================================
# PERFORMANCE ANALYTICS - CHART COMPONENTS
# =============================================================================

def render_pair_cost_chart(data: List[Dict]):
    """Render pair_cost time series chart."""
    if not data:
        st.markdown("""
        <div class="panel" style="text-align: center; padding: 40px;">
            <span style="color: #5a8a6a;">No pair cost data yet</span>
        </div>
        """, unsafe_allow_html=True)
        return

    df = pd.DataFrame(data)
    df["timestamp"] = pd.to_datetime(df["timestamp"])

    fig = go.Figure()

    # Add pair cost scatter with color by coin
    colors = {"BTC": "#f7931a", "ETH": "#627eea", "SOL": "#00ffa3", "XRP": "#c0c0c0"}

    for coin in colors.keys():
        coin_df = df[df["coin"] == coin]
        if len(coin_df) > 0:
            fig.add_trace(go.Scatter(
                x=coin_df["timestamp"],
                y=coin_df["pair_cost"],
                mode="markers",
                name=coin,
                marker=dict(color=colors[coin], size=6),
                hovertemplate=f"{coin}: %{{y:.4f}}<extra></extra>"
            ))

    # Add target line
    fig.add_hline(y=0.982, line_dash="dash", line_color="#00ff6a",
                  annotation_text="TARGET (0.982)", annotation_position="right")

    fig.update_layout(
        template="plotly_dark",
        paper_bgcolor="rgba(17, 25, 22, 0)",
        plot_bgcolor="rgba(17, 25, 22, 0.8)",
        margin=dict(l=40, r=20, t=20, b=40),
        xaxis=dict(
            showgrid=True,
            gridcolor="rgba(26, 48, 37, 0.5)",
            tickfont=dict(family="JetBrains Mono", size=9, color="#5a8a6a")
        ),
        yaxis=dict(
            showgrid=True,
            gridcolor="rgba(26, 48, 37, 0.5)",
            tickfont=dict(family="JetBrains Mono", size=9, color="#5a8a6a"),
            range=[0.96, 1.02]
        ),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1,
            font=dict(size=10, color="#00ff6a"),
            bgcolor="rgba(13, 40, 24, 0.8)",
            bordercolor="#1a5c35",
            borderwidth=1
        ),
        height=200
    )

    st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})


def render_locked_profit_chart(data: List[Dict]):
    """Render cumulative locked profit chart."""
    if not data:
        st.markdown("""
        <div class="panel" style="text-align: center; padding: 40px;">
            <span style="color: #5a8a6a;">No profit data yet</span>
        </div>
        """, unsafe_allow_html=True)
        return

    df = pd.DataFrame(data)
    df["timestamp"] = pd.to_datetime(df["timestamp"])

    fig = go.Figure()

    fig.add_trace(go.Scatter(
        x=df["timestamp"],
        y=df["cumulative_profit"],
        mode="lines",
        name="Cumulative Profit",
        line=dict(color="#00ff6a", width=2),
        fill="tozeroy",
        fillcolor="rgba(0, 255, 106, 0.1)"
    ))

    fig.update_layout(
        template="plotly_dark",
        paper_bgcolor="rgba(17, 25, 22, 0)",
        plot_bgcolor="rgba(17, 25, 22, 0.8)",
        margin=dict(l=40, r=20, t=20, b=40),
        xaxis=dict(
            showgrid=True,
            gridcolor="rgba(26, 48, 37, 0.5)",
            tickfont=dict(family="JetBrains Mono", size=9, color="#5a8a6a")
        ),
        yaxis=dict(
            showgrid=True,
            gridcolor="rgba(26, 48, 37, 0.5)",
            tickprefix="$",
            tickfont=dict(family="JetBrains Mono", size=9, color="#5a8a6a")
        ),
        showlegend=False,
        height=200
    )

    st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})


def render_pair_cost_histogram(data: List[Dict]):
    """Render pair_cost distribution histogram."""
    if not data:
        st.markdown("""
        <div class="panel" style="text-align: center; padding: 40px;">
            <span style="color: #5a8a6a;">No pair cost data yet</span>
        </div>
        """, unsafe_allow_html=True)
        return

    df = pd.DataFrame(data)
    pair_costs = df["pair_cost"].dropna()

    fig = go.Figure()

    fig.add_trace(go.Histogram(
        x=pair_costs,
        nbinsx=30,
        marker_color="#00ff6a",
        opacity=0.7
    ))

    # Add target line
    fig.add_vline(x=0.982, line_dash="dash", line_color="#ffd93d",
                  annotation_text="TARGET", annotation_position="top")

    fig.update_layout(
        template="plotly_dark",
        paper_bgcolor="rgba(17, 25, 22, 0)",
        plot_bgcolor="rgba(17, 25, 22, 0.8)",
        margin=dict(l=40, r=20, t=20, b=40),
        xaxis=dict(
            showgrid=True,
            gridcolor="rgba(26, 48, 37, 0.5)",
            tickfont=dict(family="JetBrains Mono", size=9, color="#5a8a6a"),
            title=dict(text="Pair Cost", font=dict(size=9, color="#5a8a6a"))
        ),
        yaxis=dict(
            showgrid=True,
            gridcolor="rgba(26, 48, 37, 0.5)",
            tickfont=dict(family="JetBrains Mono", size=9, color="#5a8a6a"),
            title=dict(text="Count", font=dict(size=9, color="#5a8a6a"))
        ),
        showlegend=False,
        height=200
    )

    st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})


def render_window_summary_table(data: List[Dict]):
    """Render 15-minute window summary table."""
    if not data:
        st.markdown("""
        <div class="panel" style="text-align: center; padding: 40px;">
            <span style="color: #5a8a6a;">No window data yet</span>
        </div>
        """, unsafe_allow_html=True)
        return

    df = pd.DataFrame(data)

    # Format the dataframe for display
    df["window_start"] = pd.to_datetime(df["window_start"]).dt.strftime("%H:%M")
    df["avg_pair_cost"] = df["avg_pair_cost"].apply(lambda x: f"{x:.4f}" if pd.notna(x) else "N/A")
    df["total_volume"] = df["total_volume"].apply(lambda x: f"${x:.2f}" if pd.notna(x) else "$0.00")
    df["total_locked_profit"] = df["total_locked_profit"].apply(lambda x: f"${x:.2f}" if pd.notna(x) else "$0.00")

    # Rename columns for display
    display_df = df[["window_start", "trade_count", "total_volume", "avg_pair_cost",
                     "total_locked_profit", "pairs_completed", "dry_count", "live_count"]].head(12)
    display_df.columns = ["Time", "Trades", "Volume", "Avg Pair", "Profit", "Pairs", "DRY", "LIVE"]

    st.dataframe(display_df, use_container_width=True, hide_index=True, height=220)


def render_engine_health(engine_state: Dict):
    """Render engine health section with wallet and market info."""
    status, age = get_engine_status(engine_state)

    # Parse last_tick data
    last_tick_data = engine_state.get("last_tick", {}).get("value", {})
    if isinstance(last_tick_data, str):
        try:
            last_tick_data = json.loads(last_tick_data)
        except:
            last_tick_data = {}

    markets_found = last_tick_data.get("markets_found", 0)
    opportunities = last_tick_data.get("opportunities", 0)
    wallet_usdc = last_tick_data.get("wallet_usdc")
    tick_count = last_tick_data.get("tick", 0)
    dry_run = last_tick_data.get("dry_run", True)
    auto_mode = last_tick_data.get("auto_mode", False)
    last_tick_time = engine_state.get("last_tick", {}).get("updated_at")

    # Parse last_trade data
    last_trade_data = engine_state.get("last_trade", {}).get("value", {})
    if isinstance(last_trade_data, str):
        try:
            last_trade_data = json.loads(last_trade_data)
        except:
            last_trade_data = {}

    last_trade_time = engine_state.get("last_trade", {}).get("updated_at")

    status_class = "status-online" if status == "ONLINE" else "status-offline"
    age_str = f"{age}s ago" if age >= 0 else "Never"
    wallet_str = f"${wallet_usdc:.2f}" if wallet_usdc is not None else "N/A"
    mode_str = "DRY_RUN" if dry_run else "LIVE"
    auto_str = "ON" if auto_mode else "OFF"

    st.markdown(f"""
    <div class="panel">
        <div class="panel-header">
            <span class="panel-title">Engine Health</span>
            <span class="status-badge {status_class}">{status}</span>
        </div>
        <div style="display: grid; grid-template-columns: repeat(4, 1fr); gap: 12px; margin-top: 12px;">
            <div class="metric-box">
                <div class="metric-value" style="font-size: 16px;">{wallet_str}</div>
                <div class="metric-label">Wallet USDC</div>
            </div>
            <div class="metric-box">
                <div class="metric-value" style="font-size: 16px;">{markets_found}</div>
                <div class="metric-label">Markets</div>
            </div>
            <div class="metric-box">
                <div class="metric-value" style="font-size: 16px;">{age_str}</div>
                <div class="metric-label">Last Tick</div>
            </div>
            <div class="metric-box">
                <div class="metric-value" style="font-size: 16px;">{format_time_ago(last_trade_time)}</div>
                <div class="metric-label">Last Trade</div>
            </div>
        </div>
        <div style="display: flex; justify-content: space-between; font-size: 10px; color: #5a8a6a; margin-top: 12px; padding: 8px; background: rgba(0, 50, 30, 0.2); border-radius: 4px;">
            <span>Tick: <strong style="color: #e0e0e0;">#{tick_count}</strong></span>
            <span>Mode: <strong style="color: {'#ffd93d' if dry_run else '#00ff6a'};">{mode_str}</strong></span>
            <span>Auto: <strong style="color: {'#00ff6a' if auto_mode else '#7a9a8a'};">{auto_str}</strong></span>
            <span>Opps: <strong style="color: {'#00ff6a' if opportunities > 0 else '#7a9a8a'};">{opportunities}</strong></span>
        </div>
    </div>
    """, unsafe_allow_html=True)


# =============================================================================
# MAIN DASHBOARD
# =============================================================================

def main():
    """Main dashboard entry point."""

    # Track refresh count in session state
    if "refresh_count" not in st.session_state:
        st.session_state.refresh_count = 0
    st.session_state.refresh_count += 1

    # Check database connection
    if not HAS_PSYCOPG2:
        st.error("psycopg2 not installed. Run: pip install psycopg2-binary")
        return

    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        st.error("DATABASE_URL environment variable not set")
        st.info("Set DATABASE_URL in your .env file or environment")
        return

    conn = get_db_connection()
    if not conn:
        st.error("Cannot connect to database")
        st.info("Check DATABASE_URL and ensure PostgreSQL is running")
        return

    # Fetch all data
    engine_state = fetch_engine_state()
    trade_stats = fetch_trade_stats()
    recent_trades = fetch_recent_trades(50)
    coin_stats = fetch_coin_stats()
    last_trades = fetch_last_trade_per_coin()
    equity_data = fetch_equity_curve()

    # Extract live data from engine_state for coin cards
    last_tick_data = engine_state.get("last_tick", {}).get("value", {})
    if isinstance(last_tick_data, str):
        try:
            last_tick_data = json.loads(last_tick_data)
        except:
            last_tick_data = {}

    live_data = {
        "binance_prices": last_tick_data.get("binance_prices", {}),
        "latest_pairs": last_tick_data.get("latest_pairs", {}),
    }

    # Render top bar with refresh count
    render_top_bar(trade_stats, engine_state, st.session_state.refresh_count)

    # Main layout: 2 columns
    col_left, col_right = st.columns([1, 2])

    with col_left:
        # 4 Coin Cards (2x2 grid)
        st.markdown('<div class="panel-title" style="margin-bottom: 8px;">COIN POSITIONS</div>', unsafe_allow_html=True)

        row1_col1, row1_col2 = st.columns(2)
        with row1_col1:
            render_coin_card("BTC", coin_stats.get("BTC", {}), last_trades.get("BTC", {}), live_data)
        with row1_col2:
            render_coin_card("ETH", coin_stats.get("ETH", {}), last_trades.get("ETH", {}), live_data)

        row2_col1, row2_col2 = st.columns(2)
        with row2_col1:
            render_coin_card("SOL", coin_stats.get("SOL", {}), last_trades.get("SOL", {}), live_data)
        with row2_col2:
            render_coin_card("XRP", coin_stats.get("XRP", {}), last_trades.get("XRP", {}), live_data)

        # Engine Health
        st.markdown("<div style='margin-top: 16px;'></div>", unsafe_allow_html=True)
        render_engine_health(engine_state)

    with col_right:
        # Equity Chart
        st.markdown('<div class="panel-title" style="margin-bottom: 8px;">CUMULATIVE PROFIT (LIVE TRADES)</div>', unsafe_allow_html=True)
        render_equity_chart(equity_data)

        # Recent Trades Table
        st.markdown("<div style='margin-top: 16px;'></div>", unsafe_allow_html=True)
        st.markdown('<div class="panel-title" style="margin-bottom: 8px;">RECENT TRADES</div>', unsafe_allow_html=True)
        render_trades_table(recent_trades)

    # ==========================================================================
    # PERFORMANCE ANALYTICS SECTION (12-hour backtest workflow)
    # ==========================================================================
    st.markdown("<div style='margin-top: 24px;'></div>", unsafe_allow_html=True)

    with st.expander("PERFORMANCE ANALYTICS (12h Backtest)", expanded=False):
        # Export/Clear buttons row
        btn_col1, btn_col2, btn_col3, btn_col4 = st.columns([2, 2, 2, 6])

        with btn_col1:
            # Fetch 12h trades for export
            trades_12h = get_trades_last_12h()
            if trades_12h:
                csv_data = export_trades_to_csv(trades_12h)
                st.download_button(
                    label=f"Export CSV ({len(trades_12h)} trades)",
                    data=csv_data,
                    file_name=f"polymarket_trades_{datetime.now().strftime('%Y%m%d_%H%M')}.csv",
                    mime="text/csv",
                    key="export_csv"
                )
            else:
                st.button("Export CSV (no trades)", disabled=True, key="export_disabled")

        with btn_col2:
            if st.button("Clear 24h+ Old", key="clear_old"):
                if clear_trade_history(24):
                    st.success("Cleared trades older than 24h")
                    st.rerun()

        with btn_col3:
            if st.button("Clear ALL Trades", key="clear_all", type="secondary"):
                if clear_trade_history(0):
                    st.warning("All trades cleared!")
                    st.rerun()

        st.markdown("<div style='margin-top: 12px;'></div>", unsafe_allow_html=True)

        # Fetch analytics data
        pair_cost_data = get_pair_cost_series()
        profit_data = get_locked_profit_series()
        window_data = get_window_summary()

        # Charts row 1: Pair Cost over time + Cumulative Profit
        chart_row1_col1, chart_row1_col2 = st.columns(2)

        with chart_row1_col1:
            st.markdown('<div class="panel-title" style="margin-bottom: 4px; font-size: 10px;">PAIR COST OVER TIME</div>', unsafe_allow_html=True)
            render_pair_cost_chart(pair_cost_data)

        with chart_row1_col2:
            st.markdown('<div class="panel-title" style="margin-bottom: 4px; font-size: 10px;">CUMULATIVE LOCKED PROFIT</div>', unsafe_allow_html=True)
            render_locked_profit_chart(profit_data)

        # Charts row 2: Pair Cost Histogram + Window Summary
        chart_row2_col1, chart_row2_col2 = st.columns(2)

        with chart_row2_col1:
            st.markdown('<div class="panel-title" style="margin-bottom: 4px; font-size: 10px;">PAIR COST DISTRIBUTION</div>', unsafe_allow_html=True)
            render_pair_cost_histogram(pair_cost_data)

        with chart_row2_col2:
            st.markdown('<div class="panel-title" style="margin-bottom: 4px; font-size: 10px;">15-MIN WINDOW SUMMARY</div>', unsafe_allow_html=True)
            render_window_summary_table(window_data)

    # Auto-refresh every 2 seconds
    time.sleep(2)
    st.rerun()


if __name__ == "__main__":
    main()
