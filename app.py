"""
================================================================================
POLYMARKET 15-MIN COMBO PRINTER — PROFESSIONAL TRADING DASHBOARD
================================================================================

A professional-grade Streamlit trading dashboard for Polymarket's Up/Down
15-minute prediction markets using the "gabagool-style" combo/hedge strategy.

SUPPORTED MARKETS:
    - Bitcoin (BTC)
    - Ethereum (ETH)
    - Solana (SOL)
    - XRP

CLOUD DEPLOYMENT (Streamlit Community Cloud):
    1. Create new GitHub repo
    2. Upload this app.py + requirements.txt
    3. Deploy at https://share.streamlit.io
    4. Share the link with the squad

Each browser tab = independent wallet session
Private keys stay in browser memory only (never stored on server)

REQUIREMENTS.TXT:
    streamlit>=1.28.0
    py-clob-client>=0.17.0
    web3>=6.11.0
    pytz>=2023.3
    requests>=2.31.0
    python-dateutil>=2.8.0
    pandas>=2.0.0
    eth-account>=0.10.0
    plotly>=5.18.0

STRATEGY:
    Buy both UP and DOWN tokens to lock in guaranteed profit when pair cost < $1.
    Each pair pays out exactly $1 at resolution regardless of outcome.
    Profit = $1 * locked_shares - cost_of_locked_shares

gabagool style - Dec 2025 - 4X PRINTING SEASON with the bros
================================================================================
"""

import time
import json
import base64
from datetime import datetime
from typing import Optional, Dict, Any, Tuple, List
from io import BytesIO

import streamlit as st
import requests
import pytz
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from web3 import Web3
from eth_account import Account

# py-clob-client imports
from py_clob_client.client import ClobClient
from py_clob_client.clob_types import OrderArgs

# =============================================================================
# PAGE CONFIGURATION
# =============================================================================

st.set_page_config(
    page_title="Polymarket 15-Min Combo Printer",
    page_icon="💎",
    layout="wide",
    initial_sidebar_state="expanded"
)

# =============================================================================
# DARK THEME CSS - PROFESSIONAL TRADING DASHBOARD
# =============================================================================

DARK_THEME_CSS = """
<style>
    /* Global Dark Theme */
    .stApp {
        background-color: #0e1117;
        color: #ffffff;
    }

    /* Force all text to be white/light */
    .stApp, .stApp p, .stApp span, .stApp div, .stApp label,
    .stApp h1, .stApp h2, .stApp h3, .stApp h4, .stApp h5, .stApp h6,
    .stMarkdown, .stMarkdown p, .stMarkdown span,
    [data-testid="stMarkdownContainer"], [data-testid="stMarkdownContainer"] p {
        color: #ffffff !important;
    }

    /* Caption and secondary text */
    .stCaption, small, .stApp small {
        color: #888888 !important;
    }

    /* Hide Streamlit branding - but keep essential UI */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    /* Keep header visible for sidebar toggle */
    header[data-testid="stHeader"] {
        background-color: #0e1117 !important;
    }

    /* Main container */
    .main .block-container {
        padding-top: 1rem;
        padding-bottom: 1rem;
        max-width: 100%;
    }

    /* Custom card styling */
    .pro-card {
        background: linear-gradient(145deg, #161a22, #1a1f2a);
        border: 1px solid #2d333b;
        border-radius: 12px;
        padding: 20px;
        margin: 10px 0;
        box-shadow: 0 4px 20px rgba(0,0,0,0.4);
    }

    .pro-card-header {
        background: linear-gradient(90deg, #161a22, #1e242e);
        border: 1px solid #2d333b;
        border-radius: 12px 12px 0 0;
        padding: 15px 20px;
        margin: -20px -20px 15px -20px;
        border-bottom: 1px solid #2d333b;
    }

    /* Header bar */
    .header-bar {
        background: linear-gradient(180deg, #161a22, #0e1117);
        border-bottom: 2px solid #00c853;
        padding: 15px 30px;
        margin: -1rem -1rem 1rem -1rem;
        display: flex;
        justify-content: space-between;
        align-items: center;
    }

    .header-title {
        color: #00c853;
        font-size: 1.8em;
        font-weight: 700;
        letter-spacing: 2px;
        text-shadow: 0 0 20px rgba(0,200,83,0.3);
    }

    .header-stats {
        display: flex;
        gap: 40px;
    }

    .header-stat {
        text-align: center;
    }

    .header-stat-value {
        font-size: 1.5em;
        font-weight: bold;
        color: #00c853;
    }

    .header-stat-label {
        font-size: 0.75em;
        color: #888;
        text-transform: uppercase;
        letter-spacing: 1px;
    }

    /* Market cards */
    .market-card {
        background: linear-gradient(145deg, #161a22, #1a1f2a);
        border: 1px solid #2d333b;
        border-radius: 16px;
        padding: 20px;
        text-align: center;
        transition: all 0.3s ease;
        height: 100%;
    }

    .market-card:hover {
        border-color: #00c853;
        box-shadow: 0 0 30px rgba(0,200,83,0.2);
    }

    .market-card-active {
        border-left: 4px solid #00c853;
    }

    .market-card-waiting {
        border-left: 4px solid #ffc107;
        opacity: 0.7;
    }

    .coin-symbol {
        font-size: 2.5em;
        font-weight: 800;
        margin-bottom: 5px;
    }

    .coin-btc { color: #f7931a; }
    .coin-eth { color: #627eea; }
    .coin-sol { color: #00ffa3; }
    .coin-xrp { color: #23292f; background: linear-gradient(135deg, #fff, #888); -webkit-background-clip: text; -webkit-text-fill-color: transparent; }

    /* Gauge styling */
    .gauge-container {
        position: relative;
        width: 120px;
        height: 120px;
        margin: 10px auto;
    }

    /* Profit/Loss colors */
    .profit-positive {
        color: #00c853 !important;
        text-shadow: 0 0 10px rgba(0,200,83,0.5);
    }

    .profit-negative {
        color: #ff5252 !important;
        text-shadow: 0 0 10px rgba(255,82,82,0.5);
    }

    /* Big number display */
    .big-number {
        font-size: 3em;
        font-weight: 800;
        line-height: 1;
    }

    .big-number-label {
        font-size: 0.8em;
        color: #888;
        text-transform: uppercase;
        letter-spacing: 2px;
        margin-top: 5px;
    }

    /* Table styling */
    .pro-table {
        width: 100%;
        border-collapse: collapse;
        font-family: 'SF Mono', 'Monaco', monospace;
    }

    .pro-table th {
        background: #161a22;
        color: #888;
        padding: 12px;
        text-align: left;
        font-size: 0.75em;
        text-transform: uppercase;
        letter-spacing: 1px;
        border-bottom: 2px solid #2d333b;
    }

    .pro-table td {
        padding: 12px;
        border-bottom: 1px solid #2d333b;
        color: #fff;
    }

    .pro-table tr:hover {
        background: #1a1f2a;
    }

    /* Button styling */
    .stButton > button {
        background: linear-gradient(145deg, #1e242e, #161a22);
        border: 1px solid #2d333b;
        color: #fff;
        font-weight: 600;
        transition: all 0.3s ease;
    }

    .stButton > button:hover {
        border-color: #00c853;
        box-shadow: 0 0 20px rgba(0,200,83,0.3);
    }

    /* Buy buttons */
    .buy-up-btn {
        background: linear-gradient(145deg, #1b5e20, #2e7d32) !important;
        border-color: #4caf50 !important;
    }

    .buy-down-btn {
        background: linear-gradient(145deg, #b71c1c, #c62828) !important;
        border-color: #f44336 !important;
    }

    /* Tab styling */
    .stTabs [data-baseweb="tab-list"] {
        background: #161a22;
        border-radius: 8px;
        padding: 5px;
        gap: 5px;
    }

    .stTabs [data-baseweb="tab"] {
        background: transparent;
        color: #888;
        border-radius: 6px;
        padding: 10px 20px;
        font-weight: 600;
    }

    .stTabs [aria-selected="true"] {
        background: linear-gradient(145deg, #1e242e, #252b38);
        color: #00c853;
        border-bottom: 2px solid #00c853;
    }

    /* Metric cards */
    .stMetric {
        background: linear-gradient(145deg, #161a22, #1a1f2a);
        border: 1px solid #2d333b;
        border-radius: 10px;
        padding: 15px;
    }

    .stMetric label {
        color: #888 !important;
    }

    .stMetric [data-testid="stMetricValue"] {
        color: #fff !important;
    }

    /* Sidebar styling */
    [data-testid="stSidebar"] {
        background: linear-gradient(180deg, #161a22, #0e1117);
        border-right: 1px solid #2d333b;
    }

    [data-testid="stSidebar"] .stMarkdown {
        color: #888;
    }

    /* Countdown timer */
    .countdown {
        font-size: 3em;
        font-weight: 800;
        font-family: 'SF Mono', 'Monaco', monospace;
        text-align: center;
        padding: 10px;
        border-radius: 8px;
    }

    .countdown-live {
        color: #00c853;
        text-shadow: 0 0 30px rgba(0,200,83,0.5);
        animation: pulse 2s infinite;
    }

    .countdown-warning {
        color: #ffc107;
    }

    .countdown-danger {
        color: #ff5252;
        animation: blink 1s infinite;
    }

    @keyframes pulse {
        0%, 100% { opacity: 1; }
        50% { opacity: 0.7; }
    }

    @keyframes blink {
        0%, 100% { opacity: 1; }
        50% { opacity: 0.3; }
    }

    /* Pair cost display */
    .pair-cost-display {
        font-size: 2.5em;
        font-weight: 800;
        text-align: center;
        padding: 20px;
        border-radius: 12px;
        margin: 10px 0;
    }

    .pair-cost-good {
        background: linear-gradient(145deg, #1b5e20, #2e7d32);
        color: #69f0ae;
        border: 2px solid #4caf50;
    }

    .pair-cost-marginal {
        background: linear-gradient(145deg, #f57f17, #ff8f00);
        color: #fff;
        border: 2px solid #ffc107;
    }

    .pair-cost-bad {
        background: linear-gradient(145deg, #b71c1c, #c62828);
        color: #ff8a80;
        border: 2px solid #f44336;
    }

    /* Stats grid */
    .stats-grid {
        display: grid;
        grid-template-columns: repeat(4, 1fr);
        gap: 15px;
        margin: 20px 0;
    }

    .stat-box {
        background: linear-gradient(145deg, #161a22, #1a1f2a);
        border: 1px solid #2d333b;
        border-radius: 10px;
        padding: 15px;
        text-align: center;
    }

    .stat-value {
        font-size: 1.8em;
        font-weight: 700;
        color: #fff;
    }

    .stat-label {
        font-size: 0.7em;
        color: #888;
        text-transform: uppercase;
        letter-spacing: 1px;
        margin-top: 5px;
    }

    /* Divider */
    hr {
        border: none;
        border-top: 1px solid #2d333b;
        margin: 20px 0;
    }

    /* Input fields */
    .stTextInput input {
        background: #161a22;
        border: 1px solid #2d333b;
        color: #fff;
    }

    /* Expander */
    .streamlit-expanderHeader {
        background: #161a22;
        border: 1px solid #2d333b;
        border-radius: 8px;
    }

    /* Toast/alerts */
    .stAlert {
        background: #161a22;
        border: 1px solid #2d333b;
    }

    /* Progress bar */
    .stProgress > div > div {
        background: linear-gradient(90deg, #00c853, #69f0ae);
    }

    /* Selectbox and dropdown styling */
    .stSelectbox > div > div {
        background: #161a22;
        border: 1px solid #2d333b;
        color: #fff;
    }

    .stSelectbox label {
        color: #888 !important;
    }

    /* Dataframe styling */
    .stDataFrame {
        background: #161a22;
    }

    .stDataFrame [data-testid="stDataFrameResizable"] {
        background: #0e1117;
    }

    /* File uploader */
    .stFileUploader {
        background: #161a22;
        border: 1px solid #2d333b;
        border-radius: 8px;
    }

    .stFileUploader label {
        color: #888 !important;
    }

    /* Warning/Info/Error boxes text */
    .stAlert > div {
        color: #fff !important;
    }

    /* Spinner text */
    .stSpinner > div {
        color: #fff !important;
    }

    /* Column headers in dataframe */
    [data-testid="stDataFrame"] th {
        background: #161a22 !important;
        color: #888 !important;
    }

    [data-testid="stDataFrame"] td {
        background: #0e1117 !important;
        color: #fff !important;
    }

    /* Binance price ticker */
    .binance-ticker {
        display: inline-block;
        padding: 4px 8px;
        border-radius: 4px;
        font-size: 0.85em;
        font-weight: 600;
        margin-left: 8px;
    }

    .binance-up {
        color: #00c853;
    }

    .binance-down {
        color: #ff5252;
    }

    /* Sidebar styling */
    [data-testid="stSidebar"] {
        background-color: #0e1117 !important;
        border-right: 1px solid #2d333b !important;
    }

    [data-testid="stSidebar"] * {
        color: #ffffff !important;
    }

    [data-testid="stSidebar"] input {
        background-color: #161a22 !important;
        color: #ffffff !important;
        border: 1px solid #2d333b !important;
    }

    [data-testid="stSidebar"] .stButton button {
        background-color: #00c853 !important;
        color: #000 !important;
        font-weight: bold !important;
    }

    /* Ensure text inputs are visible */
    .stTextInput input {
        background-color: #161a22 !important;
        color: #ffffff !important;
        border: 1px solid #2d333b !important;
    }

    .stTextInput label {
        color: #ffffff !important;
    }
</style>
"""

# Inject CSS
st.markdown(DARK_THEME_CSS, unsafe_allow_html=True)

# Show title immediately (prevents blank screen)
st.markdown("""
<div style='text-align: center; padding: 20px;'>
    <h1 style='color: #00c853; margin: 0;'>💎 POLYMARKET 15-MIN COMBO PRINTER</h1>
    <p style='color: #888; margin-top: 5px;'>gabagool style • Dec 2025</p>
</div>
""", unsafe_allow_html=True)

# =============================================================================
# CONFIGURATION
# =============================================================================

# Polymarket APIs
CLOB_HOST = "https://clob.polymarket.com"
GAMMA_API_HOST = "https://gamma-api.polymarket.com"
CHAIN_ID = 137  # Polygon Mainnet

# Contract addresses (Dec 2025 - Native Circle USDC)
USDC_ADDRESS = "0x3c499c542cEF5E3811e1192ce70d8cc03d5c3359"
CONDITIONAL_TOKENS = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"

# Exchange contracts to approve
EXCHANGE_CONTRACTS = [
    "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E",  # main
    "0xC5d563A36AE78145C45a50134d48A1215220f80a",  # neg risk
    "0xd91E80cF2E7be2e162c6513ceD06f1dD0dA35296",  # neg risk adapter
]

# Supported coins for Up/Down markets
SLUG_COINS = ["btc", "eth", "sol", "xrp"]

# Coin to Binance symbol mapping
COIN_TO_BINANCE = {
    "BTC": "BTCUSDT",
    "ETH": "ETHUSDT",
    "SOL": "SOLUSDT",
    "XRP": "XRPUSDT"
}

# Trading safety parameters
MAX_IMBALANCE = 500          # Max allowed share imbalance
WARN_IMBALANCE = 400         # Threshold to disable heavier side
NO_TRADE_SECONDS = 90        # No trades in last 90 seconds
PRICE_SLIPPAGE = 0.006       # Cross spread by 0.6 cents
BUY_AMOUNTS = [10, 25, 50, 100]  # USD amounts for buy buttons
REFRESH_INTERVAL = 8         # Seconds between auto-refresh

# Timezone
ET = pytz.timezone("US/Eastern")

# Minimal ERC20 ABI for approvals and balance
MINIMAL_ERC20_ABI = [
    {
        "constant": False,
        "inputs": [
            {"name": "spender", "type": "address"},
            {"name": "amount", "type": "uint256"}
        ],
        "name": "approve",
        "outputs": [{"name": "", "type": "bool"}],
        "type": "function"
    },
    {
        "constant": True,
        "inputs": [{"name": "_owner", "type": "address"}],
        "name": "balanceOf",
        "outputs": [{"name": "balance", "type": "uint256"}],
        "type": "function"
    },
    {
        "constant": True,
        "inputs": [
            {"name": "_owner", "type": "address"},
            {"name": "_spender", "type": "address"}
        ],
        "name": "allowance",
        "outputs": [{"name": "", "type": "uint256"}],
        "type": "function"
    }
]

# ERC1155 ABI for Conditional Tokens (setApprovalForAll + isApprovedForAll)
MINIMAL_ERC1155_ABI = [
    {
        "constant": False,
        "inputs": [
            {"name": "operator", "type": "address"},
            {"name": "approved", "type": "bool"}
        ],
        "name": "setApprovalForAll",
        "outputs": [],
        "type": "function"
    },
    {
        "constant": True,
        "inputs": [
            {"name": "_owner", "type": "address"},
            {"name": "_operator", "type": "address"}
        ],
        "name": "isApprovedForAll",
        "outputs": [{"name": "", "type": "bool"}],
        "type": "function"
    }
]

# =============================================================================
# SESSION STATE INITIALIZATION
# =============================================================================

if 'state' not in st.session_state:
    st.session_state.state = {
        "markets": {},
        "history": [],
        "allowance_approved": False,
        "trade_log": [],
        "equity_history": [],
        "session_start": datetime.now(ET).isoformat(),
        "total_trades": 0,
        "daily_pnl": {},
    }

# Ensure all keys exist (migration safety)
defaults = {
    "markets": {},
    "history": [],
    "allowance_approved": False,
    "trade_log": [],
    "equity_history": [],
    "session_start": datetime.now(ET).isoformat(),
    "total_trades": 0,
    "daily_pnl": {},
}

for key, default in defaults.items():
    if key not in st.session_state.state:
        st.session_state.state[key] = default

if 'client' not in st.session_state:
    st.session_state.client = None

if 'wallet_connected' not in st.session_state:
    st.session_state.wallet_connected = False
    st.session_state.private_key = ""
    st.session_state.rpc_url = "https://polygon-rpc.com"

if 'binance_data' not in st.session_state:
    st.session_state.binance_data = {}

# =============================================================================
# BINANCE PRICE DATA
# =============================================================================

def get_binance_data() -> Dict[str, Dict]:
    """
    Fetch live prices from Binance for all supported coins.
    Returns dict: {symbol: {"price": float, "change": float}}
    Uses Binance.US API for US-based servers (Streamlit Cloud).
    Falls back to CoinGecko if Binance fails.
    """
    symbols = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT"]
    data = {}

    # Try Binance.US first (works from US servers)
    for sym in symbols:
        try:
            r = requests.get(
                f"https://api.binance.us/api/v3/ticker/24hr?symbol={sym}",
                timeout=3
            )
            if r.status_code == 200:
                j = r.json()
                data[sym] = {
                    "price": float(j.get("lastPrice", 0)),
                    "change": float(j.get("priceChangePercent", 0))
                }
            else:
                data[sym] = {"price": 0.0, "change": 0.0}
        except Exception:
            data[sym] = {"price": 0.0, "change": 0.0}

    # If all failed, try CoinGecko as fallback
    if all(d["price"] == 0.0 for d in data.values()):
        try:
            cg_ids = "bitcoin,ethereum,solana,ripple"
            r = requests.get(
                f"https://api.coingecko.com/api/v3/simple/price?ids={cg_ids}&vs_currencies=usd&include_24hr_change=true",
                timeout=5
            )
            if r.status_code == 200:
                j = r.json()
                mapping = {
                    "BTCUSDT": ("bitcoin", j.get("bitcoin", {})),
                    "ETHUSDT": ("ethereum", j.get("ethereum", {})),
                    "SOLUSDT": ("solana", j.get("solana", {})),
                    "XRPUSDT": ("ripple", j.get("ripple", {}))
                }
                for sym, (_, info) in mapping.items():
                    data[sym] = {
                        "price": float(info.get("usd", 0)),
                        "change": float(info.get("usd_24h_change", 0))
                    }
        except Exception:
            pass

    return data


def format_binance_price(coin: str, binance_data: Dict) -> str:
    """Format Binance price with change indicator for display."""
    symbol = COIN_TO_BINANCE.get(coin, "")
    if not symbol or symbol not in binance_data:
        return ""

    info = binance_data[symbol]
    price = info.get("price", 0)
    change = info.get("change", 0)

    if price == 0:
        return ""

    # Format price based on magnitude
    if price >= 1000:
        price_str = f"${price:,.2f}"
    elif price >= 1:
        price_str = f"${price:.2f}"
    else:
        price_str = f"${price:.4f}"

    # Color based on change
    change_color = "#00c853" if change >= 0 else "#ff5252"
    change_sign = "+" if change >= 0 else ""

    return f"""<span style='color: {change_color}; font-weight: 600;'>{price_str} <span style='font-size: 0.85em;'>({change_sign}{change:.2f}%)</span></span>"""


# =============================================================================
# CLOB LIVE PRICE FETCHING - THE REAL EDGE!
# =============================================================================

def get_clob_midpoints(up_token_id: str, down_token_id: str) -> Tuple[float, float]:
    """
    Fetch LIVE prices from CLOB /midpoint API.

    THIS IS THE ONLY SOURCE OF REAL TRADEABLE PRICES!

    Gamma API outcomePrices is STALE and always sums to 1.0 (theoretical).
    CLOB midpoint shows the actual market prices where edges exist.

    Example:
        Gamma (STALE): Up=0.355 + Down=0.645 = 1.0000 ← WRONG!
        CLOB (LIVE):   Up=0.96  + Down=0.035 = 0.995  ← THE EDGE!
    """
    up_price = 0.5
    down_price = 0.5

    try:
        # Get Up price from CLOB
        r = requests.get(
            f"https://clob.polymarket.com/midpoint?token_id={up_token_id}",
            timeout=3
        )
        if r.status_code == 200:
            up_price = float(r.json().get("mid", 0.5))
    except Exception:
        pass

    try:
        # Get Down price from CLOB
        r = requests.get(
            f"https://clob.polymarket.com/midpoint?token_id={down_token_id}",
            timeout=3
        )
        if r.status_code == 200:
            down_price = float(r.json().get("mid", 0.5))
    except Exception:
        pass

    return up_price, down_price


# =============================================================================
# PLOTLY CHART FUNCTIONS
# =============================================================================

def create_equity_curve(equity_history: List[Dict], height: int = 300) -> go.Figure:
    """Create professional dark-themed equity curve."""
    if not equity_history:
        fig = go.Figure()
        fig.add_annotation(
            text="No trading data yet",
            xref="paper", yref="paper",
            x=0.5, y=0.5,
            showarrow=False,
            font=dict(size=16, color="#888")
        )
    else:
        times = [e.get("timestamp", "") for e in equity_history]
        values = [e.get("total_profit", 0) for e in equity_history]

        line_color = "#00c853" if values[-1] >= 0 else "#ff5252"
        fill_color = "rgba(0, 200, 83, 0.1)" if values[-1] >= 0 else "rgba(255, 82, 82, 0.1)"

        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=times,
            y=values,
            mode='lines',
            line=dict(color=line_color, width=2),
            fill='tozeroy',
            fillcolor=fill_color,
            name='Equity'
        ))

    fig.update_layout(
        plot_bgcolor='#0e1117',
        paper_bgcolor='#0e1117',
        font=dict(color='#888'),
        margin=dict(l=50, r=20, t=20, b=40),
        height=height,
        xaxis=dict(
            showgrid=True,
            gridcolor='#2d333b',
            showline=True,
            linecolor='#2d333b'
        ),
        yaxis=dict(
            showgrid=True,
            gridcolor='#2d333b',
            showline=True,
            linecolor='#2d333b',
            tickprefix='$'
        ),
        showlegend=False,
        hovermode='x unified'
    )

    return fig


def create_mini_sparkline(equity_history: List[Dict], width: int = 200, height: int = 50) -> go.Figure:
    """Create mini sparkline for header."""
    fig = go.Figure()

    if equity_history:
        values = [e.get("total_profit", 0) for e in equity_history[-20:]]
        line_color = "#00c853" if (values[-1] if values else 0) >= 0 else "#ff5252"

        fig.add_trace(go.Scatter(
            y=values,
            mode='lines',
            line=dict(color=line_color, width=2),
            fill='tozeroy',
            fillcolor=f"rgba({76 if values[-1] >= 0 else 255}, {175 if values[-1] >= 0 else 82}, {80 if values[-1] >= 0 else 82}, 0.2)"
        ))

    fig.update_layout(
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)',
        margin=dict(l=0, r=0, t=0, b=0),
        width=width,
        height=height,
        xaxis=dict(visible=False),
        yaxis=dict(visible=False),
        showlegend=False
    )

    return fig


def create_pair_cost_gauge(pair_cost: float) -> go.Figure:
    """Create circular gauge for pair cost visualization."""
    if pair_cost < 0.98:
        color = "#00c853"
    elif pair_cost <= 0.985:
        color = "#ffc107"
    else:
        color = "#ff5252"

    fig = go.Figure(go.Indicator(
        mode="gauge+number",
        value=pair_cost,
        number={'prefix': "$", 'font': {'size': 24, 'color': color}},
        gauge={
            'axis': {'range': [0.95, 1.0], 'tickcolor': '#888'},
            'bar': {'color': color},
            'bgcolor': '#161a22',
            'bordercolor': '#2d333b',
            'steps': [
                {'range': [0.95, 0.98], 'color': 'rgba(0, 200, 83, 0.2)'},
                {'range': [0.98, 0.985], 'color': 'rgba(255, 193, 7, 0.2)'},
                {'range': [0.985, 1.0], 'color': 'rgba(255, 82, 82, 0.2)'}
            ],
            'threshold': {
                'line': {'color': '#fff', 'width': 2},
                'thickness': 0.8,
                'value': pair_cost
            }
        }
    ))

    fig.update_layout(
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)',
        font=dict(color='#888'),
        margin=dict(l=20, r=20, t=30, b=10),
        height=150
    )

    return fig


def create_pnl_bar_chart(trades: List[Dict], height: int = 200) -> go.Figure:
    """Create P/L bar chart for recent trades."""
    fig = go.Figure()

    if trades:
        profits = []
        labels = []
        colors = []

        for t in trades[:10]:
            pnl = t.get("profit", 0)
            profits.append(pnl)
            labels.append(t.get("time", ""))
            colors.append("#00c853" if pnl >= 0 else "#ff5252")

        fig.add_trace(go.Bar(
            x=labels,
            y=profits,
            marker_color=colors
        ))

    fig.update_layout(
        plot_bgcolor='#0e1117',
        paper_bgcolor='#0e1117',
        font=dict(color='#888'),
        margin=dict(l=40, r=20, t=10, b=40),
        height=height,
        xaxis=dict(showgrid=False),
        yaxis=dict(showgrid=True, gridcolor='#2d333b', tickprefix='$'),
        showlegend=False
    )

    return fig


# =============================================================================
# WEB3 SETUP
# =============================================================================

def get_web3() -> Web3:
    """Get Web3 instance connected to Polygon."""
    return Web3(Web3.HTTPProvider(st.session_state.rpc_url))


def get_wallet_address() -> str:
    """Derive wallet address from private key."""
    account = Account.from_key(st.session_state.private_key)
    return account.address


def get_usdc_balance() -> Optional[float]:
    """Get USDC balance for the configured wallet."""
    try:
        web3 = get_web3()
        if not web3.is_connected():
            return None

        wallet_address = get_wallet_address()
        usdc = web3.eth.contract(
            address=Web3.to_checksum_address(USDC_ADDRESS),
            abi=MINIMAL_ERC20_ABI
        )
        raw_balance = usdc.functions.balanceOf(wallet_address).call()
        return float(raw_balance) / 1_000_000
    except Exception:
        return None


def get_matic_balance() -> Optional[float]:
    """Get MATIC (native token) balance for gas fees."""
    try:
        web3 = get_web3()
        if not web3.is_connected():
            return None

        wallet_address = get_wallet_address()
        raw_balance = web3.eth.get_balance(wallet_address)
        return float(raw_balance) / 1e18
    except Exception:
        return None


def check_existing_approvals() -> bool:
    """
    Check if USDC and CT approvals already exist on-chain.
    Returns True if ALL approvals are already set, False otherwise.
    """
    try:
        web3 = get_web3()
        if not web3.is_connected():
            return False

        wallet_address = get_wallet_address()

        usdc = web3.eth.contract(
            address=Web3.to_checksum_address(USDC_ADDRESS),
            abi=MINIMAL_ERC20_ABI
        )
        ct = web3.eth.contract(
            address=Web3.to_checksum_address(CONDITIONAL_TOKENS),
            abi=MINIMAL_ERC1155_ABI
        )

        min_allowance = 10**18
        for contract_addr in EXCHANGE_CONTRACTS:
            allowance = usdc.functions.allowance(
                wallet_address,
                Web3.to_checksum_address(contract_addr)
            ).call()
            if allowance < min_allowance:
                return False

        for contract_addr in EXCHANGE_CONTRACTS:
            is_approved = ct.functions.isApprovedForAll(
                wallet_address,
                Web3.to_checksum_address(contract_addr)
            ).call()
            if not is_approved:
                return False

        return True

    except Exception:
        return False


def approve_all_contracts() -> bool:
    """Approve USDC and Conditional Tokens for all exchange contracts."""
    if st.session_state.state.get("allowance_approved", False):
        st.info("Allowances already approved!")
        return True

    try:
        web3 = get_web3()
        if not web3.is_connected():
            st.error("Failed to connect to Polygon RPC")
            return False

        account = Account.from_key(st.session_state.private_key)
        wallet_address = account.address

        usdc = web3.eth.contract(
            address=Web3.to_checksum_address(USDC_ADDRESS),
            abi=MINIMAL_ERC20_ABI
        )
        ct = web3.eth.contract(
            address=Web3.to_checksum_address(CONDITIONAL_TOKENS),
            abi=MINIMAL_ERC1155_ABI
        )

        progress = st.progress(0)
        status = st.empty()

        for i, contract_addr in enumerate(EXCHANGE_CONTRACTS):
            status.info(f"Approving USDC for contract {i+1}/3...")
            nonce = web3.eth.get_transaction_count(wallet_address)
            tx = usdc.functions.approve(
                Web3.to_checksum_address(contract_addr),
                2**256 - 1
            ).build_transaction({
                "chainId": 137,
                "gas": 120_000,
                "maxFeePerGas": web3.to_wei(120, "gwei"),
                "maxPriorityFeePerGas": web3.to_wei(40, "gwei"),
                "nonce": nonce,
            })
            signed_tx = account.sign_transaction(tx)
            tx_hash = web3.eth.send_raw_transaction(signed_tx.raw_transaction)
            web3.eth.wait_for_transaction_receipt(tx_hash, timeout=120)
            progress.progress((i + 1) / 6)
            time.sleep(1)

        for i, contract_addr in enumerate(EXCHANGE_CONTRACTS):
            status.info(f"Approving CT for contract {i+1}/3...")
            nonce = web3.eth.get_transaction_count(wallet_address)
            tx = ct.functions.setApprovalForAll(
                Web3.to_checksum_address(contract_addr),
                True
            ).build_transaction({
                "chainId": 137,
                "gas": 120_000,
                "maxFeePerGas": web3.to_wei(120, "gwei"),
                "maxPriorityFeePerGas": web3.to_wei(40, "gwei"),
                "nonce": nonce,
            })
            signed_tx = account.sign_transaction(tx)
            tx_hash = web3.eth.send_raw_transaction(signed_tx.raw_transaction)
            web3.eth.wait_for_transaction_receipt(tx_hash, timeout=120)
            progress.progress((i + 4) / 6)
            time.sleep(1)

        st.session_state.state["allowance_approved"] = True
        status.success("All approvals complete!")
        return True

    except Exception as e:
        st.error(f"Approval error: {e}")
        return False


# =============================================================================
# CLOB CLIENT INITIALIZATION
# =============================================================================

def get_clob_client() -> Optional[ClobClient]:
    """Get or create the ClobClient instance."""
    if st.session_state.client is not None:
        return st.session_state.client

    try:
        wallet_address = get_wallet_address()
        client = ClobClient(
            host=CLOB_HOST,
            key=st.session_state.private_key,
            chain_id=CHAIN_ID,
            signature_type=0,
            funder=wallet_address
        )
        creds = client.create_or_derive_api_creds()
        client.set_api_creds(creds)
        st.session_state.client = client
        return client
    except Exception as e:
        st.error(f"Failed to initialize CLOB client: {e}")
        return None


# =============================================================================
# MARKET DETECTION - GAMMA API WITH LIVE PRICES FROM TOKENS
# =============================================================================

def get_current_15m_timestamp() -> int:
    """Get Unix timestamp for current 15-minute window start."""
    return int(time.time() // 900 * 900)


def fetch_market_by_slug(slug: str) -> Optional[Dict]:
    """Fetch a specific market from Gamma API by slug with token prices."""
    try:
        response = requests.get(
            f"{GAMMA_API_HOST}/markets?slug={slug}",
            timeout=10
        )
        if response.status_code == 200:
            data = response.json()
            if data and len(data) > 0:
                market = data[0]
                if market.get("active", False) and not market.get("closed", False):
                    return market
        return None
    except Exception:
        return None


def find_active_market_for_coin(coin: str) -> Optional[Dict]:
    """
    Find active market for a specific coin.
    CRITICAL: Extract prices from the tokens array, NOT from outcomePrices.
    The tokens array contains live prices directly from Gamma API.
    """
    current_ts = get_current_15m_timestamp()
    timestamps_to_check = [current_ts, current_ts + 900, current_ts - 900]

    for ts in timestamps_to_check:
        slug = f"{coin}-updown-15m-{ts}"
        market = fetch_market_by_slug(slug)

        if market:
            # Parse token IDs
            token_ids_raw = market.get("clobTokenIds", [])
            if isinstance(token_ids_raw, str):
                try:
                    token_ids = json.loads(token_ids_raw)
                except:
                    token_ids = []
            else:
                token_ids = token_ids_raw

            # Parse outcomes
            outcomes_raw = market.get("outcomes", ["Up", "Down"])
            if isinstance(outcomes_raw, str):
                try:
                    outcomes = json.loads(outcomes_raw)
                except:
                    outcomes = ["Up", "Down"]
            else:
                outcomes = outcomes_raw

            # ============================================================
            # LIVE PRICE EXTRACTION FROM CLOB API
            # DO NOT USE Gamma outcomePrices - they're stale/theoretical!
            # CLOB /midpoint gives real tradeable prices where edges exist
            # ============================================================

            if len(token_ids) >= 2 and len(outcomes) >= 2:
                # Find up/down token indices
                up_idx, down_idx = 0, 1
                for i, outcome in enumerate(outcomes):
                    if str(outcome).lower() == "up":
                        up_idx = i
                    elif str(outcome).lower() == "down":
                        down_idx = i

                up_token_id = token_ids[up_idx]
                down_token_id = token_ids[down_idx]

                # GET LIVE PRICES FROM CLOB API - THE REAL EDGE!
                up_price, down_price = get_clob_midpoints(up_token_id, down_token_id)

                # Parse end time
                end_time = None
                end_date_str = market.get("endDate") or market.get("end_date_iso")
                if end_date_str:
                    try:
                        from dateutil import parser as dateutil_parser
                        end_time = dateutil_parser.parse(end_date_str)
                        if end_time.tzinfo is None:
                            end_time = ET.localize(end_time)
                    except:
                        end_time = None

                return {
                    "condition_id": market.get("conditionId"),
                    "coin": coin.upper(),
                    "question": market.get("question", f"{coin.upper()} Up or Down"),
                    "slug": slug,
                    "end_time": end_time,
                    "up_token_id": up_token_id,
                    "down_token_id": down_token_id,
                    "up_price": up_price,
                    "down_price": down_price,
                    "active": True,
                }

    return None


def find_all_active_updown_markets() -> List[Dict]:
    """Find all active 15-min Up/Down markets with live prices."""
    all_markets = []

    for coin in SLUG_COINS:
        market = find_active_market_for_coin(coin)
        if market:
            all_markets.append(market)
        else:
            all_markets.append({
                "condition_id": None,
                "coin": coin.upper(),
                "question": f"{coin.upper()} Up or Down - Waiting...",
                "slug": None,
                "end_time": None,
                "up_token_id": None,
                "down_token_id": None,
                "up_price": 0.5,
                "down_price": 0.5,
                "active": False,
            })

    coin_order = {"BTC": 0, "ETH": 1, "SOL": 2, "XRP": 3}
    all_markets.sort(key=lambda x: coin_order.get(x.get("coin", "ZZZ"), 99))
    return all_markets


def get_seconds_remaining(end_time) -> int:
    """Calculate seconds remaining until market ends."""
    if end_time is None:
        return 999
    try:
        now = datetime.now(ET)
        if end_time.tzinfo is None:
            end_time = ET.localize(end_time)
        delta = end_time - now
        return max(0, int(delta.total_seconds()))
    except:
        return 999


def format_countdown(seconds: int) -> str:
    """Format seconds as MM:SS countdown string."""
    if seconds >= 999:
        return "LIVE"
    minutes, secs = divmod(seconds, 60)
    return f"{minutes:02d}:{secs:02d}"


# =============================================================================
# MARKET STATE MANAGEMENT
# =============================================================================

def get_market_state(condition_id: str, coin: str) -> Dict:
    """Get or create state for a specific market."""
    markets = st.session_state.state.get("markets", {})

    if "markets" not in st.session_state.state:
        st.session_state.state["markets"] = {}
        markets = st.session_state.state["markets"]

    if condition_id not in markets:
        markets[condition_id] = {
            "coin": coin,
            "shares_up": 0.0,
            "spent_up": 0.0,
            "shares_down": 0.0,
            "spent_down": 0.0,
            "trade_log": [],
        }

    return markets[condition_id]


def archive_old_markets(active_condition_ids: List[str]):
    """Archive markets that are no longer active."""
    markets = st.session_state.state.get("markets", {})
    history = st.session_state.state.get("history", [])

    to_archive = []
    for cid, mstate in list(markets.items()):
        if cid not in active_condition_ids:
            try:
                shares_up = mstate.get("shares_up", 0.0)
                shares_down = mstate.get("shares_down", 0.0)

                if shares_up > 0 or shares_down > 0:
                    spent_up = mstate.get("spent_up", 0.0)
                    spent_down = mstate.get("spent_down", 0.0)

                    locked_shares = min(shares_up, shares_down)
                    if locked_shares > 0 and shares_up > 0 and shares_down > 0:
                        avg_up = spent_up / shares_up
                        avg_down = spent_down / shares_down
                        pair_cost = avg_up + avg_down
                        locked_profit = locked_shares * (1 - pair_cost)
                    else:
                        locked_profit = 0

                    history.insert(0, {
                        "coin": mstate.get("coin", "???"),
                        "market_id": cid[:12] + "...",
                        "end_time": datetime.now(ET).strftime("%H:%M"),
                        "shares_up": round(shares_up, 2),
                        "shares_down": round(shares_down, 2),
                        "locked_profit": round(locked_profit, 2)
                    })

                    total_profit = get_total_locked_profit() + get_total_history_profit()
                    st.session_state.state["equity_history"].append({
                        "timestamp": datetime.now(ET).strftime("%H:%M:%S"),
                        "total_profit": total_profit
                    })

            except Exception:
                pass

            to_archive.append(cid)

    for cid in to_archive:
        try:
            del markets[cid]
        except:
            pass

    st.session_state.state["history"] = history[:100]


# =============================================================================
# TRADING FUNCTIONS - USE MARKET PRICES, NOT client.get_midpoint
# =============================================================================

def get_order_book_ask(client: ClobClient, token_id: str) -> float:
    """Get best ask price from order book for execution."""
    try:
        ob = client.get_order_book(token_id)
        if ob.asks:
            return float(ob.asks[0].price)
        return 0.99
    except Exception:
        return 0.99


def check_safety(mstate: Dict, side: str, seconds_remaining: int) -> Tuple[bool, str]:
    """Check if a trade is allowed under safety rules."""
    if seconds_remaining < NO_TRADE_SECONDS and seconds_remaining != 999:
        return False, f"Trading disabled - {seconds_remaining}s remaining"

    shares_up = mstate.get("shares_up", 0.0)
    shares_down = mstate.get("shares_down", 0.0)
    current_imbalance = abs(shares_up - shares_down)

    if shares_up > shares_down:
        heavier_side = "up"
    elif shares_down > shares_up:
        heavier_side = "down"
    else:
        heavier_side = None

    if heavier_side == side:
        if current_imbalance >= MAX_IMBALANCE:
            return False, f"Max imbalance ({current_imbalance:.0f}/{MAX_IMBALANCE})"
        if current_imbalance >= WARN_IMBALANCE:
            return False, f"Imbalance warning ({current_imbalance:.0f}/{WARN_IMBALANCE})"

    return True, ""


def should_disable_button(mstate: Dict, side: str, seconds_remaining: int) -> bool:
    """Determine if a buy button should be disabled."""
    if seconds_remaining < NO_TRADE_SECONDS and seconds_remaining != 999:
        return True

    shares_up = mstate.get("shares_up", 0.0)
    shares_down = mstate.get("shares_down", 0.0)
    current_imbalance = abs(shares_up - shares_down)

    if shares_up > shares_down and side == "up" and current_imbalance >= WARN_IMBALANCE:
        return True
    if shares_down > shares_up and side == "down" and current_imbalance >= WARN_IMBALANCE:
        return True

    return False


def execute_market_buy(
    client: ClobClient,
    token_id: str,
    side: str,
    cost_usd: float,
    mstate: Dict,
    seconds_remaining: int,
    coin: str = ""
) -> Tuple[bool, str, float, float]:
    """Execute a market buy order with safety checks."""
    is_allowed, reason = check_safety(mstate, side, seconds_remaining)
    if not is_allowed:
        return False, reason, 0, 0

    try:
        ob = client.get_order_book(token_id)
        if not ob.asks:
            return False, "No asks available", 0, 0

        best_ask = float(ob.asks[0].price)
        exec_price = round(best_ask + PRICE_SLIPPAGE, 3)
        exec_price = min(exec_price, 0.99)

        size = cost_usd / best_ask
        if size < 0.01:
            return False, f"Order too small: {size:.4f} shares", 0, 0

        order_args = OrderArgs(
            token_id=token_id,
            price=exec_price,
            size=size,
            side="BUY"
        )

        response = client.create_and_post_order(order_args)

        if not response or "orderID" not in response:
            error_msg = response.get("error", "Unknown error") if response else "No response"
            return False, f"Order failed: {error_msg}", 0, 0

        order_id = response["orderID"]
        time.sleep(3)

        try:
            order_status = client.get_order(order_id)
            filled_size = float(order_status.get("size_matched", 0))
        except:
            filled_size = size

        if filled_size <= 0:
            return False, "Order not filled", 0, 0

        actual_cost = filled_size * exec_price

        trade_record = {
            "time": datetime.now(ET).strftime("%H:%M:%S"),
            "coin": coin,
            "side": side.upper(),
            "usdc": round(actual_cost, 2),
            "shares": round(filled_size, 2),
            "price": round(exec_price, 3)
        }

        if "trade_log" not in mstate:
            mstate["trade_log"] = []
        mstate["trade_log"].insert(0, trade_record)
        mstate["trade_log"] = mstate["trade_log"][:50]

        st.session_state.state["trade_log"].insert(0, trade_record)
        st.session_state.state["trade_log"] = st.session_state.state["trade_log"][:200]

        if side == "up":
            mstate["shares_up"] = mstate.get("shares_up", 0.0) + filled_size
            mstate["spent_up"] = mstate.get("spent_up", 0.0) + actual_cost
        else:
            mstate["shares_down"] = mstate.get("shares_down", 0.0) + filled_size
            mstate["spent_down"] = mstate.get("spent_down", 0.0) + actual_cost

        st.session_state.state["total_trades"] = st.session_state.state.get("total_trades", 0) + 1

        total_profit = get_total_locked_profit() + get_total_history_profit()
        st.session_state.state["equity_history"].append({
            "timestamp": datetime.now(ET).strftime("%H:%M:%S"),
            "total_profit": total_profit
        })

        return True, f"Bought {filled_size:.2f} {side.upper()} @ ${exec_price:.3f}", filled_size, actual_cost

    except Exception as e:
        return False, f"Execution error: {str(e)}", 0, 0


# =============================================================================
# COMPUTED METRICS
# =============================================================================

def calculate_metrics(mstate: Dict) -> Dict[str, Any]:
    """Calculate all position metrics for a market."""
    try:
        shares_up = mstate.get("shares_up", 0.0)
        shares_down = mstate.get("shares_down", 0.0)
        spent_up = mstate.get("spent_up", 0.0)
        spent_down = mstate.get("spent_down", 0.0)

        avg_up = spent_up / shares_up if shares_up > 0 else 0
        avg_down = spent_down / shares_down if shares_down > 0 else 0
        avg_pair_cost = avg_up + avg_down if (shares_up > 0 and shares_down > 0) else 0

        locked_shares = min(shares_up, shares_down)
        locked_profit = locked_shares * (1 - avg_pair_cost) if locked_shares > 0 and avg_pair_cost > 0 else 0

        unbalanced = abs(shares_up - shares_down)
        imbalance_side = "up" if shares_up > shares_down else ("down" if shares_down > shares_up else None)

        return {
            "avg_up": avg_up,
            "avg_down": avg_down,
            "avg_pair_cost": avg_pair_cost,
            "locked_shares": locked_shares,
            "locked_profit": locked_profit,
            "unbalanced": unbalanced,
            "imbalance_side": imbalance_side,
            "total_spent": spent_up + spent_down
        }
    except Exception:
        return {
            "avg_up": 0, "avg_down": 0, "avg_pair_cost": 0,
            "locked_shares": 0, "locked_profit": 0,
            "unbalanced": 0, "imbalance_side": None, "total_spent": 0
        }


def get_total_locked_profit() -> float:
    """Calculate total locked profit across all active markets."""
    total = 0.0
    try:
        markets = st.session_state.state.get("markets", {})
        for mstate in markets.values():
            try:
                shares_up = mstate.get("shares_up", 0.0)
                spent_up = mstate.get("spent_up", 0.0)
                shares_down = mstate.get("shares_down", 0.0)
                spent_down = mstate.get("spent_down", 0.0)

                avg_up = spent_up / shares_up if shares_up > 0 else 0
                avg_down = spent_down / shares_down if shares_down > 0 else 0

                pair_cost = avg_up + avg_down
                locked = min(shares_up, shares_down)
                total += locked * (1 - pair_cost)
            except:
                continue
    except:
        pass
    return round(total, 3)


def get_total_history_profit() -> float:
    """Calculate total profit from historical markets."""
    total = 0.0
    try:
        for entry in st.session_state.state.get("history", []):
            try:
                total += float(entry.get("locked_profit", 0))
            except:
                continue
    except:
        pass
    return round(total, 3)


def calculate_session_stats() -> Dict[str, Any]:
    """Calculate comprehensive session statistics."""
    state = st.session_state.state

    total_trades = state.get("total_trades", 0)
    history = state.get("history", [])
    trade_log = state.get("trade_log", [])

    total_profit = get_total_locked_profit() + get_total_history_profit()

    winning_markets = sum(1 for h in history if h.get("locked_profit", 0) > 0)
    total_markets = len(history)
    win_rate = (winning_markets / total_markets * 100) if total_markets > 0 else 0

    avg_pair_costs = []
    for mstate in state.get("markets", {}).values():
        m = calculate_metrics(mstate)
        if m["avg_pair_cost"] > 0:
            avg_pair_costs.append(m["avg_pair_cost"])
    avg_pair_cost = sum(avg_pair_costs) / len(avg_pair_costs) if avg_pair_costs else 0

    profits = [h.get("locked_profit", 0) for h in history]
    best_profit = max(profits) if profits else 0
    worst_profit = min(profits) if profits else 0

    total_volume = sum(t.get("usdc", 0) for t in trade_log)

    equity = state.get("equity_history", [])
    if equity:
        peak = 0
        max_dd = 0
        for e in equity:
            val = e.get("total_profit", 0)
            peak = max(peak, val)
            dd = peak - val
            max_dd = max(max_dd, dd)
    else:
        max_dd = 0

    return {
        "total_trades": total_trades,
        "total_profit": total_profit,
        "win_rate": win_rate,
        "avg_pair_cost": avg_pair_cost,
        "best_profit": best_profit,
        "worst_profit": worst_profit,
        "total_volume": total_volume,
        "max_drawdown": max_dd,
        "markets_completed": total_markets,
    }


# =============================================================================
# BACKUP/RESTORE FUNCTIONS
# =============================================================================

def export_state_json() -> str:
    """Export current state to JSON string."""
    return json.dumps(st.session_state.state, default=str, indent=2)


def import_state_json(json_str: str) -> bool:
    """Import state from JSON string."""
    try:
        data = json.loads(json_str)
        st.session_state.state.update(data)
        return True
    except Exception as e:
        st.error(f"Import failed: {e}")
        return False


# =============================================================================
# SIDEBAR - WALLET & CONTROLS
# =============================================================================

def render_sidebar():
    """Render professional sidebar with wallet and controls."""

    st.sidebar.markdown("""
    <div style='text-align: center; padding: 10px 0;'>
        <span style='font-size: 1.5em;'>💎</span>
        <span style='color: #00c853; font-weight: bold; font-size: 1.1em;'> WALLET</span>
    </div>
    """, unsafe_allow_html=True)

    if not st.session_state.wallet_connected:
        private_key_input = st.sidebar.text_input(
            "Private Key",
            type="password",
            help="Use a dedicated hot wallet"
        )

        rpc_url_input = st.sidebar.text_input(
            "Polygon RPC",
            value="https://polygon-rpc.com"
        )

        if st.sidebar.button("Connect Wallet", type="primary", use_container_width=True):
            pk = private_key_input.strip()
            if not pk.startswith("0x"):
                pk = "0x" + pk

            if len(pk) == 66:
                try:
                    Account.from_key(pk)
                    st.session_state.private_key = pk
                    st.session_state.rpc_url = rpc_url_input
                    st.session_state.wallet_connected = True
                    st.rerun()
                except Exception as e:
                    st.sidebar.error(f"Invalid key: {e}")
            else:
                st.sidebar.error("Key must be 64 hex characters")

        st.sidebar.markdown("---")
        st.sidebar.caption("gabagool style - Dec 2025")
        st.stop()

    try:
        wallet_addr = get_wallet_address()
        st.sidebar.markdown(f"""
        <div style='background: #161a22; padding: 15px; border-radius: 10px; border: 1px solid #2d333b; margin-bottom: 15px;'>
            <div style='color: #00c853; font-weight: bold;'>Connected</div>
            <div style='color: #888; font-family: monospace; font-size: 0.85em;'>{wallet_addr[:8]}...{wallet_addr[-6:]}</div>
        </div>
        """, unsafe_allow_html=True)
    except:
        st.sidebar.error("Invalid key")
        st.stop()

    st.sidebar.markdown("#### Balances")

    col1, col2 = st.sidebar.columns(2)

    usdc_bal = get_usdc_balance()
    matic_bal = get_matic_balance()

    with col1:
        if usdc_bal is not None:
            st.sidebar.markdown(f"""
            <div style='background: linear-gradient(145deg, #1b5e20, #2e7d32); padding: 12px; border-radius: 8px; text-align: center;'>
                <div style='color: #69f0ae; font-size: 1.4em; font-weight: bold;'>${usdc_bal:.2f}</div>
                <div style='color: #a5d6a7; font-size: 0.7em;'>USDC</div>
            </div>
            """, unsafe_allow_html=True)
        else:
            st.sidebar.warning("USDC: Error")

    with col2:
        if matic_bal is not None:
            st.sidebar.markdown(f"""
            <div style='background: #161a22; padding: 12px; border-radius: 8px; text-align: center; border: 1px solid #2d333b;'>
                <div style='color: #fff; font-size: 1.1em; font-weight: bold;'>{matic_bal:.4f}</div>
                <div style='color: #888; font-size: 0.7em;'>MATIC</div>
            </div>
            """, unsafe_allow_html=True)
        else:
            st.sidebar.warning("MATIC: Error")

    st.sidebar.markdown("---")

    st.sidebar.markdown("#### Data Management")

    backup_data = export_state_json()
    b64 = base64.b64encode(backup_data.encode()).decode()
    filename = f"polymarket_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"

    st.sidebar.markdown(f"""
    <a href="data:application/json;base64,{b64}" download="{filename}"
       style="display: block; text-align: center; background: #161a22; border: 1px solid #2d333b;
              padding: 10px; border-radius: 8px; color: #00c853; text-decoration: none;
              font-weight: bold; margin-bottom: 10px;">
        Download Backup
    </a>
    """, unsafe_allow_html=True)

    uploaded_file = st.sidebar.file_uploader("Restore Backup", type="json", label_visibility="collapsed")
    if uploaded_file:
        content = uploaded_file.read().decode()
        if st.sidebar.button("Restore Data", use_container_width=True):
            if import_state_json(content):
                st.sidebar.success("Restored!")
                st.rerun()

    st.sidebar.markdown("---")

    if st.sidebar.button("Disconnect", use_container_width=True):
        st.session_state.wallet_connected = False
        st.session_state.private_key = ""
        st.session_state.client = None
        st.rerun()

    st.sidebar.markdown("---")
    st.sidebar.caption("gabagool style - Dec 2025")
    st.sidebar.caption("4X PRINTING SEASON")


# =============================================================================
# HEADER BAR WITH BINANCE PRICES
# =============================================================================

def render_header(binance_data: Dict):
    """Render professional fixed header with stats and Binance prices."""
    stats = calculate_session_stats()
    total_profit = stats["total_profit"]
    profit_class = "profit-positive" if total_profit >= 0 else "profit-negative"
    profit_sign = "+" if total_profit >= 0 else ""

    # Build Binance ticker string
    ticker_html = ""
    for coin in ["BTC", "ETH", "SOL", "XRP"]:
        price_html = format_binance_price(coin, binance_data)
        if price_html:
            ticker_html += f"<span style='margin-right: 20px;'><span style='color: #888;'>{coin}:</span> {price_html}</span>"

    st.markdown(f"""
    <div style='background: linear-gradient(180deg, #161a22 0%, #0e1117 100%);
                border-bottom: 2px solid #00c853; padding: 20px 30px; margin: -1rem -1rem 1.5rem -1rem;
                box-shadow: 0 4px 30px rgba(0,200,83,0.2);'>
        <div style='display: flex; justify-content: space-between; align-items: center; flex-wrap: wrap; gap: 20px;'>
            <div>
                <div style='color: #00c853; font-size: 1.6em; font-weight: 800; letter-spacing: 3px;
                            text-shadow: 0 0 30px rgba(0,200,83,0.4);'>
                    POLYMARKET 15-MIN COMBO PRINTER
                </div>
                <div style='color: #888; font-size: 0.85em; letter-spacing: 1px; margin-top: 5px;'>
                    {ticker_html if ticker_html else "BTC • ETH • SOL • XRP — GABAGOOL STYLE"}
                </div>
            </div>
            <div style='display: flex; gap: 40px; align-items: center;'>
                <div style='text-align: center;'>
                    <div class='{profit_class}' style='font-size: 2em; font-weight: 800;'>
                        {profit_sign}${abs(total_profit):.2f}
                    </div>
                    <div style='color: #888; font-size: 0.7em; text-transform: uppercase; letter-spacing: 1px;'>
                        Total Locked
                    </div>
                </div>
                <div style='text-align: center;'>
                    <div style='color: #fff; font-size: 1.5em; font-weight: 700;'>{stats["total_trades"]}</div>
                    <div style='color: #888; font-size: 0.7em; text-transform: uppercase; letter-spacing: 1px;'>
                        Trades
                    </div>
                </div>
                <div style='text-align: center;'>
                    <div style='color: #fff; font-size: 1.5em; font-weight: 700;'>
                        ${stats["avg_pair_cost"]:.4f}
                    </div>
                    <div style='color: #888; font-size: 0.7em; text-transform: uppercase; letter-spacing: 1px;'>
                        Avg Pair Cost
                    </div>
                </div>
                <div style='text-align: center;'>
                    <div style='color: #ffc107; font-size: 1.5em; font-weight: 700;'>
                        {stats["win_rate"]:.0f}%
                    </div>
                    <div style='color: #888; font-size: 0.7em; text-transform: uppercase; letter-spacing: 1px;'>
                        Win Rate
                    </div>
                </div>
            </div>
        </div>
    </div>
    """, unsafe_allow_html=True)


# =============================================================================
# DASHBOARD TAB WITH BINANCE PRICES
# =============================================================================

def render_dashboard_tab(all_markets: List[Dict], client: ClobClient, binance_data: Dict):
    """Render main dashboard with overview of all markets."""

    st.markdown("### Market Overview")

    cols = st.columns(4)

    for i, market in enumerate(all_markets):
        coin = market["coin"]
        is_active = market.get("active", False)

        with cols[i]:
            # Use prices from market dict (from Gamma API tokens), NOT client.get_midpoint
            if is_active:
                up_price = market.get("up_price", 0.5)
                down_price = market.get("down_price", 0.5)
                pair_cost = up_price + down_price
            else:
                pair_cost = 0

            if is_active and market.get("condition_id"):
                mstate = get_market_state(market["condition_id"], coin)
                metrics = calculate_metrics(mstate)
            else:
                metrics = {"locked_profit": 0, "locked_shares": 0}

            coin_colors = {"BTC": "#f7931a", "ETH": "#627eea", "SOL": "#00ffa3", "XRP": "#888"}
            coin_color = coin_colors.get(coin, "#888")

            status_color = "#00c853" if is_active else "#ffc107"
            status_text = "LIVE" if is_active else "WAITING"

            pair_cost_color = "#00c853" if pair_cost < 0.98 else ("#ffc107" if pair_cost <= 0.985 else "#ff5252")

            profit = metrics["locked_profit"]
            profit_color = "#00c853" if profit >= 0 else "#ff5252"
            profit_sign = "+" if profit >= 0 else ""

            # Binance price for this coin
            binance_html = format_binance_price(coin, binance_data)

            st.markdown(f"""
            <div style='background: linear-gradient(145deg, #161a22, #1a1f2a);
                        border: 1px solid #2d333b; border-left: 4px solid {status_color};
                        border-radius: 12px; padding: 20px; text-align: center;
                        transition: all 0.3s ease;'>
                <div style='color: {coin_color}; font-size: 2.2em; font-weight: 800;'>{coin}</div>
                <div style='font-size: 0.9em; margin: 5px 0;'>{binance_html if binance_html else ""}</div>
                <div style='color: {status_color}; font-size: 0.8em; font-weight: 600;
                            letter-spacing: 2px; margin: 5px 0;'>{status_text}</div>
                {"<div style='color: " + pair_cost_color + "; font-size: 1.8em; font-weight: 700; margin: 15px 0;'>$" + f"{pair_cost:.4f}" + "</div>" if is_active else "<div style='color: #888; font-size: 1.2em; margin: 15px 0;'>---</div>"}
                <div style='color: #888; font-size: 0.75em; text-transform: uppercase;'>Pair Cost</div>
                <div style='border-top: 1px solid #2d333b; margin-top: 15px; padding-top: 15px;'>
                    <div style='color: {profit_color}; font-size: 1.3em; font-weight: 700;'>
                        {profit_sign}${abs(profit):.2f}
                    </div>
                    <div style='color: #888; font-size: 0.7em;'>Locked Profit</div>
                </div>
            </div>
            """, unsafe_allow_html=True)

    st.markdown("---")

    col1, col2 = st.columns([2, 1])

    with col1:
        st.markdown("### Equity Curve")
        equity_history = st.session_state.state.get("equity_history", [])
        fig = create_equity_curve(equity_history, height=350)
        st.plotly_chart(fig, use_container_width=True, config={'displayModeBar': False})

    with col2:
        st.markdown("### Session Stats")
        stats = calculate_session_stats()

        st.markdown(f"""
        <div style='background: #161a22; border: 1px solid #2d333b; border-radius: 10px; padding: 15px;'>
            <div style='display: flex; justify-content: space-between; padding: 10px 0; border-bottom: 1px solid #2d333b;'>
                <span style='color: #888;'>Markets Completed</span>
                <span style='color: #fff; font-weight: bold;'>{stats["markets_completed"]}</span>
            </div>
            <div style='display: flex; justify-content: space-between; padding: 10px 0; border-bottom: 1px solid #2d333b;'>
                <span style='color: #888;'>Total Volume</span>
                <span style='color: #fff; font-weight: bold;'>${stats["total_volume"]:.2f}</span>
            </div>
            <div style='display: flex; justify-content: space-between; padding: 10px 0; border-bottom: 1px solid #2d333b;'>
                <span style='color: #888;'>Best Market</span>
                <span style='color: #00c853; font-weight: bold;'>+${stats["best_profit"]:.2f}</span>
            </div>
            <div style='display: flex; justify-content: space-between; padding: 10px 0; border-bottom: 1px solid #2d333b;'>
                <span style='color: #888;'>Worst Market</span>
                <span style='color: #ff5252; font-weight: bold;'>${stats["worst_profit"]:.2f}</span>
            </div>
            <div style='display: flex; justify-content: space-between; padding: 10px 0;'>
                <span style='color: #888;'>Max Drawdown</span>
                <span style='color: #ffc107; font-weight: bold;'>${stats["max_drawdown"]:.2f}</span>
            </div>
        </div>
        """, unsafe_allow_html=True)

    st.markdown("### Recent Trades")
    trade_log = st.session_state.state.get("trade_log", [])[:10]

    if trade_log:
        df = pd.DataFrame(trade_log)
        st.dataframe(df, use_container_width=True, hide_index=True)
    else:
        st.info("No trades yet. Start trading to see activity here.")


# =============================================================================
# MARKET TAB UI WITH LIVE PRICES FROM GAMMA API
# =============================================================================

def render_market_tab(market: Dict, client: ClobClient, binance_data: Dict):
    """Render full trading interface for a single market."""
    coin = market["coin"]

    # Waiting state
    if not market.get("active", False) or market.get("condition_id") is None:
        st.markdown(f"""
        <div style='background: linear-gradient(145deg, #161a22, #1a1f2a);
                    border: 1px solid #ffc107; border-radius: 20px;
                    padding: 60px; text-align: center; margin: 40px 0;'>
            <div style='font-size: 4em; margin-bottom: 20px;'>⏳</div>
            <h2 style='color: #ffc107; margin-bottom: 15px;'>{coin} - Waiting for Next Window</h2>
            <p style='color: #888; font-size: 1.1em;'>Next 15-minute market starting soon</p>
            <p style='color: #555; font-size: 0.9em; margin-top: 20px;'>
                Markets refresh every 15 minutes. Short gaps during oracle finalization are normal.
            </p>
        </div>
        """, unsafe_allow_html=True)

        now = datetime.now(ET)
        st.markdown(f"""
        <div style='text-align: center; color: #888;'>
            Current Time (ET): <span style='color: #fff; font-weight: bold;'>{now.strftime("%I:%M:%S %p")}</span>
        </div>
        """, unsafe_allow_html=True)
        return

    condition_id = market["condition_id"]
    mstate = get_market_state(condition_id, coin)
    seconds_remaining = get_seconds_remaining(market.get("end_time"))

    # Binance price display
    binance_html = format_binance_price(coin, binance_data)

    # Header row: Question + Countdown + Binance Price
    col1, col2 = st.columns([3, 1])

    with col1:
        st.markdown(f"**{market['question']}**")
        if binance_html:
            st.markdown(f"<div style='margin-top: 5px;'>Binance: {binance_html}</div>", unsafe_allow_html=True)

    with col2:
        if seconds_remaining < 999:
            if seconds_remaining < NO_TRADE_SECONDS:
                countdown_class = "countdown-danger"
            elif seconds_remaining < 180:
                countdown_class = "countdown-warning"
            else:
                countdown_class = "countdown-live"

            st.markdown(f"""
            <div class='countdown {countdown_class}'>{format_countdown(seconds_remaining)}</div>
            """, unsafe_allow_html=True)

            if seconds_remaining < NO_TRADE_SECONDS:
                st.error("TRADING DISABLED")
        else:
            st.markdown("""
            <div class='countdown countdown-live'>LIVE</div>
            """, unsafe_allow_html=True)

    st.markdown("---")

    # ==========================================================================
    # CRITICAL: Use prices from market dict (Gamma API tokens), NOT get_midpoint
    # ==========================================================================
    mid_up = market.get("up_price", 0.5)
    mid_down = market.get("down_price", 0.5)
    pair_cost = mid_up + mid_down

    # Get ask prices from order book for execution display
    ask_up = get_order_book_ask(client, market["up_token_id"])
    ask_down = get_order_book_ask(client, market["down_token_id"])

    # Pair cost display
    if pair_cost < 0.98:
        cost_class = "pair-cost-good"
    elif pair_cost <= 0.985:
        cost_class = "pair-cost-marginal"
    else:
        cost_class = "pair-cost-bad"

    st.markdown(f"""
    <div class='pair-cost-display {cost_class}'>
        PAIR COST: ${pair_cost:.4f}
    </div>
    """, unsafe_allow_html=True)

    # Price cards
    col1, col2 = st.columns(2)

    with col1:
        st.markdown(f"""
        <div style='background: linear-gradient(145deg, #1b5e20, #2e7d32);
                    border: 1px solid #4caf50; border-radius: 10px; padding: 20px; text-align: center;'>
            <div style='color: #a5d6a7; font-size: 0.8em; text-transform: uppercase;'>UP Price</div>
            <div style='color: #69f0ae; font-size: 2em; font-weight: 800;'>${mid_up:.4f}</div>
            <div style='color: #a5d6a7; font-size: 0.9em;'>Ask: ${ask_up:.4f}</div>
        </div>
        """, unsafe_allow_html=True)

    with col2:
        st.markdown(f"""
        <div style='background: linear-gradient(145deg, #b71c1c, #c62828);
                    border: 1px solid #f44336; border-radius: 10px; padding: 20px; text-align: center;'>
            <div style='color: #ffcdd2; font-size: 0.8em; text-transform: uppercase;'>DOWN Price</div>
            <div style='color: #ff8a80; font-size: 2em; font-weight: 800;'>${mid_down:.4f}</div>
            <div style='color: #ffcdd2; font-size: 0.9em;'>Ask: ${ask_down:.4f}</div>
        </div>
        """, unsafe_allow_html=True)

    st.markdown("---")

    # Position stats
    metrics = calculate_metrics(mstate)

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric("UP Shares", f"{mstate.get('shares_up', 0):.2f}")
        if mstate.get('shares_up', 0) > 0:
            st.caption(f"Avg: ${metrics['avg_up']:.4f}")

    with col2:
        st.metric("DOWN Shares", f"{mstate.get('shares_down', 0):.2f}")
        if mstate.get('shares_down', 0) > 0:
            st.caption(f"Avg: ${metrics['avg_down']:.4f}")

    with col3:
        st.metric("Locked Pairs", f"{metrics['locked_shares']:.2f}")
        profit = metrics['locked_profit']
        profit_color = "#00c853" if profit >= 0 else "#ff5252"
        profit_sign = "+" if profit >= 0 else ""
        st.markdown(f"<span style='color: {profit_color}; font-size: 1.5em; font-weight: bold;'>{profit_sign}${abs(profit):.2f}</span>", unsafe_allow_html=True)

    with col4:
        st.metric("Imbalance", f"{metrics['unbalanced']:.2f}")
        if metrics['imbalance_side']:
            st.caption(f"Heavy: {metrics['imbalance_side'].upper()}")
        if metrics['unbalanced'] >= WARN_IMBALANCE:
            st.warning("HIGH!")

    st.markdown("---")

    # Buy buttons
    st.markdown("### Trade")

    if pair_cost > 0.99:
        st.error("Pair cost > $0.99 - Avoid trading!")

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("**BUY UP**")
        up_disabled = should_disable_button(mstate, "up", seconds_remaining)

        cols = st.columns(len(BUY_AMOUNTS))
        for i, amount in enumerate(BUY_AMOUNTS):
            with cols[i]:
                if st.button(
                    f"${amount}",
                    key=f"buy_up_{amount}_{condition_id}",
                    disabled=up_disabled,
                    use_container_width=True
                ):
                    with st.spinner(f"Buying ${amount} UP..."):
                        success, msg, _, _ = execute_market_buy(
                            client, market["up_token_id"], "up", amount,
                            mstate, seconds_remaining, coin
                        )
                        if success:
                            st.success(msg)
                        else:
                            st.error(msg)
                        time.sleep(1)
                        st.rerun()

    with col2:
        st.markdown("**BUY DOWN**")
        down_disabled = should_disable_button(mstate, "down", seconds_remaining)

        cols = st.columns(len(BUY_AMOUNTS))
        for i, amount in enumerate(BUY_AMOUNTS):
            with cols[i]:
                if st.button(
                    f"${amount}",
                    key=f"buy_down_{amount}_{condition_id}",
                    disabled=down_disabled,
                    use_container_width=True
                ):
                    with st.spinner(f"Buying ${amount} DOWN..."):
                        success, msg, _, _ = execute_market_buy(
                            client, market["down_token_id"], "down", amount,
                            mstate, seconds_remaining, coin
                        )
                        if success:
                            st.success(msg)
                        else:
                            st.error(msg)
                        time.sleep(1)
                        st.rerun()

    # Market trade log
    trade_log = mstate.get("trade_log", [])
    if trade_log:
        st.markdown("---")
        st.markdown("**Recent Trades**")
        df = pd.DataFrame(trade_log[:10])
        st.dataframe(df, use_container_width=True, hide_index=True)


# =============================================================================
# JOURNAL TAB
# =============================================================================

def render_journal_tab():
    """Render full trade journal with filtering."""
    st.markdown("### Trade Journal")

    trade_log = st.session_state.state.get("trade_log", [])

    if not trade_log:
        st.info("No trades recorded yet. Start trading to build your journal.")
        return

    col1, col2, col3 = st.columns([1, 1, 2])

    with col1:
        coin_filter = st.selectbox("Filter by Coin", ["All", "BTC", "ETH", "SOL", "XRP"])

    with col2:
        side_filter = st.selectbox("Filter by Side", ["All", "UP", "DOWN"])

    filtered = trade_log
    if coin_filter != "All":
        filtered = [t for t in filtered if t.get("coin") == coin_filter]
    if side_filter != "All":
        filtered = [t for t in filtered if t.get("side") == side_filter]

    total_volume = sum(t.get("usdc", 0) for t in filtered)
    avg_price = sum(t.get("price", 0) for t in filtered) / len(filtered) if filtered else 0

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total Trades", len(filtered))
    col2.metric("Total Volume", f"${total_volume:.2f}")
    col3.metric("Avg Price", f"${avg_price:.4f}")
    col4.metric("Total Shares", f"{sum(t.get('shares', 0) for t in filtered):.2f}")

    st.markdown("---")

    if filtered:
        df = pd.DataFrame(filtered)
        st.dataframe(df, use_container_width=True, hide_index=True, height=500)

        csv = df.to_csv(index=False)
        b64 = base64.b64encode(csv.encode()).decode()
        filename = f"trade_journal_{datetime.now().strftime('%Y%m%d')}.csv"

        st.markdown(f"""
        <a href="data:text/csv;base64,{b64}" download="{filename}"
           style="display: inline-block; background: #161a22; border: 1px solid #00c853;
                  padding: 10px 20px; border-radius: 8px; color: #00c853; text-decoration: none;
                  font-weight: bold;">
            Export to CSV
        </a>
        """, unsafe_allow_html=True)
    else:
        st.warning("No trades match the selected filters.")


# =============================================================================
# STATS TAB
# =============================================================================

def render_stats_tab():
    """Render comprehensive statistics dashboard."""
    st.markdown("### Performance Statistics")

    stats = calculate_session_stats()

    col1, col2, col3, col4, col5 = st.columns(5)

    profit_color = "#00c853" if stats["total_profit"] >= 0 else "#ff5252"
    profit_sign = "+" if stats["total_profit"] >= 0 else ""

    with col1:
        st.markdown(f"""
        <div style='background: #161a22; border: 1px solid #2d333b; border-radius: 10px; padding: 20px; text-align: center;'>
            <div style='color: {profit_color}; font-size: 2em; font-weight: 800;'>{profit_sign}${abs(stats["total_profit"]):.2f}</div>
            <div style='color: #888; font-size: 0.75em; text-transform: uppercase;'>Total P&L</div>
        </div>
        """, unsafe_allow_html=True)

    with col2:
        st.markdown(f"""
        <div style='background: #161a22; border: 1px solid #2d333b; border-radius: 10px; padding: 20px; text-align: center;'>
            <div style='color: #fff; font-size: 2em; font-weight: 800;'>{stats["total_trades"]}</div>
            <div style='color: #888; font-size: 0.75em; text-transform: uppercase;'>Total Trades</div>
        </div>
        """, unsafe_allow_html=True)

    with col3:
        st.markdown(f"""
        <div style='background: #161a22; border: 1px solid #2d333b; border-radius: 10px; padding: 20px; text-align: center;'>
            <div style='color: #ffc107; font-size: 2em; font-weight: 800;'>{stats["win_rate"]:.1f}%</div>
            <div style='color: #888; font-size: 0.75em; text-transform: uppercase;'>Win Rate</div>
        </div>
        """, unsafe_allow_html=True)

    with col4:
        st.markdown(f"""
        <div style='background: #161a22; border: 1px solid #2d333b; border-radius: 10px; padding: 20px; text-align: center;'>
            <div style='color: #00c853; font-size: 2em; font-weight: 800;'>+${stats["best_profit"]:.2f}</div>
            <div style='color: #888; font-size: 0.75em; text-transform: uppercase;'>Best Market</div>
        </div>
        """, unsafe_allow_html=True)

    with col5:
        st.markdown(f"""
        <div style='background: #161a22; border: 1px solid #2d333b; border-radius: 10px; padding: 20px; text-align: center;'>
            <div style='color: #ff5252; font-size: 2em; font-weight: 800;'>${stats["max_drawdown"]:.2f}</div>
            <div style='color: #888; font-size: 0.75em; text-transform: uppercase;'>Max Drawdown</div>
        </div>
        """, unsafe_allow_html=True)

    st.markdown("---")

    st.markdown("### Equity Curve")
    equity_history = st.session_state.state.get("equity_history", [])
    fig = create_equity_curve(equity_history, height=400)
    st.plotly_chart(fig, use_container_width=True, config={'displayModeBar': False})

    st.markdown("---")

    st.markdown("### Completed Markets")

    history = st.session_state.state.get("history", [])

    if history:
        df = pd.DataFrame(history[:50])
        st.dataframe(df, use_container_width=True, hide_index=True)
    else:
        st.info("No completed markets yet.")

    st.markdown("---")

    st.markdown("### Session Info")

    session_start = st.session_state.state.get("session_start", "Unknown")

    col1, col2 = st.columns(2)

    with col1:
        st.markdown(f"""
        <div style='background: #161a22; border: 1px solid #2d333b; border-radius: 10px; padding: 15px;'>
            <div style='color: #888; font-size: 0.8em;'>Session Started</div>
            <div style='color: #fff; font-weight: bold;'>{session_start}</div>
        </div>
        """, unsafe_allow_html=True)

    with col2:
        st.markdown(f"""
        <div style='background: #161a22; border: 1px solid #2d333b; border-radius: 10px; padding: 15px;'>
            <div style='color: #888; font-size: 0.8em;'>Total Volume Traded</div>
            <div style='color: #fff; font-weight: bold;'>${stats["total_volume"]:.2f}</div>
        </div>
        """, unsafe_allow_html=True)


# =============================================================================
# MAIN APPLICATION
# =============================================================================

def main():
    """Main Streamlit application."""

    # Render sidebar
    render_sidebar()

    state = st.session_state.state

    # Check if approvals already exist on-chain (auto-detect)
    if not state.get("allowance_approved", False):
        with st.spinner("Checking existing approvals..."):
            if check_existing_approvals():
                st.session_state.state["allowance_approved"] = True
                st.rerun()

    # Setup check - only show if approvals don't exist on-chain
    if not state.get("allowance_approved", False):
        st.warning("First-time setup required: Approve token spending")

        col1, col2, col3 = st.columns([1, 2, 1])
        with col2:
            if st.button("SETUP: Approve Allowances", type="primary", use_container_width=True):
                with st.spinner("Sending approval transactions..."):
                    if approve_all_contracts():
                        st.rerun()

        st.stop()

    # Get CLOB client
    client = get_clob_client()
    if client is None:
        st.error("Failed to initialize trading client.")
        st.stop()

    # Fetch Binance data every refresh
    binance_data = get_binance_data()
    st.session_state.binance_data = binance_data

    # Render header with Binance prices
    render_header(binance_data)

    # Get all markets with live prices from Gamma API tokens
    all_markets = find_all_active_updown_markets()

    # Archive old markets
    active_ids = [m["condition_id"] for m in all_markets if m.get("condition_id")]
    archive_old_markets(active_ids)

    # Main tabs
    tab_names = ["DASHBOARD"]
    for m in all_markets:
        status = "🟢" if m.get("active", False) else "⏳"
        tab_names.append(f"{status} {m['coin']} 15m")
    tab_names.extend(["JOURNAL", "STATS"])

    tabs = st.tabs(tab_names)

    # Dashboard tab
    with tabs[0]:
        render_dashboard_tab(all_markets, client, binance_data)

    # Market tabs
    for i, market in enumerate(all_markets):
        with tabs[i + 1]:
            render_market_tab(market, client, binance_data)

    # Journal tab
    with tabs[-2]:
        render_journal_tab()

    # Stats tab
    with tabs[-1]:
        render_stats_tab()

    # Refresh
    st.markdown("---")

    col1, col2, col3 = st.columns([1, 1, 1])
    with col2:
        if st.button("Refresh Now", use_container_width=True):
            st.rerun()

    # Auto-refresh
    time.sleep(REFRESH_INTERVAL)
    st.rerun()


if __name__ == "__main__":
    main()
