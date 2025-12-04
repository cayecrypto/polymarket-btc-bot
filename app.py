"""
================================================================================
POLYMARKET 15-MIN COMBO PRINTER — GOD MODE TERMINAL
================================================================================

A single-screen, zero-scroll hedge fund terminal for Polymarket's Up/Down
15-minute prediction markets. Maximum density, maximum alpha.

SUPPORTED MARKETS:
    - Bitcoin (BTC)
    - Ethereum (ETH)
    - Solana (SOL)
    - XRP

STRATEGY:
    Buy both UP and DOWN tokens to lock in guaranteed profit when pair cost < $1.
    Each pair pays out exactly $1 at resolution regardless of outcome.

gabagool style - Dec 2025 - 4X PRINTING SEASON with the bros
================================================================================
"""

import os
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
from web3 import Web3
from eth_account import Account

# py-clob-client imports
from py_clob_client.client import ClobClient
from py_clob_client.clob_types import OrderArgs, ApiCreds

# Monkey-patch httpx to add browser headers (bypass Cloudflare)
import httpx
_original_httpx_client_init = httpx.Client.__init__
def _patched_httpx_client_init(self, *args, **kwargs):
    _original_httpx_client_init(self, *args, **kwargs)
    self.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Origin": "https://polymarket.com",
        "Referer": "https://polymarket.com/",
        "Sec-Ch-Ua": '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
        "Sec-Ch-Ua-Mobile": "?0",
        "Sec-Ch-Ua-Platform": '"Windows"',
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-site",
    })
httpx.Client.__init__ = _patched_httpx_client_init

# =============================================================================
# PAGE CONFIGURATION - FULLSCREEN GOD MODE
# =============================================================================

st.set_page_config(
    page_title="POLYMARKET TERMINAL",
    page_icon="💎",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# =============================================================================
# DARK TERMINAL CSS - HEDGE FUND AESTHETIC
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

    /* HIDE STREAMLIT CHROME - BUT KEEP SIDEBAR TOGGLE */
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

    /* Keep header for sidebar toggle but make it minimal and visible */
    [data-testid="stHeader"] {
        background: #0a0f0d !important;
        height: auto !important;
        min-height: 2.5rem !important;
        z-index: 999 !important;
    }

    /* Style ALL possible sidebar toggle buttons - make them VERY visible */
    [data-testid="stHeader"] button[kind="header"],
    [data-testid="collapsedControl"],
    button[data-testid="stSidebarCollapseButton"],
    [data-testid="stHeader"] button {
        color: #00ff6a !important;
        background: #0d2818 !important;
        border: 1px solid #1a5c35 !important;
        border-radius: 4px !important;
        opacity: 1 !important;
        visibility: visible !important;
    }

    /* Collapsed sidebar expand button */
    [data-testid="collapsedControl"] {
        left: 0.5rem !important;
        top: 0.5rem !important;
    }

    [data-testid="collapsedControl"] svg {
        fill: #00ff6a !important;
        stroke: #00ff6a !important;
    }

    /* Main content area - NO overflow hidden, let content flow */
    [data-testid="stAppViewContainer"] {
        background: transparent !important;
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

    /* MAIN CONTAINER */
    .main-container {
        display: flex;
        padding-top: 56px;
        height: calc(100vh - 40px);
        gap: 16px;
        padding: 56px 16px 8px 16px;
    }

    /* LEFT COLUMN - 33% - MARKET CARDS */
    .left-column {
        width: 33%;
        display: flex;
        flex-direction: column;
        gap: 8px;
        height: 100%;
    }

    /* MARKET CARD - COMPACT */
    .market-card {
        background: linear-gradient(145deg, #111916 0%, #0f1512 100%);
        border: 1px solid #1a3025;
        border-radius: 8px;
        padding: 12px 16px;
        flex: 1;
        display: flex;
        flex-direction: column;
        transition: all 0.2s ease;
    }

    .market-card:hover {
        border-color: #2a5035;
        box-shadow: 0 0 30px rgba(0, 255, 106, 0.1);
    }

    .market-card.edge {
        border-color: #00ff6a;
        box-shadow: 0 0 20px rgba(0, 255, 106, 0.2);
    }

    .market-card.auto-flash {
        animation: auto-flash 0.5s ease-out;
    }

    @keyframes auto-flash {
        0% { border-color: #00ff6a; box-shadow: 0 0 40px rgba(0, 255, 106, 0.8); }
        100% { border-color: #1a3025; box-shadow: none; }
    }

    .market-card-header {
        display: flex;
        justify-content: space-between;
        align-items: center;
        margin-bottom: 8px;
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

    .coin-price {
        font-size: 12px;
        color: #7a9a8a;
    }

    .coin-change {
        font-size: 11px;
        font-weight: 600;
    }

    .coin-change.up { color: #00ff6a; }
    .coin-change.down { color: #ff4d4d; }

    .market-locked {
        text-align: right;
    }

    .locked-value {
        font-family: 'JetBrains Mono', monospace;
        font-weight: 700;
        font-size: 14px;
        color: #00ff6a;
    }

    .locked-label {
        font-size: 9px;
        color: #5a8a6a;
        text-transform: uppercase;
        letter-spacing: 1px;
    }

    /* PAIR COST ROW */
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

    .locked-inline {
        font-family: 'JetBrains Mono', monospace;
        font-size: 12px;
        color: #00ff6a;
        font-weight: 600;
    }

    /* COUNTDOWN TIMER */
    .market-countdown {
        text-align: right;
    }

    .countdown-value {
        font-family: 'JetBrains Mono', monospace;
        font-size: 18px;
        font-weight: 700;
    }

    .countdown-label {
        font-size: 9px;
        color: #5a8a6a;
        text-transform: uppercase;
        letter-spacing: 1px;
    }

    .countdown-normal .countdown-value {
        color: #00ff6a;
    }

    .countdown-urgent .countdown-value {
        color: #ff4d4d;
        animation: pulse-urgent 1s infinite;
    }

    .countdown-inactive .countdown-value {
        color: #5a6a5a;
    }

    @keyframes pulse-urgent {
        0%, 100% { opacity: 1; }
        50% { opacity: 0.5; }
    }

    /* LIVE CRYPTO PRICE */
    .coin-live-price {
        font-family: 'JetBrains Mono', monospace;
        font-size: 12px;
        color: #e0e0e0;
        margin-left: 8px;
    }

    .coin-change {
        font-family: 'JetBrains Mono', monospace;
        font-size: 11px;
        margin-left: 4px;
    }

    .coin-change.up { color: #00ff6a; }
    .coin-change.down { color: #ff4d4d; }

    /* PRICE ROW */
    .price-row {
        display: flex;
        justify-content: space-between;
        align-items: center;
        font-size: 12px;
        color: #7a9a8a;
        margin-bottom: 4px;
    }

    .price-up { color: #00cc55; }
    .price-down { color: #ff6b6b; }
    .imbal { color: #ffd93d; }

    /* POSITION ROW */
    .position-row {
        display: flex;
        justify-content: space-between;
        align-items: center;
        font-size: 11px;
        color: #7a9a8a;
        margin-top: 6px;
        padding-top: 6px;
        border-top: 1px solid rgba(26, 48, 37, 0.5);
    }

    .pos-label {
        font-size: 10px;
        color: #5a8a6a;
        font-weight: 600;
    }

    .pos-up {
        font-family: 'JetBrains Mono', monospace;
        color: #00cc55;
        font-size: 10px;
    }

    .pos-down {
        font-family: 'JetBrains Mono', monospace;
        color: #ff6b6b;
        font-size: 10px;
    }

    /* BUY BUTTONS */
    .btn-row {
        display: flex;
        gap: 6px;
        margin-top: 8px;
    }

    .buy-btn {
        flex: 1;
        padding: 6px 4px;
        background: linear-gradient(145deg, #0f2018, #142820);
        border: 1px solid #1a4030;
        border-radius: 4px;
        color: #00ff6a;
        font-family: 'JetBrains Mono', monospace;
        font-size: 11px;
        font-weight: 600;
        cursor: pointer;
        transition: all 0.15s ease;
    }

    .buy-btn:hover {
        background: linear-gradient(145deg, #1a3528, #204030);
        border-color: #00ff6a;
        box-shadow: 0 0 15px rgba(0, 255, 106, 0.2);
    }

    .buy-btn:disabled {
        opacity: 0.3;
        cursor: not-allowed;
    }

    /* RIGHT COLUMN - 67% */
    .right-column {
        width: 67%;
        display: flex;
        flex-direction: column;
        gap: 12px;
        height: 100%;
    }

    /* EQUITY CHART */
    .equity-panel {
        background: linear-gradient(145deg, #111916 0%, #0f1512 100%);
        border: 1px solid #1a3025;
        border-radius: 8px;
        padding: 12px 16px;
        flex: 1.2;
        display: flex;
        flex-direction: column;
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

    /* OPPORTUNITIES LIST */
    .opportunities-panel {
        background: linear-gradient(145deg, #111916 0%, #0f1512 100%);
        border: 1px solid #1a3025;
        border-radius: 8px;
        padding: 12px 16px;
        flex: 1;
        overflow-y: auto;
    }

    .opp-row {
        display: flex;
        justify-content: space-between;
        align-items: center;
        padding: 6px 0;
        border-bottom: 1px solid rgba(26, 48, 37, 0.5);
        font-size: 12px;
    }

    .opp-row:last-child {
        border-bottom: none;
    }

    .opp-time {
        font-family: 'JetBrains Mono', monospace;
        color: #5a8a6a;
        font-size: 11px;
    }

    .opp-coin {
        font-weight: 700;
        color: #e0e0e0;
    }

    .opp-pair {
        font-family: 'JetBrains Mono', monospace;
        color: #00ff6a;
        font-weight: 600;
    }

    .opp-edge {
        font-family: 'JetBrains Mono', monospace;
        color: #ffd93d;
        font-size: 11px;
    }

    .opp-best {
        background: rgba(0, 255, 106, 0.1);
        padding: 2px 6px;
        border-radius: 3px;
        font-size: 9px;
        color: #00ff6a;
        font-weight: 700;
    }

    .missed-profit {
        margin-top: 8px;
        padding-top: 8px;
        border-top: 1px solid #1a3025;
        font-size: 11px;
        color: #7a9a8a;
    }

    .missed-value {
        color: #ffd93d;
        font-weight: 700;
    }

    .cumulative-missed {
        margin-top: 6px;
        padding: 8px;
        background: linear-gradient(145deg, #2a1a1a, #1a1010);
        border: 1px solid #ff6b6b;
        border-radius: 4px;
        font-size: 12px;
        font-weight: 700;
        color: #ff6b6b;
        text-align: center;
    }

    .cumulative-value {
        color: #ff4d4d;
        font-weight: 800;
        font-size: 14px;
    }

    /* BOTTOM TICKER */
    .bottom-ticker {
        background: linear-gradient(90deg, #0d1810 0%, #0f1f18 50%, #0d1810 100%);
        border: 1px solid #1a3025;
        border-radius: 8px;
        padding: 8px 0;
        overflow: hidden;
        margin-top: 16px;
    }

    .ticker-content {
        display: flex;
        animation: scroll 30s linear infinite;
        white-space: nowrap;
    }

    @keyframes scroll {
        0% { transform: translateX(0); }
        100% { transform: translateX(-50%); }
    }

    .ticker-item {
        display: inline-flex;
        align-items: center;
        gap: 8px;
        padding: 0 24px;
        font-size: 11px;
        color: #7a9a8a;
    }

    .ticker-time {
        font-family: 'JetBrains Mono', monospace;
        color: #5a8a6a;
    }

    .ticker-coin {
        font-weight: 700;
        color: #e0e0e0;
    }

    .ticker-amount {
        color: #00cc55;
    }

    .ticker-profit {
        font-family: 'JetBrains Mono', monospace;
        color: #00ff6a;
        font-weight: 700;
    }

    /* SIDEBAR OVERRIDES */
    [data-testid="stSidebar"] {
        background: linear-gradient(180deg, #0a0f0d 0%, #0d1210 100%) !important;
        border-right: 1px solid #1a3025 !important;
    }

    [data-testid="stSidebar"] * {
        color: #e0e0e0 !important;
    }

    [data-testid="stSidebar"] input {
        background: #111916 !important;
        border: 1px solid #1a3025 !important;
        color: #e0e0e0 !important;
    }

    [data-testid="stSidebar"] .stButton button {
        background: linear-gradient(145deg, #0f2018, #142820) !important;
        border: 1px solid #00ff6a !important;
        color: #00ff6a !important;
        font-weight: 700 !important;
    }

    /* STREAMLIT BUTTON OVERRIDE */
    .stButton > button {
        background: linear-gradient(145deg, #0f2018, #142820) !important;
        border: 1px solid #1a4030 !important;
        color: #00ff6a !important;
        font-family: 'JetBrains Mono', monospace !important;
        font-weight: 600 !important;
        transition: all 0.15s ease !important;
    }

    .stButton > button:hover {
        border-color: #00ff6a !important;
        box-shadow: 0 0 15px rgba(0, 255, 106, 0.3) !important;
    }

    /* PLOTLY CHART CONTAINER */
    .stPlotlyChart {
        flex: 1;
    }

    /* TEXT INPUT STYLING */
    .stTextInput input {
        background: #111916 !important;
        border: 1px solid #1a3025 !important;
        color: #e0e0e0 !important;
        font-family: 'JetBrains Mono', monospace !important;
    }

    /* HIDE STREAMLIT ELEMENTS */
    .stSpinner > div {
        color: #00ff6a !important;
    }

    /* FILE UPLOADER - DARK THEME */
    [data-testid="stFileUploader"] {
        background: #111916 !important;
        border: 1px solid #1a3025 !important;
        border-radius: 6px !important;
    }

    [data-testid="stFileUploader"] section {
        background: #111916 !important;
        border: 1px dashed #1a4030 !important;
        border-radius: 6px !important;
        padding: 12px !important;
    }

    [data-testid="stFileUploader"] section > div {
        color: #5a8a6a !important;
    }

    [data-testid="stFileUploader"] button {
        background: linear-gradient(145deg, #0f2018, #142820) !important;
        border: 1px solid #1a4030 !important;
        color: #00ff6a !important;
    }

    [data-testid="stFileUploader"] small {
        color: #5a8a6a !important;
    }

    /* File uploader drag text */
    [data-testid="stFileUploaderDropzone"] {
        background: #111916 !important;
        border-color: #1a4030 !important;
    }

    [data-testid="stFileUploaderDropzone"] span {
        color: #5a8a6a !important;
    }

    /* Uploaded file name */
    [data-testid="stFileUploader"] [data-testid="stMarkdownContainer"] {
        color: #00ff6a !important;
    }

    /* RESPONSIVE */
    @media (max-width: 1200px) {
        .top-bar { padding: 8px 12px; }
        .top-bar-stat { gap: 4px; }
        .top-bar-stat-value { font-size: 12px; }
    }
</style>
"""

# Inject CSS
st.markdown(TERMINAL_CSS, unsafe_allow_html=True)

# =============================================================================
# CONFIGURATION
# =============================================================================

CLOB_HOST = "https://clob.polymarket.com"
GAMMA_API_HOST = "https://gamma-api.polymarket.com"
CHAIN_ID = 137

USDC_ADDRESS = "0x3c499c542cEF5E3811e1192ce70d8cc03d5c3359"
CONDITIONAL_TOKENS = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"

EXCHANGE_CONTRACTS = [
    "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E",
    "0xC5d563A36AE78145C45a50134d48A1215220f80a",
    "0xd91E80cF2E7be2e162c6513ceD06f1dD0dA35296",
]

SLUG_COINS = ["btc", "eth", "sol", "xrp"]

COIN_TO_BINANCE = {
    "BTC": "BTCUSDT",
    "ETH": "ETHUSDT",
    "SOL": "SOLUSDT",
    "XRP": "XRPUSDT"
}

MAX_IMBALANCE = 500
WARN_IMBALANCE = 400
NO_TRADE_SECONDS = 90
PRICE_SLIPPAGE = 0.006
BUY_AMOUNTS = [10, 25, 50, 100]
BUY_PERCENTAGES = [5, 10, 25, 50]  # Percentage of available bankroll
REFRESH_INTERVAL = 2  # Fast polling for live prices

# =============================================================================
# AUTO MODE PARAMETERS - GABAGOOL STRATEGY (DO NOT CHANGE)
# =============================================================================
TARGET_PAIR_COST = 0.982          # Stop buying when pair cost <= this
MIN_IMPROVEMENT_REQUIRED = 0.004  # Only buy if projected pair drops by at least 0.4¢
MAX_DIRECTIONAL_RISK_PCT = 0.35   # Never risk more than 35% of bankroll directionally
MAX_TRADE_PCT = 0.12              # Max 12% of free capital per trade
MIN_TRADE_USD = 2                 # Minimum trade size (lowered for testing with small bankrolls)
MAX_TRADE_USD = 100               # Maximum trade size
MIN_TIME_REMAINING = 90           # Don't trade with less than 90s remaining
AUTO_TRADE_COOLDOWN = 15          # Minimum seconds between auto trades (rate limit)

ET = pytz.timezone("US/Eastern")

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
        "opportunities": [],
        "cumulative_missed_profit": 0.0,  # Running total of missed profit
        "cumulative_missed_count": 0,      # Total opportunities missed
    }

defaults = {
    "markets": {},
    "history": [],
    "allowance_approved": False,
    "trade_log": [],
    "equity_history": [],
    "session_start": datetime.now(ET).isoformat(),
    "total_trades": 0,
    "daily_pnl": {},
    "opportunities": [],
    "cumulative_missed_profit": 0.0,
    "cumulative_missed_count": 0,
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

if 'auto_mode' not in st.session_state:
    st.session_state.auto_mode = False

if 'auto_log' not in st.session_state:
    st.session_state.auto_log = []  # Last 30 auto trades

if 'last_auto_trade_time' not in st.session_state:
    st.session_state.last_auto_trade_time = 0  # Unix timestamp of last auto trade

if 'button_mode' not in st.session_state:
    st.session_state.button_mode = "percent"  # "percent" or "dollar"

# =============================================================================
# BINANCE PRICE DATA
# =============================================================================

def get_binance_data() -> Dict[str, Dict]:
    """Fetch live prices from Binance for all supported coins."""
    symbols = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT"]
    data = {}

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


def format_price(price: float) -> str:
    """Format price for display."""
    if price >= 1000:
        return f"${price:,.0f}"
    elif price >= 1:
        return f"${price:.2f}"
    else:
        return f"${price:.4f}"


# =============================================================================
# CLOB LIVE PRICE FETCHING
# =============================================================================

def get_clob_midpoints(up_token_id: str, down_token_id: str) -> Tuple[float, float]:
    """Fetch LIVE prices from CLOB /midpoint API."""
    up_price = 0.5
    down_price = 0.5

    try:
        r = requests.get(
            f"https://clob.polymarket.com/midpoint?token_id={up_token_id}",
            timeout=5
        )
        if r.status_code == 200:
            data = r.json()
            up_price = float(data.get("mid", 0.5))
    except Exception as e:
        print(f"[CLOB] Up price fetch error: {e}")

    try:
        r = requests.get(
            f"https://clob.polymarket.com/midpoint?token_id={down_token_id}",
            timeout=5
        )
        if r.status_code == 200:
            data = r.json()
            down_price = float(data.get("mid", 0.5))
    except Exception as e:
        print(f"[CLOB] Down price fetch error: {e}")

    # Debug: print pair cost
    pair = up_price + down_price
    print(f"[CLOB] Fetched: up={up_price:.3f}, down={down_price:.3f}, pair={pair:.4f}")

    return up_price, down_price


# =============================================================================
# PLOTLY CHARTS
# =============================================================================

def create_equity_curve(equity_history: List[Dict], height: int = 200) -> go.Figure:
    """Create terminal-style equity curve."""
    fig = go.Figure()

    if equity_history:
        times = [e.get("timestamp", "") for e in equity_history]
        values = [e.get("total_profit", 0) for e in equity_history]

        current_val = values[-1] if values else 0
        line_color = "#00ff6a" if current_val >= 0 else "#ff4d4d"
        fill_color = "rgba(0, 255, 106, 0.1)" if current_val >= 0 else "rgba(255, 77, 77, 0.1)"

        fig.add_trace(go.Scatter(
            x=times,
            y=values,
            mode='lines',
            line=dict(color=line_color, width=2),
            fill='tozeroy',
            fillcolor=fill_color,
            hovertemplate='$%{y:.2f}<extra></extra>'
        ))

    fig.update_layout(
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)',
        font=dict(color='#5a8a6a', family='JetBrains Mono'),
        margin=dict(l=40, r=10, t=10, b=30),
        height=height,
        xaxis=dict(
            showgrid=True,
            gridcolor='rgba(26, 48, 37, 0.5)',
            showline=False,
            tickfont=dict(size=10)
        ),
        yaxis=dict(
            showgrid=True,
            gridcolor='rgba(26, 48, 37, 0.5)',
            showline=False,
            tickprefix='$',
            tickfont=dict(size=10)
        ),
        showlegend=False,
        hovermode='x unified'
    )

    return fig


# =============================================================================
# WEB3 SETUP
# =============================================================================

def get_web3() -> Web3:
    return Web3(Web3.HTTPProvider(st.session_state.rpc_url))


def get_wallet_address() -> str:
    account = Account.from_key(st.session_state.private_key)
    return account.address


def get_usdc_balance() -> Optional[float]:
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
    if st.session_state.state.get("allowance_approved", False):
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
# CLOB CLIENT
# =============================================================================

def get_clob_client() -> Optional[ClobClient]:
    if st.session_state.client is not None:
        return st.session_state.client

    try:
        # Check for official API credentials from environment variables
        api_key = os.environ.get("POLYMARKET_API_KEY")
        api_secret = os.environ.get("POLYMARKET_API_SECRET")
        api_passphrase = os.environ.get("POLYMARKET_API_PASSPHRASE")

        if api_key and api_secret and api_passphrase:
            # Use official API credentials (no Cloudflare issues)
            # Create ApiCreds object with the official credentials
            creds = ApiCreds(
                api_key=api_key,
                api_secret=api_secret,
                api_passphrase=api_passphrase
            )
            # Need private key to initialize client, then set creds
            env_private_key = os.environ.get("POLYMARKET_PRIVATE_KEY")
            if env_private_key:
                pk = env_private_key.strip()
                if not pk.startswith("0x"):
                    pk = "0x" + pk
                # Get wallet address for funder parameter
                wallet_account = Account.from_key(pk)
                funder_address = wallet_account.address

                # signature_type=2 for browser proxy wallets (Polymarket website API keys)
                client = ClobClient(
                    host=CLOB_HOST,
                    key=pk,
                    chain_id=CHAIN_ID,
                    signature_type=2,
                    funder=funder_address
                )
                # CRITICAL: Must call set_api_creds() to activate credentials
                # The creds parameter in constructor is IGNORED by py-clob-client
                client.set_api_creds(creds)
            else:
                # No private key - create client with just creds
                client = ClobClient(
                    host=CLOB_HOST,
                    chain_id=CHAIN_ID,
                    signature_type=2
                )
                # CRITICAL: Must call set_api_creds() to activate credentials
                client.set_api_creds(creds)
            st.session_state.client = client
            st.session_state.api_cred_status = "official API"
            return client

        # Fallback: derive from private key (may hit Cloudflare)
        if not st.session_state.private_key:
            st.error("No API credentials or private key configured")
            return None

        client = ClobClient(
            host=CLOB_HOST,
            key=st.session_state.private_key,
            chain_id=CHAIN_ID
        )

        cred_status = "unknown"
        try:
            creds = client.derive_api_key()
            cred_status = "derived"
        except Exception as e1:
            cred_status = f"derive failed: {str(e1)[:40]}"
            try:
                creds = client.create_or_derive_api_creds()
                cred_status = "created"
            except Exception as e2:
                cred_status = f"FAILED: {str(e2)[:40]}"
                st.session_state.api_cred_status = cred_status
                raise e2
        client.set_api_creds(creds)
        st.session_state.client = client
        st.session_state.api_cred_status = cred_status
        return client
    except Exception as e:
        st.error(f"Failed to initialize CLOB client: {e}")
        return None


# =============================================================================
# MARKET DETECTION
# =============================================================================

def get_current_15m_timestamp() -> int:
    return int(time.time() // 900 * 900)


def fetch_market_by_slug(slug: str) -> Optional[Dict]:
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
    current_ts = get_current_15m_timestamp()
    timestamps_to_check = [current_ts, current_ts + 900, current_ts - 900]

    for ts in timestamps_to_check:
        slug = f"{coin}-updown-15m-{ts}"
        market = fetch_market_by_slug(slug)

        if market:
            token_ids_raw = market.get("clobTokenIds", [])
            if isinstance(token_ids_raw, str):
                try:
                    token_ids = json.loads(token_ids_raw)
                except:
                    token_ids = []
            else:
                token_ids = token_ids_raw

            outcomes_raw = market.get("outcomes", ["Up", "Down"])
            if isinstance(outcomes_raw, str):
                try:
                    outcomes = json.loads(outcomes_raw)
                except:
                    outcomes = ["Up", "Down"]
            else:
                outcomes = outcomes_raw

            if len(token_ids) >= 2 and len(outcomes) >= 2:
                up_idx, down_idx = 0, 1
                for i, outcome in enumerate(outcomes):
                    if str(outcome).lower() == "up":
                        up_idx = i
                    elif str(outcome).lower() == "down":
                        down_idx = i

                up_token_id = token_ids[up_idx]
                down_token_id = token_ids[down_idx]

                up_price, down_price = get_clob_midpoints(up_token_id, down_token_id)

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


# =============================================================================
# MARKET STATE MANAGEMENT
# =============================================================================

def get_market_state(condition_id: str, coin: str) -> Dict:
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
# TRADING FUNCTIONS
# =============================================================================

def get_order_book_ask(client: ClobClient, token_id: str) -> float:
    try:
        ob = client.get_order_book(token_id)
        if ob.asks:
            return float(ob.asks[0].price)
        return 0.99
    except Exception:
        return 0.99


def check_safety(mstate: Dict, side: str, seconds_remaining: int) -> Tuple[bool, str]:
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
    is_allowed, reason = check_safety(mstate, side, seconds_remaining)
    if not is_allowed:
        return False, reason, 0, 0

    try:
        try:
            ob = client.get_order_book(token_id)
        except Exception as e:
            error_str = str(e)[:80]
            return False, f"Order book error: {error_str}", 0, 0

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

        try:
            response = client.create_and_post_order(order_args)
        except Exception as e:
            error_str = str(e)[:80]
            return False, f"Order API error: {error_str}", 0, 0

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
# AUTO MODE FUNCTIONS - GABAGOOL STRATEGY
# =============================================================================

def evaluate_auto_trade(
    market: Dict,
    mstate: Dict,
    available_usdc: float
) -> Optional[Dict]:
    """
    Evaluate whether to execute an auto trade for this market.
    Returns trade details dict if should trade, None otherwise.

    STRATEGY: Buy ONLY the cheaper side to improve pair cost.
    - Never buy both sides at once
    - Keep adding until avg_yes + avg_no <= TARGET_PAIR_COST
    - Never sell
    """
    if not market.get("active"):
        return None

    condition_id = market.get("condition_id")
    if not condition_id:
        return None

    # Time check - don't trade with less than 90s remaining
    seconds_remaining = get_seconds_remaining(market.get("end_time"))
    if seconds_remaining < MIN_TIME_REMAINING and seconds_remaining != 999:
        return None

    # Get current positions
    shares_up = mstate.get("shares_up", 0.0)
    shares_down = mstate.get("shares_down", 0.0)
    spent_up = mstate.get("spent_up", 0.0)
    spent_down = mstate.get("spent_down", 0.0)

    # Calculate current average costs
    avg_up = spent_up / shares_up if shares_up > 0 else 0
    avg_down = spent_down / shares_down if shares_down > 0 else 0

    # Current pair cost (only if we have positions on BOTH sides)
    if shares_up > 0 and shares_down > 0:
        current_pair_cost = avg_up + avg_down
    else:
        current_pair_cost = 1.0  # No pair yet, treat as expensive

    # If already at target, skip
    if current_pair_cost <= TARGET_PAIR_COST:
        return None

    # Get live market prices
    up_price = market.get("up_price", 0.5)
    down_price = market.get("down_price", 0.5)

    # Skip if prices are invalid
    if up_price <= 0 or down_price <= 0:
        return None

    # Determine cheaper side
    if up_price < down_price:
        cheaper_side = "up"
        cheaper_price = up_price
        cheaper_token_id = market.get("up_token_id")
        current_shares = shares_up
        current_spent = spent_up
        other_avg = avg_down if shares_down > 0 else 0.5  # Assume 0.5 if no position
    else:
        cheaper_side = "down"
        cheaper_price = down_price
        cheaper_token_id = market.get("down_token_id")
        current_shares = shares_down
        current_spent = spent_down
        other_avg = avg_up if shares_up > 0 else 0.5

    if not cheaper_token_id:
        return None

    # Calculate current imbalance (in USD terms)
    current_imbalance_usd = abs((shares_up * up_price) - (shares_down * down_price))

    # Dynamic sizing based on pair cost urgency
    base_trade_usd = available_usdc * MAX_TRADE_PCT

    if current_pair_cost > 0.980:
        multiplier = 1.0   # Panic mode - full size
    elif current_pair_cost > 0.975:
        multiplier = 0.85
    else:
        multiplier = 0.6

    trade_usd = base_trade_usd * multiplier
    trade_usd = max(MIN_TRADE_USD, min(MAX_TRADE_USD, trade_usd))

    # Make sure we have enough capital
    if trade_usd > available_usdc:
        return None

    # Calculate projected outcome
    projected_shares = current_shares + (trade_usd / cheaper_price)
    projected_spent = current_spent + trade_usd
    projected_avg_cheaper = projected_spent / projected_shares

    # Projected pair cost
    if cheaper_side == "up":
        projected_pair = projected_avg_cheaper + other_avg
    else:
        projected_pair = other_avg + projected_avg_cheaper

    # Check improvement threshold (at least 0.4¢ improvement)
    improvement = current_pair_cost - projected_pair
    if improvement < MIN_IMPROVEMENT_REQUIRED:
        return None

    # Check projected pair is at or below target
    if projected_pair > TARGET_PAIR_COST:
        return None

    # Check directional risk limit (35% of bankroll)
    projected_imbalance = current_imbalance_usd + trade_usd
    if projected_imbalance > available_usdc * MAX_DIRECTIONAL_RISK_PCT:
        return None

    # All checks passed - return trade details
    return {
        "coin": market["coin"],
        "side": cheaper_side,
        "token_id": cheaper_token_id,
        "trade_usd": trade_usd,
        "current_pair": current_pair_cost,
        "projected_pair": projected_pair,
        "improvement": improvement,
        "seconds_remaining": seconds_remaining,
    }


def execute_auto_trade(
    trade_info: Dict,
    market: Dict,
    mstate: Dict,
    client: ClobClient
) -> Tuple[bool, str, float]:
    """Execute an auto trade and log it."""

    coin = trade_info["coin"]
    side = trade_info["side"]
    token_id = trade_info["token_id"]
    trade_usd = trade_info["trade_usd"]
    seconds_remaining = trade_info["seconds_remaining"]

    # Execute the buy
    success, msg, filled_shares, actual_cost = execute_market_buy(
        client, token_id, side, trade_usd,
        mstate, seconds_remaining, coin
    )

    if success:
        # Calculate profit locked by this trade
        new_metrics = calculate_metrics(mstate)

        # Log to auto log
        auto_entry = {
            "time": datetime.now(ET).strftime("%H:%M:%S"),
            "coin": coin,
            "side": side.upper(),
            "size": round(actual_cost, 2),
            "old_pair": round(trade_info["current_pair"], 4),
            "new_pair": round(trade_info["projected_pair"], 4),
            "locked": round(new_metrics["locked_profit"], 2),
            "status": "OK"
        }
        st.session_state.auto_log.insert(0, auto_entry)
        st.session_state.auto_log = st.session_state.auto_log[:30]

        # Rate limit protection: sleep after successful trade
        time.sleep(1.5)

        return True, f"AUTO: {coin} {side.upper()} ${actual_cost:.2f}", actual_cost

    # Log failed trade
    fail_entry = {
        "time": datetime.now(ET).strftime("%H:%M:%S"),
        "coin": coin,
        "side": side.upper(),
        "size": round(trade_usd, 2),
        "old_pair": round(trade_info["current_pair"], 4),
        "new_pair": 0,
        "locked": 0,
        "status": "FAILED",
        "error": msg[:40]
    }
    st.session_state.auto_log.insert(0, fail_entry)
    st.session_state.auto_log = st.session_state.auto_log[:30]

    return False, msg, 0


def run_auto_mode_cycle(markets: List[Dict], client: ClobClient) -> List[str]:
    """Run one cycle of auto mode across all markets."""

    if not st.session_state.auto_mode:
        return []

    # Rate limit: check cooldown since last trade
    current_time = time.time()
    time_since_last = current_time - st.session_state.last_auto_trade_time
    if time_since_last < AUTO_TRADE_COOLDOWN:
        return []  # Still in cooldown, skip this cycle

    messages = []

    # Get available capital
    usdc_balance = get_usdc_balance() or 0
    available_usdc = usdc_balance - 5  # Keep $5 buffer

    if available_usdc < MIN_TRADE_USD:
        return []

    # Evaluate each market
    for market in markets:
        if not market.get("active"):
            continue

        condition_id = market.get("condition_id")
        if not condition_id:
            continue

        mstate = get_market_state(condition_id, market["coin"])

        # Evaluate if we should trade
        trade_info = evaluate_auto_trade(market, mstate, available_usdc)

        if trade_info:
            try:
                # Execute the trade
                success, msg, cost = execute_auto_trade(trade_info, market, mstate, client)
                messages.append(msg)

                # Check for 403/rate limit in returned message (not exception)
                if "403" in msg or "rate" in msg.lower():
                    # Rate limited - massive backoff (60 seconds)
                    st.session_state.last_auto_trade_time = time.time() + 60
                    break

                # ALWAYS update cooldown after any trade attempt (success or failure)
                st.session_state.last_auto_trade_time = time.time()

                if success:
                    # Update available capital
                    available_usdc -= cost

                # ALWAYS break after one trade attempt - max 1 per cycle
                break

            except Exception as e:
                # Update cooldown on exception too
                st.session_state.last_auto_trade_time = time.time() + 60  # Big backoff on exception
                error_msg = str(e)
                messages.append(f"AUTO: Error - {error_msg[:50]}")
                break  # Stop trying on error

    return messages


# =============================================================================
# COMPUTED METRICS
# =============================================================================

def calculate_metrics(mstate: Dict) -> Dict[str, Any]:
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
        imbalance_signed = shares_up - shares_down

        return {
            "avg_up": avg_up,
            "avg_down": avg_down,
            "avg_pair_cost": avg_pair_cost,
            "locked_shares": locked_shares,
            "locked_profit": locked_profit,
            "unbalanced": unbalanced,
            "imbalance_side": imbalance_side,
            "imbalance_signed": imbalance_signed,
            "total_spent": spent_up + spent_down
        }
    except Exception:
        return {
            "avg_up": 0, "avg_down": 0, "avg_pair_cost": 0,
            "locked_shares": 0, "locked_profit": 0,
            "unbalanced": 0, "imbalance_side": None, "imbalance_signed": 0, "total_spent": 0
        }


def get_total_locked_profit() -> float:
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
    state = st.session_state.state

    total_trades = state.get("total_trades", 0)
    history = state.get("history", [])
    trade_log = state.get("trade_log", [])

    total_profit = get_total_locked_profit() + get_total_history_profit()

    winning_markets = sum(1 for h in history if h.get("locked_profit", 0) > 0)
    total_markets = len(history)
    win_rate = (winning_markets / total_markets * 100) if total_markets > 0 else 100

    avg_pair_costs = []
    for mstate in state.get("markets", {}).values():
        m = calculate_metrics(mstate)
        if m["avg_pair_cost"] > 0:
            avg_pair_costs.append(m["avg_pair_cost"])
    avg_pair_cost = sum(avg_pair_costs) / len(avg_pair_costs) if avg_pair_costs else 0

    total_volume = sum(t.get("usdc", 0) for t in trade_log)

    # Calculate equity from USDC balance + locked profit
    usdc_bal = get_usdc_balance() or 0
    equity = usdc_bal + total_profit

    return {
        "total_trades": total_trades,
        "total_profit": total_profit,
        "win_rate": win_rate,
        "avg_pair_cost": avg_pair_cost,
        "total_volume": total_volume,
        "equity": equity,
        "markets_completed": total_markets,
    }


# =============================================================================
# BACKUP/RESTORE
# =============================================================================

def export_state_json() -> str:
    return json.dumps(st.session_state.state, default=str, indent=2)


def import_state_json(json_str: str) -> bool:
    try:
        data = json.loads(json_str)
        st.session_state.state.update(data)
        return True
    except Exception as e:
        st.error(f"Import failed: {e}")
        return False


# =============================================================================
# OPPORTUNITY LOGGER
# =============================================================================

def log_opportunity(coin: str, pair_cost: float, up_price: float, down_price: float):
    """Log market opportunity for tracking. Only logs edges >= 2% (pair cost < 0.98)."""
    MIN_EDGE_PCT = 2.0  # Only log opportunities with at least 2% edge (pair < 0.98)

    if pair_cost < 1.0:
        edge = (1.0 - pair_cost) * 100

        # Only log if edge is significant enough
        if edge < MIN_EDGE_PCT:
            return

        # Calculate missed profit for this opportunity using bankroll-based sizing
        usdc_balance = get_usdc_balance() or 0
        available_usdc = max(usdc_balance - 5, 0)
        trade_size = available_usdc * MAX_TRADE_PCT
        trade_size = max(MIN_TRADE_USD, min(MAX_TRADE_USD, trade_size))
        missed_profit_this = (edge / 100) * trade_size

        opp = {
            "time": datetime.now(ET).strftime("%H:%M"),
            "coin": coin,
            "pair_cost": pair_cost,
            "edge": edge,
            "up": up_price,
            "down": down_price,
            "missed_profit": missed_profit_this,
            "trade_size": trade_size,
        }
        opportunities = st.session_state.state.get("opportunities", [])
        # Avoid duplicates within same minute
        if not opportunities or opportunities[0].get("time") != opp["time"] or opportunities[0].get("coin") != coin:
            opportunities.insert(0, opp)
            st.session_state.state["opportunities"] = opportunities[:50]

            # Accumulate missed profit (only if AUTO MODE is OFF - if ON, we're trading it!)
            if not st.session_state.auto_mode:
                st.session_state.state["cumulative_missed_profit"] = st.session_state.state.get("cumulative_missed_profit", 0.0) + missed_profit_this
                st.session_state.state["cumulative_missed_count"] = st.session_state.state.get("cumulative_missed_count", 0) + 1


# =============================================================================
# SIDEBAR - COMPACT WALLET
# =============================================================================

def render_sidebar():
    """Compact wallet sidebar."""
    with st.sidebar:
        st.markdown("""
        <div style='text-align: center; padding: 10px 0; border-bottom: 1px solid #1a3025; margin-bottom: 15px;'>
            <span style='color: #00ff6a; font-family: JetBrains Mono; font-weight: 800; font-size: 14px; letter-spacing: 2px;'>
                💎 WALLET
            </span>
        </div>
        """, unsafe_allow_html=True)

        # Check if official API credentials are set - auto-connect if so
        api_key = os.environ.get("POLYMARKET_API_KEY")
        api_secret = os.environ.get("POLYMARKET_API_SECRET")
        api_passphrase = os.environ.get("POLYMARKET_API_PASSPHRASE")
        env_private_key = os.environ.get("POLYMARKET_PRIVATE_KEY")

        if api_key and api_secret and api_passphrase and not st.session_state.wallet_connected:
            # Auto-connect with official API
            st.session_state.wallet_connected = True
            st.session_state.rpc_url = "https://polygon-rpc.com"
            st.session_state.api_cred_status = "official API"
            # Also set private key from env if available (for balance queries)
            if env_private_key:
                pk = env_private_key.strip()
                if not pk.startswith("0x"):
                    pk = "0x" + pk
                st.session_state.private_key = pk

        if not st.session_state.wallet_connected:
            private_key_input = st.text_input(
                "Private Key",
                type="password",
                help="Hot wallet only"
            )

            rpc_url_input = st.text_input(
                "RPC URL",
                value="https://polygon-rpc.com"
            )

            if st.button("CONNECT", type="primary", use_container_width=True):
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
                        st.error(f"Invalid: {e}")
                else:
                    st.error("64 hex chars required")

            return False  # Signal wallet not connected

        try:
            # Check if using official API
            api_key = os.environ.get("POLYMARKET_API_KEY")
            using_official_api = bool(api_key and os.environ.get("POLYMARKET_API_SECRET"))

            # Check if we have a private key for balance queries (either from manual input or env var)
            has_private_key = bool(st.session_state.get("private_key"))

            if has_private_key:
                # Can query balance with private key
                wallet_addr = get_wallet_address()
                usdc_bal = get_usdc_balance() or 0
                matic_bal = get_matic_balance() or 0
                display_addr = f"{wallet_addr[:6]}...{wallet_addr[-4:]}"
                if using_official_api:
                    display_addr += " (API)"
            elif using_official_api:
                # Official API without private key - can't query balance
                wallet_addr = "API Mode"
                usdc_bal = 0
                matic_bal = 0
                display_addr = "Official API (no balance)"
            else:
                wallet_addr = get_wallet_address()
                usdc_bal = get_usdc_balance() or 0
                matic_bal = get_matic_balance() or 0
                display_addr = f"{wallet_addr[:6]}...{wallet_addr[-4:]}"

            st.markdown(f"""
            <div style='background: #111916; padding: 12px; border-radius: 6px; border: 1px solid #1a3025; margin-bottom: 12px;'>
                <div style='color: #00ff6a; font-size: 10px; letter-spacing: 1px;'>CONNECTED</div>
                <div style='font-family: JetBrains Mono; font-size: 11px; color: #7a9a8a;'>{display_addr}</div>
            </div>
            """, unsafe_allow_html=True)

            # Show balance if we have a private key (can query on-chain)
            if has_private_key:
                st.markdown(f"""
                <div style='display: flex; gap: 8px; margin-bottom: 12px;'>
                    <div style='flex: 1; background: #111916; padding: 10px; border-radius: 6px; text-align: center; border: 1px solid #1a3025;'>
                        <div style='color: #00ff6a; font-family: JetBrains Mono; font-weight: 700;'>${usdc_bal:.2f}</div>
                        <div style='color: #5a8a6a; font-size: 9px;'>USDC</div>
                    </div>
                    <div style='flex: 1; background: #111916; padding: 10px; border-radius: 6px; text-align: center; border: 1px solid #1a3025;'>
                        <div style='color: #e0e0e0; font-family: JetBrains Mono; font-weight: 700;'>{matic_bal:.3f}</div>
                        <div style='color: #5a8a6a; font-size: 9px;'>MATIC</div>
                    </div>
                </div>
                """, unsafe_allow_html=True)

            # Show API credential status
            api_status = st.session_state.get("api_cred_status", "not initialized")
            status_color = "#00ff6a" if api_status in ["derived", "created", "official API"] else "#ff6b6b"
            st.markdown(f"""
            <div style='background: #111916; padding: 8px; border-radius: 6px; border: 1px solid #1a3025; margin-bottom: 12px;'>
                <div style='color: #5a8a6a; font-size: 9px;'>API CREDS</div>
                <div style='color: {status_color}; font-size: 10px; font-family: JetBrains Mono;'>{api_status}</div>
            </div>
            """, unsafe_allow_html=True)

            # Trade button mode toggle
            st.markdown("""
            <div style='margin-top: 12px; padding-top: 12px; border-top: 1px solid #1a3025;'>
                <div style='color: #5a8a6a; font-size: 10px; letter-spacing: 1px; margin-bottom: 8px;'>TRADE BUTTONS</div>
            </div>
            """, unsafe_allow_html=True)

            button_mode = st.radio(
                "Mode",
                ["percent", "dollar"],
                format_func=lambda x: "% of Bankroll" if x == "percent" else "$ Fixed",
                horizontal=True,
                key="button_mode_radio",
                label_visibility="collapsed"
            )
            if button_mode != st.session_state.button_mode:
                st.session_state.button_mode = button_mode
                st.rerun()

        except Exception as e:
            st.error(f"Error: {e}")
            return False  # Signal error occurred

        # Backup download
        backup_data = export_state_json()
        b64 = base64.b64encode(backup_data.encode()).decode()
        filename = f"backup_{datetime.now().strftime('%Y%m%d_%H%M')}.json"

        st.markdown(f"""
        <a href="data:application/json;base64,{b64}" download="{filename}"
           style="display: block; text-align: center; background: #111916; border: 1px solid #1a3025;
                  padding: 8px; border-radius: 6px; color: #00ff6a; text-decoration: none;
                  font-family: JetBrains Mono; font-size: 11px; font-weight: 600; margin-bottom: 8px;">
            DOWNLOAD BACKUP
        </a>
        """, unsafe_allow_html=True)

        uploaded_file = st.file_uploader("Restore", type="json", label_visibility="collapsed")
        if uploaded_file:
            content = uploaded_file.read().decode()
            if st.button("RESTORE", use_container_width=True):
                if import_state_json(content):
                    st.success("Restored!")
                    st.rerun()

        if st.button("DISCONNECT", use_container_width=True):
            st.session_state.wallet_connected = False
            st.session_state.private_key = ""
            st.session_state.client = None
            st.rerun()

    return True  # Wallet connected successfully


# =============================================================================
# GOD MODE TERMINAL UI
# =============================================================================

def render_top_bar(stats: Dict, total_profit: float):
    """Render thin fixed top stats bar."""
    profit_class = "positive" if total_profit >= 0 else ""
    profit_sign = "+" if total_profit >= 0 else ""
    profit_pct = (total_profit / max(stats["equity"], 1)) * 100 if stats["equity"] > 0 else 0

    st.markdown(f"""
    <div class="top-bar">
        <div class="top-bar-left">
            <span class="top-bar-title">POLYMARKET TERMINAL</span>
            <div class="top-bar-stat">
                <span class="top-bar-stat-label">LOCKED</span>
                <span class="top-bar-stat-value {profit_class}">{profit_sign}${abs(total_profit):.2f} ({profit_sign}{profit_pct:.1f}%)</span>
            </div>
            <div class="top-bar-stat">
                <span class="top-bar-stat-label">TRADES</span>
                <span class="top-bar-stat-value neutral">{stats["total_trades"]}</span>
            </div>
            <div class="top-bar-stat">
                <span class="top-bar-stat-label">AVG PAIR</span>
                <span class="top-bar-stat-value neutral">{stats["avg_pair_cost"]:.3f}</span>
            </div>
            <div class="top-bar-stat">
                <span class="top-bar-stat-label">EQUITY</span>
                <span class="top-bar-stat-value neutral">${stats["equity"]:,.0f}</span>
            </div>
            <div class="top-bar-stat">
                <span class="top-bar-stat-label">WIN RATE</span>
                <span class="top-bar-stat-value positive">{stats["win_rate"]:.0f}%</span>
            </div>
        </div>
    </div>
    """, unsafe_allow_html=True)


def render_market_card(market: Dict, binance_data: Dict, client: ClobClient, idx: int):
    """Render compact market card with trading buttons."""
    coin = market["coin"]
    is_active = market.get("active", False)

    # Binance data
    symbol = COIN_TO_BINANCE.get(coin, "")
    b_data = binance_data.get(symbol, {})
    b_price = b_data.get("price", 0)
    b_change = b_data.get("change", 0)

    # Pair cost
    up_price = market.get("up_price", 0.5)
    down_price = market.get("down_price", 0.5)
    pair_cost = up_price + down_price

    # Determine pair cost color class
    if pair_cost < 0.98:
        pair_class = "good"
    elif pair_cost <= 0.985:
        pair_class = "marginal"
    else:
        pair_class = "bad"

    # Market state
    condition_id = market.get("condition_id")
    if is_active and condition_id:
        mstate = get_market_state(condition_id, coin)
        metrics = calculate_metrics(mstate)
        locked_profit = metrics["locked_profit"]
        imbalance = int(metrics["imbalance_signed"])
        seconds_remaining = get_seconds_remaining(market.get("end_time"))

        # Position info
        shares_up = mstate.get("shares_up", 0.0)
        shares_down = mstate.get("shares_down", 0.0)
        avg_up = metrics["avg_up"]
        avg_down = metrics["avg_down"]
        has_position = shares_up > 0 or shares_down > 0

        # Log opportunity
        log_opportunity(coin, pair_cost, up_price, down_price)
    else:
        mstate = {}
        locked_profit = 0
        imbalance = 0
        seconds_remaining = 999
        shares_up = 0
        shares_down = 0
        avg_up = 0
        avg_down = 0
        has_position = False

    # Coin color
    coin_colors = {"BTC": "#f7931a", "ETH": "#627eea", "SOL": "#00ffa3", "XRP": "#c0c0c0"}
    coin_color = coin_colors.get(coin, "#888")

    # Change color
    change_class = "up" if b_change >= 0 else "down"
    change_sign = "+" if b_change >= 0 else ""

    # Edge class
    edge_class = "edge" if pair_cost < 0.98 and is_active else ""

    # Format countdown timer
    if seconds_remaining < 999 and seconds_remaining > 0:
        mins = seconds_remaining // 60
        secs = seconds_remaining % 60
        countdown_str = f"{mins}:{secs:02d}"
        countdown_class = "countdown-urgent" if seconds_remaining < 60 else "countdown-normal"
    else:
        countdown_str = "--:--"
        countdown_class = "countdown-inactive"

    # Format crypto price
    if b_price > 0:
        price_str = f"${b_price:,.2f}" if b_price < 100 else f"${b_price:,.0f}"
        change_str = f'<span class="coin-change {change_class}">{change_sign}{b_change:.2f}%</span>'
    else:
        price_str = ""
        change_str = ""

    # Build position row HTML if we have a position
    if has_position:
        position_html = f'<div class="position-row"><span class="pos-label">POS</span><span class="pos-up">▲ {shares_up:.1f} @{avg_up:.3f}</span><span class="pos-down">▼ {shares_down:.1f} @{avg_down:.3f}</span></div>'
    else:
        position_html = ""

    card_html = f"""
    <div class="market-card {edge_class}">
        <div class="market-card-header">
            <div class="coin-badge">
                <span class="coin-symbol" style="color: {coin_color};">{coin}</span>
                <span class="coin-live-price">{price_str}</span>
                {change_str}
            </div>
            <div class="market-countdown {countdown_class}">
                <div class="countdown-value">{countdown_str}</div>
                <div class="countdown-label">LEFT</div>
            </div>
        </div>
        <div class="pair-row">
            <span class="pair-label">PAIR</span>
            <span class="pair-cost {pair_class}">{pair_cost:.4f}</span>
            <span class="locked-inline">+${locked_profit:.2f}</span>
        </div>
        <div class="price-row">
            <span>Up <span class="price-up">{up_price:.3f}</span></span>
            <span>Down <span class="price-down">{down_price:.3f}</span></span>
            <span>Imbal <span class="imbal">{imbalance:+d}</span></span>
        </div>
        {position_html}
    </div>
    """

    st.markdown(card_html, unsafe_allow_html=True)

    # Buy buttons (only if active)
    if is_active and condition_id and client:
        btn_cols = st.columns(4)

        # Get available bankroll for percent mode
        usdc_balance = get_usdc_balance() or 0
        available = max(usdc_balance - 5, 0)  # Keep $5 buffer

        if st.session_state.button_mode == "percent":
            # Percentage-based buttons
            for i, pct in enumerate(BUY_PERCENTAGES):
                with btn_cols[i]:
                    trade_amount = (pct / 100) * available
                    trade_amount = max(MIN_TRADE_USD, min(trade_amount, MAX_TRADE_USD * 5))  # Min $8, max $500
                    disabled = should_disable_button(mstate, "up", seconds_remaining) and should_disable_button(mstate, "down", seconds_remaining)
                    # Two-line label showing % and calculated $
                    label = f"{pct}% (${trade_amount:.0f})"
                    if st.button(label, key=f"buy_{coin}_pct{pct}_{idx}", disabled=disabled, use_container_width=True):
                        # Buy both sides
                        with st.spinner(f"Buying {pct}% (${trade_amount:.0f}) combo..."):
                            half = trade_amount / 2
                            success_up, msg_up, _, _ = execute_market_buy(
                                client, market["up_token_id"], "up", half,
                                mstate, seconds_remaining, coin
                            )
                            success_down, msg_down, _, _ = execute_market_buy(
                                client, market["down_token_id"], "down", half,
                                mstate, seconds_remaining, coin
                            )
                            if success_up and success_down:
                                st.success(f"Combo: {msg_up} + {msg_down}")
                            else:
                                st.warning(f"Partial: UP={msg_up}, DOWN={msg_down}")
                            time.sleep(1)
                            st.rerun()
        else:
            # Legacy dollar mode
            for i, amount in enumerate(BUY_AMOUNTS):
                with btn_cols[i]:
                    disabled = should_disable_button(mstate, "up", seconds_remaining) and should_disable_button(mstate, "down", seconds_remaining)
                    if st.button(f"${amount}", key=f"buy_{coin}_{amount}_{idx}", disabled=disabled, use_container_width=True):
                        # Buy both sides
                        with st.spinner(f"Buying ${amount} combo..."):
                            half = amount / 2
                            success_up, msg_up, _, _ = execute_market_buy(
                                client, market["up_token_id"], "up", half,
                                mstate, seconds_remaining, coin
                            )
                            success_down, msg_down, _, _ = execute_market_buy(
                                client, market["down_token_id"], "down", half,
                                mstate, seconds_remaining, coin
                            )
                            if success_up and success_down:
                                st.success(f"Combo: {msg_up} + {msg_down}")
                            else:
                                st.warning(f"Partial: UP={msg_up}, DOWN={msg_down}")
                            time.sleep(1)
                            st.rerun()


def render_opportunities_panel():
    """Render recent opportunities list."""
    opportunities = st.session_state.state.get("opportunities", [])

    # Find best opportunity
    best_idx = -1
    best_edge = 0
    for i, opp in enumerate(opportunities[:12]):
        if opp.get("edge", 0) > best_edge:
            best_edge = opp["edge"]
            best_idx = i

    opps_html = ""
    for i, opp in enumerate(opportunities[:12]):
        is_best = i == best_idx
        best_badge = '<span class="opp-best">BEST</span>' if is_best else ''
        time_str = opp.get("time", "")
        coin_str = opp.get("coin", "")
        pair_val = opp.get("pair_cost", 0)
        edge_val = opp.get("edge", 0)

        opps_html += f'<div class="opp-row"><span class="opp-time">{time_str}</span><span class="opp-coin">{coin_str}</span><span class="opp-pair">{pair_val:.3f}</span><span class="opp-edge">edge {edge_val:.1f}%</span>{best_badge}</div>'

    # Calculate trade size based on current bankroll (same as AUTO MODE logic)
    usdc_balance = get_usdc_balance() or 0
    available_usdc = max(usdc_balance - 5, 0)  # Keep $5 buffer

    # Dynamic trade size: 12% of available, capped at $100, min $8
    dynamic_trade_size = available_usdc * MAX_TRADE_PCT
    dynamic_trade_size = max(MIN_TRADE_USD, min(MAX_TRADE_USD, dynamic_trade_size))

    # Calculate potential profit using the gabagool strategy sizing
    # Each opportunity = buying cheaper side, edge% profit on trade size
    missed_profit = sum(opp.get("edge", 0) * dynamic_trade_size for opp in opportunities[:12]) / 100
    opp_count = len([o for o in opportunities[:12] if o.get("edge", 0) >= 0.5])

    if not opps_html:
        opps_html = '<div style="color: #5a8a6a; text-align: center; padding: 20px;">No good opportunities yet (need ≥2% edge / pair &lt;0.98)</div>'

    # Get cumulative totals
    cumulative_missed = st.session_state.state.get("cumulative_missed_profit", 0.0)
    cumulative_count = st.session_state.state.get("cumulative_missed_count", 0)

    # Get session start time
    session_start = st.session_state.state.get("session_start", "")
    try:
        start_dt = datetime.fromisoformat(session_start)
        session_duration = datetime.now(ET) - start_dt.replace(tzinfo=ET)
        hours = session_duration.total_seconds() / 3600
        duration_str = f"{hours:.1f}h"
    except:
        duration_str = "??h"

    # Show bankroll info and cumulative total
    bankroll_pct = int(MAX_TRADE_PCT * 100)
    return f'<div class="opportunities-panel"><div class="panel-header"><span class="panel-title">OPPORTUNITIES ({opp_count} recent)</span></div>{opps_html}<div class="missed-profit">Recent @ ${dynamic_trade_size:.0f}/trade: <span class="missed-value">+${missed_profit:.2f}</span></div><div class="cumulative-missed">SESSION TOTAL ({duration_str}, {cumulative_count} opps): <span class="cumulative-value">+${cumulative_missed:.2f}</span></div></div>'


def render_bottom_ticker():
    """Render scrolling recent trades ticker."""
    trade_log = st.session_state.state.get("trade_log", [])[:20]

    ticker_items = ""
    for t in trade_log:
        # Calculate approximate profit contribution
        price = t.get("price", 0)
        profit = t.get("usdc", 0) * (1 - price) if price > 0 else 0

        ticker_items += f"""
        <div class="ticker-item">
            <span class="ticker-time">{t.get("time", "")}</span>
            <span class="ticker-coin">{t.get("coin", "")}</span>
            bought
            <span class="ticker-amount">${t.get("usdc", 0):.0f}</span>
            @ {t.get("price", 0):.3f} →
            <span class="ticker-profit">+${profit:.2f} locked</span>
        </div>
        """

    # Duplicate for seamless scroll
    ticker_content = ticker_items + ticker_items if ticker_items else '<div class="ticker-item">No trades yet - start trading to see activity here</div>'

    st.markdown(f"""
    <div class="bottom-ticker">
        <div class="ticker-content">
            {ticker_content}
        </div>
    </div>
    """, unsafe_allow_html=True)


def render_auto_toggle():
    """Render the AUTO mode toggle switch."""
    # Custom CSS for the toggle
    st.markdown("""
    <style>
    /* Style the toggle container */
    div[data-testid="stToggle"] {
        background: linear-gradient(145deg, #111916, #0f1512) !important;
        padding: 12px 20px !important;
        border-radius: 8px !important;
        border: 1px solid #1a3025 !important;
    }
    div[data-testid="stToggle"] label {
        font-family: 'JetBrains Mono', monospace !important;
        font-size: 16px !important;
        font-weight: 700 !important;
        letter-spacing: 2px !important;
    }
    /* Pulsing glow when ON */
    div[data-testid="stToggle"]:has(input:checked) {
        border-color: #00ff6a !important;
        box-shadow: 0 0 20px rgba(0, 255, 106, 0.3) !important;
        animation: pulse-auto 2s infinite !important;
    }
    @keyframes pulse-auto {
        0%, 100% { box-shadow: 0 0 20px rgba(0, 255, 106, 0.3); }
        50% { box-shadow: 0 0 30px rgba(0, 255, 106, 0.6); }
    }
    </style>
    """, unsafe_allow_html=True)

    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        # Use actual toggle
        auto_on = st.toggle(
            "🤖 AUTO MODE — PRINTING" if st.session_state.auto_mode else "🤖 AUTO MODE",
            value=st.session_state.auto_mode,
            key="auto_toggle"
        )
        if auto_on != st.session_state.auto_mode:
            st.session_state.auto_mode = auto_on
            st.rerun()


def render_auto_log():
    """Render expandable auto trade log."""
    auto_log = st.session_state.auto_log

    if not auto_log:
        return

    # Calculate total auto profit (only from successful trades)
    total_auto_profit = sum(entry.get("locked", 0) for entry in auto_log if entry.get("status") != "FAILED")
    success_count = sum(1 for entry in auto_log if entry.get("status") != "FAILED")
    fail_count = sum(1 for entry in auto_log if entry.get("status") == "FAILED")

    header = f"🤖 AUTO LOG ({success_count} OK"
    if fail_count > 0:
        header += f", {fail_count} failed"
    header += f") — ${total_auto_profit:.2f} locked"

    with st.expander(header):
        log_html = '<div style="font-family: JetBrains Mono; font-size: 11px;">'

        for entry in auto_log[:30]:
            time_str = entry.get("time", "")
            coin = entry.get("coin", "")
            side = entry.get("side", "")
            size = entry.get("size", 0)
            old_pair = entry.get("old_pair", 0)
            new_pair = entry.get("new_pair", 0)
            locked = entry.get("locked", 0)
            status = entry.get("status", "OK")
            error = entry.get("error", "")

            if status == "FAILED":
                # Failed trade - show in red
                log_html += f'<div style="padding: 6px 0; border-bottom: 1px solid #3a2020; background: #1a0808; display: flex; justify-content: space-between; align-items: center;"><span style="color: #ff4444;">{time_str}</span><span style="color: #ff6666; font-weight: 700;">{coin}</span><span style="color: #ff4444; font-weight: 600;">FAILED</span><span style="color: #ff6666;">${size:.0f}</span><span style="color: #ff4444; font-size: 10px;">{error}</span></div>'
            else:
                # Successful trade
                side_color = "#00cc55" if side == "UP" else "#ff6b6b"
                log_html += f'<div style="padding: 6px 0; border-bottom: 1px solid #1a3025; display: flex; justify-content: space-between; align-items: center;"><span style="color: #5a8a6a;">{time_str}</span><span style="color: #e0e0e0; font-weight: 700;">{coin}</span><span style="color: {side_color}; font-weight: 600;">{side}</span><span style="color: #7a9a8a;">${size:.0f}</span><span style="color: #5a8a6a;">{old_pair:.3f}→{new_pair:.3f}</span><span style="color: #00ff6a; font-weight: 700;">+${locked:.2f}</span></div>'

        log_html += '</div>'
        st.markdown(log_html, unsafe_allow_html=True)


# =============================================================================
# MAIN APPLICATION
# =============================================================================

def main():
    """Main god-mode terminal."""

    # Railway deployment detection
    if os.environ.get("RAILWAY_ENVIRONMENT"):
        st.sidebar.success("🚂 Railway deployment active")

    # Sidebar wallet
    wallet_connected = render_sidebar()

    # If wallet not connected, show connect prompt in main area
    if not wallet_connected:
        # Force sidebar to be expanded using Streamlit's native approach
        st.markdown("""
        <style>
        /* Force sidebar to be visible/expanded on connect screen */
        [data-testid="stSidebar"] {
            display: block !important;
            width: 300px !important;
            min-width: 300px !important;
            transform: translateX(0) !important;
        }
        [data-testid="stSidebar"] > div:first-child {
            width: 300px !important;
        }
        /* Hide the collapse button since we want sidebar always open here */
        [data-testid="collapsedControl"] {
            display: none !important;
        }
        </style>
        """, unsafe_allow_html=True)

        # Center content
        st.markdown("""
        <div style='text-align: center; padding: 100px 20px;'>
            <h1 style='color: #00ff6a; font-family: JetBrains Mono; font-weight: 700; font-size: 32px; margin-bottom: 10px;'>
                POLYMARKET TERMINAL
            </h1>
            <p style='color: #7a9a8a; font-family: JetBrains Mono; font-size: 14px; margin-bottom: 20px;'>
                15-Minute Combo Trading System
            </p>
            <p style='color: #5a8a6a; font-family: JetBrains Mono; font-size: 13px;'>
                ← Enter your private key in the sidebar to connect
            </p>
        </div>
        """, unsafe_allow_html=True)
        return  # Exit main() early - don't try to render dashboard

    state = st.session_state.state

    # Check approvals
    if not state.get("allowance_approved", False):
        with st.spinner("Checking approvals..."):
            if check_existing_approvals():
                st.session_state.state["allowance_approved"] = True
                st.rerun()

    # Setup screen if needed
    if not state.get("allowance_approved", False):
        st.markdown("""
        <div style='text-align: center; padding: 100px 20px;'>
            <h2 style='color: #00ff6a;'>First-Time Setup Required</h2>
            <p style='color: #7a9a8a;'>Approve token spending to start trading</p>
        </div>
        """, unsafe_allow_html=True)

        col1, col2, col3 = st.columns([1, 2, 1])
        with col2:
            if st.button("SETUP: APPROVE ALLOWANCES", type="primary", use_container_width=True):
                with st.spinner("Sending approval transactions..."):
                    if approve_all_contracts():
                        st.rerun()
        st.stop()

    # Get client
    client = get_clob_client()
    if client is None:
        st.error("Failed to initialize trading client.")
        st.stop()

    # Fetch data
    binance_data = get_binance_data()
    st.session_state.binance_data = binance_data

    all_markets = find_all_active_updown_markets()

    # Archive old markets
    active_ids = [m["condition_id"] for m in all_markets if m.get("condition_id")]
    archive_old_markets(active_ids)

    # Calculate stats
    stats = calculate_session_stats()
    total_profit = get_total_locked_profit() + get_total_history_profit()

    # Render top bar
    render_top_bar(stats, total_profit)

    # AUTO MODE toggle
    render_auto_toggle()

    # Run auto mode cycle if enabled
    if st.session_state.auto_mode and client:
        auto_messages = run_auto_mode_cycle(all_markets, client)
        # Show toast notifications for auto trades
        for msg in auto_messages:
            st.toast(msg, icon="🤖")

    # Main layout: two columns
    left_col, right_col = st.columns([1, 2])

    with left_col:
        # Market cards
        for idx, market in enumerate(all_markets):
            render_market_card(market, binance_data, client, idx)

    with right_col:
        # Equity curve panel
        st.markdown("""
        <div class="equity-panel">
            <div class="panel-header">
                <span class="panel-title">EQUITY CURVE</span>
            </div>
        """, unsafe_allow_html=True)

        equity_history = state.get("equity_history", [])
        fig = create_equity_curve(equity_history, height=250)
        st.plotly_chart(fig, use_container_width=True, config={'displayModeBar': False})

        st.markdown("</div>", unsafe_allow_html=True)

        # Opportunities panel
        st.markdown(render_opportunities_panel(), unsafe_allow_html=True)

        # Auto log (if any auto trades)
        render_auto_log()

    # Bottom ticker
    render_bottom_ticker()

    # Auto-refresh
    time.sleep(REFRESH_INTERVAL)
    st.rerun()


if __name__ == "__main__":
    main()
