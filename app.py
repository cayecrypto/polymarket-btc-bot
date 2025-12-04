"""
================================================================================
POLYMARKET 15-MIN COMBO BOT — MULTI-MARKET EDITION (BTC • ETH • SOL • XRP)
================================================================================

A Streamlit-based manual trading bot for Polymarket's Up/Down 15-minute
prediction markets using the "gabagool-style" combo/hedge arbitrage strategy.

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

STRATEGY:
    Buy both UP and DOWN tokens to lock in guaranteed profit when pair cost < $1.
    Each pair pays out exactly $1 at resolution regardless of outcome.
    Profit = $1 * locked_shares - cost_of_locked_shares

gabagool style - Dec 2025 - 4X PRINTING SEASON with the bros
================================================================================
"""

import re
import time
from datetime import datetime
from typing import Optional, Dict, Any, Tuple, List

import streamlit as st
import requests
import pytz
import pandas as pd
from web3 import Web3
from eth_account import Account
import dateutil.parser

# py-clob-client imports
from py_clob_client.client import ClobClient
from py_clob_client.clob_types import OrderArgs

# =============================================================================
# PAGE CONFIGURATION
# =============================================================================

st.set_page_config(
    page_title="Polymarket 15-Min Combo Bot",
    page_icon="🔥",
    layout="wide",
    initial_sidebar_state="expanded"
)

# =============================================================================
# HEADER BANNER
# =============================================================================

st.markdown("""
<div style="background: linear-gradient(90deg, #ff6b35, #f7c531, #1fff9f, #00c8ff); padding: 25px; border-radius: 16px; text-align: center; margin-bottom: 30px; box-shadow: 0 6px 30px rgba(0,0,0,0.3);">
    <h1 style="color: black; margin:0; font-size: 2.5em; text-shadow: 1px 1px 2px rgba(255,255,255,0.3);">🔥 POLYMARKET 15-MIN COMBO BOT 🔥</h1>
    <p style="color: black; margin:10px 0 0 0; font-size: 1.5em; font-weight: bold;">NOW 4X PRINTING (BTC • ETH • SOL • XRP)</p>
    <p style="color: black; margin:8px 0; font-size: 1.1em;">Each tab = independent wallet • Multi-market simultaneous trading • Keep open 24/7</p>
</div>
""", unsafe_allow_html=True)

# =============================================================================
# CONFIGURATION
# =============================================================================

# Polymarket CLOB API
CLOB_HOST = "https://clob.polymarket.com"
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
COIN_KEYWORDS = ["bitcoin", "btc", "ethereum", "eth", "solana", "sol", "xrp", "ripple"]

COIN_MAP = {
    "bitcoin": "BTC",
    "btc": "BTC",
    "ethereum": "ETH",
    "eth": "ETH",
    "solana": "SOL",
    "sol": "SOL",
    "xrp": "XRP",
    "ripple": "XRP",
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
    }
]

# ERC1155 ABI for Conditional Tokens (setApprovalForAll)
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
    }
]

# =============================================================================
# SESSION STATE INITIALIZATION - 100% SAFE
# =============================================================================

if 'state' not in st.session_state:
    st.session_state.state = {
        "markets": {},      # condition_id → position dict
        "history": [],      # archived completed markets
        "allowance_approved": False,
        "trade_log": []     # global trade log
    }

# Ensure markets key exists (migration safety)
if "markets" not in st.session_state.state:
    st.session_state.state["markets"] = {}

if "history" not in st.session_state.state:
    st.session_state.state["history"] = []

if "allowance_approved" not in st.session_state.state:
    st.session_state.state["allowance_approved"] = False

if "trade_log" not in st.session_state.state:
    st.session_state.state["trade_log"] = []

if 'client' not in st.session_state:
    st.session_state.client = None

if 'wallet_connected' not in st.session_state:
    st.session_state.wallet_connected = False
    st.session_state.private_key = ""
    st.session_state.rpc_url = "https://polygon-rpc.com"

# =============================================================================
# SIDEBAR - WALLET SETUP
# =============================================================================

st.sidebar.header("🔑 Wallet Setup")
st.sidebar.markdown("Private key stays in browser session only")

# If not connected, show input form
if not st.session_state.wallet_connected:
    private_key_input = st.sidebar.text_input(
        "Private Key (with or without 0x)",
        type="password",
        help="Use a dedicated hot wallet with only your trading funds."
    )

    rpc_url_input = st.sidebar.text_input(
        "Polygon RPC URL",
        value="https://polygon-rpc.com",
        help="Free public or use your Alchemy/Infura link for zero lag"
    )

    if st.sidebar.button("🔓 Connect Wallet", type="primary", use_container_width=True):
        # Normalize private key - add 0x if missing
        pk = private_key_input.strip()
        if not pk.startswith("0x"):
            pk = "0x" + pk

        # Validate: should be 66 chars with 0x prefix (64 hex + "0x")
        if len(pk) == 66:
            try:
                # Test if it's a valid key by deriving address
                test_account = Account.from_key(pk)

                # Success! Store in session
                st.session_state.private_key = pk
                st.session_state.rpc_url = rpc_url_input
                st.session_state.wallet_connected = True
                st.rerun()
            except Exception as e:
                st.sidebar.error(f"Invalid private key: {e}")
        else:
            st.sidebar.error(f"Private key should be 64 hex characters (got {len(pk)-2})")

    st.sidebar.markdown("---")
    st.sidebar.caption("gabagool style • Dec 2025 • 4X printing season")
    st.warning("⚠️ Enter your private key and click 'Connect Wallet' to start")
    st.stop()

# Wallet is connected
PRIVATE_KEY = st.session_state.private_key
POLYGON_RPC_URL = st.session_state.rpc_url

# =============================================================================
# WEB3 SETUP
# =============================================================================

def get_web3() -> Web3:
    """Get Web3 instance connected to Polygon."""
    return Web3(Web3.HTTPProvider(POLYGON_RPC_URL))


def get_wallet_address() -> str:
    """Derive wallet address from private key."""
    account = Account.from_key(PRIVATE_KEY)
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
        # USDC has 6 decimals
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


def approve_all_contracts() -> bool:
    """
    Approve USDC (ERC20) and Conditional Tokens (ERC1155) for all exchange contracts.
    Must use manual signing for HTTPProvider.
    USDC uses approve(), CT uses setApprovalForAll().
    """
    if st.session_state.state.get("allowance_approved", False):
        st.info("Allowances already approved!")
        return True

    try:
        web3 = get_web3()
        if not web3.is_connected():
            st.error("Failed to connect to Polygon RPC")
            return False

        account = Account.from_key(PRIVATE_KEY)
        wallet_address = account.address

        usdc = web3.eth.contract(
            address=Web3.to_checksum_address(USDC_ADDRESS),
            abi=MINIMAL_ERC20_ABI
        )
        ct = web3.eth.contract(
            address=Web3.to_checksum_address(CONDITIONAL_TOKENS),
            abi=MINIMAL_ERC1155_ABI  # ERC1155 for setApprovalForAll
        )

        st.warning("Sending approvals — this will take ~15 seconds...")

        # USDC approvals (ERC20 - keep these)
        for contract_addr in EXCHANGE_CONTRACTS:
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
            st.info(f"USDC approval sent → {tx_hash.hex()[:10]}...")
            web3.eth.wait_for_transaction_receipt(tx_hash, timeout=120)
            time.sleep(1)

        # Conditional Tokens approvals (ERC1155 - use setApprovalForAll)
        for contract_addr in EXCHANGE_CONTRACTS:
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
            st.info(f"CT approval sent → {tx_hash.hex()[:10]}...")
            web3.eth.wait_for_transaction_receipt(tx_hash, timeout=120)
            time.sleep(1)

        st.session_state.state["allowance_approved"] = True
        st.success("✅ All 6 approvals succeeded — you are now fully live forever!")
        st.balloons()
        return True

    except Exception as e:
        st.error(f"Approval error: {e}")
        return False


# =============================================================================
# SIDEBAR - WALLET INFO + BALANCES
# =============================================================================

try:
    wallet_addr = get_wallet_address()
    st.sidebar.success(f"✅ {wallet_addr[:6]}...{wallet_addr[-4:]}")
except:
    st.sidebar.error("Invalid private key")
    st.stop()

# Wallet balances in sidebar - BIG GREEN NUMBERS
st.sidebar.markdown("---")
st.sidebar.subheader("💰 Balances")

try:
    usdc_bal = get_usdc_balance()
    matic_bal = get_matic_balance()

    if usdc_bal is not None:
        st.sidebar.markdown(f"<h2 style='color: #00c853; margin: 0;'>${usdc_bal:.2f} USDC</h2>", unsafe_allow_html=True)
    else:
        st.sidebar.warning("USDC: Error loading")

    if matic_bal is not None:
        st.sidebar.info(f"MATIC Balance: {matic_bal:.6f}")
    else:
        st.sidebar.warning("MATIC: Error loading")
except:
    st.sidebar.error("Balance check failed — check RPC")

st.sidebar.markdown("---")

if st.sidebar.button("🔒 Disconnect Wallet", use_container_width=True):
    st.session_state.wallet_connected = False
    st.session_state.private_key = ""
    st.session_state.client = None
    st.rerun()

st.sidebar.markdown("---")
st.sidebar.caption("gabagool style • Dec 2025 • 4X printing season")


# =============================================================================
# CLOB CLIENT INITIALIZATION
# =============================================================================

def get_clob_client() -> Optional[ClobClient]:
    """
    Get or create the ClobClient instance.
    Uses signature_type=0 for EOA wallets (MetaMask/Rabby).
    """
    # Return cached client if exists
    if st.session_state.client is not None:
        return st.session_state.client

    try:
        # Derive wallet address from private key
        wallet_address = get_wallet_address()

        # Initialize client with EOA signature type
        client = ClobClient(
            host=CLOB_HOST,
            key=PRIVATE_KEY,
            chain_id=CHAIN_ID,
            signature_type=0,  # EOA wallet
            funder=wallet_address
        )

        # Derive and set API credentials
        creds = client.create_or_derive_api_creds()
        client.set_api_creds(creds)

        # Cache in session state
        st.session_state.client = client

        return client

    except Exception as e:
        st.error(f"Failed to initialize CLOB client: {e}")
        return None


# =============================================================================
# MARKET DETECTION - BULLETPROOF FOR LIVE DATA
# =============================================================================

def detect_coin_from_question(question: str) -> Optional[str]:
    """
    Detect which coin this market is for.
    Returns the coin symbol (BTC, ETH, SOL, XRP) or None.
    """
    q_lower = question.lower()

    # Must be an "up or down" market
    if "up or down" not in q_lower:
        return None

    # Check for each supported coin keyword
    for keyword in COIN_KEYWORDS:
        if keyword in q_lower:
            return COIN_MAP.get(keyword)

    return None


def parse_market_time_flexible(question: str) -> Optional[Tuple[datetime, datetime]]:
    """
    Bulletproof time parsing for market questions.
    Uses dateutil.parser for maximum flexibility.
    """
    try:
        # Extract time string after the dash
        if " - " not in question:
            return None

        time_str = question.split(" - ", 1)[1]

        # Split into start and end times
        # Handle various separators: "-", "–", "to"
        time_str = time_str.replace("–", "-")  # en-dash to hyphen

        # Find the time range pattern
        # Could be "3:45PM-4:00PM ET" or "3:45 PM - 4:00 PM ET"
        parts = time_str.split("-")
        if len(parts) < 2:
            return None

        start_str = parts[0].strip()
        end_str = parts[1].strip()

        # Remove "ET" from end if present
        end_str = end_str.replace(" ET", "").replace("ET", "").strip()

        # Get today's date in ET
        now = datetime.now(ET)
        today_str = now.strftime("%B %d, %Y")  # "December 4, 2025"

        # Parse times with today's date
        try:
            start_dt = dateutil.parser.parse(f"{today_str} {start_str}")
            end_dt = dateutil.parser.parse(f"{today_str} {end_str}")
        except:
            # Try parsing from question itself if date is included
            # "December 4, 3:45PM-4:00PM ET"
            try:
                # Extract date from before the time
                date_match = re.search(r"(\w+ \d{1,2})", question)
                if date_match:
                    date_str = date_match.group(1) + f", {now.year}"
                    start_dt = dateutil.parser.parse(f"{date_str} {start_str}")
                    end_dt = dateutil.parser.parse(f"{date_str} {end_str}")
                else:
                    return None
            except:
                return None

        # Localize to ET
        start_dt = ET.localize(start_dt.replace(tzinfo=None))
        end_dt = ET.localize(end_dt.replace(tzinfo=None))

        # Handle midnight crossing
        if end_dt < start_dt:
            end_dt = end_dt.replace(day=end_dt.day + 1)

        return start_dt, end_dt

    except Exception:
        return None


def fetch_active_markets() -> List[Dict]:
    """Fetch all active markets from Polymarket CLOB API."""
    try:
        response = requests.get(
            f"{CLOB_HOST}/markets?active=true",
            timeout=15,
            headers={"Accept": "application/json"}
        )
        response.raise_for_status()
        data = response.json()

        # API returns list directly or {"data": [...]}
        if isinstance(data, dict) and "data" in data:
            return data["data"]
        elif isinstance(data, list):
            return data
        return []
    except requests.RequestException as e:
        st.warning(f"Error fetching markets: {e}")
        return []


def find_all_active_updown_markets() -> List[Dict]:
    """
    Find all currently active Up/Down 15-minute markets for supported coins.
    Bulletproof detection for live markets.
    """
    markets_data = fetch_active_markets()
    now = datetime.now(ET)
    active_markets = []

    for m in markets_data:
        try:
            # Skip inactive/closed markets
            if not m.get("active", False) or m.get("closed", False):
                continue

            question = m.get("question", "")
            q_lower = question.lower()

            # Check if it's an up/down market for supported coins
            if "up or down" not in q_lower:
                continue

            if not any(coin in q_lower for coin in COIN_KEYWORDS):
                continue

            # Detect coin
            coin = detect_coin_from_question(question)
            if coin is None:
                continue

            # Parse time window
            times = parse_market_time_flexible(question)
            if times is None:
                continue

            start_time, end_time = times

            # Check if current time is within market window
            if not (start_time <= now <= end_time):
                continue

            # Extract token IDs for Up and Down outcomes
            tokens = m.get("tokens", [])
            up_token = None
            down_token = None

            for token in tokens:
                outcome = token.get("outcome", "").lower()
                if outcome == "up":
                    up_token = token
                elif outcome == "down":
                    down_token = token

            if up_token and down_token:
                active_markets.append({
                    "condition_id": m.get("condition_id"),
                    "coin": coin,
                    "question": question,
                    "start_time": start_time,
                    "end_time": end_time,
                    "up_token_id": up_token["token_id"],
                    "down_token_id": down_token["token_id"],
                    "up_price": float(up_token.get("price", 0.5)),
                    "down_price": float(down_token.get("price", 0.5)),
                })
        except Exception:
            continue

    # Sort by coin name for consistent tab order
    coin_order = {"BTC": 0, "ETH": 1, "SOL": 2, "XRP": 3}
    active_markets.sort(key=lambda x: coin_order.get(x.get("coin", "ZZZ"), 99))

    return active_markets


def get_seconds_remaining(end_time: datetime) -> int:
    """Calculate seconds remaining until market ends."""
    now = datetime.now(ET)
    delta = end_time - now
    return max(0, int(delta.total_seconds()))


def format_countdown(seconds: int) -> str:
    """Format seconds as MM:SS countdown string."""
    minutes, secs = divmod(seconds, 60)
    return f"{minutes:02d}:{secs:02d}"


# =============================================================================
# MARKET STATE MANAGEMENT - CRASH-PROOF
# =============================================================================

def get_market_state(condition_id: str, coin: str) -> Dict:
    """Get or create state for a specific market. Always returns valid dict."""
    markets = st.session_state.state.get("markets", {})

    # Ensure markets exists in state
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
                # Check if there were any positions
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
            except Exception:
                pass

            to_archive.append(cid)

    # Remove archived markets
    for cid in to_archive:
        try:
            del markets[cid]
        except:
            pass

    # Keep only last 50 history entries
    st.session_state.state["history"] = history[:50]


# =============================================================================
# TRADING FUNCTIONS
# =============================================================================

def get_prices(client: ClobClient, up_token_id: str, down_token_id: str) -> Tuple[float, float, float, float]:
    """
    Get mid prices and ask prices for both tokens.
    Returns: (mid_up, mid_down, ask_up, ask_down)
    """
    try:
        # Get midpoints using built-in function
        mid_up = client.get_midpoint(up_token_id)
        mid_down = client.get_midpoint(down_token_id)

        # Get orderbooks for ask prices
        ob_up = client.get_order_book(up_token_id)
        ob_down = client.get_order_book(down_token_id)

        # Extract best ask prices
        ask_up = float(ob_up.asks[0].price) if ob_up.asks else 0.99
        ask_down = float(ob_down.asks[0].price) if ob_down.asks else 0.99

        return float(mid_up), float(mid_down), ask_up, ask_down

    except Exception:
        return 0.50, 0.50, 0.50, 0.50


def check_safety(mstate: Dict, side: str, seconds_remaining: int) -> Tuple[bool, str]:
    """
    Check if a trade is allowed under safety rules.
    Returns: (is_allowed, reason_if_blocked)
    """
    # Rule 1: No trading in final 90 seconds
    if seconds_remaining < NO_TRADE_SECONDS:
        return False, f"Trading disabled - {seconds_remaining}s remaining (min: {NO_TRADE_SECONDS}s)"

    # Rule 2: Check imbalance limits
    shares_up = mstate.get("shares_up", 0.0)
    shares_down = mstate.get("shares_down", 0.0)
    current_imbalance = abs(shares_up - shares_down)

    # Determine heavier side
    if shares_up > shares_down:
        heavier_side = "up"
    elif shares_down > shares_up:
        heavier_side = "down"
    else:
        heavier_side = None

    # If this trade would increase imbalance
    if heavier_side == side:
        if current_imbalance >= MAX_IMBALANCE:
            return False, f"Max imbalance reached ({current_imbalance:.0f}/{MAX_IMBALANCE})"
        if current_imbalance >= WARN_IMBALANCE:
            return False, f"Imbalance warning ({current_imbalance:.0f}/{WARN_IMBALANCE})"

    return True, ""


def should_disable_button(mstate: Dict, side: str, seconds_remaining: int) -> bool:
    """Determine if a buy button should be disabled."""
    # Disable if in final window
    if seconds_remaining < NO_TRADE_SECONDS:
        return True

    # Disable heavier side at warning threshold
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
    seconds_remaining: int
) -> Tuple[bool, str, float, float]:
    """
    Execute a market buy order with safety checks and order polling.
    Returns: (success, message, filled_size, actual_cost)
    """
    # Safety check
    is_allowed, reason = check_safety(mstate, side, seconds_remaining)
    if not is_allowed:
        return False, reason, 0, 0

    try:
        # Get order book to find best ask
        ob = client.get_order_book(token_id)

        if not ob.asks:
            return False, "No asks available in order book", 0, 0

        best_ask = float(ob.asks[0].price)

        # Calculate execution price (slightly above best ask)
        exec_price = round(best_ask + PRICE_SLIPPAGE, 3)
        exec_price = min(exec_price, 0.99)  # Cap at 0.99

        # Calculate share quantity
        size = cost_usd / best_ask
        if size < 0.01:
            return False, f"Order too small: {size:.4f} shares", 0, 0

        # Create order
        order_args = OrderArgs(
            token_id=token_id,
            price=exec_price,
            size=size,
            side="BUY"
        )

        # Execute order
        response = client.create_and_post_order(order_args)

        if not response or "orderID" not in response:
            error_msg = response.get("error", "Unknown error") if response else "No response"
            return False, f"Order failed: {error_msg}", 0, 0

        order_id = response["orderID"]

        # Poll for fill after 3 seconds
        time.sleep(3)

        try:
            order_status = client.get_order(order_id)
            filled_size = float(order_status.get("size_matched", 0))
        except:
            # If polling fails, assume fill at calculated size
            filled_size = size

        if filled_size <= 0:
            return False, "Order not filled - may need retry", 0, 0

        actual_cost = filled_size * exec_price

        # Update market state
        trade_record = {
            "time": datetime.now(ET).strftime("%H:%M:%S"),
            "side": side.upper(),
            "usdc": round(actual_cost, 2),
            "shares": round(filled_size, 2),
            "price": round(exec_price, 3)
        }

        if "trade_log" not in mstate:
            mstate["trade_log"] = []
        mstate["trade_log"].insert(0, trade_record)
        mstate["trade_log"] = mstate["trade_log"][:50]  # Keep last 50

        if side == "up":
            mstate["shares_up"] = mstate.get("shares_up", 0.0) + filled_size
            mstate["spent_up"] = mstate.get("spent_up", 0.0) + actual_cost
        else:
            mstate["shares_down"] = mstate.get("shares_down", 0.0) + filled_size
            mstate["spent_down"] = mstate.get("spent_down", 0.0) + actual_cost

        return True, f"Bought {filled_size:.2f} {side.upper()} @ ${exec_price:.3f}", filled_size, actual_cost

    except Exception as e:
        return False, f"Execution error: {str(e)}", 0, 0


# =============================================================================
# COMPUTED METRICS - COMPLETELY CRASH-PROOF
# =============================================================================

def calculate_metrics(mstate: Dict) -> Dict[str, Any]:
    """Calculate all position metrics for a market. Never crashes."""
    try:
        shares_up = mstate.get("shares_up", 0.0)
        shares_down = mstate.get("shares_down", 0.0)
        spent_up = mstate.get("spent_up", 0.0)
        spent_down = mstate.get("spent_down", 0.0)

        # Average costs - safe division
        if shares_up == 0:
            avg_up = 0
        else:
            avg_up = spent_up / shares_up

        if shares_down == 0:
            avg_down = 0
        else:
            avg_down = spent_down / shares_down

        # Pair cost (blended)
        avg_pair_cost = avg_up + avg_down if (shares_up > 0 and shares_down > 0) else 0

        # Locked shares (complete pairs)
        locked_shares = min(shares_up, shares_down)

        # Locked profit
        if locked_shares > 0 and avg_pair_cost > 0:
            locked_profit = locked_shares * (1 - avg_pair_cost)
        else:
            locked_profit = 0

        # Unbalanced
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


def calculate_projected_pair_cost(mstate: Dict, buy_amount: float, cheaper_side: str, cheaper_ask: float) -> float:
    """Calculate projected pair cost if user buys $X of cheaper side."""
    try:
        shares_up = mstate.get("shares_up", 0.0)
        shares_down = mstate.get("shares_down", 0.0)
        spent_up = mstate.get("spent_up", 0.0)
        spent_down = mstate.get("spent_down", 0.0)

        if cheaper_ask <= 0:
            return 0

        new_shares = buy_amount / cheaper_ask

        if cheaper_side == "up":
            new_shares_up = shares_up + new_shares
            new_spent_up = spent_up + buy_amount
            new_avg_up = new_spent_up / new_shares_up if new_shares_up > 0 else 0
            new_avg_down = spent_down / shares_down if shares_down > 0 else 0
        else:
            new_shares_down = shares_down + new_shares
            new_spent_down = spent_down + buy_amount
            new_avg_up = spent_up / shares_up if shares_up > 0 else 0
            new_avg_down = new_spent_down / new_shares_down if new_shares_down > 0 else 0

        return new_avg_up + new_avg_down
    except Exception:
        return 0


def get_pair_cost_color(pair_cost: float) -> str:
    """Get color indicator for pair cost gauge."""
    if pair_cost < 0.98:
        return "green"
    elif pair_cost <= 0.985:
        return "orange"
    else:
        return "red"


def get_total_locked_profit() -> float:
    """Calculate total locked profit across all active markets. CRASH-PROOF."""
    total = 0.0
    try:
        markets = st.session_state.state.get("markets", {})
        for mstate in markets.values():
            try:
                shares_up = mstate.get("shares_up", 0.0)
                spent_up = mstate.get("spent_up", 0.0)
                shares_down = mstate.get("shares_down", 0.0)
                spent_down = mstate.get("spent_down", 0.0)

                if shares_up == 0:
                    avg_up = 0
                else:
                    avg_up = spent_up / shares_up

                if shares_down == 0:
                    avg_down = 0
                else:
                    avg_down = spent_down / shares_down

                pair_cost = avg_up + avg_down
                locked = min(shares_up, shares_down)
                total += locked * (1 - pair_cost)
            except Exception:
                continue
    except Exception:
        pass
    return round(total, 3)


def get_total_history_profit() -> float:
    """Calculate total locked profit from all historical markets. CRASH-PROOF."""
    total = 0.0
    try:
        for entry in st.session_state.state.get("history", []):
            try:
                total += float(entry.get("locked_profit", 0))
            except:
                continue
    except Exception:
        pass
    return round(total, 3)


# =============================================================================
# MARKET TAB UI
# =============================================================================

def render_market_tab(market: Dict, client: ClobClient):
    """Render the full dashboard for a single market inside its tab."""
    condition_id = market["condition_id"]
    coin = market["coin"]
    mstate = get_market_state(condition_id, coin)

    seconds_remaining = get_seconds_remaining(market["end_time"])

    # Countdown + Question header
    col1, col2 = st.columns([3, 1])

    with col1:
        st.markdown(f"**{market['question']}**")

    with col2:
        countdown_color = "#ff5252" if seconds_remaining < NO_TRADE_SECONDS else "#00c853"
        st.markdown(
            f"<div style='font-size: 2.5em; font-weight: bold; color: {countdown_color}; text-align: center;'>{format_countdown(seconds_remaining)}</div>",
            unsafe_allow_html=True
        )
        if seconds_remaining < NO_TRADE_SECONDS:
            st.error("⏰ TRADING DISABLED")

    st.divider()

    # Get prices
    mid_up, mid_down, ask_up, ask_down = get_prices(client, market["up_token_id"], market["down_token_id"])

    # PAIR COST GAUGE - BIG AND COLORED
    pair_cost = mid_up + mid_down
    pair_color = get_pair_cost_color(pair_cost)

    color_hex = {"green": "#00c853", "orange": "#ffc107", "red": "#ff5252"}[pair_color]
    bg_hex = {"green": "#00c85322", "orange": "#ffc10722", "red": "#ff525222"}[pair_color]

    st.markdown(f"""
    <div style='background-color: {bg_hex}; padding: 20px; border-radius: 12px; text-align: center; margin-bottom: 20px;'>
        <div style='color: {color_hex}; font-size: 2.5em; font-weight: bold;'>
            PAIR COST: ${pair_cost:.4f}
        </div>
        <div style='color: #888; font-size: 0.9em; margin-top: 5px;'>
            🟢 &lt; $0.98 good | 🟡 $0.98-0.985 marginal | 🔴 &gt; $0.985 avoid
        </div>
    </div>
    """, unsafe_allow_html=True)

    # Prices row
    col1, col2 = st.columns(2)

    with col1:
        st.metric("⬆️ UP Mid", f"${mid_up:.4f}", delta=f"Ask: ${ask_up:.4f}")

    with col2:
        st.metric("⬇️ DOWN Mid", f"${mid_down:.4f}", delta=f"Ask: ${ask_down:.4f}")

    st.divider()

    # POSITION STATS
    metrics = calculate_metrics(mstate)

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric("UP Shares", f"{mstate.get('shares_up', 0):.2f}")
        if mstate.get('shares_up', 0) > 0:
            st.caption(f"Avg: ${metrics['avg_up']:.4f} | Spent: ${mstate.get('spent_up', 0):.2f}")

    with col2:
        st.metric("DOWN Shares", f"{mstate.get('shares_down', 0):.2f}")
        if mstate.get('shares_down', 0) > 0:
            st.caption(f"Avg: ${metrics['avg_down']:.4f} | Spent: ${mstate.get('spent_down', 0):.2f}")

    with col3:
        st.metric("Locked Pairs", f"{metrics['locked_shares']:.2f}")
        if metrics['locked_profit'] >= 0:
            st.markdown(f"<span style='color: #00c853; font-size: 1.3em; font-weight: bold;'>+${metrics['locked_profit']:.2f}</span>", unsafe_allow_html=True)
        else:
            st.markdown(f"<span style='color: #ff5252; font-size: 1.3em; font-weight: bold;'>${metrics['locked_profit']:.2f}</span>", unsafe_allow_html=True)

    with col4:
        st.metric("Unbalanced", f"{metrics['unbalanced']:.2f}")
        if metrics['imbalance_side']:
            st.caption(f"Heavy: {metrics['imbalance_side'].upper()}")
        if metrics['unbalanced'] >= WARN_IMBALANCE:
            st.warning("⚠️ High!")

    # Projected pair cost
    if mstate.get('shares_up', 0) > 0 or mstate.get('shares_down', 0) > 0:
        cheaper_side = "up" if ask_up < ask_down else "down"
        cheaper_ask = min(ask_up, ask_down)
        projected = calculate_projected_pair_cost(mstate, 50, cheaper_side, cheaper_ask)

        if projected > 0:
            st.info(f"💡 Buy $50 {cheaper_side.upper()} → new pair cost = **${projected:.4f}**")

    st.divider()

    # BUY BUTTONS
    st.markdown("### 🛒 Trade")

    if pair_cost > 0.99:
        st.error("⚠️ Pair cost > $0.99 - Avoid trading!")

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("**⬆️ Buy UP**")
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
                        success, msg, filled, cost = execute_market_buy(
                            client, market["up_token_id"], "up", amount,
                            mstate, seconds_remaining
                        )
                        if success:
                            st.success(msg)
                        else:
                            st.error(msg)
                        time.sleep(1)
                        st.rerun()

    with col2:
        st.markdown("**⬇️ Buy DOWN**")
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
                        success, msg, filled, cost = execute_market_buy(
                            client, market["down_token_id"], "down", amount,
                            mstate, seconds_remaining
                        )
                        if success:
                            st.success(msg)
                        else:
                            st.error(msg)
                        time.sleep(1)
                        st.rerun()

    # Trade log for this market
    trade_log = mstate.get("trade_log", [])
    if trade_log:
        st.divider()
        st.markdown("**📝 Recent Trades**")
        df = pd.DataFrame(trade_log[:10])
        st.dataframe(df, use_container_width=True, hide_index=True)


# =============================================================================
# MAIN APPLICATION
# =============================================================================

def main():
    """Main Streamlit application."""

    state = st.session_state.state

    # Custom CSS
    st.markdown("""
    <style>
        .big-font { font-size: 2em !important; font-weight: bold; }
        .profit-green { color: #00c853; }
        .profit-red { color: #ff5252; }
    </style>
    """, unsafe_allow_html=True)

    # =========================================================================
    # SETUP BUTTON (if allowances not approved)
    # =========================================================================
    if not state.get("allowance_approved", False):
        st.warning("⚠️ First-time setup required: Approve token spending")

        col1, col2, col3 = st.columns([1, 2, 1])
        with col2:
            if st.button("🔐 SETUP: Approve USDC & Token Allowances",
                        type="primary", use_container_width=True):
                with st.spinner("Sending approval transactions..."):
                    if approve_all_contracts():
                        st.rerun()

        st.divider()

    # =========================================================================
    # CLOB CLIENT
    # =========================================================================
    client = get_clob_client()
    if client is None:
        st.error("Failed to initialize trading client. Check your private key.")
        st.stop()

    # =========================================================================
    # PROFIT SUMMARY - TOP OF PAGE
    # =========================================================================
    active_profit = get_total_locked_profit()
    history_profit = get_total_history_profit()
    total_profit = active_profit + history_profit

    col1, col2, col3 = st.columns(3)

    with col1:
        if active_profit >= 0:
            st.markdown(f"<h3 style='color: #00c853;'>Active Locked: +${active_profit:.2f}</h3>", unsafe_allow_html=True)
        else:
            st.markdown(f"<h3 style='color: #ff5252;'>Active Locked: ${active_profit:.2f}</h3>", unsafe_allow_html=True)

    with col2:
        if history_profit >= 0:
            st.markdown(f"<h3 style='color: #888;'>Historical: +${history_profit:.2f}</h3>", unsafe_allow_html=True)
        else:
            st.markdown(f"<h3 style='color: #ff5252;'>Historical: ${history_profit:.2f}</h3>", unsafe_allow_html=True)

    with col3:
        if total_profit >= 0:
            st.markdown(f"<h3 style='color: #1fff9f;'>TOTAL: +${total_profit:.2f}</h3>", unsafe_allow_html=True)
        else:
            st.markdown(f"<h3 style='color: #ff5252;'>TOTAL: ${total_profit:.2f}</h3>", unsafe_allow_html=True)

    st.divider()

    # =========================================================================
    # MARKET DETECTION
    # =========================================================================
    active_markets = find_all_active_updown_markets()

    # Archive old markets
    active_ids = [m["condition_id"] for m in active_markets]
    archive_old_markets(active_ids)

    if not active_markets:
        st.markdown("""
        <div style='background-color: #1a1a2e; padding: 40px; border-radius: 16px; text-align: center; margin: 40px 0;'>
            <h2 style='color: #ffc107;'>⏳ Waiting for next 15-min windows...</h2>
            <p style='color: #888; font-size: 1.2em;'>Markets run 24/7 with occasional short gaps during oracle finalization.</p>
            <p style='color: #888;'>BTC, ETH, SOL, XRP markets refresh every 15 minutes.</p>
            <p style='color: #666; font-size: 0.9em;'>Next market should start within a few minutes.</p>
        </div>
        """, unsafe_allow_html=True)

        # Show current time
        now = datetime.now(ET)
        st.metric("Current Time (ET)", now.strftime("%I:%M:%S %p"))

        # Auto-refresh
        time.sleep(5)
        st.rerun()
        return

    # =========================================================================
    # MULTI-MARKET TABS
    # =========================================================================

    # Create tab names
    tab_names = [f"{m['coin']} 15m" for m in active_markets]

    if len(active_markets) == 1:
        # Single market - no tabs needed
        render_market_tab(active_markets[0], client)
    else:
        # Multiple markets - use tabs
        tabs = st.tabs(tab_names)

        for i, tab in enumerate(tabs):
            with tab:
                render_market_tab(active_markets[i], client)

    # =========================================================================
    # HISTORY
    # =========================================================================
    history = state.get("history", [])
    if history:
        st.divider()
        with st.expander(f"📊 Market History ({len(history)} completed)"):
            df_history = pd.DataFrame(history[:20])
            st.dataframe(df_history, use_container_width=True, hide_index=True)

    # =========================================================================
    # REFRESH
    # =========================================================================
    st.divider()

    col1, col2, col3 = st.columns([1, 1, 1])
    with col2:
        if st.button("🔄 Refresh Now", use_container_width=True):
            st.rerun()

    # Auto-refresh
    time.sleep(REFRESH_INTERVAL)
    st.rerun()


if __name__ == "__main__":
    main()
