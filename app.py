"""
================================================================================
POLYMARKET BITCOIN UP/DOWN 15-MINUTE TRADING BOT - CLOUD VERSION
================================================================================

A Streamlit-based manual trading bot for Polymarket's Bitcoin Up/Down 15-minute
prediction markets using the "gabagool-style" combo/hedge arbitrage strategy.

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

gabagool style - Dec 2025 - printing season with the bros
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

# py-clob-client imports
from py_clob_client.client import ClobClient
from py_clob_client.clob_types import OrderArgs

# =============================================================================
# PAGE CONFIGURATION
# =============================================================================

st.set_page_config(
    page_title="BTC Up/Down Bot - Cloud",
    page_icon="🔥",
    layout="wide",
    initial_sidebar_state="expanded"
)

# =============================================================================
# HEADER BANNER
# =============================================================================

st.markdown("""
<div style="background: linear-gradient(90deg, #1fff9f, #00c8ff); padding: 20px; border-radius: 12px; text-align: center; margin-bottom: 30px; box-shadow: 0 4px 20px rgba(0,0,0,0.2);">
    <h1 style="color: black; margin:0; font-size: 2.8em;">🔥 POLYMARKET 15-MIN BTC COMBO BOT 🔥</h1>
    <p style="color: black; margin:8px 0 0 0; font-size: 1.3em;"><strong>Cloud Version — Share this link with the boys</strong></p>
    <p style="color: black; margin:5px 0; font-size: 1.1em;">Each tab = independent wallet • Keep open 24/7 • Refresh keeps your position</p>
</div>
""", unsafe_allow_html=True)

# =============================================================================
# SESSION STATE INITIALIZATION
# =============================================================================

if 'state' not in st.session_state:
    st.session_state.state = {
        "current_market_id": "",
        "shares_up": 0.0,
        "spent_up": 0.0,
        "shares_down": 0.0,
        "spent_down": 0.0,
        "history": [],
        "allowance_approved": False,
        "trade_log": []  # list of dicts: {"time": ..., "side": "Up/Down", "usdc": amount, "shares": filled, "price": avg_price}
    }

if 'client' not in st.session_state:
    st.session_state.client = None

# =============================================================================
# SIDEBAR - WALLET SETUP
# =============================================================================

st.sidebar.header("🔑 Wallet Setup (private to your browser only)")
st.sidebar.markdown("Your private key stays in your browser session — never sent anywhere. Close tab = gone.")

# Initialize wallet_connected state
if 'wallet_connected' not in st.session_state:
    st.session_state.wallet_connected = False
    st.session_state.private_key = ""
    st.session_state.rpc_url = "https://polygon-rpc.com"

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
                from eth_account import Account
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
    st.sidebar.caption("gabagool style • Dec 2025 • printing season with the bros")
    st.warning("⚠️ Enter your private key and click 'Connect Wallet' to start")
    st.stop()

# Wallet is connected - show status and disconnect option
PRIVATE_KEY = st.session_state.private_key
POLYGON_RPC_URL = st.session_state.rpc_url

if st.sidebar.button("🔒 Disconnect Wallet", use_container_width=True):
    st.session_state.wallet_connected = False
    st.session_state.private_key = ""
    st.session_state.client = None
    st.rerun()

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

# Trading safety parameters
MAX_IMBALANCE = 500          # Max allowed share imbalance
WARN_IMBALANCE = 400         # Threshold to disable heavier side
NO_TRADE_SECONDS = 90        # No trades in last 90 seconds
PRICE_SLIPPAGE = 0.006       # Cross spread by 0.6 cents
BUY_AMOUNTS = [10, 25, 50, 100]  # USD amounts for buy buttons
REFRESH_INTERVAL = 8         # Seconds between auto-refresh

# Timezone
ET = pytz.timezone("America/New_York")

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
    except Exception as e:
        st.error(f"Error getting USDC balance: {e}")
        return None


def get_pol_balance() -> Optional[float]:
    """Get POL (native token) balance for gas fees."""
    try:
        web3 = get_web3()
        if not web3.is_connected():
            return None

        wallet_address = get_wallet_address()
        raw_balance = web3.eth.get_balance(wallet_address)
        return float(Web3.from_wei(raw_balance, "ether"))
    except Exception as e:
        st.error(f"Error getting POL balance: {e}")
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
# MARKET DETECTION
# =============================================================================

# Pattern to match Bitcoin Up/Down market questions
# Example: "Bitcoin Up or Down - December 3, 3:45PM-4:00PM ET"
MARKET_PATTERN = re.compile(
    r"Bitcoin Up or Down.*?-\s*(\w+)\s+(\d+),?\s*(\d{1,2}):(\d{2})\s*(AM|PM)\s*-\s*(\d{1,2}):(\d{2})\s*(AM|PM)\s*ET",
    re.IGNORECASE
)


def parse_market_time(question: str) -> Optional[Tuple[datetime, datetime]]:
    """
    Parse market start and end times from the question string.

    Args:
        question: Market question like "Bitcoin Up or Down - December 3, 3:45PM-4:00PM ET"

    Returns:
        Tuple of (start_time, end_time) as timezone-aware datetime in ET,
        or None if parsing fails
    """
    match = MARKET_PATTERN.search(question)
    if not match:
        return None

    month_name, day, start_hour, start_min, start_ampm, end_hour, end_min, end_ampm = match.groups()

    # Convert to 24-hour format
    start_h = int(start_hour)
    end_h = int(end_hour)

    if start_ampm.upper() == "PM" and start_h != 12:
        start_h += 12
    elif start_ampm.upper() == "AM" and start_h == 12:
        start_h = 0

    if end_ampm.upper() == "PM" and end_h != 12:
        end_h += 12
    elif end_ampm.upper() == "AM" and end_h == 12:
        end_h = 0

    # Get current year
    now = datetime.now(ET)
    year = now.year

    # Parse month
    month_map = {
        "january": 1, "february": 2, "march": 3, "april": 4,
        "may": 5, "june": 6, "july": 7, "august": 8,
        "september": 9, "october": 10, "november": 11, "december": 12
    }
    month = month_map.get(month_name.lower())
    if not month:
        return None

    try:
        start_time = ET.localize(datetime(year, month, int(day), start_h, int(start_min)))
        end_time = ET.localize(datetime(year, month, int(day), end_h, int(end_min)))

        # Handle midnight crossing (e.g., 11:45PM - 12:00AM)
        if end_time < start_time:
            end_time = end_time.replace(day=end_time.day + 1)

        return start_time, end_time
    except ValueError:
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

        # API returns {"data": [...]} or direct list
        if isinstance(data, dict) and "data" in data:
            return data["data"]
        elif isinstance(data, list):
            return data
        return []
    except requests.RequestException as e:
        st.warning(f"Error fetching markets: {e}")
        return []


def find_active_bitcoin_market() -> Optional[Dict]:
    """
    Find the currently active Bitcoin Up/Down 15-minute market.

    Returns:
        Market dict with condition_id, tokens, question, and parsed times,
        or None if no suitable market found
    """
    markets = fetch_active_markets()
    now = datetime.now(ET)

    for market in markets:
        # Skip inactive/closed markets
        if not market.get("active", False) or market.get("closed", False):
            continue

        question = market.get("question", "")

        # Check if it's a Bitcoin Up/Down market
        if "bitcoin up or down" not in question.lower():
            continue

        times = parse_market_time(question)
        if times is None:
            continue

        start_time, end_time = times

        # Check if current time is within market window
        if start_time <= now <= end_time:
            # Extract token IDs for Up and Down outcomes
            tokens = market.get("tokens", [])
            up_token = None
            down_token = None

            for token in tokens:
                outcome = token.get("outcome", "").lower()
                if outcome == "up":
                    up_token = token
                elif outcome == "down":
                    down_token = token

            if up_token and down_token:
                return {
                    "condition_id": market.get("condition_id"),
                    "question": question,
                    "start_time": start_time,
                    "end_time": end_time,
                    "up_token_id": up_token["token_id"],
                    "down_token_id": down_token["token_id"],
                    "up_price": float(up_token.get("price", 0.5)),
                    "down_price": float(down_token.get("price", 0.5)),
                }

    return None


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
# TRADING FUNCTIONS
# =============================================================================

def get_prices(client: ClobClient, up_token_id: str, down_token_id: str) -> Tuple[float, float, float, float]:
    """
    Get mid prices and ask prices for both tokens.

    Returns:
        Tuple of (mid_up, mid_down, ask_up, ask_down)
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

        return mid_up, mid_down, ask_up, ask_down

    except Exception as e:
        st.warning(f"Error fetching prices: {e}")
        return 0.50, 0.50, 0.50, 0.50


def check_safety(side: str, seconds_remaining: int) -> Tuple[bool, str]:
    """
    Check if a trade is allowed under safety rules.

    Returns:
        Tuple of (is_allowed, reason_if_blocked)
    """
    state = st.session_state.state

    # Rule 1: No trading in final 90 seconds
    if seconds_remaining < NO_TRADE_SECONDS:
        return False, f"Trading disabled - {seconds_remaining}s remaining (min: {NO_TRADE_SECONDS}s)"

    # Rule 2: Check imbalance limits
    shares_up = state["shares_up"]
    shares_down = state["shares_down"]
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


def should_disable_button(side: str, seconds_remaining: int) -> bool:
    """Determine if a buy button should be disabled."""
    state = st.session_state.state

    # Disable if in final window
    if seconds_remaining < NO_TRADE_SECONDS:
        return True

    # Disable heavier side at warning threshold
    shares_up = state["shares_up"]
    shares_down = state["shares_down"]
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
    market_id: str,
    seconds_remaining: int
) -> Tuple[bool, str, float, float]:
    """
    Execute a market buy order with safety checks and order polling.

    Returns:
        Tuple of (success, message, filled_size, actual_cost)
    """
    state = st.session_state.state

    # Safety check
    is_allowed, reason = check_safety(side, seconds_remaining)
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

        # Update session state
        trade_record = {
            "time": datetime.now(ET).strftime("%H:%M:%S"),
            "side": side.upper(),
            "usdc": round(actual_cost, 2),
            "shares": round(filled_size, 2),
            "price": round(exec_price, 3)
        }
        state["trade_log"].insert(0, trade_record)
        state["trade_log"] = state["trade_log"][:50]  # Keep last 50

        if side == "up":
            state["shares_up"] += filled_size
            state["spent_up"] += actual_cost
        else:
            state["shares_down"] += filled_size
            state["spent_down"] += actual_cost

        return True, f"Bought {filled_size:.2f} {side.upper()} @ ${exec_price:.3f}", filled_size, actual_cost

    except Exception as e:
        return False, f"Execution error: {str(e)}", 0, 0


# =============================================================================
# COMPUTED METRICS
# =============================================================================

def calculate_metrics() -> Dict[str, Any]:
    """Calculate all position metrics."""
    state = st.session_state.state

    shares_up = state["shares_up"]
    shares_down = state["shares_down"]
    spent_up = state["spent_up"]
    spent_down = state["spent_down"]

    # Average costs
    avg_up = spent_up / shares_up if shares_up > 0 else 0
    avg_down = spent_down / shares_down if shares_down > 0 else 0

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


def calculate_projected_pair_cost(
    buy_amount: float,
    cheaper_side: str,
    cheaper_ask: float
) -> float:
    """
    Calculate projected pair cost if user buys $X of cheaper side.
    """
    state = st.session_state.state

    shares_up = state["shares_up"]
    shares_down = state["shares_down"]
    spent_up = state["spent_up"]
    spent_down = state["spent_down"]

    new_shares = buy_amount / cheaper_ask if cheaper_ask > 0 else 0

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


def get_pair_cost_color(pair_cost: float) -> str:
    """Get color indicator for pair cost gauge."""
    if pair_cost < 0.98:
        return "green"
    elif pair_cost <= 0.985:
        return "orange"
    else:
        return "red"


def get_total_history_profit() -> float:
    """Calculate total locked profit from all historical markets."""
    total = 0.0
    for entry in st.session_state.state.get("history", []):
        total += entry.get("locked_profit", 0)
    return total


def reset_for_new_market(new_market_id: str, old_question: str = ""):
    """Archive current market and reset for a new one."""
    state = st.session_state.state

    # Archive if there was an active market with positions
    if state["current_market_id"] and (state["shares_up"] > 0 or state["shares_down"] > 0):
        # Calculate locked profit for history
        locked_shares = min(state["shares_up"], state["shares_down"])
        if locked_shares > 0:
            avg_up = state["spent_up"] / state["shares_up"] if state["shares_up"] > 0 else 0
            avg_down = state["spent_down"] / state["shares_down"] if state["shares_down"] > 0 else 0
            pair_cost = avg_up + avg_down
            locked_profit = locked_shares * (1 - pair_cost)
        else:
            locked_profit = 0

        history_entry = {
            "market_id": state["current_market_id"][:16] + "...",
            "question": old_question[:40] + "..." if len(old_question) > 40 else old_question,
            "end_time": datetime.now(ET).strftime("%H:%M"),
            "shares_up": round(state["shares_up"], 2),
            "shares_down": round(state["shares_down"], 2),
            "locked_profit": round(locked_profit, 2)
        }
        state["history"].insert(0, history_entry)
        state["history"] = state["history"][:20]  # Keep last 20

    # Reset for new market
    state["current_market_id"] = new_market_id
    state["shares_up"] = 0.0
    state["spent_up"] = 0.0
    state["shares_down"] = 0.0
    state["spent_down"] = 0.0
    state["trade_log"] = []


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
        .countdown { font-size: 3em; font-weight: bold; text-align: center; }
        .profit-green { color: #00c853; font-size: 1.5em; font-weight: bold; }
        .profit-red { color: #ff5252; font-size: 1.5em; font-weight: bold; }
        .warning-yellow { color: #ffc107; }
        .pair-green { background-color: #00c85322; padding: 10px; border-radius: 5px; }
        .pair-yellow { background-color: #ffc10722; padding: 10px; border-radius: 5px; }
        .pair-red { background-color: #ff525222; padding: 10px; border-radius: 5px; }
    </style>
    """, unsafe_allow_html=True)

    # Show wallet address in sidebar
    try:
        wallet_addr = get_wallet_address()
        st.sidebar.success(f"Wallet: {wallet_addr[:8]}...{wallet_addr[-6:]}")
    except:
        st.sidebar.error("Invalid private key")
        st.stop()

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
    # MARKET DETECTION
    # =========================================================================
    market = find_active_bitcoin_market()

    if market is None:
        st.warning("🔍 No active Bitcoin Up/Down market found. Waiting for next window...")
        st.info("Markets run every 15 minutes, 24/7. Next market should start shortly.")

        # Show current time
        now = datetime.now(ET)
        st.metric("Current Time (ET)", now.strftime("%I:%M:%S %p"))

        # Sidebar footer
        st.sidebar.markdown("---")
        st.sidebar.caption("gabagool style • Dec 2025 • printing season with the bros")

        # Auto-refresh
        time.sleep(5)
        st.rerun()
        return

    # Check if market changed
    if state["current_market_id"] != market["condition_id"]:
        old_question = state.get("current_question", "")
        reset_for_new_market(market["condition_id"], old_question)
        state["current_question"] = market["question"]

    # Calculate timing
    seconds_remaining = get_seconds_remaining(market["end_time"])

    # =========================================================================
    # HEADER: Market Info + Countdown
    # =========================================================================
    col1, col2 = st.columns([3, 1])

    with col1:
        st.subheader(market["question"])
        st.caption(f"Market ID: {market['condition_id'][:20]}...")

    with col2:
        countdown_color = "#ff5252" if seconds_remaining < NO_TRADE_SECONDS else "#00c853"
        st.markdown(
            f"<div class='countdown' style='color: {countdown_color}'>{format_countdown(seconds_remaining)}</div>",
            unsafe_allow_html=True
        )
        if seconds_remaining < NO_TRADE_SECONDS:
            st.error("⏰ TRADING DISABLED")

    st.divider()

    # =========================================================================
    # PRICES
    # =========================================================================
    mid_up, mid_down, ask_up, ask_down = get_prices(
        client, market["up_token_id"], market["down_token_id"]
    )

    col1, col2, col3 = st.columns(3)

    with col1:
        st.metric("UP Mid Price", f"${mid_up:.3f}")
        st.caption(f"Ask: ${ask_up:.3f}")

    with col2:
        st.metric("DOWN Mid Price", f"${mid_down:.3f}")
        st.caption(f"Ask: ${ask_down:.3f}")

    with col3:
        # Live pair cost gauge (using mid prices)
        pair_cost = mid_up + mid_down
        pair_color = get_pair_cost_color(pair_cost)

        color_hex = {"green": "#00c853", "orange": "#ffc107", "red": "#ff5252"}[pair_color]
        bg_class = {"green": "pair-green", "orange": "pair-yellow", "red": "pair-red"}[pair_color]

        st.markdown(f"""
        <div class='{bg_class}'>
            <div style='color: {color_hex}; font-size: 1.8em; font-weight: bold;'>
                Pair Cost: ${pair_cost:.3f}
            </div>
            <small>Green &lt; $0.98 | Yellow $0.98-0.985 | Red &gt; $0.985</small>
        </div>
        """, unsafe_allow_html=True)

    st.divider()

    # =========================================================================
    # POSITION STATS
    # =========================================================================
    st.subheader("📊 Position")

    metrics = calculate_metrics()

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric("UP Shares", f"{state['shares_up']:.2f}")
        if state['shares_up'] > 0:
            st.caption(f"Avg: ${metrics['avg_up']:.3f}")
            st.caption(f"Spent: ${state['spent_up']:.2f}")

    with col2:
        st.metric("DOWN Shares", f"{state['shares_down']:.2f}")
        if state['shares_down'] > 0:
            st.caption(f"Avg: ${metrics['avg_down']:.3f}")
            st.caption(f"Spent: ${state['spent_down']:.2f}")

    with col3:
        st.metric("Locked Shares", f"{metrics['locked_shares']:.2f}")

        if metrics['locked_profit'] >= 0:
            st.markdown(f"<div class='profit-green'>Locked: +${metrics['locked_profit']:.2f}</div>",
                       unsafe_allow_html=True)
        else:
            st.markdown(f"<div class='profit-red'>Locked: ${metrics['locked_profit']:.2f}</div>",
                       unsafe_allow_html=True)

    with col4:
        st.metric("Unbalanced", f"{metrics['unbalanced']:.2f}")
        if metrics['imbalance_side']:
            st.caption(f"Heavy: {metrics['imbalance_side'].upper()}")

        if metrics['unbalanced'] >= WARN_IMBALANCE:
            st.warning(f"⚠️ High imbalance!")

    # Projected pair cost
    if state['shares_up'] > 0 or state['shares_down'] > 0:
        cheaper_side = "up" if ask_up < ask_down else "down"
        cheaper_ask = min(ask_up, ask_down)
        projected = calculate_projected_pair_cost(50, cheaper_side, cheaper_ask)

        if projected > 0:
            st.info(f"💡 If you buy $50 of {cheaper_side.upper()} now → new pair cost = **${projected:.4f}**")

    st.divider()

    # =========================================================================
    # BUY BUTTONS
    # =========================================================================
    st.subheader("🛒 Trade")

    # Warning for pair cost
    if pair_cost > 0.99:
        st.error("⚠️ Pair cost > $0.99 - Avoid trading!")

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("**Buy UP**")
        up_disabled = should_disable_button("up", seconds_remaining)

        cols = st.columns(len(BUY_AMOUNTS))
        for i, amount in enumerate(BUY_AMOUNTS):
            with cols[i]:
                if st.button(
                    f"${amount}",
                    key=f"buy_up_{amount}",
                    disabled=up_disabled,
                    use_container_width=True
                ):
                    with st.spinner(f"Buying ${amount} UP..."):
                        success, msg, filled, cost = execute_market_buy(
                            client, market["up_token_id"], "up", amount,
                            market["condition_id"], seconds_remaining
                        )
                        if success:
                            st.success(msg)
                        else:
                            st.error(msg)
                        time.sleep(1)
                        st.rerun()

    with col2:
        st.markdown("**Buy DOWN**")
        down_disabled = should_disable_button("down", seconds_remaining)

        cols = st.columns(len(BUY_AMOUNTS))
        for i, amount in enumerate(BUY_AMOUNTS):
            with cols[i]:
                if st.button(
                    f"${amount}",
                    key=f"buy_down_{amount}",
                    disabled=down_disabled,
                    use_container_width=True
                ):
                    with st.spinner(f"Buying ${amount} DOWN..."):
                        success, msg, filled, cost = execute_market_buy(
                            client, market["down_token_id"], "down", amount,
                            market["condition_id"], seconds_remaining
                        )
                        if success:
                            st.success(msg)
                        else:
                            st.error(msg)
                        time.sleep(1)
                        st.rerun()

    st.divider()

    # =========================================================================
    # WALLET BALANCES
    # =========================================================================
    st.subheader("💰 Wallet")

    col1, col2, col3 = st.columns(3)

    with col1:
        usdc_balance = get_usdc_balance()
        if usdc_balance is not None:
            st.metric("USDC Balance", f"${usdc_balance:.2f}")
        else:
            st.metric("USDC Balance", "Error")

    with col2:
        pol_balance = get_pol_balance()
        if pol_balance is not None:
            st.metric("POL (Gas)", f"{pol_balance:.4f}")
        else:
            st.metric("POL (Gas)", "Error")

    with col3:
        st.metric("Wallet", f"{wallet_addr[:8]}...{wallet_addr[-6:]}")

    st.divider()

    # =========================================================================
    # TRADE LOG
    # =========================================================================
    st.subheader("📝 Trade Log")

    trade_log = state.get("trade_log", [])
    if trade_log:
        df = pd.DataFrame(trade_log[:20])
        st.dataframe(df, use_container_width=True, hide_index=True)
    else:
        st.caption("No trades yet this market")

    # =========================================================================
    # HISTORY
    # =========================================================================
    history = state.get("history", [])
    if history:
        with st.expander(f"📊 Market History ({len(history)} markets)"):
            total_profit = get_total_history_profit()

            if total_profit >= 0:
                st.markdown(f"<div class='profit-green'>Total Historical Profit: +${total_profit:.2f}</div>",
                           unsafe_allow_html=True)
            else:
                st.markdown(f"<div class='profit-red'>Total Historical Profit: ${total_profit:.2f}</div>",
                           unsafe_allow_html=True)

            st.divider()

            df_history = pd.DataFrame(history[:10])
            st.dataframe(df_history, use_container_width=True, hide_index=True)

    # =========================================================================
    # REFRESH BUTTON + AUTO-REFRESH
    # =========================================================================
    st.divider()

    col1, col2, col3 = st.columns([1, 1, 1])
    with col2:
        if st.button("🔄 Refresh Now", use_container_width=True):
            st.rerun()

    # Sidebar footer
    st.sidebar.markdown("---")
    st.sidebar.caption("gabagool style • Dec 2025 • printing season with the bros")

    # Auto-refresh
    time.sleep(REFRESH_INTERVAL)
    st.rerun()


if __name__ == "__main__":
    main()
