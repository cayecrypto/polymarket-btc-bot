"""
================================================================================
POLYMARKET 15-MIN COMBO PRINTER — ARB ENGINE V2
================================================================================

Headless Railway worker for automated arbitrage trading on Polymarket's
15-minute Up/Down prediction markets.

STRATEGY:
    Buy both UP and DOWN tokens to lock in guaranteed profit when pair cost < $1.
    Each pair pays out exactly $1 at resolution regardless of outcome.

ENV VARS REQUIRED:
    - DATABASE_URL: Postgres connection string
    - DRY_RUN: "true" or "false" (default: true)
    - AUTO_MODE: "true" or "false" (default: false)
    - POLYMARKET_API_KEY, POLYMARKET_API_SECRET, POLYMARKET_API_PASSPHRASE
    - POLYMARKET_PRIVATE_KEY
    - RPC_URL (optional, defaults to polygon-rpc.com)

================================================================================
"""

import os
import time
import json
import logging
from datetime import datetime, timezone
from typing import Optional, Dict, Any, Tuple, List
from concurrent.futures import ThreadPoolExecutor, as_completed

from dotenv import load_dotenv
load_dotenv()  # Load .env file if present

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import pytz
import psycopg2
from psycopg2 import OperationalError
from web3 import Web3
from eth_account import Account

from py_clob_client.client import ClobClient
from py_clob_client.clob_types import OrderArgs, ApiCreds

# =============================================================================
# WEBSOCKET CLIENT (optional - for real-time prices)
# =============================================================================

try:
    from ws_client import start_ws_listener, stop_ws_listener, get_ws_price, is_ws_fresh, update_subscriptions
    WS_AVAILABLE = True
except ImportError:
    WS_AVAILABLE = False
    logger = None  # Will be set after logging setup

    def start_ws_listener(token_ids):
        pass

    def stop_ws_listener():
        pass

    def get_ws_price(token_id):
        return None

    def is_ws_fresh(token_id, max_age=1.5):
        return False

    def update_subscriptions(token_ids):
        pass


# =============================================================================
# SHARED HTTP SESSION (connection pooling & keep-alive)
# =============================================================================

def create_http_session() -> requests.Session:
    """Create a persistent HTTP session with connection pooling."""
    session = requests.Session()

    # Set up retry strategy
    retry_strategy = Retry(
        total=2,
        backoff_factor=0.1,
        status_forcelist=[429, 500, 502, 503, 504],
    )

    # Mount adapters with connection pooling
    adapter = HTTPAdapter(
        pool_connections=10,
        pool_maxsize=20,
        max_retries=retry_strategy
    )
    session.mount("https://", adapter)
    session.mount("http://", adapter)

    # Set headers for Cloudflare bypass
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Connection": "keep-alive",
    })

    return session


# Global shared HTTP session (created at module load)
_http_session: Optional[requests.Session] = None


def get_http_session() -> requests.Session:
    """Get or create the shared HTTP session."""
    global _http_session
    if _http_session is None:
        _http_session = create_http_session()
    return _http_session

# =============================================================================
# HTTPX MONKEY-PATCH — CLOUDFLARE BYPASS
# =============================================================================

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
# LOGGING SETUP
# =============================================================================

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("arb-engine")

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

# Trading parameters (from app.py)
TARGET_PAIR_COST = 0.982
MIN_IMPROVEMENT_REQUIRED = 0.004
MAX_DIRECTIONAL_RISK_PCT = 0.35
MAX_TRADE_PCT = 0.12
MIN_TRADE_USD = 2
MAX_TRADE_USD = 100
MIN_TIME_REMAINING = 90
AUTO_TRADE_COOLDOWN = 15
MAX_IMBALANCE = 500
WARN_IMBALANCE = 400
NO_TRADE_SECONDS = 90
PRICE_SLIPPAGE = 0.006

# =============================================================================
# SAFETY LAYER CONSTANTS (pre-live trading hardening)
# =============================================================================

# SAFETY LAYER 1: Time-to-expiry cutoff
# Do NOT open new trades when window is about to close (prevents late entries)
MIN_SECONDS_TO_OPEN_TRADE = 25  # Minimum seconds remaining to open a new trade

# SAFETY LAYER 2: Order-book freshness
# Maximum age of order book data before we refuse to trade on it
MAX_BOOK_AGE_SECONDS = 1.5  # If book is older than 1.5s, skip trading

# SAFETY LAYER 4: Directional exposure cap (per market)
# Prevent over-tilting to one side on any single market
MAX_DIRECTIONAL_EXPOSURE_FRACTION = 0.35  # 35% of bankroll max directional exposure

ET = pytz.timezone("US/Eastern")

# Engine parameters
TICK_INTERVAL = 0.5              # Fast tick: 0.5s
DISCOVERY_INTERVAL = 60.0        # Slow discovery: 60s
HEARTBEAT_INTERVAL = 10
STALE_MIDPOINT_THRESHOLD = 3.0   # seconds
DB_TICK_THROTTLE = 3.0           # Only write engine_state every 3s

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


# =============================================================================
# SAFE CALL WRAPPER
# =============================================================================

def safe_call(func, *args, default=None, **kwargs):
    """Execute function with exception handling, return default on failure."""
    try:
        return func(*args, **kwargs)
    except Exception as e:
        logger.warning(f"safe_call failed: {func.__name__} - {e}")
        return default


# =============================================================================
# DATABASE FUNCTIONS (with auto-reconnect)
# =============================================================================

_db_connection = None

def get_db_connection():
    """Get or create Postgres connection with auto-reconnect."""
    global _db_connection

    # Test existing connection
    if _db_connection is not None:
        try:
            cur = _db_connection.cursor()
            cur.execute("SELECT 1")
            cur.close()
            return _db_connection
        except (OperationalError, Exception):
            logger.warning("Database connection lost, reconnecting...")
            try:
                _db_connection.close()
            except Exception:
                pass
            _db_connection = None

    # Create new connection
    try:
        database_url = os.environ.get("DATABASE_URL")
        if not database_url:
            logger.warning("DATABASE_URL not set, DB operations will be skipped")
            return None

        _db_connection = psycopg2.connect(database_url)
        _db_connection.autocommit = True
        logger.info("Database connection established")
        return _db_connection
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        _db_connection = None
        return None


def init_db_schema():
    """Create tables if they don't exist and add new columns for backtest workflow."""
    conn = get_db_connection()
    if not conn:
        return

    try:
        cur = conn.cursor()

        cur.execute("""
            CREATE TABLE IF NOT EXISTS engine_state (
                key TEXT PRIMARY KEY,
                value JSONB,
                updated_at TIMESTAMP DEFAULT NOW()
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS trade_logs (
                id SERIAL PRIMARY KEY,
                timestamp TIMESTAMP DEFAULT NOW(),
                market TEXT,
                side TEXT,
                amount_usd NUMERIC,
                shares NUMERIC,
                price NUMERIC,
                pair_cost NUMERIC,
                locked_profit NUMERIC,
                dry_run BOOLEAN,
                success BOOLEAN,
                error TEXT,
                tx_hash TEXT
            )
        """)

        # Add new columns for 12-hour backtest workflow (if they don't exist)
        # These columns enable comprehensive trade analysis
        new_columns = [
            ("coin", "VARCHAR(10)"),
            ("trade_type", "VARCHAR(50)"),
            ("avg_yes_cost_after", "NUMERIC"),
            ("avg_no_cost_after", "NUMERIC"),
            ("locked_shares", "NUMERIC"),
            ("projected_final_profit", "NUMERIC"),
            ("condition_id", "TEXT"),  # For debugging position persistence
        ]

        for col_name, col_type in new_columns:
            try:
                cur.execute(f"""
                    ALTER TABLE trade_logs ADD COLUMN IF NOT EXISTS {col_name} {col_type}
                """)
            except Exception as col_err:
                logger.debug(f"Column {col_name} may already exist: {col_err}")

        # Create eval_logs table for diagnostics (instrumentation for DRY_RUN validation)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS eval_logs (
                id BIGSERIAL PRIMARY KEY,
                ts TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                coin TEXT NOT NULL,
                market_id TEXT NOT NULL,
                expiry_ts TIMESTAMPTZ,
                side_considered TEXT NOT NULL,
                decision TEXT NOT NULL,
                reason TEXT NOT NULL,
                current_qty_yes NUMERIC,
                current_qty_no NUMERIC,
                current_pair_cost NUMERIC,
                projected_pair_cost NUMERIC,
                time_to_expiry_s NUMERIC,
                directional_exposure NUMERIC
            )
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_eval_logs_ts ON eval_logs(ts)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_eval_logs_coin ON eval_logs(coin)")

        cur.close()
        logger.info("Database schema initialized (including backtest columns and eval_logs)")
    except Exception as e:
        logger.error(f"Schema init failed: {e}")


def db_write(query: str, params: tuple):
    """Execute a write query with auto-reconnect and commit."""
    conn = get_db_connection()
    if not conn:
        return False

    try:
        cur = conn.cursor()
        cur.execute(query, params)
        cur.close()
        return True
    except OperationalError as e:
        logger.warning(f"DB OperationalError, will reconnect: {e}")
        global _db_connection
        _db_connection = None
        return False
    except Exception as e:
        logger.warning(f"DB write failed: {e}")
        return False


# =============================================================================
# EVAL DECISION LOGGING (Non-invasive instrumentation for DRY_RUN validation)
# =============================================================================

def log_eval_decision(
    coin: str,
    market_id: str,
    expiry_ts,
    side_considered: str,
    decision: str,
    reason: str,
    current_qty_yes: float = None,
    current_qty_no: float = None,
    current_pair_cost: float = None,
    projected_pair_cost: float = None,
    time_to_expiry_s: float = None,
    directional_exposure: float = None
) -> None:
    """
    Log evaluation decisions for diagnostics. Non-blocking, never affects trading.
    Only logs NEAR-MISS rejections (projected_pair_cost <= TARGET + 0.01) or EXECUTE.
    """
    try:
        conn = get_db_connection()
        if not conn:
            return
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO eval_logs (
                ts, coin, market_id, expiry_ts, side_considered, decision, reason,
                current_qty_yes, current_qty_no, current_pair_cost, projected_pair_cost,
                time_to_expiry_s, directional_exposure
            ) VALUES (NOW(), %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            coin, market_id, expiry_ts, side_considered, decision, reason,
            current_qty_yes, current_qty_no, current_pair_cost, projected_pair_cost,
            time_to_expiry_s, directional_exposure
        ))
        cur.close()
    except Exception:
        pass  # Never break engine loop


def write_tick(
    tick_time: datetime,
    markets_found: int,
    opportunities: int = 0,
    tick_count: int = 0,
    wallet_usdc: float = 0.0,
    binance_prices: Optional[Dict[str, Dict]] = None,
    latest_pairs: Optional[Dict[str, Dict]] = None,
    dry_run: bool = True,
    auto_mode: bool = False
):
    """
    Upsert last_tick to engine_state table with rich context data.

    Args:
        tick_time: Current tick timestamp
        markets_found: Number of active markets
        opportunities: Number of trade opportunities found
        tick_count: Monotonically increasing tick counter
        wallet_usdc: Current USDC balance
        binance_prices: Dict of {coin: {price, change}} from Binance
        latest_pairs: Dict of {coin: {pair_cost, up_price, down_price}} live market data
        dry_run: Whether engine is in DRY_RUN mode
        auto_mode: Whether AUTO_MODE is enabled
    """
    # Build the payload
    payload = {
        "markets_found": markets_found,
        "opportunities": opportunities,
        "timestamp": tick_time.isoformat(),
        "tick": tick_count,
        "wallet_usdc": round(wallet_usdc, 2),
        "dry_run": dry_run,
        "auto_mode": auto_mode,
    }

    # Add binance prices if available
    if binance_prices:
        # Convert from {"BTCUSDT": {...}} to {"BTC": {...}}
        payload["binance_prices"] = {
            "BTC": binance_prices.get("BTCUSDT", {"price": 0, "change": 0}),
            "ETH": binance_prices.get("ETHUSDT", {"price": 0, "change": 0}),
            "SOL": binance_prices.get("SOLUSDT", {"price": 0, "change": 0}),
            "XRP": binance_prices.get("XRPUSDT", {"price": 0, "change": 0}),
        }

    # Add latest pair costs if available
    if latest_pairs:
        payload["latest_pairs"] = latest_pairs

    success = db_write("""
        INSERT INTO engine_state (key, value, updated_at)
        VALUES ('last_tick', %s, %s)
        ON CONFLICT (key) DO UPDATE SET
            value = EXCLUDED.value,
            updated_at = EXCLUDED.updated_at
    """, (
        json.dumps(payload),
        tick_time
    ))
    if success:
        logger.debug(f"DB_WRITE | engine_state.last_tick | markets={markets_found} | opps={opportunities}")


def write_trade(trade_record: Dict, dry_run: bool):
    """
    Insert trade log entry with comprehensive backtest fields.

    Expected trade_record fields:
        Required:
        - market: str (e.g., "btc-updown-15m-1234567890")
        - side: str ("up" or "down")
        - amount_usd: float
        - success: bool

        Optional (for backtest analysis):
        - coin: str (e.g., "BTC")
        - trade_type: str (FIRST_LEG, SECOND_LEG_PAIR_COMPLETE, ADDING_TO_POSITION, etc.)
        - shares: float
        - price: float
        - pair_cost: float
        - avg_yes_cost_after: float
        - avg_no_cost_after: float
        - locked_shares: float
        - locked_profit: float
        - projected_final_profit: float
        - error: str
        - tx_hash: str
    """
    pair_cost_val = trade_record.get("pair_cost", 0)

    # DEBUG: Log what we're about to write
    mode_str = "DRY_RUN" if dry_run else "LIVE"
    pair_cost_str = f"{pair_cost_val:.4f}" if pair_cost_val else "None"
    trade_type = trade_record.get("trade_type", "UNKNOWN")
    logger.debug(
        f"DB_WRITE_TRADE_DEBUG | {mode_str} | {trade_type} | market={trade_record.get('market')} | "
        f"side={trade_record.get('side')} | pair_cost={pair_cost_str}"
    )

    success = db_write("""
        INSERT INTO trade_logs (
            timestamp, market, side, amount_usd, shares, price,
            pair_cost, locked_profit, dry_run, success, error, tx_hash,
            coin, trade_type, avg_yes_cost_after, avg_no_cost_after,
            locked_shares, projected_final_profit, condition_id
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (
        datetime.now(timezone.utc),
        trade_record.get("market", ""),
        trade_record.get("side", ""),
        trade_record.get("amount_usd", 0),
        trade_record.get("shares"),
        trade_record.get("price"),
        pair_cost_val,
        trade_record.get("locked_profit"),
        dry_run,
        trade_record.get("success", False),
        trade_record.get("error", ""),
        trade_record.get("tx_hash"),
        # New backtest fields
        trade_record.get("coin"),
        trade_record.get("trade_type"),
        trade_record.get("avg_yes_cost_after"),
        trade_record.get("avg_no_cost_after"),
        trade_record.get("locked_shares"),
        trade_record.get("projected_final_profit"),
        trade_record.get("condition_id"),  # For debugging position persistence
    ))
    if success:
        logger.debug(f"DB_WRITE | trade_logs | {mode_str} | {trade_type} | {trade_record.get('market')} | ${trade_record.get('amount_usd', 0):.2f} | pair_cost={pair_cost_str}")


def write_last_trade_time():
    """Update last_trade timestamp in engine_state."""
    success = db_write("""
        INSERT INTO engine_state (key, value, updated_at)
        VALUES ('last_trade', %s, %s)
        ON CONFLICT (key) DO UPDATE SET
            value = EXCLUDED.value,
            updated_at = EXCLUDED.updated_at
    """, (
        json.dumps({"timestamp": datetime.now(timezone.utc).isoformat()}),
        datetime.now(timezone.utc)
    ))
    if success:
        logger.debug("DB_WRITE | engine_state.last_trade | updated")


# =============================================================================
# IN-MEMORY STATE (with cached markets)
# =============================================================================

class EngineState:
    """Runtime state for the engine (not persisted across restarts)."""

    def __init__(self):
        # Position tracking (condition_id -> position data)
        self.positions: Dict[str, Dict] = {}

        # Cached market data (from slow discovery)
        self.cached_markets: List[Dict] = []
        self.last_discovery_time: float = 0

        # Trade tracking
        self.last_trade_time: float = 0
        self.total_trades: int = 0
        self.opportunities_this_tick: int = 0

        # Midpoint freshness
        self.last_midpoint_update: float = 0

        # DB throttle tracking
        self.last_db_tick_write: float = 0

        # USDC balance caching (reduce RPC calls)
        self.cached_usdc_balance: float = 0.0
        self.last_balance_fetch: float = 0
        self.balance_cache_ttl: float = 30.0  # Cache balance for 30 seconds

        # Binance prices caching
        self.cached_binance_prices: Optional[Dict[str, Dict]] = None
        self.last_binance_fetch: float = 0
        self.binance_cache_ttl: float = 5.0  # Refresh Binance prices every 5s

        # Ask prices caching (for edge_pair_cost display)
        self.cached_ask_prices: Optional[Dict[str, Dict]] = None
        self.last_ask_fetch: float = 0
        self.ask_cache_ttl: float = 3.0  # Refresh asks every 3s (faster than binance)

        # SAFETY LAYER 2: Order book timestamps per condition_id
        # Track when we last fetched fresh order book data for each market
        self.orderbook_timestamps: Dict[str, float] = {}  # condition_id -> timestamp

        # WebSocket integration state
        self.ws_started: bool = False  # True once WS listener has been started
        self.ws_token_ids: List[str] = []  # Token IDs currently subscribed

    def get_market(self, condition_id: str, coin: str = "") -> Dict:
        """Get or initialize position state for a market."""
        if condition_id not in self.positions:
            self.positions[condition_id] = {
                "coin": coin,
                "shares_up": 0.0,
                "spent_up": 0.0,
                "shares_down": 0.0,
                "spent_down": 0.0,
                "trade_log": []
            }
        return self.positions[condition_id]

    def update_position(self, condition_id: str, side: str, shares: float, cost: float):
        """Update position after a trade."""
        if condition_id not in self.positions:
            self.positions[condition_id] = {
                "coin": "",
                "shares_up": 0.0,
                "spent_up": 0.0,
                "shares_down": 0.0,
                "spent_down": 0.0,
                "trade_log": []
            }
        mstate = self.positions[condition_id]
        if side == "up":
            mstate["shares_up"] = mstate.get("shares_up", 0.0) + shares
            mstate["spent_up"] = mstate.get("spent_up", 0.0) + cost
        else:
            mstate["shares_down"] = mstate.get("shares_down", 0.0) + shares
            mstate["spent_down"] = mstate.get("spent_down", 0.0) + cost

    def needs_discovery(self) -> bool:
        """Check if market discovery is needed."""
        if not self.cached_markets:
            return True
        time_since_discovery = time.time() - self.last_discovery_time
        return time_since_discovery >= DISCOVERY_INTERVAL

    def has_valid_cache(self) -> bool:
        """Check if we have a valid market cache."""
        if not self.cached_markets:
            return False
        # Check if any active markets exist
        return any(m.get("active") for m in self.cached_markets)

    def get_cached_usdc_balance(self, force_refresh: bool = False) -> float:
        """Get USDC balance with caching to reduce RPC calls."""
        now = time.time()
        if force_refresh or (now - self.last_balance_fetch) >= self.balance_cache_ttl:
            balance = safe_call(get_usdc_balance, default=None)
            if balance is not None:
                self.cached_usdc_balance = balance
                self.last_balance_fetch = now
        return self.cached_usdc_balance

    def get_cached_binance_prices(self, session: requests.Session = None, force_refresh: bool = False) -> Optional[Dict[str, Dict]]:
        """Get Binance prices with caching."""
        now = time.time()
        if force_refresh or (now - self.last_binance_fetch) >= self.binance_cache_ttl:
            prices = safe_call(fetch_binance_prices, session, default=None)
            if prices is not None:
                self.cached_binance_prices = prices
                self.last_binance_fetch = now
        return self.cached_binance_prices

    def get_cached_ask_prices(self, client, markets: List[Dict], force_refresh: bool = False) -> Optional[Dict[str, Dict]]:
        """
        Get ask prices with caching for edge_pair_cost display.
        Also updates order book timestamps for SAFETY LAYER 2.
        """
        now = time.time()
        if force_refresh or (now - self.last_ask_fetch) >= self.ask_cache_ttl:
            # Pass self (state) to fetch_all_asks so it can update order book timestamps
            asks = safe_call(fetch_all_asks, client, markets, self, default=None)
            if asks is not None:
                self.cached_ask_prices = asks
                self.last_ask_fetch = now
        return self.cached_ask_prices

    # =========================================================================
    # SAFETY LAYER 2: Order book timestamp management
    # =========================================================================

    def set_orderbook_timestamp(self, condition_id: str, timestamp: float = None):
        """Record when we fetched fresh order book data for a market."""
        if timestamp is None:
            timestamp = time.time()
        self.orderbook_timestamps[condition_id] = timestamp

    def get_orderbook_age(self, condition_id: str) -> float:
        """
        Get the age of order book data for a market in seconds.
        Returns infinity if never fetched.
        """
        ts = self.orderbook_timestamps.get(condition_id)
        if ts is None:
            return float('inf')
        return time.time() - ts

    def is_orderbook_fresh(self, condition_id: str, max_age: float = None) -> bool:
        """
        Check if order book data is fresh enough for trading.
        Uses MAX_BOOK_AGE_SECONDS if max_age not specified.
        """
        if max_age is None:
            max_age = MAX_BOOK_AGE_SECONDS
        return self.get_orderbook_age(condition_id) <= max_age

    # =========================================================================
    # SAFETY LAYER 3: Position tracking helpers
    # =========================================================================

    def get_position(self, condition_id: str) -> Dict[str, float]:
        """
        Get position data for a market with safe defaults.

        Returns:
            {
                "shares_up": float,
                "shares_down": float,
                "spent_up": float,
                "spent_down": float,
                "avg_pair_cost": float,
                "net_directional_shares": float  # positive = net UP, negative = net DOWN
            }
        """
        mstate = self.positions.get(condition_id, {})
        shares_up = mstate.get("shares_up", 0.0)
        shares_down = mstate.get("shares_down", 0.0)
        spent_up = mstate.get("spent_up", 0.0)
        spent_down = mstate.get("spent_down", 0.0)

        # Calculate average costs
        avg_up = spent_up / shares_up if shares_up > 0 else 0.0
        avg_down = spent_down / shares_down if shares_down > 0 else 0.0
        avg_pair_cost = avg_up + avg_down if (shares_up > 0 and shares_down > 0) else 0.0

        return {
            "shares_up": shares_up,
            "shares_down": shares_down,
            "spent_up": spent_up,
            "spent_down": spent_down,
            "avg_pair_cost": avg_pair_cost,
            "net_directional_shares": shares_up - shares_down
        }


# =============================================================================
# WEB3 & WALLET FUNCTIONS
# =============================================================================

_web3_instance = None
_wallet_address = None

def get_web3() -> Web3:
    """Get Web3 instance connected to Polygon."""
    global _web3_instance
    if _web3_instance is None:
        rpc_url = os.environ.get("RPC_URL", "https://polygon-rpc.com")
        _web3_instance = Web3(Web3.HTTPProvider(rpc_url))
    return _web3_instance


def get_wallet_address() -> Optional[str]:
    """Derive wallet address from private key."""
    global _wallet_address
    if _wallet_address is None:
        pk = os.environ.get("POLYMARKET_PRIVATE_KEY", "")
        if pk:
            try:
                if pk.startswith("0x"):
                    pk = pk[2:]
                account = Account.from_key(pk)
                _wallet_address = account.address
            except Exception as e:
                logger.error(f"Failed to derive wallet address: {e}")
    return _wallet_address


def get_usdc_balance() -> Optional[float]:
    """Query USDC balance from Polygon."""
    try:
        w3 = get_web3()
        address = get_wallet_address()
        if not address:
            return None

        contract = w3.eth.contract(
            address=Web3.to_checksum_address(USDC_ADDRESS),
            abi=MINIMAL_ERC20_ABI
        )
        raw_balance = contract.functions.balanceOf(
            Web3.to_checksum_address(address)
        ).call()
        return raw_balance / 1e6  # USDC has 6 decimals
    except Exception as e:
        logger.warning(f"get_usdc_balance failed: {e}")
        return None


# =============================================================================
# CLOB CLIENT
# =============================================================================

_clob_client = None

def get_clob_client() -> Optional[ClobClient]:
    """Initialize CLOB client with API credentials."""
    global _clob_client

    if _clob_client is not None:
        return _clob_client

    try:
        api_key = os.environ.get("POLYMARKET_API_KEY")
        api_secret = os.environ.get("POLYMARKET_API_SECRET")
        api_passphrase = os.environ.get("POLYMARKET_API_PASSPHRASE")
        private_key = os.environ.get("POLYMARKET_PRIVATE_KEY", "")

        # Strip 0x prefix if present
        if private_key.startswith("0x"):
            private_key = private_key[2:]

        if api_key and api_secret and api_passphrase:
            client = ClobClient(
                host=CLOB_HOST,
                key=private_key if private_key else None,
                chain_id=CHAIN_ID
            )
            creds = ApiCreds(
                api_key=api_key,
                api_secret=api_secret,
                api_passphrase=api_passphrase
            )
            client.set_api_creds(creds)
            _clob_client = client
            logger.info("CLOB client initialized with official API credentials")
            return client

        if private_key:
            client = ClobClient(
                host=CLOB_HOST,
                key=private_key,
                chain_id=CHAIN_ID
            )
            try:
                creds = client.derive_api_key()
                client.set_api_creds(creds)
                logger.info("CLOB client initialized with derived credentials")
            except Exception:
                try:
                    creds = client.create_or_derive_api_creds()
                    client.set_api_creds(creds)
                    logger.info("CLOB client initialized with created credentials")
                except Exception as e:
                    logger.error(f"Failed to derive/create API creds: {e}")
                    return None
            _clob_client = client
            return client

        logger.error("No API credentials or private key configured")
        return None

    except Exception as e:
        logger.error(f"Failed to initialize CLOB client: {e}")
        return None


# =============================================================================
# BINANCE PRICE DATA
# =============================================================================

def fetch_binance_prices(session: requests.Session = None) -> Optional[Dict[str, Dict]]:
    """
    SAFE WRAPPER: Fetch live prices from Binance for all supported coins.
    Returns dict like {"BTCUSDT": {"price": 100000.0, "change": 1.5}, ...}
    Uses shared session for connection pooling.
    """
    if session is None:
        session = get_http_session()

    try:
        symbols = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT"]
        data = {}

        for sym in symbols:
            try:
                r = session.get(
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

        # Fallback to CoinGecko if Binance fails
        if all(d["price"] == 0.0 for d in data.values()):
            try:
                cg_ids = "bitcoin,ethereum,solana,ripple"
                r = session.get(
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

    except Exception as e:
        logger.warning(f"fetch_binance_prices failed: {e}")
        return None


# =============================================================================
# CLOB MIDPOINT PRICE FETCHING (FAST - called each tick)
# =============================================================================

def get_clob_midpoint_single(token_id: str, session: requests.Session = None) -> Optional[float]:
    """Fetch a single midpoint price from CLOB API using shared session."""
    if session is None:
        session = get_http_session()
    try:
        r = session.get(
            f"https://clob.polymarket.com/midpoint?token_id={token_id}",
            timeout=1.5  # Aggressive timeout for fast ticks
        )
        if r.status_code == 200:
            data = r.json()
            mid = data.get("mid")
            if mid is not None:
                price = float(mid)
                logger.debug(f"MIDPOINT_RAW | token_id={token_id[:16]}... | mid={price:.4f}")
                return price
    except Exception as e:
        logger.debug(f"MIDPOINT_FAIL | token_id={token_id[:16]}... | error={e}")
    return None


def refresh_midpoints_only(markets: List[Dict], state: EngineState, session: requests.Session = None) -> Tuple[List[Dict], bool]:
    """
    FAST: Refresh ONLY midpoint prices for cached markets using PARALLEL HTTP requests.
    Does NOT re-discover markets from Gamma API.
    Uses shared HTTP session for connection pooling.
    Returns: (updated_markets, any_success)
    """
    if session is None:
        session = get_http_session()

    fetch_time = time.time()

    # Collect all token IDs to fetch
    fetch_tasks = []  # [(market_idx, 'up'|'down', token_id)]
    for idx, market in enumerate(markets):
        if not market.get("active"):
            continue
        up_token_id = market.get("up_token_id")
        down_token_id = market.get("down_token_id")
        if up_token_id:
            fetch_tasks.append((idx, 'up', up_token_id))
        if down_token_id:
            fetch_tasks.append((idx, 'down', down_token_id))

    # Parallel fetch all midpoints using shared session
    results = {}  # {(market_idx, 'up'|'down'): price}

    if fetch_tasks:
        with ThreadPoolExecutor(max_workers=8) as executor:
            future_to_task = {
                executor.submit(get_clob_midpoint_single, token_id, session): (idx, side)
                for idx, side, token_id in fetch_tasks
            }
            for future in as_completed(future_to_task):
                idx, side = future_to_task[future]
                try:
                    price = future.result()
                    if price is not None:
                        results[(idx, side)] = price
                except Exception:
                    pass

    # Build updated markets
    updated_markets = []
    any_success = False

    for idx, market in enumerate(markets):
        if not market.get("active"):
            updated_markets.append(market)
            continue

        up_price = results.get((idx, 'up'))
        down_price = results.get((idx, 'down'))

        if up_price is not None and down_price is not None:
            market_copy = market.copy()
            market_copy["up_price"] = up_price
            market_copy["down_price"] = down_price
            market_copy["midpoint_timestamp"] = fetch_time
            updated_markets.append(market_copy)
            any_success = True
            state.last_midpoint_update = fetch_time
        else:
            # Keep old prices but mark as potentially stale
            updated_markets.append(market)

    return updated_markets, any_success


# =============================================================================
# GAMMA API MARKET DISCOVERY (SLOW - called every 60s)
# =============================================================================

def get_current_15m_timestamp() -> int:
    """Get current 15-minute period timestamp."""
    return int(time.time() // 900 * 900)


def fetch_market_by_slug(slug: str, session: requests.Session = None) -> Optional[Dict]:
    """Fetch market details from Gamma API by slug using shared session."""
    if session is None:
        session = get_http_session()
    try:
        response = session.get(
            f"{GAMMA_API_HOST}/markets?slug={slug}",
            timeout=5
        )
        if response.status_code == 200:
            data = response.json()
            if data and len(data) > 0:
                market = data[0]
                if market.get("active", False) and not market.get("closed", False):
                    return market
        return None
    except Exception as e:
        logger.debug(f"fetch_market_by_slug({slug}) failed: {e}")
        return None


def find_active_market_for_coin(coin: str, session: requests.Session = None) -> Optional[Dict]:
    """Find active 15-minute up/down market for a coin (SLOW - does HTTP calls)."""
    if session is None:
        session = get_http_session()

    current_ts = get_current_15m_timestamp()
    timestamps_to_check = [current_ts, current_ts + 900, current_ts - 900]

    for ts in timestamps_to_check:
        slug = f"{coin}-updown-15m-{ts}"
        market = fetch_market_by_slug(slug, session)

        if market:
            token_ids_raw = market.get("clobTokenIds", [])
            if isinstance(token_ids_raw, str):
                try:
                    token_ids = json.loads(token_ids_raw)
                except Exception:
                    token_ids = []
            else:
                token_ids = token_ids_raw

            outcomes_raw = market.get("outcomes", ["Up", "Down"])
            if isinstance(outcomes_raw, str):
                try:
                    outcomes = json.loads(outcomes_raw)
                except Exception:
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

                # Get initial midpoints during discovery (using shared session)
                up_price = get_clob_midpoint_single(up_token_id, session)
                down_price = get_clob_midpoint_single(down_token_id, session)

                end_time = None
                end_date_str = market.get("endDate") or market.get("end_date_iso")
                if end_date_str:
                    try:
                        from dateutil import parser as dateutil_parser
                        end_time = dateutil_parser.parse(end_date_str)
                        if end_time.tzinfo is None:
                            end_time = ET.localize(end_time)
                    except Exception:
                        end_time = None

                return {
                    "condition_id": market.get("conditionId"),
                    "coin": coin.upper(),
                    "question": market.get("question", f"{coin.upper()} Up or Down"),
                    "slug": slug,
                    "end_time": end_time,
                    "up_token_id": up_token_id,
                    "down_token_id": down_token_id,
                    "up_price": up_price if up_price is not None else 0.5,
                    "down_price": down_price if down_price is not None else 0.5,
                    "active": True,
                    "midpoint_timestamp": time.time(),
                }

    return None


def validate_market_structure(market: Dict) -> Tuple[bool, str]:
    """
    Validate market structure before trading.

    Checks:
        - condition_id is non-null
        - up_token_id and down_token_id exist and are numeric strings
        - up_token_id != down_token_id
        - market has a future expiry timestamp

    Returns: (is_valid, reason)
    """
    coin = market.get("coin", "???")

    # Check condition_id
    condition_id = market.get("condition_id")
    if not condition_id:
        return False, f"{coin}: Missing condition_id"

    # Check token IDs exist
    up_token = market.get("up_token_id")
    down_token = market.get("down_token_id")

    if not up_token:
        return False, f"{coin}: Missing up_token_id"
    if not down_token:
        return False, f"{coin}: Missing down_token_id"

    # Check token IDs are numeric strings (Polymarket uses large integer strings)
    try:
        up_int = int(up_token)
        down_int = int(down_token)
    except (ValueError, TypeError):
        return False, f"{coin}: Token IDs must be numeric strings (up={up_token[:20] if up_token else None}...)"

    # Check tokens are different
    if up_token == down_token:
        return False, f"{coin}: up_token_id == down_token_id (duplicate tokens)"

    # Check expiry is in future
    end_time = market.get("end_time")
    if end_time:
        try:
            now = datetime.now(ET)
            if end_time.tzinfo is None:
                end_time = ET.localize(end_time)
            if end_time <= now:
                return False, f"{coin}: Market already expired"
        except Exception:
            pass  # Can't validate expiry, allow it

    return True, "OK"


def run_market_discovery(session: requests.Session = None) -> List[Dict]:
    """
    SLOW: Full market discovery from Gamma API.
    Called only on startup and every DISCOVERY_INTERVAL seconds.
    Uses shared HTTP session for connection pooling.
    Returns list of market dicts with token IDs.
    """
    if session is None:
        session = get_http_session()

    discovery_start = time.time()
    all_markets = []

    for coin in SLUG_COINS:
        market = find_active_market_for_coin(coin, session)
        if market:
            all_markets.append(market)

            # Log detailed market info at DEBUG level
            cid = market.get("condition_id", "")[:16] if market.get("condition_id") else "None"
            up_tok = market.get("up_token_id", "")[:16] if market.get("up_token_id") else "None"
            down_tok = market.get("down_token_id", "")[:16] if market.get("down_token_id") else "None"
            seconds_left = get_seconds_remaining(market.get("end_time"))
            slug = market.get("slug", "")[:40]

            logger.debug(
                f"MARKET_DISCOVERY | {coin.upper()} | cid={cid}... | "
                f"up={up_tok}... | down={down_tok}... | expires_in={seconds_left}s | slug={slug}"
            )
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
                "midpoint_timestamp": 0,
            })
            logger.debug(f"MARKET_DISCOVERY | {coin.upper()} | NO ACTIVE MARKET FOUND")

    coin_order = {"BTC": 0, "ETH": 1, "SOL": 2, "XRP": 3}
    all_markets.sort(key=lambda x: coin_order.get(x.get("coin", "ZZZ"), 99))

    discovery_elapsed = time.time() - discovery_start
    active_count = sum(1 for m in all_markets if m.get("active"))

    # Health validation: warn if fewer than 4 active markets
    if active_count < 4:
        missing_coins = [m["coin"] for m in all_markets if not m.get("active")]
        logger.warning(
            f"MARKET_HEALTH | Only {active_count}/4 markets active | "
            f"Missing: {', '.join(missing_coins)}"
        )

    logger.info(f"Refreshed market discovery | markets={active_count} | elapsed={discovery_elapsed:.2f}s")

    return all_markets


def get_seconds_remaining(end_time) -> int:
    """Calculate seconds until market expiration."""
    if end_time is None:
        return 999
    try:
        now = datetime.now(ET)
        if end_time.tzinfo is None:
            end_time = ET.localize(end_time)
        delta = end_time - now
        return max(0, int(delta.total_seconds()))
    except Exception:
        return 999


# =============================================================================
# METRICS & CALCULATIONS
# =============================================================================

def calculate_metrics(mstate: Dict) -> Dict[str, Any]:
    """
    Calculate position metrics including pair_cost and locked_profit.

    Returns:
        {
            "avg_up": float,
            "avg_down": float,
            "avg_pair_cost": float,  # This is the pair_cost
            "locked_shares": float,
            "locked_profit": float,
            "unbalanced": float,
            "imbalance_side": str or None,
            "imbalance_signed": float,
            "total_spent": float
        }
    """
    try:
        shares_up = mstate.get("shares_up", 0.0)
        shares_down = mstate.get("shares_down", 0.0)
        spent_up = mstate.get("spent_up", 0.0)
        spent_down = mstate.get("spent_down", 0.0)

        avg_up = spent_up / shares_up if shares_up > 0 else 0
        avg_down = spent_down / shares_down if shares_down > 0 else 0
        avg_pair_cost = avg_up + avg_down if (shares_up > 0 and shares_down > 0) else 0

        locked_shares = min(shares_up, shares_down)
        # locked_profit = min(shares_up, shares_down) * (1 - pair_cost)
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


def calculate_locked_profit(mstate: Dict) -> float:
    """
    Calculate locked profit for a market position.
    locked_profit = min(shares_up, shares_down) * (1 - pair_cost)
    """
    metrics = calculate_metrics(mstate)
    return metrics.get("locked_profit", 0.0)


# =============================================================================
# SAFETY LAYER 4: DIRECTIONAL EXPOSURE CHECK
# =============================================================================

def check_directional_exposure(
    state: 'EngineState',
    condition_id: str,
    proposed_side: str,
    proposed_usd: float,
    bankroll: float,
    spot_price: float = 1.0
) -> Tuple[bool, str, float]:
    """
    SAFETY LAYER 4: Check if a proposed trade would exceed directional exposure limits.

    This prevents over-tilting to one side on any single market by capping
    directional exposure at MAX_DIRECTIONAL_EXPOSURE_FRACTION of bankroll.

    Args:
        state: EngineState with position tracking
        condition_id: Market condition ID
        proposed_side: "up" or "down"
        proposed_usd: Dollar amount of proposed trade
        bankroll: Current total bankroll (USDC)
        spot_price: Current underlying spot price (for USD estimation)

    Returns:
        (is_allowed, reason, current_exposure_usd)
        - is_allowed: True if trade is within limits
        - reason: Human-readable reason if blocked
        - current_exposure_usd: Current directional exposure in USD
    """
    # Get current position
    pos = state.get_position(condition_id)
    net_shares = pos["net_directional_shares"]  # positive = net UP, negative = net DOWN

    # Estimate current directional exposure in USD
    # Use abs(net_shares) * estimated_price_per_share
    # For binary options, shares are worth ~$0.50 on average, so we use a conservative estimate
    current_exposure_usd = abs(net_shares) * 0.50  # Conservative estimate

    # Calculate max allowed exposure
    max_exposure_usd = bankroll * MAX_DIRECTIONAL_EXPOSURE_FRACTION

    # Calculate projected exposure after trade
    if proposed_side == "up":
        # Buying UP increases net UP exposure
        if net_shares >= 0:
            # Already net UP, adding more UP exposure
            projected_net_shares = net_shares + (proposed_usd / 0.50)  # Estimate shares
        else:
            # Net DOWN, buying UP reduces DOWN exposure
            projected_net_shares = net_shares + (proposed_usd / 0.50)
    else:
        # Buying DOWN increases net DOWN exposure (decreases net UP)
        if net_shares <= 0:
            # Already net DOWN, adding more DOWN exposure
            projected_net_shares = net_shares - (proposed_usd / 0.50)  # More negative
        else:
            # Net UP, buying DOWN reduces UP exposure
            projected_net_shares = net_shares - (proposed_usd / 0.50)

    projected_exposure_usd = abs(projected_net_shares) * 0.50

    # Check if projected exposure exceeds limit
    if projected_exposure_usd > max_exposure_usd:
        return (
            False,
            f"Directional cap exceeded: ${projected_exposure_usd:.2f} > ${max_exposure_usd:.2f} ({MAX_DIRECTIONAL_EXPOSURE_FRACTION*100:.0f}% of ${bankroll:.2f})",
            current_exposure_usd
        )

    return (True, "OK", current_exposure_usd)


# =============================================================================
# TRADING FUNCTIONS
# =============================================================================

def get_order_book_ask(client: ClobClient, token_id: str) -> float:
    """Get best ask price from order book."""
    try:
        ob = client.get_order_book(token_id)
        if ob.asks:
            return float(ob.asks[0].price)
        return 0.99
    except Exception:
        return 0.99


def fetch_all_asks(client: ClobClient, markets: List[Dict], state: 'EngineState' = None) -> Dict[str, Dict[str, float]]:
    """
    Fetch best ask prices for all active markets in parallel.
    Also updates order book timestamps in state for SAFETY LAYER 2.

    WEBSOCKET INTEGRATION:
    - First checks if WebSocket prices are available and fresh (<1.5s old)
    - If WS fresh: uses WS best_ask directly (faster, more accurate)
    - If WS stale/unavailable: falls back to HTTP polling
    - Records "source" field: "ws" or "poll"

    Args:
        client: CLOB client
        markets: List of market dicts
        state: Optional EngineState to update order book timestamps

    Returns: {coin: {"ask_up": float, "ask_down": float, "edge_pair_cost": float, "condition_id": str, "source": str}}
    """
    if not client:
        return {}

    # Collect all token IDs to fetch, also track condition_ids
    fetch_tasks = []  # [(coin, 'up'|'down', token_id, condition_id)]
    token_map = {}  # token_id -> (coin, side, condition_id)

    for market in markets:
        if not market.get("active"):
            continue
        coin = market.get("coin")
        condition_id = market.get("condition_id")
        if not coin or not condition_id:
            continue
        up_token_id = market.get("up_token_id")
        down_token_id = market.get("down_token_id")
        if up_token_id:
            fetch_tasks.append((coin, 'up', up_token_id, condition_id))
            token_map[up_token_id] = (coin, 'up', condition_id)
        if down_token_id:
            fetch_tasks.append((coin, 'down', down_token_id, condition_id))
            token_map[down_token_id] = (coin, 'down', condition_id)

    if not fetch_tasks:
        return {}

    fetch_timestamp = time.time()  # Record when we started fetching

    # =========================================================================
    # WEBSOCKET PRICE CHECK (priority over HTTP polling)
    # =========================================================================
    results = {}  # {(coin, 'up'|'down'): (price, source)}
    condition_ids_fetched = set()
    ws_used = {}  # {coin: bool} - track if WS was used for this coin
    tasks_needing_http = []  # Tasks where WS failed/stale

    for coin, side, token_id, condition_id in fetch_tasks:
        ws_price = None
        use_ws = False

        # Try WebSocket first if available
        if WS_AVAILABLE:
            ws_data = get_ws_price(token_id)
            if ws_data is not None and is_ws_fresh(token_id, max_age=1.5):
                # Use best_ask from WebSocket
                ws_price = ws_data.get("best_ask")
                if ws_price is not None and ws_price > 0:
                    use_ws = True
                    ws_age_ms = (time.time() - ws_data.get("ts", 0)) * 1000
                    results[(coin, side)] = (ws_price, "ws")
                    condition_ids_fetched.add(condition_id)
                    ws_used[coin] = True
                    # Update orderbook timestamp for WS too
                    if state is not None:
                        state.set_orderbook_timestamp(condition_id, ws_data.get("ts", fetch_timestamp))
                    logger.info(f"WS_LATENCY | {coin} {side} | age={ws_age_ms:.0f}ms | ask={ws_price:.4f}")

        if not use_ws:
            # WebSocket unavailable or stale - queue for HTTP polling
            tasks_needing_http.append((coin, side, token_id, condition_id))
            if WS_AVAILABLE:
                logger.debug(f"WS_STALE_FALLBACK | {coin} {side} | token={token_id[:16]}...")

    # =========================================================================
    # HTTP POLLING FALLBACK (for tokens without fresh WS data)
    # =========================================================================
    if tasks_needing_http:
        http_start = time.time()

        def fetch_single_ask(task):
            coin, side, token_id, condition_id = task
            try:
                t0 = time.time()
                ob = client.get_order_book(token_id)
                elapsed_ms = (time.time() - t0) * 1000
                if ob.asks:
                    return (coin, side, float(ob.asks[0].price), condition_id, elapsed_ms)
                return (coin, side, None, condition_id, elapsed_ms)
            except Exception:
                return (coin, side, None, condition_id, 0)

        with ThreadPoolExecutor(max_workers=8) as executor:
            futures = [executor.submit(fetch_single_ask, task) for task in tasks_needing_http]
            for future in as_completed(futures):
                try:
                    coin, side, price, condition_id, elapsed_ms = future.result()
                    if price is not None:
                        results[(coin, side)] = (price, "poll")
                        condition_ids_fetched.add(condition_id)
                        logger.info(f"HTTP_LATENCY | {coin} {side} | fetch={elapsed_ms:.0f}ms | ask={price:.4f}")
                except Exception:
                    pass

    # SAFETY LAYER 2: Update order book timestamps in state (for HTTP-fetched)
    if state is not None:
        for condition_id in condition_ids_fetched:
            # Only update if not already updated by WS
            if state.get_orderbook_age(condition_id) > 0.1:  # Not recently updated
                state.set_orderbook_timestamp(condition_id, fetch_timestamp)

    # Build result dict per coin, including condition_id and source for reference
    ask_data = {}
    coin_to_condition = {t[0]: t[3] for t in fetch_tasks}  # coin -> condition_id

    coins_seen = set(t[0] for t in fetch_tasks)
    for coin in coins_seen:
        up_result = results.get((coin, 'up'))
        down_result = results.get((coin, 'down'))

        ask_up = up_result[0] if up_result else None
        ask_down = down_result[0] if down_result else None
        source_up = up_result[1] if up_result else None
        source_down = down_result[1] if down_result else None

        # Determine overall source (ws if both are ws, poll otherwise)
        if source_up == "ws" and source_down == "ws":
            source = "ws"
        elif source_up == "ws" or source_down == "ws":
            source = "mixed"
        else:
            source = "poll"

        if ask_up is not None and ask_down is not None:
            edge_pair_cost = ask_up + ask_down
        else:
            edge_pair_cost = None

        ask_data[coin] = {
            "ask_up": ask_up,
            "ask_down": ask_down,
            "edge_pair_cost": round(edge_pair_cost, 4) if edge_pair_cost else None,
            "condition_id": coin_to_condition.get(coin),
            "fetch_timestamp": fetch_timestamp,
            "source": source
        }

    return ask_data


def check_safety(mstate: Dict, side: str, seconds_remaining: int) -> Tuple[bool, str]:
    """Pre-trade safety validation."""
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

    Uses midpoint prices from market dict (refreshed each tick).
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

    # Calculate current average costs using calculate_metrics
    metrics = calculate_metrics(mstate)
    avg_up = metrics["avg_up"]
    avg_down = metrics["avg_down"]

    # Determine position state for logging
    coin = market.get("coin", "???")
    has_up_only = shares_up > 0 and shares_down == 0
    has_down_only = shares_down > 0 and shares_up == 0
    has_both = shares_up > 0 and shares_down > 0
    has_neither = shares_up == 0 and shares_down == 0

    # =========================================================================
    # INSTRUMENTATION: Track for eval_logs (does NOT affect trading logic)
    # =========================================================================
    _eval_side = None
    _eval_projected = None
    _eval_current_imbalance = None

    # Debug log current position state
    if shares_up > 0 or shares_down > 0:
        logger.debug(
            f"POSITION_STATE | {coin} | "
            f"shares_up={shares_up:.2f} | shares_down={shares_down:.2f} | "
            f"avg_up={avg_up:.4f} | avg_down={avg_down:.4f}"
        )

    # Current pair cost (only if we have positions on BOTH sides)
    if shares_up > 0 and shares_down > 0:
        current_pair_cost = metrics["avg_pair_cost"]
    else:
        current_pair_cost = 1.0  # No pair yet, treat as expensive

    # If already at target, skip
    if current_pair_cost <= TARGET_PAIR_COST:
        if has_both:
            logger.debug(f"SKIP_AT_TARGET | {coin} | pair_cost={current_pair_cost:.4f} <= {TARGET_PAIR_COST}")
        return None

    # Get live market midpoint prices (refreshed each tick)
    up_price = market.get("up_price", 0.5)
    down_price = market.get("down_price", 0.5)

    # Skip if prices are invalid
    if up_price is None or down_price is None:
        return None
    if up_price <= 0 or down_price <= 0:
        return None

    # Check if pair cost opportunity exists (sum < 1.0)
    market_pair_cost = up_price + down_price

    # DEBUG: Log pair cost evaluation
    logger.debug(
        f"PAIR_COST_DEBUG | {market.get('coin', 'UNK')} | up={up_price:.4f} | down={down_price:.4f} | "
        f"market_pair={market_pair_cost:.4f} | position_pair={current_pair_cost:.4f} | target={TARGET_PAIR_COST}"
    )

    if market_pair_cost >= 1.0:
        logger.debug(f"PAIR_COST_REJECT | {market.get('coin', 'UNK')} | reason=market_pair >= 1.0 ({market_pair_cost:.4f})")
        return None  # No edge

    # Determine cheaper side
    if up_price < down_price:
        cheaper_side = "up"
        cheaper_price = up_price
        cheaper_token_id = market.get("up_token_id")
        current_shares = shares_up
        current_spent = spent_up
        other_avg = avg_down if shares_down > 0 else 0.5
    else:
        cheaper_side = "down"
        cheaper_price = down_price
        cheaper_token_id = market.get("down_token_id")
        current_shares = shares_down
        current_spent = spent_down
        other_avg = avg_up if shares_up > 0 else 0.5

    if not cheaper_token_id:
        return None

    # INSTRUMENTATION: Capture side being evaluated
    _eval_side = cheaper_side.upper()

    # Calculate current imbalance (in USD terms)
    current_imbalance_usd = abs((shares_up * up_price) - (shares_down * down_price))

    # INSTRUMENTATION: Capture current imbalance
    _eval_current_imbalance = current_imbalance_usd

    # Dynamic sizing based on pair cost urgency
    base_trade_usd = available_usdc * MAX_TRADE_PCT

    if current_pair_cost > 0.980:
        multiplier = 1.0
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

    # INSTRUMENTATION: Capture projected pair cost
    _eval_projected = projected_pair

    # Check improvement threshold
    improvement = current_pair_cost - projected_pair
    if improvement < MIN_IMPROVEMENT_REQUIRED:
        # INSTRUMENTATION: Log near-miss rejection
        if _eval_projected is not None and _eval_projected <= TARGET_PAIR_COST + 0.01:
            safe_call(log_eval_decision,
                coin=coin,
                market_id=condition_id,
                expiry_ts=market.get("end_time"),
                side_considered=_eval_side or "UNKNOWN",
                decision="REJECT",
                reason="IMPROVEMENT_TOO_SMALL",
                current_qty_yes=shares_up,
                current_qty_no=shares_down,
                current_pair_cost=current_pair_cost,
                projected_pair_cost=_eval_projected,
                time_to_expiry_s=seconds_remaining,
                directional_exposure=_eval_current_imbalance
            )
        return None

    # Check projected pair is at or below target
    if projected_pair > TARGET_PAIR_COST:
        logger.debug(
            f"NO_TRADE_PROJECTED_HIGH | {coin} | "
            f"projected_pair={projected_pair:.4f} > target={TARGET_PAIR_COST}"
        )
        # INSTRUMENTATION: Log near-miss rejection
        if _eval_projected is not None and _eval_projected <= TARGET_PAIR_COST + 0.01:
            safe_call(log_eval_decision,
                coin=coin,
                market_id=condition_id,
                expiry_ts=market.get("end_time"),
                side_considered=_eval_side or "UNKNOWN",
                decision="REJECT",
                reason="PAIR_COST_TOO_HIGH",
                current_qty_yes=shares_up,
                current_qty_no=shares_down,
                current_pair_cost=current_pair_cost,
                projected_pair_cost=_eval_projected,
                time_to_expiry_s=seconds_remaining,
                directional_exposure=_eval_current_imbalance
            )
        return None

    # Check directional risk limit
    projected_imbalance = current_imbalance_usd + trade_usd
    if projected_imbalance > available_usdc * MAX_DIRECTIONAL_RISK_PCT:
        logger.debug(
            f"NO_TRADE_DIRECTIONAL_RISK | {coin} | "
            f"projected_imbalance=${projected_imbalance:.2f} > limit=${available_usdc * MAX_DIRECTIONAL_RISK_PCT:.2f}"
        )
        # INSTRUMENTATION: Log near-miss rejection
        if _eval_projected is not None and _eval_projected <= TARGET_PAIR_COST + 0.01:
            safe_call(log_eval_decision,
                coin=coin,
                market_id=condition_id,
                expiry_ts=market.get("end_time"),
                side_considered=_eval_side or "UNKNOWN",
                decision="REJECT",
                reason="DIRECTIONAL_CAP",
                current_qty_yes=shares_up,
                current_qty_no=shares_down,
                current_pair_cost=current_pair_cost,
                projected_pair_cost=_eval_projected,
                time_to_expiry_s=seconds_remaining,
                directional_exposure=projected_imbalance
            )
        return None

    # Determine trade type for detailed logging
    if has_neither:
        trade_type = "FIRST_LEG_OPPORTUNITY"
    elif (has_up_only and cheaper_side == "down") or (has_down_only and cheaper_side == "up"):
        trade_type = "SECOND_LEG_OPPORTUNITY"
    elif (has_up_only and cheaper_side == "up") or (has_down_only and cheaper_side == "down"):
        trade_type = "ADDING_TO_SINGLE_LEG"
    else:
        trade_type = "REBALANCING_PAIR"

    # Log the opportunity decision
    logger.debug(
        f"{trade_type} | {coin} {cheaper_side.upper()} | "
        f"trade_usd=${trade_usd:.2f} | price={cheaper_price:.4f} | "
        f"current_pair={current_pair_cost:.4f} -> projected={projected_pair:.4f} | "
        f"improvement={improvement:.4f}"
    )

    # INSTRUMENTATION: Log successful trade evaluation
    safe_call(log_eval_decision,
        coin=coin,
        market_id=condition_id,
        expiry_ts=market.get("end_time"),
        side_considered=cheaper_side.upper(),
        decision="EXECUTE",
        reason=trade_type,
        current_qty_yes=shares_up,
        current_qty_no=shares_down,
        current_pair_cost=current_pair_cost,
        projected_pair_cost=projected_pair,
        time_to_expiry_s=seconds_remaining,
        directional_exposure=current_imbalance_usd
    )

    # All checks passed - return trade info
    return {
        "coin": market["coin"],
        "condition_id": condition_id,
        "side": cheaper_side,
        "token_id": cheaper_token_id,
        "trade_usd": trade_usd,
        "current_pair": current_pair_cost,
        "projected_pair": projected_pair,
        "improvement": improvement,
        "seconds_remaining": seconds_remaining,
        "market_slug": market.get("slug", ""),
        "market_pair_cost": market_pair_cost,
        "price": cheaper_price,
    }


def execute_market_buy(
    client: ClobClient,
    token_id: str,
    side: str,
    cost_usd: float,
    mstate: Dict,
    seconds_remaining: int,
    coin: str = ""
) -> Tuple[bool, str, float, float, Optional[str]]:
    """
    Execute a market buy order.
    Returns: (success, message, filled_shares, actual_cost, tx_hash)
    """
    is_allowed, reason = check_safety(mstate, side, seconds_remaining)
    if not is_allowed:
        return False, reason, 0, 0, None

    try:
        try:
            ob = client.get_order_book(token_id)
        except Exception as e:
            error_str = str(e)[:80]
            return False, f"Order book error: {error_str}", 0, 0, None

        if not ob.asks:
            return False, "No asks available", 0, 0, None

        best_ask = float(ob.asks[0].price)
        exec_price = round(best_ask + PRICE_SLIPPAGE, 3)
        exec_price = min(exec_price, 0.99)

        size = cost_usd / best_ask
        if size < 0.01:
            return False, f"Order too small: {size:.4f} shares", 0, 0, None

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
            return False, f"Order API error: {error_str}", 0, 0, None

        if not response or "orderID" not in response:
            error_msg = response.get("error", "Unknown error") if response else "No response"
            return False, f"Order failed: {error_msg}", 0, 0, None

        order_id = response["orderID"]
        tx_hash = response.get("transactionHash", response.get("transactHash", order_id))

        time.sleep(3)

        try:
            order_status = client.get_order(order_id)
            filled_size = float(order_status.get("size_matched", 0))
        except Exception:
            filled_size = size

        if filled_size <= 0:
            return False, "Order not filled", 0, 0, tx_hash

        actual_cost = filled_size * exec_price

        return True, f"Bought {filled_size:.2f} {side.upper()} @ ${exec_price:.3f}", filled_size, actual_cost, tx_hash

    except Exception as e:
        return False, f"Execution error: {str(e)}", 0, 0, None


def execute_auto_trade(
    trade_info: Dict,
    market: Dict,
    mstate: Dict,
    client: ClobClient,
    state: EngineState
) -> Tuple[bool, str, float, float, float, Optional[str]]:
    """
    Execute an auto trade and update state.
    Returns: (success, message, actual_cost, filled_shares, locked_profit, tx_hash)
    """
    coin = trade_info["coin"]
    side = trade_info["side"]
    token_id = trade_info["token_id"]
    trade_usd = trade_info["trade_usd"]
    seconds_remaining = trade_info["seconds_remaining"]
    condition_id = trade_info.get("condition_id") or market.get("condition_id")

    success, msg, filled_shares, actual_cost, tx_hash = execute_market_buy(
        client, token_id, side, trade_usd,
        mstate, seconds_remaining, coin
    )

    locked_profit = 0.0

    if success and condition_id:
        state.update_position(condition_id, side, filled_shares, actual_cost)
        state.total_trades += 1

        # Calculate locked profit after this trade
        updated_mstate = state.get_market(condition_id, coin)
        locked_profit = calculate_locked_profit(updated_mstate)

        # Rate limit protection
        time.sleep(1.5)

        return True, f"AUTO: {coin} {side.upper()} ${actual_cost:.2f}", actual_cost, filled_shares, locked_profit, tx_hash

    return False, msg, 0, 0, 0, tx_hash


# =============================================================================
# MAIN ENGINE LOOP
# =============================================================================

def run_engine():
    """Main perpetual trading loop with cached market discovery."""
    DRY_RUN = os.environ.get("DRY_RUN", "true").lower() == "true"
    AUTO_MODE = os.environ.get("AUTO_MODE", "false").lower() == "true"

    logger.info("=" * 60)
    logger.info("POLYMARKET ARB ENGINE V2 STARTING")
    logger.info(f"DRY_RUN = {DRY_RUN}")
    logger.info(f"AUTO_MODE = {AUTO_MODE}")
    logger.info(f"TICK_INTERVAL = {TICK_INTERVAL}s (fast)")
    logger.info(f"DISCOVERY_INTERVAL = {DISCOVERY_INTERVAL}s (slow)")
    logger.info(f"TARGET_PAIR_COST = {TARGET_PAIR_COST}")
    logger.info(f"STALE_MIDPOINT_THRESHOLD = {STALE_MIDPOINT_THRESHOLD}s")
    logger.info(f"DB_TICK_THROTTLE = {DB_TICK_THROTTLE}s")
    logger.info("--- SAFETY LAYERS ---")
    logger.info(f"MIN_SECONDS_TO_OPEN_TRADE = {MIN_SECONDS_TO_OPEN_TRADE}s (time cutoff)")
    logger.info(f"MAX_BOOK_AGE_SECONDS = {MAX_BOOK_AGE_SECONDS}s (book freshness)")
    logger.info(f"MAX_DIRECTIONAL_EXPOSURE_FRACTION = {MAX_DIRECTIONAL_EXPOSURE_FRACTION*100:.0f}% (exposure cap)")
    logger.info("=" * 60)

    # Initialize shared HTTP session (connection pooling)
    http_session = get_http_session()
    logger.info("HTTP session initialized with connection pooling")

    # Initialize
    init_db_schema()
    state = EngineState()
    client = get_clob_client()

    if not client and not DRY_RUN and AUTO_MODE:
        logger.error("CLOB client initialization failed - cannot run in live trading mode")
        return

    wallet = get_wallet_address()
    if wallet:
        logger.info(f"Wallet: {wallet[:10]}...{wallet[-8:]}")

    # Initialize with a fresh balance fetch
    usdc = state.get_cached_usdc_balance(force_refresh=True)
    logger.info(f"Initial USDC balance: ${usdc:.2f}")

    # =========================================================================
    # INITIAL MARKET DISCOVERY (blocking on startup)
    # =========================================================================
    logger.info("Running initial market discovery...")
    state.cached_markets = run_market_discovery(http_session)
    state.last_discovery_time = time.time()
    active_count = sum(1 for m in state.cached_markets if m.get("active"))
    logger.info(f"Initial discovery complete | active_markets={active_count}")

    # =========================================================================
    # WEBSOCKET INITIALIZATION (non-blocking, optional)
    # =========================================================================
    if WS_AVAILABLE and not state.ws_started:
        # Collect all token IDs from discovered markets
        ws_token_ids = []
        for market in state.cached_markets:
            if market.get("active"):
                up_token = market.get("up_token_id")
                down_token = market.get("down_token_id")
                if up_token:
                    ws_token_ids.append(up_token)
                if down_token:
                    ws_token_ids.append(down_token)

        if ws_token_ids:
            try:
                start_ws_listener(ws_token_ids)
                state.ws_started = True
                state.ws_token_ids = ws_token_ids
                logger.info(f"WS_START | tokens={len(ws_token_ids)}")
            except Exception as e:
                logger.warning(f"WS_START_FAILED | error={e}")
        else:
            logger.info("No token IDs for WebSocket - will use HTTP polling only")
    elif not WS_AVAILABLE:
        logger.info("WebSocket client not available - using HTTP polling only")

    last_heartbeat = 0
    tick_count = 0

    while True:
        try:
            tick_start = time.time()
            tick_count += 1
            state.opportunities_this_tick = 0

            # =================================================================
            # SLOW DISCOVERY (every 60s)
            # =================================================================
            if state.needs_discovery():
                logger.info("Running scheduled market discovery...")
                new_markets = safe_call(run_market_discovery, http_session, default=None)
                if new_markets:
                    state.cached_markets = new_markets
                    state.last_discovery_time = time.time()

                    # Update WebSocket subscriptions if token IDs changed
                    if WS_AVAILABLE and state.ws_started:
                        new_ws_token_ids = []
                        for market in new_markets:
                            if market.get("active"):
                                up_token = market.get("up_token_id")
                                down_token = market.get("down_token_id")
                                if up_token:
                                    new_ws_token_ids.append(up_token)
                                if down_token:
                                    new_ws_token_ids.append(down_token)

                        # Only update if token set changed
                        if set(new_ws_token_ids) != set(state.ws_token_ids):
                            update_subscriptions(new_ws_token_ids)
                            state.ws_token_ids = new_ws_token_ids
                            logger.info(f"WS_UPDATE_SUBSCRIPTIONS | tokens={len(new_ws_token_ids)}")
                else:
                    logger.warning("Discovery failed, keeping old cache")

            # =================================================================
            # HEARTBEAT (every 10s)
            # =================================================================
            if tick_start - last_heartbeat >= HEARTBEAT_INTERVAL:
                # Force refresh balance on heartbeat (every 10s is reasonable)
                usdc = state.get_cached_usdc_balance(force_refresh=True)
                active_count = sum(1 for m in state.cached_markets if m.get("active"))
                time_since_discovery = tick_start - state.last_discovery_time
                logger.info(
                    f"HEARTBEAT | tick={tick_count} | bankroll=${usdc:.2f} | "
                    f"auto_mode={AUTO_MODE} | trades={state.total_trades} | "
                    f"markets={active_count} | discovery_age={time_since_discovery:.0f}s"
                )
                last_heartbeat = tick_start

            # =================================================================
            # FAST: REFRESH MIDPOINTS ONLY (every tick)
            # =================================================================
            if not state.has_valid_cache():
                logger.warning("No valid market cache, skipping tick")
                time.sleep(TICK_INTERVAL)
                continue

            # Refresh midpoints using cached markets and shared session
            markets, midpoint_success = refresh_midpoints_only(state.cached_markets, state, http_session)

            if not midpoint_success:
                logger.warning("Failed to refresh any midpoints, skipping tick")
                time.sleep(TICK_INTERVAL)
                continue

            # Update cache with fresh midpoints
            state.cached_markets = markets

            # =================================================================
            # STALE MIDPOINT PROTECTION
            # =================================================================
            time_since_midpoint = tick_start - state.last_midpoint_update
            if state.last_midpoint_update > 0 and time_since_midpoint > STALE_MIDPOINT_THRESHOLD:
                logger.warning(f"Stale midpoints ({time_since_midpoint:.1f}s old) — skipping tick")
                time.sleep(TICK_INTERVAL)
                continue

            # Count active markets
            active_count = sum(1 for m in markets if m.get("active"))

            # Log tick status
            midpoint_age = time.time() - state.last_midpoint_update
            logger.debug(f"Tick OK | markets={active_count} | midpoints_age={midpoint_age:.2f}s")

            # =================================================================
            # EVALUATE OPPORTUNITIES
            # =================================================================
            # Use cached balance (refreshes every 30s) to reduce RPC calls
            usdc = state.get_cached_usdc_balance()
            available = usdc - 5  # Keep $5 buffer

            # Fetch Binance prices (cached for 5s)
            binance_prices = state.get_cached_binance_prices(http_session)

            # Fetch ask prices for edge_pair_cost (cached for 3s)
            ask_prices = state.get_cached_ask_prices(client, markets)

            # Build latest_pairs dict from current market data
            latest_pairs = {}
            for m in markets:
                if m.get("active") and m.get("coin"):
                    coin = m["coin"]
                    up_p = m.get("up_price")
                    down_p = m.get("down_price")
                    # Use None checks instead of truthiness (0 is valid price)
                    if up_p is not None and down_p is not None:
                        pair_cost = up_p + down_p
                    else:
                        pair_cost = 1.0

                    # Get ask-based edge_pair_cost from cached asks
                    coin_asks = ask_prices.get(coin, {}) if ask_prices else {}
                    edge_pair_cost = coin_asks.get("edge_pair_cost")
                    price_source = coin_asks.get("source", "poll")  # "ws", "poll", or "mixed"

                    latest_pairs[coin] = {
                        "pair_cost": round(pair_cost, 4),
                        "up_price": round(up_p, 4) if up_p is not None else None,
                        "down_price": round(down_p, 4) if down_p is not None else None,
                        "edge_pair_cost": edge_pair_cost,  # Ask-based (what you'd actually pay)
                        "source": price_source,  # "ws" = WebSocket, "poll" = HTTP, "mixed" = partial WS
                    }

            # =================================================================
            # PRICE_SNAPSHOT DIAGNOSTIC LOG - every tick, every coin
            # Shows actual ask prices being used for trade evaluation
            # =================================================================
            for market in markets:
                if not market.get("active"):
                    continue
                coin = market.get("coin")
                if not coin:
                    continue
                condition_id = market.get("condition_id", "")
                coin_asks = ask_prices.get(coin, {}) if ask_prices else {}
                ask_up = coin_asks.get("ask_up")
                ask_down = coin_asks.get("ask_down")
                source = coin_asks.get("source", "none")
                # Determine per-side source: ws=both ws, poll=both poll, mixed=one each
                if source == "ws":
                    source_up, source_down = "ws", "ws"
                elif source == "poll":
                    source_up, source_down = "poll", "poll"
                elif source == "mixed":
                    # Mixed means one is ws, one is poll - check which
                    source_up, source_down = "ws/poll", "poll/ws"
                else:
                    source_up, source_down = "none", "none"
                sum_pair = (ask_up or 0) + (ask_down or 0)
                up_str = f"{ask_up:.4f}" if ask_up else "N/A"
                down_str = f"{ask_down:.4f}" if ask_down else "N/A"
                cond_str = condition_id[:16] if condition_id else "N/A"
                logger.info(
                    f"PRICE_SNAPSHOT | coin={coin} | cond_id={cond_str}... | "
                    f"up_ask={up_str} | down_ask={down_str} | "
                    f"source_up={source_up} | source_down={source_down} | sum_pair_cost={sum_pair:.4f}"
                )

            opportunities = []
            for market in markets:
                coin = market.get("coin", "???")

                if not market.get("active"):
                    continue

                condition_id = market.get("condition_id")
                if not condition_id:
                    continue

                # ============================================================
                # MARKET VALIDATION - Skip invalid markets before trading
                # ============================================================
                is_valid, validation_msg = validate_market_structure(market)
                if not is_valid:
                    logger.warning(f"MARKET_INVALID | {validation_msg} | Skipping trade evaluation")
                    # Mark as invalid in latest_pairs for dashboard
                    if coin in latest_pairs:
                        latest_pairs[coin]["valid"] = False
                        latest_pairs[coin]["validation_error"] = validation_msg
                    continue

                # Mark as valid in latest_pairs
                if coin in latest_pairs:
                    latest_pairs[coin]["valid"] = True
                    latest_pairs[coin]["condition_id"] = condition_id[:16] if condition_id else None
                    latest_pairs[coin]["seconds_remaining"] = get_seconds_remaining(market.get("end_time"))
                    latest_pairs[coin]["slug"] = market.get("slug", "")

                mstate = state.get_market(condition_id, coin)

                # Use calculate_metrics for accurate pair cost evaluation
                metrics = calculate_metrics(mstate)

                # Get market prices for logging
                up_price = market.get("up_price", 0.5)
                down_price = market.get("down_price", 0.5)
                market_pair = up_price + down_price if (up_price and down_price) else 1.0
                our_pair = metrics.get("avg_pair_cost", 1.0)

                # DEBUG: Log market data before evaluate_auto_trade
                logger.debug(
                    f"MARKET_DATA | {coin} | up_price={up_price:.4f} | down_price={down_price:.4f} | "
                    f"market_pair={market_pair:.4f} | position_pair={our_pair:.4f} | target={TARGET_PAIR_COST}"
                )

                trade_info = evaluate_auto_trade(market, mstate, available)

                if trade_info:
                    opportunities.append(trade_info)
                    # Log opportunity found
                    logger.debug(
                        f"OPPORTUNITY | {coin} | up={up_price:.4f} down={down_price:.4f} | "
                        f"market_pair={market_pair:.4f} | our_pair={our_pair:.4f} | "
                        f"target={TARGET_PAIR_COST} | decision=TRADE"
                    )
                else:
                    # Log why no trade (only at debug level, won't spam)
                    logger.debug(
                        f"NO_TRADE | {coin} | up={up_price:.4f} down={down_price:.4f} | "
                        f"market_pair={market_pair:.4f} | our_pair={our_pair:.4f} | "
                        f"target={TARGET_PAIR_COST}"
                    )

            state.opportunities_this_tick = len(opportunities)

            # Log opportunity summary at INFO level (only when opportunities exist)
            if opportunities and tick_count % 5 == 0:  # Every 5th tick with opportunities
                opp_summary = ", ".join(f"{o['coin']}:{o['market_pair_cost']:.4f}" for o in opportunities[:3])
                logger.info(f"OPPORTUNITIES | count={len(opportunities)} | {opp_summary}")

            # Write tick to DB (THROTTLED - only every DB_TICK_THROTTLE seconds)
            now = time.time()
            if now - state.last_db_tick_write >= DB_TICK_THROTTLE:
                safe_call(
                    write_tick,
                    datetime.now(timezone.utc),
                    active_count,
                    len(opportunities),
                    tick_count=tick_count,
                    wallet_usdc=usdc,
                    binance_prices=binance_prices,
                    latest_pairs=latest_pairs,
                    dry_run=DRY_RUN,
                    auto_mode=AUTO_MODE
                )
                state.last_db_tick_write = now

            # =================================================================
            # SKIP TRADING IF AUTO_MODE IS OFF
            # =================================================================
            if not AUTO_MODE:
                # Just monitoring mode - no trading
                elapsed = time.time() - tick_start
                logger.debug(f"TICK_DURATION | elapsed={elapsed:.3f}s")
                if elapsed < TICK_INTERVAL:
                    time.sleep(TICK_INTERVAL - elapsed)
                continue

            # =================================================================
            # TRADING LOGIC (AUTO_MODE=true)
            # =================================================================
            if available < MIN_TRADE_USD:
                # Not enough capital, just monitor
                elapsed = time.time() - tick_start
                if elapsed < TICK_INTERVAL:
                    time.sleep(TICK_INTERVAL - elapsed)
                continue

            # Rate limit check
            time_since_last = tick_start - state.last_trade_time
            if time_since_last < AUTO_TRADE_COOLDOWN:
                elapsed = time.time() - tick_start
                if elapsed < TICK_INTERVAL:
                    time.sleep(TICK_INTERVAL - elapsed)
                continue

            # Execute first opportunity (ONE TRADE PER TICK)
            for trade_info in opportunities:
                coin = trade_info["coin"]
                condition_id = trade_info["condition_id"]
                side = trade_info["side"]
                token_id = trade_info["token_id"]
                trade_usd = trade_info["trade_usd"]
                current_pair = trade_info["current_pair"]
                projected_pair = trade_info["projected_pair"]
                market_slug = trade_info.get("market_slug", f"{coin}-15m")
                market_pair_cost = trade_info.get("market_pair_cost", 0)
                price = trade_info.get("price", 0)
                seconds_remaining = trade_info.get("seconds_remaining", 999)

                # Find the market object
                market = next((m for m in markets if m.get("condition_id") == condition_id), None)
                if not market:
                    continue

                mstate = state.get_market(condition_id, coin)

                # =============================================================
                # SAFETY LAYER 1: Time-to-expiry cutoff
                # =============================================================
                if seconds_remaining < MIN_SECONDS_TO_OPEN_TRADE and seconds_remaining != 999:
                    logger.info(
                        f"SKIP_TRADE_TIME | {coin} | seconds_remaining={seconds_remaining} | "
                        f"min_required={MIN_SECONDS_TO_OPEN_TRADE}"
                    )
                    continue

                # =============================================================
                # SAFETY LAYER 2: Order-book freshness check
                # =============================================================
                book_age = state.get_orderbook_age(condition_id)
                if book_age > MAX_BOOK_AGE_SECONDS:
                    logger.info(
                        f"SKIP_TRADE_STALE_BOOK | {coin} | book_age={book_age:.2f}s | "
                        f"max_allowed={MAX_BOOK_AGE_SECONDS}s"
                    )
                    continue

                # =============================================================
                # SAFETY LAYER 4: Directional exposure cap
                # =============================================================
                exposure_allowed, exposure_reason, current_exposure = check_directional_exposure(
                    state, condition_id, side, trade_usd, usdc
                )
                if not exposure_allowed:
                    logger.info(
                        f"SKIP_TRADE_DIRECTIONAL_CAP | {coin} | {exposure_reason}"
                    )
                    continue

                # All safety checks passed - proceed with trade
                logger.debug(
                    f"SAFETY_CHECKS_PASSED | {coin} | time_ok={seconds_remaining}s | "
                    f"book_fresh={book_age:.2f}s | exposure_ok=${current_exposure:.2f}"
                )

                if DRY_RUN:
                    # DRY_RUN: Simulate trade execution and UPDATE POSITION STATE
                    # This ensures two-leg pair completion works identically in DRY_RUN and LIVE

                    # Estimate shares bought (same calculation as live trade)
                    simulated_shares = trade_usd / price if price > 0 else 0
                    simulated_cost = trade_usd

                    # Get position state BEFORE this trade for logging
                    pos_before = state.get_position(condition_id)
                    shares_up_before = pos_before["shares_up"]
                    shares_down_before = pos_before["shares_down"]

                    # Determine if this is FIRST LEG or SECOND LEG
                    is_first_leg = (shares_up_before == 0 and shares_down_before == 0)
                    is_second_leg = (
                        (side == "up" and shares_down_before > 0 and shares_up_before == 0) or
                        (side == "down" and shares_up_before > 0 and shares_down_before == 0)
                    )
                    is_adding_to_side = (
                        (side == "up" and shares_up_before > 0) or
                        (side == "down" and shares_down_before > 0)
                    )

                    # DEBUG: Log trade classification inputs for debugging accounting bug
                    logger.info(
                        f"TRADE_CLASSIFY | {coin} | cond_id={condition_id[:16]}... | "
                        f"shares_up_before={shares_up_before:.2f} | shares_down_before={shares_down_before:.2f} | "
                        f"side={side} | is_first_leg={is_first_leg} | is_second_leg={is_second_leg}"
                    )

                    # ========================================================
                    # CRITICAL FIX: Update EngineState.positions in DRY_RUN
                    # ========================================================
                    state.update_position(condition_id, side, simulated_shares, simulated_cost)

                    # DEBUG: Log position update for debugging position persistence
                    logger.info(
                        f"POSITION_UPDATE | {coin} | cond_id={condition_id[:16]}... | "
                        f"side={side} | +shares={simulated_shares:.2f} | +cost=${simulated_cost:.2f}"
                    )

                    # Get position state AFTER this trade
                    pos_after = state.get_position(condition_id)
                    metrics_after = calculate_metrics(state.get_market(condition_id, coin))
                    avg_pair_after = metrics_after.get("avg_pair_cost", 0)
                    locked_profit_after = metrics_after.get("locked_profit", 0)
                    locked_shares_after = metrics_after.get("locked_shares", 0)

                    # Determine trade type for logging
                    if is_first_leg:
                        trade_type = "FIRST_LEG"
                    elif is_second_leg:
                        trade_type = "SECOND_LEG_PAIR_COMPLETE"
                    elif is_adding_to_side:
                        trade_type = "ADDING_TO_POSITION"
                    else:
                        trade_type = "TRADE"

                    # Log with detailed position info
                    logger.info(
                        f"[DRY_RUN] {trade_type} | {coin} {side.upper()} ${trade_usd:.2f} | "
                        f"shares={simulated_shares:.2f} @ {price:.4f} | "
                        f"market_pair: {market_pair_cost:.4f} | "
                        f"our_pair: {current_pair:.4f} -> {avg_pair_after:.4f}"
                    )

                    # Log position state after trade
                    logger.info(
                        f"[DRY_RUN] STATE_AFTER | {coin} | "
                        f"shares_up={pos_after['shares_up']:.2f} | "
                        f"shares_down={pos_after['shares_down']:.2f} | "
                        f"avg_pair_cost={avg_pair_after:.4f} | "
                        f"locked_shares={locked_shares_after:.2f} | "
                        f"locked_profit=${locked_profit_after:.4f}"
                    )

                    # Special logging for pair completion
                    if is_second_leg and locked_shares_after > 0:
                        logger.info(
                            f"[DRY_RUN] PAIR_COMPLETE | {coin} | "
                            f"locked_shares={locked_shares_after:.2f} | "
                            f"locked_profit=${locked_profit_after:.4f} | "
                            f"avg_pair_cost={avg_pair_after:.4f}"
                        )

                    # Calculate projected final profit for backtest analysis
                    projected_final_profit = (
                        locked_shares_after * (1 - avg_pair_after)
                        if locked_shares_after > 0 and avg_pair_after > 0
                        else 0
                    )

                    # DEBUG: Detect accounting bug - second leg should have avg_pair_after > 0
                    if is_second_leg and avg_pair_after == 0:
                        logger.warning(
                            f"ACCOUNTING_BUG | {coin} | Second leg but avg_pair_after=0! "
                            f"shares_up={pos_after['shares_up']:.2f} | shares_down={pos_after['shares_down']:.2f} | "
                            f"cond_id={condition_id[:16]}..."
                        )

                    # Log to DB as dry run with comprehensive backtest fields
                    safe_call(write_trade, {
                        "market": market_slug,
                        "coin": coin,
                        "trade_type": trade_type,
                        "side": side,
                        "amount_usd": trade_usd,
                        "shares": simulated_shares,
                        "price": price,
                        "pair_cost": avg_pair_after if avg_pair_after > 0 else market_pair_cost,
                        "avg_yes_cost_after": metrics_after.get("avg_up", 0),
                        "avg_no_cost_after": metrics_after.get("avg_down", 0),
                        "locked_shares": locked_shares_after,
                        "locked_profit": locked_profit_after if locked_profit_after > 0 else None,
                        "projected_final_profit": projected_final_profit if projected_final_profit > 0 else None,
                        "condition_id": condition_id,  # For debugging position persistence
                        "success": True,
                        "error": "",
                        "tx_hash": None  # Not executed (dry run)
                    }, dry_run=True)

                    state.last_trade_time = time.time()
                    state.total_trades += 1

                else:
                    # LIVE TRADE: Execute real order
                    # Get position state BEFORE trade for trade_type determination
                    pos_before = state.get_position(condition_id)
                    shares_up_before = pos_before["shares_up"]
                    shares_down_before = pos_before["shares_down"]

                    # Determine trade type BEFORE execution
                    is_first_leg = (shares_up_before == 0 and shares_down_before == 0)
                    is_second_leg = (
                        (side == "up" and shares_down_before > 0 and shares_up_before == 0) or
                        (side == "down" and shares_up_before > 0 and shares_down_before == 0)
                    )
                    is_adding_to_side = (
                        (side == "up" and shares_up_before > 0) or
                        (side == "down" and shares_down_before > 0)
                    )

                    if is_first_leg:
                        trade_type = "FIRST_LEG"
                    elif is_second_leg:
                        trade_type = "SECOND_LEG_PAIR_COMPLETE"
                    elif is_adding_to_side:
                        trade_type = "ADDING_TO_POSITION"
                    else:
                        trade_type = "TRADE"

                    success, msg, cost, shares, locked_profit, tx_hash = execute_auto_trade(
                        trade_info, market, mstate, client, state
                    )

                    if success:
                        # Get metrics AFTER trade for logging
                        metrics_after = calculate_metrics(state.get_market(condition_id, coin))
                        avg_pair_after = metrics_after.get("avg_pair_cost", 0)
                        locked_shares_after = metrics_after.get("locked_shares", 0)

                        # Calculate projected final profit
                        projected_final_profit = (
                            locked_shares_after * (1 - avg_pair_after)
                            if locked_shares_after > 0 and avg_pair_after > 0
                            else 0
                        )

                        logger.info(
                            f"TRADE | {trade_type} | {coin} {side.upper()} ${cost:.2f} | "
                            f"shares={shares:.2f} | price={price:.4f} | "
                            f"pair: {current_pair:.4f} -> {avg_pair_after:.4f} | "
                            f"locked_profit=${locked_profit:.4f} | "
                            f"tx={tx_hash[:16] if tx_hash else 'N/A'}..."
                        )
                        safe_call(write_trade, {
                            "market": market_slug,
                            "coin": coin,
                            "trade_type": trade_type,
                            "side": side,
                            "amount_usd": cost,
                            "shares": shares,
                            "price": price,
                            "pair_cost": avg_pair_after if avg_pair_after > 0 else current_pair,
                            "avg_yes_cost_after": metrics_after.get("avg_up", 0),
                            "avg_no_cost_after": metrics_after.get("avg_down", 0),
                            "locked_shares": locked_shares_after,
                            "locked_profit": locked_profit,
                            "projected_final_profit": projected_final_profit if projected_final_profit > 0 else None,
                            "success": True,
                            "error": "",
                            "tx_hash": tx_hash
                        }, dry_run=False)
                    else:
                        logger.warning(f"TRADE FAILED | {trade_type} | {coin} {side} | {msg}")
                        safe_call(write_trade, {
                            "market": market_slug,
                            "coin": coin,
                            "trade_type": trade_type,
                            "side": side,
                            "amount_usd": trade_usd,
                            "shares": None,
                            "price": price,
                            "pair_cost": current_pair,
                            "avg_yes_cost_after": None,
                            "avg_no_cost_after": None,
                            "locked_shares": None,
                            "locked_profit": None,
                            "projected_final_profit": None,
                            "success": False,
                            "error": msg[:200],
                            "tx_hash": tx_hash
                        }, dry_run=False)

                        # Check for rate limit (403)
                        if "403" in msg or "rate" in msg.lower():
                            state.last_trade_time = time.time() + 60

                    state.last_trade_time = time.time()
                    safe_call(write_last_trade_time)

                # ONE TRADE PER TICK MAX
                break

            # Sleep for remainder of tick and log duration
            elapsed = time.time() - tick_start
            logger.debug(f"TICK_DURATION | elapsed={elapsed:.3f}s")
            if elapsed < TICK_INTERVAL:
                time.sleep(TICK_INTERVAL - elapsed)

        except KeyboardInterrupt:
            logger.info("Shutdown requested")
            break
        except Exception as e:
            logger.error(f"Tick error: {e}")
            time.sleep(1)


# =============================================================================
# ENTRY POINT
# =============================================================================

if __name__ == "__main__":
    run_engine()
