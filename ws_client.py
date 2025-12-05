"""
Polymarket Real-Time WebSocket Client
=====================================
Non-blocking WebSocket listener for Polymarket CLOB orderbook data.
Runs in a background thread with its own asyncio event loop.

PUBLIC API:
    start_ws_listener(token_ids: List[str]) - Start the WebSocket listener
    stop_ws_listener() - Stop the WebSocket listener
    get_ws_price(token_id: str) -> dict | None - Get latest price data
    is_ws_fresh(token_id: str, max_age: float = 1.5) -> bool - Check if data is fresh
"""

import asyncio
import json
import logging
import random
import threading
import time
from typing import Dict, List, Optional

try:
    import websockets
    from websockets.exceptions import ConnectionClosed, WebSocketException
except ImportError:
    raise ImportError("websockets library required: pip install websockets")

# =============================================================================
# CONFIGURATION
# =============================================================================

WS_BASE_URL = "wss://ws-subscriptions-clob.polymarket.com"
MARKET_CHANNEL = "market"

# Reconnect settings
BASE_BACKOFF = 2.0
MAX_BACKOFF = 60.0

# WebSocket settings
PING_INTERVAL = 30.0
PING_TIMEOUT = 10.0

# =============================================================================
# LOGGING
# =============================================================================

logger = logging.getLogger("ws_client")
if not logger.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter(
        "%(asctime)s | WS | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    ))
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)

# =============================================================================
# THREAD-SAFE DATA STORE
# =============================================================================

_ws_lock = threading.Lock()
_ws_data: Dict[str, Dict] = {}

# Global client reference
_ws_client: Optional["WebSocketClient"] = None
_ws_thread: Optional[threading.Thread] = None


def _update_price(token_id: str, best_bid: float, best_ask: float) -> None:
    """Thread-safe update of price data."""
    with _ws_lock:
        _ws_data[token_id] = {
            "ts": time.time(),
            "best_bid": best_bid,
            "best_ask": best_ask
        }


def _clear_data() -> None:
    """Thread-safe clear of all data."""
    with _ws_lock:
        _ws_data.clear()


def _build_ws_url() -> str:
    """Build the WebSocket URL for market channel."""
    return f"{WS_BASE_URL}/ws/{MARKET_CHANNEL}"


# =============================================================================
# PUBLIC API
# =============================================================================

def get_ws_price(token_id: str) -> Optional[Dict]:
    """
    Get the latest WebSocket price data for a token.

    Returns:
        dict with keys: ts, best_bid, best_ask
        None if no data or data is stale (older than 1.5 seconds)
    """
    with _ws_lock:
        data = _ws_data.get(token_id)
        if data is None:
            return None
        # Return None if stale (default 1.5s threshold)
        if time.time() - data["ts"] > 1.5:
            return None
        return data.copy()


def is_ws_fresh(token_id: str, max_age: float = 1.5) -> bool:
    """
    Check if WebSocket data for a token is fresh.

    Args:
        token_id: The token ID to check
        max_age: Maximum age in seconds (default 1.5)

    Returns:
        True if data exists and is younger than max_age
    """
    with _ws_lock:
        data = _ws_data.get(token_id)
        if data is None:
            return False
        return (time.time() - data["ts"]) <= max_age


def start_ws_listener(token_ids: List[str]) -> None:
    """
    Start the WebSocket listener in a background thread.

    Args:
        token_ids: List of Polymarket token IDs to subscribe to
    """
    global _ws_client, _ws_thread

    if _ws_thread is not None and _ws_thread.is_alive():
        logger.warning("WebSocket listener already running")
        return

    if not token_ids:
        logger.warning("No token IDs provided, not starting WebSocket")
        return

    _clear_data()
    _ws_client = WebSocketClient(token_ids)

    def _run_in_thread():
        """Thread entry point - creates new event loop and runs client."""
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(_ws_client.run())
        except Exception as e:
            logger.error(f"WebSocket thread error: {e}")
        finally:
            loop.close()

    _ws_thread = threading.Thread(target=_run_in_thread, daemon=True)
    _ws_thread.start()
    logger.info(f"WebSocket listener started for {len(token_ids)} tokens")


def stop_ws_listener() -> None:
    """Stop the WebSocket listener."""
    global _ws_client, _ws_thread

    if _ws_client is not None:
        _ws_client.stop()
        _ws_client = None

    _ws_thread = None
    _clear_data()
    logger.info("WebSocket listener stopped")


def update_subscriptions(token_ids: List[str]) -> None:
    """
    Update the token subscriptions without restarting.

    Args:
        token_ids: New list of token IDs to subscribe to
    """
    global _ws_client

    if _ws_client is not None:
        _ws_client.update_tokens(token_ids)
    else:
        start_ws_listener(token_ids)


# =============================================================================
# WEBSOCKET CLIENT CLASS
# =============================================================================

class WebSocketClient:
    """Async WebSocket client for Polymarket orderbook data."""

    def __init__(self, token_ids: List[str]):
        self._token_ids = list(token_ids)
        self._running = True
        self._ws = None
        self._tokens_updated = False
        self._new_tokens: List[str] = []

        # Exponential backoff state
        self._reconnect_attempts = 0
        self._base_backoff = BASE_BACKOFF
        self._max_backoff = MAX_BACKOFF

    def stop(self) -> None:
        """Signal the client to stop."""
        self._running = False

    def update_tokens(self, token_ids: List[str]) -> None:
        """Update token subscriptions (thread-safe)."""
        self._new_tokens = list(token_ids)
        self._tokens_updated = True

    def _calculate_backoff(self) -> float:
        """Calculate backoff delay with exponential increase and jitter."""
        if self._reconnect_attempts == 0:
            return 0

        # Exponential backoff: base * 2^(attempts-1), capped at max
        backoff = min(
            self._max_backoff,
            self._base_backoff * (2 ** (self._reconnect_attempts - 1))
        )

        # Add jitter: 0-20% of backoff
        jitter = random.uniform(0, backoff * 0.2)

        return backoff + jitter

    async def run(self) -> None:
        """Main run loop with auto-reconnect and exponential backoff."""
        while self._running:
            try:
                await self._connect_and_listen()
            except Exception as e:
                if self._running:
                    self._reconnect_attempts += 1
                    sleep_for = self._calculate_backoff()
                    logger.warning(
                        f"WS_RECONNECT | attempt={self._reconnect_attempts} | "
                        f"sleep={sleep_for:.1f}s | error={e}"
                    )
                    await asyncio.sleep(sleep_for)

    async def _connect_and_listen(self) -> None:
        """Connect to WebSocket and process messages."""
        ws_url = _build_ws_url()

        try:
            async with websockets.connect(
                ws_url,
                ping_interval=PING_INTERVAL,
                ping_timeout=PING_TIMEOUT,
                close_timeout=5.0
            ) as ws:
                self._ws = ws
                logger.info(f"WS_CONNECTED | url={ws_url}")

                # Subscribe to market channel with assets_ids
                await self._subscribe(ws)

                # Reset reconnect attempts on successful connection + subscription
                self._reconnect_attempts = 0

                # Process messages
                while self._running:
                    # Check for token updates
                    if self._tokens_updated:
                        self._token_ids = self._new_tokens
                        self._tokens_updated = False
                        await self._subscribe(ws)
                        logger.info(f"WS_RESUBSCRIBE | assets={len(self._token_ids)}")

                    try:
                        message = await asyncio.wait_for(
                            ws.recv(),
                            timeout=PING_INTERVAL + 5
                        )
                        await self._handle_message(message)
                    except asyncio.TimeoutError:
                        # No message received, but connection is alive (ping/pong)
                        continue

        except ConnectionClosed as e:
            if self._running:
                logger.warning(f"WS_CLOSED | code={e.code} | reason={e.reason or 'None'}")
        except WebSocketException as e:
            if self._running:
                logger.warning(f"WS_EXCEPTION | {e}")
        except Exception as e:
            if self._running:
                logger.error(f"WS_ERROR | {e}")
        finally:
            self._ws = None

    async def _subscribe(self, ws) -> None:
        """Send subscription message for all tokens using Polymarket protocol."""
        if not self._token_ids:
            return

        # Polymarket market channel subscription format
        # Must use "assets_ids" (plural-plural) and "type": "market"
        subscribe_msg = {
            "assets_ids": list(self._token_ids),
            "type": MARKET_CHANNEL
        }

        await ws.send(json.dumps(subscribe_msg))
        logger.info(f"WS_SUBSCRIBE | assets={len(self._token_ids)}")

    async def _handle_message(self, raw_message: str) -> None:
        """Process incoming WebSocket message."""
        try:
            data = json.loads(raw_message)

            # Handle different message types
            msg_type = data.get("type") or data.get("event_type")

            if msg_type in ("book", "orderbook", "market"):
                await self._handle_orderbook(data)
            elif msg_type == "price_change":
                await self._handle_price_change(data)
            # Silently ignore other message types (subscribed, ping, etc.)

        except json.JSONDecodeError:
            # Silently ignore malformed JSON
            pass
        except Exception:
            # Silently ignore other parsing errors
            pass

    async def _handle_orderbook(self, data: dict) -> None:
        """Handle orderbook message format."""
        try:
            # Try different field names for token ID
            token_id = data.get("asset_id") or data.get("market") or data.get("symbol")
            if not token_id:
                return

            bids = data.get("bids", [])
            asks = data.get("asks", [])

            # Extract best bid/ask
            best_bid = 0.0
            best_ask = 1.0

            if bids:
                # bids[0] can be dict or list [price, size]
                if isinstance(bids[0], dict):
                    best_bid = float(bids[0].get("price", 0))
                elif isinstance(bids[0], (list, tuple)) and len(bids[0]) >= 1:
                    best_bid = float(bids[0][0])

            if asks:
                if isinstance(asks[0], dict):
                    best_ask = float(asks[0].get("price", 1))
                elif isinstance(asks[0], (list, tuple)) and len(asks[0]) >= 1:
                    best_ask = float(asks[0][0])

            _update_price(token_id, best_bid, best_ask)

        except (KeyError, IndexError, ValueError, TypeError):
            # Silently ignore parsing errors
            pass

    async def _handle_price_change(self, data: dict) -> None:
        """Handle price_change message format."""
        try:
            token_id = data.get("asset_id") or data.get("market")
            if not token_id:
                return

            # price_change may have direct price fields
            price = data.get("price")
            if price is not None:
                price = float(price)
                # Use price as midpoint estimate
                _update_price(token_id, price - 0.005, price + 0.005)
                return

            # Or may have bid/ask directly
            best_bid = float(data.get("bid", data.get("best_bid", 0)))
            best_ask = float(data.get("ask", data.get("best_ask", 1)))

            _update_price(token_id, best_bid, best_ask)

        except (KeyError, ValueError, TypeError):
            # Silently ignore parsing errors
            pass


# =============================================================================
# MODULE TEST
# =============================================================================

if __name__ == "__main__":
    import sys

    # Example token IDs (replace with real ones for testing)
    test_tokens = [
        # These are example token IDs - replace with actual Polymarket token IDs
        "21742633143463906290569050155826241533067272736897614950488156847949938836455",
        "48331043336612883890938759509493159234755048973500640148014422747788308965732"
    ]

    if len(sys.argv) > 1:
        test_tokens = sys.argv[1:]

    print(f"Starting WebSocket listener for {len(test_tokens)} tokens...")
    print("Press Ctrl+C to stop\n")

    start_ws_listener(test_tokens)

    try:
        while True:
            time.sleep(1)
            print("-" * 60)
            for token_id in test_tokens:
                data = get_ws_price(token_id)
                fresh = is_ws_fresh(token_id)
                if data:
                    print(f"Token: {token_id[:20]}...")
                    print(f"  Bid: {data['best_bid']:.4f}  Ask: {data['best_ask']:.4f}  Fresh: {fresh}")
                else:
                    print(f"Token: {token_id[:20]}... - No data (fresh={fresh})")
    except KeyboardInterrupt:
        print("\nStopping...")
        stop_ws_listener()
        print("Done.")
