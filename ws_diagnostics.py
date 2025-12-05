"""
WebSocket Diagnostics Tool
==========================
Standalone validation harness for testing ws_client.py
NOT part of the main engine loop - run manually for testing.

Usage:
    python ws_diagnostics.py [token_id1] [token_id2] ...

If no token IDs provided, uses default test tokens.
"""

import sys
import time
from ws_client import start_ws_listener, stop_ws_listener, get_ws_price, is_ws_fresh

# Default test token IDs (replace with actual active market tokens)
DEFAULT_TOKENS = [
    "21742633143463906290569050155826241533067272736897614950488156847949938836455",
    "48331043336612883890938759509493159234755048973500640148014422747788308965732"
]


def run_diagnostics(token_ids: list, duration: int = 30):
    """
    Run WebSocket diagnostics for specified tokens.

    Args:
        token_ids: List of token IDs to monitor
        duration: How long to run in seconds (default 30)
    """
    print(f"WS_DIAG | Starting diagnostics for {len(token_ids)} tokens")
    print(f"WS_DIAG | Duration: {duration}s")
    print("-" * 70)

    start_ws_listener(token_ids)

    # Give WebSocket time to connect
    time.sleep(2)

    start_time = time.time()
    tick = 0

    try:
        while time.time() - start_time < duration:
            tick += 1
            print(f"\n--- Tick {tick} ---")

            for token_id in token_ids:
                data = get_ws_price(token_id)
                fresh = is_ws_fresh(token_id, max_age=1.5)

                if data:
                    age = time.time() - data.get("ts", 0)
                    print(
                        f"WS_DIAG | token={token_id[:16]}... | "
                        f"best_bid={data.get('best_bid', 0):.4f} | "
                        f"best_ask={data.get('best_ask', 0):.4f} | "
                        f"age={age:.2f}s | fresh={fresh}"
                    )
                else:
                    print(f"WS_DIAG | token={token_id[:16]}... | NO DATA | fresh={fresh}")

            time.sleep(1)

    except KeyboardInterrupt:
        print("\n\nWS_DIAG | Interrupted by user")

    finally:
        stop_ws_listener()
        print("\nWS_DIAG | Diagnostics complete")


if __name__ == "__main__":
    # Use command line args or defaults
    if len(sys.argv) > 1:
        tokens = sys.argv[1:]
    else:
        tokens = DEFAULT_TOKENS
        print("WS_DIAG | Using default test tokens (provide token IDs as args to test specific markets)")

    run_diagnostics(tokens, duration=30)
