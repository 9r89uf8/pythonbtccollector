"""Fixed history lifetime for non-ghost collector data."""

from price_collector.market import MARKET_MS


HISTORY_RETENTION_DAYS = 10
HISTORY_RETENTION_MS = HISTORY_RETENTION_DAYS * 86_400_000


def retained_market_floor_ms(now_ms: int) -> int:
    """First retained market, rounding the ten-day cutoff up to its boundary.

    Expiring complete market groups can remove up to five minutes early; it
    never retains an older constituent merely to finish its market window.
    The explicit clock also keeps backfills and retention on the same policy.
    """
    if isinstance(now_ms, bool) or not isinstance(now_ms, int) or now_ms < 0:
        raise ValueError("now_ms must be a non-negative integer")
    cutoff_ms = max(0, now_ms - HISTORY_RETENTION_MS)
    return ((cutoff_ms + MARKET_MS - 1) // MARKET_MS) * MARKET_MS
