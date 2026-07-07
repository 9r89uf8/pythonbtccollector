from dataclasses import dataclass


MARKET_MS = 300_000


@dataclass(frozen=True)
class MarketWindow:
    market_id: int
    market_start_ms: int
    market_end_ms: int


def market_for_sample_second(sample_second_ms: int) -> MarketWindow:
    if sample_second_ms % 1000 != 0:
        raise ValueError("sample_second_ms must be floored to a whole second")

    market_start_ms = (sample_second_ms // MARKET_MS) * MARKET_MS
    market_end_ms = market_start_ms + MARKET_MS
    market_id = market_start_ms // MARKET_MS

    return MarketWindow(
        market_id=market_id,
        market_start_ms=market_start_ms,
        market_end_ms=market_end_ms,
    )

