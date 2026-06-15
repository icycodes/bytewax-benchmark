from datetime import datetime
def merger(state1, state2):
    if state1["min_ts"] is None:
        return state2
    if state2["min_ts"] is None:
        return state1
        
    new_state = {
        "window_start": state1["window_start"] or state2["window_start"],
        "open": state1["open"] if state1["min_ts"] < state2["min_ts"] else state2["open"],
        "close": state1["close"] if state1["max_ts"] > state2["max_ts"] else state2["close"],
        "high": max(state1["high"], state2["high"]),
        "low": min(state1["low"], state2["low"]),
        "volume": state1["volume"] + state2["volume"],
        "min_ts": min(state1["min_ts"], state2["min_ts"]),
        "max_ts": max(state1["max_ts"], state2["max_ts"]),
    }
    return new_state
