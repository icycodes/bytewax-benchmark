Time-based aggregations require defining specific clocks and windowers. Bytewax can process out-of-order data using event time.

You need to implement a dataflow that reads a stream of timestamped user click events. Group these events into 5-minute tumbling windows using an `EventClock` and output the total count of clicks per window for each user. 

**Constraints:**
- Must use `bytewax.operators.windowing.fold_window`.
- Must extract the timestamp from the event using a timezone-aware UTC datetime.
- The `TumblingWindower` must be aligned to a fixed datetime epoch.