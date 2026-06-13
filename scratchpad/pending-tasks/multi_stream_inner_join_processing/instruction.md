Bytewax supports joining multiple streams based on a shared key, acting as an inner join by default (waiting for values on all sides).

You need to build a dataflow that ingests two mock streams: `orders` and `payments`. Join these two streams on `order_id` and output a single stream containing only the orders that have successfully matched with a payment.

**Constraints:**
- Both input streams must be converted to Keyed Streams before joining.
- Must use `bytewax.operators.join` to perform the join operation.
- The output must naturally take the shape of `(order_id, (order_data, payment_data))`.