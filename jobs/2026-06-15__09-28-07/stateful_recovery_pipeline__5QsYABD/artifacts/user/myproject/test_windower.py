from datetime import datetime, timezone, timedelta
from bytewax.operators.windowing import SlidingWindower

align_to = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
windower = SlidingWindower(
    length=timedelta(seconds=60),
    offset=timedelta(seconds=30),
    align_to=align_to
)
print(dir(windower))
