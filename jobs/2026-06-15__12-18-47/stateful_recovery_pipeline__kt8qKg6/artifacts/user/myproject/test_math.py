from datetime import datetime, timezone
align_to = datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc)
print("win -1 open:", align_to.timestamp() - 30)
print("win 0 open:", align_to.timestamp())
