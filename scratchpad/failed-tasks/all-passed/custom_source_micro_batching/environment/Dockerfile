FROM ubuntu:24.04

RUN apt-get update -y && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        curl wget ca-certificates git python3 python3-pip python3-pytest bash ripgrep && \
    ln -s /usr/bin/python3 /usr/bin/python && \
    pip3 install --break-system-packages pytest-json-ctrf bytewax==0.21.1 && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

RUN mkdir -p /home/user && chmod 0755 /home/user

RUN mkdir -p /home/user/app && \
    cd /home/user/app && \
    python3 -c "\
import sqlite3; \
conn = sqlite3.connect('events.db'); \
c = conn.cursor(); \
c.execute('CREATE TABLE events (id INTEGER PRIMARY KEY, user_id TEXT, event_type TEXT, payload TEXT)'); \
events = [ \
    (1, 'user1', 'click', 'A'), \
    (2, 'user1', 'click', 'B'), \
    (3, 'user2', 'click', 'C'), \
    (4, 'user1', 'click', 'D'), \
    (5, 'user1', 'click', 'E'), \
    (6, 'user2', 'click', 'F'), \
    (7, 'user1', 'click', 'G'), \
]; \
c.executemany('INSERT INTO events VALUES (?, ?, ?, ?)', events); \
conn.commit(); \
conn.close()"

LABEL Author="generate-harbor-tasks"
LABEL Description="Bytewax environment with pre-populated SQLite database for micro-batching task."

CMD ["/bin/bash", "-c", "trap : TERM INT; sleep infinity & wait"]

# Container contents:
#   - always required apt packages: curl wget ca-certificates git python3 python3-pip python3-pytest bash ripgrep
#   - always required pip3 packages: pytest-json-ctrf
#   - bytewax==0.21.1 installed via pip3
#   - /home/user/app: project directory
#   - /home/user/app/events.db: SQLite database with 7 events
