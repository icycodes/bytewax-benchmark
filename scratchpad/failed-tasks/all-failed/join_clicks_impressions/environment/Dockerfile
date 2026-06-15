FROM ubuntu:24.04

RUN apt-get update -y && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        curl wget ca-certificates git python3 python3-pip python3-pytest bash ripgrep && \
    ln -s /usr/bin/python3 /usr/bin/python && \
    pip3 install --break-system-packages pytest-json-ctrf bytewax==0.21.1 && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

RUN mkdir -p /home/user && chmod 0755 /home/user

RUN mkdir -p /home/user/ad_pipeline && \
    cd /home/user/ad_pipeline && \
    cat >impressions.jsonl <<'EOF'
{"ad_id": 1, "user_id": "u1"}
{"ad_id": 2, "user_id": "u2"}
{"ad_id": 3, "user_id": "u3"}
EOF

RUN cd /home/user/ad_pipeline && \
    cat >clicks.jsonl <<'EOF'
{"ad_id": "1", "click_time": "2026-06-15T10:00:00Z"}
{"ad_id": "3", "click_time": "2026-06-15T10:05:00Z"}
EOF

RUN cd /home/user/ad_pipeline && \
    mkdir -p ./recovery_dir && \
    python -m bytewax.recovery ./recovery_dir 1

LABEL Author="generate-harbor-tasks"
LABEL Description="Bytewax stream join task environment with pre-populated JSONL files and recovery directory."

CMD ["/bin/bash", "-c", "trap : TERM INT; sleep infinity & wait"]

# Container contents:
#   - always required apt packages: curl wget ca-certificates git python3 python3-pip python3-pytest bash ripgrep
#   - always required pip3 packages: pytest-json-ctrf
#   - bytewax==0.21.1 installed via pip3
#   - /home/user/ad_pipeline: Project directory
#   - /home/user/ad_pipeline/impressions.jsonl: Impressions dataset
#   - /home/user/ad_pipeline/clicks.jsonl: Clicks dataset
#   - /home/user/ad_pipeline/recovery_dir: Initialized SQLite recovery partitions
