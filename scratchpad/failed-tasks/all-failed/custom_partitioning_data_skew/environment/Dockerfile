FROM ubuntu:24.04

RUN apt-get update -y && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        curl wget ca-certificates git python3 python3-pip python3-pytest bash ripgrep && \
    ln -s /usr/bin/python3 /usr/bin/python && \
    pip3 install --break-system-packages pytest-json-ctrf bytewax && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

RUN mkdir -p /home/user && chmod 0755 /home/user

RUN mkdir -p /home/user/myproject && \
    cd /home/user/myproject && \
    cat >input.jsonl <<'EOF'
{"key": "A", "value": 1}
{"key": "B", "value": 2}
{"key": "C", "value": 3}
{"key": "A", "value": 4}
{"key": "D", "value": 5}
{"key": "B", "value": 6}
{"key": "A", "value": 7}
{"key": "E", "value": 8}
{"key": "C", "value": 9}
{"key": "A", "value": 10}
EOF

LABEL Author="generate-harbor-tasks"
LABEL Description="Bytewax custom partitioning data skew task environment"

CMD ["/bin/bash", "-c", "trap : TERM INT; sleep infinity & wait"]

# Container contents:
#   - always required apt packages: curl wget ca-certificates git python3 python3-pip python3-pytest bash ripgrep
#   - always required pip3 packages: pytest-json-ctrf
#   - bytewax installed via pip3
#   - /home/user/myproject: Bytewax project directory
#   - /home/user/myproject/input.jsonl: initial input data file
