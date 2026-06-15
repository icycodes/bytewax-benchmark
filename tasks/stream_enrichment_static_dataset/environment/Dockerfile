FROM ubuntu:24.04

RUN apt-get update -y && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        curl wget ca-certificates git python3 python3-pip python3-pytest bash ripgrep && \
    ln -s /usr/bin/python3 /usr/bin/python && \
    pip3 install --break-system-packages pytest-json-ctrf bytewax==0.21.1 && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

RUN mkdir -p /home/user && chmod 0755 /home/user

RUN mkdir -p /home/user/bytewax_project && \
    cd /home/user/bytewax_project && \
    cat >products.json <<'EOF'
{
  "p1": {"category": "electronics", "price": 100.0},
  "p2": {"category": "books", "price": 15.0}
}
EOF
    
RUN cd /home/user/bytewax_project && \
    cat >transactions.jsonl <<'EOF'
{"transaction_id": "t1", "product_id": "p1", "timestamp": "2026-01-01T12:00:10Z", "quantity": 1}
{"transaction_id": "t2", "product_id": "p2", "timestamp": "2026-01-01T12:00:20Z", "quantity": 2}
{"transaction_id": "t3", "product_id": "p1", "timestamp": "2026-01-01T12:00:45Z", "quantity": 2}
{"transaction_id": "t4", "product_id": "p3", "timestamp": "2026-01-01T12:00:50Z", "quantity": 1}
{"transaction_id": "t5", "product_id": "p1", "timestamp": "2026-01-01T12:01:05Z", "quantity": 1}
EOF

LABEL Author="generate-harbor-tasks"
LABEL Description="Bytewax environment with pre-existing products.json and transactions.jsonl data."

CMD ["/bin/bash", "-c", "trap : TERM INT; sleep infinity & wait"]

# Container contents:
#   - always required apt packages: curl wget ca-certificates git python3 python3-pip python3-pytest bash ripgrep
#   - always required pip3 packages: pytest-json-ctrf
#   - bytewax==0.21.1 installed via pip3
#   - /home/user/bytewax_project: project directory
#   - /home/user/bytewax_project/products.json: static dataset
#   - /home/user/bytewax_project/transactions.jsonl: input stream
