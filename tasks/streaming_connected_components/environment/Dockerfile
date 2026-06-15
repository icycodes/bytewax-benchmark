FROM ubuntu:24.04

RUN apt-get update -y && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        curl wget ca-certificates git python3 python3-pip python3-pytest bash ripgrep && \
    ln -s /usr/bin/python3 /usr/bin/python && \
    pip3 install --break-system-packages pytest-json-ctrf bytewax==0.21.1 && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

RUN mkdir -p /home/user && chmod 0755 /home/user

RUN mkdir -p /home/user/graph_pipeline && \
    cd /home/user/graph_pipeline && \
    cat >input_edges.jsonl <<'EOF'
{"u": "A", "v": "B"}
{"u": "B", "v": "C"}
{"u": "A", "v": "C"}
{"u": "D", "v": "E"}
{"u": "C", "v": "E"}
EOF

LABEL Author="generate-harbor-tasks"
LABEL Description="Bytewax graph processing environment with input edges."

CMD ["/bin/bash", "-c", "trap : TERM INT; sleep infinity & wait"]

# Container contents:
#   - always required apt packages: curl wget ca-certificates git python3 python3-pip python3-pytest bash ripgrep
#   - always required pip3 packages: pytest-json-ctrf
#   - bytewax==0.21.1 installed via pip3 for graph processing
#   - /home/user/graph_pipeline: project directory
#   - /home/user/graph_pipeline/input_edges.jsonl: input file for the graph pipeline
