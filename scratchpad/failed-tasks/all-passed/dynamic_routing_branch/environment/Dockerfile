FROM ubuntu:24.04

RUN apt-get update -y && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        curl wget ca-certificates git python3 python3-pip python3-pytest bash ripgrep && \
    ln -s /usr/bin/python3 /usr/bin/python && \
    pip3 install --break-system-packages pytest-json-ctrf bytewax && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

RUN mkdir -p /home/user && chmod 0755 /home/user

RUN mkdir -p /home/user/bytewax_routing && \
    cd /home/user/bytewax_routing && \
    cat >sensors.json <<'EOF'
{"sensor_type": "temperature", "value_c": 25.0}
{"sensor_type": "humidity", "value": 60.5}
{"sensor_type": "temperature", "value_c": 30.0}
{"invalid_json": true}
this is not json
EOF

LABEL Author="generate-harbor-tasks"
LABEL Description="Bytewax dataflow task environment for dynamic routing."

CMD ["/bin/bash", "-c", "trap : TERM INT; sleep infinity & wait"]

# Container contents:
#   - always required apt packages: curl wget ca-certificates git python3 python3-pip python3-pytest bash ripgrep
#   - always required pip3 packages: pytest-json-ctrf
#   - bytewax installed via pip3
#   - /home/user/bytewax_routing: project directory
#   - /home/user/bytewax_routing/sensors.json: initial input file
