FROM ubuntu:24.04

RUN apt-get update -y && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        curl wget ca-certificates git python3 python3-pip python3-pytest bash ripgrep && \
    ln -s /usr/bin/python3 /usr/bin/python && \
    pip3 install --break-system-packages pytest-json-ctrf bytewax==0.21.1 && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

RUN mkdir -p /home/user && chmod 0755 /home/user

RUN mkdir -p /home/user/bytewax_project/input_data && \
    cd /home/user/bytewax_project && \
    echo -e "hello\nworld" > input_data/file1.txt && \
    echo -e "async\nstreams" > input_data/file2.txt && \
    echo -e "bytewax\nrocks" > input_data/file3.txt

WORKDIR /home/user/bytewax_project

LABEL Author="generate-harbor-tasks"
LABEL Description="Bytewax task environment with bytewax==0.21.1 installed and sample input data."

CMD ["/bin/bash", "-c", "trap : TERM INT; sleep infinity & wait"]

# Container contents:
#   - always required apt packages: curl wget ca-certificates git python3 python3-pip python3-pytest bash ripgrep
#   - always required pip3 packages: pytest-json-ctrf
#   - bytewax==0.21.1 installed via pip3
#   - /home/user/bytewax_project: project directory
#   - /home/user/bytewax_project/input_data: sample text files for testing
