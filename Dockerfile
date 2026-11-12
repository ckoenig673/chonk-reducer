FROM linuxserver/ffmpeg:9.0.2-cli-ls89@sha256:822d04f0b2d0c4847430f5cc90f20ea48be866bb502ece63fdeb729400bf5361

# Base image is Ubuntu; install python + tzdata
RUN apt-get update \
 && apt-get install -y --no-install-recommends python3 python3-pip tzdata \
 && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /tmp/requirements.txt
RUN python3 -m pip install --break-system-packages --no-cache-dir -r /tmp/requirements.txt
