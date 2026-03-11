FROM linuxserver/ffmpeg:latest

# Base image is Ubuntu; install python + tzdata
RUN apt-get update \
 && apt-get install -y --no-install-recommends python3 python3-pip tzdata \
 && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /tmp/requirements.txt
RUN python3 -m pip install --break-system-packages --no-cache-dir -r /tmp/requirements.txt
