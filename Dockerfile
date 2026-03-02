FROM linuxserver/ffmpeg:latest

# Base image is Ubuntu; install python + tzdata
RUN apt-get update \
 && apt-get install -y --no-install-recommends python3 tzdata \
 && rm -rf /var/lib/apt/lists/*
