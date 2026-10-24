FROM node:22-slim

RUN apt-get update \
 && apt-get install -y --no-install-recommends python3 ca-certificates \
 && rm -rf /var/lib/apt/lists/* \
 && npm i -g opencode-ai \
 && opencode --version

WORKDIR /app
COPY oc_bridge.py /app/oc_bridge.py

ENV HOME_BASE=/data/home STATE_DIR=/data/state
VOLUME /data
EXPOSE 8790

HEALTHCHECK --interval=30s --timeout=5s --start-period=90s --retries=3 \
  CMD python3 -c "import urllib.request;urllib.request.urlopen('http://127.0.0.1:8790/healthz',timeout=4)"

ENTRYPOINT ["python3", "/app/oc_bridge.py"]
