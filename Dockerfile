FROM python:3.11-slim
WORKDIR /app
RUN pip install --no-cache-dir aiohttp websockets scipy requests python-dotenv web3 py-clob-client uvloop orjson

# Build-time args → runtime ENV vars
ARG POLY_ADDRESS
ARG POLY_PRIVATE_KEY
ARG POLY_NETWORK=polygon
ARG CLOB_API_KEY
ARG BANKROLL=100.0
ARG DRY_RUN=true
ARG DATA_DIR=/data

ENV POLY_ADDRESS=$POLY_ADDRESS
ENV POLY_PRIVATE_KEY=$POLY_PRIVATE_KEY
ENV POLY_NETWORK=$POLY_NETWORK
ENV CLOB_API_KEY=$CLOB_API_KEY
ENV BANKROLL=$BANKROLL
ENV DRY_RUN=$DRY_RUN
ENV DATA_DIR=$DATA_DIR

RUN mkdir -p /data

COPY clawdbot_paper.py .
COPY clawdbot_live.py .
COPY runtime_utils.py .
COPY clawdbot_copyflow.json .
COPY scripts ./scripts
CMD ["python", "-u", "clawdbot_live.py"]
