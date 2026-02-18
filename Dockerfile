FROM python:3.11-slim
WORKDIR /app
RUN pip install --no-cache-dir aiohttp websockets scipy requests python-dotenv eth-account py-clob-client
COPY clawdbot_paper.py .
COPY clawdbot_live.py .
CMD ["python", "-u", "clawdbot_live.py"]
