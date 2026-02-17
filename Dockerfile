FROM python:3.11-slim
WORKDIR /app
RUN pip install --no-cache-dir aiohttp websockets scipy
COPY clawdbot_paper.py .
CMD ["python", "-u", "clawdbot_paper.py"]
