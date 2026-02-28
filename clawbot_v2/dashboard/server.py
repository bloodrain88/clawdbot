from __future__ import annotations

import asyncio
from aiohttp import web

from clawbot_v2.data.snapshot_store import SnapshotStore
from clawbot_v2.infra.log import get_logger


HTML = """<!doctype html><html><head><meta charset='utf-8'><title>Clawdbot V2 Dashboard</title></head>
<body style='font-family:system-ui;background:#060b16;color:#dbe4ff;padding:16px'>
<h2>Clawdbot V2 Dashboard</h2>
<pre id='out'>loading...</pre>
<script>
async function tick(){
  try{
    const r=await fetch('/api',{cache:'no-store'});
    const j=await r.json();
    document.getElementById('out').textContent=JSON.stringify(j,null,2);
  }catch(e){document.getElementById('out').textContent='dashboard error: '+e;}
}
setInterval(tick,2000);tick();
</script>
</body></html>"""


async def run_dashboard(*, data_dir: str, port: int, log_level: str = "INFO") -> None:
    log = get_logger("clawbot-v2-dashboard", log_level)
    store = SnapshotStore(data_dir)

    async def handle_html(_req: web.Request) -> web.Response:
        return web.Response(text=HTML, content_type="text/html")

    async def handle_api(_req: web.Request) -> web.Response:
        return web.json_response(store.read(), headers={"Cache-Control": "no-store"})

    app = web.Application()
    app.router.add_get("/", handle_html)
    app.router.add_get("/api", handle_api)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    log.info("dashboard running on :%s", port)

    while True:
        await asyncio.sleep(3600)
