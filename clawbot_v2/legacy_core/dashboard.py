from __future__ import annotations

import importlib

_LOADED = False


def _ensure_legacy_globals() -> None:
    global _LOADED
    if _LOADED:
        return
    legacy = importlib.import_module("clawdbot_live")
    g = globals()
    for k, v in vars(legacy).items():
        if k not in g:
            g[k] = v
    _LOADED = True


async def dashboard_loop(self):
    _ensure_legacy_globals()
    """Serve a live web dashboard on DASHBOARD_PORT (default 8080)."""
    port = int(os.environ.get("DASHBOARD_PORT", "8080"))
    from aiohttp import web

    HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>ClawdBot</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Schibsted+Grotesk:wght@400;500;600;700;800&family=Plus+Jakarta+Sans:wght@400;500;600;700;800&display=swap" rel="stylesheet">
<script src="https://cdn.jsdelivr.net/npm/chart.js@4/dist/chart.umd.min.js"></script>
<style>
:root{
  --bg:#06070a;--s1:#0b0d12;--s2:#10131a;--s3:#151a24;
  --b1:#1b2230;--b2:#273248;--b3:#34435f;
  --t1:#edf2ff;--t2:#97a5c2;--t3:#4f607d;
  --g:#00cc78;--gb:rgba(0,204,120,.09);--gbd:rgba(0,204,120,.22);
  --r:#ff3d3d;--rb:rgba(255,61,61,.09);--rbd:rgba(255,61,61,.22);
  --bl:#5f8dff;--blb:rgba(95,141,255,.10);--blbd:rgba(95,141,255,.24);
  --y:#f5a623;--yb:rgba(245,166,35,.09);
  --rd:12px;--rdl:16px;
}
*{box-sizing:border-box;margin:0;padding:0}
html{font-size:16px;-webkit-font-smoothing:antialiased;-moz-osx-font-smoothing:grayscale}
body{
  font-family:'Plus Jakarta Sans','Schibsted Grotesk',system-ui,-apple-system,sans-serif;
  font-variant-numeric:tabular-nums;
  background:var(--bg);color:var(--t1);min-height:100vh;
  background-image:
    radial-gradient(ellipse 88% 52% at 50% -5%,rgba(95,141,255,.09) 0%,transparent 62%),
    radial-gradient(ellipse 40% 30% at 88% 14%,rgba(0,204,120,.05) 0%,transparent 55%);
}
body::before{
  content:"";position:fixed;inset:0;pointer-events:none;z-index:-1;
  background:
    radial-gradient(900px 420px at 10% -5%, rgba(0,204,120,.06), transparent 62%),
    radial-gradient(860px 420px at 92% -8%, rgba(95,141,255,.08), transparent 66%);
}
::-webkit-scrollbar{width:4px;height:4px}
::-webkit-scrollbar-track{background:transparent}
::-webkit-scrollbar-thumb{background:var(--b2);border-radius:2px}

/* HEADER */
.H{
  position:sticky;top:0;z-index:50;height:48px;
  display:flex;align-items:center;justify-content:space-between;padding:0 28px;
  background:rgba(6,7,10,.82);backdrop-filter:blur(24px);
  border-bottom:1px solid rgba(79,96,125,.22);
}
.Hl{display:flex;align-items:center;gap:12px}
.logo{display:flex;align-items:center;gap:7px;font-size:.86rem;font-weight:700;letter-spacing:-.025em}
.ld{width:5px;height:5px;border-radius:50%;background:var(--g);flex-shrink:0;animation:lp 2.8s ease-in-out infinite}
@keyframes lp{0%,100%{opacity:1;box-shadow:0 0 0 0 rgba(0,204,120,.6)}55%{opacity:.85;box-shadow:0 0 0 5px rgba(0,204,120,0)}}
.logo b{color:var(--g)}
.tbadge{font-size:.7rem;color:var(--t3);padding:2px 7px;background:var(--s1);border:1px solid var(--b1);border-radius:3px}
.drybadge{font-size:.58rem;font-weight:700;letter-spacing:.08em;text-transform:uppercase;padding:2px 8px;border-radius:4px;background:var(--yb);color:var(--y);border:1px solid rgba(245,166,35,.3);display:none}
.Hr{display:flex;align-items:center}
.hs{display:flex;flex-direction:column;align-items:flex-end;gap:1px;padding:5px 14px;border-right:1px solid var(--b1)}
.hs:first-child{border-left:1px solid var(--b1)}
.hsl{font-size:.52rem;font-weight:700;color:var(--t3);text-transform:uppercase;letter-spacing:.1em}
.hsv{font-size:.95rem;font-weight:600}

/* WRAP */
.W{padding:22px 28px;max-width:1920px;margin:0 auto;display:flex;flex-direction:column;gap:20px}

/* PRICES */
.pc-row{display:flex;gap:4px;flex-wrap:wrap}
.pc{display:flex;align-items:center;gap:6px;background:var(--s1);border:1px solid var(--b1);border-radius:5px;padding:5px 10px;cursor:default;transition:border-color .15s}
.pc:hover{border-color:var(--b2)}
.pc .a{font-size:.61rem;font-weight:700;color:var(--t3);letter-spacing:.07em;text-transform:uppercase}
.pc .v{font-size:.86rem;font-weight:600}

/* METRICS BAR */
.mbar{display:grid;grid-template-columns:repeat(3,minmax(220px,1fr));gap:10px}
.mi{
  padding:16px 18px;border:1px solid var(--b1);border-radius:var(--rdl);
  display:flex;flex-direction:column;gap:6px;transition:all .18s;cursor:default;
  background:linear-gradient(180deg,rgba(16,19,26,.95) 0%, rgba(11,13,18,.95) 100%);
}
.mi:hover{border-color:var(--b2);transform:translateY(-1px);box-shadow:0 10px 26px rgba(0,0,0,.28)}
.mi-l{font-size:.52rem;font-weight:700;color:var(--t3);text-transform:uppercase;letter-spacing:.1em}
.mi-v{font-size:1.52rem;font-weight:800;letter-spacing:-.03em;line-height:1}
.mi-s{font-size:.63rem;color:var(--t2);margin-top:1px}
.mi-s b{color:var(--t1);font-weight:500}

/* SECTION */
.sh{display:flex;align-items:baseline;gap:8px;margin-bottom:10px}
.st{font-size:.6rem;font-weight:700;color:var(--t3);text-transform:uppercase;letter-spacing:.1em}

/* POSITIONS */
.pgrid{display:grid;grid-template-columns:repeat(auto-fill,minmax(360px,1fr));gap:14px}
.pcard{background:#0b111b;border:1px solid #1b2a3f;border-radius:18px;overflow:hidden;display:flex;flex-direction:column;transition:border-color .2s,box-shadow .2s}
.pcard:hover{box-shadow:0 10px 34px rgba(0,0,0,.46)}
.pcard{animation:fadeIn .28s ease both}
@keyframes fadeIn{from{opacity:0;transform:translateY(4px)}to{opacity:1;transform:translateY(0)}}
.pcard.lead{border-color:#1f6a4f;box-shadow:inset 0 0 0 1px rgba(0,204,120,.18)}
.pcard.trail{border-color:#7a2c38;box-shadow:inset 0 0 0 1px rgba(255,61,61,.16)}
.pcard.lead .ca{background:linear-gradient(180deg,rgba(9,26,20,.92) 0%,rgba(8,16,13,.98) 100%)}
.pcard.trail .ca{background:linear-gradient(180deg,rgba(30,12,16,.90) 0%,rgba(15,10,11,.98) 100%)}
.bstate{font-size:.62rem;font-weight:800;letter-spacing:.06em;padding:2px 8px;border-radius:4px;text-transform:uppercase}
.bstate.win{background:rgba(0,204,120,.14);border:1px solid rgba(0,204,120,.40);color:#52d69d}
.bstate.lose{background:rgba(255,61,61,.14);border:1px solid rgba(255,61,61,.38);color:#ff7c7c}
.bstate.na{background:rgba(95,141,255,.12);border:1px solid rgba(95,141,255,.30);color:#8eb0ff}
.ph{padding:14px 16px 0;display:flex;align-items:flex-start;justify-content:space-between;gap:8px}
.phl{display:flex;align-items:flex-start;gap:6px;flex-direction:column}
.psym{font-size:1rem;font-weight:800;letter-spacing:-.02em}
.pevt{font-size:.8rem;color:var(--t2)}
.pdur{font-size:.62rem;color:var(--t3);background:var(--s2);border:1px solid var(--b1);padding:2px 5px;border-radius:3px}
.pup{font-size:.62rem;font-weight:600;padding:2px 7px;border-radius:3px;background:var(--blb);color:var(--bl);border:1px solid var(--blbd)}
.pdn{font-size:.62rem;font-weight:600;padding:2px 7px;border-radius:3px;background:var(--rb);color:var(--r);border:1px solid var(--rbd)}
.phr{display:flex;align-items:center;gap:5px}
.blead{font-size:.58rem;font-weight:700;letter-spacing:.05em;padding:2px 7px;border-radius:3px;background:var(--gb);color:var(--g);border:1px solid var(--gbd)}
.btrail{font-size:.58rem;font-weight:700;letter-spacing:.05em;padding:2px 7px;border-radius:3px;background:var(--rb);color:var(--r);border:1px solid var(--rbd)}
.bunk{font-size:.58rem;font-weight:600;padding:2px 6px;border-radius:3px;background:var(--s2);color:var(--t3);border:1px solid var(--b1)}
.stag{font-size:.62rem;color:var(--t3)}
.pdata{display:grid;grid-template-columns:1fr 1fr auto;padding:10px 16px 12px;gap:10px;border-bottom:1px solid #1a2a41}
.di{display:flex;flex-direction:column;gap:3px}
.dl{font-size:.56rem;font-weight:700;color:#6f85a9;text-transform:uppercase;letter-spacing:.08em}
.dv{font-size:1.05rem;font-weight:700}
.pcount{display:flex;flex-direction:column;align-items:flex-end;justify-content:center}
.pcount .dl{font-size:.52rem}
.pcv{font-size:1.8rem;font-weight:800;line-height:1;color:#ff6b6b}
.pcv small{font-size:.65rem;color:#7f90ad;margin-left:2px}
.ca{height:156px;border-top:1px solid var(--b1);border-bottom:1px solid var(--b1);background:linear-gradient(180deg,rgba(18,22,30,.95) 0%,rgba(9,11,16,.98) 100%);position:relative}
.ca::after{
  content:"";position:absolute;inset:0;pointer-events:none;
  background:linear-gradient(180deg,rgba(95,141,255,.04) 0%,rgba(0,0,0,0) 65%);
}
canvas{display:block;width:100%!important}
.pf{padding:9px 16px;display:flex;justify-content:space-between;align-items:center;font-size:.7rem}
.pf .lbl{color:var(--t2)}
.pf b{color:var(--t1);font-size:.75rem}
.pf .rk{color:var(--t3);font-size:.64rem;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;max-width:150px}
.tb{height:2px;background:var(--b1)}
.tbf{height:100%;transition:width .5s linear}

/* BOTTOM */
.bot{display:grid;grid-template-columns:300px 1fr;gap:16px;align-items:start}
@media(max-width:780px){.bot{grid-template-columns:1fr}}

/* SIDE PANEL */
.lpanel{display:flex;flex-direction:column;gap:9px}
.card{background:var(--s1);border:1px solid var(--b1);border-radius:var(--rdl);overflow:hidden}
.ch{padding:10px 14px;border-bottom:1px solid var(--b1);font-size:.58rem;font-weight:700;color:var(--t3);text-transform:uppercase;letter-spacing:.1em}
.ce{padding:13px;text-align:center;font-size:.7rem;color:var(--t3)}
.gates{padding:8px 10px;display:flex;flex-wrap:wrap;gap:4px}
.gtag{font-size:.58rem;font-weight:700;text-transform:uppercase;letter-spacing:.06em;padding:3px 7px;border-radius:4px;background:var(--blb);color:var(--bl);border:1px solid var(--blbd)}
.skrow{display:flex;justify-content:space-between;align-items:center;padding:6px 12px;border-bottom:1px solid rgba(20,20,40,.8)}
.skrow:last-child{border-bottom:none}
.skrow:hover{background:var(--s2)}
.skr{font-size:.64rem;color:var(--t2);line-height:1.4}
.skc{font-size:.76rem;font-weight:600;color:var(--t1)}

/* EXECQ TABLE */
.etw{background:var(--s1);border:1px solid var(--b1);border-radius:var(--rdl);overflow:hidden}
.eth{padding:11px 16px;border-bottom:1px solid var(--b1);display:flex;align-items:center;justify-content:space-between}
.etl{font-size:.58rem;font-weight:700;color:var(--t3);text-transform:uppercase;letter-spacing:.1em}
.ett{font-size:.82rem;font-weight:700}
.et{width:100%;border-collapse:collapse}
.et th{font-size:.56rem;font-weight:700;color:var(--t3);text-transform:uppercase;letter-spacing:.09em;padding:9px 14px;border-bottom:1px solid var(--b1);text-align:left}
.et th:last-child{text-align:right}
.et td{padding:9px 14px;border-bottom:1px solid var(--b1);vertical-align:middle}
.et tr:last-child td{border-bottom:none}
.et tr:hover td{background:var(--s2)}
.etbk{font-size:.8rem;color:var(--t1);font-weight:600}
.etfo{font-size:.78rem;color:var(--t2)}
.wrc{display:flex;align-items:center;gap:7px}
.wrt{flex:1;height:3px;background:var(--b1);border-radius:2px;overflow:hidden;min-width:55px;position:relative}
.wrt::after{content:'';position:absolute;left:50%;top:0;bottom:0;width:1px;background:var(--b2)}
.wrf{height:100%;border-radius:2px;transition:width .5s ease}
.wrn{font-size:.76rem;font-weight:600;min-width:40px;text-align:right}
.etpf{font-size:.76rem;color:var(--t2)}
.etpnl{font-size:.78rem;font-weight:700;text-align:right}

/* UTILS */
.g{color:var(--g)}.r{color:var(--r)}.y{color:var(--y)}.d{color:var(--t2)}.dm{color:var(--t3)}

@media(max-width:1240px){
  .mbar{grid-template-columns:repeat(2,minmax(220px,1fr))}
  .pgrid{grid-template-columns:repeat(auto-fill,minmax(320px,1fr))}
}
@media(max-width:860px){
  .H{padding:0 14px}
  .W{padding:14px}
  .mbar{grid-template-columns:1fr}
  .pgrid{grid-template-columns:1fr}
}
</style>
</head>
<body>
<div class="H">
  <div class="Hl">
    <div class="logo"><div class="ld"></div>Clawd<b>Bot</b></div>
    <span class="tbadge" id="ts">--:--:--</span>
    <span class="drybadge" id="dry">Dry Run</span>
  </div>
  <div class="Hr" id="hstats"></div>
</div>
<div class="W">
  <div class="pc-row" id="prices"></div>
  <div class="mbar" id="mbar"></div>
  <div>
    <div class="sh"><span class="st" id="pos-title">Open Positions</span></div>
    <div class="pgrid" id="positions"></div>
  </div>
  <div class="bot">
    <div class="lpanel" id="lpanel"></div>
    <div id="execq"></div>
  </div>
</div>

<script>
const ch={},cm={};
const _mid404=new Set();
let _midSeries={},_posMeta={};
function fmt(n,d=2){return Number(n).toLocaleString('en-US',{minimumFractionDigits:d,maximumFractionDigits:d})}
function fmtT(ts){const x=new Date(ts*1e3);return x.toLocaleTimeString('en-US',{hour:'2-digit',minute:'2-digit',second:'2-digit',hour12:false})}
function pfx(v){return v>0?'+':''}
function pnl(v){return(v>=0?'+':'-')+'$'+fmt(Math.abs(v))}
function pdec(p){if(p<=0)return 5;if(p<0.1)return 6;if(p<1)return 5;if(p<10)return 4;if(p<100)return 3;return 2}
function fmtETRange(sTs,eTs){
  try{
    const opt={hour:'numeric',minute:'2-digit',hour12:true,timeZone:'America/New_York'};
    const s=new Date((sTs||0)*1e3).toLocaleTimeString('en-US',opt);
    const e=new Date((eTs||0)*1e3).toLocaleTimeString('en-US',opt);
    return `${s}-${e} ET`;
  }catch(e){return '';}
}
function countdown(minsLeft){
  const t=Math.max(0,Math.floor((minsLeft||0)*60));
  const m=Math.floor(t/60), s=t%60;
  return {m:String(m).padStart(2,'0'),s:String(s).padStart(2,'0')};
}
function countdownSec(secLeft){
  const t=Math.max(0,Math.floor(secLeft||0));
  const m=Math.floor(t/60), s=t%60;
  return {m:String(m).padStart(2,'0'),s:String(s).padStart(2,'0')};
}
function tickTime(){
  const now=Math.floor(Date.now()/1000);
  for(const [uid,pm] of Object.entries(_posMeta||{})){
    const durSec=Math.max(60,Math.round((pm.duration||15)*60));
    const secLeft=Math.max(0,Math.round((pm.end_ts||now)-now));
    const pct=Math.min(100,Math.max(0,((durSec-secLeft)/durSec)*100));
    const cd=countdownSec(secLeft);
    const tEl=document.getElementById('tm'+uid);
    const pEl=document.getElementById('pb'+uid);
    if(tEl)tEl.innerHTML=`${cd.m}<small>m</small> ${cd.s}<small>s</small>`;
    if(pEl)pEl.style.width=`${pct}%`;
  }
}

function renderHeader(d){
  document.getElementById('ts').textContent=d.ts;
  if(d.dry_run)document.getElementById('dry').style.display='';
  const p=d.pnl,dp=+(d.daily_pnl_total||0),wr=d.wr;
  document.getElementById('hstats').innerHTML=[
    ['P&L',pnl(p),p>=0?'g':'r'],
    ['Today',pnl(dp),dp>=0?'g':'r'],
    ['WR',wr+'%',wr>=52?'g':wr>=48?'y':'r'],
    ['Trades',''+d.trades,'d'],
    ['Equity','$'+fmt(d.total_equity),''],
    ['RTDS',d.rtds_ok?'●':'○',d.rtds_ok?'g':'r'],
  ].map(([l,v,c])=>
    `<div class="hs"><div class="hsl">${l}</div><div class="hsv ${c}">${v}</div></div>`
  ).join('');
}

function renderPrices(d){
  document.getElementById('prices').innerHTML=
    Object.entries(d.prices).map(([a,p])=>
      `<div class="pc"><span class="a">${a}</span><span class="v">$${fmt(p,2)}</span></div>`
    ).join('');
}

function renderMetrics(d){
  const p=d.pnl,pc=p>=0?'g':'r';
  const dp=+(d.daily_pnl_total||0),dpc=dp>=0?'g':'r';
  const wr=d.wr,wrc=wr>=52?'g':wr>=48?'y':'r';
  const dwr=+(d.daily_wr||0),dwrc=dwr>=52?'g':dwr>=48?'y':'r';
  const l=Math.max(0,(d.trades||0)-(d.wins||0));
  const dl=Math.max(0,(d.daily_outcomes||0)-(d.daily_wins||0));
  document.getElementById('mbar').innerHTML=[
    ['Portfolio','$'+fmt(d.total_equity),'Free <b>$'+fmt(d.usdc)+'</b> · Open <b>'+d.open_count+'</b>'],
    ['Session P&L','<span class="'+pc+'">'+pnl(p)+'</span>','ROI <b class="'+pc+'">'+pfx(d.roi)+d.roi.toFixed(1)+'%</b>'],
    ['Today','<span class="'+dpc+'">'+pnl(dp)+'</span>',(d.daily_wins||0)+'W · '+dl+'L · <b>'+(d.daily_outcomes||0)+'</b>'],
    ['Win Rate','<span class="'+wrc+'">'+wr+'%</span>',d.wins+'W / '+l+'L of <b>'+d.trades+'</b>'],
    ['Today WR','<span class="'+dwrc+'">'+dwr.toFixed(1)+'%</span>','<b>'+(d.daily_day||'')+'</b>'],
    ['Open Stake','$'+fmt(d.open_stake),'Mark <b>$'+fmt(d.open_mark)+'</b>'],
  ].map(([l,v,s])=>
    `<div class="mi"><div class="mi-l">${l}</div><div class="mi-v">${v}</div><div class="mi-s">${s}</div></div>`
  ).join('');
}

function drawSparkline(canvas,wp,openP,lead){
  const rect=canvas.getBoundingClientRect();
  const cssW=Math.max(16,Math.floor(rect.width||canvas.clientWidth||320));
  const cssH=Math.max(16,Math.floor(rect.height||canvas.clientHeight||120));
  const dpr=Math.max(1,window.devicePixelRatio||1);
  canvas.width=Math.floor(cssW*dpr);
  canvas.height=Math.floor(cssH*dpr);
  const g=canvas.getContext('2d');
  if(!g)return;
  g.setTransform(1,0,0,1,0,0);
  g.scale(dpr,dpr);
  g.clearRect(0,0,cssW,cssH);

  const line=lead===null?'#3a3a5a':(lead?'#00cc78':'#ff3d3d');
  const fill=lead===null?'rgba(58,58,90,.07)':(lead?'rgba(0,204,120,.09)':'rgba(255,61,61,.09)');
  const minP=Math.min(...wp.map(x=>x.p));
  const maxP=Math.max(...wp.map(x=>x.p));
  const span=Math.max(1e-6,maxP-minP);
  const padX=6,padY=6;
  const w=Math.max(8,cssW-padX*2),h=Math.max(8,cssH-padY*2);
  const pts=wp.map((x,i)=>{
    const xx=padX+(i/(Math.max(1,wp.length-1)))*w;
    const yy=padY+(1-(x.p-minP)/span)*h;
    return [xx,yy];
  });

  const fg=g.createLinearGradient(0,padY,0,padY+h);
  fg.addColorStop(0,fill);fg.addColorStop(1,'rgba(0,0,0,0)');
  g.beginPath();
  pts.forEach(([x,y],i)=>{if(i===0)g.moveTo(x,y);else g.lineTo(x,y);});
  g.lineTo(padX+w,padY+h);g.lineTo(padX,padY+h);g.closePath();
  g.fillStyle=fg;g.fill();

  if(openP>0){
    const oy=padY+(1-(openP-minP)/span)*h;
    g.setLineDash([3,4]);g.lineWidth=1;g.strokeStyle='rgba(255,255,255,.1)';
    g.beginPath();g.moveTo(padX,oy);g.lineTo(padX+w,oy);g.stroke();g.setLineDash([]);
  }

  g.lineWidth=2.0;g.strokeStyle=line;
  g.beginPath();
  pts.forEach(([x,y],i)=>{if(i===0)g.moveTo(x,y);else g.lineTo(x,y);});
  g.stroke();
  const lp=pts[pts.length-1];
  if(lp){
    g.beginPath();
    g.fillStyle=line;
    g.arc(lp[0],lp[1],2.2,0,Math.PI*2);
    g.fill();
  }
}

function drawChart(id,pts,openP,curP,sTs,eTs,now){
  const ctx=document.getElementById(id);if(!ctx)return;
  const _w=Math.floor(ctx.getBoundingClientRect().width||ctx.clientWidth||0);
  if(_w<=8){requestAnimationFrame(()=>drawChart(id,pts,openP,curP,sTs,eTs,now));return;}
  let wp=(pts||[]).filter(x=>x.t>=sTs-5);
  if(!wp.length){
    const op=(typeof openP==='number'&&openP>0)?openP:0.5;
    const cp=(typeof curP==='number'&&curP>0)?curP:op;
    const t0=Math.max((sTs||now)-4,0);
    const t1=Math.max((sTs||now)-1,0);
    const t2=Math.max(now, t1+1);
    wp=[{t:t0,p:op},{t:t1,p:(op+cp)/2},{t:t2,p:cp}];
  }else if(wp.length===1){
    const p0=wp[0].p;
    wp=[{t:Math.max(wp[0].t-1,0),p:p0},{t:wp[0].t,p:p0}];
  }
  const avg=wp.reduce((s,x)=>s+x.p,0)/wp.length;
  const dec=pdec(avg);
  const labels=wp.map(x=>fmtT(x.t));
  const data=wp.map(x=>x.p);
  const dMin=Math.min(...data);
  const dMax=Math.max(...data);
  const dMid=(dMin+dMax)/2;
  let span=Math.max(1e-9,dMax-dMin);
  // Keep y-range tight to expose micro-movements.
  if(span<Math.max(Math.abs(dMid)*0.000004,1e-6))span=Math.max(Math.abs(dMid)*0.000004,1e-6);
  const pad=span*0.18;
  const yMin=dMid-(span/2)-pad;
  const yMax=dMid+(span/2)+pad;
  const last=data[data.length-1];
  const lead=openP>0?last>=openP:null;
  const lc=lead===null?'#3a3a5a':(lead?'#00cc78':'#ff3d3d');
  const fg=lead===null?'rgba(58,58,90,.07)':(lead?'rgba(0,204,120,.09)':'rgba(255,61,61,.09)');
  if(typeof Chart==='undefined'){
    if(ch[id]){try{ch[id].destroy()}catch(e){}delete ch[id];}
    drawSparkline(ctx,wp,openP,lead);
    return;
  }
  if(ch[id]&&cm[id]&&cm[id].lead===lead){
    try{
      const c=ch[id];
      c.data.labels=labels;c.data.datasets[0].data=data;
      if(c.data.datasets[1])c.data.datasets[1].data=wp.map(()=>openP);
      if(c.options&&c.options.scales&&c.options.scales.y){
    c.options.scales.y.min=yMin;
    c.options.scales.y.max=yMax;
      }
      c.update('none');return;
    }catch(e){
      if(ch[id]){try{ch[id].destroy()}catch(e2){}delete ch[id];}
      drawSparkline(ctx,wp,openP,lead);
      return;
    }
  }
  if(ch[id]){ch[id].destroy();delete ch[id];}
  cm[id]={lead};
  const ds=[{
    data,borderColor:lc,borderWidth:1.8,pointRadius:0,pointHoverRadius:2,
    tension:0.08,fill:'origin',
    backgroundColor(cx){
      const g=cx.chart.ctx.createLinearGradient(0,0,0,120);
      g.addColorStop(0,fg);g.addColorStop(1,'rgba(0,0,0,0)');return g;
    }
  }];
  if(openP>0)ds.push({data:wp.map(()=>openP),borderColor:'rgba(255,255,255,.1)',borderWidth:1,borderDash:[3,4],pointRadius:0,fill:false,tension:0});
  try{
    ch[id]=new Chart(ctx,{type:'line',data:{labels,datasets:ds},options:{
      animation:false,responsive:true,maintainAspectRatio:false,
      interaction:{mode:'nearest',intersect:false},
      plugins:{legend:{display:false},tooltip:{mode:'index',intersect:false,
    displayColors:false,
    backgroundColor:'rgba(12,15,21,.96)',borderColor:'rgba(120,136,168,.28)',borderWidth:1,
    titleColor:'#8e9ab4',bodyColor:'#eef3ff',padding:7,
    callbacks:{label:c=>'$'+fmt(c.raw,dec)}}},
      scales:{
    x:{display:false,grid:{display:false},border:{display:false}},
    y:{display:false,grid:{display:false},border:{display:false},
      min:yMin,max:yMax,
      ticks:{callback:v=>'$'+fmt(v,dec)}}
      }
    }});
  }catch(e){
    if(ch[id]){try{ch[id].destroy()}catch(e2){}delete ch[id];}
    drawSparkline(ctx,wp,openP,lead);
  }
}

function renderPositions(d){
  const now=d.now_ts;
  document.getElementById('pos-title').textContent='Open Positions'+(d.positions.length?' · '+d.positions.length:'');
  const el=document.getElementById('positions');
  // Rebuilding cards replaces canvas nodes: drop stale chart instances first.
  Object.keys(ch).forEach(k=>{try{if(ch[k])ch[k].destroy()}catch(e){} delete ch[k];});
  Object.keys(cm).forEach(k=>{delete cm[k];});
  if(!d.positions.length){el.innerHTML='';return;}
  _posMeta={};
  el.innerHTML=d.positions.map((p,idx)=>{
    const uid=((p.cid_full||p.cid||'x').replace(/[^a-zA-Z0-9]/g,'').slice(-20)||'x')+'_'+idx;
    const cid='c'+uid;
    const durSec=Math.max(60,Math.round((p.duration||15)*60));
    const secLeftRaw=Math.max(0,Math.round((p.end_ts||now)-now));
    const secLeft=Math.min(durSec,secLeftRaw);
    const pct=Math.min(100,Math.max(0,((durSec-secLeft)/durSec)*100));
    const cls=p.lead===null?'':p.lead?' lead':' trail';
    const sideH=p.side==='Up'?'<span class="pup">UP ▲</span>':'<span class="pdn">DOWN ▼</span>';
    const bH=p.lead===null?'<span class="bstate na">Unknown</span>':p.lead?'<span class="bstate win">Winning</span>':'<span class="bstate lose">Losing</span>';
    const srcH=p.src==='fallback'?'<span class="bunk">FALLBACK</span>':'';
    const scoreH=p.score!=null?`<span class="stag">${p.score}</span>`:'';
    const mc=p.move_pct>=0?'g':'r';
    const bc=p.lead===null?'#303050':(p.lead?'var(--g)':'var(--r)');
    const cd=countdownSec(secLeft);
    const hasBeat=(p.open_p||0)>0;
    const delta=hasBeat?((p.cur_p||0)-(p.open_p||0)):0;
    const dc=delta>=0?'g':'r';
    const openTxt=hasBeat?('$'+fmt(p.open_p,pdec(p.open_p))):'N/A';
    const curTxt=(p.cur_p||0)>0?('$'+fmt(p.cur_p,pdec(p.cur_p))):'N/A';
    const deltaTxt=hasBeat?`${pfx(delta)}${fmt(Math.abs(delta),pdec(Math.abs(delta)||0.01))}`:'N/A';
    return `<div class="pcard${cls}"><div class="ph"><div class="phl">
  <span class="psym">${p.asset} Up or Down - ${p.duration||15} Minutes</span>
  <span class="pevt">${fmtETRange(p.start_ts,p.end_ts)}</span>
  <span class="pdur">${p.duration||15}m</span>${sideH}</div>
<div class="phr">${bH}${srcH}${scoreH}</div></div>
<div class="pdata">
<div class="di"><div class="dl">Price to beat</div><div class="dv">${openTxt}</div></div>
<div class="di"><div class="dl">Current price</div><div class="dv">${curTxt} <span class="${hasBeat?dc:'dm'}" style="font-size:.8rem">${deltaTxt}</span></div><div class="dv d" style="font-size:.82rem" id="m${uid}">—</div></div>
<div class="pcount"><div class="dl">Time left</div><div class="pcv" id="tm${uid}">${cd.m}<small>m</small> ${cd.s}<small>s</small></div></div>
</div>
<div class="ca"><canvas id="${cid}" height="120"></canvas></div>
<div class="pf"><span class="lbl">Stake <b>$${fmt(p.stake)}</b> · Move <b class="${mc}">${pfx(p.move_pct)}${p.move_pct.toFixed(2)}%</b></span>
<span class="rk">${p.rk||''}</span></div>
<div class="tb"><div class="tbf" id="pb${uid}" style="width:${pct}%;background:${bc}"></div></div>
</div>`;
  }).join('');
  d.positions.forEach((p,idx)=>{
    const uid=((p.cid_full||p.cid||'x').replace(/[^a-zA-Z0-9]/g,'').slice(-20)||'x')+'_'+idx;
    _posMeta[uid]={open_p:p.open_p,cur_p:p.cur_p,start_ts:p.start_ts,end_ts:p.end_ts,duration:p.duration||15};
    const basePts=d.charts[p.asset]||[];
    drawChart('c'+uid,basePts,p.open_p,p.cur_p,p.start_ts,p.end_ts,now);
  });
  _mt={};d.positions.forEach((p,idx)=>{
    const uid=((p.cid_full||p.cid||'x').replace(/[^a-zA-Z0-9]/g,'').slice(-20)||'x')+'_'+idx;
    const tid=String(p.token_id||'').trim();
    if(tid && !_mid404.has(tid))_mt[uid]=tid;
  });
  tickTime();
  pollMid();
}

function renderLeft(d){
  let h='';
  if(d.active_gates&&d.active_gates.length){
    h+=`<div class="card"><div class="ch">Active Gates</div><div class="gates">`+
      d.active_gates.map(g=>`<span class="gtag">${g}</span>`).join('')+`</div></div>`;
  }
  const sk=d.skip_top||[];
  if(!sk.length){
    h+=`<div class="card"><div class="ch">Skip Reasons</div><div class="ce">None in 15m</div></div>`;
  }else{
    h+=`<div class="card"><div class="ch">Skip Reasons · 15m</div>`+
      sk.map(s=>`<div class="skrow"><span class="skr">${s.reason}</span><span class="skc">${s.count}</span></div>`).join('')+`</div>`;
  }
  document.getElementById('lpanel').innerHTML=h;
}

function renderExecQ(rows){
  const el=document.getElementById('execq');
  if(!rows||!rows.length){el.innerHTML='';return;}
  const tot=rows.reduce((s,r)=>s+r.pnl,0);
  const trs=rows.map(r=>{
    const wr=r.wr;
    const wc=wr===null?'dm':wr>=52?'g':wr>=48?'y':'r';
    const bw=wr===null?0:Math.min(100,wr);
    const bc=wr===null?'#242438':wr>=52?'var(--g)':wr>=48?'var(--y)':'var(--r)';
    return `<tr>
<td class="etbk">${r.bucket}</td>
<td class="etfo">${r.fills}/${r.outcomes}</td>
<td><div class="wrc"><div class="wrt"><div class="wrf" style="width:${bw}%;background:${bc}"></div></div><span class="wrn ${wc}">${wr===null?'—':wr+'%'}</span></div></td>
<td class="etpf">${r.pf===null?'<span class="dm">—</span>':r.pf.toFixed(2)}</td>
<td class="etpnl"><span class="${r.pnl>=0?'g':'r'}">${pnl(r.pnl)}</span></td>
</tr>`;
  }).join('');
  el.innerHTML=`<div class="etw"><div class="eth">
  <span class="etl">ExecQ — Active Buckets</span>
  <span class="ett ${tot>=0?'g':'r'}">${pnl(tot)}</span>
</div>
<table class="et">
<thead><tr><th>Bucket</th><th>F/O</th><th>Win Rate</th><th>PF</th><th>PnL</th></tr></thead>
<tbody>${trs}</tbody>
</table></div>`;
}

async function refresh(){
  try{
    const d=await fetch('/api?t='+Date.now(),{cache:'no-store'}).then(r=>r.json());
    renderHeader(d);renderPrices(d);renderMetrics(d);
    renderPositions(d);renderLeft(d);renderExecQ(d.execq_all);
  }catch(e){console.warn(e);}
}

let _mt={};
async function pollMid(){
  for(const [cid,tid] of Object.entries(_mt)){
    if(!tid)continue;
    try{
      const r=await fetch('https://clob.polymarket.com/midpoint?token_id='+tid);
      const el=document.getElementById('m'+cid);
      if(r.status===404){
    // Invalid or stale token id for this market side; stop polling this row.
    _mid404.add(String(tid));
    if(el){el.textContent='n/a';el.className='dv dm';}
    delete _mt[cid];
    continue;
      }
      if(!r.ok)continue;
      const j=await r.json();
      if(el){
    if(j&&j.mid!=null){
      const v=parseFloat(j.mid);
      if(Number.isFinite(v)){
        el.textContent=v.toFixed(3);
        el.className='dv '+(v>=0.5?'g':'r');
        const pm=_posMeta[cid];
        // Keep midpoint as metric text only; chart uses underlying asset price series.
        if(pm && Number(pm.open_p)<=2){
          const now=Math.floor(Date.now()/1000);
          const s=_midSeries[cid]||[];
          s.push({t:now,p:v});
          if(s.length>320)s.splice(0,s.length-320);
          _midSeries[cid]=s;
        }
      }
    }else{
      el.textContent='—';
      el.className='dv d';
    }
      }
    }catch(e){}
  }
}

refresh();
setInterval(refresh,5000);
setInterval(tickTime,1000);
setInterval(pollMid,2000);
</script>
</body></html>"""

    _NO_CACHE_HEADERS = {
        "Access-Control-Allow-Origin": "*",
        "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
        "Pragma": "no-cache",
        "Expires": "0",
    }

    async def handle_html(request):
        return web.Response(
            text=HTML,
            content_type="text/html",
            headers=_NO_CACHE_HEADERS,
        )

    async def handle_api(request):
        try:
            data = self._dashboard_data()
        except Exception as e:
            data = {"error": str(e)}
        return web.Response(
            text=json.dumps(data),
            content_type="application/json",
            headers=_NO_CACHE_HEADERS,
        )

    async def handle_reload_buckets(request):
        try:
            self._bucket_stats.rows.clear()
            _bs_load(self._bucket_stats)
            n = sum(v.get("outcomes", 0) for v in self._bucket_stats.rows.values())
            return web.Response(
                text=json.dumps({"ok": True, "outcomes": n, "buckets": len(self._bucket_stats.rows)}),
                content_type="application/json",
                headers=_NO_CACHE_HEADERS,
            )
        except Exception as e:
            return web.Response(text=json.dumps({"ok": False, "error": str(e)}),
                                content_type="application/json")

    async def handle_analyze(request):
        """Detailed breakdown by score|entry_band|asset for a given date prefix."""
        try:
            date = request.rel_url.query.get("date", "")[:10]
            dur_filter = request.rel_url.query.get("dur", "")
            from collections import defaultdict as _dd
            rows = _dd(lambda: {"wins": 0, "outcomes": 0, "pnl": 0.0,
                                "gross_win": 0.0, "gross_loss": 0.0})
            for r in self._metrics_resolve_rows_cached(ttl_sec=15.0):
                if date and not str(r.get("ts", "")).startswith(date):
                    continue
                if dur_filter and str(r.get("duration", "15")) != dur_filter:
                    continue
                score = int(r.get("score") or 0)
                entry = float(r.get("entry_price") or 0)
                pnl   = float(r.get("pnl") or 0)
                won   = r.get("result") == "WIN"
                asset = str(r.get("asset", "?"))
                sc = "s12+" if score >= 12 else ("s9-11" if score >= 9 else "s0-8")
                if entry < 0.30:   eb = "<30c"
                elif entry < 0.51: eb = "30-50c"
                elif entry < 0.61: eb = "51-60c"
                elif entry < 0.71: eb = "61-70c"
                else:              eb = ">70c"
                by = request.rel_url.query.get("by", "bucket")
                if by == "asset":
                    k = asset
                elif by == "asset-score":
                    k = asset + "|" + sc
                else:
                    k = sc + "|" + eb
                rows[k]["outcomes"] += 1
                rows[k]["pnl"] += pnl
                if won:
                    rows[k]["wins"] += 1
                    rows[k]["gross_win"] += max(0.0, pnl)
                else:
                    rows[k]["gross_loss"] += max(0.0, -pnl)
            out = []
            for k, v in sorted(rows.items(), key=lambda x: x[1]["pnl"], reverse=True):
                n = v["outcomes"]
                wr = round(v["wins"] / n * 100, 1) if n else 0
                pf = round(v["gross_win"] / v["gross_loss"], 2) if v["gross_loss"] > 0 else None
                be = round(v["gross_loss"] / (v["gross_win"] + v["gross_loss"]) * 100, 1) if (v["gross_win"] + v["gross_loss"]) > 0 else None
                out.append({"bucket": k, "n": n, "wr": wr, "pf": pf,
                            "be_wr": be, "pnl": round(v["pnl"], 2)})
            return web.Response(text=json.dumps({"date": date, "rows": out}),
                                content_type="application/json",
                                headers=_NO_CACHE_HEADERS)
        except Exception as e:
            return web.Response(text=json.dumps({"error": str(e)}),
                                content_type="application/json")

    async def handle_daily(request):
        try:
            dur_filter = request.rel_url.query.get("dur", "")
            daily = {}
            for r in self._metrics_resolve_rows_cached(ttl_sec=15.0):
                if dur_filter and str(r.get("duration", "15")) != dur_filter:
                    continue
                day = str(r.get("ts", ""))[:10]
                pnl = float(r.get("pnl") or 0)
                won = r.get("result") == "WIN"
                if day not in daily:
                    daily[day] = {"wins": 0, "outcomes": 0, "pnl": 0.0}
                daily[day]["outcomes"] += 1
                daily[day]["pnl"] += pnl
                if won:
                    daily[day]["wins"] += 1
            out = []
            cumul = 0.0
            for day in sorted(daily):
                v = daily[day]
                wr = round(v["wins"] / v["outcomes"] * 100, 1) if v["outcomes"] else 0
                cumul += v["pnl"]
                out.append({"day": day, "n": v["outcomes"], "wr": wr,
                            "pnl": round(v["pnl"], 2), "cumul": round(cumul, 2)})
            return web.Response(text=json.dumps(out),
                                content_type="application/json",
                                headers=_NO_CACHE_HEADERS)
        except Exception as e:
            return web.Response(text=json.dumps({"error": str(e)}),
                                content_type="application/json")

    async def handle_dur_stats(request):
        """Duration breakdown (5m vs 15m) from metrics file."""
        try:
            from collections import defaultdict as _dd
            rows = _dd(lambda: {"wins": 0, "losses": 0, "pnl": 0.0,
                                "gross_win": 0.0, "gross_loss": 0.0})
            n5 = n15 = 0
            for r in self._metrics_resolve_rows_cached(ttl_sec=15.0):
                dur = int(r.get("duration") or 15)
                score = int(r.get("score") or 0)
                entry = float(r.get("entry_price") or 0)
                pnl = float(r.get("pnl") or 0)
                won = r.get("result") == "WIN"
                sc = "s12+" if score >= 12 else ("s9-11" if score >= 9 else "s0-8")
                if entry < 0.30:   eb = "<30c"
                elif entry < 0.51: eb = "30-50c"
                elif entry < 0.61: eb = "51-60c"
                elif entry < 0.71: eb = "61-70c"
                else:              eb = ">70c"
                k = f"{dur}m|{sc}|{eb}"
                rows[k]["wins" if won else "losses"] += 1
                rows[k]["pnl"] += pnl
                if won: rows[k]["gross_win"] += max(0.0, pnl)
                else:   rows[k]["gross_loss"] += max(0.0, -pnl)
                if dur == 5: n5 += 1
                else: n15 += 1
            out = []
            for k, v in sorted(rows.items(), key=lambda x: x[1]["pnl"], reverse=True):
                n = v["wins"] + v["losses"]
                wr = round(v["wins"] / n * 100, 1) if n else 0
                denom = v["gross_win"] + v["gross_loss"]
                be = round(v["gross_loss"] / denom * 100, 1) if denom > 0 else None
                pf = round(v["gross_win"] / v["gross_loss"], 2) if v["gross_loss"] > 0 else None
                out.append({"bucket": k, "n": n, "wr": wr, "pf": pf,
                            "be_wr": be, "pnl": round(v["pnl"], 2)})
            return web.Response(
                text=json.dumps({"n5m": n5, "n15m": n15, "rows": out}),
                content_type="application/json",
                headers=_NO_CACHE_HEADERS)
        except Exception as e:
            return web.Response(text=json.dumps({"error": str(e)}),
                                content_type="application/json")

    _CORR_CACHE_PATH = "/data/corr_cache.json"
    _corr_cache = {"source": "polymarket", "windows": 0, "rows": [], "built_at": ""}

    def _build_corr_cache_sync():
        """Fetch Polymarket historical data (15m + 5m) and compute direction correlation.
        Runs in a worker thread to avoid blocking the main asyncio event loop.
        """
        import urllib.request as _ur
        from collections import defaultdict as _dd
        from itertools import combinations
        # {asset: (series_id, duration_label)}
        _SERIES = {
            "BTC|15m": (10192, "15m"), "ETH|15m": (10191, "15m"),
            "SOL|15m": (10423, "15m"), "XRP|15m": (10422, "15m"),
            "BTC|5m":  (10684, "5m"),  "ETH|5m":  (10683, "5m"),
        }
        _HDRS = {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}
        # windows[slot][asset|dur] = direction
        windows = _dd(dict)
        counts = {}
        for key, (sid, dur) in _SERIES.items():
            asset = key.split("|")[0]
            offset = 0; total = 0
            while True:
                url = (f"https://gamma-api.polymarket.com/events"
                       f"?series_id={sid}&closed=true&limit=200&offset={offset}")
                req = _ur.Request(url, headers=_HDRS)
                try:
                    with _ur.urlopen(req, timeout=15) as resp:
                        events = json.loads(resp.read())
                except Exception:
                    break
                if not events:
                    break
                for ev in events:
                    for mkt in ev.get("markets", []):
                        end = str(ev.get("endDate", ""))[:16]
                        if not end:
                            continue
                        try:
                            prices   = json.loads(mkt["outcomePrices"]) if isinstance(mkt.get("outcomePrices"), str) else mkt.get("outcomePrices", [])
                            outcomes = json.loads(mkt["outcomes"])      if isinstance(mkt.get("outcomes"), str)      else mkt.get("outcomes", [])
                            p0, p1 = float(prices[0]), float(prices[1])
                        except Exception:
                            continue
                        if p0 > 0.95:   winner = outcomes[0]
                        elif p1 > 0.95: winner = outcomes[1]
                        else:           continue
                        slot = end + "|" + dur
                        wkey = asset + "|" + dur
                        if wkey not in windows[slot]:
                            windows[slot][wkey] = winner
                            total += 1
                offset += 200
                if len(events) < 200:
                    break
            counts[key] = total
        pairs = _dd(lambda: {"uu": 0, "dd": 0, "sp": 0, "n": 0})
        for slot, assets in windows.items():
            dur = slot.rsplit("|", 1)[-1]
            # only pair assets within same duration
            same_dur = [(k, v) for k, v in assets.items() if k.endswith("|" + dur)]
            for (a1, d1), (a2, d2) in combinations(same_dur, 2):
                a1n = a1.split("|")[0]; a2n = a2.split("|")[0]
                key = "|".join(sorted([a1n, a2n])) + "|" + dur
                pairs[key]["n"] += 1
                if d1 == "Up"   and d2 == "Up":   pairs[key]["uu"] += 1
                elif d1 == "Down" and d2 == "Down": pairs[key]["dd"] += 1
                else:                               pairs[key]["sp"] += 1
        out = []
        for k, v in sorted(pairs.items()):
            n = v["n"]
            if n == 0: continue
            same_pct = round((v["uu"] + v["dd"]) / n * 100, 1)
            out.append({"pair": k, "n": n, "up_up": v["uu"],
                        "down_down": v["dd"], "split": v["sp"],
                        "same_pct": same_pct})
        result = {"source": "polymarket", "windows": len(windows),
                  "rows": out, "built_at": datetime.now(timezone.utc).isoformat()}
        _corr_cache.update(result)
        try:
            with open(_CORR_CACHE_PATH, "w") as _f:
                json.dump(result, _f)
        except Exception:
            pass

    async def _build_corr_cache_async():
        # Offload blocking urllib/file work so trading loops stay responsive.
        await asyncio.to_thread(_build_corr_cache_sync)

    async def _corr_refresh_loop():
        """Rebuild correlation cache once at startup then every 6 hours."""
        try:
            with open(_CORR_CACHE_PATH) as _f:
                _corr_cache.update(json.load(_f))
        except Exception:
            pass
        # Let trading loops settle first; correlation cache is non-critical.
        await asyncio.sleep(2)
        while True:
            try:
                await asyncio.wait_for(_build_corr_cache_async(), timeout=120)
            except Exception:
                pass
            await asyncio.sleep(6 * 3600)

    async def handle_corr(request):
        """Token direction correlation from Polymarket historical data (cached)."""
        return web.Response(text=json.dumps(_corr_cache),
                            content_type="application/json",
                            headers=_NO_CACHE_HEADERS)

    app = web.Application()
    app.router.add_get("/", handle_html)
    app.router.add_get("/api", handle_api)
    app.router.add_get("/daily", handle_daily)
    app.router.add_get("/analyze", handle_analyze)
    app.router.add_get("/dur-stats", handle_dur_stats)
    app.router.add_get("/corr", handle_corr)
    app.router.add_get("/reload-buckets", handle_reload_buckets)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    print(f"[DASH] Dashboard running on http://0.0.0.0:{port}")
    asyncio.ensure_future(_corr_refresh_loop())
    while True:
        await asyncio.sleep(3600)

    # ── MAIN ──────────────────────────────────────────────────────────────────
