"""Demo visual do SalesFlow em localhost.

    python demo.py            # abre http://127.0.0.1:5000

Roda o pipeline REAL (mesmo process_block / Coordinator / workers HTTP do projeto) sobre
uma base sintética pequena e mostra, ao vivo: blocos, workers, falhas e o conferência
com o baseline. Cada bloco recebe uma carga artificial de CPU (ajustável) só para dar
tempo de enxergar o fluxo; por isso os tempos da demo NÃO são os do benchmark.
"""
import argparse
import os
import subprocess
import sys
import tempfile
import threading
import time
import webbrowser
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor

import uvicorn
from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from pydantic import BaseModel

from baseline import run_baseline
from common import merge, new_agg, process_block, split_blocks
from coordinator import Coordinator, DistributedError
from scripts.run_local_workers import start_workers, stop_workers

ROOT = os.path.dirname(os.path.abspath(__file__))
app = FastAPI()
LOCK = threading.RLock()
S = {"status": "idle"}   # estado lido pela página
CTX = {}                 # caminho dos dados, baseline, processos dos workers


def burn(ms):
    """Gasta `ms` de tempo de CPU da própria thread (não de relógio): threads sob o GIL
    disputam o interpretador e não ganham tempo, como no trabalho real."""
    end = time.thread_time() + ms / 1000
    while time.thread_time() < end:
        pass


def demo_block(path, start, end, delay_ms):
    """Executa em threads/processos do modo paralelo. Devolve (agregado, quem executou)."""
    burn(delay_ms)
    agg = process_block(path, start, end)
    return agg, f"{os.getpid()}-{threading.get_ident() % 1000}"


class RunReq(BaseModel):
    mode: str            # sequencial | thread | process | distribuido
    workers: int = 3
    blocks: int = 24
    delay_ms: int = 150
    kill_after_s: float = 0   # distribuído: derruba o worker 2 após N segundos (0 = não)


def set_block(bid, **kw):
    with LOCK:
        S["blocks"][bid].update(kw)


def log(msg):
    with LOCK:
        S["log"].append(f"{time.perf_counter() - S['t0']:6.2f}s  {msg}")


def finish(total, reassign=0, error=None):
    base = CTX["baseline"]
    with LOCK:
        S["elapsed"] = time.perf_counter() - S["t0"]
        if error:
            S["status"], S["error"] = "error", error
            log("ERRO: " + error)
        else:
            ok = total == base
            top = lambda d: sorted(d.items(), key=lambda kv: -kv[1])[:5]
            S["result"] = {"correto": ok, "reatribuicoes": reassign,
                           "valid": total["valid"], "invalid": total["invalid"], "ignored": total["ignored"],
                           "categorias": top(total["por_categoria"]), "marcas": top(total["por_marca"])}
            S["history"].append({"mode": S["mode_label"], "workers": S["n_workers"],
                                 "tempo": round(S["elapsed"], 2), "correto": ok, "reat": reassign})
            S["status"] = "done"
            log(f"concluído: total {'== baseline ✔' if ok else '!= baseline ✘'}")


def run_parallel_like(req, blocks, path):
    Pool = ThreadPoolExecutor if req.mode == "thread" else ProcessPoolExecutor
    total = new_agg()
    with Pool(max_workers=req.workers) as pool:
        futs = {pool.submit(demo_block, path, s, e, req.delay_ms): b for b, s, e in blocks}
        for b in futs.values():
            set_block(b, state="queued")
        from concurrent.futures import as_completed
        for fut in as_completed(futs):
            b = futs[fut]
            agg, who = fut.result()
            total = merge(total, agg)
            set_block(b, state="done", worker=who)
            log(f"bloco {b} concluído por {'thread' if req.mode == 'thread' else 'processo'} {who}")
    return total


def run_sequential(req, blocks, path):
    total = new_agg()
    for b, s, e in blocks:
        set_block(b, state="running", worker="único")
        burn(req.delay_ms)
        total = merge(total, process_block(path, s, e))
        set_block(b, state="done")
    return total


def run_distributed(req, blocks, path):
    extra = {"DELAY_MS": str(req.delay_ms), "DELAY_MODE": "cpu"}
    log(f"subindo {req.workers} workers HTTP…")
    procs, urls = start_workers(req.workers, path, extra_env=extra)
    with LOCK:
        CTX["procs"] = procs
        S["workers"] = [{"name": f"worker{i + 1}", "url": u, "state": "alive", "done": 0}
                        for i, u in enumerate(urls)]
    idx = {u: i for i, u in enumerate(urls)}
    S["t0"] = time.perf_counter()  # o tempo não inclui a subida dos workers

    def on_event(ev, **d):
        w = S["workers"][idx[d["worker"]]] if "worker" in d else None
        name = w["name"] if w else ""
        if ev == "dispatch":
            set_block(d["block"], state="running", worker=name)
            log(f"bloco {d['block']} → {name}")
        elif ev == "done":
            with LOCK:
                w["done"] += 1
            set_block(d["block"], state="done", worker=name)
            log(f"bloco {d['block']} ✔ {name}")
        elif ev == "retry":
            set_block(d["block"], state="retry", worker="")
            log(f"bloco {d['block']} falhou em {name}: volta para a fila (tentativa {d['attempt']})")
        elif ev == "suspect":
            with LOCK:
                if w["state"] != "down":
                    w["state"] = "suspect"
            log(f"{name} marcado como suspeito (checando /health)")
        elif ev == "recovered":
            with LOCK:
                if w["state"] != "down":
                    w["state"] = "alive"
                    log(f"{name} voltou")

    timer = None
    if req.kill_after_s > 0 and len(procs) > 1:
        timer = threading.Timer(req.kill_after_s, kill_worker, args=(1,))
        timer.start()
    c = Coordinator(urls, timeout=15, backoff=0.2, health_interval=0.3, stall_timeout=6, on_event=on_event)
    try:
        total, _ = c.run(path, CTX["file_size"] // req.blocks + 1)
        return total, c.reassignments
    finally:
        if timer:
            timer.cancel()
        stop_workers(procs)
        CTX["procs"] = []


def kill_worker(i):
    procs = CTX.get("procs") or []
    if i < len(procs) and procs[i].poll() is None:
        procs[i].kill()
        with LOCK:
            if i < len(S.get("workers", [])):
                S["workers"][i]["state"] = "down"
        log(f"💥 worker{i + 1} derrubado (kill)")


def run_job(req):
    path = CTX["path"]
    labels = {"sequencial": "Sequencial (blocos, 1 fio)", "thread": "Paralelo: threads",
              "process": "Paralelo: processos", "distribuido": "Distribuído (HTTP)"}
    size = CTX["file_size"] // req.blocks + 1
    blocks = split_blocks(path, size)
    S.update(status="running", mode=req.mode, mode_label=labels[req.mode], n_workers=req.workers if req.mode != "sequencial" else 1,
             blocks=[{"id": b, "state": "pending", "worker": ""} for b, _, _ in blocks], workers=[],
             log=[], result=None, error=None, elapsed=0, t0=time.perf_counter())
    reassign = 0
    try:
        if req.mode == "sequencial":
            total = run_sequential(req, blocks, path)
        elif req.mode in ("thread", "process"):
            total = run_parallel_like(req, blocks, path)
        else:
            total, reassign = run_distributed(req, blocks, path)
        finish(total, reassign)
    except DistributedError as e:
        finish(None, error=str(e))
    except Exception as e:  # noqa: BLE001 - mostra qualquer erro na página
        finish(None, error=f"{type(e).__name__}: {e}")


@app.post("/api/run")
def api_run(req: RunReq):
    if S.get("status") == "running":
        return {"ok": False, "msg": "já existe uma execução em andamento"}
    req.blocks = max(2, min(req.blocks, 200))
    threading.Thread(target=run_job, args=(req,), daemon=True).start()
    return {"ok": True}


@app.post("/api/kill/{i}")
def api_kill(i: int):
    kill_worker(i)
    return {"ok": True}


@app.get("/api/state")
def api_state():
    with LOCK:
        out = dict(S)
        if out.get("status") == "running":
            out["elapsed"] = time.perf_counter() - out["t0"]
        out["log"] = out.get("log", [])[-14:]
        out["info"] = CTX["info"]
        return out


@app.get("/", response_class=HTMLResponse)
def index():
    return PAGE


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rows", type=int, default=150_000, help="linhas da base sintética da demo")
    ap.add_argument("--port", type=int, default=5000)
    ap.add_argument("--no-browser", action="store_true")
    a = ap.parse_args()
    tmp = tempfile.mkdtemp(prefix="salesflow_demo_")
    path = os.path.join(tmp, "demo.csv")
    print(f"Gerando {a.rows:,} linhas sintéticas…")
    subprocess.run([sys.executable, os.path.join(ROOT, "scripts", "generate_synthetic.py"),
                    "--rows", str(a.rows), "--out", path], check=True, cwd=ROOT, stdout=subprocess.DEVNULL)
    base, t = run_baseline(path)
    CTX.update(path=path, baseline=base, file_size=os.path.getsize(path), procs=[],
               info={"linhas": a.rows, "mb": round(os.path.getsize(path) / 1048576, 1),
                     "valid": base["valid"], "invalid": base["invalid"], "ignored": base["ignored"],
                     "nucleos": os.cpu_count()})
    S["history"] = []
    url = f"http://127.0.0.1:{a.port}"
    print(f"Demo em {url}  (Ctrl+C para sair)")
    if not a.no_browser:
        threading.Timer(1.0, lambda: webbrowser.open(url)).start()
    try:
        uvicorn.run(app, host="127.0.0.1", port=a.port, log_level="warning")
    finally:
        stop_workers(CTX.get("procs") or [])


PAGE = r"""<!doctype html>
<html lang="pt-BR"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>SalesFlow · demo</title>
<style>
:root{--bg:#f6f7f9;--card:#fff;--fg:#1c2230;--mut:#6b7385;--line:#dfe3ea;--pend:#cfd5df;--queue:#e6c86a;--run:#3b82f6;--done:#22a06b;--retry:#f08a24;--down:#d64545;--acc:#3b5bdb}
@media(prefers-color-scheme:dark){:root{--bg:#14171d;--card:#1d222b;--fg:#e7eaf0;--mut:#8f98ab;--line:#2c3340;--pend:#3a4252;--queue:#b59a3d;--acc:#7c93ff}}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--fg);font:15px/1.45 system-ui,sans-serif}
main{max-width:1100px;margin:0 auto;padding:20px 16px 40px}
h1{font-size:22px;margin:0 0 2px}.sub{color:var(--mut);margin:0 0 16px}
.card{background:var(--card);border:1px solid var(--line);border-radius:12px;padding:16px;margin-bottom:14px}
.card h2{font-size:13px;text-transform:uppercase;letter-spacing:.06em;color:var(--mut);margin:0 0 12px}
.row{display:flex;flex-wrap:wrap;gap:14px;align-items:end}
label{display:flex;flex-direction:column;font-size:12px;color:var(--mut);gap:4px}
select,input{font:inherit;color:var(--fg);background:var(--bg);border:1px solid var(--line);border-radius:8px;padding:7px 9px}
button{font:inherit;border:0;border-radius:8px;padding:9px 16px;cursor:pointer;background:var(--acc);color:#fff;font-weight:600}
button.sec{background:var(--bg);color:var(--fg);border:1px solid var(--line)}button:disabled{opacity:.45;cursor:default}
.grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(46px,1fr));gap:6px}
.b{height:40px;border-radius:7px;background:var(--pend);color:#fff;font-size:11px;display:flex;flex-direction:column;align-items:center;justify-content:center;transition:background .2s}
.b small{opacity:.85;font-size:9px}.b.queued{background:var(--queue)}.b.running{background:var(--run);animation:p 1s infinite}
.b.done{background:var(--done)}.b.retry{background:var(--retry)}
@keyframes p{50%{opacity:.6}}
.legend{display:flex;gap:14px;flex-wrap:wrap;margin-top:10px;font-size:12px;color:var(--mut)}
.legend i{display:inline-block;width:11px;height:11px;border-radius:3px;margin-right:5px;vertical-align:-1px}
.workers{display:grid;grid-template-columns:repeat(auto-fill,minmax(200px,1fr));gap:10px}
.w{border:1px solid var(--line);border-radius:10px;padding:10px 12px}.w b{display:flex;justify-content:space-between}
.dot{display:inline-block;width:9px;height:9px;border-radius:50%;margin-right:6px}
.alive .dot{background:var(--done)}.suspect .dot{background:var(--retry)}.down .dot{background:var(--down)}
.w button{margin-top:8px;padding:5px 10px;font-size:12px;background:var(--down)}
.two{display:grid;grid-template-columns:1fr 1fr;gap:14px}@media(max-width:760px){.two{grid-template-columns:1fr}}
pre{margin:0;font:12px/1.5 ui-monospace,monospace;color:var(--mut);white-space:pre-wrap;min-height:120px}
table{width:100%;border-collapse:collapse;font-size:13px}td,th{text-align:left;padding:5px 6px;border-bottom:1px solid var(--line)}th{color:var(--mut);font-weight:500}
td.n,th.n{text-align:right;font-variant-numeric:tabular-nums}
.ok{color:var(--done);font-weight:700}.bad{color:var(--down);font-weight:700}
.kpi{display:flex;gap:26px;flex-wrap:wrap;margin-bottom:10px}.kpi div{font-size:12px;color:var(--mut)}.kpi b{display:block;font-size:22px;color:var(--fg)}
.note{font-size:12px;color:var(--mut);margin-top:8px}
</style></head><body><main>
<h1>SalesFlow · demo do fluxo</h1>
<p class="sub" id="info">carregando…</p>

<div class="card"><h2>Configurar execução</h2><div class="row">
<label>Modo<select id="mode">
<option value="sequencial">Sequencial (1 fio)</option><option value="thread">Paralelo: threads</option>
<option value="process">Paralelo: processos</option><option value="distribuido" selected>Distribuído (HTTP)</option></select></label>
<label>Workers<input id="workers" type="number" min="1" max="8" value="3" style="width:70px"></label>
<label>Blocos<input id="blocks" type="number" min="4" max="120" value="24" style="width:70px"></label>
<label>Carga por bloco (ms)<input id="delay" type="number" min="0" max="2000" value="150" style="width:90px"></label>
<label>Derrubar worker2 após (s)<input id="kill" type="number" min="0" step="0.5" value="0" style="width:90px"></label>
<button id="go">▶ Executar</button></div>
<p class="note">A carga de CPU por bloco é artificial, só para dar tempo de ver o fluxo. Compare os modos com a mesma carga e os mesmos blocos. No distribuído também dá para derrubar um worker clicando no botão dele durante a execução.</p></div>

<div class="card"><h2>Blocos <span id="prog" style="float:right;text-transform:none;letter-spacing:0"></span></h2>
<div class="grid" id="grid"><span class="note">Nenhuma execução ainda.</span></div>
<div class="legend"><span><i style="background:var(--pend)"></i>pendente</span><span><i style="background:var(--queue)"></i>na fila do pool</span>
<span><i style="background:var(--run)"></i>em execução</span><span><i style="background:var(--done)"></i>concluído</span><span><i style="background:var(--retry)"></i>reatribuído</span></div></div>

<div class="card" id="wcard" style="display:none"><h2>Workers</h2><div class="workers" id="workers_box"></div></div>

<div class="two">
<div class="card"><h2>Registro</h2><pre id="log"></pre></div>
<div class="card"><h2>Resultado</h2><div id="res"><span class="note">Aguardando execução.</span></div></div></div>

<div class="card"><h2>Execuções desta sessão</h2><table id="hist"><thead><tr><th>Modo</th><th class="n">Workers</th><th class="n">Tempo (s)</th><th class="n">Reatrib.</th><th>Total == baseline</th></tr></thead><tbody></tbody></table></div>
</main>
<script>
const $=id=>document.getElementById(id);
const brl=c=>(c/100).toLocaleString('pt-BR',{style:'currency',currency:'BRL'});
$('go').onclick=async()=>{
  const body={mode:$('mode').value,workers:+$('workers').value,blocks:+$('blocks').value,delay_ms:+$('delay').value,kill_after_s:+$('kill').value};
  const r=await (await fetch('/api/run',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)})).json();
  if(!r.ok) alert(r.msg);
};
const kill=i=>fetch('/api/kill/'+i,{method:'POST'});
function rows(list){return list.map(([k,v])=>`<tr><td>${k}</td><td class="n">${brl(v)}</td></tr>`).join('')}
async function tick(){
  let s; try{s=await (await fetch('/api/state')).json()}catch(e){return}
  const i=s.info; $('info').textContent=`Base sintética da demo: ${i.linhas.toLocaleString('pt-BR')} linhas · ${i.mb} MB · ${i.valid.toLocaleString('pt-BR')} compras válidas · ${i.nucleos} núcleos`;
  const running=s.status==='running'; $('go').disabled=running;
  if(s.blocks){
    $('grid').innerHTML=s.blocks.map(b=>`<div class="b ${b.state}" title="bloco ${b.id} ${b.worker||''}">${b.id}<small>${b.worker?b.worker.replace('worker','w'):''}</small></div>`).join('');
    const d=s.blocks.filter(b=>b.state==='done').length;
    $('prog').textContent=`${d}/${s.blocks.length} · ${s.elapsed.toFixed(1)} s`+(s.status==='done'?' ✔':'');
  }
  const ws=s.workers||[]; $('wcard').style.display=ws.length?'':'none';
  $('workers_box').innerHTML=ws.map((w,k)=>`<div class="w ${w.state}"><b><span><span class="dot"></span>${w.name}</span><span>${w.done} blocos</span></b>
   <span class="note">${{alive:'vivo',suspect:'suspeito',down:'fora do ar'}[w.state]} · ${w.url.replace('http://','')}</span><br>
   ${w.state!=='down'&&running?`<button onclick="kill(${k})">💥 derrubar</button>`:''}</div>`).join('');
  $('log').textContent=(s.log||[]).join('\n');
  if(s.error) $('res').innerHTML=`<p class="bad">Falhou de forma explícita: ${s.error}</p><p class="note">O coordenador nunca entrega um total incompleto.</p>`;
  else if(s.result){const r=s.result;
    $('res').innerHTML=`<div class="kpi"><div><b>${s.elapsed.toFixed(2)} s</b>tempo</div><div><b>${r.reatribuicoes}</b>reatribuições</div>
    <div><b class="${r.correto?'ok':'bad'}">${r.correto?'✔ igual':'✘ difere'}</b>total vs. baseline</div></div>
    <table><tr><th>Top categorias</th><th class="n">Receita</th></tr>${rows(r.categorias)}</table>
    <table style="margin-top:8px"><tr><th>Top marcas</th><th class="n">Receita</th></tr>${rows(r.marcas)}</table>
    <p class="note">válidas ${r.valid.toLocaleString('pt-BR')} · inválidas ${r.invalid.toLocaleString('pt-BR')} · ignoradas ${r.ignored.toLocaleString('pt-BR')}</p>`;}
  else if(running) $('res').innerHTML='<span class="note">Executando…</span>';
  $('hist').tBodies[0].innerHTML=(s.history||[]).map(h=>`<tr><td>${h.mode}</td><td class="n">${h.workers}</td><td class="n">${h.tempo.toFixed(2)}</td><td class="n">${h.reat}</td><td class="${h.correto?'ok':'bad'}">${h.correto?'sim':'NÃO'}</td></tr>`).join('');
}
setInterval(tick,250);tick();
</script></body></html>"""

if __name__ == "__main__":
    main()
