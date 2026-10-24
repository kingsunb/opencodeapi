#!/usr/bin/env python3
"""
oc_bridge.py —— 真实 opencode serve 池 + resin 出口切换，暴露 OpenAI 兼容端点。

链路：
  OpenAI 客户端 / NovaVeil
        |  POST /v1/chat/completions
        v
  oc_bridge (本文件, 0.0.0.0:8790, 内嵌账号体检页 GET /)
        |  POST /session , POST /session/{id}/message   (每账号一个 serve 实例)
        v
  opencode serve xN --HTTPS_PROXY--> resin (账号粘性出口 IP) --> opencode.ai Zen

提前判错：常驻订阅每实例 GET /event；错误部件事件先于 message 端点返回时，
立即分类并 interrupt 该轮，不再等 opencode 内部重试拖满几分钟。
端点返回路径保留为兜底。

403/429 切换与 NovaVeil opencode 代理池同口径：
冷却(默认 24h, 可配)落盘 -> 释放 resin 租约 -> 换号重试至多 3 次。
已知上游免费额度按出口 IP 且 UTC 日重置，页面提供手动解冻。

仅 stateless：每请求新建会话，整段历史重发。usage 取自上游真实 tokens。
"""
import argparse
import json
import os
import re
import shutil
import signal
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from collections import deque
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

MINIMAL_PROMPT = "You are a helpful assistant."
MODEL_UA = "opencode/1.19.0"
ZEN_MODELS_URL = "https://opencode.ai/zen/v1/models"
SWITCH_STATUS = {403, 429}

# 执行类工具全部拒绝，不影响请求里的工具定义形态
DENY_TOOLS = {
    "bash": "deny",
    "edit": "deny",
    "webfetch": "deny",
    "websearch": "deny",
    "task": "deny",
    "external_directory": "deny",
}

ARGS = None
ACCOUNTS = []
STARTED_AT = time.time()
SWITCH_COUNT = 0
REQ_LOGS = deque(maxlen=200)
REQ_LOGS_LOCK = threading.Lock()
_models_cache = {"ts": 0.0, "data": None}
_models_lock = threading.Lock()
_pool_lock = threading.Lock()
_egress_cache = {"ts": 0.0, "data": {}}


def log(*a):
    print(time.strftime("[%Y-%m-%d %H:%M:%S]"), *a, flush=True)


class Account:
    def __init__(self, name, port):
        self.name = name
        self.port = port
        self.base = f"http://127.0.0.1:{port}"
        self.proc = None
        self.cooldown_until = 0.0
        self.last_used = 0.0
        self.success_count = 0
        self.fail_429 = 0
        self.fail_403 = 0
        self.fail_other = 0
        self.last_success_ts = 0.0
        self.last_latency_ms = 0
        self.last_error = ""
        self.proxy_url = (
            f"http://{ARGS.platform}.{name}:{ARGS.proxy_token}@{ARGS.resin_hostport}"
        )

    def alive(self):
        return self.proc is not None and self.proc.poll() is None

    def cooled(self):
        return self.cooldown_until > time.time()

    def record_ok(self, ms):
        self.success_count += 1
        self.last_success_ts = time.time()
        self.last_latency_ms = ms

    def record_fail(self, status, detail=""):
        if status == 429:
            self.fail_429 += 1
        elif status == 403:
            self.fail_403 += 1
        else:
            self.fail_other += 1
        self.last_error = f"{status} {detail}".strip()[:160]

    def to_json(self):
        now = time.time()
        return {
            "name": self.name, "port": self.port, "alive": self.alive(),
            "cooled": self.cooled(),
            "cooldown_until": self.cooldown_until,
            "cooldown_remaining_s": max(0, int(self.cooldown_until - now)),
            "success_count": self.success_count,
            "fail_429": self.fail_429, "fail_403": self.fail_403,
            "fail_other": self.fail_other,
            "last_success_ts": self.last_success_ts,
            "last_latency_ms": self.last_latency_ms,
            "last_error": self.last_error,
            "last_used": self.last_used,
        }


# ---------- 本地 serve 调用 ----------
def oc(a, method, path, body=None, timeout=300):
    """返回 (status, parsed_json_or_raw_dict)。网络异常归一为 status=0。"""
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(
        a.base + path, data=data,
        headers={"Content-Type": "application/json"}, method=method)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            raw = r.read().decode(errors="replace")
            try:
                return r.status, json.loads(raw or "{}")
            except Exception:
                return r.status, {"_html": raw[:200]}
    except urllib.error.HTTPError as e:
        raw = e.read(4000).decode(errors="replace")
        try:
            return e.code, json.loads(raw)
        except Exception:
            return e.code, {"raw": raw}
    except Exception as e:
        return 0, {"raw": repr(e)}


def oc_bin():
    if ARGS.oc_bin:
        return ARGS.oc_bin
    for c in (shutil.which("opencode"), shutil.which("opencode.exe")):
        if c and os.path.exists(c):
            return c
    sys.exit("[bridge] 找不到 opencode 二进制")


def agent_config():
    agent = {"mode": "primary", "prompt": ARGS.prompt}
    if ARGS.deny_tools:
        agent["permission"] = DENY_TOOLS
    return {"agent": {ARGS.agent: agent}}


def start_account(a):
    env = dict(os.environ)
    home = os.path.join(ARGS.home_base, a.name)
    os.makedirs(home, exist_ok=True)
    env["HOME"] = home
    env["HTTP_PROXY"] = a.proxy_url
    env["HTTPS_PROXY"] = a.proxy_url
    env["NO_PROXY"] = "127.0.0.1,localhost"
    env["OPENCODE_CONFIG_CONTENT"] = json.dumps(agent_config(), ensure_ascii=False)
    a.proc = subprocess.Popen(
        [oc_bin(), "serve", "--port", str(a.port), "--hostname", "127.0.0.1"],
        env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def supervisor():
    while True:
        for a in ACCOUNTS:
            if not a.alive():
                log(f"[sup] {a.name} 进程退出，重拉")
                try:
                    start_account(a)
                except Exception as e:
                    log(f"[sup] {a.name} 重拉失败: {e!r}")
        time.sleep(5)


def wait_account(a, tries=90):
    for _ in range(tries):
        st, _d = oc(a, "GET", "/global/health", timeout=3)
        if st == 200:
            return True
        time.sleep(0.5)
    return False


# ---------- /event 订阅与提前判错 ----------
_waiters = {}
_waiters_lock = threading.Lock()


class Waiter:
    def __init__(self):
        self.ev = threading.Event()
        self.status = None
        self.detail = ""
        self.done = threading.Event()
        self.resp = None


def register_waiter(port, sid):
    w = Waiter()
    with _waiters_lock:
        _waiters[(port, sid)] = w
    return w


def resolve_waiter(port, sid, status, detail):
    with _waiters_lock:
        w = _waiters.pop((port, sid), None)
    if w:
        w.status = status
        w.detail = detail
        w.ev.set()


def unregister_waiter(port, sid):
    with _waiters_lock:
        _waiters.pop((port, sid), None)


def classify_error(obj):
    """错误部件/信息 -> {403,429} 或 None(泛化错误)。"""
    if not isinstance(obj, dict):
        return None, ""
    for k in ("status", "statusCode", "code"):
        v = obj.get(k)
        if isinstance(v, int) and v in SWITCH_STATUS:
            return v, str(obj.get("message") or obj.get("name") or "")[:120]
    s = json.dumps(obj, ensure_ascii=False)
    low = s.lower()
    if "free usage limit" in low or "rate limit" in low or "too many requests" in low:
        return 429, "free usage limit"
    if "forbidden" in low:
        return 403, "forbidden"
    m = re.search(r'"(?:status|statusCode)"\s*:\s*"(?:(403|429))"', s)
    if not m:
        m = re.search(r'"(?:status|statusCode)"\s*:\s*(403|429)', s)
    if m:
        return int(m.group(1)), "status field"
    return None, "generic error"


def classify_error_text(s):
    """日志错误文本 -> {403,429} 或 None。只认确定性错误，放过瞬态。"""
    low = (s or "").lower()
    if ("free usage limit" in low or "rate limit" in low
            or "too many requests" in low or "status code: 429" in low):
        return 429
    if "forbidden" in low or "status code: 403" in low:
        return 403
    return None


def log_watcher(a):
    """tail serve 日志：stream error 行 +0.5s 内可见，是额度类错误的最早信号。"""
    path = os.path.join(ARGS.home_base, a.name,
                        ".local/share/opencode/log/opencode.log")
    while not os.path.exists(path):
        time.sleep(1)
    f = open(path, errors="replace")
    f.seek(0, 2)
    while True:
        try:
            line = f.readline()
            if not line:
                try:
                    if os.path.getsize(path) < f.tell():
                        f.close()
                        f = open(path, errors="replace")
                except OSError:
                    pass
                time.sleep(0.3)
                continue
            if 'message="stream error"' not in line:
                continue
            m = re.search(r"session\.id=(ses_\S+)", line)
            if not m:
                continue
            em = re.search(r'error\.error="(.*)"', line)
            status = classify_error_text(em.group(1) if em else line)
            if status in SWITCH_STATUS:
                log(f"[log] {a.name} 日志先行判错 status={status}")
                resolve_waiter(a.port, m.group(1), status, "log: stream error")
        except Exception as e:
            log(f"[log] {a.name} watcher 异常: {e!r}")
            time.sleep(1)


def event_reader(a):
    """常驻订阅实例 /event，把错误部件事件分发给等待者。"""
    while True:
        try:
            req = urllib.request.Request(a.base + "/event")
            r = urllib.request.urlopen(req, timeout=None)
            log(f"[ev] {a.name} 事件流已连接")
            for raw in r:
                line = raw.decode(errors="replace").strip()
                if not line.startswith("data:"):
                    continue
                try:
                    ev = json.loads(line[5:].strip())
                except Exception:
                    continue
                props = ev.get("properties") or {}
                part = props.get("part")
                if isinstance(part, dict) and part.get("type") == "error":
                    sid = props.get("sessionID") or part.get("sessionID") or ""
                    status, detail = classify_error(part)
                    log(f"[ev] {a.name} 错误部件 sid={sid[:16]} status={status} {detail}")
                    resolve_waiter(a.port, sid, status or 599, detail)
        except Exception as e:
            log(f"[ev] {a.name} 事件流断开: {e!r}")
        time.sleep(2)


# ---------- 账号池与冷却落盘 ----------
def pick_account():
    with _pool_lock:
        cands = [a for a in ACCOUNTS if not a.cooled()]
        if not cands:
            return None
        cands.sort(key=lambda x: x.last_used)
        cands[0].last_used = time.time()
        return cands[0]


def cooldown_account(a, hours):
    with _pool_lock:
        a.cooldown_until = time.time() + hours * 3600
    save_state()
    log(f"[pool] {a.name} 冷却 {hours}h")


def unfreeze_account(a):
    with _pool_lock:
        a.cooldown_until = 0.0
    save_state()
    log(f"[pool] {a.name} 解冻")


def save_state():
    data = {a.name: a.cooldown_until for a in ACCOUNTS if a.cooldown_until > time.time()}
    try:
        os.makedirs(ARGS.state_dir, exist_ok=True)
        tmp = os.path.join(ARGS.state_dir, "cooldowns.json.tmp")
        with open(tmp, "w") as f:
            json.dump(data, f)
        os.replace(tmp, os.path.join(ARGS.state_dir, "cooldowns.json"))
    except Exception as e:
        log(f"[state] 保存失败: {e!r}")


def load_state():
    try:
        with open(os.path.join(ARGS.state_dir, "cooldowns.json")) as f:
            data = json.load(f)
        for a in ACCOUNTS:
            if a.name in data:
                a.cooldown_until = float(data[a.name])
                log(f"[state] {a.name} 恢复冷却")
    except Exception:
        pass


# ---------- resin 管理 API ----------
def resin_req(method, path):
    req = urllib.request.Request(
        ARGS.resin_url + path, method=method,
        headers={"Authorization": "Bearer " + ARGS.admin_token})
    with urllib.request.urlopen(req, timeout=10) as r:
        return r.status, json.loads(r.read().decode() or "{}")


_platform_id = None


def platform_id():
    global _platform_id
    if _platform_id is None:
        try:
            _st, d = resin_req("GET", "/api/v1/platforms")
            for p in d.get("items", []):
                if p.get("name") == ARGS.platform:
                    _platform_id = p["id"]
                    break
        except Exception as e:
            log(f"[resin] 平台查询失败: {e!r}")
        _platform_id = _platform_id or "unknown"
    return _platform_id


_RELEASE_PATHS = (
    "/api/v1/platforms/{pid}/accounts/{acct}/leases",
    "/api/v1/platforms/{pid}/leases/{acct}",
)


def release_lease(a):
    pid = platform_id()
    for tpl in _RELEASE_PATHS:
        path = tpl.format(pid=pid, acct=a.name)
        try:
            st, _d = resin_req("DELETE", path)
            if st < 300:
                log(f"[resin] 已释放租约 {a.name} via {path}")
                return True
        except urllib.error.HTTPError as e:
            if e.code != 404:
                log(f"[resin] 释放失败 {path}: HTTP {e.code}")
        except Exception as e:
            log(f"[resin] 释放异常 {path}: {e!r}")
    log(f"[resin] 释放路径全部失败，仅保留冷却")
    return False


def egress_info():
    """各账号最近一次经 resin 的出口信息，15s 缓存。"""
    now = time.time()
    if _egress_cache["data"] and now - _egress_cache["ts"] < 15:
        return _egress_cache["data"], True
    out = {}
    ok = True
    for a in ACCOUNTS:
        try:
            _st, d = resin_req(
                "GET", f"/api/v1/request-logs?account={a.name}&limit=1")
            items = d.get("items") or []
            if items:
                out[a.name] = {
                    "egress_ip": items[0].get("egress_ip") or "",
                    "node_tag": (items[0].get("node_tag") or "")[:40],
                    "ts": (items[0].get("ts") or "")[:19],
                }
        except Exception:
            ok = False
    _egress_cache.update(ts=now, data=out)
    return out, ok


# ---------- OpenAI <-> opencode 转换 ----------
def _text_of(content):
    if isinstance(content, list):
        return "".join(p.get("text", "") for p in content
                       if isinstance(p, dict) and p.get("type") in ("text", "input_text"))
    if isinstance(content, str):
        return content
    return "" if content is None else str(content)


def split_messages(messages):
    systems, lines = [], []
    label = {"assistant": "Assistant", "tool": "Tool"}
    for m in messages or []:
        text = _text_of(m.get("content"))
        role = m.get("role")
        if role == "system":
            if text:
                systems.append(text)
        elif role == "user":
            lines.append(text)
        else:
            lines.append(f"{label.get(role, role or 'User')}: {text}")
    return "\n\n".join(systems), "\n\n".join(lines)


def normalize_model(raw):
    model = str(raw or "").strip()
    model = model.split("/")[-1]
    if "(" in model:
        model = model.split("(")[0].strip()
    return model


def usage_of(info):
    """info.tokens -> OpenAI usage；无真实数据返回 None，绝不造假。"""
    if not isinstance(info, dict):
        return None
    t = info.get("tokens")
    if not isinstance(t, dict):
        return None
    prompt = int(t.get("input") or 0)
    comp = int(t.get("output") or 0) + int(t.get("reasoning") or 0)
    if prompt == 0 and comp == 0:
        return None
    return {"prompt_tokens": prompt, "completion_tokens": comp,
            "total_tokens": int(t.get("total") or (prompt + comp))}


def extract_text(d):
    parts = d.get("parts") or (d.get("info") or {}).get("parts") or []
    return "".join(p.get("text", "") for p in parts
                   if isinstance(p, dict) and p.get("type") == "text").strip()


def scan_error_parts(d):
    if not isinstance(d, dict):
        return None, ""
    for p in d.get("parts") or []:
        if isinstance(p, dict) and p.get("type") == "error":
            return classify_error(p)
    info = d.get("info")
    if isinstance(info, dict) and isinstance(info.get("error"), dict):
        return classify_error(info["error"])
    return None, ""


def interrupt_session(a, sid):
    for tpl in ("/session/{sid}/interrupt", "/api/session/{sid}/interrupt"):
        st, d = oc(a, "POST", tpl.format(sid=sid), {}, timeout=8)
        if st == 200 and isinstance(d, dict) and "_html" not in d:
            return True
    log(f"[ask] {a.name} interrupt 不可用 sid={sid[:16]}")
    return False


def ask(a, model, prompt, system=None):
    """返回 (text, usage, None) 或 (None, None, status)。"""
    st, d = oc(a, "POST", "/session", {"title": "bridge"}, timeout=15)
    if st != 200 or not isinstance(d, dict) or "id" not in d:
        log(f"[ask] {a.name} 建会话失败 status={st}")
        return None, None, 598
    sid = d["id"]
    body = {"model": {"providerID": "opencode", "modelID": model},
            "parts": [{"type": "text", "text": prompt or "."}],
            "agent": ARGS.agent}
    if system:
        body["system"] = system

    w = Waiter()
    with _waiters_lock:
        _waiters[(a.port, sid)] = w

    def poster():
        w.resp = oc(a, "POST", f"/session/{sid}/message", body,
                    timeout=ARGS.upstream_timeout)
        w.done.set()

    threading.Thread(target=poster, daemon=True).start()

    early = None
    while True:
        if w.ev.wait(timeout=1.0):
            early = (w.status, w.detail)
            break
        if w.done.is_set():
            break

    resp = w.resp
    if early is not None:
        status, detail = early
        log(f"[ask] {a.name} 事件先行判错 status={status}")
        threading.Thread(target=interrupt_session, args=(a, sid), daemon=True).start()
        return None, None, status
    unregister_waiter(a.port, sid)

    if resp is None:
        return None, None, 598
    st, d = resp
    if st == 0:
        log(f"[ask] {a.name} serve 连接失败: {d.get('raw')}")
        return None, None, 598
    if st in SWITCH_STATUS:
        return None, None, st
    if st != 200 or not isinstance(d, dict) or "_html" in d:
        status, _det = scan_error_parts(d) if isinstance(d, dict) else (None, "")
        if status is None:
            status = 502 if (st >= 500 or st == 0) else st
        return None, None, status
    status, detail = scan_error_parts(d)
    if status:
        log(f"[ask] {a.name} 响应含错误部件 status={status} {detail}")
        return None, None, status
    text = extract_text(d)
    if not text:
        log(f"[ask] {a.name} 空输出")
        return None, None, 599
    usage = usage_of(d.get("info"))
    return text, usage, None


def record_log(account, model, outcome, status, ms):
    with REQ_LOGS_LOCK:
        REQ_LOGS.appendleft({
            "ts": time.strftime("%m-%d %H:%M:%S"), "account": account,
            "model": model, "outcome": outcome, "status": status, "ms": ms,
        })


def chat_roundtrip(model, prompt, system, probe=False, account=None):
    """带换号重试的一轮完整调用。probe=True 或指定 account 时不换号。"""
    global SWITCH_COUNT
    prefix = "probe_" if probe else ""
    switches = 0
    while True:
        a = account or pick_account()
        if a is None:
            return 503, "所有账号均在冷却中", None
        t0 = time.time()
        text, usage, status = ask(a, model, prompt, system or None)
        ms = int((time.time() - t0) * 1000)
        if status is None:
            a.record_ok(ms)
            record_log(a.name, model, f"{prefix}ok", 200, ms)
            if probe:
                log(f"[probe] {a.name} 健康 {ms}ms")
            return 200, text, usage
        a.record_fail(status, "")
        outcome = {403: f"{prefix}403", 429: f"{prefix}429",
                   599: f"{prefix}empty"}.get(status, f"{prefix}failed")
        record_log(a.name, model, outcome, status, ms)
        log(f"[chat] {a.name} {model} 失败 status={status} {ms}ms{' (probe)' if probe else ''}")
        if status in SWITCH_STATUS:
            cooldown_account(a, ARGS.cooldown_hours)
            release_lease(a)
        if probe or status == 598:
            return status if status >= 400 else 502, f"upstream {status}", None
        switches += 1
        if switches > ARGS.max_switches:
            return 502, f"upstream {status} after {switches} attempts", None
        SWITCH_COUNT += 1


# ---------- 模型清单 ----------
def _proxied_fetch(a, url, timeout=15):
    opener = urllib.request.build_opener(
        urllib.request.ProxyHandler({"http": a.proxy_url, "https": a.proxy_url}))
    req = urllib.request.Request(url, headers={
        "User-Agent": MODEL_UA, "Authorization": "Bearer public"})
    with opener.open(req, timeout=timeout) as r:
        return json.loads(r.read().decode())


def upstream_models():
    with _models_lock:
        if _models_cache["data"] and time.time() - _models_cache["ts"] < 60:
            return _models_cache["data"]
    data, seen = [], set()

    def add(m):
        mid = m.get("id") if isinstance(m, dict) else m
        if mid and mid not in seen:
            seen.add(mid)
            data.append(m if isinstance(m, dict) else
                        {"id": mid, "object": "model", "created": int(time.time()),
                         "owned_by": "opencode"})

    a = pick_account()
    if a is not None:
        try:
            for m in (_proxied_fetch(a, ZEN_MODELS_URL) or {}).get("data") or []:
                add(m)
        except Exception as e:
            log(f"[models] zen 直取失败: {e!r}")
    for inst in ACCOUNTS:
        st, d = oc(inst, "GET", "/config/providers", timeout=20)
        if st == 200 and isinstance(d, dict):
            for prov in d.get("providers", []):
                for mid in (prov.get("models") or {}):
                    add(mid)
    if data:
        with _models_lock:
            _models_cache.update(ts=time.time(), data=data)
    return data


# ---------- 体检页面 ----------
_INDEX_HTML = """<!doctype html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>OpenCode 桥 · 账号体检</title>
<style>
:root{--bd:#e2e5ea;--mut:#6b7280;--ok:#177245;--warn:#b45309;--bad:#b91c1c;--bg:#f7f8fa}
*{box-sizing:border-box}
body{margin:0;font:14px/1.6 system-ui,"Segoe UI","Microsoft YaHei",sans-serif;background:var(--bg);color:#111}
header{display:flex;flex-wrap:wrap;align-items:baseline;gap:12px;padding:16px 20px;background:#fff;border-bottom:1px solid var(--bd)}
h1{font-size:18px;margin:0}
header .meta{color:var(--mut);font-size:12px}
main{padding:16px 20px;max-width:1200px;margin:0 auto}
.cards{display:flex;flex-wrap:wrap;gap:10px;margin-bottom:16px}
.card{background:#fff;border:1px solid var(--bd);border-radius:8px;padding:10px 16px;min-width:130px}
.card .v{font-size:20px;font-weight:600}
.card .k{font-size:12px;color:var(--mut)}
h2{font-size:15px;margin:22px 0 8px}
table{width:100%;border-collapse:collapse;background:#fff;border:1px solid var(--bd);border-radius:8px;overflow:hidden}
th,td{padding:7px 10px;border-bottom:1px solid var(--bd);text-align:left;font-size:13px;vertical-align:top}
th{background:#f0f2f5;font-weight:600;white-space:nowrap}
tr:last-child td{border-bottom:none}
td.num{text-align:right;font-variant-numeric:tabular-nums}
.b{display:inline-block;padding:1px 8px;border-radius:10px;font-size:12px;color:#fff;white-space:nowrap}
.b-ok{background:var(--ok)}.b-warn{background:var(--warn)}.b-bad{background:var(--bad)}.b-mut{background:#9ca3af}
button{font:inherit;padding:3px 10px;margin:1px;border:1px solid var(--bd);border-radius:6px;background:#fff;cursor:pointer}
button:hover{background:#eef1f4}
button.warn{color:var(--warn);border-color:var(--warn)}
button.danger{color:var(--bad);border-color:var(--bad)}
.mut{color:var(--mut);font-size:12px}
.toolbar{display:flex;flex-wrap:wrap;gap:8px;align-items:center;margin:8px 0}
.toolbar input{font:inherit;padding:4px 8px;border:1px solid var(--bd);border-radius:6px;width:220px}
.toast{position:fixed;right:16px;bottom:16px;background:#111;color:#fff;padding:8px 14px;border-radius:8px;opacity:0;transition:opacity .3s;pointer-events:none}
.toast.show{opacity:.9}
</style>
</head>
<body>
<header><h1>OpenCode 桥 · 账号体检</h1><div class="meta" id="meta"></div></header>
<main>
<div class="cards" id="cards"></div>
<div class="toolbar">
  <span>测试模型</span><input id="probeModel" placeholder="mimo-v2.6-flash-free">
  <button onclick="probeAll()">全部体检</button>
  <span class="mut" id="probeHint"></span>
</div>
<h2>账号台账</h2>
<table id="accounts"><thead><tr>
<th>账号</th><th>状态</th><th>出口 IP</th><th>节点</th><th class="num">成功</th>
<th class="num">429</th><th class="num">403</th><th class="num">其他</th>
<th>最近成功</th><th>最近延迟</th><th>最近错误</th><th>操作</th>
</tr></thead><tbody></tbody></table>
<h2>最近请求</h2>
<table id="logs"><thead><tr>
<th>时间</th><th>账号</th><th>模型</th><th>结论</th><th class="num">HTTP</th><th class="num">耗时</th>
</tr></thead><tbody></tbody></table>
<p class="mut">冷却默认 24 小时（可配 COOLDOWN_HOURS）；免费额度按出口 IP 且 UTC 日重置，被拒后可提前解冻。页面每 5 秒自动刷新。</p>
</main>
<div class="toast" id="toast"></div>
<script>
const $=s=>document.querySelector(s);
function esc(s){return String(s??"").replace(/[&<>"]/g,c=>({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;"}[c]))}
function badge(ok,txt,cls){return `<span class="b ${cls}">${esc(txt)}</span>`}
function hms(sec){if(sec<=0)return "";const h=Math.floor(sec/3600),m=Math.floor(sec%3600/60);return h?`${h}时${m}分`:`${m}分`}
function ts(t){return t?new Date(t*1000).toLocaleString("zh-CN",{hour12:false}):"—"}
function toast(msg){const t=$("#toast");t.textContent=msg;t.classList.add("show");setTimeout(()=>t.classList.remove("show"),2500)}
async function api(path,body){const r=await fetch(path,{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify(body||{})});return r.json()}
const OUTCOME={ok:["健康","b-ok"],probe_ok:["体检健康","b-ok"],probe_rejected:["体检被拒","b-warn"],probe_failed:["体检失败","b-bad"],"403":["被拒 403","b-warn"],"429":["限流 429","b-warn"],empty:["空输出","b-warn"],failed:["失败","b-bad"]};
async function refresh(){
 try{
  const s=await(await fetch("/api/status")).json();
  $("#meta").textContent=`resin ${s.resin_url} · 平台 ${s.platform} · ${s.models_count} 模型 · 运行 ${hms(s.uptime_s)||"0分"} · resin ${s.resin_ok?"连接正常":"不可达"}`;
  $("#probeModel").placeholder=s.probe_model;
  const c=s.summary;
  $("#cards").innerHTML=[
   ["可用账号",`${c.usable} / ${s.accounts.length}`],
   ["冷却中",c.cooling],["进程异常",c.down],
   ["累计换号",c.switches],["24h 成功",s.success_24h],["24h 失败",s.fail_24h]
  ].map(([k,v])=>`<div class="card"><div class="v">${esc(v)}</div><div class="k">${k}</div></div>`).join("");
  $("#accounts tbody").innerHTML=s.accounts.map(a=>{
   const eg=a.egress||{};
   let st;
   if(!a.alive)st=badge(0,"进程异常","b-bad");
   else if(a.cooled)st=badge(0,`冷却 ${hms(a.cooldown_remaining_s)||"<1分"}`,"b-warn");
   else st=badge(1,"可用","b-ok");
   const ops=[
    `<button onclick="probeOne('${esc(a.name)}')">体检</button>`,
    a.cooled?`<button onclick="unfreeze('${esc(a.name)}')">解冻</button>`:`<button class="warn" onclick="cool('${esc(a.name)}')">冷却24h</button>`,
    `<button class="danger" onclick="release('${esc(a.name)}')">释放租约</button>`
   ].join(" ");
   return `<tr><td>${esc(a.name)}</td><td>${st}</td><td>${esc(eg.egress_ip||"—")}</td><td class="mut">${esc(eg.node_tag||"—")}</td>
   <td class="num">${a.success_count}</td><td class="num">${a.fail_429}</td><td class="num">${a.fail_403}</td><td class="num">${a.fail_other}</td>
   <td>${a.last_success_ts?ts(a.last_success_ts):"—"}</td><td>${a.last_latency_ms?a.last_latency_ms+" ms":"—"}</td>
   <td class="mut">${esc(a.last_error||"—")}</td><td>${ops}</td></tr>`}).join("");
  const l=await(await fetch("/api/logs")).json();
  $("#logs tbody").innerHTML=(l.items||[]).slice(0,50).map(e=>{
   const o=OUTCOME[e.outcome]||[e.outcome,"b-mut"];
   return `<tr><td class="mut">${esc(e.ts)}</td><td>${esc(e.account)}</td><td>${esc(e.model)}</td>
   <td>${badge(0,o[0],o[1])}</td><td class="num">${esc(e.status)}</td><td class="num">${e.ms} ms</td></tr>`}).join("")
   ||'<tr><td colspan="6" class="mut">暂无请求</td></tr>';
 }catch(e){$("#meta").textContent="加载失败："+e}
}
async function probeOne(name){
 $("#probeHint").textContent=`${name} 体检中…`;
 try{
  const r=await api(`/api/accounts/${name}/probe`,{model:$("#probeModel").value.trim()||undefined});
  $("#probeHint").textContent=`${name}：${r.outcome==="probe_ok"?"健康":r.outcome==="probe_rejected"?"被拒("+r.status+")":"失败("+r.status+")"} ${r.ms} ms`;
 }catch(e){$("#probeHint").textContent=name+" 体检请求失败"}
 refresh();
}
function probeAll(){
 const names=[...document.querySelectorAll("#accounts tbody tr td:first-child")].map(x=>x.textContent);
 $("#probeHint").textContent=`并发体检 ${names.length} 个账号…`;
 names.forEach(n=>probeOne(n));
}
async function cool(name){await api(`/api/accounts/${name}/cooldown`,{minutes:1440});toast(name+" 已冷却 24h");refresh()}
async function unfreeze(name){await api(`/api/accounts/${name}/cooldown`,{minutes:0});toast(name+" 已解冻");refresh()}
async function release(name){await api(`/api/accounts/${name}/release`,{});toast(name+" 租约已释放");refresh()}
refresh();setInterval(refresh,5000);
</script>
</body>
</html>
"""


# ---------- HTTP 端点 ----------
class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *a):
        pass

    def _json(self, code, obj):
        b = json.dumps(obj, ensure_ascii=False).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Content-Length", str(len(b)))
        self.end_headers()
        self.wfile.write(b)

    def do_OPTIONS(self):
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Headers", "*")
        self.send_header("Access-Control-Allow-Methods", "POST, GET, OPTIONS")
        self.send_header("Content-Length", "0")
        self.end_headers()

    def do_GET(self):
        p = self.path.rstrip("/")
        if p in ("", "/ui", "/index.html"):
            b = _INDEX_HTML.encode()
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(b)))
            self.end_headers()
            self.wfile.write(b)
            return
        if p.endswith("/models"):
            self._json(200, {"object": "list", "data": upstream_models()})
        elif p == "/healthz" or "health" in p:
            now = time.time()
            self._json(200, {"ok": True, "accounts": [{
                "name": a.name, "alive": a.alive(), "cooled": a.cooled(),
                "cooldown_remaining_s": max(0, int(a.cooldown_until - now)),
            } for a in ACCOUNTS]})
        elif p == "/api/status":
            eg, resin_ok = egress_info()
            accounts = []
            for a in ACCOUNTS:
                j = a.to_json()
                j["egress"] = eg.get(a.name)
                accounts.append(j)
            with REQ_LOGS_LOCK:
                recent = list(REQ_LOGS)[:500]
            day = [e for e in recent if not e["outcome"].startswith("probe_")]
            self._json(200, {
                "ok": True,
                "started_at": STARTED_AT,
                "uptime_s": int(time.time() - STARTED_AT),
                "resin_url": ARGS.resin_url, "platform": ARGS.platform,
                "resin_ok": resin_ok,
                "models_count": len(upstream_models()),
                "probe_model": ARGS.probe_model,
                "accounts": accounts,
                "summary": {
                    "usable": sum(1 for a in ACCOUNTS if a.alive() and not a.cooled()),
                    "cooling": sum(1 for a in ACCOUNTS if a.cooled()),
                    "down": sum(1 for a in ACCOUNTS if not a.alive()),
                    "switches": SWITCH_COUNT,
                },
                "success_24h": sum(1 for e in day if e["outcome"] == "ok"),
                "fail_24h": sum(1 for e in day if e["outcome"] != "ok"),
            })
        elif p == "/api/logs":
            with REQ_LOGS_LOCK:
                items = list(REQ_LOGS)
            self._json(200, {"items": items})
        elif p == "/debug/state" and ARGS.debug:
            now = time.time()
            self._json(200, {"accounts": [{
                "name": a.name, "last_used": a.last_used,
                "cooldown_until": a.cooldown_until, "cooled": a.cooled(),
            } for a in ACCOUNTS], "platform_id": platform_id()})
        else:
            self._json(404, {"error": {"message": "not found"}})

    def do_POST(self):
        ln = int(self.headers.get("Content-Length") or 0)
        try:
            req = json.loads(self.rfile.read(ln) or b"{}")
        except Exception as e:
            return self._json(400, {"error": {"message": f"bad json: {e}"}})
        p = self.path.rstrip("/")
        if p.endswith("/chat/completions"):
            return self.handle_chat(req)
        if ARGS.debug and p == "/debug/cooldown":
            a = self.find_account(req.get("account"))
            if not a:
                return self._json(404, {"error": {"message": "no such account"}})
            cooldown_account(a, float(req.get("minutes") or 60) / 60.0)
            return self._json(200, {"ok": True})
        m = re.match(r"^/api/accounts/([^/]+)/(cooldown|release|probe)$", p)
        if m:
            a = self.find_account(m.group(1))
            if not a:
                return self._json(404, {"error": {"message": "no such account"}})
            action = m.group(2)
            if action == "cooldown":
                minutes = float(req.get("minutes") or 1440)
                if minutes <= 0:
                    unfreeze_account(a)
                else:
                    cooldown_account(a, minutes / 60.0)
                return self._json(200, {"ok": True, "cooldown_remaining_s":
                                        max(0, int(a.cooldown_until - time.time()))})
            if action == "release":
                ok = release_lease(a)
                return self._json(200, {"ok": ok})
            if action == "probe":
                model = normalize_model(req.get("model") or ARGS.probe_model)
                t0 = time.time()
                _status, _msg, _usage = chat_roundtrip(model, "hi", None,
                                                       probe=True, account=a)
                if _status == 200 and a.cooled():
                    unfreeze_account(a)
                outcome = ("probe_ok" if _status == 200 else
                           "probe_rejected" if _status in SWITCH_STATUS else "probe_failed")
                return self._json(200, {"outcome": outcome, "status": _status,
                                        "ms": int((time.time() - t0) * 1000)})
        return self._json(404, {"error": {"message": "not found"}})

    def find_account(self, name):
        for a in ACCOUNTS:
            if a.name == name:
                return a
        return None

    def handle_chat(self, req):
        model = normalize_model(req.get("model"))
        if ARGS.free_models and model not in ARGS.free_models:
            return self._json(404, {"error": {
                "message": f"model {model!r} not in allowlist", "type": "ModelError"}})
        system_text, prompt = split_messages(req.get("messages"))
        status, payload, usage = chat_roundtrip(model, prompt, system_text)
        if status != 200:
            return self._json(status if status >= 400 else 502, {"error": {
                "message": str(payload), "type": "UpstreamError"}})

        cid = f"chatcmpl-{int(time.time() * 1000)}"
        if req.get("stream"):
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.send_header("Cache-Control", "no-cache")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.send_header("Connection", "close")
            self.end_headers()

            def chunk(delta, finish=None, extra=None):
                d = {"id": cid, "object": "chat.completion.chunk",
                     "created": int(time.time()), "model": model,
                     "choices": [{"index": 0, "delta": delta, "finish_reason": finish}]}
                if extra:
                    d.update(extra)
                self.wfile.write(f"data: {json.dumps(d, ensure_ascii=False)}\n\n".encode())

            try:
                chunk({"role": "assistant", "content": ""})
                for i in range(0, len(payload), 24):
                    chunk({"content": payload[i:i + 24]})
                chunk({}, "stop")
                if usage:
                    chunk({}, None, {"usage": usage})
                self.wfile.write(b"data: [DONE]\n\n")
            except (BrokenPipeError, ConnectionResetError):
                pass
            self.wfile.flush()
            self.close_connection = True
        else:
            body = {"id": cid, "object": "chat.completion",
                    "created": int(time.time()), "model": model,
                    "choices": [{"index": 0,
                                 "message": {"role": "assistant", "content": payload},
                                 "finish_reason": "stop"}]}
            if usage:
                body["usage"] = usage
            self._json(200, body)


def flush_children(*_):
    for a in ACCOUNTS:
        if a.proc:
            try:
                a.proc.terminate()
            except Exception:
                pass
    for a in ACCOUNTS:
        if a.proc:
            try:
                a.proc.kill()
            except Exception:
                pass
    sys.exit(0)


def env_int(name, default):
    try:
        return int(os.environ.get(name, "") or default)
    except ValueError:
        return default


def main():
    global ARGS, ACCOUNTS
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=env_int("BRIDGE_PORT", 8790))
    ap.add_argument("--resin-url", default=os.environ.get("RESIN_URL", "http://resin:2260"))
    ap.add_argument("--proxy-token", default=os.environ.get("RESIN_PROXY_TOKEN", ""))
    ap.add_argument("--admin-token", default=os.environ.get("RESIN_ADMIN_TOKEN", ""))
    ap.add_argument("--platform", default=os.environ.get("RESIN_PLATFORM", "Default"))
    ap.add_argument("--accounts", default=os.environ.get("BRIDGE_ACCOUNTS", "bridge-1,bridge-2,bridge-3"))
    ap.add_argument("--cooldown-hours", type=float, default=24.0)
    ap.add_argument("--max-switches", type=int, default=3)
    ap.add_argument("--oc-port-base", type=int, default=14100)
    ap.add_argument("--oc-bin", default=os.environ.get("OPENCODE_BIN"))
    ap.add_argument("--home-base", default=os.environ.get("HOME_BASE", "/data/home"))
    ap.add_argument("--state-dir", default=os.environ.get("STATE_DIR", "/data/state"))
    ap.add_argument("--agent", default=os.environ.get("OC_AGENT", "bridge"))
    ap.add_argument("--prompt", default=os.environ.get("OC_PROMPT", MINIMAL_PROMPT))
    ap.add_argument("--full-prompt", action="store_true")
    ap.add_argument("--deny-tools", type=int, default=env_int("OC_DENY_TOOLS", 1))
    ap.add_argument("--upstream-timeout", type=int, default=env_int("OC_UPSTREAM_TIMEOUT", 600))
    ap.add_argument("--free-models", default=os.environ.get("BRIDGE_FREE_MODELS", ""))
    ap.add_argument("--probe-model", default=os.environ.get("PROBE_MODEL", "mimo-v2.6-flash-free"))
    ap.add_argument("--debug", type=int, default=env_int("BRIDGE_DEBUG", 0))
    ARGS = ap.parse_args()
    ARGS.free_models = [m.strip() for m in (ARGS.free_models or "").split(",") if m.strip()]
    if ARGS.full_prompt:
        ARGS.agent = "build"
    if not ARGS.proxy_token:
        sys.exit("[bridge] 缺少 RESIN_PROXY_TOKEN")
    ARGS.resin_hostport = ARGS.resin_url.split("//", 1)[-1].rstrip("/")

    ACCOUNTS = [Account(n, ARGS.oc_port_base + i)
                for i, n in enumerate(x.strip() for x in ARGS.accounts.split(",") if x.strip())]
    if not ACCOUNTS:
        sys.exit("[bridge] 账号列表为空")

    signal.signal(signal.SIGTERM, flush_children)
    signal.signal(signal.SIGINT, flush_children)
    load_state()

    for a in ACCOUNTS:
        try:
            start_account(a)
        except Exception as e:
            log(f"[boot] {a.name} 启动失败: {e!r}")
    threading.Thread(target=supervisor, daemon=True).start()
    for a in ACCOUNTS:
        if wait_account(a):
            log(f"[boot] {a.name} serve 就绪 port={a.port}")
            threading.Thread(target=event_reader, args=(a,), daemon=True).start()
            threading.Thread(target=log_watcher, args=(a,), daemon=True).start()
        else:
            log(f"[boot] {a.name} 未就绪(后台重试)")

    models_n = len(upstream_models())
    log(f"[bridge] 端点 0.0.0.0:{ARGS.port} 页面 GET / 账号={[a.name for a in ACCOUNTS]} "
        f"模型 {models_n} 个 平台={ARGS.platform} resin={ARGS.resin_url}")
    server = ThreadingHTTPServer(("0.0.0.0", ARGS.port), Handler)
    server.daemon_threads = True
    server.serve_forever()


if __name__ == "__main__":
    main()
