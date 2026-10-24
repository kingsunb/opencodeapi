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

提前判错：常驻 tail serve 日志 + 订阅 /event 事件流；额度类错误约 0.5s 内
识别，interrupt 该轮并换号，不等 opencode 内部重试拖满几分钟。
端点返回路径保留为兜底。

403/429 切换与 NovaVeil opencode 代理池同口径：
冷却(默认 24h, 可配)落盘 -> 释放 resin 租约 -> 换号重试至多 3 次。
已知上游免费额度按出口 IP 且 UTC 日重置，页面提供手动解冻与自动体检。

台账持久化：账号(含运行期添加)、体检结论、成败统计、成功天数落 accounts.json；
代理连接(resin 地址/token/平台)可在页面修改，保存前先做连通性测试，
保存后滚动重启 serve 实例生效。
自动体检：周期对全部账号体检，健康自动入池(解冻)，被拒冷却 24h 并释放租约。

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
from concurrent.futures import ThreadPoolExecutor
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
_dirty = threading.Event()
AUTO = {"enabled": False, "interval_hours": 24.0, "concurrency": 4, "last_run": 0.0}


def log(*a):
    print(time.strftime("[%Y-%m-%d %H:%M:%S]"), *a, flush=True)


def mask_secret(s):
    if not s:
        return ""
    return "****" + s[-4:] if len(s) > 8 else "****"


class Account:
    def __init__(self, name, port, source="config"):
        self.name = name
        self.port = port
        self.source = source
        self.added_at = time.time()
        self.base = f"http://127.0.0.1:{port}"
        self.proc = None
        self.stopping = False
        self.cooldown_until = 0.0
        self.last_used = 0.0
        self.success_count = 0
        self.fail_429 = 0
        self.fail_403 = 0
        self.fail_other = 0
        self.last_success_ts = 0.0
        self.last_latency_ms = 0
        self.last_error = ""
        self.success_days = set()
        self.probe = {}
        self.proxy_url = ""

    def rebuild_proxy_url(self):
        self.proxy_url = (
            f"http://{ARGS.platform}.{self.name}:{ARGS.proxy_token}@{ARGS.resin_hostport}"
        )

    def alive(self):
        return self.proc is not None and self.proc.poll() is None

    def cooled(self):
        return self.cooldown_until > time.time()

    def record_ok(self, ms):
        self.success_count += 1
        self.last_success_ts = time.time()
        self.last_latency_ms = ms
        self.success_days.add(time.strftime("%Y-%m-%d"))
        _dirty.set()

    def record_fail(self, status, detail=""):
        if status == 429:
            self.fail_429 += 1
        elif status == 403:
            self.fail_403 += 1
        else:
            self.fail_other += 1
        self.last_error = f"{status} {detail}".strip()[:160]
        _dirty.set()

    def to_json(self):
        now = time.time()
        return {
            "name": self.name, "port": self.port, "source": self.source,
            "added_at": self.added_at,
            "alive": self.alive(), "cooled": self.cooled(),
            "cooldown_until": self.cooldown_until,
            "cooldown_remaining_s": max(0, int(self.cooldown_until - now)),
            "success_count": self.success_count,
            "fail_429": self.fail_429, "fail_403": self.fail_403,
            "fail_other": self.fail_other,
            "last_success_ts": self.last_success_ts,
            "last_latency_ms": self.last_latency_ms,
            "last_error": self.last_error,
            "last_used": self.last_used,
            "success_days": len(self.success_days),
            "probe": self.probe,
        }

    def ledger_json(self):
        return {
            "name": self.name, "port": self.port, "source": self.source,
            "added_at": self.added_at, "cooldown_until": self.cooldown_until,
            "success_count": self.success_count,
            "fail_429": self.fail_429, "fail_403": self.fail_403,
            "fail_other": self.fail_other,
            "last_success_ts": self.last_success_ts,
            "last_latency_ms": self.last_latency_ms, "last_error": self.last_error,
            "success_days": sorted(self.success_days), "probe": self.probe,
        }


# ---------- 台账落盘 ----------
def _state_file(name):
    return os.path.join(ARGS.state_dir, name)


def save_accounts():
    data = {
        "next_port": getattr(ARGS, "next_port", ARGS.oc_port_base + len(ACCOUNTS)),
        "accounts": [a.ledger_json() for a in ACCOUNTS],
    }
    try:
        os.makedirs(ARGS.state_dir, exist_ok=True)
        tmp = _state_file("accounts.json.tmp")
        with open(tmp, "w") as f:
            json.dump(data, f, ensure_ascii=False)
        os.replace(tmp, _state_file("accounts.json"))
    except Exception as e:
        log(f"[state] 台账保存失败: {e!r}")
    finally:
        _dirty.clear()


def save_auto():
    try:
        os.makedirs(ARGS.state_dir, exist_ok=True)
        tmp = _state_file("auto.json.tmp")
        with open(tmp, "w") as f:
            json.dump(AUTO, f)
        os.replace(tmp, _state_file("auto.json"))
    except Exception as e:
        log(f"[state] 自动体检配置保存失败: {e!r}")


def save_proxy_file():
    data = {"resin_url": ARGS.resin_url, "platform": ARGS.platform,
            "proxy_token": ARGS.proxy_token, "admin_token": ARGS.admin_token}
    try:
        os.makedirs(ARGS.state_dir, exist_ok=True)
        tmp = _state_file("proxy.json.tmp")
        with open(tmp, "w") as f:
            json.dump(data, f)
        os.replace(tmp, _state_file("proxy.json"))
    except Exception as e:
        log(f"[state] 代理配置保存失败: {e!r}")


def _apply_ledger(a, rec):
    a.added_at = float(rec.get("added_at") or time.time())
    a.cooldown_until = float(rec.get("cooldown_until") or 0)
    a.success_count = int(rec.get("success_count") or 0)
    a.fail_429 = int(rec.get("fail_429") or 0)
    a.fail_403 = int(rec.get("fail_403") or 0)
    a.fail_other = int(rec.get("fail_other") or 0)
    a.last_success_ts = float(rec.get("last_success_ts") or 0)
    a.last_latency_ms = int(rec.get("last_latency_ms") or 0)
    a.last_error = str(rec.get("last_error") or "")
    a.success_days = set(rec.get("success_days") or [])
    a.probe = rec.get("probe") or {}


def load_state():
    # 旧 cooldowns.json 迁移
    legacy = {}
    try:
        with open(_state_file("cooldowns.json")) as f:
            legacy = json.load(f) or {}
        os.replace(_state_file("cooldowns.json"), _state_file("cooldowns.json.imported"))
    except Exception:
        pass
    data = {}
    try:
        with open(_state_file("accounts.json")) as f:
            data = json.load(f) or {}
    except Exception:
        pass
    ARGS.next_port = int(data.get("next_port") or (ARGS.oc_port_base + 64))
    ledger = {a["name"]: a for a in data.get("accounts") or []}
    for a in ACCOUNTS:
        rec = ledger.pop(a.name, None)
        if rec:
            _apply_ledger(a, rec)
        elif a.name in legacy:
            a.cooldown_until = float(legacy[a.name])
    # 台账里有、环境配置里没有的账号 = 运行期添加的手动账号
    for name, rec in ledger.items():
        try:
            a = Account(name, int(rec.get("port") or ARGS.next_port), "manual")
            ARGS.next_port = max(ARGS.next_port, a.port + 1)
            _apply_ledger(a, rec)
            ACCOUNTS.append(a)
        except Exception as e:
            log(f"[state] 台账账号 {name} 恢复失败: {e!r}")
    if legacy:
        log(f"[state] 迁移旧冷却状态 {len(legacy)} 条")
    try:
        with open(_state_file("auto.json")) as f:
            cfg = json.load(f) or {}
        AUTO.update({k: cfg[k] for k in AUTO if k in cfg})
    except Exception:
        pass
    try:
        with open(_state_file("proxy.json")) as f:
            pc = json.load(f) or {}
        if pc.get("resin_url"):
            ARGS.resin_url = pc["resin_url"]
            ARGS.platform = pc.get("platform") or ARGS.platform
            ARGS.proxy_token = pc.get("proxy_token") or ARGS.proxy_token
            ARGS.admin_token = pc.get("admin_token") or ARGS.admin_token
    except Exception:
        pass


def flusher():
    while True:
        if _dirty.wait(timeout=5.0):
            save_accounts()


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


def stop_account(a):
    a.stopping = True
    if a.proc:
        try:
            a.proc.terminate()
        except Exception:
            pass


def supervisor():
    while True:
        for a in list(ACCOUNTS):
            if a.stopping:
                continue
            if not a.alive():
                log(f"[sup] {a.name} 进程退出，重拉")
                try:
                    start_account(a)
                except Exception as e:
                    log(f"[sup] {a.name} 重拉失败: {e!r}")
        time.sleep(5)


def wait_account(a, tries=90):
    for _ in range(tries):
        if a.stopping:
            return False
        st, _d = oc(a, "GET", "/global/health", timeout=3)
        if st == 200:
            return True
        time.sleep(0.5)
    return False


# ---------- /event 订阅 + 日志 tail 提前判错 ----------
_waiters = {}
_waiters_lock = threading.Lock()


class Waiter:
    def __init__(self):
        self.ev = threading.Event()
        self.status = None
        self.detail = ""
        self.done = threading.Event()
        self.resp = None


def resolve_waiter(port, sid, status, detail):
    with _waiters_lock:
        w = _waiters.pop((port, sid), None)
    if w:
        w.status = status
        w.detail = detail
        w.ev.set()


def classify_error(obj):
    """错误部件/信息 -> (status, detail)。status 仅 {403,429} 或 None。"""
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
    m = re.search(r'"(?:status|statusCode)"\s*:\s*"?(403|429)"?', s)
    if m:
        return int(m.group(1)), "status field"
    return None, "generic error"


def classify_error_text(s):
    """日志错误文本 -> (status, detail)。只认确定性错误，放过瞬态。"""
    low = (s or "").lower()
    if ("free usage limit" in low or "rate limit" in low
            or "too many requests" in low or "status code: 429" in low):
        return 429, (s or "")[:120]
    if "forbidden" in low or "status code: 403" in low:
        return 403, (s or "")[:120]
    return None, ""


def log_watcher(a):
    """tail serve 日志：stream error 行 +0.5s 内可见，是额度类错误的最早信号。"""
    path = os.path.join(ARGS.home_base, a.name,
                        ".local/share/opencode/log/opencode.log")
    while not os.path.exists(path):
        if a.stopping:
            return
        time.sleep(1)
    f = open(path, errors="replace")
    f.seek(0, 2)
    while not a.stopping:
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
            status, detail = classify_error_text(em.group(1) if em else line)
            if status in SWITCH_STATUS:
                log(f"[log] {a.name} 日志先行判错 status={status}")
                resolve_waiter(a.port, m.group(1), status, detail)
        except Exception as e:
            log(f"[log] {a.name} watcher 异常: {e!r}")
            time.sleep(1)


def event_reader(a):
    """常驻订阅实例 /event，把错误部件事件分发给等待者。"""
    while not a.stopping:
        try:
            req = urllib.request.Request(a.base + "/event")
            r = urllib.request.urlopen(req, timeout=None)
            log(f"[ev] {a.name} 事件流已连接")
            for raw in r:
                if a.stopping:
                    return
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
                    if status:
                        log(f"[ev] {a.name} 错误部件 sid={sid[:16]} status={status}")
                        resolve_waiter(a.port, sid, status, detail)
        except Exception:
            if a.stopping:
                return
            time.sleep(2)


# ---------- 账号池与冷却 ----------
def pick_account():
    with _pool_lock:
        cands = [a for a in ACCOUNTS if not a.cooled() and a.alive()]
        if not cands:
            cands = [a for a in ACCOUNTS if not a.cooled()]
        if not cands:
            return None
        cands.sort(key=lambda x: x.last_used)
        cands[0].last_used = time.time()
        return cands[0]


def cooldown_account(a, hours):
    with _pool_lock:
        a.cooldown_until = time.time() + hours * 3600
    save_accounts()
    log(f"[pool] {a.name} 冷却 {hours}h")


def unfreeze_account(a):
    with _pool_lock:
        a.cooldown_until = 0.0
    save_accounts()
    log(f"[pool] {a.name} 解冻")


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
    return False


def ask(a, model, prompt, system=None):
    """返回 (text, usage, None, "") 或 (None, None, status, detail)。"""
    st, d = oc(a, "POST", "/session", {"title": "bridge"}, timeout=15)
    if st != 200 or not isinstance(d, dict) or "id" not in d:
        log(f"[ask] {a.name} 建会话失败 status={st}")
        return None, None, 598, "serve 会话失败"
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
        return None, None, status, detail
    with _waiters_lock:
        _waiters.pop((a.port, sid), None)

    if resp is None:
        return None, None, 598, "serve 连接失败"
    st, d = resp
    if st == 0:
        log(f"[ask] {a.name} serve 连接失败: {d.get('raw')}")
        return None, None, 598, str(d.get("raw"))[:120]
    if st in SWITCH_STATUS:
        return None, None, st, f"HTTP {st}"
    if st != 200 or not isinstance(d, dict) or "_html" in d:
        status, detail = scan_error_parts(d) if isinstance(d, dict) else (None, "")
        if status is None:
            status = 502 if st >= 500 else st
            detail = f"serve HTTP {st}"
        return None, None, status, detail
    status, detail = scan_error_parts(d)
    if status:
        log(f"[ask] {a.name} 响应含错误部件 status={status}")
        return None, None, status, detail
    text = extract_text(d)
    if not text:
        log(f"[ask] {a.name} 空输出")
        return None, None, 599, "空输出"
    usage = usage_of(d.get("info"))
    return text, usage, None, ""


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
            return 503, "所有账号均在冷却中", None, ""
        t0 = time.time()
        text, usage, status, detail = ask(a, model, prompt, system or None)
        ms = int((time.time() - t0) * 1000)
        if status is None:
            a.record_ok(ms)
            record_log(a.name, model, f"{prefix}ok", 200, ms)
            return 200, text, usage, ""
        a.record_fail(status, detail)
        outcome = {403: f"{prefix}403", 429: f"{prefix}429",
                   599: f"{prefix}empty"}.get(status, f"{prefix}failed")
        record_log(a.name, model, outcome, status, ms)
        log(f"[chat] {a.name} {model} 失败 status={status} {ms}ms{' (probe)' if probe else ''}")
        if status in SWITCH_STATUS:
            cooldown_account(a, ARGS.cooldown_hours)
            release_lease(a)
        if probe or account is not None or status == 598:
            return (status if status >= 400 else 502), f"upstream {status}", None, detail
        switches += 1
        if switches > ARGS.max_switches:
            return 502, f"upstream {status} after {switches} attempts", None, detail
        SWITCH_COUNT += 1


def probe_one(a, model):
    """单账号体检：结论 + 上游详情落台账，健康自动入池(解冻)。"""
    t0 = time.time()
    status, _msg, _usage, detail = chat_roundtrip(model, "hi", None,
                                                  probe=True, account=a)
    ms = int((time.time() - t0) * 1000)
    if status == 200:
        outcome = "healthy"
        if a.cooled():
            unfreeze_account(a)
    elif status in SWITCH_STATUS:
        outcome = "rejected"
    else:
        outcome = "failed"
    a.probe = {"outcome": outcome, "status": status, "ms": ms,
               "detail": (detail or "")[:200], "at": time.time(),
               "model": model}
    save_accounts()
    return {"name": a.name, "outcome": outcome, "status": status,
            "ms": ms, "detail": (detail or "")[:200]}


def batch_probe(names, model, concurrency):
    targets = [a for a in ACCOUNTS if not names or a.name in names]
    workers = max(1, min(int(concurrency or 4), len(targets) or 1))
    with ThreadPoolExecutor(max_workers=workers) as ex:
        results = list(ex.map(lambda a: probe_one(a, model), targets))
    summary = {"healthy": sum(1 for r in results if r["outcome"] == "healthy"),
               "rejected": sum(1 for r in results if r["outcome"] == "rejected"),
               "failed": sum(1 for r in results if r["outcome"] == "failed")}
    return summary, results


def auto_loop():
    while True:
        try:
            if (AUTO.get("enabled") and ACCOUNTS
                    and time.time() - AUTO.get("last_run", 0)
                    >= float(AUTO.get("interval_hours", 24)) * 3600):
                AUTO["last_run"] = time.time()
                save_auto()
                log("[auto] 自动体检开始")
                summary, _r = batch_probe([], ARGS.probe_model,
                                          AUTO.get("concurrency") or 4)
                log(f"[auto] 自动体检完成 {summary}")
        except Exception as e:
            log(f"[auto] 异常: {e!r}")
        time.sleep(30)


# ---------- 账号增删 ----------
def add_accounts(names):
    added, skipped = [], []
    for name in names:
        name = str(name).strip()
        if not name or any(a.name == name for a in ACCOUNTS):
            if name:
                skipped.append(name)
            continue
        if len(ACCOUNTS) >= ARGS.max_accounts:
            skipped.append(name)
            continue
        a = Account(name, ARGS.next_port, "manual")
        ARGS.next_port += 1
        a.rebuild_proxy_url()
        ACCOUNTS.append(a)
        save_accounts()
        try:
            start_account(a)
        except Exception as e:
            log(f"[boot] {a.name} 启动失败: {e!r}")
        threading.Thread(target=wait_then_readers, args=(a,), daemon=True).start()
        added.append(name)
    return added, skipped


def wait_then_readers(a):
    if wait_account(a):
        log(f"[boot] {a.name} serve 就绪 port={a.port}")
        threading.Thread(target=event_reader, args=(a,), daemon=True).start()
        threading.Thread(target=log_watcher, args=(a,), daemon=True).start()


def remove_account(a):
    stop_account(a)
    ACCOUNTS.remove(a)
    save_accounts()
    log(f"[pool] {a.name} 已删除")


# ---------- 代理设置 ----------
def test_proxy(candidate):
    """candidate: {resin_url, proxy_token, platform, account?} -> (ok, egress_ip|err)。"""
    url = str(candidate.get("resin_url") or "").strip().rstrip("/")
    token = str(candidate.get("proxy_token") or "").strip()
    platform = str(candidate.get("platform") or ARGS.platform).strip()
    if not url.startswith(("http://", "https://")):
        return False, "resin_url 必须以 http(s):// 开头"
    if not token:
        return False, "缺少 proxy_token"
    hostport = url.split("//", 1)[-1]
    acct = candidate.get("account") or (ACCOUNTS[0].name if ACCOUNTS else "probe")
    proxy_url = f"http://{platform}.{acct}:{token}@{hostport}"
    opener = urllib.request.build_opener(
        urllib.request.ProxyHandler({"http": proxy_url, "https": proxy_url}))
    req = urllib.request.Request("https://api.ipify.org",
                                 headers={"User-Agent": MODEL_UA})
    try:
        with opener.open(req, timeout=12) as r:
            return True, r.read().decode()[:64]
    except Exception as e:
        return False, repr(e)[:160]


def apply_proxy(resin_url, platform, proxy_token, admin_token):
    global _platform_id
    ARGS.resin_url = resin_url.rstrip("/")
    ARGS.resin_hostport = ARGS.resin_url.split("//", 1)[-1]
    ARGS.platform = platform
    if proxy_token:
        ARGS.proxy_token = proxy_token
    if admin_token:
        ARGS.admin_token = admin_token
    _platform_id = None
    _egress_cache.update(ts=0.0, data={})
    with _models_lock:
        _models_cache.update(ts=0.0, data=None)
    for a in ACCOUNTS:
        a.rebuild_proxy_url()
        stop_account(a)
    save_proxy_file()
    threading.Thread(target=_restart_all_serves, daemon=True).start()


def _restart_all_serves():
    for a in list(ACCOUNTS):
        a.stopping = False
        try:
            start_account(a)
        except Exception as e:
            log(f"[proxy] {a.name} 重启失败: {e!r}")
    for a in list(ACCOUNTS):
        if wait_account(a):
            log(f"[proxy] {a.name} serve 已按新代理重启")
        else:
            a.stopping = False
            log(f"[proxy] {a.name} 重启未就绪(后台重试)")


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
main{padding:16px 20px;max-width:1280px;margin:0 auto}
.cards{display:flex;flex-wrap:wrap;gap:10px;margin-bottom:16px}
.card{background:#fff;border:1px solid var(--bd);border-radius:8px;padding:10px 16px;min-width:130px}
.card .v{font-size:20px;font-weight:600}
.card .k{font-size:12px;color:var(--mut)}
h2{font-size:15px;margin:22px 0 8px}
fieldset{border:1px solid var(--bd);border-radius:8px;background:#fff;margin:0 0 16px;padding:10px 14px}
legend{font-size:13px;font-weight:600;padding:0 6px}
.row{display:flex;flex-wrap:wrap;gap:8px;align-items:center;margin:6px 0}
.row label{font-size:13px;color:var(--mut);min-width:90px}
input{font:inherit;padding:4px 8px;border:1px solid var(--bd);border-radius:6px}
input.w260{width:260px}input.w110{width:110px}
button{font:inherit;padding:3px 10px;margin:1px;border:1px solid var(--bd);border-radius:6px;background:#fff;cursor:pointer}
button:hover{background:#eef1f4}
button.warn{color:var(--warn);border-color:var(--warn)}
button.danger{color:var(--bad);border-color:var(--bad)}
button.primary{background:#1f6feb;color:#fff;border-color:#1f6feb}
table{width:100%;border-collapse:collapse;background:#fff;border:1px solid var(--bd);border-radius:8px;overflow:hidden}
th,td{padding:7px 10px;border-bottom:1px solid var(--bd);text-align:left;font-size:13px;vertical-align:top}
th{background:#f0f2f5;font-weight:600;white-space:nowrap}
tr:last-child td{border-bottom:none}
td.num{text-align:right;font-variant-numeric:tabular-nums}
.b{display:inline-block;padding:1px 8px;border-radius:10px;font-size:12px;color:#fff;white-space:nowrap}
.b-ok{background:var(--ok)}.b-warn{background:var(--warn)}.b-bad{background:var(--bad)}.b-mut{background:#9ca3af}
.mut{color:var(--mut);font-size:12px}
.filters button.on{background:#1f6feb;color:#fff;border-color:#1f6feb}
.toast{position:fixed;right:16px;bottom:16px;background:#111;color:#fff;padding:8px 14px;border-radius:8px;opacity:0;transition:opacity .3s;pointer-events:none;max-width:460px}
.toast.show{opacity:.92}
</style>
</head>
<body>
<header><h1>OpenCode 桥 · 账号体检</h1><div class="meta" id="meta"></div></header>
<main>
<div class="cards" id="cards"></div>

<fieldset><legend>代理设置（resin）</legend>
<div class="row"><label>resin 地址</label><input id="pResin" class="w260" placeholder="http://resin:2260"></div>
<div class="row"><label>平台名</label><input id="pPlatform" class="w110" placeholder="Default"></div>
<div class="row"><label>代理 token</label><input id="pProxyToken" class="w260" type="password"><span class="mut" id="pProxyMask"></span></div>
<div class="row"><label>管理 token</label><input id="pAdminToken" class="w260" type="password"><span class="mut" id="pAdminMask"></span></div>
<div class="row">
  <button onclick="proxyTest()">测试连通</button>
  <button class="primary" onclick="proxySave()">保存并生效（重启 serve）</button>
  <span class="mut" id="proxyHint">留空 token 表示沿用现有值；保存前先做连通性测试，保存后逐账号滚动重启。</span>
</div></fieldset>

<fieldset><legend>体检</legend>
<div class="row">
  <label>测试模型</label><input id="probeModel" class="w260">
  <label>并发数</label><input id="probeConc" class="w110" value="4">
  <button class="primary" onclick="probeAll()">一键测试台账</button>
</div>
<div class="row">
  <label>自动体检</label>
  <input type="checkbox" id="autoOn" onchange="autoSave()">
  <span>每</span><input id="autoHours" class="w110" value="24"><span>小时（健康自动入池，被拒冷却 24h）</span>
  <span class="mut" id="autoHint"></span>
</div>
<div class="row">
  <label>添加账号</label><input id="addNames" class="w260" placeholder="账号名，逗号分隔">
  <button onclick="addAccounts()">添加</button>
  <span class="mut">手动账号持久保存，每号启动一个 serve 实例（内存约 200MB）</span>
</div></fieldset>

<div class="row filters" id="filters">
  <button data-f="all" class="on" onclick="setFilter('all')">全部</button>
  <button data-f="usable" onclick="setFilter('usable')">可用</button>
  <button data-f="cooling" onclick="setFilter('cooling')">冷却中</button>
  <button data-f="bad" onclick="setFilter('bad')">不可用</button>
</div>
<h2>账号台账</h2>
<table id="accounts"><thead><tr>
<th>账号</th><th>状态</th><th>体检结论</th><th>出口 IP / 节点</th><th class="num">成功</th>
<th class="num">429</th><th class="num">403</th><th class="num">其他</th><th class="num">成功天数</th>
<th>最近成功</th><th>最近错误</th><th>操作</th>
</tr></thead><tbody></tbody></table>

<h2>最近请求</h2>
<table id="logs"><thead><tr>
<th>时间</th><th>账号</th><th>模型</th><th>结论</th><th class="num">HTTP</th><th class="num">耗时</th>
</tr></thead><tbody></tbody></table>
<p class="mut">冷却默认 24 小时（COOLDOWN_HOURS 可调）；免费额度按出口 IP 且 UTC 日重置，被拒后可解冻或等自动体检拉回。页面每 5 秒自动刷新。</p>
</main>
<div class="toast" id="toast"></div>
<script>
const $=s=>document.querySelector(s);
let CUR=null,FILTER='all';
function esc(s){return String(s??"").replace(/[&<>"]/g,c=>({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;"}[c]))}
function badge(txt,cls){return `<span class="b ${cls}">${esc(txt)}</span>`}
function hms(sec){if(sec<=0)return "";const h=Math.floor(sec/3600),m=Math.floor(sec%3600/60);return h?`${h}时${m}分`:`${m}分`}
function ts(t){return t?new Date(t*1000).toLocaleString("zh-CN",{hour12:false}):"—"}
function toast(msg){const t=$("#toast");t.textContent=msg;t.classList.add("show");setTimeout(()=>t.classList.remove("show"),4000)}
async function api(path,body){const r=await fetch(path,{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify(body||{})});return r.json()}
const OUTCOME={ok:["成功","b-ok"],probe_ok:["体检成功","b-ok"],"403":["被拒 403","b-warn"],"429":["限流 429","b-warn"],empty:["空输出","b-warn"],failed:["失败","b-bad"]};
const PROBE={healthy:["健康","b-ok"],rejected:["被拒","b-warn"],failed:["失败","b-bad"]};
function setFilter(f){FILTER=f;document.querySelectorAll("#filters button").forEach(b=>b.classList.toggle("on",b.dataset.f===f));if(CUR)render(CUR)}
async function refresh(){
 try{
  const s=await(await fetch("/api/status")).json();CUR=s;
  $("#meta").textContent=`resin ${s.resin_url} · 平台 ${s.platform} · ${s.models_count} 模型 · 运行 ${hms(s.uptime_s)||"0分"} · resin ${s.resin_ok?"正常":"不可达"}`;
  const c=s.summary;
  $("#cards").innerHTML=[
   ["可用账号",`${c.usable} / ${s.accounts.length}`],
   ["冷却中",c.cooling],["进程异常",c.down],
   ["累计换号",c.switches],["24h 成功",s.success_24h],["24h 失败",s.fail_24h]
  ].map(([k,v])=>`<div class="card"><div class="v">${esc(v)}</div><div class="k">${k}</div></div>`).join("");
  if(document.activeElement!==$("#pResin"))$("#pResin").value=s.proxy.resin_url||"";
  if(document.activeElement!==$("#pPlatform"))$("#pPlatform").value=s.proxy.platform||"";
  $("#pProxyMask").textContent=s.proxy.proxy_token_masked||"未设置";
  $("#pAdminMask").textContent=s.proxy.admin_token_masked||"未设置";
  if(document.activeElement!==$("#probeModel"))$("#probeModel").value=s.probe_model||"";
  $("#autoOn").checked=s.auto.enabled;
  if(document.activeElement!==$("#autoHours"))$("#autoHours").value=s.auto.interval_hours;
  $("#autoHint").textContent=s.auto.enabled?`上次 ${s.auto.last_run?ts(s.auto.last_run):"未运行"}`:"已关闭";
  render(s);
  const l=await(await fetch("/api/logs")).json();
  $("#logs tbody").innerHTML=(l.items||[]).slice(0,50).map(e=>{
   const o=OUTCOME[e.outcome]||[e.outcome,"b-mut"];
   return `<tr><td class="mut">${esc(e.ts)}</td><td>${esc(e.account)}</td><td>${esc(e.model)}</td>
   <td>${badge(o[0],o[1])}</td><td class="num">${esc(e.status)}</td><td class="num">${e.ms} ms</td></tr>`}).join("")
   ||'<tr><td colspan="6" class="mut">暂无请求</td></tr>';
 }catch(e){$("#meta").textContent="加载失败："+e}
}
function render(s){
  const list=s.accounts.filter(a=>FILTER==="all"||(FILTER==="usable"?a.alive&&!a.cooled:FILTER==="cooling"?a.cooled:!a.alive));
  $("#accounts tbody").innerHTML=list.map(a=>{
   const eg=a.egress||{};
   let st;
   if(!a.alive)st=badge("进程异常","b-bad");
   else if(a.cooled)st=badge(`冷却 ${hms(a.cooldown_remaining_s)||"<1分"}`,"b-warn");
   else st=badge("可用","b-ok");
   const p=a.probe||{};
   let pv=p.at?badge((PROBE[p.outcome]||[p.outcome,"b-mut"])[0],(PROBE[p.outcome]||["","b-mut"])[1]):'<span class="mut">未体检</span>';
   if(p.at)pv+=` <span class="mut">HTTP ${esc(p.status||"")} ${p.ms||0}ms ${esc((p.detail||"").slice(0,60))}<br>${ts(p.at)} · ${esc(p.model||"")}</span>`;
   const src=a.source==="config"?'<span class="mut">配置</span>':'<span class="mut">手动</span>';
   const ops=[
    `<button onclick="probeOne('${esc(a.name)}')">体检</button>`,
    a.cooled?`<button onclick="unfreeze('${esc(a.name)}')">解冻</button>`:`<button class="warn" onclick="cool('${esc(a.name)}')">冷却24h</button>`,
    `<button class="danger" onclick="release('${esc(a.name)}')">释放租约</button>`,
    a.source==="manual"?`<button class="danger" onclick="delAccount('${esc(a.name)}')">删除</button>`:""
   ].join(" ");
   return `<tr><td>${esc(a.name)}<br>${src}</td><td>${st}</td><td>${pv}</td>
   <td>${esc(eg.egress_ip||"—")}<br><span class="mut">${esc(eg.node_tag||"—")}</span></td>
   <td class="num">${a.success_count}</td><td class="num">${a.fail_429}</td><td class="num">${a.fail_403}</td><td class="num">${a.fail_other}</td><td class="num">${a.success_days}</td>
   <td>${a.last_success_ts?ts(a.last_success_ts):"—"}</td>
   <td class="mut">${esc(a.last_error||"—")}</td><td>${ops}</td></tr>`}).join("")
   ||'<tr><td colspan="12" class="mut">无匹配账号</td></tr>';
}
async function probeOne(name){
 toast(name+" 体检中…");
 try{const r=await api(`/api/accounts/${name}/probe`,{model:$("#probeModel").value.trim()||undefined});
  toast(`${name}：${r.outcome} HTTP ${r.status} ${r.ms}ms ${r.detail||""}`)}catch(e){toast(name+" 体检请求失败")}
 refresh();
}
async function probeAll(){
 const conc=Math.max(1,Math.min(20,parseInt($("#probeConc").value)||4));
 toast(`并发 ${conc} 体检台账中…`);
 try{
  const r=await api("/api/probe",{model:$("#probeModel").value.trim()||undefined,concurrency:conc});
  toast(`体检完成：健康 ${r.summary.healthy}，被拒 ${r.summary.rejected}，失败 ${r.summary.failed}`)
 }catch(e){toast("体检请求失败")}
 refresh();
}
async function autoSave(){
 try{await api("/api/auto",{enabled:$("#autoOn").checked,interval_hours:parseFloat($("#autoHours").value)||24,concurrency:parseInt($("#probeConc").value)||4});
  toast("自动体检配置已保存")}catch(e){toast("保存失败")}
 refresh();
}
async function proxyTest(){
 const r=await api("/api/proxy/test",{resin_url:$("#pResin").value.trim(),platform:$("#pPlatform").value.trim()||undefined,proxy_token:$("#pProxyToken").value.trim()||undefined});
 toast(r.ok?`连通，出口 IP：${r.egress_ip}`:`失败：${r.error}`);
}
async function proxySave(){
 if(!confirm("保存新代理配置并滚动重启全部 serve 实例？"))return;
 const r=await api("/api/proxy",{resin_url:$("#pResin").value.trim(),platform:$("#pPlatform").value.trim()||undefined,proxy_token:$("#pProxyToken").value.trim(),admin_token:$("#pAdminToken").value.trim()});
 toast(r.ok?`已保存，出口 IP：${r.egress_ip}，serve 重启中`:`失败：${r.error}`);
 $("#pProxyToken").value="";$("#pAdminToken").value="";refresh();
}
async function addAccounts(){
 const names=($("#addNames").value||"").split(",").map(x=>x.trim()).filter(Boolean);
 if(!names.length)return;
 const r=await api("/api/accounts/add",{names});
 toast(`已添加 ${(r.added||[]).join(", ")||"无"}${(r.skipped||[]).length?`，跳过 ${r.skipped.join(", ")}`:""}`);
 $("#addNames").value="";refresh();
}
async function delAccount(name){if(!confirm(`删除账号 ${name} 并停掉其 serve 实例？`))return;await api(`/api/accounts/${name}/delete`,{});toast(name+" 已删除");refresh()}
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

    def _html(self, b):
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
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
            return self._html(_INDEX_HTML.encode())
        if p.endswith("/models"):
            return self._json(200, {"object": "list", "data": upstream_models()})
        if p == "/healthz" or "health" in p:
            now = time.time()
            return self._json(200, {"ok": True, "accounts": [{
                "name": a.name, "alive": a.alive(), "cooled": a.cooled(),
                "cooldown_remaining_s": max(0, int(a.cooldown_until - now)),
            } for a in ACCOUNTS]})
        if p == "/api/status":
            return self.api_status()
        if p == "/api/logs":
            with REQ_LOGS_LOCK:
                items = list(REQ_LOGS)
            return self._json(200, {"items": items})
        if p == "/api/proxy":
            return self._json(200, {
                "resin_url": ARGS.resin_url, "platform": ARGS.platform,
                "proxy_token_masked": mask_secret(ARGS.proxy_token),
                "admin_token_masked": mask_secret(ARGS.admin_token)})
        if p == "/debug/state" and ARGS.debug:
            now = time.time()
            return self._json(200, {"accounts": [{
                "name": a.name, "last_used": a.last_used,
                "cooldown_until": a.cooldown_until, "cooled": a.cooled(),
            } for a in ACCOUNTS], "platform_id": platform_id()})
        self._json(404, {"error": {"message": "not found"}})

    def api_status(self):
        eg, resin_ok = egress_info()
        accounts = []
        for a in ACCOUNTS:
            j = a.to_json()
            j["egress"] = eg.get(a.name)
            accounts.append(j)
        with REQ_LOGS_LOCK:
            recent = list(REQ_LOGS)[:500]
        day = [e for e in recent if not str(e["outcome"]).startswith("probe_")]
        self._json(200, {
            "ok": True,
            "started_at": STARTED_AT,
            "uptime_s": int(time.time() - STARTED_AT),
            "resin_url": ARGS.resin_url, "platform": ARGS.platform,
            "resin_ok": resin_ok,
            "models_count": len(upstream_models()),
            "probe_model": ARGS.probe_model,
            "max_accounts": ARGS.max_accounts,
            "proxy": {
                "resin_url": ARGS.resin_url, "platform": ARGS.platform,
                "proxy_token_masked": mask_secret(ARGS.proxy_token),
                "admin_token_masked": mask_secret(ARGS.admin_token),
            },
            "auto": dict(AUTO),
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

    def find_account(self, name):
        for a in ACCOUNTS:
            if a.name == name:
                return a
        return None

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
        m = re.match(r"^/api/accounts/([^/]+)/(cooldown|release|probe|delete)$", p)
        if m:
            a = self.find_account(m.group(1))
            action = m.group(2)
            if action == "delete":
                if not a:
                    return self._json(200, {"ok": True})
                if a.source != "manual":
                    return self._json(400, {"error": {"message": "配置来源账号不可删除"}})
                remove_account(a)
                return self._json(200, {"ok": True})
            if not a:
                return self._json(404, {"error": {"message": "no such account"}})
            if action == "cooldown":
                minutes = float(req.get("minutes") or 1440)
                if minutes <= 0:
                    unfreeze_account(a)
                else:
                    cooldown_account(a, minutes / 60.0)
                return self._json(200, {"ok": True, "cooldown_remaining_s":
                                        max(0, int(a.cooldown_until - time.time()))})
            if action == "release":
                return self._json(200, {"ok": release_lease(a)})
            if action == "probe":
                model = normalize_model(req.get("model") or ARGS.probe_model)
                return self._json(200, probe_one(a, model))
        if p == "/api/accounts/add":
            names = req.get("names") or []
            if isinstance(names, str):
                names = names.split(",")
            added, skipped = add_accounts(names)
            return self._json(200, {"added": added, "skipped": skipped})
        if p == "/api/probe":
            model = normalize_model(req.get("model") or ARGS.probe_model)
            conc = int(req.get("concurrency") or 4)
            names = req.get("accounts") or []
            summary, results = batch_probe(names, model, conc)
            return self._json(200, {"summary": summary, "results": results})
        if p == "/api/auto":
            if "enabled" in req:
                AUTO["enabled"] = bool(req["enabled"])
            if "interval_hours" in req:
                AUTO["interval_hours"] = max(0.5, float(req["interval_hours"] or 24))
            if "concurrency" in req:
                AUTO["concurrency"] = max(1, min(20, int(req["concurrency"] or 4)))
            save_auto()
            return self._json(200, {"ok": True, "auto": dict(AUTO)})
        if p == "/api/proxy/test":
            cand = {"resin_url": req.get("resin_url") or ARGS.resin_url,
                    "proxy_token": req.get("proxy_token") or ARGS.proxy_token,
                    "platform": req.get("platform") or ARGS.platform}
            ok, info = test_proxy(cand)
            return self._json(200, {"ok": ok, **({"egress_ip": info} if ok else {"error": info})})
        if p == "/api/proxy":
            url = str(req.get("resin_url") or "").strip()
            platform = str(req.get("platform") or ARGS.platform).strip()
            token = str(req.get("proxy_token") or "").strip()
            admin = str(req.get("admin_token") or "").strip()
            if not url.startswith(("http://", "https://")):
                return self._json(400, {"ok": False, "error": "resin_url 必须以 http(s):// 开头"})
            if not token and not ARGS.proxy_token:
                return self._json(400, {"ok": False, "error": "缺少 proxy_token"})
            cand = {"resin_url": url, "proxy_token": token or ARGS.proxy_token,
                    "platform": platform}
            ok, info = test_proxy(cand)
            if not ok:
                return self._json(400, {"ok": False, "error": f"连通性测试失败：{info}"})
            apply_proxy(url, platform, token, admin)
            return self._json(200, {"ok": True, "egress_ip": info})
        return self._json(404, {"error": {"message": "not found"}})

    def handle_chat(self, req):
        model = normalize_model(req.get("model"))
        if ARGS.free_models and model not in ARGS.free_models:
            return self._json(404, {"error": {
                "message": f"model {model!r} not in allowlist", "type": "ModelError"}})
        system_text, prompt = split_messages(req.get("messages"))
        status, payload, usage, _detail = chat_roundtrip(model, prompt, system_text)
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
    ap.add_argument("--max-accounts", type=int, default=env_int("MAX_ACCOUNTS", 8))
    ap.add_argument("--debug", type=int, default=env_int("BRIDGE_DEBUG", 0))
    ARGS = ap.parse_args()
    ARGS.free_models = [m.strip() for m in (ARGS.free_models or "").split(",") if m.strip()]
    if ARGS.full_prompt:
        ARGS.agent = "build"
    if not ARGS.proxy_token:
        sys.exit("[bridge] 缺少 RESIN_PROXY_TOKEN（env 或 proxy.json）")
    ARGS.resin_hostport = ARGS.resin_url.split("//", 1)[-1].rstrip("/")

    names = [x.strip() for x in ARGS.accounts.split(",") if x.strip()]
    ACCOUNTS = [Account(n, ARGS.oc_port_base + i, "config")
                for i, n in enumerate(names)]
    ARGS.next_port = ARGS.oc_port_base + max(64, len(names))
    if not ACCOUNTS:
        sys.exit("[bridge] 账号列表为空")

    signal.signal(signal.SIGTERM, flush_children)
    signal.signal(signal.SIGINT, flush_children)
    load_state()
    for a in ACCOUNTS:
        a.rebuild_proxy_url()
    save_accounts()

    for a in ACCOUNTS:
        try:
            start_account(a)
        except Exception as e:
            log(f"[boot] {a.name} 启动失败: {e!r}")
    threading.Thread(target=supervisor, daemon=True).start()
    threading.Thread(target=flusher, daemon=True).start()
    threading.Thread(target=auto_loop, daemon=True).start()
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
