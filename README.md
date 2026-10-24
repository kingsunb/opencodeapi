# opencodeapi

把**真实 opencode 客户端**（`opencode serve` 无头服务）变成 OpenAI 兼容端点的桥。
不做指纹伪装：官方客户端自己能过 Zen 免费档的客户端闸门，桥只负责协议翻译、
账号池与出口切换。

```
OpenAI 客户端 / NovaVeil
      |  POST /v1/chat/completions
      v
oc_bridge (本镜像, 0.0.0.0:8790, 内嵌账号体检页 GET /)
      |  POST /session , POST /session/{id}/message   (每账号一个 serve 实例)
      v
opencode serve xN --HTTPS_PROXY--> resin (账号粘性出口 IP) --> opencode.ai Zen
```

## 特性

- **真客户端指纹**：请求由真实 opencode 二进制发出，闸门校验天然合法；
- **账号池 + 出口切换**：每个账号对应 resin 的一个粘性出口 IP；上游 403/429 时
  冷却该账号（默认 24h，落盘持久）、释放 resin 租约、换号重试至多 3 次，
  与 NovaVeil 的 opencode 代理池同口径；
- **提前判错**：常驻 tail serve 日志 + 订阅 `/event` 事件流，额度类错误在约
  0.5 秒内识别并切换，不等 opencode 内部重试拖满几分钟；
- **真实用量**：usage 取自上游返回的 tokens（`prompt=input`，
  `completion=output+reasoning`），非流式带 usage 对象，流式在 `[DONE]`
  前发 usage 尾帧，绝不返回全零 usage；
- **工具拒绝**：agent 的 bash/edit/webfetch 等执行类工具默认 deny，
  请求里的工具定义形态不受影响；
- **账号体检页**：`GET /` 提供摘要卡、账号台账（出口 IP/成败计数/最近错误）、
  请求流水与操作（体检、冷却、解冻、释放租约），5 秒自刷。

## 部署

前置：一个 resin 实例（`ghcr.io/resinat/resin`，粘性代理池），
代理凭据格式为 `http://<平台>.<账号>:<proxy_token>@<resin地址>`。

```sh
cp docker-compose.example.yml docker-compose.yml
# 编辑 docker-compose.yml，填入 RESIN_PROXY_TOKEN / RESIN_ADMIN_TOKEN
docker compose up -d
```

桥默认加入 `novaveil_default` 等外部网络的方式见示例文件注释；
与 resin 同 compose 部署时用服务名直连即可。

## 环境变量

| 变量 | 默认 | 说明 |
| --- | --- | --- |
| `RESIN_URL` | `http://resin:2260` | resin 地址（代理与管理 API 同端口） |
| `RESIN_PROXY_TOKEN` | 必填 | resin 代理 token |
| `RESIN_ADMIN_TOKEN` | 空 | resin 管理 token，用于释放租约 |
| `RESIN_PLATFORM` | `Default` | resin 平台名（用户名为 `平台.账号`） |
| `BRIDGE_ACCOUNTS` | `bridge-1,bridge-2,bridge-3` | 账号列表，逗号分隔，每号一个 serve 实例 |
| `COOLDOWN_HOURS` | `24` | 403/429 冷却时长；免费额度按出口 IP 且 UTC 日重置，可调短 |
| `MAX_SWITCHES`（`--max-switches`） | `3` | 单请求换号重试上限 |
| `OC_UPSTREAM_TIMEOUT` | `600` | 单次上游生成超时（秒） |
| `OC_DENY_TOOLS` | `1` | 置 0 可放开 agent 工具（不建议） |
| `PROBE_MODEL` | `mimo-v2.6-flash-free` | 体检用模型 |
| `BRIDGE_DEBUG` | `0` | 置 1 开启 `/debug/state`、`/debug/cooldown` |
| `BRIDGE_PORT` | `8790` | 监听端口 |
| `OC_PORT_BASE` | `14100` | serve 实例端口基数 |

## API

| 端点 | 说明 |
| --- | --- |
| `POST /v1/chat/completions` | OpenAI 兼容对话，支持 `stream` |
| `GET /v1/models` | 上游全量模型清单 |
| `GET /` | 账号体检页面 |
| `GET /api/status` | 账号台账与摘要（JSON） |
| `GET /api/logs` | 最近 200 条请求流水 |
| `POST /api/accounts/{name}/probe` | 体检单账号，健康且在冷却则自动解冻 |
| `POST /api/accounts/{name}/cooldown` | `{"minutes":1440}` 冷却；`0` 解冻 |
| `POST /api/accounts/{name}/release` | 释放该账号的 resin 租约 |
| `GET /healthz` | 存活检查 |

## 安全

本服务**不做鉴权**，且 agent 沙箱内运行的是上游模型驱动的 agent loop——
务必只部署在内网，对外一律前置带鉴权的网关（例如 NovaVeil 渠道直连本桥），
不要把本服务的端口直接发布到公网。
