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
- **账号体检页**（`GET /`，5 秒自刷）：
  - 摘要卡 + 账号台账（状态、体检结论与详情、出口 IP/节点、成败计数、
    成功天数、最近错误）+ 最近请求流水；
  - 体检：单账号体检、一键测试台账（并发 1-20 可调），结论与模型回答落台账；
    测试模型下拉选择（自动筛选 `-free` 模型，默认 `deepseek-v4.1-flash-free`）；
  - 生成新账号：一键生成 x 个 `oc-xxxxxxxx` 账号并发体检，健康自动入池，
    被拒冷却 24h 并停实例省内存（解冻或体检时按需拉起）；
  - 自动体检：可开关与设间隔，健康自动入池（解冻），被拒冷却 24h；
  - 账号增删：运行期添加手动账号（持久保存，按需启动 serve 实例），
    手动账号可删除；
  - 代理设置：页面修改 resin 地址/平台/token，先连通性测试再保存，
    保存后逐账号滚动重启 serve 生效；
- **冷却与重置自适应**：免费额度按出口 IP 且 UTC 日重置，被拒账号可手动
  解冻，或等自动体检在下轮拉回。

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
| `RESIN_PROXY_TOKEN` | 必填 | resin 代理 token，代理通道（CONNECT/模型拉取）用 |
| `RESIN_ADMIN_TOKEN` | 空（可选） | resin 管理 token：用于 403/429 后释放租约（冷却期满换新出口 IP）与页面显示账号出口 IP；不填则仅保留冷却，账号会粘回原出口 |
| `RESIN_PLATFORM` | `Default` | resin 平台名（用户名为 `平台.账号`） |
| `BRIDGE_ACCOUNTS` | `bridge-1,bridge-2,bridge-3` | 初始账号列表（来源=配置），每号一个 serve 实例 |
| `COOLDOWN_HOURS` | `24` | 403/429 冷却时长；免费额度按出口 IP 且 UTC 日重置，可调短 |
| `MAX_SWITCHES`（`--max-switches`） | `3` | 单请求换号重试上限 |
| `OC_UPSTREAM_TIMEOUT` | `600` | 单次上游生成超时（秒） |
| `OC_DENY_TOOLS` | `1` | 置 0 可放开 agent 工具（不建议） |
| `PROBE_MODEL` | `deepseek-v4.1-flash-free` | 默认体检模型（页面下拉可改，选择会持久化） |
| `MAX_ACCOUNTS` | `10` | 账号总数上限（含生成与手动添加） |
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
| `POST /api/probe` | 并发体检：`{model, concurrency, accounts?}`，缺省测全部 |
| `POST /api/probe/generate` | 生成新账号并体检：`{count, model, concurrency}` |
| `POST /api/auto` | 自动体检开关与间隔：`{enabled, interval_hours, concurrency}` |
| `POST /api/accounts/add` | 添加手动账号：`{names: ["a","b"]}` |
| `POST /api/accounts/{name}/delete` | 删除手动账号（配置来源不可删） |
| `POST /api/accounts/{name}/cooldown` | `{"minutes":1440}` 冷却；`0` 解冻 |
| `POST /api/accounts/{name}/release` | 释放该账号的 resin 租约 |
| `GET /api/proxy` | 当前代理配置（token 掩码） |
| `POST /api/proxy/test` | 连通性测试，返回出口 IP |
| `POST /api/proxy` | 保存代理配置并滚动重启 serve（保存前先测试） |
| `GET /healthz` | 存活检查 |

落盘状态（`STATE_DIR`）：`accounts.json` 台账（账号/体检/成败统计/冷却）、
`proxy.json` 代理配置、`auto.json` 自动体检配置。

## 安全

本服务**不做鉴权**，且 agent 沙箱内运行的是上游模型驱动的 agent loop——
务必只部署在内网，对外一律前置带鉴权的网关（例如 NovaVeil 渠道直连本桥），
不要把本服务的端口直接发布到公网。
