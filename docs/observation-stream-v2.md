# 流内容观测协议 v2

宿主与 recorder 协商 `gpt-load.observation.v2.msgpack`。现有请求头、响应头和请求结果继续保存；新增 `upstream.stream` 记录解析及翻译前的 SSE 响应正文、CPA 转换后的 WebSocket 出站应用消息，以及业务校验和翻译前的 WebSocket 入站应用消息。宿主与 recorder 需要一起更新。

## 采集位置

### 请求头与响应头

以下位置针对 gpt-load 的 Codex CPA 执行链路。原始源码基线为 gpt-load `df9c69633d56a9f144a5bdaad99bf01a086ec184`、CPA `7357dfa88ffa9f1b0a9e0796fa07b2ef0cfb54b8`；下文同时说明账号隐私处理加入后的发送末端行为。其他 provider 的采集位置应依据各自执行链路确认。

| 事件与传输 | 采集时机 | 记录值及后续处理 |
| --- | --- | --- |
| `upstream.request`，HTTP/SSE | CPA 完成请求构造后，进入 `noRedirectRoundTripper.RoundTrip` 时；先发布观测，再调用内层 transport | 已经过 CPA 的鉴权、账号、选取头部及 routing hint 等处理。后续 `codexHeadersRoundTripper` 仍会固定 User-Agent/Version、整理 Session-Id，并应用显式配置的 Originator；底层 HTTP transport 还会完成协议发送处理。 |
| `upstream.request`，WS 握手 | gpt-load 桥接预处理头部后保存快照，在 `GotConn` 回调中发布 | 快照来自传给 CPA 的 `Options.Headers`，包含桥接的身份及会话整理。CPA 随后构造的实际握手头另有字段筛选、账号鉴权、routing hint、Beta 和模型头覆盖，WebSocket 库再生成握手协议头。回调发生时间与快照所代表的处理阶段需分别理解。 |
| `upstream.response`，HTTP/SSE | 内层 transport 返回上游 `http.Response` 后，在 `noRedirectRoundTripper.RoundTrip` 中读取 `resp.Header` | 保存 HTTP 客户端收到并解析的上游响应头，早于 CPA 后续响应处理和 gpt-load 面向下游的头部筛选。 |
| `upstream.response`，WS 握手 | CPA 拨号获得握手结果后，由桥接 `completeHandshake` 发布 | 保存 WebSocket 客户端取得的上游握手响应头；成功升级与取得响应头的握手失败均按实际结果记录，早于下游头部处理。 |

因此，分析请求头时应使用对应的采集阶段：HTTP 记录代表 CPA 构造结果，WS 记录代表桥接输入快照。核实最终发出的所有头部需要发送末端或受控上游接收侧的观测。例如 WS 快照中的 `Via`、`X-Forwarded-For`、`X-Forwarded-Host` 只证明桥接输入中存在这些字段，实际转发取决于后续 CPA 头部构造。

响应头表示上游客户端解析后的字段值；HTTP 头名大小写、字段排列以及协议编码等线级细节由客户端库处理。观测中的响应头也早于下游响应筛选，客户端最终收到的字段应结合下游处理判断。

账号隐私标识在选定凭据后、CPA 构造请求前派生。HTTP 发送末端及 WS 实际拨号前还会补齐已派生的支持头部、从当前账号 CookieJar 生成 Cookie，并注入当前账号和逻辑轮次的 turn-state。因此 HTTP 请求头事件及 WS 桥接快照可能尚未体现这些末端操作；验证最终 Cookie、路由状态与头部一致性，应使用受控上游接收值。HTTP 转换路径的 installation metadata 也会在发送末端补齐。

内部路由状态在响应头下游筛选前读取；HTTP 的 `codex.response.metadata` 在解压后、CPA 解析和协议翻译前读取，WS metadata 在业务事件交付时读取。读取保持响应字节原样，独立于观测订阅，单条 `response.completed` 保留逻辑轮次状态。观测协议与流内容的原始采集语义继续沿用下文约定。

### 流正文

- **SSE**：HTTP 内容解压后，SSE 解析、模型字段补齐及协议翻译前。按业务消费者实际读取的字节采集，每块最多 32 KiB；块边界可穿过 UTF-8 字符、行和 SSE 事件。
- **WebSocket 请求方向**：CPA 完成正文转换、最终规则处理及序列化后，交给上游写入函数的应用消息，写入返回后发布，附 `write_result`。采集点位于 WebSocket 帧封装、掩码和传输压缩之前。
- **WebSocket 响应方向**：库完成消息重组/解压后、业务校验和翻译前，包含已读到的未知或无效应用消息。
- 方向以宿主的上游连接为准：`request` 为宿主发往上游，`response` 为上游发回宿主。WebSocket 保存文本/二进制应用消息，握手仍通过既有事件关联。

同一条 WS 请求的头部快照与请求正文位于不同处理阶段：头部是 CPA 构造握手前的桥接快照，正文是 CPA 转换后的出站应用消息。`upstream.stream` 的 SSE 采集范围为响应方向；协议 v2 此项扩展未增加 HTTP/SSE 入站或出站请求正文采集。分析客户端原始请求与上游请求之间的字段变化，需要另外取得转换前的请求正文。

内容采集随现有观测订阅启用，独立于请求日志正文开关。未订阅时跳过载荷快照和编码，流计数保留，以便识别中途订阅的缺失前缀。

### 采集时间与事件顺序

`observed_at` 表示采集侧记录事件的时间，recorder 收到事件后落盘。队列投递和网络传输会发生在两者之间。WS 请求消息在写入调用返回后发布，因此响应记录可能先于相应的请求消息记录出现；使用连接、执行尝试及流内序号关联数据，跨方向全局 `seq` 按发布顺序解释。

采集位置源码：gpt-load `third_party/cpaembedded/embedded/embedded.go` 中的 `executionContext`、`noRedirectRoundTripper.RoundTrip`；`codex_headers.go` 中的 `codexHeadersRoundTripper.RoundTrip`；`codex_websocket.go` 中的 `requestHeaders` 快照、`GotConn` 回调与 `completeHandshake`。CPA 最终头部构造位于 `internal/runtime/executor/codex_executor_request.go`、`codex_websockets_request.go`。

## 内容事件

事件外层沿用 `boot_id`、全局 `seq`、`observed_at`、`type` 和 `data`。`type` 为 `upstream.stream`，字段在 `data` 内：

| 字段 | 含义 |
| --- | --- |
| `protocol` | 业务协议，例如 `openai-responses` |
| `transport` | `sse` 或 `websocket` |
| `direction` | `request` 或 `response` |
| `kind` | `data` 或 `end` |
| `stream_sequence` | data 的流内序号，从 1 开始 |
| `byte_offset` | 当前 data 前累计的原始 payload 字节数，从 0 开始 |
| `body_size` | 当前 data 的原始字节数，正文被省略时仍保留 |
| `body` | MessagePack binary；decode 的 JSONL 中表现为 base64 字符串 |
| `message_type` | WebSocket data 为 `text` 或 `binary` |
| `write_result` | WebSocket request data 为 `success` 或 `error` |
| `last_stream_sequence` | end 记录的最后 data 序号，空流为 0 |
| `total_body_bytes` | end 记录的累计原始 payload 字节数 |
| `end_reason` | 流方向结束的原因 |
| `close_code` | 已知时保存 WebSocket 关闭代码 |

SSE 流由 `(boot_id, request_id, attempt_id, exchange_sequence, direction)` 标识。WebSocket 流由 `(boot_id, upstream_connection_id, direction)` 标识，计数跨同连接的业务轮次持续。消息归属明确时附请求、执行尝试、传输及凭证身份；无法确认的消息仅保留连接级关联。

`write_result=success` 表示本地写入成功返回；`error` 保留本次尝试发送的原始消息。请求方向的偏移和计数包括失败发送。跨方向全局 seq 表示发布顺序，响应可能在发送调用返回前到达。

## 结束与完整性

SSE 结束原因包括 `eof`、`read_error`、`canceled`、`closed_early`。WebSocket 包括 `closed`、`read_error`、`write_error`、`canceled`，每个连接方向各结算一次。业务终态继续使用原始事件及 `request.finished`，与传输结束分别解释。

恢复 SSE 正文时，逐流检查以下条件并拼接 base64 解码后的 body：

1. 第一个 data 的 `stream_sequence=1`、`byte_offset=0`。
2. 后续序号连续，偏移等于此前 `body_size` 累计和。
3. 每个 body 的长度等于 `body_size`，所有记录都没有 `body_omitted`。
4. end 的最后序号及累计字节数与 data 闭合。

WebSocket 使用相同检查，但每个 data 对应一条独立消息，保留边界。空消息的 body 是空二进制且 body_size=0；正文省略的记录缺少 body 并带显式省略标记。

若 end 缺失，则结束状态未知；若出现偏移缺口或省略标记，则内容不完整。SSE `closed_early`/错误记录可证明已读范围闭合，完整上游响应仍需结合结束原因及业务终态判断。文件记录边界完整或 `request.finished` 成功只描述各自层面。

## 预算与投递

沿用有界队列及尽力投递：默认每订阅 200 条、4,000,000 字节，单事件 1,048,576 字节，发送超时 5 秒。宿主参数为 `OBSERVATION_QUEUE_MAX_EVENTS`、`OBSERVATION_QUEUE_MAX_BYTES`、`OBSERVATION_EVENT_MAX_BYTES`、`OBSERVATION_WRITE_TIMEOUT_SECONDS`。

流内容会增加事件量。慢消费或断线时队列可淘汰旧事件；单事件过大时依次省略正文、产生 oversized 降级。流内计数、偏移和原始大小在投递前产生，因此留下可识别缺口。SSE 块大小有界，WebSocket 超大载荷沿用业务消息上限及观测预算。

文件继续采用四字节大端长度加原始 MessagePack，权限保持 0600。敏感正文进入授权观测流和文件，生命周期诊断省略正文与凭证。通用 decode 导出事实，完整性校验与领域解析由研究工具承担。

## 源码依赖

CPA 观察接口维护在 [LakeLink/CLIProxyAPI](https://github.com/LakeLink/CLIProxyAPI/commit/7357dfa88ffa9f1b0a9e0796fa07b2ef0cfb54b8) fork 中，提供解压后的 HTTP 正文观察和 WebSocket 原始读写/结束观察。宿主与嵌入桥通过各自 `go.mod` 中相同的固定版本替换引用该 fork；Docker 使用标准模块下载流程。采集关联、分块和容量策略继续由嵌入桥与宿主负责。
