# 流内容观测协议 v2

宿主与 recorder 协商 `gpt-load.observation.v2.msgpack`。现有请求头、响应头和请求结果继续保存；新增 `upstream.stream` 记录转换前的 SSE 响应正文和 WebSocket 双向应用消息。宿主与 recorder 需要一起更新。

## 采集位置

- **SSE**：HTTP 内容解压后，SSE 解析、模型字段补齐及协议翻译前。按业务消费者实际读取的字节采集，每块最多 32 KiB；块边界可穿过 UTF-8 字符、行和 SSE 事件。
- **WebSocket 请求方向**：完成序列化后交给上游写入函数的应用消息，写入返回后发布，附 `write_result`。
- **WebSocket 响应方向**：库完成消息重组/解压后、业务校验和翻译前，包含已读到的未知或无效应用消息。
- 方向以宿主的上游连接为准：`request` 为宿主发往上游，`response` 为上游发回宿主。WebSocket 保存文本/二进制应用消息，握手仍通过既有事件关联。

内容采集随现有观测订阅启用，独立于请求日志正文开关。未订阅时跳过载荷快照和编码，流计数保留，以便识别中途订阅的缺失前缀。

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