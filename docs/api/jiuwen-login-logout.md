# ADX Jiuwen 账号接口：登录、登出与额度查询

本文描述当前 ADX Ingress 的 Jiuwen 登录、登出与免费额度查询接口，供客户端对接。示例中的 `<gateway-host>` 是实际配置的 Jiuwen 网关域名，`<business-token>` 是登录响应中的 `token`。

## 调用流程

1. 客户端通过华为 Account Kit 获取一次性授权码，或从当前 AGC Auth 用户获取 access token。授权包含 `openid profile` 时，后端可从已验签 Account Kit ID Token 的 `display_name` 读取华为昵称；仅授权 `openid` 的用户仍可登录。
2. 客户端把凭据提交给对应入口：`POST /api/v1/auth/huawei/login` 兑换 Account Kit 授权码；`POST /api/v1/auth/agc/login` 验证 AGC 用户 token。两者都建立 ADX 业务会话并返回相同格式的 Bearer token。
3. 客户端在 `/ws`、文件上传下载和额度查询等业务请求中发送 `Authorization: Bearer <business-token>`。Ingress 根据会话确定用户身份。
4. 用户退出时调用 `POST /api/v1/auth/logout`，撤销当前 token，并清除客户端保存的 token。

ADX 只接收客户端已经获取的 Account Kit 授权码或 AGC 用户 access token；不提供原 jiuwenswarm 的 `/api/v1/auth/authorize`、`/api/v1/auth/callback`、`/api/v1/auth/claim` 浏览器授权流程，也不通过 Cookie 返回业务会话。

原 jiuwenswarm 也有 `POST /api/v1/auth/logout`，但它使用 Cookie 或 `X-Auth-Session`，并要求 `X-Jiuwen-Auth: 1`。ADX 的同名路由使用 Bearer 业务 token，成功返回 204；客户端不能直接复用原登出调用。`POST /api/v1/auth/huawei/login` 则是 ADX 新增的接口。

## 华为 Account Kit 登录

`POST /api/v1/auth/huawei/login`

请求头：`Content-Type: application/json`。登录时无需业务 Bearer token。不接受 URL 查询参数。

请求体：

```json
{
  "authorizationCode": "<华为 Account Kit 授权码>",
  "agreementVersion": "1"
}
```

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| `authorizationCode` | string，必填 | 客户端从华为 Account Kit 获取的授权码；不能为空。 |
| `agreementVersion` | string，可选 | **首次注册时必填**，值须与服务端当前协议版本一致；已有账号登录可不传。 |

请求体不接受其他字段。成功后返回 `200 OK`、`Content-Type: application/json` 和 `Cache-Control: no-store`：

```json
{
  "userId": "<ADX 用户 ID>",
  "token": "<业务会话 token>",
  "tokenType": "Bearer",
  "expiresIn": 604800
}
```

`expiresIn` 的单位是秒，由部署配置决定；当前代码默认 604800 秒（7 天）。每次成功登录会建立一条独立会话，不会自动撤销其他设备的会话。登录阶段不向客户端下发 LiteLLM key，也不会仅因登录就创建 Sandbox。后端保存已验签的华为昵称，重新登录时刷新；未返回有效昵称时保留已知昵称。请求体不接受客户端自报昵称，成功响应格式不变。

调用示例：

```bash
curl -i 'https://<gateway-host>/api/v1/auth/huawei/login' \
  -H 'Content-Type: application/json' \
  --data '{"authorizationCode":"<authorization-code>","agreementVersion":"1"}'
```

## AGC Auth 登录

`POST /api/v1/auth/agc/login`

请求头和成功响应与 Account Kit 登录相同。登录时无需业务 Bearer token，不接受 URL 查询参数。请求体为：

```json
{
  "agcAccessToken": "<AGC 用户 accessToken>",
  "agreementVersion": "1"
}
```

`agcAccessToken` 必须是 1～8192 字节、不含空白的可打印 ASCII 字符。它来自当前 AGC Auth 用户的 `getToken(true)`，不是应用级 token、Account Kit 授权码或 ADX 业务 token。请求体不接受手机号、验证码、AGC UID、本地 owner ID、ADX user ID、项目或租户；Gateway 只使用服务端验证得到的 UID。

```bash
curl -i 'https://<gateway-host>/api/v1/auth/agc/login' \
  -H 'Content-Type: application/json' \
  --data '{"agcAccessToken":"<agc-user-access-token>","agreementVersion":"1"}'
```

当前 Gateway 直接匹配带版本前缀的 `/api/v1/auth/agc/login`。客户端的 `authBaseUrl` 若已是 `https://<gateway-host>/api/v1`，只追加 `/auth/agc/login`，不要再重复添加 `/api/v1`。

Ingress 使用固定的 AGC 中国区服务地址和部署配置的项目/API client 凭据：先获取短期服务凭据，再对每次用户登录调用官方 `verify-token` 接口检查有效性与撤销状态。验证结果必须属于配置项目，且 JWT 的 `iss`、`aud`、`sub`、`exp` 必须与该项目和远端返回 UID 一致。JWT 中的 `agc.sign_in_provider` 不作为账号类型判断依据，因为它不等同于客户端 SDK 的匿名账号标记。服务端只缓存服务凭据，不缓存用户 token 或验证结果。

每次验票失败只记录一条脱敏告警，包含失败阶段、分类原因，以及能够取得的上游 HTTP 状态和 `ret.code`；不记录用户 token、UID、资料或服务端凭据。

AGC 身份按“ADX tenant + provider `agc` + AGC project ID + 已验证 UID”独立映射。它不会因为 UID、手机号或端侧 owner ID 与 Account Kit 身份相同而自动合并；两种 provider 的历史资源默认彼此独立。

## 登出

`POST /api/v1/auth/logout`

请求头：`Authorization: Bearer <business-token>`。无需请求体，不接受 URL 查询参数。

```bash
curl -i -X POST 'https://<gateway-host>/api/v1/auth/logout' \
  -H 'Authorization: Bearer <business-token>'
```

成功返回 `204 No Content`，响应体为空，并带有 `Cache-Control: no-store`。登出只撤销该 token 对应的业务会话；重复登出同一格式有效的 token 也返回 204。其他设备的会话、用户账号、LiteLLM key 和已有 Sandbox 不会因此删除。

登出后，新业务请求无法再使用该 token。已经建立的 WebSocket 会在下一次会话检查或会话到期时关闭；会话检查间隔为 30 秒，因此登出不保证 WebSocket 在 HTTP 响应返回的瞬间断开。

## 查询额度

`GET /api/v1/auth/quota` 使用登录响应中的业务 token 查询本人模型额度。

### 请求

```http
GET /api/v1/auth/quota HTTP/1.1
Host: <gateway-host>
Authorization: Bearer <business-token>
```

- 无请求体，不接受 URL 查询参数。
- 必须使用 `Authorization: Bearer <business-token>`；ADX 管理密钥、华为授权码、文件 token 和 `X-Auth` 均不能代替它。
- 如果请求携带 `Origin`，其值须在网关允许列表中。

调用示例：

```bash
curl -i 'https://<gateway-host>/api/v1/auth/quota' \
  -H 'Authorization: Bearer <business-token>'
```

### 成功响应

返回 `200 OK`、`Content-Type: application/json` 和 `Cache-Control: no-store`。示例：

```json
{
  "ok": true,
  "available": true,
  "quota": {
    "total": 5.0,
    "balance": 3.75,
    "used": 1.25,
    "low_balance": false,
    "exhausted": false,
    "reset_period": "",
    "reset_at": ""
  }
}
```

| 字段 | 类型 | 含义 |
| --- | --- | --- |
| `ok` | boolean | 请求成功时为 `true`。 |
| `available` | boolean | 当前成功响应固定为 `true`；**不要用它判断是否仍有额度**。 |
| `quota.total` | number | LiteLLM key 的 `max_budget`，按 1:1 数值显示为积分。 |
| `quota.used` | number | LiteLLM key 的 `spend`，按 1:1 数值显示为已用积分。 |
| `quota.balance` | number | `total - used`，保留到小数点后最多 6 位；超额时可能为负数。 |
| `quota.low_balance` | boolean | `total > 0` 且已使用至少 90% 时为 `true`。 |
| `quota.exhausted` | boolean | 总额和已用量均可读取，且 `balance <= 0` 时为 `true`。客户端应优先检查此字段。 |
| `quota.reset_period` | string | LiteLLM key 的 `budget_duration`；一次性额度返回空字符串。 |
| `quota.reset_at` | string | 有重置周期时返回 LiteLLM 的 `budget_reset_at`；一次性额度返回空字符串。 |

如果 LiteLLM 没有返回有效的 `max_budget` 或 `spend`，对应的 `total`、`used` 和无法计算的 `balance` 使用 `-1` 表示未知。此时 `exhausted` 不会仅因数据缺失而变为 `true`；客户端不要把 `-1` 当作可用余额。

**计量口径：** 这些数字是积分展示值，取自 LiteLLM 的金额预算与消费记录，并非输入/输出 token 数量。当前代码按 1:1 显示 LiteLLM 的预算数值，没有进行 token 换算。仓库[账号配置示例](../../gateway/examples/accounts.json)设置 `max_budget: 5.0`、`budget_duration: "1d"`，表示每天 5 积分。配合 [MaaS 统一计价配置](../../build/config/examples/litellm-maas.md)，每百万输入加输出 token（含缓存输入）计 1 积分，对应每日 500 万 token；此时 spend 不代表上游实际美元成本。LiteLLM 1.104.2 按配置的 UTC 零点重置，即北京时间 08:00；并发或进行中的请求可能超额。实际额度由部署配置和用户现有 LiteLLM key 决定，已有 Key 需通过管理 API 单独更新。

### 查询时的行为

接口先验证业务会话，再查询该用户的 LiteLLM key 信息。如果用户尚无 key，首次查询会创建并保存该用户的 key，然后读取额度；**查询本身不扣除额度，也不创建 Sandbox**。已有 key 的预算规则不会因部署配置改动而自动重置。准备凭据时会将该用户 key 的模型权限更新为 `all-proxy-models`，覆盖 LiteLLM 当前及后续新增模型；Account Kit 用户的 `key_alias` 为 `huawei-<华为昵称>-<ADX用户ID前8位>`，没有可用昵称时为 `huawei-<ADX用户ID前8位>`；AGC 用户当前不导入昵称，别名为 `agc-<ADX用户ID前8位>`。昵称只保留字母、数字、中文等 Unicode 字母数字以及空格、下划线和连字符，最长 128 字符。更新只改变模型权限和显示别名，保留 key、预算、消费记录和限流。模板声明决定 Jiuwen 展示的模型列表。旧用户需再次完成华为登录才能取得此前未保存的昵称，之后查询额度或准备新绑定时同步别名；旧业务 token 也会读取账户最新昵称。暖沙箱调用不会触发别名同步。

账户 schema 保持未发布的 v1。全新环境应用 `gateway/src/ingress/accounts/schema.sql`；若开发环境已有旧版 v1 表，由 schema owner 直接补充可空的 `users.display_name` 字段后再启动新版 Ingress，不新增 v2 版本标记或迁移文件，已有用户、会话和加密 Key 保持不变。Key 显示名称不会自动改变 LiteLLM 原生限流错误；客户端错误使用别名需要另外配置 LiteLLM 错误回调。

## 常见错误

错误响应为 JSON，例如：

```json
{"error":"UNAUTHORIZED","code":"UNAUTHORIZED"}
```

| HTTP 状态 | `code` | 场景 |
| --- | --- | --- |
| 400 | `INVALID_LOGIN_REQUEST` | 登录 JSON 格式、字段或授权码不合法；登录请求体过大。 |
| 400 | `INVALID_QUERY` | 任一接口的 URL 带查询参数。 |
| 401 | `UNAUTHORIZED` | 登录时华为授权码或身份验证失败；登出、额度查询缺少或使用了无效、过期、已撤销的业务 token。 |
| 401 | `INVALID_AGC_ACCESS_TOKEN` | AGC 用户 token 无效、过期、已撤销或属于其他项目。 |
| 403 | `AGREEMENT_REQUIRED` | 首次注册未提供当前协议版本。 |
| 403 | `ACCOUNT_DISABLED` | 已映射的业务账号被禁用或删除。 |
| 403 | `FORBIDDEN` | 额度查询时 LiteLLM key 与当前用户不匹配。 |
| 403 | `ACCESS_DENIED` | 请求的 `Host` 或可选的 `Origin` 不在网关配置允许范围。 |
| 405 | `METHOD_NOT_ALLOWED` | 登录、登出未使用 POST，或额度查询未使用 GET。 |
| 415 | `JSON_REQUIRED` | 登录未使用 `application/json`。 |
| 503 | `AUTH_UNAVAILABLE` | 华为或 AGC 身份服务、AGC API client 授权、账号存储或 LiteLLM 管理接口不可用，或认证、额度处理超时。 |

业务 token 只用于 ADX 业务接口认证，不是华为授权码、AGC 用户 token、文件 token 或 LiteLLM key。客户端应保存登录响应中的 token，并在登出后删除本地副本。
