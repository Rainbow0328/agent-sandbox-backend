# Agent Sandbox Backends

面向 AI Agent 的 OpenSandbox Backend SDK。第一版只支持阿里开源的 OpenSandbox，目标是让
用户用少量配置创建一个可直接交给 Deep Agents 的 Backend，同时在 Sandbox 内维护 Web
Console 可读取的完整操作历史。

SDK 不实现 Sandbox Runtime，也不要求 Web Console 在线。SDK 与 Web Console 不直接通信；
两者通过同一个 Sandbox 及其中的 SQLite History 数据协作。

## 安装

```powershell
pip install agent-sandbox-backends
```

需要 Deep Agents 适配器时：

```powershell
pip install "agent-sandbox-backends[deepagents]"
```

源码开发安装：

```powershell
pip install -e ".[deepagents]"
```

## 快速开始

OpenSandbox Service 未开启鉴权时不需要 API Key：

```python
from agent_sandbox_backends import create_opensandbox_backend


backend = await create_opensandbox_backend(
    "http://your-opensandbox-service:8080",
    sandbox_name="research-workspace",
)
```

高层入口的第一版默认值：

- `api_key=None`：Service 未开启鉴权时无需设置。
- `sandbox_name=None`：可选显示名称，写入 Sandbox metadata。
- `image="python:3.12"`：默认创建 Python 3.12 Sandbox。
- `history=HistoryConfig(mode=HistoryMode.SANDBOX)`：历史写入 Sandbox 内 SQLite。
- `sandbox_ttl_seconds=None`：不覆盖 OpenSandbox 的原生默认 TTL。
- `use_server_proxy=True`：默认通过 OpenSandbox Service 代理访问 Sandbox。
- `cleanup=CleanupPolicy.ON_CLOSE`：关闭 Backend 时删除由它创建的 Sandbox。
- `workdir="/workspace"`：默认工作目录。

URL 必须是 OpenSandbox Service 根地址，可写为 `host:port` 或完整的 `http/https` URL；
不要附加 `/v1`、查询参数、URL 凭证或 Web Console 路径。

OpenSandbox 不提供独立的 Sandbox 名称字段。SDK 会把 `sandbox_name` 保存为
`agent_sandbox.name` metadata，供列表和 Web Console 显示。名称允许重复，所有生命周期、
文件、命令和 History 操作仍必须使用服务生成的 `sandbox_id`；界面遇到同名项时应同时展示
短 Sandbox ID。名称最长 128 个字符，支持中文，但不能包含控制字符。

## Deep Agents 集成

```python
from agent_sandbox_backends import CleanupPolicy, create_opensandbox_backend
from agent_sandbox_backends.integrations.deepagents import as_deepagents_backend
from deepagents import create_deep_agent


core_backend = await create_opensandbox_backend(
    "http://your-opensandbox-service:8080",
    cleanup=CleanupPolicy.ON_CLOSE,
)
backend = as_deepagents_backend(core_backend)
agent = create_deep_agent(model=model, backend=backend)
```

适配器支持文件查看、读取、写入、编辑、搜索、上传、下载和命令执行。模型侧返回的命令
输出受 `CommandResultConfig` 限制，但 Sandbox History 仍按 History 配置保存完整原始输出。

## 必要参数

常用创建参数如下：

```python
from agent_sandbox_backends import (
    CleanupPolicy,
    ConcurrencyConfig,
    HistoryConfig,
    HistoryMode,
    create_opensandbox_backend,
)


backend = await create_opensandbox_backend(
    "https://your-opensandbox-service.example",
    api_key=None,
    image="python:3.12",
    workdir="/workspace",
    env={"APP_ENV": "development"},
    metadata={"project": "example"},
    cleanup=CleanupPolicy.ON_CLOSE,
    sandbox_ttl_seconds=None,
    use_server_proxy=True,
    request_timeout_seconds=30,
    ready_timeout_seconds=30,
    concurrency=ConcurrencyConfig(
        max_parallel_commands=4,
        max_parallel_file_reads=32,
        max_parallel_file_writes=8,
        max_parallel_uploads=1,
    ),
    history=HistoryConfig(
        mode=HistoryMode.SANDBOX,
        ttl_days=7,
        max_database_bytes=128 * 1024 * 1024,
        max_operation_output_bytes=16 * 1024 * 1024,
    ),
)
```

`request_timeout_seconds` 控制普通 OpenSandbox 请求超时；`ready_timeout_seconds` 控制创建
Sandbox 后等待其服务就绪的时间。网络不可达时单纯增加 readiness 超时通常无效，应检查
`use_server_proxy` 和 OpenSandbox Server 的 Docker `host_ip` 配置。

### Sandbox TTL 与清理 TTL

`sandbox_ttl_seconds` 是 OpenSandbox 服务端 Sandbox 生命周期：

```python
backend = await create_opensandbox_backend(
    "http://your-opensandbox-service:8080",
    sandbox_ttl_seconds=3600,
)
```

- 不传或传 `None`：SDK 不向 OpenSandbox 传递 `timeout`，使用 OpenSandbox 自身默认值。
- 当前固定依赖的 OpenSandbox 0.1.14 默认值是 600 秒。
- 传入正数：转换为 OpenSandbox `timeout=timedelta(seconds=...)`。
- 不能传 `0` 或负数；如果希望永久保留，应使用 OpenSandbox 自身支持的明确配置方式，
  SDK 不会把 `None` 转换成 OpenSandbox 的“手动清理”语义。

`cleanup_ttl_seconds` 是另一项独立配置，只在 `cleanup=CleanupPolicy.TTL` 时使用，表示调用
`backend.close()` 后，SDK 进程等待多久再主动删除 Sandbox：

```python
backend = await create_opensandbox_backend(
    "http://your-opensandbox-service:8080",
    cleanup=CleanupPolicy.TTL,
    cleanup_ttl_seconds=60,
)
```

该延迟清理任务依赖当前 Python 进程继续运行，不替代 OpenSandbox 服务端 TTL。通常优先使用
OpenSandbox 的 `sandbox_ttl_seconds`；只有确实需要“Backend 关闭后延迟删除”时才使用
`cleanup_ttl_seconds`。

## 历史策略

高层入口默认将历史写入 Sandbox 内的 `/.agent-history/history.sqlite3`。记录包含操作类型、状态、
完整参数、结果、stdout/stderr、时间、`sandbox_id`、`sandbox_instance_id`、`actor_type`、
`actor_id`、`thread_id`、`run_id` 和 `correlation_id`，供 Web Console 按 Sandbox 查询。

设置 Agent 身份：

```python
with backend.agent_context(
    agent_id="research-agent",
    thread_id="thread-1",
    run_id="run-1",
):
    await backend.execute("python --version")
```

默认历史配置：

- `ttl_days=7`：查询或清理触发时删除过期记录。
- `max_database_bytes=128 MiB`：超过限制时按策略删除最旧历史。
- `max_operation_output_bytes=16 MiB`：限制单次操作保存的输出。
- `capture_stdout=True`、`capture_stderr=True`：保存完整命令输出直到达到配置上限。

显式关闭历史：

```python
from agent_sandbox_backends import HistoryConfig, HistoryMode

backend = await create_opensandbox_backend(
    "http://your-opensandbox-service:8080",
    history=HistoryConfig(mode=HistoryMode.NONE),
)
```

使用 `CleanupPolicy.ON_CLOSE` 删除 Sandbox 时，其中的历史数据库也会一起删除。需要关闭
Backend 后继续让 Web Console 查看时，应使用 `CleanupPolicy.NEVER`，并由用户或 Console
显式清理 Sandbox。

## 文件上传安全

SDK 不会默认允许任意本地路径上传。用户必须声明允许读取的本地根目录：

```python
from pathlib import Path

from agent_sandbox_backends import UploadConfig, UploadSpec, create_opensandbox_backend


backend = await create_opensandbox_backend(
    "http://your-opensandbox-service:8080",
    uploads=(UploadSpec(source="./project", target="/workspace/project"),),
    upload_config=UploadConfig(allowed_local_roots=(Path.cwd(),)),
)
```

默认排除 `.env`、`.ssh`、`.aws`、`.git`、虚拟环境等敏感或无关内容，并限制 Sandbox
写入根目录。上传实现包含路径逃逸、符号链接、特殊文件、归档炸弹、校验和及回滚保护。

## 并发与异常

默认并发限制适合用户级、多子 Agent 共享同一 Sandbox 的场景。命令、文件读写和上传分别
限流；文件写入使用 Keyed RW Lock；关闭和删除会等待活动操作并阻止新操作进入。

常用异常可直接从顶层导入：

```python
from agent_sandbox_backends import (
    CommandQueueTimeoutError,
    FileNotFoundError,
    ProviderError,
    SandboxBackendError,
    SandboxNotFoundError,
    UploadPolicyError,
)

try:
    result = await backend.execute("python --version")
except CommandQueueTimeoutError:
    ...
except SandboxNotFoundError:
    ...
except ProviderError as error:
    if error.retryable:
        ...
except SandboxBackendError:
    ...
```

Provider 异常会统一携带可用的 `provider_name`、`sandbox_id`、`operation`、
`provider_error_code`、`provider_request_id` 和 `retryable` 信息。

Sandbox History 会保存完整命令字符串和业务操作参数。API Key 不会写入 History，但不要把
密码、Token 等敏感值直接拼进命令字符串或 `metadata`；优先通过 Sandbox 环境变量或挂载的
秘密管理机制传递，并限制 History 数据库和 Web Console 的访问权限。

## 底层入口

`create_backend()` 仍作为扩展和内部测试入口保留，可传入 Provider 实例、Registry、自定义
History Store 和 History Transport。普通 OpenSandbox 用户优先使用
`create_opensandbox_backend()`，以获得稳定默认值和 URL 校验。

## 本地检查

```text
uv sync
uv run ruff check .
uv run pyright
uv build
```

## 安全与版本

- 安全漏洞请按照 `SECURITY.md` 私下报告。
- 版本变化见 `CHANGELOG.md`。
