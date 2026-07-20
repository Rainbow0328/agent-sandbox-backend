# Changelog

本项目遵循语义化版本号。正式版本发布后，同一版本号不会重复构建或覆盖。

## 0.1.0 - 2026-07-20

- 首次公开版本，只支持 OpenSandbox Provider。
- 提供 `create_opensandbox_backend()` 高层入口。
- 默认使用 `python:3.12` 镜像和 Sandbox SQLite History。
- 支持生命周期、文件、命令、上传、下载和 Deep Agents Backend 适配。
- 支持 Actor、Thread、Run、Correlation 标识及 Web Console History 数据。
- 支持用户级多 Agent 并发限制、生命周期 Gate 和文件读写锁。
- 支持本地上传根目录限制、敏感文件排除、路径逃逸和归档安全检查。
- 支持 OpenSandbox 原生 Sandbox TTL 和独立 SDK 延迟清理 TTL。
