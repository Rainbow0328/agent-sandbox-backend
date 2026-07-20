# Security Policy

## Supported Versions

当前仅维护最新发布版本。

## Reporting a Vulnerability

请优先使用 GitHub 仓库的 **Private vulnerability reporting** 或 Security Advisory 私下报告。
不要在公开 Issue 中提交可直接利用的漏洞细节、真实凭证、私网地址或 Sandbox 数据。

报告建议包含：

- 受影响版本和运行环境。
- OpenSandbox 部署方式及是否启用 Service Proxy。
- 最小复现步骤。
- 预期行为与实际行为。
- 可能影响的文件、命令、History 或凭证范围。

本项目会优先处理路径逃逸、任意宿主机文件读取、凭证泄露、Sandbox 隔离绕过、History
越权读取和未经授权的命令执行问题。
