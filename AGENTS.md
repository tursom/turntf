# Repository Guidelines

## Proto

- 修改 `proto/` 后，在 `turntf/` 仓库根目录运行 `./scripts/gen-proto.sh`。
- 将同次变更生成的 `internal/proto/*.pb.go` 与 proto 修改一并提交。

## 验证

- 仓库 CI 当前会执行 `go test ./... -count=1` 和 `./scripts/smoke.sh`。
- 本地提交前至少运行 `go test ./... -count=1`；如果改动影响启动流程、认证、HTTP API 或其他基础可用路径，再补跑 `./scripts/smoke.sh`。

## 提交

- 提交信息使用 Conventional Commits，格式为 `<type>: <summary>` 或 `<type>(<scope>): <summary>`。
- 提交摘要可以使用中文或英文，但应简洁并准确描述改动。

## 语言

- 计划和文档使用中文。
