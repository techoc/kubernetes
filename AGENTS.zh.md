# Kubernetes

## 沟通偏好

- 冷峻、简洁、低调的幽默。不吹捧，不硬玩梗。省去开场白和收尾语。
- 注释解释"为什么"，而不是"是什么"。
- 错误信息：具体、可执行。不要输出含糊的"出了点问题"。

## 约束

- **生成文件只读。** 绝不手工编辑 `zz_generated.*` 或 `generated.pb.go`。运行 `make update`。
- **`go.mod`/`go.work` 是生成文件。** 使用 `hack/pin-dependency.sh` + `hack/update-vendor.sh`。绝不用 `go mod tidy`。
- **对 `k8s.io/*` 而言，staging 是唯一事实来源**（`staging/src/k8s.io/`）。不要从 staging 中导入 `k8s.io/kubernetes`。
- **必须有样板头。** 每个 `.go` 文件都需要 `hack/boilerplate/boilerplate.go.txt` 里的许可证头。

## 贡献者指南

- 保持改动聚焦、易于审查
- 添加或更新相关测试
- 提交信息中不要出现 `@mentions` 或 `fixes #...` 之类的关键字
- 提交信息中不要添加 `Co-authored-by:`

## 命令

运行 `make help` 查看所有可用目标。常用工作流：

```
make test WHAT=./pkg/kubelet GOFLAGS=-v     # 单元测试（单个包）
make test-integration WHAT=./test/integration/scheduler
make verify                                 # 全部校验检查
make update                                 # 所有生成器和格式化工具
```

## 风格

- 包名：小写、单个单词、与目录名一致。
