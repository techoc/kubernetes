# 贡献前约束与 PR 自检

这里只收录本仓库 `AGENTS.md`、`CONTRIBUTING.md` 与本方案直接相关的约束。完整社区流程以 `CONTRIBUTING.md` 指向的 Contributor's Guide 为准。

## 1. 写代码前

- 变更保持聚焦、可 review；不要顺手重构无关代码
- 先判断改动落在 `pkg/`、`cmd/` 还是 `staging/src/k8s.io/`，别在 `vendor/` 改 `k8s.io/*`
- `staging/src/k8s.io/` 是 `k8s.io/*` 源码真相；staging 不得 import `k8s.io/kubernetes`
- 包名小写、单词、与目录一致
- 每个 `.go` 文件都要有 `hack/boilerplate/boilerplate.go.txt` 的 license header
- 注释解释 why，不解释一眼能懂的 what
- 错误信息具体、可行动；不要输出“something went wrong”
- 测试要有真实行为价值，不写只为覆盖率服务的琐碎单测

## 2. 生成文件与依赖

| 不要做 | 正确做法 |
| --- | --- |
| 手改 `zz_generated.*`、`generated.pb.go` | 改输入，再 `make update` |
| 手改 `go.mod` / `go.work` / `go.sum` | `hack/pin-dependency.sh` + `hack/update-vendor.sh` |
| `go mod tidy` | `hack/update-vendor.sh` |
| 从 staging 反向 import 主仓库 | 把公共逻辑放在合适的 `k8s.io/*` 模块或 `pkg/` |
| 改 `vendor/k8s.io/client-go` | 改 `staging/src/k8s.io/client-go` |

`make update` 会改文件：运行前 `git status --short`，运行后 `git diff --stat`，确认只有预期生成物。

## 3. 测试选择

| 变更类型 | 最低验证 |
| --- | --- |
| 纯策略 / 校验 / util | 对应包 `make test WHAT=...` |
| API 类型 / scheme / storage | 包单测 + `make test-integration WHAT=./test/integration/apiserver ...` + `make verify` |
| admission / auth | 包单测 + 相关 integration + `make verify WHAT="boilerplate gofmt test-code typecheck"` |
| controller / scheduler | 包单测 + 对应 `test/integration/...` + 必要时性能 / 调度测试 |
| kubelet / CRI | 包单测 + 能运行时的 node E2E 子集；环境不足要记录约束 |
| 生成 tag / feature gate / docs / OpenAPI | `make update` 后检查 diff，再 `make verify` |
| 依赖 / vendor | `hack/update-vendor.sh` + `make verify WHAT=vendor` |
| CLI | `make test-cmd WHAT=...` 及对应单测 |

不要提交已知错误 commit 再追加 fixup；review 中的修复要 squash 回对应 commit。

## 4. PR 自检清单

```bash
git diff --check
git status --short
make test WHAT=./pkg/registry/core/pod GOFLAGS=-v          # 换成实际改动包
make test-integration WHAT=./test/integration/pods GOFLAGS=-v  # 涉及组件协同时
make verify WHAT="boilerplate gofmt test-code typecheck"
make update SILENT=false                                   # 仅当生成输入变更
git diff --stat
make verify
```

提交说明里写：

- 问题、根因、行为变化、兼容性影响
- 运行过的测试命令与结果
- 是否使用 AI、如何使用
- 提醒人类作者对全部提交负责，并回看 `CONTRIBUTING.md`

不要写：

- `@mentions` 或 `fixes #...` 关键词
- `Co-authored-by:`
- 已知错误提交 + 后续 fixup 的历史

## 5. 提交前最后五问

1. 这个改动是否落在正确的源码层：`pkg/`、`cmd/` 还是 `staging/`？
2. 是否碰了生成文件或依赖文件？如果是，是否通过对应 update 重新生成？
3. 测试是否覆盖真实行为，而不是只让覆盖率数字好看？
4. 错误信息、注释、包名是否符合仓库约束？
5. PR 描述是否说明 AI 使用情况，并明确人类作者责任？

先签 CLA，再走社区流程。学习阶段可以只做本地实验；提交到上游是另一件事，别让练习 diff 混进 PR。
