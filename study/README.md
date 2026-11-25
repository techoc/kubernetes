# Kubernetes 源码学习方案

## 起点

- 仓库快照：`master`，`v1.38.0-beta.0-494-g94841abaf20`
- 工具链：`Go 1.27.4 darwin/arm64`；`go.mod` 要求 `go 1.27.0`
- 学习方式：以一条 Pod 生命周期为主线，穿插 API 机制、控制平面、节点、测试与贡献流程
- 周期：10 周，每周 6–8 小时；压缩版见 `06-progress.md`
- 原则：先跑定向测试，再读代码；先画调用链，再谈架构名词；不手改生成文件

本方案只写入 `study/`，不读取、不修改、不删除仓库中的其他学习资料。

## 文档导航

| 文件                 | 用途                                              |
|----------------------|---------------------------------------------------|
| `02-curriculum.md`   | 10 周目标、精读路径、每周产出与验收标准           |
| `01-code-map.md`     | 目录边界、Pod 创建链路、函数级代码地图            |
| `07-mental-model.md` | 接口的骨架作用、GVK/GVR 与内外部类型、术语速查    |
| `08-gotchas.md`      | 22 条反直觉清单：以为是什么、实际是什么、怎么验证 |
| `03-exercises.md`    | 从单元到集成测试、可复现实验、Debug 任务          |
| `04-workflow.md`     | 构建、测试、verify、update、生成物与依赖边界      |
| `05-contribution.md` | 贡献前约束、PR 自检清单、review 处理              |
| `06-progress.md`     | 打卡表、能力矩阵、阶段复盘问题                    |
| `11-translation-plan.md` | 全仓库中文注释计划：边界、批次、守卫与生成物同步 |
| `12-translation-progress.md` | 中文注释翻译进度表，与学习进度分开 |

建议阅读顺序：`02-curriculum.md` → `07-mental-model.md` → `01-code-map.md` → `03-exercises.md`。`08-gotchas.md`
放在手边当纠偏清单，每读完一个子系统扫一遍。`04-workflow.md` 在第 1 周就要扫一遍，真正动手前再精读。

## 核心主线

```
kubectl create -f pod.yaml
  → client-go REST / typed client
  → kube-apiserver handler chain
  → mutating admission
  → Pod strategy + validation + validating admission
  → etcd
  → informer / scheduler / binding
  → kubelet pod worker
  → CRI RunPodSandbox / CreateContainer / StartContainer
```

控制器只负责把 `Deployment`、`ReplicaSet` 等上层期望状态收敛成 Pod，不是直接创建 Pod 的必经路径。方案会专门用一条支线讲清 reconcile 与声明式 API。

仓库没有 `kubectl create pod` 子命令；`staging/src/k8s.io/kubectl/pkg/cmd/create/create.go:102` 的 `NewCmdCreate` 注册的是
`deployment`、`job` 等。练习统一用 `kubectl create -f pod.yaml` 或 typed client。两条客户端路径的详细对照见
`01-code-map.md` §2.1。

## 学习产出

每周留下一个可复查的工件，不要只做阅读笔记：

1. 一张调用链图：输入、输出、关键函数、状态落点
2. 一段最小测试或 trace：说明为什么链路上某个分支会走 / 不会走
3. 三个问题：已解决、仍困惑、下一步要验证
4. 一份 diff 摘要：如果改了代码，写清影响面与验证命令

## 硬约束

- `staging/src/k8s.io/` 是 `k8s.io/*` 源码真相；staging 不得反向 import `k8s.io/kubernetes`
- `zz_generated.*`、`generated.pb.go` 只读；修改输入后跑 `make update`
- `go.mod` / `go.work` 是生成文件；依赖变更用 `hack/pin-dependency.sh` + `hack/update-vendor.sh`，禁止 `go mod tidy`
- 每个 `.go` 文件需要 `hack/boilerplate/boilerplate.go.txt` 的 license header
- 注释解释 why，不解释一眼能懂的 what；错误信息要具体、可行动
- 不写只为覆盖率服务的测试；改动保持聚焦、可 review

## 环境提醒

- `make help` 在无 `TERM` 环境可能因 `tput` 失败；用 `TERM=xterm make help`
- `make test-integration` 需要 PATH 中有 etcd；先运行 `hack/install-etcd.sh` 并按输出导出 PATH
- `test/e2e_node` 需要 Linux 与 password-less sudo；macOS 阶段只做代码阅读
- `make verify` 是全量检查，耗时较长；日常用 `make verify WHAT="boilerplate gofmt test-code typecheck"`
- `make update` 会改文件，运行前先 `git status --short`，运行后检查 `git diff`

## 第一周第一小时

```bash
TERM=xterm make help
make test WHAT=./pkg/registry/core/pod GOFLAGS=-v
KUBE_TEST_ARGS='-run ^TestPodStrategyValidate$' make test WHAT=./pkg/registry/core/pod GOFLAGS=-v
```

然后打开 `pkg/registry/core/pod/strategy_test.go:1885` 与 `pkg/registry/core/pod/strategy.go:125`。先看断言，再看 `Validate` 如何把规则接进 apiserver。这就是整份方案的入口。
