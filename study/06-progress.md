# 进度与自评

复制本表到自己的笔记里，或直接在仓库工作副本上勾选。不要用“已读”糊弄自己，勾选项都要能当场演示。

## 周打卡

| 周 | 主题 | 工件 | 验收命令 / 问题 | 完成 |
| --- | --- | --- | --- | --- |
| 1 | 项目地图与开发回路 | 依赖方向图 + 命令副作用表 | `TERM=xterm make help`；解释 `make update` 为什么有副作用 | [ ] |
| 2 | API 模型与 client-go | GVK 对照表 + watch 小程序 | 解释 scheme / typed client / dynamic client 分工 | [ ] |
| 3 | apiserver 请求链 | POST 时序图 + strategy 单测 | `KUBE_TEST_ARGS='-run ^TestPodStrategyValidate$' make test WHAT=./pkg/registry/core/pod GOFLAGS=-v` | [ ] |
| 4 | Pod storage 与 admission | subresource 表 | 解释 `Strategy` 与 `Store` 的职责边界 | [ ] |
| 5 | 控制器与调度器 | 状态机图 + 插件扩展点表 | `make test-integration WHAT=./test/integration/deployment GOFLAGS=-v` | [ ] |
| 6 | kubelet 主循环 | channel 事件表 + 事件流图 | 解释 per-Pod worker 的作用 | [ ] |
| 7 | CRI 生命周期 | RPC 对照表 + 启动时序图 | 从 `SyncPod` 追到 `StartContainer` | [ ] |
| 8 | 测试金字塔 | 决策树 + 一次完整验证记录 | 解释 conformance 清单治理 | [ ] |
| 9 | 生成物与扩展 | 输入 → 生成器 → 输出映射表 | `make update SILENT=false` 后能解释 `git diff` | [ ] |
| 10 | 子系统结业 | 30 分钟讲稿 + PR 风格描述 | 能讲清一个子系统的边界、状态、失败路径 | [ ] |

## 能力矩阵

每项 0–3 分：0 没碰过，1 能复述，2 能定位代码，3 能改并验证。

| 能力 | 0 | 1 | 2 | 3 | 证据 |
| --- | --- | --- | --- | --- | --- |
| 区分 `cmd/` / `pkg/` / `staging/` / `vendor/` | [ ] | [ ] | [ ] | [ ] | |
| 解释 GVK、Scheme、conversion、defaulting | [ ] | [ ] | [ ] | [ ] | |
| 画出 POST 到 etcd 的完整顺序 | [ ] | [ ] | [ ] | [ ] | |
| 定位 Pod validation 与 admission 代码 | [ ] | [ ] | [ ] | [ ] | |
| 解释 informer / workqueue / reconcile | [ ] | [ ] | [ ] | [ ] | |
| 画出 scheduling cycle 与 binding cycle | [ ] | [ ] | [ ] | [ ] | |
| 画出 kubelet syncLoop 到 CRI 的路径 | [ ] | [ ] | [ ] | [ ] | |
| 选择正确的单测 / integration / e2e 层级 | [ ] | [ ] | [ ] | [ ] | |
| 安全运行 `make update` 并审查生成 diff | [ ] | [ ] | [ ] | [ ] | |
| 写出符合仓库约束的 PR 描述 | [ ] | [ ] | [ ] | [ ] | |

## 主链路口试题

不看文件，能回答 8/12 才算主线过关：

1. `staging/src/k8s.io/client-go` 为什么不能去 `vendor/` 改？
2. `kubectl create -f pod.yaml` 的 dynamic 路径与 typed client 路径在哪分叉、在哪汇合？
3. `DefaultBuildHandlerChain` 与 create handler 里的 admission 链分别做什么？
4. mutating admission、`PrepareForCreate`、静态 validation、validating admission 的顺序是什么？
5. `pkg/registry/core/pod/strategy.go` 与 `generic/registry/store.go` 各负责什么？
6. `pkg/registry/core/rest/storage_core.go` 怎样把 `pods` 路径接到 REST storage？
7. 直接创建 Pod 会不会经过 Deployment controller？为什么？
8. scheduler 的 `Bind` 插件最终写回哪个 API？服务端由谁处理？
9. kubelet 为什么需要 per-Pod worker？`syncLoopIteration` 主要消费哪些事件？
10. CRI 的三个启动相关 RPC 是什么，分别由哪个 kubelet 文件调用？
11. `make verify WHAT="gofmt typecheck"` 的 `WHAT` 值为什么不带脚本前缀和扩展名？
12. 改了 `+k8s:` tag 后，为什么不能只手改 `zz_generated.*`？

## 阶段复盘

每两周写 10 行以内：

- 最意外的一个调用顺序
- 一个曾经以为存在、实际不存在的命令或路径
- 一次测试失败的真实原因
- 一个仍无法解释的状态变化
- 下一阶段准备用哪个测试验证它

## 结业标准

- 能在 30 分钟内徒手画出 Pod 创建到容器启动的调用链
- 能说出链路上每一层的输入、输出、错误与状态落点
- 能独立完成“小改动 → 定向测试 → verify → PR 风格描述”
- 能准确列出本仓库的生成文件、依赖与 staging 约束
- 能选一个子系统继续深入，而不是继续横向扫目录

到此还觉得 Kubernetes 很简单，通常说明还没跑过 `make verify`。
