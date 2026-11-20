# 练习手册

所有命令都在仓库根目录执行。练习以“理解行为”为目标，`git diff` 只保留真实想提交的改动。

## 0.1 make 命令速解

本手册反复出现下面三条命令，先看懂它们再往下走：

```bash
TERM=xterm make help
make test WHAT=./pkg/registry/core/pod GOFLAGS=-v
KUBE_TEST_ARGS='-run ^TestPodStrategyValidate$' make test WHAT=./pkg/registry/core/pod GOFLAGS=-v
```

| 命令                                                            | 做什么                           | 关键点                                                                                                                                                                                                            |
|-----------------------------------------------------------------|----------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `TERM=xterm make help`                                          | 列出所有可用 make 目标，不改文件 | `TERM=xterm` 只对这一条命令生效；帮助脚本用 `tput`，无终端环境里 `TERM` 为空会报错                                                                                                                                |
| `make test WHAT=./pkg/registry/core/pod GOFLAGS=-v`             | 跑该包的全部单测                 | `WHAT=` 限定范围，不写会枚举整个 workspace；`GOFLAGS=-v` 原样传给 go test；脚本默认带 `-race`、超时和 cache mutation detector                                                                                     |
| `KUBE_TEST_ARGS='-run ^TestPodStrategyValidate$' make test ...` | 只跑名字精确匹配的那个测试       | `KUBE_TEST_ARGS` 经 `Makefile:50` 的 `.EXPORT_ALL_VARIABLES` 传给 `test.sh`，最终变成 go test 参数；`^`/`$` 是正则锚点，防止误匹配到 `TestPodStrategyValidateUpdate`（同文件 1983 行）；单引号防止 shell 展开 `$` |

实际链路：`Makefile` → `hack/make-rules/test.sh` → `gotestsum` → `go test`。学习阶段永远带 `WHAT`，否则全仓库单测会吃掉整个下午。

递进关系：第 1 条是“有哪些命令可用”，第 2 条是“这个包整体没坏”，第 3 条是“精准复现某个测试的行为”。日常改代码先用第 3
条做快速反馈，改完再跑第 2 条确认没波及其他测试。

## 0. 环境准备

```bash
go version
TERM=xterm make help
git status --short
```

集成测试需要 etcd：

```bash
hack/install-etcd.sh
# 按脚本输出执行，例如：
export PATH="${PATH}:${PWD}/third_party/etcd"
etcd --version
```

不要手动 `go mod tidy`。依赖调整走 `hack/pin-dependency.sh` + `hack/update-vendor.sh`。

## 1. 第一个闭环：Pod strategy

目标：观察“测试 → 生产代码 → 定向验证”的最短回路。命令各参数的含义见 §0.1。

```bash
make test WHAT=./pkg/registry/core/pod GOFLAGS=-v
KUBE_TEST_ARGS='-run ^TestPodStrategyValidate$' make test WHAT=./pkg/registry/core/pod GOFLAGS=-v
```

精读顺序：

1. `pkg/registry/core/pod/strategy_test.go:1885`
2. `pkg/registry/core/pod/strategy.go:125`
3. `pkg/apis/core/validation/validation.go:5746`
4. `staging/src/k8s.io/apiserver/pkg/registry/rest/create.go:103`

练习：

- 在 `strategy_test.go` 增加一个有业务含义的边界用例，先让断言失败
- 用最小改动修复 `strategy.go` 或补充输入约束
- 不要为覆盖率添加机械断言；不要改 `zz_generated.*`
- 新增 `.go` 文件时复制 `hack/boilerplate/boilerplate.go.txt` 的 license header

验证：

```bash
make test WHAT=./pkg/registry/core/pod GOFLAGS=-v
make verify WHAT="boilerplate gofmt test-code typecheck"
```

复盘问题：

- 这个错误在 apiserver 哪一层被返回？
- `PrepareForCreate` 与 `Validate` 谁先执行？
- 如果这是 validating admission，应写在哪一层？

## 2. 请求链 trace

目标：不看文档，按调用栈复述 POST 处理。

在下列函数设置断点或加临时日志，记录入参与返回：

- `staging/src/k8s.io/apiserver/pkg/endpoints/handlers/create.go:53`
- `staging/src/k8s.io/apiserver/pkg/registry/generic/registry/store.go:564`
- `staging/src/k8s.io/apiserver/pkg/registry/rest/create.go:103`
- `pkg/registry/core/pod/strategy.go:88`
- `staging/src/k8s.io/apiserver/pkg/storage/etcd3/store.go:293`

输出一张表：

| 顺序 | 函数 | 修改对象了吗 | 能拒绝请求吗 | 状态落点 |
| --- | --- | --- | --- | --- |
| 1 | `createHandler` | mutating admission 可能修改 | 是 | 请求上下文 |
| 2 | `BeforeCreate` | `PrepareForCreate` 可能修改 | 是 | 内存对象 |
| 3 | validating admission | 否 | 是 | 内存对象 |
| 4 | `Storage.Create` | 否 | 是 | etcd |

不要提交临时日志。练习结束后 `git diff --check`，确认工作区干净。

## 3. 集成测试：Pod 生命周期

```bash
make test-integration WHAT=./test/integration/pods GOFLAGS=-v KUBE_TEST_ARGS='-run ^TestPodUpdateActiveDeadlineSeconds$'
make test-integration WHAT=./test/integration/deployment GOFLAGS=-v
make test-integration WHAT=./test/integration/scheduler GOFLAGS=-v
```

精读：

- `test/integration/pods/pods_test.go:217`
- `test/integration/deployment/deployment_test.go:124`
- `test/integration/scheduler/scheduler_test.go:304`
- `test/integration/framework/main_test.go`
- `test/integration/framework/test_server.go`
- `test/integration/framework/controlplane_utils.go`

练习：

- 记录 `test-integration.sh` 启动的 etcd 与 control plane 组件
- 在 `TestPodUpdateActiveDeadlineSeconds` 旁标出每一步 API 写入与期望状态变化
- 对比 `TestDeploymentRollingUpdate`：哪些断言需要真 apiserver，单测里的 fake client 无法覆盖
- 对 `TestMultipleSchedulingProfiles` 画出 informer cache → scheduling queue → binding 的路径

失败排查：

- `etcd: command not found`：执行 `hack/install-etcd.sh` 并按输出导出 PATH
- 端口占用 / 残留进程：检查 integration 框架清理逻辑，确认没有遗留控制面进程
- 工作树不干净影响生成物类 verify：先保存实验改动，再用干净 worktree 重跑

## 4. client-go 最小控制器

在仓库外的临时目录创建独立 Go module，避免把练习代码混进 Kubernetes 工作区：

```bash
mkdir -p /tmp/k8s-study-client && cd /tmp/k8s-study-client
go mod init example.com/pod-watch
```

程序要求：

1. 用 `clientcmd.BuildConfigFromFlags` 读取 kubeconfig
2. 用 `kubernetes.NewForConfig` 创建 clientset
3. 对某 namespace 的 Pod 做一次 `List`，随后 `Watch`
4. 将 ADDED / MODIFIED / DELETED 事件打印成一行日志
5. Ctrl-C 能干净退出

对照文件：

- `staging/src/k8s.io/client-go/tools/clientcmd/client_config.go:681`
- `staging/src/k8s.io/client-go/kubernetes/clientset.go:500`
- `staging/src/k8s.io/client-go/kubernetes/typed/core/v1/pod.go:68`
- `staging/src/k8s.io/client-go/rest/request.go:1142`
- `staging/src/k8s.io/sample-controller/controller.go`

验收：能解释 `Watch` 与 `Informer` 的差异，以及为什么控制器一般不在事件回调里长耗时 reconcile。

## 5. kubelet / CRI 静态 trace

本机不做 Node E2E，先画完整链路：

```text
NewSourceApiserver
  → PodConfig.Updates
  → syncLoopIteration
  → HandlePodAdditions
  → podWorkers.UpdatePod
  → podWorkerLoop
  → Kubelet.SyncPod
  → kubeGenericRuntimeManager.SyncPod
  → createPodSandbox / startContainer
  → remoteRuntimeService.RunPodSandbox / CreateContainer / StartContainer
  → CRI api.proto
```

精读 `pkg/kubelet/kubelet.go:2858` 时，为每类事件写一行：来源、触发的 SyncPod 类型、是否可能重入、错误如何处理。

选做：在 Linux 且具备 password-less sudo / 容器运行时的机器上，阅读 `test/e2e_node/README.md` 后再尝试：

```bash
make test-e2e-node FOCUS=Kubelet SKIP=container
```

macOS 或权限不足时不要硬跑；把它记成环境前置条件，不是代码问题。

## 6. 生成物实验

只在临时分支或干净 worktree 做，避免用实验 diff 污染主工作区。

```bash
git status --short
# 改一个 +k8s: tag 输入，例如 types.go 上的生成标记
make update SILENT=false
git diff --stat
make verify WHAT="codegen gofmt boilerplate"
```

观察：

- `hack/make-rules/update.sh:38` 的 `BASH_TARGETS` 依次运行了什么
- 哪些 `zz_generated.*` 被重写
- `hack/lib/verify-generated.sh` 为什么用干净 worktree 对比 diff

实验结束后恢复该 worktree，保留笔记，不保留无意义生成物改动。

## 7. 一次完整改动

选一个足够小的问题，例如某条 Pod 校验错误信息不够具体，或某个边界用例缺失。按以下顺序：

```bash
git diff --check
make test WHAT=./pkg/registry/core/pod GOFLAGS=-v
# 如涉及 apiserver / controller / scheduler 协作：
make test-integration WHAT=./test/integration/pods GOFLAGS=-v
make verify WHAT="boilerplate gofmt test-code typecheck"
make verify
```

提交前写清：

- 问题与根因，不用“something went wrong”
- 改动影响面：API、行为、兼容性、测试
- 运行过的命令与结果
- 是否使用 AI、如何使用
- 人类作者仍需对全部提交负责，并回看 `CONTRIBUTING.md`

## 8. E2E / conformance 阅读练习

不要求先有真集群。阅读以下路径并回答：

- `test/e2e/e2e_test.go`：测试是怎样被聚合进 suite 的？
- `test/e2e/framework/framework.go`、`test_context.go`：kubeconfig 与 provider 配置从哪来？
- `test/e2e/common/node/pods.go:230`：为什么 `framework.ConformanceIt` 会影响 `testdata/conformance.yaml`？
- `test/conformance/walk_test.go`：清单校验如何发现新增 / 删除的 conformance 测试？
- `test/cmd/legacy-script.sh`：`WHAT=deployment` 为什么能找到 `run_deployment_tests`？

有集群后再：

```bash
make WHAT=test/e2e/e2e.test
hack/ginkgo-e2e.sh
make test-cmd WHAT="deployment impersonation"
```

新增 / 删除 conformance 测试要按 `test/conformance/README.md` 处理 golden list，并留意 sig-architecture review 要求；不要把这些清单当普通生成物擅自忽略 diff。
