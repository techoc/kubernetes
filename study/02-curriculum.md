# 10 周课程表

节奏：每周 6–8 小时，2 小时读代码、2 小时跑测试 / trace、1–2 小时写图与笔记、1 小时复盘。每周以“可演示”为完成标准，不以“读了多少文件”为标准。

## 阶段 A：跑得起来，看得懂边界（第 1–2 周）

### 第 1 周：项目地图与开发回路

目标：知道源码放哪、命令从哪进、测试怎样跑、生成物为何不能手改。

精读：

- `AGENTS.md`、`CONTRIBUTING.md`、`README.md:35`
- `07-mental-model.md`、`08-gotchas.md` 先扫一遍，后面几周带着它们读代码
- `Makefile:127`、`Makefile:160`、`Makefile:186`、`Makefile:208`、`Makefile:524`
- `hack/make-rules/test.sh`、`hack/make-rules/verify.sh`、`hack/make-rules/update.sh`
- `go.work:9`、`go.mod:220`、`vendor/modules.txt`

动手：

```bash
TERM=xterm make help
make test WHAT=./pkg/registry/core/pod GOFLAGS=-v
make verify WHAT="boilerplate gofmt test-code"
```

产出：

- 一张 `cmd/`、`pkg/`、`staging/`、`vendor/` 依赖方向图
- 一张 `make test / test-integration / verify / update` 副作用表，标出谁会改文件
- 说明为什么 `make update` 前要检查 `git status --short`

验收：能解释 `staging/src/k8s.io/client-go` 与 `vendor/` 的关系；能说出至少三个生成物目录。

### 第 2 周：API 模型与 client-go

目标：理解 Scheme、GVK、版本转换、LIST/WATCH、workqueue，能自己写一个最小控制器。

精读：

- `staging/src/k8s.io/api/core/v1/types.go`、`register.go`
- `pkg/apis/core/types.go`、`pkg/apis/core/install/install.go:34`、`pkg/api/legacyscheme/scheme.go`
- `staging/src/k8s.io/apimachinery/pkg/runtime/scheme.go:100`
- `staging/src/k8s.io/client-go/rest/request.go:242`
- `staging/src/k8s.io/client-go/tools/cache/controller.go`
- `staging/src/k8s.io/client-go/util/workqueue/queue.go`
- `staging/src/k8s.io/sample-controller/controller.go`

本周对照 `07-mental-model.md` §1 的"六张面孔"，确认自己说的 GVK、GVR、外部类型、内部类型不是同一个东西。

动手：

- 用 `client-go` 写一个独立小工具：列出某 namespace 的 Pod，并 watch 变化
- 在纸上画出 `Reflector → DeltaFIFO → Informer cache → handler → workqueue → reconcile`
- 对照 `staging/src/k8s.io/sample-controller/hack/update-codegen.sh`，看 `+k8s:` tag 与生成物的关系

产出：一份 Pod GVK ↔ Go type ↔ REST path 对照表；一个能编译运行的最小 watch 程序。

验收：能说清 typed client、dynamic client、scheme、codec 各自解决什么问题。

## 阶段 B：沿主链路钻进控制面（第 3–5 周）

### 第 3 周：apiserver 请求链

目标：徒手画出 `POST /api/v1/namespaces/{ns}/pods` 从路由到 etcd 的顺序。

精读：

- `cmd/kube-apiserver/app/server.go:74` → `pkg/controlplane/instance.go:321` → `pkg/controlplane/apiserver/apis.go:88`
- `staging/src/k8s.io/apiserver/pkg/server/genericapiserver.go:885`
- `staging/src/k8s.io/apiserver/pkg/endpoints/installer.go:288`、`staging/src/k8s.io/apiserver/pkg/endpoints/installer.go:1309`
- `staging/src/k8s.io/apiserver/pkg/server/config.go:1076`
- `staging/src/k8s.io/apiserver/pkg/endpoints/handlers/create.go:53`
- `staging/src/k8s.io/apiserver/pkg/registry/generic/registry/store.go:564`
- `staging/src/k8s.io/apiserver/pkg/registry/rest/create.go:103`
- `staging/src/k8s.io/apiserver/pkg/storage/etcd3/store.go:293`

动手：精读 `pkg/registry/core/pod/strategy_test.go:1885`，跑单测定向验证；给 `TestPodStrategyValidate` 增加一个真实边界用例，只测试行为，不刷覆盖率。

产出：一张带 mutating / validating admission、strategy、storage 的时序图；说明校验失败时对象在哪一层被拒绝。

验收：能解释 `PrepareForCreate`、`Validate`、`Canonicalize` 分别在何时调用。

### 第 4 周：Pod 资源语义、storage 与 admission

目标：理解一个资源是怎样被“装配”出来的，而不是只有通用 handler。

精读：

- `pkg/registry/core/rest/storage_core.go:140`、`pkg/registry/core/rest/storage_core.go:237`
- `pkg/registry/core/pod/storage/storage.go:81`
- `pkg/registry/core/pod/strategy.go:68`
- `pkg/apis/core/validation/validation.go:5746`
- `pkg/kubeapiserver/options/admission.go:59`
- `staging/src/k8s.io/apiserver/pkg/admission/chain.go:31`
- `staging/src/k8s.io/pod-security-admission/`

动手：在 `pkg/registry/core/pod/storage/storage_test.go` 找与 binding / status 相关的测试；对照 `test/integration/apiserver/apiserver_test.go`，理解真实 apiserver 与 etcd 下的行为。

产出：一张 `Pod / status / binding / ephemeralcontainers / resize` subresource 表，标注每个接口的写入语义。

验收：能说清 `Strategy` 和 `Store` 的职责边界；能指出 Pod Security 是 admission 而非字段校验。

### 第 5 周：控制器与调度器

目标：把声明式 API 的“期望状态 → 实际状态”闭环补齐。

精读：

- `cmd/kube-controller-manager/app/controllermanager.go:117`
- `cmd/kube-controller-manager/app/controller_descriptor.go:106`
- `pkg/controller/deployment/deployment_controller.go:575`
- `pkg/controller/replicaset/replica_set.go:759`
- `pkg/scheduler/scheduler.go:327`
- `pkg/scheduler/eventhandlers.go:254`
- `pkg/scheduler/schedule_one.go:68`
- `pkg/scheduler/framework/interface.go`
- `pkg/scheduler/framework/runtime/framework.go:1091`
- `pkg/scheduler/framework/plugins/defaultbinder/default_binder.go:52`

动手：

```bash
make test-integration WHAT=./test/integration/deployment GOFLAGS=-v
make test-integration WHAT=./test/integration/scheduler GOFLAGS=-v
```

若本机缺 etcd，先执行 `hack/install-etcd.sh` 并按输出导出 PATH；macOS 上跑不了所有 node 相关用例时，记录失败原因，不绕过约束。

产出：一张 `Deployment → ReplicaSet → Pod → scheduling queue → binding` 的状态机图；列出 scheduler 插件扩展点及触发顺序。

验收：能解释为什么直接创建 Pod 不需要 Deployment controller；能定位 `BindPod` 写回 API 的服务端处理位置。

## 阶段 C：到节点上（第 6–7 周）

### 第 6 周：kubelet 主循环

目标：理解 apiserver 里的 Pod 对象如何变成节点上的期望状态。

精读：

- `cmd/kubelet/app/server.go:544`
- `pkg/kubelet/config/apiserver.go:36`
- `pkg/kubelet/config/config.go:61`
- `pkg/kubelet/kubelet.go:1965`、`pkg/kubelet/kubelet.go:2783`、`pkg/kubelet/kubelet.go:2995`
- `pkg/kubelet/pod_workers.go:770`
- `pkg/kubelet/kubelet.go:2135`
- `pkg/kubelet/kubelet_pods.go`

动手：精读 `pkg/kubelet/kubelet_pods_test.go`；把 `syncLoopIteration` 的每种 channel 事件写成输入 / 输出表；运行 `make test WHAT=./pkg/kubelet GOFLAGS=-v` 的定向子集。

产出：一张 kubelet 内部事件流图，标出 `PodConfig`、pod worker、status manager、runtime 的边界。

验收：能解释 per-Pod worker 为什么存在，以及 `SyncPod` 被调用的几种来源。

### 第 7 周：CRI 与容器生命周期

目标：把 kubelet 的抽象调用映射到 CRI RPC。

精读：

- `pkg/kubelet/container/runtime.go`
- `pkg/kubelet/kuberuntime/kuberuntime_manager.go:1837`
- `pkg/kubelet/kuberuntime/kuberuntime_sandbox.go:42`
- `pkg/kubelet/kuberuntime/kuberuntime_container.go:213`
- `staging/src/k8s.io/cri-client/pkg/remote_runtime.go:309`
- `staging/src/k8s.io/cri-api/pkg/apis/runtime/v1/api.proto:24`
- `test/e2e_node/container_lifecycle_test.go`

动手：只做代码级 trace：从 `SyncPod` 到 `RunPodSandbox`、`CreateContainer`、`StartContainer`；记录每一步的错误如何冒泡。Node E2E 只阅读 `test/e2e_node/README.md` 与 runner，不强求在本机运行。

产出：一张容器启动时序图；一份 CRI 方法 ↔ kubelet 调用点 ↔ proto 定义对照表。

验收：能说清 CRI 边界两侧的仓库目录，以及为什么 `api.pb.go` 不能手改。

## 阶段 D：工程化与扩展（第 8–10 周）

### 第 8 周：测试金字塔

目标：知道什么该写单测，什么必须上 integration / e2e / conformance。

精读：

- `hack/make-rules/test.sh:30`、`hack/make-rules/test-integration.sh:30`
- `test/integration/framework/main_test.go`、`test_server.go`、`controlplane_utils.go`
- `test/integration/pods/pods_test.go:217`
- `test/e2e/e2e_test.go`、`test/e2e/framework/framework.go`
- `test/e2e/common/node/pods.go:230`
- `test/conformance/README.md`、`test/conformance/walk_test.go`、`testdata/conformance.yaml`
- `test/cmd/legacy-script.sh`、`hack/make-rules/test-cmd.sh`

动手：完成 `03-exercises.md` 的“一次完整改动”练习；用 `make test-cmd WHAT="deployment impersonation"` 观察命令行测试如何起本地控制面。

产出：一张测试选择决策树；记录一次故意失败 → 定位 → 修复 → 定向验证 → 完整 verify 的过程。

验收：能解释 conformance 不是独立框架，而是 `framework.ConformanceIt` 清单治理。

### 第 9 周：生成物、Feature Gate、扩展机制

目标：改 API tag / feature gate / CRD 时知道该动哪里、该跑哪个 update。

精读：

- `hack/make-rules/update.sh:38`、`hack/update-codegen.sh`
- `hack/lib/verify-generated.sh`
- `staging/src/k8s.io/code-generator/cmd/`
- `pkg/features/kube_features.go`
- `staging/src/k8s.io/component-base/featuregate/feature_gate.go`
- `hack/update-featuregates.sh`、`hack/verify-featuregates.sh`
- `staging/src/k8s.io/apiextensions-apiserver/`、`staging/src/k8s.io/sample-apiserver/`

动手：在临时分支或干净 worktree 里只改一个 `+k8s:` tag 输入，运行 `make update SILENT=false`，检查 `git diff` 是否只有预期生成物；不把实验 diff 混入最终改动。

产出：一张“输入文件 → 生成器 → 输出文件 → verify 脚本”映射表；说明 `make update` 与 `make verify` 的先后关系。

验收：能说出至少五类 `+k8s:` 生成物和对应生成命令；能解释 `quick-verify` 与全量 `verify` 的取舍。

### 第 10 周：选一个子系统做结业项目

从下面选一个方向，不再横向铺开：

| 方向 | 入口 | 结业要求 |
| --- | --- | --- |
| Service 网络 | `cmd/kube-proxy/proxy.go`、`pkg/proxy/servicechangetracker.go`、`pkg/proxy/endpointslicecache.go`、`pkg/proxy/nftables/proxier.go` | 画清 Service / EndpointSlice 变化到规则同步的链路 |
| 存储 | `pkg/volume/plugins.go`、`pkg/controller/volume/attachdetach/`、`pkg/kubelet/volumemanager/`、`staging/src/k8s.io/csi-translation-lib/` | 画清 PVC / PV / attach / mount 的状态协调边界 |
| 安全 | `pkg/kubeapiserver/authorizer/`、`pkg/apis/rbac/`、`pkg/registry/rbac/`、`staging/src/k8s.io/pod-security-admission/` | 画清 authentication / authorization / admission 三层 |
| 调度扩展 | `pkg/scheduler/framework/plugins/`、`pkg/scheduler/framework/plugins/dynamicresources/`、`test/e2e_dra/README.md` | 说清一个新插件需要实现哪些扩展点、如何注册、如何测试 |
| 可观测性 | `pkg/controller/podautoscaler/`、`pkg/kubelet/prober/`、`staging/src/k8s.io/client-go/tools/leaderelection/` | 说清指标、探针、leader election 各自解决何种失效 |

结业产出：

1. 30 分钟可讲完的子系统架构与调用链
2. 一个定向测试改动或 trace 脚本；如果改生产代码，附完整验证记录
3. 一份 PR 风格描述：问题、根因、改动、测试、风险
4. 一份“还不懂但会继续查”的清单，写清下一步验证方法

## 压缩版

只有 20 小时时，按 `第 1 周 → 第 3 周 → 第 4 周 → 第 5 周 → 第 8 周` 执行，每周只做“精读最小集 + 一个定向测试 + 一张图”。跳过网络、存储、安全与高级调度，别把主线读成目录清单。
