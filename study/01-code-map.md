# 代码地图

本文件的路径、函数与行号按当前 `master` 快照核对。行号会随提交漂移，函数名比行号稳定。

## 1. 三层源码边界

| 层 | 位置 | 作用 | 阅读规则 |
| --- | --- | --- | --- |
| 二进制入口 | `cmd/` | `kube-apiserver`、`kube-controller-manager`、`kube-scheduler`、`kubelet`、`kube-proxy`、`kubectl` 的 `main`、options、装配 | 从 `main` 进入，不在这里找业务逻辑 |
| 主仓库实现 | `pkg/` | 控制平面、控制器、调度器、kubelet、registry、admission 等 `k8s.io/kubernetes` 内部实现 | 读业务策略与组件协调 |
| 可发布模块 | `staging/src/k8s.io/` | `api`、`apimachinery`、`apiserver`、`client-go`、`kubectl`、`code-generator` 等 | 改 `k8s.io/*` 时优先看这里 |

`go.work:9` 起把每个 staging 模块纳入 workspace；`go.mod:220` 起用 `replace k8s.io/... => ./staging/src/k8s.io/...` 指回本地。`vendor/modules.txt` 只有 `=> ./staging/src/k8s.io/...` 的标记，不是 staging 模块副本。`vendor/` 主要承载第三方依赖。

容易走错的三条路：

- 改 `client-go` 行为却去 `vendor/k8s.io/client-go`；正确位置是 `staging/src/k8s.io/client-go`
- 改 Pod 类型却去 `api/`；外部类型在 `staging/src/k8s.io/api/core/v1/types.go`，内部类型在 `pkg/apis/core/types.go`
- 在 staging 包里 import `k8s.io/kubernetes`；这违反 AGENTS.md 与模块依赖方向

## 2. 主链路：一个 Pod 被创建

### 2.1 kubectl → REST

| 阶段 | 文件 | 锚点 |
| --- | --- | --- |
| 进程入口 | `cmd/kubectl/kubectl.go:31` | `main` |
| 命令树 | `staging/src/k8s.io/kubectl/pkg/cmd/cmd.go:165` | `NewKubectlCommand` |
| create 命令 | `staging/src/k8s.io/kubectl/pkg/cmd/create/create.go:95` | `NewCmdCreate` |
| 执行 | `staging/src/k8s.io/kubectl/pkg/cmd/create/create.go:236` | `CreateOptions.RunCreate` |
| 通用 REST | `staging/src/k8s.io/cli-runtime/pkg/resource/helper.go:206` | `Helper.Create` |
| POST 组装 | `staging/src/k8s.io/cli-runtime/pkg/resource/helper.go:240` | `Helper.createResource` |
| REST 请求起点 | `staging/src/k8s.io/client-go/rest/request.go:243` | `Request.Resource` |
| REST 请求终点 | `staging/src/k8s.io/client-go/rest/request.go:1142` | `Request.Do` |
| typed client | `staging/src/k8s.io/client-go/kubernetes/clientset.go:500` | `NewForConfig` |
| 泛型 client | `staging/src/k8s.io/client-go/gentype/type.go:205` | `Client.Create` |
| Pod client | `staging/src/k8s.io/client-go/kubernetes/typed/core/v1/pod.go:68` | `newPods` |
| kubeconfig | `staging/src/k8s.io/client-go/tools/clientcmd/client_config.go:681` | `BuildConfigFromFlags` |
| RESTClient | `staging/src/k8s.io/client-go/rest/config.go:344` | `RESTClientFor` |

先把 `kubectl create -f pod.yaml` 的 dynamic/resource builder 路径和 `Clientset.CoreV1().Pods(ns).Create(...)` 的 typed 路径分开画。相同点是最后都落到 REST `POST`；不同点是对象发现、版本协商与类型转换。

### 2.2 apiserver 启动与路由

| 阶段 | 文件 | 锚点 |
| --- | --- | --- |
| 命令与 server chain | `cmd/kube-apiserver/app/server.go:74` | `NewAPIServerCommand` / `Run` / `CreateServerChain` |
| 实例装配 | `pkg/controlplane/instance.go:321` | `CompletedConfig.New` |
| API 安装 | `pkg/controlplane/apiserver/apis.go:88` | `Server.InstallAPIs` |
| legacy group | `staging/src/k8s.io/apiserver/pkg/server/genericapiserver.go:885` | `InstallLegacyAPIGroup` |
| 资源路由 | `staging/src/k8s.io/apiserver/pkg/endpoints/installer.go:288` | `registerResourceHandlers` |
| POST handler 绑定 | `staging/src/k8s.io/apiserver/pkg/endpoints/installer.go:1309` | `handlers.CreateResource` |
| 过滤链 | `staging/src/k8s.io/apiserver/pkg/server/config.go:1076` | `DefaultBuildHandlerChain` |
| admission 注册 | `pkg/kubeapiserver/options/admission.go:59` | `RegisterAllAdmissionPlugins` |
| admission 应用 | `staging/src/k8s.io/apiserver/pkg/server/options/admission.go:130` | `ApplyTo` |
| admission 链 | `staging/src/k8s.io/apiserver/pkg/admission/chain.go:31` | `Admit` / `Validate` |

`DefaultBuildHandlerChain` 负责 authentication、authorization、audit、priority-and-fairness、impersonation 等横切关注点。不要把过滤链和 `handler → strategy → validating admission → storage` 的业务链混成一条线。

### 2.3 handler → strategy → admission → etcd

| 阶段 | 文件 | 锚点 |
| --- | --- | --- |
| create handler | `staging/src/k8s.io/apiserver/pkg/endpoints/handlers/create.go:53` | `createHandler` |
| POST 入口 | `staging/src/k8s.io/apiserver/pkg/endpoints/handlers/create.go:245` | `CreateResource` |
| store | `staging/src/k8s.io/apiserver/pkg/registry/generic/registry/store.go:564` | `Store.Create` / `Store.create` |
| create 前置 | `staging/src/k8s.io/apiserver/pkg/registry/rest/create.go:103` | `BeforeCreate` |
| validating admission | `staging/src/k8s.io/apiserver/pkg/registry/rest/create.go:218` | `AdmissionToValidateObjectFunc` |
| Pod 策略 | `pkg/registry/core/pod/strategy.go:68` | `Strategy` / `PrepareForCreate` / `Validate` / `Canonicalize` |
| Pod 字段校验 | `pkg/apis/core/validation/validation.go:5746` | `ValidatePodCreate` |
| storage 装配 | `pkg/registry/core/pod/storage/storage.go:81` | `NewStorage` |
| etcd 工厂 | `pkg/controlplane/apiserver/config.go:210` | `NewStorageFactoryConfigEffectiveVersion` / `Etcd.ApplyWithStorageFactoryTo` |
| Store 完成 | `staging/src/k8s.io/apiserver/pkg/registry/generic/registry/store.go:1638` | `CompleteWithOptions` |
| etcd 写入 | `staging/src/k8s.io/apiserver/pkg/storage/etcd3/store.go:293` | `Create` / `GuaranteedUpdate` |

真实顺序值得抄在纸上：mutating admission → `PrepareForCreate` → 静态 `Validate` → validating admission → `Storage.Create`。`zz_generated.*` 不在这个链条的业务语义层，别把生成代码当起点。

### 2.4 API 版本 / scheme / REST storage

| 概念 | 文件 |
| --- | --- |
| 外部类型 | `staging/src/k8s.io/api/core/v1/types.go`、`staging/src/k8s.io/api/core/v1/register.go` |
| 内部类型 | `pkg/apis/core/types.go`、`pkg/apis/core/register.go` |
| defaults / conversion | `pkg/apis/core/v1/` |
| 校验 | `pkg/apis/core/validation/validation.go` |
| Scheme | `pkg/apis/core/install/install.go:34`、`pkg/api/legacyscheme/scheme.go`、`staging/src/k8s.io/apimachinery/pkg/runtime/scheme.go:100` |
| REST storage 注册 | `pkg/registry/core/rest/storage_core.go:140` |
| `pods` 资源 | `pkg/registry/core/rest/storage_core.go:237` |
| Pod storage / binding / status | `pkg/registry/core/pod/storage/storage.go:81` |
| OpenAPI | `api/openapi-spec/swagger.json`、`pkg/generated/openapi/zz_generated.openapi.go` |

`Scheme` 解决 GVK ↔ Go type；`RESTStorage` 解决资源路径 ↔ CRUD + subresource；`Strategy` 解决某个资源在生命周期事件中的规则。这三层不要互相替代。

## 3. 控制面支线：控制器、调度器、绑定

### 3.1 controller-manager

| 阶段 | 文件 | 锚点 |
| --- | --- | --- |
| 命令 | `cmd/kube-controller-manager/app/controllermanager.go:117` | `NewControllerManagerCommand` / `Run` / `BuildControllers` / `RunControllers` |
| 注册表 | `cmd/kube-controller-manager/app/controller_descriptor.go:106` | `KnownControllers` / `NewControllerDescriptors` |
| Deployment | `pkg/controller/deployment/deployment_controller.go:172` | `Run` |
| Deployment reconcile | `pkg/controller/deployment/deployment_controller.go:575` | `syncDeployment` |
| ReplicaSet | `pkg/controller/replicaset/replica_set.go:279` | `Run` |
| ReplicaSet reconcile | `pkg/controller/replicaset/replica_set.go:759` | `syncReplicaSet` |
| informer / cache | `staging/src/k8s.io/client-go/tools/cache/controller.go` | `Controller` |
| 队列 | `staging/src/k8s.io/client-go/util/workqueue/queue.go` | `Interface` |
| 最小控制器样本 | `staging/src/k8s.io/sample-controller/controller.go` | 完整 reconcile 闭环 |

控制器模式：`LIST + WATCH → informer cache → event handler enqueue → workqueue → reconcile → 写回 API`。事件 handler 应只入队，不在 watch 回调里做长活。

### 3.2 scheduler

| 阶段 | 文件 | 锚点 |
| --- | --- | --- |
| 命令 | `cmd/kube-scheduler/app/server.go:95` | `NewSchedulerCommand` / `Run` / `Setup` |
| 调度器 | `pkg/scheduler/scheduler.go:327` | `New` / `Run` |
| 事件入队 | `pkg/scheduler/eventhandlers.go:254` | `addPodToSchedulingQueue` / `addAllEventHandlers` |
| 单次调度 | `pkg/scheduler/schedule_one.go:68` | `ScheduleOne` |
| scheduling cycle | `pkg/scheduler/schedule_one.go:170` | `schedulingCycle` |
| binding cycle | `pkg/scheduler/schedule_one.go:347` | `bindingCycle` |
| bind | `pkg/scheduler/schedule_one.go:775` | `bind` |
| 插件扩展点 | `pkg/scheduler/framework/interface.go` | `Framework` |
| 插件执行 | `pkg/scheduler/framework/runtime/framework.go:1091` | `RunPreFilterPlugins` / `RunFilterPlugins` / `RunScorePlugins` / `RunBindPlugins` |
| 默认 binder | `pkg/scheduler/framework/plugins/defaultbinder/default_binder.go:52` | `Bind` |
| binding 写入 | `pkg/scheduler/util/utils.go:108` | `BindPod` |
| 服务端 binding | `pkg/registry/core/pod/storage/storage.go:175` | `BindingREST` |

插件阅读顺序：`PreFilter → Filter → PostFilter → PreScore → Score → Reserve → Permit → PreBind → Bind → PostBind`。先读 `interface.go` 的扩展点契约，再看 `runtime/framework.go` 的调度执行，不要一开始扎进某个打分算法。

## 4. 节点侧：kubelet → CRI

| 阶段 | 文件 | 锚点 |
| --- | --- | --- |
| 启动 | `cmd/kubelet/app/server.go:544` | `Run` / `RunKubelet` / `startKubelet` |
| apiserver Pod 源 | `pkg/kubelet/config/apiserver.go:36` | `NewSourceApiserver` |
| PodConfig | `pkg/kubelet/config/config.go:61` | `NewPodConfig` / `Updates` / `podStorage.merge` |
| 主循环 | `pkg/kubelet/kubelet.go:1965` | `Run` |
| syncLoop | `pkg/kubelet/kubelet.go:2783` | `syncLoop` / `syncLoopIteration` |
| 新增 Pod | `pkg/kubelet/kubelet.go:2995` | `HandlePodAdditions` |
| SyncPod | `pkg/kubelet/kubelet.go:2135` | `Kubelet.SyncPod` |
| per-Pod worker | `pkg/kubelet/pod_workers.go:770` | `UpdatePod` / `podWorkerLoop` |
| runtime 边界 | `pkg/kubelet/container/runtime.go` | `Runtime` 接口 |
| runtime SyncPod | `pkg/kubelet/kuberuntime/kuberuntime_manager.go:1837` | `kubeGenericRuntimeManager.SyncPod` |
| sandbox | `pkg/kubelet/kuberuntime/kuberuntime_sandbox.go:42` | `createPodSandbox` / `RunPodSandbox` |
| container | `pkg/kubelet/kuberuntime/kuberuntime_container.go:213` | `startContainer` / `CreateContainer` / `StartContainer` |
| CRI client | `staging/src/k8s.io/cri-client/pkg/remote_runtime.go:309` | `RunPodSandbox` / `CreateContainer` / `StartContainer` |
| CRI 协议 | `staging/src/k8s.io/cri-api/pkg/apis/runtime/v1/api.proto:24` | `RuntimeService` |

kubelet 的核心不是“调用 Docker”，而是把期望 Pod 状态、实际容器状态、per-Pod worker 和 CRI 调用持续对齐。读 `syncLoopIteration` 时把每类 channel 事件画成表：谁产生、谁消费、最终触发哪个 `SyncPod` 类型。

## 5. 网络与其他子系统入口

| 子系统 | 入口 |
| --- | --- |
| kube-proxy 启动 | `cmd/kube-proxy/proxy.go`、`cmd/kube-proxy/app/server.go` |
| Service / EndpointSlice 缓存 | `pkg/proxy/servicechangetracker.go`、`pkg/proxy/endpointslicecache.go` |
| iptables / nftables proxier | `pkg/proxy/iptables/proxier.go`、`pkg/proxy/nftables/proxier.go` |
| EndpointSlice 模块 | `staging/src/k8s.io/endpointslice/`、`staging/src/k8s.io/kube-proxy/` |
| 卷插件 | `pkg/volume/plugins.go` |
| kubelet 卷管理器 | `pkg/kubelet/volumemanager/` |
| attach/detach 控制器 | `pkg/controller/volume/attachdetach/` |
| HPA | `pkg/controller/podautoscaler/`、`pkg/controller/podautoscaler/replica_calculator.go` |
| RBAC / authz | `pkg/apis/rbac/`、`pkg/registry/rbac/`、`pkg/kubeapiserver/authorizer/`、`pkg/auth/authorizer/` |
| Pod Security | `staging/src/k8s.io/pod-security-admission/` |
| Feature gates | `pkg/features/kube_features.go`、`staging/src/k8s.io/component-base/featuregate/feature_gate.go` |
| CRD | `staging/src/k8s.io/apiextensions-apiserver/` |
| sample apiserver | `staging/src/k8s.io/sample-apiserver/` |
| DRA | `staging/src/k8s.io/dynamic-resource-allocation/`、`test/e2e_dra/` |

这些是第二主线，不要在第 1–4 周展开。等 Pod 主链路能徒手画出来，再按工作需求选择。

## 6. 生成物与生成器

- 总入口：`make update` → `hack/make-rules/update.sh:38` 的 `BASH_TARGETS`
- 代码生成：`hack/update-codegen.sh`，按 `+k8s:` tag 运行 protobuf、deepcopy、defaulter、conversion、validation、openapi、client、lister、informer、applyconfiguration 等生成器
- 生成器源码：`staging/src/k8s.io/code-generator/cmd/`
- 典型产物：`staging/src/k8s.io/api/core/v1/zz_generated.deepcopy.go`、`pkg/apis/core/v1/zz_generated.conversion.go`、`pkg/generated/openapi/zz_generated.openapi.go`
- 典型产物目录：`staging/src/k8s.io/client-go/` 下的 `clientset/`、`informers/`、`listers/`、`applyconfigurations/`、`fake/`
- 协议产物：`staging/src/k8s.io/cri-api/pkg/apis/runtime/v1/api.pb.go`、`staging/src/k8s.io/apimachinery/pkg/runtime/generated.pb.go`
- 检查范式：`hack/lib/verify-generated.sh` 在临时 worktree 跑 update 脚本并检查 diff

看到 `Code generated by ... DO NOT EDIT.` 就停手，去改输入 tag / 类型定义 / 生成配置。

## 7. 测试地图

| 层级 | 目录 / 入口 | 重点 |
| --- | --- | --- |
| 单元 | `pkg/**/*_test.go`、`staging/src/k8s.io/**/*_test.go`、`hack/make-rules/test.sh` | 纯逻辑、fake client、表驱动边界 |
| 集成 | `test/integration/**`、`staging/src/k8s.io/*/test/integration/**`、`hack/make-rules/test-integration.sh` | 真 apiserver + etcd，组件协作 |
| 框架 | `test/integration/framework/main_test.go`、`test_server.go`、`controlplane_utils.go` | `EtcdMain`、control plane 启动、序列化配置 |
| E2E | `test/e2e/e2e_test.go`、`test/e2e/framework/framework.go` | 真集群、Ginkgo、用户可见行为 |
| Node E2E | `test/e2e_node/e2e_node_suite_test.go`、`runner/local/run_local.go` | kubelet / CRI 真节点行为 |
| Conformance | `test/conformance/walk_test.go`、`testdata/conformance.yaml` | `framework.ConformanceIt` 清单治理 |
| CLI / cmd | `test/cmd/legacy-script.sh`、`hack/make-rules/test-cmd.sh` | 本地控制面 + kubectl 命令流程 |

适合初学者精读的五组：

1. `pkg/registry/core/pod/strategy_test.go:1885` → `pkg/registry/core/pod/strategy.go:125`
2. `pkg/controller/deployment/sync_test.go` → `pkg/controller/deployment/sync.go` → `test/integration/deployment/deployment_test.go:124`
3. `pkg/scheduler/schedule_one_test.go` → `pkg/scheduler/schedule_one.go` → `test/integration/scheduler/scheduler_test.go:304`
4. `pkg/kubelet/kubelet_pods_test.go` → `pkg/kubelet/kubelet_pods.go` → `test/e2e_node/container_lifecycle_test.go`
5. `test/e2e/common/node/pods.go:230` → `test/e2e/framework/pod/create.go` → `test/e2e/framework/framework.go`

## 8. 第一遍只读这五段

1. `staging/src/k8s.io/apiserver/pkg/endpoints/handlers/create.go` + `staging/src/k8s.io/apiserver/pkg/registry/generic/registry/store.go`
2. `pkg/registry/core/pod/strategy.go` + `pkg/registry/core/pod/storage/storage.go` + `pkg/apis/core/validation/validation.go`
3. `cmd/kube-apiserver/app/server.go` + `pkg/controlplane/instance.go` + `pkg/registry/core/rest/storage_core.go`
4. `pkg/scheduler/schedule_one.go` + `pkg/scheduler/framework/runtime/framework.go` + `pkg/scheduler/framework/plugins/defaultbinder/default_binder.go`
5. `pkg/kubelet/kubelet.go` + `pkg/kubelet/kuberuntime/kuberuntime_manager.go` + `staging/src/k8s.io/cri-client/pkg/remote_runtime.go` + `staging/src/k8s.io/cri-api/pkg/apis/runtime/v1/api.proto`

按这五段读完后，应当能解释：一个 POST 在哪里变成对象、在哪里被改、在哪里被拒、在哪里落盘、在哪里变成节点上的容器。
