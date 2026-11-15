# Kubernetes 源码学习方案

本方案帮助你系统性地学习 Kubernetes 源码。从整体架构到核心组件，再到高级特性和开发实践，循序渐进。

## 学习路线总览

```
第1阶段：项目结构和开发环境 → 第2阶段：API 机制和 client-go
→ 第3阶段：控制平面组件 → 第4阶段：节点组件
→ 第5阶段：网络、存储、安全 → 第6阶段：高级主题
```

每个阶段包含：
- **核心概念**：需要理解的关键知识点
- **关键文件**：需要阅读的源码文件路径
- **实践任务**：动手验证理解的任务

## 阶段文件索引

每个阶段有独立的详细文件，包含更具体的学习步骤、代码示例和实践任务：

| 阶段 | 主题 | 详细文件 |
|------|------|----------|
| 第1阶段 | 项目结构和开发环境 | [01-project-structure.md](./01-project-structure.md) |
| 第2阶段 | API 机制和 client-go | [02-api-mechanism.md](./02-api-mechanism.md) |
| 第3阶段 | 控制平面组件 | [03-control-plane.md](./03-control-plane.md) |
| 第4阶段 | 节点组件 | [04-node-components.md](./04-node-components.md) |
| 第5阶段 | 网络、存储、安全 | [05-network-storage-security.md](./05-network-storage-security.md) |
| 第6阶段 | 高级主题 | [06-advanced-topics.md](./06-advanced-topics.md) |

---

## 第1阶段：项目结构和开发环境

**目标**：理解项目整体布局，能够编译和运行本地集群。

### 核心概念

- **目录结构**：
  - `cmd/` — 各组件的入口（`kube-apiserver`、`kube-controller-manager`、`kube-scheduler`、`kubelet`、`kube-proxy`、`kubectl`）
  - `pkg/` — 核心实现包
  - `staging/src/k8s.io/` — 独立发布的模块（client-go、api、apimachinery 等），**这是源码真相来源**
  - `api/` — API 类型定义
  - `test/` — 测试基础设施
  - `hack/` — 开发脚本
  - `vendor/` — 第三方依赖（vendored）

- **构建系统**：
  - `make` — 构建所有二进制
  - `make WHAT=./pkg/kubelet` — 构建单个包
  - `make test WHAT=./pkg/kubelet` — 运行单元测试
  - `make test-integration` — 运行集成测试
  - `make verify` — 所有验证检查
  - `make update` — 所有代码生成器和格式化器

- **staging 机制**：`staging/src/k8s.io/` 是 `k8s.io/*` 模块的源码真相来源。通过 Go workspace 和 `replace` 指令解析。**永远不要从 staging 导入 `k8s.io/kubernetes`**。

- **本地开发**：`hack/local-up-cluster.sh` 启动本地集群，用于开发和调试。

### 关键文件

| 文件 | 说明 |
|------|------|
| `Makefile` | 构建入口 |
| `go.work` | Go workspace 配置 |
| `hack/make-rules/build.sh` | 构建规则 |
| `hack/local-up-cluster.sh` | 本地集群启动脚本 |
| `CONTRIBUTING.md` | 贡献指南 |

### 实践任务

1. 阅读 `Makefile`，理解主要 target
2. 执行 `make` 编译项目
3. 运行 `hack/local-up-cluster.sh` 启动本地集群（Linux 需要 root 权限）
4. 用 `kubectl` 连接本地集群，创建 Pod 并观察

---

## 第2阶段：API 机制和 client-go

**目标**：理解 Kubernetes 的 API 资源模型、版本管理、代码生成和客户端库。

### 核心概念

- **API 资源模型**：
  - 资源由 Go 类型表示，注册到 `runtime.Scheme`
  - `GroupVersionKind` (GVK) 映射到 Go 类型
  - 支持版本转换、默认值设置和验证
  - API group 通过 `APIGroupInfo` 注册到服务器

- **API 版本管理**：
  - Group 组织相关资源，Version（如 `v1`、`v1beta1`）是版本化的 API 表示
  - 转换函数在不同版本间翻译
  - 服务器发布版本优先级

- **Feature Gates**：
  - 特性开关，定义在 `pkg/features/kube_features.go`
  - 生命周期：Alpha（默认关闭）→ Beta（默认开启）→ GA（锁定默认值）
  - 支持版本化评估和 n-3 兼容性策略

- **client-go 核心**：
  - `kubernetes.Clientset` — 内置 API 的 typed 客户端
  - `dynamic.Client` — 处理 CRD 等未结构化资源的动态客户端
  - `Reflector` — LIST + WATCH，同步到本地缓存
  - `Informer` — 连接 list/watch 到本地缓存和事件处理器
  - `Lister` — 控制器代码只读访问缓存
  - `workqueue` — 带退避的重试队列

- **代码生成**：
  - `deepcopy-gen` — 生成 DeepCopy 方法
  - `client-gen` — 生成 typed clientset
  - `informer-gen` — 生成 informers
  - `lister-gen` — 生成只读缓存访问器
  - 生成代码输出到 `staging/src/k8s.io/client-go`

### 关键文件

| 文件/目录 | 说明 |
|-----------|------|
| `staging/src/k8s.io/apimachinery/` | API 运行时基础（Scheme、runtime、watch） |
| `staging/src/k8s.io/api/` | 内置 API 类型定义 |
| `staging/src/k8s.io/client-go/` | client-go 客户端库 |
| `staging/src/k8s.io/client-go/informers/` | 生成的 informers |
| `staging/src/k8s.io/client-go/listers/` | 生成的 listers |
| `staging/src/k8s.io/client-go/tools/cache/` | Reflector、Informer 实现 |
| `staging/src/k8s.io/client-go/util/workqueue/` | 工作队列 |
| `pkg/features/kube_features.go` | 全局 Feature Gate 注册 |
| `hack/update-codegen.sh` | 代码生成脚本 |

### 实践任务

1. 追踪一个 API 类型（如 `Pod`）从 `api/core/v1/pod.go` 的定义到注册到 Scheme 的过程
2. 阅读 `client-go/tools/cache/reflector.go`，理解 LIST + WATCH 同步机制
3. 阅读 `client-go/tools/cache/shared_informer.go`，理解 Informer 的事件分发
4. 写一个简单的程序，用 client-go 创建 Pod 并 watch 其状态变化

---

## 第3阶段：控制平面组件

**目标**：理解 API Server、Scheduler 和 Controller Manager 的架构和核心逻辑。

### 3.1 API Server

**核心概念**：
- API 入口点，所有组件通过它协调
- 请求路径：Authentication → Authorization → Admission → REST handling
- 支持 pluggable 认证（client cert、bearer token、OIDC、webhook）
- 授权模式：RBAC（默认）、ABAC（遗留）、Webhook
- Admission 插件：Mutating（可修改对象）→ Validating（可拒绝对象）
- Pod Security Admission — 在 namespace 级别执行 Pod Security Standards
- 使用 etcd 作为存储后端

**关键文件**：

| 文件/目录 | 说明 |
|-----------|------|
| `cmd/kube-apiserver/` | API Server 入口 |
| `pkg/apiserver/` | API Server 核心实现 |
| `pkg/authentication/` | 认证框架 |
| `pkg/authorization/` | 授权框架 |
| `pkg/registry/` | 各资源的 REST 实现 |
| `staging/src/k8s.io/apiserver/` | 通用 API Server 框架 |

### 3.2 Controller Manager

**核心概念**：
- 运行多个控制器，每个控制器是一个独立的 reconcile 循环
- **控制器模式**：Reflector → DeltaFIFO → Informer cache → Event handler enqueue → Worker reconcile
- 事件处理器应该 enqueue 工作，而不是直接做耗时的 reconcile
- 失败可以带退避重新入队，不会阻塞 watch 流

**核心控制器**：
- `Deployment` → 协调 ReplicaSet 进行 rollout
- `ReplicaSet` → 维护期望的 Pod 副本数
- `Node Lifecycle` → 跟踪节点健康（基于 Lease）
- `Endpoints/EndpointSlice` → 维护 Service 和 Pod 的端点映射
- `Job`、`DaemonSet`、`StatefulSet`、`CronJob` — 工作负载控制器
- `HPA` — 水平自动扩缩容
- `Garbage Collection` — 基于 owner reference 的垃圾回收
- `AttachDetach` — 卷的 attach/detach 协调
- `CRD Finalizer` — CRD 删除时清理关联的 CR

**关键文件**：

| 文件/目录 | 说明 |
|-----------|------|
| `cmd/kube-controller-manager/` | Controller Manager 入口 |
| `pkg/controller/` | 所有控制器实现 |
| `pkg/controller/replicaset/` | ReplicaSet 控制器（入门推荐） |
| `pkg/controller/deployment/` | Deployment 控制器 |
| `pkg/controller/node/` | 节点控制器 |
| `pkg/controller/endpoints/` | Endpoints 控制器 |
| `pkg/controller/volume/` | 卷相关控制器 |

### 3.3 Scheduler

**核心概念**：
- 将未绑定的 Pod 分配到节点
- **插件框架**，按调度周期执行：
  - `PreFilter` — 准备数据，限制下游节点评估
  - `Filter` — 排除不能运行 Pod 的节点
  - `PostFilter` — 无可行节点时运行（如抢占）
  - `PreScore` — 评分前准备
  - `Score` — 对剩余节点排名
  - `Reserve` — 为选中节点更新插件状态（支持 Unreserve 回滚）
  - `Permit` — 允许/拒绝/延迟绑定
  - `PreBind` → `Bind` → `PostBind` — 绑定流程

- **内置插件**：
  - `PrioritySort` — 队列排序
  - `DefaultPreemption` — 抢占
  - `PodTopologySpread` — 拓扑分布
  - `NodeAffinity` / `InterPodAffinity` — 亲和性/反亲和性
  - `VolumeBinding` — 卷绑定
  - `DynamicResources` — DRA 动态资源分配

**关键文件**：

| 文件/目录 | 说明 |
|-----------|------|
| `cmd/kube-scheduler/` | Scheduler 入口 |
| `pkg/scheduler/` | Scheduler 核心实现 |
| `pkg/scheduler/framework/` | 插件框架 |
| `pkg/scheduler/framework/plugins/` | 内置插件 |
| `pkg/scheduler/framework/plugins/defaultscheduler/` | 默认调度器配置 |

### 实践任务

1. 阅读 ReplicaSet 控制器（`pkg/controller/replicaset/`），理解 reconcile 循环
2. 追踪一个 Pod 从创建到被 ReplicaSet 控制器处理的完整事件流
3. 阅读 Scheduler 插件框架（`pkg/scheduler/framework/`），理解扩展点
4. 用 `hack/local-up-cluster.sh` 启动集群，观察 Scheduler 日志

---

## 第4阶段：节点组件

**目标**：理解 Kubelet 和 Kube-proxy 的架构。

### 4.1 Kubelet

**核心概念**：
- 管理节点上的 Pod 生命周期
- 从 API Server 获取 Pod 期望状态
- 通过 per-Pod workers 同步 Pod 状态
- 通过 status manager 上报合成的 Pod 状态
- 与 CRI（Container Runtime Interface）交互管理容器
- 管理卷的 mount/unmount（节点本地卷管理器）
- 管理健康检查探针（liveness、readiness、startup）

**关键文件**：

| 文件/目录 | 说明 |
|-----------|------|
| `cmd/kubelet/` | Kubelet 入口 |
| `pkg/kubelet/` | Kubelet 核心实现 |
| `pkg/kubelet/kubelet.go` | Kubelet 主结构体 |
| `pkg/kubelet/pod_workers/` | Per-Pod 工作器 |
| `pkg/kubelet/prober/` | 探针管理 |
| `pkg/kubelet/volumemanager/` | 节点卷管理器 |
| `pkg/kubelet/cm/` | 容器管理器 |

### 4.2 Kube-proxy

**核心概念**：
- 跟踪 Service 和 Endpoint 变化
- 同步网络规则实现 Service 流量转发
- **代理模式**：
  - `iptables` — Linux，基于 iptables 规则
  - `IPVS` — Linux，基于 IPVS 负载均衡
  - `nftables` — Linux，基于 nftables（DNAT + SNAT）
  - Windows kernel mode — Windows

**关键文件**：

| 文件/目录 | 说明 |
|-----------|------|
| `cmd/kube-proxy/` | Kube-proxy 入口 |
| `pkg/proxy/` | Proxy 核心实现 |
| `pkg/proxy/iptables/` | iptables 模式 |
| `pkg/proxy/ipvs/` | IPVS 模式 |
| `pkg/proxy/nftables/` | nftables 模式 |

### 实践任务

1. 阅读 Kubelet 的 `HandlePodSyncs` 和 `podWorkers.UpdatePod`，理解 Pod 同步流程
2. 阅读 kube-proxy 的 iptables 模式，理解 Service 流量如何被转发
3. 在本地集群中观察 kubelet 日志，看 Pod 创建时的同步过程

---

## 第5阶段：网络、存储、安全

**目标**：理解 Kubernetes 的网络模型、存储架构和安全机制。

### 5.1 网络

**核心概念**：
- **Service** — 稳定的后端地址抽象
- **Endpoint/EndpointSlice** — 流量可达的目标标识
- **CNI** — Pod 网络插件接口
- **NetworkPolicy** — 流量规则表达
- **kube-proxy** — Service 流量处理（见第4阶段）

**关键文件**：

| 文件/目录 | 说明 |
|-----------|------|
| `pkg/controller/endpoints/` | Endpoints 控制器 |
| `pkg/controller/endpoint_slice/` | EndpointSlice 控制器 |
| `pkg/proxy/` | kube-proxy 实现 |

### 5.2 存储

**核心概念**：
- **VolumePlugin** — 通用卷插件抽象
- **In-tree volumes** — 内置卷插件
- **CSI** — 外部存储驱动接口（替代 in-tree 的方向）
- **Attach/Detach Controller** — 对比期望和实际 attach 状态，异步执行 attach/detach
- **VolumeAttachment** — CSI 驱动的 attach 状态协调
- **节点卷管理器** — 跟踪节点上 Pod 所需的卷，reconcile attach/mount 状态

**关键文件**：

| 文件/目录 | 说明 |
|-----------|------|
| `pkg/volume/` | 卷插件框架 |
| `pkg/controller/volume/` | 卷相关控制器 |
| `pkg/controller/volume/attach_detach/` | Attach/Detach 控制器 |
| `pkg/kubelet/volumemanager/` | 节点卷管理器 |
| `staging/src/k8s.io/csi-translation-lib/` | CSI 迁移库 |

### 5.3 安全

**核心概念**：
- **认证** — 建立用户身份（client cert、bearer token、OIDC、webhook）
- **RBAC** — 基于角色的权限控制（Role/ClusterRole + RoleBinding/ClusterRoleBinding）
- **Admission** — Mutating/Validating Webhook
- **Pod Security** — Namespace 级别的 Pod Security Standards（enforce/audit/warn）
- **ServiceAccount** — 工作负载的 API 身份
- **Secret** — 受权限控制的敏感数据 API 资源

**关键文件**：

| 文件/目录 | 说明 |
|-----------|------|
| `pkg/authentication/` | 认证框架 |
| `pkg/authorization/` | 授权框架 |
| `pkg/authorization/rbac/` | RBAC 实现 |
| `pkg/apis/rbac/` | RBAC API 类型 |
| `staging/src/k8s.io/apiserver/pkg/admission/` | Admission 框架 |

### 实践任务

1. 阅读 RBAC 的 bootstrap 角色数据，理解权限规则的结构
2. 追踪一个 API 请求经过认证→授权→Admission 的完整路径
3. 阅读 Attach/Detach 控制器，理解卷 attach 的 reconcile 逻辑

---

## 第6阶段：高级主题

**目标**：深入理解 CRD/Operator 模式、可观测性、高级调度等。

### 6.1 CRD 和 Operator 模式

**核心概念**：
- **CRD** — 注册新的 API 类型，无需修改核心 API Server
- **Custom Resource (CR)** — CRD 的实例
- **Operator** — 控制器模式：watch CR，reconcile 实际状态到期望状态
- **apiextensions-apiserver** — 服务 CRD 资源的 API 扩展服务器
- **Status subresource** — 控制器独立更新 status（`UpdateStatus`）
- **Finalizer** — 延迟删除直到清理完成
- **Validation** — CRD schema 验证 vs Validating Webhook
- **Conversion Webhook** — CRD 不同 API 版本间的对象转换

**关键文件**：

| 文件/目录 | 说明 |
|-----------|------|
| `staging/src/k8s.io/apiextensions-apiserver/` | API 扩展服务器 |
| `pkg/controller/crd/` | CRD 相关控制器 |
| `staging/src/k8s.io/sample-controller/` | 示例控制器 |

### 6.2 可观测性

**核心概念**：
- **Metrics Server** — 从 kubelet 收集资源指标，服务 metrics API（`kubectl top`、HPA）
- **HPA** — 消费 MetricsClient，通过 ReplicaCalculator 计算期望副本数
- **Events** — API 对象，由组件 broadcaster 记录到 API Server
- **klog** — 组件日志
- **Probes** — Liveness（重启）、Readiness（流量）、Startup（启动门控）
- **Leader Election** — 共享资源锁 + Lease 定时参数（LeaseDuration、RenewDeadline、RetryPeriod）

**关键文件**：

| 文件/目录 | 说明 |
|-----------|------|
| `pkg/controller/horizontalpodautoscaler/` | HPA 控制器 |
| `pkg/kubelet/prober/` | 探针管理 |
| `staging/src/k8s.io/client-go/tools/leaderelection/` | Leader 选举 |

### 6.3 高级调度

**核心概念**：
- **Priority/Preemption** — Pod 优先级和抢占
- **PodTopologySpread** — 跨拓扑域分布
- **NodeAffinity/InterPodAffinity** — 节点亲和性和 Pod 间亲和性
- **VolumeBinding** — 存储可行性与调度的连接
- **DRA (Dynamic Resource Allocation)** — 动态资源分配（GPU 等）

**关键文件**：

| 文件/目录 | 说明 |
|-----------|------|
| `pkg/scheduler/framework/plugins/` | 调度插件 |
| `pkg/scheduler/framework/plugins/defaultpreemption/` | 默认抢占 |
| `pkg/scheduler/framework/plugins/podtopologyspread/` | 拓扑分布 |
| `pkg/scheduler/framework/plugins/dynamicresources/` | DRA |

### 实践任务

1. 阅读示例控制器（`staging/src/k8s.io/sample-controller/`），理解 Operator 模式
2. 写一个简单的 CRD + 控制器，实现基本的 reconcile 逻辑
3. 阅读 HPA 控制器，理解 metrics 消费和扩缩容计算
4. 阅读 Leader Election 实现，理解 Lease 机制

---

## 学习建议

### 阅读顺序

1. 先通读第1阶段，建立项目全局观
2. 第2阶段是基础，API 机制和 client-go 是理解后续所有组件的前提
3. 第3阶段从 ReplicaSet 控制器入手，它是理解控制器模式的最佳入口
4. 第4阶段理解节点如何执行工作负载
5. 第5阶段根据兴趣选择深入方向
6. 第6阶段在掌握基础后探索

### 调试技巧

- 用 `hack/local-up-cluster.sh` 启动本地集群，加 `-d` 参数打印命令但不执行
- 通过环境变量配置行为：`CLUSTER_CIDR`、`SERVICE_CLUSTER_IP_RANGE`、`FEATURE_GATES`
- 用 `make test WHAT=./pkg/controller/replicaset -v` 运行特定包的测试
- 在源码中加 `klog.V(4).InfoS()` 输出调试信息

### 约束提醒

- **生成的文件只读**：`zz_generated.*` 和 `generated.pb.go` 不要手改，运行 `make update`
- **go.mod/go.work 是生成的**：用 `hack/pin-dependency.sh` + `hack/update-vendor.sh` 管理依赖
- **staging 是源码真相**：修改 `k8s.io/*` 模块在 `staging/src/k8s.io/` 下进行
- **Boilerplate**：每个 `.go` 文件需要 license header（`hack/boilerplate/boilerplate.go.txt`）

---

## 资源链接

- [Kubernetes 官方文档](https://kubernetes.io/docs/)
- [client-go 文档](https://github.com/kubernetes/client-go)
- [API Machinery 文档](https://github.com/kubernetes/apiserver)
- [贡献指南](../CONTRIBUTING.md)
