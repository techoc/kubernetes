# 第3阶段：控制平面组件

**目标**：理解 API Server、Scheduler 和 Controller Manager 的架构和核心逻辑。

**前置要求**：完成第2阶段，理解 Informer、workqueue、Reflector。

---

## 1. 你需要做什么

### 1.1 API Server

API Server 是 Kubernetes 的控制中心，所有组件通过它与 etcd 交互。

**需要做的事**：

#### 1.1.1 理解启动流程

1. **阅读 API Server 启动入口**：
   - `cmd/kube-apiserver/app/server.go:74` — `NewAPIServerCommand()` 创建 cobra 命令
   - `cmd/kube-apiserver/app/server.go:152` — `Run()` 启动 API Server
   - `cmd/kube-apiserver/app/options/options.go:73` — `NewServerRunOptions()` 创建配置选项
   - `cmd/kube-apiserver/app/config.go:74` — `NewConfig()` 构建配置

2. **理解通用 API Server 框架**：
   - `staging/src/k8s.io/apiserver/pkg/server/` — 通用框架
   - `GenericAPIServer` — 核心结构体
   - `InstallAPIs()` — 注册 API group
   - `Handler()` — HTTP handler

3. **理解 API group 注册**：
   - `APIGroupInfo` — 描述一个 API group 的信息
   - `RESTStorageProviders` — 提供 REST 存储实现
   - 每个 API group（如 `core`、`apps`、`batch`）通过 `APIGroupInfo` 注册

**实验代码**：查看 API Server 的启动流程
```go
// cmd/kube-apiserver/app/server.go:74
func NewAPIServerCommand() *cobra.Command {
    s := options.NewServerRunOptions()  // 第75行：创建选项
    ctx := signals.SetupSignalContext()
    // ...
    cmd.RunE = func(cmd *cobra.Command, args []string) error {
        completedOpts, err := s.Complete()  // 完成配置
        // ...
        returnedErr := Run(ctx, *completedOpts)  // 第152行：启动
        return returnedErr
    }
    return cmd
}

// cmd/kube-apiserver/app/server.go:152
func Run(ctx context.Context, opts options.CompletedOptions) error {
    // 1. 创建配置
    config, err := NewConfig(opts)
    // 2. 完成配置
    completedConfig, err := config.Complete()
    // 3. 创建 API Server 实例
    server, err := completedConfig.New()
    // 4. 启动 HTTP 服务器
    err = server.GenericAPIServer.PrepareRun().Run(ctx)
    return err
}
```

#### 1.1.2 理解请求处理链路

一个 API 请求的处理路径：

```
HTTP Request
    │
    ▼
Authentication (认证)
    │
    ▼
Authorization (授权)
    │
    ▼
Admission (准入控制)
    │
    ▼
REST Handler (业务逻辑)
    │
    ▼
etcd (存储)
```

**需要做的事**：

1. **认证（Authentication）** — 建立用户身份：
   - 阅读 `pkg/authentication/` 和 `staging/src/k8s.io/apiserver/pkg/authentication/`
   - 理解 `Authenticator` 接口
   - 认证方式：
     - Client certificate — 客户端证书
     - Bearer token — 令牌认证
     - OIDC — OpenID Connect
     - Webhook — 外部认证服务
   - `UnionAuthenticator` — 组合多个认证器

2. **授权（Authorization）** — 检查权限：
   - 阅读 `pkg/authorization/` 和 `staging/src/k8s.io/apiserver/pkg/authorization/`
   - 理解 `Authorizer` 接口
   - 授权模式：
     - RBAC（默认）— 基于角色的访问控制
     - ABAC（遗留）— 基于属性的访问控制
     - Webhook — 外部授权服务
   - `UnionAuthorizer` / `RiskierAuthorizer` — 组合多个授权器

3. **准入控制（Admission）** — 修改或拒绝请求：
   - 阅读 `staging/src/k8s.io/apiserver/pkg/admission/`
   - `AdmissionInterface` 接口
   - 两类插件：
     - Mutating — 可以修改对象（如设置默认值）
     - Validating — 可以拒绝对象（如验证字段）
   - 执行顺序：先 Mutating，后 Validating
   - Webhook 准入：
     - `MutatingAdmissionWebhook` — 外部服务修改对象
     - `ValidatingAdmissionWebhook` — 外部服务验证对象
   - Pod Security Admission — namespace 级别的 Pod Security Standards

4. **REST Handler** — 业务逻辑：
   - 阅读 `pkg/registry/` — 各资源的 REST 实现
   - `RESTCreator` — 创建 REST 存储
   - `Strategy` 接口 — Create、Update、Delete 等业务逻辑
   - `etcdhelper` — etcd 操作封装

#### 1.1.3 理解 etcd 集成

1. **阅读 `staging/src/k8s.io/apiserver/pkg/registry/generic/registry/store.go`**：
   - `Store` — 通用 etcd 存储
   - `Create`、`Update`、`Delete`、`Get`、`List` 的 etcd 实现
   - `resourceVersion` — 乐观锁机制

2. **理解 etcd 存储键结构**：
   ```
   /registry/<resource>/<namespace>/<name>
   /registry/pods/default/my-pod
   /registry/services/kube-system/kube-dns
   ```

3. **理解 `resourceVersion`**：
   - 每次写入递增
   - WATCH 基于 `resourceVersion`
   - 乐观锁：更新时检查 `resourceVersion` 是否匹配

---

### 1.2 Controller Manager

Controller Manager 运行多个控制器，每个控制器是一个独立的 reconcile 循环。

**需要做的事**：

#### 1.2.1 理解控制器模式

**核心模式**（第2阶段已介绍，这里深入实现）：

```
Informer → EventHandler → workqueue → Worker → Reconcile → API Server
```

1. **阅读 ReplicaSet 控制器** — 最简单的控制器之一：
   - `pkg/controller/replicaset/replica_set.go:155` — `NewReplicaSetController()` 创建控制器
   - `pkg/controller/replicaset/replica_set.go:279` — `Run()` 启动 Informer 和 Worker
   - `pkg/controller/replicaset/replica_set.go:759` — `syncReplicaSet()` 同步单个 ReplicaSet

2. **理解事件处理器**：
   ```go
   // 当 Pod 变化时，找到它的 ReplicaSet 并 enqueue
   podUpdates := cache.ResourceEventHandlerFuncs{
       AddFunc: m.podAdded,
       UpdateFunc: m.podUpdated,
       DeleteFunc: m.podDeleted,
   }
   ```
   - `podAdded` / `podUpdated` / `podDeleted` — 从 Pod 找到 ReplicaSet，enqueue ReplicaSet
   - **关键**：事件处理器只 enqueue，不做耗时操作

3. **理解 reconcile 逻辑**：
   ```go
   // pkg/controller/replicaset/replica_set.go:759
   func (rsc *ReplicaSetController) syncReplicaSet(ctx context.Context, key string) error {
       // 1. 从 Lister 获取 ReplicaSet
       rs, err := rsc.rsLister.ReplicaSets(ns).Get(name)
       // 2. 从 Lister 获取当前 Pod
       pods, err := rsc.podLister.Pods(ns).List(selector)
       // 3. 比较期望副本数和实际副本数
       diff := diff(int32(len(pods)), int32(*rs.Spec.Replicas))
       // 4. 如果需要，创建或删除 Pod
       if diff > 0 {
           rsc.increaseReplicass(diff, rs)
       } else if diff < 0 {
           rsc.decreaseReplicas(-diff, rs, pods)
       }
       // 5. 更新 ReplicaSet 的 status
   }
   ```

4. **理解 Worker 模式**：
   ```go
   // pkg/controller/replicaset/replica_set.go:279
   func (rsc *ReplicaSetController) Run(ctx context.Context, workers int) {
       // 启动 Informer
       go rsc.podInformer.Informer().Run(ctx.Done())
       go rsc.rsInformer.Informer().Run(ctx.Done())
       // 等待缓存同步
       if !cache.WaitForNamedCacheSync(ctx, "ReplicaSet", rsc.rsInformer.Informer().HasSynced) {
           return
       }
       // 启动 Worker
       for i := 0; i < workers; i++ {
           go wait.UntilWithContext(ctx, rsc.worker, time.Second)
       }
       <-ctx.Done()
   }
   ```
   - 多个 Worker goroutine 并发处理
   - `processNextWorkItem()` — 从队列取出、处理、标记完成

#### 1.2.2 理解核心控制器

阅读以下控制器，理解它们如何协作：

1. **Deployment 控制器**（`pkg/controller/deployment/`）：
   - 协调 ReplicaSet 进行 rollout
   - 创建新 ReplicaSet，缩容旧 ReplicaSet
   - 理解 `Rollback`、`Pause`、`Resume` 逻辑

2. **Node Lifecycle 控制器**（`pkg/controller/node/`）：
   - 跟踪节点健康（基于 Lease）
   - 节点不健康时，标记 Pod 为 NotReady
   - 理解 `NodeMonitorController` 和 `CIDRAllocator`

3. **Endpoints/EndpointSlice 控制器**（`pkg/controller/endpoints/` 和 `endpoint_slice/`）：
   - 维护 Service 和 Pod 的端点映射
   - Pod 变化时更新 EndpointSlice
   - Service 变化时重新计算端点

4. **其他控制器**（按需阅读）：
   - `Job` — `pkg/controller/job/`
   - `DaemonSet` — `pkg/controller/daemon/`
   - `StatefulSet` — `pkg/controller/statefulset/`
   - `HPA` — `pkg/controller/horizontalpodautoscaler/`
   - `Garbage Collection` — `pkg/controller/garbagecollector/`
   - `AttachDetach` — `pkg/controller/volume/attach_detach/`

#### 1.2.3 理解 Controller Manager 启动

1. **阅读 Controller Manager 启动入口**：
   - `cmd/kube-controller-manager/app/controllermanager.go:115` — `NewControllerManagerCommand()` 创建命令
   - `cmd/kube-controller-manager/app/controllermanager.go:591` — `Run()` 启动所有控制器
   - 控制器注册和启动
   - Feature Gate 控制哪些控制器启用
   - Leader Election — 只有一个 Controller Manager 实例工作

2. **理解控制器分组**：
   - 控制器按功能分组启动
   - 依赖关系：某些控制器需要先启动
   - `StartedControllers` — 已启动的控制器列表

---

### 1.3 Scheduler

Scheduler 将未绑定的 Pod 分配到节点。

**需要做的事**：

#### 1.3.1 理解调度流程

```
1. Informer 检测到未绑定的 Pod
2. Pod 进入调度队列
3. SchedulingCycle 开始：
   a. PreFilter — 准备数据
   b. Filter — 排除不可行节点
   c. PostFilter — 无可行节点时运行（如抢占）
   d. PreScore — 评分前准备
   e. Score — 对剩余节点排名
   f. Reserve — 为选中节点更新状态
   g. Permit — 允许/拒绝/延迟绑定
   h. PreBind → Bind → PostBind — 绑定流程
4. Pod 绑定到节点，Kubelet 拉取并启动
```

#### 1.3.2 理解插件框架

1. **阅读 `pkg/scheduler/framework/`**：
   - `Framework` 接口 — 调度框架
   - `Plugin` 接口 — 调度插件
   - `Handle` 接口 — 插件可以访问的共享数据
   - `CycleState` — 调度周期内的状态

2. **理解插件类型**：
   ```go
   type PreFilterPlugin interface {
       Plugin
       PreFilter(ctx context.Context, cycleState *CycleState, pod *v1.Pod) (*PreFilterResult, *Status)
       PreFilterExtend(ctx context.Context, cycleState *CycleState, pod *v1.Pod) (*PreFilterResult, *Status)
   }
   type FilterPlugin interface {
       Plugin
       Filter(ctx context.Context, cycleState *CycleState, pod *v1.Pod, nodeInfo *NodeInfo) *Status
   }
   type ScorePlugin interface {
       Plugin
       Score(ctx context.Context, cycleState *CycleState, pod *v1.Pod, nodeID string) (int64, *Status)
       ScoreExtensions() ScoreExtension
   }
   // ... Reserve, Permit, PreBind, Bind, PostBind
   ```

3. **理解插件注册**：
   - `Register()` — 注册插件到框架
   - `Factory` 函数 — 创建插件实例
   - 插件通过配置文件启用/禁用

#### 1.3.3 理解内置插件

阅读 `pkg/scheduler/framework/plugins/` 下的内置插件：

1. **`PrioritySort`** — 队列排序：
   - 高优先级 Pod 先调度
   - 理解 `PodPriority` 和 `PreemptionPolicy`

2. **`DefaultPreemption`** — 抢占：
   - 当没有可行节点时，尝试抢占低优先级 Pod
   - 理解 `NominatedNodeName` 机制

3. **`PodTopologySpread`** — 拓扑分布：
   - 跨拓扑域（如 zone、node）分布 Pod
   - 理解 `TopologyKey` 和 `MaxSkew`

4. **`NodeAffinity`** — 节点亲和性：
   - `requiredDuringSchedulingIgnoredDuringExecution` — 硬约束
   - `preferredDuringSchedulingIgnoredDuringExecution` — 软约束

5. **`InterPodAffinity`** — Pod 间亲和性：
   - 同/不同 namespace 的 Pod 亲和/反亲和
   - 理解 `WeightedPodAffinityTerm`

6. **`VolumeBinding`** — 卷绑定：
   - 存储可行性与调度的连接
   - 理解 `External Provisioner` 和 `Immediate` 绑定模式

7. **`DynamicResources`** — DRA：
   - 动态资源分配（GPU 等）
   - 理解 `ResourceClaim` 和 `ResourceSlice`

#### 1.3.4 理解调度器启动

1. **阅读 `cmd/kube-scheduler/app/server.go`**：
   - 调度器配置
   - 插件配置文件加载
   - Leader Election

2. **阅读 Scheduler 核心实现**：
   - `pkg/scheduler/scheduler.go:327` — `New()` 创建调度器实例
   - `pkg/scheduler/scheduler.go:532` — `Run()` 调度循环
   - `pkg/scheduler/schedule_one.go:68` — `ScheduleOne()` 单次调度流程
   - `pkg/scheduler/schedule_one.go:170` — `schedulingCycle()` 调度周期
   - `pkg/scheduler/schedule_one.go:282` — `schedulingAlgorithm()` Filter + Score
   - `pkg/scheduler/schedule_one.go:347` — `bindingCycle()` 绑定流程

3. **理解调度配置文件**：
   ```yaml
   apiVersion: kubescheduler.config.k8s.io/v1
   kind: KubeSchedulerConfiguration
   profiles:
   - schedulerName: default-scheduler
     plugins:
       Filter:
         enabled: [{name: NodePorts}]
         disabled: [{name: VolumeRestrictions}]
   ```

---

## 2. 关键文件清单

| 文件/目录 | 说明 | 阅读重点 |
|-----------|------|----------|
| `cmd/kube-apiserver/app/server.go` | API Server 入口 | `NewAPIServerCommand()` 第74行 → `Run()` 第152行 |
| `cmd/kube-apiserver/app/config.go` | API Server 配置 | `NewConfig()` 第74行 |
| `staging/src/k8s.io/apiserver/pkg/server/` | 通用 API Server 框架 | `GenericAPIServer` |
| `pkg/authentication/` | 认证框架 | `Authenticator` 接口 |
| `pkg/authorization/` | 授权框架 | `Authorizer` 接口 |
| `staging/src/k8s.io/apiserver/pkg/admission/` | Admission 框架 | `AdmissionInterface` |
| `pkg/registry/` | REST 存储实现 | `Strategy` 接口 |
| `cmd/kube-controller-manager/app/controllermanager.go` | CM 入口 | `NewControllerManagerCommand()` 第115行 → `Run()` 第591行 |
| `pkg/controller/replicaset/replica_set.go` | ReplicaSet 控制器 | `NewReplicaSetController()` 第155行、`Run()` 第279行、`syncReplicaSet()` 第759行 |
| `pkg/controller/deployment/` | Deployment 控制器 | Rollout 逻辑 |
| `pkg/controller/node/` | 节点控制器 | `NodeMonitorController` |
| `pkg/controller/endpoints/` | Endpoints 控制器 | 端点更新 |
| `cmd/kube-scheduler/app/server.go` | Scheduler 入口 | 配置和启动 |
| `pkg/scheduler/scheduler.go` | 调度器核心 | `New()` 第327行、`Run()` 第532行 |
| `pkg/scheduler/schedule_one.go` | 单次调度流程 | `ScheduleOne()` 第68行、`schedulingCycle()` 第170行、`schedulingAlgorithm()` 第282行 |
| `pkg/scheduler/framework/` | 插件框架 | `Framework`、`Plugin` 接口 |
| `pkg/scheduler/framework/plugins/` | 内置插件 | 各插件实现 |

---

## 3. 实践任务清单

- [ ] **任务 1**：阅读 ReplicaSet 控制器的 `syncReplicaSet()`，画出 reconcile 流程图
  - 实验：查看 `pkg/controller/replicaset/replica_set.go:759`，理解如何比较期望和实际副本数
- [ ] **任务 2**：追踪一个 Pod 从创建到被 ReplicaSet 控制器处理的完整事件流
  - 实验：在 ReplicaSet 控制器的事件处理器中加日志，观察 Pod 变化如何触发 ReplicaSet 的 reconcile
- [ ] **任务 3**：阅读 Scheduler 插件框架，理解 `PreFilter` → `Filter` → `Score` → `Reserve` → `Bind` 的调用链
  - 实验：查看 `pkg/scheduler/schedule_one.go:282` 的 `schedulingAlgorithm()`，理解 Filter 和 Score 阶段
- [ ] **任务 4**：阅读一个内置插件（如 `NodeAffinity`），理解插件如何实现接口
- [ ] **任务 5**：追踪一个 API 请求经过认证→授权→Admission→REST Handler 的完整路径
- [ ] **任务 6**：阅读 `GenericAPIServer`，理解 API group 如何注册
- [ ] **任务 7**：用 `hack/local-up-cluster.sh` 启动集群，观察 Scheduler 和 Controller Manager 的日志
- [ ] **任务 8**：理解 Leader Election 机制，为什么 Controller Manager 和 Scheduler 需要它

**实验代码**：编写一个简单的控制器，监听 Pod 变化并打印日志
```go
package main

import (
    "context"
    "fmt"
    "time"

    v1 "k8s.io/api/core/v1"
    "k8s.io/client-go/informers"
    "k8s.io/client-go/kubernetes"
    "k8s.io/client-go/tools/cache"
    "k8s.io/client-go/tools/clientcmd"
    "k8s.io/client-go/util/homedir"
    "k8s.io/client-go/util/workqueue"
    "k8s.io/apimachinery/pkg/util/wait"
)

type PodController struct {
    client    kubernetes.Interface
    podLister corelisters.PodLister
    queue     workqueue.RateLimitingInterface
}

func NewPodController(client kubernetes.Interface, informerFactory informers.SharedInformerFactory) *PodController {
    c := &PodController{
        client:    client,
        podLister: informerFactory.CoreV1().Pods().Lister(),
        queue:     workqueue.NewRateLimitingQueue(workqueue.DefaultControllerRateLimiter()),
    }
    informer := informerFactory.CoreV1().Pods().Informer()
    informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
        AddFunc:    c.onAdd,
        UpdateFunc: c.onUpdate,
        DeleteFunc: c.onDelete,
    })
    return c
}

func (c *PodController) onAdd(obj interface{}) {
    pod := obj.(*v1.Pod)
    key := fmt.Sprintf("%s/%s", pod.Namespace, pod.Name)
    c.queue.Add(key)
}

func (c *PodController) onUpdate(oldObj, newObj interface{}) {
    oldPod := oldObj.(*v1.Pod)
    newPod := newObj.(*v1.Pod)
    if oldPod.ResourceVersion != newPod.ResourceVersion {
        key := fmt.Sprintf("%s/%s", newPod.Namespace, newPod.Name)
        c.queue.AddRateLimited(key)
    }
}

func (c *PodController) onDelete(obj interface{}) {
    pod := obj.(*v1.Pod)
    key := fmt.Sprintf("%s/%s", pod.Namespace, pod.Name)
    c.queue.Add(key)
}

func (c *PodController) Run(ctx context.Context, workers int) {
    defer c.queue.ShutDown()
    for i := 0; i < workers; i++ {
        go wait.UntilWithContext(ctx, c.worker, time.Second)
    }
    <-ctx.Done()
}

func (c *PodController) worker(ctx context.Context) {
    for c.processNextItem(ctx) {
    }
}

func (c *PodController) processNextItem(ctx context.Context) bool {
    key, shutdown := c.queue.Get()
    if shutdown {
        return false
    }
    defer c.queue.Done(key)
    err := c.syncHandler(ctx, key.(string))
    c.handleErr(err, key)
    return true
}

func (c *PodController) syncHandler(ctx context.Context, key string) error {
    namespace, name, _ := cache.SplitMetaNamespaceKey(key)
    pod, err := c.podLister.Pods(namespace).Get(name)
    if err != nil {
        // Pod 可能已被删除
        return nil
    }
    fmt.Printf("Reconcile Pod: %s/%s, Phase: %s, Node: %s\n",
        pod.Namespace, pod.Name, pod.Status.Phase, pod.Spec.NodeName)
    return nil
}

func (c *PodController) handleErr(err error, key interface{}) {
    if err == nil {
        c.queue.Forget(key)
        return
    }
    if c.queue.NumRequeues(key) < 5 {
        c.queue.AddRateLimited(key)
        return
    }
    c.queue.Forget(key)
    fmt.Printf("Failed to sync %s: %v\n", key, err)
}

func main() {
    home := homedir.HomeDir()
    config, err := clientcmd.BuildConfigFromFlags("", home+"/.kube/config")
    if err != nil {
        panic(err)
    }
    clientset, err := kubernetes.NewForConfig(config)
    if err != nil {
        panic(err)
    }

    informerFactory := informers.NewSharedInformerFactory(clientset, 0)
    controller := NewPodController(clientset, informerFactory)

    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()

    go informerFactory.Start(ctx.Done())
    if !cache.WaitForCacheSync(ctx.Done(), informerFactory.CoreV1().Pods().Informer().HasSynced) {
        panic("timeout waiting for cache sync")
    }

    controller.Run(ctx, 2)
}
```

---

## 4. 核心模式总结

### 控制器 reconcile 模式

```go
// 标准控制器结构
type Controller struct {
    // Informer
    podInformer coreinformers.PodInformer
    rsInformer  appsinformers.ReplicaSetInformer
    // Lister
    podLister  corelisters.PodLister
    rsLister   appslisters.ReplicaSetLister
    // Client
    kubeClient kubernetes.Interface
    // Queue
    workqueue workqueue.RateLimitingInterface
}

func (c *Controller) Run(workers int, stopCh <-chan struct{}) {
    // 启动 Informer
    go c.podInformer.Informer().Run(stopCh)
    go c.rsInformer.Informer().Run(stopCh)
    // 等待缓存同步
    cache.WaitForCacheSync(stopCh, c.podInformer.Informer().HasSynced, c.rsInformer.Informer().HasSynced)
    // 启动 Worker
    for i := 0; i < workers; i++ {
        go wait.Until(c.worker, time.Second, stopCh)
    }
    <-stopCh
}

func (c *Controller) worker() {
    for c.processNextWorkItem() {
    }
}

func (c *Controller) processNextWorkItem() bool {
    key, shutdown := c.workqueue.Get()
    defer c.workqueue.Done(key)
    if shutdown {
        return false
    }
    err := c.syncHandler(key.(string))
    c.handleErr(err, key)
    return true
}

func (c *Controller) syncHandler(key string) error {
    // 1. 解析 key 为 namespace/name
    // 2. 从 Lister 获取对象
    // 3. 比较期望状态和实际状态
    // 4. 执行必要的操作
    // 5. 更新 status
}

func (c *Controller) handleErr(err error, key interface{}) {
    if err == nil {
        c.workqueue.Forget(key)
        return
    }
    if c.workqueue.NumRequeues(key) < maxRetries {
        c.workqueue.AddRateLimited(key)
        return
    }
    c.workqueue.Forget(key)
    klog.Errorf("Failed to sync %s: %v", key, err)
}
```

### 调度插件扩展点

```
QueueSort → PreEnqueue → InternalQueueAdded → ...
    │
    ▼
PreFilter → Filter → PostFilter → PreScore → Score → Reserve → Permit
    │                                                                      │
    ├── Unreserve (on Reserve/Permit failure)                              │
    │                                                                      ▼
    └───────────────────────────────────────────────────────→ PreBind → Bind → PostBind
```

---

## 5. 检查点

进入第4阶段前，确保你能回答：

1. API Server 的请求处理链路是什么？每个阶段的作用？
2. ReplicaSet 控制器的 reconcile 逻辑是什么？如何决定创建还是删除 Pod？
3. Scheduler 插件框架的扩展点有哪些？按什么顺序执行？
4. 为什么 Controller Manager 需要 Leader Election？
5. Informer 的 EventHandler 为什么只 enqueue 而不直接 reconcile？
6. etcd 的 `resourceVersion` 如何保证数据一致性？
7. Admission 插件的执行顺序是什么？Mutating 和 Validating 的区别？
8. 抢占（Preemption）的触发条件和执行流程？

---

## 6. 下一步

完成本阶段后，进入 [第4阶段：节点组件](./04-node-components.md)。

节点组件是 Kubernetes 的执行层 — Kubelet 管理 Pod 生命周期，kube-proxy 处理 Service 流量。理解控制平面如何工作后，理解节点如何执行工作负载是自然的下一步。
