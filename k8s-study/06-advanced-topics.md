# 第6阶段：高级主题

**目标**：深入理解 CRD/Operator 模式、可观测性、高级调度等。

**前置要求**：完成第5阶段，掌握 Kubernetes 核心机制。

---

## 1. 你需要做什么

### 1.1 CRD 和 Operator 模式

CRD（Custom Resource Definition）允许扩展 Kubernetes API，Operator 模式基于 CRD 实现自动化运维。

**需要做的事**：

#### 1.1.1 理解 CRD 机制

1. **理解 CRD 模型**：
   - CRD 注册新的 API 类型，无需修改核心 API Server
   - Custom Resource (CR) — CRD 的实例
   - CRD 通过 `apiextensions-apiserver` 服务

2. **阅读 `staging/src/k8s.io/apiextensions-apiserver/`**：
   - API 扩展服务器的实现
   - CRD 的 REST 存储
   - 版本管理和转换

3. **理解 CRD 结构**：
   ```yaml
   apiVersion: apiextensions.k8s.io/v1
   kind: CustomResourceDefinition
   metadata:
     name: mycrds.example.com
   spec:
     group: example.com
     versions:
     - name: v1
       served: true
       storage: true
       schema:
         openAPIV3Schema:
           type: object
           properties:
             spec:
               type: object
               properties:
                 replicas:
                   type: integer
             status:
               type: object
     scope: Namespaced
     names:
       plural: mycrds
       singular: mycrd
       kind: MyCRD
   ```
   - `group` — API 组
   - `versions` — 版本列表
   - `schema` — OpenAPI v3 schema，用于验证
   - `scope` — `Namespaced` 或 `Cluster`
   - `names` — 资源名称

4. **理解 CRD 的验证**：
   - CRD schema 验证 — 基于 OpenAPI v3
   - Validating Webhook — 外部验证服务
   - 两者的区别和使用场景

#### 1.1.2 理解 Operator 模式

1. **理解 Operator 概念**：
   - Operator = 控制器 + CRD
   - Watch CR，reconcile 实际状态到期望状态
   - 封装运维知识到代码中

2. **阅读 `staging/src/k8s.io/sample-controller/`** — 示例控制器：
   - 最简单的 Operator 示例
   - 理解 Informer + workqueue + reconcile 模式
   - CR 的定义和控制器实现

3. **理解 Status subresource**：
   - 控制器独立更新 status（`UpdateStatus`）
   - 更新 status 不会触发控制器的 reconcile
   - 允许多个组件并发更新 status

4. **理解 Finalizer**：
   - 延迟删除直到清理完成
   - 删除时，API Server 设置 `deletionTimestamp`
   - 控制器检测到 finalizer，执行清理逻辑
   - 清理完成后移除 finalizer，对象真正删除

5. **理解 Conversion Webhook**：
   - CRD 不同 API 版本间的对象转换
   - 当存储版本和请求版本不同时触发
   - 外部 HTTPS 服务执行转换

#### 1.1.3 理解 controller-runtime（扩展阅读）

虽然 Kubernetes 核心使用 client-go，但 Operator 开发通常使用 `controller-runtime`：

1. **理解 controller-runtime 的抽象**：
   - `Controller` — 控制器骨架
   - `Reconciler` 接口 — 用户实现 reconcile 逻辑
   - `Manager` — 管理控制器和共享资源

2. **推荐阅读**：
   - `sigs.k8s.io/controller-runtime` 文档
   - Kubebuilder 教程

---

### 1.2 可观测性

Kubernetes 的可观测性包括指标、事件、日志、健康检查和 Leader Election。

**需要做的事**：

#### 1.2.1 理解指标（Metrics）

1. **理解 Metrics Server**：
   - 从 kubelet 收集资源指标
   - 服务 metrics API（`kubectl top`、HPA）
   - 15 秒分辨率
   - 注意：Metrics Server 是独立项目，不在 Kubernetes 核心仓库中

2. **理解组件指标**：
   - 每个组件暴露 `/metrics` 端点
   - 使用 `prometheus` 格式
   - 阅读 `staging/src/k8s.io/component-base/metrics/`

3. **理解 kubelet 指标**：
   - 阅读 `pkg/kubelet/stats/`
   - 容器资源使用统计
   - 节点资源统计

#### 1.2.2 理解 HPA（Horizontal Pod Autoscaler）

1. **阅读 `pkg/controller/horizontalpodautoscaler/`**：
   - `horizontalpodautoscaler` — HPA 控制器
   - 消费 MetricsClient，获取资源指标
   - 通过 ReplicaCalculator 计算期望副本数

2. **理解扩缩容逻辑**：
   ```
   1. 获取当前指标（CPU、内存、自定义指标）
   2. 计算目标副本数
   3. 应用扩缩容策略（增量/减量限制）
   4. 更新 Deployment/Scale 的 replicas
   ```

3. **理解指标类型**：
   - 资源指标 — CPU、内存
   - 对象指标 — 来自其他对象（如 QueueLength）
   - 外部指标 — 来自外部系统（如 Prometheus）

#### 1.2.3 理解事件（Events）

1. **理解事件系统**：
   - 事件是 API 对象
   - 由组件 broadcaster 记录到 API Server
   - 阅读 `staging/src/k8s.io/client-go/tools/record/`

2. **理解事件类型**：
   - `Normal` — 正常事件
   - `Warning` — 警告事件
   - 事件原因（Reason）和消息（Message）

3. **理解 broadcaster**：
   - `EventBroadcaster` — 事件广播器
   - `Recorder` — 事件记录器
   - 异步发送到 API Server

#### 1.2.4 理解日志（Logging）

1. **理解 klog**：
   - Kubernetes 的日志库
   - 阅读 `staging/src/k8s.io/klog/v2/`
   - 级别：`V(0)` 到 `V(10)`
   - `InfoS()` — 结构化日志

2. **理解组件日志**：
   - 每个组件独立日志
   - 无集群级别的日志聚合
   - 日志聚合需要外部工具（如 EFK、Loki）

#### 1.2.5 理解健康检查探针

第4阶段已介绍探针，这里深入理解：

1. **阅读 `pkg/kubelet/prober/`**：
   - `proberManager` — 探针管理器
   - `probeRunner` — 执行探针
   - 探针结果如何影响 Pod 状态

2. **理解探针的 reconcile**：
   - 探针定期执行
   - 结果更新 Pod 的 `ready` 状态
   - Liveness 失败触发容器重启

#### 1.2.6 理解 Leader Election

1. **阅读 `staging/src/k8s.io/client-go/tools/leaderelection/leaderelection.go`**：
   - `NewLeaderElector()` 构造函数：第82行 — 创建 Leader 选举器
   - `Run()` 方法：第284行 — 启动选举循环
   - `LeaderElector` — Leader 选举器
   - 基于资源锁的选举机制

2. **理解选举流程**：
   ```
   1. 每个实例尝试获取 Lease
   2. 成功获取的实例成为 Leader
   3. Leader 定期更新 Lease
   4. 如果 Leader 停止更新，其他实例竞选
   ```

3. **理解 Lease 参数**：
   - `LeaseDuration` — Lease 有效期（默认 15 秒）
   - `RenewDeadline` — Leader 更新 Lease 的截止时间（默认 10 秒）
   - `RetryPeriod` — 重试间隔（默认 2 秒）

4. **理解资源锁**：
   - `LeaseLock` — 基于 Lease 对象（推荐）
   - `ConfigMapLock` — 基于 ConfigMap（遗留）
   - `EndpointsLock` — 基于 Endpoints（遗留）

5. **理解健康检查**：
   - Leader 暴露 `/healthz` 端点
   - 非 Leader 返回 500
   - 确保只有 Leader 提供服务

---

### 1.3 高级调度

在理解第3阶段调度器基础后，深入高级调度特性。

**需要做的事**：

#### 1.3.1 理解优先级和抢占

1. **理解 Pod 优先级**：
   - `priorityClassName` — 优先级类
   - `Priority` — 数值，越高越优先
   - 高优先级 Pod 可以抢占低优先级 Pod

2. **阅读 `pkg/scheduler/framework/plugins/defaultpreemption/`**：
   - 默认抢占逻辑
   - 当没有可行节点时触发
   - 选择victim Pod 并驱逐

3. **理解抢占流程**：
   ```
   1. 高优先级 Pod 无法调度
   2. 检查是否有低优先级 Pod 可以抢占
   3. 设置 NominatedNodeName
   4. 驱逐 victim Pod
   5. 等待 victim Pod 删除
   6. 调度高优先级 Pod
   ```

#### 1.3.2 理解拓扑分布（Topology Spread）

1. **理解 Topology Spread Constraints**：
   - 跨拓扑域（如 zone、node）分布 Pod
   - `TopologyKey` — 拓扑域标签
   - `MaxSkew` — 最大偏差
   - `WhenUnsatisfiable` — 不满足时的策略

2. **阅读 `pkg/scheduler/framework/plugins/podtopologyspread/`**：
   - 拓扑分布插件实现
   - 计算每个拓扑域的 Pod 数量
   - 选择偏差最小的节点

3. **理解调度策略**：
   - `ScheduleAnyway` — 软约束，仍然调度
   - `DoNotSchedule` — 硬约束，不调度

#### 1.3.3 理解亲和性和反亲和性

1. **理解 Node Affinity**：
   - `requiredDuringSchedulingIgnoredDuringExecution` — 硬约束
   - `preferredDuringSchedulingIgnoredDuringExecution` — 软约束
   - 基于节点标签

2. **理解 Inter-Pod Affinity**：
   - Pod 间亲和性/反亲和性
   - 同/不同 namespace 的 Pod
   - `WeightedPodAffinityTerm` — 权重

3. **阅读 `pkg/scheduler/framework/plugins/nodeaffinity/` 和 `interpodaffinity/`**：
   - 插件实现
   - Filter 和 Score 扩展点

#### 1.3.4 理解 Volume Binding

1. **理解 VolumeBinding 插件**：
   - 存储可行性与调度的连接
   - 阅读 `pkg/scheduler/framework/plugins/volumebinding/`

2. **理解绑定模式**：
   - `WaitForFirstConsumer` — 等待 Pod 调度后再绑定
   - `Immediate` — 立即绑定
   - 影响调度的可行性判断

#### 1.3.5 理解 DRA（Dynamic Resource Allocation）

1. **理解 DRA 概念**：
   - 动态资源分配（GPU 等）
   - `ResourceClaim` — 资源请求
   - `ResourceSlice` — 资源切片

2. **阅读 `pkg/scheduler/framework/plugins/dynamicresources/`**：
   - DRA 插件实现
   - 与调度器的集成

3. **理解 DRA 流程**：
   ```
   1. Pod 声明 ResourceClaim
   2. DRA 驱动分配资源
   3. 创建 ResourceSlice
   4. 调度器使用 DRA 插件检查可行性
   5. Kubelet 使用分配的资源
   ```

#### 1.3.6 理解 PDB（PodDisruptionBudget）

1. **理解 PDB 模型**：
   - 限制同时驱逐的 Pod 数量
   - `minAvailable` — 最小可用数量
   - `maxUnavailable` — 最大不可用数量

2. **理解实现**：
   - PDB 不是调度器插件
   - 由 `disruption` 控制器管理
   - 调度器通过 `Eviction` API 检查 PDB

3. **阅读 `pkg/controller/disruption/`**：
   - PDB 控制器实现
   - 驱逐检查逻辑

---

### 1.4 API 生命周期和治理

**需要做的事**：

#### 1.4.1 理解 API 版本演进

1. **理解 API 版本策略**：
   - `v1` — 稳定版本，不再添加字段
   - `v1beta1` — 测试版本，可能变更
   - `v1alpha1` — 实验版本，可能删除

2. **理解版本转换**：
   - Hub/Spoke 模式
   - 内部版本作为 Hub
   - 外部版本通过转换函数连接

3. **理解字段管理**：
   - 添加字段 — 向后兼容
   - 删除字段 — 需要废弃周期
   - 修改字段类型 — 不兼容，需要新版本

#### 1.4.2 理解 API Priority and Fairness (APF)

1. **理解 APF 概念**：
   - 控制 API 请求的优先级和公平性
   - 防止低优先级请求耗尽资源
   - 阅读 `staging/src/k8s.io/apiserver/pkg/apf/`

2. **理解配置**：
   - `FlowSchema` — 流模式
   - `PriorityLevelConfiguration` — 优先级级别配置
   - 基于用户、动词、资源的匹配

#### 1.4.3 理解垃圾回收

1. **理解 Owner Reference**：
   - 对象间的父子关系
   - `ownerReferences` 字段
   - 级联删除

2. **阅读 `pkg/controller/garbagecollector/`**：
   - 垃圾回收控制器
   - 监听删除事件
   - 级联删除子对象

3. **理解删除策略**：
   - `Foreground` — 前台级联删除
   - `Background` — 后台级联删除
   - `Orphan` — 孤儿化

---

## 2. 关键文件清单

| 文件/目录 | 说明 | 阅读重点 |
|-----------|------|----------|
| `staging/src/k8s.io/apiextensions-apiserver/` | API 扩展服务器 | CRD REST 存储 |
| `pkg/controller/crd/` | CRD 相关控制器 | Finalizer 处理 |
| `staging/src/k8s.io/sample-controller/` | 示例控制器 | Operator 模式 |
| `pkg/controller/horizontalpodautoscaler/` | HPA 控制器 | 扩缩容计算 |
| `pkg/kubelet/prober/` | 探针管理 | 探针执行 |
| `staging/src/k8s.io/client-go/tools/leaderelection/leaderelection.go` | Leader 选举 | `NewLeaderElector()` 第82行、`Run()` 第284行 |
| `staging/src/k8s.io/client-go/tools/record/` | 事件记录 | `EventBroadcaster` |
| `staging/src/k8s.io/component-base/metrics/` | 组件指标 | 指标注册 |
| `pkg/scheduler/framework/plugins/defaultpreemption/` | 默认抢占 | 抢占逻辑 |
| `pkg/scheduler/framework/plugins/podtopologyspread/` | 拓扑分布 | 偏差计算 |
| `pkg/scheduler/framework/plugins/dynamicresources/` | DRA | 资源分配 |
| `pkg/controller/disruption/` | PDB 控制器 | 驱逐检查 |
| `pkg/controller/garbagecollector/` | 垃圾回收 | 级联删除 |
| `staging/src/k8s.io/apiserver/pkg/apf/` | API 优先级和公平性 | 流控制 |

---

## 3. 实践任务清单

- [ ] **任务 1**：阅读示例控制器，理解 Operator 模式的完整实现
- [ ] **任务 2**：写一个简单的 CRD + 控制器，实现基本的 reconcile 逻辑
  - 实验：创建一个简单的 CRD 并验证
    ```bash
    # 1. 创建 CRD
    kubectl apply -f - <<'EOF'
    apiVersion: apiextensions.k8s.io/v1
    kind: CustomResourceDefinition
    metadata:
      name: mycrds.example.com
    spec:
      group: example.com
      versions:
      - name: v1
        served: true
        storage: true
        schema:
          openAPIV3Schema:
            type: object
            properties:
              spec:
                type: object
                properties:
                  replicas:
                    type: integer
              status:
                type: object
                properties:
                  readyReplicas:
                    type: integer
        subresources:
          status: {}
      scope: Namespaced
      names:
        plural: mycrds
        singular: mycrd
        kind: MyCRD
    EOF

    # 2. 创建 CR 实例
    kubectl apply -f - <<'EOF'
    apiVersion: example.com/v1
    kind: MyCRD
    metadata:
      name: my-crd-instance
      namespace: default
    spec:
      replicas: 3
    EOF

    # 3. 验证 CRD 和 CR
    kubectl get crd mycrds.example.com
    kubectl get mycrds -n default
    kubectl get mycrds my-crd-instance -n default -o yaml

    # 4. 清理
    kubectl delete mycrds my-crd-instance -n default
    kubectl delete crd mycrds.example.com
    ```
- [ ] **任务 3**：阅读 HPA 控制器，理解 metrics 消费和扩缩容计算
  - 注意：HPA 控制器在 `kubernetes/autoscaler` 仓库中，不在核心仓库
- [ ] **任务 4**：阅读 Leader Election 实现，理解 Lease 机制
  - 实验：查看 `staging/src/k8s.io/client-go/tools/leaderelection/leaderelection.go:82` 的 `NewLeaderElector()` 函数
- [ ] **任务 5**：阅读抢占插件，理解 victim Pod 的选择逻辑
  - 实验：查看 `pkg/scheduler/framework/plugins/defaultpreemption/` 目录
- [ ] **任务 6**：阅读拓扑分布插件，理解偏差计算
  - 实验：查看 `pkg/scheduler/framework/plugins/podtopologyspread/` 目录
- [ ] **任务 7**：理解 Finalizer 的工作流程，写一个带 finalizer 的控制器
- [ ] **任务 8**：阅读垃圾回收控制器，理解级联删除的两种策略
  - 实验：查看 `pkg/controller/garbagecollector/` 目录
- [ ] **任务 9**：理解 APF 的配置，如何为不同用户设置不同的优先级
- [ ] **任务 10**：阅读 DRA 插件，理解动态资源分配的完整流程
  - 实验：查看 `pkg/scheduler/framework/plugins/dynamicresources/` 目录

**实验代码**：理解 Leader Election 的工作原理
```go
// staging/src/k8s.io/client-go/tools/leaderelection/leaderelection.go:82
func NewLeaderElector(lec LeaderElectionConfig) (*LeaderElector, error) {
    if lec.LeaseDuration <= lec.RenewDeadline {
        return nil, fmt.Errorf("leaseDuration must be greater than renewDeadline")
    }
    if lec.RenewDeadline <= 0 {
        return nil, fmt.Errorf("renewDeadline must be greater than 0")
    }
    // ...
    return &LeaderElector{
        LeaseDuration: lec.LeaseDuration,
        RenewDeadline: lec.RenewDeadline,
        RetryPeriod:   lec.RetryPeriod,
        // ...
    }, nil
}

// staging/src/k8s.io/client-go/tools/leaderelection/leaderelection.go:284
func (le *LeaderElector) Run(ctx context.Context) {
    defer runtime.HandleCrashWithContext(ctx)
    // 1. 尝试获取 Lease
    // 2. 如果成功，成为 Leader，执行 OnStartedLeading
    // 3. 定期更新 Lease
    // 4. 如果失败，等待并重试
    // 5. 如果失去 Leader，执行 OnStoppedLeading
}
```

**实验代码**：查看集群中的 Leader Election 状态
```bash
# 1. 查看 Controller Manager 的 Lease
kubectl get leases -n kube-system | grep controller-manager

# 2. 查看 Scheduler 的 Lease
kubectl get leases -n kube-system | grep scheduler

# 3. 查看 Lease 详情
kubectl get lease -n kube-system -l component=controller-manager -o yaml

# 4. 观察 Holder Identity（当前 Leader 的身份）
kubectl get lease -n kube-system -o jsonpath='{range .items[*]}{.metadata.name}:{.spec.holderIdentity}"\n"{end}'
```

---

## 4. 核心模式总结

### Operator 模式

```
1. 定义 CRD
    │
    ▼
2. 创建 CR 实例
    │
    ▼
3. Operator 控制器
    ├── Informer 监听 CR 变化
    ├── EventHandler enqueue 到 workqueue
    ├── Worker 从队列取出
    └── Reconcile 执行
        ├── 读取 CR 的 spec（期望状态）
        ├── 读取 CR 的 status（实际状态）
        ├── 创建/更新/删除子资源
        ├── 更新 CR 的 status
        └── 处理 finalizer（清理）
```

### Leader Election 流程

```
实例 A          实例 B          etcd (Lease 对象)
  │               │                 │
  ├── 尝试获取 Lease ──────────→ 失败（已被 B 持有）
  │               │                 │
  │               ├── 获取 Lease ──→ 成功
  │               │                 │
  │               ├── 成为 Leader
  │               │                 │
  │               ├── 定期更新 Lease
  │               │                 │
  ├── 检测到 Lease 过期
  │               │                 │
  ├── 尝试获取 Lease ──────────→ 成功（B 已停止更新）
  │               │                 │
  ├── 成为 Leader
```

### HPA 扩缩容流程

```
1. HPA 控制器定期运行
    │
    ▼
2. 获取当前指标
    ├── MetricsClient → Metrics Server → kubelet
    └── 获取 CPU/内存/自定义指标
    │
    ▼
3. 计算目标副本数
    ├── 基于目标利用率
    ├── 应用扩缩容策略
    └── ReplicaCalculator
    │
    ▼
4. 更新 Scale 对象
    ├── Deployment/ReplicaSet/StatefulSet
    └── 控制器检测到 replicas 变化
    │
    ▼
5. 控制器创建/删除 Pod
```

---

## 5. 检查点

完成学习方案前，确保你能回答：

1. CRD 和普通的 API 资源有什么区别？
2. Operator 模式和普通控制器的区别？
3. Finalizer 的工作流程是什么？为什么需要它？
4. Status subresource 的作用是什么？
5. HPA 如何获取指标？扩缩容的计算逻辑是什么？
6. Leader Election 的 Lease 机制如何工作？
7. 抢占的触发条件和执行流程？
8. 拓扑分布的 MaxSkew 如何计算？
9. DRA 和传统的资源请求有什么区别？
10. 垃圾回收的 Foreground 和 Background 策略的区别？

---

## 6. 学习完成

恭喜你完成了 Kubernetes 源码学习方案的所有 6 个阶段！

### 后续建议

1. **持续实践**：选择一个感兴趣的领域深入，如调度器插件开发、Operator 开发等
2. **阅读 PR**：浏览 Kubernetes 仓库的 PR，理解社区的代码审查流程
3. **参与贡献**：从文档、测试开始，逐步参与代码贡献
4. **关注 SIG**：Kubernetes 按 SIG（Special Interest Group）组织，找到感兴趣的领域

### 资源链接

- [Kubernetes 官方文档](https://kubernetes.io/docs/)
- [client-go 文档](https://github.com/kubernetes/client-go)
- [API Machinery 文档](https://github.com/kubernetes/apiserver)
- [Kubernetes SIG 列表](https://github.com/kubernetes/community/tree/master/sigs)
- [Kubernetes 贡献指南](../CONTRIBUTING.md)
- [Kubernetes 开发指南](https://github.com/kubernetes/community/tree/master/contributors/devel)
