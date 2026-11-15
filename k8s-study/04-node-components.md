# 第4阶段：节点组件

**目标**：理解 Kubelet 和 Kube-proxy 的架构。

**前置要求**：完成第3阶段，理解控制器模式和 API Server。

---

## 1. 你需要做什么

### 1.1 Kubelet

Kubelet 是运行在每个节点上的代理，负责管理 Pod 的生命周期。

**需要做的事**：

#### 1.1.1 理解 Kubelet 架构

Kubelet 的核心职责：
- 从 API Server 获取 Pod 期望状态
- 通过 CRI（Container Runtime Interface）管理容器
- 管理卷的 mount/unmount
- 管理健康检查探针
- 上报 Pod 和节点状态

**组件关系**：
```
Kubelet
├── Pod workers (per-Pod sync)
├── Status manager (status update)
├── CRI client (container runtime)
├── Volume manager (mount/unmount)
├── Prober (health checks)
├── Container manager (resource management)
└── PLEG (pod lifecycle event generator)
```

#### 1.1.2 理解启动流程

1. **阅读 Kubelet 启动入口**：
   - `cmd/kubelet/app/server.go` — Kubelet 主入口
   - `pkg/kubelet/kubelet.go:479` — `NewMainKubelet()` 创建 Kubelet 实例
   - `pkg/kubelet/kubelet.go:1965` — `Run()` 启动 Kubelet 主循环
   - `pkg/kubelet/kubelet.go:2783` — `syncLoop()` 同步循环
   - `pkg/kubelet/kubelet.go:2858` — `syncLoopIteration()` 单次迭代

2. **阅读 `pkg/kubelet/kubelet.go`** — Kubelet 主结构体：
   - `Kubelet` 结构体 — 数十个字段，理解每个字段的职责
   - `podWorkers` — per-Pod 工作器
   - `statusManager` — 状态管理器
   - `containerRuntime` — CRI 客户端
   - `volumeManager` — 卷管理器
   - `proberManager` — 探针管理器

3. **理解 Kubelet 的配置**：
   - `KubeletConfiguration` — 配置结构体
   - `KubeletFlags` — 命令行标志
   - `KubeletPluginWatcher` — 插件监听器

**实验代码**：查看 Kubelet 的启动流程
```go
// pkg/kubelet/kubelet.go:1965
func (kl *Kubelet) Run(ctx context.Context, updates <-chan kubetypes.PodUpdate) {
    logger := klog.FromContext(ctx)
    // 1. 启动各种管理器
    kl.statusManager.Start(ctx)
    kl.volumeManager.Start(ctx)
    kl.proberManager.Start(ctx)
    // 2. 启动同步循环
    kl.syncLoop(ctx, updates, nil)
}

// pkg/kubelet/kubelet.go:2783
func (kl *Kubelet) syncLoop(ctx context.Context, updates <-chan kubetypes.PodUpdate, handler SyncHandler) {
    logger := klog.FromContext(ctx)
    logger.Info("Starting kubelet main sync loop")
    // 主循环：处理 Pod 更新、定期同步、垃圾回收
    for {
        select {
        case u, ok := <-updates:
            if !ok {
                return
            }
            kl.handlePodSyncs(u)
        case <-kl.syncCh:
            kl.handlePodSyncs(kl.getPodsToSync())
        case <-kl.housekeepingCh:
            kl.doGCMutex()
        case <-kl.plegCh:
            kl.handlePodSyncs(kl.getPodsToSync())
        }
    }
}
```

#### 1.1.3 理解 Pod 同步流程

**核心流程**：
```
1. Kubelet 通过 Informer 监听 Pod 变化
2. HandlePodSyncs() 收到同步请求
3. 分发到 per-Pod worker: podWorkers.UpdatePod()
4. worker 执行 syncPod()
5. 对比期望状态和实际状态
6. 通过 CRI 创建/更新/删除容器
7. 更新 Pod 状态
```

**需要做的事**：

1. **阅读 `pkg/kubelet/kubelet.go` 的 `HandlePodSyncs()`**：
   ```go
   func (kl *Kubelet) HandlePodSyncs(pods []*v1.Pod) {
       // 将 Pod 同步请求分发到 per-Pod worker
       for _, pod := range pods {
           kl.podWorkers.UpdatePod(...)
       }
   }
   ```

2. **阅读 `pkg/kubelet/pod_workers.go`** — per-Pod 工作器：
   - 每个 Pod 一个 worker，避免并发修改
   - `UpdatePod()` — 触发 Pod 同步
   - `syncPod()` — 实际同步逻辑
   - 理解 `mirrorPod` 和 `staticPod` 的特殊处理

3. **理解 `syncPod()` 流程**：
   ```go
   func (kl *Kubelet) syncPod(...) {
       // 1. 检查 Pod 是否应该运行
       // 2. 计算容器变化
       // 3. 通过 CRI 管理容器
       // 4. 管理卷
       // 5. 更新状态
   }
   ```

**实验代码**：查看 Kubelet 如何同步 Pod
```go
// pkg/kubelet/kubelet.go:1965 — Run 启动同步循环
// pkg/kubelet/kubelet.go:2783 — syncLoop 主循环
// pkg/kubelet/kubelet.go:2858 — syncLoopIteration 单次迭代
// pkg/kubelet/pod_workers.go — per-Pod worker 管理
```

4. **理解 PLEG（Pod Lifecycle Event Generator）**：
   - 定期探测容器运行时，生成 Pod 生命周期事件
   - 独立于 CRI 的同步机制
   - 阅读 `pkg/kubelet/pleg/`

#### 1.1.4 理解 CRI 集成

1. **阅读 `pkg/kubelet/cri/`**：
   - `cri.RuntimeService` — 容器运行时服务
   - `cri.ImageService` — 镜像服务
   - `remote.RuntimeService` — 远程 CRI 实现（gRPC）

2. **理解 CRI 接口**：
   - `RunPodSandbox()` / `StopPodSandbox()` / `RemovePodSandbox()` — Pod 沙箱
   - `CreateContainer()` / `StartContainer()` / `StopContainer()` / `RemoveContainer()` — 容器
   - `ListContainers()` / `ContainerStatus()` — 容器状态
   - `PullImage()` / `ListImages()` — 镜像管理

3. **理解容器运行时**：
   - containerd — 主流运行时
   - CRI-O — 另一个主流运行时
   - Docker（已弃用）

#### 1.1.5 理解卷管理

1. **阅读 `pkg/kubelet/volumemanager/`** — 节点卷管理器：
   - `volumeManager` — 跟踪节点上 Pod 所需的卷
   - `operationExecutor` — 执行卷操作
   - `actualStateOfWorld` — 实际状态
   - `desiredStateOfWorld` — 期望状态

2. **理解卷操作**：
   - `SetUp()` — 挂载卷
   - `TearDown()` — 卸载卷
   - `Mount()` / `Unmount()` — 具体操作

3. **理解卷插件**：
   - `pkg/volume/` — 卷插件框架
   - `VolumePlugin` 接口
   - In-tree 插件 vs CSI 插件

#### 1.1.6 理解探针管理

1. **阅读 `pkg/kubelet/prober/`**：
   - `proberManager` — 探针管理器
   - `probeManager` — 具体探针管理
   - 三种探针：
     - Liveness — 失败时重启容器
     - Readiness — 失败时从 Service 端点移除
     - Startup — 失败时重启，成功后禁用

2. **理解探针执行**：
   - `probeRunner` — 执行探针
   - TCP、HTTP、Exec 三种探针类型
   - 探针结果如何影响 Pod 状态

#### 1.1.7 理解状态管理

1. **阅读 `pkg/kubelet/status/`**：
   - `statusManager` — 状态管理器
   - 合成 Pod 状态：容器状态 + 卷状态 + 探针状态
   - 上报到 API Server

2. **理解状态上报**：
   - 定期上报节点状态
   - Pod 状态变化时立即上报
   - 使用 `Lease` 进行心跳

---

### 1.2 Kube-proxy

Kube-proxy 负责 Service 的网络规则同步。

**需要做的事**：

#### 1.2.1 理解代理模式

Kube-proxy 支持多种代理模式：

1. **iptables 模式**（默认）：
   - 使用 iptables 规则实现 Service 流量转发
   - 全量同步：每次变化重建所有规则
   - 阅读 `pkg/proxy/iptables/`

2. **IPVS 模式**：
   - 使用 IPVS 负载均衡
   - 增量同步：只更新变化的规则
   - 阅读 `pkg/proxy/ipvs/`

3. **nftables 模式**：
   - 使用 nftables 替代 iptables
   - DNAT + SNAT 实现
   - 阅读 `pkg/proxy/nftables/`

4. **Windows kernel mode**：
   - Windows 节点的网络规则

#### 1.2.2 理解 iptables 模式

1. **阅读 `pkg/proxy/iptables/proxier.go`**：
   - `NewProxier()` 构造函数：第209行 — 创建 iptables 代理
   - `Proxier` — iptables 代理
   - `Sync()` — 同步规则
   - `onServiceAdded()` / `onServiceUpdated()` / `onServiceRemoved()` — Service 变化
   - `onEndpointsAdded()` / `onEndpointsUpdated()` / `onEndpointsRemoved()` — 端点变化

2. **理解 iptables 规则结构**：
   ```
   KUBE-SERVICES chain
       └── 匹配 Service ClusterIP
           └── DNAT 到 KUBE-SEP-* chain
               └── 实际 Pod IP

   KUBE-NODEPORTS chain
       └── 匹配 NodePort
           └── 跳转到 KUBE-SERVICES
   ```

3. **理解 Service 类型**：
   - ClusterIP — 集群内部访问
   - NodePort — 节点端口暴露
   - LoadBalancer — 外部负载均衡器
   - ExternalName — CNAME 别名

#### 1.2.3 理解 IPVS 模式

1. **阅读 `pkg/proxy/ipvs/proxier.go`**：
   - `Proxier` — IPVS 代理
   - `syncProxyRules()` — 同步 IPVS 规则
   - `addVirtualService()` / `deleteVirtualService()` — 虚拟服务
   - `addVirtualServer()` / `deleteVirtualServer()` — 虚拟服务器

2. **理解 IPVS 负载均衡算法**：
   - RR（Round Robin）
   - LC（Least Connection）
   - DH（Destination Hashing）
   - SH（Source Hashing）

#### 1.2.4 理解 kube-proxy 启动

1. **阅读 `cmd/kube-proxy/app/server_others.go`**：
   - 配置加载
   - 代理模式选择
   - 启动流程

2. **理解配置**：
   ```yaml
   apiVersion: kubeproxy.config.k8s.io/v1alpha1
   kind: KubeProxyConfiguration
   mode: "iptables"  # iptables, ipvs, nftables
   ```

---

## 2. 关键文件清单

| 文件/目录 | 说明 | 阅读重点 |
|-----------|------|----------|
| `cmd/kubelet/app/server.go` | Kubelet 入口 | 启动配置 |
| `pkg/kubelet/kubelet.go` | Kubelet 主结构体 | `NewMainKubelet()` 第479行、`Run()` 第1965行、`syncLoop()` 第2783行、`syncLoopIteration()` 第2858行 |
| `pkg/kubelet/pod_workers.go` | Per-Pod 工作器 | `UpdatePod()`、`syncPod()` |
| `pkg/kubelet/pleg/` | PLEG | 生命周期事件生成 |
| `pkg/kubelet/cri/` | CRI 集成 | `RuntimeService` 接口 |
| `pkg/kubelet/volumemanager/` | 卷管理器 | `operationExecutor` |
| `pkg/kubelet/prober/` | 探针管理 | `proberManager` |
| `pkg/kubelet/status/` | 状态管理 | `statusManager` |
| `cmd/kube-proxy/app/server.go` | kube-proxy 入口 | 模式选择 |
| `pkg/proxy/` | Proxy 核心 | `Proxier` 接口 |
| `pkg/proxy/iptables/proxier.go` | iptables 模式 | `NewProxier()` 第209行、`Sync()` |
| `pkg/proxy/ipvs/` | IPVS 模式 | `syncProxyRules()` |
| `pkg/proxy/nftables/` | nftables 模式 | DNAT/SNAT |

---

## 3. 实践任务清单

- [ ] **任务 1**：阅读 `HandlePodSyncs()` 和 `podWorkers.UpdatePod()`，画出 Pod 同步流程图
  - 实验：查看 `pkg/kubelet/kubelet.go:1965` 的 `Run()` 函数，追踪到 `syncLoop()`（第2783行）
- [ ] **任务 2**：阅读 `syncPod()`，理解 Kubelet 如何对比期望和实际状态
- [ ] **任务 3**：阅读 CRI 接口，理解 Kubelet 如何与容器运行时交互
- [ ] **任务 4**：阅读 `proberManager`，理解探针如何影响 Pod 状态
- [ ] **任务 5**：阅读 iptables 模式的 `Sync()`，理解 Service 规则如何生成
  - 实验：查看 `pkg/proxy/iptables/proxier.go:209` 的 `NewProxier()` 函数
- [ ] **任务 6**：在本地集群中观察 kubelet 日志，看 Pod 创建时的同步过程
  - 实验：在 kind 集群中查看 kubelet 日志
    ```bash
    kubectl logs -n kube-system -l k8s-app=kube-proxy --tail=50
    ```
- [ ] **任务 7**：用 `iptables-save` 查看 kube-proxy 生成的规则
  - 实验：在节点上查看 iptables 规则
    ```bash
    # 查看 KUBE-SERVICES chain
    iptables-save | grep KUBE-SERVICES | head -20
    # 查看 KUBE-NODEPORTS chain
    iptables-save | grep KUBE-NODEPORTS | head -20
    ```
- [ ] **任务 8**：理解 PLEG 的作用，为什么 Kubelet 需要它

**实验代码**：查看 kube-proxy 的 iptables 规则生成
```bash
# 1. 创建一个 Service
kubectl expose deployment nginx --port=80 --target-port=80 --name=nginx-svc

# 2. 查看生成的 iptables 规则
iptables-save | grep nginx-svc

# 3. 查看 KUBE-SERVICES chain 的规则
iptables -t nat -L KUBE-SERVICES -n -v | head -20

# 4. 查看 KUBE-NODEPORTS chain 的规则
iptables -t nat -L KUBE-NODEPORTS -n -v | head -20

# 5. 删除 Service 并观察规则变化
kubectl delete service nginx-svc
```

---

## 4. 核心模式总结

### Kubelet Pod 同步流程

```
API Server
    │
    ▼ (Informer)
Kubelet
    │
    ├── HandlePodSyncs()
    │       │
    │       ▼
    │   podWorkers.UpdatePod()
    │       │
    │       ▼
    │   syncPod()
    │       │
    │       ├── 计算容器变化
    │       │       │
    │       │       ├── 通过 CRI 创建/删除容器
    │       │       │
    │       │       ▼
    │       │   管理卷 (volumeManager)
    │       │       │
    │       │       ▼
    │       │   更新状态 (statusManager)
    │       │
    │       ▼
    │   管理探针 (proberManager)
    │
    ▼ (PLEG)
容器运行时 (containerd/cri-o)
```

### kube-proxy 规则同步

```
Service/Endpoint Informer
    │
    ▼
Proxier
    │
    ├── onServiceAdded/Updated/Removed()
    ├── onEndpointsAdded/Updated/Removed()
    │
    ▼
Sync()
    │
    ├── iptables 模式：全量重建规则
    ├── IPVS 模式：增量更新规则
    └── nftables 模式：全量重建规则
    │
    ▼
网络规则 (iptables/IPVS/nftables)
```

---

## 5. 检查点

进入第5阶段前，确保你能回答：

1. Kubelet 如何获取 Pod 的期望状态？
2. per-Pod worker 的作用是什么？为什么需要它？
3. `syncPod()` 的主要步骤是什么？
4. CRI 接口提供了哪些核心操作？
5. 三种探针（Liveness、Readiness、Startup）的区别和作用？
6. kube-proxy 的 iptables 模式和 IPVS 模式的区别？
7. PLEG 的作用是什么？为什么 Kubelet 需要它？
8. 卷管理器如何跟踪期望状态和实际状态？

---

## 6. 下一步

完成本阶段后，进入 [第5阶段：网络、存储、安全](./05-network-storage-security.md)。

网络、存储、安全是 Kubernetes 的三大基础设施。理解节点组件如何工作后，理解这些跨节点的基础设施是自然的下一步。
