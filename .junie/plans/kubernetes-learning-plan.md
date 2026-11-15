---
sessionId: session-261114-223853-11qz
---

# 学习概述

## Kubernetes 全面学习计划

### 项目概况
Kubernetes 是一个用 Go 编写的容器编排系统，代码库包含约 50 万行代码。核心组件包括 API Server、Scheduler、Controller Manager、Kubelet 和 kube-proxy。

### 代码库结构
```
├── cmd/              # 可执行程序入口
│   ├── kube-apiserver/
│   ├── kube-scheduler/
│   ├── kube-controller-manager/
│   ├── kubelet/
│   ├── kube-proxy/
│   └── kubectl/
├── pkg/              # 核心库
│   ├── controlplane/ # API Server 控制面逻辑
│   ├── scheduler/    # 调度器
│   ├── controller/   # 控制器
│   ├── kubelet/      # Kubelet
│   ├── proxy/        # 网络代理
│   ├── volume/       # 卷插件
│   ├── registry/     # REST 存储实现
│   ├── apis/         # 内部 API 类型
│   └── features/     # 功能门控
├── staging/src/k8s.io/  # 独立发布的模块
│   ├── api/          # API 类型定义
│   ├── apimachinery/ # API 基础设施
│   ├── apiserver/    # 通用 API Server 框架
│   ├── client-go/    # 客户端库
│   ├── kube-scheduler/ # 调度器框架
│   └── kubelet/      # Kubelet API
├── test/             # 测试
│   ├── integration/  # 集成测试
│   ├── e2e/          # 端到端测试
│   └── e2e_node/     # 节点测试
└── hack/             # 构建和开发脚本
```

### 学习路径
分为 6 个阶段，从宏观到微观，从核心到周边，从理论到实践。

# 阶段一：项目整体架构

### 阶段一：理解 Kubernetes 整体架构

**目标**：建立对项目的整体认知，理解各组件如何协作。

**学习内容**：

1. **核心组件概览**
   - `cmd/` 下的各组件入口文件
   - 组件间通信方式（API、gRPC、本地 socket）

2. **关键文件**
   - `pkg/controlplane/instance.go` - API Server 控制面组装
   - `pkg/scheduler/scheduler.go` - 调度器主循环
   - `pkg/kubelet/kubelet.go` - Kubelet 核心结构体
   - `pkg/features/kube_features.go` - 功能门控定义

3. **Staging 模块机制**
   - `staging/src/k8s.io/` 下的独立模块
   - 理解为什么需要 staging（client-go、apimachinery 等被其他项目独立使用）

4. **构建系统**
   - `Makefile` - 主要构建目标
   - `hack/make-rules/` - 构建脚本
   - `hack/update-*.sh` - 代码生成脚本

**实践**：
- 阅读 `Makefile`，理解 `make all`、`make test`、`make verify` 的作用
- 运行 `make help` 查看所有可用目标
- 尝试构建一个组件：`make kubectl`

# 阶段二：API Server 深入

### 阶段二：API Server 深入

**目标**：理解 Kubernetes 的核心——API Server 如何处理请求、存储数据、触发事件。

**学习内容**：

1. **启动流程**
   - `cmd/kube-apiserver/app/server.go` - 启动入口
   - `pkg/controlplane/instance.go` - 控制面组装
   - Generic APIServer 框架（`staging/src/k8s.io/apiserver/`）

2. **REST 存储层**
   - `pkg/registry/` - 各资源的 REST 存储实现
   - `pkg/registry/core/pod/` - Pod 的存储实现为例
     - `storage.go` - 存储创建
     - `strategy.go` - 创建/更新策略

3. **Admission 机制**
   - Admission Controller 链
   - Validating/Mutating Admission Webhook

4. **Watch 机制**
   - Informer 模式
   - `staging/src/k8s.io/client-go/tools/cache/`

5. **API 版本管理**
   - `staging/src/k8s.io/api/` - API 类型定义
   - `pkg/apis/` - 内部类型
   - 版本转换机制

**实践**：
- 跟踪一个 Pod 创建请求从 API Server 接收到存入 etcd 的完整流程
- 调试运行 API Server，观察请求处理

# 阶段三：Scheduler 深入

### 阶段三：Scheduler 深入（重点）

**目标**：深入理解 Pod 调度机制，包括插件化框架、调度流程、内置插件。

**学习内容**：

1. **调度器架构**
   - `pkg/scheduler/scheduler.go` - 调度器主结构
   - `pkg/scheduler/schedule_one.go` - 单次调度流程
   - 调度队列：`pkg/scheduler/backend/queue/`

2. **插件化框架**
   - `pkg/scheduler/framework/interface.go` - 插件接口定义
   - `pkg/scheduler/framework/types.go` - 核心类型
   - 调度插件扩展点：
     - QueueSort - 队列排序
     - PreFilter - 预过滤
     - Filter - 节点过滤
     - PreScore - 预打分
     - Score - 节点打分
     - Reserve - 预留
     - Permit - 许可
     - PreBind/Bind/PostBind - 绑定

3. **内置插件**（`pkg/scheduler/framework/plugins/`）
   - `noderesources/` - 资源分配
   - `tainttoleration/` - 污点容忍
   - `nodeaffinity/` - 节点亲和性
   - `interpodaffinity/` - Pod 亲和性
   - `podtopologyspread/` - 拓扑分布
   - `volumebinding/` - 卷绑定
   - `queuesort/` - 队列排序
   - `defaultbinder/` - 默认绑定器
   - `defaultpreemption/` - 默认抢占

4. **缓存层**
   - `pkg/scheduler/backend/cache/` - 内部缓存
   - `pkg/scheduler/backend/api_cache/` - API 缓存

5. **配置**
   - `pkg/scheduler/apis/config/` - 调度器配置 API

**实践**：
- 实现一个简单的调度插件
- 调试调度流程，观察各插件的调用顺序
- 运行 `test/integration/scheduler/` 中的集成测试

# 阶段四：Controller Manager 深入

### 阶段四：Controller Manager 深入

**目标**：理解控制器的设计模式、各类控制器的实现。

**学习内容**：

1. **控制器框架**
   - `pkg/controller/` - 控制器实现
   - `cmd/kube-controller-manager/app/` - 启动入口
   - Controller-Manager 如何组织各控制器

2. **核心控制器**
   - `pkg/controller/deployment/` - Deployment 控制器
   - `pkg/controller/replicaset/` - ReplicaSet 控制器
   - `pkg/controller/daemon/` - DaemonSet 控制器
   - `pkg/controller/statefulset/` - StatefulSet 控制器
   - `pkg/controller/nodelifecycle/` - 节点生命周期
   - `pkg/controller/garbagecollector/` - 垃圾回收

3. **控制器通用模式**
   - Informer + WorkQueue 模式
   - `pkg/controller/controller_utils.go` - 通用工具
   - `pkg/controller/controller_ref_manager.go` - 所有者引用管理

4. **Staging 中的控制器**
   - `staging/src/k8s.io/controller-manager/` - 控制器管理器框架

**实践**：
- 实现一个简单的自定义控制器
- 跟踪 Deployment 从创建到 Pod 运行的完整流程

# 阶段五：Kubelet 深入

### 阶段五：Kubelet 深入

**目标**：理解节点代理如何管理容器生命周期、处理卷、报告状态。

**学习内容**：

1. **Kubelet 核心**
   - `pkg/kubelet/kubelet.go` - 核心结构体（约 3800 行）
   - `pkg/kubelet/server/` - 服务端（API、cgroups）
   - `pkg/kubelet/config/` - 配置源

2. **容器运行时**
   - `pkg/kubelet/container/runtime.go` - 运行时接口
   - `pkg/kubelet/kuberuntime/` - CRI 实现
   - `staging/src/k8s.io/cri-api/` - CRI API 定义
   - `staging/src/k8s.io/cri-client/` - CRI 客户端

3. **Pod 生命周期**
   - `pkg/kubelet/pod_workers.go` - Pod 工作线程
   - `pkg/kubelet/container/sync_result.go` - 同步结果
   - Mirror Pod 机制

4. **卷管理**
   - `pkg/kubelet/volumemanager/` - 卷管理器
   - `pkg/volume/` - 卷插件框架
   - `pkg/volume/csi/` - CSI 插件

5. **资源管理**
   - `pkg/kubelet/cm/` - 容器管理器（CPU Manager、Topology Manager）
   - `pkg/kubelet/lifecycle/` - 生命周期管理
   - `pkg/kubelet/qos/` - QoS 层级

6. **状态报告**
   - `pkg/kubelet/kubelet_node_status.go` - 节点状态
   - `pkg/kubelet/status/` - Pod 状态管理

**实践**：
- 跟踪 Pod 从同步到容器创建的完整流程
- 调试 Kubelet，观察容器生命周期管理

# 阶段六：周边系统与工程实践

### 阶段六：周边系统与工程实践

**目标**：掌握测试体系、网络代理、构建发布流程。

**学习内容**：

1. **kube-proxy**
   - `pkg/proxy/iptables/` - iptables 模式
   - `pkg/proxy/ipvs/` - IPVS 模式
   - `pkg/proxy/nftables/` - nftables 模式
   - Service 流量转发机制

2. **测试体系**
   - `test/integration/` - 集成测试框架
   - `test/e2e/` - 端到端测试
   - `test/e2e_node/` - 节点测试
   - 如何编写和运行测试

3. **代码生成**
   - `staging/src/k8s.io/code-generator/` - 代码生成器
   - `hack/update-codegen.sh` - 更新生成的代码
   - DeepCopy、Conversion、Defaults 生成

4. **依赖管理**
   - `hack/pin-dependency.sh` - 添加/更新依赖
   - `hack/update-vendor.sh` - 更新 vendor
   - go.work 工作空间

5. **提交规范**
   - `CONTRIBUTING.md` - 贡献指南
   - `make verify` - 所有验证检查
   - `make update` - 所有生成器和格式化器

**实践**：
- 运行一组集成测试
- 尝试添加一个简单的功能门控
- 运行 `make verify` 和 `make update`

# Delivery Steps

### * Step 1: 阶段一：项目整体架构认知
建立对 Kubernetes 代码库的整体认知
- 阅读 `Makefile` 和 `hack/` 下的关键脚本，理解构建系统
- 浏览 `cmd/` 下各组件的入口文件，理解各组件的启动方式
- 阅读 `pkg/features/kube_features.go`，了解功能门控机制
- 理解 `staging/src/k8s.io/` 的模块划分和发布机制
- 运行 `make help` 查看可用目标，尝试 `make kubectl` 构建单个组件

### ✓ Step 2: 阶段二：API Server 控制面深入
深入理解 API Server 的请求处理、存储和事件机制
- 阅读 `cmd/kube-apiserver/app/server.go`，理解启动流程
- 阅读 `pkg/controlplane/instance.go`，理解控制面如何组装各资源的 REST Storage
- 以 Pod 为例，阅读 `pkg/registry/core/pod/`，理解 REST Storage 的实现模式
- 学习 Generic APIServer 框架（`staging/src/k8s.io/apiserver/`）的核心概念
- 学习 Informer 模式（`staging/src/k8s.io/client-go/tools/cache/`）
- 实践：跟踪一个 Pod 创建请求从接收到存储的完整调用链

### ✓ Step 3: 阶段三：Scheduler 调度器深入
深入理解调度器的插件化框架和调度流程
- 阅读 `pkg/scheduler/scheduler.go`，理解调度器主循环
- 阅读 `pkg/scheduler/schedule_one.go`，理解单次调度流程
- 阅读 `pkg/scheduler/framework/interface.go` 和 `types.go`，理解插件接口
- 学习内置插件：`noderesources`、`tainttoleration`、`nodeaffinity`、`interpodaffinity`、`podtopologyspread`、`volumebinding`、`queuesort`、`defaultbinder`、`defaultpreemption`
- 学习调度队列：`pkg/scheduler/backend/queue/`
- 实践：实现一个简单的调度插件，调试观察各插件的调用顺序

### ✓ Step 4: 阶段四：Controller Manager 控制器深入
理解控制器的设计模式和各类控制器的实现
- 阅读 `pkg/controller/controller_utils.go` 和 `controller_ref_manager.go`，理解控制器通用模式
- 以 Deployment 为例，阅读 `pkg/controller/deployment/` 和 `pkg/controller/replicaset/`
- 阅读 `pkg/controller/nodelifecycle/`，理解节点生命周期管理
- 学习 Informer + WorkQueue 模式在各控制器中的应用
- 实践：跟踪 Deployment 从创建到 Pod 运行的完整流程

### ✓ Step 5: 阶段五：Kubelet 节点代理深入
理解 Kubelet 如何管理容器生命周期、处理卷、报告状态
- 阅读 `pkg/kubelet/kubelet.go` 的核心结构体定义和同步循环
- 阅读 `pkg/kubelet/container/runtime.go`，理解容器运行时接口
- 阅读 `pkg/kubelet/pod_workers.go`，理解 Pod 工作线程机制
- 学习 CRI：`staging/src/k8s.io/cri-api/` 和 `pkg/kubelet/kuberuntime/`
- 学习卷管理：`pkg/kubelet/volumemanager/` 和 `pkg/volume/`
- 学习资源管理：`pkg/kubelet/cm/`（CPU Manager、Topology Manager）
- 实践：跟踪 Pod 从同步到容器创建的完整流程

### ✓ Step 6: 阶段六：周边系统与工程实践
掌握测试体系、网络代理、构建发布流程
- 学习 kube-proxy：`pkg/proxy/iptables/` 和 `pkg/proxy/ipvs/`，理解 Service 流量转发
- 学习测试体系：`test/integration/` 的集成测试框架
- 学习代码生成：`staging/src/k8s.io/code-generator/` 和 `hack/update-codegen.sh`
- 学习依赖管理：`hack/pin-dependency.sh` 和 `hack/update-vendor.sh`
- 实践：运行一组集成测试，尝试添加一个简单的功能门控，运行 `make verify` 和 `make update`