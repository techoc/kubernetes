---
sessionId: session-261114-225812-1b65
---

# 学习方案

### 背景
Go 新手 + Kubernetes 入门，首次阅读 kubernetes/kubernetes 源码。项目庞大（40+万行 Go 代码），需要循序渐进。

### 项目概览
Kubernetes 是容器编排系统，核心组件：
- **API Server** (`cmd/kube-apiserver`) — 集群入口，REST API 服务
- **Scheduler** (`cmd/kube-scheduler`) — Pod 调度，决定 Pod 跑哪个节点
- **Controller Manager** (`cmd/kube-controller-manager`) — 控制器集合，维护期望状态
- **Kubelet** (`cmd/kubelet`) — 节点代理，管理本节点容器生命周期
- **kube-proxy** (`cmd/kube-proxy`) — 服务网络，维护网络规则
- **kubectl** (`cmd/kubectl`) — CLI 客户端

代码组织：
- `cmd/` — 各组件入口（main 函数）
- `pkg/` — 核心业务逻辑
- `staging/src/k8s.io/` — 独立发布的库（api, client-go, apimachinery 等）
- `test/` — 测试（e2e、集成、单元测试）
- `hack/` — 构建、验证、代码生成脚本

### 学习路线

#### 第一阶段：项目结构和构建系统（1-2 周）
**目标**：能编译运行项目，理解代码组织方式

- 阅读 `README.md` 和 `CONTRIBUTING.md`，了解项目背景
- 理解 `Makefile` 的主要目标：`make all`、`make test`、`make verify`、`make update`
- 了解 `hack/` 目录：构建脚本 (`hack/build-go.sh`)、测试脚本 (`hack/test-go.sh`)、验证脚本 (`hack/verify-*.sh`) 和更新脚本 (`hack/update-*.sh`)
- 理解 `staging/` 机制：`staging/README.md` 解释了为什么代码放在这里，以及如何发布为独立模块
- 了解 `go.work` 工作空间：Kubernetes 使用 Go workspace 管理 staging 模块
- 运行 `make` 编译项目，观察输出目录 `_output/`
- 运行 `make test WHAT=./pkg/controller` 跑一组单元测试

#### 第二阶段：API 系统和核心概念（2-3 周）
**目标**：理解 Kubernetes 的 API 对象模型和 API 机制

- 从 `staging/src/k8s.io/api/core/v1/types.go` 开始，理解 Pod、Node、Service 等核心类型的定义
- 阅读 `staging/src/k8s.io/apimachinery/` — API 基础设施：
  - `pkg/runtime/` — 对象编解码、序列化
  - `pkg/apis/meta/v1/` — 元数据类型（ListMeta、ObjectMeta）
  - `pkg/watch/` — Watch 机制
- 理解 `pkg/features/` — Feature Gate 机制，如何控制功能开关
- 阅读 `pkg/api/` — API 注册和编组逻辑

#### 第三阶段：API Server（2-3 周）
**目标**：理解集群入口如何工作

- 从入口开始：`cmd/kube-apiserver/apiserver.go` → `cmd/kube-apiserver/app/`
- 阅读 `cmd/kube-apiserver/app/server.go` — 如何构建和启动 API Server
- 理解 `pkg/kubeapiserver/` — API Server 的具体配置：
  - `default_storage_factory_builder.go` — 如何注册各种资源的存储后端
  - `admission/` — Admission 插件链
  - `authenticator/`、`authorizer/` — 认证和授权
- 理解 Generic APIServer（`staging/src/k8s.io/apiserver/`）：
  - `pkg/server/` — 通用 API Server 框架
  - `pkg/registry/generic/` — 通用注册表
  - `pkg/endpoints/` — 端点发现

#### 第四阶段：Controller Manager（2-3 周）
**目标**：理解控制器模式——Kubernetes 的核心设计哲学

- 入口：`cmd/kube-controller-manager/app/`
- 理解控制器通用模式：
  - `pkg/controller/controller_utils.go` — 工作队列、反压、分片
  - `pkg/controller/controller_ref_manager.go` — 所有者引用管理
- 选择一个具体控制器深入：
  - `pkg/controller/deployment/` — Deployment 控制器（推荐，覆盖 ReplicaSet → Pod 链路）
  - `pkg/controller/replicaset/` — ReplicaSet 控制器
  - `pkg/controller/endpoint/` — Endpoint 控制器
- 理解 `staging/src/k8s.io/client-go/`：
  - `tools/cache/` — Informer 机制（ListWatch → DeltaFIFO → SharedInformer）
  - `tools/clientcmd/` — kubeconfig 加载
  - `tools/record/` — 事件记录

#### 第五阶段：Scheduler（1-2 周）
**目标**：理解 Pod 如何被调度到节点

- 入口：`cmd/kube-scheduler/app/`
- 核心：`pkg/scheduler/scheduler.go` — 调度器主循环
- `pkg/scheduler/algorithm.go` — 调度算法接口
- `pkg/scheduler/framework/` — 插件化调度框架：
  - 调度阶段：Filter → Score → Reserve → Bind
  - 插件接口和扩展点
- `pkg/scheduler/profile/` — 调度配置 Profile

#### 第六阶段：Kubelet（2-3 周）
**目标**：理解节点上如何管理容器

- 入口：`cmd/kubelet/app/server.go`
- 核心：`pkg/kubelet/kubelet.go` — Kubelet 主结构体
- 关键子系统：
  - `pkg/kubelet/container/` — 容器运行时接口
  - `pkg/kubelet/volume/`、`pkg/volume/` — 卷管理
  - `pkg/kubelet/prober/` — 健康检查
  - `pkg/kubelet/lifecycle/` — Pod  admission 决策
  - `pkg/kubelet/cm/` — 容器管理器（资源管理）
  - `pkg/kubelet/eviction/` — 驱逐机制

#### 第七阶段：kubectl 和客户端（1-2 周）
**目标**：理解 CLI 如何与集群交互

- 入口：`cmd/kubectl/kubectl.go`
- `pkg/kubectl/` — 命令实现
- 理解 `staging/src/k8s.io/cli-runtime/` — CLI 运行时

#### 第八阶段：测试和贡献（持续）
**目标**：了解如何为项目做贡献

- `test/` 目录结构：
  - `test/e2e/` — 端到端测试
  - `test/integration/` — 集成测试
  - `test/e2e_node/` — 节点级 e2e
- 阅读 `CONTRIBUTING.md` 和 `hack/` 中的验证脚本
- 了解代码生成：`hack/update-codegen.sh`、`make update`

### 学习方法建议
1. **不要试图读完所有代码** — 按组件深入，每个组件选 1-2 个核心流程跟踪
2. **配合运行** — 使用 `make quick-release` 或 `hack/local-up-cluster.sh` 启动本地集群，边看边调试
3. **画图** — 对每个组件画数据流图，理解请求如何流转
4. **从测试入手** — 单元测试比源码更容易理解预期行为
5. **先理解模式** — Kubernetes 大量使用控制器模式、Informer 模式、工作队列模式，先理解这些通用模式
6. **利用 DeepWiki** — 项目有 DeepWiki 文档覆盖所有主要模块，可以作为快速参考

# Delivery Steps

### * Step 1: 阶段一：项目结构和构建系统
理解项目整体布局，能编译和运行测试
- 阅读 README.md、CONTRIBUTING.md，了解项目背景
- 理解 Makefile 主要目标（all/test/verify/update）
- 了解 hack/ 目录：构建、测试、验证、代码生成脚本
- 理解 staging/ 机制和 go.work 工作空间
- 运行 make 编译项目，运行一组单元测试

###   Step 2: 阶段二：API 系统和核心概念
理解 Kubernetes API 对象模型
- 阅读 staging/src/k8s.io/api/core/v1/types.go，理解 Pod/Node/Service 等核心类型
- 阅读 staging/src/k8s.io/apimachinery/，理解运行时、编解码、Watch 机制
- 理解 pkg/features/ 的 Feature Gate 机制
- 阅读 pkg/api/ 的 API 注册逻辑

###   Step 3: 阶段三：API Server
理解集群入口组件
- 从 cmd/kube-apiserver/apiserver.go 入口跟踪到 app/server.go
- 理解 pkg/kubeapiserver/ 的存储工厂、Admission、认证授权
- 理解 Generic APIServer 框架（staging/src/k8s.io/apiserver/）

###   Step 4: 阶段四：Controller Manager
理解控制器模式，Kubernetes 核心设计哲学
- 阅读 cmd/kube-controller-manager/app/ 入口
- 理解 pkg/controller/ 的通用模式（工作队列、反压、分片）
- 深入 Deployment 控制器（pkg/controller/deployment/）
- 理解 Informer 机制（staging/src/k8s.io/client-go/tools/cache/）

###   Step 5: 阶段五：Scheduler
理解 Pod 调度机制
- 阅读 cmd/kube-scheduler/app/ 入口
- 理解 pkg/scheduler/scheduler.go 主循环
- 理解插件化调度框架（pkg/scheduler/framework/）：Filter → Score → Reserve → Bind

###   Step 6: 阶段六：Kubelet
理解节点容器管理
- 阅读 cmd/kubelet/app/server.go 入口
- 理解 pkg/kubelet/kubelet.go 主结构体
- 了解关键子系统：容器运行时、卷管理、健康检查、资源管理、驱逐

###   Step 7: 阶段七：kubectl 和客户端
理解 CLI 客户端
- 阅读 cmd/kubectl/kubectl.go 入口
- 理解 pkg/kubectl/ 的命令实现
- 了解 staging/src/k8s.io/cli-runtime/ CLI 运行时

###   Step 8: 阶段八：测试和贡献准备
了解测试体系和贡献流程
- 了解 test/ 目录结构（e2e、integration、node 测试）
- 阅读 CONTRIBUTING.md
- 了解代码生成流程（hack/update-codegen.sh、make update）
- 准备第一个贡献：选择一个 good-first-issue