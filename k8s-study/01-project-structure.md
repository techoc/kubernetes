# 第1阶段：项目结构和开发环境

**目标**：理解项目整体布局，能够编译和运行本地集群。

**前置要求**：
- Go 1.23+
- 熟悉 Go 基础（包、模块、goroutine）

---

## 1. 你需要做什么

### 1.1 理解目录结构

Kubernetes 仓库非常大（20万+行 Go 代码），第一步是建立全局观。

**需要做的事**：

1. **浏览 `cmd/` 目录** — 每个子目录是一个可独立编译的二进制组件：
   ```
   cmd/kube-apiserver/          # API 服务器
   cmd/kube-controller-manager/ # 控制器管理器
   cmd/kube-scheduler/          # 调度器
   cmd/kubelet/                 # 节点代理
   cmd/kube-proxy/              # 网络代理
   cmd/kubectl/                 # CLI 工具
   ```
   每个组件的 `app/` 子目录包含 `Run*()` 函数 — 这是每个组件的启动入口。

2. **浏览 `pkg/` 目录** — 核心实现包，按功能模块组织：
   - `pkg/controller/` — 所有控制器实现
   - `pkg/scheduler/` — 调度器实现
   - `pkg/kubelet/` — Kubelet 实现
   - `pkg/proxy/` — kube-proxy 实现
   - `pkg/registry/` — 各资源的 REST 存储实现
   - `pkg/authentication/` 和 `pkg/authorization/` — 认证和授权

3. **理解 `staging/` 的特殊地位** — `staging/src/k8s.io/` 下的目录最终会发布为独立模块：
   - `client-go` — 客户端库
   - `api` — 内置 API 类型
   - `apimachinery` — API 运行时基础
   - `apiserver` — 通用 API Server 框架
   - `apiextensions-apiserver` — CRD 支持
   
   **关键规则**：staging 是 `k8s.io/*` 的源码真相来源。修改这些模块的代码在 staging 目录下进行，而不是 vendor 里。

4. **了解辅助目录**：
   - `api/` — API 类型定义的入口（symlink 到 staging）
   - `test/` — 测试基础设施（e2e、integration、lib）
   - `hack/` — 开发脚本（构建、验证、代码生成）
   - `vendor/` — vendored 的第三方依赖
   - `plugin/` — 遗留插件

**建议**：用 `tree -L 2 cmd/ pkg/ staging/src/k8s.io/` 快速浏览顶层结构。

---

### 1.2 理解构建系统

Kubernetes 使用 `make` 作为构建入口，底层是 `hack/make-rules/` 下的 shell 脚本。

**需要做的事**：

1. **阅读 `Makefile` 前 100 行** — 了解主要 target：
   - `all` / 默认 target — 构建所有平台的所有二进制
   - `cross` — 交叉编译多平台
   - `test` — 运行单元测试
   - `verify` — 运行所有验证检查
   - `update` — 运行所有代码生成器和格式化器

2. **理解 `make` 的核心参数**：
   ```bash
   make                              # 构建所有
   make quick-release               # 快速构建当前平台
   make WHAT=./pkg/kubelet          # 只构建指定包
   make test WHAT=./pkg/controller/replicaset  # 运行指定包的测试
   make verify                      # 所有验证（gofmt、go vet、license header 等）
   make update                      # 所有生成器和格式化器
   ```

3. **阅读 `hack/make-rules/build.sh`** — 理解二进制是如何编译的：
   - Go build 标志（LDFLAGS、GOFLAGS）
   - 输出目录 `_output/local/bin/`
   - 交叉编译逻辑

4. **理解 `go.work` 的作用** — Go workspace 文件通过 `replace` 指令将 `k8s.io/*` 模块指向本地 staging 目录，这样修改 staging 代码后不需要重新 vendor。

**建议**：在 Linux 环境下执行 `make` 编译完整项目，观察构建过程和输出。

---

### 1.3 理解测试体系

Kubernetes 有三层测试：

**需要做的事**：

1. **单元测试** — 最快的测试，针对单个包：
   ```bash
   make test WHAT=./pkg/controller/replicaset GOFLAGS=-v
   ```
   了解测试文件命名（`*_test.go`）和常用测试模式（Table-driven tests）。

2. **集成测试** — 需要 etcd，测试组件间的交互：
   ```bash
   make test-integration WHAT=./test/integration/scheduler
   ```

3. **E2E 测试** — 需要完整集群，测试端到端行为：
   ```bash
   # 在已部署的集群上运行
   go test ./test/e2e -args --ginkgo.focus="Pod should start and stop"
   ```

4. **验证检查** — `make verify` 包含：
   - `gofmt` / `goimports` 格式检查
   - `go vet` 静态分析
   - License header 检查
   - 生成代码是否最新
   - 依赖版本一致性

**建议**：运行 `make verify` 一次，了解所有检查项。

---

### 1.4 理解 staging 模块系统

**需要做的事**：

1. **阅读 `hack/update-vendor.sh`** — 理解依赖如何被 vendor：
   - 使用 `go mod vendor`
   - staging 模块通过 `replace` 指向本地

2. **理解依赖管理命令**：
   ```bash
   # 添加/更新依赖 — 不要用 go mod tidy
   ./hack/pin-dependency.sh github.com/some/package v1.2.3
   ./hack/update-vendor.sh
   ```

3. **理解代码生成流程**：
   ```bash
   ./hack/update-codegen.sh  # 重新生成 deepcopy、clientset、informer、lister
   make update               # 所有生成器和格式化器
   ```

4. **记住关键约束**：
   - `zz_generated.*` 和 `generated.pb.go` 是只读的，不要手改
   - `go.mod` / `go.sum` / `go.work` 是生成的，不要手改
   - 每个 `.go` 文件需要 license header（`hack/boilerplate/boilerplate.go.txt`）
   - staging 目录永远不要导入 `k8s.io/kubernetes`

---

### 1.5 运行本地集群

**需要做的事**：

1. **阅读 `hack/local-up-cluster.sh`** — 理解它做了什么：
   - 启动 etcd
   - 启动 kube-apiserver
   - 启动 kube-controller-manager
   - 启动 kube-scheduler
   - 配置 kubelet
   - 设置 kubectl kubeconfig

2. **了解环境变量**：
   ```bash
   CLUSTER_CIDR=10.244.0.0/16          # Pod 网络
   SERVICE_CLUSTER_IP_RANGE=10.96.0.0/12  # Service 网络
   FEATURE_GATES="Feature1=true,Feature2=false"  # 特性开关
   ```

3. **尝试启动**（需要 Linux + root）：
   ```bash
   # 先打印命令，不执行
   sudo CLUSTER_CIDR=10.244.0.0/16 ./hack/local-up-cluster.sh -d
   
   # 实际启动
   sudo CLUSTER_CIDR=10.244.0.0/16 ./hack/local-up-cluster.sh
   ```

4. **验证集群**：
   ```bash
   kubectl get nodes
   kubectl run nginx --image=nginx
   kubectl get pods -o wide
   kubectl logs nginx
   ```

**注意**：macOS 上无法直接运行 `local-up-cluster.sh`，建议使用 `kind` 或 `minikube` 作为替代。

---

## 2. 关键文件清单

| 文件 | 说明 | 阅读重点 |
|------|------|----------|
| `Makefile` | 构建入口 | 前 100 行的 target 定义 |
| `go.work` | Go workspace | `replace` 指令如何指向 staging |
| `hack/make-rules/build.sh` | 构建规则 | Go build 标志、输出目录 |
| `hack/local-up-cluster.sh` | 本地集群 | 组件启动顺序、端口配置 |
| `hack/update-codegen.sh` | 代码生成 | 调用了哪些 generator |
| `hack/update-vendor.sh` | 依赖管理 | vendor 流程 |
| `hack/boilerplate/boilerplate.go.txt` | License header | 每个 .go 文件需要的头 |
| `CONTRIBUTING.md` | 贡献指南 | 提交流程、代码规范 |
| `cmd/kube-apiserver/app/server.go` | API Server 入口 | `NewAPIServerCommand()` 第74行 → `Run()` 第152行 |
| `cmd/kube-apiserver/app/options/options.go` | API Server 选项 | `NewServerRunOptions()` 第73行 |
| `cmd/kube-apiserver/app/config.go` | API Server 配置 | `NewConfig()` 第74行 |
| `cmd/kube-controller-manager/app/controllermanager.go` | CM 入口 | `NewControllerManagerCommand()` 第115行 → `Run()` 第591行 |
| `cmd/kube-scheduler/app/server.go` | Scheduler 入口 | `NewSchedulerCommand()` → `Run()` |
| `cmd/kubelet/app/server.go` | Kubelet 入口 | `NewOptions()` → `Run()` |
| `cmd/kube-proxy/app/server.go` | kube-proxy 入口 | `NewOptions()` → `Run()` |

---

## 3. 实践任务清单

完成以下任务来验证你的理解：

- [ ] **任务 1**：浏览 `cmd/` 下所有组件的 `main.go`，找到每个组件的 `Run*()` 入口函数
  - 实验：查看 `cmd/kube-apiserver/app/server.go:74` 的 `NewAPIServerCommand()` 函数，追踪调用链到 `Run()` 函数（第152行）
  - 实验：查看 `cmd/kube-controller-manager/app/controllermanager.go:115` 的 `NewControllerManagerCommand()`，追踪到 `Run()`（第591行）
- [ ] **任务 2**：阅读 `Makefile` 前 100 行，列出至少 5 个常用 target 及其作用
- [ ] **任务 3**：执行 `make verify`，记录所有检查项和结果
- [ ] **任务 4**：在 Linux 环境（或 Docker 容器）中执行 `make` 编译项目，观察输出
- [ ] **任务 5**：启动本地集群（或 kind/minikube），创建 Pod 并观察其生命周期
  - 实验：用 `kind` 创建集群并验证
    ```bash
    kind create cluster --name k8s-study
    kubectl cluster-info --context kind-k8s-study
    kubectl run nginx --image=nginx --port=80
    kubectl get pods -o wide -w  # 观察 Pod 从 Pending 到 Running 的状态变化
    ```
- [ ] **任务 6**：阅读 `go.work`，理解 `replace` 指令如何将 `k8s.io/client-go` 指向 `staging/src/k8s.io/client-go`
  - 实验：查看 `go.work` 中的 `replace` 指令，确认 staging 模块映射
    ```bash
    grep "replace" go.work | head -20
    ```

---

## 4. 检查点

进入第2阶段前，确保你能回答：

1. `cmd/`、`pkg/`、`staging/` 三个目录各放什么代码？
2. 为什么 `staging/` 是源码真相来源？直接改 `vendor/` 里的代码为什么不行？
3. `make test`、`make verify`、`make update` 分别做什么？
4. 如何添加一个新依赖？为什么不能用 `go mod tidy`？
5. 本地集群启动时，各组件的启动顺序是什么？它们之间如何通信？

---

## 5. 下一步

完成本阶段后，进入 [第2阶段：API 机制和 client-go](./02-api-mechanism.md)。

API 机制是理解所有后续组件的基础 — 控制器、调度器、Kubelet 都依赖 client-go 的 Informer 和 workqueue 模式。
