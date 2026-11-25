# Kubernetes 仓库目录地图（第 01 课）

> 本笔记按本机仓库实际结构核对。记录时间：2026-11-24。
>
> 仓库：`/Users/kannakamui/dev/projects/GolandProjects/kubernetes`
> 分支：`release-1.37-zh`
> HEAD：`cff6d0d8806`（`v1.37.2-125-gcff6d0d8806-dirty`）
> `go.mod` 声明：`go 1.26.0`。
>
> 注意：当前工作区已有大量未提交修改（检查时 211 个路径处于修改状态）。本课只新增本笔记，不清理、不覆盖、不提交其他修改。

## 1. 一句话理解仓库

Kubernetes 主仓库不是单一服务，而是由多个可执行组件、核心实现、可发布公共模块、扩展插件，以及构建/测试/发布工具共同组成的多模块 Go 工程。

先记住四条边界：

1. `cmd/`：程序从哪里启动。
2. `pkg/`：Kubernetes 主仓库内部的核心实现。
3. `staging/src/k8s.io/`：以 `k8s.io/*` 模块形式维护和发布的公共组件源码，staging 是源码真相。
4. `plugin/`：部分可插拔扩展实现；不要把它和 Scheduler Framework Plugins 混为一谈。

## 2. 仓库根目录地图

| 目录/文件 | 职责 | 阅读时关注 |
| --- | --- | --- |
| `cmd/` | 组件二进制入口、代码生成和仓库检查工具 | `main.go`、`app/`、命令装配 |
| `pkg/` | Kubernetes 主仓库内部实现 | API Server、控制器、调度器、Kubelet、存储、代理 |
| `staging/` | 待发布到独立 `k8s.io/*` 仓库的模块源码 | `staging/README.md`、模块边界 |
| `plugin/` | Admission、Auth 等插件实现 | 插件注册、扩展点、启用方式 |
| `api/` | API 相关规范与发现文档资源 | OpenAPI spec、API rules、discovery |
| `build/` | 构建规则、构建镜像、组件镜像配置 | `build/root/Makefile`、镜像目录 |
| `cluster/` | 集群部署脚本、插件和环境配置 | GCE、Kubemark、addons |
| `hack/` | 开发自动化脚本、代码生成、验证、依赖维护 | `hack/make-rules/`、`hack/update-*.sh` |
| `test/` | 单元之外的集成、端到端、节点和性能测试资源 | `test/integration/`、`test/e2e_*` |
| `third_party/` | Kubernetes 维护的第三方分支、工具或平台资源 | forked 依赖与构建工具 |
| `vendor/` | Go vendor 模式下的第三方依赖副本 | 依赖来源；不是 staging 源码真相 |
| `docs/` | 仓库内文档资源 | 按具体文件用途阅读 |
| `CHANGELOG/` | 版本变更记录 | 发布行为与兼容性变化 |
| `logo/` | 项目标识资源 | 非运行时逻辑 |
| `study/` | 本仓库源码学习笔记 | 本学习计划与阶段产物 |
| `k8s-study/` | 既有 Kubernetes 学习资料 | 作为补充资料，不与源码混淆 |
| `.github/`、`OWNERS* ` | GitHub 协作配置、代码所有权信息 | PR 模板、审批责任人 |
| `go.mod`、`go.work` | Go 模块依赖与工作区配置（生成文件） | staging 模块如何参与本地构建 |
| `Makefile` | 根构建入口，实际链接到 `build/root/Makefile` | make target 的定义 |

## 3. 四个核心目录的细分

### 3.1 cmd：从进程入口开始

`cmd/` 不全是业务代码。它同时包含 Kubernetes 组件入口和生成/检查工具。

主要组件：

| 命令目录 | 组件职责 | 下一站 |
| --- | --- | --- |
| `cmd/kube-apiserver/` | 集群 API 入口，接收并处理资源请求 | `cmd/kube-apiserver/app/`、`pkg/controlplane/`、`pkg/registry/` |
| `cmd/kube-controller-manager/` | 装配并运行多个控制器 | `cmd/kube-controller-manager/app/`、`pkg/controller/` |
| `cmd/kube-scheduler/` | 为未调度 Pod 选择 Node | `cmd/kube-scheduler/app/`、`pkg/scheduler/` |
| `cmd/kubelet/` | 管理单个 Node 上的 Pod 和容器生命周期 | `cmd/kubelet/app/`、`pkg/kubelet/` |
| `cmd/kube-proxy/` | 维护 Service 流量转发规则 | `cmd/kube-proxy/app/`、`pkg/proxy/` |
| `cmd/kubectl/` | 命令行客户端入口 | `staging/src/k8s.io/kubectl/pkg/cmd/` |
| `cmd/kubeadm/` | 集群引导和生命周期管理工具 | `cmd/kubeadm/app/` |

其他如 `genman`、`genkubedocs`、`genfeaturegates`、`import-boss`、`dependencycheck` 等，属于生成或仓库校验工具。

入口阅读规律：

```
cmd/<组件>/main 文件
  -> app.New...Command() 构造命令
  -> 参数解析、配置和组件装配
  -> pkg/ 中的核心实现
```

实际例子：

- `cmd/kube-scheduler/scheduler.go:25`：main 调用 `app.NewSchedulerCommand()`。
- `cmd/kube-apiserver/apiserver.go:33`：main 调用 `app.NewAPIServerCommand()`。
- `cmd/kubelet/kubelet.go:35`：main 调用 `app.NewKubeletCommand()`。
- `cmd/kubectl/kubectl.go:34`：构造默认 kubectl 命令。

设计原因：入口保持薄，启动参数和依赖装配集中在 app 层，业务机制放在可测试、可复用的包中。因此读源码时，不要停留在 main 文件。

### 3.2 pkg：主仓库核心实现

重点子目录：

| 目录 | 主要职责 |
| --- | --- |
| `pkg/controlplane/` | 控制平面实例和 API Server 装配 |
| `pkg/kubeapiserver/` | Kubernetes API Server 特定配置和扩展 |
| `pkg/registry/` | 各 API 资源的 REST 存储实现 |
| `pkg/apis/` | Kubernetes 内部 API 类型、版本转换、默认值和校验 |
| `pkg/controller/` | 各种控制器及其协调循环 |
| `pkg/scheduler/` | Scheduler 核心、调度队列、框架和内置调度插件 |
| `pkg/kubelet/` | 节点代理、Pod worker、探针、卷管理等 |
| `pkg/proxy/` | kube-proxy 的网络规则与代理实现 |
| `pkg/volume/` | 卷插件与存储操作抽象 |
| `pkg/admission/` | Kubernetes 主仓库侧的准入配置/集成 |
| `pkg/auth/` | 认证、授权相关的 Kubernetes 集成 |
| `pkg/util/` | 内部通用工具 |

先用一组“入口—实现”配对记忆：

- Scheduler：`cmd/kube-scheduler/` → `pkg/scheduler/`
- Controller Manager：`cmd/kube-controller-manager/` → `pkg/controller/`
- Kubelet：`cmd/kubelet/` → `pkg/kubelet/`
- Kube-proxy：`cmd/kube-proxy/` → `pkg/proxy/`
- API Server：`cmd/kube-apiserver/` → `pkg/controlplane/` + `pkg/registry/` + staging 的 apiserver 通用框架

### 3.3 staging：公共模块的源码真相

权威说明：`staging/README.md`。

主要模块（按职责分组，不代表完整列表）：

| 模块 | 主要职责 |
| --- | --- |
| `staging/src/k8s.io/api` | 对外发布的 Kubernetes API 类型，例如 core/v1 Pod |
| `staging/src/k8s.io/apimachinery` | API 机器基础设施：runtime、Scheme、对象元数据、watch 等 |
| `staging/src/k8s.io/apiserver` | 通用 API Server 框架 |
| `staging/src/k8s.io/client-go` | Go 客户端、REST、Informer、Lister、WorkQueue |
| `staging/src/k8s.io/kubectl` | kubectl 命令实现 |
| `staging/src/k8s.io/cli-runtime` | kubectl 等 CLI 使用的资源构建与执行基础设施 |
| `staging/src/k8s.io/kube-scheduler` | Scheduler 的可发布模块边界/配置 API 等 |
| `staging/src/k8s.io/kubelet` | Kubelet 相关可发布 API/模块 |
| `staging/src/k8s.io/cri-api`、`cri-client` | 容器运行时接口定义与客户端 |
| `staging/src/k8s.io/component-base` | 组件通用基础能力，如 CLI、日志、指标 |
| `staging/src/k8s.io/code-generator` | Kubernetes API/client 代码生成工具 |
| `staging/src/k8s.io/apiextensions-apiserver` | CRD API 扩展服务器 |

为什么要 staging：

- 公共组件可以独立发布到各自的 `k8s.io/*` 仓库。
- Kubernetes 主仓库开发时仍能在同一个工作区内联调这些模块。
- 模块边界明确，减少公共库对整个 `k8s.io/kubernetes` 主仓库的反向依赖。

本地解析机制：

- `go.work` 的 `use` 将 staging 下各模块加入 workspace。
- 根 `go.mod` 的 `replace` 将 `k8s.io/client-go` 等模块映射到本地 staging 路径。
- 所以修改 `k8s.io/client-go` 应改 `staging/src/k8s.io/client-go`，不要改 `vendor/`。

禁止方向：staging 模块不能反向 import `k8s.io/kubernetes`。参见根目录 `AGENTS.md`。

### 3.4 plugin：扩展点实现，不是所有插件的总目录

当前顶层 `plugin/` 的实际结构主要是：

- `plugin/pkg/admission/`：准入插件实现，例如 AlwaysPullImages、NodeRestriction、ResourceQuota 等。
- `plugin/pkg/auth/`：认证/授权相关插件实现。

特别注意：

- `plugin/` 顶层目录不等于 Kubernetes 所有插件。
- Scheduler Framework 的内置调度插件主要在 `pkg/scheduler/framework/plugins/`。
- client-go 的认证插件入口在 `staging/src/k8s.io/client-go/plugin/pkg/client/auth/`。
- CSI 插件、CNI 插件也不是简单地都放在顶层 `plugin/`。

## 4. 一张调用关系总图

```
用户 / 自动化系统
      |
      v
kubectl（cmd/kubectl）
      |
      | client-go REST 请求
      v
kube-apiserver（cmd/kube-apiserver）
      |
      +--> 通用 API Server 框架（staging/src/k8s.io/apiserver）
      |
      +--> API 类型与运行时（staging/src/k8s.io/api、apimachinery）
      |
      +--> 资源策略、校验、REST storage（pkg/apis、pkg/registry）
      |
      v
     etcd
      |
      | LIST/WATCH
      +-----------------------------+
      |                             |
      v                             v
Controller Manager              Scheduler
cmd/...                         cmd/...
pkg/controller                  pkg/scheduler
      |                             |
      | 创建/更新 Pod                | 选择 Node 并绑定
      +--------------+--------------+
                     |
                     v
              kube-apiserver
                     |
                     | Pod 期望状态
                     v
                  Kubelet
               cmd/kubelet
               pkg/kubelet
                     |
                     | CRI
                     v
          容器运行时（containerd 等）
```

这张图是概念主线，不表示组件之间所有通信都只有这一种方式。多数控制面组件通过 API Server 读写资源；Kubelet 还会通过 CRI 与容器运行时通信。

## 5. 本课必须认识的数据结构

本课只建立定位，不要求一次掌握所有字段：

| 数据结构/抽象 | 先回答的问题 | 重点源码位置 |
| --- | --- | --- |
| `Pod` / `PodSpec` / `PodStatus` | 用户期望什么，系统观察到什么？ | `staging/src/k8s.io/api/core/v1/types.go` |
| `ObjectMeta` | 名称、命名空间、标签、注解、UID 等元数据放哪里？ | `staging/src/k8s.io/apimachinery/pkg/apis/meta/v1/types.go` |
| GVK / GVR | 对象类型和 REST 资源路径如何表达？ | `staging/src/k8s.io/apimachinery/pkg/runtime/schema/` |
| `runtime.Scheme` | 如何把 GVK 与 Go 类型关联、并支持转换？ | `staging/src/k8s.io/apimachinery/pkg/runtime/scheme.go` |
| `RESTMapper` | 客户端如何把 GVK 映射为资源名和 scope？ | `staging/src/k8s.io/apimachinery/pkg/api/meta/` |
| `RESTStorage` / `Store` | API 请求最终由谁实现 Create/Update/Delete？ | `staging/src/k8s.io/apiserver/pkg/registry/`、`pkg/registry/` |
| Informer / Lister / WorkQueue | 控制器如何低成本感知变化并异步协调？ | `staging/src/k8s.io/client-go/tools/cache/`、`staging/src/k8s.io/client-go/util/workqueue/` |
| Scheduler Framework | 调度插件如何在固定扩展点参与调度？ | `pkg/scheduler/framework/` |

## 6. 本课源码阅读路线（60 分钟）

| 时间 | 任务 | 完成产物 |
| --- | --- | --- |
| 0–10 分钟 | README、CONTRIBUTING、AGENTS | 说清项目用途和贡献约束 |
| 10–20 分钟 | 根目录 + cmd | 找到 6 个核心组件入口 |
| 20–35 分钟 | pkg + plugin | 能把组件入口映射到核心实现 |
| 35–45 分钟 | staging + go.work + go.mod | 解释 staging 为什么是源码真相 |
| 45–55 分钟 | 调用关系图 | 口述一次 Pod 从 API 请求到 Node 执行的主线 |
| 55–60 分钟 | 回答思考题 | 暴露理解缺口，安排下一课 |

## 7. 自测：先用自己的话回答

不要复制笔记，尽量用自己的话回答：

1. 为什么从 `cmd/kube-scheduler` 只能看到很薄的 main，而真正的调度逻辑在 `pkg/scheduler`？
2. 如果要修改 `k8s.io/client-go/tools/cache` 的行为，应该改哪个目录？为什么不是 `vendor/`？
3. `pkg/apis/core/types.go` 与 `staging/src/k8s.io/api/core/v1/types.go` 中的 Pod 类型为什么可能同时存在？它们分别面向什么边界？
4. 顶层 `plugin/` 与 `pkg/scheduler/framework/plugins/` 有什么区别？
5. Controller Manager、Scheduler、Kubelet 分别负责 Pod 生命周期中的哪一段？它们为什么不直接互相调用函数完成所有事情？

### 验收标准

- 能在仓库里快速定位 cmd、pkg、staging、plugin。
- 能口述 `cmd -> app -> pkg` 的启动与装配关系。
- 能解释 staging、go.work、go.mod replace 三者的关系。
- 能画出 API Server、etcd、Controller、Scheduler、Kubelet 的主线。
- 每个答案至少能给出一个具体源码路径作为证据。

## 8. 阅读时的安全边界

- 本课不修改 Kubernetes 业务源码。
- 不编辑生成文件、`go.mod`、`go.work`。
- 不清理或覆盖现有工作区修改。
- 当前仓库已有大量未提交修改；任何后续代码改动前必须先看 `git status --short` 和目标文件 diff。
