# Kubernetes 仓库架构图

> 按本机仓库 `release-1.37-zh` 分支的实际目录结构核对，2026-11-25。
> 这张图是**源码仓库架构**：进入每个组件的命令、实现和公共模块边界；不是 Kubernetes 集群的网络拓扑图。
> 节点标签是目录或组件名，不是进程实例的部署位置。

## 图例

| 颜色 | 含义 |
| --- | --- |
| 蓝 | 命令入口（`cmd/`） |
| 橙 | 主仓库内部实现（`pkg/`） |
| 绿 | 公共可发布模块（`staging/src/k8s.io/`） |
| 黄 | 插件与扩展实现（`plugin/`、`pkg/scheduler/framework/plugins/`） |
| 紫 | 外部入口与工具（用户、etcd、容器运行时、构建工具） |

节点边框颜色即分类，所有节点都有文字标签；虚线只表示构建/测试工具不参与运行时请求链。

## 架构图（Mermaid）

```mermaid
---
config:
  theme: base
  themeVariables:
    primaryColor: "#ffffff"
    primaryTextColor: "#18181b"
    primaryBorderColor: "#71717a"
    lineColor: "#71717a"
    fontSize: 13px
---
flowchart LR
  subgraph CLIENT["客户端"]
    direction TB
    USER(["用户 / 自动化系统"])
    KUBECTL["kubectl<br/>cmd/kubectl"]
    ADMIN["kubeadm / kubemark<br/>kubectl-convert"]
  end

  subgraph CMD["cmd/ — 薄入口：flag 解析与装配"]
    direction TB
    APIE["kube-apiserver"]
    CM["kube-controller-manager"]
    SCHED["kube-scheduler"]
    KLET["kubelet"]
    PROXY["kube-proxy"]
  end

  subgraph PKG["pkg/ — 主仓库内部实现"]
    direction TB
    CP["pkg/controlplane<br/>pkg/kubeapiserver"]
    REG["pkg/registry<br/>REST storage"]
    PKGAPI["pkg/apis<br/>内部类型 / 转换 / 校验"]
    CTRL["pkg/controller<br/>reconcile 循环"]
    SCHEDCORE["pkg/scheduler<br/>调度队列 + Framework"]
    KLETCORE["pkg/kubelet<br/>Pod worker / 探针 / 卷"]
    PROXYCORE["pkg/proxy + pkg/volume<br/>网络规则 / 存储抽象"]
    AUTHZ["pkg/admission · pkg/auth"]
  end

  subgraph STAGING["staging/src/k8s.io/ — 公共模块（源码真相）"]
    direction TB
    APSERVER["k8s.io/apiserver<br/>通用 API Server 框架"]
    APIM["k8s.io/api + apimachinery<br/>外部类型 / Scheme / runtime"]
    CLIENTGO["k8s.io/client-go<br/>REST / Informer / WorkQueue"]
    CLI["k8s.io/cli-runtime + kubectl"]
    COMP["k8s.io/component-base"]
    CRI["k8s.io/cri-api · cri-client"]
    MODULES["kube-scheduler · kubelet · kube-proxy 模块"]
    GEN["k8s.io/code-generator"]
    EXTAPI["apiextensions-apiserver · kube-aggregator"]
  end

  subgraph PLUGINS["插件与扩展实现"]
    direction TB
    ADMPLUG["plugin/pkg/admission"]
    AUTHPLUG["plugin/pkg/auth"]
    SCHPLUG["pkg/scheduler/framework/plugins"]
  end

  subgraph EXTERNAL["外部系统"]
    direction TB
    ETCD[("etcd")]
    RUNTIME[("容器运行时<br/>containerd / CRI-O")]
  end

  TOOLS["构建与验证<br/>hack/ · build/ · test/ · api/"]

  USER --> KUBECTL
  USER --> ADMIN
  KUBECTL --> CLI
  CLI --> CLIENTGO
  USER --> APIE
  ADMIN --> APIE

  APIE --> CP
  CM --> CTRL
  SCHED --> SCHEDCORE
  KLET --> KLETCORE
  PROXY --> PROXYCORE
  CP --> APSERVER
  APSERVER --> REG
  REG --> PKGAPI
  REG --> ETCD
  CP --> AUTHZ

  ETCD -->|LIST / WATCH| CLIENTGO
  CLIENTGO --> CTRL
  CLIENTGO --> SCHEDCORE
  CTRL --> APIE
  SCHEDCORE --> APIE
  APIE --> KLETCORE
  KLETCORE --> CRI
  CRI --> RUNTIME

  APIM --> PKGAPI
  APIM --> APSERVER
  COMP --> APIE
  GEN --> APIM
  MODULES --> SCHEDCORE
  MODULES --> KLETCORE
  EXTAPI --> APSERVER
  AUTHZ --> ADMPLUG
  AUTHZ --> AUTHPLUG
  SCHPLUG --> SCHEDCORE

  TOOLS -.->|make update / verify| PKG
  TOOLS -.->|生成与依赖维护| STAGING

  classDef cmd fill:#eff6ff,stroke:#2a78d6,stroke-width:2px,color:#18181b;
  classDef pkg fill:#fff7ed,stroke:#eb6834,stroke-width:2px,color:#18181b;
  classDef staging fill:#f0fdf4,stroke:#1baf7a,stroke-width:2px,color:#18181b;
  classDef plugin fill:#fefce8,stroke:#eda100,stroke-width:2px,color:#18181b;
  classDef external fill:#faf5ff,stroke:#6250d6,stroke-width:2px,color:#18181b;

  class KUBECTL,APIE,CM,SCHED,KLET,PROXY,ADMIN cmd;
  class CP,REG,PKGAPI,CTRL,SCHEDCORE,KLETCORE,PROXYCORE,AUTHZ pkg;
  class APSERVER,APIM,CLIENTGO,CLI,COMP,CRI,MODULES,GEN,EXTAPI staging;
  class ADMPLUG,AUTHPLUG,SCHPLUG plugin;
  class USER,ETCD,RUNTIME,TOOLS external;
```

## 怎么读这张图

1. **入口层很薄。** `cmd/<组件>/main.go` 只负责启动和装配；`NewXxxCommand()` 之后的实现在 `pkg/` 或 `staging/`。
2. **控制面围绕 API Server。** Controller Manager 与 Scheduler 主要通过 LIST/WATCH 获得变化、通过 REST 写回资源，不互相直接调用函数。
3. **节点侧走 CRI。** Kubelet 从 API Server 读 Pod 期望状态，通过 CRI 驱动容器运行时；kube-proxy 负责 Service 转发规则。
4. **staging 是 `k8s.io/*` 的源码真相。** 修改客户端或 apiserver 公共框架，要改 `staging/src/k8s.io/`，不是 `vendor/`；staging 不得反向 import `k8s.io/kubernetes`。
5. **plugin/ 不等于所有插件。** 调度插件在 `pkg/scheduler/framework/plugins/`；client-go 认证插件在 `staging/src/k8s.io/client-go/plugin/pkg/client/auth/`。

## 一句话主线

`kubectl → client-go REST → kube-apiserver（apiserver 框架 + registry + etcd）→ informer/watch → scheduler/controller → API Server → kubelet → CRI → 容器运行时`

细节与函数级入口见 `09-repository-directory-map.md` §4、`01-code-map.md` 与 `07-mental-model.md`。
