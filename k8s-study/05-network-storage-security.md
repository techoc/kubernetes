# 第5阶段：网络、存储、安全

**目标**：理解 Kubernetes 的网络模型、存储架构和安全机制。

**前置要求**：完成第4阶段，理解 Kubelet 和 kube-proxy。

---

## 1. 你需要做什么

### 1.1 网络

Kubernetes 网络模型的核心原则：每个 Pod 有独立的 IP，Pod 之间可以直接通信。

**需要做的事**：

#### 1.1.1 理解 Service 和 Endpoint

1. **理解 Service 模型**：
   - Service 提供稳定的网络入口
   - 通过 label selector 关联 Pod
   - Endpoint/EndpointSlice 记录实际可达的 Pod IP

2. **阅读 `pkg/controller/endpoints/`** — Endpoints 控制器：
   - 监听 Pod 和 Service 变化
   - 计算 Service 的端点集合
   - 更新 Endpoint 对象

3. **阅读 `pkg/controller/endpoint_slice/`** — EndpointSlice 控制器：
   - `pkg/controller/endpointslice/endpointslice_controller.go:83` — `NewController()` 创建控制器
   - EndpointSlice 是 Endpoint 的替代方案
   - 按数量分片，支持大规模集群
   - 理解 `MaxNumPerSlice`

4. **理解 Service 类型**：
   - `ClusterIP` — 集群内部虚拟 IP
   - `NodePort` — 在每个节点上暴露端口
   - `LoadBalancer` — 外部负载均衡器
   - `ExternalName` — CNAME 别名

5. **理解流量路径**：
   ```
   Client → Service ClusterIP → kube-proxy 规则 → Endpoint Pod IP → Pod
   ```

#### 1.1.2 理解 CNI 集成

1. **理解 CNI 接口**：
   - CNI（Container Network Interface）— Pod 网络插件标准
   - 接口：`AddNetwork()`、`DelNetwork()`、`CheckNetwork()`
   - 主流插件：Calico、Cilium、Flannel、Weave

2. **阅读 `pkg/kubelet/network/`**：
   - Kubelet 如何调用 CNI
   - `CNIConfig` — CNI 配置
   - Pod 创建时调用 `AddNetwork()`
   - Pod 删除时调用 `DelNetwork()`

3. **理解 Pod 网络模型**：
   - 每个 Pod 一个网络命名空间
   - Pod 内容器共享网络命名空间
   - CNI 负责分配 IP 和配置网络

#### 1.1.3 理解 NetworkPolicy

1. **理解 NetworkPolicy 模型**：
   - 定义 Pod 之间的流量规则
   - 入站（Ingress）和出站（Egress）规则
   - 通过 label selector 选择 Pod

2. **理解实现**：
   - NetworkPolicy 是 API 对象
   - 由 CNI 插件负责执行
   - Kubernetes 核心不执行策略，只存储和分发

3. **阅读 `pkg/apis/networking/`**：
   - NetworkPolicy API 类型
   - 验证逻辑

#### 1.1.4 理解 kube-proxy（深入）

第4阶段已介绍 kube-proxy，这里深入理解：

1. **阅读 `pkg/proxy/`** — 代理核心：
   - `Proxier` 接口
   - `ServicePort` / `EndpointSlice` 模型
   - 会话亲和性（Session Affinity）

2. **理解 Service 负载均衡**：
   - 随机选择端点
   - 会话亲和性：基于源 IP 或 Cookie
   - 端点切片的高效遍历

---

### 1.2 存储

Kubernetes 存储架构支持多种卷类型和外部存储驱动。

**需要做的事**：

#### 1.2.1 理解卷插件框架

1. **阅读 `pkg/volume/`** — 卷插件框架：
   - `VolumePlugin` 接口 — 通用卷插件抽象
   - `ExpandableVolumePlugin` — 支持扩展
   - `DeprecatedProxyVolumePlugin` — 代理卷插件

2. **理解 In-tree 卷插件**：
   - `pkg/volume/` 下的内置插件
   - `hostPath`、`emptyDir`、`configMap`、`secret`、`nfs` 等
   - 迁移方向：逐步替换为 CSI

3. **理解 CSI（Container Storage Interface）**：
   - 外部存储驱动的标准接口
   - 替代 in-tree 插件的方向
   - 阅读 `staging/src/k8s.io/csi-translation-lib/` — CSI 迁移库

#### 1.2.2 理解 Attach/Detach 控制器

1. **阅读 `pkg/controller/volume/attach_detach/`**：
   - `attachDetachController` — 卷 attach/detach 控制器
   - 对比期望状态和实际状态
   - 异步执行 attach/detach 操作

2. **理解工作流程**：
   ```
   1. 控制器检测到 Pod 绑定到节点
   2. 检查卷是否需要 attach
   3. 调用云 provider 或 CSI 驱动 attach 卷
   4. 更新 VolumeAttachment 对象
   5. Kubelet 检测到卷已 attach
   6. Kubelet 挂载卷到 Pod
   ```

3. **理解 `VolumeAttachment`**：
   - API 对象，记录卷的 attach 状态
   - CSI 驱动的 attach 状态协调
   - 防止多节点同时 attach

#### 1.2.3 理解节点卷管理器

1. **阅读 `pkg/kubelet/volumemanager/`**：
   - `volumeManager` — 节点卷管理器
   - `desiredStateOfWorld` — 期望状态
   - `actualStateOfWorld` — 实际状态
   - `operationExecutor` — 执行卷操作

2. **理解 reconcile 流程**：
   ```
   1. 更新 desiredStateOfWorld（基于 Pod 需求）
   2. 更新 actualStateOfWorld（基于实际状态）
   3. 对比两者，计算差异
   4. 执行 SetUp/TearDown 操作
   ```

3. **理解卷操作**：
   - `SetUp()` — 挂载卷到节点
   - `Stage()` — 准备卷（CSI）
   - `Publish()` — 发布卷到 Pod（CSI）
   - `Unstage()` / `Unpublish()` — 反向操作

#### 1.2.4 理解 CSI 架构

1. **理解 CSI 组件**：
   - CSI 驱动 — 外部进程，实现 CSI 接口
   - `external-attacher` — 处理 attach/detach
   - `external-provisioner` — 处理动态供应
   - `external-resizer` — 处理卷扩展
   - `external-snapshotter` — 处理快照

2. **理解 CSI 接口**：
   - `ControllerPublishVolume()` / `ControllerUnpublishVolume()` — 控制器级操作
   - `NodeStageVolume()` / `NodePublishVolume()` — 节点级操作
   - `CreateVolume()` / `DeleteVolume()` — 动态供应

3. **阅读 `staging/src/k8s.io/csi-translation-lib/`**：
   - In-tree 到 CSI 的迁移
   - 兼容层

#### 1.2.5 理解 PersistentVolume 和 PersistentVolumeClaim

1. **理解 PV/PVC 模型**：
   - `PersistentVolume` — 集群中的存储资源
   - `PersistentVolumeClaim` — Pod 对存储的请求
   - `StorageClass` — 动态供应的配置

2. **理解绑定流程**：
   ```
   1. 用户创建 PVC
   2. 控制器找到匹配的 PV（或动态供应）
   3. PV 和 PVC 绑定
   4. Pod 引用 PVC
   5. Kubelet 挂载卷
   ```

3. **阅读 `pkg/controller/volume/persistentvolume/`**：
   - `persistentvolumecontroller` — PV/PVC 控制器
   - 绑定逻辑
   - 回收策略

---

### 1.3 安全

Kubernetes 安全包括认证、授权、准入控制和 Pod 安全。

**需要做的事**：

#### 1.3.1 理解认证（Authentication）

1. **阅读 `pkg/authentication/` 和 `staging/src/k8s.io/apiserver/pkg/authentication/`**：
   - `Authenticator` 接口
   - `User` 结构 — 用户身份

2. **理解认证方式**：
   - **Client certificate** — 客户端证书
     - `pkg/authentication/request/x509/`
     - 基于 TLS 证书
   - **Bearer token** — 令牌认证
     - `pkg/authentication/token/`
     - ServiceAccount token、静态 token、projected token
   - **OIDC** — OpenID Connect
     - 第三方身份提供商
   - **Webhook** — 外部认证服务
     - `pkg/authentication/token/webhook/`
     - 调用外部 API 验证令牌

3. **理解 ServiceAccount**：
   - Pod 的 API 身份
   - 自动挂载 token 到 Pod
   - `BoundServiceAccountTokenVolume` — 投影卷

#### 1.3.2 理解授权（Authorization）

1. **阅读 `pkg/authorization/` 和 `staging/src/k8s.io/apiserver/pkg/authorization/`**：
   - `Authorizer` 接口
   - `Attributes` — 请求属性

2. **理解 RBAC（基于角色的访问控制）**：
   - 阅读 `pkg/authorization/rbac/` 和 `pkg/apis/rbac/`
   - `Role` / `ClusterRole` — 权限规则
   - `RoleBinding` / `ClusterRoleBinding` — 绑定用户到角色
   - `Subject` — 用户、组、ServiceAccount

3. **理解 RBAC 规则**：
   ```yaml
   apiVersion: rbac.authorization.k8s.io/v1
   kind: Role
   metadata:
     namespace: default
     name: pod-reader
   rules:
   - apiGroups: [""]
     resources: ["pods"]
     verbs: ["get", "list", "watch"]
   ```
   - `apiGroups` — API 组
   - `resources` — 资源类型
   - `verbs` — 允许的操作

4. **理解 bootstrap 角色**：
   - 阅读 `pkg/apis/rbac/bootstrap.go`
   - 内置角色和绑定
   - `cluster-admin`、`admin`、`edit`、`view`

5. **理解授权模式组合**：
   - `UnionAuthorizer` — 任一授权器允许即可
   - `RiskierAuthorizer` — 按顺序检查，第一个非拒绝的结果
   - `DenyAuthorizer` — 拒绝所有

#### 1.3.3 理解准入控制（Admission）

1. **阅读 `staging/src/k8s.io/apiserver/pkg/admission/`**：
   - `AdmissionInterface` 接口
   - `AdmissionChain` — 插件链

2. **理解插件类型**：
   - **Mutating** — 可以修改对象
     - `NamespaceLifecycle` — namespace 生命周期
     - `PodNodeEnvironment` — Pod 节点环境
     - `MutatingAdmissionWebhook` — 外部 webhook
   - **Validating** — 可以拒绝对象
     - `ResourceQuota` — 资源配额
     - `LimitRanger` — 资源限制
     - `ValidatingAdmissionWebhook` — 外部 webhook
     - `PodSecurity` — Pod 安全标准

3. **理解 Webhook**：
   - `MutatingWebhookConfiguration` / `ValidatingWebhookConfiguration`
   - 外部 HTTPS 服务
   - 失败策略：`Fail` 或 `Ignore`

4. **理解 Pod Security Admission**：
   - 替代已废弃的 PodSecurityPolicy
   - Namespace 级别的策略
   - 三个级别：`privileged`、`baseline`、`restricted`
   - 三种模式：`enforce`、`audit`、`warn`

#### 1.3.4 理解 Secret 管理

1. **理解 Secret 类型**：
   - `Opaque` — 通用秘密
   - `kubernetes.io/service-account-token` — ServiceAccount 令牌
   - `kubernetes.io/dockerconfigjson` — 镜像仓库凭证
   - `kubernetes.io/tls` — TLS 证书

2. **理解 Secret 的存储**：
   - 存储在 etcd 中
   - 默认不加密（建议启用加密）
   - `EncryptionConfiguration` — 加密配置

3. **阅读 `pkg/registry/core/secret/`**：
   - Secret 的 REST 实现
   - 权限控制

---

## 2. 关键文件清单

| 文件/目录 | 说明 | 阅读重点 |
|-----------|------|----------|
| `pkg/controller/endpoints/` | Endpoints 控制器 | 端点计算 |
| `pkg/controller/endpointslice/endpointslice_controller.go` | EndpointSlice 控制器 | `NewController()` 第83行 |
| `pkg/kubelet/network/` | CNI 集成 | `CNIConfig` |
| `pkg/apis/networking/` | NetworkPolicy API | 类型定义 |
| `pkg/proxy/` | kube-proxy 核心 | `Proxier` 接口 |
| `pkg/volume/` | 卷插件框架 | `VolumePlugin` 接口 |
| `pkg/controller/volume/attach_detach/` | Attach/Detach 控制器 | reconcile 逻辑 |
| `pkg/kubelet/volumemanager/` | 节点卷管理器 | `operationExecutor` |
| `staging/src/k8s.io/csi-translation-lib/` | CSI 迁移库 | 兼容层 |
| `pkg/controller/volume/persistentvolume/` | PV/PVC 控制器 | 绑定逻辑 |
| `pkg/authentication/` | 认证框架 | `Authenticator` 接口 |
| `pkg/authorization/` | 授权框架 | `Authorizer` 接口 |
| `pkg/apis/rbac/` | RBAC API 类型 | `types.go`、`validation/validation.go` |
| `pkg/apis/rbac/v1/` | RBAC v1 版本 | `defaults.go`、`helpers.go` |
| `staging/src/k8s.io/apiserver/pkg/admission/` | Admission 框架 | `AdmissionInterface` |

---

## 3. 实践任务清单

- [ ] **任务 1**：阅读 Endpoints 控制器，理解 Service 端点如何计算
  - 实验：查看 `pkg/controller/endpointslice/endpointslice_controller.go:83` 的 `NewController()` 函数
- [ ] **任务 2**：追踪一个 API 请求经过认证→授权→Admission 的完整路径
- [ ] **任务 3**：阅读 RBAC 的 bootstrap 角色数据，理解权限规则的结构
  - 实验：查看 `pkg/apis/rbac/` 目录下的类型定义和验证逻辑
- [ ] **任务 4**：阅读 Attach/Detach 控制器，理解卷 attach 的 reconcile 逻辑
- [ ] **任务 5**：阅读 CSI 迁移库，理解 in-tree 到 CSI 的迁移策略
- [ ] **任务 6**：理解 Pod Security Admission 的三个级别和三种模式
- [ ] **任务 7**：在本地集群中创建 RBAC 角色和绑定，测试权限
  - 实验：创建 RBAC 角色并测试权限
    ```bash
    # 1. 创建 Role
    kubectl apply -f - <<'EOF'
    apiVersion: rbac.authorization.k8s.io/v1
    kind: Role
    metadata:
      namespace: default
      name: pod-reader
    rules:
    - apiGroups: [""]
      resources: ["pods"]
      verbs: ["get", "list", "watch"]
    EOF

    # 2. 创建 ServiceAccount
    kubectl create serviceaccount test-sa -n default

    # 3. 绑定 Role 到 ServiceAccount
    kubectl apply -f - <<'EOF'
    apiVersion: rbac.authorization.k8s.io/v1
    kind: RoleBinding
    metadata:
      name: read-pods
      namespace: default
    subjects:
    - kind: ServiceAccount
      name: test-sa
      namespace: default
    roleRef:
      kind: Role
      name: pod-reader
      apiGroup: rbac.authorization.k8s.io
    EOF

    # 4. 使用 ServiceAccount 测试权限
    kubectl auth can-i list pods --as=system:serviceaccount:default:test-sa -n default
    kubectl auth can-i delete pods --as=system:serviceaccount:default:test-sa -n default
    ```
- [ ] **任务 8**：阅读 VolumeAttachment 对象，理解 attach 状态的协调

**实验代码**：查看 Service 和 EndpointSlice 的关系
```bash
# 1. 创建 Deployment 和 Service
kubectl create deployment test-svc --image=nginx --replicas=3
kubectl expose deployment test-svc --port=80

# 2. 查看 Service 详情
kubectl get svc test-svc -o yaml

# 3. 查看 EndpointSlice
kubectl get endpointslice -l kubernetes.io/service-name=test-svc -o yaml

# 4. 查看 Pod 的 IP 地址
kubectl get pods -l app=test-svc -o wide

# 5. 验证 Service 流量转发
kubectl run test-client --image=curlimages/curl --rm -it --restart=Never -- curl test-svc:80

# 6. 清理
kubectl delete deployment test-svc
kubectl delete service test-svc
```

---

## 4. 核心模式总结

### 网络流量路径

```
Client
    │
    ▼
Service ClusterIP / NodePort / LoadBalancer
    │
    ▼ (kube-proxy 规则)
iptables / IPVS / nftables
    │
    ▼ (DNAT)
Endpoint Pod IP
    │
    ▼ (CNI 网络)
Pod 网络命名空间
    │
    ▼
Container
```

### 卷 attach/mount 流程

```
1. 用户创建 PVC
    │
    ▼
2. PV/PVC 控制器绑定 PV 和 PVC
    │
    ▼
3. Pod 引用 PVC，调度到节点
    │
    ▼
4. Attach/Detach 控制器
    ├── 检测到 Pod 绑定
    ├── 调用云 provider / CSI 驱动 attach
    └── 更新 VolumeAttachment
    │
    ▼
5. Kubelet 卷管理器
    ├── 检测到卷已 attach
    ├── Stage 卷（CSI）
    ├── Publish 卷到 Pod（CSI）
    └── 挂载到容器
```

### 安全请求路径

```
HTTP Request
    │
    ▼
Authentication
    ├── Client cert
    ├── Bearer token
    ├── OIDC
    └── Webhook
    │
    ▼ (建立用户身份)
Authorization
    ├── RBAC
    ├── ABAC
    └── Webhook
    │
    ▼ (检查权限)
Admission
    ├── Mutating (修改对象)
    └── Validating (拒绝对象)
    │
    ▼
REST Handler → etcd
```

---

## 5. 检查点

进入第6阶段前，确保你能回答：

1. Service 的流量如何从 ClusterIP 到达 Pod？
2. EndpointSlice 相比 Endpoint 的优势是什么？
3. CNI 插件在 Pod 生命周期中的哪个阶段被调用？
4. Attach/Detach 控制器和 Kubelet 卷管理器的分工是什么？
5. CSI 驱动的核心接口有哪些？
6. RBAC 的 Role 和 ClusterRole 的区别？
7. Admission 插件的执行顺序是什么？
8. Pod Security Admission 的三个级别分别限制什么？
9. ServiceAccount token 如何挂载到 Pod？
10. PV/PVC 的绑定流程是什么？

---

## 6. 下一步

完成本阶段后，进入 [第6阶段：高级主题](./06-advanced-topics.md)。

高级主题包括 CRD/Operator 模式、可观测性、高级调度等。在掌握网络、存储、安全基础后，可以深入这些更复杂的主题。
