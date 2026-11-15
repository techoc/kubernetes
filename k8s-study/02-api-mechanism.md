# 第2阶段：API 机制和 client-go

**目标**：理解 Kubernetes 的 API 资源模型、版本管理、代码生成和客户端库。

**前置要求**：完成第1阶段，熟悉 Go 接口、泛型、反射。

---

## 1. 你需要做什么

### 1.1 理解 API 资源模型

Kubernetes 中一切皆对象 — Pod、Service、Deployment 等都是 API 资源，底层是 Go 结构体。

**需要做的事**：

1. **理解 `runtime.Object` 接口** — 所有 API 对象实现的接口：
   ```go
   // staging/src/k8s.io/apimachinery/pkg/runtime/interfaces.go
   type Object interface {
       CacheableObject
       Reset()
       GenerateAlias()
       GetObjectKind() schema.ObjectKind
       DeepCopyObject() Object
   }
   ```
   阅读这个接口，理解为什么 API 对象需要这些方法。

2. **理解 `runtime.Scheme`** — 类型注册表：
   - `Scheme` 结构体：`staging/src/k8s.io/apimachinery/pkg/runtime/scheme.go:49`
   - `AddKnownTypes()` 方法：`staging/src/k8s.io/apimachinery/pkg/runtime/scheme.go:150` — 注册一组类型到 Scheme
   - `NewScheme()` 构造函数：`staging/src/k8s.io/apimachinery/pkg/runtime/scheme.go:100`
   
   阅读 `scheme.go`，理解注册和查找流程。

3. **追踪一个具体 API 类型的完整注册流程** — 以 `Pod` 为例：
   - 类型定义：`staging/src/k8s.io/api/core/v1/pod.go` 中的 `type Pod struct`
   - 注册到 Scheme：`staging/src/k8s.io/api/core/v1/register.go` 中的 `AddKnownTypes()`
   - GVK 定义：`SchemeGroupVersion = schema.GroupVersion{Group: "", Version: "v1"}`
   
   **实验代码**：查看 Pod 类型的注册过程
   ```go
   // staging/src/k8s.io/api/core/v1/register.go — 查看 AddKnownTypes 函数
   // 确认 Pod, PodList, PodSpec 等类型被注册到 Scheme
   ```

4. **理解 `runtime.Encoder` / `Decoder`** — 对象的序列化：
   - JSON encoder/decoder
   - Protocol Buffer encoder/decoder
   - `runtime.SerializerInfo` — 多种序列化格式的集合

---

### 1.2 理解 API 版本管理

**需要做的事**：

1. **理解 Group 和 Version**：
   - Core group（空字符串 `""`）— Pod、Service、Namespace 等核心资源
   - Named group — `apps`（Deployment）、`batch`（Job）、`rbac.authorization.k8s.io`（RBAC）等
   - Version — `v1`（稳定）、`v1beta1`（测试）、`v1alpha1`（实验）

2. **理解版本转换（Conversion）**：
   - 当客户端使用 `v1beta1` 而服务器内部使用 `v1` 时，需要转换
   - `Hub` / `Spoke` 模式：一个版本作为 Hub，其他版本通过转换函数连接到 Hub
   - 阅读 `staging/src/k8s.io/api/apps/v1beta1/conversion.go` 了解转换函数的写法

3. **理解默认值（Defaulter）**：
   - `SetDefaults_PodSpec()` — 为字段设置默认值
   - 在 `staging/src/k8s.io/api/core/v1/defaults.go` 中定义

4. **理解验证（Validation）**：
   - `ValidatePodSpec()` — 验证字段合法性
   - 在 `pkg/apis/core/validation/` 中定义
   - 区分 `fieldpath` 错误和常规错误

---

### 1.3 理解 Feature Gates

**需要做的事**：

1. **阅读 `pkg/features/kube_features.go`**：
   - 所有 Feature Gate 的注册地
   - `defaultVersionedKubernetesFeatureGates` map：第1257行 — 按字母顺序排列的所有特性门
   - `defaultKubernetesFeatureGateDependencies` map：第2378行 — 特性之间的依赖关系
   - `init()` 函数：第2833行 — 注册所有特性门

2. **理解 Feature Gate 生命周期**：
   ```
   Alpha (默认关闭) → Beta (默认开启) → GA (锁定默认值, 门最终移除)
   ```
   - `LockToDefault: true` — GA 特性，门被锁定
   - `PreRelease: beta` — Beta 阶段
   - `Default: false` — Alpha 阶段

3. **理解版本化评估**：
   - Feature Gate 可以按 Kubernetes 版本有不同的默认值
   - `VersionedGateName` — 支持 n-3 兼容性策略

4. **实验代码**：查看 Feature Gate 的定义
   ```go
   // pkg/features/kube_features.go:1257
   var defaultVersionedKubernetesFeatureGates = map[featuregate.Feature]featuregate.VersionedSpecs{
       AllowDNSOnlyNodeCSR: {
           {Version: version.MustParse("1.0"), Default: true, PreRelease: featuregate.GA},
       },
       // ... 更多特性
   }
   ```
   找一个 Alpha 和一个 GA 的 Feature Gate，对比它们的配置。

---

### 1.4 理解 client-go 核心

client-go 是 Kubernetes 客户端库，所有组件都用它与 API Server 通信。

**需要做的事**：

1. **理解 `kubernetes.Clientset`** — 内置 API 的 typed 客户端：
   ```go
   clientset, err := kubernetes.NewForConfig(config)
   pods := clientset.CoreV1().Pods("default")
   pod, err := pods.Get(context.TODO(), "my-pod", metav1.GetOptions{})
   ```
   阅读 `staging/src/k8s.io/client-go/kubernetes/clientset.go`，理解 Clientset 的结构。

2. **理解 `dynamic.Client`** — 处理 CRD 等未结构化资源：
   ```go
   dynamicClient := dynamic.NewForConfigOrDie(config)
   unstructuredObj := &unstructured.Unstructured{
       Object: map[string]interface{}{
           "apiVersion": "example.com/v1",
           "kind":       "MyCRD",
       },
   }
   ```

3. **理解 REST Client** — 底层 HTTP 客户端：
   - `staging/src/k8s.io/client-go/rest/client.go`
   - 处理认证、重试、速率限制
   - `Create`、`Update`、`Delete`、`Get`、`List`、`Watch` 的底层实现

4. **理解 `Reflector`** — LIST + WATCH 同步机制（**核心中的核心**）：
   ```
   API Server ──LIST──→ Reflector ──→ DeltaFIFO ──→ Store (cache)
            └──WATCH──┘
   ```
   阅读 `staging/src/k8s.io/client-go/tools/cache/reflector.go`：
   - `NewReflector()` 构造函数：第241行
   - `NewReflectorWithOptions()` 完整构造函数：第298行
   - `Run()` 启动反射器：第418行
   - `ListAndWatch()` 先 LIST 全量，再 WATCH 增量：第465行
   - `ListAndWatchWithContext()` 带上下文的实现：第472行
   
   **重点理解**：
   - 为什么需要 LIST + WATCH 而不是只 WATCH？
   - `resourceVersion` 如何保证不丢失事件？
   - 网络断开后如何恢复？

5. **理解 `SharedInformer`** — 带事件分发的缓存：
   ```
   Reflector → DeltaFIFO → Store → ResourceEventHandler
   ```
   阅读 `staging/src/k8s.io/client-go/tools/cache/shared_informer.go`：
   - `NewSharedInformer()` 构造函数：第421行
   - `NewSharedIndexInformer()` 带索引的构造函数：第427行
   - `NewSharedIndexInformerWithOptions()` 完整构造函数：第449行
   - `AddEventHandler()` 注册事件处理器：第1030行
   - `Run()` 启动 Informer：第893行
   
   **关键设计**：
   - Informer 是共享的，多个控制器可以订阅同一个 Informer
   - 事件处理器在单独的 goroutine 中调用，不阻塞 Reflector
   - `Resync` 周期性地触发 `Update` 事件，即使对象没有变化

6. **理解 `Lister`** — 只读缓存访问：
   ```go
   podLister := informers.CoreV1().Pods().Lister()
   pod, err := podLister.Pods("default").Get("my-pod")
   ```
   - Lister 从 Informer 的 Store 读取，不直接访问 API Server
   - 线程安全，适合控制器并发读取

7. **理解 `workqueue`** — 带退避的重试队列：
   ```go
   queue := workqueue.NewRateLimitingQueue(workqueue.DefaultControllerRateLimiter())
   queue.Add("key")
   key, shutdown := queue.Get()
   defer queue.Done(key)
   process(key)
   queue.Forget(key)  // 处理成功后忘记，重置退避
   ```
   阅读 `staging/src/k8s.io/client-go/util/workqueue/`：
   - `DefaultControllerRateLimiter()` 指数退避：`default_rate_limiters.go:44`
   - `NewRateLimitingQueue()` 构造函数：`rate_limiting_queue.go:74`
   - `RateLimitingInterface` — 支持退避的队列
   - `Forget()` — 处理成功后重置退避计数

**实验代码**：使用 client-go 连接集群并监听 Pod 变化
```go
package main

import (
    "context"
    "fmt"
    "time"

    v1 "k8s.io/api/core/v1"
    metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
    "k8s.io/client-go/informers"
    "k8s.io/client-go/kubernetes"
    "k8s.io/client-go/tools/cache"
    "k8s.io/client-go/util/homedir"
    "k8s.io/client-go/util/workqueue"
    "k8s.io/client-go/rest"
    "k8s.io/client-go/tools/clientcmd"
    "k8s.io/client-go/util/homedir"
)

func main() {
    // 1. 从 kubeconfig 创建客户端
    home := homedir.HomeDir()
    config, err := clientcmd.BuildConfigFromFlags("", home+"/.kube/config")
    if err != nil {
        panic(err)
    }
    clientset, err := kubernetes.NewForConfig(config)
    if err != nil {
        panic(err)
    }

    // 2. 创建 SharedInformer
    informerFactory := informers.NewSharedInformerFactory(clientset, 0)
    podInformer := informerFactory.CoreV1().Pods().Informer()

    // 3. 注册事件处理器
    podInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
        AddFunc: func(obj interface{}) {
            pod := obj.(*v1.Pod)
            fmt.Printf("Pod 创建: %s/%s\n", pod.Namespace, pod.Name)
        },
        UpdateFunc: func(oldObj, newObj interface{}) {
            oldPod := oldObj.(*v1.Pod)
            newPod := newObj.(*v1.Pod)
            if oldPod.ResourceVersion != newPod.ResourceVersion {
                fmt.Printf("Pod 更新: %s/%s (RV: %s → %s)\n",
                    newPod.Namespace, newPod.Name, oldPod.ResourceVersion, newPod.ResourceVersion)
            }
        },
        DeleteFunc: func(obj interface{}) {
            pod := obj.(*v1.Pod)
            fmt.Printf("Pod 删除: %s/%s\n", pod.Namespace, pod.Name)
        },
    })

    // 4. 启动 Informer
    stopCh := make(chan struct{})
    defer close(stopCh)
    go informerFactory.Start(stopCh)

    // 5. 等待缓存同步
    if !cache.WaitForCacheSync(stopCh, podInformer.HasSynced) {
        panic("timeout waiting for informer cache to sync")
    }
    fmt.Println("Informer 缓存已同步，开始监听 Pod 变化...")

    // 6. 创建一个 Pod 观察事件
    pod := &v1.Pod{
        ObjectMeta: metav1.ObjectMeta{
            Name:   "test-pod-" + fmt.Sprint(time.Now().Unix()),
            Namespace: "default",
        },
        Spec: v1.PodSpec{
            Containers: []v1.Container{{
                Name:  "nginx",
                Image: "nginx:1.14",
            }},
            RestartPolicy: v1.RestartPolicyNever,
        },
    }
    _, err = clientset.CoreV1().Pods("default").Create(context.TODO(), pod, metav1.CreateOptions{})
    if err != nil {
        panic(err)
    }
    fmt.Printf("已创建 Pod: %s\n", pod.Name)

    // 7. 等待一段时间后删除
    time.Sleep(5 * time.Second)
    err = clientset.CoreV1().Pods("default").Delete(context.TODO(), pod.Name, metav1.DeleteOptions{})
    if err != nil {
        panic(err)
    }
    fmt.Printf("已删除 Pod: %s\n", pod.Name)

    // 8. 使用 workqueue 处理事件
    queue := workqueue.NewRateLimitingQueue(workqueue.DefaultControllerRateLimiter())
    // ... 在实际控制器中，事件处理器会将 key 加入 queue，worker goroutine 从 queue 取出处理
    _ = queue
    time.Sleep(2 * time.Second)
}
```

---

### 1.5 理解代码生成

Kubernetes 使用代码生成器自动生成大量样板代码。

**需要做的事**：

1. **理解生成器体系**：
   ```
   staging/src/k8s.io/code-generator/
   ├── generators/
   │   ├── deepcopy-gen/    # DeepCopy 方法
   │   ├── client-gen/      # Typed clientset
   │   ├── informer-gen/    # Informers
   │   ├── lister-gen/      # Listers
   │   ├── openapi-gen/     # OpenAPI spec
   │   ├── registration-gen/ # Scheme 注册
   │   └── ...
   └── generate-internal.sh  # 生成入口
   ```

2. **理解 `deepcopy-gen`**：
   - 为 API 类型生成 `DeepCopy()` 和 `DeepCopyInto()` 方法
   - 通过注释 `+k8s:deepcopy-gen=interfaces,k8s.io/apimachinery/pkg/runtime.Object` 标记
   - 生成代码在 `zz_generated.deepcopy.go` 中

3. **理解 `client-gen`**：
   - 根据 API 类型生成 typed client
   - 配置在 `staging/src/k8s.io/client-go/clientset.go` 的注释中
   - 生成 `kubernetes.Clientset` 和各个 version 的 client

4. **理解 `informer-gen` 和 `lister-gen`**：
   - 为每个 API 类型生成 Informer 和 Lister
   - 生成代码在 `staging/src/k8s.io/client-go/informers/` 和 `listers/` 中

5. **运行代码生成**：
   ```bash
   ./hack/update-codegen.sh
   ```
   观察生成了哪些文件。

6. **理解注释标记**：
   - `+k8s:deepcopy-gen=interfaces,...` — 生成 DeepCopy
   - `+k8s:openapi-gen=true` — 生成 OpenAPI spec
   - `+genclient` — 生成 client
   - `+k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object`

---

## 2. 关键文件清单

| 文件/目录 | 说明 | 阅读重点 |
|-----------|------|----------|
| `staging/src/k8s.io/apimachinery/pkg/runtime/` | API 运行时基础 | `scheme.go`、`interfaces.go` |
| `staging/src/k8s.io/api/core/v1/` | Pod 等核心 API 类型 | `pod.go`、`register.go`、`defaults.go` |
| `staging/src/k8s.io/client-go/kubernetes/` | Typed clientset | `clientset.go` |
| `staging/src/k8s.io/client-go/dynamic/` | 动态客户端 | `dynamic.go` |
| `staging/src/k8s.io/client-go/tools/cache/` | Reflector/Informer | `reflector.go`、`shared_informer.go` |
| `staging/src/k8s.io/client-go/util/workqueue/` | 工作队列 | `rate_limiting.go`、`default_rate_limiters.go` |
| `staging/src/k8s.io/client-go/informers/` | 生成的 Informers | 结构、如何组织 |
| `staging/src/k8s.io/client-go/listers/` | 生成的 Listers | 只读访问模式 |
| `pkg/features/kube_features.go` | Feature Gate 注册 | `defaultFeatureGates` |
| `staging/src/k8s.io/code-generator/` | 代码生成器 | 各 generator 的入口 |
| `hack/update-codegen.sh` | 代码生成脚本 | 调用流程 |

---

## 3. 实践任务清单

- [ ] **任务 1**：追踪 `Pod` 类型从 `api/core/v1/pod.go` 定义到注册到 Scheme 的完整过程
- [ ] **任务 2**：阅读 `reflector.go` 的 `ListAndWatch()`，画出 LIST + WATCH 的流程图
- [ ] **任务 3**：阅读 `shared_informer.go`，理解 `AddEventHandler()` 如何分发事件
- [ ] **任务 4**：阅读 `workqueue` 的退避逻辑，理解 `Forget()` 的作用
- [ ] **任务 5**：写一个简单程序，用 client-go 创建 Pod 并 watch 其状态变化
- [ ] **任务 6**：找一个 API 类型的 `zz_generated.deepcopy.go`，理解生成逻辑
- [ ] **任务 7**：在 `kube_features.go` 中找一个 Alpha 和 GA 的 Feature Gate，对比配置
- [ ] **任务 8**：运行 `./hack/update-codegen.sh`，观察生成了哪些文件

---

## 4. 核心模式总结

### 控制器模式（Controller Pattern）

这是 Kubernetes 最核心的设计模式，后续所有控制器都基于此：

```
1. Reflector 通过 LIST + WATCH 同步 API Server 的数据到本地缓存
2. Informer 监听缓存变化，触发事件处理器
3. 事件处理器将工作项 enqueue 到 workqueue
4. Worker goroutine 从 workqueue 取出工作项，执行 reconcile
5. Reconcile 比较期望状态和实际状态，执行必要的操作
6. 操作结果写回 API Server，Reflector 再次检测到变化，触发下一轮 reconcile
```

**关键点**：
- 事件处理器只 enqueue，不做耗时操作
- Reconcile 是幂等的，可以重复执行
- 失败可以重新入队，带退避
- 不阻塞 watch 流

### Informer 共享模式

```
API Server
    │
    ▼
Reflector → DeltaFIFO → Store (cache)
                    │
                    ▼
            SharedInformer
                    │
        ┌───────────┼───────────┐
        ▼           ▼           ▼
   Handler A    Handler B    Handler C
   (Controller  (Scheduler  (Kubelet
    ReplicaSet)  Controller)  Controller)
```

一个 Informer 被多个控制器共享，避免重复的 LIST/WATCH。

---

## 5. 检查点

进入第3阶段前，确保你能回答：

1. `runtime.Scheme` 的作用是什么？GVK 和 Go type 如何映射？
2. Reflector 为什么需要 LIST + WATCH？只 WATCH 有什么问题？
3. Informer 和 Reflector 的区别是什么？
4. workqueue 的退避机制如何工作？`Forget()` 的作用是什么？
5. 事件处理器为什么只 enqueue 而不直接 reconcile？
6. `deepcopy-gen` 如何知道为哪些类型生成代码？
7. Feature Gate 从 Alpha 到 GA 的生命周期是什么？
8. Clientset 和 dynamic client 的区别和使用场景？

---

## 6. 下一步

完成本阶段后，进入 [第3阶段：控制平面组件](./03-control-plane.md)。

控制平面是 Kubernetes 的大脑 — API Server 是所有组件的协调中心，Controller Manager 运行所有控制器，Scheduler 决定 Pod 运行在哪里。理解第2阶段的 Informer + workqueue 模式是理解这些组件的前提。
