# 反直觉清单：读代码前先拆掉的误解

每条都写清"以为是什么 / 实际是什么 / 怎么验证"。行号按 `release-1.37-zh` 的 `205d183e7fb` 核对。

## 客户端

**1. "kubectl create -f 走 dynamic client"。** 不走。它走 cli-runtime 的 `resource.Builder` + `resource.Helper`，底层是按文件
GVK 现造的 RESTClient，对象全程 unstructured。dynamic client 是另一个独立包（`staging/src/k8s.io/client-go/dynamic/`
），kubectl 这条路径没用它。验证：从 `CreateOptions.RunCreate`（`create/create.go:269`）跟到 `resource.NewHelper`，全程搜不到
`dynamic.Interface`。详见 `01-code-map.md` §2.1。

**2. "kubectl 有 create pod 子命令"。** 没有。`create` 的子命令是 deployment、job、cronjob、namespace 等；Pod 只能靠 `-f`。验证：
`create/create.go:141-157` 的 `AddCommand` 列表里没有 Pod。

**3. "typed client 会做版本协商"。** 不会。`corev1.SchemeGroupVersion` 在生成代码里写死，codec 和 protobuf
偏好也是编译期决定的。运行时发现是 Builder 路径的事。验证：`typed/core/v1/pod.go:66` 的 `newPods` 只传了 `"pods"` 字符串和
`scheme.ParameterCodec`；`client_config.go:329` 的 `RESTClientFor` 要求调用方先填好 `config.GroupVersion`。

**4. "rest.Config 只是 kubeconfig 的别名"。** 不是。`BuildConfigFromFlags` 解析 kubeconfig 产出 `*rest.Config`，后者还承载
UserAgent、限速器、TLS transport、content config，可由代码程序化构造。验证：`client_config.go:680` vs `rest/config.go` 的
`Config` 结构体。

**5. "Unstructured 就是低配版 typed"。** 它压根没有 Go 类型，是 `map[string]interface{}` +
GVK。好处是未知类型（CRD）也能创建；代价是字段错误要到服务端才暴露。验证：
`staging/src/k8s.io/apimachinery/pkg/apis/meta/v1/unstructured/`。

## apiserver

**6. "所有认证授权都在 handler 之前的一次过滤链里完成"。** 过滤链（`DefaultBuildHandlerChain`）做的是
authentication、authorization、audit、flow control 等横切关注点；admission 在业务 handler 内部、策略校验前后各有一次。二者是两个阶段，职责不重叠。验证：
`01-code-map.md` §2.2 与 §2.3 的表，对比 `server/config.go:1076` 和 `admission/chain.go`。

**7. "mutating admission 一次搞定所有改写"。** 不是一次：mutating admission → `PrepareForCreate`（资源 strategy 的
defaulting）→ 静态 `Validate` → validating admission → `Storage.Create`。`PrepareForCreate` 属于 strategy，不属于 admission
插件。验证：`store.go:545` 的 `create` 与 `rest/create.go` 的 `BeforeCreate`，再对照 `pkg/registry/core/pod/strategy.go`。

**8. "校验失败发生在 storage 层"。** 绝大多数发生在更前面：字段校验在 `pkg/apis/core/validation/validation.go` 的
`ValidatePodCreate`，admission 在各自插件里；storage 只做最后的持久化冲突处理。验证：看一个失败请求的错误文本，能反推是哪一层返回的。

**9. "Pod Security 是一组字段校验"。** 它是 admission 插件（`staging/src/k8s.io/pod-security-admission/`），在 Pod 创建 /
更新时按 namespace 的 exemption / warn / audit / enforce 策略评估，不是 `ValidatePodCreate` 里的 if。验证：
`pkg/kubeapiserver/options/admission.go` 的插件注册列表。

**10. "内部类型是外部类型的别名"。** 不是。外部类型（`staging/src/k8s.io/api/core/v1`）是 wire 契约，内部类型（`pkg/apis/core`
）是 apiserver 内部的版本无关表示，之间有 conversion 与 defaulting。apiserver 处理请求时先在内部类型上操作，再编码成请求或响应所需的版本。验证：
`pkg/apis/core/v1/conversion.go` 的生成物与 `pkg/apis/core/install/install.go`。

**11. "CRD 只是存了个 schema 给 apiserver 读"。** CRD 会动态注册 REST 存储、参与 discovery、让 `RESTMapper` 认得新 Kind。装
CRD 前后 `kubectl create -f` 的行为不同，就是因为 `mapper.infoForData`（`cli-runtime/pkg/resource/mapper.go:42`）的
`RESTMapping` 从失败变成成功。验证：未安装 CRD 时创建自定义 Kind，会看到 `ensure CRDs are installed first`。

## 控制面与节点

**12. "直接创建 Pod 会经过 Deployment controller"。** 不会。Deployment → ReplicaSet → Pod 是控制器写 API 的因果链，不是任一
Pod 创建请求的处理链。直接 POST Pod 只走 apiserver 的通用资源链。验证：`01-code-map.md` §3.1 的控制器模式，控制器读 informer
cache 再写回 API，不在 POST 请求栈里。

**13. "scheduler 把 Pod 搬到了目标节点"。** 它只写一个 Binding 对象（服务端由
`pkg/registry/core/pod/storage/storage.go:175` 的 `BindingREST` 处理），把 `spec.nodeName` 填上。真正把容器跑起来的是目标节点上的
kubelet。验证：`pkg/scheduler/util/utils.go` 的 `BindPod` 与 `defaultbinder` 插件。

**14. "kubelet 一条主 goroutine 串行处理所有 Pod"。** `syncLoop` 是事件入口，具体工作交给 per-Pod worker 串行化，不同 Pod
之间可并行。验证：`pkg/kubelet/pod_workers.go` 的 `UpdatePod` / `podWorkerLoop`。

**15. "kubelet 直接调 Docker / containerd 的 HTTP API"。** 它只认识 CRI（`container.Runtime` 接口 + gRPC），具体运行时由 CRI
实现对接。kubelet 代码里没有 docker client 调用就是证据。验证：`pkg/kubelet/container/runtime.go:74` 与
`staging/src/k8s.io/cri-client/pkg/remote_runtime.go`。

**16. "PLEG 是 CRI 的一部分 / 等于容器运行时"。** PLEG 是 kubelet 侧的 runtime 健康探测与容器事件源（`pkg/kubelet/pleg/`
），通过 CRI 查询实现，不是 CRI 本身。验证：`pkg/kubelet/pleg/` 目录与 `Runtime.Version()` 的调用点。

## 工程与测试

**17. "`go.mod` 改一行版本就能升级依赖"。** `go.mod` / `go.work` 是生成物，必须走 `hack/pin-dependency.sh` +
`hack/update-vendor.sh`。直接 `go mod tidy` 会破坏 staging 模块与 vendor 的一致性。验证：`04-workflow.md` §7。

**18. "vendor/k8s.io/client-go 是 client-go 的源码"。** 真相在 `staging/src/k8s.io/client-go`；vendor
里的第三方依赖才是主角。改错地方不会被编进产物。验证：`go.work` 的 replace 指向与 `AGENTS.md` 的 staging 约束。

**19. "`zz_generated.*` 手改一下就能过 verify"。** 手改产物会被下一次 `make update` 覆盖，`verify-codegen` 在干净 worktree
跑生成器后比对 diff，直接判失败。改输入（tag / 类型 / proto / 文档源），跑 `make update`。验证：`04-workflow.md` §5-6。

**20. "conformance 是独立测试框架"。** 它是 `framework.ConformanceIt` 标记 + `testdata/conformance.yaml` golden list
治理，底层还是 E2E Ginkgo。新增 / 删除 conformance 用例要按 `test/conformance/README.md` 更新清单，还要过 sig-architecture
review。验证：`test/conformance/walk_test.go`。

**21. "test-integration 和 E2E 都是'跑起来看看'"。** integration 起的是受控的 etcd + 真 apiserver + 组件，不需要集群；E2E
要真集群和 provider 配置；node E2E 还要 Linux 与 password-less sudo。macOS 上跑不了 node E2E 是环境约束，不是代码缺陷。验证：
`04-workflow.md` §3、§8。

**22. "feature gate 是配置文件里的开关"。** 代码里是编译期注册、带 lifecycle 阶段的 `FeatureGate` 对象；
`pkg/features/kube_features.go` 改了之后要跑 `hack/update-featuregates.sh` 更新参考数据，再 verify。验证：
`hack/verify-featuregates.sh` 比对的文件。

## 怎么用这份清单

遇到新代码路径时先问：这条是不是撞上了上面某一条？如果撞上了，先按"验证"列去代码或命令里确认，再继续读。每读完一个子系统，回来把新发现的误解追加进去——这份清单比笔记更值钱，因为它记录的是你曾经相信过的错事。
