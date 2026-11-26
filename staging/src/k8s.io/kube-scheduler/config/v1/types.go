/*
Copyright 2022 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package v1

import (
	"bytes"
	"fmt"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	componentbaseconfigv1alpha1 "k8s.io/component-base/config/v1alpha1"
	"sigs.k8s.io/yaml"
)

// 这里的默认值同时被 CLI 标志和配置文件解码器使用。把锁对象身份固定成常量，
// 是为了让多个 scheduler 副本在选主时指向同一个资源；改其中任何一个都会导致
// 副本之间互相看不见对方的锁。
const (
	// SchedulerDefaultLockObjectNamespace defines default scheduler lock object namespace ("kube-system")
	SchedulerDefaultLockObjectNamespace string = metav1.NamespaceSystem

	// SchedulerDefaultLockObjectName defines default scheduler lock object name ("kube-scheduler")
	SchedulerDefaultLockObjectName = "kube-scheduler"

	// SchedulerDefaultProviderName 是默认 provider 的名字。
	SchedulerDefaultProviderName = "DefaultProvider"
)

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// KubeSchedulerConfiguration 是调度器的配置对象。
// 这是 scheduler 的顶层配置对象。它用指针字段表示“未指定则沿用默认值”，
// 这样配置文件只写少量覆盖项，其余仍走程序默认值；布尔值则没有这层区分。
type KubeSchedulerConfiguration struct {
	metav1.TypeMeta `json:""`

	// Parallelism 是调度单个 Pod 时算法的并行度，必须大于 0，默认 16。
	Parallelism *int32 `json:"parallelism,omitempty"`

	// LeaderElection 是 leader election 客户端的配置。
	LeaderElection componentbaseconfigv1alpha1.LeaderElectionConfiguration `json:"leaderElection"`

	// ClientConnection 指定 proxy 与 apiserver 通信所用的 kubeconfig 文件
	// 及客户端连接参数。
	ClientConnection componentbaseconfigv1alpha1.ClientConnectionConfiguration `json:"clientConnection"`

	// DebuggingConfiguration 保存调试相关功能的配置。
	// TODO: We might wanna make this a substruct like Debugging componentbaseconfigv1alpha1.DebuggingConfiguration
	componentbaseconfigv1alpha1.DebuggingConfiguration `json:""`

	// PercentageOfNodesToScore 是找到足够多可行节点后停止继续搜索的比例。
	// 它只影响可行节点的搜索范围，
	// 用于提升调度性能。无论该值多大，
	// 调度器至少会找 minFeasibleNodesToFind 个可行节点。
	// 例如集群有 500 个节点、该值为 30 时，
	// 找到 150 个可行节点后就停止继续搜索。
	// 值为 0 时按集群规模取默认的 5%~50% 比例；
	// 该全局值会被 profile 级别的设置覆盖。
	PercentageOfNodesToScore *int32 `json:"percentageOfNodesToScore,omitempty"`

	// PodInitialBackoffSeconds 是不可调度 Pod 的初始退避时长。
	// 指定时必须大于 0；为 null 时使用默认值 1s。
	//
	PodInitialBackoffSeconds *int64 `json:"podInitialBackoffSeconds,omitempty"`

	// PodMaxBackoffSeconds 是不可调度 Pod 的最大退避时长。
	// 指定时必须大于 podInitialBackoffSeconds；为 null 时
	// 使用默认值 10s。
	PodMaxBackoffSeconds *int64 `json:"podMaxBackoffSeconds,omitempty"`

	// Profiles 是 kube-scheduler 支持的调度配置集。Pod 可
	// 通过设置 schedulerName 选择特定 profile；
	// 未指定 schedulerName 的 Pod 若存在
	// "default-scheduler" profile，则使用它调度。
	// +listType=map
	// +listMapKey=schedulerName
	Profiles []KubeSchedulerProfile `json:"profiles,omitempty"`

	// Extenders 是调度器扩展列表，每项包含与对应扩展通信所需的参数。
	// 这些扩展由所有调度 profile 共享。
	// +listType=set
	Extenders []Extender `json:"extenders,omitempty"`

	// DelayCacheUntilActive 控制何时开始填充缓存。为 true 且启用 leader election 时，
	// 调度器会等到当选 leader 后才填充 informer 缓存；
	// 代价是故障切换更慢，好处是等待期间的开销更低。
	// 默认 false。
	DelayCacheUntilActive bool `json:"delayCacheUntilActive,omitempty"`
}

// DecodeNestedObjects 对已知类型的插件参数做解码。
// PluginConfig.Args 是 RawExtension，普通的 scheme 解码不会进去。插件参数的类型
// 要等到按插件名拼出 GVK 之后才知道，所以这里单独走一遍。严格解码错误先收集再
// 合并返回，目的是把所有位置的问题一次性报全，而不是遇到第一个就停。
func (c *KubeSchedulerConfiguration) DecodeNestedObjects(d runtime.Decoder) error {
	var strictDecodingErrs []error
	for i := range c.Profiles {
		prof := &c.Profiles[i]
		for j := range prof.PluginConfig {
			err := prof.PluginConfig[j].decodeNestedObjects(d)
			if err != nil {
				decodingErr := fmt.Errorf("decoding .profiles[%d].pluginConfig[%d]: %w", i, j, err)
				if runtime.IsStrictDecodingError(err) {
					strictDecodingErrs = append(strictDecodingErrs, decodingErr)
				} else {
					return decodingErr
				}
			}
		}
	}
	if len(strictDecodingErrs) > 0 {
		return runtime.NewStrictDecodingError(strictDecodingErrs)
	}
	return nil
}

// EncodeNestedObjects 对插件参数做解码。
func (c *KubeSchedulerConfiguration) EncodeNestedObjects(e runtime.Encoder) error {
	for i := range c.Profiles {
		prof := &c.Profiles[i]
		for j := range prof.PluginConfig {
			err := prof.PluginConfig[j].encodeNestedObjects(e)
			if err != nil {
				return fmt.Errorf("encoding .profiles[%d].pluginConfig[%d]: %w", i, j, err)
			}
		}
	}
	return nil
}

// KubeSchedulerProfile 是一个调度 profile。
// Profile 把“用哪些插件”和“插件怎么配”绑定在一个调度器名字上。同一进程里
// 可以并存多套插件组合，Pod 通过 spec.schedulerName 选一套，互不影响。
type KubeSchedulerProfile struct {
	// SchedulerName 是该 profile 关联的调度器名字。
	// If SchedulerName matches with the pod's "spec.schedulerName", then the pod
	// 的 Pod 使用它调度。
	SchedulerName *string `json:"schedulerName,omitempty"`

	// PercentageOfNodesToScore 是找到足够多可行节点后停止继续搜索的比例。
	// 它只影响可行节点的搜索范围，
	// 用于提升调度性能。无论该值多大，
	// 调度器至少会找 minFeasibleNodesToFind 个可行节点。
	// 例如集群有 500 个节点、该值为 30 时，
	// 找到 150 个可行节点后就停止继续搜索。
	// 值为 0 时按集群规模取默认的 5%~50% 比例；
	// 该值会覆盖全局设置；为空时
	// global PercentageOfNodesToScore will be used.
	PercentageOfNodesToScore *int32 `json:"percentageOfNodesToScore,omitempty"`

	// Plugins 指定要启用或禁用的插件集合。
	// Enabled 是在默认插件之外额外启用的插件；
	// Disabled 是需要关闭的默认插件。
	// 某扩展点未指定启用或禁用插件时，
	// 使用该扩展点的默认插件集。
	// 一旦指定 QueueSort 插件，所有 profile 都必须指定相同的
	// QueueSort 插件及其 PluginConfig。
	Plugins *Plugins `json:"plugins,omitempty"`

	// PluginConfig 是可选的插件参数集合，按插件分别配置。
	// 不写某个插件的参数即表示使用该插件的默认配置。
	// for that plugin.
	// +listType=map
	// +listMapKey=name
	PluginConfig []PluginConfig `json:"pluginConfig,omitempty"`
}

// Plugins 包含多个扩展点。一旦指定，该扩展点列出的插件
// 就是全部启用的插件；未指定的扩展点
// 则使用默认插件集。
// Enabled 插件在默认插件之后按此处顺序调用；若需
// 在默认插件之前调用，须先禁用默认插件，再在此按期望顺序重新启用。
// 把扩展点列成显式字段而不是 map，好处是配置结构固定、可生成代码，
// 也让“某个扩展点没写”明确表示沿用默认插件集。
type Plugins struct {
	// PreEnqueue 是在 Pod 入调度队列前调用的插件列表。
	PreEnqueue PluginSet `json:"preEnqueue,omitempty"`

	// QueueSort 是调度队列排序时调用的插件列表。
	QueueSort PluginSet `json:"queueSort,omitempty"`

	// PreFilter is a list of plugins that should be invoked at "PreFilter" extension point of the scheduling framework.
	PreFilter PluginSet `json:"preFilter,omitempty"`

	// Filter 是过滤掉无法运行 Pod 的节点时调用的插件列表。
	Filter PluginSet `json:"filter,omitempty"`

	// PostFilter 在过滤阶段之后、且没有任何可行节点时调用。
	PostFilter PluginSet `json:"postFilter,omitempty"`

	// PreScore 是打分前调用的插件列表。
	PreScore PluginSet `json:"preScore,omitempty"`

	// Score 是对通过过滤的节点打分排序时调用的插件列表。
	Score PluginSet `json:"score,omitempty"`

	// Reserve 是在为 Pod 分配节点后预留/释放资源时
	// 调用的插件列表。
	Reserve PluginSet `json:"reserve,omitempty"`

	// Permit 控制 Pod 的绑定，可阻止或延迟绑定。
	Permit PluginSet `json:"permit,omitempty"`

	// PreBind 是绑定 Pod 前调用的插件列表。
	PreBind PluginSet `json:"preBind,omitempty"`

	// Bind is a list of plugins that should be invoked at "Bind" extension point of the scheduling framework.
	// 调度器按顺序调用，一旦某个插件成功就跳过其余插件。
	Bind PluginSet `json:"bind,omitempty"`

	// PostBind 是 Pod 成功绑定后调用的插件列表。
	PostBind PluginSet `json:"postBind,omitempty"`

	// MultiPoint 是用于在所有有效扩展点上启用插件的简化配置。
	// 通过 MultiPoint 启用的插件会自动注册到它实现的每个扩展点；
	// 通过 MultiPoint 禁用则撤销该行为。
	// The same is true for disabling "*" through MultiPoint (no default plugins will be automatically registered).
	// 仍可通过各扩展点单独禁用插件。
	// 插件配置的优先级遵循以下层次：
	//   1. Specific extension points
	//   2. Explicitly configured MultiPoint plugins
	// 3. 作为 MultiPoint 插件的默认插件集。
	// 高优先级的配置先生效，并覆盖 MultiPoint 中的设置。
	// 用户显式配置的插件也优先于默认插件。
	// 同一层次内 Enabled 优先于 Disabled。例如，某插件既
	// set in both `multiPoint.Enabled` and `multiPoint.Disabled`, the plugin will be enabled. Similarly,
	// including `multiPoint.Disabled = '*'` and `multiPoint.Enabled = pluginA` will still register that specific
	// 又经由 MultiPoint 配置时，与其它扩展点的行为一致。
	MultiPoint PluginSet `json:"multiPoint,omitempty"`

	// PlacementGenerate 是在 PodGroup 调度周期为 PodGroup 确定放置位置时调用的插件列表。
	PlacementGenerate PluginSet `json:"placementGenerate,omitempty"`

	// PlacementScore 是在工作负载调度周期为 PodGroup 分配方案排序时调用的插件列表。
	PlacementScore PluginSet `json:"placementScore,omitempty"`

	// PodGroupPostFilter 是在工作负载调度阶段之后调用的插件列表，
	// 仅在 PodGroup 无法调度时触发，语义等同于单个 Pod 的 PostFilter。
	PodGroupPostFilter PluginSet `json:"podGroupPostFilter,omitempty"`
}

// PluginSet 指定某扩展点上启用与禁用的插件。
// 数组为空、缺失或为 nil 时，使用该扩展点的默认插件。
// Enabled 与 Disabled 分开而不是只给一个列表，是为了让“默认插件 + 追加/剔除”
// 这种增量配置成为常态；全量替换则通过 Disabled: ['*'] 表达。
type PluginSet struct {
	// Enabled 列出在默认插件之外额外启用的插件。
	// 若默认插件也在配置文件中出现，其权重
	// be overridden accordingly.
	// 它们在默认插件之后按此处给出的顺序调用。
	// +listType=atomic
	Enabled []Plugin `json:"enabled,omitempty"`
	// Disabled 列出需要关闭的默认插件。
	// When all default plugins need to be disabled, an array containing only one "*" should be provided.
	// +listType=map
	// +listMapKey=name
	Disabled []Plugin `json:"disabled,omitempty"`
}

// Plugin 指定插件名及适用的权重；权重只用于 Score 与 PlacementScore 插件。
type Plugin struct {
	// Name 是插件名。
	Name string `json:"name"`
	// Weight 是插件权重，仅用于 Score 与 PlacementScore 插件。
	Weight *int32 `json:"weight,omitempty"`
}

// PluginConfig 指定插件初始化时传入的参数。
// A plugin that is invoked at multiple extension points is initialized once. Args can have arbitrary structure.
// 如何解释这些参数由插件自己决定。
// Args 用 RawExtension 而非固定结构，是因为参数形状归插件自己定义；
// 配置 API 无需认识每个插件的参数类型就能把它们原样带进来。
type PluginConfig struct {
	// Name 是被配置插件的名字。
	Name string `json:"name"`
	// Args defines the arguments passed to the plugins at the time of initialization. Args can have arbitrary structure.
	Args runtime.RawExtension `json:"args,omitempty"`
}

func (c *PluginConfig) decodeNestedObjects(d runtime.Decoder) error {
	gvk := SchemeGroupVersion.WithKind(c.Name + "Args")
	// 先 dry-run 检测并跳过 out-of-tree 插件的参数。
	// 树外插件不会注册进这个 scheme；先用空输入探一次，未注册就直接放过，
	// 把参数的解释权留给插件自己。
	if _, _, err := d.Decode(nil, &gvk, nil); runtime.IsNotRegisteredError(err) {
		return nil
	}

	var strictDecodingErr error
	obj, parsedGvk, err := d.Decode(c.Args.Raw, &gvk, nil)
	if err != nil {
		decodingArgsErr := fmt.Errorf("decoding args for plugin %s: %w", c.Name, err)
		if obj != nil && runtime.IsStrictDecodingError(err) {
			strictDecodingErr = runtime.NewStrictDecodingError([]error{decodingArgsErr})
		} else {
			return decodingArgsErr
		}
	}
	if parsedGvk.GroupKind() != gvk.GroupKind() {
		return fmt.Errorf("args for plugin %s were not of type %s, got %s", c.Name, gvk.GroupKind(), parsedGvk.GroupKind())
	}
	c.Args.Object = obj
	return strictDecodingErr
}

func (c *PluginConfig) encodeNestedObjects(e runtime.Encoder) error {
	if c.Args.Object == nil {
		return nil
	}
	var buf bytes.Buffer
	err := e.Encode(c.Args.Object, &buf)
	if err != nil {
		return err
	}
	// <e> 编码器可能输出 YAML，而上层编码器期望
	// JSON，因此要把 YAML 转回 JSON。
	// This is a no-op if <e> produces JSON.
	// 外层配置按 JSON 结构拼装，内层编码器却是可替换的；这里做一次归一，
	// 免得 RawExtension 里塞进一段 YAML 文本破坏外层输出。
	json, err := yaml.YAMLToJSON(buf.Bytes())
	if err != nil {
		return err
	}
	c.Args.Raw = json
	return nil
}

// Extender 保存与扩展通信所需的参数。若某个 verb 未指定或为空，
// 就认为该扩展没有提供对应能力。
// 每个动词独立配置，调度器靠空字符串判断该扩展点是否存在，这样外部进程可以
// 只实现自己关心的少数几个回调，不必凑齐整套接口。
type Extender struct {
	// URLPrefix 是扩展服务的地址前缀。
	URLPrefix string `json:"urlPrefix"`
	// filter 调用的 verb，不支持时留空；调用时追加到 URLPrefix 后。
	FilterVerb string `json:"filterVerb,omitempty"`
	// preempt 调用的 verb，不支持时留空；调用时追加到 URLPrefix 后。
	PreemptVerb string `json:"preemptVerb,omitempty"`
	// prioritize 调用的 verb，不支持时留空；调用时追加到 URLPrefix 后。
	PrioritizeVerb string `json:"prioritizeVerb,omitempty"`
	// 扩展 prioritize 返回的节点分数的数值乘数。
	// 权重应为正整数。
	Weight int64 `json:"weight,omitempty"`
	// bind 调用的 verb，不支持时留空；调用时追加到 URLPrefix 后。
	// 若扩展实现了该方法，则由它负责把 Pod 绑定到 apiserver，且只允许一个
	// can implement this function.
	BindVerb string `json:"bindVerb,omitempty"`
	// EnableHTTPS 指定与扩展通信是否使用 https。
	EnableHTTPS bool `json:"enableHTTPS,omitempty"`
	// TLSConfig 是传输层安全配置。
	TLSConfig *ExtenderTLSConfig `json:"tlsConfig,omitempty"`
	// HTTPTimeout 是调用扩展的超时时长。filter 超时会使 Pod 调度失败；
	// prioritize 超时则忽略结果，由 k8s/其它扩展的优先级选节点。
	HTTPTimeout metav1.Duration `json:"httpTimeout,omitempty"`
	// NodeCacheCapable 表示扩展能缓存节点信息，
	// 因此调度器只发送候选节点的最少信息，
	// 假定它已缓存集群中所有节点的完整信息。
	NodeCacheCapable bool `json:"nodeCacheCapable,omitempty"`
	// ManagedResources 是该扩展管理的扩展资源列表：
	// - 只有 Pod 请求了列表中至少一种扩展资源，才会在 Filter、Prioritize
	//   和 Bind（若它担任 binder）阶段发给该扩展；列表为空时所有 Pod 都发。
	// - 某资源设置 IgnoredByScheduler 为 true 时，kube-scheduler 会在 predicate 中忽略它。
	// +optional
	// +listType=atomic
	ManagedResources []ExtenderManagedResource `json:"managedResources,omitempty"`
	// Ignorable 表示扩展是否可忽略：出错或不可达时
	// 不应导致调度失败。
	Ignorable bool `json:"ignorable,omitempty"`
}

// ExtenderManagedResource 描述扩展资源的管理参数。
// managed by an extender.
type ExtenderManagedResource struct {
	// Name 是扩展资源的名字。
	Name string `json:"name"`
	// IgnoredByScheduler 表示 kube-scheduler 是否应在 predicate
	// resource when applying predicates.
	IgnoredByScheduler bool `json:"ignoredByScheduler,omitempty"`
}

// ExtenderTLSConfig 包含与扩展启用 TLS 的设置。
type ExtenderTLSConfig struct {
	// 不校验服务端 TLS 证书，仅供测试使用。
	Insecure bool `json:"insecure,omitempty"`
	// ServerName 作为 SNI 传给服务端，并用于客户端校验证书。
	// 为空时则使用连接服务端时的主机名校验证书。
	// server is used.
	ServerName string `json:"serverName,omitempty"`

	// Server requires TLS client certificate authentication
	CertFile string `json:"certFile,omitempty"`
	// Server requires TLS client certificate authentication
	KeyFile string `json:"keyFile,omitempty"`
	// 信任的服务端根证书。
	CAFile string `json:"caFile,omitempty"`

	// CertData 是 PEM 编码的字节，通常来自客户端证书文件。
	// CertData takes precedence over CertFile
	// +listType=atomic
	CertData []byte `json:"certData,omitempty"`
	// KeyData 是 PEM 编码的字节，通常来自客户端证书私钥文件。
	// KeyData takes precedence over KeyFile
	// +listType=atomic
	KeyData []byte `json:"keyData,omitempty"`
	// CAData 是 PEM 编码的字节，通常来自根证书包。
	// CAData takes precedence over CAFile
	// +listType=atomic
	CAData []byte `json:"caData,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// DynamicResourcesArgs 是 DynamicResources 插件的配置参数。
// DRA 的过滤成本随 ResourceClaim 数量和设备复杂度变化，不设上限时一个 Pod 就可能
// 拖住整个调度循环。这两个超时把最坏情况兜住，又允许管理员按实测指标收紧。
type DynamicResourcesArgs struct {
	metav1.TypeMeta `json:""`

	// FilterTimeout 限制每个节点上 filter 操作查找可分配设备
	// 的最长耗时。
	// a pod to that node.
	// 典型情况下该操作耗时 10~200 毫秒，
	// 但也会随 ResourceClaim 的请求数、Claim 数量、
	// ResourceSlice 中发布的设备数量以及
	// 请求复杂程度而增加。除 CEL 求值外，
	// 其它检查（如容量核对）同样耗时。
	// checks, match attributes, etc.).
	// 因此插件施加该超时。超时后该节点上的 Pod
	// 被视为不可调度。
	// 若其它节点过滤成功，则选择那些节点。
	// 若所有节点都失败，Pod 进入不可调度队列；
	// 当 ResourceSlice 或 ResourceClaim 的变化可能让下一次调度成功时会重新检查；
	// 反复失败则会指数退避。默认 10 秒。
	// 这样足以避免最坏情况，又不影响 DRA 的正常使用。
	// 但过滤过慢也会拖慢不使用 DRA 的 Pod。管理员可参考
	// `scheduler_framework_extension_point_duration_seconds` 指标缩短该超时。
	// 设为 0 则完全关闭超时。
	FilterTimeout *metav1.Duration `json:"filterTimeout"`

	// BindingTimeout 限制 PreBind 扩展点等待 ResourceClaim 设备
	// BindingConditions 满足的最长时间。等待期间调度器
	// 定期检查设备状态。若超时前所需条件未全部为真
	// （或出现 bindingFailureConditions），则清除分配，
	// Pod 重新进入调度队列。
	// 若仍可行可能选到同一或其它节点；
	// 否则进入不可调度队列，
	// 按集群变化与退避重试。
	// Defaults & feature gates:
	// - 启用 DRADeviceBindingConditions 时默认 10 分钟。
	// - 仅当 DRADeviceBindingConditions 与 DRAResourceClaimDeviceStatus 同时启用时有效，
	//   否则不要填写该字段。
	// - 禁用 DRADeviceBindingConditions 时填写该字段会报错。
	// Valid values:
	//   - >=1s (non-zero). No upper bound is enforced.
	// Tuning guidance:
	// - 值越小，设备未就绪时重试越快，但驱动就绪较慢时抖动会增加。
	// - 收紧前请结合调度延迟指标（PreBind duration、
	//   `scheduler_framework_extension_point_duration_seconds`）与驱动就绪行为评估。
	BindingTimeout *metav1.Duration `json:"bindingTimeout,omitempty"`
}

// 两个默认值分开声明：过滤是每次调度都要走的热路径，给 10 秒足够兜底；
// 绑定要等设备上报就绪条件，周期长得多，默认放到 10 分钟。
const DynamicResourcesFilterTimeoutDefault = 10 * time.Second
const DynamicResourcesBindingTimeoutDefault = 600 * time.Second
