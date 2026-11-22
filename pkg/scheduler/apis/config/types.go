/*
Copyright 2018 The Kubernetes Authors.

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

package config

import (
	"math"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	componentbaseconfig "k8s.io/component-base/config"
)

const (
	// DefaultKubeSchedulerPort 是调度器状态服务的默认端口，
	// 启动时可通过命令行参数覆盖。
	DefaultKubeSchedulerPort = 10259
)

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// KubeSchedulerConfiguration 是调度器的配置对象。
type KubeSchedulerConfiguration struct {
	// TypeMeta 包含 API 版本和 kind。在 kube-scheduler 中，把带版本的
	// KubeSchedulerConfiguration 转换为本内部类型后，会把 APIVersion 设为所转换类型的
	// scheme 组/版本。这在 cmd/kube-scheduler 中有两处完成：(1) 从文件加载配置时，
	// (2) 生成默认配置时。后续根据该字段里的版本类型做决策，例如：(1) 校验时检查是否
	// 使用了已移除的插件，(2) 把配置写回文件，(3) 初始化调度器。
	metav1.TypeMeta

	// Parallelism 定义调度 Pod 时算法的并行度，必须大于 0，默认 16。
	Parallelism int32

	// LeaderElection 定义 leader 选举客户端的配置。
	LeaderElection componentbaseconfig.LeaderElectionConfiguration

	// ClientConnection 指定 proxy server 与 apiserver 通信时使用的 kubeconfig 文件和客户端连接设置。
	ClientConnection componentbaseconfig.ClientConnectionConfiguration

	// DebuggingConfiguration 保存调试相关特性的配置。
	// TODO: 也许要把它改成类似 Debugging componentbaseconfig.DebuggingConfiguration 的子结构。
	componentbaseconfig.DebuggingConfiguration

	// PercentageOfNodesToScore 是一个百分比：一旦找到这么多比例的节点可运行某 Pod，
	// 调度器就停止在集群中继续搜索可行节点，借此提升性能。无论该值是多少，
	// 调度器至少会尝试找到 "minFeasibleNodesToFind" 个可行节点。
	// 例如集群有 500 个节点且该值为 30，找到 150 个可行节点后就停止继续搜索。
	// 值为 0 时按集群规模使用默认百分比（5%--50%）。
	// 该字段会被 profile 级别的 PercentageOfNodesToScore 覆盖。
	PercentageOfNodesToScore *int32

	// PodInitialBackoffSeconds 是不可调度 Pod 的初始退避时长。
	// 若指定则必须大于 0；为 null 时使用默认值（1 秒）。
	PodInitialBackoffSeconds int64

	// PodMaxBackoffSeconds 是不可调度 Pod 的最大退避时长。
	// 若指定则必须大于或等于 podInitialBackoffSeconds；为 null 时使用默认值（10 秒）。
	PodMaxBackoffSeconds int64

	// Profiles 是 kube-scheduler 支持的调度 profile 列表。
	// Pod 可通过设置对应的 scheduler name 选择使用某个 profile；
	// 未指定 scheduler name 的 Pod，若这里存在 "default-scheduler" profile，则使用它。
	Profiles []KubeSchedulerProfile

	// Extenders 是调度扩展器列表，每个元素保存与扩展器通信所需的参数。
	// 这些扩展器被所有调度 profile 共享。
	Extenders []Extender

	// DelayCacheUntilActive 指定何时开始缓存。若为 true 且启用了 leader 选举，
	// 调度器会等到自己成为 leader 后才填充 informer 缓存。这样在等待成为 leader 期间
	// 内存开销更低，但故障切换会更慢。默认 false。
	DelayCacheUntilActive bool
}

// KubeSchedulerProfile 定义一个调度 profile。
type KubeSchedulerProfile struct {
	// SchedulerName 是与该 profile 关联的调度器名称。
	// 若 SchedulerName 与 Pod 的 "spec.schedulerName" 匹配，则该 Pod 使用此 profile 调度。
	SchedulerName string

	// PercentageOfNodesToScore 是一个百分比：一旦找到这么多比例的节点可运行某 Pod，
	// 调度器就停止在集群中继续搜索可行节点，借此提升性能。无论该值是多少，
	// 调度器至少会尝试找到 "minFeasibleNodesToFind" 个可行节点。
	// 例如集群有 500 个节点且该值为 30，找到 150 个可行节点后就停止继续搜索。
	// 值为 0 时按集群规模使用默认百分比（5%--50%）。
	// 该字段会覆盖全局的 PercentageOfNodesToScore；为空时使用全局值。
	PercentageOfNodesToScore *int32

	// Plugins 指定要启用或禁用的插件集合。
	// Enabled 是在默认插件之外额外启用的插件；Disabled 是要禁用的默认插件。
	// 某个扩展点若未指定启用或禁用插件，则使用该扩展点的默认插件（如果有）。
	// 一旦指定了 QueueSort 插件，所有 profile 都必须指定相同的 QueueSort 插件和 PluginConfig。
	Plugins *Plugins

	// PluginConfig 是每个插件的可选自定义参数集合。
	// 省略某个插件的配置参数，等同于使用该插件的默认配置。
	PluginConfig []PluginConfig
}

// Plugins 包含多个扩展点的插件配置。一旦指定，某扩展点的插件列表就是该点唯一启用的插件。
// 若配置中省略某个扩展点，则该扩展点使用默认插件集合。
// Enabled 插件按此处给出的顺序、在默认插件之后调用；若要在默认插件之前调用，
// 必须先禁用默认插件，再在此按期望顺序重新启用。
type Plugins struct {
	// PreEnqueue 是在把 Pod 加入调度队列之前调用的插件列表。
	PreEnqueue PluginSet

	// QueueSort 是在调度队列中对 Pod 排序时调用的插件列表。
	QueueSort PluginSet

	// PreFilter 是在调度框架的 "PreFilter" 扩展点调用的插件列表。
	PreFilter PluginSet

	// Filter 是在过滤掉无法运行 Pod 的节点时调用的插件列表。
	Filter PluginSet

	// PostFilter 是在过滤阶段之后、且没有为 Pod 找到可行节点时才调用的插件列表。
	PostFilter PluginSet

	// PreScore 是在打分之前调用的插件列表。
	PreScore PluginSet

	// Score 是在对通过过滤阶段的节点排序时调用的插件列表。
	Score PluginSet

	// Reserve 是在为 Pod 分配节点后，预留/释放资源时调用的插件列表。
	Reserve PluginSet

	// Permit 是控制 Pod 绑定的插件列表，可以阻止或延迟 Pod 的绑定。
	Permit PluginSet

	// PreBind 是在 Pod 绑定之前调用的插件列表。
	PreBind PluginSet

	// Bind 是在调度框架的 "Bind" 扩展点调用的插件列表。
	// 调度器按顺序调用这些插件，一旦某个返回成功就跳过其余插件。
	Bind PluginSet

	// PostBind 是在 Pod 成功绑定之后调用的插件列表。
	PostBind PluginSet

	// MultiPoint 是简化配置字段，用于在插件所有合法的扩展点上都启用该插件。
	MultiPoint PluginSet

	// PlacementGenerate 是在 PodGroup 调度周期中、为 PodGroup 决定放置方案时调用的插件列表。
	PlacementGenerate PluginSet

	// PlacementScore 是在工作负载调度周期中、对 PodGroup 放置方案排序时调用的插件列表。
	PlacementScore PluginSet

	// PodGroupPostFilter 是在工作负载调度阶段之后、且 PodGroup 无法被调度时才调用的插件列表
	// （相当于单个 Pod 的 PostFilter）。
	PodGroupPostFilter PluginSet
}

// PluginSet 指定某个扩展点启用和禁用的插件。
// 若数组为空、缺失或为 nil，则该扩展点使用默认插件。
type PluginSet struct {
	// Enabled 指定在默认插件之外额外启用的插件。
	// 这些插件在默认插件之后调用，调用顺序与此处一致。
	Enabled []Plugin
	// Disabled 指定要禁用的默认插件。
	// 若要禁用所有默认插件，提供一个只包含 "*" 的数组即可。
	Disabled []Plugin
}

// Plugin 指定插件名称及其适用时的权重。权重仅用于 Score 和 PlacementScore 插件。
type Plugin struct {
	// Name 定义插件名称。
	Name string
	// Weight 定义插件权重，仅用于 Score 和 PlacementScore 插件。
	Weight int32
}

// PluginConfig 指定插件初始化时要传入的参数。
// 在多个扩展点被调用的插件只初始化一次。Args 可以是任意结构，
// 具体如何解析由插件自己决定。
type PluginConfig struct {
	// Name 定义被配置的插件名称。
	Name string
	// Args 定义初始化时传给插件的参数，可以是任意结构。
	Args runtime.Object
}

/*
 * 注意：以下变量和方法有意不纳入 staging 镜像。
 */
const (
	// DefaultPercentageOfNodesToScore 定义默认打分节点比例：找到该比例的可行节点后就不再继续找。
	// 值为 0 表示自适应，即由调度器自行推算一个合适的默认值。
	DefaultPercentageOfNodesToScore = 0

	// MaxCustomPriorityScore 是 UtilizationShapePoint 期望的最大分数。
	MaxCustomPriorityScore int64 = 10

	// MaxTotalScore 是最大总分。
	MaxTotalScore int64 = math.MaxInt64

	// MaxWeight 定义自定义 PriorityPolicy 允许的最大权重值。
	MaxWeight = MaxTotalScore / MaxCustomPriorityScore
)

// Names 返回所有已启用插件的名称列表（去重后）。
func (p *Plugins) Names() []string {
	if p == nil {
		return nil
	}
	extensions := []PluginSet{
		p.PreEnqueue,
		p.PreFilter,
		p.Filter,
		p.PostFilter,
		p.Reserve,
		p.PreScore,
		p.Score,
		p.PreBind,
		p.Bind,
		p.PostBind,
		p.Permit,
		p.QueueSort,
		p.PlacementGenerate,
		p.PlacementScore,
	}
	n := sets.New[string]()
	for _, e := range extensions {
		for _, pg := range e.Enabled {
			n.Insert(pg.Name)
		}
	}
	return sets.List(n)
}

// Extender 保存与扩展器通信所需的参数。若某个动词未指定/为空，
// 就认为扩展器选择不提供该扩展能力。
type Extender struct {
	// URLPrefix 是扩展器的访问地址前缀。
	URLPrefix string
	// FilterVerb 是过滤调用的 HTTP 动词，不支持时为空。
	// 调用过滤接口时该动词会拼在 URLPrefix 之后。
	FilterVerb string
	// PreemptVerb 是抢占调用的动词，不支持时为空。
	// 调用抢占接口时该动词会拼在 URLPrefix 之后。
	PreemptVerb string
	// PrioritizeVerb 是打分调用的动词，不支持时为空。
	// 调用打分接口时该动词会拼在 URLPrefix 之后。
	PrioritizeVerb string
	// Weight 是扩展器打分结果的数值乘数，必须是正整数。
	Weight int64
	// BindVerb 是绑定调用的动词，不支持时为空。
	// 调用绑定接口时该动词会拼在 URLPrefix 之后。
	// 若扩展器实现了绑定，则由它负责把 Pod 绑定到 apiserver；只能有一个扩展器实现该功能。
	BindVerb string
	// EnableHTTPS 指定与扩展器通信时是否使用 https。
	EnableHTTPS bool
	// TLSConfig 指定传输层安全配置。
	TLSConfig *ExtenderTLSConfig
	// HTTPTimeout 指定调用扩展器的超时时长。过滤超时会导致 Pod 调度失败；
	// 打分超时则被忽略，改由 k8s/其他扩展器的打分来选择节点。
	HTTPTimeout metav1.Duration
	// NodeCacheCapable 表示扩展器自身能缓存节点信息，
	// 因此调度器只需发送候选节点的最小信息——假设扩展器已缓存集群中所有节点的完整数据。
	NodeCacheCapable bool
	// ManagedResources 是本扩展器管理的扩展资源列表。
	// - 只有当 Pod 请求了列表中至少一种扩展资源时，才会在 Filter、Prioritize 和 Bind
	//   （若该扩展器负责绑定）阶段把它发给扩展器。列表为空或未指定时，所有 Pod 都会发给它。
	// - 若某资源设置了 IgnoredByScheduler=true，kube-scheduler 在 predicate 检查时会跳过该资源。
	// +optional
	ManagedResources []ExtenderManagedResource
	// Ignorable 指定扩展器是否可忽略：若可忽略，扩展器报错或不可达时调度不会失败。
	Ignorable bool
}

// ExtenderManagedResource 描述扩展器所管理的扩展资源的参数。
type ExtenderManagedResource struct {
	// Name 是扩展资源名称。
	Name string
	// IgnoredByScheduler 表示 kube-scheduler 在应用 predicate 时是否忽略该资源。
	IgnoredByScheduler bool
}

// ExtenderTLSConfig 包含与扩展器启用 TLS 相关的设置。
type ExtenderTLSConfig struct {
	// Insecure 表示访问服务端时不校验证书，仅供测试使用。
	Insecure bool
	// ServerName 通过 SNI 传给服务端，客户端也用它来校验服务端证书。
	// 若为空，则使用连接服务端时的主机名。
	ServerName string

	// CertFile 是服务端要求 TLS 客户端证书认证时使用的证书文件。
	CertFile string
	// KeyFile 是服务端要求 TLS 客户端证书认证时使用的私钥文件。
	KeyFile string
	// CAFile 是校验服务端所用的可信根证书文件。
	CAFile string

	// CertData 保存 PEM 编码的字节（通常来自客户端证书文件）。
	// CertData 优先于 CertFile。
	CertData []byte
	// KeyData 保存 PEM 编码的字节（通常来自客户端私钥文件）。
	// KeyData 优先于 KeyFile。
	KeyData []byte `datapolicy:"security-key"`
	// CAData 保存 PEM 编码的字节（通常来自根证书包）。
	// CAData 优先于 CAFile。
	CAData []byte
}
