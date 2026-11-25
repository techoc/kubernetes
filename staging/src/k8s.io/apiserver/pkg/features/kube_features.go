/*
Copyright 2017 The Kubernetes Authors.

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

// Package features 定义 apiserver 使用的特性门控（feature gate）。
//
// 每个门控的演进遵循 Alpha → Beta → GA/Deprecated 的生命周期：
// Alpha 默认关闭、允许删除；Beta 默认开启、承诺兼容；GA 后锁定为默认值；
// Deprecated 表示功能将移除。LockToDefault 一旦置位，用户不能再覆盖默认值。
package features

import (
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/version"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/component-base/featuregate"
	zpagesfeatures "k8s.io/component-base/zpages/features"
)

// 每个特性门控都应在此按照以下模板登记：
//
// // owner: @username
// MyFeature featuregate.Feature = "MyFeature"
//
// 特性门控按区分大小写的字母序排列（大写字符排在小写字符之前），
// 让变更分散在文件各处，降低合并冲突的概率。
//
// 约定：常量名与字符串值必须完全一致，否则 --feature-gates 无法对应；
// 顺序按字母序排列以减少合并冲突。
const (
	// owner: @ilackams
	//
	// 压缩 REST 响应（仅 GET 和 LIST）。
	APIResponseCompression featuregate.Feature = "APIResponseCompression"

	// owner: @roycaihw
	//
	// 为集群中的每个 kube-apiserver 分配标识。
	APIServerIdentity featuregate.Feature = "APIServerIdentity"

	// owner: @pmengelbert
	// kep: https://kep.k8s.io/6060
	//
	// 控制 apiserver 是否签发令牌，供 Kubernetes API Server 与聚合 API Server
	// 向 webhook 证明身份。
	APIServerWebhookAuthenticationToken featuregate.Feature = "APIServerWebhookAuthenticationToken"

	// owner: @linxiulei
	//
	// 在独立 goroutine 中处理 watch 请求。
	APIServingWithRoutine featuregate.Feature = "APIServingWithRoutine"

	// owner: @jefftree
	//
	// 移除 apidiscovery.k8s.io/v2beta1 组版本。聚合发现使用自己的处理器，
	// 其生命周期不同于传统 Kubernetes 资源。
	AggregatedDiscoveryRemoveBetaType featuregate.Feature = "AggregatedDiscoveryRemoveBetaType"

	// owner: @modulitos
	//
	// 允许证书认证根据 x509 证书设置 user.DefaultInfo.UID。
	AllowParsingUserUIDFromCertAuth featuregate.Feature = "AllowParsingUserUIDFromCertAuth"

	// owner: @stlaz @tkashem @dgrisonnet @ibihim
	// kep: https://kep.k8s.io/3926
	//
	// 允许集群管理员识别解密失败或无法解码为对象的资源，并引入新的删除选项，
	// 使其仅通过 Kubernetes API 即可删除这些损坏资源。
	AllowUnsafeMalformedObjectDeletion featuregate.Feature = "AllowUnsafeMalformedObjectDeletion"

	// owner: @benluddy
	// kep: https://kep.k8s.io/4222
	//
	// 支持使用 CBOR 编码请求和响应，并将其作为自定义资源的首选存储编码。
	CBORServingAndStorage featuregate.Feature = "CBORServingAndStorage"

	// owner: @serathius
	// 并发解码 watch 对象，避免安装转换 webhook 后 watch cache 因等待解码而饥饿。
	ConcurrentWatchObjectDecode featuregate.Feature = "ConcurrentWatchObjectDecode"

	// owner: @yedou37
	//
	// 当缓存无法提供一致 LIST 时，跳过回退到存储的超时等待，改为返回可重试响应。
	// See https://github.com/kubernetes/kubernetes/issues/138494.
	ConsistentListFromCacheSkipTimeoutFallback featuregate.Feature = "ConsistentListFromCacheSkipTimeoutFallback"

	// owner: @enj @qiujian16
	// kep: https://kep.k8s.io/5284
	//
	// 传统 impersonation 一旦授权即拥有目标用户的全部权限；该特性把授权范围
	// 收窄到具体请求（动词/资源/名字等），降低误用与越权风险。
	ConstrainedImpersonation featuregate.Feature = "ConstrainedImpersonation"

	// owner: @jefftree
	// kep: https://kep.k8s.io/4355
	//
	// 在 API Server 中启用协调式领导者选举。
	CoordinatedLeaderElection featuregate.Feature = "CoordinatedLeaderElection"

	// owner: @jpbetz @aaron-prindle @yongruilin
	// kep: http://kep.k8s.io/5073
	// beta: v1.33
	//
	// 启用时，将声明式校验结果与手写校验结果比较，并把不一致记录为指标。
	// 声明式校验本身始终执行，不受此门控影响；按 tag 强制执行的策略由
	// 生命周期前缀和 DeclarativeValidationBeta 门控控制。
	DeclarativeValidation featuregate.Feature = "DeclarativeValidation"

	// owner: @jpbetz @aaron-prindle @yongruilin
	// kep: http://kep.k8s.io/5073
	// beta: v1.36
	//
	// 此门控是 Beta 阶段校验规则（+k8s:beta）的全局安全开关。发现回归问题时，
	// 集群管理员可关闭它，让这些规则退回影子模式。
	//
	// 影子模式仍执行声明式校验，并把与手写校验的不一致记录为指标，但失败不再拒绝请求。
	// 手写校验始终具有权威性并强制执行。按生命周期前缀执行的策略如下：
	// - 标准 tag（无前缀）：始终强制执行（不受此门控影响）。
	// - Beta tag（+k8s:beta）：门控开启时强制执行（默认），否则进入影子模式。
	// - Alpha tag（+k8s:alpha）：始终处于影子模式。
	DeclarativeValidationBeta featuregate.Feature = "DeclarativeValidationBeta"

	// owner: @jpbetz @aaron-prindle @yongruilin
	// kep: http://kep.k8s.io/5073
	// beta: v1.33
	//
	// Deprecated: in favor of DeclarativeValidationBeta.
	//
	// 已弃用：由 DeclarativeValidationBeta 取代。开启时声明式校验错误会直接返回给调用方，
	// 关闭时始终返回手写校验错误（即“影子模式”）。注意两种实现的错误数量与文案可能不同。
	//
	DeclarativeValidationTakeover featuregate.Feature = "DeclarativeValidationTakeover"

	// owner: @serathius
	// kep: https://kep.k8s.io/4988
	//
	// 检测 watch cache 与 etcd 之间的不一致，是诊断缓存 bug 的重要手段；
	// 开启会带来额外开销，因此历史上曾以 Alpha 形式提供。
	DetectCacheInconsistency featuregate.Feature = "DetectCacheInconsistency"

	// owner: @jefftree
	//
	// 在 etcd 的列表操作中启用 RangeStream RPC。
	EtcdRangeStream featuregate.Feature = "EtcdRangeStream"

	// owner: @aramase @BenTheElder
	// kep: https://kep.k8s.io/5793
	//
	// 从 ValidatingAdmissionWebhook 和 MutatingAdmissionWebhook 中排除同一组
	// 认证与授权虚拟资源；ValidatingAdmissionPolicy 和 MutatingAdmissionPolicy
	// 已经排除这些资源。
	ExcludeAdmissionWebhookVirtualResources featuregate.Feature = "ExcludeAdmissionWebhookVirtualResources"

	// owner: @aramase
	// kep: https://kep.k8s.io/3299
	// deprecated: v1.28
	//
	// 已弃用：KMS v1 存在安全与可扩展性缺陷，应迁移到 KMS v2。
	KMSv1 featuregate.Feature = "KMSv1"

	// owner: @serathius
	// kep: https://kep.k8s.io/4988
	//
	// 生成 watch cache store 的快照，并使用快照服务 LIST 请求。
	ListFromCacheSnapshot featuregate.Feature = "ListFromCacheSnapshot"

	// owner: @aramase @BenTheElder
	// kep: https://kep.k8s.io/5793
	//
	// 启用基于清单的 webhook 与策略准入控制配置。开启后，准入 webhook 和策略
	// 可在 API Server 启动时从磁盘清单文件加载，从而在启动早期强制执行，
	// 并防止通过 API 修改配置。
	ManifestBasedAdmissionControlConfig featuregate.Feature = "ManifestBasedAdmissionControlConfig"

	// owner: @alexzielenski, @cici37, @jiahuif, @jpbetz
	// kep: https://kep.k8s.io/3962
	//
	// 在准入链中启用 MutatingAdmissionPolicy。
	MutatingAdmissionPolicy featuregate.Feature = "MutatingAdmissionPolicy"

	// owner: @jiahuif
	// kep: https://kep.k8s.io/2887
	//
	// 在 kube-apiserver 返回的 OpenAPI 规范中填充 "enum" 字段。
	OpenAPIEnums featuregate.Feature = "OpenAPIEnums"

	// owner: @stlaz
	//
	// 允许 kube-apiserver 通过请求头认证接受 UID，并让 API 聚合器在向聚合 API
	// 的后端服务转发请求时，通过标准请求头补充 UID。
	RemoteRequestHeaderUID featuregate.Feature = "RemoteRequestHeaderUID"

	// owner: @cici37
	//
	// 允许 watch cache 在专用 RPC 上创建 watch，避免被其他 watch 请求饿死。
	SeparateCacheWatchRPC featuregate.Feature = "SeparateCacheWatchRPC"

	// owner: @jefftree
	// kep: https://kep.k8s.io/5866
	//
	// 为 List/Watch 请求启用 shard selector 参数，使客户端可基于元数据字段
	// （如 UID）的哈希范围接收经过筛选的对象子集。
	ShardedListAndWatch featuregate.Feature = "ShardedListAndWatch"

	// owner: @serathius
	//
	// 让 APF 根据对象大小估算请求成本。
	SizeBasedListCostEstimate featuregate.Feature = "SizeBasedListCostEstimate"

	// owner: @caesarxuchao @roycaihw
	//
	// 启用存储版本 API。
	StorageVersionAPI featuregate.Feature = "StorageVersionAPI"

	// owner: @caesarxuchao
	//
	// 允许 apiserver 在发现文档中公开存储版本哈希。
	StorageVersionHash featuregate.Feature = "StorageVersionHash"

	// owner: @aramase, @enj, @nabokihms
	// kep: https://kep.k8s.io/3331
	//
	// 在结构化认证配置中启用 Egress Selector。
	StructuredAuthenticationConfigurationEgressSelector featuregate.Feature = "StructuredAuthenticationConfigurationEgressSelector"

	// owner: @aramase, @enj, @nabokihms
	// kep: https://kep.k8s.io/3331
	//
	// 为结构化认证配置启用 JWKs 指标。
	StructuredAuthenticationConfigurationJWKSMetrics featuregate.Feature = "StructuredAuthenticationConfigurationJWKSMetrics"

	// owner: @aramase
	//
	// 在 TokenRequest API 中校验服务账号 UID，确保所请求令牌的 UID
	// 与目标服务账号一致，防止 API 被滥用于为错误账号签发令牌。
	TokenRequestServiceAccountUIDValidation featuregate.Feature = "TokenRequestServiceAccountUIDValidation"

	// owner: @enj
	//
	// 默认开启以防御未认证客户端发起 HTTP/2 攻击；位于 L7 负载均衡之后
	// 或私有网络中的集群可评估关闭，以避免连接复用性能回退。
	//
	// 已知可考虑关闭这些防护措施的情形包括：
	//
	// API Server 位于已配置 HTTP/2 攻击防护的 L7 负载均衡之后时，可关闭此保护，
	// 避免未认证客户端迫使负载均衡与 API Server 之间无法复用连接
	// （多个入站连接本可共享同一个后端连接）。
	//
	// API Server 位于私有网络时，也可关闭此保护，避免未认证客户端的性能回退。
	UnauthenticatedHTTP2DOSMitigation featuregate.Feature = "UnauthenticatedHTTP2DOSMitigation"

	// owner: @richabanker
	//
	// 版本偏斜时，将客户端代理到能够处理该请求的 apiserver。
	UnknownVersionInteroperabilityProxy featuregate.Feature = "UnknownVersionInteroperabilityProxy"

	// owner: @wojtek-t
	//
	// 启用存储就绪的启动后钩子（post-start-hook）。
	WatchCacheInitializationPostStartHook featuregate.Feature = "WatchCacheInitializationPostStartHook"

	// owner: @p0lyn0mial
	//
	// 允许 API Server 逐条流式传输对象，而不是分块传输。
	//
	// 开启后 watch 事件逐条流式编码，减少大对象列表带来的内存峰值；
	// 1.33 曾因 json/proto 编码器表现更佳而回退默认值。
	WatchList featuregate.Feature = "WatchList"

	// owner: @p0lyn0mial
	//
	// 为 WatchList 响应启用压缩。
	WatchListCompression featuregate.Feature = "WatchListCompression"

	// owner: @aojea
	// beta: v1.37
	//
	// 使用自定义解析型 RoundTripper，在服务端点间均衡 admission webhook 请求，
	// 不再按服务名称缓存连接。
	WebhookRoundTripLoadBalancing featuregate.Feature = "WebhookRoundTripLoadBalancing"
)

func init() {
	// 把本包的版本化门控注册到全局可变门控表，并合并 zpages 的门控。
	// 必须在 flag 解析前完成，否则 --feature-gates 会因未知门控而报错。
	runtime.Must(utilfeature.DefaultMutableFeatureGate.AddVersioned(defaultVersionedKubernetesFeatureGates))
	runtime.Must(zpagesfeatures.AddFeatureGates(utilfeature.DefaultMutableFeatureGate))
}

// defaultVersionedKubernetesFeatureGates 包含所有 Kubernetes 专属特性键及其版本化规格。
// 新增特性时，需先在上方定义键，再在此登记；这些特性会用于所有 Kubernetes 二进制。
//
// 各条目按字母序排列，并以空行分隔，避免新增或删除条目时产生大范围 gofmt 差异。
//
// 每个门控按版本列出 (Version, Default, PreRelease)：门控的默认值随版本演进，
// 同一二进制在不同 --emulated-version 下会得到不同的默认行为。
var defaultVersionedKubernetesFeatureGates = map[featuregate.Feature]featuregate.VersionedSpecs{
	APIResponseCompression: {
		{Version: version.MustParse("1.8"), Default: false, PreRelease: featuregate.Alpha},
		{Version: version.MustParse("1.16"), Default: true, PreRelease: featuregate.Beta},
	},

	APIServerIdentity: {
		{Version: version.MustParse("1.20"), Default: false, PreRelease: featuregate.Alpha},
		{Version: version.MustParse("1.26"), Default: true, PreRelease: featuregate.Beta},
	},

	APIServerWebhookAuthenticationToken: {
		{Version: version.MustParse("1.37"), Default: false, PreRelease: featuregate.Alpha},
	},

	APIServingWithRoutine: {
		{Version: version.MustParse("1.30"), Default: false, PreRelease: featuregate.Alpha},
	},

	AggregatedDiscoveryRemoveBetaType: {
		{Version: version.MustParse("1.0"), Default: false, PreRelease: featuregate.GA},
		{Version: version.MustParse("1.33"), Default: true, PreRelease: featuregate.Deprecated},
		{Version: version.MustParse("1.35"), Default: true, PreRelease: featuregate.Deprecated, LockToDefault: true},
	},

	AllowParsingUserUIDFromCertAuth: {
		{Version: version.MustParse("1.33"), Default: true, PreRelease: featuregate.Beta},
	},

	AllowUnsafeMalformedObjectDeletion: {
		{Version: version.MustParse("1.32"), Default: false, PreRelease: featuregate.Alpha},
		{Version: version.MustParse("1.37"), Default: true, PreRelease: featuregate.Beta},
	},

	CBORServingAndStorage: {
		{Version: version.MustParse("1.32"), Default: false, PreRelease: featuregate.Alpha},
	},

	ConcurrentWatchObjectDecode: {
		{Version: version.MustParse("1.31"), Default: false, PreRelease: featuregate.Beta},
		{Version: version.MustParse("1.37"), Default: true, PreRelease: featuregate.Beta},
	},

	ConsistentListFromCacheSkipTimeoutFallback: {
		{Version: version.MustParse("1.37"), Default: false, PreRelease: featuregate.Alpha},
	},

	ConstrainedImpersonation: {
		{Version: version.MustParse("1.35"), Default: false, PreRelease: featuregate.Alpha},
		{Version: version.MustParse("1.36"), Default: true, PreRelease: featuregate.Beta},
	},

	CoordinatedLeaderElection: {
		{Version: version.MustParse("1.31"), Default: false, PreRelease: featuregate.Alpha},
		{Version: version.MustParse("1.33"), Default: false, PreRelease: featuregate.Beta},
	},

	DeclarativeValidation: {
		{Version: version.MustParse("1.33"), Default: true, PreRelease: featuregate.Beta},
		{Version: version.MustParse("1.36"), Default: true, PreRelease: featuregate.GA, LockToDefault: true}, // 1.36 进入 GA 并锁定默认值，计划在 1.39 移除。
	},

	DeclarativeValidationBeta: {
		{Version: version.MustParse("1.36"), Default: true, PreRelease: featuregate.Beta},
	},

	DeclarativeValidationTakeover: {
		{Version: version.MustParse("1.33"), Default: false, PreRelease: featuregate.Beta},
		{Version: version.MustParse("1.36"), Default: false, PreRelease: featuregate.Deprecated},
		{Version: version.MustParse("1.37"), Default: false, PreRelease: featuregate.Deprecated, LockToDefault: true},
	},

	DetectCacheInconsistency: {
		{Version: version.MustParse("1.34"), Default: true, PreRelease: featuregate.Beta},
	},

	EtcdRangeStream: {
		{Version: version.MustParse("1.37"), Default: true, PreRelease: featuregate.Beta},
	},

	ExcludeAdmissionWebhookVirtualResources: {
		{Version: version.MustParse("1.37"), Default: true, PreRelease: featuregate.Beta},
	},

	KMSv1: {
		{Version: version.MustParse("1.0"), Default: true, PreRelease: featuregate.GA},
		{Version: version.MustParse("1.28"), Default: true, PreRelease: featuregate.Deprecated},
		{Version: version.MustParse("1.29"), Default: false, PreRelease: featuregate.Deprecated},
	},

	ListFromCacheSnapshot: {
		{Version: version.MustParse("1.33"), Default: false, PreRelease: featuregate.Alpha},
		{Version: version.MustParse("1.34"), Default: true, PreRelease: featuregate.Beta},
	},

	ManifestBasedAdmissionControlConfig: {
		{Version: version.MustParse("1.36"), Default: false, PreRelease: featuregate.Alpha},
		{Version: version.MustParse("1.37"), Default: true, PreRelease: featuregate.Beta},
	},

	MutatingAdmissionPolicy: {
		{Version: version.MustParse("1.32"), Default: false, PreRelease: featuregate.Alpha},
		{Version: version.MustParse("1.34"), Default: false, PreRelease: featuregate.Beta},
		{Version: version.MustParse("1.36"), Default: true, PreRelease: featuregate.GA},
	},

	OpenAPIEnums: {
		{Version: version.MustParse("1.23"), Default: false, PreRelease: featuregate.Alpha},
		{Version: version.MustParse("1.24"), Default: true, PreRelease: featuregate.Beta},
	},

	RemoteRequestHeaderUID: {
		{Version: version.MustParse("1.32"), Default: false, PreRelease: featuregate.Alpha},
		{Version: version.MustParse("1.33"), Default: true, PreRelease: featuregate.Beta},
	},

	SeparateCacheWatchRPC: {
		{Version: version.MustParse("1.28"), Default: true, PreRelease: featuregate.Beta},
		{Version: version.MustParse("1.33"), Default: false, PreRelease: featuregate.Deprecated},
		{Version: version.MustParse("1.36"), Default: false, LockToDefault: true, PreRelease: featuregate.Deprecated},
	},

	ShardedListAndWatch: {
		{Version: version.MustParse("1.36"), Default: false, PreRelease: featuregate.Alpha},
	},

	SizeBasedListCostEstimate: {
		{Version: version.MustParse("1.34"), Default: true, PreRelease: featuregate.Beta},
	},

	StorageVersionAPI: {
		{Version: version.MustParse("1.20"), Default: false, PreRelease: featuregate.Alpha},
	},

	StorageVersionHash: {
		{Version: version.MustParse("1.14"), Default: false, PreRelease: featuregate.Alpha},
		{Version: version.MustParse("1.15"), Default: true, PreRelease: featuregate.Beta},
	},

	StructuredAuthenticationConfigurationEgressSelector: {
		{Version: version.MustParse("1.34"), Default: true, PreRelease: featuregate.Beta},
	},

	StructuredAuthenticationConfigurationJWKSMetrics: {
		{Version: version.MustParse("1.35"), Default: true, PreRelease: featuregate.Beta},
	},

	TokenRequestServiceAccountUIDValidation: {
		{Version: version.MustParse("1.34"), Default: true, PreRelease: featuregate.Beta},
	},

	UnauthenticatedHTTP2DOSMitigation: {
		{Version: version.MustParse("1.25"), Default: false, PreRelease: featuregate.Beta},
		{Version: version.MustParse("1.29"), Default: true, PreRelease: featuregate.Beta},
	},

	UnknownVersionInteroperabilityProxy: {
		{Version: version.MustParse("1.28"), Default: false, PreRelease: featuregate.Alpha},
		{Version: version.MustParse("1.36"), Default: true, PreRelease: featuregate.Beta},
	},

	WatchCacheInitializationPostStartHook: {
		{Version: version.MustParse("1.31"), Default: false, PreRelease: featuregate.Beta},
		{Version: version.MustParse("1.36"), Default: true, PreRelease: featuregate.Beta},
		{Version: version.MustParse("1.37"), Default: true, PreRelease: featuregate.GA, LockToDefault: true},
	},

	WatchList: {
		{Version: version.MustParse("1.27"), Default: false, PreRelease: featuregate.Alpha},
		{Version: version.MustParse("1.32"), Default: true, PreRelease: featuregate.Beta},
		// 1.33 改回 false，因为 json 与 proto 流式编码器的表现似乎更好。
		{Version: version.MustParse("1.33"), Default: false, PreRelease: featuregate.Beta},
		{Version: version.MustParse("1.34"), Default: true, PreRelease: featuregate.Beta},
	},

	WatchListCompression: {
		{Version: version.MustParse("1.37"), Default: true, PreRelease: featuregate.Beta},
	},

	WebhookRoundTripLoadBalancing: {
		{Version: version.MustParse("1.37"), Default: true, PreRelease: featuregate.Beta},
	},
}
