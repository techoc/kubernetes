/*
Copyright 2024 The Kubernetes Authors.

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

// Package features 定义 client-go 已知的特性门控及其版本化默认值。
//
// 门控只影响客户端库自身行为，通常通过环境变量在进程启动时配置；
// 这里的常量名同时是环境变量后缀，改名等同于破坏兼容性。
package features

import (
	"k8s.io/apimachinery/pkg/util/version"
)

// Every feature gate should have an entry here following this template:
//
// // owner: @username
// // alpha: v1.4
// MyFeature featuregate.Feature = "MyFeature"
//
// Feature gates should be listed in alphabetical, case-sensitive
// (upper before any lower case character) order. This reduces the risk
// of code conflicts because changes are more likely to be scattered
// across the file.
//
// 每个常量上方的 owner/alpha/beta/GA/kep 注释是元数据，不能删除或改写；
// 新增门控时保持字母序，并在 defaultVersionedKubernetesFeatureGates 中登记。
const (
	// owner: @michaelasp
	// beta: v1.36
	//
	// Allow the client to process events atomically rather than a stream of
	// events for items popped off the FIFO.
	//
	// 开启后 FIFO 以原子事件（ReplacedAll 等）交付，配合事务性 Store 可避免
	// 中间状态被处理；关闭时回落到传统的逐条 Delta 流。
	AtomicFIFO Feature = "AtomicFIFO"

	// owner: @yt2985
	// beta: 1.36
	//
	// If enabled, allows clients to gracefully handle Certificate Authority (CA)
	// rotations without dropping connections or requiring a restart.
	//
	// 客户端会同时信任旧新两组 CA 并在轮换完成后重建连接池，避免滚动更新
	// API Server 证书时出现连接中断。
	ClientsAllowCARotation Feature = "ClientsAllowCARotation"

	// owner: @benluddy
	// kep: https://kep.k8s.io/4222
	// alpha: 1.32
	//
	// If disabled, clients configured to accept "application/cbor" will instead accept
	// "application/json" with the same relative preference, and clients configured to write
	// "application/cbor" or "application/apply-patch+cbor" will instead write
	// "application/json" or "application/apply-patch+yaml", respectively.
	//
	// 关闭时即使服务端支持 CBOR 也退回 JSON，用于排除 CBOR 解析差异导致的问题。
	ClientsAllowCBOR Feature = "ClientsAllowCBOR"

	// owner: @enj
	// beta: v1.36
	//
	// If enabled, the client-go TLS transport cache uses weak pointers to allow
	// garbage collection of unused transports, preventing unbounded cache growth.
	//
	// 缓存键包含 TLS 配置，长期运行的客户端频繁变更配置时若不回收会持续增长。
	ClientsAllowTLSCacheGC Feature = "ClientsAllowTLSCacheGC"

	// owner: @benluddy
	// kep: https://kep.k8s.io/4222
	// alpha: 1.32
	//
	// If enabled, and only if ClientsAllowCBOR is also enabled, the default request content
	// type (if not explicitly configured) and the dynamic client's request content type both
	// become "application/cbor" instead of "application/json". The default content type for
	// apply patch requests becomes "application/apply-patch+cbor" instead of
	// "application/apply-patch+yaml".
	//
	// 仅在 ClientsAllowCBOR 同样开启时生效；偏好 CBOR 是为了减少序列化开销和
	// 传输体积，但服务端必须显式协商支持。
	ClientsPreferCBOR Feature = "ClientsPreferCBOR"

	// owner: @deads2k
	// beta: v1.33
	//
	// Refactor informers to deliver watch stream events in order instead of out of order.
	//
	// 有序交付让 handler 观察到的对象状态与 watch 事件顺序一致，减少控制器
	// 基于乱序旧状态做出错误决策的概率。
	InOrderInformers Feature = "InOrderInformers"

	// owner: @yue9944882
	// beta: v1.35
	//
	// Allow InOrderInformer to process incoming events in batches to expedite the process rate.
	//
	// 批量处理在保证同对象事件顺序的前提下摊薄调度与锁开销，提高吞吐。
	InOrderInformersBatchProcess Feature = "InOrderInformersBatchProcess"

	// owner: @enj, @michaelasp
	// alpha: v1.30
	// GA: v1.35
	//
	// 已 GA 的门控仍保留条目以便从旧版本平滑升级；Store 会记录最后同步的
	// resourceVersion，供需要一致性读的消费者使用。
	InformerResourceVersion Feature = "InformerResourceVersion"

	// owner: @michaelasp
	// beta: v1.36
	//
	// Allow the FIFO to unlock while processing items to allow other goroutines to add items to the queue.
	//
	// 处理期间释放锁可减少生产者阻塞，但需要 FIFO 内部保证同一对象不会被
	// 并发处理。
	UnlockWhileProcessingFIFO Feature = "UnlockWhileProcessingFIFO"

	// owner: @p0lyn0mial
	// beta: v1.30
	//
	// Allow the client to get a stream of individual items instead of chunking from the server.
	//
	// WatchList 用流式初始事件替代分页 List，降低大规模集合首次同步的
	// 内存与延迟，但要求服务端支持相应语义。
	WatchListClient Feature = "WatchListClient"
)

// defaultVersionedKubernetesFeatureGates consists of all known Kubernetes-specific feature keys.
//
// To add a new feature, define a key for it above and add it here.
// After registering with the binary, the features are, by default, controllable using environment variables.
// For more details, please see envVarFeatureGates implementation.
//
// 每个条目按版本列出默认值和成熟度；LockToDefault 表示 GA 后不可再关闭，
// 以避免已删除旧代码路径被重新启用。查找时取不高于当前版本的最后一条。
var defaultVersionedKubernetesFeatureGates = map[Feature]VersionedSpecs{
	AtomicFIFO: {
		{Version: version.MustParse("1.36"), Default: true, PreRelease: Beta},
	},
	ClientsAllowCARotation: {
		{Version: version.MustParse("1.36"), Default: true, PreRelease: Beta},
	},
	ClientsAllowCBOR: {
		{Version: version.MustParse("1.32"), Default: false, PreRelease: Alpha},
	},
	ClientsAllowTLSCacheGC: {
		{Version: version.MustParse("1.36"), Default: true, PreRelease: Beta},
	},
	ClientsPreferCBOR: {
		{Version: version.MustParse("1.32"), Default: false, PreRelease: Alpha},
	},
	InOrderInformers: {
		{Version: version.MustParse("1.33"), Default: true, PreRelease: Beta},
		{Version: version.MustParse("1.36"), Default: true, PreRelease: GA, LockToDefault: true},
	},
	InOrderInformersBatchProcess: {
		{Version: version.MustParse("1.35"), Default: true, PreRelease: Beta},
	},
	InformerResourceVersion: {
		{Version: version.MustParse("1.30"), Default: false, PreRelease: Alpha},
		{Version: version.MustParse("1.35"), Default: true, PreRelease: GA},
	},
	UnlockWhileProcessingFIFO: {
		{Version: version.MustParse("1.36"), Default: true, PreRelease: Beta},
	},
	WatchListClient: {
		{Version: version.MustParse("1.30"), Default: false, PreRelease: Beta},
		{Version: version.MustParse("1.35"), Default: true, PreRelease: Beta},
	},
}
