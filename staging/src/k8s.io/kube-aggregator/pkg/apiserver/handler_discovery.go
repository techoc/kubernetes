/*
Copyright 2016 The Kubernetes Authors.

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

package apiserver

import (
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	apidiscoveryv2 "k8s.io/api/apidiscovery/v2"
	apidiscoveryv2conversion "k8s.io/apiserver/pkg/apis/apidiscovery/v2"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apiserver/pkg/authentication/user"
	"k8s.io/apiserver/pkg/endpoints"
	discoveryendpoint "k8s.io/apiserver/pkg/endpoints/discovery/aggregated"
	"k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/apiserver/pkg/util/responsewriter"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"
	apiregistrationv1 "k8s.io/kube-aggregator/pkg/apis/apiregistration/v1"
	"k8s.io/kube-aggregator/pkg/apis/apiregistration/v1/helper"
	"k8s.io/kube-aggregator/pkg/apiserver/scheme"
)

var APIRegistrationGroupVersion metav1.GroupVersion = metav1.GroupVersion{Group: "apiregistration.k8s.io", Version: "v1"}

// 上限是 20000。这里设得更大，是为了让 apiregistration 组恒定排在 discovery 列表最前，与 v1 discovery 的行为保持一致；
// 其他聚合组的优先级由 APIService 指定，最大只能到 20000。
var APIRegistrationGroupPriority int = 20001

// discoveryRefreshInterval 是 discovery manager 从聚合 API server 重新拉取发现文档以感知变化的间隔；
// 它同时封顶失败拉取的指数退避，保证重试等待从不超过正常刷新周期。
// 定时刷新与失败退避共用同一个时长：成功同步后会按这个间隔重新入队以感知聚合服务的变更；
// 失败重试的退避上限也设为它，保证重试永远不会比正常刷新周期更慢。
const discoveryRefreshInterval = 1 * time.Minute

// 聚合 discovery 内容的 content-type 对应 GVK。
var v2GVK = schema.GroupVersionKind{
	Group:   "apidiscovery.k8s.io",
	Version: "v2",
	Kind:    "APIGroupDiscoveryList",
}

// DiscoveryManager 依据 APIService 列表与对应的 proxyHandler，为每个 server 缓存发现文档。
//

// 定义成接口是为了把“发现文档聚合”这一职责与具体的 discoveryManager 实现解耦：
// 上层只需知道可以添加/删除 APIService 并启动 worker，无需了解缓存与拉取细节；
// 同时标注的方法线程安全约定说明这些接口可由事件回调与 worker 并发调用。
type DiscoveryAggregationController interface {
	// AddAPIService 向聚合发现控制器新增或更新一个 APIService，线程安全。
	AddAPIService(apiService *apiregistrationv1.APIService, handler http.Handler)

	// RemoveAPIService 从聚合发现控制器中移除一个 APIService，线程安全。
	RemoveAPIService(apiServiceName string)

	// Run 启动 worker，等待 APIService 的增删改并联系聚合 API server 更新统一的发现文档。
	Run(stopCh <-chan struct{}, discoverySyncedCh chan<- struct{})
}

// discoveryManager 把各个聚合 API Server 的发现文档合并成一份统一的视图。
// 关键设计是按 Service（而非 APIService）缓存发现文档：多个 APIService 可能指向同一个
// Service，而该 Service 的 /apis 文档一次就包含所有 group/version；按 Service 缓存能避免
// 重复拉取同一份文档。apiServices 与 cachedResults 用两把独立的锁保护，
// 因为它们读写模式不同，分开加锁能缩短临界区、避免互相阻塞。
type discoveryManager struct {
	// 保护 apiServices 的锁。
	servicesLock sync.RWMutex

	// 从 APIService 名字（本地 server 用唯一字符串）到其连接信息的映射。
	apiServices map[string]groupVersionInfo

	// 保护 cachedResults 的锁。
	resultsLock sync.RWMutex

	// 从 APIService.Spec.Service 到上次拉取结果的映射；多个 APIService 可能共用同一个 Service。
	cachedResults map[serviceKey]cachedResult

	// 待刷新的 dirty apiServiceKey 队列。
	// 重要的是：该队列的调和逻辑不能在某个 key 于上次联系服务端之前入队时，
	// 还反复去请求 apiserver。
	// 入队与时间戳配合做去重：即使同一 APIService 被反复标记为 dirty，
	// 只要缓存的拉取时间晚于最后一次标记时间，fetchFreshDiscoveryForService 就会直接复用缓存，
	// 不会对聚合 API Server 发起多余请求。
	dirtyAPIServiceQueue workqueue.TypedRateLimitingInterface[string]

	// 保存所有已知 group/version 的合并 handler。
	mergedDiscoveryHandler discoveryendpoint.ResourceManager

	// Codecs 用于解码聚合 apiserver 的响应。
	codecs serializer.CodecFactory
}

// Service/Spec 中与缓存键相关的字段版本。
// 只取 namespace、name、port 三个会影响“连到哪个服务”的字段作为缓存键。
// 其他字段（如 label、端口名）变化不影响发现文档本身，不纳入键可以避免不必要的缓存失效。
type serviceKey struct {
	Namespace string
	Name      string
	Port      int32
}

// String 返回供日志使用的可读表示。
func (s serviceKey) String() string {
	return fmt.Sprintf("%v/%v:%v", s.Namespace, s.Name, s.Port)
}

// newServiceKey 从 APIService 的 ServiceReference 构造缓存键。
// Port 是可选字段，缺省时按 API 约定回退到 443；这里不依赖 apiserver 侧常量，
// 以免两处默认值定义漂移导致缓存键与实际连接端口不一致。
func newServiceKey(service apiregistrationv1.ServiceReference) serviceKey {
	// 文档规定为兼容起见默认 443。
	// BETA: 是否应抽成共享常量，避免与实现里的默认值漂移？
	port := int32(443)
	if service.Port != nil {
		port = *service.Port
	}

	return serviceKey{
		Name:      service.Name,
		Namespace: service.Namespace,
		Port:      port,
	}
}

// cachedResult 保存某个 Service 最近一次拉取到的发现文档。
// 把原本按 group 分层的文档摊平成以 GroupVersion 为键的 map，
// 是因为合并时需要按 APIService 单独取出某个 group/version，直接按键查比逐层遍历方便。
type cachedResult struct {
	// discovery 是该 Service 最近缓存的发现文档，以 GroupVersion 为键。
	discovery map[metav1.GroupVersion]apidiscoveryv2.APIVersionDiscovery

	// etag 是缓存发现文档的 ETag 哈希。
	etag string

	// lastUpdated 保证早于 server 返回发现数据的时刻。
	lastUpdated time.Time
}

// groupVersionInfo 保存某个 APIService/GroupVersion 的信息。
type groupVersionInfo struct {
	// 该 APIService 被标记为 dirty 的日期。
	// 保证它一定晚于该 APIService 最近一次已知被修改的时间。
	//
	// 用于请求去重，确保调和某个 apiservice 所用的数据是在 APIService 变更之后获取的：
	// real_apiservice_change_time < groupVersionInfo.lastMarkedDirty < cachedResult.lastUpdated < real_document_fresh_time
	//
	// 这保证了：若 apiservice 在上次缓存条目写入之后发生了变更，发现文档一定会被重新拉取。
	// 这组时间戳不等式是去重的核心：只有 lastUpdated 严格晚于 lastMarkedDirty 才认为缓存可用。
	// 取时间必须在发请求之前，才能保证缓存内容一定不早于“变脏”那一刻，避免竞态下使用陈旧数据。
	lastMarkedDirty time.Time

	// service 是该 GroupVersion 对应的 ServiceReference，指明如何访问负责它的 server。
	service serviceKey

	// groupPriority 是该 APIService 所在组在排序中的优先级。
	groupPriority int

	// versionPriority 是该 APIService 版本在排序中的优先级。
	versionPriority int

	// handler 是访问该 Service 的方法。
	handler http.Handler
}

var _ DiscoveryAggregationController = &discoveryManager{}

// NewDiscoveryManager 构造发现文档管理器。
// 这里单独建一个 scheme 并注册 apidiscovery v2 类型及其转换函数，
// 是因为要解码的是聚合发现文档，不能污染 apiserver 主 scheme。
func NewDiscoveryManager(
	target discoveryendpoint.ResourceManager,
) DiscoveryAggregationController {
	discoveryScheme := runtime.NewScheme()
	utilruntime.Must(apidiscoveryv2.AddToScheme(discoveryScheme))
	// 注册 apidiscovery 的转换函数。
	utilruntime.Must(apidiscoveryv2conversion.RegisterConversions(discoveryScheme))
	codecs := serializer.NewCodecFactory(discoveryScheme)

	return &discoveryManager{
		mergedDiscoveryHandler: target,
		apiServices:            make(map[string]groupVersionInfo),
		cachedResults:          make(map[serviceKey]cachedResult),
		dirtyAPIServiceQueue: workqueue.NewTypedRateLimitingQueueWithConfig(
			workqueue.NewTypedItemExponentialFailureRateLimiter[string](500*time.Millisecond, discoveryRefreshInterval),
			workqueue.TypedRateLimitingQueueConfig[string]{Name: "discovery-manager"},
		),
		codecs: codecs,
	}
}

// 返回给定 apiservice 的 discovery 数据，并缓存结果。
// 若结果是在 apiservice 最后一次标记 dirty 之后拉取的，则返回缓存结果。
// 若拉取出错，且存在过期的缓存结果，则返回它并附带一个非 nil 错误。
// 若结果是最新的，返回 nil 错误和非 nil 结果。
// 优先尝试聚合发现（/apis 返回 APIGroupDiscoveryList），失败时按传统 discovery
// （/apis/<group>/<version> 返回 APIResourceList）回退，以兼容尚未提供 v2 文档的聚合服务。
// 注意传统回退只拿到单个 group/version 的数据，因此不会写回共享缓存，
// 否则会用一份不完整文档覆盖本应包含所有 group/version 的缓存条目。
func (dm *discoveryManager) fetchFreshDiscoveryForService(gv metav1.GroupVersion, info groupVersionInfo) (*cachedResult, error) {
	// 取该 APIService 对应 Service 的最新缓存结果。
	cached, exists := dm.getCacheEntryForService(info.service)

	// 若条目存在且更新时间晚于给定时间，直接返回。
	// 缓存时间晚于“变脏”时间即可直接返回，无需发请求。
	// 这是去重的关键路径：同一 Service 被多个 APIService 引用时，只有第一个触发拉取，其余复用结果。
	if exists && cached.lastUpdated.After(info.lastMarkedDirty) {
		return &cached, nil
	}

	// 已有可用 handler 且缓存过旧时，立即刷新缓存。
	handler := http.TimeoutHandler(info.handler, 5*time.Second, "request timed out")
	req, err := http.NewRequest("GET", "/apis", nil)
	if err != nil {
		// NewRequest 理论上不会失败，若失败则记录错误后处理。
		return &cached, fmt.Errorf("failed to create http.Request: %v", err)
	}

	// 为请求应用聚合器身份。
	// 以 system:masters 身份访问聚合服务，保证即使目标服务对外开放了授权检查，
	// 发现文档的拉取也不会因权限不足而失败；聚合层自身是可信组件。
	req = req.WithContext(
		request.WithUser(
			req.Context(), &user.DefaultInfo{Name: "system:kube-aggregator", Groups: []string{"system:masters"}}))
	req = req.WithContext(request.WithRequestInfo(req.Context(), &request.RequestInfo{
		Path:              req.URL.Path,
		IsResourceRequest: false,
	}))
	req.Header.Add("Accept", discovery.AcceptV2)

	if exists && len(cached.etag) > 0 {
		req.Header.Add("If-None-Match", cached.etag)
	}

	// lastUpdated 必须保守地取在发请求之前，才能用它去重，
	// 保证缓存时间不晚于实际响应，避免错过重新拉取。
	now := time.Now()
	writer := responsewriter.NewInMemoryResponseWriter()
	handler.ServeHTTP(writer, req)

	isV2GVK, _ := discovery.ContentTypeIsGVK(writer.Header().Get("Content-Type"), v2GVK)

	switch {
	case writer.RespCode() == http.StatusNotModified:
		// 保留旧条目，只更新时间戳。
		// 服务端用 ETag 判断文档没变化，内容仍是旧的那份，但拉取时间要刷新为 now，
		// 这样后续 apiservice 变更时能正确判断缓存是否需要重新拉取。
		cached = cachedResult{
			discovery:   cached.discovery,
			etag:        cached.etag,
			lastUpdated: now,
		}

		dm.setCacheEntryForService(info.service, cached)
		return &cached, nil
	case writer.RespCode() == http.StatusServiceUnavailable:
		return nil, fmt.Errorf("service %s returned non-success response code: %v",
			info.service.String(), writer.RespCode())
	case writer.RespCode() == http.StatusOK && isV2GVK:
		parsed := &apidiscoveryv2.APIGroupDiscoveryList{}
		if err := runtime.DecodeInto(dm.codecs.UniversalDecoder(), writer.Data(), parsed); err != nil {
			return nil, err
		}

		klog.V(3).Infof("DiscoveryManager: Successfully downloaded discovery for %s", info.service.String())

		// 把 discovery 信息转成 map，便于后续按 GroupVersion 快速查找。
		// 摊平成 map 便于 syncAPIService 按 GroupVersion 直接取出自己那一份。
		discoMap := map[metav1.GroupVersion]apidiscoveryv2.APIVersionDiscovery{}
		for _, g := range parsed.Items {
			for _, v := range g.Versions {
				discoMap[metav1.GroupVersion{Group: g.Name, Version: v.Version}] = v
				for i := range v.Resources {
					// 避免 v0.26.0-v0.26.3 客户端出现 nil panic，详见 https://github.com/kubernetes/kubernetes/issues/118361。
					// 老版本客户端生成的文档里 ResponseKind 可能为 nil，直接下传会让它解引用时空指针。
					// 在这里补成空对象，代价小且能兼容 v0.26.0-v0.26.3 的客户端。
					if v.Resources[i].ResponseKind == nil {
						v.Resources[i].ResponseKind = &metav1.GroupVersionKind{}
					}
					for j := range v.Resources[i].Subresources {
						if v.Resources[i].Subresources[j].ResponseKind == nil {
							v.Resources[i].Subresources[j].ResponseKind = &metav1.GroupVersionKind{}
						}
					}
				}
			}
		}

		// 保存缓存结果。
		// 把 Etag 一并缓存，下次请求带上 If-None-Match；目标服务文档没变时返回 304，
		// 既省去重复解码，也减少对聚合 API Server 的负载。
		cached = cachedResult{
			discovery:   discoMap,
			etag:        writer.Header().Get("Etag"),
			lastUpdated: now,
		}
		dm.setCacheEntryForService(info.service, cached)
		return &cached, nil
	default:
		// 无法为 Aggregated Discovery 获取可接受的响应，回退到传统 discovery 信息。
		// 没有版本号的 group（如本地那些只支持老式 discovery 的）无法构造传统路径，
		// 直接放弃；其余情况下按 /api/<version> 或 /apis/<group>/<version> 回退。
		if len(gv.Version) == 0 {
			return nil, errors.New("not found")
		}

		var path string
		if len(gv.Group) == 0 {
			path = "/api/" + gv.Version
		} else {
			path = "/apis/" + gv.Group + "/" + gv.Version
		}

		req, err := http.NewRequest("GET", path, nil)
		if err != nil {
			// NewRequest 理论上不会失败，若失败则记录错误后继续。
			return nil, fmt.Errorf("failed to create http.Request: %v", err)
		}

		// 给请求加上聚合器身份。
		req = req.WithContext(
			request.WithUser(
				req.Context(), &user.DefaultInfo{Name: "system:kube-aggregator"}))

		// 历史发现接口只接受 JSON，不用 Protobuf。
		req.Header.Add("Accept", runtime.ContentTypeJSON)

		if exists && len(cached.etag) > 0 {
			req.Header.Add("If-None-Match", cached.etag)
		}

		writer := responsewriter.NewInMemoryResponseWriter()
		handler.ServeHTTP(writer, req)

		if writer.RespCode() != http.StatusOK {
			return nil, fmt.Errorf("failed to download legacy discovery for %s: %v", path, writer.String())
		}

		parsed := &metav1.APIResourceList{}
		if err := runtime.DecodeInto(scheme.Codecs.UniversalDecoder(), writer.Data(), parsed); err != nil {
			return nil, err
		}

		// 构造只含单个 group/version 的发现 map。
		resources, err := endpoints.ConvertGroupVersionIntoToDiscovery(parsed.APIResources)
		if err != nil {
			return nil, err
		}
		klog.V(3).Infof("DiscoveryManager: Successfully downloaded legacy discovery for %s", info.service.String())

		discoMap := map[metav1.GroupVersion]apidiscoveryv2.APIVersionDiscovery{
			// 把旧式 APIGroupList 转成新格式的信息。
			gv: {
				Version:   gv.Version,
				Resources: resources,
			},
		}

		cached = cachedResult{
			discovery:   discoMap,
			lastUpdated: now,
		}

		// 不要把这次结果写入缓存：传统回退只拉取一个 group version，
		// 而一个 APIService 可能服务多个 group version。
		// 传统回退只包含一个 group/version，而同一个 Service 往往服务多个版本；
		// 若写回共享缓存，后续其他 APIService 会读到这份残缺文档，所以此处只返回不缓存。
		return &cached, nil
	}
}

// 尝试同步单个 APIService。
// 把一个 APIService 对应的 discovery 信息合并进统一文档。
// 即使拉取失败，也会插入一个标记 stale 的空条目：宁可在 discovery 中暴露一个
// 资源为空、新鲜度为 stale 的 group/version，也不要让整个组凭空消失，
// 这样客户端能感知到“存在但暂时不可用”，而不是误以为该 API 不存在。
func (dm *discoveryManager) syncAPIService(apiServiceName string) error {
	info, exists := dm.getInfoForAPIService(apiServiceName)

	gv := helper.APIServiceNameToGroupVersion(apiServiceName)
	mgv := metav1.GroupVersion{Group: gv.Group, Version: gv.Version}

	if !exists {
		// APIService 已删除，从合并的发现文档中移除。
		dm.mergedDiscoveryHandler.RemoveGroupVersion(mgv)
		return nil
	}

	// 取该 APIService 对应 Service 的最新缓存结果。
	cached, err := dm.fetchFreshDiscoveryForService(mgv, info)

	var entry apidiscoveryv2.APIVersionDiscovery

	// 从 group/version 的发现信息中取出该 APIService 的资源信息。
	if cached == nil {
		// 拉取失败且缓存中没有该 group/version：
		// 放一个空条目表示它存在但暂无资源，并标记为 stale 说明拉取失败。
		// TODO: 是否需要把错误状态也放进版本信息里？
		entry = apidiscoveryv2.APIVersionDiscovery{
			Version: gv.Version,
		}
	} else {
		// 在发现文档中查找本 group/version 对应的条目。
		entry, exists = cached.discovery[mgv]
		if exists {
			// 找到对应条目，直接纳入文档。
		} else {
			// 拉取成功但 server 未返回该 group/version，仍需占位。
			entry = apidiscoveryv2.APIVersionDiscovery{
				Version: gv.Version,
			}
		}
	}

	// 条目是否 stale 取决于 `fetchFreshDiscoveryForService` 是否返回了错误。
	// 用错误是否存在来决定 Freshness：拉取失败时即便有旧缓存可用，也要标记为 stale，
	// 让客户端知道这份数据可能已过时，而不是当作最新内容信任。
	if err == nil {
		entry.Freshness = apidiscoveryv2.DiscoveryFreshnessCurrent
	} else {
		entry.Freshness = apidiscoveryv2.DiscoveryFreshnessStale
	}

	dm.mergedDiscoveryHandler.AddGroupVersion(gv.Group, entry)
	dm.mergedDiscoveryHandler.SetGroupVersionPriority(metav1.GroupVersion(gv), info.groupPriority, info.versionPriority)
	// 返回错误让 worker 退避重试；stale 条目已加入，等待服务恢复期间发现文档仍能反映现状。
	return err
}

func (dm *discoveryManager) getAPIServiceKeys() []string {
	dm.servicesLock.RLock()
	defer dm.servicesLock.RUnlock()
	keys := []string{}
	for key := range dm.apiServices {
		keys = append(keys, key)
	}
	return keys
}

// 启动一个 goroutine，等待 apiservice 的添加/更新事件并相应更新 discovery 文档。
// 启动时先把已知的 APIService 全部同步一遍并关闭 discoverySyncedCh，
// 确保 readiness 检查通过时统一文档已完整；之后才启动 worker 处理后续变更。
func (dm *discoveryManager) Run(stopCh <-chan struct{}, discoverySyncedCh chan<- struct{}) {
	klog.Info("Starting ResourceDiscoveryManager")

	// stopCh 已关闭，关闭队列。
	defer dm.dirtyAPIServiceQueue.ShutDown()

	// 确保 apiregistration.k8s.io 是 discovery 组列表中的第一个 group。
	// 用 BuiltinSource 设置这个优先级，把它与聚合来源的 APIService 区分开；
	// 一个超大优先级值保证它始终显示在最前面，符合客户端的预期顺序。
	dm.mergedDiscoveryHandler.WithSource(discoveryendpoint.BuiltinSource).SetGroupVersionPriority(APIRegistrationGroupVersion, APIRegistrationGroupPriority, 0)

	// 就绪检查通过前，必须先同步所有 APIService。
	var wg sync.WaitGroup
	// 在 key 的副本上迭代，与 syncAPIService 保持线程安全。
	// 先复制 key 快照，避免遍历 map 的同时 syncAPIService 修改 apiServices 导致并发问题。
	keys := dm.getAPIServiceKeys()

	for _, key := range keys {
		wg.Add(1)
		go func(k string) {
			defer wg.Done()
			// 即使返回了错误，该 APIService 也已被添加、只是被标记为 stale。这里忽略返回值。
			// 启动阶段并发同步各个 APIService 以缩短就绪等待时间；
			// 个别失败不影响整体就绪，stale 条目后续由 worker 继续重试。
			_ = dm.syncAPIService(k)
		}(key)
	}
	wg.Wait()

	if discoverySyncedCh != nil {
		close(discoverySyncedCh)
	}

	// 启动 worker。
	// 这些 worker 等待 APIService 被标记为 dirty。
	// worker 确保 ServiceReference 上托管的缓存 discovery 文档至少与 APIService 一样新，
	// 然后把该 APIService 的 groupversion 并入合并后的文档。
	// 用两个 worker 并发处理不同 key：同一个 key 由 workqueue 保证不会并发处理，
	// 因此无需担心对同一 APIService 的并发拉取，又能提高多个 APIService 同时变更时的吞吐。
	for i := 0; i < 2; i++ {
		go func() {
			for {
				next, shutdown := dm.dirtyAPIServiceQueue.Get()
				if shutdown {
					return
				}

				func() {
					defer dm.dirtyAPIServiceQueue.Done(next)

					// 失败时按退避重新入队；成功时除清掉失败计数外，
					// 还要 markDirty 并延迟一个刷新周期后再次入队，
					// 这样即使聚合服务的文档本身变了、没有 APIService 事件，也能被感知到。
					if err := dm.syncAPIService(next); err != nil {
						dm.dirtyAPIServiceQueue.AddRateLimited(next)
					} else {
						dm.dirtyAPIServiceQueue.Forget(next)
						// 按刷新周期重新入队，感知聚合 server 发现文档的变化。
						dm.markDirty(next)
						dm.dirtyAPIServiceQueue.AddAfter(next, discoveryRefreshInterval)
					}
				}()
			}
		}()
	}

	<-stopCh
}

// 对当前所有已知 APIService 使用的 service 做一次快照，
// 并清掉不在快照中的缓存条目。
// 删除 APIService 或修改其 Service 引用后调用，回收不再被任何 APIService 使用的缓存条目，
// 防止长期运行下缓存无限膨胀。先快照再清理是为了避免同时持有两把锁造成死锁。
func (dm *discoveryManager) removeUnusedServices() {
	usedServiceKeys := sets.Set[serviceKey]{}

	func() {
		dm.servicesLock.Lock()
		defer dm.servicesLock.Unlock()

		// 收集所有 APIService 使用的 service（包括本地的）。
		for _, info := range dm.apiServices {
			usedServiceKeys.Insert(info.service)
		}
	}()

	// 分两段加锁、中间不持锁，避免死锁；快照与清理之间若有变化也无害，
	// 最坏多留或早删一条缓存，下次拉取会重建。

	// 分两段加锁、中间不持锁，避免死锁；快照与清理之间若有 APIService 变化也无害：
	// 最坏情况是多留或早删一条缓存，下次拉取时会按需重建。
	func() {
		dm.resultsLock.Lock()
		defer dm.resultsLock.Unlock()

		for key := range dm.cachedResults {
			if !usedServiceKeys.Has(key) {
				delete(dm.cachedResults, key)
			}
		}
	}()
}

// 让 discovery manager 跟踪一个 APIService（即使它已经已知）。
// 注册或更新一个 APIService 的信息，并把它标记为 dirty 以触发一次同步。
// 用 time.Now 作为 lastMarkedDirty，保证随后拉取到的文档时间晚于这次变更，
// 从而一定会被重新拉取而不是命中旧缓存。
func (dm *discoveryManager) AddAPIService(apiService *apiregistrationv1.APIService, handler http.Handler) {
	// 若 Service 为 nil，其信息包含在一个本地 APIService 中，
	// 而本地 APIService 早已被添加到 manager。
	// 本地 APIService 由聚合层自己提供，不需要联系远端 Service，
	// 其信息已由内置来源初始化，因此直接跳过。
	if apiService.Spec.Service == nil {
		return
	}

	// 新增或更新 APIService 记录，并标为 dirty 触发同步。
	dm.setInfoForAPIService(apiService.Name, &groupVersionInfo{
		groupPriority:   int(apiService.Spec.GroupPriorityMinimum),
		versionPriority: int(apiService.Spec.VersionPriority),
		handler:         handler,
		lastMarkedDirty: time.Now(),
		service:         newServiceKey(*apiService.Spec.Service),
	})
	dm.removeUnusedServices()
	dm.dirtyAPIServiceQueue.Add(apiService.Name)
}

// RemoveAPIService 移除对某个 APIService 的跟踪。
// 只有确实删除了记录才需要清理缓存并入队；若本来就不存在，
// 说明是重复删除事件，不必产生额外的同步工作。
func (dm *discoveryManager) RemoveAPIService(apiServiceName string) {
	if dm.setInfoForAPIService(apiServiceName, nil) != nil {
		// 只有确实删除了记录时才标脏、清理缓存并入队。
		dm.removeUnusedServices()
		dm.dirtyAPIServiceQueue.Add(apiServiceName)
	}
}

//
// 受锁保护的访问器
// 以下访问器把加锁细节收敛在一处，调用方无需关心 servicesLock / resultsLock 的存在。
// 每个 accessor 都只做一件事，便于组合出“先快照、再处理”的无死锁流程。
//

func (dm *discoveryManager) getCacheEntryForService(key serviceKey) (cachedResult, bool) {
	dm.resultsLock.RLock()
	defer dm.resultsLock.RUnlock()

	result, ok := dm.cachedResults[key]
	return result, ok
}

func (dm *discoveryManager) setCacheEntryForService(key serviceKey, result cachedResult) {
	dm.resultsLock.Lock()
	defer dm.resultsLock.Unlock()

	dm.cachedResults[key] = result
}

func (dm *discoveryManager) getInfoForAPIService(name string) (groupVersionInfo, bool) {
	dm.servicesLock.RLock()
	defer dm.servicesLock.RUnlock()

	result, ok := dm.apiServices[name]
	return result, ok
}

// markDirty 把 APIService 的最近变更时间推进到当前时刻。
// 成功同步后调用它，是为了配合定时重新入队：届时必须重新拉取一次文档，
// 否则缓存时间晚于旧的 dirty 时间，会被误判为仍然新鲜。
func (dm *discoveryManager) markDirty(name string) {
	dm.servicesLock.Lock()
	defer dm.servicesLock.Unlock()
	if info, exists := dm.apiServices[name]; exists {
		info.lastMarkedDirty = time.Now()
		dm.apiServices[name] = info
	}
}

// setInfoForAPIService 统一处理 APIService 信息的写入与删除。
// 返回旧值是让调用方（RemoveAPIService）能判断“是否真的删掉了东西”，
// 从而避免对不存在的对象做无意义的缓存清理和入队。
func (dm *discoveryManager) setInfoForAPIService(name string, result *groupVersionInfo) (oldValueIfExisted *groupVersionInfo) {
	dm.servicesLock.Lock()
	defer dm.servicesLock.Unlock()

	if oldValue, exists := dm.apiServices[name]; exists {
		oldValueIfExisted = &oldValue
	}

	if result != nil {
		dm.apiServices[name] = *result
	} else {
		delete(dm.apiServices, name)
	}

	return oldValueIfExisted
}
