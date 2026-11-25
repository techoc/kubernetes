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

// Aggregated discovery content-type GVK.
var v2GVK = schema.GroupVersionKind{
	Group:   "apidiscovery.k8s.io",
	Version: "v2",
	Kind:    "APIGroupDiscoveryList",
}

// Given a list of APIServices and proxyHandlers for contacting them,
// DiscoveryManager caches a list of discovery documents for each server
//

// 定义成接口是为了把“发现文档聚合”这一职责与具体的 discoveryManager 实现解耦：
// 上层只需知道可以添加/删除 APIService 并启动 worker，无需了解缓存与拉取细节；
// 同时标注的方法线程安全约定说明这些接口可由事件回调与 worker 并发调用。
type DiscoveryAggregationController interface {
	// Adds or Updates an APIService from the Aggregated Discovery Controller's
	// knowledge base
	// Thread-safe
	AddAPIService(apiService *apiregistrationv1.APIService, handler http.Handler)

	// Removes an APIService from the Aggregated Discovery Controller's Knowledge
	// bank
	// Thread-safe
	RemoveAPIService(apiServiceName string)

	// Spawns a worker which waits for added/updated apiservices and updates
	// the unified discovery document by contacting the aggregated api services
	Run(stopCh <-chan struct{}, discoverySyncedCh chan<- struct{})
}

// discoveryManager 把各个聚合 API Server 的发现文档合并成一份统一的视图。
// 关键设计是按 Service（而非 APIService）缓存发现文档：多个 APIService 可能指向同一个
// Service，而该 Service 的 /apis 文档一次就包含所有 group/version；按 Service 缓存能避免
// 重复拉取同一份文档。apiServices 与 cachedResults 用两把独立的锁保护，
// 因为它们读写模式不同，分开加锁能缩短临界区、避免互相阻塞。
type discoveryManager struct {
	// Locks `apiServices`
	servicesLock sync.RWMutex

	// Map from APIService's name (or a unique string for local servers)
	// to information about contacting that API Service
	apiServices map[string]groupVersionInfo

	// Locks cachedResults
	resultsLock sync.RWMutex

	// Map from APIService.Spec.Service to the previously fetched value
	// (Note that many APIServices might use the same APIService.Spec.Service)
	cachedResults map[serviceKey]cachedResult

	// 待刷新的 dirty apiServiceKey 队列。
	// 重要的是：该队列的调和逻辑不能在某个 key 于上次联系服务端之前入队时，
	// 还反复去请求 apiserver。
	// 入队与时间戳配合做去重：即使同一 APIService 被反复标记为 dirty，
	// 只要缓存的拉取时间晚于最后一次标记时间，fetchFreshDiscoveryForService 就会直接复用缓存，
	// 不会对聚合 API Server 发起多余请求。
	dirtyAPIServiceQueue workqueue.TypedRateLimitingInterface[string]

	// Merged handler which stores all known groupversions
	mergedDiscoveryHandler discoveryendpoint.ResourceManager

	// Codecs is the serializer used for decoding aggregated apiserver responses
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

// Human-readable String representation used for logs
func (s serviceKey) String() string {
	return fmt.Sprintf("%v/%v:%v", s.Namespace, s.Name, s.Port)
}

// newServiceKey 从 APIService 的 ServiceReference 构造缓存键。
// Port 是可选字段，缺省时按 API 约定回退到 443；这里不依赖 apiserver 侧常量，
// 以免两处默认值定义漂移导致缓存键与实际连接端口不一致。
func newServiceKey(service apiregistrationv1.ServiceReference) serviceKey {
	// Docs say. Defaults to 443 for compatibility reasons.
	// BETA: Should this be a shared constant to avoid drifting with the
	// implementation?
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
	// Currently cached discovery document for this service
	// Map from group name to version name to
	discovery map[metav1.GroupVersion]apidiscoveryv2.APIVersionDiscovery

	// ETag hash of the cached discoveryDocument
	etag string

	// Guaranteed to be a time less than the time the server responded with the
	// discovery data.
	lastUpdated time.Time
}

// Information about a specific APIService/GroupVersion
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

	// ServiceReference of this GroupVersion. This identifies the Service which
	// describes how to contact the server responsible for this GroupVersion.
	service serviceKey

	// groupPriority describes the priority of the APIService's group for sorting
	groupPriority int

	// groupPriority describes the priority of the APIService version for sorting
	versionPriority int

	// Method for contacting the service
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
	// Register conversion for apidiscovery
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
	// Lookup last cached result for this apiservice's service.
	cached, exists := dm.getCacheEntryForService(info.service)

	// 若条目存在且更新时间晚于给定时间，直接返回。
	// 缓存时间晚于“变脏”时间即可直接返回，无需发请求。
	// 这是去重的关键路径：同一 Service 被多个 APIService 引用时，只有第一个触发拉取，其余复用结果。
	if exists && cached.lastUpdated.After(info.lastMarkedDirty) {
		return &cached, nil
	}

	// If we have a handler to contact the server for this APIService, and
	// the cache entry is too old to use, refresh the cache entry now.
	handler := http.TimeoutHandler(info.handler, 5*time.Second, "request timed out")
	req, err := http.NewRequest("GET", "/apis", nil)
	if err != nil {
		// NewRequest should not fail, but if it does for some reason,
		// log it and continue
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

	// Important that the time recorded in the data's "lastUpdated" is conservatively
	// from BEFORE the request is dispatched so that lastUpdated can be used to
	// de-duplicate requests.
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
			// NewRequest should not fail, but if it does for some reason,
			// log it and continue
			return nil, fmt.Errorf("failed to create http.Request: %v", err)
		}

		// Apply aggregator user to request
		req = req.WithContext(
			request.WithUser(
				req.Context(), &user.DefaultInfo{Name: "system:kube-aggregator"}))

		// req.Header.Add("Accept", runtime.ContentTypeProtobuf)
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

		// Create a discomap with single group-version
		resources, err := endpoints.ConvertGroupVersionIntoToDiscovery(parsed.APIResources)
		if err != nil {
			return nil, err
		}
		klog.V(3).Infof("DiscoveryManager: Successfully downloaded legacy discovery for %s", info.service.String())

		discoMap := map[metav1.GroupVersion]apidiscoveryv2.APIVersionDiscovery{
			// Convert old-style APIGroupList to new information
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
		// apiservice was removed. remove it from merged discovery
		dm.mergedDiscoveryHandler.RemoveGroupVersion(mgv)
		return nil
	}

	// Lookup last cached result for this apiservice's service.
	cached, err := dm.fetchFreshDiscoveryForService(mgv, info)

	var entry apidiscoveryv2.APIVersionDiscovery

	// Extract the APIService's specific resource information from the
	// groupversion
	if cached == nil {
		// There was an error fetching discovery for this APIService, and
		// there is nothing in the cache for this GV.
		//
		// Just use empty GV to mark that GV exists, but no resources.
		// Also mark that it is stale to indicate the fetch failed
		// TODO: Maybe also stick in a status for the version the error?
		entry = apidiscoveryv2.APIVersionDiscovery{
			Version: gv.Version,
		}
	} else {
		// Find our specific groupversion within the discovery document
		entry, exists = cached.discovery[mgv]
		if exists {
			// The stale/fresh entry has our GV, so we can include it in the doc
		} else {
			// Successfully fetched discovery information from the server, but
			// the server did not include this groupversion?
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
	// Return the error so the worker retries with backoff. The stale entry
	// is already added above, so discovery reflects the current state while
	// we wait for the service to become reachable.
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

	// Shutdown the queue since stopCh was signalled
	defer dm.dirtyAPIServiceQueue.ShutDown()

	// 确保 apiregistration.k8s.io 是 discovery 组列表中的第一个 group。
	// 用 BuiltinSource 设置这个优先级，把它与聚合来源的 APIService 区分开；
	// 一个超大优先级值保证它始终显示在最前面，符合客户端的预期顺序。
	dm.mergedDiscoveryHandler.WithSource(discoveryendpoint.BuiltinSource).SetGroupVersionPriority(APIRegistrationGroupVersion, APIRegistrationGroupPriority, 0)

	// Ensure that all APIServices are present before readiness check succeeds
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
						// Re-enqueue after the refresh interval to pick up changes
						// in the aggregated server's discovery document.
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

		// Mark all non-local APIServices as dirty
		for _, info := range dm.apiServices {
			usedServiceKeys.Insert(info.service)
		}
	}()

	// Avoids double lock. It is okay if a service is added/removed between these
	//
	// functions. This is just a cache and that should be infrequent.

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

	// Add or update APIService record and mark it as dirty
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
		// mark dirty if there was actually something deleted
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
