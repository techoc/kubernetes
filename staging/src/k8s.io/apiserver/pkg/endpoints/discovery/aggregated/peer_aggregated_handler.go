/*
Copyright 2025 The Kubernetes Authors.

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

package aggregated

import (
	"net/http"
	"reflect"
	"sort"
	"sync/atomic"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/apimachinery/pkg/runtime/serializer/cbor"
	"k8s.io/apiserver/pkg/endpoints/metrics"
	"k8s.io/apiserver/pkg/endpoints/request"
	genericfeatures "k8s.io/apiserver/pkg/features"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/klog/v2"

	apidiscoveryv2 "k8s.io/api/apidiscovery/v2"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	utilsort "k8s.io/apimachinery/pkg/util/sort"
)

// PeerDiscoveryProvider 定义获取对端资源以进行对等聚合发现的接口。
// 实现对端数据来源可以是对等 apiserver 的聚合发现端点。
type PeerDiscoveryProvider interface {
	GetPeerResources() map[string][]apidiscoveryv2.APIGroupDiscovery
}

// PeerAggregatedResourceManager 定义管理对等聚合发现资源的接口，
// 该资源同时包含本地服务器与对端服务器的数据。
type PeerAggregatedResourceManager interface {
	// InvalidateCache 失效对等聚合发现缓存。
	// 对端发现数据发生变化时应调用。
	InvalidateCache()

	// ServeHTTP 处理对等聚合发现的 HTTP 请求。
	http.Handler
}

// peerAggregatedDiscoveryHandler 把本地与对端的发现文档合并后按需缓存。
// 缓存指针用 atomic 发布，使并发读无需加锁；写路径只做整体替换，不原地修改。
type peerAggregatedDiscoveryHandler struct {
	serverID              string
	localResourceManager  ResourceManager
	peerDiscoveryProvider PeerDiscoveryProvider
	serializer            runtime.NegotiatedSerializer
	cache                 atomic.Pointer[cachedGroupList]
	serveHTTPFunc         func(http.ResponseWriter, *http.Request)
}

// NewPeerAggregatedDiscoveryHandler 创建一个新的对等聚合发现 handler。
func NewPeerAggregatedDiscoveryHandler(serverID string, localDiscoveryProvider ResourceManager, peerDiscoveryProvider PeerDiscoveryProvider, path string) PeerAggregatedResourceManager {
	scheme := runtime.NewScheme()
	utilruntime.Must(apidiscoveryv2.AddToScheme(scheme))
	var opts []serializer.CodecFactoryOptionsMutator
	// CBOR 仅在特性门控开启时注册，保证与客户端协商能力一致。
	if utilfeature.DefaultFeatureGate.Enabled(genericfeatures.CBORServingAndStorage) {
		opts = append(opts, serializer.WithSerializer(cbor.NewSerializerInfo))
	}
	codecs := serializer.NewCodecFactory(scheme, opts...)

	registerPeerMetrics()

	h := &peerAggregatedDiscoveryHandler{
		serverID:              serverID,
		localResourceManager:  localDiscoveryProvider,
		peerDiscoveryProvider: peerDiscoveryProvider,
		serializer:            codecs,
	}

	// 本地资源变化时必须让合并结果失效，否则会对外提供陈旧视图。
	h.localResourceManager.AddInvalidationCallback(func() {
		h.InvalidateCache()
	})

	// serveHTTP 的指标包装
	// 指标包在合并逻辑外层，统计的是端点整体耗时。
	h.serveHTTPFunc = metrics.InstrumentHandlerFunc(request.MethodGet,
		"", "", "", path, "", metrics.APIServerComponent, false, "",
		h.serveHTTP)

	return h
}

// InvalidateCache 失效对等聚合发现缓存。
// 对端发现数据发生变化时应调用。
func (h *peerAggregatedDiscoveryHandler) InvalidateCache() {
	h.cache.Store(nil)
	klog.V(4).Info("Invalidated peer-aggregated discovery caches")
}

// ServeHTTP 走带指标的包装函数。
func (h *peerAggregatedDiscoveryHandler) ServeHTTP(resp http.ResponseWriter, req *http.Request) {
	h.serveHTTPFunc(resp, req)
}

// serveHTTP 取缓存的合并结果并写出；ETag 复用缓存中的值以支持条件请求。
func (h *peerAggregatedDiscoveryHandler) serveHTTP(resp http.ResponseWriter, req *http.Request) {
	cache := h.fetchFromCache()
	response := cache.cachedResponse
	etag := cache.cachedResponseETag

	writeDiscoveryResponse(&response, etag, h.serializer, resp, req)
}

// fetchFromCache 返回缓存的合并结果；未命中时重建并写回。
// 并发未命中时允许多次重复计算，最后一次 Store 胜出，结果本身是幂等的。
func (h *peerAggregatedDiscoveryHandler) fetchFromCache() *cachedGroupList {
	cacheLoad := h.cache.Load()
	if cacheLoad != nil {
		PeerAggregatedCacheHitsCounter.Inc()
		return cacheLoad
	}

	PeerAggregatedCacheMissesCounter.Inc()

	// 获取本地组
	// 仅在本地管理器支持内部接口时才能直接读其缓存；否则退化为空集合。
	var localGroups []apidiscoveryv2.APIGroupDiscovery
	if rdm, ok := h.localResourceManager.(resourceManager); ok {
		localCache := rdm.fetchFromCache()
		if localCache != nil {
			localGroups = localCache.cachedResponse.Items
		}
	}

	// 若配置了 provider，则获取对端资源
	// 未配置对端来源时只暴露本地资源，便于单机部署。
	var mergedGroups []apidiscoveryv2.APIGroupDiscovery
	if h.peerDiscoveryProvider != nil {
		peerResources := h.peerDiscoveryProvider.GetPeerResources()
		mergedGroups = h.mergeResources(localGroups, peerResources)
	} else {
		mergedGroups = localGroups
	}

	response := apidiscoveryv2.APIGroupDiscoveryList{
		Items: mergedGroups,
	}
	etag, err := calculateETag(response)
	if err != nil {
		// ETag 计算失败不应阻断发现请求，只是失去条件请求能力。
		klog.Errorf("failed to calculate etag for discovery document: %v", err)
		etag = ""
	}

	cached := &cachedGroupList{
		cachedResponse:     response,
		cachedResponseETag: etag,
	}
	h.cache.Store(cached)
	return cached
}

// mergeResources 合并本地与对端的 APIGroupDiscovery 列表，尽可能保留相对顺序。
// localGroups 是本地服务器的 APIGroupDiscovery 对象列表。
// peerGroupDiscovery 是对端服务器 ID 到其 APIGroupDiscovery 列表的映射。
//
// 顺序稳定性是对外契约的一部分：客户端可能按顺序渲染 kubectl api-resources，
// 因此这里用拓扑式合并（MergePreservingRelativeOrder）而非简单排序。
func (h *peerAggregatedDiscoveryHandler) mergeResources(
	localGroups []apidiscoveryv2.APIGroupDiscovery,
	peerGroupDiscovery map[string][]apidiscoveryv2.APIGroupDiscovery,
) []apidiscoveryv2.APIGroupDiscovery {
	discoveryByServerID := h.combineServerDiscoveryLists(localGroups, peerGroupDiscovery)
	if len(discoveryByServerID) <= 1 {
		// 无需合并也无需排序。
		return localGroups
	}

	mergedGroupMap := make(map[string]apidiscoveryv2.APIGroupDiscovery)
	allGroupNames := make([][]string, 0, len(discoveryByServerID))
	// 按 serverID 排序遍历，保证相同输入得到相同的合并结果（确定性 ETag）。
	sortedServerIDs := sortServerIDs(discoveryByServerID)

	// contentChanged 记录是否有对端新增了组/版本/资源。
	contentChanged := false
	// groupNameListsAreIdentical 记录所有对端的组名列表是否与本地完全一致。
	groupNameListsAreIdentical := true
	localGroupNames := localGroupNames(localGroups)
	for _, serverID := range sortedServerIDs {
		discoveryGroups := discoveryByServerID[serverID]
		groupNames, didThisServerMerge := h.processServerGroups(discoveryGroups, mergedGroupMap)
		if didThisServerMerge {
			contentChanged = true
		}
		allGroupNames = append(allGroupNames, groupNames)
		// 检查该服务器的组顺序是否与本地不同。
		// 1. 可以发现简单的重排（例如 ["g1", "g2"] 与 ["g2", "g1"]）。
		// 2. 可以发现新增或删除的组（例如 ["g1"] 与 ["g1", "g2"]）。
		// 这两种情况下列表都不再一致，必须继续走完整合并，
		// 以确保结果确定。
		if !reflect.DeepEqual(localGroupNames, groupNames) {
			groupNameListsAreIdentical = false
		}
	}

	// 情形 1：完全短路。
	// 什么都没变（既无新内容，也无顺序变化）。
	// 完全无变化时直接返回本地切片，避免分配与排序。
	if !contentChanged && groupNameListsAreIdentical {
		return localGroups
	}

	var finalGroupOrder []string
	// 情形 2：内容有变，但组顺序没变。
	// 必须返回新内容，但可以跳过开销较大的排序。
	if contentChanged && groupNameListsAreIdentical {
		finalGroupOrder = localGroupNames
	} else {
		// 情形 3：组列表不同（新增组、顺序不同，或两者皆有）。
		// 只能执行完整的拓扑排序。
		finalGroupOrder = utilsort.MergePreservingRelativeOrder(allGroupNames)
	}

	return assembleGroups(mergedGroupMap, finalGroupOrder)
}

// processServerGroups 把单个服务器的组并入 mergedGroupMap，并返回该服务器的组名顺序。
// contentMerged 表示本次合并是否真正改变了已有组的内容。
func (h *peerAggregatedDiscoveryHandler) processServerGroups(
	discoveryGroups []apidiscoveryv2.APIGroupDiscovery,
	mergedGroupMap map[string]apidiscoveryv2.APIGroupDiscovery,
) (groupNames []string, contentMerged bool) {
	groupNames = make([]string, 0, len(discoveryGroups))
	contentMerged = false

	for _, group := range discoveryGroups {
		if existing, ok := mergedGroupMap[group.Name]; ok {
			// 合并版本与资源。
			mergedG, didMerge := mergeVersionsAcrossGroup(existing, group)
			mergedGroupMap[group.Name] = mergedG
			if didMerge {
				contentMerged = true
			}
		} else {
			// 首次出现的组直接放入，后续服务器若提供同名组再合并。
			mergedGroupMap[group.Name] = group
		}
		groupNames = append(groupNames, group.Name)
	}

	return groupNames, contentMerged
}

// mergeVersionsAcrossGroup 合并两个 APIGroupDiscovery 对象。
// 返回合并后的新对象，以及一个表示结果是否与 a 不同的布尔值。
// 只要新增了版本/资源，或者顺序不同，结果就可能不同。
func mergeVersionsAcrossGroup(a, b apidiscoveryv2.APIGroupDiscovery) (apidiscoveryv2.APIGroupDiscovery, bool) {
	versionOrder := [][]string{}
	mergedVersionMap := make(map[string]apidiscoveryv2.APIVersionDiscovery)

	aVersionNames := make([]string, 0, len(a.Versions))
	bVersionNames := make([]string, 0, len(b.Versions))
	// 该标志记录 'b' 是否修改了已有的版本。
	contentMerged := false

	for _, v := range a.Versions {
		aVersionNames = append(aVersionNames, v.Version)
		mergedVersionMap[v.Version] = v
	}
	versionOrder = append(versionOrder, aVersionNames)

	for _, v := range b.Versions {
		bVersionNames = append(bVersionNames, v.Version)
		if existing, ok := mergedVersionMap[v.Version]; !ok {
			mergedVersionMap[v.Version] = v
		} else {
			// 双方都有该版本，必须合并它们的资源
			mergedV, didMerge := mergeResourcesAcrossVersion(existing, v)
			mergedVersionMap[v.Version] = mergedV
			if didMerge {
				contentMerged = true
			}
		}
	}
	versionOrder = append(versionOrder, bVersionNames)

	// 情形 1：完全短路。
	// 没有新增版本/资源，且顺序相同。
	versionNamesAreIdentical := reflect.DeepEqual(aVersionNames, bVersionNames)
	if !contentMerged && versionNamesAreIdentical {
		return a, false
	}

	var finalVersionOrder []string
	if versionNamesAreIdentical {
		// 情形 2：内容有变，但版本顺序没变。
		// 可以跳过开销较大的排序，直接使用已知顺序。
		finalVersionOrder = aVersionNames
	} else {
		// 情形 3：版本列表不同（新增版本、顺序不同，或两者皆有）。
		// 必须执行拓扑排序。
		finalVersionOrder = utilsort.MergePreservingRelativeOrder(versionOrder)
	}

	mergedVersions := orderedAPIVersionList(mergedVersionMap, finalVersionOrder)
	return apidiscoveryv2.APIGroupDiscovery{
		// 深拷贝 ObjectMeta，避免与调用方共享底层 map/slice。
		ObjectMeta: *a.ObjectMeta.DeepCopy(),
		TypeMeta:   a.TypeMeta,
		Versions:   mergedVersions,
	}, true
}

// mergeResourcesAcrossVersion 合并两个 APIVersionDiscovery 对象。
// 返回合并后的新对象，以及一个表示结果是否与 a 不同的布尔值。
// 只要新增了资源，或者顺序不同，结果就可能不同。
func mergeResourcesAcrossVersion(a, b apidiscoveryv2.APIVersionDiscovery) (apidiscoveryv2.APIVersionDiscovery, bool) {
	resourceOrder := [][]string{}
	mergedResourceMap := make(map[string]apidiscoveryv2.APIResourceDiscovery)

	aResourceNames := make([]string, 0, len(a.Resources))
	bResourceNames := make([]string, 0, len(b.Resources))
	// 该标志记录 'b' 是否修改了已有的资源。
	contentChanged := false

	for _, r := range a.Resources {
		aResourceNames = append(aResourceNames, r.Resource)
		mergedResourceMap[r.Resource] = r
	}
	resourceOrder = append(resourceOrder, aResourceNames)

	for _, r := range b.Resources {
		bResourceNames = append(bResourceNames, r.Resource)
		if existing, ok := mergedResourceMap[r.Resource]; !ok {
			mergedResourceMap[r.Resource] = r
		} else {
			// 完全相同则跳过，避免把“无差异”误判为内容变化。
			if reflect.DeepEqual(existing, r) {
				continue
			}
			contentChanged = true
			mergedR := mergeResourceCapabilities(existing, r)
			mergedResourceMap[r.Resource] = mergedR
		}
	}
	resourceOrder = append(resourceOrder, bResourceNames)

	// 情形 1：完全短路。
	// 'b' 没有新增资源，且资源顺序相同。
	resourceNameListIsIdentical := reflect.DeepEqual(aResourceNames, bResourceNames)
	if !contentChanged && resourceNameListIsIdentical {
		return a, false
	}

	var finalResourceOrder []string
	if resourceNameListIsIdentical {
		// 情形 2：内容有变，但资源顺序没变。
		// 可以跳过开销较大的排序，直接使用已知顺序。
		finalResourceOrder = aResourceNames
	} else {
		// 情形 3：资源列表不同（新增资源、顺序不同，或两者皆有）。
		// 必须执行拓扑排序。
		finalResourceOrder = utilsort.MergePreservingRelativeOrder(resourceOrder)
	}

	mergedResources := orderedAPIResourceList(mergedResourceMap, finalResourceOrder)
	return apidiscoveryv2.APIVersionDiscovery{
		Version:   a.Version,
		Resources: mergedResources,
		Freshness: a.Freshness,
	}, true
}

// mergeResourceCapabilities 接收两个资源，返回一个包含二者 verbs 与子资源并集的新对象。
// 结果切片会排序，以保证合并是确定性的。
func mergeResourceCapabilities(a, b apidiscoveryv2.APIResourceDiscovery) apidiscoveryv2.APIResourceDiscovery {
	// 1. 合并 Verbs
	verbSet := make(map[string]struct{})
	for _, v := range a.Verbs {
		verbSet[v] = struct{}{}
	}
	for _, v := range b.Verbs {
		verbSet[v] = struct{}{}
	}

	var newVerbs []string
	if len(verbSet) > 0 {
		newVerbs = make([]string, 0, len(verbSet))
		for v := range verbSet {
			newVerbs = append(newVerbs, v)
		}
		// 排序以获得确定的 ETag
		sort.Strings(newVerbs)
	}

	// 2. 合并 Subresources
	// 子资源按名字去重；同名时后者覆盖前者（对端数据更新视为更权威）。
	subresourceSet := make(map[string]apidiscoveryv2.APISubresourceDiscovery)
	for _, s := range a.Subresources {
		subresourceSet[s.Subresource] = s
	}
	for _, s := range b.Subresources {
		subresourceSet[s.Subresource] = s
	}

	var newSubresources []apidiscoveryv2.APISubresourceDiscovery
	if len(subresourceSet) > 0 {
		newSubresources = make([]apidiscoveryv2.APISubresourceDiscovery, 0, len(subresourceSet))
		for _, s := range subresourceSet {
			newSubresources = append(newSubresources, s)
		}
		// 排序以获得确定的 ETag
		sort.Slice(newSubresources, func(i, j int) bool {
			return newSubresources[i].Subresource < newSubresources[j].Subresource
		})
	}

	// 3. 返回合并后的新对象
	// 优先沿用 'a' 的元数据（如 ResponseKind），只有 'a' 为空时才用 'b' 的。
	mergedResource := a
	if len(mergedResource.Resource) == 0 {
		mergedResource = b
	}
	mergedResource.Verbs = newVerbs
	mergedResource.Subresources = newSubresources

	return mergedResource
}

// assembleGroups 由合并后的 map 与有序列表构造最终切片。
func assembleGroups(
	groupMap map[string]apidiscoveryv2.APIGroupDiscovery,
	orderedGroups []string,
) []apidiscoveryv2.APIGroupDiscovery {
	result := make([]apidiscoveryv2.APIGroupDiscovery, 0, len(orderedGroups))
	for _, groupName := range orderedGroups {
		// 顺序列表中可能出现重复项，但 map 查找天然去重。
		if group, ok := groupMap[groupName]; ok {
			result = append(result, group)
		}
	}
	return result
}

// orderedAPIVersionList 构造最终的 APIGroupDiscovery 结构体。
func orderedAPIVersionList(
	versionMap map[string]apidiscoveryv2.APIVersionDiscovery,
	orderedVersions []string,
) []apidiscoveryv2.APIVersionDiscovery {
	mergedVersions := make([]apidiscoveryv2.APIVersionDiscovery, 0, len(orderedVersions))
	for _, vName := range orderedVersions {
		if v, ok := versionMap[vName]; ok {
			mergedVersions = append(mergedVersions, v)
			delete(versionMap, vName) // 避免重复
		}
	}

	return mergedVersions
}

// orderedAPIResourceList 构造最终的 APIVersionDiscovery 结构体。
func orderedAPIResourceList(
	resourceMap map[string]apidiscoveryv2.APIResourceDiscovery,
	orderedResources []string,
) []apidiscoveryv2.APIResourceDiscovery {
	mergedResources := make([]apidiscoveryv2.APIResourceDiscovery, 0, len(orderedResources))
	for _, rName := range orderedResources {
		if r, ok := resourceMap[rName]; ok {
			mergedResources = append(mergedResources, r)
			delete(resourceMap, rName)
		}
	}

	return mergedResources
}

// combineServerDiscoveryLists 把本地结果与对端结果合到一张表，本地以自身 serverID 为键。
func (h *peerAggregatedDiscoveryHandler) combineServerDiscoveryLists(localGroups []apidiscoveryv2.APIGroupDiscovery, peerGroupDiscovery map[string][]apidiscoveryv2.APIGroupDiscovery,
) map[string][]apidiscoveryv2.APIGroupDiscovery {
	allServerLists := make(map[string][]apidiscoveryv2.APIGroupDiscovery, len(peerGroupDiscovery)+1)
	allServerLists[h.serverID] = localGroups
	for peerID, peerList := range peerGroupDiscovery {
		allServerLists[peerID] = peerList
	}
	return allServerLists
}

// sortServerIDs 返回有序的 serverID 列表，用于确定性遍历 map。
func sortServerIDs(allServerLists map[string][]apidiscoveryv2.APIGroupDiscovery) []string {
	allServerIDs := make([]string, 0, len(allServerLists))
	for serverID := range allServerLists {
		allServerIDs = append(allServerIDs, serverID)
	}
	sort.Strings(allServerIDs)
	return allServerIDs
}

// localGroupNames 提取本地发现文档中的组名序列。
func localGroupNames(localGroups []apidiscoveryv2.APIGroupDiscovery) []string {
	localGroupNames := make([]string, 0, len(localGroups))
	for _, g := range localGroups {
		localGroupNames = append(localGroupNames, g.Name)
	}
	return localGroupNames
}

// TestMergeResources 仅供测试使用，让测试无需导出 peerAggregatedDiscoveryHandler 即可调用 MergeResources。
func TestMergeResources(serverID string, localGroups []apidiscoveryv2.APIGroupDiscovery, peerGroupDiscovery map[string][]apidiscoveryv2.APIGroupDiscovery) []apidiscoveryv2.APIGroupDiscovery {
	h := &peerAggregatedDiscoveryHandler{serverID: serverID}
	return h.mergeResources(localGroups, peerGroupDiscovery)
}

// TestFetchFromCache 仅供测试使用，让测试无需导出 peerAggregatedDiscoveryHandler 即可调用 fetchFromCache。
// 返回缓存的响应与 ETag。
func TestFetchFromCache(serverID string, localResourceManager ResourceManager, peerDiscoveryProvider PeerDiscoveryProvider) (apidiscoveryv2.APIGroupDiscoveryList, string) {
	h := &peerAggregatedDiscoveryHandler{
		serverID:              serverID,
		localResourceManager:  localResourceManager,
		peerDiscoveryProvider: peerDiscoveryProvider,
	}
	cached := h.fetchFromCache()
	return cached.cachedResponse, cached.cachedResponseETag
}
