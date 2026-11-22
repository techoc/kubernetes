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

// PeerDiscoveryProvider defines an interface to get peer resources for peer-aggregated discovery.
// 实现对端数据来源可以是对等 apiserver 的聚合发现端点。
type PeerDiscoveryProvider interface {
	GetPeerResources() map[string][]apidiscoveryv2.APIGroupDiscovery
}

// PeerAggregatedResourceManager defines the interface for managing peer-aggregated discovery resources
// that combines both local and peer server resources.
type PeerAggregatedResourceManager interface {
	// InvalidateCache invalidates the peer-aggregated discovery caches
	// This should be called when peer discovery data changes.
	InvalidateCache()

	// ServeHTTP handles peer-aggregated discovery HTTP requests.
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

// NewPeerAggregatedDiscoveryHandler creates a new handler for peer-aggregated discovery.
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

	// Instrumentation wrapper for serveHTTP
	// 指标包在合并逻辑外层，统计的是端点整体耗时。
	h.serveHTTPFunc = metrics.InstrumentHandlerFunc(request.MethodGet,
		"", "", "", path, "", metrics.APIServerComponent, false, "",
		h.serveHTTP)

	return h
}

// InvalidateCache invalidates the peer-aggregated discovery caches.
// This should be called when peer discovery data changes.
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

	// Get local groups
	// 仅在本地管理器支持内部接口时才能直接读其缓存；否则退化为空集合。
	var localGroups []apidiscoveryv2.APIGroupDiscovery
	if rdm, ok := h.localResourceManager.(resourceManager); ok {
		localCache := rdm.fetchFromCache()
		if localCache != nil {
			localGroups = localCache.cachedResponse.Items
		}
	}

	// Get peer resources if provider is set
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

// mergeResources merges local and peer APIGroupDiscovery lists, preserving relative order as much as possible.
// localGroups is a list of APIGroupDiscovery objects from the local server.
// peerGroupDiscovery is a map of peer server IDs to their APIGroupDiscovery lists.
//
// 顺序稳定性是对外契约的一部分：客户端可能按顺序渲染 kubectl api-resources，
// 因此这里用拓扑式合并（MergePreservingRelativeOrder）而非简单排序。
func (h *peerAggregatedDiscoveryHandler) mergeResources(
	localGroups []apidiscoveryv2.APIGroupDiscovery,
	peerGroupDiscovery map[string][]apidiscoveryv2.APIGroupDiscovery,
) []apidiscoveryv2.APIGroupDiscovery {
	discoveryByServerID := h.combineServerDiscoveryLists(localGroups, peerGroupDiscovery)
	if len(discoveryByServerID) <= 1 {
		// No merging or sorting needed.
		return localGroups
	}

	mergedGroupMap := make(map[string]apidiscoveryv2.APIGroupDiscovery)
	allGroupNames := make([][]string, 0, len(discoveryByServerID))
	// 按 serverID 排序遍历，保证相同输入得到相同的合并结果（确定性 ETag）。
	sortedServerIDs := sortServerIDs(discoveryByServerID)

	// contentChanged tracks if any new groups/versions/resources were added in a peer.
	contentChanged := false
	// groupNameListsAreIdentical tracks if all peers have the exact same group names as local.
	groupNameListsAreIdentical := true
	localGroupNames := localGroupNames(localGroups)
	for _, serverID := range sortedServerIDs {
		discoveryGroups := discoveryByServerID[serverID]
		groupNames, didThisServerMerge := h.processServerGroups(discoveryGroups, mergedGroupMap)
		if didThisServerMerge {
			contentChanged = true
		}
		allGroupNames = append(allGroupNames, groupNames)
		// Check if this server's group order differs from local.
		// 1. It catches simple re-orderings (e.g., ["g1", "g2"] vs ["g2", "g1"]).
		// 2. It catches new or removed groups (e.g., ["g1"] vs ["g1", "g2"]).
		// In either case, the list is not identical, and we must fall through
		// to the full merge to ensure a deterministic result.
		if !reflect.DeepEqual(localGroupNames, groupNames) {
			groupNameListsAreIdentical = false
		}
	}

	// Case 1: Total short-circuit.
	// Nothing changed (no new content AND no order change).
	// 完全无变化时直接返回本地切片，避免分配与排序。
	if !contentChanged && groupNameListsAreIdentical {
		return localGroups
	}

	var finalGroupOrder []string
	// Case 2: Content changed, but group order didn't.
	// We must return the new content, but we can skip the expensive sort.
	if contentChanged && groupNameListsAreIdentical {
		finalGroupOrder = localGroupNames
	} else {
		// Case 3: Group lists were different (new groups, different order, or both).
		// We have no choice but to run the full topological sort.
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
			// Merge versions and resources.
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

// mergeVersionsAcrossGroup merges two APIGroupDiscovery objects.
// It returns a new merged object and a boolean indicating if the result differs from a.
// The result may differ if any new versions/resources were added or if the order was different.
func mergeVersionsAcrossGroup(a, b apidiscoveryv2.APIGroupDiscovery) (apidiscoveryv2.APIGroupDiscovery, bool) {
	versionOrder := [][]string{}
	mergedVersionMap := make(map[string]apidiscoveryv2.APIVersionDiscovery)

	aVersionNames := make([]string, 0, len(a.Versions))
	bVersionNames := make([]string, 0, len(b.Versions))
	// This flag tracks if 'b' modified existing versions.
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
			// Version exists in both, must merge their resources
			mergedV, didMerge := mergeResourcesAcrossVersion(existing, v)
			mergedVersionMap[v.Version] = mergedV
			if didMerge {
				contentMerged = true
			}
		}
	}
	versionOrder = append(versionOrder, bVersionNames)

	// Case 1: Total short-circuit.
	// No new versions/resources were added and order is the same.
	versionNamesAreIdentical := reflect.DeepEqual(aVersionNames, bVersionNames)
	if !contentMerged && versionNamesAreIdentical {
		return a, false
	}

	var finalVersionOrder []string
	if versionNamesAreIdentical {
		// Case 2: Content changed, but version order didn't.
		// We can skip the expensive sort and just use the known order.
		finalVersionOrder = aVersionNames
	} else {
		// Case 3: Version lists were different (new versions, different order, or both).
		// Must perform topological sort.
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

// mergeResourcesAcrossVersion merges two APIVersionDiscovery objects.
// It returns a new merged object and a boolean indicating if the result differs from a.
// The result may differ if any new resources were added or if the order was different.
func mergeResourcesAcrossVersion(a, b apidiscoveryv2.APIVersionDiscovery) (apidiscoveryv2.APIVersionDiscovery, bool) {
	resourceOrder := [][]string{}
	mergedResourceMap := make(map[string]apidiscoveryv2.APIResourceDiscovery)

	aResourceNames := make([]string, 0, len(a.Resources))
	bResourceNames := make([]string, 0, len(b.Resources))
	// This flag tracks if 'b' modified existing resources.
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

	// Case 1: Total short-circuit.
	// 'b' didn't add any new resources AND the resource order is the same.
	resourceNameListIsIdentical := reflect.DeepEqual(aResourceNames, bResourceNames)
	if !contentChanged && resourceNameListIsIdentical {
		return a, false
	}

	var finalResourceOrder []string
	if resourceNameListIsIdentical {
		// Case 2: Content changed, but resource order didn't.
		// We can skip the expensive sort and just use the known order.
		finalResourceOrder = aResourceNames
	} else {
		// Case 3: Resource lists were different (new resources, different order, or both).
		// Must perform topological sort.
		finalResourceOrder = utilsort.MergePreservingRelativeOrder(resourceOrder)
	}

	mergedResources := orderedAPIResourceList(mergedResourceMap, finalResourceOrder)
	return apidiscoveryv2.APIVersionDiscovery{
		Version:   a.Version,
		Resources: mergedResources,
		Freshness: a.Freshness,
	}, true
}

// mergeResourceCapabilities takes two resources and returns a new one
// containing the union of their verbs and subresources.
// The resulting slices are sorted to ensure the merge is deterministic.
func mergeResourceCapabilities(a, b apidiscoveryv2.APIResourceDiscovery) apidiscoveryv2.APIResourceDiscovery {
	// 1. Merge Verbs
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
		// Sort for deterministic ETag
		sort.Strings(newVerbs)
	}

	// 2. Merge Subresources
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
		// Sort for deterministic ETag
		sort.Slice(newSubresources, func(i, j int) bool {
			return newSubresources[i].Subresource < newSubresources[j].Subresource
		})
	}

	// 3. Return the new, merged object
	// We prefer 'a's metadata (like ResponseKind) but 'b's could be used if 'a' is empty.
	mergedResource := a
	if len(mergedResource.Resource) == 0 {
		mergedResource = b
	}
	mergedResource.Verbs = newVerbs
	mergedResource.Subresources = newSubresources

	return mergedResource
}

// assembleGroups builds the final slice from the merged map and ordered list.
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

// orderedAPIVersionList builds the final APIGroupDiscovery struct.
func orderedAPIVersionList(
	versionMap map[string]apidiscoveryv2.APIVersionDiscovery,
	orderedVersions []string,
) []apidiscoveryv2.APIVersionDiscovery {
	mergedVersions := make([]apidiscoveryv2.APIVersionDiscovery, 0, len(orderedVersions))
	for _, vName := range orderedVersions {
		if v, ok := versionMap[vName]; ok {
			mergedVersions = append(mergedVersions, v)
			delete(versionMap, vName) // Avoid duplicates
		}
	}

	return mergedVersions
}

// orderedAPIResourceList builds the final APIVersionDiscovery struct.
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

// TestMergeResources is for testing only. It allows tests to call MergeResources without exporting peerAggregatedDiscoveryHandler.
func TestMergeResources(serverID string, localGroups []apidiscoveryv2.APIGroupDiscovery, peerGroupDiscovery map[string][]apidiscoveryv2.APIGroupDiscovery) []apidiscoveryv2.APIGroupDiscovery {
	h := &peerAggregatedDiscoveryHandler{serverID: serverID}
	return h.mergeResources(localGroups, peerGroupDiscovery)
}

// TestFetchFromCache is for testing only. It allows tests to call fetchFromCache without exporting peerAggregatedDiscoveryHandler.
// Returns the cached response and ETag.
func TestFetchFromCache(serverID string, localResourceManager ResourceManager, peerDiscoveryProvider PeerDiscoveryProvider) (apidiscoveryv2.APIGroupDiscoveryList, string) {
	h := &peerAggregatedDiscoveryHandler{
		serverID:              serverID,
		localResourceManager:  localResourceManager,
		peerDiscoveryProvider: peerDiscoveryProvider,
	}
	cached := h.fetchFromCache()
	return cached.cachedResponse, cached.cachedResponseETag
}
