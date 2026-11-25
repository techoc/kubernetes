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

package aggregated

import (
	"fmt"
	"net/http"
	"reflect"
	"sort"
	"sync"
	"sync/atomic"

	apidiscoveryv2 "k8s.io/api/apidiscovery/v2"
	apidiscoveryv2beta1 "k8s.io/api/apidiscovery/v2beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/apimachinery/pkg/runtime/serializer/cbor"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/version"
	apidiscoveryv2conversion "k8s.io/apiserver/pkg/apis/apidiscovery/v2"
	"k8s.io/apiserver/pkg/endpoints/handlers/negotiation"
	"k8s.io/apiserver/pkg/endpoints/handlers/responsewriters"
	"k8s.io/apiserver/pkg/endpoints/metrics"
	"k8s.io/apiserver/pkg/endpoints/request"
	genericfeatures "k8s.io/apiserver/pkg/features"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/klog/v2"
)

// Source 标识发现信息的来源，数值越小优先级越高；同一 group-version
// 被多个来源声明时，只有最小数值的来源会出现在最终文档中。
type Source uint

// Source 数值最小的 GroupVersion 优先。
// 数值刻意留出间隔，便于将来在已有来源之间插入新来源而不重排所有常量。
const (
	AggregatorSource Source = 0
	BuiltinSource    Source = 100
	CRDSource        Source = 200
)

// 本 handler 负责 /apis 端点，返回按 group version 索引的聚合 API 资源列表。
//
// 所有修改方法都要求线程安全：kube-aggregator、CRD 控制器与内置注册
// 会并发更新同一份文档。实现上以读写锁保护 map，读取路径走原子快照。
type ResourceManager interface {
	// AddGroupVersion 把指定 group-version 加入发现文档；
	// 若已在跟踪，则更新其中保存的 APIVersionDiscovery。
	// 线程安全
	AddGroupVersion(groupName string, value apidiscoveryv2.APIVersionDiscovery)

	// SetGroupVersionPriority 设置在排序某个组和 group-version 时使用的优先级。
	// 若同一组的两个版本上报了不同的优先级，取较大的那个。
	// 若该组未知，则忽略该优先级。
	// 一旦该 group-version 被移除，其优先级也会被遗忘。
	SetGroupVersionPriority(gv metav1.GroupVersion, grouppriority, versionpriority int)

	// RemoveGroup 删除某个组下的全部 group-version。
	// 线程安全
	RemoveGroup(groupName string)

	// RemoveGroupVersion 删除指定的 group-version。若一个组的所有版本都被删除，
	// 则整个组从列表中移除。
	// 线程安全
	RemoveGroupVersion(gv metav1.GroupVersion)

	// SetGroups 重置管理器已知的 group-version 列表，替换为给定的这些组。
	// 线程安全
	SetGroups([]apidiscoveryv2.APIGroupDiscovery)

	// WithSource 返回使用另一来源的同一个资源管理器。
	// 来源用于决定如何对组去重：
	// 取数值最小的来源。
	WithSource(source Source) ResourceManager

	// AddInvalidationCallback 注册一个回调，在发现缓存失效时调用。
	AddInvalidationCallback(callback func())

	http.Handler
}

// resourceManager 是 ResourceManager 的视图：同一底层 manager 可派生出多个
// 携带不同 Source 的视图，从而让各来源互不覆盖地写入。
type resourceManager struct {
	source Source
	*resourceDiscoveryManager
}

// AddGroupVersion 以本视图的 Source 写入一个 group-version 的发现信息。
func (rm resourceManager) AddGroupVersion(groupName string, value apidiscoveryv2.APIVersionDiscovery) {
	rm.resourceDiscoveryManager.AddGroupVersion(rm.source, groupName, value)
}

// SetGroupVersionPriority 以本视图的 Source 设置排序优先级。
func (rm resourceManager) SetGroupVersionPriority(gv metav1.GroupVersion, grouppriority, versionpriority int) {
	rm.resourceDiscoveryManager.SetGroupVersionPriority(rm.source, gv, grouppriority, versionpriority)
}

// RemoveGroup 以本视图的 Source 删除整个组。
func (rm resourceManager) RemoveGroup(groupName string) {
	rm.resourceDiscoveryManager.RemoveGroup(rm.source, groupName)
}

// RemoveGroupVersion 以本视图的 Source 删除单个 group-version。
func (rm resourceManager) RemoveGroupVersion(gv metav1.GroupVersion) {
	rm.resourceDiscoveryManager.RemoveGroupVersion(rm.source, gv)
}

// SetGroups 以本视图的 Source 整体替换已知的组列表。
func (rm resourceManager) SetGroups(groups []apidiscoveryv2.APIGroupDiscovery) {
	rm.resourceDiscoveryManager.SetGroups(rm.source, groups)
}

// WithSource 返回共享同一底层存储的新视图；底层 manager 不变，
// 因此不同来源的写入会在计算最终文档时合并。
func (rm resourceManager) WithSource(source Source) ResourceManager {
	return resourceManager{
		source:                   source,
		resourceDiscoveryManager: rm.resourceDiscoveryManager,
	}
}

// AddInvalidationCallback 注册缓存失效回调（如通知 apiservice 重新计算）。
func (rm resourceManager) AddInvalidationCallback(callback func()) {
	rm.resourceDiscoveryManager.AddInvalidationCallback(callback)
}

// groupKey 是 apiGroups 的键：组名 + 来源，使不同来源的同名组可以共存。
type groupKey struct {
	name string

	// source 标识该组来自哪里，并决定重复的组中发现信息采用哪一个。
	source Source
}

// groupVersionKey 是优先级映射的键；带 source 是因为优先级按来源分别记录。
type groupVersionKey struct {
	metav1.GroupVersion
	source Source
}

// resourceDiscoveryManager 是所有视图共享的存储。
type resourceDiscoveryManager struct {
	serializer runtime.NegotiatedSerializer
	// cache 用原子指针实现，以避免加锁
	// 读路径（/apis 每个客户端都会请求）不取锁，直接原子读取预计算结果。
	cache atomic.Pointer[cachedGroupList]

	serveHTTPFunc http.HandlerFunc

	// 写操作由锁保护。
	// 资源管理器索引的全部 apigroup 与资源列表
	lock                 sync.RWMutex
	apiGroups            map[groupKey]*apidiscoveryv2.APIGroupDiscovery
	versionPriorities    map[groupVersionKey]priorityInfo
	invalidationCallback atomic.Pointer[func()]
}

// priorityInfo 保存单个 group-version 的排序权重。
type priorityInfo struct {
	GroupPriorityMinimum int
	VersionPriority      int
}

// AddInvalidationCallback 覆盖式保存回调（仅支持一个），因为缓存失效动作是全局的。
func (rdm *resourceDiscoveryManager) AddInvalidationCallback(callback func()) {
	rdm.invalidationCallback.Store(&callback)
}

// invalidateCacheLocked 清空缓存并通知外部；调用方必须已持有写锁，
// 以保证缓存失效与状态变更的先后顺序对读者可见。
func (rdm *resourceDiscoveryManager) invalidateCacheLocked() {
	rdm.cache.Store(nil)
	if callback := rdm.invalidationCallback.Load(); callback != nil {
		(*callback)()
	}
}

// NewResourceManager 构造共享的发现管理器，并注册 v2/v2beta1 两个版本及转换函数，
// 使同一个内部对象可以按客户端 Accept 协商输出为不同版本。
func NewResourceManager(path string) ResourceManager {
	scheme := runtime.NewScheme()
	utilruntime.Must(apidiscoveryv2.AddToScheme(scheme))
	utilruntime.Must(apidiscoveryv2beta1.AddToScheme(scheme))
	// 注册 apidiscovery 的转换函数
	utilruntime.Must(apidiscoveryv2conversion.RegisterConversions(scheme))

	// CBOR 仅在特性门控开启时注册，避免在未启用的集群上暴露新编码。
	var opts []serializer.CodecFactoryOptionsMutator
	if utilfeature.DefaultFeatureGate.Enabled(genericfeatures.CBORServingAndStorage) {
		opts = append(opts, serializer.WithSerializer(cbor.NewSerializerInfo))
	}
	codecs := serializer.NewCodecFactory(scheme, opts...)
	rdm := &resourceDiscoveryManager{
		serializer:        codecs,
		versionPriorities: make(map[groupVersionKey]priorityInfo),
	}
	// 把 path 作为 subresource 上报指标，使 /apis 与 /api 的请求量可区分。
	rdm.serveHTTPFunc = metrics.InstrumentHandlerFunc(request.MethodGet,
		/* group = */ "",
		/* version = */ "",
		/* resource = */ "",
		/* subresource = */ path,
		/* scope = */ "",
		/* component = */ metrics.APIServerComponent,
		/* deprecated */ false,
		/* removedRelease */ "",
		rdm.serveHTTP)
	// 默认视图属于 BuiltinSource；聚合层与 CRD 通过 WithSource 派生自己的视图。
	return resourceManager{
		source:                   BuiltinSource,
		resourceDiscoveryManager: rdm,
	}
}

// SetGroupVersionPriority 记录某来源下 group-version 的排序权重。
func (rdm *resourceDiscoveryManager) SetGroupVersionPriority(source Source, gv metav1.GroupVersion, groupPriorityMinimum, versionPriority int) {
	rdm.lock.Lock()
	defer rdm.lock.Unlock()

	key := groupVersionKey{
		GroupVersion: gv,
		source:       source,
	}
	rdm.versionPriorities[key] = priorityInfo{
		GroupPriorityMinimum: groupPriorityMinimum,
		VersionPriority:      versionPriority,
	}
	rdm.invalidateCacheLocked()
}

// SetGroups 用给定来源的完整组列表替换已有数据，并清理不再被引用的优先级条目，
// 防止长期运行的进程因不断增删 CRD 而累积无用的优先级记录。
func (rdm *resourceDiscoveryManager) SetGroups(source Source, groups []apidiscoveryv2.APIGroupDiscovery) {
	rdm.lock.Lock()
	defer rdm.lock.Unlock()

	// 置 nil 后由 addGroupVersionLocked 惰性重建 map。
	rdm.apiGroups = nil
	rdm.invalidateCacheLocked()

	for _, group := range groups {
		for _, version := range group.Versions {
			rdm.addGroupVersionLocked(source, group.Name, version)
		}
	}

	// 过滤掉已不再使用的优先级条目
	for gv := range rdm.versionPriorities {
		key := groupKey{
			source: source,
			name:   gv.Group,
		}
		entry, exists := rdm.apiGroups[key]
		if !exists {
			delete(rdm.versionPriorities, gv)
			continue
		}

		containsVersion := false

		for _, v := range entry.Versions {
			if v.Version == gv.Version {
				containsVersion = true
				break
			}
		}

		if !containsVersion {
			delete(rdm.versionPriorities, gv)
		}
	}
}

// AddGroupVersion 新增或更新单个 group-version 的发现信息。
func (rdm *resourceDiscoveryManager) AddGroupVersion(source Source, groupName string, value apidiscoveryv2.APIVersionDiscovery) {
	rdm.lock.Lock()
	defer rdm.lock.Unlock()

	rdm.addGroupVersionLocked(source, groupName, value)
}

// addGroupVersionLocked 是加锁版本的实际实现；调用方必须持有写锁。
func (rdm *resourceDiscoveryManager) addGroupVersionLocked(source Source, groupName string, value apidiscoveryv2.APIVersionDiscovery) {

	if rdm.apiGroups == nil {
		rdm.apiGroups = make(map[groupKey]*apidiscoveryv2.APIGroupDiscovery)
	}

	key := groupKey{
		source: source,
		name:   groupName,
	}

	if existing, groupExists := rdm.apiGroups[key]; groupExists {
		// 该版本已存在时直接替换
		versionExists := false

		// 效率不算高，但实践中版本数量通常很少
		for i := range existing.Versions {
			if existing.Versions[i].Version == value.Version {
				// 内容完全相同则直接返回，避免每次 informer 重同步都让缓存失效。
				if reflect.DeepEqual(existing.Versions[i], value) {
					return
				}

				existing.Versions[i] = value
				versionExists = true
				break
			}
		}

		if !versionExists {
			existing.Versions = append(existing.Versions, value)
		}

	} else {
		group := &apidiscoveryv2.APIGroupDiscovery{
			ObjectMeta: metav1.ObjectMeta{
				Name: groupName,
			},
			Versions: []apidiscoveryv2.APIVersionDiscovery{value},
		}
		rdm.apiGroups[key] = group
	}
	klog.Infof("Adding GroupVersion %s %s to ResourceManager", groupName, value.Version)

	// 未显式设置优先级时使用默认值，与 kube-aggregator 的默认约定保持一致。
	gv := metav1.GroupVersion{Group: groupName, Version: value.Version}
	gvKey := groupVersionKey{
		GroupVersion: gv,
		source:       source,
	}
	if _, ok := rdm.versionPriorities[gvKey]; !ok {
		rdm.versionPriorities[gvKey] = priorityInfo{
			GroupPriorityMinimum: 1000,
			VersionPriority:      15,
		}
	}

	// 重置响应文档，使其按需重新生成
	rdm.invalidateCacheLocked()
}

// RemoveGroupVersion 删除指定来源下的单个 group-version；没有实际变化时不失效缓存。
func (rdm *resourceDiscoveryManager) RemoveGroupVersion(source Source, apiGroup metav1.GroupVersion) {
	rdm.lock.Lock()
	defer rdm.lock.Unlock()

	key := groupKey{
		source: source,
		name:   apiGroup.Group,
	}

	group, exists := rdm.apiGroups[key]
	if !exists {
		return
	}

	modified := false
	for i := range group.Versions {
		if group.Versions[i].Version == apiGroup.Version {
			group.Versions = append(group.Versions[:i], group.Versions[i+1:]...)
			modified = true
			break
		}
	}
	// 若没有任何修改，就不需要清空缓存
	if !modified {
		return
	}

	gvKey := groupVersionKey{
		GroupVersion: apiGroup,
		source:       source,
	}

	delete(rdm.versionPriorities, gvKey)
	// 版本删空后组本身也不再列出，避免出现空的 APIGroupDiscovery。
	if len(group.Versions) == 0 {
		delete(rdm.apiGroups, key)
	}

	// 重置响应文档，使其按需重新生成
	rdm.invalidateCacheLocked()
}

// RemoveGroup 删除指定来源下的整个组及其全部优先级条目。
func (rdm *resourceDiscoveryManager) RemoveGroup(source Source, groupName string) {
	rdm.lock.Lock()
	defer rdm.lock.Unlock()

	key := groupKey{
		source: source,
		name:   groupName,
	}

	delete(rdm.apiGroups, key)

	// 只清理本来源的优先级，其他来源同名组的排序信息必须保留。
	for k := range rdm.versionPriorities {
		if k.Group == groupName && k.source == source {
			delete(rdm.versionPriorities, k)
		}
	}

	// 重置响应文档，使其按需重新生成
	// 这里直接 Store(nil) 而非 invalidateCacheLocked：删除组不需要通知外部重新聚合。
	rdm.cache.Store(nil)
}

// 把 API 组列表从 map 转为 list 并按插入顺序排序，完成对外提供服务前的准备。
//
// 计算分三步：按来源去重合并 group-version、按 VersionPriority 排序版本、
// 按 GroupPriorityMinimum 排序组。调用方必须持有读锁或写锁。
func (rdm *resourceDiscoveryManager) calculateAPIGroupsLocked() []apidiscoveryv2.APIGroupDiscovery {
	regenerationCounter.Inc()
	// 按优先级重新排列 apiGroups。
	groups := []apidiscoveryv2.APIGroupDiscovery{}

	groupsToUse := map[string]apidiscoveryv2.APIGroupDiscovery{}
	sourcesUsed := map[metav1.GroupVersion]Source{}

	for key, group := range rdm.apiGroups {
		if existing, ok := groupsToUse[key.name]; ok {
			for _, v := range group.Versions {
				gv := metav1.GroupVersion{Group: key.name, Version: v.Version}

				// Source 数值越小优先级越高，因此已记录更小来源时跳过。
				if usedSource, seen := sourcesUsed[gv]; seen && key.source >= usedSource {
					continue
				} else if seen {
					// 找到重复版本的下标并替换
					for i := 0; i < len(existing.Versions); i++ {
						if existing.Versions[i].Version == v.Version {
							existing.Versions[i] = v
							break
						}
					}

				} else {
					// 新的 group-version，直接追加
					existing.Versions = append(existing.Versions, v)
				}

				sourcesUsed[gv] = key.source
				groupsToUse[key.name] = existing
			}
			// 检查是否存在版本重叠，若重叠则取来源优先级最高的那个
		} else {
			// 首次遇到该组时深拷贝，后续合并不会污染受锁保护的原始数据。
			groupsToUse[key.name] = *group.DeepCopy()
			for _, v := range group.Versions {
				gv := metav1.GroupVersion{Group: key.name, Version: v.Version}
				sourcesUsed[gv] = key.source
			}
		}
	}

	for _, group := range groupsToUse {

		// 按优先级重新排列版本，并用 Kubernetes 感知的字符串比较作为平手裁决。
		// 使用 Stable 排序，保证优先级与版本比较都相同时顺序稳定，避免文档抖动。
		sort.SliceStable(group.Versions, func(i, j int) bool {
			iVersion := group.Versions[i].Version
			jVersion := group.Versions[j].Version

			iGV := metav1.GroupVersion{Group: group.Name, Version: iVersion}
			jGV := metav1.GroupVersion{Group: group.Name, Version: jVersion}

			// 优先级必须按实际采用的来源查询，否则跨来源合并后排序会错位。
			iSource := sourcesUsed[iGV]
			jSource := sourcesUsed[jGV]

			iPriority := rdm.versionPriorities[groupVersionKey{iGV, iSource}].VersionPriority
			jPriority := rdm.versionPriorities[groupVersionKey{jGV, jSource}].VersionPriority

			// 优先级相同时按版本字符串比较器排序
			if iPriority == jPriority {
				return version.CompareKubeAwareVersionStrings(iVersion, jVersion) > 0
			}

			// 优先级更高的 i 排在 j 之前
			return iPriority > jPriority
		})

		groups = append(groups, group)
	}

	// 同一组可能有多个版本各自声明 GroupPriorityMinimum，取最大值以尊重最“重要”的版本。
	priorities := map[string]int{}
	for gv, info := range rdm.versionPriorities {
		if source := sourcesUsed[gv.GroupVersion]; source != gv.source {
			continue
		}

		if existing, exists := priorities[gv.Group]; exists {
			if existing < info.GroupPriorityMinimum {
				priorities[gv.Group] = info.GroupPriorityMinimum
			}
		} else {
			priorities[gv.Group] = info.GroupPriorityMinimum
		}
	}

	sort.SliceStable(groups, func(i, j int) bool {
		iName := groups[i].Name
		jName := groups[j].Name

		// 缺省优先级为 0
		iPriority := priorities[iName]
		jPriority := priorities[jName]

		// 按 apiservice 优先级排序发现信息。
		// 逻辑复制自 staging/src/k8s.io/kube-aggregator/pkg/apis/apiregistration/v1/helpers.go
		if iPriority == jPriority {
			// 优先级相同时用名称打破平局
			return iName < jName
		}

		// 优先级更高的 i 排在 j 之前
		return iPriority > jPriority
	})

	return groups
}

// 缓存存在则直接取用；缓存为空则先创建。
// 注意：计算在 RLock 下进行，多个并发未命中会各自计算一次，最终只有一份被保留，
// 这是以极小的重复计算换取读路径无写锁的取舍。
func (rdm *resourceDiscoveryManager) fetchFromCache() *cachedGroupList {
	rdm.lock.RLock()
	defer rdm.lock.RUnlock()

	cacheLoad := rdm.cache.Load()
	if cacheLoad != nil {
		return cacheLoad
	}
	response := apidiscoveryv2.APIGroupDiscoveryList{
		Items: rdm.calculateAPIGroupsLocked(),
	}
	etag, err := calculateETag(response)
	if err != nil {
		// etag 失败不影响正确性，只是失去条件请求能力，因此降级为空 etag。
		klog.Errorf("failed to calculate etag for discovery document: %v", err)
		etag = ""
	}
	cached := &cachedGroupList{
		cachedResponse:     response,
		cachedResponseETag: etag,
	}
	rdm.cache.Store(cached)
	return cached
}

// cachedGroupList 是一次计算的结果快照；缓存对象本身不可变，可被并发读取。
type cachedGroupList struct {
	cachedResponse apidiscoveryv2.APIGroupDiscoveryList
	// etag 只基于 JSON 对象的 SHA 散列计算。
	// 通过不同 Accept 编码（如 protobuf、json）请求同一响应会得到相同的 etag。
	// 这没问题，因为 Accept 是 Vary 头的一部分。
	// 按 RFC7231，客户端只有在 Vary 指明的头字段也匹配时才能缓存响应与 etag 的对应关系，
	// 因此即便 etag 相同，protobuf 与 json 等其他 Accept 编码
	// 也不会被当作同一个响应来缓存。
	cachedResponseETag string
}

// ServeHTTP 走指标包装后的处理函数。
func (rdm *resourceDiscoveryManager) ServeHTTP(resp http.ResponseWriter, req *http.Request) {
	rdm.serveHTTPFunc(resp, req)
}

// serveHTTP 从缓存取文档并写出；整个处理过程不接触存储锁。
func (rdm *resourceDiscoveryManager) serveHTTP(resp http.ResponseWriter, req *http.Request) {
	cache := rdm.fetchFromCache()
	response := cache.cachedResponse
	etag := cache.cachedResponseETag

	writeDiscoveryResponse(&response, etag, rdm.serializer, resp, req)
}

// writeDiscoveryResponse 按 Accept 协商目标版本并写出；etag 可用时走条件请求路径，
// 否则退化为普通协商响应。
func writeDiscoveryResponse(
	resp *apidiscoveryv2.APIGroupDiscoveryList,
	etag string,
	serializer runtime.NegotiatedSerializer,
	w http.ResponseWriter,
	req *http.Request,
) {
	mediaType, _, err := negotiation.NegotiateOutputMediaType(req, serializer, DiscoveryEndpointRestrictions)
	if err != nil {
		// 理论上不会发生。wrapper.go 只有在媒体类型通过 DiscoveryEndpointRestrictions
		// 时才会把请求代理到本 handler
		utilruntime.HandleError(err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	var targetGV schema.GroupVersion
	if mediaType.Convert == nil {
		utilruntime.HandleError(fmt.Errorf("expected aggregated discovery group version, got unknown group and version"))
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	// 只接受 apidiscovery v2/v2beta1；其他类型说明路由配置有误，直接 500 而非输出错误格式。
	if mediaType.Convert.GroupVersion() != apidiscoveryv2.SchemeGroupVersion &&
		mediaType.Convert.GroupVersion() != apidiscoveryv2beta1.SchemeGroupVersion {
		utilruntime.HandleError(fmt.Errorf("expected aggregated discovery group version, got group: %s, version %s", mediaType.Convert.Group, mediaType.Convert.Version))
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	// 特性门控移除 v2beta1 后返回 404，提示客户端改用 v2。
	if mediaType.Convert.GroupVersion() == apidiscoveryv2beta1.SchemeGroupVersion &&
		utilfeature.DefaultFeatureGate.Enabled(genericfeatures.AggregatedDiscoveryRemoveBetaType) {
		klog.Errorf("aggregated discovery version v2beta1 is removed. Please update to use v2")
		w.WriteHeader(http.StatusNotFound)
		return
	}
	targetGV = mediaType.Convert.GroupVersion()

	if len(etag) > 0 {
		// 有 etag 时使用标准的 e-tag 头
		ServeHTTPWithETag(
			resp,
			etag,
			targetGV,
			serializer,
			w,
			req,
		)
	} else {
		// 极少数情况下对象上没有缓存 etag，
		// 此时退化为普通响应。
		responsewriters.WriteObjectNegotiated(
			serializer,
			DiscoveryEndpointRestrictions,
			targetGV,
			w,
			req,
			http.StatusOK,
			resp,
			true,
		)
	}
}
