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

package peerproxy

import (
	"context"
	"sync/atomic"
	"time"

	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"

	apidiscoveryv2 "k8s.io/api/apidiscovery/v2"
)

// GVExtractor 从对象中提取 group-version。
// 它返回应从 peer discovery 中排除的 CRD 或聚合 API 的 GroupVersion，
// 避免那些已删除但仍出现在某个 peer discovery 中的 CRD/聚合 API
//
// GVExtractor 把 CRD / APIService 对象映射为其服务的 GV。各组件类型不同，
// 因此用函数注入提取逻辑，保持本包不依赖具体的 API 类型。
type GVExtractor func(obj interface{}) []schema.GroupVersion

// GVExclusionManager 管理 peer discovery 中 group-version 的排除。
// 它维护两个 atomic map：
// - currentlyActiveGVs：CRD 与聚合 APIService 当前提供的所有 GV
// - recentlyDeletedGVs：最近删除的 CRD/聚合 APIService 的 GV，
// 带删除时间戳，用于宽限期
//
// 它运行两个 worker：
// 1. Active GV Tracker：由 CRD/APIService 事件触发，重建 active GV 并
//
// 2. Peer Discovery Re-filter：限速 worker，按排除集过滤 peer 缓存
//
// 存在的原因：对等节点可能仍缓存了已删除的 CRD/聚合 API 的发现信息，
// 直接合并会把已不存在的 GV 暴露给客户端。本管理器在“原始对等缓存”
// 与“对外可见缓存”之间做一层过滤，并用宽限期容忍各节点传播延迟。
type GVExclusionManager struct {
	// 原子值保存整张 map，读路径无锁；写路径生成新 map 后整体替换。
	currentlyActiveGVs atomic.Value // map[schema.GroupVersion]struct{}
	recentlyDeletedGVs atomic.Value // map[schema.GroupVersion]time.Time

	// 用于获取 active GV 的 informer
	crdInformer         cache.SharedIndexInformer
	crdExtractor        GVExtractor
	apiServiceInformer  cache.SharedIndexInformer
	apiServiceExtractor GVExtractor

	// Worker 1：由 CRD/APIService 事件或延迟清理调度触发
	activeGVQueue workqueue.TypedRateLimitingInterface[string]
	// exclusionGracePeriod 是从排除集中清除已删除 GV 之前的宽限期；宽限期必须覆盖对等节点传播删除事件的最大延迟，否则会出现 GV 短暂“复活”。
	exclusionGracePeriod time.Duration
	// Worker 2：由 Active/Deleted GV 变化触发
	refilterQueue workqueue.TypedRateLimitingInterface[string]

	// rawPeerDiscoveryCache 仅由 peerLeaseQueue worker 在 lease 变化时写入。
	//
	rawPeerDiscoveryCache *atomic.Value // map[string]PeerDiscoveryCacheEntry
	// filteredPeerDiscoveryCache 仅由 refilter worker 在原始缓存或排除集变化时写入。
	//
	filteredPeerDiscoveryCache atomic.Value // map[string]PeerDiscoveryCacheEntry
	invalidationCallback       *atomic.Pointer[func()]
}

// NewGVExclusionManager 创建一个新的 GV 排除管理器。
// rawPeerDiscoveryCache 与 invalidationCallback 由对等租约管理逻辑共享；
// 若为 nil，本管理器仍可工作（仅维护排除集）。
func NewGVExclusionManager(
	exclusionGracePeriod time.Duration,
	rawPeerDiscoveryCache *atomic.Value,
	invalidationCallback *atomic.Pointer[func()],
) *GVExclusionManager {
	mgr := &GVExclusionManager{
		exclusionGracePeriod:  exclusionGracePeriod,
		rawPeerDiscoveryCache: rawPeerDiscoveryCache,
		invalidationCallback:  invalidationCallback,
		activeGVQueue: workqueue.NewTypedRateLimitingQueueWithConfig(
			workqueue.DefaultTypedControllerRateLimiter[string](),
			workqueue.TypedRateLimitingQueueConfig[string]{
				Name: "active-gv-tracker",
			}),
		refilterQueue: workqueue.NewTypedRateLimitingQueueWithConfig(
			workqueue.DefaultTypedControllerRateLimiter[string](),
			workqueue.TypedRateLimitingQueueConfig[string]{
				Name: "peer-discovery-refilter",
			}),
	}

	// 初始化为空 map 而非 nil，避免读取方需要额外判空。
	mgr.currentlyActiveGVs.Store(map[schema.GroupVersion]struct{}{})
	mgr.recentlyDeletedGVs.Store(map[schema.GroupVersion]time.Time{})
	mgr.filteredPeerDiscoveryCache.Store(map[string]PeerDiscoveryCacheEntry{})

	return mgr
}

// GetFilteredPeerDiscoveryCache 返回过滤后的对等发现缓存。
// 返回的 map 是当前快照；调用方不应修改（写入方只做整体替换，但 map 本身共享）。
func (m *GVExclusionManager) GetFilteredPeerDiscoveryCache() map[string]PeerDiscoveryCacheEntry {
	if cache := m.filteredPeerDiscoveryCache.Load(); cache != nil {
		if cacheMap, ok := cache.(map[string]PeerDiscoveryCacheEntry); ok {
			return cacheMap
		}
	}
	return map[string]PeerDiscoveryCacheEntry{}
}

// RegisterCRDInformerHandlers 使用自定义 extractor 为 CRD informer 注册事件处理器，extractor 负责从 CRD 对象中提取 GroupVersion。
// 三类事件统一触发全量重算：增量维护删除历史反而更容易出错，而 CRD 变更频率远低于请求频率。
func (m *GVExclusionManager) RegisterCRDInformerHandlers(crdInformer cache.SharedIndexInformer, extractor GVExtractor) error {
	m.crdInformer = crdInformer
	m.crdExtractor = extractor
	_, err := m.crdInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			m.handleGVUpdate()
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			m.handleGVUpdate()
		},
		DeleteFunc: func(obj interface{}) {
			m.handleGVUpdate()
		},
	})
	return err
}

// RegisterAPIServiceInformerHandlers 用自定义 extractor 为 APIService informer 注册事件处理器。
// extractor 负责从 APIService 对象中提取 GroupVersion，
// 并判断它们是否代表聚合 API。
func (m *GVExclusionManager) RegisterAPIServiceInformerHandlers(apiServiceInformer cache.SharedIndexInformer, extractor GVExtractor) error {
	m.apiServiceInformer = apiServiceInformer
	m.apiServiceExtractor = extractor
	_, err := m.apiServiceInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			m.handleGVUpdate()
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			m.handleGVUpdate()
		},
		DeleteFunc: func(obj interface{}) {
			m.handleGVUpdate()
		},
	})
	return err
}

// WaitForCacheSync 等待已注册的 informer 缓存完成同步；未注册任何 informer 时直接返回 true（无数据可同步）。
func (m *GVExclusionManager) WaitForCacheSync(stopCh <-chan struct{}) bool {
	synced := []cache.InformerSynced{}
	if m.crdInformer != nil {
		synced = append(synced, m.crdInformer.HasSynced)
	}
	if m.apiServiceInformer != nil {
		synced = append(synced, m.apiServiceInformer.HasSynced)
	}
	if len(synced) == 0 {
		return true
	}
	return cache.WaitForNamedCacheSync("gv-exclusion-manager", stopCh, synced...)
}

// getExclusionSet 返回合并后的排除集，即 union(currentlyActiveGVs, recentlyDeletedGVs)。
// 活跃集与最近删除集都需排除：前者对本地节点不可见（不应声明由本节点服务），
// 后者是刚删除、对等节点可能仍缓存的。
func (m *GVExclusionManager) getExclusionSet() map[schema.GroupVersion]struct{} {
	activeMap := m.loadCurrentlyActiveGVs()
	deletedMap := m.loadRecentlyDeletedGVs()

	exclusionSet := make(map[schema.GroupVersion]struct{}, len(activeMap)+len(deletedMap))
	for gv := range activeMap {
		exclusionSet[gv] = struct{}{}
	}
	for gv := range deletedMap {
		exclusionSet[gv] = struct{}{}
	}

	return exclusionSet
}

// CRD 或 APIService 事件发生时调用 handleGVUpdate。
// 它触发 reconcile：重建 active GV 集合，
func (m *GVExclusionManager) handleGVUpdate() {
	m.activeGVQueue.Add("sync")
}

// RunPeerDiscoveryActiveGVTracker 运行 Active GV Tracker worker。
// 该 worker 由 CRD/APIService 事件触发，
// 重建 active GV 集并清理过期 GV。
func (m *GVExclusionManager) RunPeerDiscoveryActiveGVTracker(ctx context.Context) {
	defer m.activeGVQueue.ShutDown()

	klog.Info("Starting Active GV Tracker worker")
	go wait.UntilWithContext(ctx, m.runActiveGVTrackerWorker, time.Second)

	<-ctx.Done()
	klog.Info("Active GV Tracker workers stopped")
}

// runActiveGVTrackerWorker 循环处理队列直到 context 取消或队列关闭。
func (m *GVExclusionManager) runActiveGVTrackerWorker(ctx context.Context) {
	for m.processNextActiveGV(ctx) {
	}
}

// processNextActiveGV 取一个任务并执行一次全量重算；返回 false 表示应退出。
func (m *GVExclusionManager) processNextActiveGV(ctx context.Context) bool {
	key, shutdown := m.activeGVQueue.Get()
	if shutdown {
		return false
	}
	defer m.activeGVQueue.Done(key)

	select {
	case <-ctx.Done():
		return false
	default:
	}

	m.reconcileActiveGVs()
	return true
}

// reconcileActiveGVs 执行以下操作：
// 1. 从 CRD 与 APIService informer 获取所有 GV
// 2. 与上次状态比较差异
// 3. 把已删除 GV 加入 recentlyDeletedGVs
// 4. 清理过期 GV，若有变化则把 Worker 2 入队。
func (m *GVExclusionManager) reconcileActiveGVs() {
	freshGVs := make(map[schema.GroupVersion]struct{})

	// 从 CRD informer 获取 GV
	if m.crdInformer != nil && m.crdExtractor != nil {
		crdList := m.crdInformer.GetStore().List()
		for _, item := range crdList {
			gvs := m.crdExtractor(item)
			for _, gv := range gvs {
				freshGVs[gv] = struct{}{}
			}
		}
	}

	// 从 APIService informer 获取 GV
	if m.apiServiceInformer != nil && m.apiServiceExtractor != nil {
		apiSvcList := m.apiServiceInformer.GetStore().List()
		for _, item := range apiSvcList {
			gvs := m.apiServiceExtractor(item)
			for _, gv := range gvs {
				freshGVs[gv] = struct{}{}
			}
		}
	}

	// 加载上次 active GV 并检测差异
	previousGVs := m.loadCurrentlyActiveGVs()
	deletedGVs, activeGVsChanged := diffGVs(previousGVs, freshGVs)

	// 更新 recentlyDeletedGVs：加入新删除的 GV、清理过期项
	recentlyDeletedChanged := m.updateRecentlyDeletedGVs(deletedGVs)

	// 排除集变化才需要重过滤；否则跳过以免无谓地重建对等缓存。
	if activeGVsChanged || recentlyDeletedChanged {
		if activeGVsChanged {
			m.currentlyActiveGVs.Store(freshGVs)
			klog.V(4).Infof("Active GVs updated: %d GVs now active", len(freshGVs))
		}
		m.TriggerRefilter()
	} else {
		klog.V(4).Infof("No changes detected in active or recently deleted GVs")
	}
}

// updateRecentlyDeletedGVs 把新删除的 GV 加入 recentlyDeletedGVs
// 并清理过期项；有变化时返回 true。
func (m *GVExclusionManager) updateRecentlyDeletedGVs(deletedGVs []schema.GroupVersion) bool {
	deletedMap := m.loadRecentlyDeletedGVs()
	// 无事可做时提前返回
	if len(deletedGVs) == 0 && len(deletedMap) == 0 {
		return false
	}

	now := time.Now()
	newDeletedMap := make(map[schema.GroupVersion]time.Time, len(deletedMap)+len(deletedGVs))
	changed := false

	// 复制现有条目，跳过过期项
	for gv, deletionTime := range deletedMap {
		if now.Sub(deletionTime) > m.exclusionGracePeriod {
			klog.V(4).Infof("Reaping GV %s (grace period expired)", gv.String())
			changed = true
			// 不加入新 map（相当于移除）
		} else {
			newDeletedMap[gv] = deletionTime
		}
	}

	// 在宽限期后安排一次延迟 sync 以清除过期的 GV；否则在无新事件时，过期条目会一直留在排除集里。
	if len(deletedGVs) > 0 {
		m.activeGVQueue.AddAfter("sync", m.exclusionGracePeriod)
	}

	for _, gv := range deletedGVs {
		newDeletedMap[gv] = now
		klog.V(4).Infof("GV %s deleted: moved to recentlyDeletedGVs", gv.String())
		changed = true
	}

	if changed {
		m.recentlyDeletedGVs.Store(newDeletedMap)
	}

	return changed
}

// diffGVs 比较新旧 GV map，返回被删除的 GV（在 old 中但不在 new 中），以及一个布尔值指示是否发生任何变更（新增或删除）。
// 注意：仅比较长度不足以识别“等量替换”，因此循环仍需标记 hasChanges。
func diffGVs(old, new map[schema.GroupVersion]struct{}) ([]schema.GroupVersion, bool) {
	var deletedGVs []schema.GroupVersion
	hasChanges := len(old) != len(new)

	// 找出已删除 GV（在旧集合但不在新集合）
	for gv := range old {
		if _, exists := new[gv]; !exists {
			deletedGVs = append(deletedGVs, gv)
			hasChanges = true
		}
	}

	return deletedGVs, hasChanges
}

// RunPeerDiscoveryRefilter 运行 Peer Discovery Re-filter worker。
// 该 worker 用排除集过滤 peer discovery 缓存。
func (m *GVExclusionManager) RunPeerDiscoveryRefilter(ctx context.Context) {
	defer m.refilterQueue.ShutDown()

	klog.Info("Starting Peer Discovery Re-filter worker")
	go wait.UntilWithContext(ctx, m.runRefilterWorker, time.Second)

	<-ctx.Done()
	klog.Info("Peer Discovery Re-filter workers stopped")
}

// runRefilterWorker 循环处理重过滤任务。
func (m *GVExclusionManager) runRefilterWorker(ctx context.Context) {
	for m.processNextRefilter(ctx) {
	}
}

// processNextRefilter 取一个重过滤任务；返回 false 表示应退出。
func (m *GVExclusionManager) processNextRefilter(ctx context.Context) bool {
	key, shutdown := m.refilterQueue.Get()
	if shutdown {
		return false
	}
	defer m.refilterQueue.Done(key)

	select {
	case <-ctx.Done():
		return false
	default:
	}

	m.refilterPeerDiscoveryCache()
	return true
}

// refilterPeerDiscoveryCache 读取原始 peer discovery 缓存，
// 应用排除规则后把结果存入过滤缓存。
func (m *GVExclusionManager) refilterPeerDiscoveryCache() {
	var cacheMap map[string]PeerDiscoveryCacheEntry
	if m.rawPeerDiscoveryCache != nil {
		if rawCache := m.rawPeerDiscoveryCache.Load(); rawCache != nil {
			cacheMap, _ = rawCache.(map[string]PeerDiscoveryCacheEntry)
		}
	}

	// 原始缓存为空时也要发布空结果，避免客户端继续看到过期内容。
	if len(cacheMap) == 0 {
		m.filteredPeerDiscoveryCache.Store(map[string]PeerDiscoveryCacheEntry{})
		klog.V(4).Infof("Raw peer discovery cache is empty or unavailable")
		return
	}

	filteredCache := m.filterPeerDiscoveryCache(cacheMap)
	m.filteredPeerDiscoveryCache.Store(filteredCache)

	// 通知发现聚合层缓存失效，以便尽快重新读取过滤后的结果。
	if m.invalidationCallback != nil {
		if callback := m.invalidationCallback.Load(); callback != nil {
			(*callback)()
		}
	}

	klog.V(4).Infof("Peer discovery cache re-filtered, %d GVs in exclusion set", len(m.getExclusionSet()))

}

// filterPeerDiscoveryCache 对传入的对等缓存应用当前排除集；无排除项时原样返回，避免不必要的 map 复制。
func (m *GVExclusionManager) filterPeerDiscoveryCache(cacheMap map[string]PeerDiscoveryCacheEntry) map[string]PeerDiscoveryCacheEntry {
	exclusionSet := m.getExclusionSet()
	if len(exclusionSet) == 0 {
		return cacheMap
	}

	filtered := make(map[string]PeerDiscoveryCacheEntry, len(cacheMap))
	for peerID, entry := range cacheMap {
		filtered[peerID] = m.filterPeerCacheEntry(entry, exclusionSet)
	}

	return filtered
}

// filterPeerCacheEntry 对单个 peer 的缓存条目应用 GV 排除。
func (m *GVExclusionManager) filterPeerCacheEntry(
	entry PeerDiscoveryCacheEntry,
	exclusionSet map[schema.GroupVersion]struct{},
) PeerDiscoveryCacheEntry {
	filteredGVRs := make(map[schema.GroupVersionResource]bool, len(entry.GVRs))
	anyExcluded := false
	for existingGVR, v := range entry.GVRs {
		gv := schema.GroupVersion{Group: existingGVR.Group, Version: existingGVR.Version}
		if _, excluded := exclusionSet[gv]; excluded {
			anyExcluded = true
			continue
		}
		filteredGVRs[existingGVR] = v
	}

	// 若没有任何 GVR 被排除，说明排除集与该对等节点的 GV 无交集，可直接原样返回条目而无需过滤 GroupDiscovery。
	// 节省一次 GroupDiscovery 的深度遍历。
	if !anyExcluded {
		return entry
	}
	filteredGroups := m.filterGroupDiscovery(entry.GroupDiscovery, exclusionSet)

	return PeerDiscoveryCacheEntry{
		GVRs:           filteredGVRs,
		GroupDiscovery: filteredGroups,
	}
}

// filterGroupDiscovery 过滤组发现条目，移除被排除的 GV；若某个组的所有版本都被排除，整个组也应从结果中移除，避免暴露空组。
func (m *GVExclusionManager) filterGroupDiscovery(
	groupDiscoveries []apidiscoveryv2.APIGroupDiscovery,
	exclusionSet map[schema.GroupVersion]struct{},
) []apidiscoveryv2.APIGroupDiscovery {
	var filteredDiscovery []apidiscoveryv2.APIGroupDiscovery
	for _, groupDiscovery := range groupDiscoveries {
		filteredGroup := apidiscoveryv2.APIGroupDiscovery{
			ObjectMeta: groupDiscovery.ObjectMeta,
		}

		for _, version := range groupDiscovery.Versions {
			gv := schema.GroupVersion{Group: groupDiscovery.Name, Version: version.Version}
			if _, found := exclusionSet[gv]; found {
				// 该版本被排除，跳过
				continue
			}
			filteredGroup.Versions = append(filteredGroup.Versions, version)
		}

		// 仅当 group 还有剩余版本时才加入最终列表
		if len(filteredGroup.Versions) > 0 {
			filteredDiscovery = append(filteredDiscovery, filteredGroup)
		}
	}
	return filteredDiscovery
}

// loadCurrentlyActiveGVs 读取活跃 GV 快照；类型不符时返回空 map 兜底。
func (m *GVExclusionManager) loadCurrentlyActiveGVs() map[schema.GroupVersion]struct{} {
	if val := m.currentlyActiveGVs.Load(); val != nil {
		if gvMap, ok := val.(map[schema.GroupVersion]struct{}); ok {
			return gvMap
		}
	}
	return map[schema.GroupVersion]struct{}{}
}

// loadRecentlyDeletedGVs 读取最近删除 GV 快照；类型不符时返回空 map 兜底。
func (m *GVExclusionManager) loadRecentlyDeletedGVs() map[schema.GroupVersion]time.Time {
	if val := m.recentlyDeletedGVs.Load(); val != nil {
		if gvMap, ok := val.(map[schema.GroupVersion]time.Time); ok {
			return gvMap
		}
	}
	return map[schema.GroupVersion]time.Time{}
}

// TriggerRefilter 触发 refilter worker，把排除规则应用到过滤缓存。
// peerLeaseQueue 更新原始缓存后应调用它。
func (m *GVExclusionManager) TriggerRefilter() {
	m.refilterQueue.Add("refilter")
}
