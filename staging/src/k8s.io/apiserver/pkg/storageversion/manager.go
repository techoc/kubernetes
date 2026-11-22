/*
Copyright 2020 The Kubernetes Authors.

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

package storageversion

import (
	"fmt"
	"sort"
	"sync"
	"sync/atomic"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	_ "k8s.io/component-base/metrics/prometheus/workqueue" // for workqueue metric registration
	"k8s.io/klog/v2"
)

// ResourceInfo contains the information to register the resource to the
// storage version API.
type ResourceInfo struct {
	GroupResource schema.GroupResource

	// EncodingVersion 是该资源写入 etcd 时使用的版本。
	EncodingVersion string
	// Used to calculate decodable versions. Can only be used after all
	// equivalent versions are registered by InstallREST.
	//
	// 依赖 InstallREST 完成后才能使用：等价资源映射需在所有版本注册完毕后才完整。
	EquivalentResourceMapper runtime.EquivalentResourceRegistry

	// DirectlyDecodableVersions is a list of versions that the converter for REST storage knows how to convert.  This
	// contains items like apiextensions.k8s.io/v1beta1 even if we don't serve that version.
	DirectlyDecodableVersions []schema.GroupVersion

	// ServedVersions holds a list of all versions of GroupResource that are served.  Note that a server may be able to
	// decode a particular version, but still not serve it.
	ServedVersions []string
}

// Manager records the resources whose StorageVersions need updates, and provides a method to update those StorageVersions.
//
// StorageVersion 对象用于让存储迁移器判断某资源是否已全部迁移到目标版本；
// 在更新完成前，写请求可能被阻塞（见 Completed）。
type Manager interface {
	// AddResourceInfo records resources whose StorageVersions need updates
	AddResourceInfo(resources ...*ResourceInfo)
	// UpdateStorageVersions tries to update the StorageVersions of the recorded resources
	UpdateStorageVersions(kubeAPIServerClientConfig *rest.Config, apiserverID string)
	// PendingUpdate returns true if the StorageVersion of the given resource is still pending update.
	PendingUpdate(gr schema.GroupResource) bool
	// LastUpdateError returns the last error hit when updating the storage version of the given resource.
	LastUpdateError(gr schema.GroupResource) error
	// Completed returns true if updating StorageVersions of all recorded resources has completed.
	Completed() bool
}

// 编译期断言：默认实现满足 Manager 接口。
var _ Manager = &defaultManager{}

// defaultManager indicates if an apiserver has completed reporting its storage versions.
type defaultManager struct {
	// completed 用原子变量，因为它在请求热路径上被高频读取，不应争用互斥锁。
	completed atomic.Bool

	mu sync.RWMutex
	// managedResourceInfos records the ResourceInfos whose StorageVersions will get updated in the next
	// UpdateStorageVersions call
	managedResourceInfos map[*ResourceInfo]struct{}
	// managedStatus records the update status of StorageVersion for each GroupResource. Since one
	// ResourceInfo may expand into multiple GroupResource (e.g. ingresses.networking.k8s.io and ingresses.extensions),
	// this map allows quick status lookup for a GroupResource, during API request handling.
	managedStatus map[schema.GroupResource]*updateStatus
}

// updateStatus 记录单个 GroupResource 的更新结果：done 表示成功，lastErr 保留最近一次失败原因。
type updateStatus struct {
	done    bool
	lastErr error
}

// NewDefaultManager creates a new defaultManager.
func NewDefaultManager() Manager {
	s := &defaultManager{}
	s.completed.Store(false)
	s.managedResourceInfos = make(map[*ResourceInfo]struct{})
	s.managedStatus = make(map[schema.GroupResource]*updateStatus)
	return s
}

// AddResourceInfo adds ResourceInfo to the manager.
func (s *defaultManager) AddResourceInfo(resources ...*ResourceInfo) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, r := range resources {
		s.managedResourceInfos[r] = struct{}{}
		s.addPendingManagedStatusLocked(r)
	}
}

// addPendingManagedStatusLocked 为 ResourceInfo 的所有等价 GroupResource 建立初始（未完成）状态。
// 调用方必须持有写锁。
func (s *defaultManager) addPendingManagedStatusLocked(r *ResourceInfo) {
	gvrs := r.EquivalentResourceMapper.EquivalentResourcesFor(r.GroupResource.WithVersion(""), "")
	for _, gvr := range gvrs {
		gr := gvr.GroupResource()
		if _, ok := s.managedStatus[gr]; !ok {
			s.managedStatus[gr] = &updateStatus{}
		}
	}
}

// UpdateStorageVersions tries to update the StorageVersions of the recorded resources
//
// 该方法在后台周期性调用；任何失败都会保留 PendingUpdate 状态并在下轮重试，
// 只有全部成功才置 Completed，从而避免在存储版本未对齐时放行写请求。
func (s *defaultManager) UpdateStorageVersions(kubeAPIServerClientConfig *rest.Config, serverID string) {
	clientset, err := kubernetes.NewForConfig(kubeAPIServerClientConfig)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("failed to get clientset: %v", err))
		return
	}
	sc := clientset.InternalV1alpha1().StorageVersions()

	s.mu.RLock()
	resources := []ResourceInfo{}
	for resource := range s.managedResourceInfos {
		resources = append(resources, *resource)
	}
	s.mu.RUnlock()
	hasFailure := false
	// Sorting the list to make sure we have a consistent dedup result, and
	// therefore avoid creating unnecessarily duplicated StorageVersion objects.
	// For example, extensions.ingresses and networking.k8s.io.ingresses share
	// the same underlying storage. Without sorting, in an HA cluster, one
	// apiserver may dedup and update StorageVersion for extensions.ingresses,
	// while another apiserver may dedup and update StorageVersion for
	// networking.k8s.io.ingresses. The storage migrator (which migrates objects
	// per GroupResource) will migrate these resources twice, since both
	// StorageVersion objects have CommonEncodingVersion (each with one server registered).
	sortResourceInfosByGroupResource(resources)
	for _, r := range dedupResourceInfos(resources) {
		decodableVersions := decodableVersions(r.DirectlyDecodableVersions, r.EquivalentResourceMapper, r.GroupResource)
		gr := r.GroupResource
		// Group must be a valid subdomain in DNS (RFC 1123)
		//
		// 核心组的 Group 为空，但 StorageVersion 资源要求合法 DNS 子域，故用 "core" 代替。
		if len(gr.Group) == 0 {
			gr.Group = "core"
		}

		servedVersions := r.ServedVersions

		if err := updateStorageVersionFor(sc, serverID, gr, r.EncodingVersion, decodableVersions, servedVersions); err != nil {
			utilruntime.HandleError(fmt.Errorf("failed to update storage version for %v: %v", r.GroupResource, err))
			s.recordStatusFailure(&r, err)
			hasFailure = true
			continue
		}
		klog.V(2).Infof("successfully updated storage version for %v", r.GroupResource)
		s.recordStatusSuccess(&r)
	}
	if hasFailure {
		return
	}
	klog.V(2).Infof("storage version updates complete")
	s.setComplete()
}

// dedupResourceInfos dedups ResourceInfos with the same underlying storage.
// ResourceInfos from the same Group with different Versions share the same underlying storage.
// ResourceInfos from different Groups may share the same underlying storage, e.g.
// networking.k8s.io ingresses and extensions ingresses. The StorageVersion manager
// only needs to update one StorageVersion for the equivalent Groups.
//
// 去重规则：若某 ResourceInfo 的所有等价 GroupResource 已被先前条目覆盖，则跳过；
// 否则记录其全部等价资源并保留该条目。输入需已排序以保证跨 apiserver 结果一致。
func dedupResourceInfos(infos []ResourceInfo) []ResourceInfo {
	var ret []ResourceInfo
	seen := make(map[schema.GroupResource]struct{})
	for _, info := range infos {
		gr := info.GroupResource
		if _, ok := seen[gr]; ok {
			continue
		}
		gvrs := info.EquivalentResourceMapper.EquivalentResourcesFor(gr.WithVersion(""), "")
		for _, gvr := range gvrs {
			seen[gvr.GroupResource()] = struct{}{}
		}
		ret = append(ret, info)
	}
	return ret
}

// sortResourceInfosByGroupResource 按 group、resource 排序，稳定去重结果。
func sortResourceInfosByGroupResource(infos []ResourceInfo) {
	sort.Sort(byGroupResource(infos))
}

// byGroupResource 实现 sort.Interface，按 Group 再按 Resource 升序。
type byGroupResource []ResourceInfo

// Len 返回元素数量。
func (s byGroupResource) Len() int { return len(s) }

// Less 先比较 Group，相等时比较 Resource。
func (s byGroupResource) Less(i, j int) bool {
	if s[i].GroupResource.Group == s[j].GroupResource.Group {
		return s[i].GroupResource.Resource < s[j].GroupResource.Resource
	}
	return s[i].GroupResource.Group < s[j].GroupResource.Group
}

// Swap 交换两个元素。
func (s byGroupResource) Swap(i, j int) { s[i], s[j] = s[j], s[i] }

// recordStatusSuccess marks updated ResourceInfo as completed.
func (s *defaultManager) recordStatusSuccess(r *ResourceInfo) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.recordStatusSuccessLocked(r)
}

// recordStatusSuccessLocked 把该 ResourceInfo 的全部等价资源标记为完成；调用方须持写锁。
func (s *defaultManager) recordStatusSuccessLocked(r *ResourceInfo) {
	gvrs := r.EquivalentResourceMapper.EquivalentResourcesFor(r.GroupResource.WithVersion(""), "")
	for _, gvr := range gvrs {
		s.recordSuccessGroupResourceLocked(gvr.GroupResource())
	}
}

// recordSuccessGroupResourceLocked 仅更新受管资源；未登记的资源直接忽略。
func (s *defaultManager) recordSuccessGroupResourceLocked(gr schema.GroupResource) {
	if _, ok := s.managedStatus[gr]; !ok {
		return
	}
	s.managedStatus[gr].done = true
	s.managedStatus[gr].lastErr = nil
}

// recordStatusFailure records latest error updating ResourceInfo.
func (s *defaultManager) recordStatusFailure(r *ResourceInfo, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.recordStatusFailureLocked(r, err)
}

// recordStatusFailureLocked 把错误传播到该 ResourceInfo 的全部等价资源；调用方须持写锁。
func (s *defaultManager) recordStatusFailureLocked(r *ResourceInfo, err error) {
	gvrs := r.EquivalentResourceMapper.EquivalentResourcesFor(r.GroupResource.WithVersion(""), "")
	for _, gvr := range gvrs {
		s.recordErrorGroupResourceLocked(gvr.GroupResource(), err)
	}
}

// recordErrorGroupResourceLocked 记录失败原因；不把 done 置 true，以便下轮重试。
func (s *defaultManager) recordErrorGroupResourceLocked(gr schema.GroupResource, err error) {
	if _, ok := s.managedStatus[gr]; !ok {
		return
	}
	s.managedStatus[gr].lastErr = err
}

// PendingUpdate returns if the StorageVersion of a resource is still wait to be updated.
//
// 未登记的资源视为无需更新（返回 false），避免对不存在的资源做写阻塞。
func (s *defaultManager) PendingUpdate(gr schema.GroupResource) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if _, ok := s.managedStatus[gr]; !ok {
		return false
	}
	return !s.managedStatus[gr].done
}

// LastUpdateError returns the last error hit when updating the storage version of the given resource.
func (s *defaultManager) LastUpdateError(gr schema.GroupResource) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if _, ok := s.managedStatus[gr]; !ok {
		return fmt.Errorf("couldn't find managed status for %v", gr)
	}
	return s.managedStatus[gr].lastErr
}

// setComplete marks the completion of updating StorageVersions. No write requests need to be blocked anymore.
func (s *defaultManager) setComplete() {
	s.completed.Store(true)
}

// Completed returns if updating StorageVersions has completed.
func (s *defaultManager) Completed() bool {
	return s.completed.Load()
}

// decodableVersions 合并“直接可解码”与“通过等价资源可解码”的版本，去重后返回字符串列表。
// 该列表会写入 StorageVersion 状态，供存储迁移器判断哪些版本的数据仍可读取。
func decodableVersions(directlyDecodableVersions []schema.GroupVersion, e runtime.EquivalentResourceRegistry, gr schema.GroupResource) []string {
	var versions []string
	for _, decodableVersions := range directlyDecodableVersions {
		versions = append(versions, decodableVersions.String())
	}

	decodingGVRs := e.EquivalentResourcesFor(gr.WithVersion(""), "")
	for _, v := range decodingGVRs {
		found := false
		for _, existingVersion := range versions {
			if existingVersion == v.GroupVersion().String() {
				found = true
			}
		}
		if found {
			continue
		}
		versions = append(versions, v.GroupVersion().String())
	}
	return versions
}
