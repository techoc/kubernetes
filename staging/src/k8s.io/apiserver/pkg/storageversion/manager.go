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
	_ "k8s.io/component-base/metrics/prometheus/workqueue" // 用于注册 workqueue 指标
	"k8s.io/klog/v2"
)

// ResourceInfo 包含把资源注册到 storage version API 所需的信息。
type ResourceInfo struct {
	GroupResource schema.GroupResource

	// EncodingVersion 是该资源写入 etcd 时使用的版本。
	EncodingVersion string
	// 用于计算可解码版本。必须在 InstallREST 注册完所有等价版本之后才能使用：
	// 等价资源映射需在所有版本注册完毕后才完整。
	EquivalentResourceMapper runtime.EquivalentResourceRegistry

	// DirectlyDecodableVersions 是 REST 存储的转换器能够转换的版本列表。即使某个版本
	// 并未对外提供服务（如 apiextensions.k8s.io/v1beta1），也会包含在其中。
	DirectlyDecodableVersions []schema.GroupVersion

	// ServedVersions 列出 GroupResource 所有对外提供服务的版本。注意：服务端可能能够
	// 解码某个版本，却并不对外提供该版本。
	ServedVersions []string
}

// Manager 记录需要更新 StorageVersion 的资源，并提供更新这些 StorageVersion 的方法。
//
// StorageVersion 对象用于让存储迁移器判断某资源是否已全部迁移到目标版本；
// 在更新完成前，写请求可能被阻塞（见 Completed）。
type Manager interface {
	// AddResourceInfo 记录需要更新 StorageVersion 的资源
	AddResourceInfo(resources ...*ResourceInfo)
	// UpdateStorageVersions 尝试更新已记录资源的 StorageVersion
	UpdateStorageVersions(kubeAPIServerClientConfig *rest.Config, apiserverID string)
	// PendingUpdate 在给定资源的 StorageVersion 仍待更新时返回 true。
	PendingUpdate(gr schema.GroupResource) bool
	// LastUpdateError 返回更新给定资源 storage version 时最近一次遇到的错误。
	LastUpdateError(gr schema.GroupResource) error
	// Completed 在所有已记录资源的 StorageVersion 均更新完成时返回 true。
	Completed() bool
}

// 编译期断言：默认实现满足 Manager 接口。
var _ Manager = &defaultManager{}

// defaultManager 表示某个 apiserver 是否已完成其 storage version 的上报。
type defaultManager struct {
	// completed 用原子变量，因为它在请求热路径上被高频读取，不应争用互斥锁。
	completed atomic.Bool

	mu sync.RWMutex
	// managedResourceInfos 记录将在下一次 UpdateStorageVersions 调用中更新 StorageVersion 的 ResourceInfo
	managedResourceInfos map[*ResourceInfo]struct{}
	// managedStatus 记录每个 GroupResource 的 StorageVersion 更新状态。由于一个
	// ResourceInfo 可能展开为多个 GroupResource（如 ingresses.networking.k8s.io 与 ingresses.extensions），
	// 该 map 使得 API 请求处理过程中可以直接按 GroupResource 快速查询状态。
	managedStatus map[schema.GroupResource]*updateStatus
}

// updateStatus 记录单个 GroupResource 的更新结果：done 表示成功，lastErr 保留最近一次失败原因。
type updateStatus struct {
	done    bool
	lastErr error
}

// NewDefaultManager 创建一个新的 defaultManager。
func NewDefaultManager() Manager {
	s := &defaultManager{}
	s.completed.Store(false)
	s.managedResourceInfos = make(map[*ResourceInfo]struct{})
	s.managedStatus = make(map[schema.GroupResource]*updateStatus)
	return s
}

// AddResourceInfo 向 manager 添加 ResourceInfo。
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

// UpdateStorageVersions 尝试更新已记录资源的 StorageVersion。
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
	// 对列表排序以保证去重结果一致，从而避免创建不必要的重复 StorageVersion 对象。
	// 例如 extensions.ingresses 与 networking.k8s.io.ingresses 共享同一底层存储。
	// 若不排序，在 HA 集群中一个 apiserver 可能对 extensions.ingresses 去重并更新
	// StorageVersion，而另一个 apiserver 对 networking.k8s.io.ingresses 去重并更新
	// StorageVersion。存储迁移器按 GroupResource 迁移对象，此时两个 StorageVersion
	// 对象都带有 CommonEncodingVersion（各自注册了一个 server），会导致这些资源被迁移两次。
	sortResourceInfosByGroupResource(resources)
	for _, r := range dedupResourceInfos(resources) {
		decodableVersions := decodableVersions(r.DirectlyDecodableVersions, r.EquivalentResourceMapper, r.GroupResource)
		gr := r.GroupResource
		// Group 必须是合法的 DNS 子域（RFC 1123）：
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

// dedupResourceInfos 对共享同一底层存储的 ResourceInfo 去重。
// 同 Group 下不同 Version 的 ResourceInfo 共享底层存储；不同 Group 的
// ResourceInfo 也可能共享，例如 networking.k8s.io ingresses 与 extensions ingresses。
// StorageVersion manager 对等价的 Group 只需更新一个 StorageVersion。
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

// recordStatusSuccess 把已更新的 ResourceInfo 标记为完成。
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

// recordStatusFailure 记录更新 ResourceInfo 时的最近一次错误。
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

// PendingUpdate 返回某个资源的 StorageVersion 是否仍有待更新。
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

// LastUpdateError 返回更新给定资源 storage version 时最近一次遇到的错误。
func (s *defaultManager) LastUpdateError(gr schema.GroupResource) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if _, ok := s.managedStatus[gr]; !ok {
		return fmt.Errorf("couldn't find managed status for %v", gr)
	}
	return s.managedStatus[gr].lastErr
}

// setComplete 标记 StorageVersion 更新已完成，此后无需再阻塞写请求。
func (s *defaultManager) setComplete() {
	s.completed.Store(true)
}

// Completed 返回 StorageVersion 的更新是否已完成。
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
