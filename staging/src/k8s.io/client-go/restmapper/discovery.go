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

package restmapper

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/discovery"

	"k8s.io/klog/v2"
)

// APIGroupResources 把某个 API group 的发现信息（各版本及其资源）聚合在一起，
// 是构造 DeferredDiscoveryRESTMapper 的输入数据。
type APIGroupResources struct {
	Group metav1.APIGroup
	// 版本字符串（不含 group 前缀）到该版本 APIResource 列表的映射。
	VersionedResources map[string][]metav1.APIResource
}

// NewDiscoveryRESTMapper 基于传入的发现结果（groups 与 resources）返回一个
// PriorityRESTMapper。
// NewDiscoveryRESTMapperWithContext 是更好的替代：支持上下文日志与取消，
// 新代码应优先使用 ...WithContext 版本。
//
// Contextual logging: Use NewDiscoveryRESTMapperWithContext instead.
func NewDiscoveryRESTMapper(groupResources []*APIGroupResources) meta.RESTMapper {
	mappers, resourcePriority, kindPriority := newDiscoveryRESTMapper(groupResources)
	multiMapper := make(meta.MultiRESTMapper, len(mappers))
	for i, m := range mappers {
		multiMapper[i] = m
	}
	// PriorityRESTMapper 会按 resourcePriority/kindPriority 的顺序在各 mapper 中挑首个命中项，
	// 从而保证 /v1 与 preferred version 优先。
	return &meta.PriorityRESTMapper{
		Delegate:         multiMapper,
		ResourcePriority: resourcePriority,
		KindPriority:     kindPriority,
	}
}

// NewDiscoveryRESTMapperWithContext returns a PriorityRESTMapper based on the discovered
// groups and resources passed in.
func NewDiscoveryRESTMapperWithContext(groupResources []*APIGroupResources) meta.RESTMapperWithContext {
	mappers, resourcePriority, kindPriority := newDiscoveryRESTMapper(groupResources)
	multiMapper := make(meta.MultiRESTMapperWithContext, len(mappers))
	for i, m := range mappers {
		multiMapper[i] = m
	}
	return &meta.PriorityRESTMapperWithContext{
		Delegate:         multiMapper,
		ResourcePriority: resourcePriority,
		KindPriority:     kindPriority,
	}
}

// newDiscoveryRESTMapper 为每个 groupVersion 构造一个 DefaultRESTMapper，
// 同时生成全局的资源/类型优先级列表。
// 不变量：优先级列表中 /v1 永远排第一，其次是各 group 的 preferred version，
// 最后才是非首选版本与仅按 group 匹配的兜底项。
func newDiscoveryRESTMapper(groupResources []*APIGroupResources) ([]*meta.DefaultRESTMapper, []schema.GroupVersionResource, []schema.GroupVersionKind) {
	var unionMapper []*meta.DefaultRESTMapper

	var groupPriority []string
	// /v1 is special.  It should always come first
	resourcePriority := []schema.GroupVersionResource{{Group: "", Version: "v1", Resource: meta.AnyResource}}
	kindPriority := []schema.GroupVersionKind{{Group: "", Version: "v1", Kind: meta.AnyKind}}

	for _, group := range groupResources {
		groupPriority = append(groupPriority, group.Group.Name)

		// 确保 preferred version 排在最前：只有它确实出现在发现结果中时才加入优先级，避免排入空版本。
		if len(group.Group.PreferredVersion.Version) != 0 {
			preferred := group.Group.PreferredVersion.Version
			if _, ok := group.VersionedResources[preferred]; ok {
				resourcePriority = append(resourcePriority, schema.GroupVersionResource{
					Group:    group.Group.Name,
					Version:  group.Group.PreferredVersion.Version,
					Resource: meta.AnyResource,
				})

				kindPriority = append(kindPriority, schema.GroupVersionKind{
					Group:   group.Group.Name,
					Version: group.Group.PreferredVersion.Version,
					Kind:    meta.AnyKind,
				})
			}
		}

		for _, discoveryVersion := range group.Group.Versions {
			resources, ok := group.VersionedResources[discoveryVersion.Version]
			if !ok {
				continue
			}

			// Add non-preferred versions after the preferred version, in case there are resources that only exist in those versions
			if discoveryVersion.Version != group.Group.PreferredVersion.Version {
				resourcePriority = append(resourcePriority, schema.GroupVersionResource{
					Group:    group.Group.Name,
					Version:  discoveryVersion.Version,
					Resource: meta.AnyResource,
				})

				kindPriority = append(kindPriority, schema.GroupVersionKind{
					Group:   group.Group.Name,
					Version: discoveryVersion.Version,
					Kind:    meta.AnyKind,
				})
			}

			gv := schema.GroupVersion{Group: group.Group.Name, Version: discoveryVersion.Version}
			versionMapper := meta.NewDefaultRESTMapper([]schema.GroupVersion{gv})

			for _, resource := range resources {
				scope := meta.RESTScopeNamespace
				if !resource.Namespaced {
					scope = meta.RESTScopeRoot
				}

				// 名字含斜杠说明是子资源，无法直接映射为独立的 GVR，故跳过。
				if strings.Contains(resource.Name, "/") {
					continue
				}

				plural := gv.WithResource(resource.Name)
				singular := gv.WithResource(resource.SingularName)
				// this is for legacy resources and servers which don't list singular forms.  For those we must still guess.
				if len(resource.SingularName) == 0 {
					_, singular = meta.UnsafeGuessKindToResource(gv.WithKind(resource.Kind))
				}

				// 同时注册小写与原始大小写的 Kind，兼容历史客户端传入不同大小写的行为。
				versionMapper.AddSpecific(gv.WithKind(strings.ToLower(resource.Kind)), plural, singular, scope)
				versionMapper.AddSpecific(gv.WithKind(resource.Kind), plural, singular, scope)
				// TODO this is producing unsafe guesses that don't actually work, but it matches previous behavior
				versionMapper.Add(gv.WithKind(resource.Kind+"List"), scope)
			}
			// TODO why is this type not in discovery (at least for "v1")
			versionMapper.Add(gv.WithKind("List"), meta.RESTScopeRoot)
			unionMapper = append(unionMapper, versionMapper)
		}
	}

	// 最后追加仅按 group 匹配的兜底项：当请求只给出 group（未指定版本）时仍能命中。
	for _, group := range groupPriority {
		resourcePriority = append(resourcePriority, schema.GroupVersionResource{
			Group:    group,
			Version:  meta.AnyVersion,
			Resource: meta.AnyResource,
		})
		kindPriority = append(kindPriority, schema.GroupVersionKind{
			Group:   group,
			Version: meta.AnyVersion,
			Kind:    meta.AnyKind,
		})
	}

	return unionMapper, resourcePriority, kindPriority
}

// GetAPIGroupResources uses the provided discovery client to gather
// discovery information and populate a slice of APIGroupResources.
//
// GetAPIGroupResourcesWithContext is a better alternative because it supports contextual logging and cancellation.
//
// Contextual logging: Use GetAPIGroupResourcesWithContext instead.
func GetAPIGroupResources(cl discovery.DiscoveryInterface) ([]*APIGroupResources, error) {
	return GetAPIGroupResourcesWithContext(context.Background(), discovery.ToDiscoveryInterfaceWithContext(cl))
}

// GetAPIGroupResourcesWithContext uses the provided discovery client to gather
// discovery information and populate a slice of APIGroupResources.
func GetAPIGroupResourcesWithContext(ctx context.Context, cl discovery.DiscoveryInterfaceWithContext) ([]*APIGroupResources, error) {
	gs, rs, err := cl.ServerGroupsAndResourcesWithContext(ctx)
	// 只有部分结果时也直接返回错误，避免基于不完整映射做出误判。
	if rs == nil || gs == nil {
		return nil, err
		// TODO track the errors and update callers to handle partial errors.
	}
	// 先按 groupVersion 建索引，避免在每个 group/version 上重复线性查找。
	rsm := map[string]*metav1.APIResourceList{}
	for _, r := range rs {
		rsm[r.GroupVersion] = r
	}

	var result []*APIGroupResources
	for _, group := range gs {
		groupResources := &APIGroupResources{
			Group:              *group,
			VersionedResources: make(map[string][]metav1.APIResource),
		}
		for _, version := range group.Versions {
			resources, ok := rsm[version.GroupVersion]
			if !ok {
				// 该版本没有对应的资源列表（可能发现失败），跳过而不是塞入空切片。
				continue
			}
			groupResources.VersionedResources[version.Version] = resources.APIResources
		}
		result = append(result, groupResources)
	}
	return result, nil
}

// DeferredDiscoveryRESTMapper 是同时实现 RESTMapper 与 RESTMapperContext 的映射器，
// 会推迟初始化，直到第一次请求映射。
// 延迟初始化可以避免在启动阶段即对 API server 发起发现请求；
// 当映射失败且缓存已不新鲜时，它会失效缓存并重新发现。
type DeferredDiscoveryRESTMapper struct {
	initMu   sync.Mutex
	delegate meta.RESTMapperWithContext
	cl       discovery.CachedDiscoveryInterfaceWithContext
}

var (
	//nolint:staticcheck // Intentionally using the old interface here.
	_ meta.ResettableRESTMapper            = &DeferredDiscoveryRESTMapper{}
	_ meta.ResettableRESTMapperWithContext = &DeferredDiscoveryRESTMapper{}
	_ fmt.Stringer                         = &DeferredDiscoveryRESTMapper{}
)

// NewDeferredDiscoveryRESTMapper returns a
// DeferredDiscoveryRESTMapper that will lazily query the provided
// client for discovery information to do REST mappings.
//
// NewDeferredDiscoveryRESTMapperWithContext is a better alternative because it supports contextual logging and cancellation.
//
// Contextual logging: Use NewDeferredDiscoveryRESTMapperWithContext instead. NewDeferredDiscoveryRESTMapper will try to convert cl to discovery.CachedDiscoveryInterfaceWithContext and use a wrapper if that is not possible, but NewDeferredDiscoveryRESTMapperWithContext ensures that no such conversion is necessary.
func NewDeferredDiscoveryRESTMapper(cl discovery.CachedDiscoveryInterface) *DeferredDiscoveryRESTMapper {
	return &DeferredDiscoveryRESTMapper{
		cl: discovery.ToCachedDiscoveryInterfaceWithContext(cl),
	}
}

// NewDeferredDiscoveryRESTMapperWithContext returns a
// DeferredDiscoveryRESTMapper that will lazily query the provided
// client for discovery information to do REST mappings.
func NewDeferredDiscoveryRESTMapperWithContext(cl discovery.CachedDiscoveryInterfaceWithContext) *DeferredDiscoveryRESTMapper {
	return &DeferredDiscoveryRESTMapper{
		cl: cl,
	}
}

// getDelegate 在首次调用时用发现信息构造 delegate，并在互斥锁保护下缓存。
// 双检模式：先判断是否已初始化，未初始化才在持锁状态下构建，避免每次映射都重新发现。
func (d *DeferredDiscoveryRESTMapper) getDelegate(ctx context.Context) (meta.RESTMapperWithContext, error) {
	d.initMu.Lock()
	defer d.initMu.Unlock()

	if d.delegate != nil {
		return d.delegate, nil
	}

	groupResources, err := GetAPIGroupResourcesWithContext(ctx, d.cl)
	if err != nil {
		return nil, err
	}

	d.delegate = NewDiscoveryRESTMapperWithContext(groupResources)
	return d.delegate, nil
}

// Reset resets the internally cached Discovery information and will
// cause the next mapping request to re-discover.
func (d *DeferredDiscoveryRESTMapper) Reset() {
	d.ResetWithContext(context.Background())
}

// ResetWithContext resets the internally cached Discovery information and will
// cause the next mapping request to re-discover.
func (d *DeferredDiscoveryRESTMapper) ResetWithContext(ctx context.Context) {
	klog.FromContext(ctx).V(5).Info("Invalidating discovery information")

	d.initMu.Lock()
	defer d.initMu.Unlock()

	// 同时失效底层发现缓存与本地 delegate，确保下一次映射拿到的是重新发现的数据。
	d.cl.InvalidateWithContext(ctx)
	d.delegate = nil
}

// KindFor takes a partial resource and returns back the single match.
// It returns an error if there are multiple matches.
//
// KindForWithContext is a better alternative because it supports contextual logging and cancellation.
//
// Contextual logging: Use KindForWithContext instead.
func (d *DeferredDiscoveryRESTMapper) KindFor(resource schema.GroupVersionResource) (gvk schema.GroupVersionKind, err error) {
	return d.KindForWithContext(context.Background(), resource)
}

// KindForWithContext takes a partial resource and returns back the single match.
// It returns an error if there are multiple matches.
func (d *DeferredDiscoveryRESTMapper) KindForWithContext(ctx context.Context, resource schema.GroupVersionResource) (gvk schema.GroupVersionKind, err error) {
	del, err := d.getDelegate(ctx)
	if err != nil {
		return schema.GroupVersionKind{}, err
	}
	gvk, err = del.KindForWithContext(ctx, resource)
	// 映射失败且发现缓存已过期时，可能是新装 CRD 未被缓存，失效后重试一次。
	if err != nil && !d.cl.FreshWithContext(ctx) {
		d.ResetWithContext(ctx)
		gvk, err = d.KindForWithContext(ctx, resource)
	}
	return
}

// KindsFor takes a partial resource and returns back the list of
// potential kinds in priority order.
//
// KindsForWithContext is a better alternative because it supports contextual logging and cancellation.
//
// Contextual logging: Use KindsForWithContext instead.
func (d *DeferredDiscoveryRESTMapper) KindsFor(resource schema.GroupVersionResource) (gvks []schema.GroupVersionKind, err error) {
	return d.KindsForWithContext(context.Background(), resource)
}

// KindsForWithContext takes a partial resource and returns back the list of
// potential kinds in priority order.
func (d *DeferredDiscoveryRESTMapper) KindsForWithContext(ctx context.Context, resource schema.GroupVersionResource) (gvks []schema.GroupVersionKind, err error) {
	del, err := d.getDelegate(ctx)
	if err != nil {
		return nil, err
	}
	gvks, err = del.KindsForWithContext(ctx, resource)
	// 结果为空且缓存不新鲜时同样重试一次；空结果通常意味着资源刚被创建。
	if len(gvks) == 0 && !d.cl.FreshWithContext(ctx) {
		d.ResetWithContext(ctx)
		gvks, err = d.KindsForWithContext(ctx, resource)
	}
	return
}

// ResourceFor takes a partial resource and returns back the single
// match. It returns an error if there are multiple matches.
//
// ResourceForWithContext is a better alternative because it supports contextual logging and cancellation.
//
// Contextual logging: Use ResourceForWithContext instead.
func (d *DeferredDiscoveryRESTMapper) ResourceFor(input schema.GroupVersionResource) (gvr schema.GroupVersionResource, err error) {
	return d.ResourceForWithContext(context.Background(), input)
}

// ResourceForWithContext takes a partial resource and returns back the single
// match. It returns an error if there are multiple matches.
func (d *DeferredDiscoveryRESTMapper) ResourceForWithContext(ctx context.Context, input schema.GroupVersionResource) (gvr schema.GroupVersionResource, err error) {
	del, err := d.getDelegate(ctx)
	if err != nil {
		return schema.GroupVersionResource{}, err
	}
	gvr, err = del.ResourceForWithContext(ctx, input)
	// 失败且缓存不新鲜时重新发现并重试，覆盖新增资源类型的场景。
	if err != nil && !d.cl.FreshWithContext(ctx) {
		d.ResetWithContext(ctx)
		gvr, err = d.ResourceForWithContext(ctx, input)
	}
	return
}

// ResourcesFor takes a partial resource and returns back the list of
// potential resource in priority order.
//
// ResourcesForWithContext is a better alternative because it supports contextual logging and cancellation.
//
// Contextual logging: Use ResourcesForWithContext instead.
func (d *DeferredDiscoveryRESTMapper) ResourcesFor(input schema.GroupVersionResource) (gvrs []schema.GroupVersionResource, err error) {
	return d.ResourcesForWithContext(context.Background(), input)
}

// ResourcesForWithContext takes a partial resource and returns back the list of
// potential resource in priority order.
func (d *DeferredDiscoveryRESTMapper) ResourcesForWithContext(ctx context.Context, input schema.GroupVersionResource) (gvrs []schema.GroupVersionResource, err error) {
	del, err := d.getDelegate(ctx)
	if err != nil {
		return nil, err
	}
	gvrs, err = del.ResourcesForWithContext(ctx, input)
	// 空结果且缓存不新鲜时重试一次。
	if len(gvrs) == 0 && !d.cl.FreshWithContext(ctx) {
		d.ResetWithContext(ctx)
		gvrs, err = d.ResourcesForWithContext(ctx, input)
	}
	return
}

// RESTMapping identifies a preferred resource mapping for the
// provided group kind.
//
// RESTMappingWithContext is a better alternative because it supports contextual logging and cancellation.
//
// Contextual logging: Use RESTMappingWithContext instead.
func (d *DeferredDiscoveryRESTMapper) RESTMapping(gk schema.GroupKind, versions ...string) (m *meta.RESTMapping, err error) {
	return d.RESTMappingWithContext(context.Background(), gk, versions...)
}

// RESTMappingWithContext identifies a preferred resource mapping for the
// provided group kind.
func (d *DeferredDiscoveryRESTMapper) RESTMappingWithContext(ctx context.Context, gk schema.GroupKind, versions ...string) (m *meta.RESTMapping, err error) {
	del, err := d.getDelegate(ctx)
	if err != nil {
		return nil, err
	}
	m, err = del.RESTMappingWithContext(ctx, gk, versions...)
	// 失败且缓存不新鲜时重新发现并重试。
	if err != nil && !d.cl.FreshWithContext(ctx) {
		d.ResetWithContext(ctx)
		m, err = d.RESTMappingWithContext(ctx, gk, versions...)
	}
	return
}

// RESTMappings returns the RESTMappings for the provided group kind
// in a rough internal preferred order. If no kind is found, it will
// return a NoResourceMatchError.
//
// RESTMappingsWithContext is a better alternative because it supports contextual logging and cancellation.
//
// Contextual logging: Use RESTMappingsWithContext instead.
func (d *DeferredDiscoveryRESTMapper) RESTMappings(gk schema.GroupKind, versions ...string) (ms []*meta.RESTMapping, err error) {
	return d.RESTMappingsWithContext(context.Background(), gk, versions...)
}

// RESTMappingsWithContext returns the RESTMappings for the provided group kind
// in a rough internal preferred order. If no kind is found, it will
// return a NoResourceMatchError.
func (d *DeferredDiscoveryRESTMapper) RESTMappingsWithContext(ctx context.Context, gk schema.GroupKind, versions ...string) (ms []*meta.RESTMapping, err error) {
	del, err := d.getDelegate(ctx)
	if err != nil {
		return nil, err
	}
	ms, err = del.RESTMappingsWithContext(ctx, gk, versions...)
	// 空结果且缓存不新鲜时重新发现并重试。
	if len(ms) == 0 && !d.cl.FreshWithContext(ctx) {
		d.ResetWithContext(ctx)
		ms, err = d.RESTMappingsWithContext(ctx, gk, versions...)
	}
	return
}

// ResourceSingularizer converts a resource name from plural to
// singular (e.g., from pods to pod).
//
// ResourceSingularizerWithContext is a better alternative because it supports contextual logging and cancellation.
//
// Contextual logging: Use ResourceSingularizerWithContext instead.
func (d *DeferredDiscoveryRESTMapper) ResourceSingularizer(resource string) (singular string, err error) {
	return d.ResourceSingularizerWithContext(context.Background(), resource)
}

// ResourceSingularizerWithContext converts a resource name from plural to
// singular (e.g., from pods to pod).
func (d *DeferredDiscoveryRESTMapper) ResourceSingularizerWithContext(ctx context.Context, resource string) (singular string, err error) {
	del, err := d.getDelegate(ctx)
	if err != nil {
		return resource, err
	}
	singular, err = del.ResourceSingularizerWithContext(ctx, resource)
	// 失败且缓存不新鲜时重新发现并重试。
	if err != nil && !d.cl.FreshWithContext(ctx) {
		d.ResetWithContext(ctx)
		singular, err = d.ResourceSingularizerWithContext(ctx, resource)
	}
	return
}

// String 返回委托 mapper 的字符串形式；delegate 尚未初始化时会尝试用后台 context 初始化。
// 注意这是调试输出，可能触发一次发现请求。
func (d *DeferredDiscoveryRESTMapper) String() string {
	del, err := d.getDelegate(context.Background() /* hopefully we already have a delegate and don't need the context */)
	if err != nil {
		return fmt.Sprintf("DeferredDiscoveryRESTMapper{%v}", err)
	}
	return fmt.Sprintf("DeferredDiscoveryRESTMapper{\n\t%v\n}", del)
}
