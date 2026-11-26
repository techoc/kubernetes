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
// 上下文日志：请改用 NewDiscoveryRESTMapperWithContext。
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

// NewDiscoveryRESTMapperWithContext 根据传入的发现结果构造 PriorityRESTMapper。
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
	// /v1 是特例，必须始终排在最前。
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

			// 非首选版本排在首选之后，以覆盖仅存在于这些版本的资源。
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
				// 兼容旧资源与不返回单数形式的服务端，此时只能靠猜测。
				if len(resource.SingularName) == 0 {
					_, singular = meta.UnsafeGuessKindToResource(gv.WithKind(resource.Kind))
				}

				// 同时注册小写与原始大小写的 Kind，兼容历史客户端传入不同大小写的行为。
				versionMapper.AddSpecific(gv.WithKind(strings.ToLower(resource.Kind)), plural, singular, scope)
				versionMapper.AddSpecific(gv.WithKind(resource.Kind), plural, singular, scope)
				// TODO：这里产生的不安全猜测并不总是有效，但为保持旧行为暂不改动。
				versionMapper.Add(gv.WithKind(resource.Kind+"List"), scope)
			}
			// TODO：为何该类型不出现在发现结果中（至少 "v1" 下）？
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

// GetAPIGroupResources 用给定的 discovery 客户端收集发现信息，填充 APIGroupResources 切片。
//
// GetAPIGroupResourcesWithContext 支持上下文日志与取消，应优先使用。
//
// 上下文日志：请改用 GetAPIGroupResourcesWithContext。
func GetAPIGroupResources(cl discovery.DiscoveryInterface) ([]*APIGroupResources, error) {
	return GetAPIGroupResourcesWithContext(context.Background(), discovery.ToDiscoveryInterfaceWithContext(cl))
}

// GetAPIGroupResourcesWithContext 用给定的 discovery 客户端收集发现信息，填充 APIGroupResources 切片。
func GetAPIGroupResourcesWithContext(ctx context.Context, cl discovery.DiscoveryInterfaceWithContext) ([]*APIGroupResources, error) {
	gs, rs, err := cl.ServerGroupsAndResourcesWithContext(ctx)
	// 只有部分结果时也直接返回错误，避免基于不完整映射做出误判。
	if rs == nil || gs == nil {
		return nil, err
		// TODO：记录这些错误并让调用方处理部分失败。
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

// NewDeferredDiscoveryRESTMapper 返回 DeferredDiscoveryRESTMapper，首次映射时才惰性查询给定客户端的发现信息。
//
// NewDeferredDiscoveryRESTMapperWithContext 支持上下文日志与取消，应优先使用。
//
// 上下文日志：请改用 NewDeferredDiscoveryRESTMapperWithContext。当前函数会尝试把 cl 转换为 discovery.CachedDiscoveryInterfaceWithContext，失败时用包装器适配；WithContext 版本则无需转换。
func NewDeferredDiscoveryRESTMapper(cl discovery.CachedDiscoveryInterface) *DeferredDiscoveryRESTMapper {
	return &DeferredDiscoveryRESTMapper{
		cl: discovery.ToCachedDiscoveryInterfaceWithContext(cl),
	}
}

// NewDeferredDiscoveryRESTMapperWithContext 返回 DeferredDiscoveryRESTMapper，首次映射时才惰性查询给定客户端的发现信息。
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

// Reset 清空内部缓存的发现信息，使下一次映射请求重新发现。
func (d *DeferredDiscoveryRESTMapper) Reset() {
	d.ResetWithContext(context.Background())
}

// ResetWithContext 清空内部缓存的发现信息，使下一次映射请求重新发现。
func (d *DeferredDiscoveryRESTMapper) ResetWithContext(ctx context.Context) {
	klog.FromContext(ctx).V(5).Info("Invalidating discovery information")

	d.initMu.Lock()
	defer d.initMu.Unlock()

	// 同时失效底层发现缓存与本地 delegate，确保下一次映射拿到的是重新发现的数据。
	d.cl.InvalidateWithContext(ctx)
	d.delegate = nil
}

// KindFor 接受部分资源信息并返回唯一匹配；多个匹配时返回错误。
//
// KindForWithContext 支持上下文日志与取消，应优先使用。
//
// 上下文日志：请改用 KindForWithContext。
func (d *DeferredDiscoveryRESTMapper) KindFor(resource schema.GroupVersionResource) (gvk schema.GroupVersionKind, err error) {
	return d.KindForWithContext(context.Background(), resource)
}

// KindForWithContext 接受部分资源信息并返回唯一匹配；多个匹配时返回错误。
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

// KindsFor 接受部分资源信息，按优先级返回可能的 Kind 列表。
//
// KindsForWithContext 支持上下文日志与取消，应优先使用。
//
// 上下文日志：请改用 KindsForWithContext。
func (d *DeferredDiscoveryRESTMapper) KindsFor(resource schema.GroupVersionResource) (gvks []schema.GroupVersionKind, err error) {
	return d.KindsForWithContext(context.Background(), resource)
}

// KindsForWithContext 接受部分资源信息，按优先级返回可能的 Kind 列表。
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

// ResourceFor 接受部分资源信息并返回唯一匹配；多个匹配时返回错误。
//
// ResourceForWithContext 支持上下文日志与取消，应优先使用。
//
// 上下文日志：请改用 ResourceForWithContext。
func (d *DeferredDiscoveryRESTMapper) ResourceFor(input schema.GroupVersionResource) (gvr schema.GroupVersionResource, err error) {
	return d.ResourceForWithContext(context.Background(), input)
}

// ResourceForWithContext 接受部分资源信息并返回唯一匹配；多个匹配时返回错误。
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

// ResourcesFor 接受部分资源信息，按优先级返回可能的资源列表。
//
// ResourcesForWithContext 支持上下文日志与取消，应优先使用。
//
// 上下文日志：请改用 ResourcesForWithContext。
func (d *DeferredDiscoveryRESTMapper) ResourcesFor(input schema.GroupVersionResource) (gvrs []schema.GroupVersionResource, err error) {
	return d.ResourcesForWithContext(context.Background(), input)
}

// ResourcesForWithContext 接受部分资源信息，按优先级返回可能的资源列表。
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

// RESTMapping 为给定 GroupKind 找出首选资源映射。
//
// RESTMappingWithContext 支持上下文日志与取消，应优先使用。
//
// 上下文日志：请改用 RESTMappingWithContext。
func (d *DeferredDiscoveryRESTMapper) RESTMapping(gk schema.GroupKind, versions ...string) (m *meta.RESTMapping, err error) {
	return d.RESTMappingWithContext(context.Background(), gk, versions...)
}

// RESTMappingWithContext 为给定 GroupKind 找出首选资源映射。
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

// RESTMappings 按大致的内部优先级返回给定 GroupKind 的所有 RESTMappings；无匹配时返回 NoResourceMatchError。
//
// RESTMappingsWithContext 支持上下文日志与取消，应优先使用。
//
// 上下文日志：请改用 RESTMappingsWithContext。
func (d *DeferredDiscoveryRESTMapper) RESTMappings(gk schema.GroupKind, versions ...string) (ms []*meta.RESTMapping, err error) {
	return d.RESTMappingsWithContext(context.Background(), gk, versions...)
}

// RESTMappingsWithContext 按大致的内部优先级返回给定 GroupKind 的所有 RESTMappings；无匹配时返回 NoResourceMatchError。
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

// ResourceSingularizer 把资源名从复数转为单数（如 pods 转为 pod）。
//
// ResourceSingularizerWithContext 支持上下文日志与取消，应优先使用。
//
// 上下文日志：请改用 ResourceSingularizerWithContext。
func (d *DeferredDiscoveryRESTMapper) ResourceSingularizer(resource string) (singular string, err error) {
	return d.ResourceSingularizerWithContext(context.Background(), resource)
}

// ResourceSingularizerWithContext 把资源名从复数转为单数（如 pods 转为 pod）。
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
