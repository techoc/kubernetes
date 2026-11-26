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

	"k8s.io/klog/v2"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/discovery"
)

// shortcutExpander 是用于 Kubernetes 资源的 RESTMapper：先展开资源短名，再调用被包装的 mapper。
//
// 它把用户输入的短名（如 "deploy"、"cm"）展开为完整资源名后，再委托给底层
// RESTMapper，因此兼容 kubectl 风格的简写。
type shortcutExpander struct {
	RESTMapper meta.RESTMapperWithContext

	discoveryClient discovery.DiscoveryInterfaceWithContext

	// warningHandler 在短名存在多个候选时收到告警；为 nil 表示静默。
	warningHandler func(string)
}

var _ meta.ResettableRESTMapper = shortcutExpander{}
var _ meta.ResettableRESTMapperWithContext = shortcutExpander{}

// NewShortcutExpander 给 RESTMapper 包一层，把通过发现得到的短名展开为完整资源名。
//
// NewShortcutExpanderWithContext 支持上下文日志与取消，应优先使用。
//
// 上下文日志：请改用 NewShortcutExpanderWithContext。
func NewShortcutExpander(delegate meta.RESTMapper, client discovery.DiscoveryInterface, warningHandler func(string)) meta.RESTMapper {
	return newShortcutExpander(meta.ToRESTMapperWithContext(delegate), discovery.ToDiscoveryInterfaceWithContext(client), warningHandler)
}

// NewShortcutExpanderWithContext 给 RESTMapper 包一层，把通过发现得到的短名展开为完整资源名。
func NewShortcutExpanderWithContext(delegate meta.RESTMapperWithContext, client discovery.DiscoveryInterfaceWithContext, warningHandler func(string)) meta.RESTMapperWithContext {
	return newShortcutExpander(delegate, client, warningHandler)
}

func newShortcutExpander(delegate meta.RESTMapperWithContext, client discovery.DiscoveryInterfaceWithContext, warningHandler func(string)) shortcutExpander {
	return shortcutExpander{RESTMapper: delegate, discoveryClient: client, warningHandler: warningHandler}
}

// KindFor 实现 meta.RESTMapper。
func (e shortcutExpander) KindFor(resource schema.GroupVersionResource) (schema.GroupVersionKind, error) {
	return e.KindForWithContext(context.Background(), resource)
}

// KindForWithContext 实现 meta.RESTMapperWithContext。
func (e shortcutExpander) KindForWithContext(ctx context.Context, resource schema.GroupVersionResource) (schema.GroupVersionKind, error) {
	// expandResourceShortcut 基于 discovery 缓存里读到的当前 API 资源来展开资源短名。
	// 对新建的 CRD 而言，这意味着我们可能没有最新的发现结果。
	// 在 k8s.io/cli-runtime/pkg/genericclioptions/config_flags.go#toRESTMapper 当前的接线下，
	// 使用的是 DeferredDiscoveryRESTMapper，它在 KindFor 失败时会清空缓存并从集群重新拉取全部数据
	// （见 k8s.io/client-go/restmapper/discovery.go#KindFor）。
	// 因此在遇到 NoMatchError 之后再调用一次 expandResourceShortcut，
	// 就应当能把 Kind 读出来交给用户，或者返回错误。
	// 这里的重试是有意为之：底层 mapper 在未命中时会刷新 discovery 缓存，
	// 第二次展开才能看到刚创建的 CRD。
	gvk, err := e.RESTMapper.KindForWithContext(ctx, e.expandResourceShortcut(ctx, resource))
	if meta.IsNoMatchError(err) {
		return e.RESTMapper.KindForWithContext(ctx, e.expandResourceShortcut(ctx, resource))
	}
	return gvk, err
}

// KindsFor 实现 meta.RESTMapper。
//
// KindsForWithContext 支持上下文日志与取消，应优先使用。
//
// 上下文日志：请改用 KindsForWithContext。
func (e shortcutExpander) KindsFor(resource schema.GroupVersionResource) ([]schema.GroupVersionKind, error) {
	return e.KindsForWithContext(context.Background(), resource)
}

// KindsForWithContext 实现 meta.RESTMapperWithContext。
func (e shortcutExpander) KindsForWithContext(ctx context.Context, resource schema.GroupVersionResource) ([]schema.GroupVersionKind, error) {
	return e.RESTMapper.KindsForWithContext(ctx, e.expandResourceShortcut(ctx, resource))
}

// ResourcesFor 实现 meta.RESTMapper。
//
// ResourcesForWithContext 支持上下文日志与取消，应优先使用。
//
// 上下文日志：请改用 ResourcesForWithContext。
func (e shortcutExpander) ResourcesFor(resource schema.GroupVersionResource) ([]schema.GroupVersionResource, error) {
	return e.ResourcesForWithContext(context.Background(), resource)
}

// ResourcesForWithContext 实现 meta.RESTMapperWithContext。
func (e shortcutExpander) ResourcesForWithContext(ctx context.Context, resource schema.GroupVersionResource) ([]schema.GroupVersionResource, error) {
	return e.RESTMapper.ResourcesForWithContext(ctx, e.expandResourceShortcut(ctx, resource))
}

// ResourceFor 实现 meta.RESTMapper。
//
// ResourceForWithContext 支持上下文日志与取消，应优先使用。
//
// 上下文日志：请改用 ResourceForWithContext。
func (e shortcutExpander) ResourceFor(resource schema.GroupVersionResource) (schema.GroupVersionResource, error) {
	return e.ResourceForWithContext(context.Background(), resource)
}

// ResourceForWithContext 实现 meta.RESTMapperWithContext。
func (e shortcutExpander) ResourceForWithContext(ctx context.Context, resource schema.GroupVersionResource) (schema.GroupVersionResource, error) {
	return e.RESTMapper.ResourceForWithContext(ctx, e.expandResourceShortcut(ctx, resource))
}

// ResourceSingularizer 实现 meta.RESTMapper。
//
// ResourceSingularizerWithContext 支持上下文日志与取消，应优先使用。
//
// 上下文日志：请改用 ResourceSingularizerWithContext。
func (e shortcutExpander) ResourceSingularizer(resource string) (string, error) {
	return e.ResourceSingularizerWithContext(context.Background(), resource)
}

// ResourceSingularizerWithContext 实现 meta.RESTMapperWithContext。
func (e shortcutExpander) ResourceSingularizerWithContext(ctx context.Context, resource string) (string, error) {
	// 先把短名展开，再用展开后的资源名去求单数形式。
	return e.RESTMapper.ResourceSingularizerWithContext(ctx, e.expandResourceShortcut(ctx, schema.GroupVersionResource{Resource: resource}).Resource)
}

// RESTMapping 实现 meta.RESTMapper。
//
// RESTMappingWithContext 支持上下文日志与取消，应优先使用。
//
// 上下文日志：请改用 RESTMappingWithContext。
func (e shortcutExpander) RESTMapping(gk schema.GroupKind, versions ...string) (*meta.RESTMapping, error) {
	return e.RESTMappingWithContext(context.Background(), gk, versions...)
}

// RESTMappingWithContext 实现 meta.RESTMapperWithContext 接口。
// 按 GroupKind 查询时不存在资源短名问题，直接透传。
func (e shortcutExpander) RESTMappingWithContext(ctx context.Context, gk schema.GroupKind, versions ...string) (*meta.RESTMapping, error) {
	return e.RESTMapper.RESTMappingWithContext(ctx, gk, versions...)
}

// RESTMappings 实现 meta.RESTMapper。
//
// RESTMappingsWithContext 支持上下文日志与取消，应优先使用。
//
// 上下文日志：请改用 RESTMappingsWithContext。
func (e shortcutExpander) RESTMappings(gk schema.GroupKind, versions ...string) ([]*meta.RESTMapping, error) {
	return e.RESTMappingsWithContext(context.Background(), gk, versions...)
}

// RESTMappingsWithContext 实现 meta.RESTMapperWithContext。
func (e shortcutExpander) RESTMappingsWithContext(ctx context.Context, gk schema.GroupKind, versions ...string) ([]*meta.RESTMapping, error) {
	return e.RESTMapper.RESTMappingsWithContext(ctx, gk, versions...)
}

// getShortcutMappings 返回资源短名与完整名的映射组：先从 API server 获取候选资源，
// 再追加硬编码列表以兼容不返回短名的旧服务端。结果按 group 优先级排序。
func (e shortcutExpander) getShortcutMappings(ctx context.Context) ([]*metav1.APIResourceList, []resourceShortcuts, error) {
	res := []resourceShortcuts{}
	// 获取服务端资源：这里可能同时返回错误 *和* 已经找到的结果，不必因为错误而失败。
	// 部分 group 不可用时仍可基于已取到的结果工作，因此仅记录日志。
	_, apiResList, err := e.discoveryClient.ServerGroupsAndResourcesWithContext(ctx)
	if err != nil {
		klog.FromContext(ctx).V(1).Info("Error loading discovery information", "err", err)
	}
	for _, apiResources := range apiResList {
		gv, err := schema.ParseGroupVersion(apiResources.GroupVersion)
		if err != nil {
			klog.FromContext(ctx).V(1).Info("Unable to parse group/version", "groupVersion", apiResources.GroupVersion, "err", err.Error())
			continue
		}
		for _, apiRes := range apiResources.APIResources {
			for _, shortName := range apiRes.ShortNames {
				rs := resourceShortcuts{
					ShortForm: schema.GroupResource{Group: gv.Group, Resource: shortName},
					LongForm:  schema.GroupResource{Group: gv.Group, Resource: apiRes.Name},
				}
				res = append(res, rs)
			}
		}
	}

	// 保持 discovery 返回的顺序，即 group 优先级顺序。
	return apiResList, res, nil
}

// expandResourceShortcut 在资源确为短名时返回 RESTMapper 可理解的完整名；未精确匹配时
// 尝试按 group 前缀匹配，最后才原样返回输入。
func (e shortcutExpander) expandResourceShortcut(ctx context.Context, resource schema.GroupVersionResource) schema.GroupVersionResource {
	// 获取短名映射，首个匹配即返回。
	if allResources, shortcutResources, err := e.getShortcutMappings(ctx); err == nil {
		// 已经与完整资源名精确匹配时就不要再做短名展开：
		// 完整资源名（或其单数形式）优先，绝不能被短名规则误改。
		for _, apiResources := range allResources {
			gv, err := schema.ParseGroupVersion(apiResources.GroupVersion)
			if err != nil {
				continue
			}
			if len(resource.Group) != 0 && resource.Group != gv.Group {
				continue
			}
			for _, apiRes := range apiResources.APIResources {
				if resource.Resource == apiRes.Name {
					return resource
				}
				if resource.Resource == apiRes.SingularName {
					return resource
				}
			}
		}

		found := false
		var rsc schema.GroupVersionResource
		// 对同一短名的多个候选只告警一次，避免刷屏。
		warnedAmbiguousShortcut := make(map[schema.GroupResource]bool)
		for _, item := range shortcutResources {
			if len(resource.Group) != 0 && resource.Group != item.ShortForm.Group {
				continue
			}
			if resource.Resource == item.ShortForm.Resource {
				if found {
					if item.LongForm.Group == rsc.Group && item.LongForm.Resource == rsc.Resource {
						// 同一 group/resource 在集群中注册多个版本是正常现象，不会导致短名歧义。
						continue
					}
					if !warnedAmbiguousShortcut[item.LongForm] {
						if e.warningHandler != nil {
							e.warningHandler(fmt.Sprintf("short name %q could also match lower priority resource %s", resource.Resource, item.LongForm.String()))
						}
						warnedAmbiguousShortcut[item.LongForm] = true
					}
					continue
				}
				rsc.Resource = item.LongForm.Resource
				rsc.Group = item.LongForm.Group
				found = true
			}
		}
		if found {
			return rsc
		}

		// 没找到精确匹配，于是按 group 前缀匹配，这样 autoscal 也能匹配到 autoscaling：
		// 前缀匹配只在调用方指定了 group 时进行，避免产生意外解析。
		if len(resource.Group) == 0 {
			return resource
		}
		for _, item := range shortcutResources {
			if !strings.HasPrefix(item.ShortForm.Group, resource.Group) {
				continue
			}
			if resource.Resource == item.ShortForm.Resource {
				resource.Resource = item.LongForm.Resource
				resource.Group = item.LongForm.Group
				return resource
			}
		}
	}

	return resource
}

// ResetWithContext 支持上下文日志与取消，应优先使用。
//
// 上下文日志：请改用 ResetWithContext。
func (e shortcutExpander) Reset() {
	e.ResetWithContext(context.Background())
}

// ResetWithContext 清除底层 mapper 的 discovery 缓存；本层自身不缓存任何状态。
func (e shortcutExpander) ResetWithContext(ctx context.Context) {
	meta.MaybeResetRESTMapperWithContext(ctx, e.RESTMapper)
}

// ResourceShortcuts 保存资源短名到完整名的映射信息。
type resourceShortcuts struct {
	ShortForm schema.GroupResource
	LongForm  schema.GroupResource
}
