/*
Copyright 2014 The Kubernetes Authors.

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

// TODO: move everything in this file to pkg/api/rest
package meta

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// restScope 是 RESTScope 接口的默认实现，只保存作用域名称，供 RESTMapping 表明
// 资源位于命名空间内还是集群根级。
//
// Implements RESTScope interface
type restScope struct {
	name RESTScopeName
}

// Name 返回该作用域的名称。
func (r *restScope) Name() RESTScopeName {
	return r.name
}

// RESTScopeNamespace 表示资源位于某个命名空间内，是绝大多数资源的默认作用域。
var RESTScopeNamespace = &restScope{
	name: RESTScopeNameNamespace,
}

// RESTScopeRoot 表示资源位于集群根级（如节点、持久卷等集群级资源）。
var RESTScopeRoot = &restScope{
	name: RESTScopeNameRoot,
}

// DefaultRESTMapper exposes mappings between the types defined in a
// runtime.Scheme. It assumes that all types defined the provided scheme
// can be mapped with the provided MetadataAccessor and Codec interfaces.
//
// The resource name of a Kind is defined as the lowercase,
// English-plural version of the Kind string.
// When converting from resource to Kind, the singular version of the
// resource name is also accepted for convenience.
//
// TODO: Only accept plural for some operations for increased control?
// (`get pod bar` vs `get pods bar`)
type DefaultRESTMapper struct {
	// defaultGroupVersions 是在未显式指定版本时，解析 RESTMapping 所用的版本优先顺序。
	defaultGroupVersions []schema.GroupVersion

	// resourceToKind 同时登记复数与单数资源，因此从资源名反查 Kind 时两种写法都能命中。
	resourceToKind map[schema.GroupVersionResource]schema.GroupVersionKind
	// kindToPluralResource 记录 Kind 到其复数资源的映射，是各查询方法的主要入口。
	kindToPluralResource map[schema.GroupVersionKind]schema.GroupVersionResource
	// kindToScope 记录每个 Kind 的作用域，RESTMapping 据此填充 Scope 字段。
	kindToScope map[schema.GroupVersionKind]RESTScope
	// 单数与复数的双向映射，供资源名归一化和 ResourceSingularizer 使用。
	singularToPlural map[schema.GroupVersionResource]schema.GroupVersionResource
	pluralToSingular map[schema.GroupVersionResource]schema.GroupVersionResource
}

// String 以 kindToPluralResource 的内容作为摘要，便于在日志和调试输出中识别映射器。
// 对 nil 指针返回 "<nil>"，避免调用方对未初始化的映射器调用时拿到空 map。
func (m *DefaultRESTMapper) String() string {
	if m == nil {
		return "<nil>"
	}
	return fmt.Sprintf("DefaultRESTMapper{kindToPluralResource=%v}", m.kindToPluralResource)
}

var _ RESTMapper = &DefaultRESTMapper{}
var _ RESTMapperWithContext = &DefaultRESTMapper{}
var _ fmt.Stringer = &DefaultRESTMapper{}

// NewDefaultRESTMapper initializes a mapping between Kind and APIVersion
// to a resource name and back based on the objects in a runtime.Scheme
// and the Kubernetes API conventions. Takes a group name, a priority list of the versions
// to search when an object has no default version (set empty to return an error),
// and a function that retrieves the correct metadata for a given version.
func NewDefaultRESTMapper(defaultGroupVersions []schema.GroupVersion) *DefaultRESTMapper {
	resourceToKind := make(map[schema.GroupVersionResource]schema.GroupVersionKind)
	kindToPluralResource := make(map[schema.GroupVersionKind]schema.GroupVersionResource)
	kindToScope := make(map[schema.GroupVersionKind]RESTScope)
	singularToPlural := make(map[schema.GroupVersionResource]schema.GroupVersionResource)
	pluralToSingular := make(map[schema.GroupVersionResource]schema.GroupVersionResource)
	// TODO: verify name mappings work correctly when versions differ

	return &DefaultRESTMapper{
		resourceToKind:       resourceToKind,
		kindToPluralResource: kindToPluralResource,
		kindToScope:          kindToScope,
		defaultGroupVersions: defaultGroupVersions,
		singularToPlural:     singularToPlural,
		pluralToSingular:     pluralToSingular,
	}
}

// Add 用启发式的复数/单数猜测注册一个 Kind，适合按 Kubernetes 命名约定自动推导资源名。
func (m *DefaultRESTMapper) Add(kind schema.GroupVersionKind, scope RESTScope) {
	plural, singular := UnsafeGuessKindToResource(kind)
	m.AddSpecific(kind, plural, singular, scope)
}

// AddSpecific 用调用方明确给出的复数与单数资源名注册 Kind，适用于猜测结果与实际
// API 资源名不一致的场景（例如不规则的复数形式）。
func (m *DefaultRESTMapper) AddSpecific(kind schema.GroupVersionKind, plural, singular schema.GroupVersionResource, scope RESTScope) {
	// 双向登记单复数，保证两种资源名都能作为查询入口，且复数名到 Kind 的反查始终可用。
	m.singularToPlural[singular] = plural
	m.pluralToSingular[plural] = singular

	// 单数与复数资源都映射到同一 Kind，这样用户无论用单数还是复数都能解析到类型。
	m.resourceToKind[singular] = kind
	m.resourceToKind[plural] = kind

	m.kindToPluralResource[kind] = plural
	m.kindToScope[kind] = scope
}

// unpluralizedSuffixes is a list of resource suffixes that are the same plural and singular
// This is only is only necessary because some bits of code are lazy and don't actually use the RESTMapper like they should.
// TODO eliminate this so that different callers can correctly map to resources.  This probably means updating all
// callers to use the RESTMapper they mean.
// 中文补充：这些后缀的单复数同形，额外加 "s" 会得到错误资源名，因此需要特判。
var unpluralizedSuffixes = []string{
	"endpoints",
}

// UnsafeGuessKindToResource converts Kind to a resource name.
// Broken. This method only "sort of" works when used outside of this package.  It assumes that Kinds and Resources match
// and they aren't guaranteed to do so.
// 中文补充：仅按 Kind 小写加复数字尾，不能处理不规则复数或与 Kind 不一致的资源名；
// 有精确资源名的场景应改用 AddSpecific。
func UnsafeGuessKindToResource(kind schema.GroupVersionKind) ( /*plural*/ schema.GroupVersionResource /*singular*/, schema.GroupVersionResource) {
	kindName := kind.Kind
	if len(kindName) == 0 {
		return schema.GroupVersionResource{}, schema.GroupVersionResource{}
	}
	// 约定资源名是 Kind 的全小写形式。
	singularName := strings.ToLower(kindName)
	singular := kind.GroupVersion().WithResource(singularName)

	// 单复数同形的后缀直接复用同一个资源名。
	for _, skip := range unpluralizedSuffixes {
		if strings.HasSuffix(singularName, skip) {
			return singular, singular
		}
	}

	// 英语复数的两条常见规则：s -> es，辅音+y -> ies；其余直接加 s。
	switch string(singularName[len(singularName)-1]) {
	case "s":
		return kind.GroupVersion().WithResource(singularName + "es"), singular
	case "y":
		return kind.GroupVersion().WithResource(strings.TrimSuffix(singularName, "y") + "ies"), singular
	}

	return kind.GroupVersion().WithResource(singularName + "s"), singular
}

// ResourceSingularizer implements RESTMapper
// It converts a resource name from plural to singular (e.g., from pods to pod)
// 中文补充：输入可以是单数或复数；若存在多个匹配，可能因无法唯一确定单数名而报错。
func (m *DefaultRESTMapper) ResourceSingularizer(resourceType string) (string, error) {
	partialResource := schema.GroupVersionResource{Resource: resourceType}
	resources, err := m.ResourcesFor(partialResource)
	if err != nil {
		return resourceType, err
	}

	// 遍历所有候选资源，检查它们推导出的单数名是否一致；不一致说明单数名有歧义。
	singular := schema.GroupVersionResource{}
	for _, curr := range resources {
		currSingular, ok := m.pluralToSingular[curr]
		if !ok {
			continue
		}
		if singular.Empty() {
			singular = currSingular
			continue
		}

		if currSingular.Resource != singular.Resource {
			return resourceType, fmt.Errorf("multiple possible singular resources (%v) found for %v", resources, resourceType)
		}
	}

	if singular.Empty() {
		return resourceType, fmt.Errorf("no singular of resource %v has been defined", resourceType)
	}

	return singular.Resource, nil
}

// ResourceSingularizerWithContext 是 ResourceSingularizer 的 context 版本；
// 默认实现忽略 ctx，仅为兼容 RESTMapperWithContext 接口。
func (m *DefaultRESTMapper) ResourceSingularizerWithContext(_ context.Context, resourceType string) (string, error) {
	return m.ResourceSingularizer(resourceType)
}

// coerceResourceForMatching makes the resource lower case and converts internal versions to unspecified (legacy behavior)
// 中文补充：为保证 "--api-version=__internal" 这类历史写法也能匹配到公开版本，先做归一化。
func coerceResourceForMatching(resource schema.GroupVersionResource) schema.GroupVersionResource {
	resource.Resource = strings.ToLower(resource.Resource)
	if resource.Version == runtime.APIVersionInternal {
		resource.Version = ""
	}

	return resource
}

// ResourcesFor 按输入的完整程度（group+version / group / version / 仅资源名）
// 查找所有匹配的复数与单数资源，并按 defaultGroupVersions 的优先级排序返回。
func (m *DefaultRESTMapper) ResourcesFor(input schema.GroupVersionResource) ([]schema.GroupVersionResource, error) {
	resource := coerceResourceForMatching(input)

	// 分别记录输入中出现了哪些维度，用来决定走精确匹配还是宽松匹配。
	hasResource := len(resource.Resource) > 0
	hasGroup := len(resource.Group) > 0
	hasVersion := len(resource.Version) > 0

	if !hasResource {
		return nil, fmt.Errorf("a resource must be present, got: %v", resource)
	}

	ret := []schema.GroupVersionResource{}
	switch {
	case hasGroup && hasVersion:
		// fully qualified.  Find the exact match
		// 三个维度都齐全时只接受完全相等（单数或复数均可）的条目，找到即停。
		for plural, singular := range m.pluralToSingular {
			if singular == resource {
				ret = append(ret, plural)
				break
			}
			if plural == resource {
				ret = append(ret, plural)
				break
			}
		}

	case hasGroup:
		// given a group, prefer an exact match.  If you don't find one, resort to a prefix match on group
		// 先按 GroupResource 精确匹配；记录是否命中，以决定是否需要退回到前缀匹配。
		foundExactMatch := false
		requestedGroupResource := resource.GroupResource()
		for plural, singular := range m.pluralToSingular {
			if singular.GroupResource() == requestedGroupResource {
				foundExactMatch = true
				ret = append(ret, plural)
			}
			if plural.GroupResource() == requestedGroupResource {
				foundExactMatch = true
				ret = append(ret, plural)
			}
		}

		// if you didn't find an exact match, match on group prefixing. This allows storageclass.storage to match
		// storageclass.storage.k8s.io
		if !foundExactMatch {
			for plural, singular := range m.pluralToSingular {
				if !strings.HasPrefix(plural.Group, requestedGroupResource.Group) {
					continue
				}
				// 只按资源名比较，允许调用方省略完整域名（如 storage 代表 storage.k8s.io）。
				if singular.Resource == requestedGroupResource.Resource {
					ret = append(ret, plural)
				}
				if plural.Resource == requestedGroupResource.Resource {
					ret = append(ret, plural)
				}
			}

		}

	case hasVersion:
		// 仅有版本时忽略 group，按版本+资源名匹配；这在无 group 的核心资源上很常见。
		for plural, singular := range m.pluralToSingular {
			if singular.Version == resource.Version && singular.Resource == resource.Resource {
				ret = append(ret, plural)
			}
			if plural.Version == resource.Version && plural.Resource == resource.Resource {
				ret = append(ret, plural)
			}
		}

	default:
		// 只有资源名时最宽松：跨 group/version 收集所有同名资源。
		for plural, singular := range m.pluralToSingular {
			if singular.Resource == resource.Resource {
				ret = append(ret, plural)
			}
			if plural.Resource == resource.Resource {
				ret = append(ret, plural)
			}
		}
	}

	if len(ret) == 0 {
		return nil, &NoResourceMatchError{PartialResource: resource}
	}

	// 排序保证结果稳定，并让调用方优先看到注册时偏好的 group/version。
	sort.Sort(resourceByPreferredGroupVersion{ret, m.defaultGroupVersions})
	return ret, nil
}

// ResourcesForWithContext 是 ResourcesFor 的 context 版本；默认实现忽略 ctx。
func (m *DefaultRESTMapper) ResourcesForWithContext(_ context.Context, input schema.GroupVersionResource) ([]schema.GroupVersionResource, error) {
	return m.ResourcesFor(input)
}

// ResourceFor 返回唯一的匹配资源；多于一个候选时返回 AmbiguousResourceError 而不是随意挑选。
func (m *DefaultRESTMapper) ResourceFor(resource schema.GroupVersionResource) (schema.GroupVersionResource, error) {
	resources, err := m.ResourcesFor(resource)
	if err != nil {
		return schema.GroupVersionResource{}, err
	}
	if len(resources) == 1 {
		return resources[0], nil
	}

	return schema.GroupVersionResource{}, &AmbiguousResourceError{PartialResource: resource, MatchingResources: resources}
}

// ResourceForWithContext 是 ResourceFor 的 context 版本；默认实现忽略 ctx。
func (m *DefaultRESTMapper) ResourceForWithContext(_ context.Context, resource schema.GroupVersionResource) (schema.GroupVersionResource, error) {
	return m.ResourceFor(resource)
}

// KindsFor 与 ResourcesFor 对应，按输入的完整程度查找匹配的 GroupVersionKind，
// 并按 defaultGroupVersions 的优先级排序后返回。
func (m *DefaultRESTMapper) KindsFor(input schema.GroupVersionResource) ([]schema.GroupVersionKind, error) {
	resource := coerceResourceForMatching(input)

	// 与 ResourcesFor 一致的维度判断，决定匹配策略的严格程度。
	hasResource := len(resource.Resource) > 0
	hasGroup := len(resource.Group) > 0
	hasVersion := len(resource.Version) > 0

	if !hasResource {
		return nil, fmt.Errorf("a resource must be present, got: %v", resource)
	}

	ret := []schema.GroupVersionKind{}
	switch {
	// fully qualified.  Find the exact match
	case hasGroup && hasVersion:
		// resourceToKind 同时登记了单数和复数，精确查表即可。
		kind, exists := m.resourceToKind[resource]
		if exists {
			ret = append(ret, kind)
		}

	case hasGroup:
		// 先按 GroupResource 精确匹配。
		foundExactMatch := false
		requestedGroupResource := resource.GroupResource()
		for currResource, currKind := range m.resourceToKind {
			if currResource.GroupResource() == requestedGroupResource {
				foundExactMatch = true
				ret = append(ret, currKind)
			}
		}

		// if you didn't find an exact match, match on group prefixing. This allows storageclass.storage to match
		// storageclass.storage.k8s.io
		if !foundExactMatch {
			for currResource, currKind := range m.resourceToKind {
				if !strings.HasPrefix(currResource.Group, requestedGroupResource.Group) {
					continue
				}
				if currResource.Resource == requestedGroupResource.Resource {
					ret = append(ret, currKind)
				}
			}

		}

	case hasVersion:
		// 仅指定版本时忽略 group，按版本与资源名匹配。
		for currResource, currKind := range m.resourceToKind {
			if currResource.Version == resource.Version && currResource.Resource == resource.Resource {
				ret = append(ret, currKind)
			}
		}

	default:
		// 只有资源名：在所有 group/version 中收集同名资源对应的 Kind。
		for currResource, currKind := range m.resourceToKind {
			if currResource.Resource == resource.Resource {
				ret = append(ret, currKind)
			}
		}
	}

	if len(ret) == 0 {
		// 注意这里用原始 input 而非归一化后的 resource 构造错误，便于调用方对照其输入排查。
		return nil, &NoResourceMatchError{PartialResource: input}
	}

	sort.Sort(kindByPreferredGroupVersion{ret, m.defaultGroupVersions})
	return ret, nil
}

// KindsForWithContext 是 KindsFor 的 context 版本；默认实现忽略 ctx。
func (m *DefaultRESTMapper) KindsForWithContext(_ context.Context, input schema.GroupVersionResource) ([]schema.GroupVersionKind, error) {
	return m.KindsFor(input)
}

// KindFor 返回唯一的匹配 Kind；存在多个候选时返回 AmbiguousResourceError。
func (m *DefaultRESTMapper) KindFor(resource schema.GroupVersionResource) (schema.GroupVersionKind, error) {
	kinds, err := m.KindsFor(resource)
	if err != nil {
		return schema.GroupVersionKind{}, err
	}
	if len(kinds) == 1 {
		return kinds[0], nil
	}

	return schema.GroupVersionKind{}, &AmbiguousResourceError{PartialResource: resource, MatchingKinds: kinds}
}

func (m *DefaultRESTMapper) KindForWithContext(_ context.Context, resource schema.GroupVersionResource) (schema.GroupVersionKind, error) {
	return m.KindFor(resource)
}

// kindByPreferredGroupVersion 按 sortOrder 中 group/version 的先后对 Kind 排序，
// 同一 group/version 内再按 Kind 名排序，从而保证查询结果稳定。
type kindByPreferredGroupVersion struct {
	list      []schema.GroupVersionKind
	sortOrder []schema.GroupVersion
}

// Len 返回待排序元素数量，实现 sort.Interface。
func (o kindByPreferredGroupVersion) Len() int { return len(o.list) }

// Swap 交换两个元素，实现 sort.Interface。
func (o kindByPreferredGroupVersion) Swap(i, j int) { o.list[i], o.list[j] = o.list[j], o.list[i] }

// Less 定义排序规则：相同 group/version 按 Kind 字典序；否则按 preferred 顺序。
// 未出现在 sortOrder 中的 group/version 视为最低优先级，排在已登记的之后。
func (o kindByPreferredGroupVersion) Less(i, j int) bool {
	lhs := o.list[i]
	rhs := o.list[j]
	if lhs == rhs {
		return false
	}

	if lhs.GroupVersion() == rhs.GroupVersion() {
		return lhs.Kind < rhs.Kind
	}

	// otherwise, the difference is in the GroupVersion, so we need to sort with respect to the preferred order
	lhsIndex := -1
	rhsIndex := -1

	for i := range o.sortOrder {
		if o.sortOrder[i] == lhs.GroupVersion() {
			lhsIndex = i
		}
		if o.sortOrder[i] == rhs.GroupVersion() {
			rhsIndex = i
		}
	}

	if rhsIndex == -1 {
		return true
	}

	return lhsIndex < rhsIndex
}

// resourceByPreferredGroupVersion 与 kindByPreferredGroupVersion 规则相同，
// 只是比较的是资源名，用于 ResourcesFor 的结果排序。
type resourceByPreferredGroupVersion struct {
	list      []schema.GroupVersionResource
	sortOrder []schema.GroupVersion
}

// Len 返回待排序元素数量，实现 sort.Interface。
func (o resourceByPreferredGroupVersion) Len() int { return len(o.list) }

// Swap 交换两个元素，实现 sort.Interface。
func (o resourceByPreferredGroupVersion) Swap(i, j int) { o.list[i], o.list[j] = o.list[j], o.list[i] }

// Less 定义排序规则：相同 group/version 按资源名字典序；否则按 preferred 顺序。
func (o resourceByPreferredGroupVersion) Less(i, j int) bool {
	lhs := o.list[i]
	rhs := o.list[j]
	if lhs == rhs {
		return false
	}

	if lhs.GroupVersion() == rhs.GroupVersion() {
		return lhs.Resource < rhs.Resource
	}

	// otherwise, the difference is in the GroupVersion, so we need to sort with respect to the preferred order
	lhsIndex := -1
	rhsIndex := -1

	for i := range o.sortOrder {
		if o.sortOrder[i] == lhs.GroupVersion() {
			lhsIndex = i
		}
		if o.sortOrder[i] == rhs.GroupVersion() {
			rhsIndex = i
		}
	}

	if rhsIndex == -1 {
		return true
	}

	return lhsIndex < rhsIndex
}

// RESTMapping returns a struct representing the resource path and conversion interfaces a
// RESTClient should use to operate on the provided group/kind in order of versions. If a version search
// order is not provided, the search order provided to DefaultRESTMapper will be used to resolve which
// version should be used to access the named group/kind.
// 中文补充：只返回第一个匹配项，是对已有多义结果的兼容处理；需要全部候选时用 RESTMappings。
func (m *DefaultRESTMapper) RESTMapping(gk schema.GroupKind, versions ...string) (*RESTMapping, error) {
	mappings, err := m.RESTMappings(gk, versions...)
	if err != nil {
		return nil, err
	}
	if len(mappings) == 0 {
		return nil, &NoKindMatchError{GroupKind: gk, SearchedVersions: versions}
	}
	// since we rely on RESTMappings method
	// take the first match and return to the caller
	// as this was the existing behavior.
	return mappings[0], nil
}

// RESTMappingWithContext 是 RESTMapping 的 context 版本；默认实现忽略 ctx。
func (m *DefaultRESTMapper) RESTMappingWithContext(_ context.Context, gk schema.GroupKind, versions ...string) (*RESTMapping, error) {
	return m.RESTMapping(gk, versions...)
}

// RESTMappings returns the RESTMappings for the provided group kind. If a version search order
// is not provided, the search order provided to DefaultRESTMapper will be used.
// 中文补充：每个返回项都包含资源名、GVK 与作用域；若某 GVK 缺少作用域信息则整体报错，
// 因为无法生成合法的 REST 路径。
func (m *DefaultRESTMapper) RESTMappings(gk schema.GroupKind, versions ...string) ([]*RESTMapping, error) {
	mappings := make([]*RESTMapping, 0)
	potentialGVK := make([]schema.GroupVersionKind, 0)
	hadVersion := false

	// Pick an appropriate version
	// 调用方显式给出的版本顺序即优先级；遇到第一个已注册的版本就停止，避免返回多义结果。
	for _, version := range versions {
		// 空版本和内部版本没有可用的 REST 路径，跳过。
		if len(version) == 0 || version == runtime.APIVersionInternal {
			continue
		}
		currGVK := gk.WithVersion(version)
		hadVersion = true
		if _, ok := m.kindToPluralResource[currGVK]; ok {
			potentialGVK = append(potentialGVK, currGVK)
			break
		}
	}
	// Use the default preferred versions
	// 调用方未指定版本（或指定的都无效）时，回落到映射器注册时配置的偏好版本。
	if !hadVersion && len(potentialGVK) == 0 {
		for _, gv := range m.defaultGroupVersions {
			if gv.Group != gk.Group {
				continue
			}
			potentialGVK = append(potentialGVK, gk.WithVersion(gv.Version))
		}
	}

	if len(potentialGVK) == 0 {
		return nil, &NoKindMatchError{GroupKind: gk, SearchedVersions: versions}
	}

	for _, gvk := range potentialGVK {
		//Ensure we have a REST mapping
		res, ok := m.kindToPluralResource[gvk]
		if !ok {
			continue
		}

		// Ensure we have a REST scope
		scope, ok := m.kindToScope[gvk]
		if !ok {
			return nil, fmt.Errorf("the provided version %q and kind %q cannot be mapped to a supported scope", gvk.GroupVersion(), gvk.Kind)
		}

		mappings = append(mappings, &RESTMapping{
			Resource:         res,
			GroupVersionKind: gvk,
			Scope:            scope,
		})
	}

	if len(mappings) == 0 {
		// 走到这里说明 Kind 已注册但资源映射缺失，用 Group/Kind 组合报告更贴近问题本身。
		return nil, &NoResourceMatchError{PartialResource: schema.GroupVersionResource{Group: gk.Group, Resource: gk.Kind}}
	}
	return mappings, nil
}

// RESTMappingsWithContext 是 RESTMappings 的 context 版本；默认实现忽略 ctx。
func (m *DefaultRESTMapper) RESTMappingsWithContext(_ context.Context, gk schema.GroupKind, versions ...string) ([]*RESTMapping, error) {
	return m.RESTMappings(gk, versions...)
}

// MaybeResetRESTMapper calls Reset() on the mapper if it is a ResettableRESTMapper or
// ResetWithContext() if it is a ResettableRESTMapperWithContext.
//
// MaybeResetRESTMapperWithContext is a better alternative because it supports contextual logging and cancellation.
//
// Contextual logging: Use MaybeResetRESTMapperWithContext instead.
// 中文补充：对不支持重置的映射器静默忽略，调用方无需先做类型断言。
func MaybeResetRESTMapper(mapper RESTMapper) {
	maybeReset(context.Background(), mapper)
}

// MaybeResetRESTMapperWithContext calls Reset() on the mapper if it is a ResettableRESTMapper or
// ResetWithContext() if it is a ResettableRESTMapperWithContext.
// 中文补充：优先使用支持 context 的重置方法，以便日志与取消信号能够传递。
func MaybeResetRESTMapperWithContext(ctx context.Context, mapper RESTMapperWithContext) {
	maybeReset(ctx, mapper)
}

// maybeReset 依次尝试支持 context 和不支持 context 的重置接口；两者都不实现时不做任何事。
func maybeReset(ctx context.Context, mapper any) {
	if m, ok := mapper.(ResettableRESTMapperWithContext); ok {
		m.ResetWithContext(ctx)
		return
	}
	if m, ok := mapper.(ResettableRESTMapper); ok {
		m.Reset()
		return
	}
}
