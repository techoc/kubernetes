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

package meta

import (
	"context"
	"fmt"

	"k8s.io/apimachinery/pkg/runtime/schema"
)

// 优先级匹配规则使用的通配符常量；模式中的对应字段为通配符时，可匹配候选项的任意值。
const (
	AnyGroup    = "*"
	AnyVersion  = "*"
	AnyResource = "*"
	AnyKind     = "*"
)

// 编译期接口断言：确保优先级映射器完整实现 RESTMapper、重置接口和字符串格式化接口。
var (
	_ ResettableRESTMapper            = PriorityRESTMapper{}
	_ ResettableRESTMapperWithContext = PriorityRESTMapperWithContext{}
	_ fmt.Stringer                    = PriorityRESTMapperWithContext{}
)

// PriorityRESTMapper 是 RESTMapper 的包装器；当资源或 Kind 存在多个匹配项时，
// 它按配置的优先级模式逐步筛选，尝试选出唯一结果。
// 新代码优先使用 PriorityRESTMapperWithContext，以支持上下文日志和取消。
type PriorityRESTMapper struct {
	// Delegate 负责查找初始的 Kind 和 Resource 候选项。
	Delegate RESTMapper

	// ResourcePriority 是依次应用于资源候选项的优先级模式列表。
	// 每个模式都会缩小候选范围，直到只剩一个结果；没有匹配项时跳过该模式，
	// 匹配多个时则以这些匹配项作为下一模式的输入。
	ResourcePriority []schema.GroupVersionResource

	// KindPriority 是依次应用于 Kind 候选项的优先级模式列表。
	// 每个模式都会缩小候选范围，直到只剩一个结果；没有匹配项时跳过该模式，
	// 匹配多个时则以这些匹配项作为下一模式的输入。
	KindPriority []schema.GroupVersionKind
}

func (m PriorityRESTMapper) String() string {
	return fmt.Sprintf("PriorityRESTMapper{\n\t%v\n\t%v\n\t%v\n}", m.ResourcePriority, m.KindPriority, m.Delegate)
}

// PriorityRESTMapperWithContext 是支持 context 的 RESTMapper 包装器；存在多个资源或 Kind 候选项时，
// 它按配置的优先级模式逐步筛选，尝试选出唯一结果。
type PriorityRESTMapperWithContext struct {
	// Delegate 负责查找初始的 Kind 和 Resource 候选项，并接收调用方的 context。
	Delegate RESTMapperWithContext

	// ResourcePriority 是依次应用于资源候选项的优先级模式列表。
	// 每个模式都会缩小候选范围，直到只剩一个结果；没有匹配项时跳过该模式，
	// 匹配多个时则以这些匹配项作为下一模式的输入。
	ResourcePriority []schema.GroupVersionResource

	// KindPriority 是依次应用于 Kind 候选项的优先级模式列表。
	// 每个模式都会缩小候选范围，直到只剩一个结果；没有匹配项时跳过该模式，
	// 匹配多个时则以这些匹配项作为下一模式的输入。
	KindPriority []schema.GroupVersionKind
}

func (m PriorityRESTMapperWithContext) String() string {
	return fmt.Sprintf("PriorityRESTMapperWithContext{\n\t%v\n\t%v\n\t%v\n}", m.ResourcePriority, m.KindPriority, m.Delegate)
}

func ToPriorityRESTMapperWithContext(m PriorityRESTMapper) PriorityRESTMapperWithContext {
	// 优先使用支持 context 的实现；此函数用于将旧版 RESTMapper 适配到新接口。
	return PriorityRESTMapperWithContext{
		Delegate:         ToRESTMapperWithContext(m.Delegate),
		ResourcePriority: m.ResourcePriority,
		KindPriority:     m.KindPriority,
	}
}

// ResourceFor 先查找所有匹配资源，再按 ResourcePriority 模式逐步筛选，直到得到唯一结果。
//
// 新代码优先使用 PriorityRESTMapperWithContext.ResourceForWithContext，以支持上下文日志和取消。
func (m PriorityRESTMapper) ResourceFor(partiallySpecifiedResource schema.GroupVersionResource) (schema.GroupVersionResource, error) {
	return ToPriorityRESTMapperWithContext(m).ResourceForWithContext(context.Background(), partiallySpecifiedResource)
}

// KindFor 先查找所有匹配的 Kind，再按 KindPriority 模式逐步筛选，直到得到唯一结果。
//
// 新代码优先使用 PriorityRESTMapperWithContext.KindForWithContext，以支持上下文日志和取消。
func (m PriorityRESTMapper) KindFor(partiallySpecifiedResource schema.GroupVersionResource) (schema.GroupVersionKind, error) {
	return ToPriorityRESTMapperWithContext(m).KindForWithContext(context.Background(), partiallySpecifiedResource)
}

// 新代码优先使用 PriorityRESTMapperWithContext.RESTMappingWithContext，以支持上下文日志和取消。
func (m PriorityRESTMapper) RESTMapping(gk schema.GroupKind, versions ...string) (mapping *RESTMapping, err error) {
	return ToPriorityRESTMapperWithContext(m).RESTMappingWithContext(context.Background(), gk, versions...)
}

// 新代码优先使用 PriorityRESTMapperWithContext.RESTMappingsWithContext，以支持上下文日志和取消。
func (m PriorityRESTMapper) RESTMappings(gk schema.GroupKind, versions ...string) ([]*RESTMapping, error) {
	return m.Delegate.RESTMappings(gk, versions...)
}

// 新代码优先使用 PriorityRESTMapperWithContext.ResourceSingularizerWithContext，以支持上下文日志和取消。
func (m PriorityRESTMapper) ResourceSingularizer(resource string) (singular string, err error) {
	return m.Delegate.ResourceSingularizer(resource)
}

// 新代码优先使用 PriorityRESTMapperWithContext.ResourcesForWithContext，以支持上下文日志和取消。
func (m PriorityRESTMapper) ResourcesFor(partiallySpecifiedResource schema.GroupVersionResource) ([]schema.GroupVersionResource, error) {
	return m.Delegate.ResourcesFor(partiallySpecifiedResource)
}

// 新代码优先使用 PriorityRESTMapperWithContext.KindsForWithContext，以支持上下文日志和取消。
func (m PriorityRESTMapper) KindsFor(partiallySpecifiedResource schema.GroupVersionResource) (gvk []schema.GroupVersionKind, err error) {
	return m.Delegate.KindsFor(partiallySpecifiedResource)
}

// 新代码优先使用 PriorityRESTMapperWithContext.ResetWithContext，以支持上下文日志和取消。
func (m PriorityRESTMapper) Reset() {
	MaybeResetRESTMapper(m.Delegate)
}

// ResourceFor 先查找所有匹配资源，再按 ResourcePriority 模式逐步筛选，直到得到唯一结果。
func (m PriorityRESTMapperWithContext) ResourceForWithContext(ctx context.Context, partiallySpecifiedResource schema.GroupVersionResource) (schema.GroupVersionResource, error) {
	originalGVRs, originalErr := m.Delegate.ResourcesForWithContext(ctx, partiallySpecifiedResource)
	// Delegate 没有返回可用候选项时，原样返回其错误，避免用歧义错误掩盖真正原因。
	if originalErr != nil && len(originalGVRs) == 0 {
		return schema.GroupVersionResource{}, originalErr
	}
	// 只有一个候选项时无需消歧，保留 Delegate 返回的非致命错误。
	if len(originalGVRs) == 1 {
		return originalGVRs[0], originalErr
	}

	// 复制候选切片，避免筛选过程修改 Delegate 持有的底层数组。
	remainingGVRs := append([]schema.GroupVersionResource{}, originalGVRs...)
	for _, pattern := range m.ResourcePriority {
		matchedGVRs := []schema.GroupVersionResource{}
		for _, gvr := range remainingGVRs {
			if resourceMatches(pattern, gvr) {
				matchedGVRs = append(matchedGVRs, gvr)
			}
		}

		switch len(matchedGVRs) {
		case 0:
			// 当前模式没有匹配项，不缩小候选范围，继续尝试下一个模式。
			continue
		case 1:
			// 只剩一个匹配项，立即返回。
			return matchedGVRs[0], originalErr
		default:
			// 匹配项多于一个时，将其作为下一模式的候选集合，从而串联多个筛选条件。
			remainingGVRs = matchedGVRs
		}
	}

	return schema.GroupVersionResource{}, &AmbiguousResourceError{PartialResource: partiallySpecifiedResource, MatchingResources: originalGVRs}
}

// KindFor 先查找所有匹配的 Kind，再按 KindPriority 模式逐步筛选，直到得到唯一结果。
func (m PriorityRESTMapperWithContext) KindForWithContext(ctx context.Context, partiallySpecifiedResource schema.GroupVersionResource) (schema.GroupVersionKind, error) {
	originalGVKs, originalErr := m.Delegate.KindsForWithContext(ctx, partiallySpecifiedResource)
	// 与 ResourceForWithContext 相同的候选缩小策略，此处应用于 Kind。
	if originalErr != nil && len(originalGVKs) == 0 {
		return schema.GroupVersionKind{}, originalErr
	}
	if len(originalGVKs) == 1 {
		return originalGVKs[0], originalErr
	}

	// 复制候选切片，避免修改 Delegate 返回的底层数组。
	remainingGVKs := append([]schema.GroupVersionKind{}, originalGVKs...)
	for _, pattern := range m.KindPriority {
		matchedGVKs := []schema.GroupVersionKind{}
		for _, gvr := range remainingGVKs {
			if kindMatches(pattern, gvr) {
				matchedGVKs = append(matchedGVKs, gvr)
			}
		}

		switch len(matchedGVKs) {
		case 0:
			// 当前模式没有匹配项，不缩小候选范围，继续尝试下一个模式。
			continue
		case 1:
			// 只剩一个匹配项，立即返回。
			return matchedGVKs[0], originalErr
		default:
			// 匹配项多于一个时，将其作为下一模式的候选集合，从而串联多个筛选条件。
			remainingGVKs = matchedGVKs
		}
	}

	return schema.GroupVersionKind{}, &AmbiguousResourceError{PartialResource: partiallySpecifiedResource, MatchingKinds: originalGVKs}
}

// resourceMatches 判断资源是否符合优先级模式；模式中值为通配符的字段可匹配任意候选值。
func resourceMatches(pattern schema.GroupVersionResource, resource schema.GroupVersionResource) bool {
	if pattern.Group != AnyGroup && pattern.Group != resource.Group {
		return false
	}
	if pattern.Version != AnyVersion && pattern.Version != resource.Version {
		return false
	}
	if pattern.Resource != AnyResource && pattern.Resource != resource.Resource {
		return false
	}

	return true
}

// kindMatches 判断 Kind 是否符合优先级模式；模式中值为通配符的字段可匹配任意候选值。
func kindMatches(pattern schema.GroupVersionKind, kind schema.GroupVersionKind) bool {
	if pattern.Group != AnyGroup && pattern.Group != kind.Group {
		return false
	}
	if pattern.Version != AnyVersion && pattern.Version != kind.Version {
		return false
	}
	if pattern.Kind != AnyKind && pattern.Kind != kind.Kind {
		return false
	}

	return true
}

func (m PriorityRESTMapperWithContext) RESTMappingWithContext(ctx context.Context, gk schema.GroupKind, versions ...string) (mapping *RESTMapping, err error) {
	mappings, originalErr := m.Delegate.RESTMappingsWithContext(ctx, gk, versions...)
	// 没有候选映射时，原样返回 Delegate 的错误。
	if originalErr != nil && len(mappings) == 0 {
		return nil, originalErr
	}

	// 调用方显式指定的版本优先于映射器中配置的 KindPriority。
	priorities := m.KindPriority
	if len(versions) > 0 {
		// 显式版本优先级最高：为每个指定版本构造 Kind 使用通配符的模式，并放在原优先级列表之前。
		priorities = make([]schema.GroupVersionKind, 0, len(m.KindPriority)+len(versions))
		for _, version := range versions {
			gv := schema.GroupVersion{
				Version: version,
				Group:   gk.Group,
			}
			priorities = append(priorities, gv.WithKind(AnyKind))
		}
		priorities = append(priorities, m.KindPriority...)
	}

	remaining := append([]*RESTMapping{}, mappings...)
	for _, pattern := range priorities {
		var matching []*RESTMapping
		for _, m := range remaining {
			if kindMatches(pattern, m.GroupVersionKind) {
				matching = append(matching, m)
			}
		}

		switch len(matching) {
		case 0:
			// 当前模式没有匹配项，不缩小候选范围，继续尝试下一个模式。
			continue
		case 1:
			// 只剩一个匹配项，立即返回。
			return matching[0], originalErr
		default:
			// 匹配项多于一个时，将其作为下一模式的候选集合，从而串联多个筛选条件。
			remaining = matching
		}
	}
	if len(remaining) == 1 {
		return remaining[0], originalErr
	}

	// 所有优先级模式执行完后仍有多个候选映射，返回包含全部候选 Kind 的歧义错误。
	var kinds []schema.GroupVersionKind
	for _, m := range mappings {
		kinds = append(kinds, m.GroupVersionKind)
	}
	return nil, &AmbiguousKindError{PartialKind: gk.WithVersion(""), MatchingKinds: kinds}
}

func (m PriorityRESTMapperWithContext) RESTMappingsWithContext(ctx context.Context, gk schema.GroupKind, versions ...string) ([]*RESTMapping, error) {
	return m.Delegate.RESTMappingsWithContext(ctx, gk, versions...)
}

func (m PriorityRESTMapperWithContext) ResourceSingularizerWithContext(ctx context.Context, resource string) (singular string, err error) {
	return m.Delegate.ResourceSingularizerWithContext(ctx, resource)
}

func (m PriorityRESTMapperWithContext) ResourcesForWithContext(ctx context.Context, partiallySpecifiedResource schema.GroupVersionResource) ([]schema.GroupVersionResource, error) {
	return m.Delegate.ResourcesForWithContext(ctx, partiallySpecifiedResource)
}

func (m PriorityRESTMapperWithContext) KindsForWithContext(ctx context.Context, partiallySpecifiedResource schema.GroupVersionResource) (gvk []schema.GroupVersionKind, err error) {
	return m.Delegate.KindsForWithContext(ctx, partiallySpecifiedResource)
}

func (m PriorityRESTMapperWithContext) ResetWithContext(ctx context.Context) {
	MaybeResetRESTMapperWithContext(ctx, m.Delegate)
}
