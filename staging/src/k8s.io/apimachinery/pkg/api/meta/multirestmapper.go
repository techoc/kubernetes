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

package meta

import (
	"context"
	"fmt"
	"strings"

	"k8s.io/apimachinery/pkg/runtime/schema"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
)

var (
	_ ResettableRESTMapper            = MultiRESTMapper{}
	_ fmt.Stringer                    = MultiRESTMapper{}
	_ ResettableRESTMapperWithContext = MultiRESTMapperWithContext{}
	_ fmt.Stringer                    = MultiRESTMapperWithContext{}
)

// MultiRESTMapper 包装多个 RESTMapper。
//
// MultiRESTMapperWithContext 是更好的替代方案，因为它支持上下文日志与取消。
//
// 上下文日志：请使用 MultiRESTMapperWithContext。
type MultiRESTMapper []RESTMapper

// MultiRESTMapperWithContext 包装多个 RESTMapperWithContext 实例。
type MultiRESTMapperWithContext []RESTMapperWithContext

// String 返回便于调试的多行字符串表示。
func (m MultiRESTMapper) String() string {
	return stringifyMapper("MultiRESTMapper", m)
}

// String 返回便于调试的多行字符串表示。
func (m MultiRESTMapperWithContext) String() string {
	return stringifyMapper("MultiRESTMapperWithContext", m)
}

// stringifyMapper 把每个 mapper 的字符串表示缩进后拼成一个整体，
// 保证嵌套输出可读。
func stringifyMapper[T any](typeName string, m []T) string {
	nested := make([]string, 0, len(m))
	for _, t := range m {
		currString := fmt.Sprintf("%v", t)
		splitStrings := strings.Split(currString, "\n")
		nested = append(nested, strings.Join(splitStrings, "\n\t"))
	}

	return fmt.Sprintf("%s{\n\t%s\n}", typeName, strings.Join(nested, "\n\t"))
}

// ToMultiRESTMapperWithContext 把 MultiRESTMapper 转换为带上下文的版本。
// nil 输入返回 nil，保持 nil 语义。
func ToMultiRESTMapperWithContext(m MultiRESTMapper) MultiRESTMapperWithContext {
	if m == nil {
		return nil
	}
	mc := make(MultiRESTMapperWithContext, len(m))
	for i, m := range m {
		mc[i] = ToRESTMapperWithContext(m)
	}
	return mc
}

// ResourceSingularizer 把 REST 资源名从复数转为单数（例如 pods -> pod）。
// 该实现支持多个 REST schema，并返回第一个匹配结果。
//
// MultiRESTMapperWithContext.ResourceSingularizerWithContext 是更好的替代方案，
// 因为它支持上下文日志与取消。
//
// 上下文日志：请使用 MultiRESTMapperWithContext.ResourceSingularizerWithContext。
func (m MultiRESTMapper) ResourceSingularizer(resource string) (singular string, err error) {
	return ToMultiRESTMapperWithContext(m).ResourceSingularizerWithContext(context.Background(), resource)
}

// MultiRESTMapperWithContext.ResourcesForWithContext 是更好的替代方案，
// 因为它支持上下文日志与取消。
//
// 上下文日志：请使用 MultiRESTMapperWithContext.ResourcesForWithContext。
func (m MultiRESTMapper) ResourcesFor(resource schema.GroupVersionResource) ([]schema.GroupVersionResource, error) {
	return ToMultiRESTMapperWithContext(m).ResourcesForWithContext(context.Background(), resource)
}

// MultiRESTMapperWithContext.KindsForWithContext 是更好的替代方案，
// 因为它支持上下文日志与取消。
//
// 上下文日志：请使用 MultiRESTMapperWithContext.KindsForWithContext。
func (m MultiRESTMapper) KindsFor(resource schema.GroupVersionResource) (gvk []schema.GroupVersionKind, err error) {
	return ToMultiRESTMapperWithContext(m).KindsForWithContext(context.Background(), resource)
}

// MultiRESTMapperWithContext.ResourceForWithContext 是更好的替代方案，
// 因为它支持上下文日志与取消。
//
// 上下文日志：请使用 MultiRESTMapperWithContext.ResourceForWithContext。
func (m MultiRESTMapper) ResourceFor(resource schema.GroupVersionResource) (schema.GroupVersionResource, error) {
	return ToMultiRESTMapperWithContext(m).ResourceForWithContext(context.Background(), resource)
}

// MultiRESTMapperWithContext.KindForWithContext 是更好的替代方案，
// 因为它支持上下文日志与取消。
//
// 上下文日志：请使用 MultiRESTMapperWithContext.KindForWithContext。
func (m MultiRESTMapper) KindFor(resource schema.GroupVersionResource) (schema.GroupVersionKind, error) {
	return ToMultiRESTMapperWithContext(m).KindForWithContext(context.Background(), resource)
}

// RESTMapping 根据 kind 和版本返回资源的 REST 映射。
// 该实现支持多个 REST schema，并返回第一个匹配结果。
//
// MultiRESTMapperWithContext.RESTMappingWithContext 是更好的替代方案，
// 因为它支持上下文日志与取消。
//
// 上下文日志：请使用 MultiRESTMapperWithContext.RESTMappingWithContext。
func (m MultiRESTMapper) RESTMapping(gk schema.GroupKind, versions ...string) (*RESTMapping, error) {
	return ToMultiRESTMapperWithContext(m).RESTMappingWithContext(context.Background(), gk, versions...)
}

// RESTMappings 返回给定 group kind 的所有可能 RESTMapping；若类型无法识别则返回错误。
//
// MultiRESTMapperWithContext.RESTMappingsWithContext 是更好的替代方案，
// 因为它支持上下文日志与取消。
//
// 上下文日志：请使用 MultiRESTMapperWithContext.RESTMappingsWithContext。
func (m MultiRESTMapper) RESTMappings(gk schema.GroupKind, versions ...string) ([]*RESTMapping, error) {
	return ToMultiRESTMapperWithContext(m).RESTMappingsWithContext(context.Background(), gk, versions...)
}

// MultiRESTMapperWithContext.Reset 是更好的替代方案，因为它支持上下文日志与取消。
//
// 上下文日志：请使用 MultiRESTMapperWithContext.Reset。
func (m MultiRESTMapper) Reset() {
	for _, t := range m {
		MaybeResetRESTMapper(t)
	}
}

// ResourceSingularizerWithContext 把 REST 资源名从复数转为单数（例如 pods -> pod）。
// 该实现支持多个 REST schema，并返回第一个成功的结果。
func (m MultiRESTMapperWithContext) ResourceSingularizerWithContext(ctx context.Context, resource string) (singular string, err error) {
	for _, t := range m {
		singular, err = t.ResourceSingularizerWithContext(ctx, resource)
		if err == nil {
			return
		}
	}
	return
}

// ResourcesForWithContext 聚合各 mapper 的结果并去重；忽略 NoMatch 错误，
// 其他错误立即返回。若最终无任何匹配则返回 NoResourceMatchError。
func (m MultiRESTMapperWithContext) ResourcesForWithContext(ctx context.Context, resource schema.GroupVersionResource) ([]schema.GroupVersionResource, error) {
	allGVRs := []schema.GroupVersionResource{}
	for _, t := range m {
		gvrs, err := t.ResourcesForWithContext(ctx, resource)
		// 忽略“无匹配”错误，其他错误向上传播。
		if IsNoMatchError(err) {
			continue
		}
		if err != nil {
			return nil, err
		}

		// 遍历已有结果去重。
		for _, curr := range gvrs {
			found := false
			for _, existing := range allGVRs {
				if curr == existing {
					found = true
					break
				}
			}

			if !found {
				allGVRs = append(allGVRs, curr)
			}
		}
	}

	if len(allGVRs) == 0 {
		return nil, &NoResourceMatchError{PartialResource: resource}
	}

	return allGVRs, nil
}

// KindsForWithContext 聚合各 mapper 的结果并去重；忽略 NoMatch 错误，
// 其他错误立即返回。若最终无任何匹配则返回 NoResourceMatchError。
func (m MultiRESTMapperWithContext) KindsForWithContext(ctx context.Context, resource schema.GroupVersionResource) (gvk []schema.GroupVersionKind, err error) {
	allGVKs := []schema.GroupVersionKind{}
	for _, t := range m {
		gvks, err := t.KindsForWithContext(ctx, resource)
		// 忽略“无匹配”错误，其他错误向上传播。
		if IsNoMatchError(err) {
			continue
		}
		if err != nil {
			return nil, err
		}

		// 遍历已有结果去重。
		for _, curr := range gvks {
			found := false
			for _, existing := range allGVKs {
				if curr == existing {
					found = true
					break
				}
			}

			if !found {
				allGVKs = append(allGVKs, curr)
			}
		}
	}

	if len(allGVKs) == 0 {
		return nil, &NoResourceMatchError{PartialResource: resource}
	}

	return allGVKs, nil
}

// ResourceForWithContext 要求 ResourcesForWithContext 恰好返回一个结果，
// 否则返回 AmbiguousResourceError。
func (m MultiRESTMapperWithContext) ResourceForWithContext(ctx context.Context, resource schema.GroupVersionResource) (schema.GroupVersionResource, error) {
	resources, err := m.ResourcesForWithContext(ctx, resource)
	if err != nil {
		return schema.GroupVersionResource{}, err
	}
	if len(resources) == 1 {
		return resources[0], nil
	}

	return schema.GroupVersionResource{}, &AmbiguousResourceError{PartialResource: resource, MatchingResources: resources}
}

// KindForWithContext 要求 KindsForWithContext 恰好返回一个结果，
// 否则返回 AmbiguousResourceError。
func (m MultiRESTMapperWithContext) KindForWithContext(ctx context.Context, resource schema.GroupVersionResource) (schema.GroupVersionKind, error) {
	kinds, err := m.KindsForWithContext(ctx, resource)
	if err != nil {
		return schema.GroupVersionKind{}, err
	}
	if len(kinds) == 1 {
		return kinds[0], nil
	}

	return schema.GroupVersionKind{}, &AmbiguousResourceError{PartialResource: resource, MatchingKinds: kinds}
}

// RESTMappingWithContext 根据 kind 和版本返回资源的 REST 映射。
// 该实现支持多个 REST schema，并返回第一个匹配结果。
func (m MultiRESTMapperWithContext) RESTMappingWithContext(ctx context.Context, gk schema.GroupKind, versions ...string) (*RESTMapping, error) {
	allMappings := []*RESTMapping{}
	errors := []error{}

	for _, t := range m {
		currMapping, err := t.RESTMappingWithContext(ctx, gk, versions...)
		// 忽略“无匹配”错误，其他错误收集起来稍后聚合。
		if IsNoMatchError(err) {
			continue
		}
		if err != nil {
			errors = append(errors, err)
			continue
		}

		allMappings = append(allMappings, currMapping)
	}

	// 即使部分 mapper 失败，只要恰好得到一个映射就直接采用。
	if len(allMappings) == 1 {
		return allMappings[0], nil
	}
	if len(allMappings) > 1 {
		var kinds []schema.GroupVersionKind
		for _, m := range allMappings {
			kinds = append(kinds, m.GroupVersionKind)
		}
		return nil, &AmbiguousKindError{PartialKind: gk.WithVersion(""), MatchingKinds: kinds}
	}
	if len(errors) > 0 {
		return nil, utilerrors.NewAggregate(errors)
	}
	return nil, &NoKindMatchError{GroupKind: gk, SearchedVersions: versions}
}

// RESTMappingsWithContext 返回给定 group kind 的所有可能 RESTMapping；
// 若类型无法识别则返回错误。
func (m MultiRESTMapperWithContext) RESTMappingsWithContext(ctx context.Context, gk schema.GroupKind, versions ...string) ([]*RESTMapping, error) {
	var allMappings []*RESTMapping
	var errors []error

	for _, t := range m {
		currMappings, err := t.RESTMappingsWithContext(ctx, gk, versions...)
		// 忽略“无匹配”错误，其他错误收集起来稍后聚合。
		if IsNoMatchError(err) {
			continue
		}
		if err != nil {
			errors = append(errors, err)
			continue
		}
		allMappings = append(allMappings, currMappings...)
	}
	if len(errors) > 0 {
		return nil, utilerrors.NewAggregate(errors)
	}
	if len(allMappings) == 0 {
		return nil, &NoKindMatchError{GroupKind: gk, SearchedVersions: versions}
	}
	return allMappings, nil
}

// ResetWithContext 重置所有支持重置的 mapper，用于刷新缓存。
func (m MultiRESTMapperWithContext) ResetWithContext(ctx context.Context) {
	for _, t := range m {
		MaybeResetRESTMapperWithContext(ctx, t)
	}
}
