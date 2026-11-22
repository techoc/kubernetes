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
	"errors"
	"fmt"

	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/sets"
)

// AmbiguousResourceError is returned if the RESTMapper finds multiple matches for a resource
//
// AmbiguousResourceError 表示 RESTMapper 根据资源信息（通常是 GVR 的部分字段）匹配到多个结果。
// 调用方需要提供更完整的 GroupVersion 或 Kind 才能消除歧义。
type AmbiguousResourceError struct {
	// PartialResource 是调用方给出的、可能不完整的资源标识。
	PartialResource schema.GroupVersionResource

	// MatchingResources 列出所有匹配到的资源；与 MatchingKinds 至少有一个非空。
	MatchingResources []schema.GroupVersionResource
	MatchingKinds     []schema.GroupVersionKind
}

// Error 按匹配结果的种类选择最具体的错误文案，便于调用方定位歧义来源。
func (e *AmbiguousResourceError) Error() string {
	switch {
	case len(e.MatchingKinds) > 0 && len(e.MatchingResources) > 0:
		return fmt.Sprintf("%v matches multiple resources %v and kinds %v", e.PartialResource, e.MatchingResources, e.MatchingKinds)
	case len(e.MatchingKinds) > 0:
		return fmt.Sprintf("%v matches multiple kinds %v", e.PartialResource, e.MatchingKinds)
	case len(e.MatchingResources) > 0:
		return fmt.Sprintf("%v matches multiple resources %v", e.PartialResource, e.MatchingResources)
	}
	// 理论上不应出现：两个列表都为空时仍返回一个可读的错误。
	return fmt.Sprintf("%v matches multiple resources or kinds", e.PartialResource)
}

// Is 只比较错误类型，使 errors.Is 能够识别任意 AmbiguousResourceError 实例。
func (*AmbiguousResourceError) Is(target error) bool {
	_, ok := target.(*AmbiguousResourceError)
	return ok
}

// AmbiguousKindError is returned if the RESTMapper finds multiple matches for a kind
//
// AmbiguousKindError 表示 RESTMapper 根据 kind 信息匹配到多个结果。与
// AmbiguousResourceError 对称，区别只在于查询入口是 kind 而不是 resource。
type AmbiguousKindError struct {
	// PartialKind 是调用方给出的、可能不完整的 kind 标识。
	PartialKind schema.GroupVersionKind

	// MatchingResources 列出所有匹配到的资源。
	MatchingResources []schema.GroupVersionResource
	MatchingKinds     []schema.GroupVersionKind
}

// Error 按匹配结果的种类选择最具体的错误文案。
func (e *AmbiguousKindError) Error() string {
	switch {
	case len(e.MatchingKinds) > 0 && len(e.MatchingResources) > 0:
		return fmt.Sprintf("%v matches multiple resources %v and kinds %v", e.PartialKind, e.MatchingResources, e.MatchingKinds)
	case len(e.MatchingKinds) > 0:
		return fmt.Sprintf("%v matches multiple kinds %v", e.PartialKind, e.MatchingKinds)
	case len(e.MatchingResources) > 0:
		return fmt.Sprintf("%v matches multiple resources %v", e.PartialKind, e.MatchingResources)
	}
	// 理论上不应出现：两个列表都为空时仍返回一个可读的错误。
	return fmt.Sprintf("%v matches multiple resources or kinds", e.PartialKind)
}

// Is 只比较错误类型，使 errors.Is 能够识别任意 AmbiguousKindError 实例。
func (*AmbiguousKindError) Is(target error) bool {
	_, ok := target.(*AmbiguousKindError)
	return ok
}

// IsAmbiguousError 判断 err 是否为资源或 kind 的歧义错误。
//
// 由于两类错误都实现了 Is，这里用 errors.Is 做类型匹配，即使错误被 fmt.Errorf("%w")
// 包裹也能正确识别。
func IsAmbiguousError(err error) bool {
	if err == nil {
		return false
	}
	return errors.Is(err, &AmbiguousResourceError{}) || errors.Is(err, &AmbiguousKindError{})
}

// NoResourceMatchError is returned if the RESTMapper can't find any match for a resource
//
// NoResourceMatchError 表示完全找不到与给定资源信息匹配的条目；调用方据此区分
// "没有此资源" 与 "匹配到多个"。
type NoResourceMatchError struct {
	// PartialResource 是调用方给出的资源标识。
	PartialResource schema.GroupVersionResource
}

// Error 输出未匹配到的资源信息。
func (e *NoResourceMatchError) Error() string {
	return fmt.Sprintf("no matches for %v", e.PartialResource)
}

// Is 只比较错误类型，便于 errors.Is 做分类判断。
func (*NoResourceMatchError) Is(target error) bool {
	_, ok := target.(*NoResourceMatchError)
	return ok
}

// NoKindMatchError is returned if the RESTMapper can't find any match for a kind
//
// NoKindMatchError 表示找不到与给定 kind 匹配的条目。SearchedVersions 用于在
// 错误信息中说明搜索范围，方便排查版本拼写问题。
type NoKindMatchError struct {
	// GroupKind is the API group and kind that was searched
	GroupKind schema.GroupKind
	// SearchedVersions is the optional list of versions the search was restricted to
	SearchedVersions []string
}

// Error 根据是否限定了版本列表选择单版本或多版本的错误文案。
func (e *NoKindMatchError) Error() string {
	// 去重并格式化为 group/version，避免重复版本让错误信息显得嘈杂。
	searchedVersions := sets.NewString()
	for _, v := range e.SearchedVersions {
		searchedVersions.Insert(schema.GroupVersion{Group: e.GroupKind.Group, Version: v}.String())
	}

	switch len(searchedVersions) {
	case 0:
		return fmt.Sprintf("no matches for kind %q in group %q", e.GroupKind.Kind, e.GroupKind.Group)
	case 1:
		return fmt.Sprintf("no matches for kind %q in version %q", e.GroupKind.Kind, searchedVersions.List()[0])
	default:
		return fmt.Sprintf("no matches for kind %q in versions %q", e.GroupKind.Kind, searchedVersions.List())
	}
}

// Is 只比较错误类型，便于 errors.Is 做分类判断。
func (*NoKindMatchError) Is(target error) bool {
	_, ok := target.(*NoKindMatchError)
	return ok
}

// IsNoMatchError 判断 err 是否为资源或 kind 的未匹配错误。
//
// 通过 errors.Is 匹配，可穿透 fmt.Errorf("%w") 的包装。
func IsNoMatchError(err error) bool {
	if err == nil {
		return false
	}
	return errors.Is(err, &NoResourceMatchError{}) || errors.Is(err, &NoKindMatchError{})
}
