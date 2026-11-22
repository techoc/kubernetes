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
	"fmt"

	"k8s.io/apimachinery/pkg/runtime/schema"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
)

var (
	_ ResettableRESTMapper = &FirstHitRESTMapper{}
)

// FirstHitRESTMapper 包装多个 RESTMapper：对每个请求按顺序询问，返回第一个成功的结果。
// 它不要求各 mapper 的意见一致，适合“互补而非重叠”的映射来源（例如内置资源与聚合 API）。
type FirstHitRESTMapper struct {
	MultiRESTMapper
}

// String 返回便于调试的字符串表示。
func (m FirstHitRESTMapper) String() string {
	return fmt.Sprintf("FirstHitRESTMapper{\n\t%v\n}", m.MultiRESTMapper)
}

// ResourceFor 依次向底层 mapper 查询资源的规范化 GroupVersionResource，返回首个命中。
// 全部失败时把各 mapper 的错误合并返回。
func (m FirstHitRESTMapper) ResourceFor(resource schema.GroupVersionResource) (schema.GroupVersionResource, error) {
	errors := []error{}
	for _, t := range m.MultiRESTMapper {
		ret, err := t.ResourceFor(resource)
		if err == nil {
			return ret, nil
		}
		// 保留每个 mapper 的错误，便于全部未命中时定位是哪个来源缺了注册。
		errors = append(errors, err)
	}

	return schema.GroupVersionResource{}, collapseAggregateErrors(errors)
}

// KindFor 依次向底层 mapper 查询资源对应的 GroupVersionKind，返回首个命中。
func (m FirstHitRESTMapper) KindFor(resource schema.GroupVersionResource) (schema.GroupVersionKind, error) {
	errors := []error{}
	for _, t := range m.MultiRESTMapper {
		ret, err := t.KindFor(resource)
		if err == nil {
			return ret, nil
		}
		errors = append(errors, err)
	}

	return schema.GroupVersionKind{}, collapseAggregateErrors(errors)
}

// RESTMapping 基于 kind 和 version 返回 REST 映射。该实现支持多个 REST schema，
// 并返回第一个匹配的结果。
func (m FirstHitRESTMapper) RESTMapping(gk schema.GroupKind, versions ...string) (*RESTMapping, error) {
	errors := []error{}
	for _, t := range m.MultiRESTMapper {
		ret, err := t.RESTMapping(gk, versions...)
		if err == nil {
			return ret, nil
		}
		errors = append(errors, err)
	}

	return nil, collapseAggregateErrors(errors)
}

// Reset 清除底层 mapper 的缓存，用于发现失败后重建发现信息。
func (m FirstHitRESTMapper) Reset() {
	m.MultiRESTMapper.Reset()
}

// collapseAggregateErrors 返回最小化的错误：空切片返回 nil，单个元素直接返回该元素，
// 且当所有错误都是 NoMatchError 时折叠为第一个——它们对外含义相同，逐个列举只会造成噪音。
func collapseAggregateErrors(errors []error) error {
	if len(errors) == 0 {
		return nil
	}
	if len(errors) == 1 {
		return errors[0]
	}

	allNoMatchErrors := true
	for _, err := range errors {
		allNoMatchErrors = allNoMatchErrors && IsNoMatchError(err)
	}
	if allNoMatchErrors {
		return errors[0]
	}

	return utilerrors.NewAggregate(errors)
}
