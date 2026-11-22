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

package storage

import (
	"context"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/sharding"
	"k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/apiserver/pkg/features"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
)

// AttrFunc returns label and field sets and the uninitialized flag for List or Watch to match.
// In any failure to parse given object, it returns error.
type AttrFunc func(obj runtime.Object) (labels.Set, fields.Set, error)

// FieldMutationFunc allows the mutation of the field selection fields.  It is mutating to
// avoid the extra allocation on this common path
type FieldMutationFunc func(obj runtime.Object, fieldSet fields.Set) error

// DefaultClusterScopedAttr 为集群级对象提供 name 字段与标签，供字段/标签选择器匹配。
func DefaultClusterScopedAttr(obj runtime.Object) (labels.Set, fields.Set, error) {
	metadata, err := meta.Accessor(obj)
	if err != nil {
		return nil, nil, err
	}
	fieldSet := fields.Set{
		"metadata.name": metadata.GetName(),
	}

	return labels.Set(metadata.GetLabels()), fieldSet, nil
}

// DefaultNamespaceScopedAttr 为命名空间级对象提供 name/namespace 字段与标签。
func DefaultNamespaceScopedAttr(obj runtime.Object) (labels.Set, fields.Set, error) {
	metadata, err := meta.Accessor(obj)
	if err != nil {
		return nil, nil, err
	}
	fieldSet := fields.Set{
		"metadata.name":      metadata.GetName(),
		"metadata.namespace": metadata.GetNamespace(),
	}

	return labels.Set(metadata.GetLabels()), fieldSet, nil
}

// WithFieldMutation 在原有属性函数之上叠加字段注入逻辑（例如按资源追加专用字段），
// 共享同一个 fieldSet 以避免额外分配。
func (f AttrFunc) WithFieldMutation(fieldMutator FieldMutationFunc) AttrFunc {
	return func(obj runtime.Object) (labels.Set, fields.Set, error) {
		labelSet, fieldSet, err := f(obj)
		if err != nil {
			return nil, nil, err
		}
		if err := fieldMutator(obj, fieldSet); err != nil {
			return nil, nil, err
		}
		return labelSet, fieldSet, nil
	}
}

// SelectionPredicate is used to represent the way to select objects from api storage.
// 同时承载 List/Watch 的过滤条件、分页状态与分片选择器。
type SelectionPredicate struct {
	Label               labels.Selector
	Field               fields.Selector
	GetAttrs            AttrFunc
	IndexLabels         []string
	IndexFields         []string
	Limit               int64
	Continue            string
	AllowWatchBookmarks bool
	// ShardSelector is the parsed shard selector for filtering objects by hash range.
	// 仅在 ShardedListAndWatch 特性开启时生效。
	ShardSelector sharding.Selector
}

// Matches returns true if the given object's labels and fields (as
// returned by s.GetAttrs) match s.Label and s.Field. An error is
// returned if s.GetAttrs fails.
func (s *SelectionPredicate) Matches(obj runtime.Object) (bool, error) {
	// 分片过滤优先执行：不满足分片的对象无需再做标签/字段匹配。
	if utilfeature.DefaultFeatureGate.Enabled(features.ShardedListAndWatch) {
		if matched, err := s.MatchesSharding(obj); err != nil || !matched {
			return matched, err
		}
	}
	// 空谓词表示匹配所有对象，快速返回可避免每次访问对象元数据。
	if s.labelFieldEmpty() {
		return true, nil
	}
	labels, fields, err := s.GetAttrs(obj)
	if err != nil {
		return false, err
	}
	matched := s.Label.Matches(labels)
	// 只有标签匹配后才评估字段，短路以减少不必要的字段计算。
	if matched && s.Field != nil {
		matched = matched && s.Field.Matches(fields)
	}
	return matched, nil
}

// MatchesObjectAttributes returns true if the given labels and fields
// match s.Label and s.Field.
func (s *SelectionPredicate) MatchesObjectAttributes(l labels.Set, f fields.Set) bool {
	if s.Label.Empty() && s.Field.Empty() {
		return true
	}
	matched := s.Label.Matches(l)
	if matched && s.Field != nil {
		matched = (matched && s.Field.Matches(f))
	}
	return matched
}

// MatchesSingleNamespace will return (namespace, true) if and only if s.Field matches on the object's
// namespace.
// 用于把列表请求下推为按命名空间前缀的直接读取；带 continue 时无法下推。
func (s *SelectionPredicate) MatchesSingleNamespace() (string, bool) {
	if len(s.Continue) > 0 || s.Field == nil {
		return "", false
	}
	if namespace, ok := s.Field.RequiresExactMatch("metadata.namespace"); ok {
		return namespace, true
	}
	return "", false
}

// MatchesSingle will return (name, true) if and only if s.Field matches on the object's
// name.
func (s *SelectionPredicate) MatchesSingle() (string, bool) {
	if len(s.Continue) > 0 || s.Field == nil {
		return "", false
	}
	// TODO: should be namespace.name
	if name, ok := s.Field.RequiresExactMatch("metadata.name"); ok {
		return name, true
	}
	return "", false
}

// Empty returns true if the predicate performs no filtering.
func (s *SelectionPredicate) Empty() bool {
	// Check the selector before the feature gate: Empty is called per event on
	// watch paths, and the nil check is free while the gate lookup is not.
	// 先做廉价的 nil 检查，再查特性门控，降低 watch 热路径开销。
	if s.ShardSelector != nil && !s.ShardSelector.Empty() &&
		utilfeature.DefaultFeatureGate.Enabled(features.ShardedListAndWatch) {
		return false
	}
	return s.labelFieldEmpty()
}

// labelFieldEmpty 判断标签与字段选择器是否都不产生过滤。
func (s *SelectionPredicate) labelFieldEmpty() bool {
	return (s.Label == nil || s.Label.Empty()) && (s.Field == nil || s.Field.Empty())
}

// For any index defined by IndexFields, if a matcher can match only (a subset)
// of objects that return <value> for a given index, a pair (<index name>, <value>)
// wil be returned.
func (s *SelectionPredicate) MatcherIndex(ctx context.Context) []MatchValue {
	var result []MatchValue
	for _, field := range s.IndexFields {
		if value, ok := s.Field.RequiresExactMatch(field); ok {
			result = append(result, MatchValue{IndexName: FieldIndex(field), Value: value})
		} else if field == "metadata.namespace" {
			// list pods in the namespace. i.e. /api/v1/namespaces/default/pods
			// 路径中的命名空间等价于 metadata.namespace 的精确匹配，可走索引。
			if namespace, isNamespaceScope := isNamespaceScopedRequest(ctx); isNamespaceScope {
				result = append(result, MatchValue{IndexName: FieldIndex(field), Value: namespace})
			}
		}
	}
	for _, label := range s.IndexLabels {
		if value, ok := s.Label.RequiresExactMatch(label); ok {
			result = append(result, MatchValue{IndexName: LabelIndex(label), Value: value})
		}
	}
	return result
}

// MatchesSharding returns true if the given object matches the sharding configuration.
// If ShardSelector is set and non-empty, it delegates to ShardSelector.Matches().
func (s *SelectionPredicate) MatchesSharding(obj runtime.Object) (bool, error) {
	if !utilfeature.DefaultFeatureGate.Enabled(features.ShardedListAndWatch) {
		return true, nil
	}
	if s.ShardSelector != nil && !s.ShardSelector.Empty() {
		return s.ShardSelector.Matches(obj)
	}
	return true, nil
}

// SetShardInfoOnList sets shard metadata on the list response if sharding is active.
// 把选择器回填到响应中，便于客户端确认本次结果只覆盖某个分片。
func (s *SelectionPredicate) SetShardInfoOnList(listObj runtime.Object) {
	if !utilfeature.DefaultFeatureGate.Enabled(features.ShardedListAndWatch) {
		return
	}
	if s.ShardSelector != nil && !s.ShardSelector.Empty() {
		if setter, ok := listObj.(metav1.ShardedListInterface); ok {
			setter.SetShardInfo(&metav1.ShardInfo{Selector: s.ShardSelector.String()})
		}
	}
}

// isNamespaceScopedRequest 从请求上下文中提取命名空间；非命名空间请求返回 false。
func isNamespaceScopedRequest(ctx context.Context) (string, bool) {
	re, _ := request.RequestInfoFrom(ctx)
	if re == nil || len(re.Namespace) == 0 {
		return "", false
	}
	return re.Namespace, true
}

// LabelIndex add prefix for label index.
// 前缀区分标签索引与字段索引，避免同名 key 冲突。
func LabelIndex(label string) string {
	return "l:" + label
}

// FiledIndex add prefix for field index.
func FieldIndex(field string) string {
	return "f:" + field
}
