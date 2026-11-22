/*
Copyright 2023 The Kubernetes Authors.

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

package authorizer

import (
	"context"
	"encoding/json"
	"sort"
	"strings"

	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apiserver/pkg/authentication/user"
	"k8s.io/apiserver/pkg/authorization/authorizer"
)

// authzResult 保存一次授权结果，便于重复命中时原样返回（包括错误）。
type authzResult struct {
	authorized authorizer.Decision
	reason     string
	err        error
}

// For now, secondary authorization checks in admission only support unconditional authorization
var _ = authorizer.UnconditionalAuthorizer(&cachingAuthorizer{})

// cachingAuthorizer 为单次准入链内的重复授权检查做结果缓存。
// 该结构体内部是普通 map，不是并发安全的；调用方必须保证同一实例
// 只被单个请求/goroutine 使用，用完即丢弃。
type cachingAuthorizer struct {
	authorizer authorizer.UnconditionalAuthorizer
	decisions  map[string]authzResult
}

// NewCachingAuthorizer returns an authorizer that caches decisions for the duration
// of the authorizers use.  Intended to be used for short-lived operations such as
// the handling of a request in the admission chain, and then discarded.
//
// 之所以不做长期缓存：授权结果会随 RBAC 规则变化而失效，长期缓存需要
// 复杂的失效机制，而准入链内的重复检查在单次请求生命周期内即可获益。
func NewCachingAuthorizer(in authorizer.UnconditionalAuthorizer) authorizer.UnconditionalAuthorizer {
	return &cachingAuthorizer{
		authorizer: in,
		decisions:  make(map[string]authzResult),
	}
}

// The attribute accessors known to cache key construction. If this fails to compile, the cache
// implementation may need to be updated.
var _ authorizer.Attributes = (interface {
	GetUser() user.Info
	GetVerb() string
	IsReadOnly() bool
	GetNamespace() string
	GetResource() string
	GetSubresource() string
	GetName() string
	GetAPIGroup() string
	GetAPIVersion() string
	IsResourceRequest() bool
	GetPath() string
	GetFieldSelector() (fields.Requirements, error)
	GetLabelSelector() (labels.Requirements, error)
})(nil)

// The user info accessors known to cache key construction. If this fails to compile, the cache
// implementation may need to be updated.
var _ user.Info = (interface {
	GetName() string
	GetUID() string
	GetGroups() []string
	GetExtra() map[string][]string
})(nil)

// Authorize returns an authorization decision by delegating to another Authorizer. If an equivalent
// check has already been performed, a cached result is returned. Not safe for concurrent use.
//
// 缓存键由属性与用户信息序列化得到。构造键时对 groups/extra 排序并
// 丢弃不可序列化的部分，是为了让语义等价的检查命中同一键；字段选择器
// 解析失败时则不纳入键，因为底层 authorizer 也不会使用它。
func (ca *cachingAuthorizer) Authorize(ctx context.Context, a authorizer.Attributes) (authorizer.Decision, string, error) {
	type SerializableAttributes struct {
		authorizer.AttributesRecord
		LabelSelector string
	}

	serializableAttributes := SerializableAttributes{
		AttributesRecord: authorizer.AttributesRecord{
			Verb:            a.GetVerb(),
			Namespace:       a.GetNamespace(),
			APIGroup:        a.GetAPIGroup(),
			APIVersion:      a.GetAPIVersion(),
			Resource:        a.GetResource(),
			Subresource:     a.GetSubresource(),
			Name:            a.GetName(),
			ResourceRequest: a.IsResourceRequest(),
			Path:            a.GetPath(),
		},
	}
	// in the error case, we won't honor this field selector, so the cache doesn't need it.
	if fieldSelector, err := a.GetFieldSelector(); len(fieldSelector) > 0 {
		serializableAttributes.FieldSelectorRequirements, serializableAttributes.FieldSelectorParsingErr = fieldSelector, err
	}
	if labelSelector, _ := a.GetLabelSelector(); len(labelSelector) > 0 {
		// the labels requirements have private elements so those don't help us serialize to a unique key
		serializableAttributes.LabelSelector = labelSelector.String()
	}

	if u := a.GetUser(); u != nil {
		di := &user.DefaultInfo{
			Name: u.GetName(),
			UID:  u.GetUID(),
		}

		// Differently-ordered groups or extras could cause otherwise-equivalent checks to
		// have distinct cache keys.
		if groups := u.GetGroups(); len(groups) > 0 {
			di.Groups = make([]string, len(groups))
			copy(di.Groups, groups)
			sort.Strings(di.Groups)
		}

		if extra := u.GetExtra(); len(extra) > 0 {
			di.Extra = make(map[string][]string, len(extra))
			for k, vs := range extra {
				vdupe := make([]string, len(vs))
				copy(vdupe, vs)
				sort.Strings(vdupe)
				di.Extra[k] = vdupe
			}
		}

		serializableAttributes.User = di
	}

	// 用 JSON 序列化而不是拼接字符串，避免字段边界歧义导致错误命中。
	var b strings.Builder
	if err := json.NewEncoder(&b).Encode(serializableAttributes); err != nil {
		return authorizer.DecisionNoOpinion, "", err
	}
	key := b.String()

	if cached, ok := ca.decisions[key]; ok {
		return cached.authorized, cached.reason, cached.err
	}

	authorized, reason, err := ca.authorizer.Authorize(ctx, a)

	// 错误结果也缓存：同一请求内重复检查大概率得到同一错误，避免重复失败开销。
	ca.decisions[key] = authzResult{
		authorized: authorized,
		reason:     reason,
		err:        err,
	}

	return authorized, reason, err
}
