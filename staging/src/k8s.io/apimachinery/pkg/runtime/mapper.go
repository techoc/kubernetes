/*
Copyright 2019 The Kubernetes Authors.

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

package runtime

import (
	"sync"

	"k8s.io/apimachinery/pkg/runtime/schema"
)

// equivalentResourceRegistry 记录“哪些资源被视为同一资源的不同版本/子资源”，
// 用于让 RESTMapper 在多版本 API 之间建立等价关系。
type equivalentResourceRegistry struct {
	// keyFunc 计算资源对应的等价键（可在不同 API 组间合并同址资源）。
	// 为 nil 或返回 "" 时，使用 resource.String() 作为键。
	keyFunc func(resource schema.GroupResource) string
	// resources 映射 key -> 子资源 -> 等价资源列表（返回结果不含子资源本身）。
	// 主资源以 subresource="" 存储。
	resources map[string]map[string][]schema.GroupVersionResource
	// kinds 映射 资源 -> 子资源 -> kind。
	kinds map[schema.GroupVersionResource]map[string]schema.GroupVersionKind
	// keys 缓存每个 GroupResource 计算出的键。
	keys map[schema.GroupResource]string

	// mutex 保护上述 map；读取用 RLock，注册用 Lock。
	mutex sync.RWMutex
}

var _ EquivalentResourceMapper = (*equivalentResourceRegistry)(nil)
var _ EquivalentResourceRegistry = (*equivalentResourceRegistry)(nil)

// NewEquivalentResourceRegistry 创建一个资源注册表，把同一 GroupResource 的
// 所有版本视为等价。
func NewEquivalentResourceRegistry() EquivalentResourceRegistry {
	return &equivalentResourceRegistry{}
}

// NewEquivalentResourceRegistryWithIdentity 使用自定义身份函数创建资源映射器。
// 若函数返回 ""，则使用 GroupResource#String 作为身份。
// 身份字符串相同的 GroupResource 被视为等价。
func NewEquivalentResourceRegistryWithIdentity(keyFunc func(schema.GroupResource) string) EquivalentResourceRegistry {
	return &equivalentResourceRegistry{keyFunc: keyFunc}
}

// EquivalentResourcesFor 返回与给定资源（及其子资源）等价的资源列表。
func (r *equivalentResourceRegistry) EquivalentResourcesFor(resource schema.GroupVersionResource, subresource string) []schema.GroupVersionResource {
	r.mutex.RLock()
	defer r.mutex.RUnlock()
	return r.resources[r.keys[resource.GroupResource()]][subresource]
}

// KindFor 返回给定资源与子资源对应的 kind；未注册时返回零值。
func (r *equivalentResourceRegistry) KindFor(resource schema.GroupVersionResource, subresource string) schema.GroupVersionKind {
	r.mutex.RLock()
	defer r.mutex.RUnlock()
	return r.kinds[resource][subresource]
}

// RegisterKindFor 注册资源/子资源与 kind 的对应关系，并维护等价资源索引。
func (r *equivalentResourceRegistry) RegisterKindFor(resource schema.GroupVersionResource, subresource string, kind schema.GroupVersionKind) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	// 按需惰性初始化各层 map，避免空注册表占用内存。
	if r.kinds == nil {
		r.kinds = map[schema.GroupVersionResource]map[string]schema.GroupVersionKind{}
	}
	if r.kinds[resource] == nil {
		r.kinds[resource] = map[string]schema.GroupVersionKind{}
	}
	r.kinds[resource][subresource] = kind

	// 计算父资源的共享键；同键资源会被归入同一等价组。
	key := ""
	gr := resource.GroupResource()
	if r.keyFunc != nil {
		key = r.keyFunc(gr)
	}
	if key == "" {
		key = gr.String()
	}

	if r.keys == nil {
		r.keys = map[schema.GroupResource]string{}
	}
	r.keys[gr] = key

	if r.resources == nil {
		r.resources = map[string]map[string][]schema.GroupVersionResource{}
	}
	if r.resources[key] == nil {
		r.resources[key] = map[string][]schema.GroupVersionResource{}
	}
	// 追加而非覆盖：同一资源的多个版本都应出现在等价列表中。
	r.resources[key][subresource] = append(r.resources[key][subresource], resource)
}
