/*
Copyright 2021 The Kubernetes Authors.

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

package v1

import (
	"context"

	policy "k8s.io/api/policy/v1"
)

// The EvictionExpansion interface allows manually adding extra methods to the ScaleInterface.
// （原文如此：此处实际扩展的是 EvictionInterface。）
//
// EvictionExpansion 通过手写代码为生成的 EvictionInterface 补充 Evict 方法，
// 因为 eviction 是以 Pod 子资源的形式提供的，无法由代码生成器直接表达。
type EvictionExpansion interface {
	Evict(ctx context.Context, eviction *policy.Eviction) error
}

// Evict 向指定 Pod 的 eviction 子资源 POST 一个 Eviction 对象。
// 该请求可能被 PDB 拒绝（返回 429），调用方需按约定处理重试。
func (c *evictions) Evict(ctx context.Context, eviction *policy.Eviction) error {
	// eviction 子资源固定挂在 core/v1 的 pods 之下，与 policy/v1 的组版本无关，
	// 因此这里显式使用 /api/v1 绝对路径。
	return c.GetClient().Post().
		AbsPath("/api/v1").
		Namespace(eviction.Namespace).
		Resource("pods").
		Name(eviction.Name).
		SubResource("eviction").
		Body(eviction).
		Do(ctx).
		Error()
}
