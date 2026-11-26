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

package v1beta1

import (
	"context"

	policy "k8s.io/api/policy/v1beta1"
)

// EvictionExpansion 为 ScaleInterface 追加手写方法。
//
// 注意：注释中的 ScaleInterface 是历史笔误，实际扩展的是 EvictionInterface。
// 该接口之所以单独存在，是因为 eviction 是 Pod 的子资源而非 policy 组的资源，
// 生成的客户端无法自动推导出正确的请求路径。
type EvictionExpansion interface {
	Evict(ctx context.Context, eviction *policy.Eviction) error
}

// Evict 向 Pod 的 eviction 子资源 POST 驱逐请求。
//
// 路径被硬编码为 core 组的 /api/v1/pods/{name}/eviction：
// eviction 资源在 API 上属于 core 组，尽管其请求体类型定义在 policy 组。
// 返回的 error 直接来自服务端响应，调用方可用 apierrors 判断冲突/NotFound 等情形。
func (c *evictions) Evict(ctx context.Context, eviction *policy.Eviction) error {
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
