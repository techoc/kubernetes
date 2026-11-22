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

package fake

import (
	"context"

	policy "k8s.io/api/policy/v1beta1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	core "k8s.io/client-go/testing"
)

// Evict 在 fake 客户端上模拟驱逐 Pod：实际 API 是向 core/v1 的
// pods/eviction 子资源发 POST，而不是对 policy/v1beta1 资源操作。
// 因此这里构造的 action 故意使用核心组 GVR 和 "eviction" 子资源，
// 以便使用者的 reactor 能按一致的方式匹配。
func (c *fakeEvictions) Evict(ctx context.Context, eviction *policy.Eviction) error {
	action := core.CreateActionImpl{}
	action.Verb = "create"
	action.Namespace = c.Namespace()
	action.Resource = schema.GroupVersionResource{Group: "", Version: "v1", Resource: "pods"}
	action.Subresource = "eviction"
	action.Object = eviction

	_, err := c.Fake.Invokes(action, eviction)
	return err
}
