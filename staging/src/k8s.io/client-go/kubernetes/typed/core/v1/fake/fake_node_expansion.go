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

	v1 "k8s.io/api/core/v1"
	types "k8s.io/apimachinery/pkg/types"
	core "k8s.io/client-go/testing"
)

// PatchStatus 在 fake 客户端中模拟对 Node 的 status 子资源打补丁。
// 与真实客户端不同，此处固定使用 StrategicMergePatchType，并忽略传入的 context；
// 之所以保留该方法，是为了让依赖 status 子资源更新的调用方在测试中也能正常工作。
// TODO: Should take a PatchType as an argument probably.
func (c *fakeNodes) PatchStatus(_ context.Context, nodeName string, data []byte) (*v1.Node, error) {
	// TODO: Should be configurable to support additional patch strategies.
	// 固定补丁类型：fake 测试对象默认就用战略合并补丁语义。
	pt := types.StrategicMergePatchType
	// 构造 RootPatchSubresourceAction 交给 Fake.Invokes，
	// 以便测试中的 reactors 能拦截并观察该请求（包括 "status" 子资源）。
	obj, err := c.Fake.Invokes(
		core.NewRootPatchSubresourceAction(c.Resource(), nodeName, pt, data, "status"), &v1.Node{})
	if obj == nil {
		return nil, err
	}

	// reactor 可能返回其他 runtime.Object 实现，这里按约定断言为 Node。
	return obj.(*v1.Node), err
}
