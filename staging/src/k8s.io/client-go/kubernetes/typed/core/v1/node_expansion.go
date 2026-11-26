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

package v1

import (
	"context"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
)

// NodeExpansion 为生成的 NodeInterface 追加手写方法。
type NodeExpansion interface {
	// PatchStatus 修改已有 Node 的状态，返回服务端返回的副本或错误。
	//
	// 之所以单独暴露，是因为生成的 NodeInterface 不包含 status 子资源操作；
	// 走 status 子资源可以绕过 spec 的写入校验，仅由 kubelet 等特权组件使用。
	PatchStatus(ctx context.Context, nodeName string, data []byte) (*v1.Node, error)
}

// PatchStatus 修改已有 Node 的状态，返回服务端返回的副本或错误。
func (c *nodes) PatchStatus(ctx context.Context, nodeName string, data []byte) (*v1.Node, error) {
	result := &v1.Node{}
	// 使用 strategic merge patch：调用方只需提交要改动的 status 字段，
	// 其余字段由服务器按合并策略保留。
	err := c.GetClient().Patch(types.StrategicMergePatchType).
		Resource("nodes").
		Name(nodeName).
		SubResource("status").
		Body(data).
		Do(ctx).
		Into(result)
	return result, err
}
