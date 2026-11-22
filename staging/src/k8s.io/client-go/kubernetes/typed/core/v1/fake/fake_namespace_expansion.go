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

package fake

import (
	"context"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	core "k8s.io/client-go/testing"
)

// Finalize 模拟对 Namespace 的 finalize 子资源请求。
//
// 服务端把 finalize 实现为“创建”语义（POST 到 /namespaces/{name}/finalize），
// 因此这里的 action 使用 create 动词而非 update；测试断言需按此匹配。
func (c *fakeNamespaces) Finalize(ctx context.Context, namespace *v1.Namespace, opts metav1.UpdateOptions) (*v1.Namespace, error) {
	action := core.CreateActionImpl{}
	action.Verb = "create"
	action.Resource = c.Resource()
	action.Subresource = "finalize"
	action.Object = namespace

	obj, err := c.Fake.Invokes(action, namespace)
	// Invokes 用 nil 对象表示“没有可返回的对象”（例如 reactor 返回错误），
	// 此时只需透传错误，避免对 nil 做类型断言而 panic。
	if obj == nil {
		return nil, err
	}

	return obj.(*v1.Namespace), err
}
