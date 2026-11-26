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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	scheme "k8s.io/client-go/kubernetes/scheme"
)

// NamespaceExpansion 为代码生成的 NamespaceInterface 追加手写方法。
//
// Finalize 是最终确定（finalize）流程的入口：它把 spec.finalizers 的变更提交给
// 服务端的 finalize 子资源，由 namespace 控制器在后台完成实际清理。
type NamespaceExpansion interface {
	Finalize(ctx context.Context, item *v1.Namespace, opts metav1.UpdateOptions) (*v1.Namespace, error)
}

// Finalize 提交 namespace 的 finalizers 变更，返回服务端返回的 namespace 或错误。
//
// 注意走的是 PUT .../namespaces/<name>/finalize 子资源，而不是普通 update；
// 该子资源只接受对 finalizers 字段的修改，其他字段的改动会被服务端拒绝。
func (c *namespaces) Finalize(ctx context.Context, namespace *v1.Namespace, opts metav1.UpdateOptions) (result *v1.Namespace, err error) {
	result = &v1.Namespace{}
	err = c.GetClient().Put().Resource("namespaces").Name(namespace.Name).VersionedParams(&opts, scheme.ParameterCodec).SubResource("finalize").Body(namespace).Do(ctx).Into(result)
	return
}
