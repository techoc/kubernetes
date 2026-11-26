/*
Copyright 2017 The Kubernetes Authors.

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

package rest

import (
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/uuid"
)

// metav1Now 返回 metav1.Now()，但允许在单测中替换。
var metav1Now = metav1.Now

// WipeObjectMetaSystemFields 清除 ObjectMeta 中由系统管理的字段。
//
// 用于把外部输入的对象“洗净”：这些字段只能由服务端产生，若允许客户端自带会造成
// 时间戳伪造、UID 冲突或误触发级联删除。
func WipeObjectMetaSystemFields(meta metav1.Object) {
	meta.SetCreationTimestamp(metav1.Time{})
	meta.SetUID("")
	meta.SetDeletionTimestamp(nil)
	meta.SetDeletionGracePeriodSeconds(nil)
	meta.SetSelfLink("")
}

// FillObjectMetaSystemFields 填充 ObjectMeta 中由系统管理的字段。
//
// 在对象首次写入存储前调用，赋予服务端生成的身份（UID）与创建时间。
func FillObjectMetaSystemFields(meta metav1.Object) {
	meta.SetCreationTimestamp(metav1Now())
	meta.SetUID(uuid.NewUUID())
}

// 若对象的 namespace 与请求 namespace 都非空且不一致，返回错误；
// 若其中之一为空，则补齐或清空对象 namespace，使二者一致。
//
// 这是防止跨命名空间写入的安全边界：两者都非空且不一致时必须拒绝，而不是静默以请求路径为准。
func EnsureObjectNamespaceMatchesRequestNamespace(requestNamespace string, obj metav1.Object) error {
	objNamespace := obj.GetNamespace()
	switch {
	case objNamespace == requestNamespace:
		// 已经一致，无需处理
		return nil

	case objNamespace == metav1.NamespaceNone:
		// 未设置，取请求中的 namespace
		obj.SetNamespace(requestNamespace)
		return nil

	case requestNamespace == metav1.NamespaceNone:
		// 集群级资源，清空 namespace
		obj.SetNamespace(metav1.NamespaceNone)
		return nil

	default:
		// 不一致，报错
		return errors.NewBadRequest("the namespace of the provided object does not match the namespace sent on the request")
	}
}

// ExpectedNamespaceForScope 根据请求 namespace 与资源作用域返回期望的 namespace。
// 集群级资源的期望命名空间恒为空串。
func ExpectedNamespaceForScope(requestNamespace string, namespaceScoped bool) string {
	if namespaceScoped {
		return requestNamespace
	}
	return ""
}

// ExpectedNamespaceForResource 根据请求 namespace 返回资源期望的 namespace。
//
// namespaces 资源本身是集群级的特例（core 组），因此期望命名空间为空。
func ExpectedNamespaceForResource(requestNamespace string, resource schema.GroupVersionResource) string {
	if resource.Resource == "namespaces" && resource.Group == "" {
		return ""
	}
	return requestNamespace
}
