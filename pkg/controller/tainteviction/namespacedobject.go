/*
Copyright 2025 The Kubernetes Authors.

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

package tainteviction

import (
	"k8s.io/apimachinery/pkg/types"
)

// NamespacedObject 表示带必需 namespace 和可选 UID 的资源名。
// 文本输出格式为 "<namespace>/<name>[:<uid>]"，JSON 输出为结构体对象。
// 保留 UID 可以在对象被删除并同名重建时区分两代资源。
type NamespacedObject struct {
	types.NamespacedName
	UID types.UID
}

// String 返回通用文本表示；UID 非空时附加 ":uid" 以区分同名对象。
func (n NamespacedObject) String() string {
	if n.UID != "" {
		return n.Namespace + string(types.Separator) + n.Name + ":" + string(n.UID)
	}
	return n.Namespace + string(types.Separator) + n.Name
}

// MarshalLog 输出包含各字段的结构体，便于结构化日志按字段检索。
func (n NamespacedObject) MarshalLog() interface{} {
	return struct {
		Name      string    `json:"name"`
		Namespace string    `json:"namespace,omitempty"`
		UID       types.UID `json:"uid,omitempty"`
	}{
		Name:      n.Name,
		Namespace: n.Namespace,
		UID:       n.UID,
	}
}
