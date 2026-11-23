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

package kubeletplugin

import (
	"k8s.io/apimachinery/pkg/types"
)

// NamespacedObject comprises a resource name with a mandatory namespace
// and optional UID. It gets rendered as "<namespace>/<name>:[<uid>]"
// (text output) or as an object (JSON output).
// NamespacedObject 由必需的命名空间、资源名与可选 UID 组成。
// 文本形式渲染为 "<namespace>/<name>:[<uid>]"，JSON 形式渲染为一个对象。
type NamespacedObject struct {
	// 内嵌的 NamespacedName 提供命名空间与名称。
	// UID 是可选的资源 UID。
	types.NamespacedName
	UID types.UID
}

// String returns the general purpose string representation
// String 返回通用文本表示；UID 非空时在名称后追加 ":<uid>"。
func (n NamespacedObject) String() string {
	// UID 非空时在名称后追加冒号与 UID。
	if n.UID != "" {
		return n.Namespace + string(types.Separator) + n.Name + ":" + string(n.UID)
	}
	return n.Namespace + string(types.Separator) + n.Name
}

// MarshalLog emits a struct containing required key/value pair
// MarshalLog 返回一个包含 name、namespace、uid 字段的结构体，供结构化日志使用。
func (n NamespacedObject) MarshalLog() interface{} {
	// 空字段通过 omitempty 省略。
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
