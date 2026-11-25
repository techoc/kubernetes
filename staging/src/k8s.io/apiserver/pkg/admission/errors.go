/*
Copyright 2015 The Kubernetes Authors.

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

package admission

import (
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime/schema"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
)

// extractResourceName 尽力为错误消息提取资源名。
// 优先使用 Attributes 上的名字；名字为空（例如 create 时名称尚未生成）时退回对象元数据，
// 再不行就使用固定的 "Unknown" 占位符。永不返回错误：错误格式化本身不能因为取不到名字而失败，
// 唯一的例外路径用 "Unknown/errorGettingName" 表示，其中斜杠保证这个名字在任何资源名下都非法，
// 从而不会与真实对象混淆。
func extractResourceName(a Attributes) (name string, resource schema.GroupResource, err error) {
	resource = a.GetResource().GroupResource()

	if len(a.GetName()) > 0 {
		return a.GetName(), resource, nil
	}

	name = "Unknown"
	obj := a.GetObject()
	if obj != nil {
		accessor, err := meta.Accessor(obj)
		if err != nil {
			// 并非所有对象都有 ObjectMeta；取不到时返回带斜杠的名字（始终非法）
			return "Unknown/errorGettingName", resource, nil
		}

		// 对象名可能尚未由 API server 生成，此时 generateName 是最有信息量的标识。
		if len(accessor.GetName()) > 0 {
			name = accessor.GetName()
		} else if len(accessor.GetGenerateName()) > 0 {
			name = accessor.GetGenerateName()
		}
	}
	return name, resource, nil
}

// NewForbidden 是一个工具函数，返回格式良好的准入控制拒绝错误响应。
//
// 把插件返回的任意错误包装成带资源/名称上下文的
// Forbidden，便于用户定位是哪个对象被拒。已经是 Forbidden 的错误直接透传，避免重复包装后
// 原因链被改写、客户端无法用 IsForbidden 判断。
func NewForbidden(a Attributes, internalError error) error {
	// 不对同类型错误重复包装
	if apierrors.IsForbidden(internalError) {
		return internalError
	}
	name, resource, err := extractResourceName(a)
	if err != nil {
		return apierrors.NewInternalError(utilerrors.NewAggregate([]error{internalError, err}))
	}
	return apierrors.NewForbidden(resource, name, internalError)
}

// NewNotFound 是一个工具函数，返回格式良好的准入控制错误响应。
//
// 与 NewForbidden 对称，供插件表示“请求引用的对象不存在”。
func NewNotFound(a Attributes) error {
	name, resource, err := extractResourceName(a)
	if err != nil {
		return apierrors.NewInternalError(err)
	}
	return apierrors.NewNotFound(resource, name)
}
