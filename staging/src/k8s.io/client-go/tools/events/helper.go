/*
Copyright 2021 The Kubernetes Authors.

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

package events

import (
	"fmt"

	corev1 "k8s.io/api/core/v1"
	eventsv1 "k8s.io/api/events/v1"
	eventsv1beta1 "k8s.io/api/events/v1beta1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
)

// mapping 记录各事件 API 版本中“被引用对象”字段的前缀。
//
// events.k8s.io 使用 regarding，core/v1 使用 involvedObject；字段选择器必须
// 与服务端字段名完全一致，否则 List/Watch 会静默返回空结果。
var mapping = map[schema.GroupVersion]string{
	eventsv1.SchemeGroupVersion:      "regarding",
	eventsv1beta1.SchemeGroupVersion: "regarding",
	corev1.SchemeGroupVersion:        "involvedObject",
}

// GetFieldSelector returns the appropriate field selector based on the API version being used to communicate with the server.
// The returned field selector can be used with List and Watch to filter desired events.
//
// 只填充调用方实际提供的维度：空值不写入 selector，避免把“未指定”误当成
// “必须为空”。未知 eventsGroupVersion 直接报错，防止拼出错误的字段名。
func GetFieldSelector(eventsGroupVersion schema.GroupVersion, regardingGroupVersionKind schema.GroupVersionKind, regardingName string, regardingUID types.UID) (fields.Selector, error) {
	field := fields.Set{}

	if _, ok := mapping[eventsGroupVersion]; !ok {
		return nil, fmt.Errorf("unknown version %v", eventsGroupVersion)
	}
	prefix := mapping[eventsGroupVersion]

	if len(regardingName) > 0 {
		field[prefix+".name"] = regardingName
	}

	if len(regardingGroupVersionKind.Kind) > 0 {
		field[prefix+".kind"] = regardingGroupVersionKind.Kind
	}

	regardingGroupVersion := regardingGroupVersionKind.GroupVersion()
	if !regardingGroupVersion.Empty() {
		field[prefix+".apiVersion"] = regardingGroupVersion.String()
	}

	if len(regardingUID) > 0 {
		field[prefix+".uid"] = string(regardingUID)
	}

	return field.AsSelector(), nil
}
