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

package v1beta1

import (
	"fmt"

	v1beta1 "k8s.io/api/events/v1beta1"
	conversion "k8s.io/apimachinery/pkg/conversion"
	"k8s.io/apimachinery/pkg/runtime"
	k8s_api "k8s.io/kubernetes/pkg/apis/core"
	k8s_api_v1 "k8s.io/kubernetes/pkg/apis/core/v1"
)

// Convert_v1beta1_Event_To_core_Event 将外部 events/v1beta1 事件转换为内部 core.Event。
func Convert_v1beta1_Event_To_core_Event(in *v1beta1.Event, out *k8s_api.Event, s conversion.Scope) error {
	// 先复用生成的字段映射，再手工处理字段重命名与拆分。
	if err := autoConvert_v1beta1_Event_To_core_Event(in, out, s); err != nil {
		return err
	}
	// Regarding 在内部表示中对应 InvolvedObject。
	if err := k8s_api_v1.Convert_v1_ObjectReference_To_core_ObjectReference(&in.Regarding, &out.InvolvedObject, s); err != nil {
		return err
	}
	// 弃用的 Source 字段仍需映射，避免历史数据丢失。
	if err := k8s_api_v1.Convert_v1_EventSource_To_core_EventSource(&in.DeprecatedSource, &out.Source, s); err != nil {
		return err
	}
	// Note、时间戳与计数在两版中字段名不同，必须手工搬运。
	out.Message = in.Note
	out.FirstTimestamp = in.DeprecatedFirstTimestamp
	out.LastTimestamp = in.DeprecatedLastTimestamp
	out.Count = in.DeprecatedCount
	return nil
}

// Convert_core_Event_To_v1beta1_Event 是上述转换的逆向实现。
func Convert_core_Event_To_v1beta1_Event(in *k8s_api.Event, out *v1beta1.Event, s conversion.Scope) error {
	if err := autoConvert_core_Event_To_v1beta1_Event(in, out, s); err != nil {
		return err
	}
	// 内部 InvolvedObject 需要转回外部的 Regarding。
	if err := k8s_api_v1.Convert_core_ObjectReference_To_v1_ObjectReference(&in.InvolvedObject, &out.Regarding, s); err != nil {
		return err
	}
	if err := k8s_api_v1.Convert_core_EventSource_To_v1_EventSource(&in.Source, &out.DeprecatedSource, s); err != nil {
		return err
	}
	// 与正向转换对应，恢复旧字段名。
	out.Note = in.Message
	out.DeprecatedFirstTimestamp = in.FirstTimestamp
	out.DeprecatedLastTimestamp = in.LastTimestamp
	out.DeprecatedCount = in.Count
	return nil
}

// AddFieldLabelConversionsForEvent 建立事件字段选择器标签的别名映射。
func AddFieldLabelConversionsForEvent(scheme *runtime.Scheme) error {
	mapping := map[string]string{
		"reason":                    "reason",
		"regarding.kind":            "involvedObject.kind",            // 把 events.k8s.io 字段映射到 ToSelectableFields 返回的字段集。
		"regarding.namespace":       "involvedObject.namespace",       // 把 events.k8s.io 字段映射到 ToSelectableFields 返回的字段集。
		"regarding.name":            "involvedObject.name",            // 把 events.k8s.io 字段映射到 ToSelectableFields 返回的字段集。
		"regarding.uid":             "involvedObject.uid",             // 把 events.k8s.io 字段映射到 ToSelectableFields 返回的字段集。
		"regarding.apiVersion":      "involvedObject.apiVersion",      // 把 events.k8s.io 字段映射到 ToSelectableFields 返回的字段集。
		"regarding.resourceVersion": "involvedObject.resourceVersion", // 把 events.k8s.io 字段映射到 ToSelectableFields 返回的字段集。
		"regarding.fieldPath":       "involvedObject.fieldPath",       // 把 events.k8s.io 字段映射到 ToSelectableFields 返回的字段集。
		"reportingController":       "reportingComponent",             // 把 events.k8s.io 字段映射到 ToSelectableFields 返回的字段集。
		"type":                      "type",
		"metadata.namespace":        "metadata.namespace",
		"metadata.name":             "metadata.name",
	}
	return scheme.AddFieldLabelConversionFunc(SchemeGroupVersion.WithKind("Event"),
		func(label, value string) (string, string, error) {
			// 未知标签必须报错，否则会静默得到空选择器并产生错误的筛选结果。
			mappedLabel, ok := mapping[label]
			if !ok {
				return "", "", fmt.Errorf("field label not supported: %s", label)
			}
			return mappedLabel, value, nil
		},
	)
}
