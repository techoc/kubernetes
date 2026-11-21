/*
Copyright 2020 The Kubernetes Authors.

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
	"fmt"

	v1 "k8s.io/api/events/v1"
	conversion "k8s.io/apimachinery/pkg/conversion"
	"k8s.io/apimachinery/pkg/runtime"
	k8s_api "k8s.io/kubernetes/pkg/apis/core"
	k8s_api_v1 "k8s.io/kubernetes/pkg/apis/core/v1"
)

// Convert_v1_Event_To_core_Event 将 events.k8s.io/v1 的 Event 转换为 core Event。
// 自动生成的转换只处理同名字段，字段改名（Regarding、Note 等）需在此手工补齐。
func Convert_v1_Event_To_core_Event(in *v1.Event, out *k8s_api.Event, s conversion.Scope) error {
	if err := autoConvert_v1_Event_To_core_Event(in, out, s); err != nil {
		return err
	}
	// 新版 regarding 对应 core 中已废弃的 involvedObject，需显式转换以保持字段兼容。
	if err := k8s_api_v1.Convert_v1_ObjectReference_To_core_ObjectReference(&in.Regarding, &out.InvolvedObject, s); err != nil {
		return err
	}
	// deprecatedSource 对应 core 中已废弃的 source 字段。
	if err := k8s_api_v1.Convert_v1_EventSource_To_core_EventSource(&in.DeprecatedSource, &out.Source, s); err != nil {
		return err
	}
	// 以下字段在新版中均已改名，逐项映射到 core 的旧字段名。
	out.Message = in.Note
	out.FirstTimestamp = in.DeprecatedFirstTimestamp
	out.LastTimestamp = in.DeprecatedLastTimestamp
	out.Count = in.DeprecatedCount
	return nil
}

// Convert_core_Event_To_v1_Event 将 core Event 转换为 events.k8s.io/v1 的 Event，
// 是 Convert_v1_Event_To_core_Event 的反向过程。
func Convert_core_Event_To_v1_Event(in *k8s_api.Event, out *v1.Event, s conversion.Scope) error {
	if err := autoConvert_core_Event_To_v1_Event(in, out, s); err != nil {
		return err
	}
	// core 的 involvedObject 在新版中改名为 regarding。
	if err := k8s_api_v1.Convert_core_ObjectReference_To_v1_ObjectReference(&in.InvolvedObject, &out.Regarding, s); err != nil {
		return err
	}
	// core 的 source 对应新版的 deprecatedSource。
	if err := k8s_api_v1.Convert_core_EventSource_To_v1_EventSource(&in.Source, &out.DeprecatedSource, s); err != nil {
		return err
	}
	// 将旧字段名逐项映射回新字段名。
	out.Note = in.Message
	out.DeprecatedFirstTimestamp = in.FirstTimestamp
	out.DeprecatedLastTimestamp = in.LastTimestamp
	out.DeprecatedCount = in.Count
	return nil
}

// AddFieldLabelConversionsForEvent 为 Event 注册字段标签的转换函数，
// 将 events.k8s.io 版本的字段路径映射为 core 版本 ToSelectableFields 可识别的字段集。
func AddFieldLabelConversionsForEvent(scheme *runtime.Scheme) error {
	mapping := map[string]string{
		"reason":                    "reason",
		"regarding.kind":            "involvedObject.kind",            // 将 events.k8s.io 字段映射为 ToSelectableFields 返回的字段集
		"regarding.namespace":       "involvedObject.namespace",       // 将 events.k8s.io 字段映射为 ToSelectableFields 返回的字段集
		"regarding.name":            "involvedObject.name",            // 将 events.k8s.io 字段映射为 ToSelectableFields 返回的字段集
		"regarding.uid":             "involvedObject.uid",             // 将 events.k8s.io 字段映射为 ToSelectableFields 返回的字段集
		"regarding.apiVersion":      "involvedObject.apiVersion",      // 将 events.k8s.io 字段映射为 ToSelectableFields 返回的字段集
		"regarding.resourceVersion": "involvedObject.resourceVersion", // 将 events.k8s.io 字段映射为 ToSelectableFields 返回的字段集
		"regarding.fieldPath":       "involvedObject.fieldPath",       // 将 events.k8s.io 字段映射为 ToSelectableFields 返回的字段集
		"reportingController":       "reportingComponent",             // 将 events.k8s.io 字段映射为 ToSelectableFields 返回的字段集
		"type":                      "type",
		"metadata.namespace":        "metadata.namespace",
		"metadata.name":             "metadata.name",
	}
	return scheme.AddFieldLabelConversionFunc(SchemeGroupVersion.WithKind("Event"),
		func(label, value string) (string, string, error) {
			mappedLabel, ok := mapping[label]
			if !ok {
				return "", "", fmt.Errorf("field label not supported: %s", label)
			}
			return mappedLabel, value, nil
		},
	)
}
