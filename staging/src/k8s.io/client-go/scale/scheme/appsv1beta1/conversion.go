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

package appsv1beta1

import (
	"fmt"

	v1beta1 "k8s.io/api/apps/v1beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/conversion"
	scheme "k8s.io/client-go/scale/scheme"
)

// Convert_scheme_ScaleStatus_To_v1beta1_ScaleStatus 把内部 ScaleStatus 转换为
// apps/v1beta1 版本。
//
// v1beta1 同时提供旧的 map 形式 Selector 与新的字符串形式 TargetSelector，
// 因此需要兼容生成：只有在选择器不含 matchExpressions 时才能无损填充旧字段。
func Convert_scheme_ScaleStatus_To_v1beta1_ScaleStatus(in *scheme.ScaleStatus, out *v1beta1.ScaleStatus, s conversion.Scope) error {
	out.Replicas = in.Replicas
	out.Selector = nil
	out.TargetSelector = ""
	if in.Selector != nil {
		// 旧字段只能表达 matchLabels；含表达式时宁可不填，也不做有损转换。
		if in.Selector.MatchExpressions == nil || len(in.Selector.MatchExpressions) == 0 {
			out.Selector = in.Selector.MatchLabels
		}

		// 新字段始终以规范化字符串表达完整选择器。
		selector, err := metav1.LabelSelectorAsSelector(in.Selector)
		if err != nil {
			return fmt.Errorf("invalid label selector: %v", err)
		}
		out.TargetSelector = selector.String()
	}

	return nil
}

// Convert_v1beta1_ScaleStatus_To_scheme_ScaleStatus 把 apps/v1beta1 ScaleStatus
// 转换为内部版本，优先使用新的 TargetSelector，回退到旧的 Selector。
func Convert_v1beta1_ScaleStatus_To_scheme_ScaleStatus(in *v1beta1.ScaleStatus, out *scheme.ScaleStatus, s conversion.Scope) error {
	out.Replicas = in.Replicas

	// Normally when 2 fields map to the same internal value we favor the old field, since
	// old clients can't be expected to know about new fields but clients that know about the
	// new field can be expected to know about the old field (though that's not quite true, due
	// to kubectl apply). However, these fields are readonly, so any non-nil value should work.
	// 此处反其道优先新字段：两者都是只读字段，不存在旧客户端回写覆盖的问题。
	if in.TargetSelector != "" {
		labelSelector, err := metav1.ParseToLabelSelector(in.TargetSelector)
		if err != nil {
			out.Selector = nil
			return fmt.Errorf("failed to parse target selector: %v", err)
		}
		out.Selector = labelSelector
	} else if in.Selector != nil {
		// 复制 map，避免内部对象与外部对象共享底层存储。
		out.Selector = new(metav1.LabelSelector)
		selector := make(map[string]string)
		for key, val := range in.Selector {
			selector[key] = val
		}
		out.Selector.MatchLabels = selector
	} else {
		out.Selector = nil
	}

	return nil
}
