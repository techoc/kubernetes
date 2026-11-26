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

package appsv1beta2

import (
	"fmt"

	v1beta2 "k8s.io/api/apps/v1beta2"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/conversion"
	scheme "k8s.io/client-go/scale/scheme"
)

// Convert_scheme_ScaleStatus_To_v1beta2_ScaleStatus 把内部 ScaleStatus 转换为 apps/v1beta2 版本。
// v1beta2 的 Selector 是旧的字符串 map，TargetSelector 是新的字符串选择器：
// 只有当内部选择器不含 matchExpressions 时，才能无损降级为 map 形式。
func Convert_scheme_ScaleStatus_To_v1beta2_ScaleStatus(in *scheme.ScaleStatus, out *v1beta2.ScaleStatus, s conversion.Scope) error {
	out.Replicas = in.Replicas
	out.Selector = nil
	out.TargetSelector = ""
	if in.Selector != nil {
		if in.Selector.MatchExpressions == nil || len(in.Selector.MatchExpressions) == 0 {
			out.Selector = in.Selector.MatchLabels
		}

		// 无论如何都生成 TargetSelector 的字符串形式，让新版客户端能拿到完整选择器。
		selector, err := metav1.LabelSelectorAsSelector(in.Selector)
		if err != nil {
			return fmt.Errorf("invalid label selector: %v", err)
		}
		out.TargetSelector = selector.String()
	}

	return nil
}

// Convert_v1beta2_ScaleStatus_To_scheme_ScaleStatus 把 apps/v1beta2 版本转换回内部 ScaleStatus。
func Convert_v1beta2_ScaleStatus_To_scheme_ScaleStatus(in *v1beta2.ScaleStatus, out *scheme.ScaleStatus, s conversion.Scope) error {
	out.Replicas = in.Replicas

	// 通常两个字段映射到同一内部值时优先取旧字段：不能要求旧客户端了解新字段，
	// 但了解新字段的客户端理应了解旧字段（不过由于历史原因并不总是如此）。
	// 与 kubectl apply 相关）。但这些字段是只读的，任何非 nil 值都可行。
	// 优先解析更完整的 TargetSelector；只有它为空时才退回旧的 Selector map。
	if in.TargetSelector != "" {
		labelSelector, err := metav1.ParseToLabelSelector(in.TargetSelector)
		if err != nil {
			out.Selector = nil
			return fmt.Errorf("failed to parse target selector: %v", err)
		}
		out.Selector = labelSelector
	} else if in.Selector != nil {
		out.Selector = new(metav1.LabelSelector)
		selector := make(map[string]string)
		// 拷贝 map，避免输出对象与输入对象共享底层存储。
		for key, val := range in.Selector {
			selector[key] = val
		}
		out.Selector.MatchLabels = selector
	} else {
		out.Selector = nil
	}

	return nil
}
