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

package extensionsv1beta1

import (
	"fmt"

	v1beta1 "k8s.io/api/extensions/v1beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/conversion"
	scheme "k8s.io/client-go/scale/scheme"
)

// Convert_scheme_ScaleStatus_To_v1beta1_ScaleStatus 把内部 ScaleStatus 转成
// extensions/v1beta1 表示。旧版本同时有 map 形式 Selector 与字符串 TargetSelector，
// 这里把结构化选择器展开成两者以兼容老客户端。
func Convert_scheme_ScaleStatus_To_v1beta1_ScaleStatus(in *scheme.ScaleStatus, out *v1beta1.ScaleStatus, s conversion.Scope) error {
	out.Replicas = in.Replicas
	out.Selector = nil
	out.TargetSelector = ""
	if in.Selector != nil {
		// 只有纯 MatchLabels 才能无损映射到 map 形式；带表达式时仅保留字符串形式。
		if in.Selector.MatchExpressions == nil || len(in.Selector.MatchExpressions) == 0 {
			out.Selector = in.Selector.MatchLabels
		}

		selector, err := metav1.LabelSelectorAsSelector(in.Selector)
		if err != nil {
			return fmt.Errorf("invalid label selector: %v", err)
		}
		out.TargetSelector = selector.String()
	}

	return nil
}

// Convert_v1beta1_ScaleStatus_To_scheme_ScaleStatus 把 v1beta1 表示归一化为内部结构。
func Convert_v1beta1_ScaleStatus_To_scheme_ScaleStatus(in *v1beta1.ScaleStatus, out *scheme.ScaleStatus, s conversion.Scope) error {
	out.Replicas = in.Replicas

	// 通常两个字段映射到同一内部值时优先旧字段，但这里两者皆只读，任一非空值都可用。
	// 优先解析信息更丰富的 TargetSelector（支持表达式），失败或为空时退回 map 形式。
	if in.TargetSelector != "" {
		labelSelector, err := metav1.ParseToLabelSelector(in.TargetSelector)
		if err != nil {
			out.Selector = nil
			return fmt.Errorf("failed to parse target selector: %v", err)
		}
		out.Selector = labelSelector
	} else if in.Selector != nil {
		// 必须深拷贝 map，避免内部对象与输入共享底层存储。
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
