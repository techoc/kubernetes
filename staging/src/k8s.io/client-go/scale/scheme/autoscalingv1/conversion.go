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

package autoscalingv1

import (
	"fmt"

	v1 "k8s.io/api/autoscaling/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/conversion"
	scheme "k8s.io/client-go/scale/scheme"
)

// Convert_scheme_ScaleStatus_To_v1_ScaleStatus 把内部 ScaleStatus 转成
// autoscaling/v1 的 ScaleStatus。v1 的 selector 是字符串形式，因此需要把
// LabelSelector 序列化；无法转换时返回错误，避免向上层返回错误的 selector。
func Convert_scheme_ScaleStatus_To_v1_ScaleStatus(in *scheme.ScaleStatus, out *v1.ScaleStatus, s conversion.Scope) error {
	out.Replicas = in.Replicas
	// 先置空，保证 nil selector 在输出中是空字符串而不是残留的零值以外的内容。
	out.Selector = ""
	if in.Selector != nil {
		selector, err := metav1.LabelSelectorAsSelector(in.Selector)
		if err != nil {
			return fmt.Errorf("invalid label selector: %v", err)
		}
		out.Selector = selector.String()
	}

	return nil
}

// Convert_v1_ScaleStatus_To_scheme_ScaleStatus 把 autoscaling/v1 的字符串
// selector 解析回内部 LabelSelector。空字符串表示没有 selector，保持 nil。
func Convert_v1_ScaleStatus_To_scheme_ScaleStatus(in *v1.ScaleStatus, out *scheme.ScaleStatus, s conversion.Scope) error {
	out.Replicas = in.Replicas
	if in.Selector != "" {
		labelSelector, err := metav1.ParseToLabelSelector(in.Selector)
		if err != nil {
			// 解析失败时显式清空，避免输出里带上部分填充的结果。
			out.Selector = nil
			return fmt.Errorf("failed to parse target selector: %v", err)
		}
		out.Selector = labelSelector
	}

	return nil
}
