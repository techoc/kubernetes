/*
Copyright 2023 The Kubernetes Authors.

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

package v1beta3

import (
	"k8s.io/api/flowcontrol/v1beta3"
	"k8s.io/apimachinery/pkg/conversion"
	"k8s.io/kubernetes/pkg/apis/flowcontrol"
)

// Convert_v1beta3_PriorityLevelConfiguration_To_flowcontrol_PriorityLevelConfiguration 把 v1beta3 对象转换为内部类型，并处理往返注解：
// v1beta3 -> internal 转换时需要移除 NominalConcurrencyShares 字段的往返注解，
// 同时不能修改源（v1beta3）对象的 annotations。
func Convert_v1beta3_PriorityLevelConfiguration_To_flowcontrol_PriorityLevelConfiguration(in *v1beta3.PriorityLevelConfiguration, out *flowcontrol.PriorityLevelConfiguration, s conversion.Scope) error {
	if err := autoConvert_v1beta3_PriorityLevelConfiguration_To_flowcontrol_PriorityLevelConfiguration(in, out, nil); err != nil {
		return err
	}

	// v1beta3 -> internal 转换期间：
	//  - 移除 'NominalConcurrencyShares' 字段的往返注解；
	//  - 确保不修改源（v1beta3）对象的 annotations。
	annotations, copied := dropPriorityLevelConcurrencyShareDefaultAnnotation(out.ObjectMeta.Annotations)
	if copied {
		out.ObjectMeta.Annotations = annotations
	}
	return nil
}

// Convert_flowcontrol_PriorityLevelConfiguration_To_v1beta3_PriorityLevelConfiguration 把内部类型转换为 v1beta3 对象，并处理往返注解：
// internal -> v1beta3 转换时，若 NominalConcurrencyShares 为 0 则添加注解以保留零值语义，
// 同时不能修改源（内部类型）对象的 annotations。
func Convert_flowcontrol_PriorityLevelConfiguration_To_v1beta3_PriorityLevelConfiguration(in *flowcontrol.PriorityLevelConfiguration, out *v1beta3.PriorityLevelConfiguration, s conversion.Scope) error {
	if err := autoConvert_flowcontrol_PriorityLevelConfiguration_To_v1beta3_PriorityLevelConfiguration(in, out, nil); err != nil {
		return err
	}

	// internal -> v1beta3 转换期间：
	//  - 当 'NominalConcurrencyShares' 字段值为 0 时，添加该字段的往返注解；
	//  - 确保不修改源（internal）对象的 annotations。
	if limited := in.Spec.Limited; limited != nil && limited.NominalConcurrencyShares == 0 {
		annotations, copied := addPriorityLevelConcurrencyShareDefaultAnnotation(out.ObjectMeta.Annotations)
		if copied {
			out.ObjectMeta.Annotations = annotations
		}
	}

	return nil
}

// dropPriorityLevelConcurrencyShareDefaultAnnotation 从注解 map 中移除保留零并发份额的注解。
// 如果输入中不存在该注解则原样返回；存在时复制 map 后再删除，避免修改调用方的 map。
func dropPriorityLevelConcurrencyShareDefaultAnnotation(in map[string]string) (map[string]string, bool) {
	if _, ok := in[v1beta3.PriorityLevelPreserveZeroConcurrencySharesKey]; !ok {
		return in, false
	}

	out := copyStringMap(in)
	delete(out, v1beta3.PriorityLevelPreserveZeroConcurrencySharesKey)
	return out, true
}

// addPriorityLevelConcurrencyShareDefaultAnnotation 向注解 map 中添加保留零并发份额的注解。
// 如果已存在该注解则原样返回；否则复制 map 后再添加，避免修改调用方的 map。
func addPriorityLevelConcurrencyShareDefaultAnnotation(in map[string]string) (map[string]string, bool) {
	if _, ok := in[v1beta3.PriorityLevelPreserveZeroConcurrencySharesKey]; ok {
		return in, false
	}

	out := copyStringMap(in)
	out[v1beta3.PriorityLevelPreserveZeroConcurrencySharesKey] = ""
	return out, true
}

// copyStringMap 返回输入 map 的一份副本。
// 如果输入为 nil，则返回一个空 map。
func copyStringMap(in map[string]string) map[string]string {
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}
