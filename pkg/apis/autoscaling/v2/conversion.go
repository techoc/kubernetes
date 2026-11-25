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

package v2

import (
	autoscalingv2 "k8s.io/api/autoscaling/v2"

	"k8s.io/apimachinery/pkg/conversion"
	"k8s.io/kubernetes/pkg/apis/autoscaling"
)

// Convert_autoscaling_HorizontalPodAutoscaler_To_v2_HorizontalPodAutoscaler 把 internal HPA
// 转成 v2 HPA。v2 的结构与 internal 一一对应，不需要借助注解往返保存字段，
// 因此要清掉可能从其他版本对象带过来的往返注解，避免被意外序列化。
func Convert_autoscaling_HorizontalPodAutoscaler_To_v2_HorizontalPodAutoscaler(in *autoscaling.HorizontalPodAutoscaler, out *autoscalingv2.HorizontalPodAutoscaler, s conversion.Scope) error {
	if err := autoConvert_autoscaling_HorizontalPodAutoscaler_To_v2_HorizontalPodAutoscaler(in, out, s); err != nil {
		return err
	}
	// v2 与 internal 之间不需要注解即可完成往返，清掉其他版本遗留的注解避免被序列化。
	out.Annotations, _ = autoscaling.DropRoundTripHorizontalPodAutoscalerAnnotations(out.Annotations)
	return nil
}

// Convert_v2_HorizontalPodAutoscaler_To_autoscaling_HorizontalPodAutoscaler 把 v2 HPA
// 转成 internal HPA。与反方向同理，清掉其他版本遗留的往返注解，
// 保证 v2 转换路径上不产生注解。
func Convert_v2_HorizontalPodAutoscaler_To_autoscaling_HorizontalPodAutoscaler(in *autoscalingv2.HorizontalPodAutoscaler, out *autoscaling.HorizontalPodAutoscaler, s conversion.Scope) error {
	if err := autoConvert_v2_HorizontalPodAutoscaler_To_autoscaling_HorizontalPodAutoscaler(in, out, s); err != nil {
		return err
	}
	// v2 与 internal 之间不需要注解即可完成往返，清掉其他版本遗留的注解避免被序列化。
	out.Annotations, _ = autoscaling.DropRoundTripHorizontalPodAutoscalerAnnotations(out.Annotations)
	return nil
}
