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

package autoscaling

// DropRoundTripHorizontalPodAutoscalerAnnotations 移除 HorizontalPodAutoscaler 中
// 用于在高版本 API 间往返保存字段的所有注解。
// 如果没有任何改动，返回原始输入 map 和 false，调用方据此可以避免不必要的拷贝。
//
// internal -> external 转换时，在设置任何自定义注解之前都应先调用它，例如：
//
//	annotations, copiedAnnotations := DropRoundTripHorizontalPodAutoscalerAnnotations(externalObj.Annotations)
//	externalObj.Annotations = annotations
//
//	if internal.SomeField != nil {
//	  if !copiedAnnotations {
//	    externalObj.Annotations = DeepCopyStringMap(externalObj.Annotations)
//	    copiedAnnotations = true
//	  }
//	  externalObj.Annotations[...] = json.Marshal(...)
//	}
func DropRoundTripHorizontalPodAutoscalerAnnotations(in map[string]string) (out map[string]string, copied bool) {
	_, hasMetricsSpecs := in[MetricSpecsAnnotation]
	_, hasBehaviorSpecs := in[BehaviorSpecsAnnotation]
	_, hasToleranceScaleDown := in[ToleranceScaleDownAnnotation]
	_, hasToleranceScaleUp := in[ToleranceScaleUpAnnotation]
	_, hasMetricsStatuses := in[MetricStatusesAnnotation]
	_, hasConditions := in[HorizontalPodAutoscalerConditionsAnnotation]
	if hasMetricsSpecs || hasBehaviorSpecs || hasToleranceScaleDown || hasToleranceScaleUp || hasMetricsStatuses || hasConditions {
		// 必须先拷贝再删除，否则会污染调用方持有的原始 map。
		out = DeepCopyStringMap(in)
		delete(out, MetricSpecsAnnotation)
		delete(out, BehaviorSpecsAnnotation)
		delete(out, ToleranceScaleDownAnnotation)
		delete(out, ToleranceScaleUpAnnotation)
		delete(out, MetricStatusesAnnotation)
		delete(out, HorizontalPodAutoscalerConditionsAnnotation)
		return out, true
	}
	return in, false
}

// DeepCopyStringMap 返回输入 map 的一份拷贝。
// 输入为 nil 时返回空 map，这样调用方可以直接往结果里写入而无需再判空。
func DeepCopyStringMap(in map[string]string) map[string]string {
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}
