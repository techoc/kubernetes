/*
Copyright 2016 The Kubernetes Authors.

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
	"encoding/json"

	autoscalingv1 "k8s.io/api/autoscaling/v1"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/conversion"
	"k8s.io/kubernetes/pkg/apis/autoscaling"
	"k8s.io/kubernetes/pkg/apis/core"
)

// Convert_autoscaling_MetricTarget_To_v1_CrossVersionObjectReference 是空转换。
// MetricTarget 与 CrossVersionObjectReference 语义完全不同，没有可映射的字段，
// v1 中相关信息通过其他字段或注解保存，因此这里不做任何事。
func Convert_autoscaling_MetricTarget_To_v1_CrossVersionObjectReference(in *autoscaling.MetricTarget, out *autoscalingv1.CrossVersionObjectReference, s conversion.Scope) error {
	return nil
}

// Convert_v1_CrossVersionObjectReference_To_autoscaling_MetricTarget 是空转换，
// 与反向的空转换对应：这两个类型之间没有可映射的字段。
func Convert_v1_CrossVersionObjectReference_To_autoscaling_MetricTarget(in *autoscalingv1.CrossVersionObjectReference, out *autoscaling.MetricTarget, s conversion.Scope) error {
	return nil
}

// Convert_autoscaling_ExternalMetricSource_To_v1_ExternalMetricSource 把 internal 的
// ExternalMetricSource 展平为 v1 的扁平字段：v1 用 TargetValue/TargetAverageValue
// 两个字段区分目标类型，而 internal 用 MetricTarget.Type 统一表示。
func Convert_autoscaling_ExternalMetricSource_To_v1_ExternalMetricSource(in *autoscaling.ExternalMetricSource, out *autoscalingv1.ExternalMetricSource, s conversion.Scope) error {
	out.MetricName = in.Metric.Name
	out.TargetValue = in.Target.Value
	out.TargetAverageValue = in.Target.AverageValue
	out.MetricSelector = in.Metric.Selector
	return nil
}

// Convert_v1_ExternalMetricSource_To_autoscaling_ExternalMetricSource 把 v1 的扁平字段
// 还原为 internal 的 MetricTarget。v1 没有显式的类型字段，只能根据 TargetValue
// 是否为 nil 推断目标类型。
func Convert_v1_ExternalMetricSource_To_autoscaling_ExternalMetricSource(in *autoscalingv1.ExternalMetricSource, out *autoscaling.ExternalMetricSource, s conversion.Scope) error {
	value := in.TargetValue
	averageValue := in.TargetAverageValue
	var metricType autoscaling.MetricTargetType
	if value == nil {
		metricType = autoscaling.AverageValueMetricType
	} else {
		metricType = autoscaling.ValueMetricType
	}
	out.Target = autoscaling.MetricTarget{
		Type:         metricType,
		Value:        value,
		AverageValue: averageValue,
	}

	out.Metric = autoscaling.MetricIdentifier{
		Name:     in.MetricName,
		Selector: in.MetricSelector,
	}
	return nil
}

// Convert_autoscaling_ObjectMetricSource_To_v1_ObjectMetricSource 把 internal 的
// ObjectMetricSource 映射到 v1：将 Target、Metric 展开成扁平字段，
// 并且只在 Value 非 nil 时才写入非指针的 TargetValue。
func Convert_autoscaling_ObjectMetricSource_To_v1_ObjectMetricSource(in *autoscaling.ObjectMetricSource, out *autoscalingv1.ObjectMetricSource, s conversion.Scope) error {
	if in.Target.Value != nil {
		out.TargetValue = *in.Target.Value
	}
	out.AverageValue = in.Target.AverageValue
	out.Target = autoscalingv1.CrossVersionObjectReference{
		Kind:       in.DescribedObject.Kind,
		Name:       in.DescribedObject.Name,
		APIVersion: in.DescribedObject.APIVersion,
	}
	out.MetricName = in.Metric.Name
	out.Selector = in.Metric.Selector
	return nil
}

// Convert_v1_ObjectMetricSource_To_autoscaling_ObjectMetricSource 把 v1 的扁平字段
// 还原为 internal 的 ObjectMetricSource，根据 AverageValue 是否为 nil 推断目标类型。
func Convert_v1_ObjectMetricSource_To_autoscaling_ObjectMetricSource(in *autoscalingv1.ObjectMetricSource, out *autoscaling.ObjectMetricSource, s conversion.Scope) error {
	var metricType autoscaling.MetricTargetType
	var targetValue *resource.Quantity
	if in.AverageValue == nil {
		metricType = autoscaling.ValueMetricType
		targetValue = &in.TargetValue
	} else {
		metricType = autoscaling.AverageValueMetricType
		// Only preserve non-zero targetValue for averageValue metrics.
		// The v1 type cannot omit the value field, and serializes value:"0", which fails validation on round-trip.
		if !in.TargetValue.IsZero() {
			targetValue = &in.TargetValue
		}
	}

	out.Target = autoscaling.MetricTarget{
		Type:         metricType,
		Value:        targetValue,
		AverageValue: in.AverageValue,
	}
	out.DescribedObject = autoscaling.CrossVersionObjectReference{
		Kind:       in.Target.Kind,
		Name:       in.Target.Name,
		APIVersion: in.Target.APIVersion,
	}
	out.Metric = autoscaling.MetricIdentifier{
		Name:     in.MetricName,
		Selector: in.Selector,
	}
	return nil
}

// Convert_autoscaling_PodsMetricSource_To_v1_PodsMetricSource 把 internal 的 Pods 指标
// 映射为 v1 结构。v1 只能表示 AverageValue 目标类型。
func Convert_autoscaling_PodsMetricSource_To_v1_PodsMetricSource(in *autoscaling.PodsMetricSource, out *autoscalingv1.PodsMetricSource, s conversion.Scope) error {
	if in.Target.AverageValue != nil {
		out.TargetAverageValue = *in.Target.AverageValue
	}

	out.MetricName = in.Metric.Name
	out.Selector = in.Metric.Selector
	return nil
}

// Convert_v1_PodsMetricSource_To_autoscaling_PodsMetricSource 把 v1 的 Pods 指标
// 还原为 internal 结构，目标类型固定为 AverageValueMetricType。
func Convert_v1_PodsMetricSource_To_autoscaling_PodsMetricSource(in *autoscalingv1.PodsMetricSource, out *autoscaling.PodsMetricSource, s conversion.Scope) error {
	metricType := autoscaling.AverageValueMetricType

	out.Target = autoscaling.MetricTarget{
		Type:         metricType,
		AverageValue: &in.TargetAverageValue,
	}
	out.Metric = autoscaling.MetricIdentifier{
		Name:     in.MetricName,
		Selector: in.Selector,
	}
	return nil
}

// Convert_autoscaling_ExternalMetricStatus_To_v1_ExternalMetricStatus 把 internal 的
// External 指标状态映射为 v1 的扁平字段，nil 的 quantity 不写出，避免留下零值。
func Convert_autoscaling_ExternalMetricStatus_To_v1_ExternalMetricStatus(in *autoscaling.ExternalMetricStatus, out *autoscalingv1.ExternalMetricStatus, s conversion.Scope) error {
	out.MetricName = in.Metric.Name
	if in.Current.Value != nil {
		out.CurrentValue = *in.Current.Value
	}
	if in.Current.AverageValue != nil {
		out.CurrentAverageValue = in.Current.AverageValue
	}
	out.MetricSelector = in.Metric.Selector
	return nil
}

// Convert_v1_ExternalMetricStatus_To_autoscaling_ExternalMetricStatus 把 v1 的
// External 指标状态还原为 internal 结构。v1 的非指针 CurrentValue 总是被取地址，
// 即使它为零值，也要保留字段存在性。
func Convert_v1_ExternalMetricStatus_To_autoscaling_ExternalMetricStatus(in *autoscalingv1.ExternalMetricStatus, out *autoscaling.ExternalMetricStatus, s conversion.Scope) error {
	value := in.CurrentValue
	averageValue := in.CurrentAverageValue
	out.Current = autoscaling.MetricValueStatus{
		Value:        &value,
		AverageValue: averageValue,
	}
	out.Metric = autoscaling.MetricIdentifier{
		Name:     in.MetricName,
		Selector: in.MetricSelector,
	}
	return nil
}

// Convert_autoscaling_ObjectMetricStatus_To_v1_ObjectMetricStatus 把 internal 的
// Object 指标状态映射为 v1 的扁平字段；AverageValue 要先拷贝再取地址，
// 避免直接引用 internal 对象的字段。
func Convert_autoscaling_ObjectMetricStatus_To_v1_ObjectMetricStatus(in *autoscaling.ObjectMetricStatus, out *autoscalingv1.ObjectMetricStatus, s conversion.Scope) error {
	if in.Current.Value != nil {
		out.CurrentValue = *in.Current.Value
	}
	if in.Current.AverageValue != nil {
		currentAverageValue := *in.Current.AverageValue
		out.AverageValue = &currentAverageValue
	}
	out.Target = autoscalingv1.CrossVersionObjectReference{
		Kind:       in.DescribedObject.Kind,
		Name:       in.DescribedObject.Name,
		APIVersion: in.DescribedObject.APIVersion,
	}
	out.MetricName = in.Metric.Name
	out.Selector = in.Metric.Selector
	return nil
}

// Convert_v1_ObjectMetricStatus_To_autoscaling_ObjectMetricStatus 把 v1 的
// Object 指标状态还原为 internal 结构。
func Convert_v1_ObjectMetricStatus_To_autoscaling_ObjectMetricStatus(in *autoscalingv1.ObjectMetricStatus, out *autoscaling.ObjectMetricStatus, s conversion.Scope) error {
	out.Current = autoscaling.MetricValueStatus{
		Value:        &in.CurrentValue,
		AverageValue: in.AverageValue,
	}
	out.DescribedObject = autoscaling.CrossVersionObjectReference{
		Kind:       in.Target.Kind,
		Name:       in.Target.Name,
		APIVersion: in.Target.APIVersion,
	}
	out.Metric = autoscaling.MetricIdentifier{
		Name:     in.MetricName,
		Selector: in.Selector,
	}
	return nil
}

// Convert_autoscaling_PodsMetricStatus_To_v1_PodsMetricStatus 把 internal 的 Pods
// 指标状态映射为 v1 的扁平字段。
func Convert_autoscaling_PodsMetricStatus_To_v1_PodsMetricStatus(in *autoscaling.PodsMetricStatus, out *autoscalingv1.PodsMetricStatus, s conversion.Scope) error {
	if in.Current.AverageValue != nil {
		out.CurrentAverageValue = *in.Current.AverageValue
	}
	out.MetricName = in.Metric.Name
	out.Selector = in.Metric.Selector
	return nil
}

// Convert_v1_PodsMetricStatus_To_autoscaling_PodsMetricStatus 把 v1 的 Pods
// 指标状态还原为 internal 结构。
func Convert_v1_PodsMetricStatus_To_autoscaling_PodsMetricStatus(in *autoscalingv1.PodsMetricStatus, out *autoscaling.PodsMetricStatus, s conversion.Scope) error {
	out.Current = autoscaling.MetricValueStatus{
		AverageValue: &in.CurrentAverageValue,
	}
	out.Metric = autoscaling.MetricIdentifier{
		Name:     in.MetricName,
		Selector: in.Selector,
	}
	return nil
}

// Convert_v1_ResourceMetricSource_To_autoscaling_ResourceMetricSource 把 v1 的
// Resource 指标映射为 internal 结构，根据 TargetAverageUtilization 是否为 nil 推断目标类型。
func Convert_v1_ResourceMetricSource_To_autoscaling_ResourceMetricSource(in *autoscalingv1.ResourceMetricSource, out *autoscaling.ResourceMetricSource, s conversion.Scope) error {
	out.Name = core.ResourceName(in.Name)
	utilization := in.TargetAverageUtilization
	averageValue := in.TargetAverageValue
	var metricType autoscaling.MetricTargetType
	if utilization == nil {
		metricType = autoscaling.AverageValueMetricType
	} else {
		metricType = autoscaling.UtilizationMetricType
	}
	out.Target = autoscaling.MetricTarget{
		Type:               metricType,
		AverageValue:       averageValue,
		AverageUtilization: utilization,
	}
	return nil
}

// Convert_autoscaling_ResourceMetricSource_To_v1_ResourceMetricSource 把 internal 的
// Resource 指标映射为 v1 的扁平字段。
func Convert_autoscaling_ResourceMetricSource_To_v1_ResourceMetricSource(in *autoscaling.ResourceMetricSource, out *autoscalingv1.ResourceMetricSource, s conversion.Scope) error {
	out.Name = v1.ResourceName(in.Name)
	out.TargetAverageUtilization = in.Target.AverageUtilization
	out.TargetAverageValue = in.Target.AverageValue
	return nil
}

// Convert_v1_ContainerResourceMetricStatus_To_autoscaling_ContainerResourceMetricStatus
// 把 v1 的容器资源指标状态还原为 internal 结构。
func Convert_v1_ContainerResourceMetricStatus_To_autoscaling_ContainerResourceMetricStatus(in *autoscalingv1.ContainerResourceMetricStatus, out *autoscaling.ContainerResourceMetricStatus, s conversion.Scope) error {
	out.Name = core.ResourceName(in.Name)
	out.Container = in.Container
	utilization := in.CurrentAverageUtilization
	averageValue := &in.CurrentAverageValue
	out.Current = autoscaling.MetricValueStatus{
		AverageValue:       averageValue,
		AverageUtilization: utilization,
	}
	return nil
}

// Convert_autoscaling_ContainerResourceMetricStatus_To_v1_ContainerResourceMetricStatus
// 把 internal 的容器资源指标状态映射为 v1 结构，nil 的 AverageValue 不写出。
func Convert_autoscaling_ContainerResourceMetricStatus_To_v1_ContainerResourceMetricStatus(in *autoscaling.ContainerResourceMetricStatus, out *autoscalingv1.ContainerResourceMetricStatus, s conversion.Scope) error {
	out.Name = v1.ResourceName(in.Name)
	out.Container = in.Container
	out.CurrentAverageUtilization = in.Current.AverageUtilization
	if in.Current.AverageValue != nil {
		out.CurrentAverageValue = *in.Current.AverageValue
	}
	return nil
}

// Convert_v1_ResourceMetricStatus_To_autoscaling_ResourceMetricStatus 把 v1 的
// Resource 指标状态还原为 internal 结构。
func Convert_v1_ResourceMetricStatus_To_autoscaling_ResourceMetricStatus(in *autoscalingv1.ResourceMetricStatus, out *autoscaling.ResourceMetricStatus, s conversion.Scope) error {
	out.Name = core.ResourceName(in.Name)
	utilization := in.CurrentAverageUtilization
	averageValue := &in.CurrentAverageValue
	out.Current = autoscaling.MetricValueStatus{
		AverageValue:       averageValue,
		AverageUtilization: utilization,
	}
	return nil
}

// Convert_autoscaling_ResourceMetricStatus_To_v1_ResourceMetricStatus 把 internal 的
// Resource 指标状态映射为 v1 结构，nil 的 AverageValue 不写出。
func Convert_autoscaling_ResourceMetricStatus_To_v1_ResourceMetricStatus(in *autoscaling.ResourceMetricStatus, out *autoscalingv1.ResourceMetricStatus, s conversion.Scope) error {
	out.Name = v1.ResourceName(in.Name)
	out.CurrentAverageUtilization = in.Current.AverageUtilization
	if in.Current.AverageValue != nil {
		out.CurrentAverageValue = *in.Current.AverageValue
	}
	return nil
}

// Convert_autoscaling_HorizontalPodAutoscaler_To_v1_HorizontalPodAutoscaler 把 internal HPA
// 转成 v1 HPA。v1 只能表达 CPU 利用率指标和部分状态，因此这里先清掉旧的往返注解，
// 再把 v1 无法表达的非 CPU 指标、完整指标状态、Behavior 和 Conditions
// 序列化到注解里；写注解前按需拷贝 map，避免修改调用方对象。
func Convert_autoscaling_HorizontalPodAutoscaler_To_v1_HorizontalPodAutoscaler(in *autoscaling.HorizontalPodAutoscaler, out *autoscalingv1.HorizontalPodAutoscaler, s conversion.Scope) error {
	if err := autoConvert_autoscaling_HorizontalPodAutoscaler_To_v1_HorizontalPodAutoscaler(in, out, s); err != nil {
		return err
	}

	// 先清掉已有的往返注解，确保最终只剩本次转换产生的注解。
	annotations, copiedAnnotations := autoscaling.DropRoundTripHorizontalPodAutoscalerAnnotations(out.Annotations)
	out.Annotations = annotations

	// CPU 利用率指标由 v1 的 TargetCPUUtilizationPercentage 字段承载，
	// 其余指标才能进入注解，避免同一份信息被重复保存。
	otherMetrics := make([]autoscalingv1.MetricSpec, 0, len(in.Spec.Metrics))
	for _, metric := range in.Spec.Metrics {
		if metric.Type == autoscaling.ResourceMetricSourceType && metric.Resource != nil && metric.Resource.Name == core.ResourceCPU && metric.Resource.Target.AverageUtilization != nil {
			continue
		}

		convMetric := autoscalingv1.MetricSpec{}
		if err := Convert_autoscaling_MetricSpec_To_v1_MetricSpec(&metric, &convMetric, s); err != nil {
			return err
		}
		otherMetrics = append(otherMetrics, convMetric)
	}

	// 即使指标对应 v1 的 CPU 利用率状态，也要把完整状态存进注解，
	// 因为 v1 字段只保留了平均利用率，会丢失原始值等信息。
	currentMetrics := make([]autoscalingv1.MetricStatus, len(in.Status.CurrentMetrics))
	for i, currentMetric := range in.Status.CurrentMetrics {
		if err := Convert_autoscaling_MetricStatus_To_v1_MetricStatus(&currentMetric, &currentMetrics[i], s); err != nil {
			return err
		}
	}

	// v1 没有 Conditions 字段，同样借助注解保存。
	currentConditions := make([]autoscalingv1.HorizontalPodAutoscalerCondition, len(in.Status.Conditions))
	for i, currentCondition := range in.Status.Conditions {
		if err := Convert_autoscaling_HorizontalPodAutoscalerCondition_To_v1_HorizontalPodAutoscalerCondition(&currentCondition, &currentConditions[i], s); err != nil {
			return err
		}
	}

	if len(otherMetrics) > 0 {
		otherMetricsEnc, err := json.Marshal(otherMetrics)
		if err != nil {
			return err
		}
		// 首次写注解前先拷贝，后续写入复用同一份拷贝。
		if !copiedAnnotations {
			copiedAnnotations = true
			out.Annotations = autoscaling.DeepCopyStringMap(out.Annotations)
		}
		out.Annotations[autoscaling.MetricSpecsAnnotation] = string(otherMetricsEnc)
	}

	if len(in.Status.CurrentMetrics) > 0 {
		currentMetricsEnc, err := json.Marshal(currentMetrics)
		if err != nil {
			return err
		}
		// 同一个 map 拷贝只做一次，避免重复分配。
		if !copiedAnnotations {
			copiedAnnotations = true
			out.Annotations = autoscaling.DeepCopyStringMap(out.Annotations)
		}
		out.Annotations[autoscaling.MetricStatusesAnnotation] = string(currentMetricsEnc)
	}

	if in.Spec.Behavior != nil {
		// TODO: 这里序列化的是 internal 类型。需要在不破坏向后兼容的前提下修正。
		behaviorEnc, err := json.Marshal(in.Spec.Behavior)
		if err != nil {
			return err
		}
		// 首次写注解前先拷贝。
		if !copiedAnnotations {
			copiedAnnotations = true
			out.Annotations = autoscaling.DeepCopyStringMap(out.Annotations)
		}
		out.Annotations[autoscaling.BehaviorSpecsAnnotation] = string(behaviorEnc)
	}

	if len(in.Status.Conditions) > 0 {
		currentConditionsEnc, err := json.Marshal(currentConditions)
		if err != nil {
			return err
		}
		// 首次写注解前先拷贝。此处之后不再写注解，赋值仅用于保持各分支写法一致。
		if !copiedAnnotations {
			//nolint:ineffassign
			copiedAnnotations = true
			out.Annotations = autoscaling.DeepCopyStringMap(out.Annotations)
		}
		out.Annotations[autoscaling.HorizontalPodAutoscalerConditionsAnnotation] = string(currentConditionsEnc)
	}

	return nil
}

// Convert_v1_HorizontalPodAutoscaler_To_autoscaling_HorizontalPodAutoscaler 把 v1 HPA
// 转成 internal HPA。v1 丢失的非 CPU 指标、指标状态、Behavior 和 Conditions
// 从注解中还原；还原后清掉这些注解。注解解析失败时保持静默，
// 让常规字段转换的结果继续可用。
func Convert_v1_HorizontalPodAutoscaler_To_autoscaling_HorizontalPodAutoscaler(in *autoscalingv1.HorizontalPodAutoscaler, out *autoscaling.HorizontalPodAutoscaler, s conversion.Scope) error {
	if err := autoConvert_v1_HorizontalPodAutoscaler_To_autoscaling_HorizontalPodAutoscaler(in, out, s); err != nil {
		return err
	}

	if otherMetricsEnc, hasOtherMetrics := out.Annotations[autoscaling.MetricSpecsAnnotation]; hasOtherMetrics {
		var otherMetrics []autoscalingv1.MetricSpec
		if err := json.Unmarshal([]byte(otherMetricsEnc), &otherMetrics); err == nil {
			// 常规 Spec 转换可能已经把 CPU 指标填成一个元素，这里要把两者合并，
			// 而不是直接覆盖，以免丢失注解中的非 CPU 指标。
			outMetrics := make([]autoscaling.MetricSpec, len(otherMetrics)+len(out.Spec.Metrics))
			for i, metric := range otherMetrics {
				if err := Convert_v1_MetricSpec_To_autoscaling_MetricSpec(&metric, &outMetrics[i], s); err != nil {
					return err
				}
			}
			if out.Spec.Metrics != nil {
				outMetrics[len(otherMetrics)] = out.Spec.Metrics[0]
			}
			out.Spec.Metrics = outMetrics
		}
	}

	if behaviorEnc, hasConstraints := out.Annotations[autoscaling.BehaviorSpecsAnnotation]; hasConstraints {
		// TODO: 这里反序列化的是 internal 类型。需要在不破坏向后兼容的前提下修正。
		var behavior autoscaling.HorizontalPodAutoscalerBehavior
		if err := json.Unmarshal([]byte(behaviorEnc), &behavior); err == nil && behavior != (autoscaling.HorizontalPodAutoscalerBehavior{}) {
			out.Spec.Behavior = &behavior
		}
	}

	if currentMetricsEnc, hasCurrentMetrics := out.Annotations[autoscaling.MetricStatusesAnnotation]; hasCurrentMetrics {
		// 注解中的状态信息更完整，直接覆盖常规转换得到的状态。
		var currentMetrics []autoscalingv1.MetricStatus
		if err := json.Unmarshal([]byte(currentMetricsEnc), &currentMetrics); err == nil {
			out.Status.CurrentMetrics = make([]autoscaling.MetricStatus, len(currentMetrics))
			for i, currentMetric := range currentMetrics {
				if err := Convert_v1_MetricStatus_To_autoscaling_MetricStatus(&currentMetric, &out.Status.CurrentMetrics[i], s); err != nil {
					return err
				}
			}
		}
	}

	// autoscaling/v1 过去在 controller 里隐式应用 CPU 默认值，v2beta1 起改为显式应用。
	// 这里能读取注解中完整的指标集合，因此在这里显式补上默认指标，
	// 保证后续版本拿到一致的默认值。
	if len(out.Spec.Metrics) == 0 {
		// 既没有其他指标，也没有显式设置 CPU 利用率，使用默认 CPU 利用率。
		out.Spec.Metrics = []autoscaling.MetricSpec{
			{
				Type: autoscaling.ResourceMetricSourceType,
				Resource: &autoscaling.ResourceMetricSource{
					Name: core.ResourceCPU,
					Target: autoscaling.MetricTarget{
						Type: autoscaling.UtilizationMetricType,
					},
				},
			},
		}
		out.Spec.Metrics[0].Resource.Target.AverageUtilization = new(int32)
		*out.Spec.Metrics[0].Resource.Target.AverageUtilization = autoscaling.DefaultCPUUtilization
	}

	if currentConditionsEnc, hasCurrentConditions := out.Annotations[autoscaling.HorizontalPodAutoscalerConditionsAnnotation]; hasCurrentConditions {
		var currentConditions []autoscalingv1.HorizontalPodAutoscalerCondition
		if err := json.Unmarshal([]byte(currentConditionsEnc), &currentConditions); err == nil {
			out.Status.Conditions = make([]autoscaling.HorizontalPodAutoscalerCondition, len(currentConditions))
			for i, currentCondition := range currentConditions {
				if err := Convert_v1_HorizontalPodAutoscalerCondition_To_autoscaling_HorizontalPodAutoscalerCondition(&currentCondition, &out.Status.Conditions[i], s); err != nil {
					return err
				}
			}
		}
	}

	// 转换到 internal 后，往返用的注解已完成使命，清掉避免污染对象。
	out.Annotations, _ = autoscaling.DropRoundTripHorizontalPodAutoscalerAnnotations(out.Annotations)

	return nil
}

// Convert_autoscaling_HorizontalPodAutoscalerSpec_To_v1_HorizontalPodAutoscalerSpec
// 把 internal HPA Spec 转成 v1 Spec，并从 Metrics 中提取 CPU 利用率写入
// v1 专用的 TargetCPUUtilizationPercentage 字段。
func Convert_autoscaling_HorizontalPodAutoscalerSpec_To_v1_HorizontalPodAutoscalerSpec(in *autoscaling.HorizontalPodAutoscalerSpec, out *autoscalingv1.HorizontalPodAutoscalerSpec, s conversion.Scope) error {
	if err := Convert_autoscaling_CrossVersionObjectReference_To_v1_CrossVersionObjectReference(&in.ScaleTargetRef, &out.ScaleTargetRef, s); err != nil {
		return err
	}

	out.MinReplicas = in.MinReplicas
	out.MaxReplicas = in.MaxReplicas

	for _, metric := range in.Metrics {
		if metric.Type == autoscaling.ResourceMetricSourceType && metric.Resource != nil && metric.Resource.Name == core.ResourceCPU && metric.Resource.Target.AverageUtilization != nil {
			out.TargetCPUUtilizationPercentage = new(int32)
			*out.TargetCPUUtilizationPercentage = *metric.Resource.Target.AverageUtilization
			break
		}
	}

	return nil
}

// Convert_v1_HorizontalPodAutoscalerSpec_To_autoscaling_HorizontalPodAutoscalerSpec
// 把 v1 Spec 转成 internal Spec，把 TargetCPUUtilizationPercentage 展开成
// 一条 CPU 资源指标；其他指标由注解恢复。
func Convert_v1_HorizontalPodAutoscalerSpec_To_autoscaling_HorizontalPodAutoscalerSpec(in *autoscalingv1.HorizontalPodAutoscalerSpec, out *autoscaling.HorizontalPodAutoscalerSpec, s conversion.Scope) error {
	if err := Convert_v1_CrossVersionObjectReference_To_autoscaling_CrossVersionObjectReference(&in.ScaleTargetRef, &out.ScaleTargetRef, s); err != nil {
		return err
	}

	out.MinReplicas = in.MinReplicas
	out.MaxReplicas = in.MaxReplicas

	if in.TargetCPUUtilizationPercentage != nil {
		out.Metrics = []autoscaling.MetricSpec{
			{
				Type: autoscaling.ResourceMetricSourceType,
				Resource: &autoscaling.ResourceMetricSource{
					Name: core.ResourceCPU,
					Target: autoscaling.MetricTarget{
						Type: autoscaling.UtilizationMetricType,
					},
				},
			},
		}
		out.Metrics[0].Resource.Target.AverageUtilization = new(int32)
		*out.Metrics[0].Resource.Target.AverageUtilization = *in.TargetCPUUtilizationPercentage
	}

	return nil
}

// Convert_autoscaling_HorizontalPodAutoscalerStatus_To_v1_HorizontalPodAutoscalerStatus
// 把 internal HPA Status 转成 v1 Status，提取 CPU 资源指标的平均利用率写入
// v1 专用字段；完整状态由外层转换保存到注解中。
func Convert_autoscaling_HorizontalPodAutoscalerStatus_To_v1_HorizontalPodAutoscalerStatus(in *autoscaling.HorizontalPodAutoscalerStatus, out *autoscalingv1.HorizontalPodAutoscalerStatus, s conversion.Scope) error {
	out.ObservedGeneration = in.ObservedGeneration
	out.LastScaleTime = in.LastScaleTime

	out.CurrentReplicas = in.CurrentReplicas
	out.DesiredReplicas = in.DesiredReplicas

	for _, metric := range in.CurrentMetrics {
		if metric.Type == autoscaling.ResourceMetricSourceType && metric.Resource != nil && metric.Resource.Name == core.ResourceCPU {
			if metric.Resource.Current.AverageUtilization != nil {

				out.CurrentCPUUtilizationPercentage = new(int32)
				*out.CurrentCPUUtilizationPercentage = *metric.Resource.Current.AverageUtilization
			}
		}
	}
	return nil
}

// Convert_v1_HorizontalPodAutoscalerStatus_To_autoscaling_HorizontalPodAutoscalerStatus
// 把 v1 Status 转成 internal Status，把 CPU 利用率展开成一条 Resource 指标状态；
// 完整的指标状态由外层转换从注解恢复。
func Convert_v1_HorizontalPodAutoscalerStatus_To_autoscaling_HorizontalPodAutoscalerStatus(in *autoscalingv1.HorizontalPodAutoscalerStatus, out *autoscaling.HorizontalPodAutoscalerStatus, s conversion.Scope) error {
	out.ObservedGeneration = in.ObservedGeneration
	out.LastScaleTime = in.LastScaleTime

	out.CurrentReplicas = in.CurrentReplicas
	out.DesiredReplicas = in.DesiredReplicas

	if in.CurrentCPUUtilizationPercentage != nil {
		out.CurrentMetrics = []autoscaling.MetricStatus{
			{
				Type: autoscaling.ResourceMetricSourceType,
				Resource: &autoscaling.ResourceMetricStatus{
					Name: core.ResourceCPU,
				},
			},
		}
		out.CurrentMetrics[0].Resource.Current.AverageUtilization = new(int32)
		*out.CurrentMetrics[0].Resource.Current.AverageUtilization = *in.CurrentCPUUtilizationPercentage
	}
	return nil
}

// Convert_v1_ContainerResourceMetricSource_To_autoscaling_ContainerResourceMetricSource
// 把 v1 的容器资源指标映射为 internal 结构，根据利用率是否为 nil 推断目标类型。
func Convert_v1_ContainerResourceMetricSource_To_autoscaling_ContainerResourceMetricSource(in *autoscalingv1.ContainerResourceMetricSource, out *autoscaling.ContainerResourceMetricSource, s conversion.Scope) error {
	out.Name = core.ResourceName(in.Name)
	out.Container = in.Container
	utilization := in.TargetAverageUtilization
	averageValue := in.TargetAverageValue
	var metricType autoscaling.MetricTargetType
	if utilization == nil {
		metricType = autoscaling.AverageValueMetricType
	} else {
		metricType = autoscaling.UtilizationMetricType
	}
	out.Target = autoscaling.MetricTarget{
		Type:               metricType,
		AverageValue:       averageValue,
		AverageUtilization: utilization,
	}
	return nil
}

// Convert_autoscaling_ContainerResourceMetricSource_To_v1_ContainerResourceMetricSource
// 把 internal 的容器资源指标映射为 v1 结构。
func Convert_autoscaling_ContainerResourceMetricSource_To_v1_ContainerResourceMetricSource(in *autoscaling.ContainerResourceMetricSource, out *autoscalingv1.ContainerResourceMetricSource, s conversion.Scope) error {
	out.Name = v1.ResourceName(in.Name)
	out.Container = in.Container
	out.TargetAverageUtilization = in.Target.AverageUtilization
	out.TargetAverageValue = in.Target.AverageValue
	return nil
}
