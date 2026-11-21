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

package autoscaling

// MetricSpecsAnnotation 用于在 autoscaling/v2 与旧版本互转时，
// 以注解形式保存 CPU 利用率以外的 HPA 指标规格（`Metrics` 字段）。
const MetricSpecsAnnotation = "autoscaling.alpha.kubernetes.io/metrics"

// MetricStatusesAnnotation 用于在 autoscaling/v2 与旧版本互转时，
// 以注解形式保存 CPU 利用率以外的 HPA 指标状态（`CurrentMetrics` 字段）。
const MetricStatusesAnnotation = "autoscaling.alpha.kubernetes.io/current-metrics"

// HorizontalPodAutoscalerConditionsAnnotation 用于在 autoscaling/v2 与旧版本互转时，
// 以注解形式保存 HPA 的 `Conditions` 字段。
const HorizontalPodAutoscalerConditionsAnnotation = "autoscaling.alpha.kubernetes.io/conditions"

// DefaultCPUUtilization 是在没有其他指标时使用的 CPU 利用率默认值。
// 放在这里是因为 v2 的默认值逻辑和 v1 转换中的伪默认值逻辑都要用到它，
// 集中定义可避免两处取值不一致。
const DefaultCPUUtilization = 80

// BehaviorSpecsAnnotation 用于在 autoscaling/v2beta2 与旧版本互转时，
// 以注解形式保存 HPA 的伸缩行为约束（`Behavior` 字段）。
const BehaviorSpecsAnnotation = "autoscaling.alpha.kubernetes.io/behavior"

// ToleranceScaleDownAnnotation 用于在 autoscaling/v2 与旧版本互转时，
// 以注解形式保存 HPA 的缩容容差（`ScaleDown.Tolerance` 字段）。
const ToleranceScaleDownAnnotation = "autoscaling.alpha.kubernetes.io/scale-down-tolerance"

// ToleranceScaleUpAnnotation 用于在 autoscaling/v2 与旧版本互转时，
// 以注解形式保存 HPA 的扩容容差（`ScaleUp.Tolerance` 字段）。
const ToleranceScaleUpAnnotation = "autoscaling.alpha.kubernetes.io/scale-up-tolerance"
