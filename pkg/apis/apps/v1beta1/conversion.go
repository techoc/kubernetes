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

package v1beta1

import (
	"fmt"

	appsv1beta1 "k8s.io/api/apps/v1beta1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/conversion"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	apps "k8s.io/kubernetes/pkg/apis/apps"
	"k8s.io/kubernetes/pkg/apis/autoscaling"
	"k8s.io/kubernetes/pkg/apis/core"
)

// addConversionFuncs 注册 v1beta1 apps 资源的转换函数。
func addConversionFuncs(scheme *runtime.Scheme) error {
	// StatefulSet 只有 ObjectMeta 字段和 status.successful 可被字段选择器使用，
	// 其余字段直接报错，避免静默返回错误的选择结果。
	if err := scheme.AddFieldLabelConversionFunc(SchemeGroupVersion.WithKind("StatefulSet"),
		func(label, value string) (string, string, error) {
			switch label {
			case "metadata.name", "metadata.namespace", "status.successful":
				return label, value, nil
			default:
				return "", "", fmt.Errorf("field label not supported for appsv1beta1.StatefulSet: %s", label)
			}
		}); err != nil {
		return err
	}

	return nil
}

// Convert_autoscaling_ScaleStatus_To_v1beta1_ScaleStatus 把 internal ScaleStatus 转成 v1beta1 版本，
// 并把选择器拆分成 TargetSelector 和仅含 matchLabels 的 Selector。
func Convert_autoscaling_ScaleStatus_To_v1beta1_ScaleStatus(in *autoscaling.ScaleStatus, out *appsv1beta1.ScaleStatus, s conversion.Scope) error {
	out.Replicas = int32(in.Replicas)
	out.TargetSelector = in.Selector

	// 旧版 Selector 只支持 matchLabels，含 matchExpressions 时留空，
	// 完整表达式仍保存在 TargetSelector 中。
	out.Selector = nil
	selector, err := metav1.ParseToLabelSelector(in.Selector)
	if err != nil {
		return fmt.Errorf("failed to parse selector: %v", err)
	}
	if len(selector.MatchExpressions) == 0 {
		out.Selector = selector.MatchLabels
	}

	return nil
}

// Convert_v1beta1_ScaleStatus_To_autoscaling_ScaleStatus 把 v1beta1 ScaleStatus 转成 internal 版本，
// 优先使用 TargetSelector，缺失时再由 Selector 的标签构造字符串选择器。
func Convert_v1beta1_ScaleStatus_To_autoscaling_ScaleStatus(in *appsv1beta1.ScaleStatus, out *autoscaling.ScaleStatus, s conversion.Scope) error {
	out.Replicas = in.Replicas

	// TargetSelector 保留完整语义，优先使用；老字段只有 matchLabels，需要转换格式。
	if in.TargetSelector != "" {
		out.Selector = in.TargetSelector
	} else if in.Selector != nil {
		set := labels.Set{}
		for key, val := range in.Selector {
			set[key] = val
		}
		out.Selector = labels.SelectorFromSet(set).String()
	} else {
		out.Selector = ""
	}
	return nil
}

// Convert_v1beta1_StatefulSetSpec_To_apps_StatefulSetSpec 在自动转换之外做了补充，
// 保持 1.17 之前对嵌套 PVC 对象 apiVersion/kind 的处理行为。
func Convert_v1beta1_StatefulSetSpec_To_apps_StatefulSetSpec(in *appsv1beta1.StatefulSetSpec, out *apps.StatefulSetSpec, s conversion.Scope) error {
	if err := autoConvert_v1beta1_StatefulSetSpec_To_apps_StatefulSetSpec(in, out, s); err != nil {
		return err
	}
	// 清空 APIVersion/Kind，与 1.17 之前的反射转换行为保持一致。
	// see https://issue.k8s.io/87583
	if out.VolumeClaimTemplates != nil {
		// 复制一份再改，避免修改共享的输入切片。
		templatesCopy := make([]core.PersistentVolumeClaim, len(out.VolumeClaimTemplates))
		copy(templatesCopy, out.VolumeClaimTemplates)
		out.VolumeClaimTemplates = templatesCopy
		for i := range out.VolumeClaimTemplates {
			out.VolumeClaimTemplates[i].APIVersion = ""
			out.VolumeClaimTemplates[i].Kind = ""
		}
	}
	return nil
}

// Convert_apps_StatefulSetSpec_To_v1beta1_StatefulSetSpec 在自动转换之外做了补充，
// 保持 1.17 之前对嵌套 PVC 对象 apiVersion/kind 的处理行为。
func Convert_apps_StatefulSetSpec_To_v1beta1_StatefulSetSpec(in *apps.StatefulSetSpec, out *appsv1beta1.StatefulSetSpec, s conversion.Scope) error {
	if err := autoConvert_apps_StatefulSetSpec_To_v1beta1_StatefulSetSpec(in, out, s); err != nil {
		return err
	}
	// 清空 APIVersion/Kind，与 1.17 之前的反射转换行为保持一致。
	// see https://issue.k8s.io/87583
	if out.VolumeClaimTemplates != nil {
		// 复制一份再改，避免修改共享的输入切片。
		templatesCopy := make([]corev1.PersistentVolumeClaim, len(out.VolumeClaimTemplates))
		copy(templatesCopy, out.VolumeClaimTemplates)
		out.VolumeClaimTemplates = templatesCopy
		for i := range out.VolumeClaimTemplates {
			out.VolumeClaimTemplates[i].APIVersion = "v1"
			out.VolumeClaimTemplates[i].Kind = "PersistentVolumeClaim"
		}
	}
	return nil
}
