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

package v1

import (
	"fmt"
	"strconv"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/conversion"
	"k8s.io/kubernetes/pkg/apis/apps"
	"k8s.io/kubernetes/pkg/apis/core"
)

// Convert_apps_DeploymentSpec_To_v1_DeploymentSpec 在这里手写，
// 因为存在警告导致 DeploymentSpec 的公开转换函数没有自动生成。
func Convert_apps_DeploymentSpec_To_v1_DeploymentSpec(in *apps.DeploymentSpec, out *appsv1.DeploymentSpec, s conversion.Scope) error {
	if err := autoConvert_apps_DeploymentSpec_To_v1_DeploymentSpec(in, out, s); err != nil {
		return err
	}
	return nil
}

// Convert_v1_Deployment_To_apps_Deployment 把 v1 Deployment 转成 internal 版本，
// 并把已废弃的 rollbackTo 注解还原成 internal 字段以完成往返转换。
func Convert_v1_Deployment_To_apps_Deployment(in *appsv1.Deployment, out *apps.Deployment, s conversion.Scope) error {
	if err := autoConvert_v1_Deployment_To_apps_Deployment(in, out, s); err != nil {
		return err
	}

	// 把注解还原成已废弃的 rollbackTo 字段，保证转换往返不丢失信息。
	// TODO: remove this conversion after we delete extensions/v1beta1 and apps/v1beta1 Deployment
	if revision := in.Annotations[appsv1.DeprecatedRollbackTo]; revision != "" {
		// 注解内容必须是数字，否则无法确定回滚目标版本。
		if revision64, err := strconv.ParseInt(revision, 10, 64); err != nil {
			return fmt.Errorf("failed to parse annotation[%s]=%s as int64: %v", appsv1.DeprecatedRollbackTo, revision, err)
		} else {
			out.Spec.RollbackTo = new(apps.RollbackConfig)
			out.Spec.RollbackTo.Revision = revision64
		}
		// 注解已被还原成字段，删除它避免 internal 对象残留外部版本信息。
		out.Annotations = deepCopyStringMap(out.Annotations)
		delete(out.Annotations, appsv1.DeprecatedRollbackTo)
	} else {
		out.Spec.RollbackTo = nil
	}

	return nil
}

// Convert_apps_Deployment_To_v1_Deployment 把 internal Deployment 转成 v1 版本，
// 并把 internal 独有的 rollbackTo 字段保存到注解中以便往返转换。
func Convert_apps_Deployment_To_v1_Deployment(in *apps.Deployment, out *appsv1.Deployment, s conversion.Scope) error {
	if err := autoConvert_apps_Deployment_To_v1_Deployment(in, out, s); err != nil {
		return err
	}

	// 下面要修改注解 map，先深拷贝以免改动输入对象。
	out.Annotations = deepCopyStringMap(out.Annotations)

	// 把已废弃的 rollbackTo 字段写入注解，保证转换往返不丢失信息。
	// TODO: remove this conversion after we delete extensions/v1beta1 and apps/v1beta1 Deployment
	if in.Spec.RollbackTo != nil {
		if out.Annotations == nil {
			out.Annotations = make(map[string]string)
		}
		out.Annotations[appsv1.DeprecatedRollbackTo] = strconv.FormatInt(in.Spec.RollbackTo.Revision, 10)
	} else {
		delete(out.Annotations, appsv1.DeprecatedRollbackTo)
	}

	return nil
}

// Convert_apps_DaemonSet_To_v1_DaemonSet 把 internal DaemonSet 转成 v1 版本，
// 并把 internal 的 TemplateGeneration 写入废弃注解以便往返转换。
func Convert_apps_DaemonSet_To_v1_DaemonSet(in *apps.DaemonSet, out *appsv1.DaemonSet, s conversion.Scope) error {
	if err := autoConvert_apps_DaemonSet_To_v1_DaemonSet(in, out, s); err != nil {
		return err
	}

	// 下面要写入注解，先深拷贝以免改动输入对象。
	out.Annotations = deepCopyStringMap(out.Annotations)
	out.Annotations[appsv1.DeprecatedTemplateGeneration] = strconv.FormatInt(in.Spec.TemplateGeneration, 10)
	return nil
}

// Convert_apps_DaemonSetSpec_To_v1_DaemonSetSpec 在这里手写，
// 因为存在警告导致 DaemonSetSpec 的公开转换函数没有自动生成。
func Convert_apps_DaemonSetSpec_To_v1_DaemonSetSpec(in *apps.DaemonSetSpec, out *appsv1.DaemonSetSpec, s conversion.Scope) error {
	if err := autoConvert_apps_DaemonSetSpec_To_v1_DaemonSetSpec(in, out, s); err != nil {
		return err
	}
	return nil
}

// Convert_v1_DaemonSet_To_apps_DaemonSet 把 v1 DaemonSet 转成 internal 版本，
// 并把废弃注解中的 templateGeneration 还原成 internal 字段。
func Convert_v1_DaemonSet_To_apps_DaemonSet(in *appsv1.DaemonSet, out *apps.DaemonSet, s conversion.Scope) error {
	if err := autoConvert_v1_DaemonSet_To_apps_DaemonSet(in, out, s); err != nil {
		return err
	}
	if value, ok := in.Annotations[appsv1.DeprecatedTemplateGeneration]; ok {
		if value64, err := strconv.ParseInt(value, 10, 64); err != nil {
			return err
		} else {
			out.Spec.TemplateGeneration = value64
			out.Annotations = deepCopyStringMap(out.Annotations)
			delete(out.Annotations, appsv1.DeprecatedTemplateGeneration)
		}
	}
	return nil
}

// deepCopyStringMap 复制字符串 map，调用方可以安全地修改副本而不影响原对象。
func deepCopyStringMap(m map[string]string) map[string]string {
	ret := make(map[string]string, len(m))
	for k, v := range m {
		ret[k] = v
	}
	return ret
}

// Convert_v1_StatefulSetSpec_To_apps_StatefulSetSpec 在自动转换之外做了补充，
// 保持 1.17 之前对嵌套 PVC 对象 apiVersion/kind 的处理行为。
func Convert_v1_StatefulSetSpec_To_apps_StatefulSetSpec(in *appsv1.StatefulSetSpec, out *apps.StatefulSetSpec, s conversion.Scope) error {
	if err := autoConvert_v1_StatefulSetSpec_To_apps_StatefulSetSpec(in, out, s); err != nil {
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

// Convert_apps_StatefulSetSpec_To_v1_StatefulSetSpec 在自动转换之外做了补充，
// 保持 1.17 之前对嵌套 PVC 对象 apiVersion/kind 的处理行为。
func Convert_apps_StatefulSetSpec_To_v1_StatefulSetSpec(in *apps.StatefulSetSpec, out *appsv1.StatefulSetSpec, s conversion.Scope) error {
	if err := autoConvert_apps_StatefulSetSpec_To_v1_StatefulSetSpec(in, out, s); err != nil {
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
