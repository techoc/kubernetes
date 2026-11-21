/*
Copyright 2015 The Kubernetes Authors.

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
	"reflect"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/conversion"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/kubernetes/pkg/apis/apps"
	"k8s.io/kubernetes/pkg/apis/core"
)

func addConversionFuncs(scheme *runtime.Scheme) error {
	// 注册字段标签（field label）转换函数，让基于字段选择器的查询在外部版本与内部版本之间通用。
	err := scheme.AddFieldLabelConversionFunc(SchemeGroupVersion.WithKind("Pod"),
		func(label, value string) (string, string, error) {
			switch label {
			case "metadata.name",
				"metadata.namespace",
				"spec.nodeName",
				"spec.restartPolicy",
				"spec.schedulerName",
				"spec.serviceAccountName",
				"spec.hostNetwork",
				"status.phase",
				"status.podIP",
				"status.podIPs",
				"status.nominatedNodeName":
				return label, value, nil
			// 兼容仍发送 spec.host 的旧 v1 客户端，将其映射到 spec.nodeName。
			case "spec.host":
				return "spec.nodeName", value, nil
			default:
				return "", "", fmt.Errorf("field label not supported: %s", label)
			}
		},
	)
	if err != nil {
		return err
	}
	err = scheme.AddFieldLabelConversionFunc(SchemeGroupVersion.WithKind("Node"),
		func(label, value string) (string, string, error) {
			switch label {
			case "metadata.name":
				return label, value, nil
			case "spec.unschedulable":
				return label, value, nil
			default:
				return "", "", fmt.Errorf("field label not supported: %s", label)
			}
		},
	)
	if err != nil {
		return err
	}
	err = scheme.AddFieldLabelConversionFunc(SchemeGroupVersion.WithKind("ReplicationController"),
		func(label, value string) (string, string, error) {
			switch label {
			case "metadata.name",
				"metadata.namespace",
				"status.replicas":
				return label, value, nil
			default:
				return "", "", fmt.Errorf("field label not supported: %s", label)
			}
		})
	if err != nil {
		return err
	}
	if err := AddFieldLabelConversionsForEvent(scheme); err != nil {
		return err
	}
	if err := AddFieldLabelConversionsForNamespace(scheme); err != nil {
		return err
	}
	if err := AddFieldLabelConversionsForSecret(scheme); err != nil {
		return err
	}
	if err := AddFieldLabelConversionsForService(scheme); err != nil {
		return err
	}
	return nil
}

// Convert_v1_ReplicationController_To_apps_ReplicaSet 把 v1 ReplicationController 转换为 apps.ReplicaSet。
func Convert_v1_ReplicationController_To_apps_ReplicaSet(in *v1.ReplicationController, out *apps.ReplicaSet, s conversion.Scope) error {
	out.ObjectMeta = in.ObjectMeta
	if err := Convert_v1_ReplicationControllerSpec_To_apps_ReplicaSetSpec(&in.Spec, &out.Spec, s); err != nil {
		return err
	}
	if err := Convert_v1_ReplicationControllerStatus_To_apps_ReplicaSetStatus(&in.Status, &out.Status, s); err != nil {
		return err
	}
	return nil
}

// Convert_v1_ReplicationControllerSpec_To_apps_ReplicaSetSpec 转换 RC spec 到 ReplicaSet spec，
// 其中 map 类型的 selector 会转换为 metav1.LabelSelector。
func Convert_v1_ReplicationControllerSpec_To_apps_ReplicaSetSpec(in *v1.ReplicationControllerSpec, out *apps.ReplicaSetSpec, s conversion.Scope) error {
	out.Replicas = *in.Replicas
	out.MinReadySeconds = in.MinReadySeconds
	if in.Selector != nil {
		out.Selector = new(metav1.LabelSelector)
		metav1.Convert_Map_string_To_string_To_v1_LabelSelector(&in.Selector, out.Selector, s)
	}
	if in.Template != nil {
		if err := Convert_v1_PodTemplateSpec_To_core_PodTemplateSpec(in.Template, &out.Template, s); err != nil {
			return err
		}
	}
	return nil
}

// Convert_v1_ReplicationControllerStatus_To_apps_ReplicaSetStatus 转换 RC status 到 ReplicaSet status，
// 并逐条转换 condition 列表。
func Convert_v1_ReplicationControllerStatus_To_apps_ReplicaSetStatus(in *v1.ReplicationControllerStatus, out *apps.ReplicaSetStatus, s conversion.Scope) error {
	out.Replicas = in.Replicas
	out.FullyLabeledReplicas = in.FullyLabeledReplicas
	out.ReadyReplicas = in.ReadyReplicas
	out.AvailableReplicas = in.AvailableReplicas
	out.ObservedGeneration = in.ObservedGeneration
	for _, cond := range in.Conditions {
		out.Conditions = append(out.Conditions, apps.ReplicaSetCondition{
			Type:               apps.ReplicaSetConditionType(cond.Type),
			Status:             core.ConditionStatus(cond.Status),
			LastTransitionTime: cond.LastTransitionTime,
			Reason:             cond.Reason,
			Message:            cond.Message,
		})
	}
	return nil
}

// Convert_apps_ReplicaSet_To_v1_ReplicationController 把 apps.ReplicaSet 转换为 v1 ReplicationController。
// 无法无损转换的字段（如 selector）会以 non-convertible 注解的形式保留在输出对象上。
func Convert_apps_ReplicaSet_To_v1_ReplicationController(in *apps.ReplicaSet, out *v1.ReplicationController, s conversion.Scope) error {
	out.ObjectMeta = in.ObjectMeta
	if err := Convert_apps_ReplicaSetSpec_To_v1_ReplicationControllerSpec(&in.Spec, &out.Spec, s); err != nil {
		fieldErr, ok := err.(*field.Error)
		if !ok {
			return err
		}
		if out.Annotations == nil {
			out.Annotations = make(map[string]string)
		}
		out.Annotations[v1.NonConvertibleAnnotationPrefix+"/"+fieldErr.Field] = reflect.ValueOf(fieldErr.BadValue).String()
	}
	if err := Convert_apps_ReplicaSetStatus_To_v1_ReplicationControllerStatus(&in.Status, &out.Status, s); err != nil {
		return err
	}
	return nil
}

// Convert_apps_ReplicaSetSpec_To_v1_ReplicationControllerSpec 转换 ReplicaSet spec 到 RC spec，
// LabelSelector 无法无损转换时返回相应错误，由上层记录为不可转换注解。
func Convert_apps_ReplicaSetSpec_To_v1_ReplicationControllerSpec(in *apps.ReplicaSetSpec, out *v1.ReplicationControllerSpec, s conversion.Scope) error {
	out.Replicas = new(int32)
	*out.Replicas = in.Replicas
	out.MinReadySeconds = in.MinReadySeconds
	var invalidErr error
	if in.Selector != nil {
		invalidErr = metav1.Convert_v1_LabelSelector_To_Map_string_To_string(in.Selector, &out.Selector, s)
	}
	out.Template = new(v1.PodTemplateSpec)
	if err := Convert_core_PodTemplateSpec_To_v1_PodTemplateSpec(&in.Template, out.Template, s); err != nil {
		return err
	}
	return invalidErr
}

// Convert_apps_ReplicaSetStatus_To_v1_ReplicationControllerStatus 转换 ReplicaSet status 到 RC status，
// 并逐条转换 condition 列表。
func Convert_apps_ReplicaSetStatus_To_v1_ReplicationControllerStatus(in *apps.ReplicaSetStatus, out *v1.ReplicationControllerStatus, s conversion.Scope) error {
	out.Replicas = in.Replicas
	out.FullyLabeledReplicas = in.FullyLabeledReplicas
	out.ReadyReplicas = in.ReadyReplicas
	out.AvailableReplicas = in.AvailableReplicas
	out.ObservedGeneration = in.ObservedGeneration
	for _, cond := range in.Conditions {
		out.Conditions = append(out.Conditions, v1.ReplicationControllerCondition{
			Type:               v1.ReplicationControllerConditionType(cond.Type),
			Status:             v1.ConditionStatus(cond.Status),
			LastTransitionTime: cond.LastTransitionTime,
			Reason:             cond.Reason,
			Message:            cond.Message,
		})
	}
	return nil
}

// Convert_core_NodeSpec_To_v1_NodeSpec 转换 NodeSpec 到 v1，
// 并把内部 PodCIDRs 的第一个值同步到 v1 中已废弃的单值字段 PodCIDR。
func Convert_core_NodeSpec_To_v1_NodeSpec(in *core.NodeSpec, out *v1.NodeSpec, s conversion.Scope) error {
	if err := autoConvert_core_NodeSpec_To_v1_NodeSpec(in, out, s); err != nil {
		return err
	}
	// 此时 autoConvert 已把 core.PodCIDRs 复制到 v1.PodCIDRs；
	// v1 中单值字段 PodCIDR（内部版本不存在）需用 PodCIDRs[0] 填充。
	if len(in.PodCIDRs) > 0 {
		out.PodCIDR = in.PodCIDRs[0]
	}
	return nil
}

// Convert_v1_NodeSpec_To_core_NodeSpec 转换 v1 NodeSpec 到内部版本，
// 处理已废弃的单值 PodCIDR 与 PodCIDRs 之间的兼容性：两者不一致时以 PodCIDR 为准。
func Convert_v1_NodeSpec_To_core_NodeSpec(in *v1.NodeSpec, out *core.NodeSpec, s conversion.Scope) error {
	if err := autoConvert_v1_NodeSpec_To_core_NodeSpec(in, out, s); err != nil {
		return err
	}
	// 若两个字段同时存在且不一致，为兼容旧客户端以单值 PodCIDR 为准。
	if (len(in.PodCIDR) > 0 && len(in.PodCIDRs) > 0) && (in.PodCIDR != in.PodCIDRs[0]) {
		out.PodCIDRs = []string{in.PodCIDR}
	}

	// 此时 autoConvert 已把 v1.PodCIDRs 复制到 core.PodCIDRs；
	// 若 v1.PodCIDRs 为空而 PodCIDR 非空，则用 PodCIDR 填充 core.PodCIDRs[0]。
	if len(in.PodCIDR) > 0 && len(in.PodCIDRs) == 0 {
		out.PodCIDRs = []string{in.PodCIDR}
	}
	return nil
}

// Convert_v1_Secret_To_core_Secret 转换 v1 Secret 到内部版本，
// 并把 StringData 中的字符串内容合并到 Data（同名 key 以 StringData 为准）。
func Convert_v1_Secret_To_core_Secret(in *v1.Secret, out *core.Secret, s conversion.Scope) error {
	if err := autoConvert_v1_Secret_To_core_Secret(in, out, s); err != nil {
		return err
	}

	// StringData 会覆盖 Data 中的同名条目。
	if len(in.StringData) > 0 {
		if out.Data == nil {
			out.Data = map[string][]byte{}
		}
		for k, v := range in.StringData {
			out.Data[k] = []byte(v)
		}
	}

	return nil
}

// Convert_v1_ResourceList_To_core_ResourceList 转换资源列表；由于两个版本的 Quantity 表示一致，这里只需复制。
// +k8s:conversion-fn=copy-only
func Convert_v1_ResourceList_To_core_ResourceList(in *v1.ResourceList, out *core.ResourceList, s conversion.Scope) error {
	if *in == nil {
		return nil
	}
	if *out == nil {
		*out = make(core.ResourceList, len(*in))
	}
	for key, val := range *in {
		// 相关处理已移至默认值填充逻辑。
		// TODO(#18538): We round up resource values to milli scale to maintain API compatibility.
		// In the future, we should instead reject values that need rounding.
		// const milliScale = -3
		// val.RoundUp(milliScale)

		(*out)[core.ResourceName(key)] = val
	}
	return nil
}

// AddFieldLabelConversionsForEvent 为 Event 注册受支持的字段标签转换，使字段选择器可用于事件查询。
func AddFieldLabelConversionsForEvent(scheme *runtime.Scheme) error {
	return scheme.AddFieldLabelConversionFunc(SchemeGroupVersion.WithKind("Event"),
		func(label, value string) (string, string, error) {
			switch label {
			case "involvedObject.kind",
				"involvedObject.namespace",
				"involvedObject.name",
				"involvedObject.uid",
				"involvedObject.apiVersion",
				"involvedObject.resourceVersion",
				"involvedObject.fieldPath",
				"reason",
				"reportingComponent",
				"source",
				"type",
				"metadata.namespace",
				"metadata.name":
				return label, value, nil
			default:
				return "", "", fmt.Errorf("field label not supported: %s", label)
			}
		})
}

// AddFieldLabelConversionsForNamespace 为 Namespace 注册受支持的字段标签转换。
func AddFieldLabelConversionsForNamespace(scheme *runtime.Scheme) error {
	return scheme.AddFieldLabelConversionFunc(SchemeGroupVersion.WithKind("Namespace"),
		func(label, value string) (string, string, error) {
			switch label {
			case "status.phase",
				"metadata.name":
				return label, value, nil
			default:
				return "", "", fmt.Errorf("field label not supported: %s", label)
			}
		})
}

// AddFieldLabelConversionsForSecret 为 Secret 注册受支持的字段标签转换。
func AddFieldLabelConversionsForSecret(scheme *runtime.Scheme) error {
	return scheme.AddFieldLabelConversionFunc(SchemeGroupVersion.WithKind("Secret"),
		func(label, value string) (string, string, error) {
			switch label {
			case "type",
				"metadata.namespace",
				"metadata.name":
				return label, value, nil
			default:
				return "", "", fmt.Errorf("field label not supported: %s", label)
			}
		})
}

// AddFieldLabelConversionsForService 为 Service 注册受支持的字段标签转换。
func AddFieldLabelConversionsForService(scheme *runtime.Scheme) error {
	return scheme.AddFieldLabelConversionFunc(SchemeGroupVersion.WithKind("Service"),
		func(label, value string) (string, string, error) {
			switch label {
			case "metadata.namespace",
				"metadata.name",
				"spec.clusterIP",
				"spec.type":
				return label, value, nil
			default:
				return "", "", fmt.Errorf("field label not supported: %s", label)
			}
		})
}

// Convert_core_PodTemplateSpec_To_v1_PodTemplateSpec 定义在自动生成文件之外，
// 以便其他内嵌 PodTemplateSpec 的 API 包中生成的转换代码能够引用到它。
// +k8s:conversion-fn=copy-only
func Convert_core_PodTemplateSpec_To_v1_PodTemplateSpec(in *core.PodTemplateSpec, out *v1.PodTemplateSpec, s conversion.Scope) error {
	return autoConvert_core_PodTemplateSpec_To_v1_PodTemplateSpec(in, out, s)
}

// Convert_v1_PodTemplateSpec_To_core_PodTemplateSpec 定义在自动生成文件之外，
// 以便其他内嵌 PodTemplateSpec 的 API 包中生成的转换代码能够引用到它。
// +k8s:conversion-fn=copy-only
func Convert_v1_PodTemplateSpec_To_core_PodTemplateSpec(in *v1.PodTemplateSpec, out *core.PodTemplateSpec, s conversion.Scope) error {
	return autoConvert_v1_PodTemplateSpec_To_core_PodTemplateSpec(in, out, s)
}

// Convert_core_PersistentVolumeSpec_To_v1_PersistentVolumeSpec 定义在自动生成文件之外，供其他 API 包使用。
// 之所以这样做，是因为它被其他 API 引用，但在代码生成时因构建标签而不可见。
// +k8s:conversion-fn=copy-only
func Convert_core_PersistentVolumeSpec_To_v1_PersistentVolumeSpec(in *core.PersistentVolumeSpec, out *v1.PersistentVolumeSpec, s conversion.Scope) error {
	return autoConvert_core_PersistentVolumeSpec_To_v1_PersistentVolumeSpec(in, out, s)
}

// Convert_v1_PersistentVolumeSpec_To_core_PersistentVolumeSpec 定义在自动生成文件之外，供其他 API 包使用。
// 之所以这样做，是因为它被其他 API 引用，但在代码生成时因构建标签而不可见。
// +k8s:conversion-fn=copy-only
func Convert_v1_PersistentVolumeSpec_To_core_PersistentVolumeSpec(in *v1.PersistentVolumeSpec, out *core.PersistentVolumeSpec, s conversion.Scope) error {
	return autoConvert_v1_PersistentVolumeSpec_To_core_PersistentVolumeSpec(in, out, s)
}

// Convert_Slice_string_To_Pointer_string 把 URL 参数解析得到的字符串切片转换为字符串指针；
// 因为 URL 参数的解码需要手工辅助转换，所以这里取切片的第一个元素。
func Convert_Slice_string_To_Pointer_string(in *[]string, out **string, s conversion.Scope) error {
	if len(*in) == 0 {
		return nil
	}
	temp := (*in)[0]
	*out = &temp
	return nil
}
