/*
Copyright The Kubernetes Authors.

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

package v1alpha1

import (
	"context"

	resourceapi "k8s.io/api/resource/v1"
	"k8s.io/apimachinery/pkg/api/operation"
	"k8s.io/apimachinery/pkg/api/validate"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

// ValidateCustom_Device_Attributes 是 Device.Attributes 字段的自定义校验函数（由 +k8s:customValidation 标记生成调用）。
// 输入 ctx 为上下文、op 为当前操作（创建/更新等）、fldPath 为错误字段路径，
// obj/oldObj 分别为新旧属性值（map[限定名]设备属性）。
// 它逐值校验 map：用 validate.SemanticDeepEqual 比较新旧值判断是否需要重新校验，
// 并对每个值调用 resourceapi.Validate_DeviceAttribute 做实际的合法性校验，返回聚合的错误列表。
func ValidateCustom_Device_Attributes(
	ctx context.Context, op operation.Operation, fldPath *field.Path,
	obj, oldObj map[resourceapi.QualifiedName]resourceapi.DeviceAttribute,
) field.ErrorList {
	return validate.EachMapVal(ctx, op, fldPath, obj, oldObj, validate.SemanticDeepEqual, resourceapi.Validate_DeviceAttribute)
}

// ValidateCustom_Device_NetworkData 是 Device.NetworkData 字段的自定义校验函数。
// 该函数本身不做额外判断，直接把校验委托给 resource 包的 resourceapi.Validate_NetworkDeviceData，
// 传入上下文、操作类型、字段路径以及新旧 NetworkDeviceData 对象，返回其校验错误。
func ValidateCustom_Device_NetworkData(
	ctx context.Context, op operation.Operation, fldPath *field.Path,
	obj, oldObj *resourceapi.NetworkDeviceData,
) field.ErrorList {
	return resourceapi.Validate_NetworkDeviceData(ctx, op, fldPath, obj, oldObj)
}
