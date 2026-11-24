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

package resourcepoolstatusrequest

import (
	"context"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/apiserver/pkg/registry/rest"
	"k8s.io/apiserver/pkg/storage/names"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/kubernetes/pkg/api/legacyscheme"
	"k8s.io/kubernetes/pkg/apis/resource"
	"k8s.io/kubernetes/pkg/apis/resource/validation"
	"k8s.io/kubernetes/pkg/features"
	"sigs.k8s.io/structured-merge-diff/v6/fieldpath"
)

// resourcePoolStatusRequestStrategy 定义 ResourcePoolStatusRequest 的创建、更新、校验和 Status 子资源策略。
type resourcePoolStatusRequestStrategy struct {
	rest.DeclarativeValidation
	names.NameGenerator
}

// Strategy 和 StatusStrategy 分别定义 ResourcePoolStatusRequest 主资源及 /status 子资源的默认策略。
var (
	Strategy       = &resourcePoolStatusRequestStrategy{rest.DeclarativeValidation{Scheme: legacyscheme.Scheme}, names.SimpleNameGenerator}
	StatusStrategy = &resourcePoolStatusRequestStatusStrategy{resourcePoolStatusRequestStrategy: Strategy}
)

// NamespaceScoped 返回 false，因为 ResourcePoolStatusRequest 是集群级资源。
func (resourcePoolStatusRequestStrategy) NamespaceScoped() bool {
	return false
}

// GetResetFields 声明主资源写入时需要重置的 Status 字段，防止客户端直接写入资源池状态请求结果。
func (*resourcePoolStatusRequestStrategy) GetResetFields() map[fieldpath.APIVersion]*fieldpath.Set {
	fields := map[fieldpath.APIVersion]*fieldpath.Set{
		"resource.k8s.io/v1alpha3": fieldpath.NewSet(
			fieldpath.MakePathOrDie("status"),
		),
	}

	return fields
}

// PrepareForCreate 清空 Status，并根据 DRAPartitionableDevicesType 特性门控清理默认分区类型字段。
func (*resourcePoolStatusRequestStrategy) PrepareForCreate(ctx context.Context, obj runtime.Object) {
	request := obj.(*resource.ResourcePoolStatusRequest)
	// 创建请求不能设置 Status；该字段由控制器维护。
	request.Status = nil
	dropDisabledDRAPartitionableDevicesTypeFields(request, nil)
}

// dropDisabledDRAPartitionableDevicesTypeFields 在 DRAPartitionableDevicesType 未启用且旧请求未使用该字段时，
// 清空默认分区类型；若旧对象已设置，则保留以兼容降级。
func dropDisabledDRAPartitionableDevicesTypeFields(request, oldRequest *resource.ResourcePoolStatusRequest) {
	if utilfeature.DefaultFeatureGate.Enabled(features.DRAPartitionableDevicesType) ||
		(oldRequest != nil && oldRequest.Spec.DefaultPartitionTypeAttribute != nil) {
		return
	}

	request.Spec.DefaultPartitionTypeAttribute = nil
}

func (*resourcePoolStatusRequestStrategy) Validate(ctx context.Context, obj runtime.Object) field.ErrorList {
	request := obj.(*resource.ResourcePoolStatusRequest)
	return validation.ValidateResourcePoolStatusRequest(request)
}

// DeclarativeValidationConfig 将结构体标签引用的 DRAPartitionableDevicesType 特性门控映射为声明式校验选项。
func (*resourcePoolStatusRequestStrategy) DeclarativeValidationConfig(ctx context.Context, obj, oldObj runtime.Object) rest.DeclarativeValidationConfig {
	return rest.DeclarativeValidationConfig{Options: map[string]bool{
		string(features.DRAPartitionableDevicesType): utilfeature.DefaultFeatureGate.Enabled(features.DRAPartitionableDevicesType),
	}}
}

// WarningsOnCreate 返回创建 ResourcePoolStatusRequest 时的非阻断性警告；当前不产生额外警告。
func (*resourcePoolStatusRequestStrategy) WarningsOnCreate(ctx context.Context, obj runtime.Object) []string {
	return nil
}

// Canonicalize 在校验后规范化 ResourcePoolStatusRequest；当前不执行额外转换。
func (*resourcePoolStatusRequestStrategy) Canonicalize(obj runtime.Object) {
}

// AllowCreateOnUpdate 返回 false，ResourcePoolStatusRequest 必须通过 POST 创建。
func (*resourcePoolStatusRequestStrategy) AllowCreateOnUpdate(ctx context.Context) bool {
	return false
}

// PrepareForUpdate 恢复旧 Status，并在特性门控关闭时保留旧对象已使用的分区类型字段。
func (*resourcePoolStatusRequestStrategy) PrepareForUpdate(ctx context.Context, obj, old runtime.Object) {
	request := obj.(*resource.ResourcePoolStatusRequest)
	oldRequest := old.(*resource.ResourcePoolStatusRequest)
	// 主资源更新接口不允许修改 Status，必须通过 /status 子资源更新。
	request.Status = oldRequest.Status
	dropDisabledDRAPartitionableDevicesTypeFields(request, oldRequest)
}

// ValidateUpdate 根据新旧对象校验 ResourcePoolStatusRequest 更新是否合法。
func (*resourcePoolStatusRequestStrategy) ValidateUpdate(ctx context.Context, obj, old runtime.Object) field.ErrorList {
	return validation.ValidateResourcePoolStatusRequestUpdate(obj.(*resource.ResourcePoolStatusRequest), old.(*resource.ResourcePoolStatusRequest))
}

// WarningsOnUpdate 返回更新 ResourcePoolStatusRequest 时的非阻断性警告；当前不产生额外警告。
func (*resourcePoolStatusRequestStrategy) WarningsOnUpdate(ctx context.Context, obj, old runtime.Object) []string {
	return nil
}

// AllowUnconditionalUpdate 返回 false，更新请求必须携带 ResourceVersion。
func (*resourcePoolStatusRequestStrategy) AllowUnconditionalUpdate(ctx context.Context) bool {
	return false
}

type resourcePoolStatusRequestStatusStrategy struct {
	*resourcePoolStatusRequestStrategy
}

// GetResetFields 声明 Status 子资源写入时需要重置的 Spec 和 metadata，确保该入口只能修改 Status。
func (*resourcePoolStatusRequestStatusStrategy) GetResetFields() map[fieldpath.APIVersion]*fieldpath.Set {
	fields := map[fieldpath.APIVersion]*fieldpath.Set{
		"resource.k8s.io/v1alpha3": fieldpath.NewSet(
			fieldpath.MakePathOrDie("metadata"),
			fieldpath.MakePathOrDie("spec"),
		),
	}

	return fields
}

// PrepareForUpdate 恢复旧 Spec，并重置受保护的元数据字段，只允许更新 Status。
func (*resourcePoolStatusRequestStatusStrategy) PrepareForUpdate(ctx context.Context, obj, old runtime.Object) {
	newRequest := obj.(*resource.ResourcePoolStatusRequest)
	oldRequest := old.(*resource.ResourcePoolStatusRequest)
	newRequest.Spec = oldRequest.Spec
	metav1.ResetObjectMetaForStatus(&newRequest.ObjectMeta, &oldRequest.ObjectMeta)
}

// ValidateUpdate 校验 ResourcePoolStatusRequest Status 更新是否合法。
func (r *resourcePoolStatusRequestStatusStrategy) ValidateUpdate(ctx context.Context, obj, old runtime.Object) field.ErrorList {
	newRequest := obj.(*resource.ResourcePoolStatusRequest)
	oldRequest := old.(*resource.ResourcePoolStatusRequest)
	return validation.ValidateResourcePoolStatusRequestStatusUpdate(newRequest, oldRequest)
}

// WarningsOnUpdate 返回 ResourcePoolStatusRequest Status 更新时的非阻断性警告；当前不产生额外警告。
func (*resourcePoolStatusRequestStatusStrategy) WarningsOnUpdate(ctx context.Context, obj, old runtime.Object) []string {
	return nil
}
