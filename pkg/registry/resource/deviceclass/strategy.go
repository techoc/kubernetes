/*
Copyright 2024 The Kubernetes Authors.

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

package deviceclass

import (
	"context"

	apiequality "k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/apiserver/pkg/registry/rest"
	"k8s.io/apiserver/pkg/storage/names"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/kubernetes/pkg/api/legacyscheme"
	"k8s.io/kubernetes/pkg/apis/resource"
	"k8s.io/kubernetes/pkg/apis/resource/validation"
	"k8s.io/kubernetes/pkg/features"
)

// deviceClassStrategy 定义 DeviceClass 的创建、更新和校验策略。
type deviceClassStrategy struct {
	rest.DeclarativeValidation
	names.NameGenerator
}

// Strategy 是创建、更新和删除 DeviceClass 时使用的默认 REST 策略。
var Strategy = deviceClassStrategy{rest.DeclarativeValidation{Scheme: legacyscheme.Scheme}, names.SimpleNameGenerator}

// NamespaceScoped 返回 false，因为 DeviceClass 是集群级资源。
func (deviceClassStrategy) NamespaceScoped() bool {
	return false
}

// PrepareForCreate 将 Generation 初始化为 1，并按 Feature Gate 清理不可用字段。
func (deviceClassStrategy) PrepareForCreate(ctx context.Context, obj runtime.Object) {
	class := obj.(*resource.DeviceClass)
	class.Generation = 1
	dropDisabledFields(class, nil)
}

// Validate 校验 DeviceClass 的设备选择规则、配置引用和扩展资源名称。
func (deviceClassStrategy) Validate(ctx context.Context, obj runtime.Object) field.ErrorList {
	deviceClass := obj.(*resource.DeviceClass)
	return validation.ValidateDeviceClass(deviceClass)
}

// WarningsOnCreate 返回创建 DeviceClass 时的非阻断性警告；当前不产生额外警告。
func (deviceClassStrategy) WarningsOnCreate(ctx context.Context, obj runtime.Object) []string {
	return nil
}

// Canonicalize 在校验后规范化 DeviceClass；当前不执行额外转换。
func (deviceClassStrategy) Canonicalize(obj runtime.Object) {
}

// AllowCreateOnUpdate 返回 false，DeviceClass 必须通过 POST 创建。
func (deviceClassStrategy) AllowCreateOnUpdate(ctx context.Context) bool {
	return false
}

// PrepareForUpdate 按旧对象保留已启用过的特性字段，并在 Spec 变化时递增 Generation。
func (deviceClassStrategy) PrepareForUpdate(ctx context.Context, obj, old runtime.Object) {
	class := obj.(*resource.DeviceClass)
	oldClass := old.(*resource.DeviceClass)

	dropDisabledFields(class, oldClass)

	// Spec 发生变化时递增 Generation；Status 不属于该资源的可写字段。
	if !apiequality.Semantic.DeepEqual(oldClass.Spec, class.Spec) {
		class.Generation = oldClass.Generation + 1
	}
}

// ValidateUpdate 根据新旧对象校验 DeviceClass 更新是否合法。
func (deviceClassStrategy) ValidateUpdate(ctx context.Context, obj, old runtime.Object) field.ErrorList {
	newClass := obj.(*resource.DeviceClass)
	oldClass := old.(*resource.DeviceClass)
	return validation.ValidateDeviceClassUpdate(newClass, oldClass)
}

// WarningsOnUpdate 返回更新 DeviceClass 时的非阻断性警告；当前不产生额外警告。
func (deviceClassStrategy) WarningsOnUpdate(ctx context.Context, obj, old runtime.Object) []string {
	return nil
}

// AllowUnconditionalUpdate 返回 true，允许通用更新流程为缺少 ResourceVersion 的请求填入最新版本。
func (deviceClassStrategy) AllowUnconditionalUpdate(ctx context.Context) bool {
	return true
}

// dropDisabledFields 清理当前 Feature Gate 未启用且旧对象未使用的字段，以兼容功能关闭和集群降级。
func dropDisabledFields(newClass, oldClass *resource.DeviceClass) {
	dropDisabledDRAExtendedResourceFields(newClass, oldClass)
}

func dropDisabledDRAExtendedResourceFields(newClass, oldClass *resource.DeviceClass) {
	if utilfeature.DefaultFeatureGate.Enabled(features.DRAExtendedResource) {
		return
	}
	if draExtendedResourceFeatureInUse(oldClass) {
		return
	}
	newClass.Spec.ExtendedResourceName = nil
}

func draExtendedResourceFeatureInUse(class *resource.DeviceClass) bool {
	if class == nil {
		return false
	}

	if class.Spec.ExtendedResourceName != nil {
		return true
	}

	return false
}
