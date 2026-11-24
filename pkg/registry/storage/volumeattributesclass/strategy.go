/*
Copyright 2023 The Kubernetes Authors.

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

// 本文件实现 volumeattributesclass 的 REST Strategy，定义作用域、写入前字段处理、校验、告警以及更新规则。
package volumeattributesclass

import (
	"context"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/apiserver/pkg/registry/rest"
	"k8s.io/apiserver/pkg/storage/names"
	"k8s.io/kubernetes/pkg/api/legacyscheme"
	"k8s.io/kubernetes/pkg/apis/storage"
	"k8s.io/kubernetes/pkg/apis/storage/validation"
)

// volumeAttributesClassStrategy 定义 VolumeAttributesClass 的创建、更新和校验策略。
type volumeAttributesClassStrategy struct {
	rest.DeclarativeValidation
	names.NameGenerator
}

// Strategy 是通过 REST API 创建、更新和删除 VolumeAttributesClass 时使用的默认策略。
var Strategy = volumeAttributesClassStrategy{rest.DeclarativeValidation{Scheme: legacyscheme.Scheme}, names.SimpleNameGenerator}

// NamespaceScoped 返回 false，因为 VolumeAttributesClass 是集群级资源。
func (volumeAttributesClassStrategy) NamespaceScoped() bool {
	return false
}

// PrepareForCreate 是创建前的字段预处理入口；VolumeAttributesClass 没有客户端可写的 Status 字段。
func (volumeAttributesClassStrategy) PrepareForCreate(ctx context.Context, obj runtime.Object) {
}

// Validate 执行创建请求的业务合法性校验，将字段路径和错误原因整理为 field.ErrorList；返回非空错误时 API 请求会被拒绝。
func (volumeAttributesClassStrategy) Validate(ctx context.Context, obj runtime.Object) field.ErrorList {
	volumeAttributesClass := obj.(*storage.VolumeAttributesClass)
	return validation.ValidateVolumeAttributesClass(volumeAttributesClass)
}

// WarningsOnCreate 返回创建 VolumeAttributesClass 时的非阻断性警告；当前不产生额外警告。
func (volumeAttributesClassStrategy) WarningsOnCreate(ctx context.Context, obj runtime.Object) []string {
	return nil
}

// Canonicalize 在校验后规范化 VolumeAttributesClass；当前不执行额外转换。
func (volumeAttributesClassStrategy) Canonicalize(obj runtime.Object) {
}

// AllowCreateOnUpdate 返回 false，VolumeAttributesClass 必须通过 POST 创建。
func (volumeAttributesClassStrategy) AllowCreateOnUpdate(ctx context.Context) bool {
	return false
}

// PrepareForUpdate 是更新前的字段预处理入口；VolumeAttributesClass 的字段约束由校验逻辑执行。
func (volumeAttributesClassStrategy) PrepareForUpdate(ctx context.Context, obj, old runtime.Object) {
}

// ValidateUpdate 对更新后的对象及其旧版本进行联合校验，检查字段合法性、不可变字段和跨版本兼容约束。
func (volumeAttributesClassStrategy) ValidateUpdate(ctx context.Context, obj, old runtime.Object) field.ErrorList {
	return validation.ValidateVolumeAttributesClassUpdate(obj.(*storage.VolumeAttributesClass), old.(*storage.VolumeAttributesClass))
}

// WarningsOnUpdate 返回更新 VolumeAttributesClass 时的非阻断性警告；当前不产生额外警告。
func (volumeAttributesClassStrategy) WarningsOnUpdate(ctx context.Context, obj, old runtime.Object) []string {
	return nil
}

// AllowUnconditionalUpdate 返回 true，允许通用更新流程为缺少 ResourceVersion 的请求填入最新版本。
func (volumeAttributesClassStrategy) AllowUnconditionalUpdate(ctx context.Context) bool {
	return true
}
