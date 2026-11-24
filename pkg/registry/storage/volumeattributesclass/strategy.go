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

// volumeAttributesClassStrategy implements behavior for VolumeAttributesClassStrategy objects
type volumeAttributesClassStrategy struct {
	rest.DeclarativeValidation
	names.NameGenerator
}

// Strategy is the default logic that applies when creating and updating
// VolumeAttributesClass objects via the REST API.
var Strategy = volumeAttributesClassStrategy{rest.DeclarativeValidation{Scheme: legacyscheme.Scheme}, names.SimpleNameGenerator}

// NamespaceScoped 声明该资源是否属于命名空间级别；返回 false 表示对象通过集群级 URL 管理，返回 true 表示 URL 中必须包含 namespace。
func (volumeAttributesClassStrategy) NamespaceScoped() bool {
	return false
}

// ResetBeforeCreate clears the Status field which is not allowed to be set by end users on creation.
func (volumeAttributesClassStrategy) PrepareForCreate(ctx context.Context, obj runtime.Object) {
}

// Validate 执行创建请求的业务合法性校验，将字段路径和错误原因整理为 field.ErrorList；返回非空错误时 API 请求会被拒绝。
func (volumeAttributesClassStrategy) Validate(ctx context.Context, obj runtime.Object) field.ErrorList {
	volumeAttributesClass := obj.(*storage.VolumeAttributesClass)
	return validation.ValidateVolumeAttributesClass(volumeAttributesClass)
}

// WarningsOnCreate returns warnings for the creation of the given object.
func (volumeAttributesClassStrategy) WarningsOnCreate(ctx context.Context, obj runtime.Object) []string {
	return nil
}

// Canonicalize normalizes the object after validation.
func (volumeAttributesClassStrategy) Canonicalize(obj runtime.Object) {
}

// AllowCreateOnUpdate 决定 Update 请求在对象不存在时是否允许退化为创建；false 表示必须先显式 Create。
func (volumeAttributesClassStrategy) AllowCreateOnUpdate(ctx context.Context) bool {
	return false
}

// PrepareForUpdate sets the Status fields which is not allowed to be set by an end user updating a PV
func (volumeAttributesClassStrategy) PrepareForUpdate(ctx context.Context, obj, old runtime.Object) {
}

// ValidateUpdate 对更新后的对象及其旧版本进行联合校验，检查字段合法性、不可变字段和跨版本兼容约束。
func (volumeAttributesClassStrategy) ValidateUpdate(ctx context.Context, obj, old runtime.Object) field.ErrorList {
	return validation.ValidateVolumeAttributesClassUpdate(obj.(*storage.VolumeAttributesClass), old.(*storage.VolumeAttributesClass))
}

// WarningsOnUpdate returns warnings for the given update.
func (volumeAttributesClassStrategy) WarningsOnUpdate(ctx context.Context, obj, old runtime.Object) []string {
	return nil
}

// AllowUnconditionalUpdate 声明是否允许不携带 ResourceVersion 前置条件的更新；true 表示允许无条件更新，false 要求并发版本检查。
func (volumeAttributesClassStrategy) AllowUnconditionalUpdate(ctx context.Context) bool {
	return true
}
