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

package controllerrevision

import (
	"context"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/apiserver/pkg/registry/rest"
	"k8s.io/apiserver/pkg/storage/names"
	"k8s.io/kubernetes/pkg/api/legacyscheme"
	"k8s.io/kubernetes/pkg/apis/apps"
	"k8s.io/kubernetes/pkg/apis/apps/validation"
)

// strategy 定义 ControllerRevision 历史快照对象的 REST 策略；它保存控制器用于回滚/版本比较的原始数据。
type strategy struct {
	rest.DeclarativeValidation
	names.NameGenerator
}

// Strategy 是通过 REST API 创建和更新 ControllerRevision 时使用的默认策略。
var Strategy = strategy{rest.DeclarativeValidation{Scheme: legacyscheme.Scheme}, names.SimpleNameGenerator}

// 编译期检查：确保 Strategy 实现 REST 创建策略接口。
var _ rest.RESTCreateStrategy = Strategy

// 编译期检查：确保 Strategy 实现 REST 更新策略接口。
var _ rest.RESTUpdateStrategy = Strategy

// NamespaceScoped 表示 ControllerRevision 是命名空间级资源。
func (strategy) NamespaceScoped() bool {
	return true
}

// Canonicalize 在校验后规范化对象；ControllerRevision 保留控制器提交的快照内容，因此无需改写。
func (strategy) Canonicalize(obj runtime.Object) {
}

// AllowCreateOnUpdate 返回 false：ControllerRevision 必须通过创建请求产生，不能由更新请求隐式创建。
func (strategy) AllowCreateOnUpdate(ctx context.Context) bool {
	return false
}

// ControllerRevision 的快照数据由所属控制器生成，创建准备阶段不改写其 Data、Revision 或 Owner 信息。
func (strategy) PrepareForCreate(ctx context.Context, obj runtime.Object) {
	_ = obj.(*apps.ControllerRevision)
}

// 创建校验检查 ControllerRevision 的名称、数据和 Revision 字段是否合法。
func (strategy) Validate(ctx context.Context, obj runtime.Object) field.ErrorList {
	revision := obj.(*apps.ControllerRevision)

	return validation.ValidateControllerRevisionCreate(revision)
}

// WarningsOnCreate 返回创建 ControllerRevision 时的非阻断性警告；当前不产生额外警告。
func (strategy) WarningsOnCreate(ctx context.Context, obj runtime.Object) []string { return nil }

// 更新准备阶段仅确认对象类型，不在 API Server 自动合并快照内容；可更新性由后续 ValidateUpdate 决定。
func (strategy) PrepareForUpdate(ctx context.Context, newObj, oldObj runtime.Object) {
	_ = oldObj.(*apps.ControllerRevision)
	_ = newObj.(*apps.ControllerRevision)
}

// AllowUnconditionalUpdate 允许不携带 resourceVersion 的无条件更新。
func (strategy) AllowUnconditionalUpdate(ctx context.Context) bool {
	return true
}

// 使用新旧快照对比校验更新，避免破坏控制器对历史版本内容和序号的引用。
func (strategy) ValidateUpdate(ctx context.Context, newObj, oldObj runtime.Object) field.ErrorList {
	oldRevision, newRevision := oldObj.(*apps.ControllerRevision), newObj.(*apps.ControllerRevision)
	return validation.ValidateControllerRevisionUpdate(newRevision, oldRevision)
}

// WarningsOnUpdate 返回更新 ControllerRevision 时的非阻断性警告；当前不产生额外警告。
func (strategy) WarningsOnUpdate(ctx context.Context, obj, old runtime.Object) []string { return nil }
