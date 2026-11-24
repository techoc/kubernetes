/*
Copyright 2014 The Kubernetes Authors.

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

package limitrange

import (
	"context"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/apiserver/pkg/registry/rest"
	"k8s.io/apiserver/pkg/storage/names"
	"k8s.io/kubernetes/pkg/api/legacyscheme"
	api "k8s.io/kubernetes/pkg/apis/core"
	"k8s.io/kubernetes/pkg/apis/core/validation"
)

// limitrangeStrategy 定义 LimitRange 的 REST 写入策略；规则内容由 ValidateLimitRange 校验，资源默认值由准入阶段应用。
type limitrangeStrategy struct {
	rest.DeclarativeValidation
	names.NameGenerator
}

// Strategy is the default logic that applies when creating and updating
// LimitRange objects via the REST API.
var Strategy = limitrangeStrategy{rest.DeclarativeValidation{Scheme: legacyscheme.Scheme}, names.SimpleNameGenerator}

func (limitrangeStrategy) NamespaceScoped() bool {
	return true
}

// 创建时若未提供名称则生成 UUID，避免 LimitRange 规则对象因缺少 metadata.name 无法持久化。
func (limitrangeStrategy) PrepareForCreate(ctx context.Context, obj runtime.Object) {
	limitRange := obj.(*api.LimitRange)
	if len(limitRange.Name) == 0 {
		limitRange.Name = string(uuid.NewUUID())
	}
}

func (limitrangeStrategy) PrepareForUpdate(ctx context.Context, obj, old runtime.Object) {
}

// Validate 检查每种资源的 Min/Max/Default/DefaultRequest/MaxLimitRequestRatio 是否构成合法约束。
func (limitrangeStrategy) Validate(ctx context.Context, obj runtime.Object) field.ErrorList {
	limitRange := obj.(*api.LimitRange)
	return validation.ValidateLimitRange(limitRange)
}

// WarningsOnCreate returns warnings for the creation of the given object.
func (limitrangeStrategy) WarningsOnCreate(ctx context.Context, obj runtime.Object) []string {
	return nil
}

// Canonicalize normalizes the object after validation.
func (limitrangeStrategy) Canonicalize(obj runtime.Object) {
}

// LimitRange 保留 update-on-create 行为；对象不存在时的更新请求可以作为创建请求处理。
func (limitrangeStrategy) AllowCreateOnUpdate(ctx context.Context) bool {
	return true
}

func (limitrangeStrategy) ValidateUpdate(ctx context.Context, obj, old runtime.Object) field.ErrorList {
	limitRange := obj.(*api.LimitRange)
	return validation.ValidateLimitRange(limitRange)
}

// WarningsOnUpdate returns warnings for the given update.
func (limitrangeStrategy) WarningsOnUpdate(ctx context.Context, obj, old runtime.Object) []string {
	return nil
}

func (limitrangeStrategy) AllowUnconditionalUpdate(ctx context.Context) bool {
	return true
}
