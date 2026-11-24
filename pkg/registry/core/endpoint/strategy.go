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

package endpoint

import (
	"context"

	"k8s.io/apimachinery/pkg/runtime"
	utilvalidation "k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/apiserver/pkg/registry/rest"
	"k8s.io/apiserver/pkg/storage/names"
	"k8s.io/kubernetes/pkg/api/legacyscheme"
	api "k8s.io/kubernetes/pkg/apis/core"
	"k8s.io/kubernetes/pkg/apis/core/validation"
	endpointscontroller "k8s.io/kubernetes/pkg/controller/endpoint"
)

// endpointsStrategy 定义 Endpoints 的 REST 写入策略；地址合法性由核心校验器检查，创建/更新告警额外提示异常 IP。
type endpointsStrategy struct {
	rest.DeclarativeValidation
	names.NameGenerator
}

// Strategy 是通过 REST API 创建和更新 Endpoints 时使用的默认策略。
var Strategy = endpointsStrategy{rest.DeclarativeValidation{Scheme: legacyscheme.Scheme}, names.SimpleNameGenerator}

// NamespaceScoped 表示 Endpoints 是命名空间级资源。
func (endpointsStrategy) NamespaceScoped() bool {
	return true
}

// PrepareForCreate 当前不重置字段；Endpoints 的创建约束由 Validate 统一执行。
// Endpoints 没有需要由 API Server 在创建前重置的字段；这里保留空钩子以满足通用 REST Strategy 接口。
func (endpointsStrategy) PrepareForCreate(ctx context.Context, obj runtime.Object) {
}

// PrepareForUpdate 当前不重置字段；Endpoints 的更新约束由 ValidateUpdate 统一执行。
func (endpointsStrategy) PrepareForUpdate(ctx context.Context, obj, old runtime.Object) {
}

// Validate 校验新创建的 Endpoints。
// Validate 检查 Endpoints 的 Subsets、Address、NotReadyAddress 和端口结构是否符合 API 约束。
func (endpointsStrategy) Validate(ctx context.Context, obj runtime.Object) field.ErrorList {
	return validation.ValidateEndpointsCreate(obj.(*api.Endpoints))
}

// WarningsOnCreate 返回创建 Endpoints 时发现的非阻断性警告。
func (endpointsStrategy) WarningsOnCreate(ctx context.Context, obj runtime.Object) []string {
	return endpointsWarnings(obj.(*api.Endpoints))
}

// Canonicalize 在校验后规范化对象；当前策略无需额外处理。
func (endpointsStrategy) Canonicalize(obj runtime.Object) {
}

// AllowCreateOnUpdate 返回 true：为兼容旧式 Endpoints 上报流程，允许更新请求在对象不存在时创建。
func (endpointsStrategy) AllowCreateOnUpdate(ctx context.Context) bool {
	return true
}

// ValidateUpdate 执行面向普通用户的 Endpoints 更新校验。
func (endpointsStrategy) ValidateUpdate(ctx context.Context, obj, old runtime.Object) field.ErrorList {
	return validation.ValidateEndpointsUpdate(obj.(*api.Endpoints), old.(*api.Endpoints))
}

// WarningsOnUpdate 返回更新 Endpoints 时发现的非阻断性警告。
func (endpointsStrategy) WarningsOnUpdate(ctx context.Context, obj, old runtime.Object) []string {
	return endpointsWarnings(obj.(*api.Endpoints))
}

// AllowUnconditionalUpdate 允许不携带 resourceVersion 的无条件更新。
func (endpointsStrategy) AllowUnconditionalUpdate(ctx context.Context) bool {
	return true
}

// endpointsWarnings 为手工维护的 Endpoints 检查异常 IP；由 Endpoints Controller 管理的对象跳过重复检查以节省开销。
func endpointsWarnings(endpoints *api.Endpoints) []string {
	// 请求来自 Endpoints Controller 时跳过异常 IP 检查：控制器在输出 Endpoints 前已修正输入中的无效 IP。
	// managed-by 标签在滚动升级期间可能因版本不一致而缺失，此时只是无法获得这项性能优化，不影响正确性。
	// managed-by 标签表明该对象由控制器生成，控制器已在写出前校正 IP，可跳过重复告警扫描。
	if endpoints.Labels[endpointscontroller.LabelManagedBy] == endpointscontroller.ControllerName {
		return nil
	}

	var warnings []string
	for i := range endpoints.Subsets {
		for j := range endpoints.Subsets[i].Addresses {
			fldPath := field.NewPath("subsets").Index(i).Child("addresses").Index(j).Child("ip")
			warnings = append(warnings, utilvalidation.GetWarningsForIP(fldPath, endpoints.Subsets[i].Addresses[j].IP)...)
		}
		for j := range endpoints.Subsets[i].NotReadyAddresses {
			fldPath := field.NewPath("subsets").Index(i).Child("notReadyAddresses").Index(j).Child("ip")
			warnings = append(warnings, utilvalidation.GetWarningsForIP(fldPath, endpoints.Subsets[i].NotReadyAddresses[j].IP)...)
		}
	}
	return warnings
}
