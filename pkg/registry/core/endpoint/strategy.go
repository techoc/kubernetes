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

// Strategy is the default logic that applies when creating and updating Endpoint
// objects via the REST API.
var Strategy = endpointsStrategy{rest.DeclarativeValidation{Scheme: legacyscheme.Scheme}, names.SimpleNameGenerator}

// NamespaceScoped is true for endpoints.
func (endpointsStrategy) NamespaceScoped() bool {
	return true
}

// PrepareForCreate clears fields that are not allowed to be set by end users on creation.
// Endpoints 没有需要由 API Server 在创建前重置的字段；这里保留空钩子以满足通用 REST Strategy 接口。
func (endpointsStrategy) PrepareForCreate(ctx context.Context, obj runtime.Object) {
}

// PrepareForUpdate clears fields that are not allowed to be set by end users on update.
func (endpointsStrategy) PrepareForUpdate(ctx context.Context, obj, old runtime.Object) {
}

// Validate validates a new endpoints.
// Validate 检查 Endpoints 的 Subsets、Address、NotReadyAddress 和端口结构是否符合 API 约束。
func (endpointsStrategy) Validate(ctx context.Context, obj runtime.Object) field.ErrorList {
	return validation.ValidateEndpointsCreate(obj.(*api.Endpoints))
}

// WarningsOnCreate returns warnings for the creation of the given object.
func (endpointsStrategy) WarningsOnCreate(ctx context.Context, obj runtime.Object) []string {
	return endpointsWarnings(obj.(*api.Endpoints))
}

// Canonicalize normalizes the object after validation.
func (endpointsStrategy) Canonicalize(obj runtime.Object) {
}

// AllowCreateOnUpdate is true for endpoints.
func (endpointsStrategy) AllowCreateOnUpdate(ctx context.Context) bool {
	return true
}

// ValidateUpdate is the default update validation for an end user.
func (endpointsStrategy) ValidateUpdate(ctx context.Context, obj, old runtime.Object) field.ErrorList {
	return validation.ValidateEndpointsUpdate(obj.(*api.Endpoints), old.(*api.Endpoints))
}

// WarningsOnUpdate returns warnings for the given update.
func (endpointsStrategy) WarningsOnUpdate(ctx context.Context, obj, old runtime.Object) []string {
	return endpointsWarnings(obj.(*api.Endpoints))
}

func (endpointsStrategy) AllowUnconditionalUpdate(ctx context.Context) bool {
	return true
}

// endpointsWarnings 为手工维护的 Endpoints 检查异常 IP；由 Endpoints Controller 管理的对象跳过重复检查以节省开销。
func endpointsWarnings(endpoints *api.Endpoints) []string {
	// Save time by not checking for bad IPs if the request is coming from the
	// Endpoints controller, since we know it fixes up any invalid IPs from its input
	// data when outputting the Endpoints. (The "managed-by" label is new, so this
	// heuristic may fail in skewed clusters, but that just means we won't get the
	// optimization during the skew.)
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
