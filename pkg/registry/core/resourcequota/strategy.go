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

package resourcequota

import (
	"context"
	"fmt"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/apiserver/pkg/registry/rest"
	"k8s.io/apiserver/pkg/storage/names"
	"k8s.io/kubernetes/pkg/api/legacyscheme"
	api "k8s.io/kubernetes/pkg/apis/core"
	"k8s.io/kubernetes/pkg/apis/core/validation"
	"sigs.k8s.io/structured-merge-diff/v6/fieldpath"
)

// resourcequotaStrategy 定义 ResourceQuota 主资源策略，隔离由配额控制器维护的 Status，并校验 Hard 配额声明。
type resourcequotaStrategy struct {
	rest.DeclarativeValidation
	names.NameGenerator
}

// Strategy is the default logic that applies when creating and updating ResourceQuota
// objects via the REST API.
var Strategy = resourcequotaStrategy{rest.DeclarativeValidation{Scheme: legacyscheme.Scheme}, names.SimpleNameGenerator}

// NamespaceScoped is true for resourcequotas.
func (resourcequotaStrategy) NamespaceScoped() bool {
	return true
}

// GetResetFields returns the set of fields that get reset by the strategy
// and should not be modified by the user.
func (resourcequotaStrategy) GetResetFields() map[fieldpath.APIVersion]*fieldpath.Set {
	fields := map[fieldpath.APIVersion]*fieldpath.Set{
		"v1": fieldpath.NewSet(
			fieldpath.MakePathOrDie("status"),
		),
	}

	return fields
}

// PrepareForCreate clears fields that are not allowed to be set by end users on creation.
// 创建时重置 Status；Used/Hard 的实际使用量由 ResourceQuota Controller 计算，不能由用户在创建请求中伪造。
func (resourcequotaStrategy) PrepareForCreate(ctx context.Context, obj runtime.Object) {
	resourcequota := obj.(*api.ResourceQuota)
	resourcequota.Status = api.ResourceQuotaStatus{}
}

// PrepareForUpdate clears fields that are not allowed to be set by end users on update.
// 普通配额更新保留旧 Status，只允许修改 Spec.Hard、ScopeSelector 等期望配置。
func (resourcequotaStrategy) PrepareForUpdate(ctx context.Context, obj, old runtime.Object) {
	newResourcequota := obj.(*api.ResourceQuota)
	oldResourcequota := old.(*api.ResourceQuota)
	newResourcequota.Status = oldResourcequota.Status
}

// Validate validates a new resourcequota.
func (resourcequotaStrategy) Validate(ctx context.Context, obj runtime.Object) field.ErrorList {
	resourcequota := obj.(*api.ResourceQuota)
	return validation.ValidateResourceQuota(resourcequota)
}

// all known resource names that we want to check for request <= limit
// 这些常见资源同时检查 requests.* 与 limits.*，在请求配额高于限制配额时给出可操作的 Warning。
var knownResourceNames = []api.ResourceName{
	api.ResourceCPU,
	api.ResourceMemory,
	api.ResourceStorage,
	api.ResourceEphemeralStorage,
}

// WarningsOnCreate returns warnings for the creation of the given object.
// WarningsOnCreate 提醒 requests 额度大于 limits 额度的可疑配置；它是警告，不替代 ResourceQuota 语义校验。
func (resourcequotaStrategy) WarningsOnCreate(ctx context.Context, obj runtime.Object) []string {
	resourcequota := obj.(*api.ResourceQuota)
	var allWarnings []string
	for _, resourceName := range knownResourceNames {
		requestResourceName := api.ResourceName(fmt.Sprintf("requests.%s", resourceName))
		request, requestOK := resourcequota.Spec.Hard[requestResourceName]
		if !requestOK && (resourceName == api.ResourceCPU || resourceName == api.ResourceMemory) {
			// try the bare name for cpu and memory
			request, requestOK = resourcequota.Spec.Hard[resourceName]
			if requestOK {
				requestResourceName = resourceName
			}
		}
		limitResourceName := api.ResourceName(fmt.Sprintf("limits.%s", resourceName))
		limit, limitOK := resourcequota.Spec.Hard[limitResourceName]
		if requestOK && limitOK && request.Cmp(limit) > 0 {
			allWarnings = append(allWarnings, fmt.Sprintf("ResourceQuota %s (%s) should be less than %s (%s)",
				requestResourceName, request.String(), limitResourceName, limit.String()))
		}
	}
	return allWarnings
}

// Canonicalize normalizes the object after validation.
func (resourcequotaStrategy) Canonicalize(obj runtime.Object) {
}

// AllowCreateOnUpdate is false for resourcequotas.
func (resourcequotaStrategy) AllowCreateOnUpdate(ctx context.Context) bool {
	return false
}

// ValidateUpdate is the default update validation for an end user.
func (resourcequotaStrategy) ValidateUpdate(ctx context.Context, obj, old runtime.Object) field.ErrorList {
	newObj, oldObj := obj.(*api.ResourceQuota), old.(*api.ResourceQuota)
	return validation.ValidateResourceQuotaUpdate(newObj, oldObj)
}

// WarningsOnUpdate returns warnings for the given update.
func (resourcequotaStrategy) WarningsOnUpdate(ctx context.Context, obj, old runtime.Object) []string {
	return nil
}

func (resourcequotaStrategy) AllowUnconditionalUpdate(ctx context.Context) bool {
	return true
}

type resourcequotaStatusStrategy struct {
	resourcequotaStrategy
}

// StatusStrategy is the default logic invoked when updating object status.
var StatusStrategy = resourcequotaStatusStrategy{Strategy}

// GetResetFields returns the set of fields that get reset by the strategy
// and should not be modified by the user.
func (resourcequotaStatusStrategy) GetResetFields() map[fieldpath.APIVersion]*fieldpath.Set {
	fields := map[fieldpath.APIVersion]*fieldpath.Set{
		"v1": fieldpath.NewSet(
			fieldpath.MakePathOrDie("spec"),
		),
	}

	return fields
}

// /status 只接受 ResourceQuota.Status 变化，Spec 始终从旧对象恢复。
func (resourcequotaStatusStrategy) PrepareForUpdate(ctx context.Context, obj, old runtime.Object) {
	newResourcequota := obj.(*api.ResourceQuota)
	oldResourcequota := old.(*api.ResourceQuota)
	newResourcequota.Spec = oldResourcequota.Spec
}

func (resourcequotaStatusStrategy) ValidateUpdate(ctx context.Context, obj, old runtime.Object) field.ErrorList {
	return validation.ValidateResourceQuotaStatusUpdate(obj.(*api.ResourceQuota), old.(*api.ResourceQuota))
}

// WarningsOnUpdate returns warnings for the given update.
func (resourcequotaStatusStrategy) WarningsOnUpdate(ctx context.Context, obj, old runtime.Object) []string {
	return nil
}
