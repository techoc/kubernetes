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

// Strategy 是通过 REST API 创建和更新 ResourceQuota 对象时应用的默认逻辑。
var Strategy = resourcequotaStrategy{rest.DeclarativeValidation{Scheme: legacyscheme.Scheme}, names.SimpleNameGenerator}

// resourcequotas 是命名空间级资源，NamespaceScoped 返回 true。
func (resourcequotaStrategy) NamespaceScoped() bool {
	return true
}

// GetResetFields 返回由策略重置、且用户不应修改的字段集合。
func (resourcequotaStrategy) GetResetFields() map[fieldpath.APIVersion]*fieldpath.Set {
	fields := map[fieldpath.APIVersion]*fieldpath.Set{
		"v1": fieldpath.NewSet(
			fieldpath.MakePathOrDie("status"),
		),
	}

	return fields
}

// PrepareForCreate 清理最终用户不允许在创建时设置的字段：
func (resourcequotaStrategy) PrepareForCreate(ctx context.Context, obj runtime.Object) {
	resourcequota := obj.(*api.ResourceQuota)
	resourcequota.Status = api.ResourceQuotaStatus{}
}

// PrepareForUpdate 清理最终用户不允许在更新时设置的字段：
func (resourcequotaStrategy) PrepareForUpdate(ctx context.Context, obj, old runtime.Object) {
	newResourcequota := obj.(*api.ResourceQuota)
	oldResourcequota := old.(*api.ResourceQuota)
	newResourcequota.Status = oldResourcequota.Status
}

// Validate 校验新建的 resourcequota。
func (resourcequotaStrategy) Validate(ctx context.Context, obj runtime.Object) field.ErrorList {
	resourcequota := obj.(*api.ResourceQuota)
	return validation.ValidateResourceQuota(resourcequota)
}

// knownResourceNames 列出需要校验 requests.* <= limits.* 的常见资源名。
var knownResourceNames = []api.ResourceName{
	api.ResourceCPU,
	api.ResourceMemory,
	api.ResourceStorage,
	api.ResourceEphemeralStorage,
}

// WarningsOnCreate 提醒 requests 额度大于 limits 额度的可疑配置。
func (resourcequotaStrategy) WarningsOnCreate(ctx context.Context, obj runtime.Object) []string {
	resourcequota := obj.(*api.ResourceQuota)
	var allWarnings []string
	for _, resourceName := range knownResourceNames {
		requestResourceName := api.ResourceName(fmt.Sprintf("requests.%s", resourceName))
		request, requestOK := resourcequota.Spec.Hard[requestResourceName]
		if !requestOK && (resourceName == api.ResourceCPU || resourceName == api.ResourceMemory) {
			// cpu 和 memory 还需要尝试不带前缀的裸名称
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

// Canonicalize 在校验之后规范化对象。
func (resourcequotaStrategy) Canonicalize(obj runtime.Object) {
}

// resourcequotas 的 AllowCreateOnUpdate 为 false。
func (resourcequotaStrategy) AllowCreateOnUpdate(ctx context.Context) bool {
	return false
}

// ValidateUpdate 是面向最终用户的默认更新校验。
func (resourcequotaStrategy) ValidateUpdate(ctx context.Context, obj, old runtime.Object) field.ErrorList {
	newObj, oldObj := obj.(*api.ResourceQuota), old.(*api.ResourceQuota)
	return validation.ValidateResourceQuotaUpdate(newObj, oldObj)
}

// WarningsOnUpdate 返回本次更新的警告信息。
func (resourcequotaStrategy) WarningsOnUpdate(ctx context.Context, obj, old runtime.Object) []string {
	return nil
}

func (resourcequotaStrategy) AllowUnconditionalUpdate(ctx context.Context) bool {
	return true
}

type resourcequotaStatusStrategy struct {
	resourcequotaStrategy
}

// StatusStrategy 是更新对象 status 时调用的默认逻辑。
var StatusStrategy = resourcequotaStatusStrategy{Strategy}

// GetResetFields 返回由策略重置、且用户不应修改的字段集合。
func (resourcequotaStatusStrategy) GetResetFields() map[fieldpath.APIVersion]*fieldpath.Set {
	fields := map[fieldpath.APIVersion]*fieldpath.Set{
		"v1": fieldpath.NewSet(
			fieldpath.MakePathOrDie("spec"),
		),
	}

	return fields
}

// PrepareForUpdate 针对 /status 只接受 ResourceQuota.Status 变化，Spec 始终从旧对象恢复。
func (resourcequotaStatusStrategy) PrepareForUpdate(ctx context.Context, obj, old runtime.Object) {
	newResourcequota := obj.(*api.ResourceQuota)
	oldResourcequota := old.(*api.ResourceQuota)
	newResourcequota.Spec = oldResourcequota.Spec
}

func (resourcequotaStatusStrategy) ValidateUpdate(ctx context.Context, obj, old runtime.Object) field.ErrorList {
	return validation.ValidateResourceQuotaStatusUpdate(obj.(*api.ResourceQuota), old.(*api.ResourceQuota))
}

// WarningsOnUpdate 返回状态更新的警告信息，这里不做任何警告。
func (resourcequotaStatusStrategy) WarningsOnUpdate(ctx context.Context, obj, old runtime.Object) []string {
	return nil
}
