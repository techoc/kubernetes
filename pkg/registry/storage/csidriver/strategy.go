/*
Copyright 2019 The Kubernetes Authors.

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

// 本文件实现 csidriver 的 REST Strategy，定义作用域、写入前字段处理、校验、告警以及更新规则。
package csidriver

import (
	"context"

	apiequality "k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/apiserver/pkg/registry/rest"
	"k8s.io/apiserver/pkg/storage/names"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/kubernetes/pkg/api/legacyscheme"
	"k8s.io/kubernetes/pkg/apis/storage"
	"k8s.io/kubernetes/pkg/apis/storage/validation"
	"k8s.io/kubernetes/pkg/features"
)

const (
	warningServiceAccountTokenInSecretsRecommended = "spec.serviceAccountTokenInSecrets is unset; if supported by this CSI driver, set to true to prevent possible logging of tokens in volume attributes"
)

// csiDriverStrategy 定义 CSIDriver 的创建、更新、校验和告警策略。
type csiDriverStrategy struct {
	rest.DeclarativeValidation
	names.NameGenerator
}

// Strategy 是通过 REST API 创建、更新和删除 CSIDriver 时使用的默认策略。
var Strategy = csiDriverStrategy{rest.DeclarativeValidation{Scheme: legacyscheme.Scheme}, names.SimpleNameGenerator}

// NamespaceScoped 返回 false，因为 CSIDriver 是集群级资源。
func (csiDriverStrategy) NamespaceScoped() bool {
	return false
}

// PrepareForCreate 清理当前 Feature Gate 未启用的字段，防止客户端在功能关闭时新写入这些字段。
func (csiDriverStrategy) PrepareForCreate(ctx context.Context, obj runtime.Object) {
	csiDriver := obj.(*storage.CSIDriver)
	if !utilfeature.DefaultFeatureGate.Enabled(features.SELinuxMountReadWriteOncePod) {
		csiDriver.Spec.SELinuxMount = nil
	}
	if !utilfeature.DefaultFeatureGate.Enabled(features.MutableCSINodeAllocatableCount) {
		csiDriver.Spec.NodeAllocatableUpdatePeriodSeconds = nil
	}
	if !utilfeature.DefaultFeatureGate.Enabled(features.CSIServiceAccountTokenSecrets) {
		csiDriver.Spec.ServiceAccountTokenInSecrets = nil
	}
	if !utilfeature.DefaultFeatureGate.Enabled(features.VolumeLimitScaling) {
		csiDriver.Spec.PreventPodSchedulingIfMissing = nil
	}
}

// Validate 执行创建请求的业务合法性校验，将字段路径和错误原因整理为 field.ErrorList；返回非空错误时 API 请求会被拒绝。
func (csiDriverStrategy) Validate(ctx context.Context, obj runtime.Object) field.ErrorList {
	csiDriver := obj.(*storage.CSIDriver)

	return validation.ValidateCSIDriver(csiDriver)
}

// WarningsOnCreate 在配置 TokenRequests 但未明确启用 ServiceAccountTokenInSecrets 时，提示潜在令牌泄露风险。
func (csiDriverStrategy) WarningsOnCreate(ctx context.Context, obj runtime.Object) []string {
	csiDriver := obj.(*storage.CSIDriver)
	var warnings []string

	// 如果配置了 TokenRequests，却没有启用 ServiceAccountTokenInSecrets，则返回安全建议。
	if utilfeature.DefaultFeatureGate.Enabled(features.CSIServiceAccountTokenSecrets) &&
		len(csiDriver.Spec.TokenRequests) > 0 &&
		csiDriver.Spec.ServiceAccountTokenInSecrets == nil {
		warnings = append(warnings, warningServiceAccountTokenInSecretsRecommended)
	}

	return warnings
}

// Canonicalize 在校验后规范化 CSIDriver；当前不执行额外转换。
func (csiDriverStrategy) Canonicalize(obj runtime.Object) {
}

// AllowCreateOnUpdate 返回 false，CSIDriver 必须通过 POST 创建。
func (csiDriverStrategy) AllowCreateOnUpdate(ctx context.Context) bool {
	return false
}

// PrepareForUpdate 清理当前 Feature Gate 未启用且旧对象未设置过的字段。
// 如果旧对象已经使用该字段，则保留其值，避免集群降级或关闭特性时破坏已有配置。
func (csiDriverStrategy) PrepareForUpdate(ctx context.Context, obj, old runtime.Object) {
	newCSIDriver := obj.(*storage.CSIDriver)
	oldCSIDriver := old.(*storage.CSIDriver)

	if oldCSIDriver.Spec.SELinuxMount == nil &&
		!utilfeature.DefaultFeatureGate.Enabled(features.SELinuxMountReadWriteOncePod) {
		newCSIDriver.Spec.SELinuxMount = nil
	}

	if oldCSIDriver.Spec.ServiceAccountTokenInSecrets == nil &&
		!utilfeature.DefaultFeatureGate.Enabled(features.CSIServiceAccountTokenSecrets) {
		newCSIDriver.Spec.ServiceAccountTokenInSecrets = nil
	}

	if oldCSIDriver.Spec.NodeAllocatableUpdatePeriodSeconds == nil &&
		!utilfeature.DefaultFeatureGate.Enabled(features.MutableCSINodeAllocatableCount) {
		newCSIDriver.Spec.NodeAllocatableUpdatePeriodSeconds = nil
	}

	if oldCSIDriver.Spec.PreventPodSchedulingIfMissing == nil &&
		!utilfeature.DefaultFeatureGate.Enabled(features.VolumeLimitScaling) {
		newCSIDriver.Spec.PreventPodSchedulingIfMissing = nil
	}

	// Spec 发生变化时递增 Generation。
	if !apiequality.Semantic.DeepEqual(oldCSIDriver.Spec, newCSIDriver.Spec) {
		newCSIDriver.Generation = oldCSIDriver.Generation + 1
	}
}

// ValidateUpdate 对更新后的对象及其旧版本进行联合校验，检查字段合法性、不可变字段和跨版本兼容约束。
func (csiDriverStrategy) ValidateUpdate(ctx context.Context, obj, old runtime.Object) field.ErrorList {
	newCSIDriverObj := obj.(*storage.CSIDriver)
	oldCSIDriverObj := old.(*storage.CSIDriver)
	return validation.ValidateCSIDriverUpdate(newCSIDriverObj, oldCSIDriverObj)
}

// WarningsOnUpdate 在 TokenRequests 发生变化且未明确启用 ServiceAccountTokenInSecrets 时返回安全建议。
func (csiDriverStrategy) WarningsOnUpdate(ctx context.Context, obj, old runtime.Object) []string {
	newCSIDriver := obj.(*storage.CSIDriver)
	oldCSIDriver := old.(*storage.CSIDriver)

	var warnings []string

	// 如果本次更新修改了 TokenRequests，却没有启用 ServiceAccountTokenInSecrets，则返回安全建议。
	if utilfeature.DefaultFeatureGate.Enabled(features.CSIServiceAccountTokenSecrets) &&
		!apiequality.Semantic.DeepEqual(oldCSIDriver.Spec.TokenRequests, newCSIDriver.Spec.TokenRequests) &&
		len(newCSIDriver.Spec.TokenRequests) > 0 &&
		newCSIDriver.Spec.ServiceAccountTokenInSecrets == nil {
		warnings = append(warnings, warningServiceAccountTokenInSecretsRecommended)
	}

	return warnings
}

// AllowUnconditionalUpdate 返回 false，更新请求必须携带 ResourceVersion。
func (csiDriverStrategy) AllowUnconditionalUpdate(ctx context.Context) bool {
	return false
}
