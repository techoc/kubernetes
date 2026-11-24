/*
Copyright 2016 The Kubernetes Authors.

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

package certificates

import (
	"context"
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/validation/field"
	genericapirequest "k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/apiserver/pkg/registry/generic"
	"k8s.io/apiserver/pkg/registry/rest"
	"k8s.io/apiserver/pkg/storage/names"
	"k8s.io/kubernetes/pkg/api/legacyscheme"
	"k8s.io/kubernetes/pkg/apis/certificates"
	"k8s.io/kubernetes/pkg/apis/certificates/validation"
	"sigs.k8s.io/structured-merge-diff/v6/fieldpath"
)

// csrStrategy 定义证书签名请求（CSR）的 REST 写入策略。
type csrStrategy struct {
	rest.DeclarativeValidation
	names.NameGenerator
}

// Strategy 是创建、更新和删除 CSR 时使用的默认 REST 策略。
var Strategy = csrStrategy{rest.DeclarativeValidation{Scheme: legacyscheme.Scheme}, names.SimpleNameGenerator}

// NamespaceScoped 表示 CSR 是集群级资源，不属于任何命名空间。
func (csrStrategy) NamespaceScoped() bool {
	return false
}

// GetResetFields 返回主资源写入时由服务端重置、不能由用户直接修改的 Spec 和 Status 字段。
func (csrStrategy) GetResetFields() map[fieldpath.APIVersion]*fieldpath.Set {
	fields := map[fieldpath.APIVersion]*fieldpath.Set{
		"certificates.k8s.io/v1": fieldpath.NewSet(
			fieldpath.MakePathOrDie("spec"),
			fieldpath.MakePathOrDie("status"),
		),
		"certificates.k8s.io/v1beta1": fieldpath.NewSet(
			fieldpath.MakePathOrDie("spec"),
			fieldpath.MakePathOrDie("status"),
		),
	}

	return fields
}

// AllowCreateOnUpdate 返回 false，CSR 不能通过更新请求隐式创建。
func (csrStrategy) AllowCreateOnUpdate(ctx context.Context) bool {
	return false
}

// PrepareForCreate 清除用户无权指定的身份和状态字段，并从请求上下文注入经过认证的用户信息。
func (csrStrategy) PrepareForCreate(ctx context.Context, obj runtime.Object) {
	csr := obj.(*certificates.CertificateSigningRequest)

	// 清除请求对象中用户自行填写的身份信息，防止伪造申请者身份。
	csr.Spec.Username = ""
	csr.Spec.UID = ""
	csr.Spec.Groups = nil
	csr.Spec.Extra = nil
	// 从请求上下文读取已认证用户，并将其身份信息写入 CSR Spec。
	if user, ok := genericapirequest.UserFrom(ctx); ok {
		csr.Spec.Username = user.GetName()
		csr.Spec.UID = user.GetUID()
		csr.Spec.Groups = user.GetGroups()
		if extra := user.GetExtra(); len(extra) > 0 {
			csr.Spec.Extra = map[string]certificates.ExtraValue{}
			for k, v := range extra {
				csr.Spec.Extra[k] = v
			}
		}
	}

	// 显式清空状态，确保用户不能在创建 CSR 时预先将其标记为已批准或已拒绝。
	csr.Status = certificates.CertificateSigningRequestStatus{}
	csr.Status.Conditions = []certificates.CertificateSigningRequestCondition{}
}

// PrepareForUpdate 保留旧对象的 Spec 和 Status；CSR 创建后不可通过主资源更新修改，只能使用专用子资源变更状态。
func (csrStrategy) PrepareForUpdate(ctx context.Context, obj, old runtime.Object) {
	newCSR := obj.(*certificates.CertificateSigningRequest)
	oldCSR := old.(*certificates.CertificateSigningRequest)

	newCSR.Spec = oldCSR.Spec
	newCSR.Status = oldCSR.Status
}

// Validate 校验新 CSR，包括对请求中 PKCS#10 签名的正确性进行验证。
func (csrStrategy) Validate(ctx context.Context, obj runtime.Object) field.ErrorList {
	csr := obj.(*certificates.CertificateSigningRequest)
	return validation.ValidateCertificateSigningRequestCreate(csr)
}

// WarningsOnCreate 返回创建 CSR 时的非阻断性警告；当前不产生额外警告。
func (csrStrategy) WarningsOnCreate(ctx context.Context, obj runtime.Object) []string { return nil }

// Canonicalize 在校验（包括签名验证）后规范化 CSR；当前无需额外处理。
func (csrStrategy) Canonicalize(obj runtime.Object) {}

// ValidateUpdate 执行 CSR 主资源更新校验。
func (csrStrategy) ValidateUpdate(ctx context.Context, obj, old runtime.Object) field.ErrorList {
	oldCSR := old.(*certificates.CertificateSigningRequest)
	newCSR := obj.(*certificates.CertificateSigningRequest)
	return validation.ValidateCertificateSigningRequestUpdate(newCSR, oldCSR)
}

// WarningsOnUpdate 返回更新 CSR 主资源时的非阻断性警告；当前不产生额外警告。
func (csrStrategy) WarningsOnUpdate(ctx context.Context, obj, old runtime.Object) []string {
	return nil
}

// AllowUnconditionalUpdate 返回 true 时，若请求未提供 ResourceVersion，通用更新流程会使用存储中的最新版本；
// 否则会校验请求版本与 etcd 中最新对象的版本是否一致。
func (csrStrategy) AllowUnconditionalUpdate(ctx context.Context) bool {
	return true
}

// csrStatusStrategy 定义 CSR Status 子资源的专用写入策略。
type csrStatusStrategy struct {
	csrStrategy
}

var StatusStrategy = csrStatusStrategy{Strategy}

// GetResetFields 返回 Status 子资源更新时由服务端重置、不能由请求修改的 Spec 和受保护条件字段。
func (csrStatusStrategy) GetResetFields() map[fieldpath.APIVersion]*fieldpath.Set {
	fields := map[fieldpath.APIVersion]*fieldpath.Set{
		"certificates.k8s.io/v1": fieldpath.NewSet(
			fieldpath.MakePathOrDie("spec"),
			fieldpath.MakePathOrDie("status", "conditions"),
		),
		"certificates.k8s.io/v1beta1": fieldpath.NewSet(
			fieldpath.MakePathOrDie("spec"),
			fieldpath.MakePathOrDie("status", "conditions"),
		),
	}

	return fields
}

func (csrStatusStrategy) PrepareForUpdate(ctx context.Context, obj, old runtime.Object) {
	newCSR := obj.(*certificates.CertificateSigningRequest)
	oldCSR := old.(*certificates.CertificateSigningRequest)

	// Status 子资源请求不能修改 CSR Spec。
	newCSR.Spec = oldCSR.Spec

	// 保留已有的 Approved/Denied 条件实例。调用方不能通过 /status 添加或删除这两类条件，
	// 否则条件数量不一致时无法恢复原值，并会在后续校验中被拒绝。
	preserveConditionInstances(newCSR, oldCSR, certificates.CertificateApproved)
	preserveConditionInstances(newCSR, oldCSR, certificates.CertificateDenied)

	populateConditionTimestamps(newCSR, oldCSR)
}

// preserveConditionInstances 将旧 CSR 中指定类型的条件实例复制到新对象；若新对象增删了该类型的实例，则返回 false。
func preserveConditionInstances(newCSR, oldCSR *certificates.CertificateSigningRequest, conditionType certificates.RequestConditionType) bool {
	oldIndices := findConditionIndices(oldCSR, conditionType)
	newIndices := findConditionIndices(newCSR, conditionType)
	if len(oldIndices) != len(newIndices) {
		// 条件实例数量发生变化，无法按位置一一恢复旧值。
		return false
	}
	// 按对应索引恢复旧条件，避免通过该子资源篡改条件内容。
	for i, oldIndex := range oldIndices {
		newCSR.Status.Conditions[newIndices[i]] = oldCSR.Status.Conditions[oldIndex]
	}
	return true
}

// findConditionIndices 返回 CSR 状态中指定类型条件的所有索引。
func findConditionIndices(csr *certificates.CertificateSigningRequest, conditionType certificates.RequestConditionType) []int {
	var retval []int
	for i, c := range csr.Status.Conditions {
		if c.Type == conditionType {
			retval = append(retval, i)
		}
	}
	return retval
}

// nowFunc 提供可替换的当前时间函数，便于单元测试稳定控制时间。
var nowFunc = metav1.Now

// populateConditionTimestamps 为新 CSR 中缺失的 LastUpdateTime 和 LastTransitionTime 填充时间戳。
func populateConditionTimestamps(newCSR, oldCSR *certificates.CertificateSigningRequest) {
	now := nowFunc()
	for i := range newCSR.Status.Conditions {
		if newCSR.Status.Conditions[i].LastUpdateTime.IsZero() {
			newCSR.Status.Conditions[i].LastUpdateTime = now
		}

		// 若旧对象存在相同 Type 和 Status 的条件，则沿用其 LastTransitionTime；否则使用当前时间。
		if newCSR.Status.Conditions[i].LastTransitionTime.IsZero() {
			lastTransition := now
			for _, oldCondition := range oldCSR.Status.Conditions {
				if oldCondition.Type == newCSR.Status.Conditions[i].Type &&
					oldCondition.Status == newCSR.Status.Conditions[i].Status &&
					!oldCondition.LastTransitionTime.IsZero() {
					lastTransition = oldCondition.LastTransitionTime
					break
				}
			}
			newCSR.Status.Conditions[i].LastTransitionTime = lastTransition
		}
	}
}

func (csrStatusStrategy) ValidateUpdate(ctx context.Context, obj, old runtime.Object) field.ErrorList {
	newCSR := obj.(*certificates.CertificateSigningRequest)
	oldCSR := old.(*certificates.CertificateSigningRequest)
	return validation.ValidateCertificateSigningRequestStatusUpdate(newCSR, oldCSR)
}

// WarningsOnUpdate 返回 Approval 子资源更新时的非阻断性警告；当前不产生额外警告。
func (csrStatusStrategy) WarningsOnUpdate(ctx context.Context, obj, old runtime.Object) []string {
	return nil
}

// Canonicalize 在 Status 更新校验后规范化对象；当前无需额外处理。
func (csrStatusStrategy) Canonicalize(obj runtime.Object) {
}

// csrApprovalStrategy 定义 CSR Approval 子资源的专用写入策略。
type csrApprovalStrategy struct {
	csrStrategy
}

var ApprovalStrategy = csrApprovalStrategy{Strategy}

// GetResetFields 返回 Approval 子资源更新时由服务端重置、不能由请求修改的 Spec 和证书字段集合。
func (csrApprovalStrategy) GetResetFields() map[fieldpath.APIVersion]*fieldpath.Set {
	fields := map[fieldpath.APIVersion]*fieldpath.Set{
		"certificates.k8s.io/v1": fieldpath.NewSet(
			fieldpath.MakePathOrDie("spec"),
			fieldpath.MakePathOrDie("status", "certificate"),
		),
		"certificates.k8s.io/v1beta1": fieldpath.NewSet(
			fieldpath.MakePathOrDie("spec"),
			fieldpath.MakePathOrDie("status", "certificate"),
		),
	}

	return fields
}

// PrepareForUpdate 将 Approval 子资源的变更限制为 Conditions，并为新增或缺失的条件时间戳补值。
func (csrApprovalStrategy) PrepareForUpdate(ctx context.Context, obj, old runtime.Object) {
	newCSR := obj.(*certificates.CertificateSigningRequest)
	oldCSR := old.(*certificates.CertificateSigningRequest)

	populateConditionTimestamps(newCSR, oldCSR)
	newConditions := newCSR.Status.Conditions

	// Approval 子资源只能修改 Conditions，Spec 和其他 Status 字段均沿用旧对象。
	newCSR.Spec = oldCSR.Spec
	newCSR.Status = oldCSR.Status
	newCSR.Status.Conditions = newConditions
}

func (csrApprovalStrategy) ValidateUpdate(ctx context.Context, obj, old runtime.Object) field.ErrorList {
	newCSR := obj.(*certificates.CertificateSigningRequest)
	oldCSR := old.(*certificates.CertificateSigningRequest)
	return validation.ValidateCertificateSigningRequestApprovalUpdate(newCSR, oldCSR)
}

// WarningsOnUpdate 返回 Approval 子资源更新时的非阻断性警告；当前不产生额外警告。
func (csrApprovalStrategy) WarningsOnUpdate(ctx context.Context, obj, old runtime.Object) []string {
	return nil
}

// GetAttrs 提取 CSR 的标签和可筛选字段，供 List、Watch 等请求执行过滤。
func GetAttrs(obj runtime.Object) (labels.Set, fields.Set, error) {
	csr, ok := obj.(*certificates.CertificateSigningRequest)
	if !ok {
		return nil, nil, fmt.Errorf("not a certificatesigningrequest")
	}
	return labels.Set(csr.Labels), SelectableFields(csr), nil
}

// SelectableFields 返回 CSR 支持字段选择器查询的字段集合。
func SelectableFields(obj *certificates.CertificateSigningRequest) fields.Set {
	objectMetaFieldsSet := generic.ObjectMetaFieldsSet(&obj.ObjectMeta, false)
	csrSpecificFieldsSet := fields.Set{
		"spec.signerName": obj.Spec.SignerName,
	}
	return generic.MergeFieldsSets(objectMetaFieldsSet, csrSpecificFieldsSet)
}
