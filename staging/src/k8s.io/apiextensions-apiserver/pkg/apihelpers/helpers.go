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

// Package apihelpers 提供 CRD 的辅助函数，包括 API 审批状态检查、
// CRD 条件（condition）管理、版本查询和 finalizer 操作。
// 这些函数被 apiserver 和 controller 广泛使用。
package apihelpers

import (
	"fmt"
	"net/url"
	"strings"
	"time"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// IsProtectedCommunityGroup 检查 CRD 的 API 组是否受社区保护。
// k8s.io 和 kubernetes.io 及其子域名是保留的 API 组，需要 API 审批 annotation 才能创建 CRD。
// 这是为了防止第三方 CRD 使用与核心 API 冲突的组名。
func IsProtectedCommunityGroup(group string) bool {
	switch {
	case group == "k8s.io" || strings.HasSuffix(group, ".k8s.io"):
		return true
	case group == "kubernetes.io" || strings.HasSuffix(group, ".kubernetes.io"):
		return true
	default:
		return false
	}

}

// APIApprovalState 表示 API 审批 annotation 的各种状态
type APIApprovalState int

const (
	// APIApprovalInvalid 表示 annotation 的值格式不正确
	APIApprovalInvalid APIApprovalState = iota
	// APIApproved 表示 annotation 包含有效的 URL，API 已通过审批
	APIApproved
	// APIApprovalBypassed 表示 annotation 以 "unapproved" 开头，说明 API 未通过审批，但允许创建
	APIApprovalBypassed
	// APIApprovalMissing 表示 annotation 为空，受保护的组必须提供审批 annotation
	APIApprovalMissing
)

// GetAPIApprovalState 检查 CRD 的 API 审批状态和对应原因。
// 受保护的 API 组（k8s.io、kubernetes.io）必须提供审批 annotation，
// 值可以是审批 URL 或以 "unapproved" 开头的豁免说明。
func GetAPIApprovalState(annotations map[string]string) (state APIApprovalState, reason string) {
	annotation := annotations[apiextensionsv1.KubeAPIApprovedAnnotation]

	// we use the result of this parsing in the switch/case below
	url, annotationURLParseErr := url.ParseRequestURI(annotation)
	switch {
	case len(annotation) == 0:
		return APIApprovalMissing, fmt.Sprintf("protected groups must have approval annotation %q, see https://github.com/kubernetes/enhancements/pull/1111", apiextensionsv1.KubeAPIApprovedAnnotation)
	case strings.HasPrefix(annotation, "unapproved"):
		return APIApprovalBypassed, fmt.Sprintf("not approved: %q", annotation)
	case annotationURLParseErr == nil && url != nil && len(url.Host) > 0 && len(url.Scheme) > 0:
		return APIApproved, fmt.Sprintf("approved in %v", annotation)
	default:
		return APIApprovalInvalid, fmt.Sprintf("protected groups must have approval annotation %q with either a URL or a reason starting with \"unapproved\", see https://github.com/kubernetes/enhancements/pull/1111", apiextensionsv1.KubeAPIApprovedAnnotation)
	}
}

// SetCRDCondition 设置 CRD 的状态条件。如果条件已存在则更新，否则创建新条件。
// LastTransitionTime 仅在条件状态发生变化或之前未设置时更新，
// 以避免相同状态的重复更新导致时间戳跳动。
func SetCRDCondition(crd *apiextensionsv1.CustomResourceDefinition, newCondition apiextensionsv1.CustomResourceDefinitionCondition) {
	newCondition.LastTransitionTime = metav1.NewTime(time.Now())

	existingCondition := FindCRDCondition(crd, newCondition.Type)
	if existingCondition == nil {
		crd.Status.Conditions = append(crd.Status.Conditions, newCondition)
		return
	}

	if existingCondition.Status != newCondition.Status || existingCondition.LastTransitionTime.IsZero() {
		existingCondition.LastTransitionTime = newCondition.LastTransitionTime
	}

	existingCondition.Status = newCondition.Status
	existingCondition.Reason = newCondition.Reason
	existingCondition.Message = newCondition.Message
}

// RemoveCRDCondition 从 CRD 的 status.conditions 中移除指定类型的条件。
func RemoveCRDCondition(crd *apiextensionsv1.CustomResourceDefinition, conditionType apiextensionsv1.CustomResourceDefinitionConditionType) {
	newConditions := []apiextensionsv1.CustomResourceDefinitionCondition{}
	for _, condition := range crd.Status.Conditions {
		if condition.Type != conditionType {
			newConditions = append(newConditions, condition)
		}
	}
	crd.Status.Conditions = newConditions
}

// FindCRDCondition 在 CRD 的 status.conditions 中查找指定类型的条件，找不到返回 nil。
func FindCRDCondition(crd *apiextensionsv1.CustomResourceDefinition, conditionType apiextensionsv1.CustomResourceDefinitionConditionType) *apiextensionsv1.CustomResourceDefinitionCondition {
	for i := range crd.Status.Conditions {
		if crd.Status.Conditions[i].Type == conditionType {
			return &crd.Status.Conditions[i]
		}
	}

	return nil
}

// IsCRDConditionTrue 检查指定条件是否存在且为 True。
func IsCRDConditionTrue(crd *apiextensionsv1.CustomResourceDefinition, conditionType apiextensionsv1.CustomResourceDefinitionConditionType) bool {
	return IsCRDConditionPresentAndEqual(crd, conditionType, apiextensionsv1.ConditionTrue)
}

// IsCRDConditionFalse 检查指定条件是否存在且为 False。
func IsCRDConditionFalse(crd *apiextensionsv1.CustomResourceDefinition, conditionType apiextensionsv1.CustomResourceDefinitionConditionType) bool {
	return IsCRDConditionPresentAndEqual(crd, conditionType, apiextensionsv1.ConditionFalse)
}

// IsCRDConditionPresentAndEqual 检查指定条件是否存在且状态等于给定值。
func IsCRDConditionPresentAndEqual(crd *apiextensionsv1.CustomResourceDefinition, conditionType apiextensionsv1.CustomResourceDefinitionConditionType, status apiextensionsv1.ConditionStatus) bool {
	for _, condition := range crd.Status.Conditions {
		if condition.Type == conditionType {
			return condition.Status == status
		}
	}
	return false
}

// IsCRDConditionEquivalent 比较两个条件是否等价（忽略时间字段）。
// 用于测试和状态比较，避免因时间戳差异导致误判。
func IsCRDConditionEquivalent(lhs, rhs *apiextensionsv1.CustomResourceDefinitionCondition) bool {
	if lhs == nil && rhs == nil {
		return true
	}
	if lhs == nil || rhs == nil {
		return false
	}

	return lhs.Message == rhs.Message && lhs.Reason == rhs.Reason && lhs.Status == rhs.Status && lhs.Type == rhs.Type
}

// CRDHasFinalizer 检查 CRD 是否包含指定的 finalizer。
func CRDHasFinalizer(crd *apiextensionsv1.CustomResourceDefinition, needle string) bool {
	for _, finalizer := range crd.Finalizers {
		if finalizer == needle {
			return true
		}
	}

	return false
}

// CRDRemoveFinalizer 从 CRD 中移除指定的 finalizer（如果存在）。
// 在 CRD 删除流程中，控制器完成清理后调用此函数移除 finalizer，允许 CRD 真正被删除。
func CRDRemoveFinalizer(crd *apiextensionsv1.CustomResourceDefinition, needle string) {
	newFinalizers := []string{}
	for _, finalizer := range crd.Finalizers {
		if finalizer != needle {
			newFinalizers = append(newFinalizers, finalizer)
		}
	}
	crd.Finalizers = newFinalizers
}

// HasServedCRDVersion 检查给定版本是否在 CRD 中且 Served 标志为 true。
// Served 为 true 表示该版本的 CR 可以通过 API 访问。
func HasServedCRDVersion(crd *apiextensionsv1.CustomResourceDefinition, version string) bool {
	for _, v := range crd.Spec.Versions {
		if v.Name == version {
			return v.Served
		}
	}
	return false
}

// GetCRDStorageVersion 返回 CRD 的存储版本。
// CRD 只能有一个 Storage 版本，所有 CR 在 etcd 中都以该版本存储。
func GetCRDStorageVersion(crd *apiextensionsv1.CustomResourceDefinition) (string, error) {
	for _, v := range crd.Spec.Versions {
		if v.Storage {
			return v.Name, nil
		}
	}
	// This should not happened if crd is valid
	return "", fmt.Errorf("invalid apiextensionsv1.CustomResourceDefinition, no storage version")
}

// IsStoredVersion 检查给定版本是否是 CRD 的已存储版本。
// status.storedVersions 记录 etcd 中实际存在的版本（可能在版本迁移过程中有多个版本共存）。
func IsStoredVersion(crd *apiextensionsv1.CustomResourceDefinition, version string) bool {
	for _, v := range crd.Status.StoredVersions {
		if version == v {
			return true
		}
	}
	return false
}

// GetSchemaForVersion 返回指定版本的验证 schema（OpenAPI v3.0 schema）。
// 如果版本不存在，返回错误。
func GetSchemaForVersion(crd *apiextensionsv1.CustomResourceDefinition, version string) (*apiextensionsv1.CustomResourceValidation, error) {
	for _, v := range crd.Spec.Versions {
		if version == v.Name {
			return v.Schema, nil
		}
	}
	return nil, fmt.Errorf("version %s not found in apiextensionsv1.CustomResourceDefinition: %v", version, crd.Name)
}

// GetSubresourcesForVersion 返回指定版本的子资源配置（如 status、scale）。
func GetSubresourcesForVersion(crd *apiextensionsv1.CustomResourceDefinition, version string) (*apiextensionsv1.CustomResourceSubresources, error) {
	for _, v := range crd.Spec.Versions {
		if version == v.Name {
			return v.Subresources, nil
		}
	}
	return nil, fmt.Errorf("version %s not found in apiextensionsv1.CustomResourceDefinition: %v", version, crd.Name)
}

// HasPerVersionSchema 检查 CRD 是否使用了每个版本独立的 schema。
// 如果任一版本定义了独立 schema，则 CRD 不支持 schema 继承。
func HasPerVersionSchema(versions []apiextensionsv1.CustomResourceDefinitionVersion) bool {
	for _, v := range versions {
		if v.Schema != nil {
			return true
		}
	}
	return false
}

// HasPerVersionSubresources 检查 CRD 是否使用了每个版本独立的子资源配置。
func HasPerVersionSubresources(versions []apiextensionsv1.CustomResourceDefinitionVersion) bool {
	for _, v := range versions {
		if v.Subresources != nil {
			return true
		}
	}
	return false
}

// HasPerVersionColumns 检查 CRD 是否使用了每个版本独立的打印列（AdditionalPrinterColumns）。
func HasPerVersionColumns(versions []apiextensionsv1.CustomResourceDefinitionVersion) bool {
	for _, v := range versions {
		if len(v.AdditionalPrinterColumns) > 0 {
			return true
		}
	}
	return false
}

// HasVersionServed 检查 CRD 是否服务（served）给定版本。
// 与 HasServedCRDVersion 相同，用于不同的调用场景。
func HasVersionServed(crd *apiextensionsv1.CustomResourceDefinition, version string) bool {
	for _, v := range crd.Spec.Versions {
		if !v.Served || v.Name != version {
			continue
		}
		return true
	}
	return false
}
