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

// Package apiextensions helpers 提供内部版本 CRD 的辅助函数。
// 与 apihelpers 包不同，这里操作的是内部版本类型（非 v1 版本），
// 支持 per-version schema、subresources 和 columns 的获取逻辑。
package apiextensions

import (
	"fmt"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// swaggerMetadataDescriptions 缓存 ObjectMeta 的 Swagger 文档描述，
// 用于默认打印列（如 Age 列）的描述文本。
var swaggerMetadataDescriptions = metav1.ObjectMeta{}.SwaggerDoc()

// SetCRDCondition 设置内部版本 CRD 的状态条件。如果条件已存在则更新，否则创建新条件。
// LastTransitionTime 仅在条件状态发生变化或之前未设置时更新。
func SetCRDCondition(crd *CustomResourceDefinition, newCondition CustomResourceDefinitionCondition) {
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

// RemoveCRDCondition 从内部版本 CRD 的 status.conditions 中移除指定类型的条件。
func RemoveCRDCondition(crd *CustomResourceDefinition, conditionType CustomResourceDefinitionConditionType) {
	newConditions := []CustomResourceDefinitionCondition{}
	for _, condition := range crd.Status.Conditions {
		if condition.Type != conditionType {
			newConditions = append(newConditions, condition)
		}
	}
	crd.Status.Conditions = newConditions
}

// FindCRDCondition 在内部版本 CRD 的 status.conditions 中查找指定类型的条件，找不到返回 nil。
func FindCRDCondition(crd *CustomResourceDefinition, conditionType CustomResourceDefinitionConditionType) *CustomResourceDefinitionCondition {
	for i := range crd.Status.Conditions {
		if crd.Status.Conditions[i].Type == conditionType {
			return &crd.Status.Conditions[i]
		}
	}

	return nil
}

// IsCRDConditionTrue 检查内部版本 CRD 的指定条件是否存在且为 True。
func IsCRDConditionTrue(crd *CustomResourceDefinition, conditionType CustomResourceDefinitionConditionType) bool {
	return IsCRDConditionPresentAndEqual(crd, conditionType, ConditionTrue)
}

// IsCRDConditionFalse 检查内部版本 CRD 的指定条件是否存在且为 False。
func IsCRDConditionFalse(crd *CustomResourceDefinition, conditionType CustomResourceDefinitionConditionType) bool {
	return IsCRDConditionPresentAndEqual(crd, conditionType, ConditionFalse)
}

// IsCRDConditionPresentAndEqual 检查内部版本 CRD 的指定条件是否存在且状态等于给定值。
func IsCRDConditionPresentAndEqual(crd *CustomResourceDefinition, conditionType CustomResourceDefinitionConditionType, status ConditionStatus) bool {
	for _, condition := range crd.Status.Conditions {
		if condition.Type == conditionType {
			return condition.Status == status
		}
	}
	return false
}

// IsCRDConditionEquivalent 比较两个内部版本条件是否等价（忽略时间字段）。
func IsCRDConditionEquivalent(lhs, rhs *CustomResourceDefinitionCondition) bool {
	if lhs == nil && rhs == nil {
		return true
	}
	if lhs == nil || rhs == nil {
		return false
	}

	return lhs.Message == rhs.Message && lhs.Reason == rhs.Reason && lhs.Status == rhs.Status && lhs.Type == rhs.Type
}

// CRDHasFinalizer 检查内部版本 CRD 是否包含指定的 finalizer。
func CRDHasFinalizer(crd *CustomResourceDefinition, needle string) bool {
	for _, finalizer := range crd.Finalizers {
		if finalizer == needle {
			return true
		}
	}

	return false
}

// CRDRemoveFinalizer 从内部版本 CRD 中移除指定的 finalizer（如果存在）。
func CRDRemoveFinalizer(crd *CustomResourceDefinition, needle string) {
	newFinalizers := []string{}
	for _, finalizer := range crd.Finalizers {
		if finalizer != needle {
			newFinalizers = append(newFinalizers, finalizer)
		}
	}
	crd.Finalizers = newFinalizers
}

// HasServedCRDVersion 检查给定版本是否在内部版本 CRD 中且 Served 标志为 true。
func HasServedCRDVersion(crd *CustomResourceDefinition, version string) bool {
	for _, v := range crd.Spec.Versions {
		if v.Name == version {
			return v.Served
		}
	}
	return false
}

// GetCRDStorageVersion 返回内部版本 CRD 的存储版本。
func GetCRDStorageVersion(crd *CustomResourceDefinition) (string, error) {
	for _, v := range crd.Spec.Versions {
		if v.Storage {
			return v.Name, nil
		}
	}
	// 如果 CRD 已通过完整校验，正常情况下不应进入此分支。
	return "", fmt.Errorf("invalid CustomResourceDefinition, no storage version")
}

// IsStoredVersion 检查给定版本是否是内部版本 CRD 的已存储版本。
func IsStoredVersion(crd *CustomResourceDefinition, version string) bool {
	for _, v := range crd.Status.StoredVersions {
		if version == v {
			return true
		}
	}
	return false
}

// GetSchemaForVersion 返回指定版本的验证 schema。
// 如果 CRD 使用 per-version schema，返回该版本的独立 schema；
// 否则返回顶层共享 schema。顶层和 per-version schema 不能同时存在。
func GetSchemaForVersion(crd *CustomResourceDefinition, version string) (*CustomResourceValidation, error) {
	if !HasPerVersionSchema(crd.Spec.Versions) {
		return crd.Spec.Validation, nil
	}
	if crd.Spec.Validation != nil {
		return nil, fmt.Errorf("malformed CustomResourceDefinition %s version %s: top-level and per-version schemas must be mutual exclusive", crd.Name, version)
	}
	for _, v := range crd.Spec.Versions {
		if version == v.Name {
			return v.Schema, nil
		}
	}
	return nil, fmt.Errorf("version %s not found in CustomResourceDefinition: %v", version, crd.Name)
}

// GetSubresourcesForVersion 返回指定版本的子资源配置（如 status、scale）。
// 顶层和 per-version subresources 不能同时存在。
func GetSubresourcesForVersion(crd *CustomResourceDefinition, version string) (*CustomResourceSubresources, error) {
	if !HasPerVersionSubresources(crd.Spec.Versions) {
		return crd.Spec.Subresources, nil
	}
	if crd.Spec.Subresources != nil {
		return nil, fmt.Errorf("malformed CustomResourceDefinition %s version %s: top-level and per-version subresources must be mutual exclusive", crd.Name, version)
	}
	for _, v := range crd.Spec.Versions {
		if version == v.Name {
			return v.Subresources, nil
		}
	}
	return nil, fmt.Errorf("version %s not found in CustomResourceDefinition: %v", version, crd.Name)
}

// GetColumnsForVersion 返回指定版本的打印列（AdditionalPrinterColumns）。
// 如果 CRD 未定义打印列，返回默认的 Age 列（显示资源创建时间）。
// 注意：返回的默认列不是原始 CRD 对象的引用，不能通过它修改原始 CRD。
func GetColumnsForVersion(crd *CustomResourceDefinition, version string) ([]CustomResourceColumnDefinition, error) {
	if !HasPerVersionColumns(crd.Spec.Versions) {
		return serveDefaultColumnsIfEmpty(crd.Spec.AdditionalPrinterColumns), nil
	}
	if len(crd.Spec.AdditionalPrinterColumns) > 0 {
		return nil, fmt.Errorf("malformed CustomResourceDefinition %s version %s: top-level and per-version additionalPrinterColumns must be mutual exclusive", crd.Name, version)
	}
	for _, v := range crd.Spec.Versions {
		if version == v.Name {
			return serveDefaultColumnsIfEmpty(v.AdditionalPrinterColumns), nil
		}
	}
	return nil, fmt.Errorf("version %s not found in CustomResourceDefinition: %v", version, crd.Name)
}

// HasPerVersionSchema 检查内部版本 CRD 是否使用了每个版本独立的 schema。
func HasPerVersionSchema(versions []CustomResourceDefinitionVersion) bool {
	for _, v := range versions {
		if v.Schema != nil {
			return true
		}
	}
	return false
}

// HasPerVersionSubresources 检查内部版本 CRD 是否使用了每个版本独立的子资源配置。
func HasPerVersionSubresources(versions []CustomResourceDefinitionVersion) bool {
	for _, v := range versions {
		if v.Subresources != nil {
			return true
		}
	}
	return false
}

// HasPerVersionColumns 检查内部版本 CRD 是否使用了每个版本独立的打印列。
func HasPerVersionColumns(versions []CustomResourceDefinitionVersion) bool {
	for _, v := range versions {
		if len(v.AdditionalPrinterColumns) > 0 {
			return true
		}
	}
	return false
}

// serveDefaultColumnsIfEmpty 如果输入列为空，返回默认的 Age 列。
// 默认列显示资源的创建时间，对应 kubectl get 的 AGE 列。
// 返回新切片，不修改原始 CRD 对象。
func serveDefaultColumnsIfEmpty(columns []CustomResourceColumnDefinition) []CustomResourceColumnDefinition {
	if len(columns) > 0 {
		return columns
	}
	return []CustomResourceColumnDefinition{
		{Name: "Age", Type: "date", Description: swaggerMetadataDescriptions["creationTimestamp"], JSONPath: ".metadata.creationTimestamp"},
	}
}

// HasVersionServed 检查内部版本 CRD 是否服务（served）给定版本。
func HasVersionServed(crd *CustomResourceDefinition, version string) bool {
	for _, v := range crd.Spec.Versions {
		if !v.Served || v.Name != version {
			continue
		}
		return true
	}
	return false
}
