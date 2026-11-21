/*
Copyright 2020 The Kubernetes Authors.

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

package validation

import (
	"fmt"
	"strings"

	apimachineryvalidation "k8s.io/apimachinery/pkg/api/validation"
	"k8s.io/apimachinery/pkg/util/sets"
	utilvalidation "k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/kubernetes/pkg/apis/apiserverinternal"
	apivalidation "k8s.io/kubernetes/pkg/apis/core/validation"
)

// ValidateStorageVersion 校验 StorageVersion 对象：校验元数据与状态两个层次。
func ValidateStorageVersion(sv *apiserverinternal.StorageVersion) field.ErrorList {
	var allErrs field.ErrorList
	allErrs = append(allErrs, apivalidation.ValidateObjectMeta(&sv.ObjectMeta, false, ValidateStorageVersionName, field.NewPath("metadata"))...)
	allErrs = append(allErrs, validateStorageVersionStatus(sv.Status, field.NewPath("status"))...)
	return allErrs
}

// ValidateStorageVersionName 实现 StorageVersion 名称的 ValidateNameFunc，要求形式为 <group>.<resource>。
func ValidateStorageVersionName(name string, prefix bool) []string {
	var allErrs []string
	idx := strings.LastIndex(name, ".")
	if idx < 0 {
		allErrs = append(allErrs, "name must be in the form of <group>.<resource>")
	} else {
		for _, msg := range utilvalidation.IsDNS1123Subdomain(name[:idx]) {
			allErrs = append(allErrs, "the group segment "+msg)
		}
		for _, msg := range utilvalidation.IsDNS1035Label(name[idx+1:]) {
			allErrs = append(allErrs, "the resource segment "+msg)
		}
	}
	return allErrs
}

// ValidateStorageVersionUpdate 校验 StorageVersion 的更新是否合法。
func ValidateStorageVersionUpdate(sv, oldSV *apiserverinternal.StorageVersion) field.ErrorList {
	// StorageVersionSpec 是空 spec，无可校验内容，因此永远不报错。
	return field.ErrorList{}
}

// ValidateStorageVersionStatusUpdate 校验 StorageVersionStatus 的更新是否合法。
func ValidateStorageVersionStatusUpdate(sv, oldSV *apiserverinternal.StorageVersion) field.ErrorList {
	var allErrs field.ErrorList
	allErrs = append(allErrs, validateStorageVersionStatus(sv.Status, field.NewPath("status"))...)
	return allErrs
}

// 校验 StorageVersionStatus：各 API Server 上报的存储版本需互不重复，并校验公共版本与 condition。
func validateStorageVersionStatus(ss apiserverinternal.StorageVersionStatus, fldPath *field.Path) field.ErrorList {
	var allErrs field.ErrorList
	allAPIServerIDs := sets.New[string]()
	for i, ssv := range ss.StorageVersions {
		if allAPIServerIDs.Has(ssv.APIServerID) {
			allErrs = append(allErrs, field.Duplicate(fldPath.Child("storageVersions").Index(i).Child("apiServerID"), ssv.APIServerID))
		} else {
			allAPIServerIDs.Insert(ssv.APIServerID)
		}
		allErrs = append(allErrs, validateServerStorageVersion(ssv, fldPath.Child("storageVersions").Index(i))...)
	}
	if err := validateCommonVersion(ss, fldPath); err != nil {
		allErrs = append(allErrs, err)
	}
	allErrs = append(allErrs, validateStorageVersionCondition(ss.Conditions, fldPath)...)
	return allErrs
}

// 校验单个 API Server 上报的存储版本：ID 格式、版本格式，以及编码版本必须出现在可解码版本中；每个服务版本也必须可解码。
func validateServerStorageVersion(ssv apiserverinternal.ServerStorageVersion, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	for _, msg := range apimachineryvalidation.NameIsDNSSubdomain(ssv.APIServerID, false) {
		allErrs = append(allErrs, field.Invalid(fldPath.Child("apiServerID"), ssv.APIServerID, msg))
	}
	if errs := isValidAPIVersion(ssv.EncodingVersion); len(errs) > 0 {
		allErrs = append(allErrs, field.Invalid(fldPath.Child("encodingVersion"), ssv.EncodingVersion, strings.Join(errs, ",")))
	}

	found := false
	for i, dv := range ssv.DecodableVersions {
		if errs := isValidAPIVersion(dv); len(errs) > 0 {
			allErrs = append(allErrs, field.Invalid(fldPath.Child("decodableVersions").Index(i), dv, strings.Join(errs, ",")))
		}
		if dv == ssv.EncodingVersion {
			found = true
		}
	}
	if !found {
		allErrs = append(allErrs, field.Invalid(fldPath.Child("decodableVersions"), ssv.DecodableVersions, fmt.Sprintf("decodableVersions must include encodingVersion %s", ssv.EncodingVersion)))
	}

	for i, sv := range ssv.ServedVersions {
		if errs := isValidAPIVersion(sv); len(errs) > 0 {
			allErrs = append(allErrs, field.Invalid(fldPath.Child("servedVersions").Index(i), sv, strings.Join(errs, ",")))
		}
		foundDecodableVersion := false
		for _, dv := range ssv.DecodableVersions {
			if sv == dv {
				foundDecodableVersion = true
				break
			}
		}
		if !foundDecodableVersion {
			allErrs = append(allErrs, field.Invalid(fldPath.Child("servedVersions").Index(i), sv, fmt.Sprintf("individual served version : %s must be included in decodableVersions : %s", sv, ssv.DecodableVersions)))
		}
	}
	return allErrs
}

// 计算所有 API Server 共用的编码版本；若不存在或不一致则返回 nil。
func commonVersion(ssv []apiserverinternal.ServerStorageVersion) *string {
	if len(ssv) == 0 {
		return nil
	}
	commonVersion := ssv[0].EncodingVersion
	for _, v := range ssv[1:] {
		if v.EncodingVersion != commonVersion {
			return nil
		}
	}
	return &commonVersion
}

// 校验用户上报的 commonEncodingVersion 与实际各服务器编码版本的一致性。
func validateCommonVersion(svs apiserverinternal.StorageVersionStatus, fldPath *field.Path) *field.Error {
	actualCommonVersion := commonVersion(svs.StorageVersions)
	if actualCommonVersion == nil && svs.CommonEncodingVersion == nil {
		return nil
	}
	if actualCommonVersion == nil && svs.CommonEncodingVersion != nil {
		return field.Invalid(fldPath.Child("commonEncodingVersion"), *svs.CommonEncodingVersion, "should be nil if servers do not agree on the same encoding version, or if there is no server reporting the supported versions yet")
	}
	if actualCommonVersion != nil && svs.CommonEncodingVersion == nil {
		return field.Invalid(fldPath.Child("commonEncodingVersion"), svs.CommonEncodingVersion, fmt.Sprintf("the common encoding version is %s", *actualCommonVersion))
	}
	if actualCommonVersion != nil && svs.CommonEncodingVersion != nil && *actualCommonVersion != *svs.CommonEncodingVersion {
		return field.Invalid(fldPath.Child("commonEncodingVersion"), *svs.CommonEncodingVersion, fmt.Sprintf("the actual common encoding version is %s", *actualCommonVersion))
	}
	return nil
}

// 校验 Status 中的 condition：类型必须唯一且为合法限定名，reason 与 message 不可为空。
func validateStorageVersionCondition(conditions []apiserverinternal.StorageVersionCondition, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	// 这里不要求 condition 的 type/status 属于预定义取值，
	// 因为将来可能新增更多 type 或 status。
	seenType := make(map[apiserverinternal.StorageVersionConditionType]int)
	for i, condition := range conditions {
		if ii, ok := seenType[condition.Type]; ok {
			allErrs = append(allErrs, field.Invalid(fldPath.Index(i).Child("type"), string(condition.Type),
				fmt.Sprintf("the type of the condition is not unique, it also appears in conditions[%d]", ii)))
		}
		seenType[condition.Type] = i
		allErrs = append(allErrs, apivalidation.ValidateQualifiedName(string(condition.Type), fldPath.Index(i).Child("type"))...)
		allErrs = append(allErrs, apivalidation.ValidateQualifiedName(string(condition.Status), fldPath.Index(i).Child("status"))...)
		if condition.Reason == "" {
			allErrs = append(allErrs, field.Required(fldPath.Index(i).Child("reason"), ""))
		}
		if condition.Message == "" {
			allErrs = append(allErrs, field.Required(fldPath.Index(i).Child("message"), ""))
		}
	}
	return allErrs
}

const dns1035LabelFmt string = "[a-z]([-a-z0-9]*[a-z0-9])?"
const dns1035LabelErrMsg string = "a DNS-1035 label, which must consist of lower case alphanumeric characters or '-', start with an alphabetic character, and end with an alphanumeric character"

// isValidAPIVersion 检查给定值是否为合法的 apiVersion。
// 合法的 apiVersion 包含一个符合 DNS_LABEL 格式的版本字符串，
// 并可带 group/ 前缀，其中 group 需符合 DNS_SUBDOMAIN 格式。
// 若不合法则返回错误信息列表，
// 否则返回空列表或 nil。
func isValidAPIVersion(apiVersion string) []string {
	var errs []string
	parts := strings.Split(apiVersion, "/")
	var version string
	switch len(parts) {
	case 1:
		version = parts[0]
	case 2:
		var group string
		group, version = parts[0], parts[1]
		if len(group) == 0 {
			errs = append(errs, "group part: "+utilvalidation.EmptyError())
		} else if msgs := utilvalidation.IsDNS1123Subdomain(group); len(msgs) != 0 {
			errs = append(errs, prefixEach(msgs, "group part: ")...)
		}
	default:
		return append(errs, "an apiVersion is "+utilvalidation.RegexError(dns1035LabelErrMsg, dns1035LabelFmt, "my-name", "abc-123")+
			" with an optional DNS subdomain prefix and '/' (e.g. 'example.com/MyVersion')")
	}

	if len(version) == 0 {
		errs = append(errs, "version part: "+utilvalidation.EmptyError())
	} else if msgs := utilvalidation.IsDNS1035Label(version); len(msgs) != 0 {
		errs = append(errs, prefixEach(msgs, "version part: ")...)
	}
	return errs
}

// 为每条错误信息加上统一前缀（原地修改），便于定位是 group 部分还是 version 部分出错。
func prefixEach(msgs []string, prefix string) []string {
	for i := range msgs {
		msgs[i] = prefix + msgs[i]
	}
	return msgs
}
