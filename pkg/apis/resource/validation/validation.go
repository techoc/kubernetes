/*
Copyright 2022 The Kubernetes Authors.

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
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"regexp"
	"slices"
	"strconv"
	"strings"

	"github.com/google/uuid"
	"gopkg.in/inf.v0"

	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/operation"
	apiresource "k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/api/validate"
	"k8s.io/apimachinery/pkg/api/validate/content"
	apimachineryvalidation "k8s.io/apimachinery/pkg/api/validation"
	metav1validation "k8s.io/apimachinery/pkg/apis/meta/v1/validation"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/apiserver/pkg/cel"
	"k8s.io/apiserver/pkg/cel/environment"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	dracel "k8s.io/dynamic-resource-allocation/cel"
	"k8s.io/dynamic-resource-allocation/structured"
	core "k8s.io/kubernetes/pkg/apis/core"
	corehelper "k8s.io/kubernetes/pkg/apis/core/helper"
	v1helper "k8s.io/kubernetes/pkg/apis/core/v1/helper"
	corevalidation "k8s.io/kubernetes/pkg/apis/core/validation"
	"k8s.io/kubernetes/pkg/apis/resource"
	"k8s.io/kubernetes/pkg/features"
)

// ResourceNormalizationRules 用于处理 v1beta1（扁平化字段）与
// v1/v1beta2（字段位于 'exactly' 之下）之间的结构差异，
// 以便校验错误路径在不同版本间保持一致。
var ResourceNormalizationRules = []field.NormalizationRule{
	{
		// ResourceClaim（资源申领对象）的错误路径 normalization 规则
		Regexp:      regexp.MustCompile(`spec\.devices\.requests\[(\d+)\]\.(deviceClassName|selectors|allocationMode|count|adminAccess|tolerations|derivedAttributes)`),
		Replacement: "spec.devices.requests[$1].exactly.$2",
	},
	{
		// ResourceClaimTemplate（RCT.Spec.Spec 会额外带一层 "spec." 前缀）
		Regexp:      regexp.MustCompile(`spec\.spec\.devices\.requests\[(\d+)\]\.(deviceClassName|selectors|allocationMode|count|adminAccess|tolerations|derivedAttributes)`),
		Replacement: "spec.spec.devices.requests[$1].exactly.$2",
	},
	{
		// 这条 v1beta1 的 'basic' 到扁平化字段的映射规则用于支持 ResourceSlice
		Regexp:      regexp.MustCompile(`spec\.devices\[(\d+)\]\.basic\.`),
		Replacement: "spec.devices[$1].",
	},
}

var (
	// 复用 CSI 驱动名称的校验函数来校验资源驱动名称，
	// 因为二者允许的名称格式完全一致。
	validateDriverName      = corevalidation.ValidateCSIDriverName
	validateDeviceName      = corevalidation.ValidateDNS1123Label
	validateDeviceClassName = corevalidation.ValidateDNS1123Subdomain
	validateRequestName     = corevalidation.ValidateDNS1123Label
	validateCounterName     = corevalidation.ValidateDNS1123Label

	// domain/ID 组合键的最大长度上限（域名最长长度 + '/' + ID 最长长度）
	attributeAndCapacityMaxKeyLength = resource.DeviceMaxDomainLength + 1 + resource.DeviceMaxIDLength
)

func validatePoolName(name string, fldPath *field.Path) field.ErrorList {
	var allErrs field.ErrorList
	if name == "" {
		allErrs = append(allErrs, field.Required(fldPath, ""))
	} else {
		if len(name) > resource.PoolNameMaxLength {
			allErrs = append(allErrs, field.TooLong(fldPath, "" /*unused*/, resource.PoolNameMaxLength).WithOrigin("format=k8s-resource-pool-name"))
		}
		parts := strings.Split(name, "/")
		for _, part := range parts {
			allErrs = append(allErrs, corevalidation.ValidateDNS1123Subdomain(part, fldPath).WithOrigin("format=k8s-resource-pool-name")...)
		}
	}
	return allErrs
}

func validateUID(uid string, fldPath *field.Path) field.ErrorList {
	var allErrs field.ErrorList
	if err := uuid.Validate(uid); err != nil {
		allErrs = append(allErrs, field.Invalid(fldPath, uid, fmt.Sprintf("error validating uid: %v", err)))
	} else if len(uid) != 36 || uid != strings.ToLower(uid) {
		allErrs = append(allErrs, field.Invalid(fldPath, uid, "uid must be in RFC 4122 normalized form, `xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx` with lowercase hexadecimal characters"))
	}
	return allErrs.WithOrigin("format=k8s-uuid")
}

// ValidateResourceClaim 校验一个 ResourceClaim（资源申领）对象的合法性。
// 依次校验对象元数据（名称必须合法、必须带命名空间）以及 devices（设备申领）规格。
func ValidateResourceClaim(resourceClaim *resource.ResourceClaim) field.ErrorList {
	allErrs := corevalidation.ValidateObjectMeta(&resourceClaim.ObjectMeta, true, corevalidation.ValidateResourceClaimName, field.NewPath("metadata"))
	allErrs = append(allErrs, validateResourceClaimSpec(&resourceClaim.Spec, field.NewPath("spec"), false)...)
	return allErrs
}

// ValidateResourceClaimUpdate 判断对 ResourceClaim 的更新是否合法。
// 元数据仍会完整校验；spec 是不可变字段，因此更新时只检查其是否被修改。
func ValidateResourceClaimUpdate(resourceClaim, oldClaim *resource.ResourceClaim) field.ErrorList {
	allErrs := corevalidation.ValidateObjectMeta(&resourceClaim.ObjectMeta, true, corevalidation.ValidateResourceClaimName, field.NewPath("metadata"))
	allErrs = append(allErrs, corevalidation.ValidateObjectMetaUpdate(&resourceClaim.ObjectMeta, &oldClaim.ObjectMeta, field.NewPath("metadata"))...)
	// spec 是不可变字段。更新时只需要检查不可变性：
	// 其他字段用户无法修改，跳过重复校验，只保留对违反不可变性这一可操作错误的检查。
	allErrs = append(allErrs, apimachineryvalidation.ValidateImmutableField(resourceClaim.Spec, oldClaim.Spec, field.NewPath("spec")).WithOrigin("immutable").MarkCoveredByDeclarative()...)
	return allErrs
}

// ValidateResourceClaimStatusUpdate 判断对 ResourceClaim 状态（status）的更新是否合法。
// 除了元数据更新校验外，还会根据 spec 中的请求名称集合来校验状态中的预留、分配结果等。
func ValidateResourceClaimStatusUpdate(resourceClaim, oldClaim *resource.ResourceClaim) field.ErrorList {
	allErrs := corevalidation.ValidateObjectMetaUpdate(&resourceClaim.ObjectMeta, &oldClaim.ObjectMeta, field.NewPath("metadata"))
	requestNames := gatherRequestNames(&resourceClaim.Spec.Devices)
	allErrs = append(allErrs, validateResourceClaimStatusUpdate(&resourceClaim.Status, &oldClaim.Status, resourceClaim.DeletionTimestamp != nil, requestNames, field.NewPath("status"))...)
	return allErrs
}

func validateResourceClaimSpec(spec *resource.ResourceClaimSpec, fldPath *field.Path, stored bool) field.ErrorList {
	allErrs := field.ErrorList{}
	allErrs = append(allErrs, validateDeviceClaim(&spec.Devices, fldPath.Child("devices"), stored)...)
	return allErrs
}

// deviceValidationOptions 保存校验 DeviceClaim（设备申领）时使用的选项和共享状态
// （例如累计器、约束引用关系），供各个子校验函数复用。
type deviceValidationOptions struct {
	// stored 表示被校验的对象是否已经存在于 API Server 中。
	// 若为 true，某些此前已通过校验的字段（如 CEL 表达式）无需重新求值即视为合法，
	// 这样可以在更新时放宽校验（ratcheting），避免旧对象因规则收紧而无法更新。
	stored bool

	// totalDerivedAttrCost 累计整个 DeviceClaim 中所有派生属性（derived attribute）
	// CEL 表达式的编译成本，确保它们的总成本不超过允许的预算上限。
	totalDerivedAttrCost *uint64

	// constraintAttributes 将请求/子请求引用（"req" 或 "req/subreq"）映射到
	// 指向该请求的约束所引用的属性名集合：
	// - 属性名出现在 "" 键下：表示对所有请求生效（全局约束）。
	// - 属性名出现在 "req" 键下：表示对 "req" 的所有子请求生效。
	// - 属性名出现在 "req/subreq" 键下：表示只对特定子请求生效。
	// 该映射用于判断某个派生属性是否真正被约束引用，避免无用的表达式。
	constraintAttributes map[string]sets.Set[string]
}

func gatherConstraintAttributes(constraints []resource.DeviceConstraint) map[string]sets.Set[string] {
	attrs := make(map[string]sets.Set[string])
	for _, constraint := range constraints {
		var attrName string
		if constraint.MatchAttribute != nil {
			attrName = string(*constraint.MatchAttribute)
		} else if constraint.DistinctAttribute != nil {
			attrName = string(*constraint.DistinctAttribute)
		} else {
			continue
		}

		if len(constraint.Requests) == 0 {
			// requests 列表为空的约束是全局约束，对所有请求生效，
			// 因此用空字符串作为键来代表“所有请求”。
			if attrs[""] == nil {
				attrs[""] = sets.New[string]()
			}
			attrs[""].Insert(attrName)
		} else {
			// 一个约束可能同时指向多个显式请求或子请求，
			// 因此需要为每个目标单独记录其使用的属性。
			for _, req := range constraint.Requests {
				if attrs[req] == nil {
					attrs[req] = sets.New[string]()
				}
				attrs[req].Insert(attrName)
			}
		}
	}
	return attrs
}

func validateDeviceClaim(deviceClaim *resource.DeviceClaim, fldPath *field.Path, stored bool) field.ErrorList {
	allErrs := field.ErrorList{}
	requestNames := gatherRequestNames(deviceClaim)

	var totalDerivedAttrCost uint64
	opts := deviceValidationOptions{
		stored:               stored,
		totalDerivedAttrCost: &totalDerivedAttrCost,
		constraintAttributes: gatherConstraintAttributes(deviceClaim.Constraints),
	}
	allErrs = append(allErrs, validateSet(deviceClaim.Requests, resource.DeviceRequestsMaxSize,
		func(request resource.DeviceRequest, fldPath *field.Path) field.ErrorList {
			return validateDeviceRequest(request, fldPath, opts)
		},
		func(request resource.DeviceRequest) string {
			return request.Name
		},
		fldPath.Child("requests"), sizeCovered, uniquenessCovered)...)
	allErrs = append(allErrs, validateSlice(deviceClaim.Constraints, resource.DeviceConstraintsMaxSize,
		func(constraint resource.DeviceConstraint, fldPath *field.Path) field.ErrorList {
			return validateDeviceConstraint(constraint, fldPath, requestNames)
		}, fldPath.Child("constraints"), sizeCovered)...)
	allErrs = append(allErrs, validateSlice(deviceClaim.Config, resource.DeviceConfigMaxSize,
		func(config resource.DeviceClaimConfiguration, fldPath *field.Path) field.ErrorList {
			return validateDeviceClaimConfiguration(config, fldPath, requestNames, stored)
		}, fldPath.Child("config"), sizeCovered)...)
	if totalDerivedAttrCost > resource.DeviceClaimDerivedAttributeCELMaxCost {
		allErrs = append(allErrs, field.Forbidden(fldPath, "too complex, total cost of derived attribute CEL expressions in the claim exceeds cost limit"))
	}

	return allErrs
}

type requestNames map[string]sets.Set[string]

func (r requestNames) Has(s string) bool {
	segments := strings.Split(s, "/")
	// 如果字符串中包含多个 '/'（即分段超过两个），
	// 则不可能与任何请求名匹配，直接返回 false。
	if len(segments) > 2 {
		return false
	}
	// 如果第一段（请求名）就没有匹配项，
	// 则无需再检查第二段（子请求名）。
	subRequestNames, found := r[segments[0]]
	if !found {
		return false
	}
	if len(segments) == 1 {
		return true
	}
	// 如果第一段已经匹配且还存在第二段，
	// 则继续检查第二段（子请求名）是否匹配。
	return subRequestNames.Has(segments[1])
}

func gatherRequestNames(deviceClaim *resource.DeviceClaim) requestNames {
	requestNames := make(requestNames)
	for _, request := range deviceClaim.Requests {
		if len(request.FirstAvailable) == 0 {
			requestNames[request.Name] = nil
			continue
		}
		subRequestNames := sets.New[string]()
		for _, subRequest := range request.FirstAvailable {
			subRequestNames.Insert(subRequest.Name)
		}
		requestNames[request.Name] = subRequestNames
	}
	return requestNames
}

func gatherAllocatedDevices(allocationResult *resource.DeviceAllocationResult) sets.Set[structured.SharedDeviceID] {
	allocatedDevices := sets.New[structured.SharedDeviceID]()
	for _, result := range allocationResult.Results {
		deviceName := result.Device
		deviceID := structured.MakeDeviceID(result.Driver, result.Pool, deviceName)
		sharedDeviceID := structured.MakeSharedDeviceID(deviceID, result.ShareID)
		allocatedDevices.Insert(sharedDeviceID)
	}
	return allocatedDevices
}

func validateDeviceRequest(request resource.DeviceRequest, fldPath *field.Path, opts deviceValidationOptions) field.ErrorList {
	allErrs := validateRequestName(request.Name, fldPath.Child("name"))
	numDeviceRequestType := 0

	hasFirstAvailable := len(request.FirstAvailable) > 0
	hasExactly := request.Exactly != nil

	if hasFirstAvailable {
		numDeviceRequestType++
	}
	if hasExactly {
		numDeviceRequestType++
	}

	switch {
	case numDeviceRequestType == 0:
		allErrs = append(allErrs, field.Required(fldPath, "exactly one of `exactly` or `firstAvailable` is required"))
	case numDeviceRequestType > 1:
		allErrs = append(allErrs, field.Invalid(fldPath, nil, "exactly one of `exactly` or `firstAvailable` is required, but multiple fields are set"))
	case hasFirstAvailable:
		allErrs = append(allErrs, validateSet(request.FirstAvailable, resource.FirstAvailableDeviceRequestMaxSize,
			func(subRequest resource.DeviceSubRequest, fldPath *field.Path) field.ErrorList {
				return validateDeviceSubRequest(subRequest, fldPath, opts, request.Name)
			},
			func(subRequest resource.DeviceSubRequest) string {
				return subRequest.Name
			},
			fldPath.Child("firstAvailable"), sizeCovered, uniquenessCovered)...)
	case hasExactly:
		allErrs = append(allErrs, validateExactDeviceRequest(*request.Exactly, fldPath.Child("exactly"), opts, request.Name)...)
	}
	return allErrs
}

func validateDeviceSubRequest(subRequest resource.DeviceSubRequest, fldPath *field.Path, opts deviceValidationOptions, parentRequestName string) field.ErrorList {
	allErrs := validateRequestName(subRequest.Name, fldPath.Child("name"))
	allErrs = append(allErrs, validateDeviceClass(subRequest.DeviceClassName, fldPath.Child("deviceClassName")).MarkCoveredByDeclarative()...)
	allErrs = append(allErrs, validateSelectorSlice(subRequest.Selectors, fldPath.Child("selectors"), opts)...)
	allErrs = append(allErrs, validateDeviceDerivedAttributes(subRequest.DerivedAttributes, fldPath.Child("derivedAttributes"), opts, parentRequestName+"/"+subRequest.Name)...)
	allErrs = append(allErrs, validateDeviceAllocationMode(subRequest.AllocationMode, subRequest.Count, fldPath.Child("allocationMode"), fldPath.Child("count"))...)
	for i, toleration := range subRequest.Tolerations {
		allErrs = append(allErrs, validateDeviceToleration(toleration, fldPath.Child("tolerations").Index(i))...)
	}
	return allErrs
}

func validateExactDeviceRequest(request resource.ExactDeviceRequest, fldPath *field.Path, opts deviceValidationOptions, requestName string) field.ErrorList {
	var allErrs field.ErrorList
	allErrs = append(allErrs, validateDeviceClass(request.DeviceClassName, fldPath.Child("deviceClassName"))...)
	allErrs = append(allErrs, validateSelectorSlice(request.Selectors, fldPath.Child("selectors"), opts)...)
	allErrs = append(allErrs, validateDeviceDerivedAttributes(request.DerivedAttributes, fldPath.Child("derivedAttributes"), opts, requestName)...)
	allErrs = append(allErrs, validateDeviceAllocationMode(request.AllocationMode, request.Count, fldPath.Child("allocationMode"), fldPath.Child("count"))...)
	for i, toleration := range request.Tolerations {
		allErrs = append(allErrs, validateDeviceToleration(toleration, fldPath.Child("tolerations").Index(i))...)
	}
	return allErrs
}

func validateDeviceAllocationMode(deviceAllocationMode resource.DeviceAllocationMode, count int64, allocModeFldPath, countFldPath *field.Path) field.ErrorList {
	var allErrs field.ErrorList
	switch deviceAllocationMode {
	case resource.DeviceAllocationModeAll:
		if count != 0 {
			allErrs = append(allErrs, field.Invalid(countFldPath, count, fmt.Sprintf("must not be specified when allocationMode is '%s'", deviceAllocationMode)))
		}
	case resource.DeviceAllocationModeExactCount:
		if count <= 0 {
			allErrs = append(allErrs, field.Invalid(countFldPath, count, "must be greater than zero"))
		}
	default:
		// 注意：声明式校验目前尚未正确强制该字段的必填语义，
		// 因为 v1beta1 使用了一种特殊的 union 形式：该字段在某些情况下确实可选，
		// 在另一些情况下则确实必填。在该问题被解决之前，请勿移除这段代码。
		allErrs = append(allErrs, field.NotSupported(allocModeFldPath, deviceAllocationMode, []resource.DeviceAllocationMode{resource.DeviceAllocationModeAll, resource.DeviceAllocationModeExactCount}).MarkCoveredByDeclarative())
	}
	return allErrs
}

func validateDeviceClass(deviceClass string, fldPath *field.Path) field.ErrorList {
	var allErrs field.ErrorList
	if deviceClass == "" {
		allErrs = append(allErrs, field.Required(fldPath, ""))
	} else {
		allErrs = append(allErrs, validateDeviceClassName(deviceClass, fldPath)...)
	}
	return allErrs
}

func validateSelectorSlice(selectors []resource.DeviceSelector, fldPath *field.Path, opts deviceValidationOptions) field.ErrorList {
	return validateSlice(selectors, resource.DeviceSelectorsMaxSize,
		func(selector resource.DeviceSelector, fldPath *field.Path) field.ErrorList {
			return validateSelector(selector, fldPath, opts)
		},
		fldPath, sizeCovered)
}

func validateSelector(selector resource.DeviceSelector, fldPath *field.Path, opts deviceValidationOptions) field.ErrorList {
	var allErrs field.ErrorList
	if selector.CEL == nil {
		allErrs = append(allErrs, field.Required(fldPath.Child("cel"), ""))
	} else {
		allErrs = append(allErrs, validateCELExpression(selector.CEL.Expression, fldPath.Child("cel", "expression"), opts, false)...)
	}
	return allErrs
}

func validateCELExpression(expression string, fldPath *field.Path, opts deviceValidationOptions, derivedAttribute bool) field.ErrorList {
	if opts.stored {
		// 已经存储的 CEL 表达式在写入时必然是合法的，因此现在也一定合法。
		// 这里提前返回可以跳过编译，既更廉价，也能在不依赖 ratcheting 的情况下
		// 更快地放宽校验规则。
		return nil
	}
	envType := environment.NewExpressions

	var allErrs field.ErrorList
	if len(expression) > resource.CELSelectorExpressionMaxLength {
		allErrs = append(allErrs, field.TooLong(fldPath, "" /*unused*/, resource.CELSelectorExpressionMaxLength))
		// 表达式过长时直接返回，不再尝试编译，避免浪费 CPU 资源。
		return allErrs
	}

	result := dracel.GetCompiler(dracel.Features{
		EnableConsumableCapacity: utilfeature.DefaultFeatureGate.Enabled(features.DRAConsumableCapacity),
		EnableListTypeAttributes: utilfeature.DefaultFeatureGate.Enabled(features.DRAListTypeAttributes),
	}).CompileCELExpression(expression, dracel.Options{
		EnvType:          &envType,
		DerivedAttribute: derivedAttribute,
	})
	if result.Error != nil {
		allErrs = append(allErrs, convertCELErrorToValidationError(fldPath, expression, result.Error))
	} else {
		if derivedAttribute {
			if result.MaxCost > min(resource.CELSelectorExpressionMaxCost, resource.DeviceClaimDerivedAttributeCELMaxCost) {
				allErrs = append(allErrs, field.Forbidden(fldPath, "too complex, exceeds cost limit"))
			}
			if opts.totalDerivedAttrCost != nil {
				*opts.totalDerivedAttrCost += result.MaxCost
			}
		} else if result.MaxCost > resource.CELSelectorExpressionMaxCost {
			allErrs = append(allErrs, field.Forbidden(fldPath, "too complex, exceeds cost limit"))
		}
	}

	return allErrs
}

func convertCELErrorToValidationError(fldPath *field.Path, expression string, err error) *field.Error {
	var celErr *cel.Error
	if errors.As(err, &celErr) {
		switch celErr.Type {
		case cel.ErrorTypeRequired:
			return field.Required(fldPath, celErr.Detail)
		case cel.ErrorTypeInvalid:
			return field.Invalid(fldPath, expression, celErr.Detail)
		case cel.ErrorTypeInternal:
			return field.InternalError(fldPath, celErr)
		}
	}
	return field.InternalError(fldPath, fmt.Errorf("unsupported error type: %w", err))
}

func isAttributeUsedByConstraint(attrName, reqName string, constraintAttributes map[string]sets.Set[string]) bool {
	// 检查该属性是否被全局约束（不指定具体请求的约束）使用。
	if globalAttrs, ok := constraintAttributes[""]; ok && globalAttrs.Has(attrName) {
		return true
	}
	// 检查该属性是否被明确指向当前请求或子请求的约束使用。
	if reqAttrs, ok := constraintAttributes[reqName]; ok && reqAttrs.Has(attrName) {
		return true
	}
	// 如果当前对象是子请求（如 "req/subreq"），还需检查属性是否被指向父请求
	// （"req"）的约束使用，因为作用于父请求的约束对其所有子请求都生效。
	if parentReqName, _, found := strings.Cut(reqName, "/"); found {
		if parentAttrs, ok := constraintAttributes[parentReqName]; ok && parentAttrs.Has(attrName) {
			return true
		}
	}
	return false
}

func validateDeviceDerivedAttributes(attrs []resource.DeviceDerivedAttribute, fldPath *field.Path, opts deviceValidationOptions, requestName string) field.ErrorList {
	return validateSet(attrs, -1,
		func(attr resource.DeviceDerivedAttribute, fldPath *field.Path) field.ErrorList {
			return validateDeviceDerivedAttribute(attr, fldPath, opts, requestName)
		},
		func(attr resource.DeviceDerivedAttribute) string {
			return string(attr.Name)
		},
		fldPath)
}

func validateDeviceDerivedAttribute(attr resource.DeviceDerivedAttribute, fldPath *field.Path, opts deviceValidationOptions, requestName string) field.ErrorList {
	var allErrs field.ErrorList

	// 非空值已由声明式校验负责检查，此处无需重复校验。
	if attr.Name != "" {
		if !isAttributeUsedByConstraint(string(attr.Name), requestName, opts.constraintAttributes) {
			allErrs = append(allErrs, field.Invalid(fldPath.Child("name"), attr.Name, "must be referenced by at least one matchAttribute or distinctAttribute constraint targeting this request"))
		}
	}
	// 非空值已由声明式校验负责检查，此处无需重复校验。
	if attr.Expression != "" {
		allErrs = append(allErrs, validateCELExpression(attr.Expression, fldPath.Child("expression"), opts, true)...)
	}
	return allErrs
}

func validateDeviceConstraint(constraint resource.DeviceConstraint, fldPath *field.Path, requestNames requestNames) field.ErrorList {
	var allErrs field.ErrorList
	allErrs = append(allErrs, validateSet(constraint.Requests, resource.DeviceRequestsMaxSize,
		func(name string, fldPath *field.Path) field.ErrorList {
			return validateRequestNameRef(name, fldPath, requestNames)
		},
		stringKey, fldPath.Child("requests"), sizeCovered, uniquenessCovered)...)
	if constraint.MatchAttribute != nil {
		allErrs = append(allErrs, validateFullyQualifiedName(*constraint.MatchAttribute, fldPath.Child("matchAttribute")).MarkCoveredByDeclarative()...)
	} else if constraint.DistinctAttribute != nil {
		allErrs = append(allErrs, validateFullyQualifiedName(*constraint.DistinctAttribute, fldPath.Child("distinctAttribute")).MarkCoveredByDeclarative()...)
	} else if utilfeature.DefaultFeatureGate.Enabled(features.DRAConsumableCapacity) {
		allErrs = append(allErrs, field.Required(fldPath, `exactly one of "matchAttribute" or "distinctAttribute" is required, but multiple fields are set`))
	} else {
		allErrs = append(allErrs, field.Required(fldPath.Child("matchAttribute"), ""))
	}
	return allErrs
}

func validateDeviceClaimConfiguration(config resource.DeviceClaimConfiguration, fldPath *field.Path, requestNames requestNames, stored bool) field.ErrorList {
	var allErrs field.ErrorList
	allErrs = append(allErrs, validateSet(config.Requests, resource.DeviceRequestsMaxSize,
		func(name string, fldPath *field.Path) field.ErrorList {
			return validateRequestNameRef(name, fldPath, requestNames)
		}, stringKey, fldPath.Child("requests"), sizeCovered, uniquenessCovered)...)
	allErrs = append(allErrs, validateDeviceConfiguration(config.DeviceConfiguration, fldPath, stored)...)
	return allErrs
}

func validateRequestNameRef(name string, fldPath *field.Path, requestNames requestNames) field.ErrorList {
	var allErrs field.ErrorList
	segments := strings.Split(name, "/")
	if len(segments) > 2 {
		allErrs = append(allErrs, field.Invalid(fldPath, name, "must be the name of a request in the claim or the name of a request and a subrequest separated by '/'"))
		return allErrs
	}

	for i := range segments {
		allErrs = append(allErrs, validateRequestName(segments[i], fldPath)...)
	}

	if !requestNames.Has(name) {
		allErrs = append(allErrs, field.Invalid(fldPath, name, "must be the name of a request in the claim or the name of a request and a subrequest separated by '/'"))
	}
	return allErrs
}

func validateDeviceConfiguration(config resource.DeviceConfiguration, fldPath *field.Path, stored bool) field.ErrorList {
	var allErrs field.ErrorList
	if config.Opaque == nil {
		allErrs = append(allErrs, field.Required(fldPath.Child("opaque"), ""))
	} else {
		allErrs = append(allErrs, validateOpaqueConfiguration(*config.Opaque, fldPath.Child("opaque"), stored)...)
	}
	return allErrs
}

func validateOpaqueConfiguration(config resource.OpaqueDeviceConfiguration, fldPath *field.Path, stored bool) field.ErrorList {
	var allErrs field.ErrorList
	allErrs = append(allErrs, validateDriverName(config.Driver, fldPath.Child("driver"), corevalidation.RequiredCovered, corevalidation.FormatCovered, corevalidation.SizeCovered)...)
	allErrs = append(allErrs, validateRawExtension(config.Parameters, fldPath.Child("parameters"), stored, resource.OpaqueParametersMaxLength)...)
	return allErrs
}

func validateResourceClaimStatusUpdate(status, oldStatus *resource.ResourceClaimStatus, claimDeleted bool, requestNames requestNames, fldPath *field.Path) field.ErrorList {
	var allErrs field.ErrorList
	allErrs = append(allErrs, validateSet(status.ReservedFor, resource.ResourceClaimReservedForMaxSize,
		validateResourceClaimUserReference,
		func(consumer resource.ResourceClaimConsumerReference) types.UID { return consumer.UID },
		fldPath.Child("reservedFor"), sizeCovered, uniquenessCovered)...)

	var allocatedDevices sets.Set[structured.SharedDeviceID]
	if status.Allocation != nil {
		allocatedDevices = gatherAllocatedDevices(&status.Allocation.Devices)
	}
	allErrs = append(allErrs, validateSet(status.Devices, -1,
		func(device resource.AllocatedDeviceStatus, fldPath *field.Path) field.ErrorList {
			return validateDeviceStatus(device, fldPath, allocatedDevices)
		},
		func(device resource.AllocatedDeviceStatus) structured.SharedDeviceID {
			deviceID := structured.MakeDeviceID(device.Driver, device.Pool, device.Device)
			return structured.MakeSharedDeviceID(deviceID, (*types.UID)(device.ShareID))
		},
		fldPath.Child("devices"), uniquenessCovered)...)

	// 接下来检查 ResourceClaim 必须满足的不变量（跨字段约束）。
	if len(status.ReservedFor) > 0 {
		if status.Allocation == nil {
			allErrs = append(allErrs, field.Forbidden(fldPath.Child("reservedFor"), "may not be specified when `allocated` is not set"))
		} else {
			// 当资源申领等待释放时，允许从 ReservedFor 中移除条目，
			// 但不允许新增条目，防止在释放过程中引入新的消费者。
			if claimDeleted {
				oldSet := sets.New(oldStatus.ReservedFor...)
				newSet := sets.New(status.ReservedFor...)
				newItems := newSet.Difference(oldSet)
				if len(newItems) > 0 {
					allErrs = append(allErrs, field.Forbidden(fldPath.Child("reservedFor"), "new entries may not be added while `deallocationRequested` or `deletionTimestamp` are set"))
				}
			}
		}
	}

	// 已经有内容的 status.Allocation 不允许更新。
	// 未修改的字段无需再次校验；在这个特定场景下更不能再校验，
	// 因为针对新分配结果的校验规则比历史上更严格，重新校验会误伤旧数据。
	if oldStatus.Allocation != nil && status.Allocation != nil {
		allErrs = append(allErrs, apimachineryvalidation.ValidateImmutableField(status.Allocation, oldStatus.Allocation, fldPath.Child("allocation")).WithOrigin("update").MarkCoveredByDeclarative()...)
	} else if status.Allocation != nil {
		allErrs = append(allErrs, validateAllocationResult(status.Allocation, fldPath.Child("allocation"), requestNames, false)...)
	}

	return allErrs
}

func validateResourceClaimUserReference(ref resource.ResourceClaimConsumerReference, fldPath *field.Path) field.ErrorList {
	var allErrs field.ErrorList
	if ref.Resource == "" {
		allErrs = append(allErrs, field.Required(fldPath.Child("resource"), ""))
	}
	if ref.Name == "" {
		allErrs = append(allErrs, field.Required(fldPath.Child("name"), ""))
	}
	if ref.UID == "" {
		allErrs = append(allErrs, field.Required(fldPath.Child("uid"), ""))
	}
	return allErrs
}

// validateAllocationResult 只针对**新**的分配结果执行校验，
// 其中至少有一条规则（adminAccess 管理员访问权限）比历史上更严格。
// 因此不得用它重新校验此前已存储的结果，否则会破坏既有对象。
func validateAllocationResult(allocation *resource.AllocationResult, fldPath *field.Path, requestNames requestNames, stored bool) field.ErrorList {
	var allErrs field.ErrorList
	allErrs = append(allErrs, validateDeviceAllocationResult(allocation.Devices, fldPath.Child("devices"), requestNames, stored)...)
	if allocation.NodeSelector != nil {
		allErrs = append(allErrs, corevalidation.ValidateNodeSelector(allocation.NodeSelector, false, fldPath.Child("nodeSelector"))...)
	}
	return allErrs
}

func validateDeviceAllocationResult(allocation resource.DeviceAllocationResult, fldPath *field.Path, requestNames requestNames, stored bool) field.ErrorList {
	var allErrs field.ErrorList
	allErrs = append(allErrs, validateSlice(allocation.Results, resource.AllocationResultsMaxSize,
		func(result resource.DeviceRequestAllocationResult, fldPath *field.Path) field.ErrorList {
			return validateDeviceRequestAllocationResult(result, fldPath, requestNames)
		}, fldPath.Child("results"), sizeCovered)...)
	allErrs = append(allErrs, validateSlice(allocation.Config, 2*resource.DeviceConfigMaxSize, /* class + claim */
		func(config resource.DeviceAllocationConfiguration, fldPath *field.Path) field.ErrorList {
			return validateDeviceAllocationConfiguration(config, fldPath, requestNames, stored)
		}, fldPath.Child("config"), sizeCovered)...)

	return allErrs
}

func validateDeviceRequestAllocationResult(result resource.DeviceRequestAllocationResult, fldPath *field.Path, requestNames requestNames) field.ErrorList {
	var allErrs field.ErrorList
	allErrs = append(allErrs, validateRequestNameRef(result.Request, fldPath.Child("request"), requestNames)...)
	allErrs = append(allErrs, validateDriverName(result.Driver, fldPath.Child("driver"), corevalidation.RequiredCovered, corevalidation.FormatCovered, corevalidation.SizeCovered)...)
	allErrs = append(allErrs, validatePoolName(result.Pool, fldPath.Child("pool")).MarkCoveredByDeclarative()...)
	allErrs = append(allErrs, validateDeviceName(result.Device, fldPath.Child("device"))...)
	allErrs = append(allErrs, validateDeviceBindingParameters(result.BindingConditions, result.BindingFailureConditions, fldPath)...)
	for i, toleration := range result.Tolerations {
		allErrs = append(allErrs, validateDeviceToleration(toleration, fldPath.Child("tolerations").Index(i))...)
	}
	if result.ShareID != nil {
		allErrs = append(allErrs, validateUID(string(*result.ShareID), fldPath.Child("shareID")).MarkCoveredByDeclarative()...)
	}
	allErrs = append(allErrs, validateSkipNodeOperations(result.SkipNodeOperations, fldPath.Child("skipNodeOperations"))...)
	return allErrs
}

func validateDeviceAllocationConfiguration(config resource.DeviceAllocationConfiguration, fldPath *field.Path, requestNames requestNames, stored bool) field.ErrorList {
	var allErrs field.ErrorList
	allErrs = append(allErrs, validateAllocationConfigSource(config.Source, fldPath.Child("source"))...)
	allErrs = append(allErrs, validateSet(config.Requests, resource.DeviceRequestsMaxSize,
		func(name string, fldPath *field.Path) field.ErrorList {
			return validateRequestNameRef(name, fldPath, requestNames)
		}, stringKey, fldPath.Child("requests"), sizeCovered, uniquenessCovered)...)
	allErrs = append(allErrs, validateDeviceConfiguration(config.DeviceConfiguration, fldPath, stored)...)
	return allErrs
}

func validateAllocationConfigSource(source resource.AllocationConfigSource, fldPath *field.Path) field.ErrorList {
	var allErrs field.ErrorList
	switch source {
	case "":
		allErrs = append(allErrs, field.Required(fldPath, "").MarkCoveredByDeclarative())
	case resource.AllocationConfigSourceClaim, resource.AllocationConfigSourceClass:
	default:
		allErrs = append(allErrs, field.NotSupported(fldPath, source, []resource.AllocationConfigSource{resource.AllocationConfigSourceClaim, resource.AllocationConfigSourceClass}).MarkCoveredByDeclarative())
	}
	return allErrs
}

// ValidateDeviceClass 校验一个 DeviceClass（设备类）对象的合法性。
// 包括元数据（名称使用长名称规则）以及选择器、配置、扩展资源名等规格字段。
func ValidateDeviceClass(class *resource.DeviceClass) field.ErrorList {
	validateClassName := func(fldPath *field.Path, name string) field.ErrorList {
		// validate.LongName 目前不区分操作类型（CREATE 还是 UPDATE），
		// 因此这里传入空的 operation.Operation{} 是安全的。
		return validate.LongName(context.Background(), operation.Operation{}, fldPath, &name, nil).MarkCoveredByDeclarative()
	}
	allErrs := corevalidation.ValidateObjectMetaWithOpts(&class.ObjectMeta, false, validateClassName, field.NewPath("metadata"))
	allErrs = append(allErrs, validateDeviceClassSpec(&class.Spec, nil, field.NewPath("spec"))...)
	return allErrs
}

// ValidateDeviceClassUpdate 判断对 DeviceClass 的更新是否合法。
// 会重新校验元数据和规格，其中未变化的选择器/配置被视为“已存储”以免重复校验 CEL。
func ValidateDeviceClassUpdate(class, oldClass *resource.DeviceClass) field.ErrorList {
	validateClassName := func(fldPath *field.Path, name string) field.ErrorList {
		return validate.LongName(context.Background(), operation.Operation{}, fldPath, &name, nil).MarkCoveredByDeclarative()
	}
	// TODO(lalitc375): Remove this if decided in https://github.com/kubernetes/kubernetes/issues/134444.
	allErrs := corevalidation.ValidateObjectMetaWithOpts(&class.ObjectMeta, false, validateClassName, field.NewPath("metadata"))
	allErrs = append(allErrs, corevalidation.ValidateObjectMetaUpdate(&class.ObjectMeta, &oldClass.ObjectMeta, field.NewPath("metadata"))...)
	allErrs = append(allErrs, validateDeviceClassSpec(&class.Spec, &oldClass.Spec, field.NewPath("spec"))...)
	return allErrs
}

func validateDeviceClassSpec(spec, oldSpec *resource.DeviceClassSpec, fldPath *field.Path) field.ErrorList {
	var allErrs field.ErrorList
	// 如果选择器与旧值完全一致，则其中的 CEL 表达式被视为“已存储”而跳过重新编译。
	// 任何变动（即使只是选择器顺序变化）都会触发按新表达式进行的完整校验。
	stored := false
	if oldSpec != nil {
		stored = apiequality.Semantic.DeepEqual(spec.Selectors, oldSpec.Selectors)
	}
	allErrs = append(allErrs, validateSlice(spec.Selectors, resource.DeviceSelectorsMaxSize,
		func(selector resource.DeviceSelector, fldPath *field.Path) field.ErrorList {
			return validateSelector(selector, fldPath, deviceValidationOptions{stored: stored})
		},
		fldPath.Child("selectors"), sizeCovered)...)
	// 配置的校验采用与上面选择器相同的“未变化即视为已存储”逻辑。
	stored = false
	if oldSpec != nil {
		stored = apiequality.Semantic.DeepEqual(spec.Config, oldSpec.Config)
	}
	allErrs = append(allErrs, validateSlice(spec.Config, resource.DeviceConfigMaxSize,
		func(config resource.DeviceClassConfiguration, fldPath *field.Path) field.ErrorList {
			return validateDeviceClassConfiguration(config, fldPath, stored)
		},
		fldPath.Child("config"), sizeCovered)...)
	if spec.ExtendedResourceName != nil && !v1helper.IsExtendedResourceName(corev1.ResourceName(*spec.ExtendedResourceName)) {
		allErrs = append(allErrs, field.Invalid(fldPath.Child("extendedResourceName"), *spec.ExtendedResourceName,
			"must be a valid extended resource name").MarkCoveredByDeclarative().WithOrigin("format=k8s-extended-resource-name"))
	}
	return allErrs
}

func validateDeviceClassConfiguration(config resource.DeviceClassConfiguration, fldPath *field.Path, stored bool) field.ErrorList {
	return validateDeviceConfiguration(config.DeviceConfiguration, fldPath, stored)
}

// ValidateResourceClaimTemplate 校验一个 ResourceClaimTemplate（资源申领模板）对象。
// 模板的 spec 内嵌了真正的 ResourceClaimSpec，校验逻辑与 ResourceClaim 一致。
func ValidateResourceClaimTemplate(template *resource.ResourceClaimTemplate) field.ErrorList {
	allErrs := corevalidation.ValidateObjectMeta(&template.ObjectMeta, true, corevalidation.ValidateResourceClaimTemplateName, field.NewPath("metadata"))
	allErrs = append(allErrs, validateResourceClaimTemplateSpec(&template.Spec, field.NewPath("spec"), false)...)
	return allErrs
}

func validateResourceClaimTemplateSpec(spec *resource.ResourceClaimTemplateSpec, fldPath *field.Path, stored bool) field.ErrorList {
	allErrs := corevalidation.ValidateTemplateObjectMeta(&spec.ObjectMeta, fldPath.Child("metadata"))
	allErrs = append(allErrs, validateResourceClaimSpec(&spec.Spec, fldPath.Child("spec"), stored)...)
	return allErrs
}

// ValidateResourceClaimTemplateUpdate 判断对 ResourceClaimTemplate 的更新是否合法。
// 模板 spec 是不可变字段，仅检查其是否被修改。
func ValidateResourceClaimTemplateUpdate(template, oldTemplate *resource.ResourceClaimTemplate) field.ErrorList {
	allErrs := corevalidation.ValidateObjectMetaUpdate(&template.ObjectMeta, &oldTemplate.ObjectMeta, field.NewPath("metadata"))
	// spec 是不可变字段。更新时只需要检查不可变性：
	// 其他字段用户无法修改，跳过重复校验，只保留对违反不可变性这一可操作错误的检查。
	allErrs = append(allErrs, apimachineryvalidation.ValidateImmutableField(template.Spec, oldTemplate.Spec, field.NewPath("spec"))...)
	return allErrs
}

func validateNodeName(name string, fldPath *field.Path) field.ErrorList {
	var allErrs field.ErrorList
	for _, msg := range corevalidation.ValidateNodeName(name, false) {
		allErrs = append(allErrs, field.Invalid(fldPath, name, msg))
	}
	return allErrs
}

// ValidateResourceSlice 判断一个 ResourceSlice（资源分片）对象是否合法。
// 会校验驱动、资源池、节点选择方式（四选一）以及设备、共享计数器等内容。
func ValidateResourceSlice(slice *resource.ResourceSlice) field.ErrorList {
	allErrs := corevalidation.ValidateObjectMeta(&slice.ObjectMeta, false, apimachineryvalidation.NameIsDNSSubdomain, field.NewPath("metadata"))
	allErrs = append(allErrs, validateResourceSliceSpec(&slice.Spec, nil, field.NewPath("spec"))...)
	return allErrs
}

// ValidateResourceSliceUpdate 判断一次 ResourceSlice 更新是否合法。
// 与创建时相比，额外校验不可变字段（资源池名、驱动名、节点名）以及变化部分的 CEL 表达式。
func ValidateResourceSliceUpdate(resourceSlice, oldResourceSlice *resource.ResourceSlice) field.ErrorList {
	allErrs := corevalidation.ValidateObjectMetaUpdate(&resourceSlice.ObjectMeta, &oldResourceSlice.ObjectMeta, field.NewPath("metadata"))
	allErrs = append(allErrs, validateResourceSliceSpec(&resourceSlice.Spec, &oldResourceSlice.Spec, field.NewPath("spec"))...)
	return allErrs
}

func validateResourceSliceSpec(spec, oldSpec *resource.ResourceSliceSpec, fldPath *field.Path) field.ErrorList {
	var allErrs field.ErrorList
	allErrs = append(allErrs, validateDriverName(spec.Driver, fldPath.Child("driver"))...)
	allErrs = append(allErrs, validateResourcePool(spec.Pool, fldPath.Child("pool"))...)
	if oldSpec != nil {
		allErrs = append(allErrs, apimachineryvalidation.ValidateImmutableField(spec.Pool.Name, oldSpec.Pool.Name, fldPath.Child("pool", "name"))...)
		allErrs = append(allErrs, apimachineryvalidation.ValidateImmutableField(spec.Driver, oldSpec.Driver, fldPath.Child("driver"))...)
		allErrs = append(allErrs, apimachineryvalidation.ValidateImmutableField(spec.NodeName, oldSpec.NodeName, fldPath.Child("nodeName"))...)
	}

	setFields := make([]string, 0, 4)
	if spec.NodeName != nil {
		if *spec.NodeName != "" {
			setFields = append(setFields, "`nodeName`")
			allErrs = append(allErrs, validateNodeName(*spec.NodeName, fldPath.Child("nodeName"))...)
		} else {
			allErrs = append(allErrs, field.Invalid(fldPath.Child("nodeName"), *spec.NodeName,
				"must be either unset or set to a non-empty string"))
		}
	}
	if spec.NodeSelector != nil {
		setFields = append(setFields, "`nodeSelector`")
		allErrs = append(allErrs, corevalidation.ValidateNodeSelector(spec.NodeSelector, false, fldPath.Child("nodeSelector"))...)
		if len(spec.NodeSelector.NodeSelectorTerms) != 1 {
			// 这项额外约束（nodeSelectorTerms 必须恰好一个）简化了
			// 从不同分片分配设备时多个节点选择器的合并逻辑。
			allErrs = append(allErrs, field.Invalid(fldPath.Child("nodeSelector", "nodeSelectorTerms"), spec.NodeSelector.NodeSelectorTerms, "must have exactly one node selector term"))
		}
	}

	if spec.AllNodes != nil {
		if *spec.AllNodes {
			setFields = append(setFields, "`allNodes`")
		} else {
			allErrs = append(allErrs, field.Invalid(fldPath.Child("allNodes"), *spec.AllNodes,
				"must be either unset or set to true"))
		}
	}

	if spec.PerDeviceNodeSelection != nil {
		if *spec.PerDeviceNodeSelection {
			setFields = append(setFields, "`perDeviceNodeSelection`")
		} else {
			allErrs = append(allErrs, field.Invalid(fldPath.Child("perDeviceNodeSelection"), *spec.PerDeviceNodeSelection,
				"must be either unset or set to true"))
		}
	}

	switch len(setFields) {
	case 0:
		allErrs = append(allErrs, field.Required(fldPath, "exactly one of `nodeName`, `nodeSelector`, `allNodes`, `perDeviceNodeSelection` is required"))
	case 1:
	default:
		allErrs = append(allErrs, field.Invalid(fldPath, fmt.Sprintf("{%s}", strings.Join(setFields, ", ")),
			"exactly one of `nodeName`, `nodeSelector`, `allNodes`, `perDeviceNodeSelection` is required, but multiple fields are set"))
	}

	if spec.SharedCounters != nil && spec.Devices != nil {
		allErrs = append(allErrs, field.Invalid(fldPath, "", "only one of `sharedCounters` or `devices` is allowed"))
	}

	maxDevices := resource.ResourceSliceMaxDevices
	if haveDeviceTaints(spec) || haveConsumesCounters(spec) || haveListAttributes(spec) {
		maxDevices = resource.ResourceSliceMaxDevicesWithAdvancedFeatures
	}
	allErrs = append(allErrs, validateSet(spec.Devices, maxDevices,
		func(device resource.Device, fldPath *field.Path) field.ErrorList {
			oldDevice := lookupDevice(oldSpec, device.Name)
			return validateDevice(device, oldDevice, fldPath, spec.PerDeviceNodeSelection)
		},
		func(device resource.Device) string {
			return device.Name
		}, fldPath.Child("devices"))...)

	allErrs = append(allErrs, validateSet(spec.SharedCounters, resource.ResourceSliceMaxCounterSets,
		validateCounterSet,
		func(counterSet resource.CounterSet) string {
			return counterSet.Name
		}, fldPath.Child("sharedCounters"), sizeCovered, uniquenessCovered)...)

	// 名称格式由声明式校验负责（参见该字段的标签），此处只校验业务约束。
	if spec.PartitionTypeAttribute != nil {
		allErrs = append(allErrs, validatePartitionTypeAttribute(spec, fldPath)...)
	}

	allErrs = append(allErrs, validateSkipNodeOperations(spec.SkipNodeOperations, fldPath.Child("skipNodeOperations"))...)

	return allErrs
}

// validatePartitionTypeAttribute 校验：声明了 partitionTypeAttribute（分区类型属性）
// 的分片必须包含消耗计数器的设备，并且每个这样的设备都必须以字符串形式携带该属性。
// 不消耗计数器的设备不受此约束，可以与它们共存于同一分片中。
func validatePartitionTypeAttribute(spec *resource.ResourceSliceSpec, fldPath *field.Path) field.ErrorList {
	var allErrs field.ErrorList
	name := string(*spec.PartitionTypeAttribute)

	if !haveConsumesCounters(spec) {
		return append(allErrs, field.Invalid(fldPath.Child("partitionTypeAttribute"), name,
			"may only be set on a slice which declares devices that consume counters"))
	}

	// 名称格式错误会由声明式校验报告；由于任何设备都不可能携带非法名称，
	// 逐个设备检查只会淹没那一条真正的错误，因此这里直接返回。
	if len(validateFullyQualifiedName(*spec.PartitionTypeAttribute, nil)) > 0 {
		return allErrs
	}

	// 属于驱动自身域的属性名也可以省略域名前缀直接声明。
	// 这里先一次性去掉前缀，避免在下面的设备循环中为每个设备都拼接 driver + "/"。
	bareName, _ := strings.CutPrefix(name, spec.Driver+"/")

	for i, device := range spec.Devices {
		if len(device.ConsumesCounters) == 0 {
			continue
		}
		attrPath := fldPath.Child("devices").Index(i).Child("attributes").Key(name)
		attribute, ok := lookupQualifiedAttribute(device.Attributes, name, bareName)
		if !ok {
			allErrs = append(allErrs, field.Required(attrPath,
				"device consumes counters and `partitionTypeAttribute` names this attribute, so it must be set"))
			continue
		}
		if attribute.StringValue == nil {
			allErrs = append(allErrs, field.Invalid(attrPath, attribute,
				"must be a string value because `partitionTypeAttribute` names this attribute"))
		}
	}

	return allErrs
}

// validateSkipNodeOperations 校验 SkipNodeOperations 上的业务逻辑约束：
// 除非同时跳过 NodeUnprepareResources（或指定了 "*"），否则不能单独跳过 NodePrepareResources。
// 其他检查（例如枚举值是否合法、集合是否唯一）由声明式校验负责。
func validateSkipNodeOperations(skipOps []resource.SkipNodeOperation, fldPath *field.Path) field.ErrorList {
	var allErrs field.ErrorList
	skipSet := sets.New(skipOps...)

	if skipSet.Has(resource.SkipNodeOperationNodePrepareResources) &&
		!skipSet.Has(resource.SkipNodeOperationNodeUnprepareResources) &&
		!skipSet.Has(resource.SkipNodeOperationAll) {
		allErrs = append(allErrs, field.Invalid(fldPath, skipOps,
			"NodePrepareResources cannot be skipped unless NodeUnprepareResources is also skipped"))
	}

	return allErrs
}

// lookupQualifiedAttribute 根据完全限定名查找设备属性。
// 不带域名的键默认属于驱动自身的域，因此驱动域内的属性既可以显式声明（带域名），
// 也可以省略域名声明。当两种形式同时存在时，显式形式优先，以保证结果确定性；
// bareName（去掉驱动域名后的名称，若属性属于其他域则为空）仅在非空时才作为后备键查找。
func lookupQualifiedAttribute(attributes map[resource.QualifiedName]resource.DeviceAttribute, name, bareName string) (resource.DeviceAttribute, bool) {
	if attribute, ok := attributes[resource.QualifiedName(name)]; ok {
		return attribute, true
	}
	if bareName != "" {
		if attribute, ok := attributes[resource.QualifiedName(bareName)]; ok {
			return attribute, true
		}
	}
	return resource.DeviceAttribute{}, false
}

func haveListAttributes(spec *resource.ResourceSliceSpec) bool {
	if spec == nil {
		return false
	}

	for _, device := range spec.Devices {
		for _, attribute := range device.Attributes {
			if attribute.BoolValues != nil ||
				attribute.IntValues != nil ||
				attribute.StringValues != nil ||
				attribute.VersionValues != nil {
				return true
			}
		}
	}
	return false
}

func haveDeviceTaints(spec *resource.ResourceSliceSpec) bool {
	if spec == nil {
		return false
	}

	for _, device := range spec.Devices {
		if len(device.Taints) > 0 {
			return true
		}
	}
	return false
}

func haveConsumesCounters(spec *resource.ResourceSliceSpec) bool {
	if spec == nil {
		return false
	}

	for _, device := range spec.Devices {
		if len(device.ConsumesCounters) > 0 {
			return true
		}
	}
	return false
}

func lookupDevice(spec *resource.ResourceSliceSpec, deviceName string) *resource.Device {
	if spec == nil {
		return nil
	}
	for i := range spec.Devices {
		device := &spec.Devices[i]
		if device.Name == deviceName {
			return device
		}
	}
	return nil
}

func validateCounterSet(counterSet resource.CounterSet, fldPath *field.Path) field.ErrorList {
	var allErrs field.ErrorList
	if counterSet.Name == "" {
		allErrs = append(allErrs, field.Required(fldPath.Child("name"), "").MarkCoveredByDeclarative())
	} else {
		allErrs = append(allErrs, validateCounterName(counterSet.Name, fldPath.Child("name")).MarkCoveredByDeclarative()...)
	}
	if len(counterSet.Counters) == 0 {
		allErrs = append(allErrs, field.Required(fldPath.Child("counters"), "").MarkCoveredByDeclarative())
	} else {
		// 所有计数器集合的总数上限由调用方统一检查，此处只校验单个集合。
		allErrs = append(allErrs, validateMap(counterSet.Counters, resource.ResourceSliceMaxCountersPerCounterSet, validation.DNS1123LabelMaxLength,
			validateCounterName, validateDeviceCounter, fldPath.Child("counters"), keysCovered)...)
	}

	return allErrs
}

func validateResourcePool(pool resource.ResourcePool, fldPath *field.Path) field.ErrorList {
	var allErrs field.ErrorList
	allErrs = append(allErrs, validatePoolName(pool.Name, fldPath.Child("name"))...)
	if pool.ResourceSliceCount <= 0 {
		allErrs = append(allErrs, field.Invalid(fldPath.Child("resourceSliceCount"), pool.ResourceSliceCount, "must be greater than zero"))
	}
	if pool.Generation < 0 {
		allErrs = append(allErrs, field.Invalid(fldPath.Child("generation"), pool.Generation, "must be greater than or equal to zero"))
	}
	return allErrs
}

func validateDevice(device resource.Device, oldDevice *resource.Device, fldPath *field.Path, perDeviceNodeSelection *bool) field.ErrorList {
	var allErrs field.ErrorList
	allowMultipleAllocations := device.AllowMultipleAllocations != nil && *device.AllowMultipleAllocations
	allErrs = append(allErrs, validateDeviceName(device.Name, fldPath.Child("name"))...)

	// 超出最大长度只报告一次：如果单个字段都超限，那么总量必然也超限。
	// 这里统计的是属性条目的总数（包含标量条目和列表中的每一项）。
	attributeAndCapacityLength := len(device.Attributes) + len(device.Capacity)
	if attributeAndCapacityLength > resource.ResourceSliceMaxAttributesAndCapacitiesPerDevice {
		allErrs = append(allErrs, field.Invalid(fldPath, attributeAndCapacityLength, fmt.Sprintf("the total number of attributes and capacities must not exceed %d", resource.ResourceSliceMaxAttributesAndCapacitiesPerDevice)))
	}
	numAttributeValues := numEntriesDeviceAttribute(device)
	if numAttributeValues > resource.ResourceSliceMaxAttributeValuesPerDevice {
		allErrs = append(allErrs, field.Invalid(fldPath, numAttributeValues, fmt.Sprintf("the total number of attribute values must not exceed %d", resource.ResourceSliceMaxAttributeValuesPerDevice)))
	}

	allErrs = append(allErrs, validateMap(device.Attributes, -1, attributeAndCapacityMaxKeyLength, validateQualifiedName, validateDeviceAttribute, fldPath.Child("attributes"))...)
	// 如果整个 capacity 与旧值相同，则可以跳过校验。
	// 对完整 spec 做 DeepEqual 也可行，但只对 capacity 做比较开销更小。
	if oldDevice == nil || !apiequality.Semantic.DeepEqual(oldDevice.Capacity, device.Capacity) {
		if allowMultipleAllocations {
			allErrs = append(allErrs, validateMap(device.Capacity, -1, attributeAndCapacityMaxKeyLength, validateQualifiedName, validateMultiAllocatableDeviceCapacity, fldPath.Child("capacity"))...)
		} else {
			allErrs = append(allErrs, validateMap(device.Capacity, -1, attributeAndCapacityMaxKeyLength, validateQualifiedName, validateSingleAllocatableDeviceCapacity, fldPath.Child("capacity"))...)
		}
	}
	// 如果整个集合（taints）与旧值相同，则可以跳过校验。
	// 对完整 spec 做 DeepEqual 也可行，但只对 taints 做比较开销更小。
	if oldDevice == nil || !apiequality.Semantic.DeepEqual(oldDevice.Taints, device.Taints) {
		allErrs = append(allErrs, validateSlice(device.Taints, resource.DeviceTaintsMaxLength,
			func(taint resource.DeviceTaint, fldPath *field.Path) field.ErrorList {
				return validateDeviceTaint(taint, nil, fldPath)
			},
			fldPath.Child("taints"))...)
	}

	allErrs = append(allErrs, validateSet(device.ConsumesCounters, resource.ResourceSliceMaxDeviceCounterConsumptionsPerDevice,
		validateDeviceCounterConsumption,
		func(deviceCapacityConsumption resource.DeviceCounterConsumption) string {
			return deviceCapacityConsumption.CounterSet
		}, fldPath.Child("consumesCounters"), sizeCovered, uniquenessCovered)...)

	if perDeviceNodeSelection != nil && *perDeviceNodeSelection {
		setFields := make([]string, 0, 3)
		if device.NodeName != nil {
			if len(*device.NodeName) != 0 {
				setFields = append(setFields, "`nodeName`")
				allErrs = append(allErrs, validateNodeName(*device.NodeName, fldPath.Child("nodeName"))...)
			} else {
				allErrs = append(allErrs, field.Invalid(fldPath.Child("nodeName"), *device.NodeName, "must not be empty"))
			}

		}
		if device.NodeSelector != nil {
			setFields = append(setFields, "`nodeSelector`")
			allErrs = append(allErrs, corevalidation.ValidateNodeSelector(device.NodeSelector, false, fldPath.Child("nodeSelector"))...)
		}
		if device.AllNodes != nil {
			if *device.AllNodes {
				setFields = append(setFields, "`allNodes`")
			} else {
				allErrs = append(allErrs, field.Invalid(fldPath.Child("allNodes"), *device.AllNodes, "must be either unset or set to true"))
			}
		}
		switch len(setFields) {
		case 0:
			allErrs = append(allErrs, field.Required(fldPath, "exactly one of `nodeName`, `nodeSelector`, or `allNodes` is required when `perDeviceNodeSelection` is set to true in the ResourceSlice spec"))
		case 1:
		default:
			allErrs = append(allErrs, field.Invalid(fldPath, fmt.Sprintf("{%s}", strings.Join(setFields, ", ")), "exactly one of `nodeName`, `nodeSelector`, or `allNodes` is required when `perDeviceNodeSelection` is set to true in the ResourceSlice spec"))
		}
	} else if (perDeviceNodeSelection == nil || !*perDeviceNodeSelection) && (device.NodeName != nil || device.NodeSelector != nil || device.AllNodes != nil) {
		allErrs = append(allErrs, field.Invalid(fldPath, nil, "`nodeName`, `nodeSelector` and `allNodes` can only be set if `perDeviceNodeSelection` is set to true in the ResourceSlice spec"))
	}

	allErrs = append(allErrs, validateDeviceBindingParameters(device.BindingConditions, device.BindingFailureConditions, fldPath)...)
	allErrs = append(allErrs, validateNodeAllocatableResources(device.NodeAllocatableResources, device.Capacity, fldPath.Child("nodeAllocatableResources"))...)

	return allErrs
}

func validateNodeAllocatableResources(mappings map[corev1.ResourceName]resource.NodeAllocatableResource, capacities map[resource.QualifiedName]resource.DeviceCapacity, fldPath *field.Path) field.ErrorList {
	var allErrs field.ErrorList
	for resourceName, mapping := range mappings {
		keyPath := fldPath.Key(string(resourceName))
		if !corehelper.IsNodeAllocatableResourceName(core.ResourceName(resourceName)) {
			allErrs = append(allErrs, field.Invalid(keyPath, resourceName, "must be a node allocatable resource name"))
		}

		if mapping.Mapping == nil && mapping.Overhead == nil {
			allErrs = append(allErrs, field.Required(keyPath, "at least one of mapping or overhead must be set"))
		}
		if mapping.Mapping != nil {
			allErrs = append(allErrs, validateNodeAllocatableMapping(mapping.Mapping, capacities, keyPath.Child("mapping"))...)
		}
		if mapping.Overhead != nil {
			allErrs = append(allErrs, validateNodeAllocatableOverheadMapping(mapping.Overhead, keyPath.Child("overhead"))...)
		}
	}
	return allErrs
}

// validateNodeAllocatableMapping 校验“映射型”节点可分配资源配置：
// 检查 capacityKey 引用是否存在、乘数是否为正数，以及 capacityKey 与 capacityMultiplier 是否成对出现。
func validateNodeAllocatableMapping(mapping *resource.NodeAllocatableMapping, capacities map[resource.QualifiedName]resource.DeviceCapacity, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	if mapping.CapacityKey != nil {
		if *mapping.CapacityKey == "" {
			allErrs = append(allErrs, field.Invalid(fldPath.Child("capacityKey"), "", "capacityKey must not be an empty string"))
		} else {
			var exists bool
			if capacities != nil {
				_, exists = capacities[*mapping.CapacityKey]
			}
			if !exists {
				allErrs = append(allErrs, field.NotFound(fldPath.Child("capacityKey"), *mapping.CapacityKey))
			}
		}
		if mapping.CapacityMultiplier == nil {
			allErrs = append(allErrs, field.Required(fldPath.Child("capacityMultiplier"), "capacityMultiplier is required when capacityKey is set").WithOrigin("dependentRequired").MarkCoveredByDeclarative())
		} else if mapping.CapacityMultiplier.Sign() <= 0 {
			allErrs = append(allErrs, field.Invalid(fldPath.Child("capacityMultiplier"), mapping.CapacityMultiplier.String(), "must be positive"))
		}
	}

	if mapping.DeviceMultiplier != nil {
		if mapping.DeviceMultiplier.Sign() <= 0 {
			allErrs = append(allErrs, field.Invalid(fldPath.Child("deviceMultiplier"), mapping.DeviceMultiplier.String(), "must be positive"))
		}
	}

	if mapping.CapacityMultiplier != nil && mapping.CapacityKey == nil {
		allErrs = append(allErrs, field.Required(fldPath.Child("capacityKey"), "capacityKey is required when capacityMultiplier is set").WithOrigin("dependentRequired").MarkCoveredByDeclarative())
	}

	return allErrs
}

// validateNodeAllocatableOverheadMapping 校验“开销型”节点可分配资源配置：
// perPod 与 perContainer 至少要设置一个，且都不能为负数。
func validateNodeAllocatableOverheadMapping(overhead *resource.NodeAllocatableOverhead, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	if overhead.PerPod == nil && overhead.PerContainer == nil {
		allErrs = append(allErrs, field.Invalid(fldPath, "", "at least one of perPod or perContainer must be set"))
		return allErrs
	}

	if overhead.PerPod != nil && overhead.PerPod.Sign() < 0 {
		allErrs = append(allErrs, field.Invalid(fldPath.Child("perPod"), overhead.PerPod.String(), "must be non-negative"))
	}

	if overhead.PerContainer != nil && overhead.PerContainer.Sign() < 0 {
		allErrs = append(allErrs, field.Invalid(fldPath.Child("perContainer"), overhead.PerContainer.String(), "must be non-negative"))
	}
	return allErrs
}

// numEntriesDeviceAttribute 统计设备属性中的条目总数，
// 列表类型属性中的每一项都会单独计数。
// 当 DRAListTypeAttributes 特性门控开启时，该函数用于强制每个设备可拥有的
// 属性与容量条目总数上限。
func numEntriesDeviceAttribute(device resource.Device) int {
	numEntries := 0
	for _, attr := range device.Attributes {
		if len(attr.BoolValues) == 0 && len(attr.IntValues) == 0 && len(attr.StringValues) == 0 && len(attr.VersionValues) == 0 {
			numEntries++
			continue
		}
		// 这些列表字段实际上是互斥的，但仍把所有字段中的条目数累加在一起。
		// 如果同时设置了多个列表字段，后续校验会失败并提示“必须且只能指定一个值”。
		numEntries += len(attr.BoolValues) + len(attr.IntValues) + len(attr.StringValues) + len(attr.VersionValues)
	}
	return numEntries
}

func validateDeviceCounterConsumption(deviceCounterConsumption resource.DeviceCounterConsumption, fldPath *field.Path) field.ErrorList {
	var allErrs field.ErrorList

	if len(deviceCounterConsumption.CounterSet) == 0 {
		allErrs = append(allErrs, field.Required(fldPath.Child("counterSet"), "").MarkCoveredByDeclarative())
	} else {
		allErrs = append(allErrs, validateCounterName(deviceCounterConsumption.CounterSet, fldPath.Child("counterSet")).MarkCoveredByDeclarative()...)
	}
	if len(deviceCounterConsumption.Counters) == 0 {
		allErrs = append(allErrs, field.Required(fldPath.Child("counters"), "").MarkCoveredByDeclarative())
	} else {
		allErrs = append(allErrs, validateMap(deviceCounterConsumption.Counters, resource.ResourceSliceMaxCountersPerDeviceCounterConsumption,
			validation.DNS1123LabelMaxLength, validateCounterName, validateDeviceCounter, fldPath.Child("counters"), keysCovered)...)
	}
	return allErrs
}

var (
	numericIdentifier = `(0|[1-9]\d*)`

	preReleaseIdentifier = `(0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*)`

	buildIdentifier = `[0-9a-zA-Z-]+`

	semverRe = regexp.MustCompile(`^` +

		// 以点分隔的版本号段（例如 1.2.3）
		numericIdentifier + `\.` + numericIdentifier + `\.` + numericIdentifier +

		// 可选的预发布版本段，以 '-' 开头、各段以点分隔（例如 -alpha.PRERELEASE.1）
		`(-` + preReleaseIdentifier + `(\.` + preReleaseIdentifier + `)*)?` +

		// 可选的构建标识段，以 '+' 开头、各段以点分隔（例如 +build.id.20240305）
		`(\+` + buildIdentifier + `(\.` + buildIdentifier + `)*)?` +
		`$`)
)

func validateDeviceAttribute(attribute resource.DeviceAttribute, fldPath *field.Path) field.ErrorList {
	var allErrs field.ErrorList
	numFields := 0
	if attribute.BoolValue != nil {
		numFields++
	}
	if attribute.IntValue != nil {
		numFields++
	}
	if attribute.StringValue != nil {
		numFields++
		allErrs = append(allErrs, validateDeviceAttributeStringValue(attribute.StringValue, fldPath.Child("string"))...)
	}
	if attribute.VersionValue != nil {
		numFields++
		allErrs = append(allErrs, validateDeviceAttributeVersionValue(attribute.VersionValue, fldPath.Child("version"))...)
	}
	if attribute.BoolValues != nil {
		numFields++
		if len(attribute.BoolValues) == 0 {
			allErrs = append(allErrs, field.Invalid(fldPath.Child("bools"), attribute.BoolValues, "must not be empty if specified"))
		}
	}
	if attribute.IntValues != nil {
		numFields++
		if len(attribute.IntValues) == 0 {
			allErrs = append(allErrs, field.Invalid(fldPath.Child("ints"), attribute.IntValues, "must not be empty if specified"))
		}
	}
	if attribute.StringValues != nil {
		numFields++
		if len(attribute.StringValues) == 0 {
			allErrs = append(allErrs, field.Invalid(fldPath.Child("strings"), attribute.StringValues, "must not be empty if specified"))
		}
		for i, item := range attribute.StringValues {
			allErrs = append(allErrs, validateDeviceAttributeStringValue(&item, fldPath.Child("strings").Index(i)).WithOrigin("maxBytes").MarkCoveredByDeclarative()...)
		}
	}
	if attribute.VersionValues != nil {
		numFields++
		if len(attribute.VersionValues) == 0 {
			allErrs = append(allErrs, field.Invalid(fldPath.Child("versions"), attribute.VersionValues, "must not be empty if specified"))
		}
		for i, item := range attribute.VersionValues {
			allErrs = append(allErrs, validateDeviceAttributeVersionValue(&item, fldPath.Child("versions").Index(i))...)
		}
	}

	switch numFields {
	case 0:
		allErrs = append(allErrs, field.Invalid(fldPath, "", "exactly one value must be specified").WithOrigin("union").MarkCoveredByDeclarative())
	case 1:
		// 恰好指定了一个值，合法。
	default:
		allErrs = append(allErrs, field.Invalid(fldPath, attribute, "exactly one value must be specified").WithOrigin("union").MarkCoveredByDeclarative())
	}
	return allErrs
}

func validateDeviceAttributeStringValue(value *string, fldPath *field.Path) field.ErrorList {
	var allErrs field.ErrorList
	if len(*value) > resource.DeviceAttributeMaxValueLength {
		allErrs = append(allErrs, field.TooLong(fldPath, "" /*unused*/, resource.DeviceAttributeMaxValueLength))
	}
	return allErrs
}

func validateDeviceAttributeVersionValue(value *string, fldPath *field.Path) field.ErrorList {
	var allErrs field.ErrorList
	if !semverRe.MatchString(*value) {
		allErrs = append(allErrs, field.Invalid(fldPath, *value, "must be a string compatible with semver.org spec 2.0.0"))
	}
	if len(*value) > resource.DeviceAttributeMaxValueLength {
		allErrs = append(allErrs, field.TooLong(fldPath, "" /*unused*/, resource.DeviceAttributeMaxValueLength))
	}
	return allErrs
}

// validateMultiAllocatableDeviceCapacity 用于校验可多次分配设备的可消耗容量中的 requestPolicy。
// 仅在容量是新增或发生修改时才会调用。
func validateMultiAllocatableDeviceCapacity(capacity resource.DeviceCapacity, fldPath *field.Path) field.ErrorList {
	var allErrs field.ErrorList
	if capacity.RequestPolicy != nil {
		allErrs = append(allErrs,
			validateRequestPolicy(capacity.Value, capacity.RequestPolicy, fldPath.Child("requestPolicy"))...)
	}
	return allErrs
}

// validateSingleAllocatableDeviceCapacity 禁止单次分配设备使用可消耗容量（consumable capacity）：
// 这类设备不允许设置 requestPolicy。
func validateSingleAllocatableDeviceCapacity(capacity resource.DeviceCapacity, fldPath *field.Path) field.ErrorList {
	var allErrs field.ErrorList
	if capacity.RequestPolicy != nil {
		allErrs = append(allErrs,
			field.Forbidden(fldPath.Child("requestPolicy"), "allowMultipleAllocations must be true"))
	}
	return allErrs
}

// validateRequestPolicy 校验容量请求策略（requestPolicy）：
// validValues（离散取值）和 validRange（连续范围）最多只能定义一个；
// 一旦定义了其中之一，就必须同时定义合法的 default。
// 仅在策略是新增或修改时才会调用。
func validateRequestPolicy(maxCapacity apiresource.Quantity, policy *resource.CapacityRequestPolicy, fldPath *field.Path) field.ErrorList {
	var allErrs field.ErrorList
	if len(policy.ValidValues) > 0 && policy.ValidRange != nil {
		allErrs = append(allErrs, field.Forbidden(fldPath, `exactly one policy can be specified, cannot specify "validValues" and "validRange" at the same time`))
	} else {
		allErrs = append(allErrs, validateValidRequestValues(maxCapacity, policy, fldPath)...)
	}
	return allErrs
}

func validateValidRequestValues(maxCapacity apiresource.Quantity, policy *resource.CapacityRequestPolicy, fldPath *field.Path) field.ErrorList {
	var allErrs field.ErrorList
	switch {
	case len(policy.ValidValues) > 0:
		if policy.Default == nil {
			allErrs = append(allErrs, field.Required(fldPath.Child("default"), "required when validValues is defined"))
		} else {
			allErrs = append(allErrs, validateRequestPolicyValidValues(*policy.Default, maxCapacity, policy.ValidValues, fldPath.Child("validValues"))...)
		}
	case policy.ValidRange != nil:
		if policy.Default == nil {
			allErrs = append(allErrs, field.Required(fldPath.Child("default"), "required when validRange is defined"))
		} else {
			allErrs = append(allErrs, validateRequestPolicyRange(*policy.Default, maxCapacity, *policy.ValidRange, fldPath.Child("validRange"))...)
		}
	}
	return allErrs
}

func validateRequestPolicyValidValues(defaultValue apiresource.Quantity, maxCapacity apiresource.Quantity, validValues []apiresource.Quantity, fldPath *field.Path) field.ErrorList {
	var allErrs field.ErrorList
	foundDefault := false

	// 检查 validValues 是否按升序排列，便于后续查找和保证确定性。
	for i := range len(validValues) - 1 {
		if validValues[i].Cmp(validValues[i+1]) > 0 {
			allErrs = append(allErrs, field.Invalid(
				fldPath.Index(i+1),
				validValues[i+1].String(),
				"values must be sorted in ascending order"))
		}
	}

	// 根据是否支持小数容量值来选择键函数：
	// DRAFractionalCapacityRange 门控开启时使用十进制精度（inf.Dec），
	// 否则使用基于整数的键进行去重判断。
	quantityKeyFunc := quantityKeyInt
	if utilfeature.DefaultFeatureGate.Enabled(features.DRAFractionalCapacityRange) {
		quantityKeyFunc = quantityKeyAsDec
	}

	allErrs = append(allErrs, validateSet(validValues, resource.CapacityRequestPolicyDiscreteMaxOptions,
		func(option apiresource.Quantity, fldPath *field.Path) field.ErrorList {
			var allErrs field.ErrorList
			if option.Cmp(maxCapacity) > 0 {
				allErrs = append(allErrs, field.Invalid(fldPath, option.String(), fmt.Sprintf("option is larger than capacity value: %s", maxCapacity.String())))
			}
			if option.Cmp(defaultValue) == 0 {
				foundDefault = true
			}
			return allErrs
		}, quantityKeyFunc, fldPath)...)
	if !foundDefault {
		allErrs = append(allErrs, field.Invalid(fldPath, defaultValue.String(), "default value is not valid according to the requestPolicy"))
	}
	return allErrs
}

// validateRequestPolicyRange 校验一个 CapacityRequestPolicyRange（连续容量请求范围）：
// 检查 min/default/max 的大小关系、是否超出容量上限，以及步长是否合法。
// 仅在范围是新增或修改时才会调用。
func validateRequestPolicyRange(defaultValue apiresource.Quantity, maxCapacity apiresource.Quantity, valueRange resource.CapacityRequestPolicyRange, fldPath *field.Path) field.ErrorList {
	var allErrs field.ErrorList
	if valueRange.Min == nil {
		allErrs = append(allErrs, field.Required(fldPath.Child("min"), "required when validRange is defined"))
		return allErrs
	}
	if valueRange.Min.Cmp(maxCapacity) > 0 {
		allErrs = append(allErrs, field.Invalid(fldPath.Child("min"), valueRange.Min.String(), fmt.Sprintf("min is larger than capacity value: %s", maxCapacity.String())))
	}
	if defaultValue.Cmp(*valueRange.Min) < 0 {
		allErrs = append(allErrs, field.Invalid(fldPath.Child("default"), defaultValue.String(), fmt.Sprintf("default is less than min: %s", valueRange.Min.String())))
	}
	if valueRange.Max != nil {
		if valueRange.Min.Cmp(*valueRange.Max) > 0 {
			allErrs = append(allErrs, field.Invalid(fldPath.Child("max"), valueRange.Min.String(), fmt.Sprintf("min is larger than max: %s", valueRange.Max.String())))
		}
		if valueRange.Max.Cmp(maxCapacity) > 0 {
			allErrs = append(allErrs, field.Invalid(fldPath.Child("max"), valueRange.Max.String(), fmt.Sprintf("max is larger than capacity value: %s", maxCapacity.String())))
		}
		if defaultValue.Cmp(*valueRange.Max) > 0 {
			allErrs = append(allErrs, field.Invalid(fldPath.Child("default"), defaultValue.String(), fmt.Sprintf("default is more than max: %s", valueRange.Max.String())))
		}
	}
	useMilli := false
	if utilfeature.DefaultFeatureGate.Enabled(features.DRAFractionalCapacityRange) {
		// default 不属于范围本身，因此 rangeHasFractional 检查不到它。
		// 即使范围是整数而 default 是小数，也必须走毫单位（milli）计算路径，
		// 否则整数路径会读取会四舍五入的 default.Value()，错误地接受非整数倍的值。
		useMilli = rangeHasFractional(valueRange) || isFractionalQuantity(defaultValue)
		// 当 DRAFractionalCapacityRange 门控开启且输入范围包含小数时，
		// 需要进行溢出保护检查。
		if useMilli {
			hasUnsafeValue := false
			for _, pair := range []struct {
				q    *apiresource.Quantity
				name string
			}{
				{&defaultValue, "default"},
				{valueRange.Min, "min"},
				{valueRange.Max, "max"},
				{valueRange.Step, "step"},
			} {
				if pair.q != nil {
					milliQuantity := apiresource.NewMilliQuantity(pair.q.MilliValue(), pair.q.Format)
					if pair.q.Cmp(*milliQuantity) != 0 {
						allErrs = append(allErrs, field.Invalid(fldPath.Child(pair.name), pair.q.String(),
							"value cannot be represented as a milli value"))
						hasUnsafeValue = true
					}
				}
			}
			if hasUnsafeValue {
				// 发现无法安全表示的值后快速返回，避免后续不安全的算术运算。
				return allErrs
			}
		}
	}
	if valueRange.Step != nil {
		if valueRange.Step.Sign() <= 0 {
			allErrs = append(allErrs, field.Invalid(fldPath.Child("step"), valueRange.Step.String(), "must be greater than zero"))
		} else {
			added := valueRange.Min.DeepCopy()
			added.Add(*valueRange.Step)
			if added.Cmp(maxCapacity) > 0 {
				allErrs = append(allErrs, field.Invalid(fldPath.Child("step"), valueRange.Step.String(), fmt.Sprintf("one step %s is larger than capacity value: %s", added.String(), maxCapacity.String())))
			}
			// 下面 validateRequestPolicyRangeStep 的整数路径使用 Quantity.Value() 读取
			// step、min、max 和 default，而该方法只有在 [0, MaxInt64] 区间内才可靠：
			// 超过该范围时，2^64 的正整数倍会被读成 0，导致步长检查出现除零；
			// 而十进制表示的负数边界可能被读回为正数。因此在算术运算前先拒绝这些值。
			// 毫单位路径则由上面的 hasUnsafeValue 检查负责兜底。
			hasUnsafeValue := false
			if !useMilli {
				for _, pair := range []struct {
					q    *apiresource.Quantity
					name string
				}{
					{&defaultValue, "default"},
					{valueRange.Min, "min"},
					{valueRange.Max, "max"},
					{valueRange.Step, "step"},
				} {
					if pair.q != nil {
						if errs := validateInt64Range(*pair.q, fldPath.Child(pair.name)); len(errs) > 0 {
							allErrs = append(allErrs, errs...)
							hasUnsafeValue = true
						}
					}
				}
			}
			if !hasUnsafeValue {
				// 只要任一字段包含小数，就使用毫单位（milli）算术避免精度丢失。
				// useMilli 仅在 DRAFractionalCapacityRange 门控开启时才可能为 true。
				allErrs = append(allErrs, validateRequestPolicyRangeStep(defaultValue, *valueRange.Min, *valueRange.Step, useMilli, fldPath.Child("step"))...)
				if valueRange.Max != nil {
					allErrs = append(allErrs, validateRequestPolicyRangeStep(*valueRange.Max, *valueRange.Min, *valueRange.Step, useMilli, fldPath.Child("step"))...)
				}
			}
		}
	}
	return allErrs
}

// validateRequestPolicyRangeStep 校验 value 是否是从 min 出发、按 step 递增得到的合法取值
// （即 value - min 必须是 step 的整数倍）。
//
// 前置条件：value、min 和 step 必须事先校验过，确保不会下溢或溢出支持的值域
// （int64 或毫单位值域），并且 step 不为零。
func validateRequestPolicyRangeStep(value, min, step apiresource.Quantity, useMilli bool, fldPath *field.Path) field.ErrorList {
	var stepVal, minVal, val int64
	if useMilli {
		stepVal = step.MilliValue()
		minVal = min.MilliValue()
		val = value.MilliValue()
	} else {
		stepVal = step.Value()
		minVal = min.Value()
		val = value.Value()
	}
	if (val-minVal)%stepVal != 0 {
		return field.ErrorList{field.Invalid(fldPath, value.String(), fmt.Sprintf("value is not a multiple of a given step (%s) from (%s)", step.String(), min.String()))}
	}
	return nil
}

// boundOverInt64Message 是可消耗容量 validRange 边界超出 Quantity.Value()
// 可用范围时返回的错误信息。
const boundOverInt64Message = "must not be larger than 9223372036854775807"

// validateInt64Range 检查 q 是否位于 [0, MaxInt64] 区间内——
// 只有在这个范围内 Quantity.Value() 返回的值才可用于边界推理。
// 超过 MaxInt64 会发生溢出，2^64 的正整数倍会被读回为 0；
// 低于零时，一旦 quantity 是十进制表示的，取整行为就不符合文档描述，
// 甚至可能读回错误的正负号（#110653）。
// 单独使用 Sign() 会漏掉第一种情况，单独使用 CmpInt64 会漏掉第二种，
// 因此这里必须同时做两项检查。
func validateInt64Range(q apiresource.Quantity, fldPath *field.Path) field.ErrorList {
	if q.Sign() < 0 {
		return field.ErrorList{field.Invalid(fldPath, q.String(), apimachineryvalidation.IsNegativeErrorMsg)}
	}
	if q.CmpInt64(math.MaxInt64) > 0 {
		return field.ErrorList{field.Invalid(fldPath, q.String(), boundOverInt64Message)}
	}
	return nil
}

// rangeHasFractional 判断范围 r 中任一非空字段是否包含亚整数精度（小数部分）。
func rangeHasFractional(r resource.CapacityRequestPolicyRange) bool {
	for _, q := range []*apiresource.Quantity{r.Min, r.Max, r.Step} {
		if q != nil && isFractionalQuantity(*q) {
			return true
		}
	}
	return false
}

// isFractionalQuantity 判断数量 q 是否包含亚整数精度（即是否为小数）。
func isFractionalQuantity(q apiresource.Quantity) bool {
	dec := q.AsDec()
	infDec := inf.Dec{}
	rounded := infDec.Round(dec, 0, inf.RoundDown)
	return dec.Cmp(rounded) != 0
}

func validateDeviceCounter(counter resource.Counter, fldPath *field.Path) field.ErrorList {
	// 计数器值只要能被解析为 Quantity 即为合法，因此这里无需额外校验。
	return nil
}

func validateQualifiedName(name resource.QualifiedName, fldPath *field.Path) field.ErrorList {
	var allErrs field.ErrorList
	parts := strings.Split(string(name), "/")
	switch len(parts) {
	case 1:
		allErrs = append(allErrs, validateCIdentifier(parts[0], fldPath)...)
	case 2:
		if len(parts[0]) == 0 {
			allErrs = append(allErrs, field.Invalid(fldPath, "", "the domain must not be empty"))
		} else {
			allErrs = append(allErrs, validateDriverName(parts[0], fldPath)...)
		}
		if len(parts[1]) == 0 {
			allErrs = append(allErrs, field.Invalid(fldPath, "", "the name must not be empty"))
		} else {
			allErrs = append(allErrs, validateCIdentifier(parts[1], fldPath)...)
		}
		// TODO: This validation is incomplete. It should reject qualified names
		// that contain more than one slash. Currently, names like "a/b/c" are not
		// handled and are implicitly accepted.
		//
		// This needs to be fixed in two places:
		// 1. Here in this function.
		// 2. In the corresponding declarative validation utility `resourcesQualifiedName`
		//    in `staging/src/k8s.io/apimachinery/pkg/api/validate/strfmt.go`.
		//
		// The fix should be introduced carefully, possibly using ratcheting to avoid
		// breaking existing, non-compliant objects.
	}

	return allErrs
}

func validateFullyQualifiedName(name resource.FullyQualifiedName, fldPath *field.Path) field.ErrorList {
	var allErrs field.ErrorList
	allErrs = append(allErrs, validateQualifiedName(resource.QualifiedName(name), fldPath)...)
	// validateQualifiedName 只会检查域名和名称两部分各自的格式是否合法，
	// 这里还需要额外确保名称中确实包含域名（即存在 '/'）。
	if !strings.Contains(string(name), "/") {
		allErrs = append(allErrs, field.Invalid(fldPath, name, "a fully qualified name must be a domain and a name separated by a slash"))
	}
	return allErrs.WithOrigin("format=k8s-resource-fully-qualified-name")
}

func validateCIdentifier(id string, fldPath *field.Path) field.ErrorList {
	var allErrs field.ErrorList
	if len(id) > resource.DeviceMaxIDLength {
		allErrs = append(allErrs, field.TooLong(fldPath, "" /*unused*/, resource.DeviceMaxIDLength))
	}
	for _, msg := range content.IsCIdentifier(id) {
		allErrs = append(allErrs, field.Invalid(fldPath, id, msg))
	}
	return allErrs
}

// validationOption 是校验辅助函数的可选标志，
// 用于标记某类检查已经由声明式校验覆盖，从而在错误上打标记。
type validationOption int

const (
	// 列表中每个元素的校验已由声明式校验覆盖。
	itemsCovered validationOption = iota
	// 列表长度（元素数量）的检查已由声明式校验覆盖。
	sizeCovered
	// 元素唯一性（无重复）的检查已由声明式校验覆盖。
	uniquenessCovered
	// Map 键的校验已由声明式校验覆盖。
	keysCovered
)

// validateItems 逐个校验切片中的元素，并可在元素校验被声明式校验覆盖时给错误打标记。
func validateItems[T any](slice []T, validateItem func(T, *field.Path) field.ErrorList, fldPath *field.Path, opts ...validationOption) field.ErrorList {
	var allErrs field.ErrorList
	for i, item := range slice {
		idxPath := fldPath.Index(i)
		errs := validateItem(item, idxPath)
		if slices.Contains(opts, itemsCovered) {
			errs = errs.MarkCoveredByDeclarative()
		}
		allErrs = append(allErrs, errs...)
	}
	return allErrs
}

// validateSlice 确保切片长度不超过指定的 maxSize，并且每个元素都合法。
// maxSize 为负数时禁用长度检查。
func validateSlice[T any](slice []T, maxSize int, validateItem func(T, *field.Path) field.ErrorList, fldPath *field.Path, opts ...validationOption) field.ErrorList {
	if maxSize >= 0 && len(slice) > maxSize {
		// 把整个字段内容写进错误信息通常会太长，尤其在元素数量已经超限时。
		// 因此这里只报告实际元素数量，而不输出字段内容。
		err := field.TooMany(fldPath, len(slice), maxSize).WithOrigin("maxItems")
		if slices.Contains(opts, sizeCovered) {
			err = err.MarkCoveredByDeclarative()
		}
		// 长度检查不通过时直接短路返回，避免继续处理超大输入造成 DoS。
		return field.ErrorList{err}
	}
	return validateItems(slice, validateItem, fldPath, opts...)
}

// validateSet 在 validateSlice 的基础上，额外确保切片中没有重复元素
// （通过 itemKey 提取的键判重），且元素数量不超过 maxSize。
func validateSet[T any, K comparable](slice []T, maxSize int, validateItem func(item T, fldPath *field.Path) field.ErrorList, itemKey func(T) K, fldPath *field.Path, opts ...validationOption) field.ErrorList {
	if maxSize >= 0 && len(slice) > maxSize {
		// 把整个字段内容写进错误信息通常会太长，尤其在元素数量已经超限时。
		// 因此这里只报告实际元素数量，而不输出字段内容。
		err := field.TooMany(fldPath, len(slice), maxSize).WithOrigin("maxItems")
		if slices.Contains(opts, sizeCovered) {
			err = err.MarkCoveredByDeclarative()
		}
		// 长度检查不通过时直接短路返回，避免继续处理超大输入造成 DoS。
		return field.ErrorList{err}
	}

	allErrs := validateItems(slice, validateItem, fldPath, opts...)

	allItems := sets.New[K]()
	for i, item := range slice {
		idxPath := fldPath.Index(i)
		key := itemKey(item)
		childPath := idxPath
		if allItems.Has(key) {
			err := field.Duplicate(childPath, key)
			if slices.Contains(opts, uniquenessCovered) {
				err = err.MarkCoveredByDeclarative()
			}
			allErrs = append(allErrs, err)
		} else {
			allItems.Insert(key)
		}
	}
	return allErrs
}

// stringKey 直接把字符串元素本身作为 validateSet 的判重键。
func stringKey(item string) string {
	return item
}

// quantityKeyAsDec 使用 Quantity 的十进制（inf.Dec）表示作为 validateSet 的判重键，
// 这样不同书写形式但数值相同的 Quantity 也能被识别为重复。
func quantityKeyAsDec(item apiresource.Quantity) string {
	return item.AsDec().String()
}

// quantityKeyInt 使用 Quantity 的十进制整数值（Value()）作为 validateSet 的判重键。
func quantityKeyInt(item apiresource.Quantity) string {
	return strconv.FormatInt(item.Value(), 10)
}

// validateMap 校验 Map 的键、值以及元素数量上限。
// maxSize 为负数时禁用长度检查。
//
// 超过 truncateKeyLen 的键会在中间被截断以控制错误信息长度；若该上限过小则会被调大，
// 因为多显示一些细节是可接受的。截断仅用于构造错误路径，
// 键本身的合法性必须由 validateKey 回调函数负责校验。
func validateMap[K ~string, T any](m map[K]T, maxSize, truncateKeyLen int, validateKey func(K, *field.Path) field.ErrorList, validateItem func(T, *field.Path) field.ErrorList, fldPath *field.Path, opts ...validationOption) field.ErrorList {
	var allErrs field.ErrorList
	if maxSize >= 0 && len(m) > maxSize {
		allErrs = append(allErrs, field.TooMany(fldPath, len(m), maxSize))
		// 长度检查不通过时直接短路返回，避免继续处理超大输入造成 DoS。
		return allErrs
	}
	for key, item := range m {
		keyPath := fldPath.Key(truncateIfTooLong(string(key), truncateKeyLen))

		keyValidationErrors := validateKey(key, fldPath)
		if slices.Contains(opts, keysCovered) {
			keyValidationErrors = keyValidationErrors.MarkCoveredByDeclarative()
		}
		allErrs = append(allErrs, keyValidationErrors...)
		allErrs = append(allErrs, validateItem(item, keyPath)...)
	}
	return allErrs
}

func truncateIfTooLong(str string, maxLen int) string {
	// 调用方传入的限制过于严格，这里把长度调大到合理值，
	// 以便错误信息能包含足够的上下文（见 PR 讨论链接）。
	if maxLen < 16 {
		maxLen = 16
	}
	if len(str) <= maxLen {
		return str
	}
	ellipsis := "..."
	remaining := maxLen - len(ellipsis)
	return str[0:(remaining+1)/2] + ellipsis + str[len(str)-remaining/2:]
}

func validateDeviceStatus(device resource.AllocatedDeviceStatus, fldPath *field.Path, allocatedDevices sets.Set[structured.SharedDeviceID]) field.ErrorList {
	var allErrs field.ErrorList
	allErrs = append(allErrs, validateDriverName(device.Driver, fldPath.Child("driver"))...)
	allErrs = append(allErrs, validatePoolName(device.Pool, fldPath.Child("pool"))...)
	allErrs = append(allErrs, validateDeviceName(device.Device, fldPath.Child("device"))...)
	if device.ShareID != nil {
		allErrs = append(allErrs, validateUID(*device.ShareID, fldPath.Child("shareID")).MarkCoveredByDeclarative()...)
	}
	deviceID := structured.MakeDeviceID(device.Driver, device.Pool, device.Device)
	sharedDeviceID := structured.MakeSharedDeviceID(deviceID, (*types.UID)(device.ShareID))
	if !allocatedDevices.Has(sharedDeviceID) {
		allErrs = append(allErrs, field.Invalid(fldPath, sharedDeviceID, "must be an allocated device in the claim"))
	}
	if len(device.Conditions) > resource.AllocatedDeviceStatusMaxConditions {
		allErrs = append(allErrs, field.TooMany(fldPath.Child("conditions"), len(device.Conditions), resource.AllocatedDeviceStatusMaxConditions))
	}
	allErrs = append(allErrs, metav1validation.ValidateConditions(device.Conditions, fldPath.Child("conditions"))...)
	if device.Data != nil && len(device.Data.Raw) > 0 { // Data 是可选字段。
		allErrs = append(allErrs, validateRawExtension(*device.Data, fldPath.Child("data"), false, resource.AllocatedDeviceStatusDataMaxLength)...)
	}
	allErrs = append(allErrs, validateNetworkDeviceData(device.NetworkData, fldPath.Child("networkData"))...)
	return allErrs
}

// validateRawExtension 按 PR #125549 的规则校验 RawExtension：
// 内容必须是合法的 JSON 对象、非空，并且在新建时不能超过长度上限
// （历史存量数据予以豁免）。
func validateRawExtension(rawExtension runtime.RawExtension, fldPath *field.Path, stored bool, rawExtensionMaxLength int) field.ErrorList {
	var allErrs field.ErrorList
	var v any
	if len(rawExtension.Raw) == 0 {
		allErrs = append(allErrs, field.Required(fldPath, ""))
	} else if !stored && len(rawExtension.Raw) > rawExtensionMaxLength {
		// 数据过大时甚至不会尝试解析，直接报长度错误。
		// 该限制只在创建时生效：由于上限是 1.32 才引入的，
		// 存量数据被豁免（grand-fathered），这也意味着该上限未来仍可调整。
		allErrs = append(allErrs, field.TooLong(fldPath, "" /* unused */, rawExtensionMaxLength))
	} else if err := json.Unmarshal(rawExtension.Raw, &v); err != nil {
		allErrs = append(allErrs, field.Invalid(fldPath, "<value omitted>", fmt.Sprintf("error parsing data as JSON: %v", err.Error())))
	} else if v == nil {
		allErrs = append(allErrs, field.Required(fldPath, ""))
	} else if _, isObject := v.(map[string]any); !isObject {
		allErrs = append(allErrs, field.Invalid(fldPath, "<value omitted>", "must be a valid JSON object"))
	}
	return allErrs
}

func validateNetworkDeviceData(networkDeviceData *resource.NetworkDeviceData, fldPath *field.Path) field.ErrorList {
	var allErrs field.ErrorList
	if networkDeviceData == nil {
		return allErrs
	}

	if len(networkDeviceData.InterfaceName) > resource.NetworkDeviceDataInterfaceNameMaxLength {
		allErrs = append(allErrs, field.TooLong(fldPath.Child("interfaceName"), "" /* unused */, resource.NetworkDeviceDataInterfaceNameMaxLength).WithOrigin("maxBytes").MarkCoveredByDeclarative())
	}

	if len(networkDeviceData.HardwareAddress) > resource.NetworkDeviceDataHardwareAddressMaxLength {
		allErrs = append(allErrs, field.TooLong(fldPath.Child("hardwareAddress"), "" /* unused */, resource.NetworkDeviceDataHardwareAddressMaxLength).WithOrigin("maxBytes").MarkCoveredByDeclarative())
	}

	allErrs = append(allErrs, validateSet(networkDeviceData.IPs, resource.NetworkDeviceDataMaxIPs,
		func(address string, fldPath *field.Path) field.ErrorList {
			return validation.IsValidInterfaceAddress(fldPath, address)
		}, stringKey, fldPath.Child("ips"), sizeCovered, uniquenessCovered)...)
	return allErrs
}

// ValidateDeviceTaintRule 判断一个 DeviceTaintRule（设备污点规则）对象是否合法。
// 会校验元数据和规则规格（设备选择器与污点定义）。
func ValidateDeviceTaintRule(deviceTaint *resource.DeviceTaintRule) field.ErrorList {
	allErrs := corevalidation.ValidateObjectMeta(&deviceTaint.ObjectMeta, false, apimachineryvalidation.NameIsDNSSubdomain, field.NewPath("metadata"))
	allErrs = append(allErrs, validateDeviceTaintRuleSpec(&deviceTaint.Spec, nil, field.NewPath("spec"))...)
	return allErrs
}

// ValidateDeviceTaintRuleUpdate 判断一次 DeviceTaintRule 更新是否合法。
// 规格校验会同时传入旧值，以便对未变化的字段跳过重复校验。
func ValidateDeviceTaintRuleUpdate(deviceTaint, oldDeviceTaint *resource.DeviceTaintRule) field.ErrorList {
	allErrs := corevalidation.ValidateObjectMetaUpdate(&deviceTaint.ObjectMeta, &oldDeviceTaint.ObjectMeta, field.NewPath("metadata"))
	allErrs = append(allErrs, validateDeviceTaintRuleSpec(&deviceTaint.Spec, &oldDeviceTaint.Spec, field.NewPath("spec"))...)
	return allErrs
}

func validateDeviceTaintRuleSpec(spec, oldSpec *resource.DeviceTaintRuleSpec, fldPath *field.Path) field.ErrorList {
	var allErrs field.ErrorList

	var oldFilter *resource.DeviceTaintSelector
	if oldSpec != nil {
		oldFilter = oldSpec.DeviceSelector // +k8s:verify-mutation:reason=clone
	}
	allErrs = append(allErrs, validateDeviceTaintSelector(spec.DeviceSelector, oldFilter, fldPath.Child("deviceSelector"))...)
	var oldTaint *resource.DeviceTaint
	if oldSpec != nil {
		oldTaint = &oldSpec.Taint // +k8s:verify-mutation:reason=clone
	}
	allErrs = append(allErrs, validateDeviceTaint(spec.Taint, oldTaint, fldPath.Child("taint"))...)
	return allErrs
}

func validateDeviceTaintSelector(filter, oldFilter *resource.DeviceTaintSelector, fldPath *field.Path) field.ErrorList {
	var allErrs field.ErrorList

	if filter == nil {
		return allErrs
	}
	if filter.Driver != nil {
		allErrs = append(allErrs, validateDriverName(*filter.Driver, fldPath.Child("driver"))...)
	}
	if filter.Pool != nil {
		allErrs = append(allErrs, validatePoolName(*filter.Pool, fldPath.Child("pool"))...)
	}
	if filter.Device != nil {
		allErrs = append(allErrs, validateDeviceName(*filter.Device, fldPath.Child("device"))...)
	}

	return allErrs
}

var validDeviceTolerationOperators = []resource.DeviceTolerationOperator{resource.DeviceTolerationOpEqual, resource.DeviceTolerationOpExists}
var validDeviceTaintEffects = sets.New(resource.DeviceTaintEffectNoSchedule, resource.DeviceTaintEffectNoExecute, resource.DeviceTaintEffectNone)

func validateDeviceTaint(taint resource.DeviceTaint, oldTaint *resource.DeviceTaint, fldPath *field.Path) field.ErrorList {
	var allErrs field.ErrorList

	allErrs = append(allErrs, metav1validation.ValidateLabelName(taint.Key, fldPath.Child("key"))...) // 包含非空检查。
	if taint.Value != "" {
		allErrs = append(allErrs, validateLabelValue(taint.Value, fldPath.Child("value"))...)
	}
	if oldTaint == nil || oldTaint.Effect != taint.Effect {
		switch {
		case taint.Effect == "":
			allErrs = append(allErrs, field.Required(fldPath.Child("effect"), "").MarkCoveredByDeclarative()) // 在污点中该字段为必填项。
		case !validDeviceTaintEffects.Has(taint.Effect):
			allErrs = append(allErrs, field.NotSupported(fldPath.Child("effect"), taint.Effect, sets.List(validDeviceTaintEffects)).MarkCoveredByDeclarative())
		}
	}

	return allErrs
}

func validateDeviceToleration(toleration resource.DeviceToleration, fldPath *field.Path) field.ErrorList {
	var allErrs field.ErrorList

	if toleration.Key != "" {
		allErrs = append(allErrs, metav1validation.ValidateLabelName(toleration.Key, fldPath.Child("key")).MarkCoveredByDeclarative()...)
	}
	switch toleration.Operator {
	case resource.DeviceTolerationOpExists:
		if toleration.Value != "" {
			allErrs = append(allErrs, field.Invalid(fldPath.Child("value"), toleration.Value, "must be empty for operator `Exists`"))
		}
	case resource.DeviceTolerationOpEqual:
		allErrs = append(allErrs, validateLabelValue(toleration.Value, fldPath.Child("value"))...)
	case "":
		allErrs = append(allErrs, field.Required(fldPath.Child("operator"), "").MarkCoveredByDeclarative())
	default:
		allErrs = append(allErrs, field.NotSupported(fldPath.Child("operator"), toleration.Operator, validDeviceTolerationOperators).MarkCoveredByDeclarative())
	}
	switch {
	case toleration.Effect == "":
		// 对容忍（toleration）而言 effect 是可选字段，为空是合法的。
	case !validDeviceTaintEffects.Has(toleration.Effect):
		allErrs = append(allErrs, field.NotSupported(fldPath.Child("effect"), toleration.Effect, sets.List(validDeviceTaintEffects)).MarkCoveredByDeclarative())
	}

	return allErrs
}

func validateLabelValue(value string, fldPath *field.Path) field.ErrorList {
	var allErrs field.ErrorList

	// metav1validation 中没有可直接复用的 ValidateLabelValue，
	// 因此这里调用 util/validation 中的 IsValidLabelValue。
	for _, msg := range validation.IsValidLabelValue(value) {
		allErrs = append(allErrs, field.Invalid(fldPath, value, msg))
	}

	return allErrs
}

func validateDeviceBindingParameters(bindingConditions, bindingFailureConditions []string, fldPath *field.Path) field.ErrorList {
	var allErrs field.ErrorList
	allErrs = append(allErrs, validateSlice(bindingConditions, resource.BindingConditionsMaxSize,
		metav1validation.ValidateLabelName, fldPath.Child("bindingConditions"), sizeCovered)...)

	allErrs = append(allErrs, validateSlice(bindingFailureConditions, resource.BindingFailureConditionsMaxSize,
		metav1validation.ValidateLabelName, fldPath.Child("bindingFailureConditions"), sizeCovered)...)

	// 确保 BindingConditions 与 BindingFailureConditions 内部都没有重复项，
	// 并且两个集合之间没有交集（同一个条件不能既表示成功又表示失败）。
	conditionsSet := sets.New[string]()
	for i, condition := range bindingConditions {
		if conditionsSet.Has(condition) {
			allErrs = append(allErrs, field.Duplicate(fldPath.Child("bindingConditions").Index(i), condition))
		} else {
			conditionsSet.Insert(condition)
		}
	}
	failureConditionsSet := sets.New[string]()
	for i, condition := range bindingFailureConditions {
		if failureConditionsSet.Has(condition) {
			allErrs = append(allErrs, field.Duplicate(fldPath.Child("bindingFailureConditions").Index(i), condition))
		} else {
			failureConditionsSet.Insert(condition)
		}
		if sets.New(bindingConditions...).Has(condition) {
			allErrs = append(allErrs, field.Invalid(fldPath.Child("bindingFailureConditions").Index(i), condition, "bindingFailureConditions must not overlap with bindingConditions"))
		}
	}

	if len(bindingConditions) == 0 && len(bindingFailureConditions) > 0 {
		allErrs = append(allErrs, field.Invalid(fldPath.Child("bindingConditions"), bindingConditions, "bindingConditions are required to use bindingFailureConditions"))
	}
	if len(bindingFailureConditions) == 0 && len(bindingConditions) > 0 {
		allErrs = append(allErrs, field.Invalid(fldPath.Child("bindingFailureConditions"), bindingFailureConditions, "bindingFailureConditions are required to use bindingConditions"))
	}

	return allErrs
}

// ValidateDeviceTaintRuleStatusUpdate 判断一次 DeviceTaintRule 状态更新是否合法。
// 会校验元数据更新、status.conditions 的合法性以及条件数量是否超限。
func ValidateDeviceTaintRuleStatusUpdate(rule, oldRule *resource.DeviceTaintRule) field.ErrorList {
	var allErrs field.ErrorList

	fldPath := field.NewPath("status")
	allErrs = corevalidation.ValidateObjectMetaUpdate(&rule.ObjectMeta, &oldRule.ObjectMeta, field.NewPath("metadata")) // 包含对非法名称变更的检查。
	allErrs = append(allErrs, metav1validation.ValidateConditions(rule.Status.Conditions, fldPath.Child("conditions"))...)
	if len(rule.Status.Conditions) > resource.DeviceTaintRuleStatusMaxConditions {
		allErrs = append(allErrs, field.TooMany(fldPath.Child("conditions"), len(rule.Status.Conditions), resource.DeviceTaintRuleStatusMaxConditions))
	}

	return allErrs
}
