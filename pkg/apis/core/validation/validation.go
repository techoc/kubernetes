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

package validation

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net"
	"path"
	"path/filepath"
	"reflect"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"unicode"
	"unicode/utf8"

	netutils "k8s.io/utils/net"

	v1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/operation"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/api/validate"
	"k8s.io/apimachinery/pkg/api/validate/content"
	apimachineryvalidation "k8s.io/apimachinery/pkg/api/validation"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	unversionedvalidation "k8s.io/apimachinery/pkg/apis/meta/v1/validation"
	"k8s.io/apimachinery/pkg/conversion"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/diff"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/apimachinery/pkg/util/validation/field"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	utilsysctl "k8s.io/component-helpers/node/util/sysctl"
	resourcehelper "k8s.io/component-helpers/resource"
	schedulinghelper "k8s.io/component-helpers/scheduling/corev1"
	kubeletapis "k8s.io/kubelet/pkg/apis"
	"k8s.io/kubernetes/pkg/apis/certificates"

	apiservice "k8s.io/kubernetes/pkg/api/service"
	podutil "k8s.io/kubernetes/pkg/api/v1/pod"
	"k8s.io/kubernetes/pkg/apis/core"
	"k8s.io/kubernetes/pkg/apis/core/helper"
	"k8s.io/kubernetes/pkg/apis/core/helper/qos"
	podshelper "k8s.io/kubernetes/pkg/apis/core/pods"
	corev1 "k8s.io/kubernetes/pkg/apis/core/v1"

	"k8s.io/kubernetes/pkg/capabilities"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/pkg/fieldpath"
)

const isNegativeErrorMsg string = apimachineryvalidation.IsNegativeErrorMsg
const isInvalidQuotaResource string = `must be a standard resource for quota`
const fieldImmutableErrorMsg string = apimachineryvalidation.FieldImmutableErrorMsg
const isNotIntegerErrorMsg string = `must be an integer`
const isNotPositiveErrorMsg string = `must be greater than zero`
const resourceStatusClaimPrefix = "claim:"

var pdPartitionErrorMsg string = validation.InclusiveRangeError(1, 255)
var fileModeErrorMsg = "must be a number between 0 and 0777 (octal), both inclusive"
var emptyDirModeErrorMsg = "must be a number between 0 and 01777 (octal), both inclusive"

// BannedOwners 是不允许作为 owner 的对象黑名单。
var BannedOwners = apimachineryvalidation.BannedOwners

// nodeDeclaredFeatureRegexp 定义了特性名称允许的格式。
// 首段必须为 UpperCamelCase；后续各段（以 '/' 分隔）
// 可以是 UpperCamelCase 或 lowerCamelCase。
var nodeDeclaredFeatureRegexp = regexp.MustCompile(`^[A-Z][a-zA-Z0-9]*(\/[a-zA-Z][a-zA-Z0-9]*)*$`)
var iscsiInitiatorIqnRegex = regexp.MustCompile(`iqn\.\d{4}-\d{2}\.([[:alnum:]-.]+)(:[^,;*&$|\s]+)$`)
var iscsiInitiatorEuiRegex = regexp.MustCompile(`^eui.[[:alnum:]]{16}$`)
var iscsiInitiatorNaaRegex = regexp.MustCompile(`^naa.[[:alnum:]]{32}$`)

var allowedEphemeralContainerFields = map[string]bool{
	"Name":                     true,
	"Image":                    true,
	"Command":                  true,
	"Args":                     true,
	"WorkingDir":               true,
	"Ports":                    false,
	"EnvFrom":                  true,
	"Env":                      true,
	"Resources":                false,
	"VolumeMounts":             true,
	"VolumeDevices":            true,
	"LivenessProbe":            false,
	"ReadinessProbe":           false,
	"StartupProbe":             false,
	"Lifecycle":                false,
	"TerminationMessagePath":   true,
	"TerminationMessagePolicy": true,
	"ImagePullPolicy":          true,
	"SecurityContext":          true,
	"Stdin":                    true,
	"StdinOnce":                true,
	"TTY":                      true,
}

// validOS 保存 pod spec 中合法的操作系统集合。
// 当前合法值为 linux、windows。
// 未来可以扩展为
// https://github.com/opencontainers/runtime-spec/blob/master/config.md#platform-specific-configuration
// 中列出的取值。
var validOS = sets.New(core.Linux, core.Windows)

// MaxPodEvictionResponders 指定 Pod 的 .spec.evictionResponders 字段中
// 最多允许的 EvictionResponders 数量。其他使用 responders 的消费方
// 可能调高该数值，为新增 responder 预留空间。
const MaxPodEvictionResponders = 10

// ValidateHasLabel 校验 metav1.ObjectMeta 必须包含指定 key 且值等于 expectedValue 的标签。
func ValidateHasLabel(meta metav1.ObjectMeta, fldPath *field.Path, key, expectedValue string) field.ErrorList {
	allErrs := field.ErrorList{}
	actualValue, found := meta.Labels[key]
	if !found {
		allErrs = append(allErrs, field.Required(fldPath.Child("labels").Key(key),
			fmt.Sprintf("must be '%s'", expectedValue)))
		return allErrs
	}
	if actualValue != expectedValue {
		allErrs = append(allErrs, field.Invalid(fldPath.Child("labels").Key(key), meta.Labels,
			fmt.Sprintf("must be '%s'", expectedValue)))
	}
	return allErrs
}

// ValidateAnnotations 校验一组注解是否定义正确（键、值格式与整体大小）。
func ValidateAnnotations(annotations map[string]string, fldPath *field.Path) field.ErrorList {
	return apimachineryvalidation.ValidateAnnotations(annotations, fldPath)
}

// ValidateUserAnnotations 校验 UserAnnotations（用户自定义注解）的键是否为合法的域名前缀形式，并检查总大小。
func ValidateUserAnnotations(userAnnotations map[string]string, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	for k := range userAnnotations {
		// 大小写不敏感，因此先转为小写再检查。
		allErrs = append(allErrs, validation.IsDomainPrefixedKey(fldPath, strings.ToLower(k))...)
	}
	if err := apimachineryvalidation.ValidateAnnotationsSize(userAnnotations); err != nil {
		allErrs = append(allErrs, field.TooLong(fldPath, "" /*unused*/, apimachineryvalidation.TotalAnnotationSizeLimitB))
	}
	return allErrs
}

// ValidateDNS1123Label 校验 value 是否为合法的 DNS-1123 标签（短名称），并将错误附带 format=k8s-short-name 来源。
func ValidateDNS1123Label(value string, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	for _, msg := range validation.IsDNS1123Label(value) {
		allErrs = append(allErrs, field.Invalid(fldPath, value, msg).WithOrigin("format=k8s-short-name"))
	}
	return allErrs
}

// ValidateQualifiedName 校验 value 是否符合 Kubernetes 所称的“限定名”（qualified name）格式。
func ValidateQualifiedName(value string, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	for _, msg := range validation.IsQualifiedName(value) {
		allErrs = append(allErrs, field.Invalid(fldPath, value, msg).WithOrigin("format=qualified-name"))
	}
	return allErrs
}

// ValidateDNS1123SubdomainWithUnderScore 校验 value 是否为合法 DNS 子域名，但允许字符串中出现下划线。
func ValidateDNS1123SubdomainWithUnderScore(value string, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	for _, msg := range validation.IsDNS1123SubdomainWithUnderscore(value) {
		allErrs = append(allErrs, field.Invalid(fldPath, value, msg).WithOrigin("format=k8s-dns-subdomain-with-underscore"))
	}
	return allErrs
}

// ValidateDNS1123Subdomain 校验 value 是否为合法的 DNS 子域名（长名称），错误附带 format=k8s-long-name 来源。
func ValidateDNS1123Subdomain(value string, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	for _, msg := range validation.IsDNS1123Subdomain(value) {
		allErrs = append(allErrs, field.Invalid(fldPath, value, msg).WithOrigin("format=k8s-long-name"))
	}
	return allErrs
}

// ValidatePodSpecificAnnotations 校验 Pod 专有注解：镜像 Pod 注解要求 spec.nodeName，同时解析 tolerations 注解并校验其合法性。
func ValidatePodSpecificAnnotations(annotations map[string]string, spec *core.PodSpec, fldPath *field.Path, opts PodValidationOptions) field.ErrorList {
	allErrs := field.ErrorList{}

	if value, isMirror := annotations[core.MirrorPodAnnotationKey]; isMirror {
		if len(spec.NodeName) == 0 {
			allErrs = append(allErrs, field.Invalid(fldPath.Key(core.MirrorPodAnnotationKey), value, "must set spec.nodeName if mirror pod annotation is set"))
		}
	}

	if annotations[core.TolerationsAnnotationKey] != "" {
		allErrs = append(allErrs, ValidateTolerationsInPodAnnotations(annotations, fldPath, opts)...)
	}

	if !opts.AllowInvalidPodDeletionCost {
		if _, err := helper.GetDeletionCostFromPodAnnotations(annotations); err != nil {
			allErrs = append(allErrs, field.Invalid(fldPath.Key(core.PodDeletionCost), annotations[core.PodDeletionCost], "must be a 32bit integer"))
		}
	}

	allErrs = append(allErrs, ValidateSeccompPodAnnotations(annotations, fldPath)...)
	allErrs = append(allErrs, ValidateAppArmorPodAnnotations(annotations, spec, fldPath)...)

	return allErrs
}

// ValidateTolerationsInPodAnnotations 校验 Pod.Annotations 中序列化后的 tolerations 数据是否合法。
func ValidateTolerationsInPodAnnotations(annotations map[string]string, fldPath *field.Path, opts PodValidationOptions) field.ErrorList {
	allErrs := field.ErrorList{}

	tolerations, err := helper.GetTolerationsFromPodAnnotations(annotations)
	if err != nil {
		allErrs = append(allErrs, field.Invalid(fldPath, core.TolerationsAnnotationKey, err.Error()))
		return allErrs
	}

	if len(tolerations) > 0 {
		allErrs = append(allErrs, ValidateTolerations(tolerations, fldPath.Child(core.TolerationsAnnotationKey), opts)...)
	}

	return allErrs
}

// ValidatePodSpecificAnnotationUpdates 校验 Pod 专有注解的更新：禁止移除或修改 AppArmor 旧注解与镜像 Pod 注解。
func ValidatePodSpecificAnnotationUpdates(newPod *core.Pod, oldAnnotations map[string]string, fldPath *field.Path, opts PodValidationOptions) field.ErrorList {
	allErrs := field.ErrorList{}
	newAnnotations := newPod.Annotations
	for k, oldVal := range oldAnnotations {
		if newVal, exists := newAnnotations[k]; exists && newVal == oldVal {
			continue // 值未变化。
		}
		if strings.HasPrefix(k, v1.DeprecatedAppArmorBetaContainerAnnotationKeyPrefix) {
			allErrs = append(allErrs, field.Forbidden(fldPath.Key(k), "may not remove or update AppArmor annotations"))
		}
		if k == core.MirrorPodAnnotationKey {
			allErrs = append(allErrs, field.Forbidden(fldPath.Key(k), "may not remove or update mirror pod annotation"))
		}
	}
	// 检查新增项
	for k := range newAnnotations {
		if _, ok := oldAnnotations[k]; ok {
			continue // 无变化。
		}
		if strings.HasPrefix(k, v1.DeprecatedAppArmorBetaContainerAnnotationKeyPrefix) {
			allErrs = append(allErrs, field.Forbidden(fldPath.Key(k), "may not add AppArmor annotations"))
		}
		if k == core.MirrorPodAnnotationKey {
			allErrs = append(allErrs, field.Forbidden(fldPath.Key(k), "may not add mirror pod annotation"))
		}
	}
	allErrs = append(allErrs, ValidatePodSpecificAnnotations(newAnnotations, &newPod.Spec, fldPath, opts)...)
	return allErrs
}

// ValidateEndpointsSpecificAnnotations 校验 Endpoints 专有注解；当前无实际检查项，保留扩展点。
func ValidateEndpointsSpecificAnnotations(annotations map[string]string, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	return allErrs
}

// ValidateNameFunc 校验给定的名称对特定资源类型是否合法。
// 并非所有资源都使用相同的名称校验规则。若名称后还会追加取值，
// 则 Prefix 为 true。若名称不合法，返回一个列表，逐项描述该值
// 不符合的校验特征；否则返回空列表或 nil。
type ValidateNameFunc apimachineryvalidation.ValidateNameFunc

// ValidateNameFuncWithErrors 校验给定的名称对特定资源类型是否合法。
type ValidateNameFuncWithErrors = apimachineryvalidation.ValidateNameFuncWithErrors

// ValidatePodName 可用于检查给定的 pod 名称是否合法。
// Prefix 表示该名称将用于生成（generateName）场景，此时
// 允许结尾为短横线。
var ValidatePodName = apimachineryvalidation.NameIsDNSSubdomain

// ValidateServiceName 可用于检查给定的 service 名称是否合法。
// Prefix 表示该名称将用于生成（generateName）场景，此时
// 允许结尾为短横线。
var ValidateServiceName = apimachineryvalidation.NameIsDNS1035Label

// ValidateNodeName 可用于检查给定的 node 名称是否合法。
// Prefix 表示该名称将用于生成（generateName）场景，此时
// 允许结尾为短横线。
var ValidateNodeName = apimachineryvalidation.NameIsDNSSubdomain

// ValidateNamespaceName 可用于检查给定的 namespace 名称是否合法。
// Prefix 表示该名称将用于生成（generateName）场景，此时
// 允许结尾为短横线。
var ValidateNamespaceName = apimachineryvalidation.ValidateNamespaceName

// ValidateLimitRangeName 可用于检查给定的 LimitRange 名称是否合法。
// Prefix 表示该名称将用于生成（generateName）场景，此时
// 允许结尾为短横线。
var ValidateLimitRangeName = apimachineryvalidation.NameIsDNSSubdomain

// ValidateResourceQuotaName 可用于检查给定的 ResourceQuota 名称是否合法。
// Prefix 表示该名称将用于生成（generateName）场景，此时
// 允许结尾为短横线。
var ValidateResourceQuotaName = apimachineryvalidation.NameIsDNSSubdomain

// ValidateSecretName 可用于检查给定的 secret 名称是否合法。
// Prefix 表示该名称将用于生成（generateName）场景，此时
// 允许结尾为短横线。
var ValidateSecretName = apimachineryvalidation.NameIsDNSSubdomain

// ValidateServiceAccountName 可用于检查给定的 ServiceAccount 名称是否合法。
// Prefix 表示该名称将用于生成（generateName）场景，此时
// 允许结尾为短横线。
var ValidateServiceAccountName = apimachineryvalidation.ValidateServiceAccountName

// ValidateEndpointsName 可用于检查给定的 endpoints 名称是否合法。
// Prefix 表示该名称将用于生成（generateName）场景，此时
// 允许结尾为短横线。
var ValidateEndpointsName = apimachineryvalidation.NameIsDNSSubdomain

// ValidateClassName 可用于检查给定的 class 名称是否合法。
// 定义在本文件是为了避免 pkg/apis/storage/validation
// （逻辑上应放置的位置）与本文件之间的循环导入。
var ValidateClassName = apimachineryvalidation.NameIsDNSSubdomain

// ValidatePriorityClassName 可用于检查给定的 priority class 名称是否合法。
var ValidatePriorityClassName = apimachineryvalidation.NameIsDNSSubdomain

// ValidateResourceClaimName 可用于检查给定的 ResourceClaim 名称是否合法。
var ValidateResourceClaimName = apimachineryvalidation.NameIsDNSSubdomain

// ValidateResourceClaimTemplateName 可用于检查给定的 ResourceClaimTemplate 名称是否合法。
var ValidateResourceClaimTemplateName = apimachineryvalidation.NameIsDNSSubdomain

// ValidatePodGroupName 可用于检查给定的 PodGroup 名称是否合法。
var ValidatePodGroupName = apimachineryvalidation.NameIsDNSSubdomain

// ValidateRuntimeClassName 校验给定的 RuntimeClass 名称是否合法（DNS 子域名规则）。
func ValidateRuntimeClassName(name string, fldPath *field.Path) field.ErrorList {
	var allErrs field.ErrorList
	for _, msg := range apimachineryvalidation.NameIsDNSSubdomain(name, false) {
		allErrs = append(allErrs, field.Invalid(fldPath, name, msg))
	}
	return allErrs
}

// validateOverhead 校验 Pod Overhead 资源列表是否合法，复用容器资源需求的校验逻辑。
func validateOverhead(overhead core.ResourceList, fldPath *field.Path, opts PodValidationOptions) field.ErrorList {
	// 复用 ResourceRequirements 的校验逻辑
	return ValidateContainerResourceRequirements(&core.ResourceRequirements{Limits: overhead}, nil, fldPath, opts)
}

// ValidateNonnegativeField 校验给定的 int64 值不为负数。
func ValidateNonnegativeField(value int64, fldPath *field.Path) field.ErrorList {
	return apimachineryvalidation.ValidateNonnegativeField(value, fldPath)
}

// ValidateNonnegativeQuantity 校验给定的 Quantity 不为负数。
func ValidateNonnegativeQuantity(value resource.Quantity, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	if value.Cmp(resource.Quantity{}) < 0 {
		allErrs = append(allErrs, field.Invalid(fldPath, value.String(), isNegativeErrorMsg))
	}
	return allErrs
}

// ValidatePositiveField 校验给定的 int64 值大于零。
func ValidatePositiveField(value int64, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	if value <= 0 {
		allErrs = append(allErrs, field.Invalid(fldPath, value, isNotPositiveErrorMsg).WithOrigin("minimum"))
	}
	return allErrs
}

// ValidatePositiveQuantityValue 校验给定的 Quantity 大于零。
func ValidatePositiveQuantityValue(value resource.Quantity, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	if value.Cmp(resource.Quantity{}) <= 0 {
		allErrs = append(allErrs, field.Invalid(fldPath, value.String(), isNotPositiveErrorMsg))
	}
	return allErrs
}

// ValidateImmutableField 校验不可变字段在更新前后保持一致。
func ValidateImmutableField(newVal, oldVal interface{}, fldPath *field.Path) field.ErrorList {
	return apimachineryvalidation.ValidateImmutableField(newVal, oldVal, fldPath)
}

// ValidateImmutableAnnotation 校验指定注解在更新前后保持不变，否则报错。
func ValidateImmutableAnnotation(newVal string, oldVal string, annotation string, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}

	if oldVal != newVal {
		allErrs = append(allErrs, field.Invalid(fldPath.Child("annotations", annotation), newVal, fieldImmutableErrorMsg))
	}
	return allErrs
}

// ValidateObjectMetaWithOpts 在对象创建时校验元数据，假定名称生成已完成；额外校验 finalizer 名称合法性。
func ValidateObjectMetaWithOpts(meta *metav1.ObjectMeta, isNamespaced bool, nameFn ValidateNameFuncWithErrors, fldPath *field.Path) field.ErrorList {
	allErrs := apimachineryvalidation.ValidateObjectMetaWithOpts(meta, isNamespaced, nameFn, fldPath)
	// 额外执行 finalizer 名称的检查
	for i := range meta.Finalizers {
		allErrs = append(allErrs, validateKubeFinalizerName(string(meta.Finalizers[i]), fldPath.Child("finalizers").Index(i))...)
	}
	return allErrs
}

// ValidateObjectMeta 在对象创建时校验元数据，假定名称生成已完成；额外校验 finalizer 名称合法性。
// TODO: 移除散落在具体资源校验（如 ValidatePodUpdate）中对本方法的调用。
func ValidateObjectMeta(meta *metav1.ObjectMeta, requiresNamespace bool, nameFn ValidateNameFunc, fldPath *field.Path) field.ErrorList {
	allErrs := apimachineryvalidation.ValidateObjectMeta(meta, requiresNamespace, apimachineryvalidation.ValidateNameFunc(nameFn), fldPath)
	// 额外执行 finalizer 名称的检查
	for i := range meta.Finalizers {
		allErrs = append(allErrs, validateKubeFinalizerName(string(meta.Finalizers[i]), fldPath.Child("finalizers").Index(i))...)
	}
	return allErrs
}

// ValidateObjectMetaUpdate 在对象更新时校验元数据，并额外校验所有 finalizer 名称的合法性。
func ValidateObjectMetaUpdate(newMeta, oldMeta *metav1.ObjectMeta, fldPath *field.Path) field.ErrorList {
	allErrs := apimachineryvalidation.ValidateObjectMetaUpdate(newMeta, oldMeta, fldPath)
	// 额外执行 finalizer 名称的检查
	for i := range newMeta.Finalizers {
		allErrs = append(allErrs, validateKubeFinalizerName(string(newMeta.Finalizers[i]), fldPath.Child("finalizers").Index(i))...)
	}

	return allErrs
}

// ValidateVolumes 校验 Pod 的卷列表：收集所有卷名并检查重复，同时识别临时 PVC 的名称冲突。
func ValidateVolumes(volumes []core.Volume, podMeta *metav1.ObjectMeta, fldPath *field.Path, opts PodValidationOptions) (map[string]core.VolumeSource, field.ErrorList) {
	allErrs := field.ErrorList{}

	allNames := sets.Set[string]{}
	allCreatedPVCs := sets.Set[string]{}
	// 确定该 Pod 会创建哪些 PVC。这需要 Pod 的精确名称，
	// 缺少名称时只能跳过这项检查工作。
	if podMeta != nil && podMeta.Name != "" {
		for _, vol := range volumes {
			if vol.VolumeSource.Ephemeral != nil {
				allCreatedPVCs.Insert(podMeta.Name + "-" + vol.Name)
			}
		}
	}
	vols := make(map[string]core.VolumeSource)
	for i, vol := range volumes {
		idxPath := fldPath.Index(i)
		namePath := idxPath.Child("name")
		el := validateVolumeSource(&vol.VolumeSource, idxPath, vol.Name, podMeta, opts)
		if len(vol.Name) == 0 {
			el = append(el, field.Required(namePath, ""))
		} else {
			el = append(el, ValidateDNS1123Label(vol.Name, namePath)...)
		}
		if allNames.Has(vol.Name) {
			el = append(el, field.Duplicate(namePath, vol.Name))
		}
		if len(el) == 0 {
			allNames.Insert(vol.Name)
			vols[vol.Name] = vol.VolumeSource
		} else {
			allErrs = append(allErrs, el...)
		}
		// PersistentVolumeClaimSource 不应引用为临时卷而创建的 PVC，
		// 这样引用没有意义。
		if vol.PersistentVolumeClaim != nil && allCreatedPVCs.Has(vol.PersistentVolumeClaim.ClaimName) {
			allErrs = append(allErrs, field.Invalid(idxPath.Child("persistentVolumeClaim").Child("claimName"), vol.PersistentVolumeClaim.ClaimName,
				"must not reference a PVC that gets created for an ephemeral volume"))
		}
	}

	return vols, allErrs
}

// IsMatchedVolume 判断给定名称的卷是否存在于卷映射中。
func IsMatchedVolume(name string, volumes map[string]core.VolumeSource) bool {
	if _, ok := volumes[name]; ok {
		return true
	}
	return false
}

// isMatchedDevice 判断给定名称的卷是否被容器使用，并报告其是否涉及 PVC（持久化卷声明）。
func isMatchedDevice(name string, volumes map[string]core.VolumeSource) (isMatched bool, isPVC bool) {
	if source, ok := volumes[name]; ok {
		if source.PersistentVolumeClaim != nil ||
			source.Ephemeral != nil {
			return true, true
		}
		return true, false
	}
	return false, false
}

// mountNameAlreadyExists 判断挂载名称是否已存在于设备映射中（名称冲突检测）。
func mountNameAlreadyExists(name string, devices map[string]string) bool {
	if _, ok := devices[name]; ok {
		return true
	}
	return false
}

// mountPathAlreadyExists 判断挂载路径是否已存在于设备映射中（路径冲突检测）。
func mountPathAlreadyExists(mountPath string, devices map[string]string) bool {
	for _, devPath := range devices {
		if mountPath == devPath {
			return true
		}
	}

	return false
}

// deviceNameAlreadyExists 判断设备名称是否已存在于挂载映射中（名称冲突检测）。
func deviceNameAlreadyExists(name string, mounts map[string]string) bool {
	if _, ok := mounts[name]; ok {
		return true
	}
	return false
}

// devicePathAlreadyExists 判断设备路径是否已存在于挂载映射中（路径冲突检测）。
func devicePathAlreadyExists(devicePath string, mounts map[string]string) bool {
	for _, mountPath := range mounts {
		if mountPath == devicePath {
			return true
		}
	}

	return false
}

// validateVolumeSource 校验 VolumeSource：确保恰好设置一种卷类型，并逐类型校验具体参数。
func validateVolumeSource(source *core.VolumeSource, fldPath *field.Path, volName string, podMeta *metav1.ObjectMeta, opts PodValidationOptions) field.ErrorList {
	numVolumes := 0
	allErrs := field.ErrorList{}
	if source.EmptyDir != nil {
		numVolumes++
		if source.EmptyDir.SizeLimit != nil && source.EmptyDir.SizeLimit.Cmp(resource.Quantity{}) < 0 {
			allErrs = append(allErrs, field.Forbidden(fldPath.Child("emptyDir").Child("sizeLimit"), "SizeLimit field must be a valid resource quantity"))
		}
		if source.EmptyDir.Mode != nil && (*source.EmptyDir.Mode > 01777 || *source.EmptyDir.Mode < 0) {
			allErrs = append(allErrs, field.Invalid(fldPath.Child("emptyDir").Child("mode"), *source.EmptyDir.Mode, emptyDirModeErrorMsg))
		}
	}
	if source.HostPath != nil {
		if numVolumes > 0 {
			allErrs = append(allErrs, field.Forbidden(fldPath.Child("hostPath"), "may not specify more than 1 volume type"))
		} else {
			numVolumes++
			allErrs = append(allErrs, validateHostPathVolumeSource(source.HostPath, fldPath.Child("hostPath"))...)
		}
	}
	if source.GitRepo != nil {
		if numVolumes > 0 {
			allErrs = append(allErrs, field.Forbidden(fldPath.Child("gitRepo"), "may not specify more than 1 volume type"))
		} else {
			numVolumes++
			allErrs = append(allErrs, validateGitRepoVolumeSource(source.GitRepo, fldPath.Child("gitRepo"))...)
		}
	}
	if source.GCEPersistentDisk != nil {
		if numVolumes > 0 {
			allErrs = append(allErrs, field.Forbidden(fldPath.Child("gcePersistentDisk"), "may not specify more than 1 volume type"))
		} else {
			numVolumes++
			allErrs = append(allErrs, validateGCEPersistentDiskVolumeSource(source.GCEPersistentDisk, fldPath.Child("persistentDisk"))...)
		}
	}
	if source.AWSElasticBlockStore != nil {
		if numVolumes > 0 {
			allErrs = append(allErrs, field.Forbidden(fldPath.Child("awsElasticBlockStore"), "may not specify more than 1 volume type"))
		} else {
			numVolumes++
			allErrs = append(allErrs, validateAWSElasticBlockStoreVolumeSource(source.AWSElasticBlockStore, fldPath.Child("awsElasticBlockStore"))...)
		}
	}
	if source.Secret != nil {
		if numVolumes > 0 {
			allErrs = append(allErrs, field.Forbidden(fldPath.Child("secret"), "may not specify more than 1 volume type"))
		} else {
			numVolumes++
			allErrs = append(allErrs, validateSecretVolumeSource(source.Secret, fldPath.Child("secret"))...)
		}
	}
	if source.NFS != nil {
		if numVolumes > 0 {
			allErrs = append(allErrs, field.Forbidden(fldPath.Child("nfs"), "may not specify more than 1 volume type"))
		} else {
			numVolumes++
			allErrs = append(allErrs, validateNFSVolumeSource(source.NFS, fldPath.Child("nfs"))...)
		}
	}
	if source.ISCSI != nil {
		if numVolumes > 0 {
			allErrs = append(allErrs, field.Forbidden(fldPath.Child("iscsi"), "may not specify more than 1 volume type"))
		} else {
			numVolumes++
			allErrs = append(allErrs, validateISCSIVolumeSource(source.ISCSI, fldPath.Child("iscsi"))...)
		}
		if source.ISCSI.InitiatorName != nil && len(volName+":"+source.ISCSI.TargetPortal) > 64 {
			tooLongErr := "Total length of <volume name>:<iscsi.targetPortal> must be under 64 characters if iscsi.initiatorName is specified."
			allErrs = append(allErrs, field.Invalid(fldPath.Child("name"), volName, tooLongErr))
		}
	}
	if source.Glusterfs != nil {
		if numVolumes > 0 {
			allErrs = append(allErrs, field.Forbidden(fldPath.Child("glusterfs"), "may not specify more than 1 volume type"))
		} else {
			numVolumes++
			allErrs = append(allErrs, validateGlusterfsVolumeSource(source.Glusterfs, fldPath.Child("glusterfs"))...)
		}
	}
	if source.Flocker != nil {
		if numVolumes > 0 {
			allErrs = append(allErrs, field.Forbidden(fldPath.Child("flocker"), "may not specify more than 1 volume type"))
		} else {
			numVolumes++
			allErrs = append(allErrs, validateFlockerVolumeSource(source.Flocker, fldPath.Child("flocker"))...)
		}
	}
	if source.PersistentVolumeClaim != nil {
		if numVolumes > 0 {
			allErrs = append(allErrs, field.Forbidden(fldPath.Child("persistentVolumeClaim"), "may not specify more than 1 volume type"))
		} else {
			numVolumes++
			allErrs = append(allErrs, validatePersistentClaimVolumeSource(source.PersistentVolumeClaim, fldPath.Child("persistentVolumeClaim"))...)
		}
	}
	if source.RBD != nil {
		if numVolumes > 0 {
			allErrs = append(allErrs, field.Forbidden(fldPath.Child("rbd"), "may not specify more than 1 volume type"))
		} else {
			numVolumes++
			allErrs = append(allErrs, validateRBDVolumeSource(source.RBD, fldPath.Child("rbd"))...)
		}
	}
	if source.Cinder != nil {
		if numVolumes > 0 {
			allErrs = append(allErrs, field.Forbidden(fldPath.Child("cinder"), "may not specify more than 1 volume type"))
		} else {
			numVolumes++
			allErrs = append(allErrs, validateCinderVolumeSource(source.Cinder, fldPath.Child("cinder"))...)
		}
	}
	if source.CephFS != nil {
		if numVolumes > 0 {
			allErrs = append(allErrs, field.Forbidden(fldPath.Child("cephFS"), "may not specify more than 1 volume type"))
		} else {
			numVolumes++
			allErrs = append(allErrs, validateCephFSVolumeSource(source.CephFS, fldPath.Child("cephfs"))...)
		}
	}
	if source.Quobyte != nil {
		if numVolumes > 0 {
			allErrs = append(allErrs, field.Forbidden(fldPath.Child("quobyte"), "may not specify more than 1 volume type"))
		} else {
			numVolumes++
			allErrs = append(allErrs, validateQuobyteVolumeSource(source.Quobyte, fldPath.Child("quobyte"))...)
		}
	}
	if source.DownwardAPI != nil {
		if numVolumes > 0 {
			allErrs = append(allErrs, field.Forbidden(fldPath.Child("downwarAPI"), "may not specify more than 1 volume type"))
		} else {
			numVolumes++
			allErrs = append(allErrs, validateDownwardAPIVolumeSource(source.DownwardAPI, fldPath.Child("downwardAPI"), opts)...)
		}
	}
	if source.FC != nil {
		if numVolumes > 0 {
			allErrs = append(allErrs, field.Forbidden(fldPath.Child("fc"), "may not specify more than 1 volume type"))
		} else {
			numVolumes++
			allErrs = append(allErrs, validateFCVolumeSource(source.FC, fldPath.Child("fc"))...)
		}
	}
	if source.FlexVolume != nil {
		if numVolumes > 0 {
			allErrs = append(allErrs, field.Forbidden(fldPath.Child("flexVolume"), "may not specify more than 1 volume type"))
		} else {
			numVolumes++
			allErrs = append(allErrs, validateFlexVolumeSource(source.FlexVolume, fldPath.Child("flexVolume"))...)
		}
	}
	if source.ConfigMap != nil {
		if numVolumes > 0 {
			allErrs = append(allErrs, field.Forbidden(fldPath.Child("configMap"), "may not specify more than 1 volume type"))
		} else {
			numVolumes++
			allErrs = append(allErrs, validateConfigMapVolumeSource(source.ConfigMap, fldPath.Child("configMap"))...)
		}
	}

	if source.AzureFile != nil {
		if numVolumes > 0 {
			allErrs = append(allErrs, field.Forbidden(fldPath.Child("azureFile"), "may not specify more than 1 volume type"))
		} else {
			numVolumes++
			allErrs = append(allErrs, validateAzureFile(source.AzureFile, fldPath.Child("azureFile"))...)
		}
	}

	if source.VsphereVolume != nil {
		if numVolumes > 0 {
			allErrs = append(allErrs, field.Forbidden(fldPath.Child("vsphereVolume"), "may not specify more than 1 volume type"))
		} else {
			numVolumes++
			allErrs = append(allErrs, validateVsphereVolumeSource(source.VsphereVolume, fldPath.Child("vsphereVolume"))...)
		}
	}
	if source.PhotonPersistentDisk != nil {
		if numVolumes > 0 {
			allErrs = append(allErrs, field.Forbidden(fldPath.Child("photonPersistentDisk"), "may not specify more than 1 volume type"))
		} else {
			numVolumes++
			allErrs = append(allErrs, validatePhotonPersistentDiskVolumeSource(source.PhotonPersistentDisk, fldPath.Child("photonPersistentDisk"))...)
		}
	}
	if source.PortworxVolume != nil {
		if numVolumes > 0 {
			allErrs = append(allErrs, field.Forbidden(fldPath.Child("portworxVolume"), "may not specify more than 1 volume type"))
		} else {
			numVolumes++
			allErrs = append(allErrs, validatePortworxVolumeSource(source.PortworxVolume, fldPath.Child("portworxVolume"))...)
		}
	}
	if source.AzureDisk != nil {
		if numVolumes > 0 {
			allErrs = append(allErrs, field.Forbidden(fldPath.Child("azureDisk"), "may not specify more than 1 volume type"))
		} else {
			numVolumes++
			allErrs = append(allErrs, validateAzureDisk(source.AzureDisk, fldPath.Child("azureDisk"))...)
		}
	}
	if source.StorageOS != nil {
		if numVolumes > 0 {
			allErrs = append(allErrs, field.Forbidden(fldPath.Child("storageos"), "may not specify more than 1 volume type"))
		} else {
			numVolumes++
			allErrs = append(allErrs, validateStorageOSVolumeSource(source.StorageOS, fldPath.Child("storageos"))...)
		}
	}
	if source.Projected != nil {
		if numVolumes > 0 {
			allErrs = append(allErrs, field.Forbidden(fldPath.Child("projected"), "may not specify more than 1 volume type"))
		} else {
			numVolumes++
			allErrs = append(allErrs, validateProjectedVolumeSource(source.Projected, fldPath.Child("projected"), opts)...)
		}
	}
	if source.ScaleIO != nil {
		if numVolumes > 0 {
			allErrs = append(allErrs, field.Forbidden(fldPath.Child("scaleIO"), "may not specify more than 1 volume type"))
		} else {
			numVolumes++
			allErrs = append(allErrs, validateScaleIOVolumeSource(source.ScaleIO, fldPath.Child("scaleIO"))...)
		}
	}
	if source.CSI != nil {
		if numVolumes > 0 {
			allErrs = append(allErrs, field.Forbidden(fldPath.Child("csi"), "may not specify more than 1 volume type"))
		} else {
			numVolumes++
			allErrs = append(allErrs, validateCSIVolumeSource(source.CSI, fldPath.Child("csi"))...)
		}
	}
	if source.Ephemeral != nil {
		if numVolumes > 0 {
			allErrs = append(allErrs, field.Forbidden(fldPath.Child("ephemeral"), "may not specify more than 1 volume type"))
		} else {
			numVolumes++
			allErrs = append(allErrs, validateEphemeralVolumeSource(source.Ephemeral, fldPath.Child("ephemeral"))...)
			// 检查 PVC 的预期名称。若相关信息缺失则跳过本检查，
			// 因为缺失本身已在别处报错。例如 validatePodMetadataAndSpec
			// 调用的 ValidateObjectMeta 会检查名称是否已设置。
			if podMeta != nil && podMeta.Name != "" && volName != "" {
				pvcName := podMeta.Name + "-" + volName
				for _, msg := range ValidatePersistentVolumeName(pvcName, false) {
					allErrs = append(allErrs, field.Invalid(fldPath.Child("name"), volName, fmt.Sprintf("PVC name %q: %v", pvcName, msg)))
				}
			}
		}
	}
	if source.Image != nil {
		if numVolumes > 0 {
			allErrs = append(allErrs, field.Forbidden(fldPath.Child("image"), "may not specify more than 1 volume type"))
		} else {
			numVolumes++
			allErrs = append(allErrs, validateImageVolumeSource(source.Image, fldPath.Child("image"), opts)...)
		}
	}

	if numVolumes == 0 {
		allErrs = append(allErrs, field.Required(fldPath, "must specify a volume type"))
	}

	return allErrs
}

// validateHostPathVolumeSource 校验 hostPath 卷：路径必填、不能含回退路径段，并校验 hostPathType。
func validateHostPathVolumeSource(hostPath *core.HostPathVolumeSource, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	if len(hostPath.Path) == 0 {
		allErrs = append(allErrs, field.Required(fldPath.Child("path"), ""))
		return allErrs
	}

	allErrs = append(allErrs, validatePathNoBacksteps(hostPath.Path, fldPath.Child("path"))...)
	allErrs = append(allErrs, validateHostPathType(hostPath.Type, fldPath.Child("type"))...)
	return allErrs
}

// validateGitRepoVolumeSource 校验 gitRepo 卷：仓库地址必填，目录必须为相对下降路径。
func validateGitRepoVolumeSource(gitRepo *core.GitRepoVolumeSource, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	if len(gitRepo.Repository) == 0 {
		allErrs = append(allErrs, field.Required(fldPath.Child("repository"), ""))
	}

	pathErrs := validateLocalDescendingPath(gitRepo.Directory, fldPath.Child("directory"))
	allErrs = append(allErrs, pathErrs...)
	return allErrs
}

// validateISCSIVolumeSource 校验 Pod 中 iSCSI 卷的门户地址、IQN、LUN、认证方式等字段。
func validateISCSIVolumeSource(iscsi *core.ISCSIVolumeSource, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	if len(iscsi.TargetPortal) == 0 {
		allErrs = append(allErrs, field.Required(fldPath.Child("targetPortal"), ""))
	}
	if len(iscsi.IQN) == 0 {
		allErrs = append(allErrs, field.Required(fldPath.Child("iqn"), ""))
	} else {
		if !strings.HasPrefix(iscsi.IQN, "iqn") && !strings.HasPrefix(iscsi.IQN, "eui") && !strings.HasPrefix(iscsi.IQN, "naa") {
			allErrs = append(allErrs, field.Invalid(fldPath.Child("iqn"), iscsi.IQN, "must be valid format starting with iqn, eui, or naa"))
		} else if strings.HasPrefix(iscsi.IQN, "iqn") && !iscsiInitiatorIqnRegex.MatchString(iscsi.IQN) {
			allErrs = append(allErrs, field.Invalid(fldPath.Child("iqn"), iscsi.IQN, "must be valid format"))
		} else if strings.HasPrefix(iscsi.IQN, "eui") && !iscsiInitiatorEuiRegex.MatchString(iscsi.IQN) {
			allErrs = append(allErrs, field.Invalid(fldPath.Child("iqn"), iscsi.IQN, "must be valid format"))
		} else if strings.HasPrefix(iscsi.IQN, "naa") && !iscsiInitiatorNaaRegex.MatchString(iscsi.IQN) {
			allErrs = append(allErrs, field.Invalid(fldPath.Child("iqn"), iscsi.IQN, "must be valid format"))
		}
	}
	if iscsi.Lun < 0 || iscsi.Lun > 255 {
		allErrs = append(allErrs, field.Invalid(fldPath.Child("lun"), iscsi.Lun, validation.InclusiveRangeError(0, 255)))
	}
	if (iscsi.DiscoveryCHAPAuth || iscsi.SessionCHAPAuth) && iscsi.SecretRef == nil {
		allErrs = append(allErrs, field.Required(fldPath.Child("secretRef"), ""))
	}
	if iscsi.InitiatorName != nil {
		initiator := *iscsi.InitiatorName
		if !strings.HasPrefix(initiator, "iqn") && !strings.HasPrefix(initiator, "eui") && !strings.HasPrefix(initiator, "naa") {
			allErrs = append(allErrs, field.Invalid(fldPath.Child("initiatorname"), initiator, "must be valid format starting with iqn, eui, or naa"))
		}
		if strings.HasPrefix(initiator, "iqn") && !iscsiInitiatorIqnRegex.MatchString(initiator) {
			allErrs = append(allErrs, field.Invalid(fldPath.Child("initiatorname"), initiator, "must be valid format"))
		} else if strings.HasPrefix(initiator, "eui") && !iscsiInitiatorEuiRegex.MatchString(initiator) {
			allErrs = append(allErrs, field.Invalid(fldPath.Child("initiatorname"), initiator, "must be valid format"))
		} else if strings.HasPrefix(initiator, "naa") && !iscsiInitiatorNaaRegex.MatchString(initiator) {
			allErrs = append(allErrs, field.Invalid(fldPath.Child("initiatorname"), initiator, "must be valid format"))
		}
	}
	return allErrs
}

// validateISCSIPersistentVolumeSource 校验 PV 中 iSCSI 卷的参数，包括 initiatorName 场景下的名称长度限制。
func validateISCSIPersistentVolumeSource(iscsi *core.ISCSIPersistentVolumeSource, pvName string, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	if len(iscsi.TargetPortal) == 0 {
		allErrs = append(allErrs, field.Required(fldPath.Child("targetPortal"), ""))
	}
	if iscsi.InitiatorName != nil && len(pvName+":"+iscsi.TargetPortal) > 64 {
		tooLongErr := "Total length of <volume name>:<iscsi.targetPortal> must be under 64 characters if iscsi.initiatorName is specified."
		allErrs = append(allErrs, field.Invalid(fldPath.Child("targetportal"), iscsi.TargetPortal, tooLongErr))
	}
	if len(iscsi.IQN) == 0 {
		allErrs = append(allErrs, field.Required(fldPath.Child("iqn"), ""))
	} else {
		if !strings.HasPrefix(iscsi.IQN, "iqn") && !strings.HasPrefix(iscsi.IQN, "eui") && !strings.HasPrefix(iscsi.IQN, "naa") {
			allErrs = append(allErrs, field.Invalid(fldPath.Child("iqn"), iscsi.IQN, "must be valid format"))
		} else if strings.HasPrefix(iscsi.IQN, "iqn") && !iscsiInitiatorIqnRegex.MatchString(iscsi.IQN) {
			allErrs = append(allErrs, field.Invalid(fldPath.Child("iqn"), iscsi.IQN, "must be valid format"))
		} else if strings.HasPrefix(iscsi.IQN, "eui") && !iscsiInitiatorEuiRegex.MatchString(iscsi.IQN) {
			allErrs = append(allErrs, field.Invalid(fldPath.Child("iqn"), iscsi.IQN, "must be valid format"))
		} else if strings.HasPrefix(iscsi.IQN, "naa") && !iscsiInitiatorNaaRegex.MatchString(iscsi.IQN) {
			allErrs = append(allErrs, field.Invalid(fldPath.Child("iqn"), iscsi.IQN, "must be valid format"))
		}
	}
	if iscsi.Lun < 0 || iscsi.Lun > 255 {
		allErrs = append(allErrs, field.Invalid(fldPath.Child("lun"), iscsi.Lun, validation.InclusiveRangeError(0, 255)))
	}
	if (iscsi.DiscoveryCHAPAuth || iscsi.SessionCHAPAuth) && iscsi.SecretRef == nil {
		allErrs = append(allErrs, field.Required(fldPath.Child("secretRef"), ""))
	}
	if iscsi.SecretRef != nil {
		if len(iscsi.SecretRef.Name) == 0 {
			allErrs = append(allErrs, field.Required(fldPath.Child("secretRef", "name"), ""))
		}
	}
	if iscsi.InitiatorName != nil {
		initiator := *iscsi.InitiatorName
		if !strings.HasPrefix(initiator, "iqn") && !strings.HasPrefix(initiator, "eui") && !strings.HasPrefix(initiator, "naa") {
			allErrs = append(allErrs, field.Invalid(fldPath.Child("initiatorname"), initiator, "must be valid format"))
		}
		if strings.HasPrefix(initiator, "iqn") && !iscsiInitiatorIqnRegex.MatchString(initiator) {
			allErrs = append(allErrs, field.Invalid(fldPath.Child("initiatorname"), initiator, "must be valid format"))
		} else if strings.HasPrefix(initiator, "eui") && !iscsiInitiatorEuiRegex.MatchString(initiator) {
			allErrs = append(allErrs, field.Invalid(fldPath.Child("initiatorname"), initiator, "must be valid format"))
		} else if strings.HasPrefix(initiator, "naa") && !iscsiInitiatorNaaRegex.MatchString(initiator) {
			allErrs = append(allErrs, field.Invalid(fldPath.Child("initiatorname"), initiator, "must be valid format"))
		}
	}
	return allErrs
}

// validateFCVolumeSource 校验 Fibre Channel 卷：targetWWNs 与 wwids 必须二选一且 LUN 等参数合法。
func validateFCVolumeSource(fc *core.FCVolumeSource, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	if len(fc.TargetWWNs) < 1 && len(fc.WWIDs) < 1 {
		allErrs = append(allErrs, field.Required(fldPath.Child("targetWWNs"), "must specify either targetWWNs or wwids, but not both"))
	}

	if len(fc.TargetWWNs) != 0 && len(fc.WWIDs) != 0 {
		allErrs = append(allErrs, field.Invalid(fldPath.Child("targetWWNs"), fc.TargetWWNs, "targetWWNs and wwids can not be specified simultaneously"))
	}

	if len(fc.TargetWWNs) != 0 {
		if fc.Lun == nil {
			allErrs = append(allErrs, field.Required(fldPath.Child("lun"), "lun is required if targetWWNs is specified"))
		} else {
			if *fc.Lun < 0 || *fc.Lun > 255 {
				allErrs = append(allErrs, field.Invalid(fldPath.Child("lun"), fc.Lun, validation.InclusiveRangeError(0, 255)))
			}
		}
	}
	return allErrs
}

// validateGCEPersistentDiskVolumeSource 校验 GCE 持久磁盘卷：磁盘名必填，分区号须在 0-255 之间。
func validateGCEPersistentDiskVolumeSource(pd *core.GCEPersistentDiskVolumeSource, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	if len(pd.PDName) == 0 {
		allErrs = append(allErrs, field.Required(fldPath.Child("pdName"), ""))
	}
	if pd.Partition < 0 || pd.Partition > 255 {
		allErrs = append(allErrs, field.Invalid(fldPath.Child("partition"), pd.Partition, pdPartitionErrorMsg))
	}
	return allErrs
}

// validateAWSElasticBlockStoreVolumeSource 校验 AWS EBS 卷：volumeID 必填，分区号须在 0-255 之间。
func validateAWSElasticBlockStoreVolumeSource(PD *core.AWSElasticBlockStoreVolumeSource, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	if len(PD.VolumeID) == 0 {
		allErrs = append(allErrs, field.Required(fldPath.Child("volumeID"), ""))
	}
	if PD.Partition < 0 || PD.Partition > 255 {
		allErrs = append(allErrs, field.Invalid(fldPath.Child("partition"), PD.Partition, pdPartitionErrorMsg))
	}
	return allErrs
}

// validateSecretVolumeSource 校验 Secret 卷：secretName 必填，defaultMode 合法，并校验 defaultUser。
func validateSecretVolumeSource(secretSource *core.SecretVolumeSource, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	if len(secretSource.SecretName) == 0 {
		allErrs = append(allErrs, field.Required(fldPath.Child("secretName"), ""))
	}

	secretMode := secretSource.DefaultMode
	if secretMode != nil && (*secretMode > 0777 || *secretMode < 0) {
		allErrs = append(allErrs, field.Invalid(fldPath.Child("defaultMode"), *secretMode, fileModeErrorMsg))
	}
	allErrs = append(allErrs, validateUserField(secretSource.DefaultUser, fldPath.Child("defaultUser"))...)

	itemsPath := fldPath.Child("items")
	for i, kp := range secretSource.Items {
		itemPath := itemsPath.Index(i)
		allErrs = append(allErrs, validateKeyToPath(&kp, itemPath)...)
	}
	return allErrs
}

// validateConfigMapVolumeSource 校验 ConfigMap 卷：name 必填，defaultMode 合法，并校验 defaultUser。
func validateConfigMapVolumeSource(configMapSource *core.ConfigMapVolumeSource, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	if len(configMapSource.Name) == 0 {
		allErrs = append(allErrs, field.Required(fldPath.Child("name"), ""))
	}

	configMapMode := configMapSource.DefaultMode
	if configMapMode != nil && (*configMapMode > 0777 || *configMapMode < 0) {
		allErrs = append(allErrs, field.Invalid(fldPath.Child("defaultMode"), *configMapMode, fileModeErrorMsg))
	}
	allErrs = append(allErrs, validateUserField(configMapSource.DefaultUser, fldPath.Child("defaultUser"))...)

	itemsPath := fldPath.Child("items")
	for i, kp := range configMapSource.Items {
		itemPath := itemsPath.Index(i)
		allErrs = append(allErrs, validateKeyToPath(&kp, itemPath)...)
	}
	return allErrs
}

// validateKeyToPath 校验 KeyToPath 映射：key 与 path 必填，path 为非保留相对路径，mode 合法。
func validateKeyToPath(kp *core.KeyToPath, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	if len(kp.Key) == 0 {
		allErrs = append(allErrs, field.Required(fldPath.Child("key"), ""))
	}
	if len(kp.Path) == 0 {
		allErrs = append(allErrs, field.Required(fldPath.Child("path"), ""))
	}
	allErrs = append(allErrs, ValidateLocalNonReservedPath(kp.Path, fldPath.Child("path"))...)
	if kp.Mode != nil && (*kp.Mode > 0777 || *kp.Mode < 0) {
		allErrs = append(allErrs, field.Invalid(fldPath.Child("mode"), *kp.Mode, fileModeErrorMsg))
	}
	allErrs = append(allErrs, validateUserField(kp.User, fldPath.Child("user"))...)

	return allErrs
}

// validatePersistentClaimVolumeSource 校验 PVC 卷源：claimName 必填。
func validatePersistentClaimVolumeSource(claim *core.PersistentVolumeClaimVolumeSource, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	if len(claim.ClaimName) == 0 {
		allErrs = append(allErrs, field.Required(fldPath.Child("claimName"), ""))
	}
	return allErrs
}

// validateNFSVolumeSource 校验 NFS 卷：server 与 path 必填，且 path 必须为绝对路径。
func validateNFSVolumeSource(nfs *core.NFSVolumeSource, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	if len(nfs.Server) == 0 {
		allErrs = append(allErrs, field.Required(fldPath.Child("server"), ""))
	}
	if len(nfs.Path) == 0 {
		allErrs = append(allErrs, field.Required(fldPath.Child("path"), ""))
	}
	if !path.IsAbs(nfs.Path) {
		allErrs = append(allErrs, field.Invalid(fldPath.Child("path"), nfs.Path, "must be an absolute path"))
	}
	return allErrs
}

// validateQuobyteVolumeSource 校验 Quobyte 卷：registry 必须是 host:port 形式，tenant 长度受限。
func validateQuobyteVolumeSource(quobyte *core.QuobyteVolumeSource, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	if len(quobyte.Registry) == 0 {
		allErrs = append(allErrs, field.Required(fldPath.Child("registry"), "must be a host:port pair or multiple pairs separated by commas"))
	} else if len(quobyte.Tenant) >= 65 {
		allErrs = append(allErrs, field.Required(fldPath.Child("tenant"), "must be a UUID and may not exceed a length of 64 characters"))
	} else {
		for _, hostPortPair := range strings.Split(quobyte.Registry, ",") {
			if _, _, err := net.SplitHostPort(hostPortPair); err != nil {
				allErrs = append(allErrs, field.Invalid(fldPath.Child("registry"), quobyte.Registry, "must be a host:port pair or multiple pairs separated by commas"))
			}
		}
	}

	if len(quobyte.Volume) == 0 {
		allErrs = append(allErrs, field.Required(fldPath.Child("volume"), ""))
	}
	return allErrs
}

// validateGlusterfsVolumeSource 校验 Pod 中 Glusterfs 卷：endpoints 名称与路径必填。
func validateGlusterfsVolumeSource(glusterfs *core.GlusterfsVolumeSource, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	if len(glusterfs.EndpointsName) == 0 {
		allErrs = append(allErrs, field.Required(fldPath.Child("endpoints"), ""))
	}
	if len(glusterfs.Path) == 0 {
		allErrs = append(allErrs, field.Required(fldPath.Child("path"), ""))
	}
	return allErrs
}

// validateGlusterfsPersistentVolumeSource 校验 PV 中 Glusterfs 卷：endpoints、路径及命名空间的一致性。
func validateGlusterfsPersistentVolumeSource(glusterfs *core.GlusterfsPersistentVolumeSource, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	if len(glusterfs.EndpointsName) == 0 {
		allErrs = append(allErrs, field.Required(fldPath.Child("endpoints"), ""))
	}
	if len(glusterfs.Path) == 0 {
		allErrs = append(allErrs, field.Required(fldPath.Child("path"), ""))
	}
	if glusterfs.EndpointsNamespace != nil {
		endpointNs := glusterfs.EndpointsNamespace
		if *endpointNs == "" {
			allErrs = append(allErrs, field.Invalid(fldPath.Child("endpointsNamespace"), *endpointNs, "if the endpointnamespace is set, it must be a valid namespace name"))
		} else {
			for _, msg := range ValidateNamespaceName(*endpointNs, false) {
				allErrs = append(allErrs, field.Invalid(fldPath.Child("endpointsNamespace"), *endpointNs, msg))
			}
		}
	}
	return allErrs
}

// validateFlockerVolumeSource 校验 Flocker 卷：datasetName 与 datasetUUID 必须二选一，且名称不含 '/'。
func validateFlockerVolumeSource(flocker *core.FlockerVolumeSource, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	if len(flocker.DatasetName) == 0 && len(flocker.DatasetUUID) == 0 {
		// TODO: 此类及类似场景可考虑补充 RequiredOneOf() 错误
		allErrs = append(allErrs, field.Required(fldPath, "one of datasetName and datasetUUID is required"))
	}
	if len(flocker.DatasetName) != 0 && len(flocker.DatasetUUID) != 0 {
		allErrs = append(allErrs, field.Invalid(fldPath, "resource", "datasetName and datasetUUID can not be specified simultaneously"))
	}
	if strings.Contains(flocker.DatasetName, "/") {
		allErrs = append(allErrs, field.Invalid(fldPath.Child("datasetName"), flocker.DatasetName, "must not contain '/'"))
	}
	return allErrs
}

var validVolumeDownwardAPIFieldPathExpressions = sets.New(
	"metadata.name",
	"metadata.namespace",
	"metadata.labels",
	"metadata.annotations",
	"metadata.uid")

// validateDownwardAPIVolumeFile 校验 DownwardAPI 卷中的单个文件项：路径合法，fieldRef 与 resourceFieldRef 不能同时设置且引用字段受支持。
func validateDownwardAPIVolumeFile(file *core.DownwardAPIVolumeFile, fldPath *field.Path, opts PodValidationOptions) field.ErrorList {
	allErrs := field.ErrorList{}
	if len(file.Path) == 0 {
		allErrs = append(allErrs, field.Required(fldPath.Child("path"), ""))
	}
	allErrs = append(allErrs, ValidateLocalNonReservedPath(file.Path, fldPath.Child("path"))...)
	if file.FieldRef != nil {
		allErrs = append(allErrs, validateObjectFieldSelector(file.FieldRef, &validVolumeDownwardAPIFieldPathExpressions, fldPath.Child("fieldRef"))...)
		if file.ResourceFieldRef != nil {
			allErrs = append(allErrs, field.Invalid(fldPath, "resource", "fieldRef and resourceFieldRef can not be specified simultaneously"))
		}
	} else if file.ResourceFieldRef != nil {
		localValidContainerResourceFieldPathPrefixes := validContainerResourceFieldPathPrefixesWithDownwardAPIHugePages
		allErrs = append(allErrs, validateContainerResourceFieldSelector(file.ResourceFieldRef, &validContainerResourceFieldPathExpressions, &localValidContainerResourceFieldPathPrefixes, fldPath.Child("resourceFieldRef"), true)...)
	} else {
		allErrs = append(allErrs, field.Required(fldPath, "one of fieldRef and resourceFieldRef is required"))
	}
	if file.Mode != nil && (*file.Mode > 0777 || *file.Mode < 0) {
		allErrs = append(allErrs, field.Invalid(fldPath.Child("mode"), *file.Mode, fileModeErrorMsg))
	}
	allErrs = append(allErrs, validateUserField(file.User, fldPath.Child("user"))...)

	return allErrs
}

// validateDownwardAPIVolumeSource 校验 DownwardAPI 卷源：defaultMode、defaultUser 与所有 items 的合法性。
func validateDownwardAPIVolumeSource(downwardAPIVolume *core.DownwardAPIVolumeSource, fldPath *field.Path, opts PodValidationOptions) field.ErrorList {
	allErrs := field.ErrorList{}

	downwardAPIMode := downwardAPIVolume.DefaultMode
	if downwardAPIMode != nil && (*downwardAPIMode > 0777 || *downwardAPIMode < 0) {
		allErrs = append(allErrs, field.Invalid(fldPath.Child("defaultMode"), *downwardAPIMode, fileModeErrorMsg))
	}
	allErrs = append(allErrs, validateUserField(downwardAPIVolume.DefaultUser, fldPath.Child("defaultUser"))...)

	for _, file := range downwardAPIVolume.Items {
		allErrs = append(allErrs, validateDownwardAPIVolumeFile(&file, fldPath, opts)...)
	}
	return allErrs
}

// validateProjectionSources 校验 projected 卷的所有投影源，检查数量上限、必填字段以及各 item 路径不重复。
func validateProjectionSources(projection *core.ProjectedVolumeSource, projectionMode *int32, fldPath *field.Path, opts PodValidationOptions) field.ErrorList {
	allErrs := field.ErrorList{}
	allPaths := sets.Set[string]{}

	for i, source := range projection.Sources {
		numSources := 0
		srcPath := fldPath.Child("sources").Index(i)
		if projPath := srcPath.Child("secret"); source.Secret != nil {
			numSources++
			if len(source.Secret.Name) == 0 {
				allErrs = append(allErrs, field.Required(projPath.Child("name"), ""))
			}
			itemsPath := projPath.Child("items")
			for i, kp := range source.Secret.Items {
				itemPath := itemsPath.Index(i)
				allErrs = append(allErrs, validateKeyToPath(&kp, itemPath)...)
				if len(kp.Path) > 0 {
					curPath := kp.Path
					if !allPaths.Has(curPath) {
						allPaths.Insert(curPath)
					} else {
						allErrs = append(allErrs, field.Invalid(fldPath, source.Secret.Name, "conflicting duplicate paths"))
					}
				}
			}
		}
		if projPath := srcPath.Child("configMap"); source.ConfigMap != nil {
			numSources++
			if len(source.ConfigMap.Name) == 0 {
				allErrs = append(allErrs, field.Required(projPath.Child("name"), ""))
			}
			itemsPath := projPath.Child("items")
			for i, kp := range source.ConfigMap.Items {
				itemPath := itemsPath.Index(i)
				allErrs = append(allErrs, validateKeyToPath(&kp, itemPath)...)
				if len(kp.Path) > 0 {
					curPath := kp.Path
					if !allPaths.Has(curPath) {
						allPaths.Insert(curPath)
					} else {
						allErrs = append(allErrs, field.Invalid(fldPath, source.ConfigMap.Name, "conflicting duplicate paths"))
					}
				}
			}
		}
		if projPath := srcPath.Child("downwardAPI"); source.DownwardAPI != nil {
			numSources++
			for _, file := range source.DownwardAPI.Items {
				allErrs = append(allErrs, validateDownwardAPIVolumeFile(&file, projPath, opts)...)
				if len(file.Path) > 0 {
					curPath := file.Path
					if !allPaths.Has(curPath) {
						allPaths.Insert(curPath)
					} else {
						allErrs = append(allErrs, field.Invalid(fldPath, curPath, "conflicting duplicate paths"))
					}
				}
			}
		}
		if projPath := srcPath.Child("serviceAccountToken"); source.ServiceAccountToken != nil {
			numSources++
			if exp := source.ServiceAccountToken.ExpirationSeconds; exp == nil {
				allErrs = append(allErrs, field.Required(projPath.Child("expirationSeconds"), ""))
			} else {
				if *exp < 10*60 {
					allErrs = append(allErrs, field.Invalid(projPath.Child("expirationSeconds"), *exp, "may not specify a duration less than 10 minutes"))
				}
				if *exp > 1<<32 {
					allErrs = append(allErrs, field.Invalid(projPath.Child("expirationSeconds"), *exp, "may not specify a duration larger than 2^32 seconds"))
				}
			}
			if source.ServiceAccountToken.Path == "" {
				allErrs = append(allErrs, field.Required(fldPath.Child("path"), ""))
			} else if !opts.AllowNonLocalProjectedTokenPath {
				allErrs = append(allErrs, ValidateLocalNonReservedPath(source.ServiceAccountToken.Path, fldPath.Child("path"))...)
			}
			allErrs = append(allErrs, validateUserField(source.ServiceAccountToken.User, projPath.Child("user"))...)
		}
		if projPath := srcPath.Child("clusterTrustBundle"); source.ClusterTrustBundle != nil {
			numSources++

			usingName := source.ClusterTrustBundle.Name != nil
			usingSignerName := source.ClusterTrustBundle.SignerName != nil

			switch {
			case usingName && usingSignerName:
				allErrs = append(allErrs, field.Invalid(projPath, source.ClusterTrustBundle, "only one of name and signerName may be used"))
			case usingName:
				if *source.ClusterTrustBundle.Name == "" {
					allErrs = append(allErrs, field.Required(projPath.Child("name"), "must be a valid object name"))
				}

				name := *source.ClusterTrustBundle.Name
				if signerName, ok := extractSignerNameFromClusterTrustBundleName(name); ok {
					validationFunc := ValidateClusterTrustBundleName(signerName)
					errMsgs := validationFunc(name, false)
					for _, msg := range errMsgs {
						allErrs = append(allErrs, field.Invalid(projPath.Child("name"), name, fmt.Sprintf("not a valid clustertrustbundlename: %v", msg)))
					}
				} else {
					validationFunc := ValidateClusterTrustBundleName("")
					errMsgs := validationFunc(name, false)
					for _, msg := range errMsgs {
						allErrs = append(allErrs, field.Invalid(projPath.Child("name"), name, fmt.Sprintf("not a valid clustertrustbundlename: %v", msg)))
					}
				}

				if source.ClusterTrustBundle.LabelSelector != nil {
					allErrs = append(allErrs, field.Invalid(projPath.Child("labelSelector"), source.ClusterTrustBundle.LabelSelector, "labelSelector must be unset if name is specified"))
				}
			case usingSignerName:
				if *source.ClusterTrustBundle.SignerName == "" {
					allErrs = append(allErrs, field.Required(projPath.Child("signerName"), "must be a valid signer name"))
				}

				allErrs = append(allErrs, ValidateSignerName(projPath.Child("signerName"), *source.ClusterTrustBundle.SignerName)...)

				labelSelectorErrs := unversionedvalidation.ValidateLabelSelector(
					source.ClusterTrustBundle.LabelSelector,
					unversionedvalidation.LabelSelectorValidationOptions{AllowInvalidLabelValueInSelector: false},
					projPath.Child("labelSelector"),
				)
				allErrs = append(allErrs, labelSelectorErrs...)

			default:
				allErrs = append(allErrs, field.Required(projPath, "either name or signerName must be specified"))
			}

			if source.ClusterTrustBundle.Path == "" {
				allErrs = append(allErrs, field.Required(projPath.Child("path"), ""))
			}

			allErrs = append(allErrs, validateUserField(source.ClusterTrustBundle.User, projPath.Child("user"))...)
			allErrs = append(allErrs, ValidateLocalNonReservedPath(source.ClusterTrustBundle.Path, projPath.Child("path"))...)

			curPath := source.ClusterTrustBundle.Path
			if !allPaths.Has(curPath) {
				allPaths.Insert(curPath)
			} else {
				allErrs = append(allErrs, field.Invalid(fldPath, curPath, "conflicting duplicate paths"))
			}
		}
		if projPath := srcPath.Child("podCertificate"); source.PodCertificate != nil {
			numSources++

			allErrs = append(allErrs, ValidateSignerName(projPath.Child("signerName"), source.PodCertificate.SignerName)...)

			if source.PodCertificate.UserAnnotations != nil {
				userAnnotationsErrors := ValidateUserAnnotations(source.PodCertificate.UserAnnotations, projPath.Child("userAnnotations"))
				allErrs = append(allErrs, userAnnotationsErrors...)
			}
			allErrs = append(allErrs, validateUserField(source.PodCertificate.User, projPath.Child("user"))...)

			switch source.PodCertificate.KeyType {
			case "RSA3072", "RSA4096", "ECDSAP256", "ECDSAP384", "ECDSAP521", "ED25519":
				// 合法取值
			default:
				allErrs = append(allErrs, field.NotSupported(projPath.Child("keyType"), source.PodCertificate.KeyType, []string{"RSA3072", "RSA4096", "ECDSAP256", "ECDSAP384", "ECDSAP521", "ED25519"}))
			}

			if source.PodCertificate.MaxExpirationSeconds != nil {
				if *source.PodCertificate.MaxExpirationSeconds < 3600 {
					allErrs = append(allErrs, field.Invalid(projPath.Child("maxExpirationSeconds"), *source.PodCertificate.MaxExpirationSeconds, "if provided, maxExpirationSeconds must be >= 3600"))
				}
				maxMaxExpirationSeconds := certificates.MaxMaxExpirationSeconds
				if IsKubernetesSignerName(source.PodCertificate.SignerName) {
					maxMaxExpirationSeconds = certificates.KubernetesMaxMaxExpirationSeconds
				}
				if *source.PodCertificate.MaxExpirationSeconds > int32(maxMaxExpirationSeconds) {
					allErrs = append(allErrs, field.Invalid(projPath.Child("maxExpirationSeconds"), *source.PodCertificate.MaxExpirationSeconds, fmt.Sprintf("if provided, maxExpirationSeconds must be <= %d", maxMaxExpirationSeconds)))
				}
			}

			numPaths := 0
			if len(source.PodCertificate.CredentialBundlePath) != 0 {
				numPaths++
				// Credential bundle 路径必须格式正常。
				allErrs = append(allErrs, ValidateLocalNonReservedPath(source.PodCertificate.CredentialBundlePath, projPath.Child("credentialBundlePath"))...)
				// Credential bundle 路径不得与其他来源的路径冲突。
				if !allPaths.Has(source.PodCertificate.CredentialBundlePath) {
					allPaths.Insert(source.PodCertificate.CredentialBundlePath)
				} else {
					allErrs = append(allErrs, field.Invalid(fldPath, source.PodCertificate.CredentialBundlePath, "conflicting duplicate paths"))
				}
			}

			if len(source.PodCertificate.KeyPath) != 0 {
				numPaths++
				allErrs = append(allErrs, ValidateLocalNonReservedPath(source.PodCertificate.KeyPath, projPath.Child("keyPath"))...)
				if !allPaths.Has(source.PodCertificate.KeyPath) {
					allPaths.Insert(source.PodCertificate.KeyPath)
				} else {
					allErrs = append(allErrs, field.Invalid(fldPath, source.PodCertificate.KeyPath, "conflicting duplicate paths"))
				}

			}

			if len(source.PodCertificate.CertificateChainPath) != 0 {
				numPaths++
				allErrs = append(allErrs, ValidateLocalNonReservedPath(source.PodCertificate.CertificateChainPath, projPath.Child("certificateChainPath"))...)
				if !allPaths.Has(source.PodCertificate.CertificateChainPath) {
					allPaths.Insert(source.PodCertificate.CertificateChainPath)
				} else {
					allErrs = append(allErrs, field.Invalid(fldPath, source.PodCertificate.CertificateChainPath, "conflicting duplicate paths"))
				}
			}

			if numPaths == 0 {
				allErrs = append(allErrs, field.Required(projPath, "specify at least one of credentialBundlePath, keyPath, and certificateChainPath"))
			}
		}
		if numSources > 1 {
			allErrs = append(allErrs, field.Forbidden(srcPath, "may not specify more than 1 volume type per source"))
		}
	}
	return allErrs
}

// validateProjectedVolumeSource 校验 projected 卷源：defaultMode、defaultUser 及各投影来源的合法性。
func validateProjectedVolumeSource(projection *core.ProjectedVolumeSource, fldPath *field.Path, opts PodValidationOptions) field.ErrorList {
	allErrs := field.ErrorList{}

	projectionMode := projection.DefaultMode
	if projectionMode != nil && (*projectionMode > 0777 || *projectionMode < 0) {
		allErrs = append(allErrs, field.Invalid(fldPath.Child("defaultMode"), *projectionMode, fileModeErrorMsg))
	}
	allErrs = append(allErrs, validateUserField(projection.DefaultUser, fldPath.Child("defaultUser"))...)

	allErrs = append(allErrs, validateProjectionSources(projection, projectionMode, fldPath, opts)...)
	return allErrs
}

var supportedHostPathTypes = sets.New(
	core.HostPathUnset,
	core.HostPathDirectoryOrCreate,
	core.HostPathDirectory,
	core.HostPathFileOrCreate,
	core.HostPathFile,
	core.HostPathSocket,
	core.HostPathCharDev,
	core.HostPathBlockDev)

// validateHostPathType 校验 hostPath 的 type 是否为受支持的类型。
func validateHostPathType(hostPathType *core.HostPathType, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}

	if hostPathType != nil && !supportedHostPathTypes.Has(*hostPathType) {
		allErrs = append(allErrs, field.NotSupported(fldPath, hostPathType, sets.List(supportedHostPathTypes)))
	}

	return allErrs
}

// validateLocalDescendingPath 校验目标路径：必须是相对路径且不包含任何 '..' 路径段。
func validateLocalDescendingPath(targetPath string, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	if path.IsAbs(targetPath) {
		allErrs = append(allErrs, field.Invalid(fldPath, targetPath, "must be a relative path"))
	}

	allErrs = append(allErrs, validatePathNoBacksteps(targetPath, fldPath)...)

	return allErrs
}

// validatePathNoBacksteps 校验目标路径不能以 '/' 分割后出现 '..' 段；假定 apiserver 与节点操作系统一致，节点侧仍需做同样检查。
func validatePathNoBacksteps(targetPath string, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	parts := strings.Split(filepath.ToSlash(targetPath), "/")
	for _, item := range parts {
		if item == ".." {
			allErrs = append(allErrs, field.Invalid(fldPath, targetPath, "must not contain '..'"))
			break // 即便路径是 `../../..`，报一个错误也足以说明问题
		}
	}
	return allErrs
}

// validateMountPropagation 校验挂载传播模式是否合法，且是否被当前容器所允许。
func validateMountPropagation(mountPropagation *core.MountPropagationMode, container *core.Container, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}

	if mountPropagation == nil {
		return allErrs
	}

	supportedMountPropagations := sets.New(
		core.MountPropagationBidirectional,
		core.MountPropagationHostToContainer,
		core.MountPropagationNone)

	if !supportedMountPropagations.Has(*mountPropagation) {
		allErrs = append(allErrs, field.NotSupported(fldPath, *mountPropagation, sets.List(supportedMountPropagations)))
	}

	if container == nil {
		// 此时容器信息尚不可得。
		// 到此停止校验即可：Pod 校验会最终拒绝
		// 在非特权容器中使用 Bidirectional 传播的 Pod。
		return allErrs
	}

	privileged := container.SecurityContext != nil && container.SecurityContext.Privileged != nil && *container.SecurityContext.Privileged
	if *mountPropagation == core.MountPropagationBidirectional && !privileged {
		allErrs = append(allErrs, field.Forbidden(fldPath, "Bidirectional mount propagation is available only to privileged containers"))
	}
	return allErrs
}

// validateMountRecursiveReadOnly 校验 recursiveReadOnly 挂载：仅能在 readOnly 为 true 时使用，且依赖相应特性门控。
func validateMountRecursiveReadOnly(mount core.VolumeMount, fldPath *field.Path) field.ErrorList {
	if mount.RecursiveReadOnly == nil {
		return nil
	}
	allErrs := field.ErrorList{}
	switch *mount.RecursiveReadOnly {
	case core.RecursiveReadOnlyDisabled:
		// 无需处理
	case core.RecursiveReadOnlyEnabled, core.RecursiveReadOnlyIfPossible:
		if !mount.ReadOnly {
			allErrs = append(allErrs, field.Forbidden(fldPath, "may only be specified when readOnly is true"))
		}
		if mount.MountPropagation != nil && *mount.MountPropagation != core.MountPropagationNone {
			allErrs = append(allErrs, field.Forbidden(fldPath, "may only be specified when mountPropagation is None or not specified"))
		}
	default:
		supportedRRO := sets.New(
			core.RecursiveReadOnlyDisabled,
			core.RecursiveReadOnlyIfPossible,
			core.RecursiveReadOnlyEnabled)
		allErrs = append(allErrs, field.NotSupported(fldPath, *mount.RecursiveReadOnly, sets.List(supportedRRO)))
	}
	return allErrs
}

// ValidateLocalNonReservedPath 校验目标路径：非绝对路径、不含 '..' 段且不以 '..' 开头（保留路径）。
func ValidateLocalNonReservedPath(targetPath string, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	allErrs = append(allErrs, validateLocalDescendingPath(targetPath, fldPath)...)
	// 若 '..' 段检查已经报过这个错误，就不重复报告。
	if strings.HasPrefix(targetPath, "..") && !strings.HasPrefix(targetPath, "../") {
		allErrs = append(allErrs, field.Invalid(fldPath, targetPath, "must not start with '..'"))
	}
	return allErrs
}

// validateRBDVolumeSource 校验 Pod 中 RBD 卷：monitors 与 image 必填。
func validateRBDVolumeSource(rbd *core.RBDVolumeSource, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	if len(rbd.CephMonitors) == 0 {
		allErrs = append(allErrs, field.Required(fldPath.Child("monitors"), ""))
	}
	if len(rbd.RBDImage) == 0 {
		allErrs = append(allErrs, field.Required(fldPath.Child("image"), ""))
	}
	return allErrs
}

// validateRBDPersistentVolumeSource 校验 PV 中 RBD 卷：monitors 与 image 必填。
func validateRBDPersistentVolumeSource(rbd *core.RBDPersistentVolumeSource, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	if len(rbd.CephMonitors) == 0 {
		allErrs = append(allErrs, field.Required(fldPath.Child("monitors"), ""))
	}
	if len(rbd.RBDImage) == 0 {
		allErrs = append(allErrs, field.Required(fldPath.Child("image"), ""))
	}
	return allErrs
}

// validateCinderVolumeSource 校验 Pod 中 Cinder 卷：volumeID 必填，secretRef.name 若设置则必填。
func validateCinderVolumeSource(cd *core.CinderVolumeSource, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	if len(cd.VolumeID) == 0 {
		allErrs = append(allErrs, field.Required(fldPath.Child("volumeID"), ""))
	}
	if cd.SecretRef != nil {
		if len(cd.SecretRef.Name) == 0 {
			allErrs = append(allErrs, field.Required(fldPath.Child("secretRef", "name"), ""))
		}
	}
	return allErrs
}

// validateCinderPersistentVolumeSource 校验 PV 中 Cinder 卷：volumeID 必填，secretRef 的 name 与 namespace 必填。
func validateCinderPersistentVolumeSource(cd *core.CinderPersistentVolumeSource, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	if len(cd.VolumeID) == 0 {
		allErrs = append(allErrs, field.Required(fldPath.Child("volumeID"), ""))
	}
	if cd.SecretRef != nil {
		if len(cd.SecretRef.Name) == 0 {
			allErrs = append(allErrs, field.Required(fldPath.Child("secretRef", "name"), ""))
		}
		if len(cd.SecretRef.Namespace) == 0 {
			allErrs = append(allErrs, field.Required(fldPath.Child("secretRef", "namespace"), ""))
		}
	}
	return allErrs
}

// validateCephFSVolumeSource 校验 Pod 中 CephFS 卷：monitors 必填。
func validateCephFSVolumeSource(cephfs *core.CephFSVolumeSource, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	if len(cephfs.Monitors) == 0 {
		allErrs = append(allErrs, field.Required(fldPath.Child("monitors"), ""))
	}
	return allErrs
}

// validateCephFSPersistentVolumeSource 校验 PV 中 CephFS 卷：monitors 必填。
func validateCephFSPersistentVolumeSource(cephfs *core.CephFSPersistentVolumeSource, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	if len(cephfs.Monitors) == 0 {
		allErrs = append(allErrs, field.Required(fldPath.Child("monitors"), ""))
	}
	return allErrs
}

// validateFlexVolumeSource 校验 Pod 中 FlexVolume：driver 必填，且选项键不得使用 kubernetes.io/k8s.io 保留命名空间。
func validateFlexVolumeSource(fv *core.FlexVolumeSource, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	if len(fv.Driver) == 0 {
		allErrs = append(allErrs, field.Required(fldPath.Child("driver"), ""))
	}

	// 确保用户指定的 options 不使用 kubernetes 保留命名空间
	for k := range fv.Options {
		namespace := k
		if parts := strings.SplitN(k, "/", 2); len(parts) == 2 {
			namespace = parts[0]
		}
		normalized := "." + strings.ToLower(namespace)
		if strings.HasSuffix(normalized, ".kubernetes.io") || strings.HasSuffix(normalized, ".k8s.io") {
			allErrs = append(allErrs, field.Invalid(fldPath.Child("options").Key(k), k, "kubernetes.io and k8s.io namespaces are reserved"))
		}
	}

	return allErrs
}

// validateFlexPersistentVolumeSource 校验 PV 中 FlexVolume：driver 必填，且选项键不得使用 kubernetes.io/k8s.io 保留命名空间。
func validateFlexPersistentVolumeSource(fv *core.FlexPersistentVolumeSource, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	if len(fv.Driver) == 0 {
		allErrs = append(allErrs, field.Required(fldPath.Child("driver"), ""))
	}

	// 确保用户指定的 options 不使用 kubernetes 保留命名空间
	for k := range fv.Options {
		namespace := k
		if parts := strings.SplitN(k, "/", 2); len(parts) == 2 {
			namespace = parts[0]
		}
		normalized := "." + strings.ToLower(namespace)
		if strings.HasSuffix(normalized, ".kubernetes.io") || strings.HasSuffix(normalized, ".k8s.io") {
			allErrs = append(allErrs, field.Invalid(fldPath.Child("options").Key(k), k, "kubernetes.io and k8s.io namespaces are reserved"))
		}
	}

	return allErrs
}

// validateAzureFile 校验 Azure File 卷：secretName 与 shareName 必填。
func validateAzureFile(azure *core.AzureFileVolumeSource, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	if azure.SecretName == "" {
		allErrs = append(allErrs, field.Required(fldPath.Child("secretName"), ""))
	}
	if azure.ShareName == "" {
		allErrs = append(allErrs, field.Required(fldPath.Child("shareName"), ""))
	}
	return allErrs
}

// validateAzureFilePV 校验 PV 中 Azure File 卷：secretName、shareName 及可选 secretNamespace 的合法性。
func validateAzureFilePV(azure *core.AzureFilePersistentVolumeSource, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	if azure.SecretName == "" {
		allErrs = append(allErrs, field.Required(fldPath.Child("secretName"), ""))
	}
	if azure.ShareName == "" {
		allErrs = append(allErrs, field.Required(fldPath.Child("shareName"), ""))
	}
	if azure.SecretNamespace != nil {
		if len(*azure.SecretNamespace) == 0 {
			allErrs = append(allErrs, field.Required(fldPath.Child("secretNamespace"), ""))
		}
	}
	return allErrs
}

// validateAzureDisk 校验 Azure Disk 卷：diskName、cachingMode、diskKind 以及 URI 格式是否受支持。
func validateAzureDisk(azure *core.AzureDiskVolumeSource, fldPath *field.Path) field.ErrorList {
	var supportedCachingModes = sets.New(
		core.AzureDataDiskCachingNone,
		core.AzureDataDiskCachingReadOnly,
		core.AzureDataDiskCachingReadWrite)

	var supportedDiskKinds = sets.New(
		core.AzureSharedBlobDisk,
		core.AzureDedicatedBlobDisk,
		core.AzureManagedDisk)

	diskURISupportedManaged := []string{"/subscriptions/{sub-id}/resourcegroups/{group-name}/providers/microsoft.compute/disks/{disk-id}"}
	diskURISupportedblob := []string{"https://{account-name}.blob.core.windows.net/{container-name}/{disk-name}.vhd"}

	allErrs := field.ErrorList{}
	if azure.DiskName == "" {
		allErrs = append(allErrs, field.Required(fldPath.Child("diskName"), ""))
	}

	if azure.DataDiskURI == "" {
		allErrs = append(allErrs, field.Required(fldPath.Child("diskURI"), ""))
	}

	if azure.CachingMode != nil && !supportedCachingModes.Has(*azure.CachingMode) {
		allErrs = append(allErrs, field.NotSupported(fldPath.Child("cachingMode"), *azure.CachingMode, sets.List(supportedCachingModes)))
	}

	if azure.Kind != nil && !supportedDiskKinds.Has(*azure.Kind) {
		allErrs = append(allErrs, field.NotSupported(fldPath.Child("kind"), *azure.Kind, sets.List(supportedDiskKinds)))
	}

	// 校验 DiskUri 是否为正确格式
	if azure.Kind != nil && *azure.Kind == core.AzureManagedDisk && strings.Index(azure.DataDiskURI, "/subscriptions/") != 0 {
		allErrs = append(allErrs, field.NotSupported(fldPath.Child("diskURI"), azure.DataDiskURI, diskURISupportedManaged))
	}

	if azure.Kind != nil && *azure.Kind != core.AzureManagedDisk && strings.Index(azure.DataDiskURI, "https://") != 0 {
		allErrs = append(allErrs, field.NotSupported(fldPath.Child("diskURI"), azure.DataDiskURI, diskURISupportedblob))
	}

	return allErrs
}

// validateVsphereVolumeSource 校验 vSphere 卷：volumePath 必填。
func validateVsphereVolumeSource(cd *core.VsphereVirtualDiskVolumeSource, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	if len(cd.VolumePath) == 0 {
		allErrs = append(allErrs, field.Required(fldPath.Child("volumePath"), ""))
	}
	return allErrs
}

// validatePhotonPersistentDiskVolumeSource 校验 Photon 持久磁盘卷：pdID 必填。
func validatePhotonPersistentDiskVolumeSource(cd *core.PhotonPersistentDiskVolumeSource, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	if len(cd.PdID) == 0 {
		allErrs = append(allErrs, field.Required(fldPath.Child("pdID"), ""))
	}
	return allErrs
}

// validatePortworxVolumeSource 校验 Portworx 卷：volumeID 必填。
func validatePortworxVolumeSource(pwx *core.PortworxVolumeSource, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	if len(pwx.VolumeID) == 0 {
		allErrs = append(allErrs, field.Required(fldPath.Child("volumeID"), ""))
	}
	return allErrs
}

// validateScaleIOVolumeSource 校验 Pod 中 ScaleIO 卷：gateway、system、volumeName 必填。
func validateScaleIOVolumeSource(sio *core.ScaleIOVolumeSource, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	if sio.Gateway == "" {
		allErrs = append(allErrs, field.Required(fldPath.Child("gateway"), ""))
	}
	if sio.System == "" {
		allErrs = append(allErrs, field.Required(fldPath.Child("system"), ""))
	}
	if sio.VolumeName == "" {
		allErrs = append(allErrs, field.Required(fldPath.Child("volumeName"), ""))
	}
	return allErrs
}

// validateScaleIOPersistentVolumeSource 校验 PV 中 ScaleIO 卷：gateway、system、volumeName 必填。
func validateScaleIOPersistentVolumeSource(sio *core.ScaleIOPersistentVolumeSource, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	if sio.Gateway == "" {
		allErrs = append(allErrs, field.Required(fldPath.Child("gateway"), ""))
	}
	if sio.System == "" {
		allErrs = append(allErrs, field.Required(fldPath.Child("system"), ""))
	}
	if sio.VolumeName == "" {
		allErrs = append(allErrs, field.Required(fldPath.Child("volumeName"), ""))
	}
	return allErrs
}

// validateLocalVolumeSource 校验 local 卷：路径必填且不含回退路径段。
func validateLocalVolumeSource(ls *core.LocalVolumeSource, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	if ls.Path == "" {
		allErrs = append(allErrs, field.Required(fldPath.Child("path"), ""))
		return allErrs
	}

	allErrs = append(allErrs, validatePathNoBacksteps(ls.Path, fldPath.Child("path"))...)
	return allErrs
}

// validateStorageOSVolumeSource 校验 Pod 中 StorageOS 卷：卷名/命名空间须为 DNS 标签，secretRef.name 若设置则必填。
func validateStorageOSVolumeSource(storageos *core.StorageOSVolumeSource, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	if len(storageos.VolumeName) == 0 {
		allErrs = append(allErrs, field.Required(fldPath.Child("volumeName"), ""))
	} else {
		allErrs = append(allErrs, ValidateDNS1123Label(storageos.VolumeName, fldPath.Child("volumeName"))...)
	}
	if len(storageos.VolumeNamespace) > 0 {
		allErrs = append(allErrs, ValidateDNS1123Label(storageos.VolumeNamespace, fldPath.Child("volumeNamespace"))...)
	}
	if storageos.SecretRef != nil {
		if len(storageos.SecretRef.Name) == 0 {
			allErrs = append(allErrs, field.Required(fldPath.Child("secretRef", "name"), ""))
		}
	}
	return allErrs
}

// validateStorageOSPersistentVolumeSource 校验 PV 中 StorageOS 卷：卷名/命名空间须为 DNS 标签，secretRef 的 name 与 namespace 必填。
func validateStorageOSPersistentVolumeSource(storageos *core.StorageOSPersistentVolumeSource, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	if len(storageos.VolumeName) == 0 {
		allErrs = append(allErrs, field.Required(fldPath.Child("volumeName"), ""))
	} else {
		allErrs = append(allErrs, ValidateDNS1123Label(storageos.VolumeName, fldPath.Child("volumeName"))...)
	}
	if len(storageos.VolumeNamespace) > 0 {
		allErrs = append(allErrs, ValidateDNS1123Label(storageos.VolumeNamespace, fldPath.Child("volumeNamespace"))...)
	}
	if storageos.SecretRef != nil {
		if len(storageos.SecretRef.Name) == 0 {
			allErrs = append(allErrs, field.Required(fldPath.Child("secretRef", "name"), ""))
		}
		if len(storageos.SecretRef.Namespace) == 0 {
			allErrs = append(allErrs, field.Required(fldPath.Child("secretRef", "namespace"), ""))
		}
	}
	return allErrs
}

// validateUserField 校验卷默认用户 ID（DefaultUser）是否为合法用户 ID。
func validateUserField(user *int64, fldPath *field.Path) field.ErrorList {
	var allErrs field.ErrorList
	if user == nil {
		return allErrs
	}
	for _, msg := range validation.IsValidUserID(*user) {
		allErrs = append(allErrs, field.Invalid(fldPath, *user, msg))
	}
	return allErrs
}

// validatePVSecretReference 校验 PV 使用的 SecretReference：name 须为 DNS 子域名，namespace 须为 DNS 标签。
func validatePVSecretReference(secretRef *core.SecretReference, fldPath *field.Path) field.ErrorList {
	var allErrs field.ErrorList
	if len(secretRef.Name) == 0 {
		allErrs = append(allErrs, field.Required(fldPath.Child("name"), ""))
	} else {
		allErrs = append(allErrs, ValidateDNS1123Subdomain(secretRef.Name, fldPath.Child("name"))...)
	}

	if len(secretRef.Namespace) == 0 {
		allErrs = append(allErrs, field.Required(fldPath.Child("namespace"), ""))
	} else {
		allErrs = append(allErrs, ValidateDNS1123Label(secretRef.Namespace, fldPath.Child("namespace"))...)
	}
	return allErrs
}

// ValidateCSIDriverNameOption 是 ValidateCSIDriverName 的选项，
// 用于标记某条校验错误信息是否已由声明式校验覆盖。
type ValidateCSIDriverNameOption int

const (
	// 必填检查已由声明式校验覆盖。
	RequiredCovered ValidateCSIDriverNameOption = iota
	// 列表长度（元素数量）的检查已由声明式校验覆盖。
	SizeCovered
	// 格式检查已由声明式校验覆盖。
	FormatCovered
)

// ValidateCSIDriverName 校验 CSI 驱动名称：非空、长度不超过 63，并符合 DNS 标签格式；opts 可声明某些错误由声明式校验覆盖。
func ValidateCSIDriverName(driverName string, fldPath *field.Path, opts ...ValidateCSIDriverNameOption) field.ErrorList {
	allErrs := field.ErrorList{}

	if len(driverName) == 0 {
		err := field.Required(fldPath, "")
		if slices.Contains(opts, RequiredCovered) {
			err = err.MarkCoveredByDeclarative()
		}
		allErrs = append(allErrs, err)
		return allErrs
	}

	if len(driverName) > 63 {
		err := field.TooLong(fldPath, "" /*unused*/, 63)
		if slices.Contains(opts, SizeCovered) {
			err = err.WithOrigin("maxLength").MarkCoveredByDeclarative()
		}
		allErrs = append(allErrs, err)
	}

	for _, msg := range validation.IsDNS1123Subdomain(strings.ToLower(driverName)) {
		err := field.Invalid(fldPath, driverName, msg)
		if slices.Contains(opts, FormatCovered) {
			err = err.WithOrigin("format=k8s-long-name-caseless").MarkCoveredByDeclarative()
		}
		allErrs = append(allErrs, err)
	}

	return allErrs
}

// validateCSIPersistentVolumeSource 校验 PV 中 CSI 卷源：驱动名、volumeHandle 及各类 Secret 引用的合法性。
func validateCSIPersistentVolumeSource(csi *core.CSIPersistentVolumeSource, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}

	allErrs = append(allErrs, ValidateCSIDriverName(csi.Driver, fldPath.Child("driver"))...)

	if len(csi.VolumeHandle) == 0 {
		allErrs = append(allErrs, field.Required(fldPath.Child("volumeHandle"), ""))
	}
	if csi.ControllerPublishSecretRef != nil {
		allErrs = append(allErrs, validatePVSecretReference(csi.ControllerPublishSecretRef, fldPath.Child("controllerPublishSecretRef"))...)
	}
	if csi.ControllerExpandSecretRef != nil {
		allErrs = append(allErrs, validatePVSecretReference(csi.ControllerExpandSecretRef, fldPath.Child("controllerExpandSecretRef"))...)
	}
	if csi.NodePublishSecretRef != nil {
		allErrs = append(allErrs, validatePVSecretReference(csi.NodePublishSecretRef, fldPath.Child("nodePublishSecretRef"))...)
	}
	if csi.NodeExpandSecretRef != nil {
		allErrs = append(allErrs, validatePVSecretReference(csi.NodeExpandSecretRef, fldPath.Child("nodeExpandSecretRef"))...)
	}
	return allErrs
}

// validateCSIVolumeSource 校验 Pod 中 CSI 卷源：驱动名与 nodePublishSecretRef 的合法性。
func validateCSIVolumeSource(csi *core.CSIVolumeSource, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	allErrs = append(allErrs, ValidateCSIDriverName(csi.Driver, fldPath.Child("driver"))...)

	if csi.NodePublishSecretRef != nil {
		if len(csi.NodePublishSecretRef.Name) == 0 {
			allErrs = append(allErrs, field.Required(fldPath.Child("nodePublishSecretRef", "name"), ""))
		} else {
			for _, msg := range ValidateSecretName(csi.NodePublishSecretRef.Name, false) {
				allErrs = append(allErrs, field.Invalid(fldPath.Child("name"), csi.NodePublishSecretRef.Name, msg))
			}
		}
	}

	return allErrs
}

// validateEphemeralVolumeSource 校验临时卷：volumeClaimTemplate 必须设置且其元数据与规格合法。
func validateEphemeralVolumeSource(ephemeral *core.EphemeralVolumeSource, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	if ephemeral.VolumeClaimTemplate == nil {
		allErrs = append(allErrs, field.Required(fldPath.Child("volumeClaimTemplate"), ""))
	} else {
		opts := ValidationOptionsForPersistentVolumeClaimTemplate(ephemeral.VolumeClaimTemplate, nil)
		allErrs = append(allErrs, ValidatePersistentVolumeClaimTemplate(ephemeral.VolumeClaimTemplate, fldPath.Child("volumeClaimTemplate"), opts)...)
	}
	return allErrs
}

// ValidatePersistentVolumeClaimTemplate 校验内嵌 PVC 模板的元数据与规格；因仅使用标签与注解，元数据检查较为宽松。
func ValidatePersistentVolumeClaimTemplate(claimTemplate *core.PersistentVolumeClaimTemplate, fldPath *field.Path, opts PersistentVolumeClaimSpecValidationOptions) field.ErrorList {
	allErrs := ValidateTemplateObjectMeta(&claimTemplate.ObjectMeta, fldPath.Child("metadata"))
	allErrs = append(allErrs, ValidatePersistentVolumeClaimSpec(&claimTemplate.Spec, fldPath.Child("spec"), opts)...)
	return allErrs
}

// ValidateTemplateObjectMeta 校验模板元数据：仅允许设置注解与标签，其他 ObjectMeta 字段一律禁止以避免混淆。
func ValidateTemplateObjectMeta(objMeta *metav1.ObjectMeta, fldPath *field.Path) field.ErrorList {
	allErrs := apimachineryvalidation.ValidateAnnotations(objMeta.Annotations, fldPath.Child("annotations"))
	allErrs = append(allErrs, unversionedvalidation.ValidateLabels(objMeta.Labels, fldPath.Child("labels"))...)
	// 其余字段均不受支持，为避免混淆必须保持未设置。
	// 也可以逐个字段拒绝，但那样 ObjectMeta 新增字段时
	// 除非更新本代码否则不会被检查。因此这里改为通过
	// 反射确保只允许设置了白名单内的字段。
	allErrs = append(allErrs, validateFieldAllowList(*objMeta, allowedTemplateObjectMetaFields, "cannot be set", fldPath)...)
	return allErrs
}

var allowedTemplateObjectMetaFields = map[string]bool{
	"Annotations": true,
	"Labels":      true,
}

// PersistentVolumeSpecValidationOptions 包含 PersistentVolume 校验的各项设置。
type PersistentVolumeSpecValidationOptions struct {
	// 允许用户修改 volume attributes 的 class
	EnableVolumeAttributesClass bool
	// 允许 RequiredNodeSelector 中存在非法的 label-value
	AllowInvalidLabelValueInRequiredNodeAffinity bool
}

// ValidatePersistentVolumeName 检查名称是否适合作为 PersistentVolume 对象的名称。
var ValidatePersistentVolumeName = apimachineryvalidation.NameIsDNSSubdomain

var supportedAccessModes = sets.New(
	core.ReadWriteOnce,
	core.ReadOnlyMany,
	core.ReadWriteMany,
	core.ReadWriteOncePod)

var supportedReclaimPolicy = sets.New(
	core.PersistentVolumeReclaimDelete,
	core.PersistentVolumeReclaimRecycle,
	core.PersistentVolumeReclaimRetain)

var supportedVolumeModes = sets.New(core.PersistentVolumeBlock, core.PersistentVolumeFilesystem)

// ValidationOptionsForPersistentVolume 根据当前特性门控与旧 PV 状态生成 PV 规格校验选项，保证升级/降级场景兼容。
func ValidationOptionsForPersistentVolume(pv, oldPv *core.PersistentVolume) PersistentVolumeSpecValidationOptions {
	opts := PersistentVolumeSpecValidationOptions{
		EnableVolumeAttributesClass:                  utilfeature.DefaultMutableFeatureGate.Enabled(features.VolumeAttributesClass),
		AllowInvalidLabelValueInRequiredNodeAffinity: false,
	}
	if oldPv != nil && oldPv.Spec.VolumeAttributesClassName != nil {
		opts.EnableVolumeAttributesClass = true
	}
	if oldPv != nil && oldPv.Spec.NodeAffinity != nil &&
		oldPv.Spec.NodeAffinity.Required != nil {
		terms := oldPv.Spec.NodeAffinity.Required.NodeSelectorTerms
		opts.AllowInvalidLabelValueInRequiredNodeAffinity = helper.HasInvalidLabelValueInNodeSelectorTerms(terms)
	}
	return opts
}

// ValidatePersistentVolumeSpec 校验 PV 规格：访问模式、容量、卷源、存储类、节点亲和性等，并支持内联卷场景。
func ValidatePersistentVolumeSpec(pvSpec *core.PersistentVolumeSpec, pvName string, validateInlinePersistentVolumeSpec bool, fldPath *field.Path, opts PersistentVolumeSpecValidationOptions) field.ErrorList {
	allErrs := field.ErrorList{}

	if validateInlinePersistentVolumeSpec {
		if pvSpec.ClaimRef != nil {
			allErrs = append(allErrs, field.Forbidden(fldPath.Child("claimRef"), "may not be specified in the context of inline volumes"))
		}
		if len(pvSpec.Capacity) != 0 {
			allErrs = append(allErrs, field.Forbidden(fldPath.Child("capacity"), "may not be specified in the context of inline volumes"))
		}
		if pvSpec.CSI == nil {
			allErrs = append(allErrs, field.Required(fldPath.Child("csi"), "has to be specified in the context of inline volumes"))
		}
	}

	if len(pvSpec.AccessModes) == 0 {
		allErrs = append(allErrs, field.Required(fldPath.Child("accessModes"), ""))
	}

	foundReadWriteOncePod, foundNonReadWriteOncePod := false, false
	for _, mode := range pvSpec.AccessModes {
		if !supportedAccessModes.Has(mode) {
			allErrs = append(allErrs, field.NotSupported(fldPath.Child("accessModes"), mode, sets.List(supportedAccessModes)))
		}

		if mode == core.ReadWriteOncePod {
			foundReadWriteOncePod = true
		} else if supportedAccessModes.Has(mode) {
			foundNonReadWriteOncePod = true
		}
	}
	if foundReadWriteOncePod && foundNonReadWriteOncePod {
		allErrs = append(allErrs, field.Forbidden(fldPath.Child("accessModes"), "may not use ReadWriteOncePod with other access modes"))
	}

	if !validateInlinePersistentVolumeSpec {
		if len(pvSpec.Capacity) == 0 {
			allErrs = append(allErrs, field.Required(fldPath.Child("capacity"), ""))
		}

		if _, ok := pvSpec.Capacity[core.ResourceStorage]; !ok || len(pvSpec.Capacity) > 1 {
			allErrs = append(allErrs, field.NotSupported(fldPath.Child("capacity"), pvSpec.Capacity, []core.ResourceName{core.ResourceStorage}))
		}
		capPath := fldPath.Child("capacity")
		for r, qty := range pvSpec.Capacity {
			allErrs = append(allErrs, validateBasicResource(qty, capPath.Key(string(r)))...)
			allErrs = append(allErrs, ValidatePositiveQuantityValue(qty, capPath.Key(string(r)))...)
		}
	}

	if len(pvSpec.PersistentVolumeReclaimPolicy) > 0 {
		if validateInlinePersistentVolumeSpec {
			if pvSpec.PersistentVolumeReclaimPolicy != core.PersistentVolumeReclaimRetain {
				allErrs = append(allErrs, field.Forbidden(fldPath.Child("persistentVolumeReclaimPolicy"), "may only be "+string(core.PersistentVolumeReclaimRetain)+" in the context of inline volumes"))
			}
		} else {
			if !supportedReclaimPolicy.Has(pvSpec.PersistentVolumeReclaimPolicy) {
				allErrs = append(allErrs, field.NotSupported(fldPath.Child("persistentVolumeReclaimPolicy"), pvSpec.PersistentVolumeReclaimPolicy, sets.List(supportedReclaimPolicy)))
			}
		}
	}

	var nodeAffinitySpecified bool
	var errs field.ErrorList
	if pvSpec.NodeAffinity != nil {
		if validateInlinePersistentVolumeSpec {
			allErrs = append(allErrs, field.Forbidden(fldPath.Child("nodeAffinity"), "may not be specified in the context of inline volumes"))
		} else {
			nodeAffinitySpecified, errs = validateVolumeNodeAffinity(pvSpec.NodeAffinity, opts, fldPath.Child("nodeAffinity"))
			allErrs = append(allErrs, errs...)
		}
	}
	numVolumes := 0
	if pvSpec.HostPath != nil {
		if numVolumes > 0 {
			allErrs = append(allErrs, field.Forbidden(fldPath.Child("hostPath"), "may not specify more than 1 volume type"))
		} else {
			numVolumes++
			allErrs = append(allErrs, validateHostPathVolumeSource(pvSpec.HostPath, fldPath.Child("hostPath"))...)
		}
	}
	if pvSpec.GCEPersistentDisk != nil {
		if numVolumes > 0 {
			allErrs = append(allErrs, field.Forbidden(fldPath.Child("gcePersistentDisk"), "may not specify more than 1 volume type"))
		} else {
			numVolumes++
			allErrs = append(allErrs, validateGCEPersistentDiskVolumeSource(pvSpec.GCEPersistentDisk, fldPath.Child("persistentDisk"))...)
		}
	}
	if pvSpec.AWSElasticBlockStore != nil {
		if numVolumes > 0 {
			allErrs = append(allErrs, field.Forbidden(fldPath.Child("awsElasticBlockStore"), "may not specify more than 1 volume type"))
		} else {
			numVolumes++
			allErrs = append(allErrs, validateAWSElasticBlockStoreVolumeSource(pvSpec.AWSElasticBlockStore, fldPath.Child("awsElasticBlockStore"))...)
		}
	}
	if pvSpec.Glusterfs != nil {
		if numVolumes > 0 {
			allErrs = append(allErrs, field.Forbidden(fldPath.Child("glusterfs"), "may not specify more than 1 volume type"))
		} else {
			numVolumes++
			allErrs = append(allErrs, validateGlusterfsPersistentVolumeSource(pvSpec.Glusterfs, fldPath.Child("glusterfs"))...)
		}
	}
	if pvSpec.Flocker != nil {
		if numVolumes > 0 {
			allErrs = append(allErrs, field.Forbidden(fldPath.Child("flocker"), "may not specify more than 1 volume type"))
		} else {
			numVolumes++
			allErrs = append(allErrs, validateFlockerVolumeSource(pvSpec.Flocker, fldPath.Child("flocker"))...)
		}
	}
	if pvSpec.NFS != nil {
		if numVolumes > 0 {
			allErrs = append(allErrs, field.Forbidden(fldPath.Child("nfs"), "may not specify more than 1 volume type"))
		} else {
			numVolumes++
			allErrs = append(allErrs, validateNFSVolumeSource(pvSpec.NFS, fldPath.Child("nfs"))...)
		}
	}
	if pvSpec.RBD != nil {
		if numVolumes > 0 {
			allErrs = append(allErrs, field.Forbidden(fldPath.Child("rbd"), "may not specify more than 1 volume type"))
		} else {
			numVolumes++
			allErrs = append(allErrs, validateRBDPersistentVolumeSource(pvSpec.RBD, fldPath.Child("rbd"))...)
		}
	}
	if pvSpec.Quobyte != nil {
		if numVolumes > 0 {
			allErrs = append(allErrs, field.Forbidden(fldPath.Child("quobyte"), "may not specify more than 1 volume type"))
		} else {
			numVolumes++
			allErrs = append(allErrs, validateQuobyteVolumeSource(pvSpec.Quobyte, fldPath.Child("quobyte"))...)
		}
	}
	if pvSpec.CephFS != nil {
		if numVolumes > 0 {
			allErrs = append(allErrs, field.Forbidden(fldPath.Child("cephFS"), "may not specify more than 1 volume type"))
		} else {
			numVolumes++
			allErrs = append(allErrs, validateCephFSPersistentVolumeSource(pvSpec.CephFS, fldPath.Child("cephfs"))...)
		}
	}
	if pvSpec.ISCSI != nil {
		if numVolumes > 0 {
			allErrs = append(allErrs, field.Forbidden(fldPath.Child("iscsi"), "may not specify more than 1 volume type"))
		} else {
			numVolumes++
			allErrs = append(allErrs, validateISCSIPersistentVolumeSource(pvSpec.ISCSI, pvName, fldPath.Child("iscsi"))...)
		}
	}
	if pvSpec.Cinder != nil {
		if numVolumes > 0 {
			allErrs = append(allErrs, field.Forbidden(fldPath.Child("cinder"), "may not specify more than 1 volume type"))
		} else {
			numVolumes++
			allErrs = append(allErrs, validateCinderPersistentVolumeSource(pvSpec.Cinder, fldPath.Child("cinder"))...)
		}
	}
	if pvSpec.FC != nil {
		if numVolumes > 0 {
			allErrs = append(allErrs, field.Forbidden(fldPath.Child("fc"), "may not specify more than 1 volume type"))
		} else {
			numVolumes++
			allErrs = append(allErrs, validateFCVolumeSource(pvSpec.FC, fldPath.Child("fc"))...)
		}
	}
	if pvSpec.FlexVolume != nil {
		numVolumes++
		allErrs = append(allErrs, validateFlexPersistentVolumeSource(pvSpec.FlexVolume, fldPath.Child("flexVolume"))...)
	}
	if pvSpec.AzureFile != nil {
		if numVolumes > 0 {
			allErrs = append(allErrs, field.Forbidden(fldPath.Child("azureFile"), "may not specify more than 1 volume type"))

		} else {
			numVolumes++
			allErrs = append(allErrs, validateAzureFilePV(pvSpec.AzureFile, fldPath.Child("azureFile"))...)
		}
	}

	if pvSpec.VsphereVolume != nil {
		if numVolumes > 0 {
			allErrs = append(allErrs, field.Forbidden(fldPath.Child("vsphereVolume"), "may not specify more than 1 volume type"))
		} else {
			numVolumes++
			allErrs = append(allErrs, validateVsphereVolumeSource(pvSpec.VsphereVolume, fldPath.Child("vsphereVolume"))...)
		}
	}
	if pvSpec.PhotonPersistentDisk != nil {
		if numVolumes > 0 {
			allErrs = append(allErrs, field.Forbidden(fldPath.Child("photonPersistentDisk"), "may not specify more than 1 volume type"))
		} else {
			numVolumes++
			allErrs = append(allErrs, validatePhotonPersistentDiskVolumeSource(pvSpec.PhotonPersistentDisk, fldPath.Child("photonPersistentDisk"))...)
		}
	}
	if pvSpec.PortworxVolume != nil {
		if numVolumes > 0 {
			allErrs = append(allErrs, field.Forbidden(fldPath.Child("portworxVolume"), "may not specify more than 1 volume type"))
		} else {
			numVolumes++
			allErrs = append(allErrs, validatePortworxVolumeSource(pvSpec.PortworxVolume, fldPath.Child("portworxVolume"))...)
		}
	}
	if pvSpec.AzureDisk != nil {
		if numVolumes > 0 {
			allErrs = append(allErrs, field.Forbidden(fldPath.Child("azureDisk"), "may not specify more than 1 volume type"))
		} else {
			numVolumes++
			allErrs = append(allErrs, validateAzureDisk(pvSpec.AzureDisk, fldPath.Child("azureDisk"))...)
		}
	}
	if pvSpec.ScaleIO != nil {
		if numVolumes > 0 {
			allErrs = append(allErrs, field.Forbidden(fldPath.Child("scaleIO"), "may not specify more than 1 volume type"))
		} else {
			numVolumes++
			allErrs = append(allErrs, validateScaleIOPersistentVolumeSource(pvSpec.ScaleIO, fldPath.Child("scaleIO"))...)
		}
	}
	if pvSpec.Local != nil {
		if numVolumes > 0 {
			allErrs = append(allErrs, field.Forbidden(fldPath.Child("local"), "may not specify more than 1 volume type"))
		} else {
			numVolumes++
			allErrs = append(allErrs, validateLocalVolumeSource(pvSpec.Local, fldPath.Child("local"))...)
			// NodeAffinity 为必填项
			if !nodeAffinitySpecified {
				allErrs = append(allErrs, field.Required(fldPath.Child("nodeAffinity"), "Local volume requires node affinity"))
			}
		}
	}
	if pvSpec.StorageOS != nil {
		if numVolumes > 0 {
			allErrs = append(allErrs, field.Forbidden(fldPath.Child("storageos"), "may not specify more than 1 volume type"))
		} else {
			numVolumes++
			allErrs = append(allErrs, validateStorageOSPersistentVolumeSource(pvSpec.StorageOS, fldPath.Child("storageos"))...)
		}
	}

	if pvSpec.CSI != nil {
		if numVolumes > 0 {
			allErrs = append(allErrs, field.Forbidden(fldPath.Child("csi"), "may not specify more than 1 volume type"))
		} else {
			numVolumes++
			allErrs = append(allErrs, validateCSIPersistentVolumeSource(pvSpec.CSI, fldPath.Child("csi"))...)
		}
	}

	if numVolumes == 0 {
		allErrs = append(allErrs, field.Required(fldPath, "must specify a volume type"))
	}

	// 不允许挂载路径为 '/' 的 hostPath 使用 'recycle' 回收策略
	if pvSpec.HostPath != nil && path.Clean(pvSpec.HostPath.Path) == "/" && pvSpec.PersistentVolumeReclaimPolicy == core.PersistentVolumeReclaimRecycle {
		allErrs = append(allErrs, field.Forbidden(fldPath.Child("persistentVolumeReclaimPolicy"), "may not be 'recycle' for a hostPath mount of '/'"))
	}

	if len(pvSpec.StorageClassName) > 0 {
		if validateInlinePersistentVolumeSpec {
			allErrs = append(allErrs, field.Forbidden(fldPath.Child("storageClassName"), "may not be specified in the context of inline volumes"))
		} else {
			for _, msg := range ValidateClassName(pvSpec.StorageClassName, false) {
				allErrs = append(allErrs, field.Invalid(fldPath.Child("storageClassName"), pvSpec.StorageClassName, msg))
			}
		}
	}
	if pvSpec.VolumeMode != nil {
		if validateInlinePersistentVolumeSpec {
			if *pvSpec.VolumeMode != core.PersistentVolumeFilesystem {
				allErrs = append(allErrs, field.Forbidden(fldPath.Child("volumeMode"), "may not specify volumeMode other than "+string(core.PersistentVolumeFilesystem)+" in the context of inline volumes"))
			}
		} else {
			if !supportedVolumeModes.Has(*pvSpec.VolumeMode) {
				allErrs = append(allErrs, field.NotSupported(fldPath.Child("volumeMode"), *pvSpec.VolumeMode, sets.List(supportedVolumeModes)))
			}
		}
	}
	if pvSpec.VolumeAttributesClassName != nil && opts.EnableVolumeAttributesClass {
		if len(*pvSpec.VolumeAttributesClassName) == 0 {
			allErrs = append(allErrs, field.Required(fldPath.Child("volumeAttributesClassName"), "an empty string is disallowed"))
		} else {
			for _, msg := range ValidateClassName(*pvSpec.VolumeAttributesClassName, false) {
				allErrs = append(allErrs, field.Invalid(fldPath.Child("volumeAttributesClassName"), *pvSpec.VolumeAttributesClassName, msg))
			}
		}
		if pvSpec.CSI == nil {
			allErrs = append(allErrs, field.Required(fldPath.Child("csi"), "has to be specified when using volumeAttributesClassName"))
		}
	}
	return allErrs
}

// ValidatePersistentVolume 校验整个 PersistentVolume 对象：元数据与规格。
func ValidatePersistentVolume(pv *core.PersistentVolume, opts PersistentVolumeSpecValidationOptions) field.ErrorList {
	metaPath := field.NewPath("metadata")
	allErrs := ValidateObjectMeta(&pv.ObjectMeta, false, ValidatePersistentVolumeName, metaPath)
	allErrs = append(allErrs, ValidatePersistentVolumeSpec(&pv.Spec, pv.ObjectMeta.Name, false, field.NewPath("spec"), opts)...)
	return allErrs
}

// ValidatePersistentVolumeUpdate 校验用户对 PV 的更新是否合法：卷源等字段不可变，部分字段允许有限度修改。
func ValidatePersistentVolumeUpdate(newPv, oldPv *core.PersistentVolume, opts PersistentVolumeSpecValidationOptions) field.ErrorList {
	allErrs := ValidatePersistentVolume(newPv, opts)

	// 若旧 PV 没有 ControllerExpandSecretRef，则允许在新对象中设置它
	if (oldPv.Spec.CSI != nil && oldPv.Spec.CSI.ControllerExpandSecretRef == nil) &&
		(newPv.Spec.CSI != nil && newPv.Spec.CSI.ControllerExpandSecretRef != nil) {
		newPv = newPv.DeepCopy()
		newPv.Spec.CSI.ControllerExpandSecretRef = nil
	}

	// PersistentVolumeSource 创建后应当不可变。
	if !apiequality.Semantic.DeepEqual(newPv.Spec.PersistentVolumeSource, oldPv.Spec.PersistentVolumeSource) {
		pvcSourceDiff := diff.Diff(oldPv.Spec.PersistentVolumeSource, newPv.Spec.PersistentVolumeSource)
		allErrs = append(allErrs, field.Forbidden(field.NewPath("spec", "persistentvolumesource"), fmt.Sprintf("spec.persistentvolumesource is immutable after creation\n%v", pvcSourceDiff)))
	}
	allErrs = append(allErrs, ValidateImmutableField(newPv.Spec.VolumeMode, oldPv.Spec.VolumeMode, field.NewPath("volumeMode"))...)

	// 若旧 PV 的 NodeAffinity 未设置，则允许设置它
	if !utilfeature.DefaultFeatureGate.Enabled(features.MutablePVNodeAffinity) &&
		oldPv.Spec.NodeAffinity != nil {
		allErrs = append(allErrs, validatePvNodeAffinity(newPv.Spec.NodeAffinity, oldPv.Spec.NodeAffinity, field.NewPath("nodeAffinity"))...)
	}

	if !apiequality.Semantic.DeepEqual(oldPv.Spec.VolumeAttributesClassName, newPv.Spec.VolumeAttributesClassName) {
		if !utilfeature.DefaultFeatureGate.Enabled(features.VolumeAttributesClass) {
			allErrs = append(allErrs, field.Forbidden(field.NewPath("spec", "volumeAttributesClassName"), "update is forbidden when the VolumeAttributesClass feature gate is disabled"))
		}
		if opts.EnableVolumeAttributesClass {
			if oldPv.Spec.VolumeAttributesClassName != nil && newPv.Spec.VolumeAttributesClassName == nil {
				allErrs = append(allErrs, field.Forbidden(field.NewPath("spec", "volumeAttributesClassName"), "update from non-nil value to nil is forbidden"))
			}
		}
	}

	return allErrs
}

// ValidatePersistentVolumeStatusUpdate 校验 PV 状态更新是否合法（仅元数据与 resourceVersion 约束）。
func ValidatePersistentVolumeStatusUpdate(newPv, oldPv *core.PersistentVolume) field.ErrorList {
	allErrs := ValidateObjectMetaUpdate(&newPv.ObjectMeta, &oldPv.ObjectMeta, field.NewPath("metadata"))
	if len(newPv.ResourceVersion) == 0 {
		allErrs = append(allErrs, field.Required(field.NewPath("resourceVersion"), ""))
	}
	return allErrs
}

type PersistentVolumeClaimSpecValidationOptions struct {
	// 允许用户从此前失败的扩容操作中恢复
	EnableRecoverFromExpansionFailure bool
	// 允许 selector 中存在非法的 label-value
	AllowInvalidLabelValueInSelector bool
	// 允许 dataSource 与 dataSourceRef 的 API group 非法
	AllowInvalidAPIGroupInDataSourceOrRef bool
	// 允许用户修改 volume attributes 的 class
	EnableVolumeAttributesClass bool
}

// ValidationOptionsForPersistentVolumeClaimCreate 为 PVC 创建操作生成校验选项，基于当前特性门控状态。
func ValidationOptionsForPersistentVolumeClaimCreate() PersistentVolumeClaimSpecValidationOptions {
	return PersistentVolumeClaimSpecValidationOptions{
		EnableRecoverFromExpansionFailure: utilfeature.DefaultFeatureGate.Enabled(features.RecoverVolumeExpansionFailure),
		AllowInvalidLabelValueInSelector:  false,
		EnableVolumeAttributesClass:       utilfeature.DefaultFeatureGate.Enabled(features.VolumeAttributesClass),
	}
}

// ValidationOptionsForPersistentVolumeClaim 根据 PVC 及旧对象状态生成校验选项，兼容旧对象中已存在的不规范值。
func ValidationOptionsForPersistentVolumeClaim(pvc, oldPvc *core.PersistentVolumeClaim) PersistentVolumeClaimSpecValidationOptions {
	opts := ValidationOptionsForPersistentVolumeClaimCreate()
	if oldPvc == nil {
		// 没有旧 PVC 时，仅根据特性门控启用情况决定选项
		return opts
	}

	// 若旧对象的 dataSource 或 dataSourceRef 中存在非法 API 组，则在新对象中继续允许
	opts.AllowInvalidAPIGroupInDataSourceOrRef = allowInvalidAPIGroupInDataSourceOrRef(&oldPvc.Spec)

	if oldPvc.Spec.VolumeAttributesClassName != nil {
		// 若旧对象设置了 volume attributes class，则对新对象继续校验它
		opts.EnableVolumeAttributesClass = true
	}

	labelSelectorValidationOpts := unversionedvalidation.LabelSelectorValidationOptions{
		AllowInvalidLabelValueInSelector: opts.AllowInvalidLabelValueInSelector,
	}
	if len(unversionedvalidation.ValidateLabelSelector(oldPvc.Spec.Selector, labelSelectorValidationOpts, nil)) > 0 {
		// 若旧对象存在非法的 label selector，则在新对象中继续容忍
		opts.AllowInvalidLabelValueInSelector = true
	}

	if helper.ClaimContainsAllocatedResources(oldPvc) ||
		helper.ClaimContainsAllocatedResourceStatus(oldPvc) {
		opts.EnableRecoverFromExpansionFailure = true
	}
	return opts
}

// ValidationOptionsForPersistentVolumeClaimTemplate 根据 PVC 模板及旧模板状态生成校验选项，兼容旧模板中已存在的非法标签选择器。
func ValidationOptionsForPersistentVolumeClaimTemplate(claimTemplate, oldClaimTemplate *core.PersistentVolumeClaimTemplate) PersistentVolumeClaimSpecValidationOptions {
	opts := PersistentVolumeClaimSpecValidationOptions{
		AllowInvalidLabelValueInSelector: false,
		EnableVolumeAttributesClass:      utilfeature.DefaultFeatureGate.Enabled(features.VolumeAttributesClass),
	}
	if oldClaimTemplate == nil {
		// 没有旧 PVC 模板时，仅根据特性门控启用情况决定选项
		return opts
	}
	labelSelectorValidationOpts := unversionedvalidation.LabelSelectorValidationOptions{
		AllowInvalidLabelValueInSelector: opts.AllowInvalidLabelValueInSelector,
	}
	if len(unversionedvalidation.ValidateLabelSelector(oldClaimTemplate.Spec.Selector, labelSelectorValidationOpts, nil)) > 0 {
		// 若旧对象存在非法的 label selector，则在新对象中继续容忍
		opts.AllowInvalidLabelValueInSelector = true
	}
	return opts
}

// allowInvalidAPIGroupInDataSourceOrRef 当 spec 中存在带非法 API 组的 dataSource 或 dataSourceRef 时返回 true，用于兼容旧对象。
func allowInvalidAPIGroupInDataSourceOrRef(spec *core.PersistentVolumeClaimSpec) bool {
	if spec.DataSource != nil && spec.DataSource.APIGroup != nil {
		return true
	}
	if spec.DataSourceRef != nil && spec.DataSourceRef.APIGroup != nil {
		return true
	}
	return false
}

// ValidatePersistentVolumeClaim 校验整个 PersistentVolumeClaim 对象：元数据与规格。
func ValidatePersistentVolumeClaim(pvc *core.PersistentVolumeClaim, opts PersistentVolumeClaimSpecValidationOptions) field.ErrorList {
	allErrs := ValidateObjectMeta(&pvc.ObjectMeta, true, ValidatePersistentVolumeName, field.NewPath("metadata"))
	allErrs = append(allErrs, ValidatePersistentVolumeClaimSpec(&pvc.Spec, field.NewPath("spec"), opts)...)
	return allErrs
}

// validateDataSource 校验 PVC 中的 DataSource 字段（名称、命名空间、API 组）是否合法。
func validateDataSource(dataSource *core.TypedLocalObjectReference, fldPath *field.Path, allowInvalidAPIGroupInDataSourceOrRef bool) field.ErrorList {
	allErrs := field.ErrorList{}

	if len(dataSource.Name) == 0 {
		allErrs = append(allErrs, field.Required(fldPath.Child("name"), ""))
	}
	if len(dataSource.Kind) == 0 {
		allErrs = append(allErrs, field.Required(fldPath.Child("kind"), ""))
	}
	apiGroup := ""
	if dataSource.APIGroup != nil {
		apiGroup = *dataSource.APIGroup
	}
	if len(apiGroup) == 0 && dataSource.Kind != "PersistentVolumeClaim" {
		allErrs = append(allErrs, field.Invalid(fldPath, dataSource.Kind, "must be 'PersistentVolumeClaim' when referencing the default apiGroup"))
	}
	if len(apiGroup) > 0 && !allowInvalidAPIGroupInDataSourceOrRef {
		for _, errString := range validation.IsDNS1123Subdomain(apiGroup) {
			allErrs = append(allErrs, field.Invalid(fldPath.Child("apiGroup"), apiGroup, errString))
		}
	}

	return allErrs
}

// validateDataSourceRef 校验 PVC 中的 DataSourceRef 字段，并兼容旧的 DataSource 表达形式。
func validateDataSourceRef(dataSourceRef *core.TypedObjectReference, fldPath *field.Path, allowInvalidAPIGroupInDataSourceOrRef bool) field.ErrorList {
	allErrs := field.ErrorList{}

	if len(dataSourceRef.Name) == 0 {
		allErrs = append(allErrs, field.Required(fldPath.Child("name"), ""))
	}
	if len(dataSourceRef.Kind) == 0 {
		allErrs = append(allErrs, field.Required(fldPath.Child("kind"), ""))
	}
	apiGroup := ""
	if dataSourceRef.APIGroup != nil {
		apiGroup = *dataSourceRef.APIGroup
	}
	if len(apiGroup) == 0 && dataSourceRef.Kind != "PersistentVolumeClaim" {
		allErrs = append(allErrs, field.Invalid(fldPath, dataSourceRef.Kind, "must be 'PersistentVolumeClaim' when referencing the default apiGroup"))
	}
	if len(apiGroup) > 0 && !allowInvalidAPIGroupInDataSourceOrRef {
		for _, errString := range validation.IsDNS1123Subdomain(apiGroup) {
			allErrs = append(allErrs, field.Invalid(fldPath.Child("apiGroup"), apiGroup, errString))
		}
	}

	if dataSourceRef.Namespace != nil && len(*dataSourceRef.Namespace) > 0 {
		for _, msg := range ValidateNameFunc(ValidateNamespaceName)(*dataSourceRef.Namespace, false) {
			allErrs = append(allErrs, field.Invalid(fldPath.Child("namespace"), *dataSourceRef.Namespace, msg))
		}
	}

	return allErrs
}

// ValidatePersistentVolumeClaimSpec 校验 PVC 规格：访问模式、资源请求、存储类、选择器与数据源等。
func ValidatePersistentVolumeClaimSpec(spec *core.PersistentVolumeClaimSpec, fldPath *field.Path, opts PersistentVolumeClaimSpecValidationOptions) field.ErrorList {
	allErrs := field.ErrorList{}
	if len(spec.AccessModes) == 0 {
		allErrs = append(allErrs, field.Required(fldPath.Child("accessModes"), "at least 1 access mode is required"))
	}
	if spec.Selector != nil {
		labelSelectorValidationOpts := unversionedvalidation.LabelSelectorValidationOptions{
			AllowInvalidLabelValueInSelector: opts.AllowInvalidLabelValueInSelector,
		}
		allErrs = append(allErrs, unversionedvalidation.ValidateLabelSelector(spec.Selector, labelSelectorValidationOpts, fldPath.Child("selector"))...)
	}

	foundReadWriteOncePod, foundNonReadWriteOncePod := false, false
	for _, mode := range spec.AccessModes {
		if !supportedAccessModes.Has(mode) {
			allErrs = append(allErrs, field.NotSupported(fldPath.Child("accessModes"), mode, sets.List(supportedAccessModes)))
		}

		if mode == core.ReadWriteOncePod {
			foundReadWriteOncePod = true
		} else if supportedAccessModes.Has(mode) {
			foundNonReadWriteOncePod = true
		}
	}
	if foundReadWriteOncePod && foundNonReadWriteOncePod {
		allErrs = append(allErrs, field.Forbidden(fldPath.Child("accessModes"), "may not use ReadWriteOncePod with other access modes"))
	}

	storageValue, ok := spec.Resources.Requests[core.ResourceStorage]
	if !ok {
		allErrs = append(allErrs, field.Required(fldPath.Child("resources").Key(string(core.ResourceStorage)), ""))
	} else if errs := ValidatePositiveQuantityValue(storageValue, fldPath.Child("resources").Key(string(core.ResourceStorage))); len(errs) > 0 {
		allErrs = append(allErrs, errs...)
	} else {
		allErrs = append(allErrs, ValidateResourceQuantityValue(core.ResourceStorage, storageValue, fldPath.Child("resources").Key(string(core.ResourceStorage)))...)
	}

	if spec.StorageClassName != nil && len(*spec.StorageClassName) > 0 {
		for _, msg := range ValidateClassName(*spec.StorageClassName, false) {
			allErrs = append(allErrs, field.Invalid(fldPath.Child("storageClassName"), *spec.StorageClassName, msg))
		}
	}
	if spec.VolumeMode != nil && !supportedVolumeModes.Has(*spec.VolumeMode) {
		allErrs = append(allErrs, field.NotSupported(fldPath.Child("volumeMode"), *spec.VolumeMode, sets.List(supportedVolumeModes)))
	}

	if spec.DataSource != nil {
		allErrs = append(allErrs, validateDataSource(spec.DataSource, fldPath.Child("dataSource"), opts.AllowInvalidAPIGroupInDataSourceOrRef)...)
	}
	if spec.DataSourceRef != nil {
		allErrs = append(allErrs, validateDataSourceRef(spec.DataSourceRef, fldPath.Child("dataSourceRef"), opts.AllowInvalidAPIGroupInDataSourceOrRef)...)
	}
	if spec.DataSourceRef != nil && spec.DataSourceRef.Namespace != nil && len(*spec.DataSourceRef.Namespace) > 0 {
		if spec.DataSource != nil {
			allErrs = append(allErrs, field.Invalid(fldPath, fldPath.Child("dataSource"),
				"may not be specified when dataSourceRef.namespace is specified"))
		}
	} else if spec.DataSource != nil && spec.DataSourceRef != nil {
		if !isDataSourceEqualDataSourceRef(spec.DataSource, spec.DataSourceRef) {
			allErrs = append(allErrs, field.Invalid(fldPath, fldPath.Child("dataSource"),
				"must match dataSourceRef"))
		}
	}
	if spec.VolumeAttributesClassName != nil && len(*spec.VolumeAttributesClassName) > 0 && opts.EnableVolumeAttributesClass {
		for _, msg := range ValidateClassName(*spec.VolumeAttributesClassName, false) {
			allErrs = append(allErrs, field.Invalid(fldPath.Child("volumeAttributesClassName"), *spec.VolumeAttributesClassName, msg))
		}
	}

	return allErrs
}

// isDataSourceEqualDataSourceRef 判断旧的 DataSource 与新的 DataSourceRef 是否指向同一对象，用于兼容性判断。
func isDataSourceEqualDataSourceRef(dataSource *core.TypedLocalObjectReference, dataSourceRef *core.TypedObjectReference) bool {
	return reflect.DeepEqual(dataSource.APIGroup, dataSourceRef.APIGroup) && dataSource.Kind == dataSourceRef.Kind && dataSource.Name == dataSourceRef.Name
}

// ValidatePersistentVolumeClaimUpdate 校验用户对 PVC 的更新是否合法：允许扩容与存储类升级，其他关键字段不可变。
func ValidatePersistentVolumeClaimUpdate(newPvc, oldPvc *core.PersistentVolumeClaim, opts PersistentVolumeClaimSpecValidationOptions) field.ErrorList {
	allErrs := ValidateObjectMetaUpdate(&newPvc.ObjectMeta, &oldPvc.ObjectMeta, field.NewPath("metadata"))
	allErrs = append(allErrs, ValidatePersistentVolumeClaim(newPvc, opts)...)
	newPvcClone := newPvc.DeepCopy()
	oldPvcClone := oldPvc.DeepCopy()

	// PVController 需要用 VolumeName 更新 PVC.Spec。
	// 为保证配额、范围限制等不被绕过，声明整体保持不可变。
	if len(oldPvc.Spec.VolumeName) == 0 {
		// volumeName 允许变更一次。
		oldPvcClone.Spec.VolumeName = newPvcClone.Spec.VolumeName // +k8s:verify-mutation:reason=clone
	}

	if validateStorageClassUpgradeFromAnnotation(oldPvcClone.Annotations, newPvcClone.Annotations,
		oldPvcClone.Spec.StorageClassName, newPvcClone.Spec.StorageClassName) {
		newPvcClone.Spec.StorageClassName = nil
		metav1.SetMetaDataAnnotation(&newPvcClone.ObjectMeta, core.BetaStorageClassAnnotation, oldPvcClone.Annotations[core.BetaStorageClassAnnotation])
	} else {
		// storageclass 注解创建后应当不可变
		// TODO: Beta 版本不再需要后移除
		allErrs = append(allErrs, ValidateImmutableAnnotation(newPvc.ObjectMeta.Annotations[v1.BetaStorageClassAnnotation], oldPvc.ObjectMeta.Annotations[v1.BetaStorageClassAnnotation], v1.BetaStorageClassAnnotation, field.NewPath("metadata"))...)

		// 若从注解升级到属性的校验失败，可再尝试按“属性从无值开始变更”来校验。
		if validateStorageClassUpgradeFromNil(oldPvc.Annotations, oldPvc.Spec.StorageClassName, newPvc.Spec.StorageClassName, opts) {
			newPvcClone.Spec.StorageClassName = oldPvcClone.Spec.StorageClassName // +k8s:verify-mutation:reason=clone
		}
		// TODO: 增加专门的错误，提示 storage class name 不可变更
		//（而不是让下方的 spec 比较返回笼统的 field forbidden 错误）
	}

	// 确保 storage 取值保持一致。
	if newPvc.Status.Phase == core.ClaimBound && newPvcClone.Spec.Resources.Requests != nil {
		newPvcClone.Spec.Resources.Requests["storage"] = oldPvc.Spec.Resources.Requests["storage"] // +k8s:verify-mutation:reason=clone
	}
	// 确保 volume attributes class name 保持一致。
	if newPvc.Status.Phase == core.ClaimBound {
		newPvcClone.Spec.VolumeAttributesClassName = oldPvcClone.Spec.VolumeAttributesClassName // +k8s:verify-mutation:reason=clone
	}

	oldSize := oldPvc.Spec.Resources.Requests["storage"]
	newSize := newPvc.Spec.Resources.Requests["storage"]
	statusSize := oldPvc.Status.Capacity["storage"]

	if !apiequality.Semantic.DeepEqual(newPvcClone.Spec, oldPvcClone.Spec) {
		specDiff := diff.Diff(oldPvcClone.Spec, newPvcClone.Spec)
		allErrs = append(allErrs, field.Forbidden(field.NewPath("spec"), fmt.Sprintf("spec is immutable after creation except resources.requests and volumeAttributesClassName for bound claims\n%v", specDiff)))
	}
	if newSize.Cmp(oldSize) < 0 {
		if !opts.EnableRecoverFromExpansionFailure {
			allErrs = append(allErrs, field.Forbidden(field.NewPath("spec", "resources", "requests", "storage"), "field can not be less than previous value"))
		} else {
			// 此校验允许把 PVC 请求大小下调到 pvc.status 记录的容量，
			// 便于用户从卷扩容失败中恢复，但 Kubernetes 并不真正
			// 支持卷缩容
			if newSize.Cmp(statusSize) <= 0 {
				allErrs = append(allErrs, field.Forbidden(field.NewPath("spec", "resources", "requests", "storage"), "field can not be less than status.capacity"))
			}
		}
	}

	allErrs = append(allErrs, ValidateImmutableField(newPvc.Spec.VolumeMode, oldPvc.Spec.VolumeMode, field.NewPath("volumeMode"))...)

	if !apiequality.Semantic.DeepEqual(oldPvc.Spec.VolumeAttributesClassName, newPvc.Spec.VolumeAttributesClassName) {
		if !utilfeature.DefaultFeatureGate.Enabled(features.VolumeAttributesClass) {
			allErrs = append(allErrs, field.Forbidden(field.NewPath("spec", "volumeAttributesClassName"), "update is forbidden when the VolumeAttributesClass feature gate is disabled"))
		}
		if opts.EnableVolumeAttributesClass {
			// 一旦 VAC 成功应用，禁止再将其移除。
			if oldPvc.Status.CurrentVolumeAttributesClassName != nil {
				if newPvc.Spec.VolumeAttributesClassName == nil {
					allErrs = append(allErrs, field.Forbidden(field.NewPath("spec", "volumeAttributesClassName"), "update to nil is forbidden when status.currentVolumeAttributesClassName is not nil"))
				} else if len(*newPvc.Spec.VolumeAttributesClassName) == 0 {
					allErrs = append(allErrs, field.Forbidden(field.NewPath("spec", "volumeAttributesClassName"), "update to empty string is forbidden when status.currentVolumeAttributesClassName is not nil"))
				}
			}
		}
	}

	return allErrs
}

// validateStorageClassUpgradeFromAnnotation 为从 beta 注解迁移到 spec.storageClassName 提供升级路径，仅在四个条件同时满足时允许更新存储类。
func validateStorageClassUpgradeFromAnnotation(oldAnnotations, newAnnotations map[string]string, oldScName, newScName *string) bool {
	oldSc, oldAnnotationExist := oldAnnotations[core.BetaStorageClassAnnotation]
	newScInAnnotation, newAnnotationExist := newAnnotations[core.BetaStorageClassAnnotation]
	return oldAnnotationExist /* 条件 1 */ &&
		oldScName == nil /* 条件 2 */ &&
		(newScName != nil && *newScName == oldSc) /* 条件 3 */ &&
		(!newAnnotationExist || newScInAnnotation == oldSc) /* 条件 4 */
}

// validateStorageClassUpgradeFromNil 为从 nil 存储类升级到显式存储类提供路径，仅在满足兼容性条件时允许更新。
func validateStorageClassUpgradeFromNil(oldAnnotations map[string]string, oldScName, newScName *string, opts PersistentVolumeClaimSpecValidationOptions) bool {
	oldAnnotation, oldAnnotationExist := oldAnnotations[core.BetaStorageClassAnnotation]
	return newScName != nil /* 条件 1 */ &&
		oldScName == nil /* 条件 2 */ &&
		(!oldAnnotationExist || *newScName == oldAnnotation) /* 条件 3 */
}

// validatePersistentVolumeClaimResourceKey 校验 PVC 资源键：仅允许 storage 类资源，且请求量必须为正数。
func validatePersistentVolumeClaimResourceKey(value string, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	for _, msg := range validation.IsQualifiedName(value) {
		allErrs = append(allErrs, field.Invalid(fldPath, value, msg))
	}
	if len(allErrs) != 0 {
		return allErrs
	}
	// 对原生资源名（无前缀或带 kubernetes.io 前缀）而言，
	// 唯一允许的取值是 storage
	if helper.IsNativeResource(core.ResourceName(value)) {
		if core.ResourceName(value) != core.ResourceStorage {
			return append(allErrs, field.NotSupported(fldPath, value, []core.ResourceName{core.ResourceStorage}))
		}
	}
	return allErrs
}

var resizeStatusSet = sets.New(core.PersistentVolumeClaimControllerResizeInProgress,
	core.PersistentVolumeClaimControllerResizeInfeasible,
	core.PersistentVolumeClaimNodeResizePending,
	core.PersistentVolumeClaimNodeResizeInProgress,
	core.PersistentVolumeClaimNodeResizeInfeasible)

// validatePodVolumeHealth 校验 Pod 状态中的卷健康信息（volumeHealth）。
func validatePodVolumeHealth(volumeHealth []core.PodVolumeHealth, spec *core.PodSpec, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	volumeNames := sets.New[string]()
	if spec != nil {
		for _, v := range spec.Volumes {
			volumeNames.Insert(v.Name)
		}
	}
	for i, vh := range volumeHealth {
		idxPath := fldPath.Index(i)
		if len(vh.Name) > 0 && !volumeNames.Has(vh.Name) {
			allErrs = append(allErrs, field.NotFound(idxPath.Child("name"), vh.Name))
		}
		allErrs = append(allErrs, validateVolumeHealthConditions(vh.HealthConditions, idxPath.Child("healthConditions"))...)
	}
	return allErrs
}

// validateVolumeHealthStatus 校验单个卷健康状态记录。
func validateVolumeHealthStatus(status *core.VolumeHealthStatus, fldPath *field.Path) field.ErrorList {
	if status == nil {
		return nil
	}
	return validateVolumeHealthConditions(status.HealthConditions, fldPath.Child("healthConditions"))
}

// validateVolumeHealthConditions 校验卷健康条件列表：条件类型、状态与时间戳。
func validateVolumeHealthConditions(conditions []core.VolumeHealthCondition, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	for i, condition := range conditions {
		allErrs = append(allErrs, validateVolumeHealthCondition(condition, fldPath.Index(i))...)
	}
	return allErrs
}

// validateVolumeHealthCondition 校验单条卷健康条件：类型、状态、原因及最后探测时间。
func validateVolumeHealthCondition(condition core.VolumeHealthCondition, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	if len(condition.Reason) == 0 {
		return allErrs
	}
	for _, msg := range unversionedvalidation.IsValidConditionReason(condition.Reason) {
		allErrs = append(allErrs, field.Invalid(fldPath.Child("reason"), condition.Reason, msg))
	}
	return allErrs
}

// ValidatePersistentVolumeClaimStatusUpdate 校验 PVC 状态更新：容量、访问模式与阶段只能按规则变化。
func ValidatePersistentVolumeClaimStatusUpdate(newPvc, oldPvc *core.PersistentVolumeClaim, validationOpts PersistentVolumeClaimSpecValidationOptions) field.ErrorList {
	allErrs := ValidateObjectMetaUpdate(&newPvc.ObjectMeta, &oldPvc.ObjectMeta, field.NewPath("metadata"))
	if len(newPvc.ResourceVersion) == 0 {
		allErrs = append(allErrs, field.Required(field.NewPath("resourceVersion"), ""))
	}
	if len(newPvc.Spec.AccessModes) == 0 {
		allErrs = append(allErrs, field.Required(field.NewPath("Spec", "accessModes"), ""))
	}

	capPath := field.NewPath("status", "capacity")
	for r, qty := range newPvc.Status.Capacity {
		allErrs = append(allErrs, validateBasicResource(qty, capPath.Key(string(r)))...)
	}
	allErrs = append(allErrs, validateVolumeHealthStatus(newPvc.Status.HealthStatus, field.NewPath("status", "healthStatus"))...)
	if validationOpts.EnableRecoverFromExpansionFailure {
		resizeStatusPath := field.NewPath("status", "allocatedResourceStatuses")
		if newPvc.Status.AllocatedResourceStatuses != nil {
			resizeStatus := newPvc.Status.AllocatedResourceStatuses
			for k, v := range resizeStatus {
				if errs := validatePersistentVolumeClaimResourceKey(k.String(), resizeStatusPath); len(errs) > 0 {
					allErrs = append(allErrs, errs...)
				}
				if !resizeStatusSet.Has(v) {
					allErrs = append(allErrs, field.NotSupported(resizeStatusPath, k, sets.List(resizeStatusSet)))
					continue
				}
			}
		}
		allocPath := field.NewPath("status", "allocatedResources")
		for r, qty := range newPvc.Status.AllocatedResources {
			if errs := validatePersistentVolumeClaimResourceKey(r.String(), allocPath); len(errs) > 0 {
				allErrs = append(allErrs, errs...)
				continue
			}

			if errs := validateBasicResource(qty, allocPath.Key(string(r))); len(errs) > 0 {
				allErrs = append(allErrs, errs...)
			} else {
				allErrs = append(allErrs, ValidateResourceQuantityValue(core.ResourceStorage, qty, allocPath.Key(string(r)))...)
			}
		}
	}
	return allErrs
}

var supportedPortProtocols = sets.New(
	core.ProtocolTCP,
	core.ProtocolUDP,
	core.ProtocolSCTP)

// validateContainerPorts 校验容器端口列表：端口范围、协议、名称唯一性以及 hostPort 合法性。
func validateContainerPorts(ports []core.ContainerPort, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}

	allNames := sets.Set[string]{}
	for i, port := range ports {
		idxPath := fldPath.Index(i)
		if len(port.Name) > 0 {
			if msgs := validation.IsValidPortName(port.Name); len(msgs) != 0 {
				for i = range msgs {
					allErrs = append(allErrs, field.Invalid(idxPath.Child("name"), port.Name, msgs[i]))
				}
			} else if allNames.Has(port.Name) {
				allErrs = append(allErrs, field.Duplicate(idxPath.Child("name"), port.Name))
			} else {
				allNames.Insert(port.Name)
			}
		}
		if port.ContainerPort == 0 {
			allErrs = append(allErrs, field.Required(idxPath.Child("containerPort"), ""))
		} else {
			for _, msg := range validation.IsValidPortNum(int(port.ContainerPort)) {
				allErrs = append(allErrs, field.Invalid(idxPath.Child("containerPort"), port.ContainerPort, msg))
			}
		}
		if port.HostPort != 0 {
			for _, msg := range validation.IsValidPortNum(int(port.HostPort)) {
				allErrs = append(allErrs, field.Invalid(idxPath.Child("hostPort"), port.HostPort, msg))
			}
		}
		if len(port.Protocol) == 0 {
			allErrs = append(allErrs, field.Required(idxPath.Child("protocol"), ""))
		} else if !supportedPortProtocols.Has(port.Protocol) {
			allErrs = append(allErrs, field.NotSupported(idxPath.Child("protocol"), port.Protocol, sets.List(supportedPortProtocols)))
		}
	}
	return allErrs
}

// ValidateEnv 校验环境变量列表：名称格式、值来源字段的互斥性与引用的合法性。
func ValidateEnv(vars []core.EnvVar, fldPath *field.Path, opts PodValidationOptions) field.ErrorList {
	allErrs := field.ErrorList{}

	for i, ev := range vars {
		idxPath := fldPath.Index(i)
		if len(ev.Name) == 0 {
			allErrs = append(allErrs, field.Required(idxPath.Child("name"), ""))
		} else {
			if opts.AllowRelaxedEnvironmentVariableValidation {
				for _, msg := range validation.IsRelaxedEnvVarName(ev.Name) {
					allErrs = append(allErrs, field.Invalid(idxPath.Child("name"), ev.Name, msg))
				}
			} else {
				for _, msg := range validation.IsEnvVarName(ev.Name) {
					allErrs = append(allErrs, field.Invalid(idxPath.Child("name"), ev.Name, msg))
				}
			}
		}
		allErrs = append(allErrs, validateEnvVarValueFrom(ev, idxPath.Child("valueFrom"), opts)...)
	}
	return allErrs
}

var validEnvDownwardAPIFieldPathExpressions = sets.New(
	"metadata.name",
	"metadata.namespace",
	"metadata.uid",
	"spec.nodeName",
	"spec.serviceAccountName",
	"status.hostIP",
	"status.hostIPs",
	"status.podIP",
	"status.podIPs",
)

var validContainerResourceFieldPathExpressions = sets.New(
	"limits.cpu",
	"limits.memory",
	"limits.ephemeral-storage",
	"requests.cpu",
	"requests.memory",
	"requests.ephemeral-storage",
)

var validContainerResourceFieldPathPrefixesWithDownwardAPIHugePages = sets.New(hugepagesRequestsPrefixDownwardAPI, hugepagesLimitsPrefixDownwardAPI)

const hugepagesRequestsPrefixDownwardAPI string = `requests.hugepages-`
const hugepagesLimitsPrefixDownwardAPI string = `limits.hugepages-`

// validateEnvVarValueFrom 校验 EnvVar.ValueFrom 的各来源字段（fieldRef/resourceFieldRef/configMapKeyRef/secretKeyRef/fileKeyRef）是否合法且互斥。
func validateEnvVarValueFrom(ev core.EnvVar, fldPath *field.Path, opts PodValidationOptions) field.ErrorList {
	allErrs := field.ErrorList{}

	if ev.ValueFrom == nil {
		return allErrs
	}

	numSources := 0

	if ev.ValueFrom.FieldRef != nil {
		numSources++
		allErrs = append(allErrs, validateObjectFieldSelector(ev.ValueFrom.FieldRef, &validEnvDownwardAPIFieldPathExpressions, fldPath.Child("fieldRef"))...)
	}
	if ev.ValueFrom.ResourceFieldRef != nil {
		numSources++
		localValidContainerResourceFieldPathPrefixes := validContainerResourceFieldPathPrefixesWithDownwardAPIHugePages
		allErrs = append(allErrs, validateContainerResourceFieldSelector(ev.ValueFrom.ResourceFieldRef, &validContainerResourceFieldPathExpressions, &localValidContainerResourceFieldPathPrefixes, fldPath.Child("resourceFieldRef"), false)...)
	}
	if ev.ValueFrom.ConfigMapKeyRef != nil {
		numSources++
		allErrs = append(allErrs, validateConfigMapKeySelector(ev.ValueFrom.ConfigMapKeyRef, fldPath.Child("configMapKeyRef"))...)
	}
	if ev.ValueFrom.SecretKeyRef != nil {
		numSources++
		allErrs = append(allErrs, validateSecretKeySelector(ev.ValueFrom.SecretKeyRef, fldPath.Child("secretKeyRef"))...)
	}

	if ev.ValueFrom.FileKeyRef != nil {
		numSources++
		allErrs = append(allErrs, validateFileKeySelector(ev.ValueFrom.FileKeyRef, fldPath.Child("fileKeyRef"))...)
	}

	if numSources == 0 {
		if opts.AllowEnvFilesValidation {
			allErrs = append(allErrs, field.Invalid(fldPath, "", "must specify one of: `fieldRef`, `resourceFieldRef`, `configMapKeyRef`, `secretKeyRef` or `fileKeyRef`"))
		} else {
			allErrs = append(allErrs, field.Invalid(fldPath, "", "must specify one of: `fieldRef`, `resourceFieldRef`, `configMapKeyRef` or `secretKeyRef`"))
		}
	} else if len(ev.Value) != 0 {
		if numSources != 0 {
			allErrs = append(allErrs, field.Invalid(fldPath, "", "may not be specified when `value` is not empty"))
		}
	} else if numSources > 1 {
		allErrs = append(allErrs, field.Invalid(fldPath, "", "may not have more than one field specified at a time"))
	}

	return allErrs
}

// validateObjectFieldSelector 校验 DownwardAPI 对象字段选择器：API 版本与字段路径是否受支持。
func validateObjectFieldSelector(fs *core.ObjectFieldSelector, expressions *sets.Set[string], fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}

	if len(fs.APIVersion) == 0 {
		allErrs = append(allErrs, field.Required(fldPath.Child("apiVersion"), ""))
		return allErrs
	}
	if len(fs.FieldPath) == 0 {
		allErrs = append(allErrs, field.Required(fldPath.Child("fieldPath"), ""))
		return allErrs
	}

	internalFieldPath, _, err := podshelper.ConvertDownwardAPIFieldLabel(fs.APIVersion, fs.FieldPath, "")
	if err != nil {
		allErrs = append(allErrs, field.Invalid(fldPath.Child("fieldPath"), fs.FieldPath, fmt.Sprintf("error converting fieldPath: %v", err)))
		return allErrs
	}

	if path, subscript, ok := fieldpath.SplitMaybeSubscriptedPath(internalFieldPath); ok {
		switch path {
		case "metadata.annotations":
			allErrs = append(allErrs, ValidateQualifiedName(strings.ToLower(subscript), fldPath)...)
		case "metadata.labels":
			allErrs = append(allErrs, ValidateQualifiedName(subscript, fldPath)...)
		default:
			allErrs = append(allErrs, field.Invalid(fldPath, path, "does not support subscript"))
		}
	} else if !expressions.Has(path) {
		allErrs = append(allErrs, field.NotSupported(fldPath.Child("fieldPath"), path, sets.List(*expressions)))
		return allErrs
	}

	return allErrs
}

// validateContainerResourceFieldSelector 校验容器资源字段选择器：资源名、divisor 与容器名引用是否合法。
func validateContainerResourceFieldSelector(fs *core.ResourceFieldSelector, expressions *sets.Set[string], prefixes *sets.Set[string], fldPath *field.Path, volume bool) field.ErrorList {
	allErrs := field.ErrorList{}

	if volume && len(fs.ContainerName) == 0 {
		allErrs = append(allErrs, field.Required(fldPath.Child("containerName"), ""))
	} else if len(fs.Resource) == 0 {
		allErrs = append(allErrs, field.Required(fldPath.Child("resource"), ""))
	} else if !expressions.Has(fs.Resource) {
		// 检查前缀是否存在
		foundPrefix := false
		if prefixes != nil {
			for _, prefix := range sets.List(*prefixes) {
				if strings.HasPrefix(fs.Resource, prefix) {
					foundPrefix = true
				}
			}
		}
		if !foundPrefix {
			allErrs = append(allErrs, field.NotSupported(fldPath.Child("resource"), fs.Resource, sets.List(*expressions)))
		}
	}
	allErrs = append(allErrs, validateContainerResourceDivisor(fs.Resource, fs.Divisor, fldPath)...)
	return allErrs
}

// ValidateEnvFrom 校验 envFrom 列表：来源类型互斥且 configMapRef/secretRef 引用合法。
func ValidateEnvFrom(vars []core.EnvFromSource, fldPath *field.Path, opts PodValidationOptions) field.ErrorList {
	allErrs := field.ErrorList{}
	for i, ev := range vars {
		idxPath := fldPath.Index(i)
		if len(ev.Prefix) > 0 {
			if opts.AllowRelaxedEnvironmentVariableValidation {
				for _, msg := range validation.IsRelaxedEnvVarName(ev.Prefix) {
					allErrs = append(allErrs, field.Invalid(idxPath.Child("prefix"), ev.Prefix, msg))
				}
			} else {
				for _, msg := range validation.IsEnvVarName(ev.Prefix) {
					allErrs = append(allErrs, field.Invalid(idxPath.Child("prefix"), ev.Prefix, msg))
				}
			}
		}

		numSources := 0
		if ev.ConfigMapRef != nil {
			numSources++
			allErrs = append(allErrs, validateConfigMapEnvSource(ev.ConfigMapRef, idxPath.Child("configMapRef"))...)
		}
		if ev.SecretRef != nil {
			numSources++
			allErrs = append(allErrs, validateSecretEnvSource(ev.SecretRef, idxPath.Child("secretRef"))...)
		}

		if numSources == 0 {
			allErrs = append(allErrs, field.Invalid(fldPath, "", "must specify one of: `configMapRef` or `secretRef`"))
		} else if numSources > 1 {
			allErrs = append(allErrs, field.Invalid(fldPath, "", "may not have more than one field specified at a time"))
		}
	}
	return allErrs
}

// validateConfigMapEnvSource 校验 envFrom 中的 ConfigMap 来源引用。
func validateConfigMapEnvSource(configMapSource *core.ConfigMapEnvSource, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	if len(configMapSource.Name) == 0 {
		allErrs = append(allErrs, field.Required(fldPath.Child("name"), ""))
	} else {
		for _, msg := range ValidateConfigMapName(configMapSource.Name, true) {
			allErrs = append(allErrs, field.Invalid(fldPath.Child("name"), configMapSource.Name, msg))
		}
	}
	return allErrs
}

// validateSecretEnvSource 校验 envFrom 中的 Secret 来源引用。
func validateSecretEnvSource(secretSource *core.SecretEnvSource, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	if len(secretSource.Name) == 0 {
		allErrs = append(allErrs, field.Required(fldPath.Child("name"), ""))
	} else {
		for _, msg := range ValidateSecretName(secretSource.Name, true) {
			allErrs = append(allErrs, field.Invalid(fldPath.Child("name"), secretSource.Name, msg))
		}
	}
	return allErrs
}

var validContainerResourceDivisorForCPU = sets.New("1m", "1")
var validContainerResourceDivisorForMemory = sets.New(
	"1",
	"1k", "1M", "1G", "1T", "1P", "1E",
	"1Ki", "1Mi", "1Gi", "1Ti", "1Pi", "1Ei")
var validContainerResourceDivisorForHugePages = sets.New(
	"1",
	"1k", "1M", "1G", "1T", "1P", "1E",
	"1Ki", "1Mi", "1Gi", "1Ti", "1Pi", "1Ei")
var validContainerResourceDivisorForEphemeralStorage = sets.New(
	"1",
	"1k", "1M", "1G", "1T", "1P", "1E",
	"1Ki", "1Mi", "1Gi", "1Ti", "1Pi", "1Ei")

// validateContainerResourceDivisor 校验资源字段选择器的 divisor 是否为正数。
func validateContainerResourceDivisor(rName string, divisor resource.Quantity, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	unsetDivisor := resource.Quantity{}
	if unsetDivisor.Cmp(divisor) == 0 {
		return allErrs
	}
	switch rName {
	case "limits.cpu", "requests.cpu":
		if !validContainerResourceDivisorForCPU.Has(divisor.String()) {
			allErrs = append(allErrs, field.Invalid(fldPath.Child("divisor"), rName, "only divisor's values 1m and 1 are supported with the cpu resource"))
		}
	case "limits.memory", "requests.memory":
		if !validContainerResourceDivisorForMemory.Has(divisor.String()) {
			allErrs = append(allErrs, field.Invalid(fldPath.Child("divisor"), rName, "only divisor's values 1, 1k, 1M, 1G, 1T, 1P, 1E, 1Ki, 1Mi, 1Gi, 1Ti, 1Pi, 1Ei are supported with the memory resource"))
		}
	case "limits.ephemeral-storage", "requests.ephemeral-storage":
		if !validContainerResourceDivisorForEphemeralStorage.Has(divisor.String()) {
			allErrs = append(allErrs, field.Invalid(fldPath.Child("divisor"), rName, "only divisor's values 1, 1k, 1M, 1G, 1T, 1P, 1E, 1Ki, 1Mi, 1Gi, 1Ti, 1Pi, 1Ei are supported with the local ephemeral storage resource"))
		}
	}
	if strings.HasPrefix(rName, hugepagesRequestsPrefixDownwardAPI) || strings.HasPrefix(rName, hugepagesLimitsPrefixDownwardAPI) {
		if !validContainerResourceDivisorForHugePages.Has(divisor.String()) {
			allErrs = append(allErrs, field.Invalid(fldPath.Child("divisor"), rName, "only divisor's values 1, 1k, 1M, 1G, 1T, 1P, 1E, 1Ki, 1Mi, 1Gi, 1Ti, 1Pi, 1Ei are supported with the hugepages resource"))
		}
	}
	return allErrs
}

// validateConfigMapKeySelector 校验 ConfigMap 键选择器：name 与 key 必填且 optional 使用正确。
func validateConfigMapKeySelector(s *core.ConfigMapKeySelector, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}

	nameFn := ValidateNameFunc(ValidateSecretName)
	for _, msg := range nameFn(s.Name, false) {
		allErrs = append(allErrs, field.Invalid(fldPath.Child("name"), s.Name, msg))
	}
	if len(s.Key) == 0 {
		allErrs = append(allErrs, field.Required(fldPath.Child("key"), ""))
	} else {
		for _, msg := range validation.IsConfigMapKey(s.Key) {
			allErrs = append(allErrs, field.Invalid(fldPath.Child("key"), s.Key, msg))
		}
	}

	return allErrs
}

// validateSecretKeySelector 校验 Secret 键选择器：name 与 key 必填且 optional 使用正确。
func validateSecretKeySelector(s *core.SecretKeySelector, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}

	nameFn := ValidateNameFunc(ValidateSecretName)
	for _, msg := range nameFn(s.Name, false) {
		allErrs = append(allErrs, field.Invalid(fldPath.Child("name"), s.Name, msg))
	}
	if len(s.Key) == 0 {
		allErrs = append(allErrs, field.Required(fldPath.Child("key"), ""))
	} else {
		for _, msg := range validation.IsConfigMapKey(s.Key) {
			allErrs = append(allErrs, field.Invalid(fldPath.Child("key"), s.Key, msg))
		}
	}

	return allErrs
}

// validateFileKeySelector 校验 FileKey 选择器：路径与 key 必填，且引用合法。
func validateFileKeySelector(s *core.FileKeySelector, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}

	// Key 字段必须非空，且必须是合法的环境变量名。
	if len(s.Key) == 0 {
		allErrs = append(allErrs, field.Required(fldPath.Child("key"), ""))
	} else {
		for _, msg := range validation.IsRelaxedEnvVarName(s.Key) {
			allErrs = append(allErrs, field.Invalid(fldPath.Child("key"), s.Key, msg))
		}
	}

	// VolumeName 字段必须非空，且必须是合法的 DNS1123 标签。
	if len(s.VolumeName) == 0 {
		allErrs = append(allErrs, field.Required(fldPath.Child("volumeName"), ""))
	} else {
		allErrs = append(allErrs, ValidateDNS1123Label(s.VolumeName, fldPath.Child("volumeName"))...)
	}

	// Path 字段必须非空，且不得包含回退段（".."）。
	if len(s.Path) == 0 {
		allErrs = append(allErrs, field.Required(fldPath.Child("path"), ""))
	} else {
		allErrs = append(allErrs, validatePathNoBacksteps(s.Path, fldPath.Child("path"))...)
	}

	return allErrs
}

// GetVolumeMountMap 将容器卷挂载列表转换为以挂载名称为键的映射，便于冲突检测。
func GetVolumeMountMap(mounts []core.VolumeMount) map[string]string {
	volmounts := make(map[string]string)

	for _, mnt := range mounts {
		volmounts[mnt.Name] = mnt.MountPath
	}

	return volmounts
}

// GetVolumeDeviceMap 将容器卷设备列表转换为以设备名称为键的映射，便于冲突检测。
func GetVolumeDeviceMap(devices []core.VolumeDevice) map[string]string {
	volDevices := make(map[string]string)

	for _, dev := range devices {
		volDevices[dev.Name] = dev.DevicePath
	}

	return volDevices
}

// ValidateVolumeMounts 校验容器卷挂载：名称存在性、mountPath 冲突、子路径与挂载传播等。
func ValidateVolumeMounts(mounts []core.VolumeMount, voldevices map[string]string, volumes map[string]core.VolumeSource, container *core.Container, fldPath *field.Path, opts PodValidationOptions) field.ErrorList {
	allErrs := field.ErrorList{}
	mountpoints := sets.New[string]()

	for i, mnt := range mounts {
		idxPath := fldPath.Index(i)
		if len(mnt.Name) == 0 {
			allErrs = append(allErrs, field.Required(idxPath.Child("name"), ""))
		}
		if !IsMatchedVolume(mnt.Name, volumes) {
			allErrs = append(allErrs, field.NotFound(idxPath.Child("name"), mnt.Name))
		}
		if len(mnt.MountPath) == 0 {
			allErrs = append(allErrs, field.Required(idxPath.Child("mountPath"), ""))
		}
		if mountpoints.Has(mnt.MountPath) {
			allErrs = append(allErrs, field.Invalid(idxPath.Child("mountPath"), mnt.MountPath, "must be unique"))
		}
		mountpoints.Insert(mnt.MountPath)

		// 检查与 VolumeDevice 是否重叠
		if mountNameAlreadyExists(mnt.Name, voldevices) {
			allErrs = append(allErrs, field.Invalid(idxPath.Child("name"), mnt.Name, "must not already exist in volumeDevices"))
		}
		if mountPathAlreadyExists(mnt.MountPath, voldevices) {
			allErrs = append(allErrs, field.Invalid(idxPath.Child("mountPath"), mnt.MountPath, "must not already exist as a path in volumeDevices"))
		}

		if len(mnt.SubPath) > 0 {
			allErrs = append(allErrs, validateLocalDescendingPath(mnt.SubPath, fldPath.Child("subPath"))...)
		}

		if len(mnt.SubPathExpr) > 0 {
			if len(mnt.SubPath) > 0 {
				allErrs = append(allErrs, field.Invalid(idxPath.Child("subPathExpr"), mnt.SubPathExpr, "subPathExpr and subPath are mutually exclusive"))
			}

			allErrs = append(allErrs, validateLocalDescendingPath(mnt.SubPathExpr, fldPath.Child("subPathExpr"))...)
		}

		if mnt.MountPropagation != nil {
			allErrs = append(allErrs, validateMountPropagation(mnt.MountPropagation, container, fldPath.Child("mountPropagation"))...)
		}
		allErrs = append(allErrs, validateMountRecursiveReadOnly(mnt, fldPath.Child("recursiveReadOnly"))...)
		allErrs = append(allErrs, validateBindMountOptions(mnt.BindMountOptions, idxPath.Child("bindMountOptions"))...)
		if len(mnt.BindMountOptions) > 0 {
			if vs, ok := volumes[mnt.Name]; ok && vs.Image != nil {
				allErrs = append(allErrs, field.Invalid(idxPath.Child("bindMountOptions"), mnt.BindMountOptions, "bindMountOptions may not be used with image volumes"))
			}
		}
	}
	return allErrs
}

var supportedBindMountOptions = sets.New(
	string(core.BindMountOptionNoExec),
	string(core.BindMountOptionNoDev),
	string(core.BindMountOptionNoSUID),
)

// validateBindMountOptions 校验 bind 挂载选项：仅在受支持的挂载类型下允许，且格式合法。
func validateBindMountOptions(bindMountOptions []string, fldPath *field.Path) field.ErrorList {
	if len(bindMountOptions) == 0 {
		return nil
	}
	allErrs := field.ErrorList{}
	seen := sets.New[string]()
	for i, opt := range bindMountOptions {
		if !supportedBindMountOptions.Has(opt) {
			allErrs = append(allErrs, field.NotSupported(fldPath.Index(i), opt, sets.List(supportedBindMountOptions)))
		}
		if seen.Has(opt) {
			allErrs = append(allErrs, field.Duplicate(fldPath.Index(i), opt))
		}
		seen.Insert(opt)
	}
	return allErrs
}

// ValidateVolumeDevices 校验容器卷设备：名称存在性、devicePath 冲突与设备类型限制。
func ValidateVolumeDevices(devices []core.VolumeDevice, volmounts map[string]string, volumes map[string]core.VolumeSource, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	devicepath := sets.New[string]()
	devicename := sets.New[string]()

	for i, dev := range devices {
		idxPath := fldPath.Index(i)
		devName := dev.Name
		devPath := dev.DevicePath
		didMatch, isPVC := isMatchedDevice(devName, volumes)
		if len(devName) == 0 {
			allErrs = append(allErrs, field.Required(idxPath.Child("name"), ""))
		}
		if devicename.Has(devName) {
			allErrs = append(allErrs, field.Invalid(idxPath.Child("name"), devName, "must be unique"))
		}
		// 块模式必须基于 PersistentVolumeClaim（PVC 引用或通用临时内联卷）
		if didMatch && !isPVC {
			allErrs = append(allErrs, field.Invalid(idxPath.Child("name"), devName, "can only use volume source type of PersistentVolumeClaim or Ephemeral for block mode"))
		}
		if !didMatch {
			allErrs = append(allErrs, field.NotFound(idxPath.Child("name"), devName))
		}
		if len(devPath) == 0 {
			allErrs = append(allErrs, field.Required(idxPath.Child("devicePath"), ""))
		}
		if devicepath.Has(devPath) {
			allErrs = append(allErrs, field.Invalid(idxPath.Child("devicePath"), devPath, "must be unique"))
		}
		if len(devPath) > 0 && len(validatePathNoBacksteps(devPath, fldPath.Child("devicePath"))) > 0 {
			allErrs = append(allErrs, field.Invalid(idxPath.Child("devicePath"), devPath, "can not contain backsteps ('..')"))
		} else {
			devicepath.Insert(devPath)
		}
		// 检查与 VolumeMount 是否重叠
		if deviceNameAlreadyExists(devName, volmounts) {
			allErrs = append(allErrs, field.Invalid(idxPath.Child("name"), devName, "must not already exist in volumeMounts"))
		}
		if devicePathAlreadyExists(devPath, volmounts) {
			allErrs = append(allErrs, field.Invalid(idxPath.Child("devicePath"), devPath, "must not already exist as a path in volumeMounts"))
		}
		if len(devName) > 0 {
			devicename.Insert(devName)
		}
	}
	return allErrs
}

// validatePodResourceClaims 校验 Pod 级别的 ResourceClaims 名称是否合法且无重复。
func validatePodResourceClaims(podMeta *metav1.ObjectMeta, claims []core.PodResourceClaim, fldPath *field.Path) field.ErrorList {
	var allErrs field.ErrorList
	podClaimNames := sets.New[string]()
	for i, claim := range claims {
		allErrs = append(allErrs, validatePodResourceClaim(podMeta, claim, &podClaimNames, fldPath.Index(i))...)
	}
	return allErrs
}

// gatherPodResourceClaimNames 收集 PodSpec.ResourceClaims 中所有非空名称的集合；名称本身的合法性由 validatePodResourceClaims 负责。
func gatherPodResourceClaimNames(claims []core.PodResourceClaim) sets.Set[string] {
	podClaimNames := sets.Set[string]{}
	for _, claim := range claims {
		if claim.Name != "" {
			podClaimNames.Insert(claim.Name)
		}
	}
	return podClaimNames
}

// validatePodResourceClaim 校验单个 Pod ResourceClaim：名称与资源声明模板/引用的合法性。
func validatePodResourceClaim(podMeta *metav1.ObjectMeta, claim core.PodResourceClaim, podClaimNames *sets.Set[string], fldPath *field.Path) field.ErrorList {
	// 静态 Pod 不支持 resource claims
	if podMeta != nil {
		if _, ok := podMeta.Annotations[core.MirrorPodAnnotationKey]; ok {
			return field.ErrorList{field.Forbidden(field.NewPath(""), "static pods do not support resource claims")}
		}
	}

	var allErrs field.ErrorList
	if claim.Name == "" {
		allErrs = append(allErrs, field.Required(fldPath.Child("name"), ""))
	} else if podClaimNames.Has(claim.Name) {
		allErrs = append(allErrs, field.Duplicate(fldPath.Child("name"), claim.Name))
	} else {
		allErrs = append(allErrs, ValidateDNS1123Label(claim.Name, fldPath.Child("name"))...)
		podClaimNames.Insert(claim.Name)
	}
	if claim.ResourceClaimName != nil && claim.ResourceClaimTemplateName != nil {
		allErrs = append(allErrs, field.Invalid(fldPath, claim, "at most one of `resourceClaimName` or `resourceClaimTemplateName` may be specified"))
	}
	if claim.ResourceClaimName == nil && claim.ResourceClaimTemplateName == nil {
		allErrs = append(allErrs, field.Invalid(fldPath, claim, "must specify one of: `resourceClaimName`, `resourceClaimTemplateName`"))
	}
	if claim.ResourceClaimName != nil {
		for _, detail := range ValidateResourceClaimName(*claim.ResourceClaimName, false) {
			allErrs = append(allErrs, field.Invalid(fldPath.Child("resourceClaimName"), *claim.ResourceClaimName, detail))
		}
	}
	if claim.ResourceClaimTemplateName != nil {
		for _, detail := range ValidateResourceClaimTemplateName(*claim.ResourceClaimTemplateName, false) {
			allErrs = append(allErrs, field.Invalid(fldPath.Child("resourceClaimTemplateName"), *claim.ResourceClaimTemplateName, detail))
		}
	}
	return allErrs
}

// validateLivenessProbe 校验 livenessProbe 配置是否合法。
func validateLivenessProbe(probe *core.Probe, gracePeriod *int64, fldPath *field.Path, opts PodValidationOptions) field.ErrorList {
	allErrs := field.ErrorList{}

	if probe == nil {
		return allErrs
	}
	allErrs = append(allErrs, validateProbe(probe, gracePeriod, fldPath, opts)...)
	if probe.SuccessThreshold != 1 {
		allErrs = append(allErrs, field.Invalid(fldPath.Child("successThreshold"), probe.SuccessThreshold, "must be 1"))
	}
	return allErrs
}

// validateReadinessProbe 校验 readinessProbe 配置是否合法。
func validateReadinessProbe(probe *core.Probe, gracePeriod *int64, fldPath *field.Path, opts PodValidationOptions) field.ErrorList {
	allErrs := field.ErrorList{}

	if probe == nil {
		return allErrs
	}
	allErrs = append(allErrs, validateProbe(probe, gracePeriod, fldPath, opts)...)
	if probe.TerminationGracePeriodSeconds != nil {
		allErrs = append(allErrs, field.Invalid(fldPath.Child("terminationGracePeriodSeconds"), probe.TerminationGracePeriodSeconds, "must not be set for readinessProbes"))
	}
	return allErrs
}

// validateStartupProbe 校验 startupProbe 配置是否合法。
func validateStartupProbe(probe *core.Probe, gracePeriod *int64, fldPath *field.Path, opts PodValidationOptions) field.ErrorList {
	allErrs := field.ErrorList{}

	if probe == nil {
		return allErrs
	}
	allErrs = append(allErrs, validateProbe(probe, gracePeriod, fldPath, opts)...)
	if probe.SuccessThreshold != 1 {
		allErrs = append(allErrs, field.Invalid(fldPath.Child("successThreshold"), probe.SuccessThreshold, "must be 1"))
	}
	return allErrs
}

// validateProbe 校验探针通用字段：探测动作、延迟、超时、周期与成功/失败阈值。
func validateProbe(probe *core.Probe, gracePeriod *int64, fldPath *field.Path, opts PodValidationOptions) field.ErrorList {
	allErrs := field.ErrorList{}

	if probe == nil {
		return allErrs
	}
	allErrs = append(allErrs, validateHandler(handlerFromProbe(&probe.ProbeHandler), gracePeriod, fldPath)...)

	allErrs = append(allErrs, ValidateNonnegativeField(int64(probe.InitialDelaySeconds), fldPath.Child("initialDelaySeconds"))...)
	allErrs = append(allErrs, ValidateNonnegativeField(int64(probe.TimeoutSeconds), fldPath.Child("timeoutSeconds"))...)
	allErrs = append(allErrs, ValidateNonnegativeField(int64(probe.PeriodSeconds), fldPath.Child("periodSeconds"))...)
	allErrs = append(allErrs, ValidateNonnegativeField(int64(probe.SuccessThreshold), fldPath.Child("successThreshold"))...)
	allErrs = append(allErrs, ValidateNonnegativeField(int64(probe.FailureThreshold), fldPath.Child("failureThreshold"))...)
	if probe.TerminationGracePeriodSeconds != nil && *probe.TerminationGracePeriodSeconds <= 0 {
		allErrs = append(allErrs, field.Invalid(fldPath.Child("terminationGracePeriodSeconds"), *probe.TerminationGracePeriodSeconds, "must be greater than 0"))
	}
	return allErrs
}

// validateInitContainerRestartPolicy 校验 init 容器（含 sidecar）的 restartPolicy 及相关规则是否合法。
func validateInitContainerRestartPolicy(restartPolicy *core.ContainerRestartPolicy, restartRules []core.ContainerRestartRule, fldPath *field.Path, opts PodValidationOptions) field.ErrorList {
	var allErrors field.ErrorList

	if restartPolicy == nil {
		if len(restartRules) > 0 {
			allErrors = append(allErrors, field.Required(fldPath.Child("restartPolicy"), "must specify restartPolicy when restart rules are used"))
		}
		return allErrors
	}
	if opts.AllowContainerRestartPolicyRules {
		switch *restartPolicy {
		case core.ContainerRestartPolicyAlways:
			if opts.AllowRestartAllContainers {
				allErrors = append(allErrors, validateContainerRestartPolicy(restartPolicy, restartRules, fldPath, opts)...)
			} else if len(restartRules) > 0 {
				allErrors = append(allErrors, field.Forbidden(fldPath.Child("restartPolicyRules"), "restartPolicyRules are not allowed for init containers with restart policy Always"))
			}
		default:
			allErrors = append(allErrors, validateContainerRestartPolicy(restartPolicy, restartRules, fldPath, opts)...)
		}
	} else {
		switch *restartPolicy {
		case core.ContainerRestartPolicyAlways:
			break
		default:
			validValues := []core.ContainerRestartPolicy{core.ContainerRestartPolicyAlways}
			allErrors = append(allErrors, field.NotSupported(fldPath.Child("restartPolicy"), *restartPolicy, validValues))
		}
	}

	return allErrors
}

type commonHandler struct {
	Exec      *core.ExecAction
	HTTPGet   *core.HTTPGetAction
	TCPSocket *core.TCPSocketAction
	GRPC      *core.GRPCAction
	Sleep     *core.SleepAction
}

// handlerFromProbe 从探针中提取其探测动作处理器，供统一校验使用。
func handlerFromProbe(ph *core.ProbeHandler) commonHandler {
	return commonHandler{
		Exec:      ph.Exec,
		HTTPGet:   ph.HTTPGet,
		TCPSocket: ph.TCPSocket,
		GRPC:      ph.GRPC,
	}
}

// handlerFromLifecycle 从生命周期钩子中提取动作处理器，供统一校验使用。
func handlerFromLifecycle(lh *core.LifecycleHandler) commonHandler {
	return commonHandler{
		Exec:      lh.Exec,
		HTTPGet:   lh.HTTPGet,
		TCPSocket: lh.TCPSocket,
		Sleep:     lh.Sleep,
	}
}

// validateSleepAction 校验生命周期 sleep 动作：sleep 字段必须设置。
func validateSleepAction(sleep *core.SleepAction, gracePeriod *int64, fldPath *field.Path) field.ErrorList {
	allErrors := field.ErrorList{}
	// 这里允许 gracePeriod 为 nil：定义该 SleepAction 的 Pod 本身
	// 可能设置了非法的 grace period，真正的问题已在那处报错，
	// 不应在这里再重复报错。
	if gracePeriod != nil && (sleep.Seconds < 0 || sleep.Seconds > *gracePeriod) {
		invalidStr := fmt.Sprintf("must be non-negative and less than terminationGracePeriodSeconds (%d)", *gracePeriod)
		allErrors = append(allErrors, field.Invalid(fldPath, sleep.Seconds, invalidStr))
	}
	return allErrors
}

// validateClientIPAffinityConfig 校验 Service 的 clientIP affinity 配置：超时时间须在允许范围内。
func validateClientIPAffinityConfig(config *core.SessionAffinityConfig, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	if config == nil {
		allErrs = append(allErrs, field.Required(fldPath, fmt.Sprintf("when session affinity type is %s", core.ServiceAffinityClientIP)))
		return allErrs
	}
	if config.ClientIP == nil {
		allErrs = append(allErrs, field.Required(fldPath.Child("clientIP"), fmt.Sprintf("when session affinity type is %s", core.ServiceAffinityClientIP)))
		return allErrs
	}
	if config.ClientIP.TimeoutSeconds == nil {
		allErrs = append(allErrs, field.Required(fldPath.Child("clientIP").Child("timeoutSeconds"), fmt.Sprintf("when session affinity type is %s", core.ServiceAffinityClientIP)))
		return allErrs
	}
	allErrs = append(allErrs, validateAffinityTimeout(config.ClientIP.TimeoutSeconds, fldPath.Child("clientIP").Child("timeoutSeconds"))...)

	return allErrs
}

// validateAffinityTimeout 校验会话亲和超时时间的合法性。
func validateAffinityTimeout(timeout *int32, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	if *timeout <= 0 || *timeout > core.MaxClientIPServiceAffinitySeconds {
		allErrs = append(allErrs, field.Invalid(fldPath, timeout, fmt.Sprintf("must be greater than 0 and less than %d", core.MaxClientIPServiceAffinitySeconds)))
	}
	return allErrs
}

// AccumulateUniqueHostPorts 提取每个容器的 hostPort 并汇总；若端口冲突则返回错误。
func AccumulateUniqueHostPorts(containers []core.Container, accumulator *sets.Set[string], fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}

	for ci, ctr := range containers {
		idxPath := fldPath.Index(ci)
		portsPath := idxPath.Child("ports")
		for pi := range ctr.Ports {
			idxPath := portsPath.Index(pi)
			port := ctr.Ports[pi].HostPort
			if port == 0 {
				continue
			}
			str := fmt.Sprintf("%s/%s/%d", ctr.Ports[pi].Protocol, ctr.Ports[pi].HostIP, port)
			if accumulator.Has(str) {
				allErrs = append(allErrs, field.Duplicate(idxPath.Child("hostPort"), str))
			} else {
				accumulator.Insert(str)
			}
		}
	}
	return allErrs
}

// checkHostPortConflicts 检查一组容器之间是否存在冲突的 Port.HostPort 值。
func checkHostPortConflicts(containers []core.Container, fldPath *field.Path) field.ErrorList {
	allPorts := sets.Set[string]{}
	return AccumulateUniqueHostPorts(containers, &allPorts, fldPath)
}

// validateExecAction 校验 exec 探测动作：命令不能为空。
func validateExecAction(exec *core.ExecAction, fldPath *field.Path) field.ErrorList {
	allErrors := field.ErrorList{}
	if len(exec.Command) == 0 {
		allErrors = append(allErrors, field.Required(fldPath.Child("command"), ""))
	}
	return allErrors
}

var supportedHTTPSchemes = sets.New(core.URISchemeHTTP, core.URISchemeHTTPS)
var supportedHTTPProtocols = sets.New(core.HTTPProtocolHTTP1, core.HTTPProtocolHTTP2)

var supportedGRPCProbeModes = sets.New(core.GRPCProbeModePlaintext, core.GRPCProbeModeTLS)

// validateHTTPGetAction 校验 HTTPGet 探测动作：主机、路径、端口、HTTP 头与 scheme 的合法性。
func validateHTTPGetAction(http *core.HTTPGetAction, fldPath *field.Path) field.ErrorList {
	allErrors := field.ErrorList{}
	if len(http.Path) == 0 {
		allErrors = append(allErrors, field.Required(fldPath.Child("path"), ""))
	}
	allErrors = append(allErrors, ValidatePortNumOrName(http.Port, fldPath.Child("port"))...)
	if !supportedHTTPSchemes.Has(http.Scheme) {
		allErrors = append(allErrors, field.NotSupported(fldPath.Child("scheme"), http.Scheme, sets.List(supportedHTTPSchemes)))
	}
	for _, header := range http.HTTPHeaders {
		for _, msg := range validation.IsHTTPHeaderName(header.Name) {
			allErrors = append(allErrors, field.Invalid(fldPath.Child("httpHeaders"), header.Name, msg))
		}
	}
	if http.Protocol != nil {
		if !supportedHTTPProtocols.Has(*http.Protocol) {
			allErrors = append(allErrors, field.NotSupported(fldPath.Child("protocol"), *http.Protocol, sets.List(supportedHTTPProtocols)))
		} else if *http.Protocol == core.HTTPProtocolHTTP2 && http.Scheme != core.URISchemeHTTP {
			allErrors = append(allErrors, field.Invalid(fldPath.Child("protocol"), *http.Protocol, "is only supported with HTTP (H2C)"))
		} else if *http.Protocol == core.HTTPProtocolHTTP2 && len(http.Host) > 0 {
			allErrors = append(allErrors, field.Invalid(fldPath.Child("host"), http.Host, "must be empty when `protocol` is \"HTTP2\""))
		}
	}
	return allErrors
}

// ValidatePortNumOrName 校验端口字段：必须是合法的数字或 IANA 服务名称。
func ValidatePortNumOrName(port intstr.IntOrString, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	if port.Type == intstr.Int {
		for _, msg := range validation.IsValidPortNum(port.IntValue()) {
			allErrs = append(allErrs, field.Invalid(fldPath, port.IntValue(), msg))
		}
	} else if port.Type == intstr.String {
		for _, msg := range validation.IsValidPortName(port.StrVal) {
			allErrs = append(allErrs, field.Invalid(fldPath, port.StrVal, msg))
		}
	} else {
		allErrs = append(allErrs, field.InternalError(fldPath, fmt.Errorf("unknown type: %v", port.Type)))
	}
	return allErrs
}

// validateTCPSocketAction 校验 TCPSocket 探测动作中的 host 与 port。
func validateTCPSocketAction(tcp *core.TCPSocketAction, fldPath *field.Path) field.ErrorList {
	return ValidatePortNumOrName(tcp.Port, fldPath.Child("port"))
}

// validateGRPCAction 校验 gRPC 探测动作：端口必填且 service 字段格式合法。
func validateGRPCAction(grpc *core.GRPCAction, fldPath *field.Path) field.ErrorList {
	allErrors := ValidatePortNumOrName(intstr.FromInt32(grpc.Port), fldPath.Child("port"))
	if grpc.Mode != nil && !supportedGRPCProbeModes.Has(*grpc.Mode) {
		allErrors = append(allErrors, field.NotSupported(fldPath.Child("mode"), *grpc.Mode, sets.List(supportedGRPCProbeModes)))
	}
	return allErrors
}

// validateHandler 校验统一探测/生命周期动作处理器：exec、httpGet、tcpSocket、grpc、sleep 必须且只能设置一种。
func validateHandler(handler commonHandler, gracePeriod *int64, fldPath *field.Path) field.ErrorList {
	numHandlers := 0
	allErrors := field.ErrorList{}
	if handler.Exec != nil {
		if numHandlers > 0 {
			allErrors = append(allErrors, field.Forbidden(fldPath.Child("exec"), "may not specify more than 1 handler type"))
		} else {
			numHandlers++
			allErrors = append(allErrors, validateExecAction(handler.Exec, fldPath.Child("exec"))...)
		}
	}
	if handler.HTTPGet != nil {
		if numHandlers > 0 {
			allErrors = append(allErrors, field.Forbidden(fldPath.Child("httpGet"), "may not specify more than 1 handler type"))
		} else {
			numHandlers++
			allErrors = append(allErrors, validateHTTPGetAction(handler.HTTPGet, fldPath.Child("httpGet"))...)
		}
	}
	if handler.TCPSocket != nil {
		if numHandlers > 0 {
			allErrors = append(allErrors, field.Forbidden(fldPath.Child("tcpSocket"), "may not specify more than 1 handler type"))
		} else {
			numHandlers++
			allErrors = append(allErrors, validateTCPSocketAction(handler.TCPSocket, fldPath.Child("tcpSocket"))...)
		}
	}
	if handler.GRPC != nil {
		if numHandlers > 0 {
			allErrors = append(allErrors, field.Forbidden(fldPath.Child("grpc"), "may not specify more than 1 handler type"))
		} else {
			numHandlers++
			allErrors = append(allErrors, validateGRPCAction(handler.GRPC, fldPath.Child("grpc"))...)
		}
	}
	if handler.Sleep != nil {
		if numHandlers > 0 {
			allErrors = append(allErrors, field.Forbidden(fldPath.Child("sleep"), "may not specify more than 1 handler type"))
		} else {
			numHandlers++
			allErrors = append(allErrors, validateSleepAction(handler.Sleep, gracePeriod, fldPath.Child("sleep"))...)
		}
	}
	if numHandlers == 0 {
		allErrors = append(allErrors, field.Required(fldPath, "must specify a handler type"))
	}
	return allErrors
}

var supportedStopSignalsLinux = sets.New(
	core.SIGABRT, core.SIGALRM, core.SIGBUS, core.SIGCHLD,
	core.SIGCLD, core.SIGCONT, core.SIGFPE, core.SIGHUP,
	core.SIGILL, core.SIGINT, core.SIGIO, core.SIGIOT,
	core.SIGKILL, core.SIGPIPE, core.SIGPOLL, core.SIGPROF,
	core.SIGPWR, core.SIGQUIT, core.SIGSEGV, core.SIGSTKFLT,
	core.SIGSTOP, core.SIGSYS, core.SIGTERM, core.SIGTRAP,
	core.SIGTSTP, core.SIGTTIN, core.SIGTTOU, core.SIGURG,
	core.SIGUSR1, core.SIGUSR2, core.SIGVTALRM, core.SIGWINCH,
	core.SIGXCPU, core.SIGXFSZ, core.SIGRTMIN, core.SIGRTMINPLUS1,
	core.SIGRTMINPLUS2, core.SIGRTMINPLUS3, core.SIGRTMINPLUS4,
	core.SIGRTMINPLUS5, core.SIGRTMINPLUS6, core.SIGRTMINPLUS7,
	core.SIGRTMINPLUS8, core.SIGRTMINPLUS9, core.SIGRTMINPLUS10,
	core.SIGRTMINPLUS11, core.SIGRTMINPLUS12, core.SIGRTMINPLUS13,
	core.SIGRTMINPLUS14, core.SIGRTMINPLUS15, core.SIGRTMAXMINUS14,
	core.SIGRTMAXMINUS13, core.SIGRTMAXMINUS12, core.SIGRTMAXMINUS11,
	core.SIGRTMAXMINUS10, core.SIGRTMAXMINUS9, core.SIGRTMAXMINUS8,
	core.SIGRTMAXMINUS7, core.SIGRTMAXMINUS6, core.SIGRTMAXMINUS5,
	core.SIGRTMAXMINUS4, core.SIGRTMAXMINUS3, core.SIGRTMAXMINUS2,
	core.SIGRTMAXMINUS1, core.SIGRTMAX)

var supportedStopSignalsWindows = sets.New(core.SIGKILL, core.SIGTERM)

// validateStopSignal 校验容器 stopSignal 是否为受支持的信号名称。
func validateStopSignal(stopSignal *core.Signal, fldPath *field.Path, os *core.PodOS) field.ErrorList {
	allErrors := field.ErrorList{}

	if os == nil {
		allErrors = append(allErrors, field.Forbidden(fldPath, "may not be set for containers with empty `spec.os.name`"))
	} else if os.Name == core.Windows {
		if !supportedStopSignalsWindows.Has(*stopSignal) {
			allErrors = append(allErrors, field.NotSupported(fldPath, stopSignal, sets.List(supportedStopSignalsWindows)))
		}
	} else if os.Name == core.Linux {
		if !supportedStopSignalsLinux.Has(*stopSignal) {
			allErrors = append(allErrors, field.NotSupported(fldPath, stopSignal, sets.List(supportedStopSignalsLinux)))
		}
	}

	return allErrors
}

// validateLifecycle 校验容器生命周期钩子（postStart、preStop、stop）的动作处理器。
func validateLifecycle(lifecycle *core.Lifecycle, gracePeriod *int64, fldPath *field.Path, os *core.PodOS) field.ErrorList {
	allErrs := field.ErrorList{}
	if lifecycle.PostStart != nil {
		allErrs = append(allErrs, validateHandler(handlerFromLifecycle(lifecycle.PostStart), gracePeriod, fldPath.Child("postStart"))...)
	}
	if lifecycle.PreStop != nil {
		allErrs = append(allErrs, validateHandler(handlerFromLifecycle(lifecycle.PreStop), gracePeriod, fldPath.Child("preStop"))...)
	}
	if lifecycle.StopSignal != nil {
		allErrs = append(allErrs, validateStopSignal(lifecycle.StopSignal, fldPath.Child("stopSignal"), os)...)
	}
	return allErrs
}

var supportedPullPolicies = sets.New(
	core.PullAlways,
	core.PullIfNotPresent,
	core.PullNever)

// validatePullPolicy 校验容器镜像拉取策略是否为受支持的值。
func validatePullPolicy(policy core.PullPolicy, fldPath *field.Path) field.ErrorList {
	allErrors := field.ErrorList{}

	switch policy {
	case core.PullAlways, core.PullIfNotPresent, core.PullNever:
		break
	case "":
		allErrors = append(allErrors, field.Required(fldPath, ""))
	default:
		allErrors = append(allErrors, field.NotSupported(fldPath, policy, sets.List(supportedPullPolicies)))
	}

	return allErrors
}

var supportedResizeResources = sets.New(core.ResourceCPU, core.ResourceMemory)
var supportedResizePolicies = sets.New(core.NotRequired, core.RestartContainer)

// validateResizePolicy 校验容器资源调整策略：资源名与重启策略是否合法。
func validateResizePolicy(policyList []core.ContainerResizePolicy, fldPath *field.Path, podRestartPolicy *core.RestartPolicy) field.ErrorList {
	allErrors := field.ErrorList{}

	// 校验资源名不重复，且资源名与策略取值均为支持的指定值
	resources := make(map[core.ResourceName]bool)
	for i, p := range policyList {
		if _, found := resources[p.ResourceName]; found {
			allErrors = append(allErrors, field.Duplicate(fldPath.Index(i), p.ResourceName))
		}
		resources[p.ResourceName] = true
		switch p.ResourceName {
		case core.ResourceCPU, core.ResourceMemory:
		case "":
			allErrors = append(allErrors, field.Required(fldPath, ""))
		default:
			allErrors = append(allErrors, field.NotSupported(fldPath, p.ResourceName, sets.List(supportedResizeResources)))
		}
		switch p.RestartPolicy {
		case core.NotRequired, core.RestartContainer:
		case "":
			allErrors = append(allErrors, field.Required(fldPath, ""))
		default:
			allErrors = append(allErrors, field.NotSupported(fldPath, p.RestartPolicy, sets.List(supportedResizePolicies)))
		}

		if *podRestartPolicy == core.RestartPolicyNever && p.RestartPolicy != core.NotRequired {
			allErrors = append(allErrors, field.Invalid(fldPath, p.RestartPolicy, "must be 'NotRequired' when pod `restartPolicy` is 'Never'"))
		}
	}

	return allErrors
}

var supportedContainerRestartPolicies = sets.New(
	core.ContainerRestartPolicyAlways,
	core.ContainerRestartPolicyNever,
	core.ContainerRestartPolicyOnFailure,
)

var supportedContainerRestartPolicyOperators = sets.New(
	core.ContainerRestartRuleOnExitCodesOpIn,
	core.ContainerRestartRuleOnExitCodesOpNotIn,
)

// 支持的 action 取决于对应特性门控是否启用。
var (
	// 未启用 AllowRestartAllContainers 时
	supportedContainerRestartRuleActions = sets.New(core.ContainerRestartRuleActionRestart)
	// 启用 AllowRestartAllContainers 时
	supportedContainerRestartRuleActionsWithRestartAllContainers = sets.New(core.ContainerRestartRuleActionRestart, core.ContainerRestartRuleActionRestartAllContainers)
)

// validateContainerRestartPolicy 校验容器级别的 restartPolicy 及 restartPolicyRules 对普通容器、init 容器与 sidecar 容器是否合法。
func validateContainerRestartPolicy(policy *core.ContainerRestartPolicy, rules []core.ContainerRestartRule, fldPath *field.Path, opts PodValidationOptions) field.ErrorList {
	var allErrs field.ErrorList
	restartPolicyFld := fldPath.Child("restartPolicy")
	if policy == nil {
		if len(rules) == 0 {
			return allErrs
		} else {
			allErrs = append(allErrs, field.Required(restartPolicyFld, "must specify restartPolicy when restart rules are used"))
		}
	} else if !supportedContainerRestartPolicies.Has(*policy) {
		allErrs = append(allErrs, field.NotSupported(restartPolicyFld, *policy, sets.List(supportedContainerRestartPolicies)))
	}

	if len(rules) > 20 {
		allErrs = append(allErrs, field.TooMany(fldPath.Child("restartPolicyRules"), len(rules), 20))
	}
	for i, rule := range rules {
		policyRulesFld := fldPath.Child("restartPolicyRules").Index(i)
		allowedActions := supportedContainerRestartRuleActions
		if opts.AllowRestartAllContainers {
			allowedActions = supportedContainerRestartRuleActionsWithRestartAllContainers
		}
		if !allowedActions.Has(rule.Action) {
			allErrs = append(allErrs, field.NotSupported(policyRulesFld.Child("action"), rule.Action, sets.List(allowedActions)))
		}

		if rule.ExitCodes != nil {
			exitCodesFld := policyRulesFld.Child("exitCodes")
			if !supportedContainerRestartPolicyOperators.Has(rule.ExitCodes.Operator) {
				allErrs = append(allErrs, field.NotSupported(exitCodesFld.Child("operator"), rule.ExitCodes.Operator, sets.List(supportedContainerRestartPolicyOperators)))
			}

			if len(rule.ExitCodes.Values) > 255 {
				allErrs = append(allErrs, field.TooMany(exitCodesFld.Child("values"), len(rule.ExitCodes.Values), 255))
			}
		} else {
			allErrs = append(allErrs, field.Required(policyRulesFld.Child("exitCodes"), "must be specified"))
		}
	}
	return allErrs
}

// validateEphemeralContainers 由 Pod 规格与模板校验调用，校验 ephemeral containers 列表；模板场景虽不允许使用该字段，仍执行校验。
func validateEphemeralContainers(ephemeralContainers []core.EphemeralContainer, containers, initContainers []core.Container, volumes map[string]core.VolumeSource, podClaimNames sets.Set[string], fldPath *field.Path, opts PodValidationOptions, podRestartPolicy *core.RestartPolicy, hostUsers bool) field.ErrorList {
	var allErrs field.ErrorList

	if len(ephemeralContainers) == 0 {
		return allErrs
	}

	otherNames, allNames := sets.Set[string]{}, sets.Set[string]{}
	for _, c := range containers {
		otherNames.Insert(c.Name)
		allNames.Insert(c.Name)
	}
	for _, c := range initContainers {
		otherNames.Insert(c.Name)
		allNames.Insert(c.Name)
	}

	for i, ec := range ephemeralContainers {
		idxPath := fldPath.Index(i)

		c := (*core.Container)(&ec.EphemeralContainerCommon)
		allErrs = append(allErrs, validateContainerCommon(c, volumes, podClaimNames, idxPath, opts, podRestartPolicy, hostUsers)...)
		// 临时容器（Ephemeral containers）在 pod 模板场景下无需放宽约束，
		// 因此在此处（已把 EphemeralContainerCommon 转为 Container）
		// 同时应用两类校验正合适。
		allErrs = append(allErrs, validateContainerOnlyForPod(c, idxPath)...)

		// 临时容器的名称必须在所有容器类型中唯一。
		if allNames.Has(ec.Name) {
			allErrs = append(allErrs, field.Duplicate(idxPath.Child("name"), ec.Name))
		} else {
			allNames.Insert(ec.Name)
		}

		// 目标容器名必须存在且不能是临时容器。
		if ec.TargetContainerName != "" && !otherNames.Has(ec.TargetContainerName) {
			allErrs = append(allErrs, field.NotFound(idxPath.Child("targetContainerName"), ec.TargetContainerName))
		}

		// 临时容器不应当被用于 Pod 的基础服务，因此 Lifecycle、探针、
		// resources 与端口等字段应当禁止。这里以“允许字段列表”的方式实现，
		// 使新增字段在被纳入临时容器前先经过考量。
		allErrs = append(allErrs, validateFieldAllowList(ec.EphemeralContainerCommon, allowedEphemeralContainerFields, "cannot be set for an Ephemeral Container", idxPath)...)

		// VolumeMount 的 subpath 基于 bind mount 实现，直到 Pod 退出才会清理，
		// 存在资源泄漏的可能；且 subpath 也暗示容器是工作负载的一部分，
		// 因此对临时容器完全禁止。
		for i, vm := range ec.VolumeMounts {
			if vm.SubPath != "" {
				allErrs = append(allErrs, field.Forbidden(idxPath.Child("volumeMounts").Index(i).Child("subPath"), "cannot be set for an Ephemeral Container"))
			}
			if vm.SubPathExpr != "" {
				allErrs = append(allErrs, field.Forbidden(idxPath.Child("volumeMounts").Index(i).Child("subPathExpr"), "cannot be set for an Ephemeral Container"))
			}
		}
	}

	return allErrs
}

// validateFieldAllowList 通过反射检查结构体仅设置了允许列表中的字段，防止新增 ObjectMeta 字段被遗漏校验。
func validateFieldAllowList(value interface{}, allowedFields map[string]bool, errorText string, fldPath *field.Path) field.ErrorList {
	var allErrs field.ErrorList

	reflectType, reflectValue := reflect.TypeOf(value), reflect.ValueOf(value)
	for i := 0; i < reflectType.NumField(); i++ {
		f := reflectType.Field(i)
		if allowedFields[f.Name] {
			continue
		}

		// 将字段取值与零值比较，判断其是否被设置过
		if !reflect.DeepEqual(reflectValue.Field(i).Interface(), reflect.Zero(f.Type).Interface()) {
			r, n := utf8.DecodeRuneInString(f.Name)
			lcName := string(unicode.ToLower(r)) + f.Name[n:]
			allErrs = append(allErrs, field.Forbidden(fldPath.Child(lcName), errorText))
		}
	}

	return allErrs
}

// validateInitContainers 由 Pod 规格与模板校验调用，校验 init containers 列表及其顺序、重启策略。
func validateInitContainers(containers []core.Container, os *core.PodOS, regularContainers []core.Container, volumes map[string]core.VolumeSource, podClaimNames sets.Set[string], gracePeriod *int64, fldPath *field.Path, opts PodValidationOptions, podRestartPolicy *core.RestartPolicy, hostUsers bool) field.ErrorList {
	var allErrs field.ErrorList

	allNames := sets.Set[string]{}
	for _, ctr := range regularContainers {
		allNames.Insert(ctr.Name)
	}
	for i, ctr := range containers {
		idxPath := fldPath.Index(i)

		// 应用所有容器类型共用的校验
		allErrs = append(allErrs, validateContainerCommon(&ctr, volumes, podClaimNames, idxPath, opts, podRestartPolicy, hostUsers)...)

		restartAlways := false
		// 应用 init 容器特有的校验
		allErrs = append(allErrs, validateInitContainerRestartPolicy(ctr.RestartPolicy, ctr.RestartPolicyRules, idxPath, opts)...)
		if ctr.RestartPolicy != nil {
			restartAlways = *ctr.RestartPolicy == core.ContainerRestartPolicyAlways
		}

		// 普通容器与 init 容器的名称必须彼此唯一；与临时容器的冲突
		// 会由 validateEphemeralContainers() 检出。
		if allNames.Has(ctr.Name) {
			allErrs = append(allErrs, field.Duplicate(idxPath.Child("name"), ctr.Name))
		} else if len(ctr.Name) > 0 {
			allNames.Insert(ctr.Name)
		}

		// 对 init 容器逐个检查端口冲突，因为 init 容器是串行运行的。
		allErrs = append(allErrs, checkHostPortConflicts([]core.Container{ctr}, fldPath)...)

		switch {
		case restartAlways:
			if ctr.Lifecycle != nil {
				allErrs = append(allErrs, validateLifecycle(ctr.Lifecycle, gracePeriod, idxPath.Child("lifecycle"), os)...)
			}
			allErrs = append(allErrs, validateLivenessProbe(ctr.LivenessProbe, gracePeriod, idxPath.Child("livenessProbe"), opts)...)
			allErrs = append(allErrs, validateReadinessProbe(ctr.ReadinessProbe, gracePeriod, idxPath.Child("readinessProbe"), opts)...)
			allErrs = append(allErrs, validateStartupProbe(ctr.StartupProbe, gracePeriod, idxPath.Child("startupProbe"), opts)...)

		default:
			// init 容器不允许设置这些字段。
			if ctr.Lifecycle != nil {
				allErrs = append(allErrs, field.Forbidden(idxPath.Child("lifecycle"), "may not be set for init containers without restartPolicy=Always"))
			}
			if ctr.LivenessProbe != nil {
				allErrs = append(allErrs, field.Forbidden(idxPath.Child("livenessProbe"), "may not be set for init containers without restartPolicy=Always"))
			}
			if ctr.ReadinessProbe != nil {
				allErrs = append(allErrs, field.Forbidden(idxPath.Child("readinessProbe"), "may not be set for init containers without restartPolicy=Always"))
			}
			if ctr.StartupProbe != nil {
				allErrs = append(allErrs, field.Forbidden(idxPath.Child("startupProbe"), "may not be set for init containers without restartPolicy=Always"))
			}
		}

		if !opts.AllowSidecarResizePolicy && len(ctr.ResizePolicy) > 0 {
			allErrs = append(allErrs, field.Invalid(idxPath.Child("resizePolicy"), ctr.ResizePolicy, "must not be set for init containers"))
		}

		if !restartAlways && !opts.AllowExistingRestartContainerForNonSidecarInitContainer {
			for j, p := range ctr.ResizePolicy {
				if p.RestartPolicy == core.RestartContainer {
					allErrs = append(allErrs, field.Invalid(fldPath.Index(i).Child("resizePolicy").Index(j).Child("restartPolicy"), p.RestartPolicy, "must not be set to 'RestartContainer' for non-sidecar initContainers"))
				}
			}
		}
	}

	return allErrs
}

// validateContainerCommon 对所有容器类型（普通、init、临时）执行的通用校验：名称、镜像格式等。
func validateContainerCommon(ctr *core.Container, volumes map[string]core.VolumeSource, podClaimNames sets.Set[string], path *field.Path, opts PodValidationOptions, podRestartPolicy *core.RestartPolicy, hostUsers bool) field.ErrorList {
	var allErrs field.ErrorList

	namePath := path.Child("name")
	if len(ctr.Name) == 0 {
		allErrs = append(allErrs, field.Required(namePath, ""))
	} else {
		allErrs = append(allErrs, ValidateDNS1123Label(ctr.Name, namePath)...)
	}

	// TODO: 不校验首尾空白字符，以保持向后兼容。
	// 例如：https://github.com/openshift/origin/issues/14659 中 image = " "
	// 是 pod 模板里的特殊记号；其他项目可能也有类似用法
	if len(ctr.Image) == 0 {
		allErrs = append(allErrs, field.Required(path.Child("image"), ""))
	}

	switch ctr.TerminationMessagePolicy {
	case core.TerminationMessageReadFile, core.TerminationMessageFallbackToLogsOnError:
	case "":
		allErrs = append(allErrs, field.Required(path.Child("terminationMessagePolicy"), ""))
	default:
		supported := []core.TerminationMessagePolicy{
			core.TerminationMessageReadFile,
			core.TerminationMessageFallbackToLogsOnError,
		}
		allErrs = append(allErrs, field.NotSupported(path.Child("terminationMessagePolicy"), ctr.TerminationMessagePolicy, supported))
	}

	volMounts := GetVolumeMountMap(ctr.VolumeMounts)
	volDevices := GetVolumeDeviceMap(ctr.VolumeDevices)
	allErrs = append(allErrs, validateContainerPorts(ctr.Ports, path.Child("ports"))...)
	allErrs = append(allErrs, ValidateEnv(ctr.Env, path.Child("env"), opts)...)
	allErrs = append(allErrs, ValidateEnvFrom(ctr.EnvFrom, path.Child("envFrom"), opts)...)
	allErrs = append(allErrs, ValidateVolumeMounts(ctr.VolumeMounts, volDevices, volumes, ctr, path.Child("volumeMounts"), opts)...)
	allErrs = append(allErrs, ValidateVolumeDevices(ctr.VolumeDevices, volMounts, volumes, path.Child("volumeDevices"))...)
	allErrs = append(allErrs, validatePullPolicy(ctr.ImagePullPolicy, path.Child("imagePullPolicy"))...)
	allErrs = append(allErrs, ValidateContainerResourceRequirements(&ctr.Resources, podClaimNames, path.Child("resources"), opts)...)
	allErrs = append(allErrs, validateResizePolicy(ctr.ResizePolicy, path.Child("resizePolicy"), podRestartPolicy)...)
	allErrs = append(allErrs, ValidateSecurityContext(ctr.SecurityContext, path.Child("securityContext"), hostUsers, opts.AllowSysAdminWhenPrivilegeEscalationFalse)...)
	return allErrs
}

// validateHostUsers 校验 hostUsers 字段与 Pod 其他设置（如 Windows hostProcess）是否兼容。
func validateHostUsers(spec *core.PodSpec, fldPath *field.Path, opts PodValidationOptions) field.ErrorList {
	allErrs := field.ErrorList{}

	// 仅在 hostUsers 为 false 时执行下列检查（否则容器与主机共用
	// userns，无需任何检查）。
	if spec.HostUsers == nil || *spec.HostUsers {
		return allErrs
	}

	// 我们决定限制 userns 与其他 host namespace 同时使用：
	// 	https://github.com/kubernetes/kubernetes/pull/111090#discussion_r935994282
	// 简而言之：这样很容易遇到看似出乎意料的权限问题，目前没有已知
	// 的好用例，且以后随时可以放开。

	if !opts.AllowUserNamespacesHostNetworkSupport {
		if spec.HostNetwork {
			allErrs = append(allErrs, field.Forbidden(fldPath.Child("hostNetwork"), "when `hostUsers` is false"))
		}
	}

	if spec.HostPID {
		allErrs = append(allErrs, field.Forbidden(fldPath.Child("HostPID"), "when `hostUsers` is false"))
	}
	if spec.HostIPC {
		allErrs = append(allErrs, field.Forbidden(fldPath.Child("HostIPC"), "when `hostUsers` is false"))
	}
	if !opts.AllowUserNamespacesWithVolumeDevices {
		// volumeDevices 无法工作：它们既不支持 idmap 挂载，我们也不会
		// 对其 chown。这种情况返回一个明确的错误。
		podshelper.VisitContainersWithPath(spec, fldPath, func(c *core.Container, containerPath *field.Path) bool {
			if len(c.VolumeDevices) > 0 {
				allErrs = append(allErrs, field.Forbidden(containerPath.Child("volumeDevices"), "when `hostUsers` is false"))
			}
			return true // 始终访问所有容器。
		})
	}

	return allErrs
}

// validateFileKeyRefVolumes 校验 FileKeyRef 环境变量引用的卷必须为 emptyDir 类型，以保证文件访问正确。
func validateFileKeyRefVolumes(spec *core.PodSpec, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}

	volumeSources := make(map[string]*core.VolumeSource)
	for i := range spec.Volumes {
		volume := &spec.Volumes[i]
		volumeSources[volume.Name] = &volume.VolumeSource
	}

	podshelper.VisitContainersWithPath(spec, fldPath, func(c *core.Container, cFldPath *field.Path) bool {
		envPath := cFldPath.Child("env")
		for j, env := range c.Env {
			// 只关心使用 FileKeyRef 的环境变量。
			if env.ValueFrom == nil || env.ValueFrom.FileKeyRef == nil {
				continue
			}

			volumeName := env.ValueFrom.FileKeyRef.VolumeName
			fileKeyRefPath := envPath.Index(j).Child("valueFrom").Child("fileKeyRef")
			volumeNamePath := fileKeyRefPath.Child("volumeName")

			source, found := volumeSources[volumeName]
			if !found {
				// 引用的卷在 pod spec 中不存在。
				allErrs = append(allErrs, field.NotFound(volumeNamePath, volumeName))
			} else if source.EmptyDir == nil {
				// 卷存在，但不是所要求的 emptyDir 类型。
				allErrs = append(allErrs, field.Invalid(volumeNamePath, volumeName, "referenced volume must be of type emptyDir"))
			}
		}

		return true
	})

	return allErrs
}

// validatePodHostName 校验 Pod 的 hostname 与 subdomain：格式合法且组合后不超过 DNS 子域名长度限制。
func validatePodHostName(spec *core.PodSpec, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}

	if spec.HostnameOverride == nil {
		return allErrs
	}

	// 若 SetHostnameAsFQDN 为 true，则不得设置 HostnameOverride。
	if spec.SetHostnameAsFQDN != nil && *spec.SetHostnameAsFQDN {
		allErrs = append(allErrs, field.Forbidden(fldPath.Child("hostnameOverride"), "may not be specified when setHostnameAsFQDN is true"))
	}
	// 若 HostNetwork 为 true，则不得设置 HostnameOverride。
	if spec.HostNetwork {
		allErrs = append(allErrs, field.Forbidden(fldPath.Child("hostnameOverride"), "may not be specified when hostNetwork is true"))
	}
	if len(*spec.HostnameOverride) > 64 {
		allErrs = append(allErrs, field.TooLong(fldPath.Child("hostnameOverride"), "" /*unused*/, 64))
	}

	// 不允许把不符合 RFC 1123 DNS 子域名格式的字符串设置为 hostname。
	allErrs = append(allErrs, ValidateDNS1123Subdomain(*spec.HostnameOverride, fldPath.Child("hostnameOverride"))...)

	return allErrs
}

// validateContainers 由 Pod 规格与模板校验调用，校验普通容器列表。
func validateContainers(containers []core.Container, os *core.PodOS, volumes map[string]core.VolumeSource, podClaimNames sets.Set[string], gracePeriod *int64, fldPath *field.Path, opts PodValidationOptions, podRestartPolicy *core.RestartPolicy, hostUsers bool) field.ErrorList {
	allErrs := field.ErrorList{}

	if len(containers) == 0 {
		return append(allErrs, field.Required(fldPath, ""))
	}

	allNames := sets.Set[string]{}
	for i, ctr := range containers {
		path := fldPath.Index(i)

		// 应用所有容器类型共用的校验
		allErrs = append(allErrs, validateContainerCommon(&ctr, volumes, podClaimNames, path, opts, podRestartPolicy, hostUsers)...)

		// 普通容器列表内的容器名称必须唯一。
		// 与 init 或临时容器名称的冲突由 init/临时容器的校验检出，
		// 以避免产生重复的错误信息。
		if allNames.Has(ctr.Name) {
			allErrs = append(allErrs, field.Duplicate(path.Child("name"), ctr.Name))
		} else {
			allNames.Insert(ctr.Name)
		}

		// 这些字段对普通容器和可重启的 init 容器
		// 是允许设置的。
		// 普通 init 容器与临时容器的校验会对
		// 这些路径返回 field.Forbidden()。
		if ctr.Lifecycle != nil {
			allErrs = append(allErrs, validateLifecycle(ctr.Lifecycle, gracePeriod, path.Child("lifecycle"), os)...)
		}
		allErrs = append(allErrs, validateLivenessProbe(ctr.LivenessProbe, gracePeriod, path.Child("livenessProbe"), opts)...)
		allErrs = append(allErrs, validateReadinessProbe(ctr.ReadinessProbe, gracePeriod, path.Child("readinessProbe"), opts)...)
		allErrs = append(allErrs, validateStartupProbe(ctr.StartupProbe, gracePeriod, path.Child("startupProbe"), opts)...)

		if opts.AllowContainerRestartPolicyRules {
			allErrs = append(allErrs, validateContainerRestartPolicy(ctr.RestartPolicy, ctr.RestartPolicyRules, path, opts)...)
		} else if ctr.RestartPolicy != nil {
			allErrs = append(allErrs, field.Forbidden(path.Child("restartPolicy"), "may not be set for non-init containers"))
		}
	}

	// 端口冲突需要在所有容器之间统一检查
	allErrs = append(allErrs, checkHostPortConflicts(containers, fldPath)...)

	return allErrs
}

// validateRestartPolicy 校验 Pod 的 restartPolicy 是否为合法值。
func validateRestartPolicy(restartPolicy *core.RestartPolicy, fldPath *field.Path) field.ErrorList {
	allErrors := field.ErrorList{}
	switch *restartPolicy {
	case core.RestartPolicyAlways, core.RestartPolicyOnFailure, core.RestartPolicyNever:
		break
	case "":
		allErrors = append(allErrors, field.Required(fldPath, ""))
	default:
		validValues := []core.RestartPolicy{core.RestartPolicyAlways, core.RestartPolicyOnFailure, core.RestartPolicyNever}
		allErrors = append(allErrors, field.NotSupported(fldPath, *restartPolicy, validValues))
	}

	return allErrors
}

// ValidatePreemptionPolicy 校验 Pod 的 preemptionPolicy 是否为合法值。
func ValidatePreemptionPolicy(preemptionPolicy *core.PreemptionPolicy, fldPath *field.Path) field.ErrorList {
	allErrors := field.ErrorList{}
	switch *preemptionPolicy {
	case core.PreemptLowerPriority, core.PreemptNever:
	case "":
		allErrors = append(allErrors, field.Required(fldPath, ""))
	default:
		validValues := []core.PreemptionPolicy{core.PreemptLowerPriority, core.PreemptNever}
		allErrors = append(allErrors, field.NotSupported(fldPath, preemptionPolicy, validValues))
	}
	return allErrors
}

// validateDNSPolicy 校验 Pod 的 dnsPolicy 是否为合法值，且与 hostNetwork 等设置兼容。
func validateDNSPolicy(dnsPolicy *core.DNSPolicy, fldPath *field.Path) field.ErrorList {
	allErrors := field.ErrorList{}
	switch *dnsPolicy {
	case core.DNSClusterFirstWithHostNet, core.DNSClusterFirst, core.DNSDefault, core.DNSNone:
	case "":
		allErrors = append(allErrors, field.Required(fldPath, ""))
	default:
		validValues := []core.DNSPolicy{core.DNSClusterFirstWithHostNet, core.DNSClusterFirst, core.DNSDefault, core.DNSNone}
		allErrors = append(allErrors, field.NotSupported(fldPath, dnsPolicy, validValues))
	}
	return allErrors
}

var validFSGroupChangePolicies = sets.New(core.FSGroupChangeOnRootMismatch, core.FSGroupChangeAlways)

// validateFSGroupChangePolicy 校验 fsGroupChangePolicy 是否为合法的策略值。
func validateFSGroupChangePolicy(fsGroupPolicy *core.PodFSGroupChangePolicy, fldPath *field.Path) field.ErrorList {
	allErrors := field.ErrorList{}
	if !validFSGroupChangePolicies.Has(*fsGroupPolicy) {
		allErrors = append(allErrors, field.NotSupported(fldPath, fsGroupPolicy, sets.List(validFSGroupChangePolicies)))
	}
	return allErrors
}

const (
	// 对各项 DNS 参数的限制，源自 Linux libc
	// 名称解析实现中的约束。
	// DNS 名称服务器的最大数量。
	MaxDNSNameservers = 3
	// search 路径列表中域名的最大数量。
	MaxDNSSearchPaths = 32
	// search 路径的最大字符数。
	MaxDNSSearchListChars = 2048
)

// validateReadinessGates 校验 Pod 的 readinessGates：条件类型格式合法且不重复。
func validateReadinessGates(readinessGates []core.PodReadinessGate, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	for i, value := range readinessGates {
		allErrs = append(allErrs, ValidateQualifiedName(string(value.ConditionType), fldPath.Index(i).Child("conditionType"))...)
	}
	return allErrs
}

// validateSchedulingGates 校验 Pod 的 schedulingGates：门控名称格式合法且不重复。
func validateSchedulingGates(schedulingGates []core.PodSchedulingGate, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	// scheduling gates 列表中不应有重复项。
	seen := sets.Set[string]{}
	for i, schedulingGate := range schedulingGates {
		allErrs = append(allErrs, ValidateQualifiedName(schedulingGate.Name, fldPath.Index(i))...)
		if seen.Has(schedulingGate.Name) {
			allErrs = append(allErrs, field.Duplicate(fldPath.Index(i), schedulingGate.Name))
		}
		seen.Insert(schedulingGate.Name)
	}
	return allErrs
}

// validatePodDNSConfig 校验 Pod 的 dnsConfig：nameservers、searches、options 及值格式。
func validatePodDNSConfig(dnsConfig *core.PodDNSConfig, dnsPolicy *core.DNSPolicy, fldPath *field.Path, opts PodValidationOptions) field.ErrorList {
	allErrs := field.ErrorList{}

	// 校验 DNSNone 场景：必须至少提供一个 DNS 名称服务器。
	if dnsPolicy != nil && *dnsPolicy == core.DNSNone {
		if dnsConfig == nil {
			return append(allErrs, field.Required(fldPath, fmt.Sprintf("must provide `dnsConfig` when `dnsPolicy` is %s", core.DNSNone)))
		}
		if len(dnsConfig.Nameservers) == 0 {
			return append(allErrs, field.Required(fldPath.Child("nameservers"), fmt.Sprintf("must provide at least one DNS nameserver when `dnsPolicy` is %s", core.DNSNone)))
		}
	}

	if dnsConfig != nil {
		// 校验 nameservers。
		if len(dnsConfig.Nameservers) > MaxDNSNameservers {
			allErrs = append(allErrs, field.Invalid(fldPath.Child("nameservers"), dnsConfig.Nameservers, fmt.Sprintf("must not have more than %v nameservers", MaxDNSNameservers)))
		}
		for i, ns := range dnsConfig.Nameservers {
			allErrs = append(allErrs, IsValidIPForLegacyField(fldPath.Child("nameservers").Index(i), ns, nil)...)
		}
		// 校验 searches。
		if len(dnsConfig.Searches) > MaxDNSSearchPaths {
			allErrs = append(allErrs, field.Invalid(fldPath.Child("searches"), dnsConfig.Searches, fmt.Sprintf("must not have more than %v search paths", MaxDNSSearchPaths)))
		}
		// 字符数统计包含 search 路径之间的空格。
		if len(strings.Join(dnsConfig.Searches, " ")) > MaxDNSSearchListChars {
			allErrs = append(allErrs, field.Invalid(fldPath.Child("searches"), dnsConfig.Searches, fmt.Sprintf("must not have more than %v characters (including spaces) in the search list", MaxDNSSearchListChars)))
		}

		for i, search := range dnsConfig.Searches {
			if search != "." {
				search = strings.TrimSuffix(search, ".")
				allErrs = append(allErrs, ValidateDNS1123SubdomainWithUnderScore(search, fldPath.Child("searches").Index(i))...)
			}
		}
		// 校验 options。
		for i, option := range dnsConfig.Options {
			if len(option.Name) == 0 {
				allErrs = append(allErrs, field.Required(fldPath.Child("options").Index(i), "must not be empty"))
			}
		}
	}
	return allErrs
}

// validatePodHostNetworkDeps 校验依赖 HostNetwork 取值的字段；所有 PodSpec 均应调用，opts 可调整强制程度。
func validatePodHostNetworkDeps(spec *core.PodSpec, fldPath *field.Path, opts PodValidationOptions) field.ErrorList {
	// 出于<某些原因>，内部版本的 Pod 把 `.HostNetwork`
	// 保留在 .SecurityContext 中。
	hostNetwork := spec.HostNetwork

	allErrors := field.ErrorList{}

	if hostNetwork {
		fldPath := fldPath.Child("containers")
		for i, container := range spec.Containers {
			portsPath := fldPath.Index(i).Child("ports")
			for i, port := range container.Ports {
				idxPath := portsPath.Index(i)
				// 执行到这里说明 HostNetwork 为 true。若该
				// PodSpec 位于 Pod 中（opts.ResourceIsPod），则 HostPort
				// 必须与 ContainerPort 相同。若 PodSpec 位于
				// 其他资源（如 Deployment）中，则允许 0（即
				// 未指定），因为 Pod 最终创建时会被默认填充，
				// 但不允许其他任何取值。
				if hp, cp := port.HostPort, port.ContainerPort; (opts.ResourceIsPod || hp != 0) && hp != cp {
					allErrors = append(allErrors, field.Invalid(idxPath.Child("hostPort"), port.HostPort, "must match `containerPort` when `hostNetwork` is true"))
				}
			}
		}
	}
	return allErrors
}

// validateImagePullSecrets 校验 imagePullSecrets 列表格式；当前仅检查 name，以保持对未来字段变更的敏感。
func validateImagePullSecrets(imagePullSecrets []core.LocalObjectReference, fldPath *field.Path) field.ErrorList {
	allErrors := field.ErrorList{}
	for i, currPullSecret := range imagePullSecrets {
		idxPath := fldPath.Index(i)
		strippedRef := core.LocalObjectReference{Name: currPullSecret.Name}
		if !reflect.DeepEqual(strippedRef, currPullSecret) {
			allErrors = append(allErrors, field.Invalid(idxPath, currPullSecret, "only name may be set"))
		}
	}
	return allErrors
}

// validateAffinity 校验 Pod 的 nodeAffinity、podAffinity 与 podAntiAffinity 是否合法。
func validateAffinity(affinity *core.Affinity, opts PodValidationOptions, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}

	if affinity != nil {
		if affinity.NodeAffinity != nil {
			allErrs = append(allErrs, validateNodeAffinity(affinity.NodeAffinity, opts, fldPath.Child("nodeAffinity"))...)
		}
		if affinity.PodAffinity != nil {
			allErrs = append(allErrs, validatePodAffinity(affinity.PodAffinity, opts.AllowInvalidLabelValueInSelector, fldPath.Child("podAffinity"))...)
		}
		if affinity.PodAntiAffinity != nil {
			allErrs = append(allErrs, validatePodAntiAffinity(affinity.PodAntiAffinity, opts.AllowInvalidLabelValueInSelector, fldPath.Child("podAntiAffinity"))...)
		}
	}

	return allErrs
}

// validateTaintEffect 校验污点（Taint）的 effect 是否为合法值。
func validateTaintEffect(effect *core.TaintEffect, allowEmpty bool, fldPath *field.Path) field.ErrorList {
	if !allowEmpty && len(*effect) == 0 {
		return field.ErrorList{field.Required(fldPath, "")}
	}

	allErrors := field.ErrorList{}
	switch *effect {
	// TODO: 实现 TaintEffectNoScheduleNoAdmit 后，用后面被注释掉的行替换下一行。
	case core.TaintEffectNoSchedule, core.TaintEffectPreferNoSchedule, core.TaintEffectNoExecute:
		// case core.TaintEffectNoSchedule, core.TaintEffectPreferNoSchedule, core.TaintEffectNoScheduleNoAdmit, core.TaintEffectNoExecute:
	default:
		validValues := []core.TaintEffect{
			core.TaintEffectNoSchedule,
			core.TaintEffectPreferNoSchedule,
			core.TaintEffectNoExecute,
			// TODO: 实现 TaintEffectNoScheduleNoAdmit 后取消注释本块。
			// core.TaintEffectNoScheduleNoAdmit,
		}
		allErrors = append(allErrors, field.NotSupported(fldPath, *effect, validValues))
	}
	return allErrors
}

// validateOnlyAddedTolerations 校验更新后的 Pod tolerations：只允许新增，不允许修改或删除。
func validateOnlyAddedTolerations(newTolerations []core.Toleration, oldTolerations []core.Toleration, fldPath *field.Path, opts PodValidationOptions) field.ErrorList {
	allErrs := field.ErrorList{}
	for _, old := range oldTolerations {
		found := false
		oldTolerationClone := old.DeepCopy()
		for _, newToleration := range newTolerations {
			// 先赋值到克隆对象再做深比较，以允许 tolerationSeconds 发生变化。
			oldTolerationClone.TolerationSeconds = newToleration.TolerationSeconds // +k8s:verify-mutation:reason=clone
			if reflect.DeepEqual(*oldTolerationClone, newToleration) {
				found = true
				break
			}
		}
		if !found {
			allErrs = append(allErrs, field.Forbidden(fldPath, "existing toleration can not be modified except its tolerationSeconds"))
			return allErrs
		}
	}

	allErrs = append(allErrs, ValidateTolerations(newTolerations, fldPath, opts, KeyFormatCovered)...)
	return allErrs
}

// validateOnlyDeletedSchedulingGates 校验更新后的 schedulingGates：只允许删除，不允许新增或修改。
func validateOnlyDeletedSchedulingGates(newGates, oldGates []core.PodSchedulingGate, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	if len(newGates) == 0 {
		return allErrs
	}

	additionalGates := make(map[string]int)
	for i, newGate := range newGates {
		additionalGates[newGate.Name] = i
	}

	for _, oldGate := range oldGates {
		delete(additionalGates, oldGate.Name)
	}

	for gate, i := range additionalGates {
		allErrs = append(allErrs, field.Forbidden(fldPath.Index(i).Child("name"), fmt.Sprintf("only deletion is allowed, but found new scheduling gate '%s'", gate)))
	}

	return allErrs
}

// ValidateHostAliases 校验 hostAliases 列表：IP 格式合法、主机名格式合法且无重复。
func ValidateHostAliases(hostAliases []core.HostAlias, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	for i, hostAlias := range hostAliases {
		allErrs = append(allErrs, IsValidIPForLegacyField(fldPath.Index(i).Child("ip"), hostAlias.IP, nil)...)
		for j, hostname := range hostAlias.Hostnames {
			allErrs = append(allErrs, ValidateDNS1123Subdomain(hostname, fldPath.Index(i).Child("hostnames").Index(j))...)
		}
	}
	return allErrs
}

// ValidateTolerationsOption 是 ValidateTolerations 的选项，用于标记
// 哪些手写校验的错误信息已由声明式校验覆盖。
type ValidateTolerationsOption int

const (
	// KeyFormatCovered 表示 toleration key 的格式检查已由声明式校验
	// 覆盖。仅容忍度经声明式校验（结构化的 PodSpec.tolerations 与
	// RuntimeClass.scheduling.tolerations 字段）的调用方可以传入。
	// ValidateTolerationsInPodAnnotations 不得传入，因为它的 tolerations
	// 从注解字符串解析而来，没有对应的声明式校验。
	KeyFormatCovered ValidateTolerationsOption = iota
)

// ValidateTolerations 校验给定的 tolerations 列表数据是否合法（键、操作符、effect 与容忍秒数）。
func ValidateTolerations(tolerations []core.Toleration, fldPath *field.Path, opts PodValidationOptions, validationOpts ...ValidateTolerationsOption) field.ErrorList {
	allErrors := field.ErrorList{}
	keyFormatCovered := slices.Contains(validationOpts, KeyFormatCovered)
	for i, toleration := range tolerations {
		idxPath := fldPath.Index(i)
		// 校验 toleration key
		if len(toleration.Key) > 0 {
			keyErrs := unversionedvalidation.ValidateLabelName(toleration.Key, idxPath.Child("key"))
			if keyFormatCovered {
				keyErrs = keyErrs.MarkCoveredByDeclarative()
			}
			allErrors = append(allErrors, keyErrs...)
		}

		// key 为空且 operator 为 Exists、value 为空，表示容忍所有污点
		if len(toleration.Key) == 0 && toleration.Operator != core.TolerationOpExists {
			allErrors = append(allErrors, field.Invalid(idxPath.Child("operator"), toleration.Operator,
				"operator must be Exists when `key` is empty, which means \"match all values and all keys\""))
		}

		if toleration.TolerationSeconds != nil && toleration.Effect != core.TaintEffectNoExecute {
			allErrors = append(allErrors, field.Invalid(idxPath.Child("effect"), toleration.Effect,
				"effect must be 'NoExecute' when `tolerationSeconds` is set"))
		}

		// 校验 toleration 的 operator 与 value
		switch toleration.Operator {
		// operator 为空表示 Equal
		case core.TolerationOpEqual, "":
			if errs := validation.IsValidLabelValue(toleration.Value); len(errs) != 0 {
				allErrors = append(allErrors, field.Invalid(idxPath.Child("operator"), toleration.Value, strings.Join(errs, ";")))
			}
		case core.TolerationOpExists:
			if len(toleration.Value) > 0 {
				allErrors = append(allErrors, field.Invalid(idxPath.Child("operator"), toleration.Value, "value must be empty when `operator` is 'Exists'"))
			}
		case core.TolerationOpLt, core.TolerationOpGt:
			// 数值比较运算符需要相应校验选项
			if !opts.AllowTaintTolerationComparisonOperators {
				validValues := []core.TolerationOperator{core.TolerationOpEqual, core.TolerationOpExists, core.TolerationOpLt, core.TolerationOpGt}
				allErrors = append(allErrors, field.NotSupported(idxPath.Child("operator"), toleration.Operator, validValues))
				break
			}

			// 校验 value 为十进制整数
			for _, msg := range content.IsDecimalInteger(toleration.Value) {
				allErrors = append(allErrors, field.Invalid(idxPath.Child("value"), toleration.Value, msg))
			}

			// 校验 value 在 int64 范围内
			if _, err := strconv.ParseInt(toleration.Value, 10, 64); err != nil {
				allErrors = append(allErrors, field.Invalid(idxPath.Child("value"), toleration.Value, err.Error()))
			}
		default:
			validValues := []core.TolerationOperator{core.TolerationOpEqual, core.TolerationOpExists}
			allErrors = append(allErrors, field.NotSupported(idxPath.Child("operator"), toleration.Operator, validValues))
		}

		// 校验 toleration effect，effect 为空表示容忍所有污点效果
		if len(toleration.Effect) > 0 {
			allErrors = append(allErrors, validateTaintEffect(&toleration.Effect, true, idxPath.Child("effect"))...)
		}
	}
	return allErrors
}

// validateContainersOnlyForPod 对 Pod（而非 Pod 模板）中的容器执行额外校验，仅补充 validateContainers 未覆盖的字段。
func validateContainersOnlyForPod(containers []core.Container, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	for i, ctr := range containers {
		allErrs = append(allErrs, validateContainerOnlyForPod(&ctr, fldPath.Index(i))...)
	}
	return allErrs
}

// validateContainerOnlyForPod 对单个容器执行仅限 Pod 的校验，由 validateContainersOnlyForPod 与临时容器校验直接调用。
func validateContainerOnlyForPod(ctr *core.Container, path *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	if len(ctr.Image) != len(strings.TrimSpace(ctr.Image)) {
		allErrs = append(allErrs, field.Invalid(path.Child("image"), ctr.Image, "must not have leading or trailing whitespace"))
	}
	return allErrs
}

// PodValidationOptions 包含 Pod 校验的各项设置。
type PodValidationOptions struct {
	// 为向后兼容，允许非法的 pod-deletion-cost 注解取值。
	AllowInvalidPodDeletionCost bool
	// 允许 LabelSelector 中存在非法的 label-value
	AllowInvalidLabelValueInSelector bool
	// 允许 pod spec 使用大页单位大小的非整数倍取值
	AllowIndivisibleHugePagesValues bool
	// 为向后兼容，允许非法的 topologySpreadConstraint labelSelector
	AllowInvalidTopologySpreadConstraintLabelSelector bool
	// 允许 projected token 卷使用非本地路径
	AllowNonLocalProjectedTokenPath bool
	// 允许在 hostNet 与 hostIPC 的 Pod 中使用带命名空间的 sysctl
	AllowNamespacedSysctlsForHostNetAndHostIPC bool
	// 被校验的顶层资源是 Pod 本身，而非嵌入在其他资源中的
	// PodSpec。
	ResourceIsPod bool
	// 允许对环境变量名采用宽松校验
	AllowRelaxedEnvironmentVariableValidation bool
	// 仅允许 SELinuxChangePolicy 取 Recursive 值。
	AllowOnlyRecursiveSELinuxChangePolicy bool
	// 表示 PodLevelResources 特性是否启用。
	PodLevelResourcesEnabled bool
	// 表示 InPlacePodLevelResourcesVerticalScaling 特性是否
	// 启用。
	InPlacePodLevelResourcesVerticalScalingEnabled bool
	// 表示 InPlacePodVerticalScalingMemoryBackedVolumes 特性是否
	// 启用。
	InPlacePodVerticalScalingMemoryBackedVolumesEnabled bool
	// 为向后兼容，允许 sidecar 容器设置 resize policy
	AllowSidecarResizePolicy bool
	// 允许 RequiredNodeSelector 中存在非法的 label-value
	AllowInvalidLabelValueInRequiredNodeAffinity bool
	// 允许 TopologySpreadConstraints 的 MatchLabelKeys 特性
	AllowMatchLabelKeysInPodTopologySpread bool
	// 允许把由 MatchLabelKeys 构造的选择器合并进 TopologySpreadConstraints 的 LabelSelector
	AllowMatchLabelKeysInPodTopologySpreadSelectorMerge bool
	// 旧 Pod 的 TopologySpreadConstraints 中 MatchLabelKeys 不符合当前（>=v1.34）校验
	OldPodViolatesMatchLabelKeysValidation bool
	// 旧 Pod 的 TopologySpreadConstraints 中 MatchLabelKeys 不符合旧版（<v1.34）校验
	OldPodViolatesLegacyMatchLabelKeysValidation bool
	// 允许容器通过环境变量文件消费环境变量。
	AllowEnvFilesValidation bool
	// 允许容器设置 restart policy 与 restart policy rules。
	AllowContainerRestartPolicyRules bool
	// 为向后兼容，允许非 sidecar 的 init 容器保留已有的 RestartContainer resize policy
	AllowExistingRestartContainerForNonSidecarInitContainer bool
	// 允许 user namespaces 与 volume devices 共存，即便其并不能正常工作
	// （只应在更新本就持有该非法配置的对象时容忍）。
	AllowUserNamespacesWithVolumeDevices bool
	// 允许污点容忍的比较运算符（Lt、Gt）
	AllowTaintTolerationComparisonOperators bool
	// 允许 hostNetwork Pod 使用 user namespaces
	AllowUserNamespacesHostNetworkSupport bool
	// 允许容器设置带 RestartAllContainers 动作的 restart policy rule，适用于 init、sidecar 与普通容器。
	AllowRestartAllContainers bool
	// 允许 container statuses 中包含 image volume 的 digest
	AllowImageVolumeWithDigest bool
	// 为向后兼容，允许空的 image volume 引用
	AllowEmptyImageVolumeReference bool
	// 即便 AllowPrivilegeEscalation 为 false，也允许容器持有 CAP_SYS_ADMIN
	AllowSysAdminWhenPrivilegeEscalationFalse bool
}

// validatePodMetadataAndSpec 校验 Pod 元数据与规格中的必填字段，由创建与更新流程共同调用。
func validatePodMetadataAndSpec(pod *core.Pod, opts PodValidationOptions) field.ErrorList {
	metaPath := field.NewPath("metadata")
	specPath := field.NewPath("spec")

	allErrs := ValidateObjectMeta(&pod.ObjectMeta, true, ValidatePodName, metaPath)
	allErrs = append(allErrs, ValidatePodSpecificAnnotations(pod.ObjectMeta.Annotations, &pod.Spec, metaPath.Child("annotations"), opts)...)
	allErrs = append(allErrs, ValidatePodSpec(&pod.Spec, &pod.ObjectMeta, specPath, opts)...)

	// 这里执行仅适用于 Pod（而不适用于 pod 模板）的额外校验，
	// 这么做是为了保持向后兼容

	if pod.Spec.ServiceAccountName == "" {
		for vi, volume := range pod.Spec.Volumes {
			path := specPath.Child("volumes").Index(vi).Child("projected")
			if volume.Projected != nil {
				for si, source := range volume.Projected.Sources {
					saPath := path.Child("sources").Index(si).Child("serviceAccountToken")
					if source.ServiceAccountToken != nil {
						allErrs = append(allErrs, field.Forbidden(saPath, "must not be specified when serviceAccountName is not set"))
					}
				}
			}
		}
	}

	if pod.Spec.TerminationGracePeriodSeconds != nil {
		allErrs = append(allErrs, ValidateNonnegativeField(*pod.Spec.TerminationGracePeriodSeconds, specPath.Child("terminationGracePeriodSeconds"))...)
	}

	allErrs = append(allErrs, validateContainersOnlyForPod(pod.Spec.Containers, specPath.Child("containers"))...)
	allErrs = append(allErrs, validateContainersOnlyForPod(pod.Spec.InitContainers, specPath.Child("initContainers"))...)
	// 临时容器的 validateContainersOnlyForPod() 检查由 validateEphemeralContainers() 完成

	return allErrs
}

// validatePodIPs 校验 Pod 状态中的 IP 列表格式与重复情况。
func validatePodIPs(pod, oldPod *core.Pod) field.ErrorList {
	allErrs := field.ErrorList{}

	podIPsField := field.NewPath("status", "podIPs")

	// 所有新增的 PodIP 必须是合法 IP，但已存在的非法 IP 可以保留。
	var existingPodIPs []string
	if oldPod != nil {
		existingPodIPs = make([]string, len(oldPod.Status.PodIPs))
		for i, podIP := range oldPod.Status.PodIPs {
			existingPodIPs[i] = podIP.IP
		}
	}
	for i, podIP := range pod.Status.PodIPs {
		allErrs = append(allErrs, IsValidIPForLegacyField(podIPsField.Index(i), podIP.IP, existingPodIPs)...)
	}

	// Pod.PodIP 多于一个时，必须是双栈的一对
	if len(pod.Status.PodIPs) > 1 {
		podIPs := make([]string, 0, len(pod.Status.PodIPs))
		for _, podIP := range pod.Status.PodIPs {
			podIPs = append(podIPs, podIP.IP)
		}

		dualStack, err := netutils.IsDualStackIPStrings(podIPs)
		if err != nil {
			allErrs = append(allErrs, field.InternalError(podIPsField, fmt.Errorf("failed to check for dual stack with error:%v", err)))
		}

		// 每个 IP 族只支持一个（即列表中最多两个 IP）。
		if !dualStack || len(podIPs) > 2 {
			allErrs = append(allErrs, field.Invalid(podIPsField, pod.Status.PodIPs, "may specify no more than one IP for each IP family"))
		}
	}

	// 作为额外的安全层，确保 podIP 与 podIPs[0].IP 一致。
	hasPodIP := len(pod.Status.PodIP) > 0
	hasPodIPs := len(pod.Status.PodIPs) > 0
	switch {
	case hasPodIP != hasPodIPs:
		allErrs = append(allErrs, field.Invalid(podIPsField, pod.Status.PodIPs, "podIP and podIPs must either both be set or both be unset"))
	case hasPodIPs && pod.Status.PodIP != pod.Status.PodIPs[0].IP:
		allErrs = append(allErrs, field.Invalid(podIPsField.Index(0).Child("ip"), pod.Status.PodIPs[0].IP, "must be equal to `podIP`"))
	}

	return allErrs
}

// validateHostIPs 校验 Pod 状态中的 hostIP 列表格式与重复情况。
func validateHostIPs(pod, oldPod *core.Pod) field.ErrorList {
	allErrs := field.ErrorList{}

	if len(pod.Status.HostIPs) == 0 {
		return allErrs
	}

	hostIPsField := field.NewPath("status", "hostIPs")

	// hostIP 必须等于 hostIPs[0].IP
	if pod.Status.HostIP != pod.Status.HostIPs[0].IP {
		allErrs = append(allErrs, field.Invalid(hostIPsField.Index(0).Child("ip"), pod.Status.HostIPs[0].IP, "must be equal to `hostIP`"))
	}

	// 所有新增的 HostIP 必须是合法 IP，但已存在的非法 IP 可以保留。
	var existingHostIPs []string
	if oldPod != nil {
		existingHostIPs = make([]string, len(oldPod.Status.HostIPs))
		for i, hostIP := range oldPod.Status.HostIPs {
			existingHostIPs[i] = hostIP.IP
		}
	}
	for i, hostIP := range pod.Status.HostIPs {
		allErrs = append(allErrs, IsValidIPForLegacyField(hostIPsField.Index(i), hostIP.IP, existingHostIPs)...)
	}

	// Pod.HostIP 多于一个时，必须是双栈的一对
	if len(pod.Status.HostIPs) > 1 {
		hostIPs := make([]string, 0, len(pod.Status.HostIPs))
		for _, hostIP := range pod.Status.HostIPs {
			hostIPs = append(hostIPs, hostIP.IP)
		}

		dualStack, err := netutils.IsDualStackIPStrings(hostIPs)
		if err != nil {
			allErrs = append(allErrs, field.InternalError(hostIPsField, fmt.Errorf("failed to check for dual stack with error:%v", err)))
		}

		// 每个 IP 族只支持一个（即列表中最多两个 IP）。
		if !dualStack || len(hostIPs) > 2 {
			allErrs = append(allErrs, field.Invalid(hostIPsField, pod.Status.HostIPs, "may specify no more than one IP for each IP family"))
		}
	}

	return allErrs
}

// ValidatePodSpec 校验 PodSpec 数据：格式、唯一性，并通过默认值与兼容逻辑规范化结构。
func ValidatePodSpec(spec *core.PodSpec, podMeta *metav1.ObjectMeta, fldPath *field.Path, opts PodValidationOptions) field.ErrorList {
	allErrs := field.ErrorList{}

	if spec.TerminationGracePeriodSeconds == nil {
		allErrs = append(allErrs, field.Required(fldPath.Child("terminationGracePeriodSeconds"), ""))
	}
	gracePeriod := spec.TerminationGracePeriodSeconds

	// hostUsers 默认为 true，因此未设置 HostUsers 字段的 spec 取值为 true。
	// 若默认值将来改变，此处的判断条件也需要跟着修改。
	hostUsers := spec.HostUsers == nil || *spec.HostUsers

	vols, vErrs := ValidateVolumes(spec.Volumes, podMeta, fldPath.Child("volumes"), opts)
	allErrs = append(allErrs, vErrs...)
	podClaimNames := gatherPodResourceClaimNames(spec.ResourceClaims)
	allErrs = append(allErrs, validatePodResourceClaims(podMeta, spec.ResourceClaims, fldPath.Child("resourceClaims"))...)
	allErrs = append(allErrs, validateContainers(spec.Containers, spec.OS, vols, podClaimNames, gracePeriod, fldPath.Child("containers"), opts, &spec.RestartPolicy, hostUsers)...)
	allErrs = append(allErrs, validateInitContainers(spec.InitContainers, spec.OS, spec.Containers, vols, podClaimNames, gracePeriod, fldPath.Child("initContainers"), opts, &spec.RestartPolicy, hostUsers)...)
	allErrs = append(allErrs, validateEphemeralContainers(spec.EphemeralContainers, spec.Containers, spec.InitContainers, vols, podClaimNames, fldPath.Child("ephemeralContainers"), opts, &spec.RestartPolicy, hostUsers)...)

	if opts.PodLevelResourcesEnabled {
		allErrs = append(allErrs, validatePodResources(spec, podClaimNames, fldPath, opts)...)
	}

	allErrs = append(allErrs, validatePodHostNetworkDeps(spec, fldPath, opts)...)
	allErrs = append(allErrs, validateRestartPolicy(&spec.RestartPolicy, fldPath.Child("restartPolicy"))...)
	allErrs = append(allErrs, validateDNSPolicy(&spec.DNSPolicy, fldPath.Child("dnsPolicy"))...)
	allErrs = append(allErrs, unversionedvalidation.ValidateLabels(spec.NodeSelector, fldPath.Child("nodeSelector"))...)
	allErrs = append(allErrs, validatePodSpecSecurityContext(spec.SecurityContext, spec, fldPath, fldPath.Child("securityContext"), opts)...)
	allErrs = append(allErrs, validateShareProcessNamespace(spec, fldPath)...)
	allErrs = append(allErrs, validateImagePullSecrets(spec.ImagePullSecrets, fldPath.Child("imagePullSecrets"))...)
	allErrs = append(allErrs, validateAffinity(spec.Affinity, opts, fldPath.Child("affinity"))...)
	allErrs = append(allErrs, validatePodDNSConfig(spec.DNSConfig, &spec.DNSPolicy, fldPath.Child("dnsConfig"), opts)...)
	allErrs = append(allErrs, validateReadinessGates(spec.ReadinessGates, fldPath.Child("readinessGates"))...)
	allErrs = append(allErrs, validateSchedulingGates(spec.SchedulingGates, fldPath.Child("schedulingGates"))...)
	allErrs = append(allErrs, validateTopologySpreadConstraints(spec.TopologySpreadConstraints, fldPath.Child("topologySpreadConstraints"), opts)...)
	allErrs = append(allErrs, validateWindowsHostProcessPod(spec, fldPath)...)
	allErrs = append(allErrs, validateHostUsers(spec, fldPath, opts)...)
	allErrs = append(allErrs, validatePodHostName(spec, fldPath)...)
	if len(spec.ServiceAccountName) > 0 {
		for _, msg := range ValidateServiceAccountName(spec.ServiceAccountName, false) {
			allErrs = append(allErrs, field.Invalid(fldPath.Child("serviceAccountName"), spec.ServiceAccountName, msg))
		}
	}
	if deprecatedServiceAccount := spec.DeprecatedServiceAccount; deprecatedServiceAccount != spec.ServiceAccountName { //nolint:staticcheck // SA1019 enforce the deprecated field stays in sync
		allErrs = append(allErrs, field.Invalid(fldPath.Child("deprecatedServiceAccount"), deprecatedServiceAccount, fmt.Sprintf("must be the same as serviceAccountName: %q", spec.ServiceAccountName)))
	}

	if len(spec.NodeName) > 0 {
		for _, msg := range ValidateNodeName(spec.NodeName, false) {
			allErrs = append(allErrs, field.Invalid(fldPath.Child("nodeName"), spec.NodeName, msg))
		}
	}

	if spec.ActiveDeadlineSeconds != nil {
		value := *spec.ActiveDeadlineSeconds
		if value < 1 || value > math.MaxInt32 {
			allErrs = append(allErrs, field.Invalid(fldPath.Child("activeDeadlineSeconds"), value, validation.InclusiveRangeError(1, math.MaxInt32)))
		}
	}

	if len(spec.Hostname) > 0 {
		allErrs = append(allErrs, ValidateDNS1123Label(spec.Hostname, fldPath.Child("hostname"))...)
	}

	if len(spec.Subdomain) > 0 {
		allErrs = append(allErrs, ValidateDNS1123Label(spec.Subdomain, fldPath.Child("subdomain"))...)
	}

	if len(spec.Tolerations) > 0 {
		allErrs = append(allErrs, ValidateTolerations(spec.Tolerations, fldPath.Child("tolerations"), opts, KeyFormatCovered)...)
	}

	if len(spec.HostAliases) > 0 {
		allErrs = append(allErrs, ValidateHostAliases(spec.HostAliases, fldPath.Child("hostAliases"))...)
	}

	if len(spec.PriorityClassName) > 0 {
		for _, msg := range ValidatePriorityClassName(spec.PriorityClassName, false) {
			allErrs = append(allErrs, field.Invalid(fldPath.Child("priorityClassName"), spec.PriorityClassName, msg))
		}
	}

	if spec.RuntimeClassName != nil {
		allErrs = append(allErrs, ValidateRuntimeClassName(*spec.RuntimeClassName, fldPath.Child("runtimeClassName"))...)
	}

	if spec.PreemptionPolicy != nil {
		allErrs = append(allErrs, ValidatePreemptionPolicy(spec.PreemptionPolicy, fldPath.Child("preemptionPolicy"))...)
	}

	if spec.Overhead != nil {
		allErrs = append(allErrs, validateOverhead(spec.Overhead, fldPath.Child("overhead"), opts)...)
	}

	if spec.OS != nil {
		osErrs := validateOS(spec, fldPath.Child("os"), opts)
		switch {
		case len(osErrs) > 0:
			allErrs = append(allErrs, osErrs...)
		case spec.OS.Name == core.Linux:
			allErrs = append(allErrs, validateLinux(spec, fldPath)...)
		case spec.OS.Name == core.Windows:
			allErrs = append(allErrs, validateWindows(spec, fldPath)...)
		}
	}

	if spec.SchedulingGroup != nil {
		allErrs = append(allErrs, validateSchedulingGroup(spec.SchedulingGroup, fldPath.Child("schedulingGroup"))...)
		if len(spec.EvictionResponders) > 0 {
			// 已由 alpha 的 DV dependentForbidden 覆盖
			allErrs = append(allErrs, field.Forbidden(fldPath.Child("schedulingGroup"), "may not be set when evictionResponders is set").WithOrigin("dependentForbidden").MarkCoveredByDeclarative())
		}
	}

	allErrs = append(allErrs, validateFileKeyRefVolumes(spec, fldPath)...)
	return allErrs
}

// validatePodResources 校验 Pod 级资源请求/限制与容器级资源的一致性。
func validatePodResources(spec *core.PodSpec, podClaimNames sets.Set[string], fldPath *field.Path, opts PodValidationOptions) field.ErrorList {
	if spec.Resources == nil {
		return nil
	}
	resourcesFldPath := fldPath.Child("resources")
	if spec.OS != nil && spec.OS.Name == core.Windows {
		// 若目标操作系统上 resources 字段本就禁止设置，
		// 则不再报告该字段取值更细粒度的错误。
		return field.ErrorList{
			field.Forbidden(resourcesFldPath, "may not be set for a windows pod"),
		}
	}

	allErrs := field.ErrorList{}

	if spec.Resources.Claims != nil {
		allErrs = append(allErrs, field.Forbidden(resourcesFldPath.Child("claims"), "claims may not be set for Resources at pod-level"))
	}

	// validatePodResourceRequirements 检查资源名称与数量是否合法，
	// 以及 requests 是否不大于 limits。
	allErrs = append(allErrs, validatePodResourceRequirements(spec.Resources, podClaimNames, resourcesFldPath, opts)...)
	allErrs = append(allErrs, validatePodResourceConsistency(spec, resourcesFldPath)...)
	return allErrs
}

// validatePodResourceConsistency 校验容器级请求总和不超过 Pod 级请求，且各容器限制不超过 Pod 级限制。
func validatePodResourceConsistency(spec *core.PodSpec, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}

	// 把 *core.PodSpec 转换为 *v1.PodSpec，以满足后续
	// resourcehelper.PodRequests 方法的调用要求，
	// 该方法需要一个 *v1.Pod 对象（内含 *v1.PodSpec）。
	v1PodSpec := &v1.PodSpec{}
	// TODO(ndixita): Convert_core_PodSpec_To_v1_PodSpec 有风险。针对
	// PodLevelResources 特性的 beta 版本，为内部 core.Pod 类型添加一份
	// AggregateContainerRequests。
	if err := corev1.Convert_core_PodSpec_To_v1_PodSpec(spec, v1PodSpec, nil); err != nil {
		allErrs = append(allErrs, field.InternalError(fldPath, fmt.Errorf("invalid %q: %v", fldPath, err.Error())))
	}

	reqPath := fldPath.Child("requests")
	// resourcehelper.AggregateContainerRequests 方法需要一个 Pod 对象来计算
	// Pod 的请求总量。因此这里用 v1PodSpec 临时构造 (&v1.Pod{Spec: *v1PodSpec})
	// 并传给 AggregateContainerRequests，以便在不修改该方法的
	// 前提下正确计算资源。
	aggrContainerReqs := resourcehelper.AggregateContainerRequests(&v1.Pod{Spec: *v1PodSpec}, resourcehelper.PodResourcesOptions{})

	// Pod 级 requests 必须 >= Pod 内所有容器的 requests 之和。
	for resourceName, ctrReqs := range aggrContainerReqs {
		// 若 Pod 级未设置该资源的 request 则跳过。
		podSpecRequests, exists := spec.Resources.Requests[core.ResourceName(resourceName.String())]
		if !exists {
			continue
		}

		fldPath := reqPath.Key(resourceName.String())
		if ctrReqs.Cmp(podSpecRequests) > 0 {
			allErrs = append(allErrs, field.Invalid(fldPath, podSpecRequests.String(), fmt.Sprintf("must be greater than or equal to aggregate container requests of %s", ctrReqs.String())))
		}
	}

	// Pod 级大页 limits 必须始终大于或等于容器级大页 limits 之和，
	// 这是因为在当前容器级大页行为下，大页资源被视为不可超卖资源
	// （request 与 limit 必须相等）。
	// 这也是 Pod 级资源不允许大页超卖的原因：Pod cgroup 的取值必须
	// 反映 Pod 级设置的 request/limit，且容器级 cgroup 取值必须在该
	// 限制之内。
	aggrContainerLims := resourcehelper.AggregateContainerLimits(&v1.Pod{Spec: *v1PodSpec}, resourcehelper.PodResourcesOptions{})
	for resourceName, ctrLims := range aggrContainerLims {
		if !helper.IsHugePageResourceName(core.ResourceName(resourceName)) {
			continue
		}

		podSpecLimits, hasLimit := spec.Resources.Limits[core.ResourceName(resourceName)]
		if !hasLimit {
			continue
		}

		if ctrLims.Cmp(podSpecLimits) > 0 {
			allErrs = append(allErrs, field.Invalid(fldPath.Child("limits").Key(string(resourceName)), podSpecLimits.String(), fmt.Sprintf("must be greater than or equal to aggregate container limits of %s", ctrLims.String())))
		}
	}

	// 单个容器的 limits 必须 <= Pod 级 limits。
	for i, ctr := range spec.Containers {
		for resourceName, ctrLimit := range ctr.Resources.Limits {
			// 若 Pod 级未设置该资源的 limit 则跳过。
			podSpecLimits, exists := spec.Resources.Limits[core.ResourceName(resourceName.String())]
			if !exists {
				continue
			}

			if ctrLimit.Cmp(podSpecLimits) > 0 {
				fldPath := fldPath.Child("containers").Index(i).Key(resourceName.String()).Child("limits")
				allErrs = append(allErrs, field.Invalid(fldPath, ctrLimit.String(), fmt.Sprintf("must be less than or equal to pod limits of %s", podSpecLimits.String())))
			}
		}
	}
	return allErrs
}

// validateLinux 校验 Pod 的 Linux 专属配置（sysctls、securityContext 等）。
func validateLinux(spec *core.PodSpec, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	securityContext := spec.SecurityContext
	if securityContext != nil && securityContext.WindowsOptions != nil {
		allErrs = append(allErrs, field.Forbidden(fldPath.Child("securityContext").Child("windowsOptions"), "windows options cannot be set for a linux pod"))
	}
	podshelper.VisitContainersWithPath(spec, fldPath, func(c *core.Container, cFldPath *field.Path) bool {
		sc := c.SecurityContext
		if sc != nil && sc.WindowsOptions != nil {
			fldPath := cFldPath.Child("securityContext")
			allErrs = append(allErrs, field.Forbidden(fldPath.Child("windowsOptions"), "windows options cannot be set for a linux pod"))
		}
		return true
	})
	return allErrs
}

// validateWindows 校验 Pod 的 Windows 专属配置（securityContext、hostProcess 等）。
func validateWindows(spec *core.PodSpec, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	securityContext := spec.SecurityContext
	// 校验 Pod 的 SecurityContext
	if securityContext != nil {
		if securityContext.AppArmorProfile != nil {
			allErrs = append(allErrs, field.Forbidden(fldPath.Child("securityContext").Child("appArmorProfile"), "cannot be set for a windows pod"))
		}
		if securityContext.SELinuxOptions != nil {
			allErrs = append(allErrs, field.Forbidden(fldPath.Child("securityContext").Child("seLinuxOptions"), "cannot be set for a windows pod"))
		}
		if securityContext.SeccompProfile != nil {
			allErrs = append(allErrs, field.Forbidden(fldPath.Child("securityContext").Child("seccompProfile"), "cannot be set for a windows pod"))
		}
		if securityContext.FSGroup != nil {
			allErrs = append(allErrs, field.Forbidden(fldPath.Child("securityContext").Child("fsGroup"), "cannot be set for a windows pod"))
		}
		if securityContext.FSGroupChangePolicy != nil {
			allErrs = append(allErrs, field.Forbidden(fldPath.Child("securityContext").Child("fsGroupChangePolicy"), "cannot be set for a windows pod"))
		}
		if len(securityContext.Sysctls) > 0 {
			allErrs = append(allErrs, field.Forbidden(fldPath.Child("securityContext").Child("sysctls"), "cannot be set for a windows pod"))
		}
		if securityContext.RunAsUser != nil {
			allErrs = append(allErrs, field.Forbidden(fldPath.Child("securityContext").Child("runAsUser"), "cannot be set for a windows pod"))
		}
		if securityContext.RunAsGroup != nil {
			allErrs = append(allErrs, field.Forbidden(fldPath.Child("securityContext").Child("runAsGroup"), "cannot be set for a windows pod"))
		}
		if securityContext.SupplementalGroups != nil {
			allErrs = append(allErrs, field.Forbidden(fldPath.Child("securityContext").Child("supplementalGroups"), "cannot be set for a windows pod"))
		}
		if securityContext.SupplementalGroupsPolicy != nil {
			allErrs = append(allErrs, field.Forbidden(fldPath.Child("securityContext").Child("supplementalGroupsPolicy"), "cannot be set for a windows pod"))
		}
		if securityContext.SELinuxChangePolicy != nil {
			allErrs = append(allErrs, field.Forbidden(fldPath.Child("securityContext").Child("seLinuxChangePolicy"), "cannot be set for a windows pod"))
		}
	}
	if spec.HostUsers != nil {
		allErrs = append(allErrs, field.Forbidden(fldPath.Child("hostUsers"), "cannot be set for a windows pod"))
	}
	if spec.HostPID {
		allErrs = append(allErrs, field.Forbidden(fldPath.Child("hostPID"), "cannot be set for a windows pod"))
	}
	if spec.HostIPC {
		allErrs = append(allErrs, field.Forbidden(fldPath.Child("hostIPC"), "cannot be set for a windows pod"))
	}
	if spec.ShareProcessNamespace != nil {
		allErrs = append(allErrs, field.Forbidden(fldPath.Child("shareProcessNamespace"), "cannot be set for a windows pod"))
	}
	podshelper.VisitContainersWithPath(spec, fldPath, func(c *core.Container, cFldPath *field.Path) bool {
		// 校验容器的 security context
		sc := c.SecurityContext
		// 按操作系统区分 podSecurityContext 校验
		// Windows 与 Linux 的 Security Context 在命名上存在部分重叠，
		// 但所有 Windows 专有选项都通过 securityContext.WindowsOptions 设置，
		// 我们将在下方校验
		// TODO: 考虑是否需要放宽这一限制或其中部分限制
		if sc != nil {
			fldPath := cFldPath.Child("securityContext")
			if sc.AppArmorProfile != nil {
				allErrs = append(allErrs, field.Forbidden(fldPath.Child("appArmorProfile"), "cannot be set for a windows pod"))
			}
			if sc.SELinuxOptions != nil {
				allErrs = append(allErrs, field.Forbidden(fldPath.Child("seLinuxOptions"), "cannot be set for a windows pod"))
			}
			if sc.SeccompProfile != nil {
				allErrs = append(allErrs, field.Forbidden(fldPath.Child("seccompProfile"), "cannot be set for a windows pod"))
			}
			if sc.Capabilities != nil {
				allErrs = append(allErrs, field.Forbidden(fldPath.Child("capabilities"), "cannot be set for a windows pod"))
			}
			if sc.ReadOnlyRootFilesystem != nil {
				allErrs = append(allErrs, field.Forbidden(fldPath.Child("readOnlyRootFilesystem"), "cannot be set for a windows pod"))
			}
			if sc.Privileged != nil {
				allErrs = append(allErrs, field.Forbidden(fldPath.Child("privileged"), "cannot be set for a windows pod"))
			}
			if sc.AllowPrivilegeEscalation != nil {
				allErrs = append(allErrs, field.Forbidden(fldPath.Child("allowPrivilegeEscalation"), "cannot be set for a windows pod"))
			}
			if sc.ProcMount != nil {
				allErrs = append(allErrs, field.Forbidden(fldPath.Child("procMount"), "cannot be set for a windows pod"))
			}
			if sc.RunAsUser != nil {
				allErrs = append(allErrs, field.Forbidden(fldPath.Child("runAsUser"), "cannot be set for a windows pod"))
			}
			if sc.RunAsGroup != nil {
				allErrs = append(allErrs, field.Forbidden(fldPath.Child("runAsGroup"), "cannot be set for a windows pod"))
			}
		}
		return true
	})
	return allErrs
}

// ValidateNodeSelectorRequirement 校验 NodeSelectorRequirement 的 key、operator 与 values 是否合法。
func ValidateNodeSelectorRequirement(rq core.NodeSelectorRequirement, allowInvalidLabelValueInRequiredNodeAffinity bool, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	switch rq.Operator {
	case core.NodeSelectorOpIn, core.NodeSelectorOpNotIn:
		if len(rq.Values) == 0 {
			allErrs = append(allErrs, field.Required(fldPath.Child("values"), "must be specified when `operator` is 'In' or 'NotIn'"))
		}
	case core.NodeSelectorOpExists, core.NodeSelectorOpDoesNotExist:
		if len(rq.Values) > 0 {
			allErrs = append(allErrs, field.Forbidden(fldPath.Child("values"), "may not be specified when `operator` is 'Exists' or 'DoesNotExist'"))
		}

	case core.NodeSelectorOpGt, core.NodeSelectorOpLt:
		if len(rq.Values) != 1 {
			allErrs = append(allErrs, field.Required(fldPath.Child("values"), "must be specified single value when `operator` is 'Lt' or 'Gt'"))
		}
	default:
		allErrs = append(allErrs, field.Invalid(fldPath.Child("operator"), rq.Operator, "not a valid selector operator"))
	}
	allErrs = append(allErrs, unversionedvalidation.ValidateLabelName(rq.Key, fldPath.Child("key"))...)
	if !allowInvalidLabelValueInRequiredNodeAffinity {
		path := fldPath.Child("values")
		for valueIndex, value := range rq.Values {
			for _, msg := range validation.IsValidLabelValue(value) {
				allErrs = append(allErrs, field.Invalid(path.Index(valueIndex), value, msg).WithOrigin("format=k8s-label-value"))
			}
		}
	}
	return allErrs
}

var nodeFieldSelectorValidators = map[string]func(string, bool) []string{
	metav1.ObjectNameField: ValidateNodeName,
}

// ValidateNodeFieldSelectorRequirement 校验 Node 字段选择器要求的字段与取值是否合法。
func ValidateNodeFieldSelectorRequirement(req core.NodeSelectorRequirement, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}

	switch req.Operator {
	case core.NodeSelectorOpIn, core.NodeSelectorOpNotIn:
		if len(req.Values) != 1 {
			allErrs = append(allErrs, field.Required(fldPath.Child("values"),
				"must be only one value when `operator` is 'In' or 'NotIn' for node field selector"))
		}
	default:
		allErrs = append(allErrs, field.Invalid(fldPath.Child("operator"), req.Operator, "not a valid selector operator"))
	}

	if vf, found := nodeFieldSelectorValidators[req.Key]; !found {
		allErrs = append(allErrs, field.Invalid(fldPath.Child("key"), req.Key, "not a valid field selector key"))
	} else {
		for i, v := range req.Values {
			for _, msg := range vf(v, false) {
				allErrs = append(allErrs, field.Invalid(fldPath.Child("values").Index(i), v, msg))
			}
		}
	}

	return allErrs
}

// ValidateNodeSelectorTerm 校验单个节点选择器项：表达式列表非空且各项合法。
func ValidateNodeSelectorTerm(term core.NodeSelectorTerm, allowInvalidLabelValueInRequiredNodeAffinity bool, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}

	for j, req := range term.MatchExpressions {
		allErrs = append(allErrs, ValidateNodeSelectorRequirement(req, allowInvalidLabelValueInRequiredNodeAffinity, fldPath.Child("matchExpressions").Index(j))...)
	}

	for j, req := range term.MatchFields {
		allErrs = append(allErrs, ValidateNodeFieldSelectorRequirement(req, fldPath.Child("matchFields").Index(j))...)
	}

	return allErrs
}

// ValidateNodeSelector 校验节点选择器整体结构：各项合法且表达式之间无冲突。
func ValidateNodeSelector(nodeSelector *core.NodeSelector, allowInvalidLabelValueInRequiredNodeAffinity bool, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}

	termFldPath := fldPath.Child("nodeSelectorTerms")
	if len(nodeSelector.NodeSelectorTerms) == 0 {
		return append(allErrs, field.Required(termFldPath, "must have at least one node selector term"))
	}

	for i, term := range nodeSelector.NodeSelectorTerms {
		allErrs = append(allErrs, ValidateNodeSelectorTerm(term, allowInvalidLabelValueInRequiredNodeAffinity, termFldPath.Index(i))...)
	}

	return allErrs
}

// validateTopologySelectorLabelRequirement 校验拓扑选择器标签要求，并构建其所有取值的集合。
func validateTopologySelectorLabelRequirement(rq core.TopologySelectorLabelRequirement, fldPath *field.Path) (sets.Set[string], field.ErrorList) {
	allErrs := field.ErrorList{}
	valueSet := make(sets.Set[string])
	valuesPath := fldPath.Child("values")
	if len(rq.Values) == 0 {
		allErrs = append(allErrs, field.Required(valuesPath, ""))
	}

	// 校验 Values 字段的不重复性
	for i, value := range rq.Values {
		if valueSet.Has(value) {
			allErrs = append(allErrs, field.Duplicate(valuesPath.Index(i), value))
		}
		valueSet.Insert(value)
	}

	allErrs = append(allErrs, unversionedvalidation.ValidateLabelName(rq.Key, fldPath.Child("key"))...)

	return valueSet, allErrs
}

// ValidateTopologySelectorTerm 校验单个拓扑选择器项，并构建其原始形式的映射。
func ValidateTopologySelectorTerm(term core.TopologySelectorTerm, fldPath *field.Path) (map[string]sets.Set[string], field.ErrorList) {
	allErrs := field.ErrorList{}
	exprMap := make(map[string]sets.Set[string])
	exprPath := fldPath.Child("matchLabelExpressions")

	// 允许 MatchLabelExpressions 为空，以备该字段将来变为可选。
	for i, req := range term.MatchLabelExpressions {
		idxPath := exprPath.Index(i)
		valueSet, exprErrs := validateTopologySelectorLabelRequirement(req, idxPath)
		allErrs = append(allErrs, exprErrs...)

		// 校验不存在重复的 key。
		if _, exists := exprMap[req.Key]; exists {
			allErrs = append(allErrs, field.Duplicate(idxPath.Child("key"), req.Key))
		}
		exprMap[req.Key] = valueSet
	}

	return exprMap, allErrs
}

// ValidateAvoidPodsInNodeAnnotations 校验 Node.Annotations 中序列化的 AvoidPods 数据是否合法。
func ValidateAvoidPodsInNodeAnnotations(annotations map[string]string, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}

	v1Avoids, err := schedulinghelper.GetAvoidPodsFromNodeAnnotations(annotations)
	if err != nil {
		allErrs = append(allErrs, field.Invalid(fldPath.Child("AvoidPods"), core.PreferAvoidPodsAnnotationKey, err.Error()))
		return allErrs
	}
	var avoids core.AvoidPods
	if err := corev1.Convert_v1_AvoidPods_To_core_AvoidPods(&v1Avoids, &avoids, nil); err != nil {
		allErrs = append(allErrs, field.Invalid(fldPath.Child("AvoidPods"), core.PreferAvoidPodsAnnotationKey, err.Error()))
		return allErrs
	}

	if len(avoids.PreferAvoidPods) != 0 {
		for i, pa := range avoids.PreferAvoidPods {
			idxPath := fldPath.Child(core.PreferAvoidPodsAnnotationKey).Index(i)
			allErrs = append(allErrs, validatePreferAvoidPodsEntry(pa, idxPath)...)
		}
	}

	return allErrs
}

// validatePreferAvoidPodsEntry 校验单条 PreferAvoidPods 条目是否合法。
func validatePreferAvoidPodsEntry(avoidPodEntry core.PreferAvoidPodsEntry, fldPath *field.Path) field.ErrorList {
	allErrors := field.ErrorList{}
	if avoidPodEntry.PodSignature.PodController == nil {
		allErrors = append(allErrors, field.Required(fldPath.Child("PodSignature"), ""))
	} else {
		if !*(avoidPodEntry.PodSignature.PodController.Controller) {
			allErrors = append(allErrors,
				field.Invalid(fldPath.Child("PodSignature").Child("PodController").Child("Controller"),
					*(avoidPodEntry.PodSignature.PodController.Controller), "must point to a controller"))
		}
	}
	return allErrors
}

// ValidatePreferredSchedulingTerms 校验软节点亲和性（PreferredSchedulingTerms）的权重与选择器是否合法。
func ValidatePreferredSchedulingTerms(terms []core.PreferredSchedulingTerm, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}

	for i, term := range terms {
		if term.Weight <= 0 || term.Weight > 100 {
			allErrs = append(allErrs, field.Invalid(fldPath.Index(i).Child("weight"), term.Weight, "must be in the range 1-100"))
		}

		// 对 preferred affinity 始终容忍非法的 label-value，
		// 因为当集群只有一个节点时它们仍可满足
		allErrs = append(allErrs, ValidateNodeSelectorTerm(term.Preference, true, fldPath.Index(i).Child("preference"))...)
	}
	return allErrs
}

// validatePodAffinityTerm 校验单个 PodAffinityTerm：标签选择器、命名空间、拓扑键等字段。
func validatePodAffinityTerm(podAffinityTerm core.PodAffinityTerm, allowInvalidLabelValueInSelector bool, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}

	allErrs = append(allErrs, ValidatePodAffinityTermSelector(podAffinityTerm, allowInvalidLabelValueInSelector, fldPath)...)
	for _, name := range podAffinityTerm.Namespaces {
		for _, msg := range ValidateNamespaceName(name, false) {
			allErrs = append(allErrs, field.Invalid(fldPath.Child("namespace"), name, msg))
		}
	}
	allErrs = append(allErrs, ValidateMatchLabelKeysAndMismatchLabelKeys(fldPath, podAffinityTerm.MatchLabelKeys, podAffinityTerm.MismatchLabelKeys, podAffinityTerm.LabelSelector)...)
	if len(podAffinityTerm.TopologyKey) == 0 {
		allErrs = append(allErrs, field.Required(fldPath.Child("topologyKey"), "can not be empty"))
	}
	return append(allErrs, unversionedvalidation.ValidateLabelName(podAffinityTerm.TopologyKey, fldPath.Child("topologyKey"))...)
}

// validatePodAffinityTerms 校验 PodAffinityTerms 列表的每一项。
func validatePodAffinityTerms(podAffinityTerms []core.PodAffinityTerm, allowInvalidLabelValueInSelector bool, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	for i, podAffinityTerm := range podAffinityTerms {
		allErrs = append(allErrs, validatePodAffinityTerm(podAffinityTerm, allowInvalidLabelValueInSelector, fldPath.Index(i))...)
	}
	return allErrs
}

// validateWeightedPodAffinityTerms 校验带权重的 Pod 亲和性项列表：权重范围与各项合法性。
func validateWeightedPodAffinityTerms(weightedPodAffinityTerms []core.WeightedPodAffinityTerm, allowInvalidLabelValueInSelector bool, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	for j, weightedTerm := range weightedPodAffinityTerms {
		if weightedTerm.Weight <= 0 || weightedTerm.Weight > 100 {
			allErrs = append(allErrs, field.Invalid(fldPath.Index(j).Child("weight"), weightedTerm.Weight, "must be in the range 1-100"))
		}
		allErrs = append(allErrs, validatePodAffinityTerm(weightedTerm.PodAffinityTerm, allowInvalidLabelValueInSelector, fldPath.Index(j).Child("podAffinityTerm"))...)
	}
	return allErrs
}

// validatePodAntiAffinity 校验 PodAntiAffinity 的 required 与 preferred 项。
func validatePodAntiAffinity(podAntiAffinity *core.PodAntiAffinity, allowInvalidLabelValueInSelector bool, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	// TODO: 实现 RequiredDuringSchedulingRequiredDuringExecution 后取消下方代码注释。
	// if podAntiAffinity.RequiredDuringSchedulingRequiredDuringExecution != nil {
	//	allErrs = append(allErrs, validatePodAffinityTerms(podAntiAffinity.RequiredDuringSchedulingRequiredDuringExecution, false,
	//		fldPath.Child("requiredDuringSchedulingRequiredDuringExecution"))...)
	// }
	if podAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution != nil {
		allErrs = append(allErrs, validatePodAffinityTerms(podAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution, allowInvalidLabelValueInSelector,
			fldPath.Child("requiredDuringSchedulingIgnoredDuringExecution"))...)
	}
	if podAntiAffinity.PreferredDuringSchedulingIgnoredDuringExecution != nil {
		allErrs = append(allErrs, validateWeightedPodAffinityTerms(podAntiAffinity.PreferredDuringSchedulingIgnoredDuringExecution, allowInvalidLabelValueInSelector,
			fldPath.Child("preferredDuringSchedulingIgnoredDuringExecution"))...)
	}
	return allErrs
}

// validateNodeAffinity 校验 NodeAffinity 的 required 与 preferred 项。
func validateNodeAffinity(na *core.NodeAffinity, opts PodValidationOptions, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	// TODO: 实现 RequiredDuringSchedulingRequiredDuringExecution 后取消接下来三行的注释。
	// if na.RequiredDuringSchedulingRequiredDuringExecution != nil {
	//	allErrs = append(allErrs, ValidateNodeSelector(na.RequiredDuringSchedulingRequiredDuringExecution, fldPath.Child("requiredDuringSchedulingRequiredDuringExecution"))...)
	// }
	if na.RequiredDuringSchedulingIgnoredDuringExecution != nil {
		allErrs = append(allErrs, ValidateNodeSelector(na.RequiredDuringSchedulingIgnoredDuringExecution, opts.AllowInvalidLabelValueInRequiredNodeAffinity, fldPath.Child("requiredDuringSchedulingIgnoredDuringExecution"))...)
	}
	if len(na.PreferredDuringSchedulingIgnoredDuringExecution) > 0 {
		allErrs = append(allErrs, ValidatePreferredSchedulingTerms(na.PreferredDuringSchedulingIgnoredDuringExecution, fldPath.Child("preferredDuringSchedulingIgnoredDuringExecution"))...)
	}
	return allErrs
}

// validatePodAffinity 校验 PodAffinity 的 required 与 preferred 项。
func validatePodAffinity(podAffinity *core.PodAffinity, allowInvalidLabelValueInSelector bool, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	// TODO: 实现 RequiredDuringSchedulingRequiredDuringExecution 后取消下方代码注释。
	// if podAffinity.RequiredDuringSchedulingRequiredDuringExecution != nil {
	//	allErrs = append(allErrs, validatePodAffinityTerms(podAffinity.RequiredDuringSchedulingRequiredDuringExecution, false,
	//		fldPath.Child("requiredDuringSchedulingRequiredDuringExecution"))...)
	// }
	if podAffinity.RequiredDuringSchedulingIgnoredDuringExecution != nil {
		allErrs = append(allErrs, validatePodAffinityTerms(podAffinity.RequiredDuringSchedulingIgnoredDuringExecution, allowInvalidLabelValueInSelector,
			fldPath.Child("requiredDuringSchedulingIgnoredDuringExecution"))...)
	}
	if podAffinity.PreferredDuringSchedulingIgnoredDuringExecution != nil {
		allErrs = append(allErrs, validateWeightedPodAffinityTerms(podAffinity.PreferredDuringSchedulingIgnoredDuringExecution, allowInvalidLabelValueInSelector,
			fldPath.Child("preferredDuringSchedulingIgnoredDuringExecution"))...)
	}
	return allErrs
}

// validateSeccompProfileField 校验容器/ Pod 的 seccompProfile 字段：类型与 localhostProfile 的匹配关系。
func validateSeccompProfileField(sp *core.SeccompProfile, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	if sp == nil {
		return allErrs
	}

	if err := validateSeccompProfileType(fldPath.Child("type"), sp.Type); err != nil {
		allErrs = append(allErrs, err)
	}

	if sp.Type == core.SeccompProfileTypeLocalhost {
		if sp.LocalhostProfile == nil {
			allErrs = append(allErrs, field.Required(fldPath.Child("localhostProfile"), "must be set when seccomp type is Localhost"))
		} else {
			allErrs = append(allErrs, validateLocalDescendingPath(*sp.LocalhostProfile, fldPath.Child("localhostProfile"))...)
		}
	} else {
		if sp.LocalhostProfile != nil {
			allErrs = append(allErrs, field.Invalid(fldPath.Child("localhostProfile"), sp, "can only be set when seccomp type is Localhost"))
		}
	}

	return allErrs
}

// ValidateSeccompProfile 校验 SeccompProfile 对象：类型合法，RuntimeDefault/Localhost 等字段组合正确。
func ValidateSeccompProfile(p string, fldPath *field.Path) field.ErrorList {
	if p == core.SeccompProfileRuntimeDefault || p == core.DeprecatedSeccompProfileDockerDefault {
		return nil
	}
	if p == v1.SeccompProfileNameUnconfined {
		return nil
	}
	if strings.HasPrefix(p, v1.SeccompLocalhostProfileNamePrefix) {
		return validateLocalDescendingPath(strings.TrimPrefix(p, v1.SeccompLocalhostProfileNamePrefix), fldPath)
	}
	return field.ErrorList{field.Invalid(fldPath, p, "must be a valid seccomp profile")}
}

// ValidateSeccompPodAnnotations 校验 Pod 注解中旧版 seccomp 注解的格式与取值。
func ValidateSeccompPodAnnotations(annotations map[string]string, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	if p, exists := annotations[core.SeccompPodAnnotationKey]; exists {
		allErrs = append(allErrs, ValidateSeccompProfile(p, fldPath.Child(core.SeccompPodAnnotationKey))...)
	}
	for k, p := range annotations {
		if strings.HasPrefix(k, core.SeccompContainerAnnotationKeyPrefix) {
			allErrs = append(allErrs, ValidateSeccompProfile(p, fldPath.Child(k))...)
		}
	}

	return allErrs
}

// validateSeccompProfileType 校验参数是否为合法的 SeccompProfileType。
func validateSeccompProfileType(fldPath *field.Path, seccompProfileType core.SeccompProfileType) *field.Error {
	switch seccompProfileType {
	case core.SeccompProfileTypeLocalhost, core.SeccompProfileTypeRuntimeDefault, core.SeccompProfileTypeUnconfined:
		return nil
	case "":
		return field.Required(fldPath, "type is required when seccompProfile is set")
	default:
		return field.NotSupported(fldPath, seccompProfileType, []core.SeccompProfileType{core.SeccompProfileTypeLocalhost, core.SeccompProfileTypeRuntimeDefault, core.SeccompProfileTypeUnconfined})
	}
}

// ValidateAppArmorProfileField 校验 AppArmorProfile 字段：类型与 profile 名称的组合是否合法。
func ValidateAppArmorProfileField(profile *core.AppArmorProfile, fldPath *field.Path) field.ErrorList {
	if profile == nil {
		return nil
	}

	allErrs := field.ErrorList{}

	switch profile.Type {
	case core.AppArmorProfileTypeLocalhost:
		if profile.LocalhostProfile == nil {
			allErrs = append(allErrs, field.Required(fldPath.Child("localhostProfile"), "must be set when AppArmor type is Localhost"))
		} else {
			localhostProfile := strings.TrimSpace(*profile.LocalhostProfile)
			if localhostProfile != *profile.LocalhostProfile {
				allErrs = append(allErrs, field.Invalid(fldPath.Child("localhostProfile"), *profile.LocalhostProfile, "must not be padded with whitespace"))
			} else if localhostProfile == "" {
				allErrs = append(allErrs, field.Required(fldPath.Child("localhostProfile"), "must be set when AppArmor type is Localhost"))
			}

			const maxLocalhostProfileLength = 4095 // PATH_MAX 减 1
			if len(*profile.LocalhostProfile) > maxLocalhostProfileLength {
				allErrs = append(allErrs, field.TooLong(fldPath.Child("localhostProfile"), "" /*unused*/, maxLocalhostProfileLength))
			}
		}

	case core.AppArmorProfileTypeRuntimeDefault, core.AppArmorProfileTypeUnconfined:
		if profile.LocalhostProfile != nil {
			allErrs = append(allErrs, field.Invalid(fldPath.Child("localhostProfile"), profile.LocalhostProfile, "can only be set when AppArmor type is Localhost"))
		}

	case "":
		allErrs = append(allErrs, field.Required(fldPath.Child("type"), "type is required when appArmorProfile is set"))

	default:
		allErrs = append(allErrs, field.NotSupported(fldPath.Child("type"), profile.Type,
			[]core.AppArmorProfileType{core.AppArmorProfileTypeLocalhost, core.AppArmorProfileTypeRuntimeDefault, core.AppArmorProfileTypeUnconfined}))
	}

	return allErrs

}

// ValidateAppArmorPodAnnotations 校验 Pod 注解中旧版 AppArmor 注解的格式。
func ValidateAppArmorPodAnnotations(annotations map[string]string, spec *core.PodSpec, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	for k, p := range annotations {
		if !strings.HasPrefix(k, v1.DeprecatedAppArmorBetaContainerAnnotationKeyPrefix) {
			continue
		}
		containerName := strings.TrimPrefix(k, v1.DeprecatedAppArmorBetaContainerAnnotationKeyPrefix)
		if !podSpecHasContainer(spec, containerName) {
			allErrs = append(allErrs, field.Invalid(fldPath.Key(k), containerName, "container not found"))
		}

		if err := ValidateAppArmorProfileFormat(p); err != nil {
			allErrs = append(allErrs, field.Invalid(fldPath.Key(k), p, err.Error()))
		}
	}

	return allErrs
}

// ValidateAppArmorProfileFormat 校验 AppArmor profile 名称格式是否合法。
func ValidateAppArmorProfileFormat(profile string) error {
	if profile == "" || profile == v1.DeprecatedAppArmorBetaProfileRuntimeDefault || profile == v1.DeprecatedAppArmorBetaProfileNameUnconfined {
		return nil
	}
	if !strings.HasPrefix(profile, v1.DeprecatedAppArmorBetaProfileNamePrefix) {
		return fmt.Errorf("invalid AppArmor profile name: %q", profile)
	}
	return nil
}

// validateAppArmorAnnotationsAndFieldsMatchOnCreate 校验创建时 AppArmor 字段与旧注解保持一致，避免双轨配置冲突。
func validateAppArmorAnnotationsAndFieldsMatchOnCreate(objectMeta metav1.ObjectMeta, podSpec *core.PodSpec, specPath *field.Path) field.ErrorList {
	if podSpec.OS != nil && podSpec.OS.Name == core.Windows {
		// Windows Pod 跳过一致性检查。
		return nil
	}

	allErrs := field.ErrorList{}

	var podProfile *core.AppArmorProfile
	if podSpec.SecurityContext != nil {
		podProfile = podSpec.SecurityContext.AppArmorProfile
	}
	podshelper.VisitContainersWithPath(podSpec, specPath, func(c *core.Container, cFldPath *field.Path) bool {
		containerProfile := podProfile
		if c.SecurityContext != nil && c.SecurityContext.AppArmorProfile != nil {
			containerProfile = c.SecurityContext.AppArmorProfile
		}

		if containerProfile == nil {
			return true
		}

		key := core.DeprecatedAppArmorAnnotationKeyPrefix + c.Name
		if annotation, found := objectMeta.Annotations[key]; found {
			apparmorPath := cFldPath.Child("securityContext").Child("appArmorProfile")

			switch containerProfile.Type {
			case core.AppArmorProfileTypeUnconfined:
				if annotation != core.DeprecatedAppArmorAnnotationValueUnconfined {
					allErrs = append(allErrs, field.Forbidden(apparmorPath.Child("type"), "apparmor type in annotation and field must match"))
				}

			case core.AppArmorProfileTypeRuntimeDefault:
				if annotation != core.DeprecatedAppArmorAnnotationValueRuntimeDefault {
					allErrs = append(allErrs, field.Forbidden(apparmorPath.Child("type"), "apparmor type in annotation and field must match"))
				}

			case core.AppArmorProfileTypeLocalhost:
				if !strings.HasPrefix(annotation, core.DeprecatedAppArmorAnnotationValueLocalhostPrefix) {
					allErrs = append(allErrs, field.Forbidden(apparmorPath.Child("type"), "apparmor type in annotation and field must match"))
				} else if containerProfile.LocalhostProfile == nil || strings.TrimPrefix(annotation, core.DeprecatedAppArmorAnnotationValueLocalhostPrefix) != *containerProfile.LocalhostProfile {
					allErrs = append(allErrs, field.Forbidden(apparmorPath.Child("localhostProfile"), "apparmor profile in annotation and field must match"))
				}
			}
		}
		return true
	})

	return allErrs
}

// podSpecHasContainer 判断 PodSpec 中是否存在具有指定名称的容器。
func podSpecHasContainer(spec *core.PodSpec, containerName string) bool {
	var hasContainer bool
	podshelper.VisitContainersWithPath(spec, field.NewPath("spec"), func(c *core.Container, _ *field.Path) bool {
		if c.Name == containerName {
			hasContainer = true
			return false
		}
		return true
	})
	return hasContainer
}

const (
	// sysctl 段的正则，段之间用点连接构成 sysctl 名称
	SysctlSegmentFmt string = "[a-z0-9]([-_a-z0-9]*[a-z0-9])?"

	// 允许包含斜杠的 sysctl 名称正则
	SysctlContainSlashFmt string = "(" + SysctlSegmentFmt + "[\\./])*" + SysctlSegmentFmt

	// sysctl 名称的最大长度
	SysctlMaxLength int = 253
)

var sysctlContainSlashRegexp = regexp.MustCompile("^" + SysctlContainSlashFmt + "$")

// IsValidSysctlName 校验给定字符串是否为合法 sysctl 名称（匹配 SysctlContainSlashFmt）。
func IsValidSysctlName(name string) bool {
	if len(name) > SysctlMaxLength {
		return false
	}
	return sysctlContainSlashRegexp.MatchString(name)
}

// validateSysctls 校验 Pod 的 sysctls 列表：名称合法且均在安全白名单内。
func validateSysctls(securityContext *core.PodSecurityContext, spec *core.PodSpec, fldPath *field.Path, opts PodValidationOptions) field.ErrorList {
	allErrs := field.ErrorList{}
	names := make(map[string]struct{})
	for i, s := range securityContext.Sysctls {
		if len(s.Name) == 0 {
			allErrs = append(allErrs, field.Required(fldPath.Index(i).Child("name"), ""))
		} else if !IsValidSysctlName(s.Name) {
			allErrs = append(allErrs, field.Invalid(fldPath.Index(i).Child("name"), s.Name, fmt.Sprintf("must have at most %d characters and match regex %s", SysctlMaxLength, sysctlContainSlashRegexp)))
		} else if _, ok := names[s.Name]; ok {
			allErrs = append(allErrs, field.Duplicate(fldPath.Index(i).Child("name"), s.Name))
		}
		if !opts.AllowNamespacedSysctlsForHostNetAndHostIPC {
			err := ValidateHostSysctl(s.Name, spec, fldPath.Index(i).Child("name"))
			if err != nil {
				allErrs = append(allErrs, err)
			}
		}
		names[s.Name] = struct{}{}
	}
	return allErrs
}

// ValidateHostSysctl 若命名空间的 sysctl 被应用于与宿主共享相应命名空间的 Pod，则返回错误。
func ValidateHostSysctl(sysctl string, spec *core.PodSpec, fldPath *field.Path) *field.Error {
	ns, _, _ := utilsysctl.GetNamespace(sysctl)
	switch {
	case spec.HostNetwork && ns == utilsysctl.NetNamespace:
		return field.Invalid(fldPath, sysctl, "may not be specified when 'hostNetwork' is true")
	case spec.HostIPC && ns == utilsysctl.IPCNamespace:
		return field.Invalid(fldPath, sysctl, "may not be specified when 'hostIPC' is true")
	}
	return nil
}

var validSELinuxChangePolicies = sets.New(core.SELinuxChangePolicyRecursive, core.SELinuxChangePolicyMountOption)

// validateSELinuxChangePolicy 校验 SELinuxChangePolicy 是否为合法的取值。
func validateSELinuxChangePolicy(seLinuxChangePolicy *core.PodSELinuxChangePolicy, fldPath *field.Path, opts PodValidationOptions) field.ErrorList {
	if seLinuxChangePolicy == nil {
		return nil
	}

	allErrs := field.ErrorList{}

	if opts.AllowOnlyRecursiveSELinuxChangePolicy {
		if *seLinuxChangePolicy != core.SELinuxChangePolicyRecursive {
			allErrs = append(allErrs, field.NotSupported(fldPath, *seLinuxChangePolicy, []core.PodSELinuxChangePolicy{core.SELinuxChangePolicyRecursive}))
		}
	} else {
		// 允许任意合法的 SELinuxChangePolicy 取值。
		if !validSELinuxChangePolicies.Has(*seLinuxChangePolicy) {
			allErrs = append(allErrs, field.NotSupported(fldPath, *seLinuxChangePolicy, sets.List(validSELinuxChangePolicies)))
		}
	}
	return allErrs
}

// validatePodSpecSecurityContext 校验 PodSpec 的 SecurityContext，无论定义在 Pod 还是内嵌模板（如 Deployment 的 pod template）中。
func validatePodSpecSecurityContext(securityContext *core.PodSecurityContext, spec *core.PodSpec, specPath, fldPath *field.Path, opts PodValidationOptions) field.ErrorList {
	allErrs := field.ErrorList{}

	if securityContext != nil {
		if securityContext.FSGroup != nil {
			for _, msg := range validation.IsValidGroupID(*securityContext.FSGroup) {
				allErrs = append(allErrs, field.Invalid(fldPath.Child("fsGroup"), *(securityContext.FSGroup), msg))
			}
		}
		if securityContext.RunAsUser != nil {
			for _, msg := range validation.IsValidUserID(*securityContext.RunAsUser) {
				allErrs = append(allErrs, field.Invalid(fldPath.Child("runAsUser"), *(securityContext.RunAsUser), msg))
			}
		}
		if securityContext.RunAsGroup != nil {
			for _, msg := range validation.IsValidGroupID(*securityContext.RunAsGroup) {
				allErrs = append(allErrs, field.Invalid(fldPath.Child("runAsGroup"), *(securityContext.RunAsGroup), msg))
			}
		}
		for g, gid := range securityContext.SupplementalGroups {
			for _, msg := range validation.IsValidGroupID(gid) {
				allErrs = append(allErrs, field.Invalid(fldPath.Child("supplementalGroups").Index(g), gid, msg))
			}
		}
		if len(securityContext.Sysctls) != 0 {
			allErrs = append(allErrs, validateSysctls(securityContext, spec, fldPath.Child("sysctls"), opts)...)
		}

		if securityContext.FSGroupChangePolicy != nil {
			allErrs = append(allErrs, validateFSGroupChangePolicy(securityContext.FSGroupChangePolicy, fldPath.Child("fsGroupChangePolicy"))...)
		}

		allErrs = append(allErrs, validateSeccompProfileField(securityContext.SeccompProfile, fldPath.Child("seccompProfile"))...)
		allErrs = append(allErrs, validateWindowsSecurityContextOptions(securityContext.WindowsOptions, fldPath.Child("windowsOptions"))...)
		allErrs = append(allErrs, ValidateAppArmorProfileField(securityContext.AppArmorProfile, fldPath.Child("appArmorProfile"))...)

		if securityContext.SupplementalGroupsPolicy != nil {
			allErrs = append(allErrs, validateSupplementalGroupsPolicy(securityContext.SupplementalGroupsPolicy, fldPath.Child("supplementalGroupsPolicy"))...)
		}

		if securityContext.SELinuxChangePolicy != nil {
			allErrs = append(allErrs, validateSELinuxChangePolicy(securityContext.SELinuxChangePolicy, fldPath.Child("seLinuxChangePolicy"), opts)...)
		}
	}

	return allErrs
}

// validateShareProcessNamespace 校验 shareProcessNamespace 与其他设置（如 hostPID）是否冲突。
func validateShareProcessNamespace(spec *core.PodSpec, fldPath *field.Path) (allErrs field.ErrorList) {
	if spec.ShareProcessNamespace != nil && spec.HostPID && *spec.ShareProcessNamespace {
		allErrs = append(allErrs, field.Invalid(fldPath.Child("shareProcessNamespace"), *spec.ShareProcessNamespace, "ShareProcessNamespace and HostPID cannot both be enabled"))
	}
	return allErrs
}

// ValidateContainerUpdates 校验容器更新是否合法（如镜像不可变等约束）。
func ValidateContainerUpdates(newContainers, oldContainers []core.Container, fldPath *field.Path) (allErrs field.ErrorList, stop bool) {
	allErrs = field.ErrorList{}
	if len(newContainers) != len(oldContainers) {
		// TODO: 在具备 strategic merge diff 后，精确定位导致该非法错误的容器
		allErrs = append(allErrs, field.Forbidden(fldPath, "pod updates may not add or remove containers"))
		return allErrs, true
	}

	// 校验更新后的容器镜像
	for i, ctr := range newContainers {
		if len(ctr.Image) == 0 {
			allErrs = append(allErrs, field.Required(fldPath.Index(i).Child("image"), ""))
		}
		// 该函数仅被 ValidatePodUpdate 调用，因此检查首尾空白是安全的。
		if len(strings.TrimSpace(ctr.Image)) != len(ctr.Image) {
			allErrs = append(allErrs, field.Invalid(fldPath.Index(i).Child("image"), ctr.Image, "must not have leading or trailing whitespace"))
		}
	}
	return allErrs, false
}

// ValidatePodCreate 在 Pod 初始创建场景下校验其合法性。
func ValidatePodCreate(pod *core.Pod, opts PodValidationOptions) field.ErrorList {
	allErrs := validatePodMetadataAndSpec(pod, opts)

	fldPath := field.NewPath("spec")
	// EphemeralContainers 只能在更新时通过 ephemeralcontainers 子资源设置
	if len(pod.Spec.EphemeralContainers) > 0 {
		allErrs = append(allErrs, field.Forbidden(fldPath.Child("ephemeralContainers"), "cannot be set on create"))
	}
	// 若仍存在 scheduling gates，则 Pod 不能被分配到节点。
	if pod.Spec.NodeName != "" && len(pod.Spec.SchedulingGates) != 0 {
		allErrs = append(allErrs, field.Forbidden(fldPath.Child("nodeName"), "cannot be set until all schedulingGates have been cleared"))
	}
	allErrs = append(allErrs, validateSeccompAnnotationsAndFields(pod.ObjectMeta, &pod.Spec, fldPath)...)
	allErrs = append(allErrs, validateAppArmorAnnotationsAndFieldsMatchOnCreate(pod.ObjectMeta, &pod.Spec, fldPath)...)

	return allErrs
}

// validateSeccompAnnotationsAndFields 遍历所有容器，确保 seccompProfile 字段与旧 seccomp 注解同时存在时二者一致。
func validateSeccompAnnotationsAndFields(objectMeta metav1.ObjectMeta, podSpec *core.PodSpec, specPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}

	if podSpec.SecurityContext != nil && podSpec.SecurityContext.SeccompProfile != nil {
		// 若 seccomp 注解与字段同时指定，二者取值必须一致。
		if annotation, found := objectMeta.Annotations[v1.SeccompPodAnnotationKey]; found {
			seccompPath := specPath.Child("securityContext").Child("seccompProfile")
			err := validateSeccompAnnotationsAndFieldsMatch(annotation, podSpec.SecurityContext.SeccompProfile, seccompPath)
			if err != nil {
				allErrs = append(allErrs, err)
			}
		}
	}

	podshelper.VisitContainersWithPath(podSpec, specPath, func(c *core.Container, cFldPath *field.Path) bool {
		var field *core.SeccompProfile
		if c.SecurityContext != nil {
			field = c.SecurityContext.SeccompProfile
		}

		if field == nil {
			return true
		}

		key := v1.SeccompContainerAnnotationKeyPrefix + c.Name
		if annotation, found := objectMeta.Annotations[key]; found {
			seccompPath := cFldPath.Child("securityContext").Child("seccompProfile")
			err := validateSeccompAnnotationsAndFieldsMatch(annotation, field, seccompPath)
			if err != nil {
				allErrs = append(allErrs, err)
			}
		}
		return true
	})

	return allErrs
}

// validateSeccompAnnotationsAndFieldsMatch 校验 seccomp 注解与字段是否匹配，兼容旧对象并存场景。
func validateSeccompAnnotationsAndFieldsMatch(annotationValue string, seccompField *core.SeccompProfile, fldPath *field.Path) *field.Error {
	if seccompField == nil {
		return nil
	}

	switch seccompField.Type {
	case core.SeccompProfileTypeUnconfined:
		if annotationValue != v1.SeccompProfileNameUnconfined {
			return field.Forbidden(fldPath.Child("type"), "seccomp type in annotation and field must match")
		}

	case core.SeccompProfileTypeRuntimeDefault:
		if annotationValue != v1.SeccompProfileRuntimeDefault && annotationValue != v1.DeprecatedSeccompProfileDockerDefault {
			return field.Forbidden(fldPath.Child("type"), "seccomp type in annotation and field must match")
		}

	case core.SeccompProfileTypeLocalhost:
		if !strings.HasPrefix(annotationValue, v1.SeccompLocalhostProfileNamePrefix) {
			return field.Forbidden(fldPath.Child("type"), "seccomp type in annotation and field must match")
		} else if seccompField.LocalhostProfile == nil || strings.TrimPrefix(annotationValue, v1.SeccompLocalhostProfileNamePrefix) != *seccompField.LocalhostProfile {
			return field.Forbidden(fldPath.Child("localhostProfile"), "seccomp profile in annotation and field must match")
		}
	}

	return nil
}

var updatablePodSpecFields = []string{
	"`spec.containers[*].image`",
	"`spec.initContainers[*].image`",
	"`spec.activeDeadlineSeconds`",
	"`spec.tolerations` (only additions to existing tolerations)",
	"`spec.terminationGracePeriodSeconds` (allow it to be set to 1 if it was previously negative)",
}

// ValidatePodUpdate 校验终端用户对 Pod 的更新是否合法；newPod 中会填入不可变字段的旧值。
func ValidatePodUpdate(newPod, oldPod *core.Pod, opts PodValidationOptions) field.ErrorList {
	fldPath := field.NewPath("metadata")
	allErrs := ValidateObjectMetaUpdate(&newPod.ObjectMeta, &oldPod.ObjectMeta, fldPath)
	allErrs = append(allErrs, validatePodMetadataAndSpec(newPod, opts)...)
	allErrs = append(allErrs, ValidatePodSpecificAnnotationUpdates(newPod, oldPod.Annotations, fldPath.Child("annotations"), opts)...)
	specPath := field.NewPath("spec")

	// 校验可更新的字段：
	// 1.  spec.containers[*].image
	// 2.  spec.initContainers[*].image
	// 3.  spec.activeDeadlineSeconds
	// 4.  spec.terminationGracePeriodSeconds
	// 5.  spec.schedulingGates

	containerErrs, stop := ValidateContainerUpdates(newPod.Spec.Containers, oldPod.Spec.Containers, specPath.Child("containers"))
	allErrs = append(allErrs, containerErrs...)
	if stop {
		return allErrs
	}
	containerErrs, stop = ValidateContainerUpdates(newPod.Spec.InitContainers, oldPod.Spec.InitContainers, specPath.Child("initContainers"))
	allErrs = append(allErrs, containerErrs...)
	if stop {
		return allErrs
	}

	// 校验更新后的 spec.activeDeadlineSeconds，允许两种更新：
	// 1.  从 nil 更新为正数
	// 2.  从正数更新为更小的非负值
	if newPod.Spec.ActiveDeadlineSeconds != nil {
		newActiveDeadlineSeconds := *newPod.Spec.ActiveDeadlineSeconds
		if newActiveDeadlineSeconds < 0 || newActiveDeadlineSeconds > math.MaxInt32 {
			allErrs = append(allErrs, field.Invalid(specPath.Child("activeDeadlineSeconds"), newActiveDeadlineSeconds, validation.InclusiveRangeError(0, math.MaxInt32)))
			return allErrs
		}
		if oldPod.Spec.ActiveDeadlineSeconds != nil {
			oldActiveDeadlineSeconds := *oldPod.Spec.ActiveDeadlineSeconds
			if oldActiveDeadlineSeconds < newActiveDeadlineSeconds {
				allErrs = append(allErrs, field.Invalid(specPath.Child("activeDeadlineSeconds"), newActiveDeadlineSeconds, "must be less than or equal to previous value"))
				return allErrs
			}
		}
	} else if oldPod.Spec.ActiveDeadlineSeconds != nil {
		allErrs = append(allErrs, field.Invalid(specPath.Child("activeDeadlineSeconds"), newPod.Spec.ActiveDeadlineSeconds, "must not update from a positive integer to nil value"))
	}

	// tolerations 只允许新增。
	allErrs = append(allErrs, validateOnlyAddedTolerations(newPod.Spec.Tolerations, oldPod.Spec.Tolerations, specPath.Child("tolerations"), opts)...)

	// schedulingGates 只允许删除。
	allErrs = append(allErrs, validateOnlyDeletedSchedulingGates(newPod.Spec.SchedulingGates, oldPod.Spec.SchedulingGates, specPath.Child("schedulingGates"))...)

	// 最后检查 pod spec 是否相等。若二者相等，直接返回当前已收集的错误即可，
	// 省去深拷贝的开销。
	if apiequality.Semantic.DeepEqual(newPod.Spec, oldPod.Spec) {
		return allErrs
	}

	// 通过在进行深比较前改写可更新字段来处理它们。
	mungedPodSpec := *newPod.Spec.DeepCopy()
	// 改写 spec.containers[*].image
	var newContainers []core.Container
	for ix, container := range mungedPodSpec.Containers {
		container.Image = oldPod.Spec.Containers[ix].Image // +k8s:verify-mutation:reason=clone
		newContainers = append(newContainers, container)
	}
	mungedPodSpec.Containers = newContainers
	// 改写 spec.initContainers[*].image
	var newInitContainers []core.Container
	for ix, container := range mungedPodSpec.InitContainers {
		container.Image = oldPod.Spec.InitContainers[ix].Image // +k8s:verify-mutation:reason=clone
		newInitContainers = append(newInitContainers, container)
	}
	mungedPodSpec.InitContainers = newInitContainers
	// 改写 spec.activeDeadlineSeconds
	mungedPodSpec.ActiveDeadlineSeconds = nil
	if oldPod.Spec.ActiveDeadlineSeconds != nil {
		activeDeadlineSeconds := *oldPod.Spec.ActiveDeadlineSeconds
		mungedPodSpec.ActiveDeadlineSeconds = &activeDeadlineSeconds
	}
	// 改写 spec.schedulingGates
	mungedPodSpec.SchedulingGates = oldPod.Spec.SchedulingGates // +k8s:verify-mutation:reason=clone
	// tolerations 在深拷贝之前已检查过，因此也一并改写
	mungedPodSpec.Tolerations = oldPod.Spec.Tolerations // +k8s:verify-mutation:reason=clone

	// 放宽对不可变字段的校验：若其原值为负数，允许被设置为 1。
	if oldPod.Spec.TerminationGracePeriodSeconds != nil && *oldPod.Spec.TerminationGracePeriodSeconds < 0 &&
		mungedPodSpec.TerminationGracePeriodSeconds != nil && *mungedPodSpec.TerminationGracePeriodSeconds == 1 {
		mungedPodSpec.TerminationGracePeriodSeconds = oldPod.Spec.TerminationGracePeriodSeconds // +k8s:verify-mutation:reason=clone
	}

	// 处理被 gating 的 Pod 特有的校验。
	podIsGated := len(oldPod.Spec.SchedulingGates) > 0
	if podIsGated {
		// 对于 gated Pod，允许向 spec.nodeSelector 新增条目（不允许删除或修改）。
		if !apiequality.Semantic.DeepEqual(mungedPodSpec.NodeSelector, oldPod.Spec.NodeSelector) {
			allErrs = append(allErrs, validateNodeSelectorMutation(specPath.Child("nodeSelector"), mungedPodSpec.NodeSelector, oldPod.Spec.NodeSelector)...)
			mungedPodSpec.NodeSelector = oldPod.Spec.NodeSelector // +k8s:verify-mutation:reason=clone
		}

		// 校验 node affinity 的变更。
		var oldNodeAffinity *core.NodeAffinity
		if oldPod.Spec.Affinity != nil {
			oldNodeAffinity = oldPod.Spec.Affinity.NodeAffinity // +k8s:verify-mutation:reason=clone
		}

		var mungedNodeAffinity *core.NodeAffinity
		if mungedPodSpec.Affinity != nil {
			mungedNodeAffinity = mungedPodSpec.Affinity.NodeAffinity // +k8s:verify-mutation:reason=clone
		}

		if !apiequality.Semantic.DeepEqual(oldNodeAffinity, mungedNodeAffinity) {
			allErrs = append(allErrs, validateNodeAffinityMutation(specPath.Child("affinity").Child("nodeAffinity"), mungedNodeAffinity, oldNodeAffinity)...)
			switch {
			case mungedPodSpec.Affinity == nil && oldNodeAffinity == nil:
				// 二者实际等同为 nil，无需改写
			case mungedPodSpec.Affinity == nil && oldNodeAffinity != nil:
				mungedPodSpec.Affinity = &core.Affinity{NodeAffinity: oldNodeAffinity} // +k8s:verify-mutation:reason=clone
			case mungedPodSpec.Affinity != nil && oldPod.Spec.Affinity == nil &&
				mungedPodSpec.Affinity.PodAntiAffinity == nil && mungedPodSpec.Affinity.PodAffinity == nil:
				// 此处确保除 NodeAffinity 外没有其他字段被修改。若确实如此
				// 且旧 Pod 的 affinity 为 nil，则把 mungedPodSpec 的 affinity 置为 nil。
				mungedPodSpec.Affinity = nil // +k8s:verify-mutation:reason=clone
			default:
				// node affinity 正在更新且旧 Pod 的 Affinity 不为 nil。
				// 把 mungedPodSpec 的 node affinity 设为旧 Pod 的 node affinity。
				mungedPodSpec.Affinity.NodeAffinity = oldNodeAffinity // +k8s:verify-mutation:reason=clone
			}
		}

		// 注意：与 NodeAffinity、NodeSelector 不同，PodAffinity/PodAntiAffinity 不能改为可变，
		// 因为存在 matchLabelKeys/mismatchLabelKeys 特性。
		// 这些特性在 Pod 创建时会自动为 PodAffinity/PodAntiAffinity 的 labelSelector 生成
		// matchExpressions。若使其可变，需要确认如何处理与校验 matchLabelKeys 等问题，
		// 以及若 fieldManager/A 设置了 matchExpressions、fieldManager/B 之后又设置
		// matchLabelKeys 会怎样（可能引发难以理解的冲突等）。
	}

	if !apiequality.Semantic.DeepEqual(mungedPodSpec, oldPod.Spec) {
		// 这个 diff 并不完美，但远比“我不会告诉你差异是什么”要好。
		// TODO: 在具备 strategic merge diff 后，精确定位导致该非法错误的字段
		specDiff := diff.Diff(oldPod.Spec, mungedPodSpec)
		errs := field.Forbidden(specPath, fmt.Sprintf("pod updates may not change fields other than %s\n%v", strings.Join(updatablePodSpecFields, ","), specDiff))
		allErrs = append(allErrs, errs)
	}
	return allErrs
}

// ValidateContainerStateTransition 校验容器状态转换是否合法，禁止非法的状态跃迁。
func ValidateContainerStateTransition(newStatuses, oldStatuses []core.ContainerStatus, fldPath *field.Path, podSpec core.PodSpec, opts PodValidationOptions) field.ErrorList {
	allErrs := field.ErrorList{}
	if opts.AllowContainerRestartPolicyRules {
		// 把 *core.PodSpec 转换为 *v1.PodSpec，以满足后续
		// AllContainersCouldRestart 与 ContainerShouldRestart 方法的调用要求，
		// 二者均需要 *v1.PodSpec 对象。
		v1PodSpec := &v1.PodSpec{}
		err := corev1.Convert_core_PodSpec_To_v1_PodSpec(&podSpec, v1PodSpec, nil)
		if err != nil {
			allErrs = append(allErrs, field.InternalError(fldPath, fmt.Errorf("invalid %q: %v", fldPath, err.Error())))
		}
		if opts.AllowRestartAllContainers {
			// 只要有任一容器的 restart rule action 为 RestartAllContainers，就意味着
			// 所有容器都可能被重启，与其自身的 restart policy 无关。因此
			// 所有容器都可以从已终止状态转为非终止状态。
			if podutil.AllContainersCouldRestart(v1PodSpec) {
				return allErrs
			}
		}
		for i, oldStatus := range oldStatuses {
			// 跳过所有未终止的容器
			if oldStatus.State.Terminated == nil {
				continue
			}
			for _, newStatus := range newStatuses {
				if newStatus.Name == oldStatus.Name && newStatus.State.Terminated == nil {
					allowed := false
					for _, c := range v1PodSpec.Containers {
						if c.Name == oldStatus.Name {
							allowed = podutil.ContainerShouldRestart(c, *v1PodSpec, oldStatus.State.Terminated.ExitCode)
							break
						}
					}
					if !allowed {
						allErrs = append(allErrs, field.Forbidden(fldPath.Index(i).Child("state"), "may not be transitioned to non-terminated state"))
					}
					break
				}
			}
		}
		return allErrs
	}
	restartPolicy := podSpec.RestartPolicy
	// 若策略为始终重启，容器允许离开终止状态
	if restartPolicy == core.RestartPolicyAlways {
		return allErrs
	}
	for i, oldStatus := range oldStatuses {
		// 跳过所有未终止的容器
		if oldStatus.State.Terminated == nil {
			continue
		}
		// 跳过失败但允许重启的容器
		if oldStatus.State.Terminated.ExitCode != 0 && restartPolicy == core.RestartPolicyOnFailure {
			continue
		}
		for _, newStatus := range newStatuses {
			if oldStatus.Name == newStatus.Name && newStatus.State.Terminated == nil {
				allErrs = append(allErrs, field.Forbidden(fldPath.Index(i).Child("state"), "may not be transitioned to non-terminated state"))
			}
		}
	}
	return allErrs
}

// ValidateInitContainerStateTransition 校验 init 容器状态转换是否合法，禁止非法的状态跃迁。
func ValidateInitContainerStateTransition(newStatuses, oldStatuses []core.ContainerStatus, fldpath *field.Path, podSpec core.PodSpec, opts PodValidationOptions) field.ErrorList {
	allErrs := field.ErrorList{}
	if opts.AllowContainerRestartPolicyRules {
		// 把 *core.PodSpec 转换为 *v1.PodSpec，以满足后续
		// AllContainersCouldRestart 与 ContainerShouldRestart 方法的调用要求，
		// 二者均需要 *v1.PodSpec 对象。
		v1PodSpec := &v1.PodSpec{}
		err := corev1.Convert_core_PodSpec_To_v1_PodSpec(&podSpec, v1PodSpec, nil)
		if err != nil {
			allErrs = append(allErrs, field.InternalError(fldpath, fmt.Errorf("invalid %q: %v", fldpath, err.Error())))
		}
		if opts.AllowRestartAllContainers {
			// 只要有任一容器的 restart rule action 为 RestartAllContainers，就意味着
			// 所有容器都可能被重启，与其自身的 restart policy 无关。因此
			// 所有容器都可以从已终止状态转为非终止状态。
			if podutil.AllContainersCouldRestart(v1PodSpec) {
				return allErrs
			}
		}
		for i, oldStatus := range oldStatuses {
			// 跳过所有未终止的容器
			if oldStatus.State.Terminated == nil {
				continue
			}
			for _, newStatus := range newStatuses {
				if newStatus.Name == oldStatus.Name && newStatus.State.Terminated == nil {
					allowed := false
					for _, c := range v1PodSpec.InitContainers {
						if c.Name == oldStatus.Name {
							allowed = podutil.ContainerShouldRestart(c, *v1PodSpec, oldStatus.State.Terminated.ExitCode)
							break
						}
					}
					if !allowed {
						allErrs = append(allErrs, field.Forbidden(fldpath.Index(i).Child("state"), "may not be transitioned to non-terminated state"))
					}
					break
				}
			}
		}
		return allErrs
	}
	// 若策略为始终重启，容器允许离开终止状态
	if podSpec.RestartPolicy == core.RestartPolicyAlways {
		return allErrs
	}
	for i, oldStatus := range oldStatuses {
		// 跳过所有未终止的容器
		if oldStatus.State.Terminated == nil {
			continue
		}
		// 跳过失败但允许重启的容器
		if oldStatus.State.Terminated.ExitCode != 0 && podSpec.RestartPolicy == core.RestartPolicyOnFailure {
			continue
		}

		// 跳过允许重启的可重启 init 容器
		isRestartableInitCtr := false
		for _, c := range podSpec.InitContainers {
			if oldStatus.Name == c.Name {
				if isRestartableInitContainer(&c) {
					isRestartableInitCtr = true
				}
				break
			}
		}
		if isRestartableInitCtr {
			continue
		}

		for _, newStatus := range newStatuses {
			if oldStatus.Name == newStatus.Name && newStatus.State.Terminated == nil {
				allErrs = append(allErrs, field.Forbidden(fldpath.Index(i).Child("state"), "may not be transitioned to non-terminated state"))
			}
		}
	}
	return allErrs
}

// ValidateEphemeralContainerStateTransition 校验临时容器不得从 terminated 状态转换回非 terminated 状态。
func ValidateEphemeralContainerStateTransition(newStatuses, oldStatuses []core.ContainerStatus, fldpath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	for i, oldStatus := range oldStatuses {
		// 跳过所有未终止的容器
		if oldStatus.State.Terminated == nil {
			continue
		}
		for _, newStatus := range newStatuses {
			if oldStatus.Name == newStatus.Name && newStatus.State.Terminated == nil {
				allErrs = append(allErrs, field.Forbidden(fldpath.Index(i).Child("state"), "may not be transitioned to non-terminated state"))
			}
		}
	}
	return allErrs
}

// ValidatePodStatusUpdate 校验 Pod 状态更新中不存在正常操作下不应发生的变化。
func ValidatePodStatusUpdate(newPod, oldPod *core.Pod, opts PodValidationOptions) field.ErrorList {
	fldPath := field.NewPath("metadata")
	allErrs := ValidateObjectMetaUpdate(&newPod.ObjectMeta, &oldPod.ObjectMeta, fldPath)
	allErrs = append(allErrs, ValidatePodSpecificAnnotationUpdates(newPod, oldPod.Annotations, fldPath.Child("annotations"), opts)...)

	fldPath = field.NewPath("status")
	allErrs = append(allErrs, validatePodConditions(newPod.Status.Conditions, fldPath.Child("conditions"))...)

	if newPod.Spec.NodeName != oldPod.Spec.NodeName {
		allErrs = append(allErrs, field.Forbidden(fldPath.Child("nodeName"), "may not be changed directly"))
	}

	if newPod.Status.NominatedNodeName != oldPod.Status.NominatedNodeName && len(newPod.Status.NominatedNodeName) > 0 {
		for _, msg := range ValidateNodeName(newPod.Status.NominatedNodeName, false) {
			allErrs = append(allErrs, field.Invalid(fldPath.Child("nominatedNodeName"), newPod.Status.NominatedNodeName, msg))
		}
	}

	// 阻止在已绑定节点的 Pod 上设置 NominatedNodeName
	if utilfeature.DefaultFeatureGate.Enabled(features.ClearingNominatedNodeNameAfterBinding) &&
		oldPod.Spec.NodeName != "" &&
		newPod.Status.NominatedNodeName != "" &&
		newPod.Status.NominatedNodeName != oldPod.Status.NominatedNodeName {
		allErrs = append(allErrs, field.Forbidden(fldPath.Child("nominatedNodeName"),
			"may not be set on pods that are already bound to a node"))
	}

	if newPod.Status.ObservedGeneration < 0 {
		allErrs = append(allErrs, field.Invalid(fldPath.Child("observedGeneration"), newPod.Status.ObservedGeneration, "must be a non-negative integer"))
	}

	// Pod QoS 不可变
	allErrs = append(allErrs, ValidateImmutableField(newPod.Status.QOSClass, oldPod.Status.QOSClass, fldPath.Child("qosClass"))...)

	// 注意：这里不检查 ContainerStatuses、InitContainerStatuses 与
	// EphemeralContainerStatuses 中是否存在重复容器名，或存在 pod spec 未定义容器的
	// 状态。改动此处可能引入破坏性变更，因此这些字段的消费方
	// 必须能处理异常数据。Kubelet 绝不会上报此类状态。
	//
	// 若 Pod 不应重启，需确保状态更新不会把
	// 任何已终止容器转为非终止状态。
	allErrs = append(allErrs, ValidateContainerStateTransition(newPod.Status.ContainerStatuses, oldPod.Status.ContainerStatuses, fldPath.Child("containerStatuses"), oldPod.Spec, opts)...)
	allErrs = append(allErrs, ValidateInitContainerStateTransition(newPod.Status.InitContainerStatuses, oldPod.Status.InitContainerStatuses, fldPath.Child("initContainerStatuses"), oldPod.Spec, opts)...)
	allErrs = append(allErrs, ValidateEphemeralContainerStateTransition(newPod.Status.EphemeralContainerStatuses, oldPod.Status.EphemeralContainerStatuses, fldPath.Child("ephemeralContainerStatuses"))...)
	allErrs = append(allErrs, validatePodResourceClaimStatuses(newPod.Status.ResourceClaimStatuses, newPod.Spec.ResourceClaims, fldPath.Child("resourceClaimStatuses"))...)
	allErrs = append(allErrs, validatePodExtendedResourceClaimStatus(newPod.Status.ExtendedResourceClaimStatus, &newPod.Spec, fldPath.Child("extendedResourceClaimStatus"))...)
	allErrs = append(allErrs, validateNodeAllocatableResourceClaimStatus(newPod.Status, &newPod.Spec, fldPath.Child("nodeAllocatableResourceClaimStatuses"))...)

	if len(newPod.Status.VolumeHealth) > 0 {
		allErrs = append(allErrs, validatePodVolumeHealth(newPod.Status.VolumeHealth, &newPod.Spec, fldPath.Child("volumeHealth"))...)
	}

	if newIPErrs := validatePodIPs(newPod, oldPod); len(newIPErrs) > 0 {
		allErrs = append(allErrs, newIPErrs...)
	}

	if newIPErrs := validateHostIPs(newPod, oldPod); len(newIPErrs) > 0 {
		allErrs = append(allErrs, newIPErrs...)
	}

	allErrs = append(allErrs, validateContainerStatusUsers(newPod.Status.ContainerStatuses, fldPath.Child("containerStatuses"), newPod.Spec.OS)...)
	allErrs = append(allErrs, validateContainerStatusUsers(newPod.Status.InitContainerStatuses, fldPath.Child("initContainerStatuses"), newPod.Spec.OS)...)
	allErrs = append(allErrs, validateContainerStatusUsers(newPod.Status.EphemeralContainerStatuses, fldPath.Child("ephemeralContainerStatuses"), newPod.Spec.OS)...)

	allErrs = append(allErrs, validateContainerStatusAllocatedResourcesStatus(newPod.Status.ContainerStatuses, fldPath.Child("containerStatuses"), newPod.Spec.Containers, &newPod.Status)...)
	allErrs = append(allErrs, validateContainerStatusAllocatedResourcesStatus(newPod.Status.InitContainerStatuses, fldPath.Child("initContainerStatuses"), newPod.Spec.InitContainers, &newPod.Status)...)
	// 临时容器不允许分配资源
	allErrs = append(allErrs, validateContainerStatusNoAllocatedResourcesStatus(newPod.Status.EphemeralContainerStatuses, fldPath.Child("ephemeralContainerStatuses"))...)

	if opts.AllowImageVolumeWithDigest {
		for i, containerStatus := range newPod.Status.ContainerStatuses {
			for j, volumeMountStatus := range containerStatus.VolumeMounts {
				validatedField := fldPath.Child("containerStatuses").Index(i).Child("volumeMounts").Index(j).Child("volumeStatus")
				allErrs = append(allErrs, validateVolumeStatus(volumeMountStatus.VolumeStatus, validatedField)...)
			}
		}
		for i, containerStatus := range newPod.Status.InitContainerStatuses {
			for j, volumeMountStatus := range containerStatus.VolumeMounts {
				validatedField := fldPath.Child("initContainerStatuses").Index(i).Child("volumeMounts").Index(j).Child("volumeStatus")
				allErrs = append(allErrs, validateVolumeStatus(volumeMountStatus.VolumeStatus, validatedField)...)
			}
		}
		for i, containerStatus := range newPod.Status.EphemeralContainerStatuses {
			for j, volumeMountStatus := range containerStatus.VolumeMounts {
				validatedField := fldPath.Child("ephemeralContainerStatuses").Index(i).Child("volumeMounts").Index(j).Child("volumeStatus")
				allErrs = append(allErrs, validateVolumeStatus(volumeMountStatus.VolumeStatus, validatedField)...)
			}
		}
	}

	return allErrs
}

// validatePodConditions 校验自定义 Pod conditions 是否合法，且 observedGeneration 为非负整数。
func validatePodConditions(conditions []core.PodCondition, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	systemConditions := sets.New(
		core.PodScheduled,
		core.PodReady,
		core.PodInitialized)
	for i, condition := range conditions {
		if condition.ObservedGeneration < 0 {
			allErrs = append(allErrs, field.Invalid(fldPath.Index(i).Child("observedGeneration"), condition.ObservedGeneration, "must be a non-negative integer"))
		}
		if systemConditions.Has(condition.Type) {
			continue
		}
		allErrs = append(allErrs, ValidateQualifiedName(string(condition.Type), fldPath.Index(i).Child("Type"))...)
	}
	return allErrs
}

// validatePodResourceClaimStatuses 校验 Pod 状态中的 ResourceClaimStatuses 列表。
func validatePodResourceClaimStatuses(statuses []core.PodResourceClaimStatus, podClaims []core.PodResourceClaim, fldPath *field.Path) field.ErrorList {
	var allErrs field.ErrorList

	claimNames := sets.New[string]()
	for i, status := range statuses {
		idxPath := fldPath.Index(i)
		// 无需检查名称内容：能匹配到某个条目即合法，
		// 否则直接在此拒绝。
		if !havePodClaim(podClaims, status.Name) {
			allErrs = append(allErrs, field.Invalid(idxPath.Child("name"), status.Name, "must match the name of an entry in `spec.resourceClaims`"))
		}
		if claimNames.Has(status.Name) {
			allErrs = append(allErrs, field.Duplicate(idxPath.Child("name"), status.Name))
		} else {
			claimNames.Insert(status.Name)
		}
		if status.ResourceClaimName != nil {
			for _, detail := range ValidateResourceClaimName(*status.ResourceClaimName, false) {
				allErrs = append(allErrs, field.Invalid(idxPath.Child("resourceClaimName"), status.ResourceClaimName, detail))
			}
		}
	}

	return allErrs
}

// validateNodeAllocatableResourceClaimStatus 校验 Pod 状态中的 NodeAllocatableResourceClaimStatus 列表。
func validateNodeAllocatableResourceClaimStatus(podStatus core.PodStatus, podSpec *core.PodSpec, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}

	if len(podStatus.NodeAllocatableResourceClaimStatuses) == 0 {
		return allErrs
	}

	for i, nodeAllocatableStatus := range podStatus.NodeAllocatableResourceClaimStatuses {
		statusFldPath := fldPath.Index(i)
		if nodeAllocatableStatus.ResourceClaimName == "" {
			continue
		}

		// 先检查 podSpec，看该 ResourceClaim 是否被直接引用；
		// 若没有，再检查 podStatus，看它是否由模板生成。
		found := false
		for _, claimRef := range podSpec.ResourceClaims {
			if (claimRef.ResourceClaimName != nil) && (*claimRef.ResourceClaimName == nodeAllocatableStatus.ResourceClaimName) {
				found = true
				break
			}
		}
		if !found {
			for _, claimRef := range podStatus.ResourceClaimStatuses {
				if (claimRef.ResourceClaimName != nil) && (*claimRef.ResourceClaimName == nodeAllocatableStatus.ResourceClaimName) {
					found = true
					break
				}
			}
		}

		if !found {
			allErrs = append(allErrs, field.Invalid(statusFldPath.Child("resourceClaimName"), nodeAllocatableStatus.ResourceClaimName, "no mapping found in pod reference"))
		}

		if len(nodeAllocatableStatus.Mapping) > 0 {
			allErrs = append(allErrs, validateNodeAllocatableMappedResources(nodeAllocatableStatus.Mapping, statusFldPath.Child("mapping"))...)
		}
		if len(nodeAllocatableStatus.Overhead) > 0 {
			allErrs = append(allErrs, validateNodeAllocatableOverheadResources(nodeAllocatableStatus.Overhead, statusFldPath.Child("overhead"))...)
		}
	}

	return allErrs
}

// validateNodeAllocatableMappedResources 校验节点可分配资源的映射列表是否合法。
func validateNodeAllocatableMappedResources(mapping []core.NodeAllocatableMappedResources, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	for i, item := range mapping {
		itemPath := fldPath.Index(i)
		if !helper.IsNodeAllocatableResourceName(item.Name) {
			allErrs = append(allErrs, field.Invalid(itemPath.Child("name"), item.Name, "must be a node allocatable resource name"))
		}
		if item.Quantity != nil && item.Quantity.Cmp(resource.Quantity{}) < 0 {
			allErrs = append(allErrs, field.Invalid(itemPath.Child("quantity"), item.Quantity.String(), "must be non-negative"))
		}
	}
	return allErrs
}

// validateNodeAllocatableOverheadResources 校验节点可分配开销资源列表是否合法。
func validateNodeAllocatableOverheadResources(overhead []core.NodeAllocatableOverheadResources, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	for i, item := range overhead {
		itemPath := fldPath.Index(i)
		if !helper.IsNodeAllocatableResourceName(item.Name) {
			allErrs = append(allErrs, field.Invalid(itemPath.Child("name"), item.Name, "must be a node allocatable resource name"))
		}
		if item.PerPod == nil && item.PerContainer == nil {
			allErrs = append(allErrs, field.Invalid(itemPath, "", "at least one of perPod or perContainer must be set"))
		} else {
			if item.PerPod != nil && item.PerPod.Cmp(resource.Quantity{}) < 0 {
				allErrs = append(allErrs, field.Invalid(itemPath.Child("perPod"), item.PerPod.String(), "must be non-negative"))
			}
			if item.PerContainer != nil && item.PerContainer.Cmp(resource.Quantity{}) < 0 {
				allErrs = append(allErrs, field.Invalid(itemPath.Child("perContainer"), item.PerContainer.String(), "must be non-negative"))
			}
		}
	}
	return allErrs
}

// validatePodExtendedResourceClaimStatus 校验 Pod 状态中的 ExtendedResourceClaimStatus 是否合法。
func validatePodExtendedResourceClaimStatus(status *core.PodExtendedResourceClaimStatus, spec *core.PodSpec, fldPath *field.Path) field.ErrorList {
	if status == nil {
		return nil
	}

	containers := make(map[string]*core.Container)
	for _, c := range spec.InitContainers {
		containers[c.Name] = &c
	}
	for _, c := range spec.Containers {
		containers[c.Name] = &c
	}

	var allErrs field.ErrorList
	rmFldPath := fldPath.Child("requestMappings")
	if len(status.RequestMappings) == 0 {
		allErrs = append(allErrs, field.Required(rmFldPath, "at least one request mapping is required"))
	}
	type key struct {
		container string
		resource  string
		request   string
	}
	seen := map[key]struct{}{}
	for i, rm := range status.RequestMappings {
		idxPath := rmFldPath.Index(i)
		c, ok := containers[rm.ContainerName]
		if ok {
			if _, ok := c.Resources.Requests[core.ResourceName(rm.ResourceName)]; !ok {
				allErrs = append(allErrs, field.Invalid(idxPath.Child("resourceName"), rm.ResourceName, "must match the extended resource name of an entry in spec.initContainers.resources.requests or spec.containers.resources.requests"))
			}
		} else {
			allErrs = append(allErrs, field.Invalid(idxPath.Child("containerName"), rm.ContainerName, "must match the name of an entry in spec.initContainers.name or spec.containers.name"))
		}
		allErrs = append(allErrs, ValidateDNS1123Label(rm.RequestName, fldPath.Child("requestName"))...)
		k := key{container: rm.ContainerName, resource: rm.ResourceName, request: rm.RequestName}
		if _, ok := seen[k]; ok {
			allErrs = append(allErrs, field.Duplicate(idxPath.Child("containerName"), rm.ContainerName))
			allErrs = append(allErrs, field.Duplicate(idxPath.Child("resourceName"), rm.ResourceName))
			allErrs = append(allErrs, field.Duplicate(idxPath.Child("requestName"), rm.RequestName))
		}
		seen[k] = struct{}{}
	}
	if len(status.ResourceClaimName) == 0 {
		allErrs = append(allErrs, field.Required(fldPath.Child("resourceClaimName"), ""))
	} else {
		for _, detail := range ValidateResourceClaimName(status.ResourceClaimName, false) {
			allErrs = append(allErrs, field.Invalid(fldPath.Child("resourceClaimName"), status.ResourceClaimName, detail))
		}
	}
	return allErrs
}

// havePodClaim 判断 Pod 是否定义了指定名称的 ResourceClaim。
func havePodClaim(podClaims []core.PodResourceClaim, name string) bool {
	for _, podClaim := range podClaims {
		if podClaim.Name == name {
			return true
		}
	}
	return false
}

// ValidatePodEphemeralContainersUpdate 校验用户对 EphemeralContainers 的更新是否合法；newPod 与 oldPod 应仅在该字段上不同。
func ValidatePodEphemeralContainersUpdate(newPod, oldPod *core.Pod, opts PodValidationOptions) field.ErrorList {
	// 第 1 部分：校验 newPod 的 spec 与 metadata 更新
	fldPath := field.NewPath("metadata")
	allErrs := ValidateObjectMetaUpdate(&newPod.ObjectMeta, &oldPod.ObjectMeta, fldPath)
	allErrs = append(allErrs, validatePodMetadataAndSpec(newPod, opts)...)
	allErrs = append(allErrs, ValidatePodSpecificAnnotationUpdates(newPod, oldPod.Annotations, fldPath.Child("annotations"), opts)...)

	// 静态 Pod 不支持临时容器 #113935
	if _, ok := oldPod.Annotations[core.MirrorPodAnnotationKey]; ok {
		return field.ErrorList{field.Forbidden(field.NewPath(""), "static pods do not support ephemeral containers")}
	}

	// 第 2 部分：校验 oldPod.Spec.EphemeralContainers 与
	// newPod.Spec.EphemeralContainers 之间的变更是否允许。
	//
	// 已有的 EphemeralContainers 不得修改。patch 不保证顺序，
	// 因此逐个单独检查。
	newContainerIndex := make(map[string]*core.EphemeralContainer)
	specPath := field.NewPath("spec").Child("ephemeralContainers")
	for i := range newPod.Spec.EphemeralContainers {
		newContainerIndex[newPod.Spec.EphemeralContainers[i].Name] = &newPod.Spec.EphemeralContainers[i]
	}
	for _, old := range oldPod.Spec.EphemeralContainers {
		if new, ok := newContainerIndex[old.Name]; !ok {
			allErrs = append(allErrs, field.Forbidden(specPath, fmt.Sprintf("existing ephemeral containers %q may not be removed\n", old.Name)))
		} else if !apiequality.Semantic.DeepEqual(old, *new) {
			specDiff := diff.Diff(old, *new)
			allErrs = append(allErrs, field.Forbidden(specPath, fmt.Sprintf("existing ephemeral containers %q may not be changed\n%v", old.Name, specDiff)))
		}
	}

	return allErrs
}

// ValidatePodResize 校验 Pod 原地调整容器资源的更新是否合法；newPod 与 oldPod 应仅在容器资源与 resizePolicy 上不同。
func ValidatePodResize(newPod, oldPod *core.Pod, opts PodValidationOptions) field.ErrorList {
	// 第 1 部分：校验 newPod 的 spec 与 metadata 更新
	fldPath := field.NewPath("metadata")
	allErrs := ValidateObjectMetaUpdate(&newPod.ObjectMeta, &oldPod.ObjectMeta, fldPath)
	allErrs = append(allErrs, validatePodMetadataAndSpec(newPod, opts)...)

	// 静态 Pod 不能调整资源。
	if _, ok := oldPod.Annotations[core.MirrorPodAnnotationKey]; ok {
		return field.ErrorList{field.Forbidden(field.NewPath(""), "static pods cannot be resized")}
	}

	// 不支持 windows Pod。
	if oldPod.Spec.OS != nil && oldPod.Spec.OS.Name == core.Windows {
		return field.ErrorList{field.Forbidden(field.NewPath(""), "windows pods cannot be resized")}
	}

	specPath := field.NewPath("spec")
	if qos.GetPodQOS(oldPod) != qos.ComputePodQOS(newPod) {
		allErrs = append(allErrs, field.Invalid(specPath, newPod.Status.QOSClass, "Pod QOS Class may not change as a result of resizing"))
	}

	if !isPodResizeRequestSupported(*oldPod) {
		allErrs = append(allErrs, field.Forbidden(specPath, "Pod running on node without support for resize"))
	}

	// 第 2 部分：校验 oldPod.Spec.Resources 与 newPod.Spec.Resources
	// 中 Pod 级资源的变更是否允许。

	isPodLevelResourcesSet := func(pod *core.Pod) bool {
		return pod.Spec.Resources != nil &&
			(len(pod.Spec.Resources.Requests)+len(pod.Spec.Resources.Limits) > 0)
	}

	newPodSpecCopy := *newPod.Spec.DeepCopy()

	if isPodLevelResourcesSet(oldPod) || isPodLevelResourcesSet(newPod) {
		// 启用了 InPlacePodLevelResourcesVerticalScaling 特性门控，
		// 才能对带 Pod 级资源的 Pod 调整资源。
		if !opts.InPlacePodLevelResourcesVerticalScalingEnabled {
			allErrs = append(allErrs, field.Forbidden(field.NewPath(""), "resize of pods with pod-level resources is forbidden when InPlacePodLevelResourcesVerticalScalingEnabled feature gate is disabled"))
			return allErrs
		}

		// newPodSpecCopy 以引用方式传入，以便中途修改
		// newPodSpecCopy.Resources，
		// 这是某个中间校验步骤所必需的。
		allErrs = append(allErrs, validatePodLevelResourcesResize(newPod, oldPod, &newPodSpecCopy, specPath, opts)...)
	}

	// 第 3 部分：校验 oldPod.Spec.Containers[].Resources 与
	// newPod.Spec.Containers[].Resources 之间的变更是否允许；同时校验 oldPod.Spec.InitContainers[].Resources 与
	// newPod.Spec.InitContainers[].Resources 之间的变更是否允许。

	// 后续的校验假定容器顺序不变，
	// 因此仅在该假设成立时继续。
	containerOrderErrs := validatePodResizeContainerOrdering(newPod, oldPod, specPath)
	allErrs = append(allErrs, containerOrderErrs...)
	if containerOrderErrs != nil {
		return allErrs
	}

	// 调整资源时不允许移除 resource requests/limits。
	for ix, ctr := range oldPod.Spec.InitContainers {
		if !utilfeature.DefaultFeatureGate.Enabled(features.InPlacePodVerticalScalingInitContainers) && !isRestartableInitContainer(&ctr) {
			continue
		}
		allErrs = append(allErrs, validateContainerResize(
			&newPod.Spec.InitContainers[ix].Resources,
			&oldPod.Spec.InitContainers[ix].Resources,
			newPod.Spec.InitContainers[ix].ResizePolicy,
			specPath.Child("initContainers").Index(ix).Child("resources"))...)
	}
	for ix := range oldPod.Spec.Containers {
		allErrs = append(allErrs, validateContainerResize(
			&newPod.Spec.Containers[ix].Resources,
			&oldPod.Spec.Containers[ix].Resources,
			newPod.Spec.Containers[ix].ResizePolicy,
			specPath.Child("containers").Index(ix).Child("resources"))...)
	}

	// 确保普通容器只有 CPU 与内存资源可变。
	var newContainers []core.Container
	for ix, container := range newPodSpecCopy.Containers {
		dropCPUMemoryResourcesFromContainer(&container, &oldPod.Spec.Containers[ix])
		if !apiequality.Semantic.DeepEqual(container, oldPod.Spec.Containers[ix]) {
			// 这通常意味着用户修改了普通容器 CPU 与内存之外的资源。
			errs := field.Forbidden(specPath, "only cpu and memory resources are mutable")
			allErrs = append(allErrs, errs)
		}
		newContainers = append(newContainers, container)
	}
	newPodSpecCopy.Containers = newContainers

	// 确保可重启 init 容器只有 CPU 与内存资源可变。
	var newInitContainers []core.Container
	for ix, container := range newPodSpecCopy.InitContainers {
		isRestartable := isRestartableInitContainer(&container)
		canResize := isRestartable || utilfeature.DefaultFeatureGate.Enabled(features.InPlacePodVerticalScalingInitContainers)
		modifiedContainer := !apiequality.Semantic.DeepEqual(container, oldPod.Spec.InitContainers[ix])

		if canResize {
			dropCPUMemoryResourcesFromContainer(&container, &oldPod.Spec.InitContainers[ix])
			if !apiequality.Semantic.DeepEqual(container, oldPod.Spec.InitContainers[ix]) {
				// 这通常意味着用户修改了 sidecar 容器 CPU 与内存之外的资源。
				errs := field.Forbidden(specPath.Child("initContainers").Index(ix), "only cpu and memory resources for init or sidecar containers are mutable")
				allErrs = append(allErrs, errs)
			}
			if modifiedContainer && !isRestartable {
				for _, resizePolicy := range container.ResizePolicy {
					if resizePolicy.RestartPolicy == core.RestartContainer {
						// TODO: 此校验可以在 1.40 之后移除，
						// 因为 https://github.com/kubernetes/kubernetes/pull/137458 已禁止
						// 为非 sidecar 的 init 容器设置 RestartContainer resize policy。
						errs := field.Forbidden(specPath.Child("initContainers").Index(ix), "non-sidecar init containers with a resize policy of RestartContainer cannot be resized")
						allErrs = append(allErrs, errs)
					}
				}
			}
		} else if modifiedContainer { // 修改了不可调整资源的 init 容器
			// 这通常意味着用户修改了非 sidecar init 容器的资源。
			errs := field.Forbidden(specPath, "resources for non-sidecar init containers are immutable")
			allErrs = append(allErrs, errs)
		}
		newInitContainers = append(newInitContainers, container)
	}
	newPodSpecCopy.InitContainers = newInitContainers

	// 第 5 部分：校验 oldPod.Spec.Volumes 与 newPod.Spec.Volumes
	// 之间的变更是否允许。调整资源时仅内存型 emptyDir 卷的 sizeLimit 可变。
	if opts.InPlacePodVerticalScalingMemoryBackedVolumesEnabled {
		if len(newPod.Spec.Volumes) != len(oldPod.Spec.Volumes) {
			allErrs = append(allErrs, field.Forbidden(field.NewPath("spec").Child("volumes"), "volumes may not be added or removed on resize"))
		} else {
			for i, newVol := range newPod.Spec.Volumes {
				oldVol := oldPod.Spec.Volumes[i]
				volPath := field.NewPath("spec").Child("volumes").Index(i)
				if newVol.Name != oldVol.Name {
					allErrs = append(allErrs, field.Forbidden(volPath.Child("name"), "volumes may not be renamed or reordered on resize"))
					continue
				}
				newVolToCompare := &newVol
				if newVol.EmptyDir != nil && oldVol.EmptyDir != nil {
					newVolCopy := newVol.DeepCopy()
					newVolCopy.EmptyDir.SizeLimit = oldVol.EmptyDir.SizeLimit // +k8s:verify-mutation:reason=clone
					newVolToCompare = newVolCopy
				}
				if !apiequality.Semantic.DeepEqual(newVolToCompare, &oldVol) {
					allErrs = append(allErrs, field.Forbidden(volPath, "only sizeLimit of memory-backed emptyDir volumes is mutable on resize"))
					continue
				}
				// 若是 emptyDir，检查可变约束
				if newVol.EmptyDir != nil && oldVol.EmptyDir != nil {
					hasOldLimit := oldVol.EmptyDir.SizeLimit != nil && !oldVol.EmptyDir.SizeLimit.IsZero()
					hasNewLimit := newVol.EmptyDir.SizeLimit != nil && !newVol.EmptyDir.SizeLimit.IsZero()
					if hasOldLimit != hasNewLimit {
						allErrs = append(allErrs, field.Forbidden(volPath.Child("emptyDir").Child("sizeLimit"), "adding or removing sizeLimit on an existing volume is not allowed"))
					} else if oldVol.EmptyDir.SizeLimit != nil && newVol.EmptyDir.SizeLimit != nil {
						if oldVol.EmptyDir.SizeLimit.Cmp(*newVol.EmptyDir.SizeLimit) != 0 {
							if newVol.EmptyDir.Medium != core.StorageMediumMemory {
								allErrs = append(allErrs, field.Forbidden(volPath.Child("emptyDir").Child("sizeLimit"), "sizeLimit is only mutable for memory-backed emptyDir volumes"))
							}
						}
					}
				}
			}
		}
		newPodSpecCopy.Volumes = oldPod.Spec.Volumes // +k8s:verify-mutation:reason=clone
	} else if !apiequality.Semantic.DeepEqual(newPod.Spec.Volumes, oldPod.Spec.Volumes) {
		allErrs = append(allErrs, field.Forbidden(field.NewPath("spec").Child("volumes"), "volumes are immutable on resize when InPlacePodVerticalScalingMemoryBackedVolumes feature gate is disabled"))
	}

	if len(allErrs) > 0 {
		return allErrs
	}

	if !apiequality.Semantic.DeepEqual(newPodSpecCopy, oldPod.Spec) {
		// 这通常意味着用户修改了 CPU 与内存之外的资源。
		errs := field.Forbidden(specPath, "only cpu and memory resources are mutable")
		allErrs = append(allErrs, errs)
	}
	return allErrs
}

// validatePodResizeContainerOrdering 校验 resize 请求中容器顺序未变：不允许添加、删除、重排或重命名容器。
func validatePodResizeContainerOrdering(newPod, oldPod *core.Pod, specPath *field.Path) field.ErrorList {
	var allErrs field.ErrorList
	if len(newPod.Spec.Containers) != len(oldPod.Spec.Containers) {
		allErrs = append(allErrs, field.Forbidden(specPath.Child("containers"), "containers may not be added or removed on resize"))
	} else {
		for i, oldCtr := range oldPod.Spec.Containers {
			if newPod.Spec.Containers[i].Name != oldCtr.Name {
				allErrs = append(allErrs, field.Forbidden(specPath.Child("containers").Index(i).Child("name"), "containers may not be renamed or reordered on resize"))
			}
		}
	}
	if len(newPod.Spec.InitContainers) != len(oldPod.Spec.InitContainers) {
		allErrs = append(allErrs, field.Forbidden(specPath.Child("initContainers"), "initContainers may not be added or removed on resize"))
	} else {
		for i, oldCtr := range oldPod.Spec.InitContainers {
			if newPod.Spec.InitContainers[i].Name != oldCtr.Name {
				allErrs = append(allErrs, field.Forbidden(specPath.Child("initContainers").Index(i).Child("name"), "initContainers may not be renamed or reordered on resize"))
			}
		}
	}
	return allErrs
}

// validatePodLevelResourcesResize 校验 Pod 级资源请求/限制的原地调整；会修改作为 newPod.Spec 深拷贝传入的 podSpecToMutate。
func validatePodLevelResourcesResize(newPod, oldPod *core.Pod, podSpecToMutate *core.PodSpec, specPath *field.Path, opts PodValidationOptions) field.ErrorList {
	var allErrs field.ErrorList

	if oldPod.Spec.Resources != nil && newPod.Spec.Resources == nil {
		return append(allErrs, field.Forbidden(specPath.Child("resources"), "pod-level resources cannot be removed"))
	}

	if oldPod.Spec.Resources != nil {
		if resourcesRemoved(newPod.Spec.Resources.Requests, oldPod.Spec.Resources.Requests) {
			allErrs = append(allErrs, field.Forbidden(specPath.Child("resources").Child("requests"), "pod-level resource requests cannot be removed"))
		}

		if resourcesRemoved(newPod.Spec.Resources.Limits, oldPod.Spec.Resources.Limits) {
			allErrs = append(allErrs, field.Forbidden(specPath.Child("resources").Child("limits"), "pod-level resource limits cannot be removed"))
		}

	}

	podSpecToMutate.Resources = dropCPUMemoryResourceRequirementsUpdates(podSpecToMutate.Resources, oldPod.Spec.Resources)

	if !apiequality.Semantic.DeepEqual(podSpecToMutate.Resources, oldPod.Spec.Resources) {
		// 这通常意味着用户修改了 Pod 级 CPU 与内存之外的资源。
		errs := field.Forbidden(specPath, "only cpu and memory resources are mutable at pod-level")
		allErrs = append(allErrs, errs)
	}

	if utilfeature.DefaultFeatureGate.Enabled(features.DRANodeAllocatableResources) && len(newPod.Status.NodeAllocatableResourceClaimStatuses) > 0 {
		v1Pod := &v1.Pod{}
		if err := corev1.Convert_core_Pod_To_v1_Pod(newPod, v1Pod, nil); err != nil {
			allErrs = append(allErrs, field.InternalError(specPath, fmt.Errorf("failed to convert pod for DRA validation: %w", err)))
		} else {
			// TODO(pravk03): 探索优化方案，避免在 validatePodResourceConsistency 与
			// validatePodLevelResourcesCoverDRA 中重复聚合容器资源。
			if ok, msg := validatePodLevelResourcesCoverDRA(v1Pod); !ok {
				allErrs = append(allErrs, field.Invalid(specPath.Child("resources"), newPod.Spec.Resources, msg))
			}
		}
	}

	return allErrs
}

// dropCPUMemoryUpdates 在调整校验中忽略 CPU/内存变化，仅比较其他资源。
func dropCPUMemoryUpdates(resourceList, oldResourceList core.ResourceList) core.ResourceList {
	var mungedResourceList core.ResourceList
	if resourceList == nil {
		if oldResourceList == nil {
			return nil
		}
		mungedResourceList = make(core.ResourceList)
	} else {
		mungedResourceList = resourceList.DeepCopy()
	}
	delete(mungedResourceList, core.ResourceCPU)
	delete(mungedResourceList, core.ResourceMemory)
	if cpu, found := oldResourceList[core.ResourceCPU]; found {
		mungedResourceList[core.ResourceCPU] = cpu
	}
	if mem, found := oldResourceList[core.ResourceMemory]; found {
		mungedResourceList[core.ResourceMemory] = mem
	}
	return mungedResourceList
}

// dropCPUMemoryResourcesFromContainer 从容器中删除 CPU 与内存资源，并在存在时从旧 Pod 容器中拷贝回来。
// TODO(ndixita): 重构以复用 dropCPUMemoryResourceRequirementsUpdates。
func dropCPUMemoryResourcesFromContainer(container *core.Container, oldPodSpecContainer *core.Container) {
	lim := dropCPUMemoryUpdates(container.Resources.Limits, oldPodSpecContainer.Resources.Limits)
	req := dropCPUMemoryUpdates(container.Resources.Requests, oldPodSpecContainer.Resources.Requests)
	// Pod 调整资源期间 resource claims 不可变，必须保留原始配置。
	container.Resources = core.ResourceRequirements{Limits: lim, Requests: req, Claims: container.Resources.Claims}
}

// dropCPUMemoryResourceRequirementsUpdates 从资源字段中删除 CPU 与内存资源，并在存在时从旧 Pod 规格中拷贝。
func dropCPUMemoryResourceRequirementsUpdates(resources *core.ResourceRequirements, oldPodResources *core.ResourceRequirements) *core.ResourceRequirements {
	if resources == nil {
		return resources
	}

	var oldReqs, oldLims core.ResourceList
	if oldPodResources != nil && oldPodResources.Requests != nil {
		oldReqs = oldPodResources.Requests // +k8s:verify-mutation:reason=clone'
	}

	if oldPodResources != nil && oldPodResources.Limits != nil {
		oldLims = oldPodResources.Limits // +k8s:verify-mutation:reason=clone'
	}

	resources.Requests = dropCPUMemoryUpdates(resources.Requests, oldReqs)
	resources.Limits = dropCPUMemoryUpdates(resources.Limits, oldLims)

	// 满足以下两个条件时，把整个 Resources 块置为 nil：
	// 1. 旧 PodSpec Resources 中没有任何资源
	// 2. 掩码后的资源只剩空的 Requests/Limits
	//    （即此前的 CPU/内存默认值已被丢弃）且没有设置不可变的 Claims。
	// 这确保空的 Resources 块（掩码后通常只含
	// 空的 CPU/内存）能被安全置为 nil，除非存在 Claims。
	if oldPodResources == nil {
		if len(resources.Requests) == 0 && len(resources.Limits) == 0 && len(resources.Claims) == 0 {
			resources = nil
		}
	}
	return resources
}

// validatePodLevelResourcesCoverDRA 校验 Pod 级资源是否覆盖 DRA（动态资源分配）相关声明。
func validatePodLevelResourcesCoverDRA(pod *v1.Pod) (bool, string) {
	if pod.Spec.Resources == nil {
		return true, ""
	}

	if pod.Spec.Resources.Requests != nil {
		opts := resourcehelper.PodResourcesOptions{
			SkipPodLevelResources:                    true,
			UseDRANodeAllocatableResourceClaimStatus: true,
		}
		requestWithoutPodLevel := resourcehelper.AggregateContainerRequests(pod, opts)

		for resName, podLevelReq := range pod.Spec.Resources.Requests {
			if !resourcehelper.IsSupportedPodLevelResource(resName) {
				continue
			}
			val, ok := requestWithoutPodLevel[resName]
			if !ok {
				continue
			}
			if val.Cmp(podLevelReq) > 0 {
				return false, fmt.Sprintf("pod level request for %s is insufficient to cover the aggregated container and node-allocatable DRA requests", resName)
			}
		}
	}

	if pod.Spec.Resources.Limits != nil {
		opts := resourcehelper.PodResourcesOptions{
			SkipPodLevelResources:                    true,
			UseDRANodeAllocatableResourceClaimStatus: true,
		}
		limitsWithoutPodLevel := resourcehelper.AggregateContainerLimits(pod, opts)

		// Pod 级大页 limits 必须始终大于或等于容器级大页 limits 与
		// DRA limits 之和
		for resourceName, ctrLims := range limitsWithoutPodLevel {
			if !helper.IsHugePageResourceName(core.ResourceName(resourceName)) {
				continue
			}

			podLevelResLimit, hasLimit := pod.Spec.Resources.Limits[resourceName]
			if !hasLimit {
				continue
			}

			if ctrLims.Cmp(podLevelResLimit) > 0 {
				return false, fmt.Sprintf("pod level limit for %s is insufficient to cover the aggregated container and node-allocatable DRA limits", resourceName)
			}
		}

		// 单个容器 limits + DRA 开销必须 <= Pod 级 limits。
		containerDRAAllocations := make(map[string]v1.ResourceList, len(pod.Spec.Containers))
		for _, ctr := range pod.Spec.Containers {
			containerDRAAllocations[ctr.Name] = resourcehelper.GetContainerDRAAllocations(pod, ctr.Name)
		}

		for _, ctr := range pod.Spec.Containers {
			for resourceName, ctrLimit := range ctr.Resources.Limits {
				if helper.IsHugePageResourceName(core.ResourceName(resourceName)) {
					continue
				}

				// 若 Pod 级未设置该资源的 limit 则跳过。
				podLevelResLimit, exists := pod.Spec.Resources.Limits[resourceName]
				if !exists {
					continue
				}

				draResAllocation := containerDRAAllocations[ctr.Name][resourceName]
				effectiveLimit := ctrLimit.DeepCopy()
				effectiveLimit.Add(draResAllocation)

				if effectiveLimit.Cmp(podLevelResLimit) > 0 {
					return false, fmt.Sprintf("pod level limit for %s is insufficient to cover the limit and DRA overhead for container %s", resourceName, ctr.Name)
				}
			}
		}
	}

	return true, ""
}

// isPodResizeRequestSupported 检查 Pod 所在节点是否启用了 InPlacePodVerticalScaling，以判断是否支持原地调整。
func isPodResizeRequestSupported(pod core.Pod) bool {
	// TODO: 在 InPlacePodVerticalScaling GA+3 个版本后移除本段。
	// 这段代码按 KEP 描述处理版本偏差（version skew）。
	// 为处理版本偏差，仅当 Pod 已有 Pod.Status.ContainerStatuses[i].Resources
	// 时才允许更新该 Pod 的 Resources。也就是说，
	// apiserver 只会允许更新运行在启用了
	// InPlacePodVerticalScaling 特性门控的节点上的 Pod。
	for _, c := range pod.Status.ContainerStatuses {
		if c.State.Running != nil {
			return c.Resources != nil
		}
	}
	// 没有运行中的容器。此时无法判断节点是否支持 resize，因此假定支持。
	return true
}

// validateContainerResize 校验 resize 请求中容器资源需求的变化；newRequirements 与 oldRequirements 必须非空。
func validateContainerResize(newRequirements, oldRequirements *core.ResourceRequirements, resizePolicies []core.ContainerResizePolicy, fldPath *field.Path) field.ErrorList {
	var allErrs field.ErrorList

	// 不支持移除资源需求。
	if resourcesRemoved(newRequirements.Requests, oldRequirements.Requests) {
		allErrs = append(allErrs, field.Forbidden(fldPath.Child("requests"), "resource requests cannot be removed"))
	}
	if resourcesRemoved(newRequirements.Limits, oldRequirements.Limits) {
		allErrs = append(allErrs, field.Forbidden(fldPath.Child("limits"), "resource limits cannot be removed"))
	}

	// TODO(tallclair): 把可调整资源的检查移到此处。

	return allErrs
}

// resourcesRemoved 判断新资源需求是否删除了旧需求中已有的资源项。
func resourcesRemoved(resourceList, oldResourceList core.ResourceList) bool {
	if len(oldResourceList) > len(resourceList) {
		return true
	}
	for name := range oldResourceList {
		if _, ok := resourceList[name]; !ok {
			return true
		}
	}

	return false
}

// ValidatePodBinding 校验 Pod 绑定对象中的必填字段是否合法。
func ValidatePodBinding(binding *core.Binding) field.ErrorList {
	allErrs := field.ErrorList{}

	if len(binding.Target.Kind) != 0 && binding.Target.Kind != "Node" {
		// TODO: 校验按版本区分后，这里会变得更复杂。
		allErrs = append(allErrs, field.NotSupported(field.NewPath("target", "kind"), binding.Target.Kind, []string{"Node", "<empty>"}))
	}
	if len(binding.Target.Name) == 0 {
		// TODO: 校验按版本区分后，这里会变得更复杂。
		allErrs = append(allErrs, field.Required(field.NewPath("target", "name"), ""))
	} else if len(binding.Target.Kind) == 0 || binding.Target.Kind == "Node" {
		// 绑定到 Node（或 Kind 为空，默认即 Node）时校验节点名格式
		for _, msg := range ValidateNodeName(binding.Target.Name, false) {
			allErrs = append(allErrs, field.Invalid(field.NewPath("target", "name"), binding.Target.Name, msg))
		}
	}

	allErrs = append(allErrs, apimachineryvalidation.ValidateAnnotations(binding.Annotations, field.NewPath("metadata", "annotations"))...)
	allErrs = append(allErrs, unversionedvalidation.ValidateLabels(binding.Labels, field.NewPath("metadata", "labels"))...)

	return allErrs
}

// ValidatePodTemplate 校验 Pod 模板中的必填字段是否设置。
func ValidatePodTemplate(pod *core.PodTemplate, opts PodValidationOptions) field.ErrorList {
	allErrs := ValidateObjectMeta(&pod.ObjectMeta, true, ValidatePodName, field.NewPath("metadata"))
	allErrs = append(allErrs, ValidatePodTemplateSpec(&pod.Template, field.NewPath("template"), opts)...)
	return allErrs
}

// ValidatePodTemplateUpdate 校验终端用户对 Pod 模板的更新是否合法；newPod 中会填入不可变字段的旧值。
func ValidatePodTemplateUpdate(newPod, oldPod *core.PodTemplate, opts PodValidationOptions) field.ErrorList {
	allErrs := ValidateObjectMetaUpdate(&newPod.ObjectMeta, &oldPod.ObjectMeta, field.NewPath("metadata"))
	allErrs = append(allErrs, ValidatePodTemplateSpec(&newPod.Template, field.NewPath("template"), opts)...)
	return allErrs
}

var supportedSessionAffinityType = sets.New(core.ServiceAffinityClientIP, core.ServiceAffinityNone)
var supportedServiceType = sets.New(core.ServiceTypeClusterIP, core.ServiceTypeNodePort,
	core.ServiceTypeLoadBalancer, core.ServiceTypeExternalName)

var supportedServiceInternalTrafficPolicy = sets.New(core.ServiceInternalTrafficPolicyCluster, core.ServiceInternalTrafficPolicyLocal)

var supportedServiceIPFamily = sets.New(core.IPv4Protocol, core.IPv6Protocol)
var supportedServiceIPFamilyPolicy = sets.New(
	core.IPFamilyPolicySingleStack,
	core.IPFamilyPolicyPreferDualStack,
	core.IPFamilyPolicyRequireDualStack)

// validateService 校验 Service 的必填字段与注解是否合法。
func validateService(service, oldService *core.Service) field.ErrorList {
	metaPath := field.NewPath("metadata")

	// 此处不校验 ObjectMeta——它由 ValidateServiceCreate/ValidateServiceUpdate
	// 处理，这两个函数会分别调用 ValidateObjectMeta 与 ValidateObjectMetaUpdate。
	var allErrs field.ErrorList

	topologyHintsVal, topologyHintsSet := service.Annotations[core.DeprecatedAnnotationTopologyAwareHints]
	topologyModeVal, topologyModeSet := service.Annotations[core.AnnotationTopologyMode]

	if topologyModeSet && topologyHintsSet && topologyModeVal != topologyHintsVal {
		message := fmt.Sprintf("must match annotations[%s] when both are specified", core.DeprecatedAnnotationTopologyAwareHints)
		allErrs = append(allErrs, field.Invalid(metaPath.Child("annotations").Key(core.AnnotationTopologyMode), topologyModeVal, message))
	}

	specPath := field.NewPath("spec")

	if len(service.Spec.Ports) == 0 && !isHeadlessService(service) && service.Spec.Type != core.ServiceTypeExternalName {
		allErrs = append(allErrs, field.Required(specPath.Child("ports"), ""))
	}
	switch service.Spec.Type {
	case core.ServiceTypeLoadBalancer:
		if isHeadlessService(service) {
			allErrs = append(allErrs, field.Invalid(specPath.Child("clusterIPs").Index(0), service.Spec.ClusterIPs[0], "may not be set to 'None' for LoadBalancer services"))
		}
	case core.ServiceTypeNodePort:
		if isHeadlessService(service) {
			allErrs = append(allErrs, field.Invalid(specPath.Child("clusterIPs").Index(0), service.Spec.ClusterIPs[0], "may not be set to 'None' for NodePort services"))
		}
	case core.ServiceTypeExternalName:
		// 必须满足 len(.spec.ClusterIPs) == 0 // 注：strategy 会依据 ClusterIP 设置 ClusterIPs
		if len(service.Spec.ClusterIPs) > 0 {
			allErrs = append(allErrs, field.Forbidden(specPath.Child("clusterIPs"), "may not be set for ExternalName services"))
		}

		// ipFamilies 与 ipFamilyPolicy 必须为 nil
		if len(service.Spec.IPFamilies) > 0 {
			allErrs = append(allErrs, field.Forbidden(specPath.Child("ipFamilies"), "may not be set for ExternalName services"))
		}
		if service.Spec.IPFamilyPolicy != nil {
			allErrs = append(allErrs, field.Forbidden(specPath.Child("ipFamilyPolicy"), "may not be set for ExternalName services"))
		}

		// 取值（CNAME）可以带结尾点，表示全限定域名
		cname := strings.TrimSuffix(service.Spec.ExternalName, ".")
		if len(cname) > 0 {
			allErrs = append(allErrs, ValidateDNS1123Subdomain(cname, specPath.Child("externalName"))...)
		} else {
			allErrs = append(allErrs, field.Required(specPath.Child("externalName"), ""))
		}
	}

	allPortNames := sets.Set[string]{}
	portsPath := specPath.Child("ports")
	for i := range service.Spec.Ports {
		portPath := portsPath.Index(i)
		allErrs = append(allErrs, validateServicePort(&service.Spec.Ports[i], len(service.Spec.Ports) > 1, isHeadlessService(service), &allPortNames, portPath)...)
	}

	if service.Spec.Selector != nil {
		allErrs = append(allErrs, unversionedvalidation.ValidateLabels(service.Spec.Selector, specPath.Child("selector"))...)
	}

	if len(service.Spec.SessionAffinity) == 0 {
		allErrs = append(allErrs, field.Required(specPath.Child("sessionAffinity"), ""))
	} else if !supportedSessionAffinityType.Has(service.Spec.SessionAffinity) {
		allErrs = append(allErrs, field.NotSupported(specPath.Child("sessionAffinity"), service.Spec.SessionAffinity, sets.List(supportedSessionAffinityType)))
	}

	if service.Spec.SessionAffinity == core.ServiceAffinityClientIP {
		allErrs = append(allErrs, validateClientIPAffinityConfig(service.Spec.SessionAffinityConfig, specPath.Child("sessionAffinityConfig"))...)
	} else if service.Spec.SessionAffinity == core.ServiceAffinityNone {
		if service.Spec.SessionAffinityConfig != nil {
			allErrs = append(allErrs, field.Forbidden(specPath.Child("sessionAffinityConfig"), fmt.Sprintf("must not be set when session affinity is %s", core.ServiceAffinityNone)))
		}
	}

	// dualstack <-> ClusterIPs <-> ipfamilies 三者的一致性
	allErrs = append(allErrs, ValidateServiceClusterIPsRelatedFields(service, oldService)...)

	// 所有新增的 ExternalIPs 必须合法且“非特殊”。（已有的 ExternalIPs
	// 可能按旧规则通过校验，但既然过去允许，现在也不能
	// 拒绝它们。）
	ipPath := specPath.Child("externalIPs")
	var existingExternalIPs []string
	if oldService != nil {
		existingExternalIPs = oldService.Spec.ExternalIPs // +k8s:verify-mutation:reason=clone
	}
	for i, ip := range service.Spec.ExternalIPs {
		idxPath := ipPath.Index(i)
		if errs := IsValidIPForLegacyField(idxPath, ip, existingExternalIPs); len(errs) != 0 {
			allErrs = append(allErrs, errs...)
		} else {
			// 出于历史原因，这里使用 ValidateEndpointIP，
			// 尽管它并不完全是恰当的检查集合。
			allErrs = append(allErrs, ValidateEndpointIP(ip, idxPath)...)
		}
	}

	if len(service.Spec.Type) == 0 {
		allErrs = append(allErrs, field.Required(specPath.Child("type"), ""))
	} else if !supportedServiceType.Has(service.Spec.Type) {
		allErrs = append(allErrs, field.NotSupported(specPath.Child("type"), service.Spec.Type, sets.List(supportedServiceType)))
	}

	if service.Spec.Type == core.ServiceTypeClusterIP {
		portsPath := specPath.Child("ports")
		for i := range service.Spec.Ports {
			portPath := portsPath.Index(i)
			if service.Spec.Ports[i].NodePort != 0 {
				allErrs = append(allErrs, field.Forbidden(portPath.Child("nodePort"), "may not be used when `type` is 'ClusterIP'"))
			}
		}
	}

	// 检查重复的 NodePort，按（protocol,port）二元组比较
	portsPath = specPath.Child("ports")
	nodePorts := make(map[core.ServicePort]bool)
	for i := range service.Spec.Ports {
		port := &service.Spec.Ports[i]
		if port.NodePort == 0 {
			continue
		}
		portPath := portsPath.Index(i)
		var key core.ServicePort
		key.Protocol = port.Protocol
		key.NodePort = port.NodePort
		_, found := nodePorts[key]
		if found {
			allErrs = append(allErrs, field.Duplicate(portPath.Child("nodePort"), port.NodePort))
		}
		nodePorts[key] = true
	}

	// 检查重复的 Ports，按（protocol,port）二元组比较
	portsPath = specPath.Child("ports")
	ports := make(map[core.ServicePort]bool)
	for i, port := range service.Spec.Ports {
		portPath := portsPath.Index(i)
		key := core.ServicePort{Protocol: port.Protocol, Port: port.Port}
		_, found := ports[key]
		if found {
			allErrs = append(allErrs, field.Duplicate(portPath, key))
		}
		ports[key] = true
	}

	// 校验 SourceRanges 字段或注解。已存在的非法 CIDR 取值不
	// 强制修复。注意即便采用更严格的 CIDR 校验，我们仍
	// 允许多余空白，因为它实际上是 API 的一部分。
	if len(service.Spec.LoadBalancerSourceRanges) > 0 {
		fieldPath := specPath.Child("LoadBalancerSourceRanges")

		if service.Spec.Type != core.ServiceTypeLoadBalancer {
			allErrs = append(allErrs, field.Forbidden(fieldPath, "may only be used when `type` is 'LoadBalancer'"))
		}
		var existingSourceRanges []string
		if oldService != nil {
			existingSourceRanges = make([]string, len(oldService.Spec.LoadBalancerSourceRanges))
			for i, value := range oldService.Spec.LoadBalancerSourceRanges {
				existingSourceRanges[i] = strings.TrimSpace(value)
			}
		}
		for idx, value := range service.Spec.LoadBalancerSourceRanges {
			// 注意：由于从注解取值迁移过程中的历史意外，
			// 这些取值允许带空格填充。
			value = strings.TrimSpace(value)
			allErrs = append(allErrs, IsValidCIDRForLegacyField(fieldPath.Index(idx), value, existingSourceRanges)...)
		}
	} else if val, annotationSet := service.Annotations[core.AnnotationLoadBalancerSourceRangesKey]; annotationSet {
		fieldPath := field.NewPath("metadata", "annotations").Key(core.AnnotationLoadBalancerSourceRangesKey)
		if service.Spec.Type != core.ServiceTypeLoadBalancer {
			allErrs = append(allErrs, field.Forbidden(fieldPath, "may only be used when `type` is 'LoadBalancer'"))
		}

		if oldService == nil || oldService.Annotations[core.AnnotationLoadBalancerSourceRangesKey] != val {
			val = strings.TrimSpace(val)
			if val != "" {
				cidrs := strings.Split(val, ",")
				for _, value := range cidrs {
					value = strings.TrimSpace(value)
					allErrs = append(allErrs, IsValidCIDRForLegacyField(fieldPath, value, nil)...)
				}
			}
		}
	}

	if service.Spec.AllocateLoadBalancerNodePorts != nil && service.Spec.Type != core.ServiceTypeLoadBalancer {
		allErrs = append(allErrs, field.Forbidden(specPath.Child("allocateLoadBalancerNodePorts"), "may only be used when `type` is 'LoadBalancer'"))
	}

	if service.Spec.Type == core.ServiceTypeLoadBalancer && service.Spec.AllocateLoadBalancerNodePorts == nil {
		allErrs = append(allErrs, field.Required(field.NewPath("allocateLoadBalancerNodePorts"), ""))
	}

	// 校验 LoadBalancerClass 字段
	allErrs = append(allErrs, validateLoadBalancerClassField(nil, service)...)

	// external traffic policy 相关字段
	allErrs = append(allErrs, validateServiceExternalTrafficPolicy(service)...)

	// internal traffic policy 字段
	allErrs = append(allErrs, validateServiceInternalTrafficFieldsValue(service)...)

	// traffic distribution 字段
	allErrs = append(allErrs, validateServiceTrafficDistribution(service)...)

	return allErrs
}

// validateServicePort 校验 ServicePort：端口范围、协议、名称与 nodePort 的合法性。
func validateServicePort(sp *core.ServicePort, requireName, isHeadlessService bool, allNames *sets.Set[string], fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}

	if requireName && len(sp.Name) == 0 {
		allErrs = append(allErrs, field.Required(fldPath.Child("name"), ""))
	} else if len(sp.Name) != 0 {
		allErrs = append(allErrs, ValidateDNS1123Label(sp.Name, fldPath.Child("name"))...)
		if allNames.Has(sp.Name) {
			allErrs = append(allErrs, field.Duplicate(fldPath.Child("name"), sp.Name))
		} else {
			allNames.Insert(sp.Name)
		}
	}

	for _, msg := range validation.IsValidPortNum(int(sp.Port)) {
		allErrs = append(allErrs, field.Invalid(fldPath.Child("port"), sp.Port, msg))
	}

	if len(sp.Protocol) == 0 {
		allErrs = append(allErrs, field.Required(fldPath.Child("protocol"), ""))
	} else if !supportedPortProtocols.Has(sp.Protocol) {
		allErrs = append(allErrs, field.NotSupported(fldPath.Child("protocol"), sp.Protocol, sets.List(supportedPortProtocols)))
	}

	allErrs = append(allErrs, ValidatePortNumOrName(sp.TargetPort, fldPath.Child("targetPort"))...)

	if sp.AppProtocol != nil {
		allErrs = append(allErrs, ValidateQualifiedName(*sp.AppProtocol, fldPath.Child("appProtocol"))...)
	}

	// 在 v1 API 中，headless service 上的 targetPort 是被容忍的。
	// 有了按版本区分的校验后，可以在新 API 版本上拒绝这种配置，但在此之前，为兼容性必须继续容忍。
	//
	// if isHeadlessService {
	// 	if sp.TargetPort.Type == intstr.String || (sp.TargetPort.Type == intstr.Int && sp.Port != sp.TargetPort.IntValue()) {
	// 		allErrs = append(allErrs, field.Invalid(fldPath.Child("targetPort"), sp.TargetPort, "must be equal to the value of 'port' when clusterIP = None"))
	// 	}
	// }

	return allErrs
}

var validExternalTrafficPolicies = sets.New(core.ServiceExternalTrafficPolicyCluster, core.ServiceExternalTrafficPolicyLocal)

// validateServiceExternalTrafficPolicy 校验 externalTrafficPolicy 的取值及与 Service 类型的兼容性。
func validateServiceExternalTrafficPolicy(service *core.Service) field.ErrorList {
	allErrs := field.ErrorList{}

	fldPath := field.NewPath("spec")

	if !apiservice.ExternallyAccessible(service) {
		if service.Spec.ExternalTrafficPolicy != "" {
			allErrs = append(allErrs, field.Invalid(fldPath.Child("externalTrafficPolicy"), service.Spec.ExternalTrafficPolicy,
				"may only be set for externally-accessible services"))
		}
	} else {
		if service.Spec.ExternalTrafficPolicy == "" {
			allErrs = append(allErrs, field.Required(fldPath.Child("externalTrafficPolicy"), ""))
		} else if !validExternalTrafficPolicies.Has(service.Spec.ExternalTrafficPolicy) {
			allErrs = append(allErrs, field.NotSupported(fldPath.Child("externalTrafficPolicy"),
				service.Spec.ExternalTrafficPolicy, sets.List(validExternalTrafficPolicies)))
		}
	}

	if !apiservice.NeedsHealthCheck(service) {
		if service.Spec.HealthCheckNodePort != 0 {
			allErrs = append(allErrs, field.Invalid(fldPath.Child("healthCheckNodePort"), service.Spec.HealthCheckNodePort,
				"may only be set when `type` is 'LoadBalancer' and `externalTrafficPolicy` is 'Local'"))
		}
	} else {
		if service.Spec.HealthCheckNodePort == 0 {
			allErrs = append(allErrs, field.Required(fldPath.Child("healthCheckNodePort"), ""))
		} else {
			for _, msg := range validation.IsValidPortNum(int(service.Spec.HealthCheckNodePort)) {
				allErrs = append(allErrs, field.Invalid(fldPath.Child("healthCheckNodePort"), service.Spec.HealthCheckNodePort, msg))
			}
		}
	}

	return allErrs
}

// validateServiceExternalTrafficFieldsUpdate 校验与外部流量策略相关字段在更新时是否合法变化。
func validateServiceExternalTrafficFieldsUpdate(before, after *core.Service) field.ErrorList {
	allErrs := field.ErrorList{}

	if apiservice.NeedsHealthCheck(before) && apiservice.NeedsHealthCheck(after) {
		if after.Spec.HealthCheckNodePort != before.Spec.HealthCheckNodePort {
			allErrs = append(allErrs, field.Forbidden(field.NewPath("spec", "healthCheckNodePort"), "field is immutable"))
		}
	}

	return allErrs
}

// validateServiceInternalTrafficFieldsValue 校验 internalTrafficPolicy 相关规格字段的取值是否合法。
func validateServiceInternalTrafficFieldsValue(service *core.Service) field.ErrorList {
	allErrs := field.ErrorList{}

	if service.Spec.InternalTrafficPolicy == nil {
		// 出于历史原因，我们不在其他 Service 类型上禁止 internalTrafficPolicy。
		// 在其进入 beta 之前并未做这项检查，而现在不想让已存储的既有对象失效。
		if service.Spec.Type == core.ServiceTypeNodePort ||
			service.Spec.Type == core.ServiceTypeLoadBalancer || service.Spec.Type == core.ServiceTypeClusterIP {
			allErrs = append(allErrs, field.Required(field.NewPath("spec").Child("internalTrafficPolicy"), ""))
		}
	}

	if service.Spec.InternalTrafficPolicy != nil && !supportedServiceInternalTrafficPolicy.Has(*service.Spec.InternalTrafficPolicy) {
		allErrs = append(allErrs, field.NotSupported(field.NewPath("spec").Child("internalTrafficPolicy"), *service.Spec.InternalTrafficPolicy, sets.List(supportedServiceInternalTrafficPolicy)))
	}

	return allErrs
}

// validateServiceTrafficDistribution 校验 trafficDistribution 字段的取值是否合法。
func validateServiceTrafficDistribution(service *core.Service) field.ErrorList {
	allErrs := field.ErrorList{}

	if service.Spec.TrafficDistribution == nil {
		return allErrs
	}

	var supportedTrafficDistribution []string
	if !utilfeature.DefaultFeatureGate.Enabled(features.PreferSameTrafficDistribution) {
		supportedTrafficDistribution = []string{
			v1.ServiceTrafficDistributionPreferClose,
		}
	} else {
		supportedTrafficDistribution = []string{
			v1.ServiceTrafficDistributionPreferClose,
			v1.ServiceTrafficDistributionPreferSameZone,
			v1.ServiceTrafficDistributionPreferSameNode,
		}
	}

	if !slices.Contains(supportedTrafficDistribution, *service.Spec.TrafficDistribution) {
		allErrs = append(allErrs, field.NotSupported(field.NewPath("spec").Child("trafficDistribution"), *service.Spec.TrafficDistribution, supportedTrafficDistribution))
	}

	return allErrs
}

// ValidateServiceCreate 校验 Service 创建请求的合法性。
func ValidateServiceCreate(service *core.Service) field.ErrorList {
	metaPath := field.NewPath("metadata")

	// KEP-5311：放宽对 Service 名称的校验
	validateServiceNameFunc := ValidateServiceName
	if utilfeature.DefaultFeatureGate.Enabled(features.RelaxedServiceNameValidation) {
		validateServiceNameFunc = apimachineryvalidation.NameIsDNSLabel
	}

	allErrs := ValidateObjectMeta(&service.ObjectMeta, true, validateServiceNameFunc, metaPath)

	return append(allErrs, validateService(service, nil)...)
}

// ValidateServiceUpdate 校验 Service 更新时必填字段是否仍然合法。
func ValidateServiceUpdate(service, oldService *core.Service) field.ErrorList {
	allErrs := ValidateObjectMetaUpdate(&service.ObjectMeta, &oldService.ObjectMeta, field.NewPath("metadata"))

	// 用户可以升级（添加第二个 clusterIP 或 ipFamily）、
	// 可以降级（移除次要的 clusterIP 或 ipFamily），
	// 但*不能*变更主/次 clusterIP 或 ipFamily——
	// 除非是在变更 to/from/ON ExternalName 类型时。

	upgradeDowngradeClusterIPsErrs := validateUpgradeDowngradeClusterIPs(oldService, service)
	allErrs = append(allErrs, upgradeDowngradeClusterIPsErrs...)

	upgradeDowngradeIPFamiliesErrs := validateUpgradeDowngradeIPFamilies(oldService, service)
	allErrs = append(allErrs, upgradeDowngradeIPFamiliesErrs...)

	upgradeDowngradeLoadBalancerClassErrs := validateLoadBalancerClassField(oldService, service)
	allErrs = append(allErrs, upgradeDowngradeLoadBalancerClassErrs...)

	allErrs = append(allErrs, validateServiceExternalTrafficFieldsUpdate(oldService, service)...)

	return append(allErrs, validateService(service, oldService)...)
}

// ValidateServiceStatusUpdate 校验 Service 状态更新时必填字段是否合法。
func ValidateServiceStatusUpdate(service, oldService *core.Service) field.ErrorList {
	allErrs := ValidateObjectMetaUpdate(&service.ObjectMeta, &oldService.ObjectMeta, field.NewPath("metadata"))
	allErrs = append(allErrs, ValidateLoadBalancerStatus(&service.Status.LoadBalancer, &oldService.Status.LoadBalancer, field.NewPath("status", "loadBalancer"), &service.Spec)...)
	return allErrs
}

// ValidateReplicationController 校验 ReplicationController 的必填字段是否设置。
func ValidateReplicationController(controller *core.ReplicationController, opts PodValidationOptions) field.ErrorList {
	validateLongName := func(fldPath *field.Path, name string) field.ErrorList {
		return validate.LongName(context.Background(), operation.Operation{}, fldPath, &name, nil).MarkCoveredByDeclarative()
	}
	allErrs := ValidateObjectMetaWithOpts(&controller.ObjectMeta, true, validateLongName, field.NewPath("metadata"))
	allErrs = append(allErrs, ValidateReplicationControllerSpec(&controller.Spec, nil, field.NewPath("spec"), opts)...)
	return allErrs
}

// ValidateReplicationControllerUpdate 校验 ReplicationController 更新时的必填字段是否合法。
func ValidateReplicationControllerUpdate(controller, oldController *core.ReplicationController, opts PodValidationOptions) field.ErrorList {
	allErrs := ValidateObjectMetaUpdate(&controller.ObjectMeta, &oldController.ObjectMeta, field.NewPath("metadata"))
	allErrs = append(allErrs, ValidateReplicationControllerSpec(&controller.Spec, &oldController.Spec, field.NewPath("spec"), opts)...)
	return allErrs
}

// ValidateReplicationControllerStatusUpdate 校验 ReplicationController 状态更新时的必填字段是否合法。
func ValidateReplicationControllerStatusUpdate(controller, oldController *core.ReplicationController) field.ErrorList {
	allErrs := ValidateObjectMetaUpdate(&controller.ObjectMeta, &oldController.ObjectMeta, field.NewPath("metadata"))
	allErrs = append(allErrs, ValidateReplicationControllerStatus(controller.Status, field.NewPath("status"))...)
	return allErrs
}

// ValidateReplicationControllerStatus 校验 ReplicationControllerStatus 的副本数字段是否合法。
func ValidateReplicationControllerStatus(status core.ReplicationControllerStatus, statusPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	allErrs = append(allErrs, ValidateNonnegativeField(int64(status.Replicas), statusPath.Child("replicas"))...)
	allErrs = append(allErrs, ValidateNonnegativeField(int64(status.FullyLabeledReplicas), statusPath.Child("fullyLabeledReplicas"))...)
	allErrs = append(allErrs, ValidateNonnegativeField(int64(status.ReadyReplicas), statusPath.Child("readyReplicas"))...)
	allErrs = append(allErrs, ValidateNonnegativeField(int64(status.AvailableReplicas), statusPath.Child("availableReplicas"))...)
	allErrs = append(allErrs, ValidateNonnegativeField(int64(status.ObservedGeneration), statusPath.Child("observedGeneration"))...)
	msg := "cannot be greater than status.replicas"
	if status.FullyLabeledReplicas > status.Replicas {
		allErrs = append(allErrs, field.Invalid(statusPath.Child("fullyLabeledReplicas"), status.FullyLabeledReplicas, msg))
	}
	if status.ReadyReplicas > status.Replicas {
		allErrs = append(allErrs, field.Invalid(statusPath.Child("readyReplicas"), status.ReadyReplicas, msg))
	}
	if status.AvailableReplicas > status.Replicas {
		allErrs = append(allErrs, field.Invalid(statusPath.Child("availableReplicas"), status.AvailableReplicas, msg))
	}
	if status.AvailableReplicas > status.ReadyReplicas {
		allErrs = append(allErrs, field.Invalid(statusPath.Child("availableReplicas"), status.AvailableReplicas, "cannot be greater than readyReplicas"))
	}
	return allErrs
}

// ValidateNonEmptySelector 校验给定的标签选择器不为空。
func ValidateNonEmptySelector(selectorMap map[string]string, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	selector := labels.Set(selectorMap).AsSelector()
	if selector.Empty() {
		allErrs = append(allErrs, field.Required(fldPath, ""))
	}
	return allErrs
}

// ValidatePodTemplateSpecForRC 校验 Pod 模板规格与期望的选择器及副本数是否一致。
func ValidatePodTemplateSpecForRC(template *core.PodTemplateSpec, selectorMap map[string]string, fldPath *field.Path, opts PodValidationOptions) field.ErrorList {
	allErrs := field.ErrorList{}
	if template == nil {
		allErrs = append(allErrs, field.Required(fldPath, ""))
	} else {
		selector := labels.Set(selectorMap).AsSelector()
		if !selector.Empty() {
			// 验证 RC 的 selector 与 template 中的 labels 相匹配。
			labels := labels.Set(template.Labels)
			if !selector.Matches(labels) {
				allErrs = append(allErrs, field.Invalid(fldPath.Child("metadata", "labels"), template.Labels, "`selector` does not match template `labels`"))
			}
		}
		allErrs = append(allErrs, ValidatePodTemplateSpec(template, fldPath, opts)...)
		// RestartPolicy 已在 ValidatePodTemplateSpec() 中完成一阶校验。
		if template.Spec.RestartPolicy != core.RestartPolicyAlways {
			allErrs = append(allErrs, field.NotSupported(fldPath.Child("spec", "restartPolicy"), template.Spec.RestartPolicy, []core.RestartPolicy{core.RestartPolicyAlways}))
		}
		if template.Spec.ActiveDeadlineSeconds != nil {
			allErrs = append(allErrs, field.Forbidden(fldPath.Child("spec", "activeDeadlineSeconds"), "activeDeadlineSeconds in ReplicationController is not Supported"))
		}
	}
	return allErrs
}

// ValidateReplicationControllerSpec 校验 ReplicationController 规格的必填字段是否设置。
func ValidateReplicationControllerSpec(spec, oldSpec *core.ReplicationControllerSpec, fldPath *field.Path, opts PodValidationOptions) field.ErrorList {
	allErrs := field.ErrorList{}
	allErrs = append(allErrs, ValidateNonnegativeField(int64(spec.MinReadySeconds), fldPath.Child("minReadySeconds")).MarkCoveredByDeclarative()...)
	allErrs = append(allErrs, ValidateNonEmptySelector(spec.Selector, fldPath.Child("selector"))...)
	if spec.Replicas == nil {
		allErrs = append(allErrs, field.Required(fldPath.Child("replicas"), "").MarkCoveredByDeclarative())
	} else {
		allErrs = append(allErrs, ValidateNonnegativeField(int64(*spec.Replicas), fldPath.Child("replicas")).MarkCoveredByDeclarative()...)
	}
	allErrs = append(allErrs, ValidatePodTemplateSpecForRC(spec.Template, spec.Selector, fldPath.Child("template"), opts)...)
	return allErrs
}

// ValidatePodTemplateSpec 校验 Pod 模板的规格是否合法。
func ValidatePodTemplateSpec(spec *core.PodTemplateSpec, fldPath *field.Path, opts PodValidationOptions) field.ErrorList {
	allErrs := field.ErrorList{}
	allErrs = append(allErrs, unversionedvalidation.ValidateLabels(spec.Labels, fldPath.Child("labels"))...)
	allErrs = append(allErrs, ValidateAnnotations(spec.Annotations, fldPath.Child("annotations"))...)
	allErrs = append(allErrs, ValidatePodSpecificAnnotations(spec.Annotations, &spec.Spec, fldPath.Child("annotations"), opts)...)
	allErrs = append(allErrs, ValidatePodSpec(&spec.Spec, nil, fldPath.Child("spec"), opts)...)
	allErrs = append(allErrs, validateSeccompAnnotationsAndFields(spec.ObjectMeta, &spec.Spec, fldPath.Child("spec"))...)
	allErrs = append(allErrs, validateAppArmorAnnotationsAndFieldsMatchOnCreate(spec.ObjectMeta, &spec.Spec, fldPath.Child("spec"))...)

	if len(spec.Spec.EphemeralContainers) > 0 {
		allErrs = append(allErrs, field.Forbidden(fldPath.Child("spec", "ephemeralContainers"), "ephemeral containers not allowed in pod template"))
	}

	return allErrs
}

// ValidateTaintsInNodeAnnotations 校验 Node.Annotations 中序列化的 taints 数据是否合法。
func ValidateTaintsInNodeAnnotations(annotations map[string]string, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}

	taints, err := helper.GetTaintsFromNodeAnnotations(annotations)
	if err != nil {
		allErrs = append(allErrs, field.Invalid(fldPath, core.TaintsAnnotationKey, err.Error()))
		return allErrs
	}

	if len(taints) > 0 {
		allErrs = append(allErrs, validateNodeTaints(taints, fldPath.Child(core.TaintsAnnotationKey))...)
	}

	return allErrs
}

// validateNodeTaints 校验给定的 taints 列表数据是否合法。
func validateNodeTaints(taints []core.Taint, fldPath *field.Path) field.ErrorList {
	allErrors := field.ErrorList{}

	uniqueTaints := map[core.TaintEffect]sets.Set[string]{}

	for i, currTaint := range taints {
		idxPath := fldPath.Index(i)
		// 校验 taint 的 key
		allErrors = append(allErrors, unversionedvalidation.ValidateLabelName(currTaint.Key, idxPath.Child("key"))...)
		// 校验 taint 的 value
		if errs := validation.IsValidLabelValue(currTaint.Value); len(errs) != 0 {
			allErrors = append(allErrors, field.Invalid(idxPath.Child("value"), currTaint.Value, strings.Join(errs, ";")))
		}
		// 校验 taint 的 effect
		allErrors = append(allErrors, validateTaintEffect(&currTaint.Effect, false, idxPath.Child("effect"))...)

		// 校验 taint 是否按 <key, effect> 唯一
		if len(uniqueTaints[currTaint.Effect]) > 0 && uniqueTaints[currTaint.Effect].Has(currTaint.Key) {
			duplicatedError := field.Duplicate(idxPath, currTaint)
			duplicatedError.Detail = "taints must be unique by key and effect pair"
			allErrors = append(allErrors, duplicatedError)
			continue
		}

		// 将 taint 加入 uniqueTaints 以做唯一性检查
		if len(uniqueTaints[currTaint.Effect]) == 0 {
			uniqueTaints[currTaint.Effect] = sets.Set[string]{}
		}
		uniqueTaints[currTaint.Effect].Insert(currTaint.Key)
	}
	return allErrors
}

// ValidateNodeSpecificAnnotations 校验 Node 专有注解（如容器运行时、镜像运行时版本）是否合法。
func ValidateNodeSpecificAnnotations(annotations map[string]string, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}

	if annotations[core.TaintsAnnotationKey] != "" {
		allErrs = append(allErrs, ValidateTaintsInNodeAnnotations(annotations, fldPath)...)
	}

	if annotations[core.PreferAvoidPodsAnnotationKey] != "" {
		allErrs = append(allErrs, ValidateAvoidPodsInNodeAnnotations(annotations, fldPath)...)
	}
	return allErrs
}

// validateNodeDeclaredFeatureName 校验节点声明的特性名称是否为合法格式。
func validateNodeDeclaredFeatureName(featureName string) error {
	if len(featureName) > validation.DNS1123SubdomainMaxLength {
		return fmt.Errorf("invalid feature name %q: must be no more than %d characters", featureName, validation.DNS1123SubdomainMaxLength)
	}
	if !nodeDeclaredFeatureRegexp.MatchString(featureName) {
		return fmt.Errorf("invalid feature name %q: must start with an UpperCamelCase segment, with subsequent segments separated by '/' (e.g., MyFeature or MyFeature/mySubFeature), and contain only alphanumeric characters and slashes", featureName)
	}
	return nil
}

// validateNodeDeclaredFeatures 校验节点声明的特性列表：名称格式合法且不重复。
func validateNodeDeclaredFeatures(nodeFeatures []string, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	// 这里无需再次检查特性开关，因为已在 dropDisabledFields()（pkg/registry/core/node/strategy.go）中处理过
	featureCount := len(nodeFeatures)
	for i, feature := range nodeFeatures {
		if err := validateNodeDeclaredFeatureName(feature); err != nil {
			allErrs = append(allErrs, field.Invalid(fldPath.Index(i), feature, err.Error()))
		}
		if i+1 < featureCount {
			nextFeature := nodeFeatures[i+1]
			if feature == nextFeature {
				allErrs = append(allErrs, field.Duplicate(fldPath.Index(i+1), nextFeature))
			} else if feature > nextFeature {
				allErrs = append(allErrs, field.Invalid(fldPath.Index(i+1), nextFeature, "list must be sorted alphabetically"))
			}
		}
	}
	return allErrs
}

// ValidateNode 校验 Node 的必填字段是否设置。
func ValidateNode(node *core.Node) field.ErrorList {
	fldPath := field.NewPath("metadata")
	allErrs := ValidateObjectMeta(&node.ObjectMeta, false, ValidateNodeName, fldPath)
	allErrs = append(allErrs, ValidateNodeSpecificAnnotations(node.ObjectMeta.Annotations, fldPath.Child("annotations"))...)
	if len(node.Spec.Taints) > 0 {
		allErrs = append(allErrs, validateNodeTaints(node.Spec.Taints, fldPath.Child("taints"))...)
	}

	// 这里只校验 spec。
	// status 的所有字段都是可选的，可以稍后更新。
	// 但即便如此，一旦被指定，就必须确保它们合法。
	allErrs = append(allErrs, ValidateNodeResources(node)...)
	allErrs = append(allErrs, validateNodeSwapStatus(node.Status.NodeInfo.Swap, fldPath.Child("nodeSwapStatus"))...)
	statusField := field.NewPath("status")
	allErrs = append(allErrs, validateNodeDeclaredFeatures(node.Status.DeclaredFeatures, statusField.Child("declaredFeatures"))...)

	// 仅在需要时校验 PodCIDRs
	if len(node.Spec.PodCIDRs) > 0 {
		podCIDRsField := field.NewPath("spec", "podCIDRs")

		// 所有 PodCIDR 都必须是合法的
		for idx, value := range node.Spec.PodCIDRs {
			allErrs = append(allErrs, IsValidCIDRForLegacyField(podCIDRsField.Index(idx), value, nil)...)
		}

		// 如果 PodCIDR 多于 1 个，则必须构成双栈配对
		if len(node.Spec.PodCIDRs) > 1 {
			dualStack, err := netutils.IsDualStackCIDRStrings(node.Spec.PodCIDRs)
			if err != nil {
				allErrs = append(allErrs, field.InternalError(podCIDRsField, fmt.Errorf("invalid PodCIDRs. failed to check with dual stack with error:%v", err)))
			}
			if !dualStack || len(node.Spec.PodCIDRs) > 2 {
				allErrs = append(allErrs, field.Invalid(podCIDRsField, node.Spec.PodCIDRs, "may specify no more than one CIDR for each IP family"))
			}
		}
	}

	return allErrs
}

// ValidateNodeResources 校验 Node 的 capacity 与 allocatable 值是否合法。
func ValidateNodeResources(node *core.Node) field.ErrorList {
	allErrs := field.ErrorList{}

	// 校验 capacity 中的资源量。
	for k, v := range node.Status.Capacity {
		resPath := field.NewPath("status", "capacity", string(k))
		allErrs = append(allErrs, ValidateResourceQuantityValue(k, v, resPath)...)
	}

	// 校验 allocatable 中的资源量。
	for k, v := range node.Status.Allocatable {
		resPath := field.NewPath("status", "allocatable", string(k))
		allErrs = append(allErrs, ValidateResourceQuantityValue(k, v, resPath)...)
	}
	return allErrs
}

// ValidateNodeUpdate 校验 Node 更新是否可以应用；会修改 oldNode。
func ValidateNodeUpdate(node, oldNode *core.Node) field.ErrorList {
	fldPath := field.NewPath("metadata")
	allErrs := ValidateObjectMetaUpdate(&node.ObjectMeta, &oldNode.ObjectMeta, fldPath)
	allErrs = append(allErrs, ValidateNodeSpecificAnnotations(node.ObjectMeta.Annotations, fldPath.Child("annotations"))...)
	// TODO: 等我们有更好的 core object.status 更新模型后再启用下面的代码。目前
	// 任何人都可以更新 node status。
	// if !apiequality.Semantic.DeepEqual(node.Status, core.NodeStatus{}) {
	// 	allErrs = append(allErrs, field.Invalid("status", node.Status, "must be empty"))
	// }
	allErrs = append(allErrs, ValidateNodeResources(node)...)

	// TODO: 对 ValidateNode() 与 ValidateNodeUpdate() 中的校验逻辑去重，因为 node 更新时这两个函数都会被调用。
	statusField := field.NewPath("status")
	allErrs = append(allErrs, validateNodeDeclaredFeatures(node.Status.DeclaredFeatures, statusField.Child("declaredFeatures"))...)

	// 校验 node status 中的 addresses 没有重复。
	addresses := make(map[core.NodeAddress]bool)
	for i, address := range node.Status.Addresses {
		if _, ok := addresses[address]; ok {
			allErrs = append(allErrs, field.Duplicate(field.NewPath("status", "addresses").Index(i), address))
		}
		addresses[address] = true
	}

	// 允许 controller manager 在 node 尚无 CIDR 时为其分配一个。
	if len(oldNode.Spec.PodCIDRs) > 0 {
		// 比较整个切片
		if len(oldNode.Spec.PodCIDRs) != len(node.Spec.PodCIDRs) {
			allErrs = append(allErrs, field.Forbidden(field.NewPath("spec", "podCIDRs"), "node updates may not change podCIDR except from \"\" to valid"))
		} else {
			for idx, value := range oldNode.Spec.PodCIDRs {
				if value != node.Spec.PodCIDRs[idx] {
					allErrs = append(allErrs, field.Forbidden(field.NewPath("spec", "podCIDRs"), "node updates may not change podCIDR except from \"\" to valid"))
				}
			}
		}
	}

	// 允许 controller manager 在 providerID 未设置时更新它
	if len(oldNode.Spec.ProviderID) > 0 && len(node.Spec.ProviderID) > 0 && oldNode.Spec.ProviderID != node.Spec.ProviderID {
		allErrs = append(allErrs, field.Invalid(field.NewPath("spec", "providerID"), nil, "field cannot be modified once set").WithOrigin("update").MarkCoveredByDeclarative())
	}
	if len(oldNode.Spec.ProviderID) > 0 && len(node.Spec.ProviderID) == 0 {
		allErrs = append(allErrs, field.Invalid(field.NewPath("spec", "providerID"), nil, "field cannot be cleared once set").WithOrigin("update").MarkCoveredByDeclarative())
	}

	if node.Spec.ConfigSource != nil {
		allErrs = append(allErrs, validateNodeConfigSourceSpec(node.Spec.ConfigSource, field.NewPath("spec", "configSource"))...)
	}
	if node.Status.Config != nil {
		allErrs = append(allErrs, validateNodeConfigStatus(node.Status.Config, field.NewPath("status", "config"))...)
	}

	// 校验更新后的 taints
	if len(node.Spec.Taints) > 0 {
		allErrs = append(allErrs, validateNodeTaints(node.Spec.Taints, fldPath.Child("taints"))...)
	}

	if node.Spec.DoNotUseExternalID != oldNode.Spec.DoNotUseExternalID {
		allErrs = append(allErrs, field.Forbidden(field.NewPath("spec", "externalID"), "may not be updated"))
	}

	// status 与 metadata 允许变更（除上述限制外），因此单独检查 spec 字段。
	// spec 只有少数几个字段，逐个检查不允许变更的字段：
	//  1. PodCIDRs - 首次设置后不可变 - 已在上方检查
	//  2. ProviderID - 首次设置后不可变 - 已在上方检查
	//  3. Unschedulable - 允许变更
	//  4. Taints - 允许变更
	//  5. ConfigSource - 允许变更（并已在上方检查）
	//  6. DoNotUseExternalID - 不可变 - 已在上方检查

	return allErrs
}

// validateNodeConfigSourceSpec Node.Spec.ConfigSource 专属校验；该字段已废弃，仅为向后兼容保留。
func validateNodeConfigSourceSpec(source *core.NodeConfigSource, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	count := int(0)
	if source.ConfigMap != nil {
		count++
		allErrs = append(allErrs, validateConfigMapNodeConfigSourceSpec(source.ConfigMap, fldPath.Child("configMap"))...)
	}
	// 随着 NodeConfigSource 将来新增子字段，在此处补充对应校验

	// 引用类子字段必须恰好有一个非 nil
	if count != 1 {
		allErrs = append(allErrs, field.Invalid(fldPath, source, "exactly one reference subfield must be non-nil"))
	}
	return allErrs
}

// validateConfigMapNodeConfigSourceSpec Node.Spec.ConfigSource.ConfigMap 专属校验；该字段已废弃，仅为向后兼容保留。
func validateConfigMapNodeConfigSourceSpec(source *core.ConfigMapNodeConfigSource, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	// spec 中不得设置 uid 和 resourceVersion
	if string(source.UID) != "" {
		allErrs = append(allErrs, field.Forbidden(fldPath.Child("uid"), "uid must not be set in spec"))
	}
	if source.ResourceVersion != "" {
		allErrs = append(allErrs, field.Forbidden(fldPath.Child("resourceVersion"), "resourceVersion must not be set in spec"))
	}
	return append(allErrs, validateConfigMapNodeConfigSource(source, fldPath)...)
}

// validateNodeConfigStatus Node.Status.Config 专属校验。
func validateNodeConfigStatus(status *core.NodeConfigStatus, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	if status.Assigned != nil {
		allErrs = append(allErrs, validateNodeConfigSourceStatus(status.Assigned, fldPath.Child("assigned"))...)
	}
	if status.Active != nil {
		allErrs = append(allErrs, validateNodeConfigSourceStatus(status.Active, fldPath.Child("active"))...)
	}
	if status.LastKnownGood != nil {
		allErrs = append(allErrs, validateNodeConfigSourceStatus(status.LastKnownGood, fldPath.Child("lastKnownGood"))...)
	}
	return allErrs
}

// validateNodeConfigSourceStatus Node.Status.Config 中 Active/Assigned/LastKnownGood 的专属校验。
func validateNodeConfigSourceStatus(source *core.NodeConfigSource, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	count := int(0)
	if source.ConfigMap != nil {
		count++
		allErrs = append(allErrs, validateConfigMapNodeConfigSourceStatus(source.ConfigMap, fldPath.Child("configMap"))...)
	}
	// 随着 NodeConfigSource 将来新增子字段，在此处补充对应校验

	// 引用类子字段必须恰好有一个非 nil
	if count != 1 {
		allErrs = append(allErrs, field.Invalid(fldPath, source, "exactly one reference subfield must be non-nil"))
	}
	return allErrs
}

// validateConfigMapNodeConfigSourceStatus Node.Status.Config 中 ConfigMap 相关状态的专属校验。
func validateConfigMapNodeConfigSourceStatus(source *core.ConfigMapNodeConfigSource, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	// status 中必须设置 uid 和 resourceVersion
	if string(source.UID) == "" {
		allErrs = append(allErrs, field.Required(fldPath.Child("uid"), ""))
	}
	if source.ResourceVersion == "" {
		allErrs = append(allErrs, field.Required(fldPath.Child("resourceVersion"), ""))
	}
	return append(allErrs, validateConfigMapNodeConfigSource(source, fldPath)...)
}

// validateConfigMapNodeConfigSource Node 配置源的通用校验。
func validateConfigMapNodeConfigSource(source *core.ConfigMapNodeConfigSource, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	// 校验目标 configmap 的 namespace
	if source.Namespace == "" {
		allErrs = append(allErrs, field.Required(fldPath.Child("namespace"), ""))
	} else {
		for _, msg := range ValidateNameFunc(ValidateNamespaceName)(source.Namespace, false) {
			allErrs = append(allErrs, field.Invalid(fldPath.Child("namespace"), source.Namespace, msg))
		}
	}
	// 校验目标 configmap 的 name
	if source.Name == "" {
		allErrs = append(allErrs, field.Required(fldPath.Child("name"), ""))
	} else {
		for _, msg := range ValidateNameFunc(ValidateConfigMapName)(source.Name, false) {
			allErrs = append(allErrs, field.Invalid(fldPath.Child("name"), source.Name, msg))
		}
	}
	// 按 configMap key 名称规则校验 kubeletConfigKey
	if source.KubeletConfigKey == "" {
		allErrs = append(allErrs, field.Required(fldPath.Child("kubeletConfigKey"), ""))
	} else {
		for _, msg := range validation.IsConfigMapKey(source.KubeletConfigKey) {
			allErrs = append(allErrs, field.Invalid(fldPath.Child("kubeletConfigKey"), source.KubeletConfigKey, msg))
		}
	}
	return allErrs
}

// validateResourceName 校验计算资源类型名称；详见 docs/design/resources.md。
func validateResourceName(value core.ResourceName, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	for _, msg := range validation.IsQualifiedName(string(value)) {
		allErrs = append(allErrs, field.Invalid(fldPath, value, msg))
	}
	if len(allErrs) != 0 {
		return allErrs
	}

	if len(strings.Split(string(value), "/")) == 1 {
		if !helper.IsStandardResourceName(value) {
			return append(allErrs, field.Invalid(fldPath, value, "must be a standard resource type or fully qualified"))
		}
	}

	return allErrs
}

// ValidateContainerResourceName 校验容器资源名称；详见 docs/design/resources.md。
func ValidateContainerResourceName(value core.ResourceName, fldPath *field.Path) field.ErrorList {
	allErrs := validateResourceName(value, fldPath)

	if len(strings.Split(string(value), "/")) == 1 {
		if !helper.IsStandardContainerResourceName(value) {
			return append(allErrs, field.Invalid(fldPath, value, "must be a standard resource for containers"))
		}
	} else if !helper.IsNativeResource(value) {
		if !helper.IsExtendedResourceName(value) {
			return append(allErrs, field.Invalid(fldPath, value, "doesn't follow extended resource name standard"))
		}
	}
	return allErrs
}

// validatePodResourceName 校验 Pod 级资源名称：既要是合法计算资源名，也要被 PodLevelResources 特性支持。
func validatePodResourceName(resourceName core.ResourceName, fldPath *field.Path) field.ErrorList {
	allErrs := validateResourceName(resourceName, fldPath)
	if len(allErrs) != 0 {
		return allErrs
	}

	if !resourcehelper.IsSupportedPodLevelResource(v1.ResourceName(resourceName)) {
		return append(allErrs, field.NotSupported(fldPath, resourceName, sets.List(resourcehelper.SupportedPodLevelResources())))
	}

	return allErrs
}

// ValidateResourceQuotaResourceName 校验可用于 ResourceQuota 的资源名称；详见 docs/design/resources.md。
func ValidateResourceQuotaResourceName(value core.ResourceName, fldPath *field.Path) field.ErrorList {
	allErrs := validateResourceName(value, fldPath)

	if len(strings.Split(string(value), "/")) == 1 {
		if !helper.IsStandardQuotaResourceName(value) {
			return append(allErrs, field.Invalid(fldPath, value, isInvalidQuotaResource))
		}
	}
	return allErrs
}

// validateLimitRangeTypeName 校验 LimitRange 的类型名称是否合法。
func validateLimitRangeTypeName(value core.LimitType, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	for _, msg := range validation.IsQualifiedName(string(value)) {
		allErrs = append(allErrs, field.Invalid(fldPath, value, msg))
	}
	if len(allErrs) != 0 {
		return allErrs
	}

	if len(strings.Split(string(value), "/")) == 1 {
		if !helper.IsStandardLimitRangeType(value) {
			return append(allErrs, field.Invalid(fldPath, value, "must be a standard limit type or fully qualified"))
		}
	}

	return allErrs
}

// validateLimitRangeResourceName 校验 LimitRange 的资源名称；除 Pod/Container 外的类型可包含存储资源而非仅 CPU/内存。
func validateLimitRangeResourceName(limitType core.LimitType, value core.ResourceName, fldPath *field.Path) field.ErrorList {
	switch limitType {
	case core.LimitTypePod, core.LimitTypeContainer:
		return ValidateContainerResourceName(value, fldPath)
	default:
		return validateResourceName(value, fldPath)
	}
}

// ValidateLimitRange 校验 LimitRange 的必填字段是否设置。
func ValidateLimitRange(limitRange *core.LimitRange) field.ErrorList {
	allErrs := ValidateObjectMeta(&limitRange.ObjectMeta, true, ValidateLimitRangeName, field.NewPath("metadata"))

	// 确保资源名称按 docs/design/resources.md 的规定正确限定（qualified）
	limitTypeSet := map[core.LimitType]bool{}
	fldPath := field.NewPath("spec", "limits")
	for i := range limitRange.Spec.Limits {
		idxPath := fldPath.Index(i)
		limit := &limitRange.Spec.Limits[i]
		allErrs = append(allErrs, validateLimitRangeTypeName(limit.Type, idxPath.Child("type"))...)

		_, found := limitTypeSet[limit.Type]
		if found {
			allErrs = append(allErrs, field.Duplicate(idxPath.Child("type"), limit.Type))
		}
		limitTypeSet[limit.Type] = true

		keys := sets.Set[string]{}
		min := map[string]resource.Quantity{}
		max := map[string]resource.Quantity{}
		defaults := map[string]resource.Quantity{}
		defaultRequests := map[string]resource.Quantity{}
		maxLimitRequestRatios := map[string]resource.Quantity{}

		for k, q := range limit.Max {
			allErrs = append(allErrs, validateLimitRangeResourceName(limit.Type, k, idxPath.Child("max").Key(string(k)))...)
			keys.Insert(string(k))
			max[string(k)] = q
		}
		for k, q := range limit.Min {
			allErrs = append(allErrs, validateLimitRangeResourceName(limit.Type, k, idxPath.Child("min").Key(string(k)))...)
			keys.Insert(string(k))
			min[string(k)] = q
		}

		if limit.Type == core.LimitTypePod {
			if len(limit.Default) > 0 {
				allErrs = append(allErrs, field.Forbidden(idxPath.Child("default"), "may not be specified when `type` is 'Pod'"))
			}
			if len(limit.DefaultRequest) > 0 {
				allErrs = append(allErrs, field.Forbidden(idxPath.Child("defaultRequest"), "may not be specified when `type` is 'Pod'"))
			}
		} else {
			for k, q := range limit.Default {
				allErrs = append(allErrs, validateLimitRangeResourceName(limit.Type, k, idxPath.Child("default").Key(string(k)))...)
				keys.Insert(string(k))
				defaults[string(k)] = q
			}
			for k, q := range limit.DefaultRequest {
				allErrs = append(allErrs, validateLimitRangeResourceName(limit.Type, k, idxPath.Child("defaultRequest").Key(string(k)))...)
				keys.Insert(string(k))
				defaultRequests[string(k)] = q
			}
		}

		if limit.Type == core.LimitTypePersistentVolumeClaim {
			_, minQuantityFound := limit.Min[core.ResourceStorage]
			_, maxQuantityFound := limit.Max[core.ResourceStorage]
			if !minQuantityFound && !maxQuantityFound {
				allErrs = append(allErrs, field.Required(idxPath.Child("limits"), "either minimum or maximum storage value is required, but neither was provided"))
			}
		}

		for k, q := range limit.MaxLimitRequestRatio {
			allErrs = append(allErrs, validateLimitRangeResourceName(limit.Type, k, idxPath.Child("maxLimitRequestRatio").Key(string(k)))...)
			keys.Insert(string(k))
			maxLimitRequestRatios[string(k)] = q
		}

		for k := range keys {
			minQuantity, minQuantityFound := min[k]
			maxQuantity, maxQuantityFound := max[k]
			defaultQuantity, defaultQuantityFound := defaults[k]
			defaultRequestQuantity, defaultRequestQuantityFound := defaultRequests[k]
			maxRatio, maxRatioFound := maxLimitRequestRatios[k]

			if minQuantityFound && maxQuantityFound && minQuantity.Cmp(maxQuantity) > 0 {
				allErrs = append(allErrs, field.Invalid(idxPath.Child("min").Key(string(k)), minQuantity, fmt.Sprintf("min value %s is greater than max value %s", minQuantity.String(), maxQuantity.String())))
			}

			if defaultRequestQuantityFound && minQuantityFound && minQuantity.Cmp(defaultRequestQuantity) > 0 {
				allErrs = append(allErrs, field.Invalid(idxPath.Child("defaultRequest").Key(string(k)), defaultRequestQuantity, fmt.Sprintf("min value %s is greater than default request value %s", minQuantity.String(), defaultRequestQuantity.String())))
			}

			if defaultRequestQuantityFound && maxQuantityFound && defaultRequestQuantity.Cmp(maxQuantity) > 0 {
				allErrs = append(allErrs, field.Invalid(idxPath.Child("defaultRequest").Key(string(k)), defaultRequestQuantity, fmt.Sprintf("default request value %s is greater than max value %s", defaultRequestQuantity.String(), maxQuantity.String())))
			}

			if defaultRequestQuantityFound && defaultQuantityFound && defaultRequestQuantity.Cmp(defaultQuantity) > 0 {
				allErrs = append(allErrs, field.Invalid(idxPath.Child("defaultRequest").Key(string(k)), defaultRequestQuantity, fmt.Sprintf("default request value %s is greater than default limit value %s", defaultRequestQuantity.String(), defaultQuantity.String())))
			}

			if defaultQuantityFound && minQuantityFound && minQuantity.Cmp(defaultQuantity) > 0 {
				allErrs = append(allErrs, field.Invalid(idxPath.Child("default").Key(string(k)), minQuantity, fmt.Sprintf("min value %s is greater than default value %s", minQuantity.String(), defaultQuantity.String())))
			}

			if defaultQuantityFound && maxQuantityFound && defaultQuantity.Cmp(maxQuantity) > 0 {
				allErrs = append(allErrs, field.Invalid(idxPath.Child("default").Key(string(k)), maxQuantity, fmt.Sprintf("default value %s is greater than max value %s", defaultQuantity.String(), maxQuantity.String())))
			}
			if maxRatioFound && maxRatio.Cmp(*resource.NewQuantity(1, resource.DecimalSI)) < 0 {
				allErrs = append(allErrs, field.Invalid(idxPath.Child("maxLimitRequestRatio").Key(string(k)), maxRatio, fmt.Sprintf("ratio %s is less than 1", maxRatio.String())))
			}
			if maxRatioFound && minQuantityFound && maxQuantityFound {
				maxRatioValue := float64(maxRatio.Value())
				minQuantityValue := minQuantity.Value()
				maxQuantityValue := maxQuantity.Value()
				if maxRatio.Value() < resource.MaxMilliValue && minQuantityValue < resource.MaxMilliValue && maxQuantityValue < resource.MaxMilliValue {
					maxRatioValue = float64(maxRatio.MilliValue()) / 1000
					minQuantityValue = minQuantity.MilliValue()
					maxQuantityValue = maxQuantity.MilliValue()
				}
				maxRatioLimit := float64(maxQuantityValue) / float64(minQuantityValue)
				if maxRatioValue > maxRatioLimit {
					allErrs = append(allErrs, field.Invalid(idxPath.Child("maxLimitRequestRatio").Key(string(k)), maxRatio, fmt.Sprintf("ratio %s is greater than max/min = %f", maxRatio.String(), maxRatioLimit)))
				}
			}

			// 对于 GPU、大页（hugepages）及其他不允许超卖的资源，
			// 若同时指定了 default 与 defaultRequest，二者的取值必须一致
			if !helper.IsOvercommitAllowed(core.ResourceName(k)) && defaultQuantityFound && defaultRequestQuantityFound && defaultQuantity.Cmp(defaultRequestQuantity) != 0 {
				allErrs = append(allErrs, field.Invalid(idxPath.Child("defaultRequest").Key(string(k)), defaultRequestQuantity, fmt.Sprintf("default value %s must equal to defaultRequest value %s in %s", defaultQuantity.String(), defaultRequestQuantity.String(), k)))
			}
		}
	}

	return allErrs
}

// ValidateServiceAccount 校验 ServiceAccount 的必填字段是否设置。
func ValidateServiceAccount(serviceAccount *core.ServiceAccount) field.ErrorList {
	allErrs := ValidateObjectMeta(&serviceAccount.ObjectMeta, true, ValidateServiceAccountName, field.NewPath("metadata"))
	return allErrs
}

// ValidateServiceAccountUpdate 校验 ServiceAccount 更新时的必填字段是否合法。
func ValidateServiceAccountUpdate(newServiceAccount, oldServiceAccount *core.ServiceAccount) field.ErrorList {
	allErrs := ValidateObjectMetaUpdate(&newServiceAccount.ObjectMeta, &oldServiceAccount.ObjectMeta, field.NewPath("metadata"))
	allErrs = append(allErrs, ValidateServiceAccount(newServiceAccount)...)
	return allErrs
}

// ValidateSecret 校验 Secret 的必填字段是否设置。
func ValidateSecret(secret *core.Secret) field.ErrorList {
	allErrs := ValidateObjectMeta(&secret.ObjectMeta, true, ValidateSecretName, field.NewPath("metadata"))

	dataPath := field.NewPath("data")
	totalSize := 0
	for key, value := range secret.Data {
		for _, msg := range validation.IsConfigMapKey(key) {
			allErrs = append(allErrs, field.Invalid(dataPath.Key(key), key, msg))
		}
		totalSize += len(value)
	}
	if totalSize > core.MaxSecretSize {
		allErrs = append(allErrs, field.TooLong(dataPath, "" /*unused*/, core.MaxSecretSize))
	}

	switch secret.Type {
	case core.SecretTypeServiceAccountToken:
		// 仅要求 Annotations[kubernetes.io/service-account.name] 存在；
		// 其余字段（如 Annotations[kubernetes.io/service-account.uid] 和 Data[token]）可能由控制器循环稍后填充
		if value := secret.Annotations[core.ServiceAccountNameKey]; len(value) == 0 {
			allErrs = append(allErrs, field.Required(field.NewPath("metadata", "annotations").Key(core.ServiceAccountNameKey), ""))
		}
	case core.SecretTypeOpaque, "":
	// 空操作（no-op）
	case core.SecretTypeDockercfg:
		dockercfgBytes, exists := secret.Data[core.DockerConfigKey]
		if !exists {
			allErrs = append(allErrs, field.Required(dataPath.Key(core.DockerConfigKey), ""))
			break
		}

		// 确保内容是格式良好的 json。
		if err := json.Unmarshal(dockercfgBytes, &map[string]interface{}{}); err != nil {
			allErrs = append(allErrs, field.Invalid(dataPath.Key(core.DockerConfigKey), "<secret contents redacted>", err.Error()))
		}
	case core.SecretTypeDockerConfigJSON:
		dockerConfigJSONBytes, exists := secret.Data[core.DockerConfigJSONKey]
		if !exists {
			allErrs = append(allErrs, field.Required(dataPath.Key(core.DockerConfigJSONKey), ""))
			break
		}

		// 确保内容是格式良好的 json。
		if err := json.Unmarshal(dockerConfigJSONBytes, &map[string]interface{}{}); err != nil {
			allErrs = append(allErrs, field.Invalid(dataPath.Key(core.DockerConfigJSONKey), "<secret contents redacted>", err.Error()))
		}
	case core.SecretTypeBasicAuth:
		_, usernameFieldExists := secret.Data[core.BasicAuthUsernameKey]
		_, passwordFieldExists := secret.Data[core.BasicAuthPasswordKey]

		// username 或 password 可以为空，但字段必须存在
		if !usernameFieldExists && !passwordFieldExists {
			allErrs = append(allErrs, field.Required(dataPath.Key(core.BasicAuthUsernameKey), ""))
			allErrs = append(allErrs, field.Required(dataPath.Key(core.BasicAuthPasswordKey), ""))
			break
		}
	case core.SecretTypeSSHAuth:
		if len(secret.Data[core.SSHAuthPrivateKey]) == 0 {
			allErrs = append(allErrs, field.Required(dataPath.Key(core.SSHAuthPrivateKey), ""))
			break
		}

	case core.SecretTypeTLS:
		if _, exists := secret.Data[core.TLSCertKey]; !exists {
			allErrs = append(allErrs, field.Required(dataPath.Key(core.TLSCertKey), ""))
		}
		if _, exists := secret.Data[core.TLSPrivateKeyKey]; !exists {
			allErrs = append(allErrs, field.Required(dataPath.Key(core.TLSPrivateKeyKey), ""))
		}
	default:
		// 空操作（no-op）
	}

	return allErrs
}

// ValidateSecretUpdate 校验 Secret 更新时的必填字段是否合法。
func ValidateSecretUpdate(newSecret, oldSecret *core.Secret) field.ErrorList {
	allErrs := ValidateObjectMetaUpdate(&newSecret.ObjectMeta, &oldSecret.ObjectMeta, field.NewPath("metadata"))

	allErrs = append(allErrs, ValidateImmutableField(newSecret.Type, oldSecret.Type, field.NewPath("type")).WithOrigin("immutable").MarkCoveredByDeclarative()...)
	if oldSecret.Immutable != nil && *oldSecret.Immutable {
		if newSecret.Immutable == nil || !*newSecret.Immutable {
			allErrs = append(allErrs, field.Forbidden(field.NewPath("immutable"), "field is immutable when `immutable` is set"))
		}
		if !reflect.DeepEqual(newSecret.Data, oldSecret.Data) {
			allErrs = append(allErrs, field.Forbidden(field.NewPath("data"), "field is immutable when `immutable` is set"))
		}
		// 不校验 StringData，因为在执行校验之前它
		// 已被转换回 Data。
	}

	allErrs = append(allErrs, ValidateSecret(newSecret)...)
	return allErrs
}

// ValidateConfigMapName 可用于检查给定的 ConfigMap 名称是否合法。
// Prefix 表示该名称将用于名称生成，此时
// 允许结尾带短横线。
var ValidateConfigMapName = apimachineryvalidation.NameIsDNSSubdomain

// ValidateConfigMap 校验 ConfigMap 的必填字段是否设置。
func ValidateConfigMap(cfg *core.ConfigMap) field.ErrorList {
	allErrs := field.ErrorList{}
	allErrs = append(allErrs, ValidateObjectMeta(&cfg.ObjectMeta, true, ValidateConfigMapName, field.NewPath("metadata"))...)

	totalSize := 0

	for key, value := range cfg.Data {
		for _, msg := range validation.IsConfigMapKey(key) {
			allErrs = append(allErrs, field.Invalid(field.NewPath("data").Key(key), key, msg))
		}
		// 检查另一组数据（binaryData）中是否存在重复的 key
		if _, isValue := cfg.BinaryData[key]; isValue {
			msg := "duplicate of key present in binaryData"
			allErrs = append(allErrs, field.Invalid(field.NewPath("data").Key(key), key, msg))
		}
		totalSize += len(value)
	}
	for key, value := range cfg.BinaryData {
		for _, msg := range validation.IsConfigMapKey(key) {
			allErrs = append(allErrs, field.Invalid(field.NewPath("binaryData").Key(key), key, msg))
		}
		totalSize += len(value)
	}
	if totalSize > core.MaxSecretSize {
		// 回传 "" 表示该错误针对的是整个对象。
		allErrs = append(allErrs, field.TooLong(field.NewPath(""), "" /*unused*/, core.MaxSecretSize))
	}

	return allErrs
}

// ValidateConfigMapUpdate 校验 ConfigMap 更新时的必填字段是否合法。
func ValidateConfigMapUpdate(newCfg, oldCfg *core.ConfigMap) field.ErrorList {
	allErrs := field.ErrorList{}
	allErrs = append(allErrs, ValidateObjectMetaUpdate(&newCfg.ObjectMeta, &oldCfg.ObjectMeta, field.NewPath("metadata"))...)

	if oldCfg.Immutable != nil && *oldCfg.Immutable {
		if newCfg.Immutable == nil || !*newCfg.Immutable {
			allErrs = append(allErrs, field.Forbidden(field.NewPath("immutable"), "field is immutable when `immutable` is set"))
		}
		if !reflect.DeepEqual(newCfg.Data, oldCfg.Data) {
			allErrs = append(allErrs, field.Forbidden(field.NewPath("data"), "field is immutable when `immutable` is set"))
		}
		if !reflect.DeepEqual(newCfg.BinaryData, oldCfg.BinaryData) {
			allErrs = append(allErrs, field.Forbidden(field.NewPath("binaryData"), "field is immutable when `immutable` is set"))
		}
	}

	allErrs = append(allErrs, ValidateConfigMap(newCfg)...)
	return allErrs
}

// validateBasicResource 校验基本资源数量是否合法。
func validateBasicResource(quantity resource.Quantity, fldPath *field.Path) field.ErrorList {
	if quantity.Value() < 0 {
		return field.ErrorList{field.Invalid(fldPath, quantity.Value(), "must be a valid resource quantity")}
	}
	return field.ErrorList{}
}

// validatePodResourceRequirements 校验 Pod 级资源需求，使用 Pod 级资源名校验规则。
func validatePodResourceRequirements(requirements *core.ResourceRequirements, podClaimNames sets.Set[string], fldPath *field.Path, opts PodValidationOptions) field.ErrorList {
	return validateResourceRequirements(requirements, validatePodResourceName, podClaimNames, fldPath, opts)
}

// ValidateContainerResourceRequirements 校验容器资源需求，使用容器资源名校验规则。
func ValidateContainerResourceRequirements(requirements *core.ResourceRequirements, podClaimNames sets.Set[string], fldPath *field.Path, opts PodValidationOptions) field.ErrorList {
	return validateResourceRequirements(requirements, ValidateContainerResourceName, podClaimNames, fldPath, opts)
}

// validateResourceRequirements 校验资源需求规格（limits、requests、claims）整体是否合法。
func validateResourceRequirements(requirements *core.ResourceRequirements, resourceNameFn func(core.ResourceName, *field.Path) field.ErrorList, podClaimNames sets.Set[string], fldPath *field.Path, opts PodValidationOptions) field.ErrorList {
	allErrs := field.ErrorList{}
	limPath := fldPath.Child("limits")
	reqPath := fldPath.Child("requests")
	limContainsCPUOrMemory := false
	reqContainsCPUOrMemory := false
	limContainsHugePages := false
	reqContainsHugePages := false
	supportedQoSComputeResources := sets.New(core.ResourceCPU, core.ResourceMemory)
	for resourceName, quantity := range requirements.Limits {

		fldPath := limPath.Key(string(resourceName))
		// 校验资源名称。
		allErrs = append(allErrs, resourceNameFn(resourceName, fldPath)...)

		// 校验资源数量。
		allErrs = append(allErrs, ValidateResourceQuantityValue(resourceName, quantity, fldPath)...)

		if helper.IsHugePageResourceName(resourceName) {
			limContainsHugePages = true
			if err := validateResourceQuantityHugePageValue(resourceName, quantity, opts); err != nil {
				allErrs = append(allErrs, field.Invalid(fldPath, quantity.String(), err.Error()))
			}
		}

		if supportedQoSComputeResources.Has(resourceName) {
			limContainsCPUOrMemory = true
		}
	}
	for resourceName, quantity := range requirements.Requests {
		fldPath := reqPath.Key(string(resourceName))
		// 校验资源名称。
		allErrs = append(allErrs, resourceNameFn(resourceName, fldPath)...)

		// 校验资源数量。
		allErrs = append(allErrs, ValidateResourceQuantityValue(resourceName, quantity, fldPath)...)

		// 检查 request <= limit。
		limitQuantity, exists := requirements.Limits[resourceName]
		if exists {
			// 对于不可超卖的资源，request 不仅不能超过 limit，也不能低于 limit，即必须相等。
			if quantity.Cmp(limitQuantity) != 0 && !helper.IsOvercommitAllowed(resourceName) {
				allErrs = append(allErrs, field.Invalid(reqPath, quantity.String(), fmt.Sprintf("must be equal to %s limit of %s", resourceName, limitQuantity.String())))
			} else if quantity.Cmp(limitQuantity) > 0 {
				allErrs = append(allErrs, field.Invalid(reqPath, quantity.String(), fmt.Sprintf("must be less than or equal to %s limit of %s", resourceName, limitQuantity.String())))
			}
		} else if !helper.IsOvercommitAllowed(resourceName) {
			allErrs = append(allErrs, field.Required(limPath, "Limit must be set for non overcommitable resources"))
		}
		if helper.IsHugePageResourceName(resourceName) {
			reqContainsHugePages = true
			if err := validateResourceQuantityHugePageValue(resourceName, quantity, opts); err != nil {
				allErrs = append(allErrs, field.Invalid(fldPath, quantity.String(), err.Error()))
			}
		}
		if supportedQoSComputeResources.Has(resourceName) {
			reqContainsCPUOrMemory = true
		}

	}
	if !limContainsCPUOrMemory && !reqContainsCPUOrMemory && (reqContainsHugePages || limContainsHugePages) {
		allErrs = append(allErrs, field.Forbidden(fldPath, "HugePages require cpu or memory"))
	}

	allErrs = append(allErrs, validateResourceClaimNames(requirements.Claims, podClaimNames, fldPath.Child("claims"))...)

	return allErrs
}

// validateResourceClaimNames 校验 ResourceRequirements.Claims 中的名称在 PodSpec.ResourceClaims 中有对应条目。
func validateResourceClaimNames(claims []core.ResourceClaim, podClaimNames sets.Set[string], fldPath *field.Path) field.ErrorList {
	var allErrs field.ErrorList
	names := sets.Set[string]{}
	for i, claim := range claims {
		name := claim.Name
		if name == "" {
			allErrs = append(allErrs, field.Required(fldPath.Index(i), ""))
		} else {
			if names.Has(name) {
				// 该 claim 的所有请求都已被引用。
				allErrs = append(allErrs, field.Duplicate(fldPath.Index(i), name))
			} else {
				key := name
				if claim.Request != "" {
					allErrs = append(allErrs, ValidateDNS1123Label(claim.Request, fldPath.Index(i).Child("request"))...)
					key += "/" + claim.Request
				}
				if names.Has(key) {
					// 完全相同的条目已被引用。
					allErrs = append(allErrs, field.Duplicate(fldPath.Index(i), key))
				} else if claim.Request == "" {
					// 引用某个 claim 时，若此前已引用过
					// 该 claim 中的某些 request，就会出现
					// 重叠。这无法通过 map 查找来检查，
					// 需要遍历。
					for key := range names {
						index := strings.Index(key, "/")
						if index < 0 {
							continue
						}
						if key[0:index] == name {
							allErrs = append(allErrs, field.Duplicate(fldPath.Index(i), name))
						}
					}
				}

				names.Insert(key)
			}
			if !podClaimNames.Has(name) {
				// field.NotFound 不接受解释性文字。
				// 在这里附加一条说明对用户更友好。
				error := field.NotFound(fldPath.Index(i), name)
				error.Detail = "must be one of the names in pod.spec.resourceClaims"
				if len(podClaimNames) == 0 {
					error.Detail += " which is empty"
				} else {
					error.Detail += ": " + strings.Join(sets.List(podClaimNames), ", ")
				}
				allErrs = append(allErrs, error)
			}
		}
	}
	return allErrs
}

// validateResourceQuantityHugePageValue 校验 huge page 资源数量是否合法（必须为整数且受支持）。
func validateResourceQuantityHugePageValue(name core.ResourceName, quantity resource.Quantity, opts PodValidationOptions) error {
	if !helper.IsHugePageResourceName(name) {
		return nil
	}

	if !opts.AllowIndivisibleHugePagesValues && !helper.IsHugePageResourceValueDivisible(name, quantity) {
		return fmt.Errorf("%s is not positive integer multiple of %s", quantity.String(), name)
	}

	return nil
}

// validateResourceQuotaScopes 校验 ResourceQuota 中每个枚举的 hard 资源约束对其作用域集合是否有效。
func validateResourceQuotaScopes(resourceQuotaSpec *core.ResourceQuotaSpec, fld *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	if len(resourceQuotaSpec.Scopes) == 0 {
		return allErrs
	}
	hardLimits := sets.New[core.ResourceName]()
	for k := range resourceQuotaSpec.Hard {
		hardLimits.Insert(k)
	}
	fldPath := fld.Child("scopes")
	scopeSet := sets.New[core.ResourceQuotaScope]()
	for _, scope := range resourceQuotaSpec.Scopes {
		if !helper.IsStandardResourceQuotaScope(scope) {
			allErrs = append(allErrs, field.Invalid(fldPath, resourceQuotaSpec.Scopes, "unsupported scope"))
		}
		for _, k := range sets.List(hardLimits) {
			if helper.IsStandardQuotaResourceName(k) && !helper.IsResourceQuotaScopeValidForResource(scope, k) {
				allErrs = append(allErrs, field.Invalid(fldPath, resourceQuotaSpec.Scopes, "unsupported scope applied to resource"))
			}
		}
		scopeSet.Insert(scope)
	}
	invalidScopePairs := []sets.Set[core.ResourceQuotaScope]{
		sets.New(core.ResourceQuotaScopeBestEffort, core.ResourceQuotaScopeNotBestEffort),
		sets.New(core.ResourceQuotaScopeTerminating, core.ResourceQuotaScopeNotTerminating),
	}
	for _, invalidScopePair := range invalidScopePairs {
		if scopeSet.HasAll(sets.List(invalidScopePair)...) {
			allErrs = append(allErrs, field.Invalid(fldPath, resourceQuotaSpec.Scopes, "conflicting scopes"))
		}
	}
	return allErrs
}

// validateScopedResourceSelectorRequirement 校验作用域选择器中的匹配表达式是否合法。
func validateScopedResourceSelectorRequirement(resourceQuotaSpec *core.ResourceQuotaSpec, fld *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	hardLimits := sets.New[core.ResourceName]()
	for k := range resourceQuotaSpec.Hard {
		hardLimits.Insert(k)
	}
	fldPath := fld.Child("matchExpressions")
	scopeSet := sets.New[core.ResourceQuotaScope]()
	for _, req := range resourceQuotaSpec.ScopeSelector.MatchExpressions {
		if !helper.IsStandardResourceQuotaScope(req.ScopeName) {
			allErrs = append(allErrs, field.Invalid(fldPath.Child("scopeName"), req.ScopeName, "unsupported scope"))
		}
		for _, k := range sets.List(hardLimits) {
			if helper.IsStandardQuotaResourceName(k) && !helper.IsResourceQuotaScopeValidForResource(req.ScopeName, k) {
				allErrs = append(allErrs, field.Invalid(fldPath, resourceQuotaSpec.ScopeSelector, "unsupported scope applied to resource"))
			}
		}
		switch req.ScopeName {
		case core.ResourceQuotaScopeBestEffort, core.ResourceQuotaScopeNotBestEffort, core.ResourceQuotaScopeTerminating, core.ResourceQuotaScopeNotTerminating, core.ResourceQuotaScopeCrossNamespacePodAffinity:
			if req.Operator != core.ScopeSelectorOpExists {
				allErrs = append(allErrs, field.Invalid(fldPath.Child("operator"), req.Operator,
					"must be 'Exists' when scope is any of ResourceQuotaScopeTerminating, ResourceQuotaScopeNotTerminating, ResourceQuotaScopeBestEffort, ResourceQuotaScopeNotBestEffort or ResourceQuotaScopeCrossNamespacePodAffinity"))
			}
		}

		switch req.Operator {
		case core.ScopeSelectorOpIn, core.ScopeSelectorOpNotIn:
			if len(req.Values) == 0 {
				allErrs = append(allErrs, field.Required(fldPath.Child("values"),
					"must be at least one value when `operator` is 'In' or 'NotIn' for scope selector"))
			}
		case core.ScopeSelectorOpExists, core.ScopeSelectorOpDoesNotExist:
			if len(req.Values) != 0 {
				allErrs = append(allErrs, field.Invalid(fldPath.Child("values"), req.Values,
					"must be no value when `operator` is 'Exist' or 'DoesNotExist' for scope selector"))
			}
		default:
			allErrs = append(allErrs, field.Invalid(fldPath.Child("operator"), req.Operator, "not a valid selector operator"))
		}
		scopeSet.Insert(req.ScopeName)
	}
	invalidScopePairs := []sets.Set[core.ResourceQuotaScope]{
		sets.New(core.ResourceQuotaScopeBestEffort, core.ResourceQuotaScopeNotBestEffort),
		sets.New(core.ResourceQuotaScopeTerminating, core.ResourceQuotaScopeNotTerminating),
	}
	for _, invalidScopePair := range invalidScopePairs {
		if scopeSet.HasAll(sets.List(invalidScopePair)...) {
			allErrs = append(allErrs, field.Invalid(fldPath, resourceQuotaSpec.Scopes, "conflicting scopes"))
		}
	}

	return allErrs
}

// validateScopeSelector 校验指定的 ScopeSelector 是否合法。
func validateScopeSelector(resourceQuotaSpec *core.ResourceQuotaSpec, fld *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	if resourceQuotaSpec.ScopeSelector == nil {
		return allErrs
	}
	allErrs = append(allErrs, validateScopedResourceSelectorRequirement(resourceQuotaSpec, fld.Child("scopeSelector"))...)
	return allErrs
}

// ValidateResourceQuota 校验 ResourceQuota 的必填字段是否设置。
func ValidateResourceQuota(resourceQuota *core.ResourceQuota) field.ErrorList {
	allErrs := ValidateObjectMeta(&resourceQuota.ObjectMeta, true, ValidateResourceQuotaName, field.NewPath("metadata"))

	allErrs = append(allErrs, ValidateResourceQuotaSpec(&resourceQuota.Spec, field.NewPath("spec"))...)
	allErrs = append(allErrs, ValidateResourceQuotaStatus(&resourceQuota.Status, field.NewPath("status"))...)

	return allErrs
}

// ValidateResourceQuotaStatus 校验 ResourceQuotaStatus 的 used/hard 值是否合法。
func ValidateResourceQuotaStatus(status *core.ResourceQuotaStatus, fld *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}

	fldPath := fld.Child("hard")
	for k, v := range status.Hard {
		resPath := fldPath.Key(string(k))
		allErrs = append(allErrs, ValidateResourceQuotaResourceName(k, resPath)...)
		allErrs = append(allErrs, ValidateResourceQuantityValue(k, v, resPath)...)
	}
	fldPath = fld.Child("used")
	for k, v := range status.Used {
		resPath := fldPath.Key(string(k))
		allErrs = append(allErrs, ValidateResourceQuotaResourceName(k, resPath)...)
		allErrs = append(allErrs, ValidateResourceQuantityValue(k, v, resPath)...)
	}

	return allErrs
}

// ValidateResourceQuotaSpec 校验 ResourceQuotaSpec 的 scopes、scopeSelector 与 hard 值是否合法。
func ValidateResourceQuotaSpec(resourceQuotaSpec *core.ResourceQuotaSpec, fld *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}

	fldPath := fld.Child("hard")
	for k, v := range resourceQuotaSpec.Hard {
		resPath := fldPath.Key(string(k))
		allErrs = append(allErrs, ValidateResourceQuotaResourceName(k, resPath)...)
		allErrs = append(allErrs, ValidateResourceQuantityValue(k, v, resPath)...)
	}

	allErrs = append(allErrs, validateResourceQuotaScopes(resourceQuotaSpec, fld)...)
	allErrs = append(allErrs, validateScopeSelector(resourceQuotaSpec, fld)...)

	return allErrs
}

// ValidateResourceQuantityValue 校验给定资源对应的数量值是否合法。
func ValidateResourceQuantityValue(resource core.ResourceName, value resource.Quantity, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	allErrs = append(allErrs, ValidateNonnegativeQuantity(value, fldPath)...)
	if helper.IsIntegerResourceName(resource) {
		if value.MilliValue()%int64(1000) != int64(0) {
			allErrs = append(allErrs, field.Invalid(fldPath, value, isNotIntegerErrorMsg))
		}
	}
	return allErrs
}

// ValidateResourceQuotaUpdate 校验终端用户对 ResourceQuota 的更新是否合法。
func ValidateResourceQuotaUpdate(newResourceQuota, oldResourceQuota *core.ResourceQuota) field.ErrorList {
	allErrs := ValidateObjectMetaUpdate(&newResourceQuota.ObjectMeta, &oldResourceQuota.ObjectMeta, field.NewPath("metadata"))
	allErrs = append(allErrs, ValidateResourceQuotaSpec(&newResourceQuota.Spec, field.NewPath("spec"))...)

	// 确保 scopes 不可变更，且资源对相应 scope 仍然合法
	fldPath := field.NewPath("spec", "scopes")
	oldScopes := sets.New[string]()
	newScopes := sets.New[string]()
	for _, scope := range newResourceQuota.Spec.Scopes {
		newScopes.Insert(string(scope))
	}
	for _, scope := range oldResourceQuota.Spec.Scopes {
		oldScopes.Insert(string(scope))
	}
	if !oldScopes.Equal(newScopes) {
		allErrs = append(allErrs, field.Invalid(fldPath, newResourceQuota.Spec.Scopes, fieldImmutableErrorMsg))
	}

	return allErrs
}

// ValidateResourceQuotaStatusUpdate 校验终端用户对 ResourceQuota 状态的更新是否合法。
func ValidateResourceQuotaStatusUpdate(newResourceQuota, oldResourceQuota *core.ResourceQuota) field.ErrorList {
	allErrs := ValidateObjectMetaUpdate(&newResourceQuota.ObjectMeta, &oldResourceQuota.ObjectMeta, field.NewPath("metadata"))
	if len(newResourceQuota.ResourceVersion) == 0 {
		allErrs = append(allErrs, field.Required(field.NewPath("resourceVersion"), ""))
	}
	fldPath := field.NewPath("status", "hard")
	for k, v := range newResourceQuota.Status.Hard {
		resPath := fldPath.Key(string(k))
		allErrs = append(allErrs, ValidateResourceQuotaResourceName(k, resPath)...)
		allErrs = append(allErrs, ValidateResourceQuantityValue(k, v, resPath)...)
	}
	fldPath = field.NewPath("status", "used")
	for k, v := range newResourceQuota.Status.Used {
		resPath := fldPath.Key(string(k))
		allErrs = append(allErrs, ValidateResourceQuotaResourceName(k, resPath)...)
		allErrs = append(allErrs, ValidateResourceQuantityValue(k, v, resPath)...)
	}
	return allErrs
}

// ValidateNamespace 校验 Namespace 的必填字段是否设置。
func ValidateNamespace(namespace *core.Namespace) field.ErrorList {
	allErrs := ValidateObjectMeta(&namespace.ObjectMeta, false, ValidateNamespaceName, field.NewPath("metadata"))
	for i := range namespace.Spec.Finalizers {
		allErrs = append(allErrs, validateFinalizerName(string(namespace.Spec.Finalizers[i]), field.NewPath("spec", "finalizers"))...)
	}
	return allErrs
}

// validateFinalizerName 校验 finalizer 名称是否合法。
func validateFinalizerName(stringValue string, fldPath *field.Path) field.ErrorList {
	allErrs := apimachineryvalidation.ValidateFinalizerName(stringValue, fldPath)
	allErrs = append(allErrs, validateKubeFinalizerName(stringValue, fldPath)...)
	return allErrs
}

// validateKubeFinalizerName 校验 finalizer 是否使用了遗留的“标准”名称（kubernetes.io 前缀）。
func validateKubeFinalizerName(stringValue string, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	if len(strings.Split(stringValue, "/")) == 1 {
		if !helper.IsStandardFinalizerName(stringValue) {
			return append(allErrs, field.Invalid(fldPath, stringValue, "name is neither a standard finalizer name nor is it fully qualified"))
		}
	}

	return allErrs
}

// ValidateNamespaceUpdate 校验 Namespace 更新是否可以应用。
func ValidateNamespaceUpdate(newNamespace *core.Namespace, oldNamespace *core.Namespace) field.ErrorList {
	allErrs := ValidateObjectMetaUpdate(&newNamespace.ObjectMeta, &oldNamespace.ObjectMeta, field.NewPath("metadata"))
	return allErrs
}

// ValidateNamespaceStatusUpdate 校验终端用户对 Namespace 状态的更新是否合法。
func ValidateNamespaceStatusUpdate(newNamespace, oldNamespace *core.Namespace) field.ErrorList {
	allErrs := ValidateObjectMetaUpdate(&newNamespace.ObjectMeta, &oldNamespace.ObjectMeta, field.NewPath("metadata"))
	if newNamespace.DeletionTimestamp.IsZero() {
		if newNamespace.Status.Phase != core.NamespaceActive {
			allErrs = append(allErrs, field.Invalid(field.NewPath("status", "Phase"), newNamespace.Status.Phase, "may only be 'Active' if `deletionTimestamp` is empty"))
		}
	} else {
		if newNamespace.Status.Phase != core.NamespaceTerminating {
			allErrs = append(allErrs, field.Invalid(field.NewPath("status", "Phase"), newNamespace.Status.Phase, "may only be 'Terminating' if `deletionTimestamp` is not empty"))
		}
	}
	return allErrs
}

// ValidateNamespaceFinalizeUpdate 校验终端用户对 Namespace finalize 操作的更新是否合法。
func ValidateNamespaceFinalizeUpdate(newNamespace, oldNamespace *core.Namespace) field.ErrorList {
	allErrs := ValidateObjectMetaUpdate(&newNamespace.ObjectMeta, &oldNamespace.ObjectMeta, field.NewPath("metadata"))

	fldPath := field.NewPath("spec", "finalizers")
	for i := range newNamespace.Spec.Finalizers {
		idxPath := fldPath.Index(i)
		allErrs = append(allErrs, validateFinalizerName(string(newNamespace.Spec.Finalizers[i]), idxPath)...)
	}
	return allErrs
}

// ValidateEndpoints 校验 Endpoints 在创建与更新场景下的合法性。
func ValidateEndpoints(endpoints, oldEndpoints *core.Endpoints) field.ErrorList {
	allErrs := ValidateObjectMeta(&endpoints.ObjectMeta, true, ValidateEndpointsName, field.NewPath("metadata"))
	allErrs = append(allErrs, ValidateEndpointsSpecificAnnotations(endpoints.Annotations, field.NewPath("annotations"))...)

	subsetErrs := validateEndpointSubsets(endpoints.Subsets, field.NewPath("subsets"))
	if len(subsetErrs) != 0 {
		// 若这是一次更新且 Subsets 未发生变化，则忽略这些错误：
		// 老版本 Kubernetes 曾认为这些数据合法，不应因为升级而导致存量对象无法更新。
		// 只有先拿到错误才做 DeepEqual，是因为 Endpoints 可能很大、比较开销高；
		// 没有错误时无需付出这份代价。
		if oldEndpoints != nil && apiequality.Semantic.DeepEqual(oldEndpoints.Subsets, endpoints.Subsets) {
			subsetErrs = nil
		}
	}
	allErrs = append(allErrs, subsetErrs...)

	return allErrs
}

// ValidateEndpointsCreate 校验 Endpoints 创建时的合法性。
func ValidateEndpointsCreate(endpoints *core.Endpoints) field.ErrorList {
	return ValidateEndpoints(endpoints, nil)
}

// ValidateEndpointsUpdate 校验 Endpoints 更新时的合法性；允许 NodeName 变化以适配 nodeIP/PodCIDR 被复用的场景。
func ValidateEndpointsUpdate(newEndpoints, oldEndpoints *core.Endpoints) field.ErrorList {
	allErrs := ValidateObjectMetaUpdate(&newEndpoints.ObjectMeta, &oldEndpoints.ObjectMeta, field.NewPath("metadata"))
	allErrs = append(allErrs, ValidateEndpoints(newEndpoints, oldEndpoints)...)
	return allErrs
}

// validateEndpointSubsets 校验 EndpointSubset 列表：地址、端口及就绪/未就绪地址的分组是否合法。
func validateEndpointSubsets(subsets []core.EndpointSubset, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	for i := range subsets {
		ss := &subsets[i]
		idxPath := fldPath.Index(i)

		// EndpointSubsets 必须包含端点地址。对于 headless service，允许其端点不配置端口。
		if len(ss.Addresses) == 0 && len(ss.NotReadyAddresses) == 0 {
			// TODO: 此类及类似场景可考虑补充 RequiredOneOf() 错误
			allErrs = append(allErrs, field.Required(idxPath, "must specify `addresses` or `notReadyAddresses`"))
		}
		for addr := range ss.Addresses {
			allErrs = append(allErrs, validateEndpointAddress(&ss.Addresses[addr], idxPath.Child("addresses").Index(addr))...)
		}
		for addr := range ss.NotReadyAddresses {
			allErrs = append(allErrs, validateEndpointAddress(&ss.NotReadyAddresses[addr], idxPath.Child("notReadyAddresses").Index(addr))...)
		}
		for port := range ss.Ports {
			allErrs = append(allErrs, validateEndpointPort(&ss.Ports[port], len(ss.Ports) > 1, idxPath.Child("ports").Index(port))...)
		}
	}

	return allErrs
}

// validateEndpointAddress 校验单个 EndpointAddress：IP、hostname、nodeName 与 targetRef 是否合法。
func validateEndpointAddress(address *core.EndpointAddress, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	allErrs = append(allErrs, IsValidIPForLegacyField(fldPath.Child("ip"), address.IP, nil)...)
	if len(address.Hostname) > 0 {
		allErrs = append(allErrs, ValidateDNS1123Label(address.Hostname, fldPath.Child("hostname"))...)
	}
	// 在端点更新期间，校验 NodeName 是 DNS 子域名，且状态转换规则允许本次更新
	if address.NodeName != nil {
		for _, msg := range ValidateNodeName(*address.NodeName, false) {
			allErrs = append(allErrs, field.Invalid(fldPath.Child("nodeName"), *address.NodeName, msg).WithOrigin("format=dns-label"))
		}
	}
	allErrs = append(allErrs, ValidateEndpointIP(address.IP, fldPath.Child("ip"))...)
	return allErrs
}

// ValidateEndpointIP 校验 Endpoints 与 EndpointSlice 地址以及（历史原因）外部 IP 的合法性；禁止某些在此类场景下无意义地址类型。
func ValidateEndpointIP(ipAddress string, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	ip := netutils.ParseIPSloppy(ipAddress)
	if ip == nil {
		allErrs = append(allErrs, field.Invalid(fldPath, ipAddress, "must be a valid IP address").WithOrigin("format=ip-sloppy"))
		return allErrs
	}
	if ip.IsUnspecified() {
		allErrs = append(allErrs, field.Invalid(fldPath, ipAddress, fmt.Sprintf("may not be unspecified (%v)", ipAddress)))
	}
	if ip.IsLoopback() {
		allErrs = append(allErrs, field.Invalid(fldPath, ipAddress, "may not be in the loopback range (127.0.0.0/8, ::1/128)"))
	}
	if ip.IsLinkLocalUnicast() {
		allErrs = append(allErrs, field.Invalid(fldPath, ipAddress, "may not be in the link-local range (169.254.0.0/16, fe80::/10)"))
	}
	if ip.IsLinkLocalMulticast() {
		allErrs = append(allErrs, field.Invalid(fldPath, ipAddress, "may not be in the link-local multicast range (224.0.0.0/24, ff02::/10)"))
	}
	return allErrs.WithOrigin("format=endpoint-ip")
}

// validateEndpointPort 校验单个 EndpointPort：名称、端口与协议是否合法。
func validateEndpointPort(port *core.EndpointPort, requireName bool, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	if requireName && len(port.Name) == 0 {
		allErrs = append(allErrs, field.Required(fldPath.Child("name"), ""))
	} else if len(port.Name) != 0 {
		allErrs = append(allErrs, ValidateDNS1123Label(port.Name, fldPath.Child("name"))...)
	}
	for _, msg := range validation.IsValidPortNum(int(port.Port)) {
		allErrs = append(allErrs, field.Invalid(fldPath.Child("port"), port.Port, msg).WithOrigin("portNum"))
	}
	if len(port.Protocol) == 0 {
		allErrs = append(allErrs, field.Required(fldPath.Child("protocol"), ""))
	} else if !supportedPortProtocols.Has(port.Protocol) {
		allErrs = append(allErrs, field.NotSupported(fldPath.Child("protocol"), port.Protocol, sets.List(supportedPortProtocols)))
	}
	if port.AppProtocol != nil {
		allErrs = append(allErrs, ValidateQualifiedName(*port.AppProtocol, fldPath.Child("appProtocol"))...)
	}
	return allErrs
}

// ValidateSecurityContext 校验 SecurityContext 中的设置是否合法（capabilities、特权、用户/组、seLinuxOptions 等）。
func ValidateSecurityContext(sc *core.SecurityContext, fldPath *field.Path, hostUsers, allowSysAdminWhenPrivilegeEscalationFalse bool) field.ErrorList {
	allErrs := field.ErrorList{}
	// 该情况只应在测试中出现，因为核心组的 SecurityContext 会被默认化（defaulted）
	if sc == nil {
		return allErrs
	}

	if sc.Privileged != nil {
		if *sc.Privileged && !capabilities.Get().AllowPrivileged {
			allErrs = append(allErrs, field.Forbidden(fldPath.Child("privileged"), "disallowed by cluster policy"))
		}
	}

	if sc.RunAsUser != nil {
		for _, msg := range validation.IsValidUserID(*sc.RunAsUser) {
			allErrs = append(allErrs, field.Invalid(fldPath.Child("runAsUser"), *sc.RunAsUser, msg))
		}
	}

	if sc.RunAsGroup != nil {
		for _, msg := range validation.IsValidGroupID(*sc.RunAsGroup) {
			allErrs = append(allErrs, field.Invalid(fldPath.Child("runAsGroup"), *sc.RunAsGroup, msg))
		}
	}

	if sc.ProcMount != nil {
		if err := ValidateProcMountType(fldPath.Child("procMount"), *sc.ProcMount); err != nil {
			allErrs = append(allErrs, err)
		}
		if hostUsers && *sc.ProcMount == core.UnmaskedProcMount {
			allErrs = append(allErrs, field.Invalid(fldPath.Child("procMount"), sc.ProcMount, "`hostUsers` must be false to use `Unmasked`"))
		}

	}
	allErrs = append(allErrs, validateSeccompProfileField(sc.SeccompProfile, fldPath.Child("seccompProfile"))...)
	if sc.AllowPrivilegeEscalation != nil && !*sc.AllowPrivilegeEscalation {
		if sc.Privileged != nil && *sc.Privileged {
			allErrs = append(allErrs, field.Invalid(fldPath, sc, "cannot set `allowPrivilegeEscalation` to false and `privileged` to true"))
		}

		if !allowSysAdminWhenPrivilegeEscalationFalse && sc.Capabilities != nil {
			for _, cap := range sc.Capabilities.Add {
				if string(cap) == "CAP_SYS_ADMIN" {
					allErrs = append(allErrs, field.Invalid(fldPath, sc, "cannot set `allowPrivilegeEscalation` to false and `capabilities.Add` CAP_SYS_ADMIN"))
				}
			}
		}
	}

	allErrs = append(allErrs, validateWindowsSecurityContextOptions(sc.WindowsOptions, fldPath.Child("windowsOptions"))...)
	allErrs = append(allErrs, ValidateAppArmorProfileField(sc.AppArmorProfile, fldPath.Child("appArmorProfile"))...)

	return allErrs
}

// maxGMSACredentialSpecLength 是 GMSA cred spec 实际内容的最大长度（以字节计）。
// 一般而言其内容不会超过几百字节，因此这里在预留充足空间的同时
// 仍提供一个上限。
// runAsUserName 字段将用于执行给定容器的入口点（entrypoint），
// 其格式可以是 "DOMAIN/USER"（DOMAIN 可选）；maxRunAsUserNameDomainLength
// 是用户 DOMAIN 的最大字符长度，maxRunAsUserNameUserLength
// 是 USER 本身的最大字符长度。DOMAIN 与 USER 各有自己的
// 限制，详细信息可参考：
// https://support.microsoft.com/en-us/help/909264/naming-conventions-in-active-directory-for-computers-domains-sites-and
// https://docs.microsoft.com/en-us/previous-versions/windows/it-pro/windows-2000-server/bb726984(v=technet.10)
const (
	maxGMSACredentialSpecLengthInKiB = 64
	maxGMSACredentialSpecLength      = maxGMSACredentialSpecLengthInKiB * 1024
	maxRunAsUserNameDomainLength     = 256
	maxRunAsUserNameUserLength       = 104
)

var (
	// runAsUserName 字段不允许包含控制字符。
	ctrlRegex = regexp.MustCompile(`[[:cntrl:]]+`)

	// 合法的 NetBios Domain 名称不能以点开头，至少 1 个字符、
	// 至多 15 个字符，且不能包含字符：\ / : * ? " < > |
	validNetBiosRegex = regexp.MustCompile(`^[^\\/:\*\?"<>|\.][^\\/:\*\?"<>|]{0,14}$`)

	// 合法的 DNS 名称只包含字母数字字符、点和短横线。
	dnsLabelFormat                 = `[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?`
	dnsSubdomainFormat             = fmt.Sprintf(`^%s(?:\.%s)*$`, dnsLabelFormat, dnsLabelFormat)
	validWindowsUserDomainDNSRegex = regexp.MustCompile(dnsSubdomainFormat)

	// 若 username 包含字符：" / \ [ ] : ; | = , + * ? < > @
	// 或仅由点和空格组成，则不合法。
	invalidUserNameCharsRegex      = regexp.MustCompile(`["/\\:;|=,\+\*\?<>@\[\]]`)
	invalidUserNameDotsSpacesRegex = regexp.MustCompile(`^[\. ]+$`)
)

// validateWindowsSecurityContextOptions 校验 Windows 容器专属的安全上下文选项是否合法。
func validateWindowsSecurityContextOptions(windowsOptions *core.WindowsSecurityContextOptions, fieldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}

	if windowsOptions == nil {
		return allErrs
	}

	if windowsOptions.GMSACredentialSpecName != nil {
		// gmsaCredentialSpecName 必须是某个自定义资源的名称
		for _, msg := range validation.IsDNS1123Subdomain(*windowsOptions.GMSACredentialSpecName) {
			allErrs = append(allErrs, field.Invalid(fieldPath.Child("gmsaCredentialSpecName"), windowsOptions.GMSACredentialSpecName, msg))
		}
	}

	if windowsOptions.GMSACredentialSpec != nil {
		if l := len(*windowsOptions.GMSACredentialSpec); l == 0 {
			allErrs = append(allErrs, field.Invalid(fieldPath.Child("gmsaCredentialSpec"), windowsOptions.GMSACredentialSpec, "gmsaCredentialSpec cannot be an empty string"))
		} else if l > maxGMSACredentialSpecLength {
			errMsg := fmt.Sprintf("gmsaCredentialSpec size must be under %d KiB", maxGMSACredentialSpecLengthInKiB)
			allErrs = append(allErrs, field.Invalid(fieldPath.Child("gmsaCredentialSpec"), windowsOptions.GMSACredentialSpec, errMsg))
		}
	}

	if windowsOptions.RunAsUserName != nil {
		if l := len(*windowsOptions.RunAsUserName); l == 0 {
			allErrs = append(allErrs, field.Invalid(fieldPath.Child("runAsUserName"), windowsOptions.RunAsUserName, "runAsUserName cannot be an empty string"))
		} else if ctrlRegex.MatchString(*windowsOptions.RunAsUserName) {
			errMsg := "runAsUserName cannot contain control characters"
			allErrs = append(allErrs, field.Invalid(fieldPath.Child("runAsUserName"), windowsOptions.RunAsUserName, errMsg))
		} else if parts := strings.Split(*windowsOptions.RunAsUserName, "\\"); len(parts) > 2 {
			errMsg := "runAsUserName cannot contain more than one backslash"
			allErrs = append(allErrs, field.Invalid(fieldPath.Child("runAsUserName"), windowsOptions.RunAsUserName, errMsg))
		} else {
			var (
				hasDomain = false
				domain    = ""
				user      string
			)
			if len(parts) == 1 {
				user = parts[0]
			} else {
				hasDomain = true
				domain = parts[0]
				user = parts[1]
			}

			if len(domain) >= maxRunAsUserNameDomainLength {
				errMsg := fmt.Sprintf("runAsUserName's Domain length must be under %d characters", maxRunAsUserNameDomainLength)
				allErrs = append(allErrs, field.Invalid(fieldPath.Child("runAsUserName"), windowsOptions.RunAsUserName, errMsg))
			}

			if hasDomain && !(validNetBiosRegex.MatchString(domain) || validWindowsUserDomainDNSRegex.MatchString(domain)) {
				errMsg := "runAsUserName's Domain doesn't match the NetBios nor the DNS format"
				allErrs = append(allErrs, field.Invalid(fieldPath.Child("runAsUserName"), windowsOptions.RunAsUserName, errMsg))
			}

			if l := len(user); l == 0 {
				errMsg := "runAsUserName's User cannot be empty"
				allErrs = append(allErrs, field.Invalid(fieldPath.Child("runAsUserName"), windowsOptions.RunAsUserName, errMsg))
			} else if l > maxRunAsUserNameUserLength {
				errMsg := fmt.Sprintf("runAsUserName's User length must not be longer than %d characters", maxRunAsUserNameUserLength)
				allErrs = append(allErrs, field.Invalid(fieldPath.Child("runAsUserName"), windowsOptions.RunAsUserName, errMsg))
			}

			if invalidUserNameDotsSpacesRegex.MatchString(user) {
				errMsg := `runAsUserName's User cannot contain only periods or spaces`
				allErrs = append(allErrs, field.Invalid(fieldPath.Child("runAsUserName"), windowsOptions.RunAsUserName, errMsg))
			}

			if invalidUserNameCharsRegex.MatchString(user) {
				errMsg := `runAsUserName's User cannot contain the following characters: "/\:;|=,+*?<>@[]`
				allErrs = append(allErrs, field.Invalid(fieldPath.Child("runAsUserName"), windowsOptions.RunAsUserName, errMsg))
			}
		}
	}

	return allErrs
}

// validateWindowsHostProcessPod 校验 Windows HostProcess Pod 的相关字段配置是否合法。
func validateWindowsHostProcessPod(podSpec *core.PodSpec, fieldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}

	// 记录容器总数与 hostProcess 容器数，供校验使用
	containerCount := 0
	hostProcessContainerCount := 0

	var podHostProcess *bool
	if podSpec.SecurityContext != nil && podSpec.SecurityContext.WindowsOptions != nil {
		podHostProcess = podSpec.SecurityContext.WindowsOptions.HostProcess
	}

	hostNetwork := podSpec.HostNetwork

	podshelper.VisitContainersWithPath(podSpec, fieldPath, func(c *core.Container, cFieldPath *field.Path) bool {
		containerCount++

		var containerHostProcess *bool = nil
		if c.SecurityContext != nil && c.SecurityContext.WindowsOptions != nil {
			containerHostProcess = c.SecurityContext.WindowsOptions.HostProcess
		}

		if podHostProcess != nil && containerHostProcess != nil && *podHostProcess != *containerHostProcess {
			errMsg := fmt.Sprintf("pod hostProcess value must be identical if both are specified, was %v", *podHostProcess)
			allErrs = append(allErrs, field.Invalid(cFieldPath.Child("securityContext", "windowsOptions", "hostProcess"), *containerHostProcess, errMsg))
		}

		switch {
		case containerHostProcess != nil && *containerHostProcess:
			// 容器显式设置了 hostProcess=true
			hostProcessContainerCount++
		case containerHostProcess == nil && podHostProcess != nil && *podHostProcess:
			// 容器从 Pod 设置继承了 hostProcess=true
			hostProcessContainerCount++
		}

		return true
	})

	if hostProcessContainerCount > 0 {
		// 目前，若 Windows Pod 包含任何 HostProcess 容器，则所有容器都必须
		// 是 HostProcess 容器（显式设置或继承）。
		if hostProcessContainerCount != containerCount {
			errMsg := "If pod contains any hostProcess containers then all containers must be HostProcess containers"
			allErrs = append(allErrs, field.Invalid(fieldPath, "", errMsg))
		}

		// 目前，包含 HostProcess 容器的 Windows Pod 还必须设置 HostNetwork。
		if !hostNetwork {
			errMsg := "hostNetwork must be true if pod contains any hostProcess containers"
			allErrs = append(allErrs, field.Invalid(fieldPath.Child("hostNetwork"), hostNetwork, errMsg))
		}

		if !capabilities.Get().AllowPrivileged {
			errMsg := "hostProcess containers are disallowed by cluster policy"
			allErrs = append(allErrs, field.Forbidden(fieldPath, errMsg))
		}
	}

	return allErrs
}

// validateOS 校验 PodSpec 中的 OS 字段是否合法。
func validateOS(podSpec *core.PodSpec, fldPath *field.Path, opts PodValidationOptions) field.ErrorList {
	allErrs := field.ErrorList{}
	os := podSpec.OS
	if os == nil {
		return allErrs
	}
	if len(os.Name) == 0 {
		return append(allErrs, field.Required(fldPath.Child("name"), ""))
	}
	if !validOS.Has(os.Name) {
		allErrs = append(allErrs, field.NotSupported(fldPath, os.Name, sets.List(validOS)))
	}
	return allErrs
}

var validLogStreams = sets.New[string](
	core.LogStreamStdout,
	core.LogStreamStderr,
	core.LogStreamAll,
)

// ValidatePodLogOptions 校验 Pod 日志请求选项：容器、跟随、尾部行数、时间戳等参数是否合法且互斥。
func ValidatePodLogOptions(opts *core.PodLogOptions, allowStreamSelection bool) field.ErrorList {
	allErrs := field.ErrorList{}
	if opts.TailLines != nil && *opts.TailLines < 0 {
		allErrs = append(allErrs, field.Invalid(field.NewPath("tailLines"), *opts.TailLines, isNegativeErrorMsg))
	}
	if opts.LimitBytes != nil && *opts.LimitBytes < 1 {
		allErrs = append(allErrs, field.Invalid(field.NewPath("limitBytes"), *opts.LimitBytes, "must be greater than 0"))
	}
	switch {
	case opts.SinceSeconds != nil && opts.SinceTime != nil:
		allErrs = append(allErrs, field.Forbidden(field.NewPath(""), "at most one of `sinceTime` or `sinceSeconds` may be specified"))
	case opts.SinceSeconds != nil:
		if *opts.SinceSeconds < 1 {
			allErrs = append(allErrs, field.Invalid(field.NewPath("sinceSeconds"), *opts.SinceSeconds, "must be greater than 0"))
		}
	}
	if allowStreamSelection {
		if opts.Stream == nil {
			allErrs = append(allErrs, field.Required(field.NewPath("stream"), "must be specified"))
		} else {
			if !validLogStreams.Has(*opts.Stream) {
				allErrs = append(allErrs, field.NotSupported(field.NewPath("stream"), *opts.Stream, validLogStreams.UnsortedList()))
			}
			if *opts.Stream != core.LogStreamAll && opts.TailLines != nil {
				allErrs = append(allErrs, field.Forbidden(field.NewPath(""), "`tailLines` and specific `stream` are mutually exclusive for now"))
			}
		}
	} else if opts.Stream != nil {
		allErrs = append(allErrs, field.Forbidden(field.NewPath("stream"), "may not be specified"))
	}
	return allErrs
}

var (
	supportedLoadBalancerIPMode = sets.New(core.LoadBalancerIPModeVIP, core.LoadBalancerIPModeProxy)
)

// ValidateLoadBalancerStatus 校验 LoadBalancerStatus 的必填字段是否合法。
func ValidateLoadBalancerStatus(status, oldStatus *core.LoadBalancerStatus, fldPath *field.Path, spec *core.ServiceSpec) field.ErrorList {
	allErrs := field.ErrorList{}
	ingrPath := fldPath.Child("ingress")
	if spec.Type != core.ServiceTypeLoadBalancer && len(status.Ingress) != 0 {
		allErrs = append(allErrs, field.Forbidden(ingrPath, "may only be used when `spec.type` is 'LoadBalancer'"))
	} else {
		var existingIngressIPs []string
		if oldStatus != nil {
			existingIngressIPs = make([]string, 0, len(oldStatus.Ingress))
			for _, ingress := range oldStatus.Ingress {
				if len(ingress.IP) > 0 {
					existingIngressIPs = append(existingIngressIPs, ingress.IP)
				}
			}
		}
		for i, ingress := range status.Ingress {
			idxPath := ingrPath.Index(i)
			if len(ingress.IP) > 0 {
				allErrs = append(allErrs, IsValidIPForLegacyField(idxPath.Child("ip"), ingress.IP, existingIngressIPs)...)
			}

			if ingress.IPMode == nil {
				if len(ingress.IP) > 0 {
					allErrs = append(allErrs, field.Required(idxPath.Child("ipMode"), "must be specified when `ip` is set"))
				}
			} else if ingress.IPMode != nil && len(ingress.IP) == 0 {
				allErrs = append(allErrs, field.Forbidden(idxPath.Child("ipMode"), "may not be specified when `ip` is not set"))
			} else if ingress.IPMode != nil && !supportedLoadBalancerIPMode.Has(*ingress.IPMode) {
				allErrs = append(allErrs, field.NotSupported(idxPath.Child("ipMode"), ingress.IPMode, sets.List(supportedLoadBalancerIPMode)))
			}

			if len(ingress.Hostname) > 0 {
				for _, msg := range validation.IsDNS1123Subdomain(ingress.Hostname) {
					allErrs = append(allErrs, field.Invalid(idxPath.Child("hostname"), ingress.Hostname, msg))
				}
				if isIP := netutils.ParseIPSloppy(ingress.Hostname) != nil; isIP {
					allErrs = append(allErrs, field.Invalid(idxPath.Child("hostname"), ingress.Hostname, "must be a DNS name, not an IP address"))
				}
			}
		}
	}
	return allErrs
}

// validateVolumeNodeAffinity 校验 PV.NodeAffinity 数据；返回是否已设置及错误列表。
func validateVolumeNodeAffinity(nodeAffinity *core.VolumeNodeAffinity, opts PersistentVolumeSpecValidationOptions, fldPath *field.Path) (bool, field.ErrorList) {
	allErrs := field.ErrorList{}

	if nodeAffinity == nil {
		return false, allErrs
	}

	if nodeAffinity.Required != nil {
		allErrs = append(allErrs, ValidateNodeSelector(nodeAffinity.Required, opts.AllowInvalidLabelValueInRequiredNodeAffinity, fldPath.Child("required"))...)
	} else {
		allErrs = append(allErrs, field.Required(fldPath.Child("required"), "must specify required node constraints"))
	}

	return true, allErrs
}

// IsDecremented 判断更新值是否小于旧值（用于数值只能递增的校验场景）。
func IsDecremented(update, old *int32) bool {
	if update == nil && old != nil {
		return true
	}
	if update == nil || old == nil {
		return false
	}
	return *update < *old
}

// ValidateProcMountType 校验参数是否为合法的 ProcMountType。
func ValidateProcMountType(fldPath *field.Path, procMountType core.ProcMountType) *field.Error {
	switch procMountType {
	case core.DefaultProcMount, core.UnmaskedProcMount:
		return nil
	default:
		return field.NotSupported(fldPath, procMountType, []core.ProcMountType{core.DefaultProcMount, core.UnmaskedProcMount})
	}
}

var (
	supportedScheduleActions = sets.New(core.DoNotSchedule, core.ScheduleAnyway)
)

// validateTopologySpreadConstraints 校验给定的 TopologySpreadConstraints 列表是否合法。
func validateTopologySpreadConstraints(constraints []core.TopologySpreadConstraint, fldPath *field.Path, opts PodValidationOptions) field.ErrorList {
	allErrs := field.ErrorList{}

	for i, constraint := range constraints {
		subFldPath := fldPath.Index(i)
		if errs := ValidatePositiveField(int64(constraint.MaxSkew), subFldPath.Child("maxSkew")); len(errs) > 0 {
			allErrs = append(allErrs, errs...)
		}
		if err := ValidateTopologyKey(subFldPath.Child("topologyKey"), constraint.TopologyKey); err != nil {
			allErrs = append(allErrs, err)
		}
		if err := ValidateWhenUnsatisfiable(subFldPath.Child("whenUnsatisfiable"), constraint.WhenUnsatisfiable); err != nil {
			allErrs = append(allErrs, err)
		}
		// 元组 {topologyKey, whenUnsatisfiable} 表示一类 spread 约束
		if err := ValidateSpreadConstraintNotRepeat(subFldPath.Child("{topologyKey, whenUnsatisfiable}"), constraint, constraints[i+1:]); err != nil {
			allErrs = append(allErrs, err)
		}
		allErrs = append(allErrs, validateMinDomains(subFldPath.Child("minDomains"), constraint.MinDomains, constraint.WhenUnsatisfiable)...)
		if err := validateNodeInclusionPolicy(subFldPath.Child("nodeAffinityPolicy"), constraint.NodeAffinityPolicy); err != nil {
			allErrs = append(allErrs, err)
		}
		if err := validateNodeInclusionPolicy(subFldPath.Child("nodeTaintsPolicy"), constraint.NodeTaintsPolicy); err != nil {
			allErrs = append(allErrs, err)
		}
		// legacyValidationFunction 指 ValidateMatchLabelKeysInTopologySpread
		// preferredValidationFunction 指 ValidateMatchLabelKeysAndMismatchLabelKeys
		// OldPodViolatesMatchLabelKeysValidation==true 表示 ValidateMatchLabelKeysAndMismatchLabelKeys 失败，即 preferredValidation 失败
		// OldPodViolatesLegacyMatchLabelKeysValidation==true 表示 ValidateMatchLabelKeysInTopologySpread 失败，即 legacyValidation 失败
		if opts.AllowMatchLabelKeysInPodTopologySpread {
			switch {
			case opts.AllowMatchLabelKeysInPodTopologySpreadSelectorMerge && opts.OldPodViolatesMatchLabelKeysValidation:
				// 该情形表示我们希望使用 preferredValidationFunction，但旧 Pod 未通过
				// preferredValidationFunction，因此必须继续使用 legacyValidationFunction。
				// 这是因为我们不允许这些字段发生变化。
				allErrs = append(allErrs, ValidateMatchLabelKeysInTopologySpread(subFldPath.Child("matchLabelKeys"), constraint.MatchLabelKeys, constraint.LabelSelector)...)
			case opts.AllowMatchLabelKeysInPodTopologySpreadSelectorMerge && !opts.OldPodViolatesMatchLabelKeysValidation:
				// 该情形表示我们希望使用 preferredValidationFunction，且旧 Pod 通过了它，
				// 因此将继续要求 preferredValidationFunction 通过。
				allErrs = append(allErrs, ValidateMatchLabelKeysAndMismatchLabelKeys(subFldPath, constraint.MatchLabelKeys, nil, constraint.LabelSelector)...)
			case !opts.AllowMatchLabelKeysInPodTopologySpreadSelectorMerge && opts.OldPodViolatesLegacyMatchLabelKeysValidation:
				// 该情形表示我们希望使用 legacyValidationFunction，但旧 Pod 未通过它，
				// 因此必须改用 preferredValidationFunction，以便其他字段的更新能够进行。
				// 这样即使先启用再禁用特性门控，我们仍然能够更新该 Pod。
				allErrs = append(allErrs, ValidateMatchLabelKeysAndMismatchLabelKeys(subFldPath, constraint.MatchLabelKeys, nil, constraint.LabelSelector)...)
			case !opts.AllowMatchLabelKeysInPodTopologySpreadSelectorMerge && !opts.OldPodViolatesLegacyMatchLabelKeysValidation:
				// 该情形表示我们希望使用 legacyValidationFunction，且旧 Pod 通过了它，
				// 因此将继续要求 legacyValidationFunction 通过。
				allErrs = append(allErrs, ValidateMatchLabelKeysInTopologySpread(subFldPath.Child("matchLabelKeys"), constraint.MatchLabelKeys, constraint.LabelSelector)...)
			default:
				// 若走到兜底分支，则使用 legacyValidationFunction，因为这是引入
				// 特性门控（MatchLabelKeysInPodTopologySpreadSelectorMerge）之前的做法。
				allErrs = append(allErrs, ValidateMatchLabelKeysInTopologySpread(subFldPath.Child("matchLabelKeys"), constraint.MatchLabelKeys, constraint.LabelSelector)...)
			}
		} else {
			allErrs = append(allErrs, ValidateMatchLabelKeysInTopologySpread(subFldPath.Child("matchLabelKeys"), constraint.MatchLabelKeys, constraint.LabelSelector)...)
		}
		if !opts.AllowInvalidTopologySpreadConstraintLabelSelector {
			allErrs = append(allErrs, unversionedvalidation.ValidateLabelSelector(constraint.LabelSelector, unversionedvalidation.LabelSelectorValidationOptions{AllowInvalidLabelValueInSelector: false}, subFldPath.Child("labelSelector"))...)
		}
	}

	return allErrs
}

// validateMinDomains 校验 minDomains 参数是否合法。
func validateMinDomains(fldPath *field.Path, minDomains *int32, action core.UnsatisfiableConstraintAction) field.ErrorList {
	if minDomains == nil {
		return nil
	}
	var allErrs field.ErrorList
	allErrs = append(allErrs, ValidatePositiveField(int64(*minDomains), fldPath)...)
	// 当 MinDomains 非 nil 时，whenUnsatisfiable 必须为 DoNotSchedule。
	if action != core.DoNotSchedule {
		allErrs = append(allErrs, field.Invalid(fldPath, minDomains, fmt.Sprintf("can only use minDomains if whenUnsatisfiable=%s, not %s", core.DoNotSchedule, action)).WithOrigin("dependsOn"))
	}
	return allErrs
}

// ValidateTopologyKey 校验参数是否为合法的 TopologyKey。
func ValidateTopologyKey(fldPath *field.Path, topologyKey string) *field.Error {
	if len(topologyKey) == 0 {
		return field.Required(fldPath, "can not be empty")
	}
	return nil
}

// ValidateWhenUnsatisfiable 校验参数是否为合法的 UnsatisfiableConstraintAction。
func ValidateWhenUnsatisfiable(fldPath *field.Path, action core.UnsatisfiableConstraintAction) *field.Error {
	if !supportedScheduleActions.Has(action) {
		return field.NotSupported(fldPath, action, sets.List(supportedScheduleActions))
	}
	return nil
}

// ValidateSpreadConstraintNotRepeat 校验给定约束是否在 TopologyKey 与 WhenUnsatisfiable 字段上与已有约束重复。
func ValidateSpreadConstraintNotRepeat(fldPath *field.Path, constraint core.TopologySpreadConstraint, restingConstraints []core.TopologySpreadConstraint) *field.Error {
	for _, restingConstraint := range restingConstraints {
		if constraint.TopologyKey == restingConstraint.TopologyKey &&
			constraint.WhenUnsatisfiable == restingConstraint.WhenUnsatisfiable {
			return field.Duplicate(fldPath, fmt.Sprintf("{%v, %v}", constraint.TopologyKey, constraint.WhenUnsatisfiable))
		}
	}
	return nil
}

var (
	supportedPodTopologySpreadNodePolicies = sets.New(core.NodeInclusionPolicyIgnore, core.NodeInclusionPolicyHonor)
)

// validateNodeInclusionPolicy 校验参数是否为合法的 NodeInclusionPolicy。
func validateNodeInclusionPolicy(fldPath *field.Path, policy *core.NodeInclusionPolicy) *field.Error {
	if policy == nil {
		return nil
	}

	if !supportedPodTopologySpreadNodePolicies.Has(*policy) {
		return field.NotSupported(fldPath, policy, sets.List(supportedPodTopologySpreadNodePolicies))
	}
	return nil
}

// ValidateMatchLabelKeysAndMismatchLabelKeys 校验 matchLabelKeys 与 mismatchLabelKeys：均为合法标签名，且同一键不能同时出现在两者中。
func ValidateMatchLabelKeysAndMismatchLabelKeys(fldPath *field.Path, matchLabelKeys, mismatchLabelKeys []string, labelSelector *metav1.LabelSelector) field.ErrorList {
	var allErrs field.ErrorList
	// 1. 校验所有 matchLabelKeys 与 mismatchLabelKeys 均为合法的标签名。
	allErrs = append(allErrs, validateLabelKeys(fldPath.Child("matchLabelKeys"), matchLabelKeys, labelSelector)...)
	allErrs = append(allErrs, validateLabelKeys(fldPath.Child("mismatchLabelKeys"), mismatchLabelKeys, labelSelector)...)

	// 2. 校验用户不能同时在 matchLabelKeys 和 labelSelector 中指定同一个键。
	// 让 labelSelector 含有 matchLabelKeys 中指定的键没有意义，
	// 因为 matchLabelKeys 会变成 `In` 的 labelSelector，对该键只匹配单一取值，
	// 我们无法再用该键做进一步过滤。
	// 另一方面，我们可能希望 labelSelector 中含有 mismatchLabelKeys 指定的键，
	// 因为 mismatchLabelKeys 会变成 `NotIn` 的 labelSelector，
	// 我们可能想用该键的其他 labelSelector 条件进一步过滤 Pod。

	// labelKeysMap 以标签键为 key，值为该标签键在 labelKeys 中的下标。
	if labelSelector != nil {
		labelKeysMap := map[string]int{}
		for i, key := range matchLabelKeys {
			labelKeysMap[key] = i
		}
		labelSelectorKeys := sets.New[string]()
		for key := range labelSelector.MatchLabels {
			labelSelectorKeys.Insert(key)
		}
		for _, matchExpression := range labelSelector.MatchExpressions {
			key := matchExpression.Key
			if i, ok := labelKeysMap[key]; ok && labelSelectorKeys.Has(key) {
				// 在调用 validateLabelKeysWithSelector 之前，labelSelector 已经包含了
				// 由 matchLabelKeys 生成的 selector。
				// 这里发现 labelSelector 中存在重复键，且该键也在 labelKeys 中被指定，
				// 即同一个键同时出现在 labelSelector 和 matchLabelKeys/mismatchLabelKeys 中。
				allErrs = append(allErrs, field.Invalid(fldPath.Index(i), key, "exists in both matchLabelKeys and labelSelector").WithOrigin("duplicatedLabelKeys"))
			}

			labelSelectorKeys.Insert(key)
		}
	}

	// 3. 校验 matchLabelKeys 与 mismatchLabelKeys 之间没有重复的键。
	mismatchLabelKeysSet := sets.New(mismatchLabelKeys...)
	for i, k := range matchLabelKeys {
		if mismatchLabelKeysSet.Has(k) {
			allErrs = append(allErrs, field.Invalid(fldPath.Child("matchLabelKeys").Index(i), k, "exists in both matchLabelKeys and mismatchLabelKeys").WithOrigin("duplicatedMismatchLabelKeys"))
		}
	}

	return allErrs
}

// ValidateMatchLabelKeysInTopologySpread 校验拓扑分布约束中 matchLabelKeys 的元素为合法标签名且未包含在 labelSelector 中。
func ValidateMatchLabelKeysInTopologySpread(fldPath *field.Path, matchLabelKeys []string, labelSelector *metav1.LabelSelector) field.ErrorList {
	if len(matchLabelKeys) == 0 {
		return nil
	}

	var allErrs field.ErrorList
	labelSelectorKeys := sets.Set[string]{}

	if labelSelector != nil {
		for key := range labelSelector.MatchLabels {
			labelSelectorKeys.Insert(key)
		}
		for _, matchExpression := range labelSelector.MatchExpressions {
			labelSelectorKeys.Insert(matchExpression.Key)
		}
	} else {
		allErrs = append(allErrs, field.Forbidden(fldPath, "must not be specified when labelSelector is not set"))
	}

	for i, key := range matchLabelKeys {
		allErrs = append(allErrs, unversionedvalidation.ValidateLabelName(key, fldPath.Index(i))...)
		if labelSelectorKeys.Has(key) {
			allErrs = append(allErrs, field.Invalid(fldPath.Index(i), key, "exists in both matchLabelKeys and labelSelector").WithOrigin("duplicatedLabelKeys"))
		}
	}

	return allErrs
}

// validateLabelKeys 校验标签键列表是否为合法标签名；用于 matchLabelKeys 或 mismatchLabelKeys。
func validateLabelKeys(fldPath *field.Path, labelKeys []string, labelSelector *metav1.LabelSelector) field.ErrorList {
	if len(labelKeys) == 0 {
		return nil
	}

	if labelSelector == nil {
		return field.ErrorList{field.Forbidden(fldPath, "must not be specified when labelSelector is not set")}
	}

	var allErrs field.ErrorList
	for i, key := range labelKeys {
		allErrs = append(allErrs, unversionedvalidation.ValidateLabelName(key, fldPath.Index(i))...)
	}

	return allErrs
}

// ValidateServiceClusterIPsRelatedFields 校验 .spec.ClusterIPs、.spec.IPFamilies、.spec.ipFamilyPolicy；因 IP 初始化与分配需要使用而导出。
func ValidateServiceClusterIPsRelatedFields(service, oldService *core.Service) field.ErrorList {
	// ExternalName 类型的 service，其 ClusterIP、ClusterIPs、IPFamilyPolicy 与 IPFamilies
	// 已在更早处校验过（必须全部未设置）
	if service.Spec.Type == core.ServiceTypeExternalName {
		return field.ErrorList{}
	}

	allErrs := field.ErrorList{}
	hasInvalidIPs := false

	specPath := field.NewPath("spec")
	clusterIPsField := specPath.Child("clusterIPs")
	ipFamiliesField := specPath.Child("ipFamilies")
	ipFamilyPolicyField := specPath.Child("ipFamilyPolicy")

	// 确保 ClusterIP 与 ClusterIPs 保持同步。大多数情况下用户只需
	// 管理其中之一，其余由我们处理（见 strategy 中的 PrepareFor*）。
	if len(service.Spec.ClusterIP) != 0 {
		// 若设置了 ClusterIP，则 ClusterIPs[0] 必须与之相同。
		if len(service.Spec.ClusterIPs) == 0 {
			allErrs = append(allErrs, field.Required(clusterIPsField, ""))
		} else if service.Spec.ClusterIPs[0] != service.Spec.ClusterIP {
			allErrs = append(allErrs, field.Invalid(clusterIPsField, service.Spec.ClusterIPs, "first value must match `clusterIP`"))
		}
	} else { // ClusterIP == ""
		// 若未设置 ClusterIP，ClusterIPs 也必须为空。
		if len(service.Spec.ClusterIPs) != 0 {
			allErrs = append(allErrs, field.Invalid(clusterIPsField, service.Spec.ClusterIPs, "must be empty when `clusterIP` is not specified"))
		}
	}

	// ipfamilies 独立校验
	// 必须是 IPv4 或 IPv6
	seen := sets.Set[core.IPFamily]{}
	for i, ipFamily := range service.Spec.IPFamilies {
		if !supportedServiceIPFamily.Has(ipFamily) {
			allErrs = append(allErrs, field.NotSupported(ipFamiliesField.Index(i), ipFamily, sets.List(supportedServiceIPFamily)))
		}
		// 重复性检查同时确保 ipfamilies 构成双栈（任意顺序）
		if seen.Has(ipFamily) {
			allErrs = append(allErrs, field.Duplicate(ipFamiliesField.Index(i), ipFamily))
		}
		seen.Insert(ipFamily)
	}

	// IPFamilyPolicy 独立校验
	// 注意：nil 是允许的，在 registry/core/service/* 的分配检查中会被默认化
	if service.Spec.IPFamilyPolicy != nil {
		// 必须是受支持的取值
		if !supportedServiceIPFamilyPolicy.Has(*(service.Spec.IPFamilyPolicy)) {
			allErrs = append(allErrs, field.NotSupported(ipFamilyPolicyField, service.Spec.IPFamilyPolicy, sets.List(supportedServiceIPFamilyPolicy)))
		}
	}

	var existingClusterIPs []string
	if oldService != nil {
		existingClusterIPs = oldService.Spec.ClusterIPs // +k8s:verify-mutation:reason=clone
	}

	// clusterIPs 独立校验
	// 校验合法 IP，并处理 None 与空字符串
	// 重复性检查在下面的 DualStackvalidation 中完成
	for i, clusterIP := range service.Spec.ClusterIPs {
		// 仅在第一处合法，且当且仅当 len(clusterIPs) == 1
		if i == 0 && clusterIP == core.ClusterIPNone {
			if len(service.Spec.ClusterIPs) > 1 {
				hasInvalidIPs = true
				allErrs = append(allErrs, field.Invalid(clusterIPsField, service.Spec.ClusterIPs, "'None' must be the first and only value"))
			}
			continue
		}

		// 是合法 IP 吗？（或者此前至少曾被认为是合法的？）
		errorMessages := IsValidIPForLegacyField(clusterIPsField.Index(i), clusterIP, existingClusterIPs)
		hasInvalidIPs = (len(errorMessages) != 0) || hasInvalidIPs
		allErrs = append(allErrs, errorMessages...)
	}

	// 最多两个
	if len(service.Spec.ClusterIPs) > 2 {
		allErrs = append(allErrs, field.Invalid(clusterIPsField, service.Spec.ClusterIPs, "may only hold up to 2 values"))
	}

	// 到这一步，若存在非法 IP 或位置错误的 none/空字符串，
	// 会使得错误信息失真（错误下标 || 本已非法的 IP 的双栈判断）。
	// 因此若 clusterIPs 校验已有错误，就在此停止
	if hasInvalidIPs {
		return allErrs
	}

	// 若有多于一个 IP，则必须是双栈 IP
	if len(service.Spec.ClusterIPs) > 1 /* 含义：其中没有 None 或空字符串 */ {
		dualStack, err := netutils.IsDualStackIPStrings(service.Spec.ClusterIPs)
		if err != nil { // 前面已经检查过；宁可多一层保险
			allErrs = append(allErrs, field.InternalError(clusterIPsField, fmt.Errorf("failed to check for dual stack with error:%v", err)))
		}

		// 每个 IP 族只支持一个（即列表中最多两个 IP）。
		if !dualStack {
			allErrs = append(allErrs, field.Invalid(clusterIPsField, service.Spec.ClusterIPs, "may specify no more than one IP for each IP family"))
		}
	}

	// 将 clusterIPs 与其所属家族对应起来（若已提供）
	if !isHeadlessService(service) && len(service.Spec.ClusterIPs) > 0 && len(service.Spec.IPFamilies) > 0 {
		for i, ip := range service.Spec.ClusterIPs {
			if i > (len(service.Spec.IPFamilies) - 1) {
				break // 没有更多家族可检查
			}

			// 4=>6
			if service.Spec.IPFamilies[i] == core.IPv4Protocol && netutils.IsIPv6String(ip) {
				allErrs = append(allErrs, field.Invalid(clusterIPsField.Index(i), ip, fmt.Sprintf("expected an IPv4 value as indicated by `ipFamilies[%v]`", i)))
			}
			// 6=>4
			if service.Spec.IPFamilies[i] == core.IPv6Protocol && !netutils.IsIPv6String(ip) {
				allErrs = append(allErrs, field.Invalid(clusterIPsField.Index(i), ip, fmt.Sprintf("expected an IPv6 value as indicated by `ipFamilies[%v]`", i)))
			}
		}
	}

	return allErrs
}

// validateUpgradeDowngradeClusterIPs 针对用户升级或降级到/自双栈场景的 clusterIPs 专项校验。
func validateUpgradeDowngradeClusterIPs(oldService, service *core.Service) field.ErrorList {
	allErrs := make(field.ErrorList, 0)

	// ExternalName 提前返回
	if service.Spec.Type == core.ServiceTypeExternalName || oldService.Spec.Type == core.ServiceTypeExternalName {
		return allErrs
	}
	newIsHeadless := isHeadlessService(service)
	oldIsHeadless := isHeadlessService(oldService)

	if oldIsHeadless && newIsHeadless {
		return allErrs
	}

	switch {
	// ClusterIP 数量无变化
	// 逐个比较
	case len(oldService.Spec.ClusterIPs) == len(service.Spec.ClusterIPs):
		for i, ip := range oldService.Spec.ClusterIPs {
			if ip != service.Spec.ClusterIPs[i] {
				allErrs = append(allErrs, field.Invalid(field.NewPath("spec", "clusterIPs").Index(i), service.Spec.ClusterIPs, "may not change once set"))
			}
		}

	// 有 IP 被释放（降级）
	case len(oldService.Spec.ClusterIPs) > len(service.Spec.ClusterIPs):
		// 主 ClusterIP 被释放了
		if len(service.Spec.ClusterIPs) == 0 {
			allErrs = append(allErrs, field.Invalid(field.NewPath("spec", "clusterIPs").Index(0), service.Spec.ClusterIPs, "primary clusterIP can not be unset"))
		}

		// 检查主 clusterIP 是否被改动
		if len(oldService.Spec.ClusterIPs) > 0 &&
			len(service.Spec.ClusterIPs) > 0 &&
			service.Spec.ClusterIPs[0] != oldService.Spec.ClusterIPs[0] {
			allErrs = append(allErrs, field.Invalid(field.NewPath("spec", "clusterIPs").Index(0), service.Spec.ClusterIPs, "may not change once set"))
		}

		// 检查次 ClusterIP 是否被释放。该 service 是否正确降级了？
		// 用户*必须*设置 IPFamilyPolicy == SingleStack
		if len(service.Spec.ClusterIPs) == 1 {
			if service.Spec.IPFamilyPolicy == nil || *(service.Spec.IPFamilyPolicy) != core.IPFamilyPolicySingleStack {
				allErrs = append(allErrs, field.Invalid(field.NewPath("spec", "ipFamilyPolicy"), service.Spec.IPFamilyPolicy, "must be set to 'SingleStack' when releasing the secondary clusterIP"))
			}
		}
	case len(oldService.Spec.ClusterIPs) < len(service.Spec.ClusterIPs):
		// 有 IP 被新增（升级）
		// 检查主 clusterIP 是否被改动
		if len(oldService.Spec.ClusterIPs) > 0 &&
			service.Spec.ClusterIPs[0] != oldService.Spec.ClusterIPs[0] {
			allErrs = append(allErrs, field.Invalid(field.NewPath("spec", "clusterIPs").Index(0), service.Spec.ClusterIPs, "may not change once set"))
		}
		// 这里不检查 Policy == RequireDualStack，因为 Validation/Creation 函数已处理
	}
	return allErrs
}

// validateUpgradeDowngradeIPFamilies 针对用户升级或降级到/自双栈场景的 ipFamilies 专项校验。
func validateUpgradeDowngradeIPFamilies(oldService, service *core.Service) field.ErrorList {
	allErrs := make(field.ErrorList, 0)
	// ExternalName 提前返回
	if service.Spec.Type == core.ServiceTypeExternalName || oldService.Spec.Type == core.ServiceTypeExternalName {
		return allErrs
	}

	oldIsHeadless := isHeadlessService(oldService)
	newIsHeadless := isHeadlessService(service)

	// 若变更为/取消 headless，则直接返回
	if newIsHeadless != oldIsHeadless {
		return allErrs
	}
	// headless service 可以更改 ipFamilies
	if newIsHeadless {
		return allErrs
	}

	switch {
	case len(oldService.Spec.IPFamilies) == len(service.Spec.IPFamilies):
		// ClusterIP 数量无变化
		// 逐个比较

		for i, ip := range oldService.Spec.IPFamilies {
			if ip != service.Spec.IPFamilies[i] {
				allErrs = append(allErrs, field.Invalid(field.NewPath("spec", "ipFamilies").Index(0), service.Spec.IPFamilies, "may not change once set"))
			}
		}

	case len(oldService.Spec.IPFamilies) > len(service.Spec.IPFamilies):
		// 有 IP 家族被释放（降级）

		// 检查主 ipfamily 是否被释放
		if len(service.Spec.ClusterIPs) == 0 {
			allErrs = append(allErrs, field.Invalid(field.NewPath("spec", "ipFamilies").Index(0), service.Spec.IPFamilies, "primary ipFamily can not be unset"))
		}

		// 检查主 ipFamily 是否被改动
		if len(service.Spec.IPFamilies) > 0 &&
			service.Spec.IPFamilies[0] != oldService.Spec.IPFamilies[0] {
			allErrs = append(allErrs, field.Invalid(field.NewPath("spec", "ipFamilies").Index(0), service.Spec.ClusterIPs, "may not change once set"))
		}

		// 检查次 IPFamily 是否被释放。该 service 是否正确降级了？
		// 用户*必须*设置 IPFamilyPolicy == SingleStack
		if len(service.Spec.IPFamilies) == 1 {
			if service.Spec.IPFamilyPolicy == nil || *(service.Spec.IPFamilyPolicy) != core.IPFamilyPolicySingleStack {
				allErrs = append(allErrs, field.Invalid(field.NewPath("spec", "ipFamilyPolicy"), service.Spec.IPFamilyPolicy, "must be set to 'SingleStack' when releasing the secondary ipFamily"))
			}
		}
	case len(oldService.Spec.IPFamilies) < len(service.Spec.IPFamilies):
		// 有 IP 家族被新增（升级）

		// 检查主 ipFamily 是否被改动
		if len(oldService.Spec.IPFamilies) > 0 &&
			len(service.Spec.IPFamilies) > 0 &&
			service.Spec.IPFamilies[0] != oldService.Spec.IPFamilies[0] {
			allErrs = append(allErrs, field.Invalid(field.NewPath("spec", "ipFamilies").Index(0), service.Spec.ClusterIPs, "may not change once set"))
		}
		// 这里不检查 Policy == RequireDualStack，因为 Validation/Creation 函数已处理
	}
	return allErrs
}

// isHeadlessService 判断 Service 是否为 headless（无集群 IP）服务。
func isHeadlessService(service *core.Service) bool {
	return service != nil &&
		len(service.Spec.ClusterIPs) == 1 &&
		service.Spec.ClusterIPs[0] == core.ClusterIPNone
}

// validateLoadBalancerClassField 校验 Service 的 loadBalancerClass 字段。
func validateLoadBalancerClassField(oldService, service *core.Service) field.ErrorList {
	allErrs := make(field.ErrorList, 0)
	if oldService != nil {
		// 校验更新操作
		if isTypeLoadBalancer(oldService) && isTypeLoadBalancer(service) {
			// 新旧均为 LoadBalancer 类型
			if !sameLoadBalancerClass(oldService, service) {
				// 不能更改 loadBalancerClass
				allErrs = append(allErrs, field.Invalid(field.NewPath("spec", "loadBalancerClass"), service.Spec.LoadBalancerClass, "may not change once set"))
			}
		}
	}

	if isTypeLoadBalancer(service) {
		// 检查 LoadBalancerClass 的格式
		if service.Spec.LoadBalancerClass != nil {
			allErrs = append(allErrs, ValidateQualifiedName(*service.Spec.LoadBalancerClass, field.NewPath("spec", "loadBalancerClass"))...)
		}
	} else {
		// 检查是否为非 LoadBalancer 类型的 service 设置了 LoadBalancerClass
		if service.Spec.LoadBalancerClass != nil {
			allErrs = append(allErrs, field.Forbidden(field.NewPath("spec", "loadBalancerClass"), "may only be used when `type` is 'LoadBalancer'"))
		}
	}
	return allErrs
}

// isTypeLoadBalancer 判断 Service 类型是否为 LoadBalancer。
func isTypeLoadBalancer(service *core.Service) bool {
	return service.Spec.Type == core.ServiceTypeLoadBalancer
}

// sameLoadBalancerClass 判断两个 Service 的 loadBalancerClass 是否相同。
func sameLoadBalancerClass(oldService, service *core.Service) bool {
	if oldService.Spec.LoadBalancerClass == nil && service.Spec.LoadBalancerClass == nil {
		return true
	}
	if oldService.Spec.LoadBalancerClass == nil || service.Spec.LoadBalancerClass == nil {
		return false
	}
	return *oldService.Spec.LoadBalancerClass == *service.Spec.LoadBalancerClass
}

// ValidatePodAffinityTermSelector 校验 PodAffinityTerm 中标签选择器的合法性，并兼容历史遗留的非法标签值。
func ValidatePodAffinityTermSelector(podAffinityTerm core.PodAffinityTerm, allowInvalidLabelValueInSelector bool, fldPath *field.Path) field.ErrorList {
	var allErrs field.ErrorList
	labelSelectorValidationOptions := unversionedvalidation.LabelSelectorValidationOptions{AllowInvalidLabelValueInSelector: allowInvalidLabelValueInSelector}
	allErrs = append(allErrs, unversionedvalidation.ValidateLabelSelector(podAffinityTerm.LabelSelector, labelSelectorValidationOptions, fldPath.Child("labelSelector"))...)
	allErrs = append(allErrs, unversionedvalidation.ValidateLabelSelector(podAffinityTerm.NamespaceSelector, labelSelectorValidationOptions, fldPath.Child("namespaceSelector"))...)
	return allErrs
}

var betaToGALabel = map[string]string{
	v1.LabelFailureDomainBetaZone:   v1.LabelTopologyZone,
	v1.LabelFailureDomainBetaRegion: v1.LabelTopologyRegion,
	kubeletapis.LabelOS:             v1.LabelOSStable,
	kubeletapis.LabelArch:           v1.LabelArchStable,
	v1.LabelInstanceType:            v1.LabelInstanceTypeStable,
}

var (
	maskNodeSelectorLabelChangeEqualities     conversion.Equalities
	initMaskNodeSelectorLabelChangeEqualities sync.Once
)

// getMaskNodeSelectorLabelChangeEqualities 构造用于比较节点选择器的 Equalities，屏蔽因掩码处理带来的无关差异。
func getMaskNodeSelectorLabelChangeEqualities() conversion.Equalities {
	initMaskNodeSelectorLabelChangeEqualities.Do(func() {
		var eqs = apiequality.Semantic.Copy()
		err := eqs.AddFunc(
			func(newReq, oldReq core.NodeSelectorRequirement) bool {
				// 允许 newReq 改为 GA 键
				if oldReq.Key != newReq.Key && betaToGALabel[oldReq.Key] == newReq.Key {
					oldReq.Key = newReq.Key // +k8s:verify-mutation:reason=clone
				}
				return apiequality.Semantic.DeepEqual(newReq, oldReq)
			},
		)
		if err != nil {
			panic(fmt.Errorf("failed to instantiate semantic equalities: %w", err))
		}
		maskNodeSelectorLabelChangeEqualities = eqs
	})
	return maskNodeSelectorLabelChangeEqualities
}

// validatePvNodeAffinity 校验 PV 节点亲和性在更新时是否合法。
func validatePvNodeAffinity(newPvNodeAffinity, oldPvNodeAffinity *core.VolumeNodeAffinity, fldPath *field.Path) field.ErrorList {
	var allErrs field.ErrorList
	if !getMaskNodeSelectorLabelChangeEqualities().DeepEqual(newPvNodeAffinity, oldPvNodeAffinity) {
		allErrs = append(allErrs, field.Invalid(fldPath, newPvNodeAffinity, fieldImmutableErrorMsg+", except for updating from beta label to GA"))
	}
	return allErrs
}

// validateNodeSelectorMutation 校验节点选择器的变更：仅允许追加，不允许修改或删除。
func validateNodeSelectorMutation(fldPath *field.Path, newNodeSelector, oldNodeSelector map[string]string) field.ErrorList {
	var allErrs field.ErrorList

	// 校验已有的节点选择器没有被删除或修改。
	for k, v1 := range oldNodeSelector {
		if v2, ok := newNodeSelector[k]; !ok || v1 != v2 {
			allErrs = append(allErrs, field.Invalid(fldPath, newNodeSelector, "only additions to spec.nodeSelector are allowed (no mutations or deletions)"))
			return allErrs
		}
	}
	return allErrs
}

// validateNodeAffinityMutation 校验节点亲和性的变更：仅允许追加选择器项，不允许修改或删除。
func validateNodeAffinityMutation(nodeAffinityPath *field.Path, newNodeAffinity, oldNodeAffinity *core.NodeAffinity) field.ErrorList {
	var allErrs field.ErrorList
	// 若旧的节点亲和性为 nil，则可以设置任意内容。
	if oldNodeAffinity == nil || oldNodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution == nil {
		return allErrs
	}

	oldTerms := oldNodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms
	var newTerms []core.NodeSelectorTerm
	if newNodeAffinity != nil && newNodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution != nil {
		newTerms = newNodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms
	}

	// 若旧 terms 为空，新 terms 可以任意设置。
	// 若旧 terms 非空，则不允许新增任何 term。
	if len(oldTerms) > 0 && len(oldTerms) != len(newTerms) {
		return append(allErrs, field.Invalid(nodeAffinityPath.Child("requiredDuringSchedulingIgnoredDuringExecution").Child("nodeSelectorTerms"), newTerms, "no additions/deletions to non-empty NodeSelectorTerms list are allowed"))
	}

	// 对于 requiredDuringSchedulingIgnoredDuringExecution，若旧的 NodeSelectorTerms
	// 为空，可以设置任意内容；若非空，则只允许向 matchExpressions 或
	// fieldExpressions 追加 NodeSelectorRequirements。
	for i := range oldTerms {
		if !validateNodeSelectorTermHasOnlyAdditions(newTerms[i], oldTerms[i]) {
			allErrs = append(allErrs, field.Invalid(nodeAffinityPath.Child("requiredDuringSchedulingIgnoredDuringExecution").Child("nodeSelectorTerms").Index(i), newTerms[i], "only additions are allowed (no mutations or deletions)"))
		}
	}
	return allErrs
}

// validateNodeSelectorTermHasOnlyAdditions 判断新的选择器项是否仅相对旧项有新增的表达式。
func validateNodeSelectorTermHasOnlyAdditions(newTerm, oldTerm core.NodeSelectorTerm) bool {
	if len(oldTerm.MatchExpressions) == 0 && len(oldTerm.MatchFields) == 0 {
		if len(newTerm.MatchExpressions) > 0 || len(newTerm.MatchFields) > 0 {
			return false
		}
	}

	// 校验 MatchExpressions 只有追加（无删除、无修改）
	if l := len(oldTerm.MatchExpressions); l > 0 {
		if len(newTerm.MatchExpressions) < l {
			return false
		}
		if !apiequality.Semantic.DeepEqual(newTerm.MatchExpressions[:l], oldTerm.MatchExpressions) {
			return false
		}
	}
	// 校验 MatchFields 只有追加（无删除、无修改）
	if l := len(oldTerm.MatchFields); l > 0 {
		if len(newTerm.MatchFields) < l {
			return false
		}
		if !apiequality.Semantic.DeepEqual(newTerm.MatchFields[:l], oldTerm.MatchFields) {
			return false
		}
	}
	return true
}

var validSupplementalGroupsPolicies = sets.New(core.SupplementalGroupsPolicyMerge, core.SupplementalGroupsPolicyStrict)

// validateSupplementalGroupsPolicy 校验 supplementalGroupsPolicy 字段是否为合法取值。
func validateSupplementalGroupsPolicy(supplementalGroupsPolicy *core.SupplementalGroupsPolicy, fldPath *field.Path) field.ErrorList {
	allErrors := field.ErrorList{}
	if !validSupplementalGroupsPolicies.Has(*supplementalGroupsPolicy) {
		allErrors = append(allErrors, field.NotSupported(fldPath, supplementalGroupsPolicy, sets.List(validSupplementalGroupsPolicies)))
	}
	return allErrors
}

// validateContainerStatusUsers 校验容器状态中的用户 ID 信息（runAsUser、附加组等）是否合法。
func validateContainerStatusUsers(containerStatuses []core.ContainerStatus, fldPath *field.Path, podOS *core.PodOS) field.ErrorList {
	allErrors := field.ErrorList{}
	osName := core.Linux
	if podOS != nil {
		osName = podOS.Name
	}
	for i, containerStatus := range containerStatuses {
		if containerStatus.User == nil {
			continue
		}
		containerUser := containerStatus.User
		switch osName {
		case core.Windows:
			if containerUser.Linux != nil {
				allErrors = append(allErrors, field.Forbidden(fldPath.Index(i).Child("user").Child("linux"), "cannot be set for a windows pod"))
			}
		case core.Linux:
			// Linux 容器用户的校验逻辑直接内联在此处而非抽成共享 helper，
			// 是为了让下面的 UID 范围（比 Pod spec 中 runAsUser 允许的
			// 范围更宽）只会用于容器状态校验。
			if linuxUser := containerUser.Linux; linuxUser != nil {
				userFldPath := fldPath.Index(i).Child("user").Child("linux")
				// UID 由容器运行时上报，可以是任意合法的
				// Linux uid_t（0 到 math.MaxUint32）。
				if linuxUser.UID < 0 || linuxUser.UID > math.MaxUint32 {
					allErrors = append(allErrors, field.Invalid(userFldPath.Child("uid"), linuxUser.UID, fmt.Sprintf("must be between 0 and %d, inclusive", int64(math.MaxUint32))))
				}
				for _, msg := range validation.IsValidGroupID(linuxUser.GID) {
					allErrors = append(allErrors, field.Invalid(userFldPath.Child("gid"), linuxUser.GID, msg))
				}
				for g, gid := range linuxUser.SupplementalGroups {
					for _, msg := range validation.IsValidGroupID(gid) {
						allErrors = append(allErrors, field.Invalid(userFldPath.Child("supplementalGroups").Index(g), gid, msg))
					}
				}
			}
		}
	}
	return allErrors
}

// validateContainerStatusNoAllocatedResourcesStatus 校验未使用扩展资源声明时，容器状态不应包含 allocatedResources 信息。
func validateContainerStatusNoAllocatedResourcesStatus(containerStatuses []core.ContainerStatus, fldPath *field.Path) field.ErrorList {
	allErrors := field.ErrorList{}

	for i, containerStatus := range containerStatuses {
		if len(containerStatus.AllocatedResourcesStatus) == 0 {
			continue
		}
		allErrors = append(allErrors, field.Forbidden(fldPath.Index(i).Child("allocatedResourcesStatus"), "must not be specified in container status"))
	}

	return allErrors
}

// validateContainerStatusAllocatedResourcesStatus 遍历已分配资源的健康状态并校验：资源名匹配容器资源需求，resourceID 非空且唯一。
func validateContainerStatusAllocatedResourcesStatus(containerStatuses []core.ContainerStatus, fldPath *field.Path, containers []core.Container, podStatus *core.PodStatus) field.ErrorList {
	allErrors := field.ErrorList{}

	for i, containerStatus := range containerStatuses {
		if containerStatus.AllocatedResourcesStatus == nil {
			continue
		}

		allocatedResources := containerStatus.AllocatedResourcesStatus
		for j, allocatedResource := range allocatedResources {
			var container core.Container
			containerFound := false
			// 按名称查找容器
			for _, c := range containers {
				if c.Name == containerStatus.Name {
					containerFound = true
					container = c
					break
				}
			}

			// 忽略缺失的容器，见 https://github.com/kubernetes/kubernetes/issues/124915
			if containerFound {
				found := false
				var errorStr string

				if strings.HasPrefix(string(allocatedResource.Name), "claim:") {
					// 假定它是 claim 名称

					errorStr = "must match one of the container's resource claims as 'claim:<claimName>/<requestName>' when container.resources.claims[*].request is set or 'claim:<claimName>' when it is empty"

					for _, c := range container.Resources.Claims {
						if resourceStatusName(c.Name, c.Request) == allocatedResource.Name {
							found = true
							break
						}
					}

					if !found {
						found = matchesExtendedResourceClaimStatus(allocatedResource.Name, container, podStatus.ExtendedResourceClaimStatus)
					}
				} else {
					// 假定它是资源名称

					errorStr = "must match one of the container's resource requests"

					for resourceName := range container.Resources.Requests {
						if resourceName == allocatedResource.Name {
							found = true
							break
						}
					}
				}
				if !found {
					allErrors = append(allErrors, field.Invalid(fldPath.Index(i).Child("allocatedResourcesStatus").Index(j).Child("name"), allocatedResource.Name, errorStr))
				}
			}

			uniqueResources := sets.New[core.ResourceID]()
			// 检查 resource ID 唯一
			for k, r := range allocatedResource.Resources {

				var supportedResourceHealthValues = sets.New(
					core.ResourceHealthStatusHealthy,
					core.ResourceHealthStatusUnhealthy,
					core.ResourceHealthStatusUnknown)

				if !supportedResourceHealthValues.Has(r.Health) {
					allErrors = append(allErrors, field.NotSupported(fldPath.Index(i).Child("allocatedResourcesStatus").Index(j).Child("resources").Index(k).Child("health"), r.Health, sets.List(supportedResourceHealthValues)))
				}

				if r.Message != nil {
					if len(*r.Message) == 0 {
						allErrors = append(allErrors, field.Required(fldPath.Index(i).Child("allocatedResourcesStatus").Index(j).Child("resources").Index(k).Child("message"), "must be non-empty if specified"))
					} else if len(*r.Message) > v1.ResourceHealthMessageMaxLength {
						allErrors = append(allErrors, field.TooLong(fldPath.Index(i).Child("allocatedResourcesStatus").Index(j).Child("resources").Index(k).Child("message"), *r.Message, v1.ResourceHealthMessageMaxLength))
					}
				}

				if uniqueResources.Has(r.ResourceID) {
					allErrors = append(allErrors, field.Duplicate(fldPath.Index(i).Child("allocatedResourcesStatus").Index(j).Child("resources").Index(k).Child("resourceID"), r.ResourceID))
				} else {
					uniqueResources.Insert(r.ResourceID)
				}
			}
		}
	}

	return allErrors
}

// matchesExtendedResourceClaimStatus 判断容器状态中的扩展资源声明状态是否与容器资源需求中的声明匹配。
func matchesExtendedResourceClaimStatus(resourceStatusName core.ResourceName, container core.Container, extendedResourceClaimStatus *core.PodExtendedResourceClaimStatus) bool {
	if extendedResourceClaimStatus == nil {
		return false
	}

	claimName, requestName, found := parseResourceStatusName(string(resourceStatusName))
	if !found || requestName == "" || claimName != extendedResourceClaimStatus.ResourceClaimName {
		return false
	}

	for _, mapping := range extendedResourceClaimStatus.RequestMappings {
		if mapping.ContainerName != container.Name || mapping.RequestName != requestName {
			continue
		}
		quantity, found := container.Resources.Requests[core.ResourceName(mapping.ResourceName)]
		if found && !quantity.IsZero() {
			return true
		}
	}

	return false
}

// resourceStatusName 返回 Pod 资源声明可接受的 ResourceStatus.Name 形式；由于 Request 可选，编码为 claim:<name> 或 claim:<name>/<request>。
func resourceStatusName(claimName, requestName string) core.ResourceName {
	if requestName == "" {
		return core.ResourceName(resourceStatusClaimPrefix + claimName)
	}
	return core.ResourceName(resourceStatusClaimPrefix + claimName + "/" + requestName)
}

// parseResourceStatusName 解析 ResourceStatus.Name 编码形式，返回声明名与请求名。
func parseResourceStatusName(resourceStatusName string) (claimName, requestName string, ok bool) {
	claimRef, ok := strings.CutPrefix(resourceStatusName, resourceStatusClaimPrefix)
	if !ok {
		return "", "", false
	}
	claimName, requestName, hasRequest := strings.Cut(claimRef, "/")
	if claimName == "" {
		return "", "", false
	}
	if hasRequest && requestName == "" {
		return "", "", false
	}
	return claimName, requestName, true
}

// validateImageVolumeSource 校验 image 卷源：引用字段是否合法且仅设置一种引用方式。
func validateImageVolumeSource(imageVolume *core.ImageVolumeSource, fldPath *field.Path, opts PodValidationOptions) field.ErrorList {
	allErrs := field.ErrorList{}
	if !opts.AllowEmptyImageVolumeReference && len(imageVolume.Reference) == 0 {
		allErrs = append(allErrs, field.Required(fldPath.Child("reference"), ""))
	}
	allErrs = append(allErrs, validatePullPolicy(imageVolume.PullPolicy, fldPath.Child("pullPolicy"))...)
	return allErrs
}

// isRestartableInitContainer 判断容器是否设置了 ContainerRestartPolicyAlways（即可重启的 init 容器/sidecar）。
func isRestartableInitContainer(initContainer *core.Container) bool {
	if initContainer == nil || initContainer.RestartPolicy == nil {
		return false
	}
	return *initContainer.RestartPolicy == core.ContainerRestartPolicyAlways
}

// IsValidIPForLegacyField 对使用旧版 IP 校验的字段的包装，负责正确设置 strictValidation；新字段应使用 IsValidIP。
func IsValidIPForLegacyField(fldPath *field.Path, value string, validOldIPs []string) field.ErrorList {
	return validation.IsValidIPForLegacyField(fldPath, value, utilfeature.DefaultFeatureGate.Enabled(features.StrictIPCIDRValidation), validOldIPs)
}

// IsValidCIDRForLegacyField 对使用旧版 CIDR 校验的字段的包装，负责正确设置 strictValidation；新字段应使用 IsValidCIDR。
func IsValidCIDRForLegacyField(fldPath *field.Path, value string, validOldCIDRs []string) field.ErrorList {
	return validation.IsValidCIDRForLegacyField(fldPath, value, utilfeature.DefaultFeatureGate.Enabled(features.StrictIPCIDRValidation), validOldCIDRs)
}

// validateNodeSwapStatus 校验节点 swap 状态中的字段是否合法。
func validateNodeSwapStatus(nodeSwapStatus *core.NodeSwapStatus, fldPath *field.Path) field.ErrorList {
	allErrors := field.ErrorList{}

	if nodeSwapStatus == nil {
		return allErrors
	}

	if nodeSwapStatus.Capacity != nil {
		capacityFld := fldPath.Child("capacity")

		errs := ValidatePositiveField(*nodeSwapStatus.Capacity, capacityFld)
		if len(errs) > 0 {
			allErrors = append(allErrors, errs...)
		}
	}

	return allErrors
}

// validateSchedulingGroup 校验 Pod 调度组（schedulingGates 相关状态）字段是否合法。
func validateSchedulingGroup(schedulingGroup *core.PodSchedulingGroup, fldPath *field.Path) field.ErrorList {
	var allErrs field.ErrorList
	if schedulingGroup.PodGroupName == nil {
		// 这是一个 one-of 集合，但当前只有一个可能的选项。
		allErrs = append(allErrs, field.Invalid(fldPath.Child("podGroupName"), schedulingGroup.PodGroupName, "must specify one of: `podGroupName`"))
	} else {
		name := *schedulingGroup.PodGroupName
		for _, detail := range ValidatePodGroupName(name, false) {
			allErrs = append(allErrs, field.Invalid(fldPath.Child("podGroupName"), name, detail))
		}
	}
	return allErrs
}

// validateImageVolumeStatus 校验 image 卷状态中的引用信息是否合法。
func validateImageVolumeStatus(imageVolumeStatus *core.ImageVolumeStatus, fldPath *field.Path) field.ErrorList {
	allErrors := field.ErrorList{}

	if imageVolumeStatus == nil {
		return allErrors
	}

	if imageVolumeStatus.ImageRef == "" {
		allErrors = append(allErrors, field.Required(fldPath.Child("imageRef"), "imageRef must not be empty"))
	}

	const maxImageRefLen = 256
	if len(imageVolumeStatus.ImageRef) > maxImageRefLen {
		allErrors = append(allErrors, field.TooLong(fldPath.Child("imageRef"), imageVolumeStatus.ImageRef, maxImageRefLen))
	}

	return allErrors
}

// validateVolumeStatus 校验卷状态结构体中最多只能填充一个字段。
func validateVolumeStatus(volumeStatus *core.VolumeStatus, fldPath *field.Path) field.ErrorList {
	allErrors := field.ErrorList{}

	if volumeStatus == nil {
		return allErrors
	}

	numStatuses := 0

	if volumeStatus.Image != nil {
		numStatuses++
		allErrors = append(allErrors, validateImageVolumeStatus(volumeStatus.Image, fldPath.Child("imageVolumeStatus"))...)
	}

	if numStatuses > 1 {
		allErrors = append(allErrors, field.Forbidden(fldPath, "may not specify more than 1 volume type"))
	}

	return allErrors
}
