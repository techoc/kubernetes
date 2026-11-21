/*
Copyright 2018 The Kubernetes Authors.

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
	"slices"
	"strings"

	"github.com/blang/semver/v4"
	"k8s.io/apimachinery/pkg/api/validation"
	utilvalidation "k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/apimachinery/pkg/util/validation/field"

	"k8s.io/kubernetes/pkg/apis/coordination"
	apivalidation "k8s.io/kubernetes/pkg/apis/core/validation"
)

// 当前支持的协调租约策略集合。
var validLeaseStrategies = []coordination.CoordinatedLeaseStrategy{coordination.OldestEmulationVersion}

// ValidateLease 校验 Lease 对象。
func ValidateLease(lease *coordination.Lease) field.ErrorList {
	allErrs := validation.ValidateObjectMeta(&lease.ObjectMeta, true, validation.NameIsDNSSubdomain, field.NewPath("metadata"))
	allErrs = append(allErrs, ValidateLeaseSpec(&lease.Spec, field.NewPath("spec"))...)
	return allErrs
}

// ValidateLeaseUpdate 校验 Lease 对象的更新。
func ValidateLeaseUpdate(lease, oldLease *coordination.Lease) field.ErrorList {
	allErrs := validation.ValidateObjectMetaUpdate(&lease.ObjectMeta, &oldLease.ObjectMeta, field.NewPath("metadata"))
	allErrs = append(allErrs, ValidateLeaseSpec(&lease.Spec, field.NewPath("spec"))...)
	return allErrs
}

// ValidateLeaseSpec 校验 Lease 的 spec 字段。
func ValidateLeaseSpec(spec *coordination.LeaseSpec, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}

	// 租约时长必须为正数，否则租约会立即过期。
	if spec.LeaseDurationSeconds != nil && *spec.LeaseDurationSeconds <= 0 {
		fld := fldPath.Child("leaseDurationSeconds")
		allErrs = append(allErrs, field.Invalid(fld, spec.LeaseDurationSeconds, "must be greater than 0"))
	}
	if spec.LeaseTransitions != nil && *spec.LeaseTransitions < 0 {
		fld := fldPath.Child("leaseTransitions")
		allErrs = append(allErrs, field.Invalid(fld, spec.LeaseTransitions, "must be greater than or equal to 0"))
	}
	if spec.Strategy != nil {
		allErrs = append(allErrs, ValidateCoordinatedLeaseStrategy(*spec.Strategy, fldPath.Child("strategy"))...)
	}
	// PreferredHolder 只有指定了策略后才有意义。
	if spec.PreferredHolder != nil && *spec.PreferredHolder != "" && (spec.Strategy == nil || *spec.Strategy == "") {
		allErrs = append(allErrs, field.Forbidden(fldPath.Child("preferredHolder"), "may only be specified if `strategy` is defined"))
	}
	// RenewTime 是 MicroTime，无需额外校验。
	return allErrs
}

// ValidateLeaseCandidate 校验 LeaseCandidate 对象。
func ValidateLeaseCandidate(lease *coordination.LeaseCandidate) field.ErrorList {
	allErrs := validation.ValidateObjectMeta(&lease.ObjectMeta, true, ValidLeaseCandidateName, field.NewPath("metadata"))
	allErrs = append(allErrs, ValidateLeaseCandidateSpec(&lease.Spec, field.NewPath("spec"))...)
	return allErrs
}

// ValidLeaseCandidateName 校验候选者名称是否为合法的 ConfigMap key。
func ValidLeaseCandidateName(name string, prefix bool) []string {
	// IsConfigMapKey 已处理前缀，允许名称以连字符结尾。
	return utilvalidation.IsConfigMapKey(name)
}

// ValidateLeaseCandidateSpecUpdate 校验候选者 spec 中不可变字段的更新。
func ValidateLeaseCandidateSpecUpdate(leaseCandidateSpec, oldLeaseCandidateSpec *coordination.LeaseCandidateSpec) field.ErrorList {
	allErrs := field.ErrorList{}
	allErrs = append(allErrs, apivalidation.ValidateImmutableField(leaseCandidateSpec.LeaseName, oldLeaseCandidateSpec.LeaseName, field.NewPath("spec").Child("leaseName"))...)
	return allErrs
}

// ValidateLeaseCandidateUpdate 校验 LeaseCandidate 对象的更新。
func ValidateLeaseCandidateUpdate(leaseCandidate, oldLeaseCandidate *coordination.LeaseCandidate) field.ErrorList {
	allErrs := validation.ValidateObjectMetaUpdate(&leaseCandidate.ObjectMeta, &oldLeaseCandidate.ObjectMeta, field.NewPath("metadata"))
	allErrs = append(allErrs, ValidateLeaseCandidateSpec(&leaseCandidate.Spec, field.NewPath("spec"))...)
	allErrs = append(allErrs, ValidateLeaseCandidateSpecUpdate(&leaseCandidate.Spec, &oldLeaseCandidate.Spec)...)
	return allErrs
}

// ValidateLeaseCandidateSpec 校验 LeaseCandidate 的 spec 字段。
func ValidateLeaseCandidateSpec(spec *coordination.LeaseCandidateSpec, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}

	if len(spec.LeaseName) == 0 {
		allErrs = append(allErrs, field.Required(fldPath.Child("leaseName"), ""))
	}

	ev := semver.Version{}
	if spec.EmulationVersion != "" {
		var err error
		// 模拟版本必须是合法的语义化版本，用于协调租约竞选。
		ev, err = semver.Parse(spec.EmulationVersion)
		if err != nil {
			fld := fldPath.Child("emulationVersion")
			allErrs = append(allErrs, field.Invalid(fld, spec.EmulationVersion, "must be a valid semantic version"))
		}
	}
	bv := semver.Version{}
	if spec.BinaryVersion == "" {
		allErrs = append(allErrs, field.Required(fldPath.Child("binaryVersion"), ""))
	} else {
		var err error
		bv, err = semver.Parse(spec.BinaryVersion)
		if err != nil {
			fld := fldPath.Child("binaryVersion")
			allErrs = append(allErrs, field.Invalid(fld, spec.BinaryVersion, "must be a valid semantic version"))
		}
	}
	// 二进制版本不能低于模拟版本，否则无法模拟旧版本行为。
	if spec.BinaryVersion != "" && spec.EmulationVersion != "" && bv.LT(ev) {
		fld := fldPath.Child("binaryVersion")
		allErrs = append(allErrs, field.Invalid(fld, spec.BinaryVersion, "must be greater than or equal to `emulationVersion`"))
	}

	if spec.Strategy == "" {
		allErrs = append(allErrs, field.Required(fldPath.Child("strategy"), ""))
	} else {
		fld := fldPath.Child("strategy")
		if spec.Strategy == coordination.OldestEmulationVersion {
			zeroVersion := semver.Version{}
			// OldestEmulationVersion 策略依赖 emulationVersion 决定候选者顺序。
			if ev.EQ(zeroVersion) {
				allErrs = append(allErrs, field.Required(fldPath.Child("emulationVersion"), "must be specified when `strategy` is 'OldestEmulationVersion'"))
			}
		}

		allErrs = append(allErrs, ValidateCoordinatedLeaseStrategy(spec.Strategy, fld)...)
	}
	// PingTime 是 MicroTime，无需额外校验。
	// RenewTime 是 MicroTime，无需额外校验。
	return allErrs
}

// ValidateCoordinatedLeaseStrategy 校验 Lease 与 LeaseCandidate 中的 Strategy 字段。
func ValidateCoordinatedLeaseStrategy(strategy coordination.CoordinatedLeaseStrategy, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}

	parts := strings.Split(string(strategy), "/")
	switch len(parts) {
	case 1:
		// 单段名称必须是 Kubernetes 预定义的策略名。
		if !slices.Contains(validLeaseStrategies, coordination.CoordinatedLeaseStrategy(parts[0])) {
			allErrs = append(allErrs, field.NotSupported(fldPath.Child("strategy"), strategy, validLeaseStrategies))
		}
		// 带域名前缀的名称按限定名规则校验，以支持自定义扩展策略。
	default:
		if msgs := utilvalidation.IsQualifiedName(string(strategy)); len(msgs) > 0 {
			for _, msg := range msgs {
				allErrs = append(allErrs, field.Invalid(fldPath.Child("strategy"), strategy, msg))
			}
		}
	}
	return allErrs
}
