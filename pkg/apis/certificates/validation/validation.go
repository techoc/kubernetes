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

package validation

import (
	"bytes"
	"crypto"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"net/mail"
	"strconv"
	"strings"
	"time"

	v1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apimachineryvalidation "k8s.io/apimachinery/pkg/api/validation"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	metav1validation "k8s.io/apimachinery/pkg/apis/meta/v1/validation"
	"k8s.io/apimachinery/pkg/util/diff"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/validation/field"
	utilcert "k8s.io/client-go/util/cert"
	"k8s.io/kubernetes/pkg/apis/certificates"
	apivalidation "k8s.io/kubernetes/pkg/apis/core/validation"
	"k8s.io/utils/clock"
)

var (
	// trueConditionTypes 是状态只能为 True 的条件类型集合。
	// 这些条件代表审批或失败这类一次性决定，不应用 False/Unknown 表达。
	trueConditionTypes = sets.NewString(
		string(certificates.CertificateApproved),
		string(certificates.CertificateDenied),
		string(certificates.CertificateFailed),
	)

	// trueStatusOnly 是只允许 True 的条件类型所对应的合法状态集合。
	trueStatusOnly = sets.NewString(string(v1.ConditionTrue))
	// allStatusValues 是普通条件类型可用的完整状态集合。
	allStatusValues = sets.NewString(string(v1.ConditionTrue), string(v1.ConditionFalse), string(v1.ConditionUnknown))
)

// certificateValidationOptions 控制校验的宽松程度。
// 一部分开关对应只有特定更新路径才允许的操作；另一部分是为了兼容历史数据而
// 必须容忍的老问题（v1beta1 API 写入的数据，或持久化对象中已经存在的问题）。
type certificateValidationOptions struct {
	// 以下开关只允许特定的更新路径使用。

	// allowSettingApprovalConditions 允许写入或修改 Approved/Denied 条件。
	allowSettingApprovalConditions bool
	// allowSettingCertificate 允许写入 status.certificate。
	allowSettingCertificate bool

	// allowBothApprovedAndDenied 允许 Approved 与 Denied 条件同时存在。
	// 历史对象中已存在此问题时为了兼容性予以容忍。
	allowBothApprovedAndDenied bool

	// 以下是对历史数据放宽的校验；包括 v1beta1 API 发起的请求，
	// 以及旧对象中已经存在该问题的更新请求。

	// allowResettingCertificate 允许在旧对象已有证书时修改 status.certificate。
	allowResettingCertificate bool
	// allowLegacySignerName 允许使用 legacy-unknown 这个 signerName。
	allowLegacySignerName bool
	// allowDuplicateConditionTypes 允许存在类型重复的条件。
	allowDuplicateConditionTypes bool
	// allowEmptyConditionType 允许条件类型为空字符串。
	allowEmptyConditionType bool
	// allowArbitraryCertificate 不校验 status.certificate 的内容。
	allowArbitraryCertificate bool
	// allowUnknownUsages 允许出现已知集合之外的 usages 值。
	allowUnknownUsages bool
	// allowDuplicateUsages 允许 usages 值重复。
	allowDuplicateUsages bool
}

// validateCSR 校验 base64 包装、PEM 编码的 PKCS#10 证书签名请求的格式和签名。
// 校验失败就不能让这个 CSR 进入后续处理流程。
func validateCSR(obj *certificates.CertificateSigningRequest) error {
	csr, err := certificates.ParseCSR(obj.Spec.Request)
	if err != nil {
		return err
	}
	// 只解析成功还不够：必须确认请求确实由其所含公钥对应的私钥签名，
	// 否则任何人都可以伪造请求内容。
	return csr.CheckSignature()
}

// validateCertificate 校验证书字段只包含可解析的 CERTIFICATE PEM 块。
// 空值表示尚未签发证书，属于合法状态。
func validateCertificate(pemData []byte) error {
	if len(pemData) == 0 {
		return nil
	}

	blocks := 0
	for {
		block, remainingData := pem.Decode(pemData)
		if block == nil {
			break
		}

		// 只允许 CERTIFICATE 块，避免把私钥或其他内容混入证书字段。
		if block.Type != utilcert.CertificateBlockType {
			return fmt.Errorf("only CERTIFICATE PEM blocks are allowed, found %q", block.Type)
		}
		// PEM 头可能改变数据的解释方式，这里一律拒绝。
		if len(block.Headers) != 0 {
			return fmt.Errorf("no PEM block headers are permitted")
		}
		blocks++

		certs, err := x509.ParseCertificates(block.Bytes)
		if err != nil {
			return err
		}
		if len(certs) == 0 {
			return fmt.Errorf("found CERTIFICATE PEM block containing 0 certificates")
		}

		pemData = remainingData
	}

	// 非空证书字段必须至少包含一个有效证书，不能只有 PEM 块外壳。
	if blocks == 0 {
		return fmt.Errorf("must contain at least one CERTIFICATE PEM block")
	}

	return nil
}

// ValidateCertificateRequestName 对 CSR 名称不做任何限制。
func ValidateCertificateRequestName(name string, prefix bool) []string {
	return nil
}

// ValidateCertificateSigningRequestCreate 对创建 CSR 的请求执行全部校验。
// 创建时用 nil 作为旧对象，因此所有兼容性宽松选项都会被关闭。
func ValidateCertificateSigningRequestCreate(csr *certificates.CertificateSigningRequest) field.ErrorList {
	opts := getValidationOptions(csr, nil)
	return validateCertificateSigningRequest(csr, opts)
}

var (
	// allValidUsages 是 CSR 中允许出现的完整 key usage 集合。
	allValidUsages = sets.NewString(
		string(certificates.UsageSigning),
		string(certificates.UsageDigitalSignature),
		string(certificates.UsageContentCommitment),
		string(certificates.UsageKeyEncipherment),
		string(certificates.UsageKeyAgreement),
		string(certificates.UsageDataEncipherment),
		string(certificates.UsageCertSign),
		string(certificates.UsageCRLSign),
		string(certificates.UsageEncipherOnly),
		string(certificates.UsageDecipherOnly),
		string(certificates.UsageAny),
		string(certificates.UsageServerAuth),
		string(certificates.UsageClientAuth),
		string(certificates.UsageCodeSigning),
		string(certificates.UsageEmailProtection),
		string(certificates.UsageSMIME),
		string(certificates.UsageIPsecEndSystem),
		string(certificates.UsageIPsecTunnel),
		string(certificates.UsageIPsecUser),
		string(certificates.UsageTimestamping),
		string(certificates.UsageOCSPSigning),
		string(certificates.UsageMicrosoftSGC),
		string(certificates.UsageNetscapeSGC),
	)
)

// validateCertificateSigningRequest 执行 CSR 的公共校验，创建和各更新路径都调用它。
// opts 决定对历史遗留数据放宽到什么程度。
func validateCertificateSigningRequest(csr *certificates.CertificateSigningRequest, opts certificateValidationOptions) field.ErrorList {
	// CSR 是集群级资源，不隶属于任何命名空间。
	isNamespaced := false
	allErrs := apivalidation.ValidateObjectMeta(&csr.ObjectMeta, isNamespaced, ValidateCertificateRequestName, field.NewPath("metadata"))

	specPath := field.NewPath("spec")
	// 请求数据本身无效时，后续字段校验没有意义，但仍继续收集其他字段的错误。
	err := validateCSR(csr)
	if err != nil {
		allErrs = append(allErrs, field.Invalid(specPath.Child("request"), csr.Spec.Request, fmt.Sprintf("%v", err)))
	}
	// 空 usages 会让签发方无法确定证书用途，因此必须显式指定。
	if len(csr.Spec.Usages) == 0 {
		allErrs = append(allErrs, field.Required(specPath.Child("usages"), ""))
	}
	if !opts.allowUnknownUsages {
		for i, usage := range csr.Spec.Usages {
			if !allValidUsages.Has(string(usage)) {
				allErrs = append(allErrs, field.NotSupported(specPath.Child("usages").Index(i), usage, allValidUsages.List()))
			}
		}
	}
	if !opts.allowDuplicateUsages {
		seen := make(map[certificates.KeyUsage]bool, len(csr.Spec.Usages))
		for i, usage := range csr.Spec.Usages {
			if seen[usage] {
				allErrs = append(allErrs, field.Duplicate(specPath.Child("usages").Index(i), usage))
			}
			seen[usage] = true
		}
	}
	// legacy-unknown 不带任何签名方归属语义，新 API 必须拒绝，
	// 只有旧对象中已经使用该值时才放行，避免来自不同 signer 的 CSR 被混为一谈。
	if !opts.allowLegacySignerName && csr.Spec.SignerName == certificates.LegacyUnknownSignerName {
		allErrs = append(allErrs, field.Invalid(specPath.Child("signerName"), csr.Spec.SignerName, "the legacy signerName is not allowed via this API version"))
	} else {
		allErrs = append(allErrs, apivalidation.ValidateSignerName(specPath.Child("signerName"), csr.Spec.SignerName)...)
	}
	// 太短的证书有效期没有实际用途，还会增加轮换压力。
	if csr.Spec.ExpirationSeconds != nil && *csr.Spec.ExpirationSeconds < 600 {
		allErrs = append(allErrs, field.Invalid(specPath.Child("expirationSeconds"), *csr.Spec.ExpirationSeconds, "may not specify a duration less than 600 seconds (10 minutes)"))
	}
	allErrs = append(allErrs, validateConditions(field.NewPath("status", "conditions"), csr, opts)...)

	// 创建和普通更新路径会校验证书内容；已经存在非法证书的历史对象除外。
	if !opts.allowArbitraryCertificate {
		if err := validateCertificate(csr.Status.Certificate); err != nil {
			allErrs = append(allErrs, field.Invalid(field.NewPath("status", "certificate"), "<certificate data>", err.Error()))
		}
	}

	return allErrs
}

// validateConditions 校验 CSR 状态中的条件列表。
// 审批类条件只允许 True，Approved 与 Denied 互斥，且类型不能重复。
func validateConditions(fldPath *field.Path, csr *certificates.CertificateSigningRequest, opts certificateValidationOptions) field.ErrorList {
	allErrs := field.ErrorList{}

	seenTypes := map[certificates.RequestConditionType]bool{}
	hasApproved := false
	hasDenied := false

	for i, c := range csr.Status.Conditions {

		if !opts.allowEmptyConditionType {
			if len(c.Type) == 0 {
				allErrs = append(allErrs, field.Required(fldPath.Index(i).Child("type"), ""))
			}
		}

		// 审批/失败条件表达的是已发生的决定，用 False 或 Unknown 会产生歧义。
		allowedStatusValues := allStatusValues
		if trueConditionTypes.Has(string(c.Type)) {
			allowedStatusValues = trueStatusOnly
		}
		switch {
		case c.Status == "":
			allErrs = append(allErrs, field.Required(fldPath.Index(i).Child("status"), ""))
		case !allowedStatusValues.Has(string(c.Status)):
			allErrs = append(allErrs, field.NotSupported(fldPath.Index(i).Child("status"), c.Status, allowedStatusValues.List()))
		}

		// 一个 CSR 不可能既被批准又被拒绝，两个条件同时存在说明状态已损坏。
		if !opts.allowBothApprovedAndDenied {
			switch c.Type {
			case certificates.CertificateApproved:
				hasApproved = true
				if hasDenied {
					allErrs = append(allErrs, field.Invalid(fldPath, c.Type, "Approved and Denied conditions are mutually exclusive").WithOrigin("zeroOrOneOf").MarkCoveredByDeclarative())
				}
			case certificates.CertificateDenied:
				hasDenied = true
				if hasApproved {
					allErrs = append(allErrs, field.Invalid(fldPath, c.Type, "Approved and Denied conditions are mutually exclusive").WithOrigin("zeroOrOneOf").MarkCoveredByDeclarative())
				}
			}
		}

		// 相同类型的多条条件会让审批控制器无法判断以哪条为准。
		if !opts.allowDuplicateConditionTypes {
			if seenTypes[c.Type] {
				allErrs = append(allErrs, field.Duplicate(fldPath.Index(i).Child("type"), c.Type))
			}
			seenTypes[c.Type] = true
		}
	}

	return allErrs
}

// ValidateCertificateSigningRequestUpdate 校验普通的 CSR 更新请求。
func ValidateCertificateSigningRequestUpdate(newCSR, oldCSR *certificates.CertificateSigningRequest) field.ErrorList {
	opts := getValidationOptions(newCSR, oldCSR)
	return validateCertificateSigningRequestUpdate(newCSR, oldCSR, opts)
}

// ValidateCertificateSigningRequestStatusUpdate 校验 status 子资源的更新。
// 只有这条路径允许签发方写入证书内容。
func ValidateCertificateSigningRequestStatusUpdate(newCSR, oldCSR *certificates.CertificateSigningRequest) field.ErrorList {
	opts := getValidationOptions(newCSR, oldCSR)
	opts.allowSettingCertificate = true
	return validateCertificateSigningRequestUpdate(newCSR, oldCSR, opts)
}

// ValidateCertificateSigningRequestApprovalUpdate 校验 approval 子资源的更新。
// 只有这条路径允许审批方设置 Approved/Denied 条件。
func ValidateCertificateSigningRequestApprovalUpdate(newCSR, oldCSR *certificates.CertificateSigningRequest) field.ErrorList {
	opts := getValidationOptions(newCSR, oldCSR)
	opts.allowSettingApprovalConditions = true
	return validateCertificateSigningRequestUpdate(newCSR, oldCSR, opts)
}

// validateCertificateSigningRequestUpdate 执行所有 CSR 更新路径共用的校验。
// 除了公共字段校验，还限制审批条件和证书内容只能由对应子资源路径修改。
func validateCertificateSigningRequestUpdate(newCSR, oldCSR *certificates.CertificateSigningRequest, opts certificateValidationOptions) field.ErrorList {
	validationErrorList := validateCertificateSigningRequest(newCSR, opts)
	metaUpdateErrorList := apivalidation.ValidateObjectMetaUpdate(&newCSR.ObjectMeta, &oldCSR.ObjectMeta, field.NewPath("metadata"))

	// 已经作出的审批或失败决定不能撤销，否则签发方可能把失败的请求重新当作待处理。
	for _, t := range []certificates.RequestConditionType{certificates.CertificateApproved, certificates.CertificateDenied, certificates.CertificateFailed} {
		oldConditions := findConditions(oldCSR, t)
		newConditions := findConditions(newCSR, t)
		if len(newConditions) < len(oldConditions) {
			validationErrorList = append(validationErrorList, field.Forbidden(field.NewPath("status", "conditions"), fmt.Sprintf("updates may not remove a condition of type %q", t)))
		}
	}

	if !opts.allowSettingApprovalConditions {
		// 非 approval 路径不能增删改审批条件，避免绕过审批子资源的权限控制。
		for _, t := range []certificates.RequestConditionType{certificates.CertificateApproved, certificates.CertificateDenied} {
			oldConditions := findConditions(oldCSR, t)
			newConditions := findConditions(newCSR, t)
			switch {
			case len(newConditions) < len(oldConditions):
				// 删除操作已在上面统一报错，这里不重复报告。
			case len(newConditions) > len(oldConditions):
				validationErrorList = append(validationErrorList, field.Forbidden(field.NewPath("status", "conditions"), fmt.Sprintf("updates may not add a condition of type %q", t)))
			case !apiequality.Semantic.DeepEqual(oldConditions, newConditions):
				conditionDiff := diff.Diff(oldConditions, newConditions)
				validationErrorList = append(validationErrorList, field.Forbidden(field.NewPath("status", "conditions"), fmt.Sprintf("updates may not modify a condition of type %q\n%v", t, conditionDiff)))
			}
		}
	}

	// 证书内容一经写入就不能被普通更新覆盖，只有 status 路径可以设置。
	if !bytes.Equal(newCSR.Status.Certificate, oldCSR.Status.Certificate) {
		if !opts.allowSettingCertificate {
			validationErrorList = append(validationErrorList, field.Forbidden(field.NewPath("status", "certificate"), "updates may not set certificate content"))
		} else if !opts.allowResettingCertificate && len(oldCSR.Status.Certificate) > 0 {
			validationErrorList = append(validationErrorList, field.Forbidden(field.NewPath("status", "certificate"), "updates may not modify existing certificate content"))
		}
	}

	return append(validationErrorList, metaUpdateErrorList...)
}

// findConditions 返回指定类型的全部条件实例。
func findConditions(csr *certificates.CertificateSigningRequest, conditionType certificates.RequestConditionType) []certificates.CertificateSigningRequestCondition {
	var retval []certificates.CertificateSigningRequestCondition
	for i, c := range csr.Status.Conditions {
		if c.Type == conditionType {
			retval = append(retval, csr.Status.Conditions[i])
		}
	}
	return retval
}

// getValidationOptions 根据旧对象返回兼容所需的校验选项。
// 创建请求没有旧对象，此时 oldCSR 为 nil。
// 与子资源能力相关的选项一律置为 false，由各子资源的入口自行开启。
func getValidationOptions(newCSR, oldCSR *certificates.CertificateSigningRequest) certificateValidationOptions {
	return certificateValidationOptions{
		allowResettingCertificate:    false,
		allowBothApprovedAndDenied:   allowBothApprovedAndDenied(oldCSR),
		allowLegacySignerName:        allowLegacySignerName(oldCSR),
		allowDuplicateConditionTypes: allowDuplicateConditionTypes(oldCSR),
		allowEmptyConditionType:      allowEmptyConditionType(oldCSR),
		allowArbitraryCertificate:    allowArbitraryCertificate(newCSR, oldCSR),
		allowDuplicateUsages:         allowDuplicateUsages(oldCSR),
		allowUnknownUsages:           allowUnknownUsages(oldCSR),
	}
}

// allowBothApprovedAndDenied 判断旧对象是否已经同时带有 Approved 和 Denied 条件。
func allowBothApprovedAndDenied(oldCSR *certificates.CertificateSigningRequest) bool {
	if oldCSR == nil {
		return false
	}
	approved := false
	denied := false
	for _, c := range oldCSR.Status.Conditions {
		if c.Type == certificates.CertificateApproved {
			approved = true
		} else if c.Type == certificates.CertificateDenied {
			denied = true
		}
	}
	// 旧数据中已经存在的问题必须容忍，否则对象将无法再被更新。
	return approved && denied
}

// allowLegacySignerName 判断旧对象是否已经使用 legacy-unknown signerName。
func allowLegacySignerName(oldCSR *certificates.CertificateSigningRequest) bool {
	switch {
	case oldCSR != nil && oldCSR.Spec.SignerName == certificates.LegacyUnknownSignerName:
		return true // 兼容已有数据
	default:
		return false
	}
}

// allowDuplicateConditionTypes 判断旧对象是否已有类型重复的条件。
func allowDuplicateConditionTypes(oldCSR *certificates.CertificateSigningRequest) bool {
	switch {
	case oldCSR != nil && hasDuplicateConditionTypes(oldCSR):
		return true // 兼容已有数据
	default:
		return false
	}
}

// hasDuplicateConditionTypes 判断 CSR 中是否存在类型重复的条件。
func hasDuplicateConditionTypes(csr *certificates.CertificateSigningRequest) bool {
	seen := map[certificates.RequestConditionType]bool{}
	for _, c := range csr.Status.Conditions {
		if seen[c.Type] {
			return true
		}
		seen[c.Type] = true
	}
	return false
}

// allowEmptyConditionType 判断旧对象是否已有类型为空的条件。
func allowEmptyConditionType(oldCSR *certificates.CertificateSigningRequest) bool {
	switch {
	case oldCSR != nil && hasEmptyConditionType(oldCSR):
		return true // 兼容已有数据
	default:
		return false
	}
}

// hasEmptyConditionType 判断 CSR 中是否存在类型为空的条件。
func hasEmptyConditionType(csr *certificates.CertificateSigningRequest) bool {
	for _, c := range csr.Status.Conditions {
		if len(c.Type) == 0 {
			return true
		}
	}
	return false
}

// allowArbitraryCertificate 判断是否可以跳过 status.certificate 的内容校验。
// 未改动证书字段，或旧对象里的证书本就非法时都要放行，否则更新会被卡住。
func allowArbitraryCertificate(newCSR, oldCSR *certificates.CertificateSigningRequest) bool {
	switch {
	case newCSR != nil && oldCSR != nil && bytes.Equal(newCSR.Status.Certificate, oldCSR.Status.Certificate):
		return true // 容忍不涉及 status.certificate 的更新
	case oldCSR != nil && validateCertificate(oldCSR.Status.Certificate) != nil:
		return true // 兼容已有数据
	default:
		return false
	}
}

// allowUnknownUsages 判断旧对象是否已经含有未知的 usages 值。
func allowUnknownUsages(oldCSR *certificates.CertificateSigningRequest) bool {
	switch {
	case oldCSR != nil && hasUnknownUsage(oldCSR.Spec.Usages):
		return true // 兼容已有数据
	default:
		return false
	}
}

// hasUnknownUsage 判断 usages 中是否存在已知集合之外的值。
func hasUnknownUsage(usages []certificates.KeyUsage) bool {
	for _, usage := range usages {
		if !allValidUsages.Has(string(usage)) {
			return true
		}
	}
	return false
}

// allowDuplicateUsages 判断旧对象是否已经含有重复的 usages 值。
func allowDuplicateUsages(oldCSR *certificates.CertificateSigningRequest) bool {
	switch {
	case oldCSR != nil && hasDuplicateUsage(oldCSR.Spec.Usages):
		return true // 兼容已有数据
	default:
		return false
	}
}

// hasDuplicateUsage 判断 usages 中是否存在重复值。
func hasDuplicateUsage(usages []certificates.KeyUsage) bool {
	seen := make(map[certificates.KeyUsage]bool, len(usages))
	for _, usage := range usages {
		if seen[usage] {
			return true
		}
		seen[usage] = true
	}
	return false
}

// ValidateClusterTrustBundleOptions 控制 ClusterTrustBundle 校验行为。
type ValidateClusterTrustBundleOptions struct {
	// SuppressBundleParsing 为 true 时跳过 trustBundle 内容的 PEM/X.509 解析。
	SuppressBundleParsing bool
}

// ValidateClusterTrustBundle 对 ClusterTrustBundle 执行全部校验。
func ValidateClusterTrustBundle(bundle *certificates.ClusterTrustBundle, opts ValidateClusterTrustBundleOptions) field.ErrorList {
	var allErrors field.ErrorList

	metaErrors := apivalidation.ValidateObjectMeta(&bundle.ObjectMeta, false, apivalidation.ValidateClusterTrustBundleName(bundle.Spec.SignerName), field.NewPath("metadata"))
	allErrors = append(allErrors, metaErrors...)

	// 未绑定 signerName 的 bundle 由名字本身携带 signer 信息，否则必须校验 signerName 格式。
	if bundle.Spec.SignerName != "" {
		signerNameErrors := apivalidation.ValidateSignerName(field.NewPath("spec", "signerName"), bundle.Spec.SignerName)
		allErrors = append(allErrors, signerNameErrors...)
	}

	if !opts.SuppressBundleParsing {
		pemErrors := validateTrustBundle(field.NewPath("spec", "trustBundle"), bundle.Spec.TrustBundle)
		allErrors = append(allErrors, pemErrors...)
	}

	return allErrors
}

// ValidateClusterTrustBundleUpdate 对 ClusterTrustBundle 的更新执行全部校验。
func ValidateClusterTrustBundleUpdate(newBundle, oldBundle *certificates.ClusterTrustBundle) field.ErrorList {
	// 没有改动 trustBundle 时跳过解析，这样 Go 的 PEM/X.509 解析库行为变化
	// 不会让已有的 bundle 突然无法更新。
	opts := ValidateClusterTrustBundleOptions{}
	if newBundle.Spec.TrustBundle == oldBundle.Spec.TrustBundle {
		opts.SuppressBundleParsing = true
	}

	var allErrors field.ErrorList
	allErrors = append(allErrors, ValidateClusterTrustBundle(newBundle, opts)...)
	allErrors = append(allErrors, apivalidation.ValidateObjectMetaUpdate(&newBundle.ObjectMeta, &oldBundle.ObjectMeta, field.NewPath("metadata"))...)
	// signerName 决定 bundle 的名字前缀和信任归属，改动它等于换了一个信任域。
	allErrors = append(allErrors, apivalidation.ValidateImmutableField(newBundle.Spec.SignerName, oldBundle.Spec.SignerName, field.NewPath("spec", "signerName")).WithOrigin("immutable").MarkAlpha().MarkCoveredByDeclarative()...)
	return allErrors
}

// validateTrustBundle 校验 trustBundle 的内容：拒绝带块内头的 PEM、无法解析为
// X.509 CA 证书的块以及重复的信任锚，并要求至少提供一个信任锚。
func validateTrustBundle(path *field.Path, in string) field.ErrorList {
	var allErrors field.ErrorList

	// 先做大小检查，避免对超大输入做昂贵的 PEM 解析。
	if len(in) > certificates.MaxTrustBundleSize {
		allErrors = append(allErrors, field.TooLong(path, "" /*unused*/, certificates.MaxTrustBundleSize))
		return allErrors
	}

	blockDedupe := map[string][]int{}

	rest := []byte(in)
	var b *pem.Block
	i := -1
	for {
		b, rest = pem.Decode(rest)
		if b == nil {
			break
		}
		i++

		// 信任包里只允许证书，混入私钥等其他块会造成严重的安全问题。
		if b.Type != "CERTIFICATE" {
			allErrors = append(allErrors, field.Invalid(path, "<value omitted>", fmt.Sprintf("entry %d has bad block type: %v", i, b.Type)))
			continue
		}

		// PEM 头会影响数据解释，这里不接受任何头。
		if len(b.Headers) != 0 {
			allErrors = append(allErrors, field.Invalid(path, "<value omitted>", fmt.Sprintf("entry %d has PEM block headers", i)))
			continue
		}

		cert, err := x509.ParseCertificate(b.Bytes)
		if err != nil {
			allErrors = append(allErrors, field.Invalid(path, "<value omitted>", fmt.Sprintf("entry %d does not parse as X.509", i)))
			continue
		}

		// 信任锚必须是 CA 证书，否则不能用来验证其他证书。
		if !cert.IsCA {
			allErrors = append(allErrors, field.Invalid(path, "<value omitted>", fmt.Sprintf("entry %d does not have the CA bit set", i)))
			continue
		}

		// 基本约束无效时，CA 位本身也不可信。
		if !cert.BasicConstraintsValid {
			allErrors = append(allErrors, field.Invalid(path, "<value omitted>", fmt.Sprintf("entry %d has invalid basic constraints", i)))
			continue
		}

		blockDedupe[string(b.Bytes)] = append(blockDedupe[string(b.Bytes)], i)
	}

	// 已经有格式错误时不再报告重复或缺失信任锚，避免噪声淹没真正的问题。
	if len(allErrors) != 0 {
		return allErrors
	}

	// 空 bundle 无法提供任何信任信息。
	if len(blockDedupe) == 0 {
		allErrors = append(allErrors, field.Invalid(path, "<value omitted>", "at least one trust anchor must be provided"))
	}

	// 相同证书重复出现说明 bundle 内容有误，按原始字节去重判断。
	for _, indices := range blockDedupe {
		if len(indices) > 1 {
			allErrors = append(allErrors, field.Invalid(path, "<value omitted>", fmt.Sprintf("duplicate trust anchor (indices %v)", indices)))
		}
	}

	return allErrors
}

// ValidatePodCertificateRequestCreate 对 PodCertificateRequest 的创建执行全部校验。
// 重点校验 Pod/ServiceAccount/Node 身份信息、有效期上限和公钥持有证明。
func ValidatePodCertificateRequestCreate(req *certificates.PodCertificateRequest) field.ErrorList {
	var allErrors field.ErrorList

	metaErrors := apivalidation.ValidateObjectMeta(&req.ObjectMeta, true, apimachineryvalidation.NameIsDNSSubdomain, field.NewPath("metadata"))
	allErrors = append(allErrors, metaErrors...)

	signerNameErrors := apivalidation.ValidateSignerName(field.NewPath("spec", "signerName"), req.Spec.SignerName)
	allErrors = append(allErrors, signerNameErrors...)

	// 用户注解未经可信来源验证，但仍需满足通用的注解大小和格式限制。
	if req.Spec.UnverifiedUserAnnotations != nil {
		userAnnotationsErrors := apivalidation.ValidateUserAnnotations(req.Spec.UnverifiedUserAnnotations, field.NewPath("spec", "unverifiedUserAnnotations"))
		allErrors = append(allErrors, userAnnotationsErrors...)
	}

	// 身份字段是签发证书的依据，必须非空且符合各自的命名规则。
	for _, msg := range apivalidation.ValidatePodName(req.Spec.PodName, false) {
		allErrors = append(allErrors, field.Invalid(field.NewPath("spec", "podName"), req.Spec.PodName, msg))
	}
	if len(req.Spec.PodUID) == 0 {
		allErrors = append(allErrors, field.Invalid(field.NewPath("spec", "podUID"), req.Spec.PodUID, "must not be empty"))
	}
	if len(req.Spec.PodUID) > 128 {
		allErrors = append(allErrors, field.TooLong(field.NewPath("spec", "podUID"), req.Spec.PodUID, 128))
	}
	for _, msg := range apivalidation.ValidateServiceAccountName(req.Spec.ServiceAccountName, false) {
		allErrors = append(allErrors, field.Invalid(field.NewPath("spec", "serviceAccountName"), req.Spec.ServiceAccountName, msg))
	}
	if len(req.Spec.ServiceAccountUID) == 0 {
		allErrors = append(allErrors, field.Invalid(field.NewPath("spec", "serviceAccountUID"), req.Spec.ServiceAccountUID, "must not be empty"))
	}
	if len(req.Spec.ServiceAccountUID) > 128 {
		allErrors = append(allErrors, field.TooLong(field.NewPath("spec", "serviceAccountUID"), req.Spec.ServiceAccountUID, 128))
	}
	for _, msg := range apivalidation.ValidateNodeName(string(req.Spec.NodeName), false) {
		allErrors = append(allErrors, field.Invalid(field.NewPath("spec", "nodeName"), req.Spec.NodeName, msg))
	}
	if len(req.Spec.NodeUID) == 0 {
		allErrors = append(allErrors, field.Invalid(field.NewPath("spec", "nodeUID"), req.Spec.NodeUID, "must not be empty"))
	}
	if len(req.Spec.NodeUID) > 128 {
		allErrors = append(allErrors, field.TooLong(field.NewPath("spec", "nodeUID"), req.Spec.NodeUID, 128))
	}

	if req.Spec.MaxExpirationSeconds == nil {
		allErrors = append(allErrors, field.Required(field.NewPath("spec", "maxExpirationSeconds"), "must be set"))
		return allErrors
	}
	// Kubernetes 内置 signer 签发的证书最长 24 小时，外部 signer 放宽到 91 天。
	if apivalidation.IsKubernetesSignerName(req.Spec.SignerName) {
		if !(certificates.MinMaxExpirationSeconds <= *req.Spec.MaxExpirationSeconds && *req.Spec.MaxExpirationSeconds <= certificates.KubernetesMaxMaxExpirationSeconds) {
			allErrors = append(allErrors, field.Invalid(field.NewPath("spec", "maxExpirationSeconds"), req.Spec.MaxExpirationSeconds, fmt.Sprintf("must be in the range [%d, %d]", certificates.MinMaxExpirationSeconds, certificates.KubernetesMaxMaxExpirationSeconds)))
		}
	} else {
		if !(certificates.MinMaxExpirationSeconds <= *req.Spec.MaxExpirationSeconds && *req.Spec.MaxExpirationSeconds <= certificates.MaxMaxExpirationSeconds) {
			allErrors = append(allErrors, field.Invalid(field.NewPath("spec", "maxExpirationSeconds"), req.Spec.MaxExpirationSeconds, fmt.Sprintf("must be in the range [%d, %d]", certificates.MinMaxExpirationSeconds, certificates.MaxMaxExpirationSeconds)))
		}
	}

	// (PKIXPublicKey, ProofOfPossession) 与 StubPKCS10Request 必须二选一且只能选一个，
	// 否则无法唯一确定要签发的公钥。

	//nolint:staticcheck // SA1019 this deprecated field still needs to be validated.
	if len(req.Spec.StubPKCS10Request) != 0 && len(req.Spec.PKIXPublicKey) == 0 && len(req.Spec.ProofOfPossession) == 0 {
		// 合法组合：仅使用 StubPKCS10Request。
		allErrors = append(allErrors, validateStubPKCS10Request(req)...)
		return allErrors
	} else if len(req.Spec.StubPKCS10Request) == 0 && len(req.Spec.PKIXPublicKey) != 0 && len(req.Spec.ProofOfPossession) != 0 {
		// 合法组合：使用已废弃但仍需兼容的 PKIXPublicKey 与 ProofOfPossession。
		allErrors = append(allErrors, validateDeprecatedPKIXPublicKey(req)...)
		return allErrors
	} else {
		// 其他组合都非法。
		allErrors = append(allErrors, field.Invalid(field.NewPath("spec"), field.OmitValueType{}, "exactly one of (stubPKCS10Request) or (pkixPublicKey, proofOfPossession) must be set"))
		return allErrors
	}
}

// validateDeprecatedPKIXPublicKey 校验已废弃的公钥及持有证明字段。
// 尽管字段已废弃，历史请求仍在使用，所以必须继续严格校验其格式和签名。
func validateDeprecatedPKIXPublicKey(req *certificates.PodCertificateRequest) field.ErrorList {
	var allErrors field.ErrorList

	//nolint:staticcheck // SA1019 this deprecated field still needs to be validated.
	if len(req.Spec.PKIXPublicKey) > certificates.MaxPKIXPublicKeySize {
		allErrors = append(allErrors, field.TooLong(field.NewPath("spec", "pkixPublicKey"), req.Spec.PKIXPublicKey, certificates.MaxPKIXPublicKeySize))
		return allErrors
	}

	//nolint:staticcheck // SA1019 this deprecated field still needs to be validated.
	if len(req.Spec.ProofOfPossession) > certificates.MaxProofOfPossessionSize {
		allErrors = append(allErrors, field.TooLong(field.NewPath("spec", "proofOfPossession"), req.Spec.ProofOfPossession, certificates.MaxProofOfPossessionSize))
		return allErrors
	}

	//nolint:staticcheck // SA1019 this deprecated field still needs to be validated.
	pubAny, err := x509.ParsePKIXPublicKey(req.Spec.PKIXPublicKey)
	if err != nil {
		allErrors = append(allErrors, field.Invalid(pkixPath, req.Spec.PKIXPublicKey, "must be a valid PKIX-serialized public key"))
		return allErrors
	}

	// 校验公钥参数是否符合强度要求，并用它验证对 PodUID 的持有证明签名，
	// 确保请求方确实持有对应私钥。
	switch pub := pubAny.(type) {
	case ed25519.PublicKey:
		// ed25519 没有可调的曲线或模数参数
		if !ed25519.Verify(pub, []byte(req.Spec.PodUID), req.Spec.ProofOfPossession) {
			allErrors = append(allErrors, field.Invalid(popPath, field.OmitValueType{}, "could not verify proof-of-possession signature"))
			return allErrors
		}

	case *ecdsa.PublicKey:
		// 只允许常见的 P256/P384/P521，避免弱曲线。
		if pub.Curve != elliptic.P256() && pub.Curve != elliptic.P384() && pub.Curve != elliptic.P521() {
			allErrors = append(allErrors, field.Invalid(pkixPath, "curve "+pub.Curve.Params().Name, "elliptic public keys must use curve P256 or P384"))
			return allErrors
		}
		if !ecdsa.VerifyASN1(pub, hashBytes([]byte(req.Spec.PodUID)), req.Spec.ProofOfPossession) {
			allErrors = append(allErrors, field.Invalid(popPath, field.OmitValueType{}, "could not verify proof-of-possession signature"))
			return allErrors
		}

	case *rsa.PublicKey:
		// 低强度 RSA 密钥不允许用于证书签发。
		if pub.Size()*8 != 3072 && pub.Size()*8 != 4096 {
			allErrors = append(allErrors, field.Invalid(pkixPath, fmt.Sprintf("%d-bit modulus", pub.Size()*8), "RSA keys must have modulus size 3072 or 4096"))
			return allErrors
		}
		if err := rsa.VerifyPSS(pub, crypto.SHA256, hashBytes([]byte(req.Spec.PodUID)), req.Spec.ProofOfPossession, nil); err != nil {
			allErrors = append(allErrors, field.Invalid(popPath, field.OmitValueType{}, "could not verify proof-of-possession signature"))
			return allErrors
		}

	default:
		allErrors = append(allErrors, field.Invalid(pkixPath, req.Spec.PKIXPublicKey, "unknown public key type; supported types are Ed25519, ECDSA, and RSA"))
		return allErrors
	}

	return allErrors
}

// validateStubPKCS10Request 校验 stub PKCS#10 请求的格式、公钥参数和签名。
func validateStubPKCS10Request(req *certificates.PodCertificateRequest) field.ErrorList {
	var allErrors field.ErrorList

	if len(req.Spec.StubPKCS10Request) > certificates.MaxStubPKCS10RequestSize {
		allErrors = append(allErrors, field.TooLong(pkcs10ReqPath, req.Spec.StubPKCS10Request, certificates.MaxStubPKCS10RequestSize))
		return allErrors
	}

	pkcs10Req, err := x509.ParseCertificateRequest(req.Spec.StubPKCS10Request)
	if err != nil {
		allErrors = append(allErrors, field.Invalid(pkcs10ReqPath, field.OmitValueType{}, "must be a valid PKCS#10 CSR"))
		return allErrors
	}

	// 检查公钥类型和参数，只接受足够强度的密钥。
	switch pkcs10Pub := pkcs10Req.PublicKey.(type) {
	case ed25519.PublicKey:
		// ed25519 没有可调的曲线或模数参数
	case *ecdsa.PublicKey:
		if pkcs10Pub.Curve != elliptic.P256() && pkcs10Pub.Curve != elliptic.P384() && pkcs10Pub.Curve != elliptic.P521() {
			allErrors = append(allErrors, field.Invalid(pkcs10ReqPath, "curve "+pkcs10Pub.Curve.Params().Name, "elliptic public keys must use curve P256, P384, or P521"))
			return allErrors
		}
	case *rsa.PublicKey:
		if pkcs10Pub.Size()*8 != 3072 && pkcs10Pub.Size()*8 != 4096 {
			allErrors = append(allErrors, field.Invalid(pkcs10ReqPath, fmt.Sprintf("%d-bit modulus", pkcs10Pub.Size()*8), "RSA keys must have modulus size 3072 or 4096"))
			return allErrors
		}
	default:
		allErrors = append(allErrors, field.Invalid(pkcs10ReqPath, field.OmitValueType{}, "unknown public key type; supported types are Ed25519, ECDSA, and RSA"))
		return allErrors
	}

	// 这里有意不校验 stub CSR 的内容：kubelet 生成的 CSR 主体是空的，
	// apiserver 不关心内容才能让后续格式演进更自由。
	// Kubernetes 自带的 signer 应当在 CSR 非空时拒绝该请求。

	// 但请求本身的签名必须有效，否则公钥可以被任意替换。
	if err := pkcs10Req.CheckSignature(); err != nil {
		allErrors = append(allErrors, field.Invalid(pkcs10ReqPath, field.OmitValueType{}, "invalid signature"))
		return allErrors
	}

	return allErrors
}

// hashBytes 计算输入的 SHA-256 摘要，供 ECDSA/RSA 持有证明签名使用。
func hashBytes(in []byte) []byte {
	out := sha256.Sum256(in)
	return out[:]
}

// 以下变量缓存常用字段路径，避免在校验代码中重复构造。
var (
	pkixPath         = field.NewPath("spec", "pkixPublicKey")
	popPath          = field.NewPath("spec", "proofOfPossession")
	pkcs10ReqPath    = field.NewPath("spec", "stubPKCS10Request")
	certChainPath    = field.NewPath("status", "certificateChain")
	notBeforePath    = field.NewPath("status", "notBefore")
	notAfterPath     = field.NewPath("status", "notAfter")
	beginRefreshPath = field.NewPath("status", "beginRefreshAt")
)

// ValidatePodCertificateRequestUpdate 校验非 status 的 PCR 更新。
// 创建后所有 spec 字段都不可变，status 更新必须走专门的 status 子资源，
// 因此这里只允许修改 metadata。
func ValidatePodCertificateRequestUpdate(newReq, oldReq *certificates.PodCertificateRequest) field.ErrorList {
	var allErrors field.ErrorList
	allErrors = append(allErrors, apivalidation.ValidateObjectMetaUpdate(&newReq.ObjectMeta, &oldReq.ObjectMeta, field.NewPath("metadata"))...)

	// spec 描述身份和公钥，一旦可变就会破坏证书与请求方的绑定关系。
	allErrors = append(allErrors, apivalidation.ValidateImmutableField(newReq.Spec, oldReq.Spec, field.NewPath("spec"))...)

	return allErrors
}

// ValidatePodCertificateRequestStatusUpdate 校验 PodCertificateRequest 的 status 更新。
// 该方法检查条件合法性、签发证书与请求公钥的一致性，以及终态之后 status 的不可变性。
func ValidatePodCertificateRequestStatusUpdate(newReq, oldReq *certificates.PodCertificateRequest, clock clock.PassiveClock) field.ErrorList {
	var allErrors field.ErrorList

	// metadata 基本不可变，但 ManagedFields 允许变化。
	// 这里依赖调用方 strategy 已用 metav1.ResetObjectMetaForStatus 处理过 newReq.ObjectMeta。
	allErrors = append(allErrors, apivalidation.ValidateObjectMetaUpdate(&newReq.ObjectMeta, &oldReq.ObjectMeta, field.NewPath("metadata"))...)
	if len(allErrors) > 0 {
		return allErrors
	}

	// 不校验 spec：调用方 strategy 已经用旧对象的 spec 覆盖了它。

	// 已知条件最多出现一次，且状态必须为 True。
	numKnownConditions := 0
	for i, cond := range newReq.Status.Conditions {
		switch cond.Type {
		case certificates.PodCertificateRequestConditionTypeIssued, certificates.PodCertificateRequestConditionTypeDenied, certificates.PodCertificateRequestConditionTypeFailed:
			numKnownConditions++
			if numKnownConditions > 1 {
				allErrors = append(allErrors, field.Invalid(field.NewPath("status", "conditions", formatIndex(i), "type"), cond.Type, `There may be at most one condition with type "Issued", "Denied", or "Failed"`))
			}
			if cond.Status != metav1.ConditionTrue {
				allErrors = append(allErrors, field.NotSupported(field.NewPath("status", "conditions", formatIndex(i), "status"), cond.Status, []metav1.ConditionStatus{metav1.ConditionTrue}))
			}
		default:
			allErrors = append(allErrors, field.NotSupported(field.NewPath("status", "conditions", formatIndex(i), "type"), cond.Type, []string{certificates.PodCertificateRequestConditionTypeIssued, certificates.PodCertificateRequestConditionTypeDenied, certificates.PodCertificateRequestConditionTypeFailed}))
		}
	}

	allErrors = append(allErrors, metav1validation.ValidateConditions(newReq.Status.Conditions, field.NewPath("status", "conditions"))...)

	// 条件决定了后续 status 字段该如何校验，条件本身有问题时提前返回。
	if len(allErrors) > 0 {
		return allErrors
	}

	// 旧对象已进入终态时，整个 status 都不再允许变化，避免签发结果被篡改。
	if pcrIsIssued(oldReq) || pcrIsDenied(oldReq) || pcrIsFailed(oldReq) {
		allErrors = append(allErrors, validateSemanticEquality(newReq.Status, oldReq.Status, field.NewPath("status"), "immutable after PodCertificateRequest is issued, denied, or failed")...)
		return allErrors
	}

	// 正在转入 Denied/Failed 终态时，只保留条件，不能携带证书等其他状态。
	if pcrIsDenied(newReq) || pcrIsFailed(newReq) {
		// 除条件外的 status 字段都必须为空。
		wantStatus := certificates.PodCertificateRequestStatus{
			Conditions: newReq.Status.Conditions,
		}
		allErrors = append(allErrors, validateSemanticEquality(newReq.Status, wantStatus, field.NewPath("status"), "non-condition status fields must be empty when denying or failing the PodCertificateRequest")...)
		return allErrors
	}

	// 正在转入 Issued 终态，需要完整校验签发的证书链和时间字段。
	if pcrIsIssued(newReq) {
		if len(newReq.Status.CertificateChain) > certificates.MaxCertificateChainSize {
			allErrors = append(allErrors, field.TooLong(field.NewPath("status", "certificateChain"), newReq.Status.CertificateChain, certificates.MaxCertificateChainSize))
			return allErrors
		}

		// 证书链的第一个块必须是叶子证书，后续块为可选的中间证书。
		leafBlock, rest := pem.Decode([]byte(newReq.Status.CertificateChain))
		if leafBlock == nil {
			allErrors = append(allErrors, field.Invalid(certChainPath, newReq.Status.CertificateChain, "issued certificate chain must contain at least one certificate"))
			return allErrors
		}
		if leafBlock.Type != "CERTIFICATE" {
			allErrors = append(allErrors, field.Invalid(certChainPath, newReq.Status.CertificateChain, "issued certificate chain must consist entirely of CERTIFICATE PEM blocks"))
			return allErrors
		}

		leafCert, err := x509.ParseCertificate(leafBlock.Bytes)
		if err != nil {
			allErrors = append(allErrors, field.Invalid(certChainPath, newReq.Status.CertificateChain, "leaf certificate does not parse as valid X.509"))
			return allErrors
		}
		// 拒绝畸形 DNS 名，避免签发出无法使用或存在歧义的证书。
		for _, dnsName := range leafCert.DNSNames {
			if dnsName == "" {
				allErrors = append(allErrors, field.Invalid(certChainPath, dnsName, "leaf certificate should not contain empty DNSName"))
			}
			if strings.Contains(dnsName, "..") {
				allErrors = append(allErrors, field.Invalid(certChainPath, dnsName, "leaf certificate's DNSName should not contain '..'"))
			}
			if strings.HasPrefix(dnsName, ".") || strings.HasSuffix(dnsName, ".") {
				allErrors = append(allErrors, field.Invalid(certChainPath, dnsName, "leaf certificate's DNSName should not start or end with '.'"))
			}
		}
		// 邮箱地址同样必须可解析，否则证书中的身份信息不可用。
		for _, emailAddress := range leafCert.EmailAddresses {
			if _, err := mail.ParseAddress(emailAddress); err != nil {
				allErrors = append(allErrors, field.Invalid(certChainPath, emailAddress, "leaf certificate should not contain invalid EmailAddress"))
			}
		}

		// Get the public key from either StubPKCS10Request or PKIXPublicKey.
		var wantPKAny crypto.PublicKey
		if len(oldReq.Spec.StubPKCS10Request) != 0 {
			pkcs10Req, err := x509.ParseCertificateRequest(oldReq.Spec.StubPKCS10Request)
			if err != nil {
				allErrors = append(allErrors, field.Invalid(pkcs10ReqPath, field.OmitValueType{}, "must be a valid PKCS#10 CSR"))
				return allErrors
			}
			wantPKAny = pkcs10Req.PublicKey
		} else {
			//nolint:staticcheck // SA1019 this deprecated field still needs to be validated.
			wantPKAny, err = x509.ParsePKIXPublicKey(oldReq.Spec.PKIXPublicKey)
			if err != nil {
				allErrors = append(allErrors, field.Invalid(pkixPath, field.OmitValueType{}, "must be a valid PKIX-serialized public key"))
				return allErrors
			}
		}

		// 叶子证书必须签发给了 spec 中请求的那把公钥，防止证书被冒领。
		switch wantPK := wantPKAny.(type) {
		case ed25519.PublicKey:
			if !wantPK.Equal(leafCert.PublicKey) {
				allErrors = append(allErrors, field.Invalid(certChainPath, newReq.Status.CertificateChain, "leaf certificate was not issued to the requested public key"))
				return allErrors
			}
		case *rsa.PublicKey:
			if !wantPK.Equal(leafCert.PublicKey) {
				allErrors = append(allErrors, field.Invalid(certChainPath, newReq.Status.CertificateChain, "leaf certificate was not issued to the requested public key"))
				return allErrors
			}
		case *ecdsa.PublicKey:
			if !wantPK.Equal(leafCert.PublicKey) {
				allErrors = append(allErrors, field.Invalid(certChainPath, newReq.Status.CertificateChain, "leaf certificate was not issued to the requested public key"))
				return allErrors
			}
		}

		// 三个时间字段必须同时存在，否则无法校验有效期和刷新窗口。
		if newReq.Status.NotBefore == nil {
			allErrors = append(allErrors, field.Required(notBeforePath, "must be present and consistent with the issued certificate"))
		}
		if newReq.Status.NotAfter == nil {
			allErrors = append(allErrors, field.Required(notAfterPath, "must be present and consistent with the issued certificate"))
		}
		if newReq.Status.BeginRefreshAt == nil {
			allErrors = append(allErrors, field.Required(beginRefreshPath, "must be present and in the range [notbefore+10min, notafter-10min]"))
		}
		if len(allErrors) > 0 {
			return allErrors
		}

		// NotBefore 必须与证书中的值一致，且不能偏离当前时间太远，
		// 避免签发过期证书或提前生效的证书。
		if !newReq.Status.NotBefore.Time.Equal(leafCert.NotBefore) {
			allErrors = append(allErrors, field.Invalid(notBeforePath, newReq.Status.NotBefore.Time, "must be set to the NotBefore time encoded in the leaf certificate"))
			return allErrors
		}
		if !timeNear(newReq.Status.NotBefore.Time, clock.Now(), 5*time.Minute) {
			allErrors = append(allErrors, field.Invalid(notBeforePath, newReq.Status.NotBefore.Time, "must be set to within 5 minutes of kube-apiserver's current time"))
			return allErrors
		}

		// NotAfter 必须与证书中的值一致。
		if !newReq.Status.NotAfter.Time.Equal(leafCert.NotAfter) {
			allErrors = append(allErrors, field.Invalid(notAfterPath, newReq.Status.NotAfter.Time, "must be set to the NotAfter time encoded in the leaf certificate"))
			return allErrors
		}

		// 有效期需在最低 1 小时和 spec.maxExpirationSeconds 之间。
		lifetime := leafCert.NotAfter.Sub(leafCert.NotBefore)
		if lifetime < 1*time.Hour {
			allErrors = append(allErrors, field.Invalid(certChainPath, lifetime, "leaf certificate lifetime must be >= 1 hour"))
			return allErrors
		}
		if lifetime > time.Duration(*newReq.Spec.MaxExpirationSeconds)*time.Second {
			allErrors = append(allErrors, field.Invalid(certChainPath, lifetime, fmt.Sprintf("leaf certificate lifetime must be <= spec.maxExpirationSeconds (%v)", *newReq.Spec.MaxExpirationSeconds)))
			return allErrors
		}

		// 刷新时间必须落在有效期内并留出 10 分钟余量，给轮换留够时间。
		if newReq.Status.BeginRefreshAt.Time.Before(newReq.Status.NotBefore.Time.Add(10 * time.Minute)) {
			allErrors = append(allErrors, field.Invalid(beginRefreshPath, newReq.Status.BeginRefreshAt.Time, "must be at least 10 minutes after status.notBefore"))
			return allErrors
		}
		if newReq.Status.BeginRefreshAt.Time.After(newReq.Status.NotAfter.Time.Add(-10 * time.Minute)) {
			allErrors = append(allErrors, field.Invalid(beginRefreshPath, newReq.Status.BeginRefreshAt.Time, "must be at least 10 minutes before status.notAfter"))
			return allErrors
		}

		// 对证书链的其余部分，只检查每个块是否为合法证书。
		// Go 的 X.509 库有意不支持校验不完整的链，因此这里不做链验证。
		for {
			var nextBlock *pem.Block
			nextBlock, rest = pem.Decode(rest)
			if nextBlock == nil {
				break
			}

			if nextBlock.Type != "CERTIFICATE" {
				allErrors = append(allErrors, field.Invalid(certChainPath, newReq.Status.CertificateChain, "issued certificate chain must consist entirely of CERTFICATE PEM blocks"))
				return allErrors
			}

			_, err := x509.ParseCertificate(nextBlock.Bytes)
			if err != nil {
				allErrors = append(allErrors, field.Invalid(certChainPath, newReq.Status.CertificateChain, "intermediate certificate does not parse as valid X.509"))
				return allErrors
			}
		}

		return allErrors
	}

	// 没有进入任何终态时，整个 status 都不可变，杜绝中间状态的随意改写。
	allErrors = append(allErrors, validateSemanticEquality(newReq.Status, oldReq.Status, field.NewPath("status"), `status is immutable unless transitioning to "Issued", "Denied", or "Failed"`)...)
	return allErrors
}

// pcrIsIssued 判断 PCR 是否已带有状态为 True 的 Issued 条件。
func pcrIsIssued(pcr *certificates.PodCertificateRequest) bool {
	for _, cond := range pcr.Status.Conditions {
		if cond.Type == certificates.PodCertificateRequestConditionTypeIssued && cond.Status == metav1.ConditionTrue {
			return true
		}
	}
	return false
}

// pcrIsDenied 判断 PCR 是否已带有状态为 True 的 Denied 条件。
func pcrIsDenied(pcr *certificates.PodCertificateRequest) bool {
	for _, cond := range pcr.Status.Conditions {
		if cond.Type == certificates.PodCertificateRequestConditionTypeDenied && cond.Status == metav1.ConditionTrue {
			return true
		}
	}
	return false
}

// pcrIsFailed 判断 PCR 是否已带有状态为 True 的 Failed 条件。
func pcrIsFailed(pcr *certificates.PodCertificateRequest) bool {
	for _, cond := range pcr.Status.Conditions {
		if cond.Type == certificates.PodCertificateRequestConditionTypeFailed && cond.Status == metav1.ConditionTrue {
			return true
		}
	}
	return false
}

// formatIndex 把下标格式化为条件字段路径中使用的 "[n]" 形式。
func formatIndex(i int) string {
	return "[" + strconv.Itoa(i) + "]"
}

// validateSemanticEquality 类似 apivalidation.ValidateImmutableField，
// 但可以自定义错误详情，用于给出更具体的不可变原因。
func validateSemanticEquality(oldVal, newVal any, fldPath *field.Path, detail string) field.ErrorList {
	allErrs := field.ErrorList{}
	if !apiequality.Semantic.DeepEqual(oldVal, newVal) {
		allErrs = append(allErrs, field.Invalid(fldPath, field.OmitValueType{}, detail))
	}
	return allErrs
}

// timeNear 判断两个时间的差值是否在 skew 范围内。
func timeNear(a, b time.Time, skew time.Duration) bool {
	return a.After(b.Add(-skew)) && a.Before(b.Add(skew))
}
