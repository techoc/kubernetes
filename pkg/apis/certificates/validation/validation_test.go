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
	"crypto"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	mathrand "math/rand"
	"reflect"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	apimachineryvalidation "k8s.io/apimachinery/pkg/api/validation"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/client-go/util/certificate/csr"
	capi "k8s.io/kubernetes/pkg/apis/certificates"
	"k8s.io/kubernetes/pkg/apis/core"
	testclock "k8s.io/utils/clock/testing"
	"k8s.io/utils/ptr"
)

var (
	validObjectMeta = metav1.ObjectMeta{Name: "testcsr"}
	validSignerName = "example.com/valid-name"
	validUsages     = []capi.KeyUsage{capi.UsageKeyEncipherment}
)

// TestValidateCertificateSigningRequestCreate 表驱动验证 CSR 创建时的 spec 校验：
// 依次覆盖 request（PEM 内容）、usages（非空、合法、不重复）、signerName（格式、域名限定、
// 路径标签合法性、长度上限）以及 expirationSeconds（最短 10 分钟）等必填与约束；
// 状态条件部分验证 Approved/Denied 互斥、Failed 可与 Approved 共存，
// 防止创建阶段就写入逻辑上互相矛盾的条件组合。
func TestValidateCertificateSigningRequestCreate(t *testing.T) {
	specPath := field.NewPath("spec")
	// maxLengthSignerName 按 validation.go 中的长度上限构造出最长的 signerName：
	// 形式为 <fqdn(253)>/<resource-namespace(63)>.<resource-name(253)>，
	// 长度定义参考 validation.go 中的上限规格。
	maxLengthFQDN := fmt.Sprintf("%s.%s.%s.%s", strings.Repeat("a", 63), strings.Repeat("a", 63), strings.Repeat("a", 63), strings.Repeat("a", 61))
	maxLengthSignerName := fmt.Sprintf("%s/%s.%s", maxLengthFQDN, strings.Repeat("a", 63), strings.Repeat("a", 253))
	tests := map[string]struct {
		csr  capi.CertificateSigningRequest
		errs field.ErrorList
	}{
		"CSR with empty request data should fail": {
			csr: capi.CertificateSigningRequest{
				ObjectMeta: validObjectMeta,
				Spec: capi.CertificateSigningRequestSpec{
					Usages:     validUsages,
					SignerName: validSignerName,
				},
			},
			errs: field.ErrorList{
				field.Invalid(specPath.Child("request"), []byte(nil), "PEM block type must be CERTIFICATE REQUEST"),
			},
		},
		"CSR with invalid request data should fail": {
			csr: capi.CertificateSigningRequest{
				ObjectMeta: validObjectMeta,
				Spec: capi.CertificateSigningRequestSpec{
					Usages:     validUsages,
					SignerName: validSignerName,
					Request:    []byte("invalid data"),
				},
			},
			errs: field.ErrorList{
				field.Invalid(specPath.Child("request"), []byte("invalid data"), "PEM block type must be CERTIFICATE REQUEST"),
			},
		},
		"CSR with no usages should fail": {
			csr: capi.CertificateSigningRequest{
				ObjectMeta: validObjectMeta,
				Spec: capi.CertificateSigningRequestSpec{
					SignerName: validSignerName,
					Request:    newCSRPEM(t),
				},
			},
			errs: field.ErrorList{
				field.Required(specPath.Child("usages"), ""),
			},
		},
		"CSR with no signerName set should fail": {
			csr: capi.CertificateSigningRequest{
				ObjectMeta: validObjectMeta,
				Spec: capi.CertificateSigningRequestSpec{
					Usages:  validUsages,
					Request: newCSRPEM(t),
				},
			},
			errs: field.ErrorList{
				field.Required(specPath.Child("signerName"), ""),
			},
		},
		"signerName contains no '/'": {
			csr: capi.CertificateSigningRequest{
				ObjectMeta: validObjectMeta,
				Spec: capi.CertificateSigningRequestSpec{
					Usages:     validUsages,
					Request:    newCSRPEM(t),
					SignerName: "an-invalid-signer-name",
				},
			},
			errs: field.ErrorList{
				field.Invalid(specPath.Child("signerName"), "an-invalid-signer-name", "must be a fully qualified domain and path of the form 'example.com/signer-name'"),
			},
		},
		"signerName contains two '/'": {
			csr: capi.CertificateSigningRequest{
				ObjectMeta: validObjectMeta,
				Spec: capi.CertificateSigningRequestSpec{
					Usages:     validUsages,
					Request:    newCSRPEM(t),
					SignerName: "an-invalid-signer-name.com/something/else",
				},
			},
			errs: field.ErrorList{
				field.Invalid(specPath.Child("signerName"), "an-invalid-signer-name.com/something/else", "must be a fully qualified domain and path of the form 'example.com/signer-name'"),
			},
		},
		"signerName domain component is not fully qualified": {
			csr: capi.CertificateSigningRequest{
				ObjectMeta: validObjectMeta,
				Spec: capi.CertificateSigningRequestSpec{
					Usages:     validUsages,
					Request:    newCSRPEM(t),
					SignerName: "example/some-signer-name",
				},
			},
			errs: field.ErrorList{
				field.Invalid(specPath.Child("signerName"), "example", "should be a domain with at least two segments separated by dots"),
			},
		},
		"signerName path component is empty": {
			csr: capi.CertificateSigningRequest{
				ObjectMeta: validObjectMeta,
				Spec: capi.CertificateSigningRequestSpec{
					Usages:     validUsages,
					Request:    newCSRPEM(t),
					SignerName: "example.com/",
				},
			},
			errs: field.ErrorList{
				field.Invalid(specPath.Child("signerName"), "", `validating label "": a lowercase RFC 1123 subdomain must consist of lower case alphanumeric characters, '-' or '.', and must start and end with an alphanumeric character (e.g. 'example.com', regex used for validation is '[a-z0-9]([-a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*')`),
			},
		},
		"signerName path component ends with a symbol": {
			csr: capi.CertificateSigningRequest{
				ObjectMeta: validObjectMeta,
				Spec: capi.CertificateSigningRequestSpec{
					Usages:     validUsages,
					Request:    newCSRPEM(t),
					SignerName: "example.com/something-",
				},
			},
			errs: field.ErrorList{
				field.Invalid(specPath.Child("signerName"), "something-", `validating label "something-": a lowercase RFC 1123 subdomain must consist of lower case alphanumeric characters, '-' or '.', and must start and end with an alphanumeric character (e.g. 'example.com', regex used for validation is '[a-z0-9]([-a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*')`),
			},
		},
		"signerName path component is a symbol": {
			csr: capi.CertificateSigningRequest{
				ObjectMeta: validObjectMeta,
				Spec: capi.CertificateSigningRequestSpec{
					Usages:     validUsages,
					Request:    newCSRPEM(t),
					SignerName: "example.com/-",
				},
			},
			errs: field.ErrorList{
				field.Invalid(specPath.Child("signerName"), "-", `validating label "-": a lowercase RFC 1123 subdomain must consist of lower case alphanumeric characters, '-' or '.', and must start and end with an alphanumeric character (e.g. 'example.com', regex used for validation is '[a-z0-9]([-a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*')`),
			},
		},
		"signerName path component contains no '.' but is valid": {
			csr: capi.CertificateSigningRequest{
				ObjectMeta: validObjectMeta,
				Spec: capi.CertificateSigningRequestSpec{
					Usages:     validUsages,
					Request:    newCSRPEM(t),
					SignerName: "example.com/some-signer-name",
				},
			},
		},
		"signerName with a total length greater than 571 characters should be rejected": {
			csr: capi.CertificateSigningRequest{
				ObjectMeta: validObjectMeta,
				Spec: capi.CertificateSigningRequestSpec{
					Usages:  validUsages,
					Request: newCSRPEM(t),
					// 该字符串超过 signerName 的长度上限（635 字符），用于触发 TooLong 错误。
					SignerName: maxLengthSignerName + ".toolong",
				},
			},
			errs: field.ErrorList{
				field.TooLong(specPath.Child("signerName"), "" /*unused*/, len(maxLengthSignerName)),
			},
		},
		"signerName with a fqdn greater than 253 characters should be rejected": {
			csr: capi.CertificateSigningRequest{
				ObjectMeta: validObjectMeta,
				Spec: capi.CertificateSigningRequestSpec{
					Usages:  validUsages,
					Request: newCSRPEM(t),
					// FQDN 部分超过 253 字符，即使总长未超限也应单独被拒绝。
					SignerName: fmt.Sprintf("%s.extra/valid-path", maxLengthFQDN),
				},
			},
			errs: field.ErrorList{
				field.TooLong(specPath.Child("signerName"), "" /*unused*/, len(maxLengthFQDN)),
			},
		},
		"signerName can have a longer path if the domain component is less than the max length": {
			csr: capi.CertificateSigningRequest{
				ObjectMeta: validObjectMeta,
				Spec: capi.CertificateSigningRequestSpec{
					Usages:     validUsages,
					Request:    newCSRPEM(t),
					SignerName: fmt.Sprintf("abc.io/%s.%s", strings.Repeat("a", 253), strings.Repeat("a", 253)),
				},
			},
		},
		"signerName with a domain label greater than 63 characters will fail": {
			csr: capi.CertificateSigningRequest{
				ObjectMeta: validObjectMeta,
				Spec: capi.CertificateSigningRequestSpec{
					Usages:     validUsages,
					Request:    newCSRPEM(t),
					SignerName: fmt.Sprintf("%s.example.io/valid-path", strings.Repeat("a", 66)),
				},
			},
			errs: field.ErrorList{
				field.Invalid(specPath.Child("signerName"), fmt.Sprintf("%s.example.io", strings.Repeat("a", 66)), fmt.Sprintf(`validating label "%s": must be no more than 63 characters`, strings.Repeat("a", 66))),
			},
		},
		"signerName of max length in format <fully-qualified-domain-name>/<resource-namespace>.<resource-name> is valid": {
			// 该用例验证 signerName 恰好取到上限时仍然合法：
			// 规定这个长度和格式是为了容纳形如下述结构的 signerName：
			// <fqdn>/<resource-namespace>.<resource-name>。
			// FQDN 最长 253 字符（DNS1123Subdomain 的上限）。
			// 命名空间最长 63 字符（DNS1123Label 的上限）。
			// 资源名最长 253 字符（DNS1123Subdomain 的上限）。
			// 再加上 '.' 和 '/' 各 1 个字符，共 2 个。
			csr: capi.CertificateSigningRequest{
				ObjectMeta: validObjectMeta,
				Spec: capi.CertificateSigningRequestSpec{
					Usages:     validUsages,
					Request:    newCSRPEM(t),
					SignerName: maxLengthSignerName,
				},
			},
		},
		"negative duration": {
			csr: capi.CertificateSigningRequest{
				ObjectMeta: validObjectMeta,
				Spec: capi.CertificateSigningRequestSpec{
					Usages:            validUsages,
					Request:           newCSRPEM(t),
					SignerName:        validSignerName,
					ExpirationSeconds: ptr.To[int32](-1),
				},
			},
			errs: field.ErrorList{
				field.Invalid(specPath.Child("expirationSeconds"), int32(-1), "may not specify a duration less than 600 seconds (10 minutes)"),
			},
		},
		"zero duration": {
			csr: capi.CertificateSigningRequest{
				ObjectMeta: validObjectMeta,
				Spec: capi.CertificateSigningRequestSpec{
					Usages:            validUsages,
					Request:           newCSRPEM(t),
					SignerName:        validSignerName,
					ExpirationSeconds: ptr.To[int32](0),
				},
			},
			errs: field.ErrorList{
				field.Invalid(specPath.Child("expirationSeconds"), int32(0), "may not specify a duration less than 600 seconds (10 minutes)"),
			},
		},
		"one duration": {
			csr: capi.CertificateSigningRequest{
				ObjectMeta: validObjectMeta,
				Spec: capi.CertificateSigningRequestSpec{
					Usages:            validUsages,
					Request:           newCSRPEM(t),
					SignerName:        validSignerName,
					ExpirationSeconds: ptr.To[int32](1),
				},
			},
			errs: field.ErrorList{
				field.Invalid(specPath.Child("expirationSeconds"), int32(1), "may not specify a duration less than 600 seconds (10 minutes)"),
			},
		},
		"too short duration": {
			csr: capi.CertificateSigningRequest{
				ObjectMeta: validObjectMeta,
				Spec: capi.CertificateSigningRequestSpec{
					Usages:            validUsages,
					Request:           newCSRPEM(t),
					SignerName:        validSignerName,
					ExpirationSeconds: csr.DurationToExpirationSeconds(time.Minute),
				},
			},
			errs: field.ErrorList{
				field.Invalid(specPath.Child("expirationSeconds"), *csr.DurationToExpirationSeconds(time.Minute), "may not specify a duration less than 600 seconds (10 minutes)"),
			},
		},
		"valid duration": {
			csr: capi.CertificateSigningRequest{
				ObjectMeta: validObjectMeta,
				Spec: capi.CertificateSigningRequestSpec{
					Usages:            validUsages,
					Request:           newCSRPEM(t),
					SignerName:        validSignerName,
					ExpirationSeconds: csr.DurationToExpirationSeconds(10 * time.Minute),
				},
			},
		},
		"missing usages": {
			csr: capi.CertificateSigningRequest{
				ObjectMeta: validObjectMeta,
				Spec: capi.CertificateSigningRequestSpec{
					Usages:     []capi.KeyUsage{},
					Request:    newCSRPEM(t),
					SignerName: validSignerName,
				},
			},
			errs: field.ErrorList{
				field.Required(specPath.Child("usages"), ""),
			},
		},
		"unknown and duplicate usages": {
			csr: capi.CertificateSigningRequest{
				ObjectMeta: validObjectMeta,
				Spec: capi.CertificateSigningRequestSpec{
					Usages:     []capi.KeyUsage{"unknown", "unknown"},
					Request:    newCSRPEM(t),
					SignerName: validSignerName,
				},
			},
			errs: field.ErrorList{
				field.NotSupported(specPath.Child("usages").Index(0), capi.KeyUsage("unknown"), allValidUsages.List()),
				field.NotSupported(specPath.Child("usages").Index(1), capi.KeyUsage("unknown"), allValidUsages.List()),
				field.Duplicate(specPath.Child("usages").Index(1), capi.KeyUsage("unknown")),
			},
		},
		"approved condition only": {
			csr: capi.CertificateSigningRequest{
				ObjectMeta: validObjectMeta,
				Spec: capi.CertificateSigningRequestSpec{
					Usages:     validUsages,
					Request:    newCSRPEM(t),
					SignerName: validSignerName,
				},
				Status: capi.CertificateSigningRequestStatus{
					Conditions: []capi.CertificateSigningRequestCondition{
						{Type: capi.CertificateApproved, Status: core.ConditionTrue},
					},
				},
			},
		},
		"denied condition only": {
			csr: capi.CertificateSigningRequest{
				ObjectMeta: validObjectMeta,
				Spec: capi.CertificateSigningRequestSpec{
					Usages:     validUsages,
					Request:    newCSRPEM(t),
					SignerName: validSignerName,
				},
				Status: capi.CertificateSigningRequestStatus{
					Conditions: []capi.CertificateSigningRequestCondition{
						{Type: capi.CertificateDenied, Status: core.ConditionTrue},
					},
				},
			},
		},
		"both approved and denied conditions": {
			csr: capi.CertificateSigningRequest{
				ObjectMeta: validObjectMeta,
				Spec: capi.CertificateSigningRequestSpec{
					Usages:     validUsages,
					Request:    newCSRPEM(t),
					SignerName: validSignerName,
				},
				Status: capi.CertificateSigningRequestStatus{
					Conditions: []capi.CertificateSigningRequestCondition{
						{Type: capi.CertificateApproved, Status: core.ConditionTrue},
						{Type: capi.CertificateDenied, Status: core.ConditionTrue},
					},
				},
			},
			errs: field.ErrorList{
				field.Invalid(field.NewPath("status", "conditions"), capi.CertificateDenied, "Approved and Denied conditions are mutually exclusive").WithOrigin("zeroOrOneOf").MarkCoveredByDeclarative(),
			},
		},
		"approved and failed conditions allowed": {
			csr: capi.CertificateSigningRequest{
				ObjectMeta: validObjectMeta,
				Spec: capi.CertificateSigningRequestSpec{
					Usages:     validUsages,
					Request:    newCSRPEM(t),
					SignerName: validSignerName,
				},
				Status: capi.CertificateSigningRequestStatus{
					Conditions: []capi.CertificateSigningRequestCondition{
						{Type: capi.CertificateApproved, Status: core.ConditionTrue},
						{Type: capi.CertificateFailed, Status: core.ConditionTrue},
					},
				},
			},
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			el := ValidateCertificateSigningRequestCreate(&test.csr)
			if !reflect.DeepEqual(el, test.errs) {
				t.Errorf("returned and expected errors did not match - expected\n%v\nbut got\n%v", test.errs.ToAggregate(), el.ToAggregate())
			}
		})
	}
}

// newCSRPEM 用随机 Ed25519 密钥现场生成一个合法的 CERTIFICATE REQUEST PEM 块，
// 供各用例作为 spec.request 的有效基线；失败时直接 Fatal，因为后续所有用例都依赖它。
func newCSRPEM(t *testing.T) []byte {
	template := &x509.CertificateRequest{
		Subject: pkix.Name{
			Organization: []string{"testing-org"},
		},
	}

	_, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}

	csrDER, err := x509.CreateCertificateRequest(rand.Reader, template, key)
	if err != nil {
		t.Fatal(err)
	}

	csrPemBlock := &pem.Block{
		Type:  "CERTIFICATE REQUEST",
		Bytes: csrDER,
	}

	p := pem.EncodeToMemory(csrPemBlock)
	if p == nil {
		t.Fatal("invalid pem block")
	}

	return p
}

// Test_getValidationOptions 验证兼容性选项的推导逻辑：
// 新建和干净对象走严格校验；旧对象里出现互斥条件、重复/空条件类型、legacy signer、
// 未知或重复 usages、以及证书内容未变（或本来就是非法内容）时，
// 自动生成对应的宽容选项，使历史遗留对象在更新时不被新规则卡死。
func Test_getValidationOptions(t *testing.T) {
	tests := []struct {
		name   string
		newCSR *capi.CertificateSigningRequest
		oldCSR *capi.CertificateSigningRequest
		want   certificateValidationOptions
	}{{
		name:   "strict create",
		oldCSR: nil,
		want:   certificateValidationOptions{},
	}, {
		name:   "strict update",
		oldCSR: &capi.CertificateSigningRequest{},
		want:   certificateValidationOptions{},
	}, {
		name: "compatible update, approved+denied",
		oldCSR: &capi.CertificateSigningRequest{Status: capi.CertificateSigningRequestStatus{
			Conditions: []capi.CertificateSigningRequestCondition{{Type: capi.CertificateApproved}, {Type: capi.CertificateDenied}},
		}},
		want: certificateValidationOptions{
			allowBothApprovedAndDenied: true,
		},
	}, {
		name:   "compatible update, legacy signerName",
		oldCSR: &capi.CertificateSigningRequest{Spec: capi.CertificateSigningRequestSpec{SignerName: capi.LegacyUnknownSignerName}},
		want: certificateValidationOptions{
			allowLegacySignerName: true,
		},
	}, {
		name: "compatible update, duplicate condition types",
		oldCSR: &capi.CertificateSigningRequest{Status: capi.CertificateSigningRequestStatus{
			Conditions: []capi.CertificateSigningRequestCondition{{Type: capi.CertificateApproved}, {Type: capi.CertificateApproved}},
		}},
		want: certificateValidationOptions{
			allowDuplicateConditionTypes: true,
		},
	}, {
		name: "compatible update, empty condition types",
		oldCSR: &capi.CertificateSigningRequest{Status: capi.CertificateSigningRequestStatus{
			Conditions: []capi.CertificateSigningRequestCondition{{}},
		}},
		want: certificateValidationOptions{
			allowEmptyConditionType: true,
		},
	}, {
		name: "compatible update, no diff to certificate",
		newCSR: &capi.CertificateSigningRequest{Status: capi.CertificateSigningRequestStatus{
			Certificate: validCertificate,
		}},
		oldCSR: &capi.CertificateSigningRequest{Status: capi.CertificateSigningRequestStatus{
			Certificate: validCertificate,
		}},
		want: certificateValidationOptions{
			allowArbitraryCertificate: true,
		},
	}, {
		name: "compatible update, existing invalid certificate",
		newCSR: &capi.CertificateSigningRequest{Status: capi.CertificateSigningRequestStatus{
			Certificate: []byte(`new - no PEM blocks`),
		}},
		oldCSR: &capi.CertificateSigningRequest{Status: capi.CertificateSigningRequestStatus{
			Certificate: []byte(`old - no PEM blocks`),
		}},
		want: certificateValidationOptions{
			allowArbitraryCertificate: true,
		},
	}, {
		name:   "compatible update, existing unknown usages",
		oldCSR: &capi.CertificateSigningRequest{Spec: capi.CertificateSigningRequestSpec{Usages: []capi.KeyUsage{"unknown"}}},
		want: certificateValidationOptions{
			allowUnknownUsages: true,
		},
	}, {
		name:   "compatible update, existing duplicate usages",
		oldCSR: &capi.CertificateSigningRequest{Spec: capi.CertificateSigningRequestSpec{Usages: []capi.KeyUsage{"any", "any"}}},
		want: certificateValidationOptions{
			allowDuplicateUsages: true,
		},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := getValidationOptions(tt.newCSR, tt.oldCSR); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("got  %#v\nwant %#v", got, tt.want)
			}
		})
	}
}

// TestValidateCertificateSigningRequestUpdate 验证普通更新子资源时的约束：
// 审批条件（Approved/Denied）和证书内容不允许随意增删，Failed 条件与 finalizer 可变更；
// 用集合比较错误而非顺序比较，因为一次更新可能同时产生多条独立错误。
func TestValidateCertificateSigningRequestUpdate(t *testing.T) {
	validUpdateMeta := validObjectMeta
	validUpdateMeta.ResourceVersion = "1"

	validUpdateMetaWithFinalizers := validUpdateMeta
	validUpdateMetaWithFinalizers.Finalizers = []string{"foo"}

	validSpec := capi.CertificateSigningRequestSpec{
		Usages:     validUsages,
		Request:    newCSRPEM(t),
		SignerName: "example.com/something",
	}

	tests := []struct {
		name   string
		newCSR *capi.CertificateSigningRequest
		oldCSR *capi.CertificateSigningRequest
		errs   []string
	}{{
		name:   "no-op",
		newCSR: &capi.CertificateSigningRequest{ObjectMeta: validUpdateMeta, Spec: validSpec},
		oldCSR: &capi.CertificateSigningRequest{ObjectMeta: validUpdateMeta, Spec: validSpec},
	}, {
		// 已有非法证书时只改 finalizer 应放行：已有脏数据不能阻塞元数据的正常维护。
		name:   "finalizer change with invalid status",
		newCSR: &capi.CertificateSigningRequest{ObjectMeta: validUpdateMeta, Spec: validSpec, Status: capi.CertificateSigningRequestStatus{Certificate: invalidCertificateNoPEM}},
		oldCSR: &capi.CertificateSigningRequest{ObjectMeta: validUpdateMetaWithFinalizers, Spec: validSpec, Status: capi.CertificateSigningRequestStatus{Certificate: invalidCertificateNoPEM}},
	}, {
		name: "add Approved condition",
		newCSR: &capi.CertificateSigningRequest{ObjectMeta: validUpdateMeta, Spec: validSpec, Status: capi.CertificateSigningRequestStatus{
			Conditions: []capi.CertificateSigningRequestCondition{{Type: capi.CertificateApproved, Status: core.ConditionTrue}},
		}},
		oldCSR: &capi.CertificateSigningRequest{ObjectMeta: validUpdateMetaWithFinalizers, Spec: validSpec},
		errs: []string{
			`status.conditions: Forbidden: updates may not add a condition of type "Approved"`,
		},
	}, {
		name:   "remove Approved condition",
		newCSR: &capi.CertificateSigningRequest{ObjectMeta: validUpdateMeta, Spec: validSpec},
		oldCSR: &capi.CertificateSigningRequest{ObjectMeta: validUpdateMetaWithFinalizers, Spec: validSpec, Status: capi.CertificateSigningRequestStatus{
			Conditions: []capi.CertificateSigningRequestCondition{{Type: capi.CertificateApproved, Status: core.ConditionTrue}},
		}},
		errs: []string{
			`status.conditions: Forbidden: updates may not remove a condition of type "Approved"`,
		},
	}, {
		name: "add Denied condition",
		newCSR: &capi.CertificateSigningRequest{ObjectMeta: validUpdateMeta, Spec: validSpec, Status: capi.CertificateSigningRequestStatus{
			Conditions: []capi.CertificateSigningRequestCondition{{Type: capi.CertificateDenied, Status: core.ConditionTrue}},
		}},
		oldCSR: &capi.CertificateSigningRequest{ObjectMeta: validUpdateMetaWithFinalizers, Spec: validSpec},
		errs: []string{
			`status.conditions: Forbidden: updates may not add a condition of type "Denied"`,
		},
	}, {
		name:   "remove Denied condition",
		newCSR: &capi.CertificateSigningRequest{ObjectMeta: validUpdateMeta, Spec: validSpec},
		oldCSR: &capi.CertificateSigningRequest{ObjectMeta: validUpdateMetaWithFinalizers, Spec: validSpec, Status: capi.CertificateSigningRequestStatus{
			Conditions: []capi.CertificateSigningRequestCondition{{Type: capi.CertificateDenied, Status: core.ConditionTrue}},
		}},
		errs: []string{
			`status.conditions: Forbidden: updates may not remove a condition of type "Denied"`,
		},
	}, {
		name: "add Failed condition",
		newCSR: &capi.CertificateSigningRequest{ObjectMeta: validUpdateMeta, Spec: validSpec, Status: capi.CertificateSigningRequestStatus{
			Conditions: []capi.CertificateSigningRequestCondition{{Type: capi.CertificateFailed, Status: core.ConditionTrue}},
		}},
		oldCSR: &capi.CertificateSigningRequest{ObjectMeta: validUpdateMetaWithFinalizers, Spec: validSpec},
		errs:   []string{},
	}, {
		name:   "remove Failed condition",
		newCSR: &capi.CertificateSigningRequest{ObjectMeta: validUpdateMeta, Spec: validSpec},
		oldCSR: &capi.CertificateSigningRequest{ObjectMeta: validUpdateMetaWithFinalizers, Spec: validSpec, Status: capi.CertificateSigningRequestStatus{
			Conditions: []capi.CertificateSigningRequestCondition{{Type: capi.CertificateFailed, Status: core.ConditionTrue}},
		}},
		errs: []string{
			`status.conditions: Forbidden: updates may not remove a condition of type "Failed"`,
		},
	}, {
		name: "set certificate",
		newCSR: &capi.CertificateSigningRequest{ObjectMeta: validUpdateMeta, Spec: validSpec, Status: capi.CertificateSigningRequestStatus{
			Certificate: validCertificate,
		}},
		oldCSR: &capi.CertificateSigningRequest{ObjectMeta: validUpdateMetaWithFinalizers, Spec: validSpec},
		errs: []string{
			`status.certificate: Forbidden: updates may not set certificate content`,
		},
	}, {
		name: "add both approved and denied conditions",
		newCSR: &capi.CertificateSigningRequest{
			ObjectMeta: validUpdateMeta,
			Spec:       validSpec,
			Status: capi.CertificateSigningRequestStatus{
				Conditions: []capi.CertificateSigningRequestCondition{
					{Type: capi.CertificateApproved, Status: core.ConditionTrue},
					{Type: capi.CertificateDenied, Status: core.ConditionTrue},
				},
			},
		},
		oldCSR: &capi.CertificateSigningRequest{
			ObjectMeta: validUpdateMetaWithFinalizers,
			Spec:       validSpec,
		},
		errs: []string{
			`status.conditions: Forbidden: updates may not add a condition of type "Approved"`,
			`status.conditions: Forbidden: updates may not add a condition of type "Denied"`,
			`status.conditions: Invalid value: "Denied": Approved and Denied conditions are mutually exclusive`,
		},
	}}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotErrs := sets.NewString()
			for _, err := range ValidateCertificateSigningRequestUpdate(tt.newCSR, tt.oldCSR) {
				gotErrs.Insert(err.Error())
			}
			wantErrs := sets.NewString(tt.errs...)
			for _, missing := range wantErrs.Difference(gotErrs).List() {
				t.Errorf("missing expected error: %s", missing)
			}
			for _, unexpected := range gotErrs.Difference(wantErrs).List() {
				t.Errorf("unexpected error: %s", unexpected)
			}
		})
	}
}

// TestValidateCertificateSigningRequestStatusUpdate 验证 status 子资源更新的校验：
// 审批条件与证书内容不可篡改，但允许一次性写入合法证书；
// 还覆盖了对存量脏数据（无效证书、未知/重复 usages）做无关修改时不被误伤，
// 以及互斥条件、重复 usages 等错误必须报出。
func TestValidateCertificateSigningRequestStatusUpdate(t *testing.T) {
	validUpdateMeta := validObjectMeta
	validUpdateMeta.ResourceVersion = "1"

	validUpdateMetaWithFinalizers := validUpdateMeta
	validUpdateMetaWithFinalizers.Finalizers = []string{"foo"}

	validSpec := capi.CertificateSigningRequestSpec{
		Usages:     validUsages,
		Request:    newCSRPEM(t),
		SignerName: "example.com/something",
	}

	tests := []struct {
		name   string
		newCSR *capi.CertificateSigningRequest
		oldCSR *capi.CertificateSigningRequest
		errs   []string
	}{{
		name:   "no-op",
		newCSR: &capi.CertificateSigningRequest{ObjectMeta: validUpdateMeta, Spec: validSpec},
		oldCSR: &capi.CertificateSigningRequest{ObjectMeta: validUpdateMeta, Spec: validSpec},
	}, {
		name:   "finalizer change with invalid status",
		newCSR: &capi.CertificateSigningRequest{ObjectMeta: validUpdateMeta, Spec: validSpec, Status: capi.CertificateSigningRequestStatus{Certificate: invalidCertificateNoPEM}},
		oldCSR: &capi.CertificateSigningRequest{ObjectMeta: validUpdateMetaWithFinalizers, Spec: validSpec, Status: capi.CertificateSigningRequestStatus{Certificate: invalidCertificateNoPEM}},
	}, {
		name: "finalizer change with duplicate and unknown usages",
		newCSR: &capi.CertificateSigningRequest{ObjectMeta: validUpdateMeta, Spec: capi.CertificateSigningRequestSpec{
			Usages:     []capi.KeyUsage{"unknown", "unknown"},
			Request:    newCSRPEM(t),
			SignerName: validSignerName,
		}},
		oldCSR: &capi.CertificateSigningRequest{ObjectMeta: validUpdateMetaWithFinalizers, Spec: capi.CertificateSigningRequestSpec{
			Usages:     []capi.KeyUsage{"unknown", "unknown"},
			Request:    newCSRPEM(t),
			SignerName: validSignerName,
		}},
	}, {
		name: "add Approved condition",
		newCSR: &capi.CertificateSigningRequest{ObjectMeta: validUpdateMeta, Spec: validSpec, Status: capi.CertificateSigningRequestStatus{
			Conditions: []capi.CertificateSigningRequestCondition{{Type: capi.CertificateApproved, Status: core.ConditionTrue}},
		}},
		oldCSR: &capi.CertificateSigningRequest{ObjectMeta: validUpdateMetaWithFinalizers, Spec: validSpec},
		errs: []string{
			`status.conditions: Forbidden: updates may not add a condition of type "Approved"`,
		},
	}, {
		name:   "remove Approved condition",
		newCSR: &capi.CertificateSigningRequest{ObjectMeta: validUpdateMeta, Spec: validSpec},
		oldCSR: &capi.CertificateSigningRequest{ObjectMeta: validUpdateMetaWithFinalizers, Spec: validSpec, Status: capi.CertificateSigningRequestStatus{
			Conditions: []capi.CertificateSigningRequestCondition{{Type: capi.CertificateApproved, Status: core.ConditionTrue}},
		}},
		errs: []string{
			`status.conditions: Forbidden: updates may not remove a condition of type "Approved"`,
		},
	}, {
		name: "add Denied condition",
		newCSR: &capi.CertificateSigningRequest{ObjectMeta: validUpdateMeta, Spec: validSpec, Status: capi.CertificateSigningRequestStatus{
			Conditions: []capi.CertificateSigningRequestCondition{{Type: capi.CertificateDenied, Status: core.ConditionTrue}},
		}},
		oldCSR: &capi.CertificateSigningRequest{ObjectMeta: validUpdateMetaWithFinalizers, Spec: validSpec},
		errs: []string{
			`status.conditions: Forbidden: updates may not add a condition of type "Denied"`,
		},
	}, {
		name:   "remove Denied condition",
		newCSR: &capi.CertificateSigningRequest{ObjectMeta: validUpdateMeta, Spec: validSpec},
		oldCSR: &capi.CertificateSigningRequest{ObjectMeta: validUpdateMetaWithFinalizers, Spec: validSpec, Status: capi.CertificateSigningRequestStatus{
			Conditions: []capi.CertificateSigningRequestCondition{{Type: capi.CertificateDenied, Status: core.ConditionTrue}},
		}},
		errs: []string{
			`status.conditions: Forbidden: updates may not remove a condition of type "Denied"`,
		},
	}, {
		name: "add Failed condition",
		newCSR: &capi.CertificateSigningRequest{ObjectMeta: validUpdateMeta, Spec: validSpec, Status: capi.CertificateSigningRequestStatus{
			Conditions: []capi.CertificateSigningRequestCondition{{Type: capi.CertificateFailed, Status: core.ConditionTrue}},
		}},
		oldCSR: &capi.CertificateSigningRequest{ObjectMeta: validUpdateMetaWithFinalizers, Spec: validSpec},
		errs:   []string{},
	}, {
		name:   "remove Failed condition",
		newCSR: &capi.CertificateSigningRequest{ObjectMeta: validUpdateMeta, Spec: validSpec},
		oldCSR: &capi.CertificateSigningRequest{ObjectMeta: validUpdateMetaWithFinalizers, Spec: validSpec, Status: capi.CertificateSigningRequestStatus{
			Conditions: []capi.CertificateSigningRequestCondition{{Type: capi.CertificateFailed, Status: core.ConditionTrue}},
		}},
		errs: []string{
			`status.conditions: Forbidden: updates may not remove a condition of type "Failed"`,
		},
	}, {
		name: "set valid certificate",
		newCSR: &capi.CertificateSigningRequest{ObjectMeta: validUpdateMeta, Spec: validSpec, Status: capi.CertificateSigningRequestStatus{
			Certificate: validCertificate,
		}},
		oldCSR: &capi.CertificateSigningRequest{ObjectMeta: validUpdateMetaWithFinalizers, Spec: validSpec},
		errs:   []string{},
	}, {
		name: "set invalid certificate",
		newCSR: &capi.CertificateSigningRequest{ObjectMeta: validUpdateMeta, Spec: validSpec, Status: capi.CertificateSigningRequestStatus{
			Certificate: invalidCertificateNoPEM,
		}},
		oldCSR: &capi.CertificateSigningRequest{ObjectMeta: validUpdateMetaWithFinalizers, Spec: validSpec},
		errs: []string{
			`status.certificate: Invalid value: "<certificate data>": must contain at least one CERTIFICATE PEM block`,
		},
	}, {
		name: "reset certificate",
		newCSR: &capi.CertificateSigningRequest{ObjectMeta: validUpdateMeta, Spec: validSpec, Status: capi.CertificateSigningRequestStatus{
			Certificate: invalidCertificateNonCertificatePEM,
		}},
		oldCSR: &capi.CertificateSigningRequest{ObjectMeta: validUpdateMetaWithFinalizers, Spec: validSpec, Status: capi.CertificateSigningRequestStatus{
			Certificate: invalidCertificateNoPEM,
		}},
		errs: []string{
			`status.certificate: Forbidden: updates may not modify existing certificate content`,
		},
	}}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotErrs := sets.NewString()
			for _, err := range ValidateCertificateSigningRequestStatusUpdate(tt.newCSR, tt.oldCSR) {
				gotErrs.Insert(err.Error())
			}
			wantErrs := sets.NewString(tt.errs...)
			for _, missing := range wantErrs.Difference(gotErrs).List() {
				t.Errorf("missing expected error: %s", missing)
			}
			for _, unexpected := range gotErrs.Difference(wantErrs).List() {
				t.Errorf("unexpected error: %s", unexpected)
			}
		})
	}
}

// TestValidateCertificateSigningRequestApprovalUpdate 验证 approval 子资源更新的特殊权限：
// 只有此路径允许添加 Approved/Denied 条件，但删除这些条件、设置证书仍然禁止，
// 以此把“审批”与“签发”“普通修改”三条权限路径严格隔开。
func TestValidateCertificateSigningRequestApprovalUpdate(t *testing.T) {
	validUpdateMeta := validObjectMeta
	validUpdateMeta.ResourceVersion = "1"

	validUpdateMetaWithFinalizers := validUpdateMeta
	validUpdateMetaWithFinalizers.Finalizers = []string{"foo"}

	validSpec := capi.CertificateSigningRequestSpec{
		Usages:     validUsages,
		Request:    newCSRPEM(t),
		SignerName: "example.com/something",
	}

	tests := []struct {
		name   string
		newCSR *capi.CertificateSigningRequest
		oldCSR *capi.CertificateSigningRequest
		errs   []string
	}{{
		name:   "no-op",
		newCSR: &capi.CertificateSigningRequest{ObjectMeta: validUpdateMeta, Spec: validSpec},
		oldCSR: &capi.CertificateSigningRequest{ObjectMeta: validUpdateMeta, Spec: validSpec},
	}, {
		name:   "finalizer change with invalid certificate",
		newCSR: &capi.CertificateSigningRequest{ObjectMeta: validUpdateMeta, Spec: validSpec, Status: capi.CertificateSigningRequestStatus{Certificate: invalidCertificateNoPEM}},
		oldCSR: &capi.CertificateSigningRequest{ObjectMeta: validUpdateMetaWithFinalizers, Spec: validSpec, Status: capi.CertificateSigningRequestStatus{Certificate: invalidCertificateNoPEM}},
	}, {
		// approval 子资源的核心能力：只有这里可以添加审批条件，且不应报错。
		name: "add Approved condition",
		newCSR: &capi.CertificateSigningRequest{ObjectMeta: validUpdateMeta, Spec: validSpec, Status: capi.CertificateSigningRequestStatus{
			Conditions: []capi.CertificateSigningRequestCondition{{Type: capi.CertificateApproved, Status: core.ConditionTrue}},
		}},
		oldCSR: &capi.CertificateSigningRequest{ObjectMeta: validUpdateMetaWithFinalizers, Spec: validSpec},
	}, {
		name:   "remove Approved condition",
		newCSR: &capi.CertificateSigningRequest{ObjectMeta: validUpdateMeta, Spec: validSpec},
		oldCSR: &capi.CertificateSigningRequest{ObjectMeta: validUpdateMetaWithFinalizers, Spec: validSpec, Status: capi.CertificateSigningRequestStatus{
			Conditions: []capi.CertificateSigningRequestCondition{{Type: capi.CertificateApproved, Status: core.ConditionTrue}},
		}},
		errs: []string{
			`status.conditions: Forbidden: updates may not remove a condition of type "Approved"`,
		},
	}, {
		name: "add Denied condition",
		newCSR: &capi.CertificateSigningRequest{ObjectMeta: validUpdateMeta, Spec: validSpec, Status: capi.CertificateSigningRequestStatus{
			Conditions: []capi.CertificateSigningRequestCondition{{Type: capi.CertificateDenied, Status: core.ConditionTrue}},
		}},
		oldCSR: &capi.CertificateSigningRequest{ObjectMeta: validUpdateMetaWithFinalizers, Spec: validSpec},
	}, {
		name:   "remove Denied condition",
		newCSR: &capi.CertificateSigningRequest{ObjectMeta: validUpdateMeta, Spec: validSpec},
		oldCSR: &capi.CertificateSigningRequest{ObjectMeta: validUpdateMetaWithFinalizers, Spec: validSpec, Status: capi.CertificateSigningRequestStatus{
			Conditions: []capi.CertificateSigningRequestCondition{{Type: capi.CertificateDenied, Status: core.ConditionTrue}},
		}},
		errs: []string{
			`status.conditions: Forbidden: updates may not remove a condition of type "Denied"`,
		},
	}, {
		name: "add Failed condition",
		newCSR: &capi.CertificateSigningRequest{ObjectMeta: validUpdateMeta, Spec: validSpec, Status: capi.CertificateSigningRequestStatus{
			Conditions: []capi.CertificateSigningRequestCondition{{Type: capi.CertificateFailed, Status: core.ConditionTrue}},
		}},
		oldCSR: &capi.CertificateSigningRequest{ObjectMeta: validUpdateMetaWithFinalizers, Spec: validSpec},
		errs:   []string{},
	}, {
		name:   "remove Failed condition",
		newCSR: &capi.CertificateSigningRequest{ObjectMeta: validUpdateMeta, Spec: validSpec},
		oldCSR: &capi.CertificateSigningRequest{ObjectMeta: validUpdateMetaWithFinalizers, Spec: validSpec, Status: capi.CertificateSigningRequestStatus{
			Conditions: []capi.CertificateSigningRequestCondition{{Type: capi.CertificateFailed, Status: core.ConditionTrue}},
		}},
		errs: []string{
			`status.conditions: Forbidden: updates may not remove a condition of type "Failed"`,
		},
	}, {
		name: "set certificate",
		newCSR: &capi.CertificateSigningRequest{ObjectMeta: validUpdateMeta, Spec: validSpec, Status: capi.CertificateSigningRequestStatus{
			Certificate: validCertificate,
		}},
		oldCSR: &capi.CertificateSigningRequest{ObjectMeta: validUpdateMetaWithFinalizers, Spec: validSpec},
		errs: []string{
			`status.certificate: Forbidden: updates may not set certificate content`,
		},
	}}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotErrs := sets.NewString()
			for _, err := range ValidateCertificateSigningRequestApprovalUpdate(tt.newCSR, tt.oldCSR) {
				gotErrs.Insert(err.Error())
			}
			wantErrs := sets.NewString(tt.errs...)
			for _, missing := range wantErrs.Difference(gotErrs).List() {
				t.Errorf("missing expected error: %s", missing)
			}
			for _, unexpected := range gotErrs.Difference(wantErrs).List() {
				t.Errorf("unexpected error: %s", unexpected)
			}
		})
	}
}

// Test_validateCertificateSigningRequestOptions 验证各宽容选项确实只放过对应的特殊点：每个用例先用宽松选项校验必须零错误，再用严格选项校验必须出现预期错误。
func Test_validateCertificateSigningRequestOptions(t *testing.T) {
	validSpec := capi.CertificateSigningRequestSpec{
		Usages:     validUsages,
		Request:    newCSRPEM(t),
		SignerName: "example.com/something",
	}

	tests := []struct {
		// 用例名称。
		name string

		// 待校验的 CSR。
		csr *capi.CertificateSigningRequest

		// 能让该 CSR 通过校验的宽容选项。
		lenientOpts certificateValidationOptions

		// 严格校验时预期错误的正则匹配（用于错误信息含随机值的场景）。
		strictRegexes []regexp.Regexp

		// 严格校验时预期出现的错误（正则匹配项过滤后剩余的部分）。
		strictErrs []string
	}{
		// 严格校验下本身就合法的用例：无需任何宽容选项。
		{
			name: "no status",
			csr:  &capi.CertificateSigningRequest{ObjectMeta: validObjectMeta, Spec: validSpec},
		}, {
			name: "approved condition",
			csr: &capi.CertificateSigningRequest{ObjectMeta: validObjectMeta, Spec: validSpec,
				Status: capi.CertificateSigningRequestStatus{
					Conditions: []capi.CertificateSigningRequestCondition{{Type: capi.CertificateApproved, Status: core.ConditionTrue}},
				},
			},
		}, {
			name: "denied condition",
			csr: &capi.CertificateSigningRequest{ObjectMeta: validObjectMeta, Spec: validSpec,
				Status: capi.CertificateSigningRequestStatus{
					Conditions: []capi.CertificateSigningRequestCondition{{Type: capi.CertificateDenied, Status: core.ConditionTrue}},
				},
			},
		}, {
			name: "failed condition",
			csr: &capi.CertificateSigningRequest{ObjectMeta: validObjectMeta, Spec: validSpec,
				Status: capi.CertificateSigningRequestStatus{
					Conditions: []capi.CertificateSigningRequestCondition{{Type: capi.CertificateFailed, Status: core.ConditionTrue}},
				},
			},
		}, {
			name: "approved+issued",
			csr: &capi.CertificateSigningRequest{ObjectMeta: validObjectMeta, Spec: validSpec,
				Status: capi.CertificateSigningRequestStatus{
					Conditions:  []capi.CertificateSigningRequestCondition{{Type: capi.CertificateApproved, Status: core.ConditionTrue}},
					Certificate: validCertificate,
				},
			},
		},

		// 旧版 signerName：严格模式必须拒绝，宽松选项放行。
		{
			name: "legacy signer",
			csr: &capi.CertificateSigningRequest{
				ObjectMeta: validObjectMeta,
				Spec: func() capi.CertificateSigningRequestSpec {
					specCopy := validSpec
					specCopy.SignerName = capi.LegacyUnknownSignerName
					return specCopy
				}(),
			},
			lenientOpts: certificateValidationOptions{allowLegacySignerName: true},
			strictErrs:  []string{`spec.signerName: Invalid value: "kubernetes.io/legacy-unknown": the legacy signerName is not allowed via this API version`},
		},

		// 各类非法条件组合：空类型、互斥、重复类型。
		{
			name: "empty condition type",
			csr: &capi.CertificateSigningRequest{ObjectMeta: validObjectMeta, Spec: validSpec,
				Status: capi.CertificateSigningRequestStatus{
					Conditions: []capi.CertificateSigningRequestCondition{{Status: core.ConditionTrue}},
				},
			},
			lenientOpts: certificateValidationOptions{allowEmptyConditionType: true},
			strictErrs:  []string{`status.conditions[0].type: Required value`},
		}, {
			name: "approved and denied",
			csr: &capi.CertificateSigningRequest{ObjectMeta: validObjectMeta, Spec: validSpec,
				Status: capi.CertificateSigningRequestStatus{
					Conditions: []capi.CertificateSigningRequestCondition{{Type: capi.CertificateApproved, Status: core.ConditionTrue}, {Type: capi.CertificateDenied, Status: core.ConditionTrue}},
				},
			},
			lenientOpts: certificateValidationOptions{allowBothApprovedAndDenied: true},
			strictErrs:  []string{`status.conditions: Invalid value: "Denied": Approved and Denied conditions are mutually exclusive`},
		}, {
			name: "duplicate condition",
			csr: &capi.CertificateSigningRequest{ObjectMeta: validObjectMeta, Spec: validSpec,
				Status: capi.CertificateSigningRequestStatus{
					Conditions: []capi.CertificateSigningRequestCondition{{Type: capi.CertificateApproved, Status: core.ConditionTrue}, {Type: capi.CertificateApproved, Status: core.ConditionTrue}},
				},
			},
			lenientOpts: certificateValidationOptions{allowDuplicateConditionTypes: true},
			strictErrs:  []string{`status.conditions[1].type: Duplicate value: "Approved"`},
		},

		// 非法证书内容：宽松选项完全跳过证书解析，严格模式逐一报出具体的格式错误。
		{
			name: "status.certificate, no PEM",
			csr: &capi.CertificateSigningRequest{ObjectMeta: validObjectMeta, Spec: validSpec,
				Status: capi.CertificateSigningRequestStatus{
					Conditions:  []capi.CertificateSigningRequestCondition{{Type: capi.CertificateApproved, Status: core.ConditionTrue}},
					Certificate: invalidCertificateNoPEM,
				},
			},
			lenientOpts: certificateValidationOptions{allowArbitraryCertificate: true},
			strictErrs:  []string{`status.certificate: Invalid value: "<certificate data>": must contain at least one CERTIFICATE PEM block`},
		}, {
			name: "status.certificate, non-CERTIFICATE PEM",
			csr: &capi.CertificateSigningRequest{ObjectMeta: validObjectMeta, Spec: validSpec,
				Status: capi.CertificateSigningRequestStatus{
					Conditions:  []capi.CertificateSigningRequestCondition{{Type: capi.CertificateApproved, Status: core.ConditionTrue}},
					Certificate: invalidCertificateNonCertificatePEM,
				},
			},
			lenientOpts: certificateValidationOptions{allowArbitraryCertificate: true},
			strictErrs:  []string{`status.certificate: Invalid value: "<certificate data>": only CERTIFICATE PEM blocks are allowed, found "CERTIFICATE1"`},
		}, {
			name: "status.certificate, PEM headers",
			csr: &capi.CertificateSigningRequest{ObjectMeta: validObjectMeta, Spec: validSpec,
				Status: capi.CertificateSigningRequestStatus{
					Conditions:  []capi.CertificateSigningRequestCondition{{Type: capi.CertificateApproved, Status: core.ConditionTrue}},
					Certificate: invalidCertificatePEMHeaders,
				},
			},
			lenientOpts: certificateValidationOptions{allowArbitraryCertificate: true},
			strictErrs:  []string{`status.certificate: Invalid value: "<certificate data>": no PEM block headers are permitted`},
		}, {
			name: "status.certificate, non-base64 PEM",
			csr: &capi.CertificateSigningRequest{ObjectMeta: validObjectMeta, Spec: validSpec,
				Status: capi.CertificateSigningRequestStatus{
					Conditions:  []capi.CertificateSigningRequestCondition{{Type: capi.CertificateApproved, Status: core.ConditionTrue}},
					Certificate: invalidCertificateNonBase64PEM,
				},
			},
			lenientOpts: certificateValidationOptions{allowArbitraryCertificate: true},
			strictErrs:  []string{`status.certificate: Invalid value: "<certificate data>": must contain at least one CERTIFICATE PEM block`},
		}, {
			name: "status.certificate, empty PEM block",
			csr: &capi.CertificateSigningRequest{ObjectMeta: validObjectMeta, Spec: validSpec,
				Status: capi.CertificateSigningRequestStatus{
					Conditions:  []capi.CertificateSigningRequestCondition{{Type: capi.CertificateApproved, Status: core.ConditionTrue}},
					Certificate: invalidCertificateEmptyPEM,
				},
			},
			lenientOpts: certificateValidationOptions{allowArbitraryCertificate: true},
			strictErrs:  []string{`status.certificate: Invalid value: "<certificate data>": found CERTIFICATE PEM block containing 0 certificates`},
		}, {
			name: "status.certificate, non-ASN1 data",
			csr: &capi.CertificateSigningRequest{ObjectMeta: validObjectMeta, Spec: validSpec,
				Status: capi.CertificateSigningRequestStatus{
					Conditions:  []capi.CertificateSigningRequestCondition{{Type: capi.CertificateApproved, Status: core.ConditionTrue}},
					Certificate: invalidCertificateNonASN1Data,
				},
			},
			lenientOpts:   certificateValidationOptions{allowArbitraryCertificate: true},
			strictRegexes: []regexp.Regexp{*regexp.MustCompile(`status.certificate: Invalid value: "\<certificate data\>": (asn1: structure error: sequence tag mismatch|x509: invalid RDNSequence)`)},
		}, {
			name: "approved and denied",
			csr: &capi.CertificateSigningRequest{ObjectMeta: validObjectMeta, Spec: validSpec,
				Status: capi.CertificateSigningRequestStatus{
					Conditions: []capi.CertificateSigningRequestCondition{{Type: capi.CertificateApproved, Status: core.ConditionTrue}, {Type: capi.CertificateDenied, Status: core.ConditionTrue}},
				},
			},
			lenientOpts: certificateValidationOptions{allowBothApprovedAndDenied: true},
			strictErrs:  []string{`status.conditions: Invalid value: "Denied": Approved and Denied conditions are mutually exclusive`},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// 先用宽松选项校验：这一遍必须零错误，否则说明宽容选项没有真正生效。
			for _, err := range validateCertificateSigningRequest(tt.csr, tt.lenientOpts) {
				t.Errorf("unexpected error with lenient options: %s", err.Error())
			}

			// 再用严格选项校验，收集全部错误用于后续比对。
			gotErrs := sets.NewString()
			for _, err := range validateCertificateSigningRequest(tt.csr, certificateValidationOptions{}) {
				gotErrs.Insert(err.Error())
			}

			// 正则匹配的错误须逐条命中并移除，确保每条正则都真正匹配到了错误，剩余错误再与严格预期集合精确比对。
			for _, expectedRegex := range tt.strictRegexes {
				matched := false
				for _, err := range gotErrs.List() {
					if expectedRegex.MatchString(err) {
						gotErrs.Delete(err)
						matched = true
					}
				}
				if !matched {
					t.Errorf("missing expected error matching: %s", expectedRegex.String())
				}
			}

			wantErrs := sets.NewString(tt.strictErrs...)
			for _, missing := range wantErrs.Difference(gotErrs).List() {
				t.Errorf("missing expected strict error: %s", missing)
			}
			for _, unexpected := range gotErrs.Difference(wantErrs).List() {
				t.Errorf("unexpected errors: %s", unexpected)
			}
		})
	}
}

// mustMakeCertificate 使用固定随机种子生成自签名证书，
// 保证 ClusterTrustBundle 测试中“内容相同但 PEM 编码不同”的重复证书用例可复现。
func mustMakeCertificate(t *testing.T, template *x509.Certificate) []byte {
	gen := mathrand.New(mathrand.NewSource(12345))

	pub, priv, err := ed25519.GenerateKey(gen)
	if err != nil {
		t.Fatalf("Error while generating key: %v", err)
	}

	cert, err := x509.CreateCertificate(gen, template, template, pub, priv)
	if err != nil {
		t.Fatalf("Error while making certificate: %v", err)
	}

	return cert
}

// mustMakePEMBlock 把证书 DER 包装成带指定类型与头部的 PEM 块，
// 方便构造合法与非法的 trustBundle 条目。
func mustMakePEMBlock(blockType string, headers map[string]string, data []byte) string {
	return string(pem.EncodeToMemory(&pem.Block{
		Type:    blockType,
		Headers: headers,
		Bytes:   data,
	}))
}

// TestValidateClusterTrustBundle 表驱动验证 ClusterTrustBundle 创建校验：
// 覆盖信任锚内容（必须至少一条、CA 位、X.509 可解析、去重、PEM 块类型与头部）、
// signerName 与对象名的前缀约束、bundle 大小上限；
// 同时验证 SuppressBundleParsing 开关能跳过内容解析，以及同一批用例在“无字段变更”的
// 更新路径上不会误报 trustBundle 错误。
func TestValidateClusterTrustBundle(t *testing.T) {
	goodCert1 := mustMakeCertificate(t, &x509.Certificate{
		SerialNumber: big.NewInt(0),
		Subject: pkix.Name{
			CommonName: "root1",
		},
		IsCA:                  true,
		BasicConstraintsValid: true,
	})

	goodCert2 := mustMakeCertificate(t, &x509.Certificate{
		SerialNumber: big.NewInt(0),
		Subject: pkix.Name{
			CommonName: "root2",
		},
		IsCA:                  true,
		BasicConstraintsValid: true,
	})

	badNotCACert := mustMakeCertificate(t, &x509.Certificate{
		SerialNumber: big.NewInt(0),
		Subject: pkix.Name{
			CommonName: "root3",
		},
	})

	goodCert1Block := string(mustMakePEMBlock("CERTIFICATE", nil, goodCert1))
	goodCert2Block := string(mustMakePEMBlock("CERTIFICATE", nil, goodCert2))

	// 仅改变 PEM 内部换行格式，DER 内容不变：用于验证去重按证书内容而非字节串比较。
	goodCert1AlternateBlock := strings.ReplaceAll(goodCert1Block, "\n", "\n\t\n")

	badNotCACertBlock := string(mustMakePEMBlock("CERTIFICATE", nil, badNotCACert))

	badBlockHeadersBlock := string(mustMakePEMBlock("CERTIFICATE", map[string]string{"key": "value"}, goodCert1))
	badBlockTypeBlock := string(mustMakePEMBlock("NOTACERTIFICATE", nil, goodCert1))
	badNonParseableBlock := string(mustMakePEMBlock("CERTIFICATE", nil, []byte("this is not a certificate")))

	// 构造超过 Secret 大小上限的 bundle，验证创建时按存储上限拒绝。
	badTooBigBundle := ""
	for i := 0; i < (core.MaxSecretSize/len(goodCert1Block))+1; i++ {
		badTooBigBundle += goodCert1Block + "\n"
	}

	testCases := []struct {
		description string
		bundle      *capi.ClusterTrustBundle
		opts        ValidateClusterTrustBundleOptions
		wantErrors  field.ErrorList
	}{
		{
			description: "valid, no signer name",
			bundle: &capi.ClusterTrustBundle{
				ObjectMeta: metav1.ObjectMeta{
					Name: "foo",
				},
				Spec: capi.ClusterTrustBundleSpec{
					TrustBundle: goodCert1Block,
				},
			},
		},
		{
			description: "invalid, too big",
			bundle: &capi.ClusterTrustBundle{
				ObjectMeta: metav1.ObjectMeta{
					Name: "foo",
				},
				Spec: capi.ClusterTrustBundleSpec{
					TrustBundle: badTooBigBundle,
				},
			},
			wantErrors: field.ErrorList{
				field.TooLong(field.NewPath("spec", "trustBundle"), "" /*unused*/, core.MaxSecretSize),
			},
		},
		{
			description: "invalid, no signer name, invalid name",
			bundle: &capi.ClusterTrustBundle{
				ObjectMeta: metav1.ObjectMeta{
					Name: "k8s.io:bar:foo",
				},
				Spec: capi.ClusterTrustBundleSpec{
					TrustBundle: goodCert1Block,
				},
			},
			wantErrors: field.ErrorList{
				field.Invalid(field.NewPath("metadata", "name"), "k8s.io:bar:foo", "ClusterTrustBundle without signer name must not have \":\" in its name"),
			},
		}, {
			description: "valid, with signer name",
			bundle: &capi.ClusterTrustBundle{
				ObjectMeta: metav1.ObjectMeta{
					Name: "k8s.io:foo:bar",
				},
				Spec: capi.ClusterTrustBundleSpec{
					SignerName:  "k8s.io/foo",
					TrustBundle: goodCert1Block,
				},
			},
		}, {
			description: "invalid, with signer name, missing name prefix",
			bundle: &capi.ClusterTrustBundle{
				ObjectMeta: metav1.ObjectMeta{
					Name: "look-ma-no-prefix",
				},
				Spec: capi.ClusterTrustBundleSpec{
					SignerName:  "k8s.io/foo",
					TrustBundle: goodCert1Block,
				},
			},
			wantErrors: field.ErrorList{
				field.Invalid(field.NewPath("metadata", "name"), "look-ma-no-prefix", "ClusterTrustBundle for signerName k8s.io/foo must be named with prefix k8s.io:foo:"),
			},
		}, {
			description: "invalid, with signer name, empty name suffix",
			bundle: &capi.ClusterTrustBundle{
				ObjectMeta: metav1.ObjectMeta{
					Name: "k8s.io:foo:",
				},
				Spec: capi.ClusterTrustBundleSpec{
					SignerName:  "k8s.io/foo",
					TrustBundle: goodCert1Block,
				},
			},
			wantErrors: field.ErrorList{
				field.Invalid(field.NewPath("metadata", "name"), "k8s.io:foo:", `a lowercase RFC 1123 subdomain must consist of lower case alphanumeric characters, '-' or '.', and must start and end with an alphanumeric character (e.g. 'example.com', regex used for validation is '[a-z0-9]([-a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*')`),
			},
		}, {
			description: "invalid, with signer name, bad name suffix",
			bundle: &capi.ClusterTrustBundle{
				ObjectMeta: metav1.ObjectMeta{
					Name: "k8s.io:foo:123notvalidDNSSubdomain",
				},
				Spec: capi.ClusterTrustBundleSpec{
					SignerName:  "k8s.io/foo",
					TrustBundle: goodCert1Block,
				},
			},
			wantErrors: field.ErrorList{
				field.Invalid(field.NewPath("metadata", "name"), "k8s.io:foo:123notvalidDNSSubdomain", `a lowercase RFC 1123 subdomain must consist of lower case alphanumeric characters, '-' or '.', and must start and end with an alphanumeric character (e.g. 'example.com', regex used for validation is '[a-z0-9]([-a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*')`),
			},
		}, {
			description: "valid, with signer name, with inter-block garbage",
			bundle: &capi.ClusterTrustBundle{
				ObjectMeta: metav1.ObjectMeta{
					Name: "k8s.io:foo:abc",
				},
				Spec: capi.ClusterTrustBundleSpec{
					SignerName:  "k8s.io/foo",
					TrustBundle: "garbage\n" + goodCert1Block + "\ngarbage\n" + goodCert2Block,
				},
			},
		}, {
			description: "invalid, no signer name, no trust anchors",
			bundle: &capi.ClusterTrustBundle{
				ObjectMeta: metav1.ObjectMeta{
					Name: "foo",
				},
				Spec: capi.ClusterTrustBundleSpec{},
			},
			wantErrors: field.ErrorList{
				field.Invalid(field.NewPath("spec", "trustBundle"), "<value omitted>", "at least one trust anchor must be provided"),
			},
		}, {
			description: "invalid, no trust anchors",
			bundle: &capi.ClusterTrustBundle{
				ObjectMeta: metav1.ObjectMeta{
					Name: "k8s.io:foo:abc",
				},
				Spec: capi.ClusterTrustBundleSpec{
					SignerName: "k8s.io/foo",
				},
			},
			wantErrors: field.ErrorList{
				field.Invalid(field.NewPath("spec", "trustBundle"), "<value omitted>", "at least one trust anchor must be provided"),
			},
		}, {
			description: "invalid, bad signer name",
			bundle: &capi.ClusterTrustBundle{
				ObjectMeta: metav1.ObjectMeta{
					Name: "invalid:foo",
				},
				Spec: capi.ClusterTrustBundleSpec{
					SignerName:  "invalid",
					TrustBundle: goodCert1Block,
				},
			},
			wantErrors: field.ErrorList{
				field.Invalid(field.NewPath("spec", "signerName"), "invalid", "must be a fully qualified domain and path of the form 'example.com/signer-name'"),
			},
		}, {
			description: "invalid, no blocks",
			bundle: &capi.ClusterTrustBundle{
				ObjectMeta: metav1.ObjectMeta{
					Name: "foo",
				},
				Spec: capi.ClusterTrustBundleSpec{
					TrustBundle: "non block garbage",
				},
			},
			wantErrors: field.ErrorList{
				field.Invalid(field.NewPath("spec", "trustBundle"), "<value omitted>", "at least one trust anchor must be provided"),
			},
		}, {
			description: "invalid, bad block type",
			bundle: &capi.ClusterTrustBundle{
				ObjectMeta: metav1.ObjectMeta{
					Name: "foo",
				},
				Spec: capi.ClusterTrustBundleSpec{
					TrustBundle: goodCert1Block + "\n" + badBlockTypeBlock,
				},
			},
			wantErrors: field.ErrorList{
				field.Invalid(field.NewPath("spec", "trustBundle"), "<value omitted>", "entry 1 has bad block type: NOTACERTIFICATE"),
			},
		}, {
			description: "invalid, block with headers",
			bundle: &capi.ClusterTrustBundle{
				ObjectMeta: metav1.ObjectMeta{
					Name: "foo",
				},
				Spec: capi.ClusterTrustBundleSpec{
					TrustBundle: goodCert1Block + "\n" + badBlockHeadersBlock,
				},
			},
			wantErrors: field.ErrorList{
				field.Invalid(field.NewPath("spec", "trustBundle"), "<value omitted>", "entry 1 has PEM block headers"),
			},
		}, {
			description: "invalid, cert is not a CA cert",
			bundle: &capi.ClusterTrustBundle{
				ObjectMeta: metav1.ObjectMeta{
					Name: "foo",
				},
				Spec: capi.ClusterTrustBundleSpec{
					TrustBundle: badNotCACertBlock,
				},
			},
			wantErrors: field.ErrorList{
				field.Invalid(field.NewPath("spec", "trustBundle"), "<value omitted>", "entry 0 does not have the CA bit set"),
			},
		}, {
			description: "invalid, duplicated blocks",
			bundle: &capi.ClusterTrustBundle{
				ObjectMeta: metav1.ObjectMeta{
					Name: "foo",
				},
				Spec: capi.ClusterTrustBundleSpec{
					TrustBundle: goodCert1Block + "\n" + goodCert1AlternateBlock,
				},
			},
			wantErrors: field.ErrorList{
				field.Invalid(field.NewPath("spec", "trustBundle"), "<value omitted>", "duplicate trust anchor (indices [0 1])"),
			},
		}, {
			description: "invalid, non-certificate entry",
			bundle: &capi.ClusterTrustBundle{
				ObjectMeta: metav1.ObjectMeta{
					Name: "foo",
				},
				Spec: capi.ClusterTrustBundleSpec{
					TrustBundle: goodCert1Block + "\n" + badNonParseableBlock,
				},
			},
			wantErrors: field.ErrorList{
				field.Invalid(field.NewPath("spec", "trustBundle"), "<value omitted>", "entry 1 does not parse as X.509"),
			},
		}, {
			description: "allow any old garbage in the PEM field if we suppress parsing",
			bundle: &capi.ClusterTrustBundle{
				ObjectMeta: metav1.ObjectMeta{
					Name: "foo",
				},
				Spec: capi.ClusterTrustBundleSpec{
					TrustBundle: "garbage",
				},
			},
			opts: ValidateClusterTrustBundleOptions{
				SuppressBundleParsing: true,
			},
		}}
	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			gotErrors := ValidateClusterTrustBundle(tc.bundle, tc.opts)
			if diff := cmp.Diff(gotErrors, tc.wantErrors); diff != "" {
				t.Fatalf("Unexpected error output from Validate; diff (-got +want)\n%s", diff)
			}

			// 对象内容不变时，更新路径不应再报 trustBundle 相关错误，
			// 对象内容未发生任何变化时，
			// ValidateClusterTrustBundleUpdate 不应报告关于
			// TrustBundle 字段的错误。
			tc.bundle.ObjectMeta.ResourceVersion = "1"
			newBundle := tc.bundle.DeepCopy()
			newBundle.ObjectMeta.ResourceVersion = "2"
			gotErrors = ValidateClusterTrustBundleUpdate(newBundle, tc.bundle)

			var filteredWantErrors field.ErrorList
			for _, err := range tc.wantErrors {
				if err.Field != "spec.trustBundle" {
					filteredWantErrors = append(filteredWantErrors, err)
				}
			}

			if diff := cmp.Diff(gotErrors, filteredWantErrors); diff != "" {
				t.Fatalf("Unexpected error output from ValidateUpdate; diff (-got +want)\n%s", diff)
			}
		})
	}
}

// TestValidateClusterTrustBundleUpdate 验证 bundle 更新时 signerName 不可变、
// 对象名必须与 signerName 前缀一致；追加证书允许，清空 bundle 或替换成垃圾内容都被拒绝。
func TestValidateClusterTrustBundleUpdate(t *testing.T) {
	goodCert1 := mustMakeCertificate(t, &x509.Certificate{
		SerialNumber: big.NewInt(0),
		Subject: pkix.Name{
			CommonName: "root1",
		},
		IsCA:                  true,
		BasicConstraintsValid: true,
	})

	goodCert2 := mustMakeCertificate(t, &x509.Certificate{
		SerialNumber: big.NewInt(0),
		Subject: pkix.Name{
			CommonName: "root2",
		},
		IsCA:                  true,
		BasicConstraintsValid: true,
	})

	goodCert1Block := string(mustMakePEMBlock("CERTIFICATE", nil, goodCert1))
	goodCert2Block := string(mustMakePEMBlock("CERTIFICATE", nil, goodCert2))

	testCases := []struct {
		description          string
		oldBundle, newBundle *capi.ClusterTrustBundle
		wantErrors           field.ErrorList
	}{{
		description: "changing signer name disallowed",
		oldBundle: &capi.ClusterTrustBundle{
			ObjectMeta: metav1.ObjectMeta{
				Name: "k8s.io:foo:bar",
			},
			Spec: capi.ClusterTrustBundleSpec{
				SignerName:  "k8s.io/foo",
				TrustBundle: goodCert1Block,
			},
		},
		newBundle: &capi.ClusterTrustBundle{
			ObjectMeta: metav1.ObjectMeta{
				Name: "k8s.io:foo:bar",
			},
			Spec: capi.ClusterTrustBundleSpec{
				SignerName:  "k8s.io/bar",
				TrustBundle: goodCert1Block,
			},
		},
		wantErrors: field.ErrorList{
			field.Invalid(field.NewPath("metadata", "name"), "k8s.io:foo:bar", "ClusterTrustBundle for signerName k8s.io/bar must be named with prefix k8s.io:bar:"),
			field.Invalid(field.NewPath("spec", "signerName"), "k8s.io/bar", "field is immutable").WithOrigin("immutable").MarkAlpha().MarkCoveredByDeclarative(),
		},
	}, {
		description: "adding certificate allowed",
		oldBundle: &capi.ClusterTrustBundle{
			ObjectMeta: metav1.ObjectMeta{
				Name: "k8s.io:foo:bar",
			},
			Spec: capi.ClusterTrustBundleSpec{
				SignerName:  "k8s.io/foo",
				TrustBundle: goodCert1Block,
			},
		},
		newBundle: &capi.ClusterTrustBundle{
			ObjectMeta: metav1.ObjectMeta{
				Name: "k8s.io:foo:bar",
			},
			Spec: capi.ClusterTrustBundleSpec{
				SignerName:  "k8s.io/foo",
				TrustBundle: goodCert1Block + "\n" + goodCert2Block,
			},
		},
	}, {
		description: "emptying trustBundle disallowed",
		oldBundle: &capi.ClusterTrustBundle{
			ObjectMeta: metav1.ObjectMeta{
				Name: "k8s.io:foo:bar",
			},
			Spec: capi.ClusterTrustBundleSpec{
				SignerName:  "k8s.io/foo",
				TrustBundle: goodCert1Block,
			},
		},
		newBundle: &capi.ClusterTrustBundle{
			ObjectMeta: metav1.ObjectMeta{
				Name: "k8s.io:foo:bar",
			},
			Spec: capi.ClusterTrustBundleSpec{
				SignerName:  "k8s.io/foo",
				TrustBundle: "",
			},
		},
		wantErrors: field.ErrorList{
			field.Invalid(field.NewPath("spec", "trustBundle"), "<value omitted>", "at least one trust anchor must be provided"),
		},
	}, {
		description: "emptying trustBundle (replace with non-block garbage) disallowed",
		oldBundle: &capi.ClusterTrustBundle{
			ObjectMeta: metav1.ObjectMeta{
				Name: "k8s.io:foo:bar",
			},
			Spec: capi.ClusterTrustBundleSpec{
				SignerName:  "k8s.io/foo",
				TrustBundle: goodCert1Block,
			},
		},
		newBundle: &capi.ClusterTrustBundle{
			ObjectMeta: metav1.ObjectMeta{
				Name: "k8s.io:foo:bar",
			},
			Spec: capi.ClusterTrustBundleSpec{
				SignerName:  "k8s.io/foo",
				TrustBundle: "non block garbage",
			},
		},
		wantErrors: field.ErrorList{
			field.Invalid(field.NewPath("spec", "trustBundle"), "<value omitted>", "at least one trust anchor must be provided"),
		},
	}}

	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			tc.oldBundle.ObjectMeta.ResourceVersion = "1"
			tc.newBundle.ObjectMeta.ResourceVersion = "2"
			gotErrors := ValidateClusterTrustBundleUpdate(tc.newBundle, tc.oldBundle)
			if diff := cmp.Diff(gotErrors, tc.wantErrors); diff != "" {
				t.Errorf("Unexpected error output from ValidateUpdate; diff (-got +want)\n%s", diff)
			}
		})
	}
}

// TestValidatePodCertificateRequestCreate 验证 PodCertificateRequest 创建校验。
// 用例覆盖：Ed25519/ECDSA/RSA 各算法的合法与非法密钥（曲线、模长限制）、
// 持有证明签名（消息错、签名密钥错）、PKCS#10 与公钥二选一互斥、
// signerName/Pod/ServiceAccount/Node 各标识字段的格式与非空、
// maxExpirationSeconds 的范围（含 kubernetes.io 签名者的更严格上限）、
// 公钥与证明的大小上限，以及用户注解键名、前缀长度、总大小的校验。
// 所有密码学材料都在测试内现场生成，保证签名验证走的是真实路径。
func TestValidatePodCertificateRequestCreate(t *testing.T) {
	podUID1 := "pod-uid-1"
	// 持有证明签名的消息就是 Pod UID，签名错消息或错密钥都应被验签拦下。
	_, _, ed25519PubPKIX1, ed25519Proof1, ed25519CSR1 := mustMakeEd25519KeyAndProof(t, []byte(podUID1), []string{})
	// 用另一个 UID 签名：密钥本身合法，但签名消息与被申请 Pod 不匹配。
	_, _, ed25519PubPKIX2, ed25519Proof2, _ := mustMakeEd25519KeyAndProof(t, []byte("other-value"), []string{})
	// 用另一把密钥签同一消息：公钥字段来自第一把密钥，验签必然失败。
	_, _, _, ed25519Proof3, _ := mustMakeEd25519KeyAndProof(t, []byte(podUID1), []string{})
	_, _, ecdsaP224PubPKIX1, ecdsaP224Proof1 := mustMakeECDSAKeyAndProof(t, elliptic.P224(), []byte(podUID1))
	_, _, ecdsaP256PubPKIX1, ecdsaP256Proof1 := mustMakeECDSAKeyAndProof(t, elliptic.P256(), []byte(podUID1))
	_, _, ecdsaP384PubPKIX1, ecdsaP384Proof1 := mustMakeECDSAKeyAndProof(t, elliptic.P384(), []byte(podUID1))
	_, _, ecdsaP521PubPKIX1, ecdsaP521Proof1 := mustMakeECDSAKeyAndProof(t, elliptic.P521(), []byte(podUID1))
	_, _, ecdsaWrongProofPKIX, ecdsaWrongProof := mustMakeECDSAKeyAndProof(t, elliptic.P384(), []byte("other-value"))
	_, _, rsa2048PubPKIX1, rsa2048Proof1 := mustMakeRSAKeyAndProof(t, 2048, []byte(podUID1))
	_, _, rsa3072PubPKIX1, rsa3072Proof1 := mustMakeRSAKeyAndProof(t, 3072, []byte(podUID1))
	_, _, rsa4096PubPKIX1, rsa4096Proof1 := mustMakeRSAKeyAndProof(t, 4096, []byte(podUID1))
	_, _, rsaWrongProofPKIX, rsaWrongProof := mustMakeRSAKeyAndProof(t, 3072, []byte("other-value"))

	podUIDEmpty := ""
	_, _, pubPKIXEmpty, proofEmpty, _ := mustMakeEd25519KeyAndProof(t, []byte(podUIDEmpty), []string{})

	testCases := []struct {
		description string
		pcr         *capi.PodCertificateRequest
		wantErrors  field.ErrorList
	}{
		{
			description: "valid Ed25519 PCR",
			pcr: &capi.PodCertificateRequest{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "foo",
					Name:      "bar",
				},
				Spec: capi.PodCertificateRequestSpec{
					SignerName:           "foo.com/abc",
					PodName:              "pod-1",
					PodUID:               types.UID(podUID1),
					ServiceAccountName:   "sa-1",
					ServiceAccountUID:    "sa-uid-1",
					NodeName:             "node-1",
					NodeUID:              "node-uid-1",
					MaxExpirationSeconds: ptr.To[int32](86400),
					PKIXPublicKey:        ed25519PubPKIX1,
					ProofOfPossession:    ed25519Proof1,
				},
			},
			wantErrors: nil,
		},
		{
			description: "valid Ed25519 PCR (using PKCS#10)",
			pcr: &capi.PodCertificateRequest{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "foo",
					Name:      "bar",
				},
				Spec: capi.PodCertificateRequestSpec{
					SignerName:           "foo.com/abc",
					PodName:              "pod-1",
					PodUID:               types.UID(podUID1),
					ServiceAccountName:   "sa-1",
					ServiceAccountUID:    "sa-uid-1",
					NodeName:             "node-1",
					NodeUID:              "node-uid-1",
					MaxExpirationSeconds: ptr.To[int32](86400),
					StubPKCS10Request:    ed25519CSR1,
				},
			},
			wantErrors: nil,
		},
		{
			description: "invalid Ed25519 PCR (both StubPKCS10Request and PKIXPublicKey set)",
			pcr: &capi.PodCertificateRequest{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "foo",
					Name:      "bar",
				},
				Spec: capi.PodCertificateRequestSpec{
					SignerName:           "foo.com/abc",
					PodName:              "pod-1",
					PodUID:               types.UID(podUID1),
					ServiceAccountName:   "sa-1",
					ServiceAccountUID:    "sa-uid-1",
					NodeName:             "node-1",
					NodeUID:              "node-uid-1",
					MaxExpirationSeconds: ptr.To[int32](86400),
					PKIXPublicKey:        ed25519PubPKIX1,
					ProofOfPossession:    ed25519Proof1,
					StubPKCS10Request:    ed25519CSR1,
				},
			},
			wantErrors: field.ErrorList{
				field.Invalid(field.NewPath("spec"), field.OmitValueType{}, "exactly one of (stubPKCS10Request) or (pkixPublicKey, proofOfPossession) must be set"),
			},
		},
		{
			description: "invalid Ed25519 proof of possession (correct key signed wrong message)",
			pcr: &capi.PodCertificateRequest{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "foo",
					Name:      "bar",
				},
				Spec: capi.PodCertificateRequestSpec{
					SignerName:           "foo.com/abc",
					PodName:              "pod-1",
					PodUID:               types.UID(podUID1),
					ServiceAccountName:   "sa-1",
					ServiceAccountUID:    "sa-uid-1",
					NodeName:             "node-1",
					NodeUID:              "node-uid-1",
					MaxExpirationSeconds: ptr.To[int32](86400),
					PKIXPublicKey:        ed25519PubPKIX2,
					ProofOfPossession:    ed25519Proof2,
				},
			},
			wantErrors: field.ErrorList{
				field.Invalid(field.NewPath("spec", "proofOfPossession"), field.OmitValueType{}, "could not verify proof-of-possession signature"),
			},
		},
		{
			description: "invalid Ed25519 proof of possession (signed by different key)",
			pcr: &capi.PodCertificateRequest{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "foo",
					Name:      "bar",
				},
				Spec: capi.PodCertificateRequestSpec{
					SignerName:           "foo.com/abc",
					PodName:              "pod-1",
					PodUID:               types.UID(podUID1),
					ServiceAccountName:   "sa-1",
					ServiceAccountUID:    "sa-uid-1",
					NodeName:             "node-1",
					NodeUID:              "node-uid-1",
					MaxExpirationSeconds: ptr.To[int32](86400),
					PKIXPublicKey:        ed25519PubPKIX1,
					ProofOfPossession:    ed25519Proof3,
				},
			},
			wantErrors: field.ErrorList{
				field.Invalid(field.NewPath("spec", "proofOfPossession"), field.OmitValueType{}, "could not verify proof-of-possession signature"),
			},
		},
		{
			description: "invalid ECDSA P224 PCR",
			pcr: &capi.PodCertificateRequest{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "foo",
					Name:      "bar",
				},
				Spec: capi.PodCertificateRequestSpec{
					SignerName:           "foo.com/abc",
					PodName:              "pod-1",
					PodUID:               types.UID(podUID1),
					ServiceAccountName:   "sa-1",
					ServiceAccountUID:    "sa-uid-1",
					NodeName:             "node-1",
					NodeUID:              "node-uid-1",
					MaxExpirationSeconds: ptr.To[int32](86400),
					PKIXPublicKey:        ecdsaP224PubPKIX1,
					ProofOfPossession:    ecdsaP224Proof1,
				},
			},
			wantErrors: field.ErrorList{
				field.Invalid(field.NewPath("spec", "pkixPublicKey"), "curve P-224", "elliptic public keys must use curve P256 or P384"),
			},
		},
		{
			description: "valid ECDSA P256 PCR",
			pcr: &capi.PodCertificateRequest{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "foo",
					Name:      "bar",
				},
				Spec: capi.PodCertificateRequestSpec{
					SignerName:           "foo.com/abc",
					PodName:              "pod-1",
					PodUID:               types.UID(podUID1),
					ServiceAccountName:   "sa-1",
					ServiceAccountUID:    "sa-uid-1",
					NodeName:             "node-1",
					NodeUID:              "node-uid-1",
					MaxExpirationSeconds: ptr.To[int32](86400),
					PKIXPublicKey:        ecdsaP256PubPKIX1,
					ProofOfPossession:    ecdsaP256Proof1,
				},
			},
			wantErrors: nil,
		},
		{
			description: "valid ECDSA P384 PCR",
			pcr: &capi.PodCertificateRequest{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "foo",
					Name:      "bar",
				},
				Spec: capi.PodCertificateRequestSpec{
					SignerName:           "foo.com/abc",
					PodName:              "pod-1",
					PodUID:               types.UID(podUID1),
					ServiceAccountName:   "sa-1",
					ServiceAccountUID:    "sa-uid-1",
					NodeName:             "node-1",
					NodeUID:              "node-uid-1",
					MaxExpirationSeconds: ptr.To[int32](86400),
					PKIXPublicKey:        ecdsaP384PubPKIX1,
					ProofOfPossession:    ecdsaP384Proof1,
				},
			},
			wantErrors: nil,
		},
		{
			description: "valid ECDSA P521 PCR",
			pcr: &capi.PodCertificateRequest{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "foo",
					Name:      "bar",
				},
				Spec: capi.PodCertificateRequestSpec{
					SignerName:           "foo.com/abc",
					PodName:              "pod-1",
					PodUID:               types.UID(podUID1),
					ServiceAccountName:   "sa-1",
					ServiceAccountUID:    "sa-uid-1",
					NodeName:             "node-1",
					NodeUID:              "node-uid-1",
					MaxExpirationSeconds: ptr.To[int32](86400),
					PKIXPublicKey:        ecdsaP521PubPKIX1,
					ProofOfPossession:    ecdsaP521Proof1,
				},
			},
			wantErrors: nil,
		},
		{
			description: "invalid ECDSA proof of possession (correct key signed wrong message)",
			pcr: &capi.PodCertificateRequest{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "foo",
					Name:      "bar",
				},
				Spec: capi.PodCertificateRequestSpec{
					SignerName:           "foo.com/abc",
					PodName:              "pod-1",
					PodUID:               types.UID(podUID1),
					ServiceAccountName:   "sa-1",
					ServiceAccountUID:    "sa-uid-1",
					NodeName:             "node-1",
					NodeUID:              "node-uid-1",
					MaxExpirationSeconds: ptr.To[int32](86400),
					PKIXPublicKey:        ecdsaWrongProofPKIX,
					ProofOfPossession:    ecdsaWrongProof,
				},
			},
			wantErrors: field.ErrorList{
				field.Invalid(field.NewPath("spec", "proofOfPossession"), field.OmitValueType{}, "could not verify proof-of-possession signature"),
			},
		},
		{
			description: "invalid RSA 2048 PCR",
			pcr: &capi.PodCertificateRequest{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "foo",
					Name:      "bar",
				},
				Spec: capi.PodCertificateRequestSpec{
					SignerName:           "foo.com/abc",
					PodName:              "pod-1",
					PodUID:               types.UID(podUID1),
					ServiceAccountName:   "sa-1",
					ServiceAccountUID:    "sa-uid-1",
					NodeName:             "node-1",
					NodeUID:              "node-uid-1",
					MaxExpirationSeconds: ptr.To[int32](86400),
					PKIXPublicKey:        rsa2048PubPKIX1,
					ProofOfPossession:    rsa2048Proof1,
				},
			},
			wantErrors: field.ErrorList{
				field.Invalid(field.NewPath("spec", "pkixPublicKey"), "2048-bit modulus", "RSA keys must have modulus size 3072 or 4096"),
			},
		},
		{
			description: "valid RSA 3072 PCR",
			pcr: &capi.PodCertificateRequest{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "foo",
					Name:      "bar",
				},
				Spec: capi.PodCertificateRequestSpec{
					SignerName:           "foo.com/abc",
					PodName:              "pod-1",
					PodUID:               types.UID(podUID1),
					ServiceAccountName:   "sa-1",
					ServiceAccountUID:    "sa-uid-1",
					NodeName:             "node-1",
					NodeUID:              "node-uid-1",
					MaxExpirationSeconds: ptr.To[int32](86400),
					PKIXPublicKey:        rsa3072PubPKIX1,
					ProofOfPossession:    rsa3072Proof1,
				},
			},
			wantErrors: nil,
		},
		{
			description: "valid RSA 4096 PCR",
			pcr: &capi.PodCertificateRequest{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "foo",
					Name:      "bar",
				},
				Spec: capi.PodCertificateRequestSpec{
					SignerName:           "foo.com/abc",
					PodName:              "pod-1",
					PodUID:               types.UID(podUID1),
					ServiceAccountName:   "sa-1",
					ServiceAccountUID:    "sa-uid-1",
					NodeName:             "node-1",
					NodeUID:              "node-uid-1",
					MaxExpirationSeconds: ptr.To[int32](86400),
					PKIXPublicKey:        rsa4096PubPKIX1,
					ProofOfPossession:    rsa4096Proof1,
				},
			},
			wantErrors: nil,
		},
		{
			description: "invalid RSA proof of possession (correct key signed wrong message)",
			pcr: &capi.PodCertificateRequest{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "foo",
					Name:      "bar",
				},
				Spec: capi.PodCertificateRequestSpec{
					SignerName:           "foo.com/abc",
					PodName:              "pod-1",
					PodUID:               types.UID(podUID1),
					ServiceAccountName:   "sa-1",
					ServiceAccountUID:    "sa-uid-1",
					NodeName:             "node-1",
					NodeUID:              "node-uid-1",
					MaxExpirationSeconds: ptr.To[int32](86400),
					PKIXPublicKey:        rsaWrongProofPKIX,
					ProofOfPossession:    rsaWrongProof,
				},
			},
			wantErrors: field.ErrorList{
				field.Invalid(field.NewPath("spec", "proofOfPossession"), field.OmitValueType{}, "could not verify proof-of-possession signature"),
			},
		},
		{
			description: "bad signer name",
			pcr: &capi.PodCertificateRequest{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "foo",
					Name:      "bar",
				},
				Spec: capi.PodCertificateRequestSpec{
					SignerName:           "not-valid-signername",
					PodName:              "pod-1",
					PodUID:               types.UID(podUID1),
					ServiceAccountName:   "sa-1",
					ServiceAccountUID:    "sa-uid-1",
					NodeName:             "node-1",
					NodeUID:              "node-uid-1",
					MaxExpirationSeconds: ptr.To[int32](86400),
					PKIXPublicKey:        ed25519PubPKIX1,
					ProofOfPossession:    ed25519Proof1,
				},
			},
			wantErrors: field.ErrorList{
				field.Invalid(field.NewPath("spec", "signerName"), "not-valid-signername", "must be a fully qualified domain and path of the form 'example.com/signer-name'"),
			},
		},
		{
			description: "bad pod name",
			pcr: &capi.PodCertificateRequest{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "foo",
					Name:      "bar",
				},
				Spec: capi.PodCertificateRequestSpec{
					SignerName:           "foo.com/abc",
					PodName:              "pod-1-bad!!!!!",
					PodUID:               types.UID(podUID1),
					ServiceAccountName:   "sa-1",
					ServiceAccountUID:    "sa-uid-1",
					NodeName:             "node-1",
					NodeUID:              "node-uid-1",
					MaxExpirationSeconds: ptr.To[int32](86400),
					PKIXPublicKey:        ed25519PubPKIX1,
					ProofOfPossession:    ed25519Proof1,
				},
			},
			wantErrors: field.ErrorList{
				field.Invalid(field.NewPath("spec", "podName"), "pod-1-bad!!!!!", "a lowercase RFC 1123 subdomain must consist of lower case alphanumeric characters, '-' or '.', and must start and end with an alphanumeric character (e.g. 'example.com', regex used for validation is '[a-z0-9]([-a-z0-9]*[a-z0-9])?(\\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*')"),
			},
		},
		{
			description: "bad pod uid",
			pcr: &capi.PodCertificateRequest{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "foo",
					Name:      "bar",
				},
				Spec: capi.PodCertificateRequestSpec{
					SignerName:           "foo.com/abc",
					PodName:              "pod-1",
					PodUID:               types.UID(""),
					ServiceAccountName:   "sa-1",
					ServiceAccountUID:    "sa-uid-1",
					NodeName:             "node-1",
					NodeUID:              "node-uid-1",
					MaxExpirationSeconds: ptr.To[int32](86400),
					PKIXPublicKey:        pubPKIXEmpty,
					ProofOfPossession:    proofEmpty,
				},
			},
			wantErrors: field.ErrorList{
				field.Invalid(field.NewPath("spec", "podUID"), types.UID(""), "must not be empty"),
			},
		},
		{
			description: "bad service account name",
			pcr: &capi.PodCertificateRequest{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "foo",
					Name:      "bar",
				},
				Spec: capi.PodCertificateRequestSpec{
					SignerName:           "foo.com/abc",
					PodName:              "pod-1",
					PodUID:               types.UID(podUID1),
					ServiceAccountName:   "sa-1-bad!!!!!",
					ServiceAccountUID:    "sa-uid-1",
					NodeName:             "node-1",
					NodeUID:              "node-uid-1",
					MaxExpirationSeconds: ptr.To[int32](86400),
					PKIXPublicKey:        ed25519PubPKIX1,
					ProofOfPossession:    ed25519Proof1,
				},
			},
			wantErrors: field.ErrorList{
				field.Invalid(field.NewPath("spec", "serviceAccountName"), "sa-1-bad!!!!!", "a lowercase RFC 1123 subdomain must consist of lower case alphanumeric characters, '-' or '.', and must start and end with an alphanumeric character (e.g. 'example.com', regex used for validation is '[a-z0-9]([-a-z0-9]*[a-z0-9])?(\\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*')"),
			},
		},
		{
			description: "bad service account uid",
			pcr: &capi.PodCertificateRequest{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "foo",
					Name:      "bar",
				},
				Spec: capi.PodCertificateRequestSpec{
					SignerName:           "foo.com/abc",
					PodName:              "pod-1",
					PodUID:               types.UID(podUID1),
					ServiceAccountName:   "sa-1",
					ServiceAccountUID:    "",
					NodeName:             "node-1",
					NodeUID:              "node-uid-1",
					MaxExpirationSeconds: ptr.To[int32](86400),
					PKIXPublicKey:        ed25519PubPKIX1,
					ProofOfPossession:    ed25519Proof1,
				},
			},
			wantErrors: field.ErrorList{
				field.Invalid(field.NewPath("spec", "serviceAccountUID"), types.UID(""), "must not be empty"),
			},
		},
		{
			description: "bad node name",
			pcr: &capi.PodCertificateRequest{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "foo",
					Name:      "bar",
				},
				Spec: capi.PodCertificateRequestSpec{
					SignerName:           "foo.com/abc",
					PodName:              "pod-1",
					PodUID:               types.UID(podUID1),
					ServiceAccountName:   "sa-1",
					ServiceAccountUID:    "sa-uid-1",
					NodeName:             "node-1-bad!!!!!",
					NodeUID:              "node-uid-1",
					MaxExpirationSeconds: ptr.To[int32](86400),
					PKIXPublicKey:        ed25519PubPKIX1,
					ProofOfPossession:    ed25519Proof1,
				},
			},
			wantErrors: field.ErrorList{
				field.Invalid(field.NewPath("spec", "nodeName"), types.NodeName("node-1-bad!!!!!"), "a lowercase RFC 1123 subdomain must consist of lower case alphanumeric characters, '-' or '.', and must start and end with an alphanumeric character (e.g. 'example.com', regex used for validation is '[a-z0-9]([-a-z0-9]*[a-z0-9])?(\\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*')"),
			},
		},
		{
			description: "bad node uid",
			pcr: &capi.PodCertificateRequest{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "foo",
					Name:      "bar",
				},
				Spec: capi.PodCertificateRequestSpec{
					SignerName:           "foo.com/abc",
					PodName:              "pod-1",
					PodUID:               types.UID(podUID1),
					ServiceAccountName:   "sa-1",
					ServiceAccountUID:    "sa-uid-1",
					NodeName:             "node-1",
					NodeUID:              "",
					MaxExpirationSeconds: ptr.To[int32](86400),
					PKIXPublicKey:        ed25519PubPKIX1,
					ProofOfPossession:    ed25519Proof1,
				},
			},
			wantErrors: field.ErrorList{
				field.Invalid(field.NewPath("spec", "nodeUID"), types.UID(""), "must not be empty"),
			},
		},
		{
			description: "maxExpirationSeconds missing",
			pcr: &capi.PodCertificateRequest{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "foo",
					Name:      "bar",
				},
				Spec: capi.PodCertificateRequestSpec{
					SignerName:         "foo.com/abc",
					PodName:            "pod-1",
					PodUID:             types.UID(podUID1),
					ServiceAccountName: "sa-1",
					ServiceAccountUID:  "sa-uid-1",
					NodeName:           "node-1",
					NodeUID:            "node-uid-1",
					PKIXPublicKey:      ed25519PubPKIX1,
					ProofOfPossession:  ed25519Proof1,
				},
			},
			wantErrors: field.ErrorList{
				field.Required(field.NewPath("spec", "maxExpirationSeconds"), "must be set"),
			},
		},
		{
			description: "maxExpirationSeconds too large",
			pcr: &capi.PodCertificateRequest{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "foo",
					Name:      "bar",
				},
				Spec: capi.PodCertificateRequestSpec{
					SignerName:           "foo.com/abc",
					PodName:              "pod-1",
					PodUID:               types.UID(podUID1),
					ServiceAccountName:   "sa-1",
					ServiceAccountUID:    "sa-uid-1",
					NodeName:             "node-1",
					NodeUID:              "node-uid-1",
					MaxExpirationSeconds: ptr.To[int32](91*86400 + 1),
					PKIXPublicKey:        ed25519PubPKIX1,
					ProofOfPossession:    ed25519Proof1,
				},
			},
			wantErrors: field.ErrorList{
				field.Invalid(field.NewPath("spec", "maxExpirationSeconds"), ptr.To[int32](91*86400+1), fmt.Sprintf("must be in the range [%d, %d]", capi.MinMaxExpirationSeconds, capi.MaxMaxExpirationSeconds)),
			},
		},
		{
			description: "maxExpirationSeconds too large (Kubernetes signer)",
			pcr: &capi.PodCertificateRequest{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "foo",
					Name:      "bar",
				},
				Spec: capi.PodCertificateRequestSpec{
					SignerName:           "kubernetes.io/abc",
					PodName:              "pod-1",
					PodUID:               types.UID(podUID1),
					ServiceAccountName:   "sa-1",
					ServiceAccountUID:    "sa-uid-1",
					NodeName:             "node-1",
					NodeUID:              "node-uid-1",
					MaxExpirationSeconds: ptr.To[int32](86401),
					PKIXPublicKey:        ed25519PubPKIX1,
					ProofOfPossession:    ed25519Proof1,
				},
			},
			wantErrors: field.ErrorList{
				field.Invalid(field.NewPath("spec", "maxExpirationSeconds"), ptr.To[int32](86401), fmt.Sprintf("must be in the range [%d, %d]", capi.MinMaxExpirationSeconds, capi.KubernetesMaxMaxExpirationSeconds)),
			},
		},
		{
			description: "maxExpirationSeconds too small",
			pcr: &capi.PodCertificateRequest{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "foo",
					Name:      "bar",
				},
				Spec: capi.PodCertificateRequestSpec{
					SignerName:           "foo.com/abc",
					PodName:              "pod-1",
					PodUID:               types.UID(podUID1),
					ServiceAccountName:   "sa-1",
					ServiceAccountUID:    "sa-uid-1",
					NodeName:             "node-1",
					NodeUID:              "node-uid-1",
					MaxExpirationSeconds: ptr.To[int32](3600 - 1),
					PKIXPublicKey:        ed25519PubPKIX1,
					ProofOfPossession:    ed25519Proof1,
				},
			},
			wantErrors: field.ErrorList{
				field.Invalid(field.NewPath("spec", "maxExpirationSeconds"), ptr.To[int32](3600-1), fmt.Sprintf("must be in the range [%d, %d]", capi.MinMaxExpirationSeconds, capi.MaxMaxExpirationSeconds)),
			},
		},
		{
			// 超长公钥直接触发大小上限；下面的公钥长度恰好超过上限一个字节，用于验证边界判断是严格的。
			description: "pkixPublicKey too long",
			pcr: &capi.PodCertificateRequest{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "foo",
					Name:      "bar",
				},
				Spec: capi.PodCertificateRequestSpec{
					SignerName:           "foo.com/abc",
					PodName:              "pod-1",
					PodUID:               types.UID(podUID1),
					ServiceAccountName:   "sa-1",
					ServiceAccountUID:    "sa-uid-1",
					NodeName:             "node-1",
					NodeUID:              "node-uid-1",
					MaxExpirationSeconds: ptr.To[int32](86400),
					PKIXPublicKey:        make([]byte, capi.MaxPKIXPublicKeySize+1),
					ProofOfPossession:    []byte("abc"),
				},
			},
			wantErrors: field.ErrorList{
				field.TooLong(field.NewPath("spec", "pkixPublicKey"), make([]byte, capi.MaxPKIXPublicKeySize+1), capi.MaxPKIXPublicKeySize),
			},
		},
		{
			description: "proofOfPossession too long",
			pcr: &capi.PodCertificateRequest{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "foo",
					Name:      "bar",
				},
				Spec: capi.PodCertificateRequestSpec{
					SignerName:           "foo.com/abc",
					PodName:              "pod-1",
					PodUID:               types.UID(podUID1),
					ServiceAccountName:   "sa-1",
					ServiceAccountUID:    "sa-uid-1",
					NodeName:             "node-1",
					NodeUID:              "node-uid-1",
					MaxExpirationSeconds: ptr.To[int32](86400),
					PKIXPublicKey:        ed25519PubPKIX1,
					ProofOfPossession:    make([]byte, capi.MaxProofOfPossessionSize+1),
				},
			},
			wantErrors: field.ErrorList{
				field.TooLong(field.NewPath("spec", "proofOfPossession"), make([]byte, capi.MaxProofOfPossessionSize+1), capi.MaxProofOfPossessionSize),
			},
		},
		{
			description: "bad user annotations key name",
			pcr: &capi.PodCertificateRequest{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "foo",
					Name:      "bar",
				},
				Spec: capi.PodCertificateRequestSpec{
					SignerName:                "foo.com/abc",
					PodName:                   "pod-1",
					PodUID:                    types.UID(podUID1),
					ServiceAccountName:        "sa-1",
					ServiceAccountUID:         "sa-uid-1",
					NodeName:                  "node-1",
					NodeUID:                   "node-uid-1",
					MaxExpirationSeconds:      ptr.To[int32](86400),
					PKIXPublicKey:             ed25519PubPKIX1,
					ProofOfPossession:         ed25519Proof1,
					UnverifiedUserAnnotations: map[string]string{"test/domain/foo": "bar"},
				},
			},
			wantErrors: field.ErrorList{
				field.Invalid(field.NewPath("spec", "unverifiedUserAnnotations"), "test/domain/foo", "a valid label key must consist of alphanumeric characters, '-', '_' or '.', and must start and end with an alphanumeric character (e.g. 'MyName',  or 'my.name',  or '123-abc', regex used for validation is '([A-Za-z0-9][-A-Za-z0-9_.]*)?[A-Za-z0-9]') with an optional DNS subdomain prefix and '/' (e.g. 'example.com/MyName')"),
			},
		},
		{
			description: "bad user annotations key prefix too long",
			pcr: &capi.PodCertificateRequest{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "foo",
					Name:      "bar",
				},
				Spec: capi.PodCertificateRequestSpec{
					SignerName:                "foo.com/abc",
					PodName:                   "pod-1",
					PodUID:                    types.UID(podUID1),
					ServiceAccountName:        "sa-1",
					ServiceAccountUID:         "sa-uid-1",
					NodeName:                  "node-1",
					NodeUID:                   "node-uid-1",
					MaxExpirationSeconds:      ptr.To[int32](86400),
					PKIXPublicKey:             ed25519PubPKIX1,
					ProofOfPossession:         ed25519Proof1,
					UnverifiedUserAnnotations: map[string]string{strings.Repeat("a", 254) + "/foo": "bar"},
				},
			},
			wantErrors: field.ErrorList{
				field.Invalid(field.NewPath("spec", "unverifiedUserAnnotations"), strings.Repeat("a", 254)+"/foo", "prefix part must be no more than 253 bytes"),
			},
		},
		{
			description: "bad user annotations key/value total size too long",
			pcr: &capi.PodCertificateRequest{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "foo",
					Name:      "bar",
				},
				Spec: capi.PodCertificateRequestSpec{
					SignerName:                "foo.com/abc",
					PodName:                   "pod-1",
					PodUID:                    types.UID(podUID1),
					ServiceAccountName:        "sa-1",
					ServiceAccountUID:         "sa-uid-1",
					NodeName:                  "node-1",
					NodeUID:                   "node-uid-1",
					MaxExpirationSeconds:      ptr.To[int32](86400),
					PKIXPublicKey:             ed25519PubPKIX1,
					ProofOfPossession:         ed25519Proof1,
					UnverifiedUserAnnotations: map[string]string{"foo/bar": strings.Repeat("d", apimachineryvalidation.TotalAnnotationSizeLimitB)},
				},
			},
			wantErrors: field.ErrorList{
				field.TooLong(field.NewPath("spec", "unverifiedUserAnnotations"), "", apimachineryvalidation.TotalAnnotationSizeLimitB),
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			gotErrors := ValidatePodCertificateRequestCreate(tc.pcr)
			if diff := cmp.Diff(gotErrors, tc.wantErrors); diff != "" {
				t.Errorf("Unexpected error output from ValidatePodCertificateRequestCreate; diff (-got +want)\n%s", diff)
				t.Logf("Got errors: %+v", gotErrors)
			}
		})
	}
}

// TestValidatePodCertificateRequestUpdate 验证 PCR 创建后 spec 整体不可变：
// 任意字段（signer、Pod/SA/Node 标识、有效期、公钥、注解）变化都报 immutability 错误，
// 保证证书申请与最初核验过的身份始终一一对应。
func TestValidatePodCertificateRequestUpdate(t *testing.T) {
	podUID1 := "pod-uid-1"
	_, _, pubPKIX1, proof1, _ := mustMakeEd25519KeyAndProof(t, []byte(podUID1), []string{})

	testCases := []struct {
		description    string
		oldPCR, newPCR *capi.PodCertificateRequest
		wantErrors     field.ErrorList
	}{

		{
			description: "changing spec fields disallowed",
			oldPCR: &capi.PodCertificateRequest{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "foo",
					Name:      "bar",
				},
				Spec: capi.PodCertificateRequestSpec{
					SignerName:                "foo.com/abc",
					PodName:                   "pod-1",
					PodUID:                    types.UID(podUID1),
					ServiceAccountName:        "sa-1",
					ServiceAccountUID:         "sa-uid-1",
					NodeName:                  "node-1",
					NodeUID:                   "node-uid-1",
					MaxExpirationSeconds:      ptr.To[int32](86400),
					PKIXPublicKey:             pubPKIX1,
					ProofOfPossession:         proof1,
					UnverifiedUserAnnotations: map[string]string{"test.domain/foo": "bar"},
				},
			},
			newPCR: &capi.PodCertificateRequest{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "foo",
					Name:      "bar",
				},
				Spec: capi.PodCertificateRequestSpec{
					SignerName:                "foo.com/new",
					PodName:                   "new",
					PodUID:                    types.UID("new"),
					ServiceAccountName:        "new",
					ServiceAccountUID:         "new",
					NodeName:                  "new",
					NodeUID:                   "new",
					MaxExpirationSeconds:      ptr.To[int32](86401),
					PKIXPublicKey:             pubPKIX1,
					ProofOfPossession:         proof1,
					UnverifiedUserAnnotations: map[string]string{"test.domain/foo": "foo"},
				},
			},
			wantErrors: field.ErrorList{
				field.Invalid(
					field.NewPath("spec"),
					capi.PodCertificateRequestSpec{
						SignerName:                "foo.com/new",
						PodName:                   "new",
						PodUID:                    types.UID("new"),
						ServiceAccountName:        "new",
						ServiceAccountUID:         "new",
						NodeName:                  "new",
						NodeUID:                   "new",
						MaxExpirationSeconds:      ptr.To[int32](86401),
						PKIXPublicKey:             pubPKIX1,
						ProofOfPossession:         proof1,
						UnverifiedUserAnnotations: map[string]string{"test.domain/foo": "foo"},
					},
					"field is immutable",
				),
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			tc.oldPCR.ObjectMeta.ResourceVersion = "1"
			tc.newPCR.ObjectMeta.ResourceVersion = "2"

			gotErrors := ValidatePodCertificateRequestUpdate(tc.newPCR, tc.oldPCR)
			if diff := cmp.Diff(gotErrors, tc.wantErrors); diff != "" {
				t.Errorf("Unexpected error output from ValidatePodCertificateRequestUpdate; diff (-got +want)\n%s", diff)
			}
		})
	}
}

// TestValidatePodCertificateRequestStatusUpdate 验证 PCR 状态机的合法性。
// 用例覆盖：无变化放行、未知条件类型拒绝、Issued/Denied/Failed 条件必须为 True、
// 各状态间合法的单向流转与禁止的流转（如 Denied/Failed/Issued 回到 pending、
// Denied 与 Failed 互换、签发后更换证书）、
// 以及已签发证书链的时间边界（notBefore/notAfter/beginRefreshAt）、时长范围、
// 叶证书 DNS 名称与邮箱地址的合法性。
// 证书由真实 CA 与中间 CA 现场签发，确保 X.509 校验路径被完整执行。
func TestValidatePodCertificateRequestStatusUpdate(t *testing.T) {
	caCertDER, caPrivKey := mustMakeCA(t)
	// 中间 CA 用于验证“由中间 CA 签发的证书链同样被接受”。
	intermediateCACertDER, intermediateCAPrivKey := mustMakeIntermediateCA(t, caCertDER, caPrivKey)

	podUID1 := "pod-uid-1"
	_, pub1, pubPKIX1, proof1, _ := mustMakeEd25519KeyAndProof(t, []byte(podUID1), []string{})

	pod1Cert1 := mustSignCertForPublicKey(t, 24*time.Hour, pub1, caCertDER, caPrivKey, false, "", "")
	pod1Cert2 := mustSignCertForPublicKey(t, 18*time.Hour, pub1, caCertDER, caPrivKey, false, "", "")
	// 50 分钟有效期：小于一小时下限，应被拒绝。
	badCertTooShort := mustSignCertForPublicKey(t, 50*time.Minute, pub1, caCertDER, caPrivKey, false, "", "")
	// 25 小时有效期：超过请求允许的最大有效期，应被拒绝。
	badCertTooLong := mustSignCertForPublicKey(t, 25*time.Hour, pub1, caCertDER, caPrivKey, false, "", "")
	// 下面几张证书分别带上空 DNS 名、连续点、前导点和尾随点，用于验证 DNS 名称校验。
	certWithBadDNSName1 := mustSignCertForPublicKey(t, 24*time.Hour, pub1, caCertDER, caPrivKey, true, "", "")
	certWithBadDNSName2 := mustSignCertForPublicKey(t, 24*time.Hour, pub1, caCertDER, caPrivKey, true, "test-name..example", "")
	certWithBadDNSName3 := mustSignCertForPublicKey(t, 24*time.Hour, pub1, caCertDER, caPrivKey, true, ".example", "")
	certWithBadDNSName4 := mustSignCertForPublicKey(t, 24*time.Hour, pub1, caCertDER, caPrivKey, true, "example.", "")
	certWithBadEmailAddress := mustSignCertForPublicKey(t, 24*time.Hour, pub1, caCertDER, caPrivKey, false, "", "email@@address")

	certFromIntermediate := mustSignCertForPublicKey(t, 24*time.Hour, pub1, intermediateCACertDER, intermediateCAPrivKey, false, "", "")

	testCases := []struct {
		description    string
		oldPCR, newPCR *capi.PodCertificateRequest
		wantErrors     field.ErrorList
	}{
		{
			description: "changing nothing is allowed",
			oldPCR: &capi.PodCertificateRequest{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "foo",
					Name:      "bar",
				},
				Spec: capi.PodCertificateRequestSpec{
					SignerName:           "foo.com/abc",
					PodName:              "pod-1",
					PodUID:               types.UID(podUID1),
					ServiceAccountName:   "sa-1",
					ServiceAccountUID:    "sa-uid-1",
					NodeName:             "node-1",
					NodeUID:              "node-uid-1",
					MaxExpirationSeconds: ptr.To[int32](86400),
					PKIXPublicKey:        pubPKIX1,
					ProofOfPossession:    proof1,
				},
			},
			newPCR: &capi.PodCertificateRequest{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "foo",
					Name:      "bar",
				},
				Spec: capi.PodCertificateRequestSpec{
					SignerName:           "foo.com/abc",
					PodName:              "pod-1",
					PodUID:               types.UID(podUID1),
					ServiceAccountName:   "sa-1",
					ServiceAccountUID:    "sa-uid-1",
					NodeName:             "node-1",
					NodeUID:              "node-uid-1",
					MaxExpirationSeconds: ptr.To[int32](86400),
					PKIXPublicKey:        pubPKIX1,
					ProofOfPossession:    proof1,
				},
			},
		},
		{
			description: "adding unknown condition types is not allowed",
			oldPCR: &capi.PodCertificateRequest{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "foo",
					Name:      "bar",
				},
				Spec: capi.PodCertificateRequestSpec{
					SignerName:           "foo.com/abc",
					PodName:              "pod-1",
					PodUID:               types.UID(podUID1),
					ServiceAccountName:   "sa-1",
					ServiceAccountUID:    "sa-uid-1",
					NodeName:             "node-1",
					NodeUID:              "node-uid-1",
					MaxExpirationSeconds: ptr.To[int32](86400),
					PKIXPublicKey:        pubPKIX1,
					ProofOfPossession:    proof1,
				},
			},
			newPCR: &capi.PodCertificateRequest{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "foo",
					Name:      "bar",
				},
				Spec: capi.PodCertificateRequestSpec{
					SignerName:           "foo.com/abc",
					PodName:              "pod-1",
					PodUID:               types.UID(podUID1),
					ServiceAccountName:   "sa-1",
					ServiceAccountUID:    "sa-uid-1",
					NodeName:             "node-1",
					NodeUID:              "node-uid-1",
					MaxExpirationSeconds: ptr.To[int32](86400),
					PKIXPublicKey:        pubPKIX1,
					ProofOfPossession:    proof1,
				},
				Status: capi.PodCertificateRequestStatus{
					Conditions: []metav1.Condition{
						{
							Type:               "Unknown",
							Status:             metav1.ConditionFalse,
							Reason:             "Foo",
							Message:            "Foo message",
							LastTransitionTime: metav1.NewTime(time.Now()),
						},
					},
				},
			},
			wantErrors: field.ErrorList{
				field.NotSupported(field.NewPath("status", "conditions", "[0]", "type"), "Unknown", []string{capi.PodCertificateRequestConditionTypeIssued, capi.PodCertificateRequestConditionTypeDenied, capi.PodCertificateRequestConditionTypeFailed}),
			},
		},
		{
			description: "Issued must have status True",
			oldPCR: &capi.PodCertificateRequest{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "foo",
					Name:      "bar",
				},
				Spec: capi.PodCertificateRequestSpec{
					SignerName:           "foo.com/abc",
					PodName:              "pod-1",
					PodUID:               types.UID(podUID1),
					ServiceAccountName:   "sa-1",
					ServiceAccountUID:    "sa-uid-1",
					NodeName:             "node-1",
					NodeUID:              "node-uid-1",
					MaxExpirationSeconds: ptr.To[int32](86400),
					PKIXPublicKey:        pubPKIX1,
					ProofOfPossession:    proof1,
				},
			},
			newPCR: &capi.PodCertificateRequest{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "foo",
					Name:      "bar",
				},
				Spec: capi.PodCertificateRequestSpec{
					SignerName:           "foo.com/abc",
					PodName:              "pod-1",
					PodUID:               types.UID(podUID1),
					ServiceAccountName:   "sa-1",
					ServiceAccountUID:    "sa-uid-1",
					NodeName:             "node-1",
					NodeUID:              "node-uid-1",
					MaxExpirationSeconds: ptr.To[int32](86400),
					PKIXPublicKey:        pubPKIX1,
					ProofOfPossession:    proof1,
				},
				Status: capi.PodCertificateRequestStatus{
					Conditions: []metav1.Condition{
						{
							Type:               "Issued",
							Status:             metav1.ConditionFalse,
							Reason:             "Foo",
							Message:            "Foo message",
							LastTransitionTime: metav1.NewTime(time.Now()),
						},
					},
				},
			},
			wantErrors: field.ErrorList{
				field.NotSupported(field.NewPath("status", "conditions", "[0]", "status"), metav1.ConditionFalse, []metav1.ConditionStatus{metav1.ConditionTrue}),
			},
		},
		{
			description: "Denied must have status True",
			oldPCR: &capi.PodCertificateRequest{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "foo",
					Name:      "bar",
				},
				Spec: capi.PodCertificateRequestSpec{
					SignerName:           "foo.com/abc",
					PodName:              "pod-1",
					PodUID:               types.UID(podUID1),
					ServiceAccountName:   "sa-1",
					ServiceAccountUID:    "sa-uid-1",
					NodeName:             "node-1",
					NodeUID:              "node-uid-1",
					MaxExpirationSeconds: ptr.To[int32](86400),
					PKIXPublicKey:        pubPKIX1,
					ProofOfPossession:    proof1,
				},
			},
			newPCR: &capi.PodCertificateRequest{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "foo",
					Name:      "bar",
				},
				Spec: capi.PodCertificateRequestSpec{
					SignerName:           "foo.com/abc",
					PodName:              "pod-1",
					PodUID:               types.UID(podUID1),
					ServiceAccountName:   "sa-1",
					ServiceAccountUID:    "sa-uid-1",
					NodeName:             "node-1",
					NodeUID:              "node-uid-1",
					MaxExpirationSeconds: ptr.To[int32](86400),
					PKIXPublicKey:        pubPKIX1,
					ProofOfPossession:    proof1,
				},
				Status: capi.PodCertificateRequestStatus{
					Conditions: []metav1.Condition{
						{
							Type:               "Denied",
							Status:             metav1.ConditionFalse,
							Reason:             "Foo",
							Message:            "Foo message",
							LastTransitionTime: metav1.NewTime(time.Now()),
						},
					},
				},
			},
			wantErrors: field.ErrorList{
				field.NotSupported(field.NewPath("status", "conditions", "[0]", "status"), metav1.ConditionFalse, []metav1.ConditionStatus{metav1.ConditionTrue}),
			},
		},
		{
			description: "Failed must have status True",
			oldPCR: &capi.PodCertificateRequest{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "foo",
					Name:      "bar",
				},
				Spec: capi.PodCertificateRequestSpec{
					SignerName:           "foo.com/abc",
					PodName:              "pod-1",
					PodUID:               types.UID(podUID1),
					ServiceAccountName:   "sa-1",
					ServiceAccountUID:    "sa-uid-1",
					NodeName:             "node-1",
					NodeUID:              "node-uid-1",
					MaxExpirationSeconds: ptr.To[int32](86400),
					PKIXPublicKey:        pubPKIX1,
					ProofOfPossession:    proof1,
				},
			},
			newPCR: &capi.PodCertificateRequest{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "foo",
					Name:      "bar",
				},
				Spec: capi.PodCertificateRequestSpec{
					SignerName:           "foo.com/abc",
					PodName:              "pod-1",
					PodUID:               types.UID(podUID1),
					ServiceAccountName:   "sa-1",
					ServiceAccountUID:    "sa-uid-1",
					NodeName:             "node-1",
					NodeUID:              "node-uid-1",
					MaxExpirationSeconds: ptr.To[int32](86400),
					PKIXPublicKey:        pubPKIX1,
					ProofOfPossession:    proof1,
				},
				Status: capi.PodCertificateRequestStatus{
					Conditions: []metav1.Condition{
						{
							Type:               "Failed",
							Status:             metav1.ConditionFalse,
							Reason:             "Foo",
							Message:            "Foo message",
							LastTransitionTime: metav1.NewTime(time.Now()),
						},
					},
				},
			},
			wantErrors: field.ErrorList{
				field.NotSupported(field.NewPath("status", "conditions", "[0]", "status"), metav1.ConditionFalse, []metav1.ConditionStatus{metav1.ConditionTrue}),
			},
		},
		{
			description: "transitioning to Denied status is allowed",
			oldPCR: &capi.PodCertificateRequest{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "foo",
					Name:      "bar",
				},
				Spec: capi.PodCertificateRequestSpec{
					SignerName:           "foo.com/abc",
					PodName:              "pod-1",
					PodUID:               types.UID(podUID1),
					ServiceAccountName:   "sa-1",
					ServiceAccountUID:    "sa-uid-1",
					NodeName:             "node-1",
					NodeUID:              "node-uid-1",
					MaxExpirationSeconds: ptr.To[int32](86400),
					PKIXPublicKey:        pubPKIX1,
					ProofOfPossession:    proof1,
				},
			},
			newPCR: &capi.PodCertificateRequest{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "foo",
					Name:      "bar",
				},
				Spec: capi.PodCertificateRequestSpec{
					SignerName:           "foo.com/abc",
					PodName:              "pod-1",
					PodUID:               types.UID(podUID1),
					ServiceAccountName:   "sa-1",
					ServiceAccountUID:    "sa-uid-1",
					NodeName:             "node-1",
					NodeUID:              "node-uid-1",
					MaxExpirationSeconds: ptr.To[int32](86400),
					PKIXPublicKey:        pubPKIX1,
					ProofOfPossession:    proof1,
				},
				Status: capi.PodCertificateRequestStatus{
					Conditions: []metav1.Condition{
						{
							Type:               capi.PodCertificateRequestConditionTypeDenied,
							Status:             metav1.ConditionTrue,
							Reason:             capi.PodCertificateRequestConditionUnsupportedKeyType,
							Message:            "Foo message",
							LastTransitionTime: metav1.NewTime(time.Now()),
						},
					},
				},
			},
		},
		{
			description: "you can't issue a certificate if you set Denied status",
			oldPCR: &capi.PodCertificateRequest{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "foo",
					Name:      "bar",
				},
				Spec: capi.PodCertificateRequestSpec{
					SignerName:           "foo.com/abc",
					PodName:              "pod-1",
					PodUID:               types.UID(podUID1),
					ServiceAccountName:   "sa-1",
					ServiceAccountUID:    "sa-uid-1",
					NodeName:             "node-1",
					NodeUID:              "node-uid-1",
					MaxExpirationSeconds: ptr.To[int32](86400),
					PKIXPublicKey:        pubPKIX1,
					ProofOfPossession:    proof1,
				},
			},
			newPCR: &capi.PodCertificateRequest{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "foo",
					Name:      "bar",
				},
				Spec: capi.PodCertificateRequestSpec{
					SignerName:           "foo.com/abc",
					PodName:              "pod-1",
					PodUID:               types.UID(podUID1),
					ServiceAccountName:   "sa-1",
					ServiceAccountUID:    "sa-uid-1",
					NodeName:             "node-1",
					NodeUID:              "node-uid-1",
					MaxExpirationSeconds: ptr.To[int32](86400),
					PKIXPublicKey:        pubPKIX1,
					ProofOfPossession:    proof1,
				},
				Status: capi.PodCertificateRequestStatus{
					Conditions: []metav1.Condition{
						{
							Type:               capi.PodCertificateRequestConditionTypeDenied,
							Status:             metav1.ConditionTrue,
							Reason:             "Whatever",
							Message:            "Foo message",
							LastTransitionTime: metav1.NewTime(time.Now()),
						},
					},
					CertificateChain: pod1Cert1,
				},
			},
			wantErrors: field.ErrorList{
				field.Invalid(
					field.NewPath("status"),
					field.OmitValueType{},
					"non-condition status fields must be empty when denying or failing the PodCertificateRequest",
				),
			},
		},
		{
			description: "transitioning to Failed status is allowed",
			oldPCR: &capi.PodCertificateRequest{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "foo",
					Name:      "bar",
				},
				Spec: capi.PodCertificateRequestSpec{
					SignerName:           "foo.com/abc",
					PodName:              "pod-1",
					PodUID:               types.UID(podUID1),
					ServiceAccountName:   "sa-1",
					ServiceAccountUID:    "sa-uid-1",
					NodeName:             "node-1",
					NodeUID:              "node-uid-1",
					MaxExpirationSeconds: ptr.To[int32](86400),
					PKIXPublicKey:        pubPKIX1,
					ProofOfPossession:    proof1,
				},
			},
			newPCR: &capi.PodCertificateRequest{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "foo",
					Name:      "bar",
				},
				Spec: capi.PodCertificateRequestSpec{
					SignerName:           "foo.com/abc",
					PodName:              "pod-1",
					PodUID:               types.UID(podUID1),
					ServiceAccountName:   "sa-1",
					ServiceAccountUID:    "sa-uid-1",
					NodeName:             "node-1",
					NodeUID:              "node-uid-1",
					MaxExpirationSeconds: ptr.To[int32](86400),
					PKIXPublicKey:        pubPKIX1,
					ProofOfPossession:    proof1,
				},
				Status: capi.PodCertificateRequestStatus{
					Conditions: []metav1.Condition{
						{
							Type:               capi.PodCertificateRequestConditionTypeFailed,
							Status:             metav1.ConditionTrue,
							Reason:             "Whatever",
							Message:            "Foo message",
							LastTransitionTime: metav1.NewTime(time.Now()),
						},
					},
				},
			},
		},
		{
			description: "you can't issue a certificate if you set Failed status",
			oldPCR: &capi.PodCertificateRequest{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "foo",
					Name:      "bar",
				},
				Spec: capi.PodCertificateRequestSpec{
					SignerName:           "foo.com/abc",
					PodName:              "pod-1",
					PodUID:               types.UID(podUID1),
					ServiceAccountName:   "sa-1",
					ServiceAccountUID:    "sa-uid-1",
					NodeName:             "node-1",
					NodeUID:              "node-uid-1",
					MaxExpirationSeconds: ptr.To[int32](86400),
					PKIXPublicKey:        pubPKIX1,
					ProofOfPossession:    proof1,
				},
			},
			newPCR: &capi.PodCertificateRequest{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "foo",
					Name:      "bar",
				},
				Spec: capi.PodCertificateRequestSpec{
					SignerName:           "foo.com/abc",
					PodName:              "pod-1",
					PodUID:               types.UID(podUID1),
					ServiceAccountName:   "sa-1",
					ServiceAccountUID:    "sa-uid-1",
					NodeName:             "node-1",
					NodeUID:              "node-uid-1",
					MaxExpirationSeconds: ptr.To[int32](86400),
					PKIXPublicKey:        pubPKIX1,
					ProofOfPossession:    proof1,
				},
				Status: capi.PodCertificateRequestStatus{
					Conditions: []metav1.Condition{
						{
							Type:               capi.PodCertificateRequestConditionTypeFailed,
							Status:             metav1.ConditionTrue,
							Reason:             "Whatever",
							Message:            "Foo message",
							LastTransitionTime: metav1.NewTime(time.Now()),
						},
					},
					CertificateChain: pod1Cert1,
				},
			},
			wantErrors: field.ErrorList{
				field.Invalid(
					field.NewPath("status"),
					field.OmitValueType{},
					"non-condition status fields must be empty when denying or failing the PodCertificateRequest",
				),
			},
		},
		{
			description: "valid issuance",
			oldPCR: &capi.PodCertificateRequest{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "foo",
					Name:      "bar",
				},
				Spec: capi.PodCertificateRequestSpec{
					SignerName:           "foo.com/abc",
					PodName:              "pod-1",
					PodUID:               types.UID(podUID1),
					ServiceAccountName:   "sa-1",
					ServiceAccountUID:    "sa-uid-1",
					NodeName:             "node-1",
					NodeUID:              "node-uid-1",
					MaxExpirationSeconds: ptr.To[int32](86400),
					PKIXPublicKey:        pubPKIX1,
					ProofOfPossession:    proof1,
				},
			},
			newPCR: &capi.PodCertificateRequest{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "foo",
					Name:      "bar",
				},
				Spec: capi.PodCertificateRequestSpec{
					SignerName:           "foo.com/abc",
					PodName:              "pod-1",
					PodUID:               types.UID(podUID1),
					ServiceAccountName:   "sa-1",
					ServiceAccountUID:    "sa-uid-1",
					NodeName:             "node-1",
					NodeUID:              "node-uid-1",
					MaxExpirationSeconds: ptr.To[int32](86400),
					PKIXPublicKey:        pubPKIX1,
					ProofOfPossession:    proof1,
				},
				Status: capi.PodCertificateRequestStatus{
					Conditions: []metav1.Condition{
						{
							Type:               capi.PodCertificateRequestConditionTypeIssued,
							Status:             metav1.ConditionTrue,
							Reason:             "Whatever",
							Message:            "Foo message",
							LastTransitionTime: metav1.NewTime(time.Now()),
						},
					},
					CertificateChain: pod1Cert1,
					NotBefore:        ptr.To(metav1.NewTime(mustParseTime(t, "1970-01-01T00:00:00Z"))),
					BeginRefreshAt:   ptr.To(metav1.NewTime(mustParseTime(t, "1970-01-01T12:00:00Z"))),
					NotAfter:         ptr.To(metav1.NewTime(mustParseTime(t, "1970-01-02T00:00:00Z"))),
				},
			},
		},
		{
			description: "valid issuance with intermediate CA",
			oldPCR: &capi.PodCertificateRequest{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "foo",
					Name:      "bar",
				},
				Spec: capi.PodCertificateRequestSpec{
					SignerName:           "foo.com/abc",
					PodName:              "pod-1",
					PodUID:               types.UID(podUID1),
					ServiceAccountName:   "sa-1",
					ServiceAccountUID:    "sa-uid-1",
					NodeName:             "node-1",
					NodeUID:              "node-uid-1",
					MaxExpirationSeconds: ptr.To[int32](86400),
					PKIXPublicKey:        pubPKIX1,
					ProofOfPossession:    proof1,
				},
			},
			newPCR: &capi.PodCertificateRequest{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "foo",
					Name:      "bar",
				},
				Spec: capi.PodCertificateRequestSpec{
					SignerName:           "foo.com/abc",
					PodName:              "pod-1",
					PodUID:               types.UID(podUID1),
					ServiceAccountName:   "sa-1",
					ServiceAccountUID:    "sa-uid-1",
					NodeName:             "node-1",
					NodeUID:              "node-uid-1",
					MaxExpirationSeconds: ptr.To[int32](86400),
					PKIXPublicKey:        pubPKIX1,
					ProofOfPossession:    proof1,
				},
				Status: capi.PodCertificateRequestStatus{
					Conditions: []metav1.Condition{
						{
							Type:               capi.PodCertificateRequestConditionTypeIssued,
							Status:             metav1.ConditionTrue,
							Reason:             "Whatever",
							Message:            "Foo message",
							LastTransitionTime: metav1.NewTime(time.Now()),
						},
					},
					CertificateChain: certFromIntermediate + "\n" + pemEncode("CERTIFICATE", intermediateCACertDER),
					NotBefore:        ptr.To(metav1.NewTime(mustParseTime(t, "1970-01-01T00:00:00Z"))),
					BeginRefreshAt:   ptr.To(metav1.NewTime(mustParseTime(t, "1970-01-01T12:00:00Z"))),
					NotAfter:         ptr.To(metav1.NewTime(mustParseTime(t, "1970-01-02T00:00:00Z"))),
				},
			},
		},
		{
			description: "Once issued, the certificate cannot be changed to a different valid certificate",
			oldPCR: &capi.PodCertificateRequest{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "foo",
					Name:      "bar",
				},
				Spec: capi.PodCertificateRequestSpec{
					SignerName:           "foo.com/abc",
					PodName:              "pod-1",
					PodUID:               types.UID(podUID1),
					ServiceAccountName:   "sa-1",
					ServiceAccountUID:    "sa-uid-1",
					NodeName:             "node-1",
					NodeUID:              "node-uid-1",
					MaxExpirationSeconds: ptr.To[int32](86400),
					PKIXPublicKey:        pubPKIX1,
					ProofOfPossession:    proof1,
				},
				Status: capi.PodCertificateRequestStatus{
					Conditions: []metav1.Condition{
						{
							Type:               capi.PodCertificateRequestConditionTypeIssued,
							Status:             metav1.ConditionTrue,
							Reason:             "Whatever",
							Message:            "Foo message",
							LastTransitionTime: metav1.NewTime(time.Now()),
						},
					},
					CertificateChain: pod1Cert1,
					NotBefore:        ptr.To(metav1.NewTime(mustParseTime(t, "1970-01-01T00:00:00Z"))),
					BeginRefreshAt:   ptr.To(metav1.NewTime(mustParseTime(t, "1970-01-01T12:00:00Z"))),
					NotAfter:         ptr.To(metav1.NewTime(mustParseTime(t, "1970-01-02T00:00:00Z"))),
				},
			},
			newPCR: &capi.PodCertificateRequest{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "foo",
					Name:      "bar",
				},
				Spec: capi.PodCertificateRequestSpec{
					SignerName:           "foo.com/abc",
					PodName:              "pod-1",
					PodUID:               types.UID(podUID1),
					ServiceAccountName:   "sa-1",
					ServiceAccountUID:    "sa-uid-1",
					NodeName:             "node-1",
					NodeUID:              "node-uid-1",
					MaxExpirationSeconds: ptr.To[int32](86400),
					PKIXPublicKey:        pubPKIX1,
					ProofOfPossession:    proof1,
				},
				Status: capi.PodCertificateRequestStatus{
					Conditions: []metav1.Condition{
						{
							Type:               capi.PodCertificateRequestConditionTypeIssued,
							Status:             metav1.ConditionTrue,
							Reason:             "Whatever",
							Message:            "Foo message",
							LastTransitionTime: metav1.NewTime(time.Now()),
						},
					},
					CertificateChain: pod1Cert2,
					NotBefore:        ptr.To(metav1.NewTime(mustParseTime(t, "1970-01-01T00:00:00Z"))),
					BeginRefreshAt:   ptr.To(metav1.NewTime(mustParseTime(t, "1970-01-01T12:00:00Z"))),
					NotAfter:         ptr.To(metav1.NewTime(mustParseTime(t, "1970-01-01T18:00:00Z"))),
				},
			},
			wantErrors: field.ErrorList{
				field.Invalid(field.NewPath("status"), field.OmitValueType{}, "immutable after PodCertificateRequest is issued, denied, or failed"),
			},
		},
		{
			description: "a request cannot be both Denied and Failed",
			oldPCR: &capi.PodCertificateRequest{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "foo",
					Name:      "bar",
				},
				Spec: capi.PodCertificateRequestSpec{
					SignerName:           "foo.com/abc",
					PodName:              "pod-1",
					PodUID:               types.UID(podUID1),
					ServiceAccountName:   "sa-1",
					ServiceAccountUID:    "sa-uid-1",
					NodeName:             "node-1",
					NodeUID:              "node-uid-1",
					MaxExpirationSeconds: ptr.To[int32](86400),
					PKIXPublicKey:        pubPKIX1,
					ProofOfPossession:    proof1,
				},
			},
			newPCR: &capi.PodCertificateRequest{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "foo",
					Name:      "bar",
				},
				Spec: capi.PodCertificateRequestSpec{
					SignerName:           "foo.com/abc",
					PodName:              "pod-1",
					PodUID:               types.UID(podUID1),
					ServiceAccountName:   "sa-1",
					ServiceAccountUID:    "sa-uid-1",
					NodeName:             "node-1",
					NodeUID:              "node-uid-1",
					MaxExpirationSeconds: ptr.To[int32](86400),
					PKIXPublicKey:        pubPKIX1,
					ProofOfPossession:    proof1,
				},
				Status: capi.PodCertificateRequestStatus{
					Conditions: []metav1.Condition{
						{
							Type:               capi.PodCertificateRequestConditionTypeDenied,
							Status:             metav1.ConditionTrue,
							Reason:             "Whatever",
							Message:            "Foo message",
							LastTransitionTime: metav1.NewTime(time.Now()),
						},
						{
							Type:               capi.PodCertificateRequestConditionTypeFailed,
							Status:             metav1.ConditionTrue,
							Reason:             "Whatever",
							Message:            "Foo message",
							LastTransitionTime: metav1.NewTime(time.Now()),
						},
					},
				},
			},
			wantErrors: field.ErrorList{
				field.Invalid(field.NewPath("status", "conditions", "[1]", "type"), "Failed", `There may be at most one condition with type "Issued", "Denied", or "Failed"`),
			},
		},
		{
			description: "certificate cannot be issued and denied",
			oldPCR: &capi.PodCertificateRequest{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "foo",
					Name:      "bar",
				},
				Spec: capi.PodCertificateRequestSpec{
					SignerName:           "foo.com/abc",
					PodName:              "pod-1",
					PodUID:               types.UID(podUID1),
					ServiceAccountName:   "sa-1",
					ServiceAccountUID:    "sa-uid-1",
					NodeName:             "node-1",
					NodeUID:              "node-uid-1",
					MaxExpirationSeconds: ptr.To[int32](86400),
					PKIXPublicKey:        pubPKIX1,
					ProofOfPossession:    proof1,
				},
			},
			newPCR: &capi.PodCertificateRequest{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "foo",
					Name:      "bar",
				},
				Spec: capi.PodCertificateRequestSpec{
					SignerName:           "foo.com/abc",
					PodName:              "pod-1",
					PodUID:               types.UID(podUID1),
					ServiceAccountName:   "sa-1",
					ServiceAccountUID:    "sa-uid-1",
					NodeName:             "node-1",
					NodeUID:              "node-uid-1",
					MaxExpirationSeconds: ptr.To[int32](86400),
					PKIXPublicKey:        pubPKIX1,
					ProofOfPossession:    proof1,
				},
				Status: capi.PodCertificateRequestStatus{
					Conditions: []metav1.Condition{
						{
							Type:               capi.PodCertificateRequestConditionTypeIssued,
							Status:             metav1.ConditionTrue,
							Reason:             "Whatever",
							Message:            "Foo message",
							LastTransitionTime: metav1.NewTime(time.Now()),
						},
						{
							Type:               capi.PodCertificateRequestConditionTypeDenied,
							Status:             metav1.ConditionTrue,
							Reason:             "Whatever",
							Message:            "Foo message",
							LastTransitionTime: metav1.NewTime(time.Now()),
						},
					},
					CertificateChain: pod1Cert1,
					NotBefore:        ptr.To(metav1.NewTime(mustParseTime(t, "1970-01-01T00:00:00Z"))),
					BeginRefreshAt:   ptr.To(metav1.NewTime(mustParseTime(t, "1970-01-01T12:00:00Z"))),
					NotAfter:         ptr.To(metav1.NewTime(mustParseTime(t, "1970-01-02T00:00:00Z"))),
				},
			},
			wantErrors: field.ErrorList{
				field.Invalid(field.NewPath("status", "conditions", "[1]", "type"), "Denied", `There may be at most one condition with type "Issued", "Denied", or "Failed"`),
			},
		},
		{
			description: "certificate cannot be issued and failed",
			oldPCR: &capi.PodCertificateRequest{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "foo",
					Name:      "bar",
				},
				Spec: capi.PodCertificateRequestSpec{
					SignerName:           "foo.com/abc",
					PodName:              "pod-1",
					PodUID:               types.UID(podUID1),
					ServiceAccountName:   "sa-1",
					ServiceAccountUID:    "sa-uid-1",
					NodeName:             "node-1",
					NodeUID:              "node-uid-1",
					MaxExpirationSeconds: ptr.To[int32](86400),
					PKIXPublicKey:        pubPKIX1,
					ProofOfPossession:    proof1,
				},
			},
			newPCR: &capi.PodCertificateRequest{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "foo",
					Name:      "bar",
				},
				Spec: capi.PodCertificateRequestSpec{
					SignerName:           "foo.com/abc",
					PodName:              "pod-1",
					PodUID:               types.UID(podUID1),
					ServiceAccountName:   "sa-1",
					ServiceAccountUID:    "sa-uid-1",
					NodeName:             "node-1",
					NodeUID:              "node-uid-1",
					MaxExpirationSeconds: ptr.To[int32](86400),
					PKIXPublicKey:        pubPKIX1,
					ProofOfPossession:    proof1,
				},
				Status: capi.PodCertificateRequestStatus{
					Conditions: []metav1.Condition{
						{
							Type:               capi.PodCertificateRequestConditionTypeIssued,
							Status:             metav1.ConditionTrue,
							Reason:             "Whatever",
							Message:            "Foo message",
							LastTransitionTime: metav1.NewTime(time.Now()),
						},
						{
							Type:               capi.PodCertificateRequestConditionTypeFailed,
							Status:             metav1.ConditionTrue,
							Reason:             "Whatever",
							Message:            "Foo message",
							LastTransitionTime: metav1.NewTime(time.Now()),
						},
					},
					CertificateChain: pod1Cert1,
					NotBefore:        ptr.To(metav1.NewTime(mustParseTime(t, "1970-01-01T00:00:00Z"))),
					BeginRefreshAt:   ptr.To(metav1.NewTime(mustParseTime(t, "1970-01-01T12:00:00Z"))),
					NotAfter:         ptr.To(metav1.NewTime(mustParseTime(t, "1970-01-02T00:00:00Z"))),
				},
			},
			wantErrors: field.ErrorList{
				field.Invalid(field.NewPath("status", "conditions", "[1]", "type"), "Failed", `There may be at most one condition with type "Issued", "Denied", or "Failed"`),
			},
		},
		{
			description: "a request cannot change from Denied to Failed",
			oldPCR: &capi.PodCertificateRequest{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "foo",
					Name:      "bar",
				},
				Spec: capi.PodCertificateRequestSpec{
					SignerName:           "foo.com/abc",
					PodName:              "pod-1",
					PodUID:               types.UID(podUID1),
					ServiceAccountName:   "sa-1",
					ServiceAccountUID:    "sa-uid-1",
					NodeName:             "node-1",
					NodeUID:              "node-uid-1",
					MaxExpirationSeconds: ptr.To[int32](86400),
					PKIXPublicKey:        pubPKIX1,
					ProofOfPossession:    proof1,
				},
				Status: capi.PodCertificateRequestStatus{
					Conditions: []metav1.Condition{
						{
							Type:               capi.PodCertificateRequestConditionTypeDenied,
							Status:             metav1.ConditionTrue,
							Reason:             "Whatever",
							Message:            "Foo message",
							LastTransitionTime: metav1.NewTime(time.Now()),
						},
					},
				},
			},
			newPCR: &capi.PodCertificateRequest{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "foo",
					Name:      "bar",
				},
				Spec: capi.PodCertificateRequestSpec{
					SignerName:           "foo.com/abc",
					PodName:              "pod-1",
					PodUID:               types.UID(podUID1),
					ServiceAccountName:   "sa-1",
					ServiceAccountUID:    "sa-uid-1",
					NodeName:             "node-1",
					NodeUID:              "node-uid-1",
					MaxExpirationSeconds: ptr.To[int32](86400),
					PKIXPublicKey:        pubPKIX1,
					ProofOfPossession:    proof1,
				},
				Status: capi.PodCertificateRequestStatus{
					Conditions: []metav1.Condition{
						{
							Type:               capi.PodCertificateRequestConditionTypeFailed,
							Status:             metav1.ConditionTrue,
							Reason:             "Whatever",
							Message:            "Foo message",
							LastTransitionTime: metav1.NewTime(time.Now()),
						},
					},
				},
			},
			wantErrors: field.ErrorList{
				field.Invalid(field.NewPath("status"), field.OmitValueType{}, `immutable after PodCertificateRequest is issued, denied, or failed`),
			},
		},
		{
			description: "a request cannot change from Failed to Denied",
			oldPCR: &capi.PodCertificateRequest{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "foo",
					Name:      "bar",
				},
				Spec: capi.PodCertificateRequestSpec{
					SignerName:           "foo.com/abc",
					PodName:              "pod-1",
					PodUID:               types.UID(podUID1),
					ServiceAccountName:   "sa-1",
					ServiceAccountUID:    "sa-uid-1",
					NodeName:             "node-1",
					NodeUID:              "node-uid-1",
					MaxExpirationSeconds: ptr.To[int32](86400),
					PKIXPublicKey:        pubPKIX1,
					ProofOfPossession:    proof1,
				},
				Status: capi.PodCertificateRequestStatus{
					Conditions: []metav1.Condition{
						{
							Type:               capi.PodCertificateRequestConditionTypeFailed,
							Status:             metav1.ConditionTrue,
							Reason:             "Whatever",
							Message:            "Foo message",
							LastTransitionTime: metav1.NewTime(time.Now()),
						},
					},
				},
			},
			newPCR: &capi.PodCertificateRequest{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "foo",
					Name:      "bar",
				},
				Spec: capi.PodCertificateRequestSpec{
					SignerName:           "foo.com/abc",
					PodName:              "pod-1",
					PodUID:               types.UID(podUID1),
					ServiceAccountName:   "sa-1",
					ServiceAccountUID:    "sa-uid-1",
					NodeName:             "node-1",
					NodeUID:              "node-uid-1",
					MaxExpirationSeconds: ptr.To[int32](86400),
					PKIXPublicKey:        pubPKIX1,
					ProofOfPossession:    proof1,
				},
				Status: capi.PodCertificateRequestStatus{
					Conditions: []metav1.Condition{
						{
							Type:               capi.PodCertificateRequestConditionTypeDenied,
							Status:             metav1.ConditionTrue,
							Reason:             "Whatever",
							Message:            "Foo message",
							LastTransitionTime: metav1.NewTime(time.Now()),
						},
					},
				},
			},
			wantErrors: field.ErrorList{
				field.Invalid(field.NewPath("status"), field.OmitValueType{}, `immutable after PodCertificateRequest is issued, denied, or failed`),
			},
		},
		{
			description: "a request cannot change from Denied to pending",
			oldPCR: &capi.PodCertificateRequest{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "foo",
					Name:      "bar",
				},
				Spec: capi.PodCertificateRequestSpec{
					SignerName:           "foo.com/abc",
					PodName:              "pod-1",
					PodUID:               types.UID(podUID1),
					ServiceAccountName:   "sa-1",
					ServiceAccountUID:    "sa-uid-1",
					NodeName:             "node-1",
					NodeUID:              "node-uid-1",
					MaxExpirationSeconds: ptr.To[int32](86400),
					PKIXPublicKey:        pubPKIX1,
					ProofOfPossession:    proof1,
				},
				Status: capi.PodCertificateRequestStatus{
					Conditions: []metav1.Condition{
						{
							Type:               capi.PodCertificateRequestConditionTypeDenied,
							Status:             metav1.ConditionTrue,
							Reason:             "Whatever",
							Message:            "Foo message",
							LastTransitionTime: metav1.NewTime(time.Now()),
						},
					},
				},
			},
			newPCR: &capi.PodCertificateRequest{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "foo",
					Name:      "bar",
				},
				Spec: capi.PodCertificateRequestSpec{
					SignerName:           "foo.com/abc",
					PodName:              "pod-1",
					PodUID:               types.UID(podUID1),
					ServiceAccountName:   "sa-1",
					ServiceAccountUID:    "sa-uid-1",
					NodeName:             "node-1",
					NodeUID:              "node-uid-1",
					MaxExpirationSeconds: ptr.To[int32](86400),
					PKIXPublicKey:        pubPKIX1,
					ProofOfPossession:    proof1,
				},
				Status: capi.PodCertificateRequestStatus{
					Conditions: []metav1.Condition{},
				},
			},
			wantErrors: field.ErrorList{
				field.Invalid(field.NewPath("status"), field.OmitValueType{}, `immutable after PodCertificateRequest is issued, denied, or failed`),
			},
		},
		{
			description: "a request cannot change from Failed to pending",
			oldPCR: &capi.PodCertificateRequest{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "foo",
					Name:      "bar",
				},
				Spec: capi.PodCertificateRequestSpec{
					SignerName:           "foo.com/abc",
					PodName:              "pod-1",
					PodUID:               types.UID(podUID1),
					ServiceAccountName:   "sa-1",
					ServiceAccountUID:    "sa-uid-1",
					NodeName:             "node-1",
					NodeUID:              "node-uid-1",
					MaxExpirationSeconds: ptr.To[int32](86400),
					PKIXPublicKey:        pubPKIX1,
					ProofOfPossession:    proof1,
				},
				Status: capi.PodCertificateRequestStatus{
					Conditions: []metav1.Condition{
						{
							Type:               capi.PodCertificateRequestConditionTypeFailed,
							Status:             metav1.ConditionTrue,
							Reason:             "Whatever",
							Message:            "Foo message",
							LastTransitionTime: metav1.NewTime(time.Now()),
						},
					},
				},
			},
			newPCR: &capi.PodCertificateRequest{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "foo",
					Name:      "bar",
				},
				Spec: capi.PodCertificateRequestSpec{
					SignerName:           "foo.com/abc",
					PodName:              "pod-1",
					PodUID:               types.UID(podUID1),
					ServiceAccountName:   "sa-1",
					ServiceAccountUID:    "sa-uid-1",
					NodeName:             "node-1",
					NodeUID:              "node-uid-1",
					MaxExpirationSeconds: ptr.To[int32](86400),
					PKIXPublicKey:        pubPKIX1,
					ProofOfPossession:    proof1,
				},
				Status: capi.PodCertificateRequestStatus{
					Conditions: []metav1.Condition{},
				},
			},
			wantErrors: field.ErrorList{
				field.Invalid(field.NewPath("status"), field.OmitValueType{}, `immutable after PodCertificateRequest is issued, denied, or failed`),
			},
		},
		{
			description: "a request cannot change from issued to pending",
			oldPCR: &capi.PodCertificateRequest{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "foo",
					Name:      "bar",
				},
				Spec: capi.PodCertificateRequestSpec{
					SignerName:           "foo.com/abc",
					PodName:              "pod-1",
					PodUID:               types.UID(podUID1),
					ServiceAccountName:   "sa-1",
					ServiceAccountUID:    "sa-uid-1",
					NodeName:             "node-1",
					NodeUID:              "node-uid-1",
					MaxExpirationSeconds: ptr.To[int32](86400),
					PKIXPublicKey:        pubPKIX1,
					ProofOfPossession:    proof1,
				},
				Status: capi.PodCertificateRequestStatus{
					Conditions: []metav1.Condition{
						{
							Type:               capi.PodCertificateRequestConditionTypeIssued,
							Status:             metav1.ConditionTrue,
							Reason:             "Whatever",
							Message:            "Foo message",
							LastTransitionTime: metav1.NewTime(time.Now()),
						},
					},
					CertificateChain: pod1Cert1,
					NotBefore:        ptr.To(metav1.NewTime(mustParseTime(t, "1970-01-01T00:00:00Z"))),
					BeginRefreshAt:   ptr.To(metav1.NewTime(mustParseTime(t, "1970-01-01T12:00:00Z"))),
					NotAfter:         ptr.To(metav1.NewTime(mustParseTime(t, "1970-01-02T00:00:00Z"))),
				},
			},
			newPCR: &capi.PodCertificateRequest{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "foo",
					Name:      "bar",
				},
				Spec: capi.PodCertificateRequestSpec{
					SignerName:           "foo.com/abc",
					PodName:              "pod-1",
					PodUID:               types.UID(podUID1),
					ServiceAccountName:   "sa-1",
					ServiceAccountUID:    "sa-uid-1",
					NodeName:             "node-1",
					NodeUID:              "node-uid-1",
					MaxExpirationSeconds: ptr.To[int32](86400),
					PKIXPublicKey:        pubPKIX1,
					ProofOfPossession:    proof1,
				},
			},
			wantErrors: field.ErrorList{
				field.Invalid(field.NewPath("status"), field.OmitValueType{}, `immutable after PodCertificateRequest is issued, denied, or failed`),
			},
		},
		{
			description: "a request cannot change from issued to Failed",
			oldPCR: &capi.PodCertificateRequest{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "foo",
					Name:      "bar",
				},
				Spec: capi.PodCertificateRequestSpec{
					SignerName:           "foo.com/abc",
					PodName:              "pod-1",
					PodUID:               types.UID(podUID1),
					ServiceAccountName:   "sa-1",
					ServiceAccountUID:    "sa-uid-1",
					NodeName:             "node-1",
					NodeUID:              "node-uid-1",
					MaxExpirationSeconds: ptr.To[int32](86400),
					PKIXPublicKey:        pubPKIX1,
					ProofOfPossession:    proof1,
				},
				Status: capi.PodCertificateRequestStatus{
					Conditions: []metav1.Condition{
						{
							Type:               capi.PodCertificateRequestConditionTypeIssued,
							Status:             metav1.ConditionTrue,
							Reason:             "Whatever",
							Message:            "Foo message",
							LastTransitionTime: metav1.NewTime(time.Now()),
						},
					},
					CertificateChain: pod1Cert1,
					NotBefore:        ptr.To(metav1.NewTime(mustParseTime(t, "1970-01-01T00:00:00Z"))),
					BeginRefreshAt:   ptr.To(metav1.NewTime(mustParseTime(t, "1970-01-01T12:00:00Z"))),
					NotAfter:         ptr.To(metav1.NewTime(mustParseTime(t, "1970-01-02T00:00:00Z"))),
				},
			},
			newPCR: &capi.PodCertificateRequest{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "foo",
					Name:      "bar",
				},
				Spec: capi.PodCertificateRequestSpec{
					SignerName:           "foo.com/abc",
					PodName:              "pod-1",
					PodUID:               types.UID(podUID1),
					ServiceAccountName:   "sa-1",
					ServiceAccountUID:    "sa-uid-1",
					NodeName:             "node-1",
					NodeUID:              "node-uid-1",
					MaxExpirationSeconds: ptr.To[int32](86400),
					PKIXPublicKey:        pubPKIX1,
					ProofOfPossession:    proof1,
				},
				Status: capi.PodCertificateRequestStatus{
					Conditions: []metav1.Condition{
						{
							Type:               capi.PodCertificateRequestConditionTypeFailed,
							Status:             metav1.ConditionTrue,
							Reason:             "Whatever",
							Message:            "Foo message",
							LastTransitionTime: metav1.NewTime(time.Now()),
						},
					},
				},
			},
			wantErrors: field.ErrorList{
				field.Invalid(field.NewPath("status"), field.OmitValueType{}, `immutable after PodCertificateRequest is issued, denied, or failed`),
			},
		},
		{
			description: "a request cannot change from issued to Denied",
			oldPCR: &capi.PodCertificateRequest{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "foo",
					Name:      "bar",
				},
				Spec: capi.PodCertificateRequestSpec{
					SignerName:           "foo.com/abc",
					PodName:              "pod-1",
					PodUID:               types.UID(podUID1),
					ServiceAccountName:   "sa-1",
					ServiceAccountUID:    "sa-uid-1",
					NodeName:             "node-1",
					NodeUID:              "node-uid-1",
					MaxExpirationSeconds: ptr.To[int32](86400),
					PKIXPublicKey:        pubPKIX1,
					ProofOfPossession:    proof1,
				},
				Status: capi.PodCertificateRequestStatus{
					Conditions: []metav1.Condition{
						{
							Type:               capi.PodCertificateRequestConditionTypeIssued,
							Status:             metav1.ConditionTrue,
							Reason:             "Whatever",
							Message:            "Foo message",
							LastTransitionTime: metav1.NewTime(time.Now()),
						},
					},
					CertificateChain: pod1Cert1,
					NotBefore:        ptr.To(metav1.NewTime(mustParseTime(t, "1970-01-01T00:00:00Z"))),
					BeginRefreshAt:   ptr.To(metav1.NewTime(mustParseTime(t, "1970-01-01T12:00:00Z"))),
					NotAfter:         ptr.To(metav1.NewTime(mustParseTime(t, "1970-01-02T00:00:00Z"))),
				},
			},
			newPCR: &capi.PodCertificateRequest{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "foo",
					Name:      "bar",
				},
				Spec: capi.PodCertificateRequestSpec{
					SignerName:           "foo.com/abc",
					PodName:              "pod-1",
					PodUID:               types.UID(podUID1),
					ServiceAccountName:   "sa-1",
					ServiceAccountUID:    "sa-uid-1",
					NodeName:             "node-1",
					NodeUID:              "node-uid-1",
					MaxExpirationSeconds: ptr.To[int32](86400),
					PKIXPublicKey:        pubPKIX1,
					ProofOfPossession:    proof1,
				},
				Status: capi.PodCertificateRequestStatus{
					Conditions: []metav1.Condition{
						{
							Type:               capi.PodCertificateRequestConditionTypeDenied,
							Status:             metav1.ConditionTrue,
							Reason:             "Whatever",
							Message:            "Foo message",
							LastTransitionTime: metav1.NewTime(time.Now()),
						},
					},
				},
			},
			wantErrors: field.ErrorList{
				field.Invalid(field.NewPath("status"), field.OmitValueType{}, `immutable after PodCertificateRequest is issued, denied, or failed`),
			},
		},
		{
			description: "notbefore must be consistent with leaf certificate",
			oldPCR: &capi.PodCertificateRequest{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "foo",
					Name:      "bar",
				},
				Spec: capi.PodCertificateRequestSpec{
					SignerName:           "foo.com/abc",
					PodName:              "pod-1",
					PodUID:               types.UID(podUID1),
					ServiceAccountName:   "sa-1",
					ServiceAccountUID:    "sa-uid-1",
					NodeName:             "node-1",
					NodeUID:              "node-uid-1",
					MaxExpirationSeconds: ptr.To[int32](86400),
					PKIXPublicKey:        pubPKIX1,
					ProofOfPossession:    proof1,
				},
			},
			newPCR: &capi.PodCertificateRequest{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "foo",
					Name:      "bar",
				},
				Spec: capi.PodCertificateRequestSpec{
					SignerName:           "foo.com/abc",
					PodName:              "pod-1",
					PodUID:               types.UID(podUID1),
					ServiceAccountName:   "sa-1",
					ServiceAccountUID:    "sa-uid-1",
					NodeName:             "node-1",
					NodeUID:              "node-uid-1",
					MaxExpirationSeconds: ptr.To[int32](86400),
					PKIXPublicKey:        pubPKIX1,
					ProofOfPossession:    proof1,
				},
				Status: capi.PodCertificateRequestStatus{
					Conditions: []metav1.Condition{
						{
							Type:               capi.PodCertificateRequestConditionTypeIssued,
							Status:             metav1.ConditionTrue,
							Reason:             "Whatever",
							Message:            "Foo message",
							LastTransitionTime: metav1.NewTime(time.Now()),
						},
					},
					CertificateChain: pod1Cert1,
					NotBefore:        ptr.To(metav1.NewTime(mustParseTime(t, "1971-01-01T00:00:00Z"))),
					BeginRefreshAt:   ptr.To(metav1.NewTime(mustParseTime(t, "1970-01-01T12:00:00Z"))),
					NotAfter:         ptr.To(metav1.NewTime(mustParseTime(t, "1970-01-02T00:00:00Z"))),
				},
			},
			wantErrors: field.ErrorList{
				field.Invalid(field.NewPath("status", "notBefore"), mustParseTime(t, "1971-01-01T00:00:00Z"), "must be set to the NotBefore time encoded in the leaf certificate"),
			},
		},
		{
			description: "notAfter must be consistent with leaf certificate",
			oldPCR: &capi.PodCertificateRequest{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "foo",
					Name:      "bar",
				},
				Spec: capi.PodCertificateRequestSpec{
					SignerName:           "foo.com/abc",
					PodName:              "pod-1",
					PodUID:               types.UID(podUID1),
					ServiceAccountName:   "sa-1",
					ServiceAccountUID:    "sa-uid-1",
					NodeName:             "node-1",
					NodeUID:              "node-uid-1",
					MaxExpirationSeconds: ptr.To[int32](86400),
					PKIXPublicKey:        pubPKIX1,
					ProofOfPossession:    proof1,
				},
			},
			newPCR: &capi.PodCertificateRequest{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "foo",
					Name:      "bar",
				},
				Spec: capi.PodCertificateRequestSpec{
					SignerName:           "foo.com/abc",
					PodName:              "pod-1",
					PodUID:               types.UID(podUID1),
					ServiceAccountName:   "sa-1",
					ServiceAccountUID:    "sa-uid-1",
					NodeName:             "node-1",
					NodeUID:              "node-uid-1",
					MaxExpirationSeconds: ptr.To[int32](86400),
					PKIXPublicKey:        pubPKIX1,
					ProofOfPossession:    proof1,
				},
				Status: capi.PodCertificateRequestStatus{
					Conditions: []metav1.Condition{
						{
							Type:               capi.PodCertificateRequestConditionTypeIssued,
							Status:             metav1.ConditionTrue,
							Reason:             "Whatever",
							Message:            "Foo message",
							LastTransitionTime: metav1.NewTime(time.Now()),
						},
					},
					CertificateChain: pod1Cert1,
					NotBefore:        ptr.To(metav1.NewTime(mustParseTime(t, "1970-01-01T00:00:00Z"))),
					BeginRefreshAt:   ptr.To(metav1.NewTime(mustParseTime(t, "1970-01-01T12:00:00Z"))),
					NotAfter:         ptr.To(metav1.NewTime(mustParseTime(t, "1971-01-02T00:00:00Z"))),
				},
			},
			wantErrors: field.ErrorList{
				field.Invalid(field.NewPath("status", "notAfter"), mustParseTime(t, "1971-01-02T00:00:00Z"), "must be set to the NotAfter time encoded in the leaf certificate"),
			},
		},
		{
			// 刷新时间过早会导致客户端拿到即将过期的证书，必须留出至少 10 分钟。
			description: "beginRefreshAt must be >= notBefore + 10 min",
			oldPCR: &capi.PodCertificateRequest{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "foo",
					Name:      "bar",
				},
				Spec: capi.PodCertificateRequestSpec{
					SignerName:           "foo.com/abc",
					PodName:              "pod-1",
					PodUID:               types.UID(podUID1),
					ServiceAccountName:   "sa-1",
					ServiceAccountUID:    "sa-uid-1",
					NodeName:             "node-1",
					NodeUID:              "node-uid-1",
					MaxExpirationSeconds: ptr.To[int32](86400),
					PKIXPublicKey:        pubPKIX1,
					ProofOfPossession:    proof1,
				},
			},
			newPCR: &capi.PodCertificateRequest{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "foo",
					Name:      "bar",
				},
				Spec: capi.PodCertificateRequestSpec{
					SignerName:           "foo.com/abc",
					PodName:              "pod-1",
					PodUID:               types.UID(podUID1),
					ServiceAccountName:   "sa-1",
					ServiceAccountUID:    "sa-uid-1",
					NodeName:             "node-1",
					NodeUID:              "node-uid-1",
					MaxExpirationSeconds: ptr.To[int32](86400),
					PKIXPublicKey:        pubPKIX1,
					ProofOfPossession:    proof1,
				},
				Status: capi.PodCertificateRequestStatus{
					Conditions: []metav1.Condition{
						{
							Type:               capi.PodCertificateRequestConditionTypeIssued,
							Status:             metav1.ConditionTrue,
							Reason:             "Whatever",
							Message:            "Foo message",
							LastTransitionTime: metav1.NewTime(time.Now()),
						},
					},
					CertificateChain: pod1Cert1,
					NotBefore:        ptr.To(metav1.NewTime(mustParseTime(t, "1970-01-01T00:00:00Z"))),
					BeginRefreshAt:   ptr.To(metav1.NewTime(mustParseTime(t, "1970-01-01T00:05:00Z"))),
					NotAfter:         ptr.To(metav1.NewTime(mustParseTime(t, "1970-01-02T00:00:00Z"))),
				},
			},
			wantErrors: field.ErrorList{
				field.Invalid(field.NewPath("status", "beginRefreshAt"), mustParseTime(t, "1970-01-01T00:05:00Z"), "must be at least 10 minutes after status.notBefore"),
			},
		},
		{
			description: "beginRefreshAt must be <= notAfter - 10 min",
			oldPCR: &capi.PodCertificateRequest{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "foo",
					Name:      "bar",
				},
				Spec: capi.PodCertificateRequestSpec{
					SignerName:           "foo.com/abc",
					PodName:              "pod-1",
					PodUID:               types.UID(podUID1),
					ServiceAccountName:   "sa-1",
					ServiceAccountUID:    "sa-uid-1",
					NodeName:             "node-1",
					NodeUID:              "node-uid-1",
					MaxExpirationSeconds: ptr.To[int32](86400),
					PKIXPublicKey:        pubPKIX1,
					ProofOfPossession:    proof1,
				},
			},
			newPCR: &capi.PodCertificateRequest{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "foo",
					Name:      "bar",
				},
				Spec: capi.PodCertificateRequestSpec{
					SignerName:           "foo.com/abc",
					PodName:              "pod-1",
					PodUID:               types.UID(podUID1),
					ServiceAccountName:   "sa-1",
					ServiceAccountUID:    "sa-uid-1",
					NodeName:             "node-1",
					NodeUID:              "node-uid-1",
					MaxExpirationSeconds: ptr.To[int32](86400),
					PKIXPublicKey:        pubPKIX1,
					ProofOfPossession:    proof1,
				},
				Status: capi.PodCertificateRequestStatus{
					Conditions: []metav1.Condition{
						{
							Type:               capi.PodCertificateRequestConditionTypeIssued,
							Status:             metav1.ConditionTrue,
							Reason:             "Whatever",
							Message:            "Foo message",
							LastTransitionTime: metav1.NewTime(time.Now()),
						},
					},
					CertificateChain: pod1Cert1,
					NotBefore:        ptr.To(metav1.NewTime(mustParseTime(t, "1970-01-01T00:00:00Z"))),
					BeginRefreshAt:   ptr.To(metav1.NewTime(mustParseTime(t, "1970-01-01T23:55:00Z"))),
					NotAfter:         ptr.To(metav1.NewTime(mustParseTime(t, "1970-01-02T00:00:00Z"))),
				},
			},
			wantErrors: field.ErrorList{
				field.Invalid(field.NewPath("status", "beginRefreshAt"), mustParseTime(t, "1970-01-01T23:55:00Z"), "must be at least 10 minutes before status.notAfter"),
			},
		},
		{
			description: "timestamps must be set",
			oldPCR: &capi.PodCertificateRequest{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "foo",
					Name:      "bar",
				},
				Spec: capi.PodCertificateRequestSpec{
					SignerName:           "foo.com/abc",
					PodName:              "pod-1",
					PodUID:               types.UID(podUID1),
					ServiceAccountName:   "sa-1",
					ServiceAccountUID:    "sa-uid-1",
					NodeName:             "node-1",
					NodeUID:              "node-uid-1",
					MaxExpirationSeconds: ptr.To[int32](86400),
					PKIXPublicKey:        pubPKIX1,
					ProofOfPossession:    proof1,
				},
			},
			newPCR: &capi.PodCertificateRequest{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "foo",
					Name:      "bar",
				},
				Spec: capi.PodCertificateRequestSpec{
					SignerName:           "foo.com/abc",
					PodName:              "pod-1",
					PodUID:               types.UID(podUID1),
					ServiceAccountName:   "sa-1",
					ServiceAccountUID:    "sa-uid-1",
					NodeName:             "node-1",
					NodeUID:              "node-uid-1",
					MaxExpirationSeconds: ptr.To[int32](86400),
					PKIXPublicKey:        pubPKIX1,
					ProofOfPossession:    proof1,
				},
				Status: capi.PodCertificateRequestStatus{
					Conditions: []metav1.Condition{
						{
							Type:               capi.PodCertificateRequestConditionTypeIssued,
							Status:             metav1.ConditionTrue,
							Reason:             "Whatever",
							Message:            "Foo message",
							LastTransitionTime: metav1.NewTime(time.Now()),
						},
					},
					CertificateChain: pod1Cert1,
				},
			},
			wantErrors: field.ErrorList{
				field.Required(field.NewPath("status", "notBefore"), "must be present and consistent with the issued certificate"),
				field.Required(field.NewPath("status", "notAfter"), "must be present and consistent with the issued certificate"),
				field.Required(field.NewPath("status", "beginRefreshAt"), "must be present and in the range [notbefore+10min, notafter-10min]"),
			},
		},
		{
			description: "certs shorter than one hour are rejected",
			oldPCR: &capi.PodCertificateRequest{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "foo",
					Name:      "bar",
				},
				Spec: capi.PodCertificateRequestSpec{
					SignerName:           "foo.com/abc",
					PodName:              "pod-1",
					PodUID:               types.UID(podUID1),
					ServiceAccountName:   "sa-1",
					ServiceAccountUID:    "sa-uid-1",
					NodeName:             "node-1",
					NodeUID:              "node-uid-1",
					MaxExpirationSeconds: ptr.To[int32](86400),
					PKIXPublicKey:        pubPKIX1,
					ProofOfPossession:    proof1,
				},
			},
			newPCR: &capi.PodCertificateRequest{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "foo",
					Name:      "bar",
				},
				Spec: capi.PodCertificateRequestSpec{
					SignerName:           "foo.com/abc",
					PodName:              "pod-1",
					PodUID:               types.UID(podUID1),
					ServiceAccountName:   "sa-1",
					ServiceAccountUID:    "sa-uid-1",
					NodeName:             "node-1",
					NodeUID:              "node-uid-1",
					MaxExpirationSeconds: ptr.To[int32](86400),
					PKIXPublicKey:        pubPKIX1,
					ProofOfPossession:    proof1,
				},
				Status: capi.PodCertificateRequestStatus{
					Conditions: []metav1.Condition{
						{
							Type:               capi.PodCertificateRequestConditionTypeIssued,
							Status:             metav1.ConditionTrue,
							Reason:             "Whatever",
							Message:            "Foo message",
							LastTransitionTime: metav1.NewTime(time.Now()),
						},
					},
					CertificateChain: badCertTooShort,
					NotBefore:        ptr.To(metav1.NewTime(mustParseTime(t, "1970-01-01T00:00:00Z"))),
					BeginRefreshAt:   ptr.To(metav1.NewTime(mustParseTime(t, "1970-01-01T00:25:00Z"))),
					NotAfter:         ptr.To(metav1.NewTime(mustParseTime(t, "1970-01-01T00:50:00Z"))),
				},
			},
			wantErrors: field.ErrorList{
				field.Invalid(field.NewPath("status", "certificateChain"), 50*time.Minute, "leaf certificate lifetime must be >= 1 hour"),
			},
		},
		{
			description: "certs longer than maxExpirationSeconds are rejected",
			oldPCR: &capi.PodCertificateRequest{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "foo",
					Name:      "bar",
				},
				Spec: capi.PodCertificateRequestSpec{
					SignerName:           "foo.com/abc",
					PodName:              "pod-1",
					PodUID:               types.UID(podUID1),
					ServiceAccountName:   "sa-1",
					ServiceAccountUID:    "sa-uid-1",
					NodeName:             "node-1",
					NodeUID:              "node-uid-1",
					MaxExpirationSeconds: ptr.To[int32](86400),
					PKIXPublicKey:        pubPKIX1,
					ProofOfPossession:    proof1,
				},
			},
			newPCR: &capi.PodCertificateRequest{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "foo",
					Name:      "bar",
				},
				Spec: capi.PodCertificateRequestSpec{
					SignerName:           "foo.com/abc",
					PodName:              "pod-1",
					PodUID:               types.UID(podUID1),
					ServiceAccountName:   "sa-1",
					ServiceAccountUID:    "sa-uid-1",
					NodeName:             "node-1",
					NodeUID:              "node-uid-1",
					MaxExpirationSeconds: ptr.To[int32](86400),
					PKIXPublicKey:        pubPKIX1,
					ProofOfPossession:    proof1,
				},
				Status: capi.PodCertificateRequestStatus{
					Conditions: []metav1.Condition{
						{
							Type:               capi.PodCertificateRequestConditionTypeIssued,
							Status:             metav1.ConditionTrue,
							Reason:             "Whatever",
							Message:            "Foo message",
							LastTransitionTime: metav1.NewTime(time.Now()),
						},
					},
					CertificateChain: badCertTooLong,
					NotBefore:        ptr.To(metav1.NewTime(mustParseTime(t, "1970-01-01T00:00:00Z"))),
					BeginRefreshAt:   ptr.To(metav1.NewTime(mustParseTime(t, "1970-01-01T12:00:00Z"))),
					NotAfter:         ptr.To(metav1.NewTime(mustParseTime(t, "1970-01-02T01:00:00Z"))),
				},
			},
			wantErrors: field.ErrorList{
				field.Invalid(field.NewPath("status", "certificateChain"), 25*time.Hour, "leaf certificate lifetime must be <= spec.maxExpirationSeconds (86400)"),
			},
		},
		{
			description: "leaf cert can not contain empty DNSName",
			oldPCR: &capi.PodCertificateRequest{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "foo",
					Name:      "bar",
				},
				Spec: capi.PodCertificateRequestSpec{
					SignerName:           "foo.com/abc",
					PodName:              "pod-1",
					PodUID:               types.UID(podUID1),
					ServiceAccountName:   "sa-1",
					ServiceAccountUID:    "sa-uid-1",
					NodeName:             "node-1",
					NodeUID:              "node-uid-1",
					MaxExpirationSeconds: ptr.To[int32](86400),
					PKIXPublicKey:        pubPKIX1,
					ProofOfPossession:    proof1,
				},
			},
			newPCR: &capi.PodCertificateRequest{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "foo",
					Name:      "bar",
				},
				Spec: capi.PodCertificateRequestSpec{
					SignerName:           "foo.com/abc",
					PodName:              "pod-1",
					PodUID:               types.UID(podUID1),
					ServiceAccountName:   "sa-1",
					ServiceAccountUID:    "sa-uid-1",
					NodeName:             "node-1",
					NodeUID:              "node-uid-1",
					MaxExpirationSeconds: ptr.To[int32](86400),
					PKIXPublicKey:        pubPKIX1,
					ProofOfPossession:    proof1,
				},
				Status: capi.PodCertificateRequestStatus{
					Conditions: []metav1.Condition{
						{
							Type:               capi.PodCertificateRequestConditionTypeIssued,
							Status:             metav1.ConditionTrue,
							Reason:             "Whatever",
							Message:            "Foo message",
							LastTransitionTime: metav1.NewTime(time.Now()),
						},
					},
					CertificateChain: certWithBadDNSName1,
					NotBefore:        ptr.To(metav1.NewTime(mustParseTime(t, "1970-01-01T00:00:00Z"))),
					BeginRefreshAt:   ptr.To(metav1.NewTime(mustParseTime(t, "1970-01-01T12:00:00Z"))),
					NotAfter:         ptr.To(metav1.NewTime(mustParseTime(t, "1970-01-02T00:00:00Z"))),
				},
			},
			wantErrors: field.ErrorList{
				field.Invalid(field.NewPath("status", "certificateChain"), "", "leaf certificate should not contain empty DNSName"),
			},
		},
		{
			description: "leaf cert can not contain DNSName contains '..'",
			oldPCR: &capi.PodCertificateRequest{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "foo",
					Name:      "bar",
				},
				Spec: capi.PodCertificateRequestSpec{
					SignerName:           "foo.com/abc",
					PodName:              "pod-1",
					PodUID:               types.UID(podUID1),
					ServiceAccountName:   "sa-1",
					ServiceAccountUID:    "sa-uid-1",
					NodeName:             "node-1",
					NodeUID:              "node-uid-1",
					MaxExpirationSeconds: ptr.To[int32](86400),
					PKIXPublicKey:        pubPKIX1,
					ProofOfPossession:    proof1,
				},
			},
			newPCR: &capi.PodCertificateRequest{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "foo",
					Name:      "bar",
				},
				Spec: capi.PodCertificateRequestSpec{
					SignerName:           "foo.com/abc",
					PodName:              "pod-1",
					PodUID:               types.UID(podUID1),
					ServiceAccountName:   "sa-1",
					ServiceAccountUID:    "sa-uid-1",
					NodeName:             "node-1",
					NodeUID:              "node-uid-1",
					MaxExpirationSeconds: ptr.To[int32](86400),
					PKIXPublicKey:        pubPKIX1,
					ProofOfPossession:    proof1,
				},
				Status: capi.PodCertificateRequestStatus{
					Conditions: []metav1.Condition{
						{
							Type:               capi.PodCertificateRequestConditionTypeIssued,
							Status:             metav1.ConditionTrue,
							Reason:             "Whatever",
							Message:            "Foo message",
							LastTransitionTime: metav1.NewTime(time.Now()),
						},
					},
					CertificateChain: certWithBadDNSName2,
					NotBefore:        ptr.To(metav1.NewTime(mustParseTime(t, "1970-01-01T00:00:00Z"))),
					BeginRefreshAt:   ptr.To(metav1.NewTime(mustParseTime(t, "1970-01-01T12:00:00Z"))),
					NotAfter:         ptr.To(metav1.NewTime(mustParseTime(t, "1970-01-02T00:00:00Z"))),
				},
			},
			wantErrors: field.ErrorList{
				field.Invalid(field.NewPath("status", "certificateChain"), "test-name..example", "leaf certificate's DNSName should not contain '..'"),
			},
		},
		{
			description: "leaf cert can not contain DNSName start with '.'",
			oldPCR: &capi.PodCertificateRequest{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "foo",
					Name:      "bar",
				},
				Spec: capi.PodCertificateRequestSpec{
					SignerName:           "foo.com/abc",
					PodName:              "pod-1",
					PodUID:               types.UID(podUID1),
					ServiceAccountName:   "sa-1",
					ServiceAccountUID:    "sa-uid-1",
					NodeName:             "node-1",
					NodeUID:              "node-uid-1",
					MaxExpirationSeconds: ptr.To[int32](86400),
					PKIXPublicKey:        pubPKIX1,
					ProofOfPossession:    proof1,
				},
			},
			newPCR: &capi.PodCertificateRequest{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "foo",
					Name:      "bar",
				},
				Spec: capi.PodCertificateRequestSpec{
					SignerName:           "foo.com/abc",
					PodName:              "pod-1",
					PodUID:               types.UID(podUID1),
					ServiceAccountName:   "sa-1",
					ServiceAccountUID:    "sa-uid-1",
					NodeName:             "node-1",
					NodeUID:              "node-uid-1",
					MaxExpirationSeconds: ptr.To[int32](86400),
					PKIXPublicKey:        pubPKIX1,
					ProofOfPossession:    proof1,
				},
				Status: capi.PodCertificateRequestStatus{
					Conditions: []metav1.Condition{
						{
							Type:               capi.PodCertificateRequestConditionTypeIssued,
							Status:             metav1.ConditionTrue,
							Reason:             "Whatever",
							Message:            "Foo message",
							LastTransitionTime: metav1.NewTime(time.Now()),
						},
					},
					CertificateChain: certWithBadDNSName3,
					NotBefore:        ptr.To(metav1.NewTime(mustParseTime(t, "1970-01-01T00:00:00Z"))),
					BeginRefreshAt:   ptr.To(metav1.NewTime(mustParseTime(t, "1970-01-01T12:00:00Z"))),
					NotAfter:         ptr.To(metav1.NewTime(mustParseTime(t, "1970-01-02T00:00:00Z"))),
				},
			},
			wantErrors: field.ErrorList{
				field.Invalid(field.NewPath("status", "certificateChain"), ".example", "leaf certificate's DNSName should not start or end with '.'"),
			},
		},
		{
			description: "leaf cert can not contain DNSName end with '.'",
			oldPCR: &capi.PodCertificateRequest{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "foo",
					Name:      "bar",
				},
				Spec: capi.PodCertificateRequestSpec{
					SignerName:           "foo.com/abc",
					PodName:              "pod-1",
					PodUID:               types.UID(podUID1),
					ServiceAccountName:   "sa-1",
					ServiceAccountUID:    "sa-uid-1",
					NodeName:             "node-1",
					NodeUID:              "node-uid-1",
					MaxExpirationSeconds: ptr.To[int32](86400),
					PKIXPublicKey:        pubPKIX1,
					ProofOfPossession:    proof1,
				},
			},
			newPCR: &capi.PodCertificateRequest{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "foo",
					Name:      "bar",
				},
				Spec: capi.PodCertificateRequestSpec{
					SignerName:           "foo.com/abc",
					PodName:              "pod-1",
					PodUID:               types.UID(podUID1),
					ServiceAccountName:   "sa-1",
					ServiceAccountUID:    "sa-uid-1",
					NodeName:             "node-1",
					NodeUID:              "node-uid-1",
					MaxExpirationSeconds: ptr.To[int32](86400),
					PKIXPublicKey:        pubPKIX1,
					ProofOfPossession:    proof1,
				},
				Status: capi.PodCertificateRequestStatus{
					Conditions: []metav1.Condition{
						{
							Type:               capi.PodCertificateRequestConditionTypeIssued,
							Status:             metav1.ConditionTrue,
							Reason:             "Whatever",
							Message:            "Foo message",
							LastTransitionTime: metav1.NewTime(time.Now()),
						},
					},
					CertificateChain: certWithBadDNSName4,
					NotBefore:        ptr.To(metav1.NewTime(mustParseTime(t, "1970-01-01T00:00:00Z"))),
					BeginRefreshAt:   ptr.To(metav1.NewTime(mustParseTime(t, "1970-01-01T12:00:00Z"))),
					NotAfter:         ptr.To(metav1.NewTime(mustParseTime(t, "1970-01-02T00:00:00Z"))),
				},
			},
			wantErrors: field.ErrorList{
				field.Invalid(field.NewPath("status", "certificateChain"), "example.", "leaf certificate's DNSName should not start or end with '.'"),
			},
		},
		{
			description: "leaf cert can not contain bad email address",
			oldPCR: &capi.PodCertificateRequest{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "foo",
					Name:      "bar",
				},
				Spec: capi.PodCertificateRequestSpec{
					SignerName:           "foo.com/abc",
					PodName:              "pod-1",
					PodUID:               types.UID(podUID1),
					ServiceAccountName:   "sa-1",
					ServiceAccountUID:    "sa-uid-1",
					NodeName:             "node-1",
					NodeUID:              "node-uid-1",
					MaxExpirationSeconds: ptr.To[int32](86400),
					PKIXPublicKey:        pubPKIX1,
					ProofOfPossession:    proof1,
				},
			},
			newPCR: &capi.PodCertificateRequest{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "foo",
					Name:      "bar",
				},
				Spec: capi.PodCertificateRequestSpec{
					SignerName:           "foo.com/abc",
					PodName:              "pod-1",
					PodUID:               types.UID(podUID1),
					ServiceAccountName:   "sa-1",
					ServiceAccountUID:    "sa-uid-1",
					NodeName:             "node-1",
					NodeUID:              "node-uid-1",
					MaxExpirationSeconds: ptr.To[int32](86400),
					PKIXPublicKey:        pubPKIX1,
					ProofOfPossession:    proof1,
				},
				Status: capi.PodCertificateRequestStatus{
					Conditions: []metav1.Condition{
						{
							Type:               capi.PodCertificateRequestConditionTypeIssued,
							Status:             metav1.ConditionTrue,
							Reason:             "Whatever",
							Message:            "Foo message",
							LastTransitionTime: metav1.NewTime(time.Now()),
						},
					},
					CertificateChain: certWithBadEmailAddress,
					NotBefore:        ptr.To(metav1.NewTime(mustParseTime(t, "1970-01-01T00:00:00Z"))),
					BeginRefreshAt:   ptr.To(metav1.NewTime(mustParseTime(t, "1970-01-01T12:00:00Z"))),
					NotAfter:         ptr.To(metav1.NewTime(mustParseTime(t, "1970-01-02T00:00:00Z"))),
				},
			},
			wantErrors: field.ErrorList{
				field.Invalid(field.NewPath("status", "certificateChain"), "email@@address", "leaf certificate should not contain invalid EmailAddress"),
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			tc.oldPCR.ObjectMeta.ResourceVersion = "1"
			tc.newPCR.ObjectMeta.ResourceVersion = "2"

			gotErrors := ValidatePodCertificateRequestStatusUpdate(tc.newPCR, tc.oldPCR, testclock.NewFakeClock(mustParseTime(t, "1970-01-01T00:00:00Z")))
			if diff := cmp.Diff(gotErrors, tc.wantErrors); diff != "" {
				t.Errorf("Unexpected error output from ValidatePodCertificateRequestUpdate; diff (-got +want)\n%s", diff)
			}
		})
	}
}

// mustMakeCA 生成测试用的 Ed25519 根 CA 证书及私钥，有效期固定，便于精确断言证书时间约束。
func mustMakeCA(t *testing.T) ([]byte, ed25519.PrivateKey) {
	signPub, signPriv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("Error while generating CA signing key: %v", err)
	}

	caCertTemplate := &x509.Certificate{
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		NotBefore:             mustParseTime(t, "1970-01-01T00:00:00Z"),
		NotAfter:              mustParseTime(t, "1971-01-01T00:00:00Z"),
	}

	caCertDER, err := x509.CreateCertificate(rand.Reader, caCertTemplate, caCertTemplate, signPub, signPriv)
	if err != nil {
		t.Fatalf("Error while creating CA certificate: %v", err)
	}

	return caCertDER, signPriv
}

// mustMakeIntermediateCA 用根 CA 签发一张中间 CA，供测试验证多级证书链被正确接受。
func mustMakeIntermediateCA(t *testing.T, rootDER []byte, rootPrivateKey crypto.PrivateKey) ([]byte, ed25519.PrivateKey) {
	intermediatePub, intermediatePriv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("Error while generating intermediate signing key: %v", err)
	}

	intermediateCertTemplate := &x509.Certificate{
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		NotBefore:             mustParseTime(t, "1970-01-01T00:00:00Z"),
		NotAfter:              mustParseTime(t, "1971-01-01T00:00:00Z"),
	}

	rootCert, err := x509.ParseCertificate(rootDER)
	if err != nil {
		t.Fatalf("Error while parsing root certificate: %v", err)
	}

	intermediateCertDER, err := x509.CreateCertificate(rand.Reader, intermediateCertTemplate, rootCert, intermediatePub, rootPrivateKey)
	if err != nil {
		t.Fatalf("Error while creating intermediate certificate: %v", err)
	}

	return intermediateCertDER, intermediatePriv
}

// mustParseTime 解析 RFC3339 时间戳，解析失败直接 Fatal：测试中的时间都是硬编码常量。
func mustParseTime(t *testing.T, stamp string) time.Time {
	got, err := time.Parse(time.RFC3339, stamp)
	if err != nil {
		t.Fatalf("Error while parsing timestamp: %v", err)
	}
	return got
}

// mustMakeEd25519KeyAndProof 生成 Ed25519 密钥对、PKIX 公钥、持有证明签名以及 PKCS#10 请求，
// 签名内容为传入的待签消息（通常是 Pod UID），便于验证签名与身份的绑定关系。
func mustMakeEd25519KeyAndProof(t *testing.T, toBeSigned []byte, pkcs10DNSSANS []string) (ed25519.PrivateKey, ed25519.PublicKey, []byte, []byte, []byte) {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("Error while generating ed25519 key: %v", err)
	}
	pubPKIX, err := x509.MarshalPKIXPublicKey(pub)
	if err != nil {
		t.Fatalf("Error while marshaling PKIX public key: %v", err)
	}
	sig := ed25519.Sign(priv, toBeSigned)

	pkcs10DER, err := x509.CreateCertificateRequest(rand.Reader, &x509.CertificateRequest{DNSNames: pkcs10DNSSANS}, priv)
	if err != nil {
		t.Fatalf("Error while creating PKCS#10 certificate signing request: %v", err)
	}

	return priv, pub, pubPKIX, sig, pkcs10DER
}

// mustMakeECDSAKeyAndProof 生成指定曲线的 ECDSA 密钥与持有证明；
// 签名前先哈希待签消息，与生产代码的验签流程保持一致。
func mustMakeECDSAKeyAndProof(t *testing.T, curve elliptic.Curve, toBeSigned []byte) (*ecdsa.PrivateKey, *ecdsa.PublicKey, []byte, []byte) {
	priv, err := ecdsa.GenerateKey(curve, rand.Reader)
	if err != nil {
		t.Fatalf("Error while generating ECDSA key: %v", err)
	}
	pubPKIX, err := x509.MarshalPKIXPublicKey(priv.Public())
	if err != nil {
		t.Fatalf("Error while marshaling PKIX public key: %v", err)
	}
	sig, err := ecdsa.SignASN1(rand.Reader, priv, hashBytes(toBeSigned))
	if err != nil {
		t.Fatalf("Error while making proof of possession: %v", err)
	}
	return priv, &priv.PublicKey, pubPKIX, sig
}

// mustMakeRSAKeyAndProof 生成指定模长的 RSA 密钥与 PSS 签名，
// 用于覆盖 RSA 模长下限（3072/4096）的允许与拒绝场景。
func mustMakeRSAKeyAndProof(t *testing.T, modulusSize int, toBeSigned []byte) (*rsa.PrivateKey, *rsa.PublicKey, []byte, []byte) {
	priv, err := rsa.GenerateKey(rand.Reader, modulusSize)
	if err != nil {
		t.Fatalf("Error while generating RSA key: %v", err)
	}
	pubPKIX, err := x509.MarshalPKIXPublicKey(&priv.PublicKey)
	if err != nil {
		t.Fatalf("Error while marshaling public key: %v", err)
	}
	sig, err := rsa.SignPSS(rand.Reader, priv, crypto.SHA256, hashBytes(toBeSigned), nil)
	if err != nil {
		t.Fatalf("Error while making proof of possession: %v", err)
	}
	return priv, &priv.PublicKey, pubPKIX, sig
}

// mustSignCertForPublicKey 用给定 CA 签发一张叶证书，
// 可按需注入非法 DNS 名或邮箱地址，以便测试证书链内容校验而不必手写 ASN.1。
func mustSignCertForPublicKey(t *testing.T, validity time.Duration, subjectPublicKey crypto.PublicKey, caCertDER []byte, caPrivateKey crypto.PrivateKey, usebadDNSName bool, badDNSName, badEmailAddress string) string {
	certTemplate := &x509.Certificate{
		Subject: pkix.Name{
			CommonName: "foo",
		},
		KeyUsage:    x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
		NotBefore:   mustParseTime(t, "1970-01-01T00:00:00Z"),
		NotAfter:    mustParseTime(t, "1970-01-01T00:00:00Z").Add(validity),
	}
	// 只有需要触发 DNS 校验失败的用例才写入 DNS 名称，其他证书保持干净。
	if usebadDNSName {
		certTemplate.DNSNames = append(certTemplate.DNSNames, badDNSName)
	}
	if badEmailAddress != "" {
		certTemplate.EmailAddresses = append(certTemplate.EmailAddresses, badEmailAddress)
	}

	caCert, err := x509.ParseCertificate(caCertDER)
	if err != nil {
		t.Fatalf("Error while parsing CA certificate: %v", err)
	}

	certDER, err := x509.CreateCertificate(rand.Reader, certTemplate, caCert, subjectPublicKey, caPrivateKey)
	if err != nil {
		t.Fatalf("Error while signing subject certificate: %v", err)
	}

	certPEM := pem.EncodeToMemory(&pem.Block{
		Type:  "CERTIFICATE",
		Bytes: certDER,
	})

	return string(certPEM)
}

// pemEncode 把 DER 数据编码为指定类型的 PEM 字符串，简化非法 PEM 用例的构造。
func pemEncode(blockType string, data []byte) string {
	return string(pem.EncodeToMemory(&pem.Block{
		Type:  blockType,
		Bytes: data,
	}))
}

var (
	validCertificate = []byte(`
Leading non-PEM content
-----BEGIN CERTIFICATE-----
MIIBqDCCAU2gAwIBAgIUfbqeieihh/oERbfvRm38XvS/xHAwCgYIKoZIzj0EAwIw
GjEYMBYGA1UEAxMPSW50ZXJtZWRpYXRlLUNBMCAXDTE2MTAxMTA1MDYwMFoYDzIx
MTYwOTE3MDUwNjAwWjAUMRIwEAYDVQQDEwlNeSBDbGllbnQwWTATBgcqhkjOPQIB
BggqhkjOPQMBBwNCAARv6N4R/sjMR65iMFGNLN1GC/vd7WhDW6J4X/iAjkRLLnNb
KbRG/AtOUZ+7upJ3BWIRKYbOabbQGQe2BbKFiap4o3UwczAOBgNVHQ8BAf8EBAMC
BaAwEwYDVR0lBAwwCgYIKwYBBQUHAwIwDAYDVR0TAQH/BAIwADAdBgNVHQ4EFgQU
K/pZOWpNcYai6eHFpmJEeFpeQlEwHwYDVR0jBBgwFoAUX6nQlxjfWnP6aM1meO/Q
a6b3a9kwCgYIKoZIzj0EAwIDSQAwRgIhAIWTKw/sjJITqeuNzJDAKU4xo1zL+xJ5
MnVCuBwfwDXCAiEAw/1TA+CjPq9JC5ek1ifR0FybTURjeQqYkKpve1dveps=
-----END CERTIFICATE-----
Intermediate non-PEM content
-----BEGIN CERTIFICATE-----
MIIBqDCCAU6gAwIBAgIUfqZtjoFgczZ+oQZbEC/BDSS2J6wwCgYIKoZIzj0EAwIw
EjEQMA4GA1UEAxMHUm9vdC1DQTAgFw0xNjEwMTEwNTA2MDBaGA8yMTE2MDkxNzA1
MDYwMFowGjEYMBYGA1UEAxMPSW50ZXJtZWRpYXRlLUNBMFkwEwYHKoZIzj0CAQYI
KoZIzj0DAQcDQgAEyWHEMMCctJg8Xa5YWLqaCPbk3MjB+uvXac42JM9pj4k9jedD
kpUJRkWIPzgJI8Zk/3cSzluUTixP6JBSDKtwwaN4MHYwDgYDVR0PAQH/BAQDAgGm
MBMGA1UdJQQMMAoGCCsGAQUFBwMCMA8GA1UdEwEB/wQFMAMBAf8wHQYDVR0OBBYE
FF+p0JcY31pz+mjNZnjv0Gum92vZMB8GA1UdIwQYMBaAFB7P6+i4/pfNjqZgJv/b
dgA7Fe4tMAoGCCqGSM49BAMCA0gAMEUCIQCTT1YWQZaAqfQ2oBxzOkJE2BqLFxhz
3smQlrZ5gCHddwIgcvT7puhYOzAgcvMn9+SZ1JOyZ7edODjshCVCRnuHK2c=
-----END CERTIFICATE-----
Trailing non-PEM content
`)

	invalidCertificateNoPEM = []byte(`no PEM content`)

	invalidCertificateNonCertificatePEM = []byte(`
Leading non-PEM content
-----BEGIN CERTIFICATE1-----
MIIBqDCCAU2gAwIBAgIUfbqeieihh/oERbfvRm38XvS/xHAwCgYIKoZIzj0EAwIw
GjEYMBYGA1UEAxMPSW50ZXJtZWRpYXRlLUNBMCAXDTE2MTAxMTA1MDYwMFoYDzIx
MTYwOTE3MDUwNjAwWjAUMRIwEAYDVQQDEwlNeSBDbGllbnQwWTATBgcqhkjOPQIB
BggqhkjOPQMBBwNCAARv6N4R/sjMR65iMFGNLN1GC/vd7WhDW6J4X/iAjkRLLnNb
KbRG/AtOUZ+7upJ3BWIRKYbOabbQGQe2BbKFiap4o3UwczAOBgNVHQ8BAf8EBAMC
BaAwEwYDVR0lBAwwCgYIKwYBBQUHAwIwDAYDVR0TAQH/BAIwADAdBgNVHQ4EFgQU
K/pZOWpNcYai6eHFpmJEeFpeQlEwHwYDVR0jBBgwFoAUX6nQlxjfWnP6aM1meO/Q
a6b3a9kwCgYIKoZIzj0EAwIDSQAwRgIhAIWTKw/sjJITqeuNzJDAKU4xo1zL+xJ5
MnVCuBwfwDXCAiEAw/1TA+CjPq9JC5ek1ifR0FybTURjeQqYkKpve1dveps=
-----END CERTIFICATE1-----
Trailing non-PEM content
`)

	invalidCertificatePEMHeaders = []byte(`
Leading non-PEM content
-----BEGIN CERTIFICATE-----
Some-Header: Some-Value
MIIBqDCCAU2gAwIBAgIUfbqeieihh/oERbfvRm38XvS/xHAwCgYIKoZIzj0EAwIw
GjEYMBYGA1UEAxMPSW50ZXJtZWRpYXRlLUNBMCAXDTE2MTAxMTA1MDYwMFoYDzIx
MTYwOTE3MDUwNjAwWjAUMRIwEAYDVQQDEwlNeSBDbGllbnQwWTATBgcqhkjOPQIB
BggqhkjOPQMBBwNCAARv6N4R/sjMR65iMFGNLN1GC/vd7WhDW6J4X/iAjkRLLnNb
KbRG/AtOUZ+7upJ3BWIRKYbOabbQGQe2BbKFiap4o3UwczAOBgNVHQ8BAf8EBAMC
BaAwEwYDVR0lBAwwCgYIKwYBBQUHAwIwDAYDVR0TAQH/BAIwADAdBgNVHQ4EFgQU
K/pZOWpNcYai6eHFpmJEeFpeQlEwHwYDVR0jBBgwFoAUX6nQlxjfWnP6aM1meO/Q
a6b3a9kwCgYIKoZIzj0EAwIDSQAwRgIhAIWTKw/sjJITqeuNzJDAKU4xo1zL+xJ5
MnVCuBwfwDXCAiEAw/1TA+CjPq9JC5ek1ifR0FybTURjeQqYkKpve1dveps=
-----END CERTIFICATE-----
Trailing non-PEM content
`)

	invalidCertificateNonBase64PEM = []byte(`
Leading non-PEM content
-----BEGIN CERTIFICATE-----
MIIBqDCCAU2gAwIBAgIUfbqeieihh/oERbfvRm38XvS/xHAwCgYIKoZIzj0EAwIw
GjEYMBYGA1UEAxMPSW50ZXJtZWRpYXRlLUNBMCAXDTE2MTAxMTA1MDYwMFoYDzIx
MTYwOTE3MDUwNjAwWjAUMRIwEAYDVQQDEwlNeSBDbGllbnQwWTATBgcqhkjOPQIB
BggqhkjOPQMBBwNCAARv6N4R/sjMR65iMFGNLN1GC/vd7WhDW6J4X/iAjkRLLnNb
KbRG/AtOUZ+7upJ3BWIRKYbOabbQGQe2BbKFiap4o3UwczAOBgNVHQ8BAf8EBAMC
BaAwEwYDVR0lBAwwCgYIKwYBBQUHAwIwDAYDVR0TAQH/BAIwADAdBgNVHQ4EFgQU
K/pZOWpNcYai6eHFpmJEeFpeQlEwHwYDVR0jBBgwFoAUX6nQlxjfWnP6aM1meO/Q
a6b3a9kwCgYIKoZIzj0EAwIDSQAwRgIhAIWTKw/sjJITqeuNzJDAKU4xo1zL+xJ5
MnVCuBwfwDXCAiEAw/1TA+CjPq9JC5ek1ifR0FybTURjeQqYkKpve1d?????????
-----END CERTIFICATE-----
Trailing non-PEM content
`)

	invalidCertificateEmptyPEM = []byte(`
Leading non-PEM content
-----BEGIN CERTIFICATE-----
-----END CERTIFICATE-----
Trailing non-PEM content
`)

	// 第一个字符被改坏，DER/ASN.1 解析必然失败，用于触发证书内容解析错误。
	// 第一个字符被改坏，DER/ASN.1 解析必然失败，用于触发证书内容解析错误。
	invalidCertificateNonASN1Data = []byte(`
Leading non-PEM content
-----BEGIN CERTIFICATE-----
MIIBqDCCAU2gAwIBAgIUfbqeieihh/oERbfvRm38XvS/xHAwCgYIKoZIzj0EAwIw
GjEYMBYGA1UEAxMPSW50ZXJtZWRpYXRlLUNBMCAXDTE2MTAxMTA1MDYwMFoYDzIx
MTYwOTE3MDUwNjAwWjAUNRIwEAYDVQQDEwlNeSBDbGllbnQwWTATBgcqhkjOPQIB
BggqhkjOPQMBBwNCAARv6N4R/sjMR65iMFGNLN1GC/vd7WhDW6J4X/iAjkRLLnNb
KbRG/AtOUZ+7upJ3BWIRKYbOabbQGQe2BbKFiap4o3UwczAOBgNVHQ8BAf8EBAMC
BaAwEwYDVR0lBAwwCgYIKwYBBQUHAwIwDAYDVR0TAQH/BAIwADAdBgNVHQ4EFgQU
K/pZOWpNcYai6eHFpmJEeFpeQlEwHwYDVR0jBBgwFoAUX6nQlxjfWnP6aM1meO/Q
a6b3a9kwCgYIKoZIzj0EAwIDSQAwRgIhAIWTKw/sjJITqeuNzJDAKU4xo1zL+xJ5
MnVCuBwfwDXCAiEAw/1TA+CjPq9JC5ek1ifR0FybTURjeQqYkKpve1dveps=
-----END CERTIFICATE-----
Trailing non-PEM content
`)
)
