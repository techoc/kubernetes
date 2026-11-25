/*
Copyright 2025 The Kubernetes Authors.

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

package rest

import (
	"bytes"
	"context"
	"fmt"
	"reflect"
	"regexp"
	"slices"
	"strings"
	"testing"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/operation"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/conversion"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/apimachinery/pkg/util/version"
	genericapirequest "k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/apiserver/pkg/features"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/apiserver/pkg/validation"
	featuregatetesting "k8s.io/component-base/featuregate/testing"
	"k8s.io/component-base/metrics/legacyregistry"
	"k8s.io/component-base/metrics/testutil"
	"k8s.io/klog/v2"
)

// TestValidateDeclaratively 端到端验证声明式校验的接入：对象先转换为外部版本，
// 再调用 scheme 注册的 ValidateFunc。
// 覆盖 create/update 的不可变检查、子资源校验、未知子资源、校验选项，
// 以及非法子资源路径的错误处理。
func TestValidateDeclaratively(t *testing.T) {
	valid := &Pod{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "v1",
			Kind:       "Pod",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: "test",
		},
	}

	invalidRestartPolicy := &Pod{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "v1",
			Kind:       "Pod",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: "test",
		},
		RestartPolicy: "INVALID",
	}

	invalidRestartPolicyErr := field.Invalid(field.NewPath("spec", "restartPolicy"), "", "Invalid value").WithOrigin("invalid-test")
	mutatedRestartPolicyErr := field.Invalid(field.NewPath("spec", "restartPolicy"), "", "Immutable field").WithOrigin("immutable-test")
	invalidStatusErr := field.Invalid(field.NewPath("status", "conditions"), "", "Invalid condition").WithOrigin("invalid-condition")
	invalidIfOptionErr := field.Invalid(field.NewPath("spec", "restartPolicy"), "", "Invalid when option is set").WithOrigin("invalid-when-option-set")
	invalidSubresourceErr := field.InternalError(nil, fmt.Errorf("unexpected error parsing subresource path: %w", fmt.Errorf("invalid subresource path: %s", "invalid/status")))

	testCases := []struct {
		name        string
		object      runtime.Object
		oldObject   runtime.Object
		subresource string
		options     map[string]bool
		expected    field.ErrorList
	}{
		{
			name:     "create",
			object:   invalidRestartPolicy,
			expected: field.ErrorList{invalidRestartPolicyErr},
		},
		{
			name:      "update",
			object:    invalidRestartPolicy,
			oldObject: valid,
			expected:  field.ErrorList{invalidRestartPolicyErr, mutatedRestartPolicyErr},
		},
		{
			name:        "update subresource with declarative validation",
			subresource: "status",
			object:      valid,
			oldObject:   valid,
			expected:    field.ErrorList{invalidStatusErr},
		},
		{
			name:        "update subresource without declarative validation",
			subresource: "scale",
			object:      valid,
			oldObject:   valid,
			expected:    field.ErrorList{}, // Expect no errors if there is no registered validation
		},
		{
			name:        "invalid subresource",
			subresource: "/invalid/status",
			object:      valid,
			oldObject:   valid,
			expected:    field.ErrorList{invalidSubresourceErr},
		},
		{
			name:     "update with option",
			options:  map[string]bool{"option1": true},
			object:   valid,
			expected: field.ErrorList{invalidIfOptionErr},
		},
	}

	ctx := context.Background()

	internalGV := schema.GroupVersion{Group: "", Version: runtime.APIVersionInternal}
	v1GV := schema.GroupVersion{Group: "", Version: "v1"}

	scheme := runtime.NewScheme()
	scheme.AddKnownTypes(internalGV, &Pod{})
	scheme.AddKnownTypes(v1GV, &v1.Pod{})

	scheme.AddValidationFunc(&v1.Pod{}, func(ctx context.Context, op operation.Operation, object, oldObject any) field.ErrorList {
		results := field.ErrorList{}
		if enabled, _ := op.HasOption("option1"); enabled {
			results = append(results, invalidIfOptionErr)
		}
		if slices.Equal(op.Request.Subresources, []string{"status"}) {
			results = append(results, invalidStatusErr)
		}
		if op.Type == operation.Update && object.(*v1.Pod).Spec.RestartPolicy != oldObject.(*v1.Pod).Spec.RestartPolicy {
			results = append(results, mutatedRestartPolicyErr)
		}
		if object.(*v1.Pod).Spec.RestartPolicy == "INVALID" {
			results = append(results, invalidRestartPolicyErr)
		}
		return results
	})
	err := scheme.AddConversionFunc(&Pod{}, &v1.Pod{}, func(a, b interface{}, scope conversion.Scope) error {
		if in, ok := a.(*Pod); ok {
			if out, ok := b.(*v1.Pod); ok {
				out.APIVersion = in.APIVersion
				out.Kind = in.Kind
				out.Spec.RestartPolicy = v1.RestartPolicy(in.RestartPolicy)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	for _, tc := range testCases {
		ctx = genericapirequest.WithRequestInfo(ctx, &genericapirequest.RequestInfo{
			APIGroup:    "",
			APIVersion:  "v1",
			Subresource: tc.subresource,
		})
		t.Run(tc.name, func(t *testing.T) {

			cfg := &ValidationConfigOption{
				DeclarativeValidationConfig: DeclarativeValidationConfig{Options: tc.options},
			}
			if tc.oldObject == nil {
				cfg.OpType = operation.Create
			} else {
				cfg.OpType = operation.Update
			}
			results := runDeclarativeValidationWithRecover(ctx, scheme, tc.object, tc.oldObject, cfg)
			matcher := field.ErrorMatcher{}.ByType().ByField().ByOrigin()
			matcher.Test(t, tc.expected, results)
		})
	}
}

// 伪造的 internal Pod 类型，因为本包无法导入 core.Pod。
// 只保留校验相关字段，避免引入 k8s.io/kubernetes 造成循环依赖。
type Pod struct {
	metav1.TypeMeta   `json:""`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	RestartPolicy     string `json:"restartPolicy"`
}

func (Pod) GetObjectKind() schema.ObjectKind { return schema.EmptyObjectKind }

func (p Pod) DeepCopyObject() runtime.Object {
	return &Pod{
		TypeMeta: metav1.TypeMeta{
			APIVersion: p.APIVersion,
			Kind:       p.Kind,
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      p.Name,
			Namespace: p.Namespace,
		},
		RestartPolicy: p.RestartPolicy,
	}
}

// TestGatherDeclarativeValidationMismatches 覆盖 gatherDeclarativeValidationMismatches
// 函数中 imperative 与 declarative 错误的所有不匹配场景。
// 该函数决定哪些 imperative/declarative 错误视为“同一错误”，
// 覆盖类型、字段、origin 与 lifecycle 标注等匹配维度。
func TestGatherDeclarativeValidationMismatches(t *testing.T) {
	pathStandard := field.NewPath("spec", "standard")
	pathBeta := field.NewPath("spec", "beta")
	pathAlpha := field.NewPath("spec", "alpha")
	pathOther := field.NewPath("spec", "other")

	// Standard Errors
	errHVStandard := field.Invalid(pathStandard, "val", "impStandard").MarkCoveredByDeclarative().WithOrigin("min")
	errDVStandard := field.Invalid(pathStandard, "val", "decStandard").WithOrigin("min")

	// Beta Errors
	errHVBeta := field.Invalid(pathBeta, "val", "impBeta").MarkCoveredByDeclarative().MarkBeta().WithOrigin("min")

	// Alpha Errors
	errDVAlpha := field.Invalid(pathAlpha, "val", "decAlpha").MarkAlpha().WithOrigin("min")
	// Note: No HV Alpha by convention, so if it exists it would be a mismatch if DV is missing.
	// But we usually don't mark HV as covered for Alpha rules.

	// Normalization / Fuzzy match helpers
	errDVStandardDiffDetail := field.Invalid(pathStandard, "val", "decStandardDiffDetail").WithOrigin("min")
	errDVStandardDiffPath := field.Invalid(field.NewPath("spec", "standardAliased"), "val", "decStandard").WithOrigin("min")

	const (
		authoritativeMsg = "This difference should not affect system operation since hand written validation is authoritative."
		disableBetaMsg   = "Consider disabling the DeclarativeValidationBeta feature gate to keep data persisted in etcd consistent with prior versions of Kubernetes."
	)

	testCases := []struct {
		name                    string
		imperativeErrors        field.ErrorList
		declarativeErrors       field.ErrorList
		enforced                bool
		expectMismatches        bool
		expectDetailsContaining []string
		normalizedRules         []field.NormalizationRule
		shortCircuitMismatch    bool
	}{
		{
			name:              "No errors - no mismatch",
			imperativeErrors:  field.ErrorList{},
			declarativeErrors: field.ErrorList{},
			expectMismatches:  false,
		},
		{
			name:              "Clean match - no mismatch",
			imperativeErrors:  field.ErrorList{errHVStandard},
			declarativeErrors: field.ErrorList{errDVStandard},
			expectMismatches:  false,
		},
		{
			name:              "Mismatch: Missing DV (Standard) + Enforced -> Disable Beta Gate",
			imperativeErrors:  field.ErrorList{errHVStandard},
			declarativeErrors: field.ErrorList{},
			enforced:          true,
			expectMismatches:  true,
			expectDetailsContaining: []string{
				"unmatched error(s) found",
				"spec.standard",
				disableBetaMsg,
			},
		},
		{
			name:              "Mismatch: Extra DV (Standard) + Not Enforced -> HV Authoritative",
			imperativeErrors:  field.ErrorList{},
			declarativeErrors: field.ErrorList{errDVStandard},
			enforced:          false,
			expectMismatches:  true,
			expectDetailsContaining: []string{
				"extra error(s) found",
				"spec.standard",
				authoritativeMsg,
			},
		},
		{
			name:              "Mismatch: Missing DV (Beta) + Enforced -> Disable Beta Gate",
			imperativeErrors:  field.ErrorList{errHVBeta},
			declarativeErrors: field.ErrorList{},
			enforced:          true,
			expectMismatches:  true,
			expectDetailsContaining: []string{
				"unmatched error(s) found",
				"spec.beta",
				disableBetaMsg,
			},
		},
		{
			name:              "Mismatch: Extra DV (Alpha) + Enforced -> HV Authoritative (Override)",
			imperativeErrors:  field.ErrorList{},
			declarativeErrors: field.ErrorList{errDVAlpha},
			enforced:          true,
			expectMismatches:  true,
			expectDetailsContaining: []string{
				"extra error(s) found",
				"spec.alpha",
				authoritativeMsg,
			},
		},
		{
			name:              "Fuzzy matching (different detail) - no mismatch",
			imperativeErrors:  field.ErrorList{errHVStandard},
			declarativeErrors: field.ErrorList{errDVStandardDiffDetail},
			expectMismatches:  false,
		},
		{
			name:              "Field normalization - no mismatch",
			imperativeErrors:  field.ErrorList{errHVStandard},
			declarativeErrors: field.ErrorList{errDVStandardDiffPath},
			normalizedRules: []field.NormalizationRule{
				{
					Regexp:      regexp.MustCompile(`spec.standardAliased`),
					Replacement: "spec.standard",
				},
			},
			expectMismatches: false,
		},
		{
			name:              "Multiple mismatches - combined log info",
			imperativeErrors:  field.ErrorList{errHVBeta},
			declarativeErrors: field.ErrorList{errDVAlpha},
			enforced:          true,
			expectMismatches:  true,
			expectDetailsContaining: []string{
				"unmatched error(s) found",
				"spec.beta",
				disableBetaMsg,
				"extra error(s) found",
				"spec.alpha",
				authoritativeMsg,
			},
		},
		{
			name: "Uncovered HV error - no mismatch (ignored)",
			imperativeErrors: field.ErrorList{
				field.Invalid(pathOther, "val", "other").WithOrigin("min"),
			},
			declarativeErrors: field.ErrorList{},
			expectMismatches:  false,
		},
		{
			name: "Short-circuit match: DV parent covers HV parent and child",
			imperativeErrors: field.ErrorList{
				field.Invalid(pathStandard, "val", "immutable").MarkCoveredByDeclarative().WithOrigin("immutable"),
				field.Required(pathStandard.Child("kind"), "val").MarkCoveredByDeclarative().WithOrigin("min"),
			},
			declarativeErrors: field.ErrorList{
				func() *field.Error {
					e := field.Invalid(pathStandard, "val", "immutable").WithOrigin("immutable")
					e.ShortCircuit = true
					return e
				}(),
			},
			expectMismatches:     false,
			shortCircuitMismatch: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			details := gatherDeclarativeValidationMismatches(tc.imperativeErrors, tc.declarativeErrors, tc.enforced, ValidationConfigOption{
				DeclarativeValidationConfig: DeclarativeValidationConfig{
					NormalizationRules:   tc.normalizedRules,
					ShortCircuitMismatch: tc.shortCircuitMismatch,
				},
			})
			// Check if mismatches were found if expected
			if tc.expectMismatches && len(details) == 0 {
				t.Errorf("Expected mismatches but got none")
			}
			// Check if details contain expected text
			detailsStr := strings.Join(details, " ")
			for _, expectedContent := range tc.expectDetailsContaining {
				if !strings.Contains(detailsStr, expectedContent) {
					t.Errorf("Expected details to contain: %q, but they didn't.\nDetails were:\n%s",
						expectedContent, strings.Join(details, "\n"))
				}
			}
			// If we don't expect any details, make sure none provided
			if len(tc.expectDetailsContaining) == 0 && len(details) > 0 {
				t.Errorf("Expected no details, but got %d details: %v", len(details), details)
			}
		})
	}
}

// TestCompareDeclarativeErrorsAndEmitMismatches 验证在错误匹配与不匹配条件下，
// 不匹配信息的预期日志输出。
// 迁移期需要同时观测两套校验的差异，本用例锁定日志输出条件，防止
// 静默漂移或日志刷屏。
func TestCompareDeclarativeErrorsAndEmitMismatches(t *testing.T) {
	replicasPath := field.NewPath("spec").Child("replicas")
	minReadySecondsPath := field.NewPath("spec").Child("minReadySeconds")

	errA := field.Invalid(replicasPath, nil, "regular error A")
	errB := field.Invalid(minReadySecondsPath, -1, "covered error B").WithOrigin("minimum")
	coveredErrB := field.Invalid(minReadySecondsPath, -1, "covered error B").WithOrigin("minimum")
	coveredErrB.CoveredByDeclarative = true

	testCases := []struct {
		name            string
		imperativeErrs  field.ErrorList
		declarativeErrs field.ErrorList
		enforced        bool
		expectLogs      bool
		expectedRegex   string
	}{
		{
			name:            "mismatched errors, log info",
			imperativeErrs:  field.ErrorList{coveredErrB},
			declarativeErrs: field.ErrorList{errA},
			enforced:        true,
			expectLogs:      true,
			// logs have a prefix of the form - E0309 21:05:33.865030 1926106 validate.go:199]
			expectedRegex: "E.*Unexpected difference between hand written validation and declarative validation error results.*Consider disabling the DeclarativeValidationBeta feature gate to keep data persisted in etcd consistent with prior versions of Kubernetes",
		},
		{
			name:            "matching errors, don't log info",
			imperativeErrs:  field.ErrorList{coveredErrB},
			declarativeErrs: field.ErrorList{errB},
			enforced:        true,
			expectLogs:      false,
			expectedRegex:   "",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			klog.SetOutput(&buf)
			klog.LogToStderr(false)
			defer klog.LogToStderr(true)
			ctx := context.Background()

			compareDeclarativeErrorsAndEmitMismatches(ctx, tc.imperativeErrs, tc.declarativeErrs, "test_validationIdentifier", tc.enforced, ValidationConfigOption{})

			klog.Flush()
			logOutput := buf.String()

			if tc.expectLogs {
				matched, err := regexp.MatchString(tc.expectedRegex, logOutput)
				if err != nil {
					t.Fatalf("Bad regex: %v", err)
				}
				if !matched {
					t.Errorf("Expected log output to match %q, but got:\n%s", tc.expectedRegex, logOutput)
				}
			} else if len(logOutput) > 0 {
				t.Errorf("Expected no mismatch logs, but found: %s", logOutput)
			}
		})
	}
}

// TestRunDeclarativeValidationWithRecover 验证校验函数 panic 时被恢复并转换成
// InternalError，而不是让请求 goroutine 崩溃。
func TestRunDeclarativeValidationWithRecover(t *testing.T) {
	ctx := genericapirequest.WithRequestInfo(context.Background(), &genericapirequest.RequestInfo{
		APIGroup:   "",
		APIVersion: "v1",
	})
	obj := &v1.Pod{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Pod"}}

	testCases := []struct {
		name       string
		opType     operation.Type
		oldObj     runtime.Object
		validateFn func(ctx context.Context, op operation.Operation, object, oldObject any) field.ErrorList
		wantErrs   field.ErrorList
	}{
		{
			name:   "no panic on create",
			opType: operation.Create,
			validateFn: func(context.Context, operation.Operation, any, any) field.ErrorList {
				return field.ErrorList{field.Invalid(field.NewPath("field"), "value", "reason")}
			},
			wantErrs: field.ErrorList{field.Invalid(field.NewPath("field"), "value", "reason")},
		},
		{
			name:   "panic on create returns InternalError",
			opType: operation.Create,
			validateFn: func(context.Context, operation.Operation, any, any) field.ErrorList {
				panic("test panic")
			},
			wantErrs: field.ErrorList{
				field.InternalError(nil, fmt.Errorf("panic during declarative validation: test panic")),
			},
		},
		{
			name:   "panic on update returns InternalError",
			opType: operation.Update,
			oldObj: &v1.Pod{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Pod"}},
			validateFn: func(context.Context, operation.Operation, any, any) field.ErrorList {
				panic("test update panic")
			},
			wantErrs: field.ErrorList{
				field.InternalError(nil, fmt.Errorf("panic during declarative validation: test update panic")),
			},
		},
		{
			name:   "nil return, no panic",
			opType: operation.Create,
			validateFn: func(context.Context, operation.Operation, any, any) field.ErrorList {
				return nil
			},
			wantErrs: nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			scheme := runtime.NewScheme()
			scheme.AddKnownTypes(schema.GroupVersion{Version: "v1"}, &v1.Pod{})
			scheme.AddValidationFunc(&v1.Pod{}, tc.validateFn)

			cfg := &ValidationConfigOption{
				ValidationIdentifier: "test_validationIdentifier",
				OpType:               tc.opType,
			}
			gotErrs := runDeclarativeValidationWithRecover(ctx, scheme, obj, tc.oldObj, cfg)

			if !equalErrorLists(gotErrs, tc.wantErrs) {
				t.Errorf("runDeclarativeValidationWithRecover() gotErrs = %#v, want %#v", gotErrs, tc.wantErrs)
			}
		})
	}
}

// TestRecordDuplicateValidationErrors 验证重复的声明式错误会通过指标暴露。
// 重复通常意味着同一规则被多次生成，属于开发期问题，需要可观测性。
func TestRecordDuplicateValidationErrors(t *testing.T) {
	ctx := context.Background()

	testCases := []struct {
		name           string
		qualifiedKind  schema.GroupKind
		errs           field.ErrorList
		expectedMetric string
	}{
		{
			name:          "detect duplicates and increment metric",
			qualifiedKind: schema.GroupKind{Group: "apps", Kind: "ReplicaSet"},
			errs: field.ErrorList{
				field.Invalid(field.NewPath("spec").Child("replicas"), -1, "must be greater than or equal to 0").WithOrigin("minimum"),
				field.Invalid(field.NewPath("spec").Child("replicas"), -1, "must be greater than or equal to 0").WithOrigin("minimum"),
				field.Invalid(field.NewPath("spec").Child("selector"), &metav1.LabelSelector{MatchLabels: map[string]string{}, MatchExpressions: []metav1.LabelSelectorRequirement{}}, "empty selector is invalid for deployment"),
				field.Invalid(field.NewPath("spec").Child("selector"), &metav1.LabelSelector{MatchLabels: map[string]string{}, MatchExpressions: []metav1.LabelSelectorRequirement{}}, "empty selector is invalid for deployment"),
			},
			expectedMetric: `
			# HELP apiserver_validation_duplicate_validation_error_total [INTERNAL] Number of duplicate validation errors during validation.
			# TYPE apiserver_validation_duplicate_validation_error_total counter
			apiserver_validation_duplicate_validation_error_total 2
			`,
		},
		{
			name:          "detect duplicates with all fields but origin being equal",
			qualifiedKind: schema.GroupKind{Group: "apps", Kind: "ReplicaSet"},
			errs: field.ErrorList{
				field.Invalid(field.NewPath("spec").Child("replicas"), -1, "must be greater than or equal to 0").WithOrigin("minimum"),
				field.Invalid(field.NewPath("spec").Child("replicas"), -1, "must be greater than or equal to 0").WithOrigin("min"),
			},
			expectedMetric: `
			# HELP apiserver_validation_duplicate_validation_error_total [INTERNAL] Number of duplicate validation errors during validation.
			# TYPE apiserver_validation_duplicate_validation_error_total counter
			apiserver_validation_duplicate_validation_error_total 1
			`,
		},
		{
			name:          "no duplicates",
			qualifiedKind: schema.GroupKind{Group: "apps", Kind: "ReplicaSet"},
			errs: field.ErrorList{
				field.Invalid(field.NewPath("spec").Child("replicas"), -1, "must be greater than or equal to 0").WithOrigin("minimum"),
				field.Invalid(field.NewPath("spec").Child("selector"), &metav1.LabelSelector{MatchLabels: map[string]string{}, MatchExpressions: []metav1.LabelSelectorRequirement{}}, "empty selector is invalid for deployment"),
			},
			expectedMetric: `
			# HELP apiserver_validation_duplicate_validation_error_total [INTERNAL] Number of duplicate validation errors during validation.
			# TYPE apiserver_validation_duplicate_validation_error_total counter
			apiserver_validation_duplicate_validation_error_total 0
			`,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			defer legacyregistry.Reset()
			defer validation.ResetValidationMetricsInstance()
			RecordDuplicateValidationErrors(ctx, tc.qualifiedKind, tc.errs)

			if err := testutil.GatherAndCompare(legacyregistry.DefaultGatherer, strings.NewReader(tc.expectedMetric), "apiserver_validation_duplicate_validation_error_total"); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// equalErrorLists 比较两个错误列表，对 nil/空切片的差异保持宽容
// （语义上二者都表示“无错误”）。
func equalErrorLists(a, b field.ErrorList) bool {
	// If both are nil, consider them equal
	if a == nil && b == nil {
		return true
	}
	// If one is nil and the other not, they're different
	if (a == nil && b != nil) || (a != nil && b == nil) {
		return false
	}
	// Both non-nil: do a normal DeepEqual
	return reflect.DeepEqual(a, b)
}

// TestMetricIdentifier 验证校验指标的标识构造：包含 GroupKind、操作、子资源与
// 版本信息，且非法输入返回错误而非生成歧义标签。
func TestMetricIdentifier(t *testing.T) {
	scheme := runtime.NewScheme()
	scheme.AddKnownTypes(schema.GroupVersion{Version: "v1"}, &v1.Pod{})

	testCases := []struct {
		name        string
		opType      operation.Type
		obj         runtime.Object
		scheme      *runtime.Scheme
		subresource string
		expected    string
		expectErr   bool
	}{
		{
			name:        "with subresource",
			opType:      operation.Create,
			obj:         &v1.Pod{TypeMeta: metav1.TypeMeta{Kind: "Pod"}},
			scheme:      scheme,
			subresource: "status",
			expected:    "pod_status_create",
			expectErr:   false,
		},
		{
			name:      "without subresource",
			opType:    operation.Update,
			obj:       &v1.Pod{TypeMeta: metav1.TypeMeta{Kind: "Pod"}},
			scheme:    scheme,
			expected:  "pod_update",
			expectErr: false,
		},
		{
			name:      "unknown operation",
			opType:    3, // not a valid operation.Type
			obj:       &v1.Pod{TypeMeta: metav1.TypeMeta{Kind: "Pod"}},
			scheme:    scheme,
			expected:  "pod_unknown_op",
			expectErr: true,
		},
		{
			name:      "no request info and no kind",
			opType:    operation.Create,
			obj:       nil,
			expected:  "unknown_resource_create",
			expectErr: true,
		},
		{
			name:      "known type without kind",
			opType:    operation.Update,
			obj:       &v1.Pod{},
			scheme:    scheme,
			expected:  "pod_update",
			expectErr: false,
		},
		{
			name:      "unknown type with scheme",
			opType:    operation.Create,
			obj:       &runtime.Unknown{}, // Not registered in the scheme
			scheme:    scheme,
			expected:  "unknown_resource_create",
			expectErr: true,
		},
		{
			name:      "unknown type without scheme",
			opType:    operation.Type(4),
			obj:       &runtime.Unknown{}, // Not registered in the scheme
			expected:  "unknown_resource_unknown_op",
			expectErr: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			if tc.obj != nil {
				ctx = genericapirequest.WithRequestInfo(ctx, &genericapirequest.RequestInfo{
					Subresource: tc.subresource,
				})
			}

			result, err := metricIdentifier(ctx, tc.scheme, tc.obj, tc.opType)
			if (err != nil) != tc.expectErr {
				t.Errorf("expected error: %v, got: %v", tc.expectErr, err)
			}
			if result != tc.expected {
				t.Errorf("expected: %s, got: %s", tc.expected, result)
			}
		})
	}
}

// TestValidateDeclarativelyWithMigrationChecks 覆盖迁移期的错误合并规则：
// 标准/DV 覆盖、beta 阶段门控、alpha 阶段 shadow，以及无 origin 的特殊配对。
// 这些规则决定最终返回给用户的是 imperative 还是 declarative 错误，
// 直接关系到版本升级期间的兼容性。
func TestValidateDeclarativelyWithMigrationChecks(t *testing.T) {
	// Standard Lifecycle (Enforced by default in explicit strategy)
	// Standard HV error marked as covered. In a fully migrated state, this should be deleted from source.
	// We include it here to verify it persists (duplicate) if not deleted, rather than being implicitly filtered.
	errHVStandardCovered := field.Forbidden(field.NewPath("spec", "standard"), "imperative standard").MarkCoveredByDeclarative()
	errDVStandard := field.Forbidden(field.NewPath("spec", "standard"), "decStandard")

	// Additional Declarative (No HV counterpart)
	errDVAdditional := field.Invalid(field.NewPath("spec", "additional"), "decAdditional", "declarative additional")

	// Beta Lifecycle (Gated by DeclarativeValidationBeta).
	// The HV error carries no lifecycle marker; it is removed when a matching enforced beta DV
	// error exists. The pair shares type, field, and origin, as real HV/DV counterparts do.
	errHVBetaCovered := field.Invalid(field.NewPath("spec", "beta"), "val", "imperative beta").WithOrigin("min").MarkCoveredByDeclarative()
	errDVBeta := field.Invalid(field.NewPath("spec", "beta"), "decBeta", "declarative beta").WithOrigin("min").MarkBeta()

	// Beta Lifecycle, origin-less non-Invalid pair. Forbidden errors carry no origin, so
	// RequireOriginWhenInvalid does not apply and the pair matches on type and field alone.
	errHVBetaForbidden := field.Forbidden(field.NewPath("spec", "betaForbidden"), "imperative beta forbidden").MarkCoveredByDeclarative()
	errDVBetaForbidden := field.Forbidden(field.NewPath("spec", "betaForbidden"), "declarative beta forbidden").MarkBeta()

	// Alpha Lifecycle (Shadowed)
	// Alpha rules should NOT mark HV as covered, so HV remains authoritative.
	errHVAlpha := field.Forbidden(field.NewPath("spec", "alpha"), "imperative alpha")
	errDVAlpha := field.Invalid(field.NewPath("spec", "alpha"), "decAlpha", "declarative alpha").MarkAlpha()

	testCases := []struct {
		name              string
		dvFeatureEnabled  bool
		betaGateEnabled   bool
		imperativeErrors  field.ErrorList
		declarativeErrors field.ErrorList
		expectedErrors    field.ErrorList
		shouldPanic       bool
	}{
		{
			name:              "Feature Disabled -> Enforces Standard (HV kept+DV returned, duplicate expected if HV not deleted)",
			imperativeErrors:  field.ErrorList{errHVStandardCovered},
			declarativeErrors: field.ErrorList{errDVStandard, errDVAdditional},
			expectedErrors:    field.ErrorList{errHVStandardCovered, errDVStandard, errDVAdditional},
		},
		{
			name:              "Feature Enabled -> Enforces Standard (HV kept+DV returned, duplicate expected if HV not deleted)",
			dvFeatureEnabled:  true,
			imperativeErrors:  field.ErrorList{errHVStandardCovered},
			declarativeErrors: field.ErrorList{errDVStandard, errDVAdditional},
			expectedErrors:    field.ErrorList{errHVStandardCovered, errDVStandard, errDVAdditional},
		},
		{
			name:             "Feature Disabled, Panics -> Returns InternalError",
			imperativeErrors: field.ErrorList{errHVStandardCovered},
			shouldPanic:      true,
			// Standard HV is kept. Panic error appended.
			expectedErrors: append(field.ErrorList{errHVStandardCovered}, field.InternalError(nil, fmt.Errorf("panic during declarative validation: test panic"))),
		},
		{
			name:              "Feature Enabled, InternalError -> Returns InternalError",
			dvFeatureEnabled:  true,
			imperativeErrors:  field.ErrorList{errHVStandardCovered},
			declarativeErrors: field.ErrorList{field.InternalError(nil, fmt.Errorf("internal error"))},
			// Standard HV kept. Internal error appended.
			expectedErrors: field.ErrorList{errHVStandardCovered, field.InternalError(nil, fmt.Errorf("internal error"))},
		},
		{
			name:              "Beta Gate Enabled -> Enforces Beta (HV removed, DV returned)",
			dvFeatureEnabled:  true,
			betaGateEnabled:   true,
			imperativeErrors:  field.ErrorList{errHVBetaCovered},
			declarativeErrors: field.ErrorList{errDVBeta},
			expectedErrors:    field.ErrorList{errDVBeta},
		},
		{
			name:              "Beta Gate Disabled -> Shadows Beta (HV kept, DV hidden)",
			dvFeatureEnabled:  true,
			betaGateEnabled:   false,
			imperativeErrors:  field.ErrorList{errHVBetaCovered},
			declarativeErrors: field.ErrorList{errDVBeta},
			expectedErrors:    field.ErrorList{errHVBetaCovered},
		},
		{
			name:              "Beta Gate Enabled, declarative not emitted -> HV kept",
			dvFeatureEnabled:  true,
			betaGateEnabled:   true,
			imperativeErrors:  field.ErrorList{errHVBetaCovered},
			declarativeErrors: field.ErrorList{},
			expectedErrors:    field.ErrorList{errHVBetaCovered},
		},
		{
			name:              "Beta Gate Enabled, origin-less Forbidden -> HV removed, DV returned",
			dvFeatureEnabled:  true,
			betaGateEnabled:   true,
			imperativeErrors:  field.ErrorList{errHVBetaForbidden},
			declarativeErrors: field.ErrorList{errDVBetaForbidden},
			expectedErrors:    field.ErrorList{errDVBetaForbidden},
		},
		{
			name:              "Alpha -> Shadows Alpha (HV kept, DV hidden)",
			dvFeatureEnabled:  true,
			betaGateEnabled:   true,
			imperativeErrors:  field.ErrorList{errHVAlpha},
			declarativeErrors: field.ErrorList{errDVAlpha},
			expectedErrors:    field.ErrorList{errHVAlpha},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Set feature gate
			if !tc.dvFeatureEnabled {
				featuregatetesting.SetFeatureGateEmulationVersionDuringTest(t, utilfeature.DefaultFeatureGate, version.MustParse("1.35"))
			} else {
				// Only set Beta gate if we are not emulating an older version where it doesn't exist
				featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.DeclarativeValidationBeta, tc.betaGateEnabled)
			}
			featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.DeclarativeValidation, tc.dvFeatureEnabled)

			// Setup scheme for this run
			localScheme := runtime.NewScheme()
			localScheme.AddKnownTypes(schema.GroupVersion{Group: "", Version: "v1"}, &v1.Pod{})
			localScheme.AddValidationFunc(&v1.Pod{}, func(ctx context.Context, op operation.Operation, object, oldObject interface{}) field.ErrorList {
				if tc.shouldPanic {
					panic("test panic")
				}
				return tc.declarativeErrors
			})

			// Setup context
			ctx := genericapirequest.WithRequestInfo(context.Background(), &genericapirequest.RequestInfo{
				APIGroup:   "",
				APIVersion: "v1",
				Resource:   "pods",
			})

			obj := &v1.Pod{
				TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "Pod"},
				ObjectMeta: metav1.ObjectMeta{Name: "test-pod"},
			}

			// Copy imperative errors because they might be modified/appended to
			inputErrs := make(field.ErrorList, len(tc.imperativeErrors))
			copy(inputErrs, tc.imperativeErrors)

			gotErrs := ValidateDeclarativelyWithMigrationChecks(ctx, localScheme, obj, nil, inputErrs, operation.Create, DeclarativeValidationConfig{})

			if !equalErrorLists(gotErrs, tc.expectedErrors) {
				t.Errorf("Expected errors: %v, got: %v", tc.expectedErrors, gotErrs)
			}
		})
	}
}
