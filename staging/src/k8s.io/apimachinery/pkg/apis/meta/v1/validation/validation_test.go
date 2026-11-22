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
	"fmt"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation/field"

	"k8s.io/utils/ptr"
)

// TestValidateLabels 验证标签键值校验：合法的键（含可选 DNS 子域前缀）、非法字符、
// 过长键值、以及含反斜杠/逗号等非法值都会得到预期错误。
func TestValidateLabels(t *testing.T) {
	successCases := []map[string]string{
		{"simple": "bar"},
		{"now-with-dashes": "bar"},
		{"1-starts-with-num": "bar"},
		{"1234": "bar"},
		{"simple/simple": "bar"},
		{"now-with-dashes/simple": "bar"},
		{"now-with-dashes/now-with-dashes": "bar"},
		{"now.with.dots/simple": "bar"},
		{"now-with.dashes-and.dots/simple": "bar"},
		{"1-num.2-num/3-num": "bar"},
		{"1234/5678": "bar"},
		{"1.2.3.4/5678": "bar"},
		{"UpperCaseAreOK123": "bar"},
		{"goodvalue": "123_-.BaR"},
	}
	for i := range successCases {
		errs := ValidateLabels(successCases[i], field.NewPath("field"))
		if len(errs) != 0 {
			t.Errorf("case[%d] expected success, got %#v", i, errs)
		}
	}

	namePartErrMsg := "name part must consist of"
	nameErrMsg := "a valid label key must consist of"
	labelErrMsg := "a valid label must be an empty string or consist of"
	maxLengthErrMsg := "must be no more than"

	labelNameErrorCases := []struct {
		labels map[string]string
		expect string
	}{
		{map[string]string{"nospecialchars^=@": "bar"}, namePartErrMsg},
		{map[string]string{"cantendwithadash-": "bar"}, namePartErrMsg},
		{map[string]string{"only/one/slash": "bar"}, nameErrMsg},
		{map[string]string{strings.Repeat("a", 254): "bar"}, maxLengthErrMsg},
	}
	for i := range labelNameErrorCases {
		errs := ValidateLabels(labelNameErrorCases[i].labels, field.NewPath("field"))
		if len(errs) != 1 {
			t.Errorf("case[%d]: expected failure", i)
		} else {
			if !strings.Contains(errs[0].Detail, labelNameErrorCases[i].expect) {
				t.Errorf("case[%d]: error details do not include %q: %q", i, labelNameErrorCases[i].expect, errs[0].Detail)
			}
		}
	}

	labelValueErrorCases := []struct {
		labels map[string]string
		expect string
	}{
		{map[string]string{"toolongvalue": strings.Repeat("a", 64)}, maxLengthErrMsg},
		{map[string]string{"backslashesinvalue": "some\\bad\\value"}, labelErrMsg},
		{map[string]string{"nocommasallowed": "bad,value"}, labelErrMsg},
		{map[string]string{"strangecharsinvalue": "?#$notsogood"}, labelErrMsg},
	}
	for i := range labelValueErrorCases {
		errs := ValidateLabels(labelValueErrorCases[i].labels, field.NewPath("field"))
		if len(errs) != 1 {
			t.Errorf("case[%d]: expected failure", i)
		} else {
			if !strings.Contains(errs[0].Detail, labelValueErrorCases[i].expect) {
				t.Errorf("case[%d]: error details do not include %q: %q", i, labelValueErrorCases[i].expect, errs[0].Detail)
			}
		}
	}
}

// TestValidDryRun 验证合法的 dry-run 取值：空列表与重复的 "All" 都允许。
func TestValidDryRun(t *testing.T) {
	tests := [][]string{
		{},
		{"All"},
		{"All", "All"},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("%v", test), func(t *testing.T) {
			if errs := ValidateDryRun(field.NewPath("dryRun"), test); len(errs) != 0 {
				t.Errorf("%v should be a valid dry-run value: %v", test, errs)
			}
		})
	}
}

// TestInvalidDryRun 验证非法的 dry-run 取值会被拒绝。
func TestInvalidDryRun(t *testing.T) {
	tests := [][]string{
		{"False"},
		{"All", "False"},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("%v", test), func(t *testing.T) {
			if len(ValidateDryRun(field.NewPath("dryRun"), test)) == 0 {
				t.Errorf("%v shouldn't be a valid dry-run value", test)
			}
		})
	}
}

// TestValidateDeleteOptionsWithIgnoreStoreReadError 验证
// ignoreStoreReadErrorWithClusterBreakingPotential 与其他删除选项互斥：
// 为 true 时不能同时设置 propagationPolicy、orphanDependents、gracePeriodSeconds
// 或 preconditions，以免绕过存储读取错误时产生语义歧义。
func TestValidateDeleteOptionsWithIgnoreStoreReadError(t *testing.T) {
	fieldPath := field.NewPath("ignoreStoreReadErrorWithClusterBreakingPotential")
	tests := []struct {
		name           string
		opts           metav1.DeleteOptions
		expectedErrors field.ErrorList
	}{
		{
			name: "option is nil",
			opts: metav1.DeleteOptions{
				IgnoreStoreReadErrorWithClusterBreakingPotential: nil,
				DryRun: []string{"All"},
			},
			expectedErrors: field.ErrorList{},
		},
		{
			name: "option is false, PropagationPolicy is set",
			opts: metav1.DeleteOptions{
				IgnoreStoreReadErrorWithClusterBreakingPotential: ptr.To[bool](false),
				DryRun:             []string{"All"},
				PropagationPolicy:  ptr.To[metav1.DeletionPropagation](metav1.DeletePropagationBackground),
				GracePeriodSeconds: ptr.To[int64](0),
				Preconditions:      &metav1.Preconditions{},
			},
			expectedErrors: field.ErrorList{},
		},
		{
			name: "option is false, OrphanDependents is set",
			opts: metav1.DeleteOptions{
				IgnoreStoreReadErrorWithClusterBreakingPotential: ptr.To[bool](false),
				DryRun: []string{"All"},
				//nolint:staticcheck // until it's being removed
				OrphanDependents:   ptr.To[bool](true),
				GracePeriodSeconds: ptr.To[int64](0),
				Preconditions:      &metav1.Preconditions{},
			},
			expectedErrors: field.ErrorList{},
		},
		{
			name: "option is true, PropagationPolicy is set",
			opts: metav1.DeleteOptions{
				IgnoreStoreReadErrorWithClusterBreakingPotential: ptr.To[bool](true),
				DryRun:             []string{"All"},
				PropagationPolicy:  ptr.To[metav1.DeletionPropagation](metav1.DeletePropagationBackground),
				GracePeriodSeconds: ptr.To[int64](0),
				Preconditions:      &metav1.Preconditions{},
			},
			expectedErrors: field.ErrorList{
				field.Invalid(fieldPath, true, "cannot be set together with .propagationPolicy"),
				field.Invalid(fieldPath, true, "cannot be set together with .gracePeriodSeconds"),
				field.Invalid(fieldPath, true, "cannot be set together with .preconditions"),
			},
		},
		{
			name: "option is true, OrphanDependents is set",
			opts: metav1.DeleteOptions{
				IgnoreStoreReadErrorWithClusterBreakingPotential: ptr.To[bool](true),
				DryRun: []string{"All"},
				//nolint:staticcheck // until it's being removed
				OrphanDependents:   ptr.To[bool](true),
				GracePeriodSeconds: ptr.To[int64](0),
				Preconditions:      &metav1.Preconditions{},
			},
			expectedErrors: field.ErrorList{
				field.Invalid(fieldPath, true, "cannot be set together with .orphanDependents"),
				field.Invalid(fieldPath, true, "cannot be set together with .gracePeriodSeconds"),
				field.Invalid(fieldPath, true, "cannot be set together with .preconditions"),
			},
		},
		{
			name: "option is true, no other option is set",
			opts: metav1.DeleteOptions{
				IgnoreStoreReadErrorWithClusterBreakingPotential: ptr.To[bool](false),
			},
			expectedErrors: field.ErrorList{},
		},
		{
			name: "option is true, dry-run is set (should be allowed)",
			opts: metav1.DeleteOptions{
				IgnoreStoreReadErrorWithClusterBreakingPotential: new(true),
				DryRun: []string{"All"},
			},
			expectedErrors: field.ErrorList{},
		},
		{
			name: "option is true, dry-run is set to an invalid value",
			opts: metav1.DeleteOptions{
				IgnoreStoreReadErrorWithClusterBreakingPotential: new(true),
				DryRun: []string{"Invalid"},
			},
			expectedErrors: field.ErrorList{
				field.NotSupported(field.NewPath("dryRun"), []string{"Invalid"}, []string{"All"}),
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			errGot := ValidateDeleteOptions(&test.opts)
			if !cmp.Equal(test.expectedErrors, errGot) {
				t.Errorf("expected error(s) to match, diff: %s", cmp.Diff(test.expectedErrors, errGot))
			}
		})
	}
}

// TestValidPatchOptions 验证合法的 PatchOptions：apply patch 要求 fieldManager，
// 非 apply 的 patch 类型可带 fieldManager 或 force。
func TestValidPatchOptions(t *testing.T) {
	tests := []struct {
		opts      metav1.PatchOptions
		patchType types.PatchType
	}{{
		opts: metav1.PatchOptions{
			Force:        ptr.To(true),
			FieldManager: "kubectl",
		},
		patchType: types.ApplyYAMLPatchType,
	}, {
		opts: metav1.PatchOptions{
			FieldManager: "kubectl",
		},
		patchType: types.ApplyYAMLPatchType,
	}, {
		opts: metav1.PatchOptions{
			Force:        ptr.To(true),
			FieldManager: "kubectl",
		},
		patchType: types.ApplyCBORPatchType,
	}, {
		opts: metav1.PatchOptions{
			FieldManager: "kubectl",
		},
		patchType: types.ApplyCBORPatchType,
	}, {
		opts:      metav1.PatchOptions{},
		patchType: types.MergePatchType,
	}, {
		opts: metav1.PatchOptions{
			FieldManager: "patcher",
		},
		patchType: types.MergePatchType,
	}}

	for _, test := range tests {
		t.Run(fmt.Sprintf("%v", test.opts), func(t *testing.T) {
			errs := ValidatePatchOptions(&test.opts, test.patchType)
			if len(errs) != 0 {
				t.Fatalf("Expected no failures, got: %v", errs)
			}
		})
	}
}

// TestInvalidPatchOptions 验证非法 PatchOptions：apply patch 缺少 fieldManager，
// 或在非 apply patch 上设置 force/fieldManager。
func TestInvalidPatchOptions(t *testing.T) {
	tests := []struct {
		opts      metav1.PatchOptions
		patchType types.PatchType
	}{
		// missing manager
		{
			opts:      metav1.PatchOptions{},
			patchType: types.ApplyYAMLPatchType,
		},
		// missing manager
		{
			opts:      metav1.PatchOptions{},
			patchType: types.ApplyCBORPatchType,
		},
		// force on non-apply
		{
			opts: metav1.PatchOptions{
				Force: ptr.To(true),
			},
			patchType: types.MergePatchType,
		},
		// manager and force on non-apply
		{
			opts: metav1.PatchOptions{
				FieldManager: "kubectl",
				Force:        ptr.To(false),
			},
			patchType: types.MergePatchType,
		},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("%v", test.opts), func(t *testing.T) {
			errs := ValidatePatchOptions(&test.opts, test.patchType)
			if len(errs) == 0 {
				t.Fatal("Expected failures, got none.")
			}
		})
	}
}

// TestValidateFieldManagerValid 验证 fieldManager 允许 Unicode（中文、emoji）等任意可打印字符。
func TestValidateFieldManagerValid(t *testing.T) {
	tests := []string{
		"filedManager",
		"你好", // Hello
		"🍔",
	}

	for _, test := range tests {
		t.Run(test, func(t *testing.T) {
			errs := ValidateFieldManager(test, field.NewPath("fieldManager"))
			if len(errs) != 0 {
				t.Errorf("Validation failed: %v", errs)
			}
		})
	}
}

// TestValidateFieldManagerInvalid 验证 fieldManager 拒绝控制字符与超过 128 字符的输入。
func TestValidateFieldManagerInvalid(t *testing.T) {
	tests := []string{
		"field\nmanager", // Contains invalid character \n
		"fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff", // Has 129 chars
	}

	for _, test := range tests {
		t.Run(test, func(t *testing.T) {
			errs := ValidateFieldManager(test, field.NewPath("fieldManager"))
			if len(errs) == 0 {
				t.Errorf("Validation should have failed")
			}
		})
	}
}

// TestValidateManagedFieldsInvalid 验证非法 managedFields 条目：未知 fieldsType/operation、
// 缺少 operation、非法 manager、过长的 subresource 都应报错；两种校验选项都要覆盖。
func TestValidateManagedFieldsInvalid(t *testing.T) {
	tests := []metav1.ManagedFieldsEntry{{
		Operation:  metav1.ManagedFieldsOperationUpdate,
		FieldsType: "RandomVersion",
		APIVersion: "v1",
	}, {
		Operation:  "RandomOperation",
		FieldsType: "FieldsV1",
		APIVersion: "v1",
	}, {
		// Operation is missing
		FieldsType: "FieldsV1",
		APIVersion: "v1",
	}, {
		Operation:  metav1.ManagedFieldsOperationUpdate,
		FieldsType: "FieldsV1",
		// Invalid fieldManager
		Manager:    "field\nmanager",
		APIVersion: "v1",
	}, {
		Operation:   metav1.ManagedFieldsOperationApply,
		FieldsType:  "FieldsV1",
		APIVersion:  "v1",
		Subresource: "TooLongTooLongTooLongTooLongTooLongTooLongTooLongTooLongTooLongTooLongTooLongTooLongTooLongTooLongTooLongTooLongTooLongTooLongTooLongTooLongTooLongTooLongTooLongTooLongTooLongTooLongTooLongTooLongTooLongTooLongTooLongTooLongTooLongTooLongTooLongTooLongTooLong",
	}}

	for _, test := range tests {
		t.Run(fmt.Sprintf("%#v", test), func(t *testing.T) {
			errs := ValidateManagedFields([]metav1.ManagedFieldsEntry{test}, field.NewPath("managedFields"))
			if len(errs) == 0 {
				t.Errorf("Validation should have failed")
			}
			errs = ValidateManagedFields([]metav1.ManagedFieldsEntry{test}, field.NewPath("managedFields"), CoveredByDeclarative)
			if len(errs) == 0 {
				t.Errorf("Validation with CoveredByDeclarative should have failed")
			}
		})
	}
}

// TestValidateMangedFieldsValid 验证合法 managedFields：fieldsType 可缺省、manager 可用 emoji、
// subresource 合法时两种校验选项都应通过。
func TestValidateMangedFieldsValid(t *testing.T) {
	tests := []metav1.ManagedFieldsEntry{{
		Operation:  metav1.ManagedFieldsOperationUpdate,
		APIVersion: "v1",
		// FieldsType is missing
	}, {
		Operation:  metav1.ManagedFieldsOperationUpdate,
		FieldsType: "FieldsV1",
		APIVersion: "v1",
	}, {
		Operation:   metav1.ManagedFieldsOperationApply,
		FieldsType:  "FieldsV1",
		APIVersion:  "v1",
		Subresource: "scale",
	}, {
		Operation:  metav1.ManagedFieldsOperationApply,
		FieldsType: "FieldsV1",
		APIVersion: "v1",
		Manager:    "🍔",
	}}

	for _, test := range tests {
		t.Run(fmt.Sprintf("%#v", test), func(t *testing.T) {
			err := ValidateManagedFields([]metav1.ManagedFieldsEntry{test}, field.NewPath("managedFields"))
			if err != nil {
				t.Errorf("Validation failed: %v", err)
			}
			err = ValidateManagedFields([]metav1.ManagedFieldsEntry{test}, field.NewPath("managedFields"), CoveredByDeclarative)
			if err != nil {
				t.Errorf("Validation with CoveredByDeclarative failed: %v", err)
			}
		})
	}
}

// TestValidateConditions 验证 Condition 各字段的校验，并覆盖若干易错规则：
// 类型重复、reason 允许冒号和逗号、reason 不能以分隔符结尾。
func TestValidateConditions(t *testing.T) {
	tests := []struct {
		name         string
		conditions   []metav1.Condition
		validateErrs func(t *testing.T, errs field.ErrorList)
	}{{
		name: "bunch-of-invalid-fields",
		conditions: []metav1.Condition{{
			Type:               ":invalid",
			Status:             "unknown",
			ObservedGeneration: -1,
			LastTransitionTime: metav1.Time{},
			Reason:             "invalid;val",
			Message:            "",
		}},
		validateErrs: func(t *testing.T, errs field.ErrorList) {
			needle := `status.conditions[0].type: Invalid value: ":invalid": name part must consist of alphanumeric characters, '-', '_' or '.', and must start and end with an alphanumeric character (e.g. 'MyName',  or 'my.name',  or '123-abc', regex used for validation is '([A-Za-z0-9][-A-Za-z0-9_.]*)?[A-Za-z0-9]')`
			if !hasError(errs, needle) {
				t.Errorf("missing %q in\n%v", needle, errorsAsString(errs))
			}
			needle = `status.conditions[0].status: Unsupported value: "unknown": supported values: "False", "True", "Unknown"`
			if !hasError(errs, needle) {
				t.Errorf("missing %q in\n%v", needle, errorsAsString(errs))
			}
			needle = `status.conditions[0].observedGeneration: Invalid value: -1: must be greater than or equal to zero`
			if !hasError(errs, needle) {
				t.Errorf("missing %q in\n%v", needle, errorsAsString(errs))
			}
			needle = `status.conditions[0].lastTransitionTime: Required value`
			if !hasError(errs, needle) {
				t.Errorf("missing %q in\n%v", needle, errorsAsString(errs))
			}
			needle = `status.conditions[0].reason: Invalid value: "invalid;val": a condition reason must start with alphabetic character, optionally followed by a string of alphanumeric characters or '_,:', and must end with an alphanumeric character or '_' (e.g. 'my_name',  or 'MY_NAME',  or 'MyName',  or 'ReasonA,ReasonB',  or 'ReasonA:ReasonB', regex used for validation is '[A-Za-z]([A-Za-z0-9_,:]*[A-Za-z0-9_])?')`
			if !hasError(errs, needle) {
				t.Errorf("missing %q in\n%v", needle, errorsAsString(errs))
			}
		},
	}, {
		name: "duplicates",
		conditions: []metav1.Condition{{
			Type: "First",
		}, {
			Type: "Second",
		}, {
			Type: "First",
		}},
		validateErrs: func(t *testing.T, errs field.ErrorList) {
			needle := `status.conditions[2]: Duplicate value: "First"`
			if !hasError(errs, needle) {
				t.Errorf("missing %q in\n%v", needle, errorsAsString(errs))
			}
		},
	}, {
		name: "colon-allowed-in-reason",
		conditions: []metav1.Condition{{
			Type:   "First",
			Reason: "valid:val",
		}},
		validateErrs: func(t *testing.T, errs field.ErrorList) {
			needle := `status.conditions[0].reason`
			if hasPrefixError(errs, needle) {
				t.Errorf("has %q in\n%v", needle, errorsAsString(errs))
			}
		},
	}, {
		name: "comma-allowed-in-reason",
		conditions: []metav1.Condition{{
			Type:   "First",
			Reason: "valid,val",
		}},
		validateErrs: func(t *testing.T, errs field.ErrorList) {
			needle := `status.conditions[0].reason`
			if hasPrefixError(errs, needle) {
				t.Errorf("has %q in\n%v", needle, errorsAsString(errs))
			}
		},
	}, {
		name: "reason-does-not-end-in-delimiter",
		conditions: []metav1.Condition{{
			Type:   "First",
			Reason: "valid,val:",
		}},
		validateErrs: func(t *testing.T, errs field.ErrorList) {
			needle := `status.conditions[0].reason: Invalid value: "valid,val:": a condition reason must start with alphabetic character, optionally followed by a string of alphanumeric characters or '_,:', and must end with an alphanumeric character or '_' (e.g. 'my_name',  or 'MY_NAME',  or 'MyName',  or 'ReasonA,ReasonB',  or 'ReasonA:ReasonB', regex used for validation is '[A-Za-z]([A-Za-z0-9_,:]*[A-Za-z0-9_])?')`
			if !hasError(errs, needle) {
				t.Errorf("missing %q in\n%v", needle, errorsAsString(errs))
			}
		},
	}}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			errs := ValidateConditions(test.conditions, field.NewPath("status").Child("conditions"))
			test.validateErrs(t, errs)
		})
	}
}

// TestLabelSelectorMatchExpression 验证 LabelSelector 中 matchExpressions 的键、操作符、
// 值分别非法时的错误数量与错误信息。
func TestLabelSelectorMatchExpression(t *testing.T) {
	testCases := []struct {
		name            string
		labelSelector   *metav1.LabelSelector
		wantErrorNumber int
		validateErrs    func(t *testing.T, errs field.ErrorList)
	}{{
		name: "Valid LabelSelector",
		labelSelector: &metav1.LabelSelector{
			MatchExpressions: []metav1.LabelSelectorRequirement{{
				Key:      "key",
				Operator: metav1.LabelSelectorOpIn,
				Values:   []string{"value"},
			}},
		},
		wantErrorNumber: 0,
		validateErrs:    nil,
	}, {
		name: "MatchExpression's key name isn't valid",
		labelSelector: &metav1.LabelSelector{
			MatchExpressions: []metav1.LabelSelectorRequirement{{
				Key:      "-key",
				Operator: metav1.LabelSelectorOpIn,
				Values:   []string{"value"},
			}},
		},
		wantErrorNumber: 1,
		validateErrs: func(t *testing.T, errs field.ErrorList) {
			errMessage := "name part must consist of alphanumeric characters"
			if !partStringInErrorMessage(errs, errMessage) {
				t.Errorf("missing %q in\n%v", errMessage, errorsAsString(errs))
			}
		},
	}, {
		name: "MatchExpression's operator isn't valid",
		labelSelector: &metav1.LabelSelector{
			MatchExpressions: []metav1.LabelSelectorRequirement{{
				Key:      "key",
				Operator: "abc",
				Values:   []string{"value"},
			}},
		},
		wantErrorNumber: 1,
		validateErrs: func(t *testing.T, errs field.ErrorList) {
			errMessage := "not a valid selector operator"
			if !partStringInErrorMessage(errs, errMessage) {
				t.Errorf("missing %q in\n%v", errMessage, errorsAsString(errs))
			}
		},
	}, {
		name: "MatchExpression's value name isn't valid",
		labelSelector: &metav1.LabelSelector{
			MatchExpressions: []metav1.LabelSelectorRequirement{{
				Key:      "key",
				Operator: metav1.LabelSelectorOpIn,
				Values:   []string{"-value"},
			}},
		},
		wantErrorNumber: 1,
		validateErrs: func(t *testing.T, errs field.ErrorList) {
			errMessage := "a valid label must be an empty string or consist of"
			if !partStringInErrorMessage(errs, errMessage) {
				t.Errorf("missing %q in\n%v", errMessage, errorsAsString(errs))
			}
		},
	}}
	for index, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			allErrs := ValidateLabelSelector(testCase.labelSelector, LabelSelectorValidationOptions{AllowInvalidLabelValueInSelector: false}, field.NewPath("labelSelector"))
			if len(allErrs) != testCase.wantErrorNumber {
				t.Errorf("case[%d]: expected failure", index)
			}
			if len(allErrs) >= 1 && testCase.validateErrs != nil {
				testCase.validateErrs(t, allErrs)
			}
		})
	}
}

// hasError 判断错误列表中是否存在与 needle 完全相同的 Error() 输出。
func hasError(errs field.ErrorList, needle string) bool {
	for _, curr := range errs {
		if curr.Error() == needle {
			return true
		}
	}
	return false
}

// hasPrefixError 判断是否有错误的 Error() 以 prefix 开头，常用于只校验字段路径前缀。
func hasPrefixError(errs field.ErrorList, prefix string) bool {
	for _, curr := range errs {
		if strings.HasPrefix(curr.Error(), prefix) {
			return true
		}
	}
	return false
}

// partStringInErrorMessage 判断是否有错误的 Error() 包含给定子串。
func partStringInErrorMessage(errs field.ErrorList, prefix string) bool {
	for _, curr := range errs {
		if strings.Contains(curr.Error(), prefix) {
			return true
		}
	}
	return false
}

// errorsAsString 把错误列表拼成多行字符串，便于测试失败时输出。
func errorsAsString(errs field.ErrorList) string {
	messages := []string{}
	for _, curr := range errs {
		messages = append(messages, curr.Error())
	}
	return strings.Join(messages, "\n")
}
