/*
Copyright 2021 The Kubernetes Authors.

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
	"testing"

	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/pod-security-admission/admission/api"
)

type (
	test struct {
		configuration   api.PodSecurityConfiguration
		expectedErrList field.ErrorList
	}
)

// 每类非法值只挑一个代表：错误文案用 "..." 占位，这里只校验字段路径和值，不绑定具体提示措辞。
const (
	invalidValueUppercase = "TEST"
	invalidValueChars     = "_&$%#"
	invalidValueTooLong   = "testtesttesttesttesttesttesttesttesttesttesttesttesttesttesttest"
	invalidValueEmpty     = ""
	validValue            = "testing"
)

func TestValidatePodSecurityConfiguration(t *testing.T) {
	// 只断言错误数量、类型和出错值：逐条比对完整 ErrorList 会让无关的文案改动打翻这个测试。
	tests := []test{
		// defaults
		// 第一个用例塞满各类非法 exemptions，第二个用例专测 defaults 的六个字段，一次覆盖两条校验分支。
		{
			expectedErrList: field.ErrorList{
				field.Invalid(exemptionsPath("namespaces", 0), invalidValueEmpty, "..."),
				field.Invalid(exemptionsPath("namespaces", 1), invalidValueChars, "..."),
				field.Invalid(exemptionsPath("namespaces", 2), invalidValueUppercase, "..."),
				field.Invalid(exemptionsPath("namespaces", 3), invalidValueTooLong, "..."),
				field.Duplicate(exemptionsPath("namespaces", 5), validValue),
				field.Invalid(exemptionsPath("runtimeClasses", 0), invalidValueEmpty, "..."),
				field.Invalid(exemptionsPath("runtimeClasses", 1), invalidValueChars, "..."),
				field.Invalid(exemptionsPath("runtimeClasses", 2), invalidValueUppercase, "..."),
				field.Duplicate(exemptionsPath("runtimeClasses", 4), validValue),
				field.Invalid(exemptionsPath("usernames", 0), invalidValueEmpty, "..."),
				field.Duplicate(exemptionsPath("usernames", 2), validValue),
			},
			configuration: api.PodSecurityConfiguration{
				Defaults: api.PodSecurityDefaults{
					Enforce:        "privileged",
					EnforceVersion: "v1.22",
					Audit:          "baseline",
					AuditVersion:   "v1.25",
					Warn:           "restricted",
					WarnVersion:    "latest",
				},
				Exemptions: api.PodSecurityExemptions{
					Namespaces: []string{
						invalidValueEmpty,
						invalidValueChars,
						invalidValueUppercase,
						invalidValueTooLong,
						validValue,
						validValue,
					},
					RuntimeClasses: []string{
						invalidValueEmpty,
						invalidValueChars,
						invalidValueUppercase,
						validValue,
						validValue,
					},
					Usernames: []string{
						invalidValueEmpty,
						validValue,
						validValue,
					},
				},
			},
		},
		{
			expectedErrList: field.ErrorList{
				field.Invalid(defaultsPath("enforce"), "baslein", "..."),
				field.Invalid(defaultsPath("enforce-version"), "v.122", "..."),
				field.Invalid(defaultsPath("warn"), "", "..."),
				field.Invalid(defaultsPath("warn-version"), "", "..."),
				field.Invalid(defaultsPath("audit"), "lorum", "..."),
				field.Invalid(defaultsPath("audit-version"), "ipsum", "..."),
			},
			configuration: api.PodSecurityConfiguration{
				Defaults: api.PodSecurityDefaults{
					Enforce:        "baslein",
					EnforceVersion: "v.122",
					Audit:          "lorum",
					AuditVersion:   "ipsum",
					Warn:           "",
					WarnVersion:    "",
				},
				Exemptions: api.PodSecurityExemptions{},
			},
		},
	}

	for _, test := range tests {
		errList := ValidatePodSecurityConfiguration(&test.configuration)
		if len(errList) != len(test.expectedErrList) {
			t.Errorf("expected %d errs, got %d", len(test.expectedErrList), len(errList))
		}

		for i, expected := range test.expectedErrList {
			if expected.Type.String() != errList[i].Type.String() {
				t.Errorf("expected err type %s, got %s",
					expected.Type.String(),
					errList[i].Type.String())
			}
			if expected.BadValue != errList[i].BadValue {
				t.Errorf("expected bad value '%s', got '%s'",
					expected.BadValue,
					errList[i].BadValue)
			}
		}
	}
}

// defaultsPath returns the appropriate defaults path
// 单独构造路径，避免测试里重复硬编码字段层级写错位置。
func defaultsPath(child string) *field.Path {
	return field.NewPath("defaults", child)
}

// exemptionsPath returns the appropriate defaults path
// 带 Index 是因为 exemptions 里每一项都要报出具体下标，便于定位是哪一条被拒。
func exemptionsPath(child string, i int) *field.Path {
	return field.NewPath("exemptions", child).Index(i)
}
