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

package test

import (
	"strings"
	"testing"
)

// ExpectError 在错误不包含 substr 匹配时调用 t.Fatalf。
// substr 为空表示期望无错误；适合在子测试中调用。
func ExpectError(t *testing.T, err error, substr string) {
	if err != nil {
		if len(substr) == 0 {
			t.Fatalf("expect nil error but got %q", err.Error())
		} else if !strings.Contains(err.Error(), substr) {
			t.Fatalf("expect error to contain %q but got %q", substr, err.Error())
		}
	} else if len(substr) > 0 {
		t.Fatalf("expect error to contain %q but got nil error", substr)
	}
}

// SkipRest 在出现非预期错误（或期望错误但未发生）时记录错误并返回 true。
// 返回值表示是否应因该错误结果跳过测试用例的剩余部分。
func SkipRest(t *testing.T, desc string, err error, contains string) bool {
	if err != nil {
		if len(contains) == 0 {
			t.Errorf("case %q, expect nil error but got %q", desc, err.Error())
		} else if !strings.Contains(err.Error(), contains) {
			t.Errorf("case %q, expect error to contain %q but got %q", desc, contains, err.Error())
		}
		return true
	} else if len(contains) > 0 {
		t.Errorf("case %q, expect error to contain %q but got nil error", desc, contains)
		return true
	}
	return false
}
