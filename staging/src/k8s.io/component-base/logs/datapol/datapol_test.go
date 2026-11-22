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

package datapol

import (
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

const (
	// marker 用作会被检验的敏感字符串，出现即代表脱敏失败。
	marker = "hunter2"
)

// withDatapolTag 演示通过 datapolicy 标签声明敏感字段。
type withDatapolTag struct {
	Key string `json:"key" datapolicy:"password"`
}

// withExternalType 演示标准库类型（http.Header）经由 GlobalDatapolicyMapping 被识别。
type withExternalType struct {
	Header http.Header `json:"header"`
}

// noDatapol 无敏感标签且字段值看起来像密码，用于验证“不会被误判”。
type noDatapol struct {
	Key string `json:"key"`
}

// 以下结构验证敏感类型嵌套在不同容器中的检测能力。
type datapolInMember struct {
	secrets withDatapolTag
}

type datapolInSlice struct {
	secrets []withDatapolTag
}

type datapolInMap struct {
	secrets map[string]withDatapolTag
}

// datapolBehindPointer 用于记录已知限制：遍历结构体字段时会跳过指针类型字段。
type datapolBehindPointer struct {
	secrets *withDatapolTag
}

// TestValidate 验证 datapol.Verify 对字段值、嵌套容器与标准库类型的判定，
// 以及格式化输出是否与“是否检测到敏感数据”的结论一致。
func TestValidate(t *testing.T) {
	testcases := []struct {
		name      string
		value     interface{}
		expect    []string
		badFilter bool
	}{{
		name:   "Empty password",
		value:  withDatapolTag{},
		expect: []string{},
	}, {
		name: "Non-empty password",
		value: withDatapolTag{
			Key: marker,
		},
		expect: []string{"password"},
	}, {
		name:   "empty external type",
		value:  withExternalType{Header: http.Header{}},
		expect: []string{},
	}, {
		name: "external type",
		value: withExternalType{Header: http.Header{
			"Authorization": []string{"Bearer hunter2"},
		}},
		expect: []string{"password", "token"},
	}, {
		name:      "no datapol tag",
		value:     noDatapol{Key: marker},
		expect:    []string{},
		badFilter: true,
	}, {
		name: "nested",
		value: datapolInMember{
			secrets: withDatapolTag{
				Key: marker,
			},
		},
		expect: []string{"password"},
	}, {
		name: "nested in pointer",
		value: datapolBehindPointer{
			secrets: &withDatapolTag{Key: marker},
		},
		expect: []string{},
	}, {
		name: "nested in slice",
		value: datapolInSlice{
			secrets: []withDatapolTag{{Key: marker}},
		},
		expect: []string{"password"},
	}, {
		name: "nested in map",
		value: datapolInMap{
			secrets: map[string]withDatapolTag{
				"key": {Key: marker},
			},
		},
		expect: []string{"password"},
	}, {
		name: "nested in map but empty",
		value: datapolInMap{
			secrets: map[string]withDatapolTag{
				"key": {},
			},
		},
		expect: []string{},
	}, {
		name: "struct in interface",
		value: struct{ v interface{} }{v: withDatapolTag{
			Key: marker,
		}},
		expect: []string{"password"},
	}, {
		name: "structptr in interface",
		value: struct{ v interface{} }{v: &withDatapolTag{
			Key: marker,
		}},
		expect: []string{},
	}}
	for _, tc := range testcases {
		res := Verify(tc.value)
		if !assert.ElementsMatch(t, tc.expect, res) {
			t.Errorf("Wrong set of tags for %q. expect %v, got %v", tc.name, tc.expect, res)
		}
		// badFilter 用例专门验证“未标记敏感”的结构会被原样打印，不能反过来要求脱敏。
		if !tc.badFilter {
			formatted := fmt.Sprintf("%v", tc.value)
			if strings.Contains(formatted, marker) != (len(tc.expect) > 0) {
				t.Errorf("Filter decision doesn't match formatted value for %q: tags: %v, format: %s", tc.name, tc.expect, formatted)
			}
		}
	}
}
