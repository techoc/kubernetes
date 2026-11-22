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
	"crypto/x509"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestTypes 验证标准库类型到数据策略标签的映射：
// http.Header/Cookie 中含 token（以及 password）字段，x509.Certificate 携带密钥材料。
// 这些标签用于在日志输出时识别并脱敏敏感字段。
func TestTypes(t *testing.T) {
	testcases := []struct {
		value  interface{}
		expect []string
	}{{
		value:  http.Header{},
		expect: []string{"password", "token"},
	}, {
		value:  http.Cookie{},
		expect: []string{"token"},
	}, {
		value:  x509.Certificate{},
		expect: []string{"security-key"},
	}}
	for _, tc := range testcases {
		types := GlobalDatapolicyMapping(tc.value)
		if !assert.ElementsMatch(t, tc.expect, types) {
			t.Errorf("Wrong set of datatypes detected for %T, want: %v, got %v", tc.value, tc.expect, types)
		}
	}
}
