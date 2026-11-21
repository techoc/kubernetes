/*
Copyright 2019 The Kubernetes Authors.

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

package authority

import (
	"crypto/x509"
	"fmt"
	"reflect"
	"testing"

	capi "k8s.io/api/certificates/v1"
)

// TestKeyUsagesFromStrings 验证字符串形式的 key usage 到 x509 位掩码的映射。
// 这是证书签名请求校验的关键一步：错误映射会导致签出的证书权限过大或过小。
func TestKeyUsagesFromStrings(t *testing.T) {
	// 表驱动用例覆盖全部受支持的用法、组合位运算、重复项（应去重）、
	// 以及未知字符串（应返回错误，防止静默放行非法用法）。
	testcases := []struct {
		usages              []capi.KeyUsage    // 输入的 CSR key usage 列表
		expectedKeyUsage    x509.KeyUsage      // 期望的 x509 普通用途位掩码（0 表示无）
		expectedExtKeyUsage []x509.ExtKeyUsage // 期望的 x509 扩展用途列表（nil 表示无）
		expectErr           bool               // 是否应当拒绝该输入
	}{
		// 普通用途：signing 映射为数字签名位。
		{
			usages:              []capi.KeyUsage{"signing"},
			expectedKeyUsage:    x509.KeyUsageDigitalSignature,
			expectedExtKeyUsage: nil,
			expectErr:           false,
		},
		// 扩展用途：client auth 属于 ExtKeyUsage，而非普通用途位。
		{
			usages:              []capi.KeyUsage{"client auth"},
			expectedKeyUsage:    0,
			expectedExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
			expectErr:           false,
		},
		// 重复的扩展用途必须去重，否则签出的证书会包含冗余项。
		{
			usages:              []capi.KeyUsage{"client auth", "client auth"},
			expectedKeyUsage:    0,
			expectedExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
			expectErr:           false,
		},
		// 多个普通用途应做按位或合并。
		{
			usages:              []capi.KeyUsage{"cert sign", "encipher only"},
			expectedKeyUsage:    x509.KeyUsageCertSign | x509.KeyUsageEncipherOnly,
			expectedExtKeyUsage: nil,
			expectErr:           false,
		},
		// 普通用途与扩展用途可同时出现，且列表顺序不应影响结果。
		{
			usages:              []capi.KeyUsage{"ocsp signing", "crl sign", "s/mime", "content commitment"},
			expectedKeyUsage:    x509.KeyUsageCRLSign | x509.KeyUsageContentCommitment,
			expectedExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageEmailProtection, x509.ExtKeyUsageOCSPSigning},
			expectErr:           false,
		},
		// 未知用法必须返回错误，避免静默丢弃导致证书权限与请求不符。
		{
			usages:              []capi.KeyUsage{"unsupported string"},
			expectedKeyUsage:    0,
			expectedExtKeyUsage: nil,
			expectErr:           true,
		},
	}

	for _, tc := range testcases {
		t.Run(fmt.Sprint(tc.usages), func(t *testing.T) {
			ku, eku, err := keyUsagesFromStrings(tc.usages)

			// 预期失败的用例只校验错误是否出现，不再检查返回值；
			// 预期成功的用例则继续检查映射结果。
			if tc.expectErr {
				if err == nil {
					t.Errorf("did not return an error, but expected one")
				}
				return
			}

			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}

			// ExtKeyUsage 是切片，必须用 DeepEqual 比较内容而不是比较指针。
			if ku != tc.expectedKeyUsage || !reflect.DeepEqual(eku, tc.expectedExtKeyUsage) {
				t.Errorf("got=(%v, %v), want=(%v, %v)", ku, eku, tc.expectedKeyUsage, tc.expectedExtKeyUsage)
			}
		})
	}
}
