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

package certificates

import (
	"testing"

	"github.com/stretchr/testify/assert"

	certificatesapi "k8s.io/api/certificates/v1"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// TestIsCertificateRequestApproved 验证 CSR 批准状态判定：
// 只有 Approved 存在且 Denied 不存在时才算批准；Denied 始终优先。
func TestIsCertificateRequestApproved(t *testing.T) {
	testCases := []struct {
		// name 是用例名；conditions 是 CSR 状态条件；expectedIsApproved 是期望判定结果。
		name               string
		conditions         []certificatesapi.CertificateSigningRequestCondition
		expectedIsApproved bool
	}{
		{
			// 无状态时未批准
			"Not any conditions exist",
			nil,
			false,
		}, {
			// 仅 Denied 时未批准；Denied 优先级高于 Approved
			"Approved not exist and Denied exist",
			[]certificatesapi.CertificateSigningRequestCondition{
				{
					Type: certificatesapi.CertificateDenied,
				},
			},
			false,
		}, {
			// 仅 Approved 时才算批准
			"Approved exist and Denied not exist",
			[]certificatesapi.CertificateSigningRequestCondition{
				{
					Type: certificatesapi.CertificateApproved,
				},
			},
			true,
		}, {
			// 同时存在时 Denied 胜出，仍视为未批准
			"Both of Approved and Denied exist",
			[]certificatesapi.CertificateSigningRequestCondition{
				{
					Type: certificatesapi.CertificateApproved,
				},
				{
					Type: certificatesapi.CertificateDenied,
				},
			},
			false,
		},
	}

	for _, tc := range testCases {
		csr := &certificatesapi.CertificateSigningRequest{
			ObjectMeta: v1.ObjectMeta{
				Name: "fake-csr",
			},
			Status: certificatesapi.CertificateSigningRequestStatus{
				Conditions: tc.conditions,
			},
		}

		// 断言布尔判定结果，失败信息带上用例名便于定位
		assert.Equalf(t, tc.expectedIsApproved, IsCertificateRequestApproved(csr), "Failed to test: %s", tc.name)
	}
}
