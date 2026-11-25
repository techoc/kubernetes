/*
Copyright The Kubernetes Authors.

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

package dra

import (
	"github.com/stretchr/testify/assert"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/kubernetes/test/utils/client-go/ktesting"
	"k8s.io/utils/ptr"
)

// testAdminAccess 创建带 AdminAccess 的 claim，检查该字段是否被保留；
// 功能开启时，还验证只有在带 Resource Admin Access label 的 namespace 中才允许使用该字段。
func testAdminAccess(tCtx ktesting.TContext, adminAccessEnabled bool) {
	tCtx.Parallel()

	namespace := createTestNamespace(tCtx, nil)
	claim1 := claim.DeepCopy()
	claim1.Namespace = namespace
	claim1.Spec.Devices.Requests[0].Exactly.AdminAccess = ptr.To(true)
	// 在非 admin namespace 中创建带 AdminAccess 的 claim。
	_, err := tCtx.Client().ResourceV1().ResourceClaims(namespace).Create(tCtx, claim1, metav1.CreateOptions{FieldValidation: "Strict"})
	if adminAccessEnabled {
		if err != nil {
			// 应产生校验错误。
			assert.ErrorContains(tCtx, err, "admin access to devices requires the `resource.kubernetes.io/admin-access: true` label on the containing namespace", "the error message should have contained the expected error message")
			return
		} else {
			tCtx.Fatal("expected validation error(s), got none")
		}

		// 在 admin namespace 中创建带 AdminAccess 的 claim。
		adminNS := createTestNamespace(tCtx, map[string]string{"resource.kubernetes.io/admin-access": "true"})
		claim2 := claim.DeepCopy()
		claim2.Namespace = adminNS
		claim2.Name = "claim2"
		claim2.Spec.Devices.Requests[0].Exactly.AdminAccess = ptr.To(true)
		claim2, err := tCtx.Client().ResourceV1().ResourceClaims(adminNS).Create(tCtx, claim2, metav1.CreateOptions{FieldValidation: "Strict"})
		tCtx.ExpectNoError(err, "create claim")
		if !ptr.Deref(claim2.Spec.Devices.Requests[0].Exactly.AdminAccess, true) {
			tCtx.Fatalf("should store AdminAccess in ResourceClaim %v", claim2)
		}
	} else {
		if claim.Spec.Devices.Requests[0].Exactly.AdminAccess != nil {
			tCtx.Fatal("should drop AdminAccess in ResourceClaim")
		}
	}
}
