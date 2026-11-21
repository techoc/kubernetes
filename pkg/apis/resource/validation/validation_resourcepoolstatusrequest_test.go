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

package validation

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/kubernetes/pkg/apis/resource"
)

const (
	// dns1123SubdomainErrorMsg 是 DNS 子域名校验失败时的标准错误信息，
	// 用于在期望值中断言名称不合法的场景。
	dns1123SubdomainErrorMsg = "a lowercase RFC 1123 subdomain must consist of lower case alphanumeric characters, '-' or '.', and must start and end with an alphanumeric character (e.g. 'example.com', regex used for validation is '[a-z0-9]([-a-z0-9]*[a-z0-9])?(\\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*')"
)

// testResourcePoolStatusRequest 构造用于校验测试的请求对象。
// spec 做深拷贝，避免多个场景共享同一份数据相互干扰。
func testResourcePoolStatusRequest(name string, spec resource.ResourcePoolStatusRequestSpec) *resource.ResourcePoolStatusRequest {
	return &resource.ResourcePoolStatusRequest{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
		Spec: *spec.DeepCopy(),
	}
}

// testResourcePoolStatusRequestForUpdate 构造用于更新场景的请求对象。
// 带上 ResourceVersion 以满足更新校验对版本信息的要求。
func testResourcePoolStatusRequestForUpdate(name string, spec resource.ResourcePoolStatusRequestSpec) *resource.ResourcePoolStatusRequest {
	return &resource.ResourcePoolStatusRequest{
		ObjectMeta: metav1.ObjectMeta{
			Name:            name,
			ResourceVersion: "1",
		},
		Spec: *spec.DeepCopy(),
	}
}

// validResourcePoolStatusRequestSpec 是各测试场景复用的基准合法 spec。
var validResourcePoolStatusRequestSpec = resource.ResourcePoolStatusRequestSpec{
	Driver:   "test.example.com",
	PoolName: new("pool-1"),
	Limit:    new(int32(100)),
}

// TestValidateResourcePoolStatusRequest 验证创建时各字段组合的校验结果：
// 合法的名称、driver、poolName、limit 边界值都应通过，缺失或非法名称应报错。
func TestValidateResourcePoolStatusRequest(t *testing.T) {
	goodName := "my-request"
	badName := "!@#$%^"

	scenarios := map[string]struct {
		request      *resource.ResourcePoolStatusRequest
		wantFailures field.ErrorList
	}{
		// 完整合法请求应通过校验。
		"valid-request": {
			request: testResourcePoolStatusRequest(goodName, validResourcePoolStatusRequestSpec),
		},
		// 仅指定 driver 也合法，poolName 和 limit 均可省略。
		"valid-driver-only": {
			request: testResourcePoolStatusRequest(goodName, resource.ResourcePoolStatusRequestSpec{
				Driver: "test.example.com",
			}),
		},
		"valid-with-pool-name": {
			request: testResourcePoolStatusRequest(goodName, resource.ResourcePoolStatusRequestSpec{
				Driver:   "test.example.com",
				PoolName: new("node-1"),
			}),
		},
		"valid-with-limit": {
			request: testResourcePoolStatusRequest(goodName, resource.ResourcePoolStatusRequestSpec{
				Driver: "test.example.com",
				Limit:  new(int32(50)),
			}),
		},
		// limit 的下边界 1 应被接受。
		"valid-limit-min": {
			request: testResourcePoolStatusRequest(goodName, resource.ResourcePoolStatusRequestSpec{
				Driver: "test.example.com",
				Limit:  new(int32(1)),
			}),
		},
		// limit 的上边界 1000 应被接受。
		"valid-limit-max": {
			request: testResourcePoolStatusRequest(goodName, resource.ResourcePoolStatusRequestSpec{
				Driver: "test.example.com",
				Limit:  new(int32(1000)),
			}),
		},
		// 缺少名称应报 Required 错误。
		"missing-name": {
			request:      testResourcePoolStatusRequest("", validResourcePoolStatusRequestSpec),
			wantFailures: field.ErrorList{field.Required(field.NewPath("metadata", "name"), "name or generateName is required")},
		},
		// 非法名称应报 DNS 子域名格式错误。
		"bad-name": {
			request:      testResourcePoolStatusRequest(badName, validResourcePoolStatusRequestSpec),
			wantFailures: field.ErrorList{field.Invalid(field.NewPath("metadata", "name"), badName, dns1123SubdomainErrorMsg)},
		},
	}

	for name, scenario := range scenarios {
		t.Run(name, func(t *testing.T) {
			errs := ValidateResourcePoolStatusRequest(scenario.request)
			assertFailures(t, scenario.wantFailures, errs)
		})
	}
}

// TestValidateResourcePoolStatusRequestUpdate 验证更新场景下无变化时校验通过。
func TestValidateResourcePoolStatusRequestUpdate(t *testing.T) {
	goodName := "my-request"

	scenarios := map[string]struct {
		oldRequest   *resource.ResourcePoolStatusRequest
		newRequest   *resource.ResourcePoolStatusRequest
		wantFailures field.ErrorList
	}{
		"no-change": {
			oldRequest: testResourcePoolStatusRequestForUpdate(goodName, validResourcePoolStatusRequestSpec),
			newRequest: testResourcePoolStatusRequestForUpdate(goodName, validResourcePoolStatusRequestSpec),
		},
	}

	for name, scenario := range scenarios {
		t.Run(name, func(t *testing.T) {
			errs := ValidateResourcePoolStatusRequestUpdate(scenario.newRequest, scenario.oldRequest)
			assertFailures(t, scenario.wantFailures, errs)
		})
	}
}

// TestValidateResourcePoolStatusRequestStatusUpdate 验证状态更新规则：
// 首次写入状态、带校验错误的池状态都应通过，状态一旦设置后再次修改应被拒绝。
func TestValidateResourcePoolStatusRequestStatusUpdate(t *testing.T) {
	goodName := "my-request"
	now := metav1.Now()

	// baseRequest 为每个场景提供独立的初始对象，防止状态被前一个场景污染。
	baseRequest := func() *resource.ResourcePoolStatusRequest {
		r := testResourcePoolStatusRequestForUpdate(goodName, validResourcePoolStatusRequestSpec)
		return r
	}

	scenarios := map[string]struct {
		oldRequest   *resource.ResourcePoolStatusRequest
		newRequest   *resource.ResourcePoolStatusRequest
		wantFailures field.ErrorList
	}{
		// 状态从无到有的首次更新，包含完整的池统计与 Conditions，应被接受。
		"valid-initial-status-update": {
			oldRequest: baseRequest(),
			newRequest: func() *resource.ResourcePoolStatusRequest {
				r := baseRequest()
				r.Status = &resource.ResourcePoolStatusRequestStatus{
					PoolCount: new(int32(2)),
					Pools: []resource.PoolStatus{
						{
							Driver:             "test.example.com",
							PoolName:           "pool-1",
							NodeName:           new("node-1"),
							TotalDevices:       new(int32(4)),
							AllocatedDevices:   new(int32(2)),
							AvailableDevices:   new(int32(2)),
							UnavailableDevices: new(int32(0)),
							ResourceSliceCount: new(int32(1)),
							Generation:         1,
						},
					},
					Conditions: []metav1.Condition{
						{
							Type:               "Complete",
							Status:             metav1.ConditionTrue,
							LastTransitionTime: now,
							Reason:             "Calculated",
						},
					},
				}
				return r
			}(),
		},
		// 池状态可以携带 ValidationError，用于上报收集过程中的不一致。
		"valid-pool-with-validation-error": {
			oldRequest: baseRequest(),
			newRequest: func() *resource.ResourcePoolStatusRequest {
				r := baseRequest()
				validationErr := "pool data is inconsistent"
				r.Status = &resource.ResourcePoolStatusRequestStatus{
					PoolCount: new(int32(1)),
					Pools: []resource.PoolStatus{
						{
							Driver:          "test.example.com",
							PoolName:        "pool-1",
							Generation:      1,
							ValidationError: &validationErr,
						},
					},
					Conditions: []metav1.Condition{
						{
							Type:               "Complete",
							Status:             metav1.ConditionTrue,
							LastTransitionTime: now,
							Reason:             "Calculated",
						},
					},
				}
				return r
			}(),
		},
		// 状态一旦设置即不可修改，避免已下发的统计结果被篡改。
		"status-immutable-after-status-set": {
			oldRequest: func() *resource.ResourcePoolStatusRequest {
				r := baseRequest()
				r.Status = &resource.ResourcePoolStatusRequestStatus{
					PoolCount: new(int32(2)),
				}
				return r
			}(),
			newRequest: func() *resource.ResourcePoolStatusRequest {
				r := baseRequest()
				r.Status = &resource.ResourcePoolStatusRequestStatus{
					PoolCount: new(int32(5)), // changed
				}
				return r
			}(),
			wantFailures: field.ErrorList{
				field.Invalid(field.NewPath("status"),
					&resource.ResourcePoolStatusRequestStatus{PoolCount: new(int32(5))},
					"field is immutable"),
			},
		},
	}

	for name, scenario := range scenarios {
		t.Run(name, func(t *testing.T) {
			errs := ValidateResourcePoolStatusRequestStatusUpdate(scenario.newRequest, scenario.oldRequest)
			assertFailures(t, scenario.wantFailures, errs)
		})
	}
}

// new 是泛型辅助函数，返回指向给定值的指针，方便在测试字典中构造指针字段。
func new[T any](v T) *T {
	return &v
}
