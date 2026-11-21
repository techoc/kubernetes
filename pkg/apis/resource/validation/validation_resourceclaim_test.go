/*
Copyright 2022 The Kubernetes Authors.

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

	apiresource "k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/apimachinery/pkg/util/version"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	featuregatetesting "k8s.io/component-base/featuregate/testing"
	"k8s.io/dynamic-resource-allocation/structured"
	"k8s.io/kubernetes/pkg/apis/core"
	"k8s.io/kubernetes/pkg/apis/resource"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/utils/ptr"
)

// testClaim 构造一个最小合法的 ResourceClaim，spec 深拷贝后供各场景局部修改。
func testClaim(name, namespace string, spec resource.ResourceClaimSpec) *resource.ResourceClaim {
	return &resource.ResourceClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: *spec.DeepCopy(),
	}
}

// 测试中共享的合法/非法取值。集中定义便于断言中精确引用预期错误值。
const (
	goodName          = "foo"
	goodName2         = "bar"
	badName           = "!@#$%^"
	goodNS            = "ns"
	badSubrequestName = "&^%$"
)

// 共享的测试数据：合法 spec、非法请求名组合，以及可复用的合法 CEL selector。
var (
	badRequestFormat      = fmt.Sprintf("%s/%s/%s", goodName, goodName, goodName)
	badFullSubrequestName = fmt.Sprintf("%s/%s", badName, badSubrequestName)
	validClaimSpec        = resource.ResourceClaimSpec{
		Devices: resource.DeviceClaim{
			Requests: []resource.DeviceRequest{{
				Name: goodName,
				Exactly: &resource.ExactDeviceRequest{
					DeviceClassName: goodName,
					AllocationMode:  resource.DeviceAllocationModeExactCount,
					Count:           1,
				},
			}},
		},
	}
	validClaimSpecWithFirstAvailable = resource.ResourceClaimSpec{
		Devices: resource.DeviceClaim{
			Requests: []resource.DeviceRequest{{
				Name: goodName,
				FirstAvailable: []resource.DeviceSubRequest{
					{
						Name:            goodName,
						DeviceClassName: goodName,
						AllocationMode:  resource.DeviceAllocationModeExactCount,
						Count:           1,
					},
					{
						Name:            goodName2,
						DeviceClassName: goodName,
						AllocationMode:  resource.DeviceAllocationModeExactCount,
						Count:           1,
					},
				},
			}},
		},
	}
	validSelector = []resource.DeviceSelector{
		{
			CEL: &resource.CELDeviceSelector{
				Expression: `device.driver == "dra.example.com"`,
			},
		},
	}
	validClaim                    = testClaim(goodName, goodNS, validClaimSpec)
	conditionValidationErrMessage = "name part must consist of alphanumeric characters, '-', '_' or '.', and must start and end with an alphanumeric character (e.g. 'MyName',  or 'my.name',  or '123-abc', regex used for validation is '([A-Za-z0-9][-A-Za-z0-9_.]*)?[A-Za-z0-9]')"
)

// TestValidateClaim 覆盖 ResourceClaim 创建校验的主要分支：
// 元数据、request/constraint/config 的引用关系、分配模式、CEL 表达式、
// 派生属性，以及 feature gate 对校验行为的影响。
func TestValidateClaim(t *testing.T) {
	now := metav1.Now()
	badValue := "spaces not allowed"

	scenarios := map[string]struct {
		claim                         *resource.ResourceClaim
		wantFailures                  field.ErrorList
		consumableCapacityFeatureGate bool
		listTypeAttributesFeatureGate bool
	}{
		"good-claim": {
			claim: testClaim(goodName, goodNS, validClaimSpec),
		},
		"missing-name": {
			wantFailures: field.ErrorList{field.Required(field.NewPath("metadata", "name"), "name or generateName is required")},
			claim:        testClaim("", goodNS, validClaimSpec),
		},
		"bad-name": {
			wantFailures: field.ErrorList{field.Invalid(field.NewPath("metadata", "name"), badName, "a lowercase RFC 1123 subdomain must consist of lower case alphanumeric characters, '-' or '.', and must start and end with an alphanumeric character (e.g. 'example.com', regex used for validation is '[a-z0-9]([-a-z0-9]*[a-z0-9])?(\\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*')")},
			claim:        testClaim(badName, goodNS, validClaimSpec),
		},
		"missing-namespace": {
			wantFailures: field.ErrorList{field.Required(field.NewPath("metadata", "namespace"), "")},
			claim:        testClaim(goodName, "", validClaimSpec),
		},
		"generate-name": {
			claim: func() *resource.ResourceClaim {
				claim := testClaim(goodName, goodNS, validClaimSpec)
				claim.GenerateName = "pvc-"
				return claim
			}(),
		},
		"uid": {
			claim: func() *resource.ResourceClaim {
				claim := testClaim(goodName, goodNS, validClaimSpec)
				claim.UID = "ac051fac-2ead-46d9-b8b4-4e0fbeb7455d"
				return claim
			}(),
		},
		"resource-version": {
			claim: func() *resource.ResourceClaim {
				claim := testClaim(goodName, goodNS, validClaimSpec)
				claim.ResourceVersion = "1"
				return claim
			}(),
		},
		"generation": {
			claim: func() *resource.ResourceClaim {
				claim := testClaim(goodName, goodNS, validClaimSpec)
				claim.Generation = 100
				return claim
			}(),
		},
		"creation-timestamp": {
			claim: func() *resource.ResourceClaim {
				claim := testClaim(goodName, goodNS, validClaimSpec)
				claim.CreationTimestamp = now
				return claim
			}(),
		},
		"deletion-grace-period-seconds": {
			claim: func() *resource.ResourceClaim {
				claim := testClaim(goodName, goodNS, validClaimSpec)
				claim.DeletionGracePeriodSeconds = ptr.To[int64](10)
				return claim
			}(),
		},
		"owner-references": {
			claim: func() *resource.ResourceClaim {
				claim := testClaim(goodName, goodNS, validClaimSpec)
				claim.OwnerReferences = []metav1.OwnerReference{
					{
						APIVersion: "v1",
						Kind:       "pod",
						Name:       "foo",
						UID:        "ac051fac-2ead-46d9-b8b4-4e0fbeb7455d",
					},
				}
				return claim
			}(),
		},
		"finalizers": {
			claim: func() *resource.ResourceClaim {
				claim := testClaim(goodName, goodNS, validClaimSpec)
				claim.Finalizers = []string{
					"example.com/foo",
				}
				return claim
			}(),
		},
		"managed-fields": {
			claim: func() *resource.ResourceClaim {
				claim := testClaim(goodName, goodNS, validClaimSpec)
				claim.ManagedFields = []metav1.ManagedFieldsEntry{
					{
						FieldsType: "FieldsV1",
						Operation:  "Apply",
						APIVersion: "apps/v1",
						Manager:    "foo",
					},
				}
				return claim
			}(),
		},
		"good-labels": {
			claim: func() *resource.ResourceClaim {
				claim := testClaim(goodName, goodNS, validClaimSpec)
				claim.Labels = map[string]string{
					"apps.kubernetes.io/name": "test",
				}
				return claim
			}(),
		},
		"bad-labels": {
			wantFailures: field.ErrorList{field.Invalid(field.NewPath("metadata", "labels"), badValue, "a valid label must be an empty string or consist of alphanumeric characters, '-', '_' or '.', and must start and end with an alphanumeric character (e.g. 'MyValue',  or 'my_value',  or '12345', regex used for validation is '(([A-Za-z0-9][-A-Za-z0-9_.]*)?[A-Za-z0-9])?')")},
			claim: func() *resource.ResourceClaim {
				claim := testClaim(goodName, goodNS, validClaimSpec)
				claim.Labels = map[string]string{
					"hello-world": badValue,
				}
				return claim
			}(),
		},
		"good-annotations": {
			claim: func() *resource.ResourceClaim {
				claim := testClaim(goodName, goodNS, validClaimSpec)
				claim.Annotations = map[string]string{
					"foo": "bar",
				}
				return claim
			}(),
		},
		"bad-annotations": {
			wantFailures: field.ErrorList{field.Invalid(field.NewPath("metadata", "annotations"), badName, "name part must consist of alphanumeric characters, '-', '_' or '.', and must start and end with an alphanumeric character (e.g. 'MyName',  or 'my.name',  or '123-abc', regex used for validation is '([A-Za-z0-9][-A-Za-z0-9_.]*)?[A-Za-z0-9]')")},
			claim: func() *resource.ResourceClaim {
				claim := testClaim(goodName, goodNS, validClaimSpec)
				claim.Annotations = map[string]string{
					badName: "hello world",
				}
				return claim
			}(),
		},
		"bad-classname": {
			wantFailures: field.ErrorList{field.Invalid(field.NewPath("spec", "devices", "requests").Index(0).Child("exactly", "deviceClassName"), badName, "a lowercase RFC 1123 subdomain must consist of lower case alphanumeric characters, '-' or '.', and must start and end with an alphanumeric character (e.g. 'example.com', regex used for validation is '[a-z0-9]([-a-z0-9]*[a-z0-9])?(\\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*')")},
			claim: func() *resource.ResourceClaim {
				claim := testClaim(goodName, goodNS, validClaimSpec)
				claim.Spec.Devices.Requests[0].Exactly.DeviceClassName = badName
				return claim
			}(),
		},
		"missing-classname": {
			wantFailures: field.ErrorList{field.Required(field.NewPath("spec", "devices", "requests").Index(0).Child("exactly", "deviceClassName"), "")},
			claim: func() *resource.ResourceClaim {
				claim := testClaim(goodName, goodNS, validClaimSpec)
				claim.Spec.Devices.Requests[0].Exactly.DeviceClassName = ""
				return claim
			}(),
		},
		"invalid-request-without-max-size-short-circuit": {
			wantFailures: field.ErrorList{
				field.Invalid(field.NewPath("spec", "devices", "constraints").Index(0).Child("requests").Index(1), badName, "a lowercase RFC 1123 label must consist of lower case alphanumeric characters or '-', and must start and end with an alphanumeric character (e.g. 'my-name',  or '123-abc', regex used for validation is '[a-z0-9]([-a-z0-9]*[a-z0-9])?')"),
				field.Invalid(field.NewPath("spec", "devices", "constraints").Index(0).Child("requests").Index(1), badName, "must be the name of a request in the claim or the name of a request and a subrequest separated by '/'"),
				field.Invalid(field.NewPath("spec", "devices", "constraints").Index(0).Child("matchAttribute"), "missing-domain", "a valid C identifier must start with alphabetic character or '_', followed by a string of alphanumeric characters or '_' (e.g. 'my_name',  or 'MY_NAME',  or 'MyName', regex used for validation is '[A-Za-z_][A-Za-z0-9_]*')").MarkCoveredByDeclarative(),
				field.Invalid(field.NewPath("spec", "devices", "constraints").Index(0).Child("matchAttribute"), resource.FullyQualifiedName("missing-domain"), "a fully qualified name must be a domain and a name separated by a slash").MarkCoveredByDeclarative(),
				field.Invalid(field.NewPath("spec", "devices", "constraints").Index(1).Child("matchAttribute"), "", "a valid C identifier must start with alphabetic character or '_', followed by a string of alphanumeric characters or '_' (e.g. 'my_name',  or 'MY_NAME',  or 'MyName', regex used for validation is '[A-Za-z_][A-Za-z0-9_]*')").MarkCoveredByDeclarative(),
				field.Invalid(field.NewPath("spec", "devices", "constraints").Index(1).Child("matchAttribute"), resource.FullyQualifiedName(""), "a fully qualified name must be a domain and a name separated by a slash").MarkCoveredByDeclarative(),
				field.Required(field.NewPath("spec", "devices", "constraints").Index(2).Child("matchAttribute"), ""),
				field.Invalid(field.NewPath("spec", "devices", "config").Index(0).Child("requests").Index(1), badName, "a lowercase RFC 1123 label must consist of lower case alphanumeric characters or '-', and must start and end with an alphanumeric character (e.g. 'my-name',  or '123-abc', regex used for validation is '[a-z0-9]([-a-z0-9]*[a-z0-9])?')"),
				field.Invalid(field.NewPath("spec", "devices", "config").Index(0).Child("requests").Index(1), badName, "must be the name of a request in the claim or the name of a request and a subrequest separated by '/'"),
			},
			claim: func() *resource.ResourceClaim {
				claim := testClaim(goodName, goodNS, validClaimSpec)
				claim.Spec.Devices.Constraints = []resource.DeviceConstraint{
					{
						Requests:       []string{claim.Spec.Devices.Requests[0].Name, badName},
						MatchAttribute: ptr.To(resource.FullyQualifiedName("missing-domain")),
					},
					{
						MatchAttribute: ptr.To(resource.FullyQualifiedName("")),
					},
					{
						MatchAttribute: nil,
					},
				}
				claim.Spec.Devices.Config = []resource.DeviceClaimConfiguration{{
					Requests: []string{claim.Spec.Devices.Requests[0].Name, badName},
					DeviceConfiguration: resource.DeviceConfiguration{
						Opaque: &resource.OpaqueDeviceConfiguration{
							Driver: "dra.example.com",
							Parameters: runtime.RawExtension{
								Raw: []byte(`{"kind": "foo", "apiVersion": "dra.example.com/v1"}`),
							},
						},
					},
				}}
				return claim
			}(),
		},
		"invalid-request-with-max-size-short-circuit": {
			wantFailures: field.ErrorList{
				field.TooMany(field.NewPath("spec", "devices", "requests"), resource.DeviceRequestsMaxSize+1, resource.DeviceRequestsMaxSize).MarkCoveredByDeclarative(),
				field.TooMany(field.NewPath("spec", "devices", "constraints"), resource.DeviceConstraintsMaxSize+1, resource.DeviceConstraintsMaxSize).MarkCoveredByDeclarative(),
				field.TooMany(field.NewPath("spec", "devices", "config"), resource.DeviceConfigMaxSize+1, resource.DeviceConfigMaxSize).MarkCoveredByDeclarative(),
			},
			// 同时构造约束和配置中的非法请求引用，验证两类字段都会报错。
			claim: func() *resource.ResourceClaim {
				claim := testClaim(goodName, goodNS, validClaimSpec)
				claim.Spec.Devices.Constraints = []resource.DeviceConstraint{
					{
						Requests:       []string{claim.Spec.Devices.Requests[0].Name, badName},
						MatchAttribute: ptr.To(resource.FullyQualifiedName("missing-domain")),
					},
					{
						MatchAttribute: ptr.To(resource.FullyQualifiedName("")),
					},
					{
						MatchAttribute: nil,
					},
				}
				// 超过上限后，校验会短路，不再逐项报告内部字段错误。
				for i := len(claim.Spec.Devices.Constraints); i < resource.DeviceConstraintsMaxSize+1; i++ {
					claim.Spec.Devices.Constraints = append(claim.Spec.Devices.Constraints, resource.DeviceConstraint{MatchAttribute: ptr.To(resource.FullyQualifiedName("foo/bar"))})
				}
				claim.Spec.Devices.Config = []resource.DeviceClaimConfiguration{{
					Requests: []string{claim.Spec.Devices.Requests[0].Name, badName},
					DeviceConfiguration: resource.DeviceConfiguration{
						Opaque: &resource.OpaqueDeviceConfiguration{
							Driver: "dra.example.com",
							Parameters: runtime.RawExtension{
								Raw: []byte(`{"kind": "foo", "apiVersion": "dra.example.com/v1"}`),
							},
						},
					},
				}}
				for i := len(claim.Spec.Devices.Config); i < resource.DeviceConfigMaxSize+1; i++ {
					claim.Spec.Devices.Config = append(claim.Spec.Devices.Config, resource.DeviceClaimConfiguration{
						DeviceConfiguration: resource.DeviceConfiguration{
							Opaque: &resource.OpaqueDeviceConfiguration{
								Driver: "dra.example.com",
								Parameters: runtime.RawExtension{
									Raw: []byte(`{"kind": "foo", "apiVersion": "dra.example.com/v1"}`),
								},
							},
						},
					})
				}
				// 复制合法 request 并改名，快速构造大量合法项以触发数量上限。
				for i := len(claim.Spec.Devices.Requests); i < resource.DeviceRequestsMaxSize+1; i++ {
					req := claim.Spec.Devices.Requests[0].DeepCopy()
					req.Name += fmt.Sprintf("%d", i)
					claim.Spec.Devices.Requests = append(claim.Spec.Devices.Requests, *req)
				}
				return claim
			}(),
		},
		"invalid-distinct-constraint": {
			wantFailures: field.ErrorList{
				field.Invalid(field.NewPath("spec", "devices", "constraints").Index(0).Child("distinctAttribute"), "missing-domain", "a valid C identifier must start with alphabetic character or '_', followed by a string of alphanumeric characters or '_' (e.g. 'my_name',  or 'MY_NAME',  or 'MyName', regex used for validation is '[A-Za-z_][A-Za-z0-9_]*')").MarkCoveredByDeclarative(),
				field.Invalid(field.NewPath("spec", "devices", "constraints").Index(0).Child("distinctAttribute"), resource.FullyQualifiedName("missing-domain"), "a fully qualified name must be a domain and a name separated by a slash").MarkCoveredByDeclarative(),
				field.Invalid(field.NewPath("spec", "devices", "constraints").Index(1).Child("distinctAttribute"), "", "a valid C identifier must start with alphabetic character or '_', followed by a string of alphanumeric characters or '_' (e.g. 'my_name',  or 'MY_NAME',  or 'MyName', regex used for validation is '[A-Za-z_][A-Za-z0-9_]*')").MarkCoveredByDeclarative(),
				field.Invalid(field.NewPath("spec", "devices", "constraints").Index(1).Child("distinctAttribute"), resource.FullyQualifiedName(""), "a fully qualified name must be a domain and a name separated by a slash").MarkCoveredByDeclarative(),
				field.Required(field.NewPath("spec", "devices", "constraints").Index(2), `exactly one of "matchAttribute" or "distinctAttribute" is required, but multiple fields are set`)},
			claim: func() *resource.ResourceClaim {
				claim := testClaim(goodName, goodNS, validClaimSpec)
				claim.Spec.Devices.Constraints = []resource.DeviceConstraint{
					{
						Requests:          []string{claim.Spec.Devices.Requests[0].Name},
						DistinctAttribute: ptr.To(resource.FullyQualifiedName("missing-domain")),
					},
					{
						DistinctAttribute: ptr.To(resource.FullyQualifiedName("")),
					},
					{
						MatchAttribute:    nil,
						DistinctAttribute: nil,
					},
				}
				return claim
			}(),
			consumableCapacityFeatureGate: true,
		},

		"valid-request": {
			claim: func() *resource.ResourceClaim {
				claim := testClaim(goodName, goodNS, validClaimSpec)
				// 恰好填满上限，验证边界值本身合法。
				for i := len(claim.Spec.Devices.Constraints); i < resource.DeviceConstraintsMaxSize; i++ {
					claim.Spec.Devices.Constraints = append(claim.Spec.Devices.Constraints, resource.DeviceConstraint{MatchAttribute: ptr.To(resource.FullyQualifiedName("foo/bar"))})
				}
				for i := len(claim.Spec.Devices.Config); i < resource.DeviceConfigMaxSize; i++ {
					claim.Spec.Devices.Config = append(claim.Spec.Devices.Config, resource.DeviceClaimConfiguration{
						DeviceConfiguration: resource.DeviceConfiguration{
							Opaque: &resource.OpaqueDeviceConfiguration{
								Driver: "dra.example.com",
								Parameters: runtime.RawExtension{
									Raw: []byte(`{"kind": "foo", "apiVersion": "dra.example.com/v1"}`),
								},
							},
						},
					})
				}
				for i := len(claim.Spec.Devices.Requests); i < resource.DeviceRequestsMaxSize; i++ {
					req := claim.Spec.Devices.Requests[0].DeepCopy()
					req.Name += fmt.Sprintf("%d", i)
					claim.Spec.Devices.Requests = append(claim.Spec.Devices.Requests, *req)
				}
				return claim
			}(),
		},
		"invalid-spec": {
			wantFailures: field.ErrorList{
				field.Invalid(field.NewPath("spec", "devices", "constraints").Index(0).Child("requests").Index(1), badName, "a lowercase RFC 1123 label must consist of lower case alphanumeric characters or '-', and must start and end with an alphanumeric character (e.g. 'my-name',  or '123-abc', regex used for validation is '[a-z0-9]([-a-z0-9]*[a-z0-9])?')"),
				field.Invalid(field.NewPath("spec", "devices", "constraints").Index(0).Child("requests").Index(1), badName, "must be the name of a request in the claim or the name of a request and a subrequest separated by '/'"),
				field.Invalid(field.NewPath("spec", "devices", "constraints").Index(0).Child("matchAttribute"), "missing-domain", "a valid C identifier must start with alphabetic character or '_', followed by a string of alphanumeric characters or '_' (e.g. 'my_name',  or 'MY_NAME',  or 'MyName', regex used for validation is '[A-Za-z_][A-Za-z0-9_]*')").MarkCoveredByDeclarative(),
				field.Invalid(field.NewPath("spec", "devices", "constraints").Index(0).Child("matchAttribute"), resource.FullyQualifiedName("missing-domain"), "a fully qualified name must be a domain and a name separated by a slash").MarkCoveredByDeclarative(),
				field.Invalid(field.NewPath("spec", "devices", "constraints").Index(1).Child("matchAttribute"), "", "a valid C identifier must start with alphabetic character or '_', followed by a string of alphanumeric characters or '_' (e.g. 'my_name',  or 'MY_NAME',  or 'MyName', regex used for validation is '[A-Za-z_][A-Za-z0-9_]*')").MarkCoveredByDeclarative(),
				field.Invalid(field.NewPath("spec", "devices", "constraints").Index(1).Child("matchAttribute"), resource.FullyQualifiedName(""), "a fully qualified name must be a domain and a name separated by a slash").MarkCoveredByDeclarative(),
				field.Required(field.NewPath("spec", "devices", "constraints").Index(2).Child("matchAttribute"), ""),
				field.Invalid(field.NewPath("spec", "devices", "config").Index(0).Child("requests").Index(1), badName, "a lowercase RFC 1123 label must consist of lower case alphanumeric characters or '-', and must start and end with an alphanumeric character (e.g. 'my-name',  or '123-abc', regex used for validation is '[a-z0-9]([-a-z0-9]*[a-z0-9])?')"),
				field.Invalid(field.NewPath("spec", "devices", "config").Index(0).Child("requests").Index(1), badName, "must be the name of a request in the claim or the name of a request and a subrequest separated by '/'"),
			},
			claim: func() *resource.ResourceClaim {
				claim := testClaim(goodName, goodNS, validClaimSpec)
				claim.Spec.Devices.Constraints = []resource.DeviceConstraint{
					{
						Requests:       []string{claim.Spec.Devices.Requests[0].Name, badName},
						MatchAttribute: ptr.To(resource.FullyQualifiedName("missing-domain")),
					},
					{
						MatchAttribute: ptr.To(resource.FullyQualifiedName("")),
					},
					{
						MatchAttribute: nil,
					},
				}
				claim.Spec.Devices.Config = []resource.DeviceClaimConfiguration{{
					Requests: []string{claim.Spec.Devices.Requests[0].Name, badName},
					DeviceConfiguration: resource.DeviceConfiguration{
						Opaque: &resource.OpaqueDeviceConfiguration{
							Driver: "dra.example.com",
							Parameters: runtime.RawExtension{
								Raw: []byte(`{"kind": "foo", "apiVersion": "dra.example.com/v1"}`),
							},
						},
					},
				}}
				return claim
			}(),
		},
		"allocation-mode": {
			wantFailures: field.ErrorList{
				field.Invalid(field.NewPath("spec", "devices", "requests").Index(2).Child("exactly", "count"), int64(-1), "must be greater than zero"),
				field.NotSupported(field.NewPath("spec", "devices", "requests").Index(3).Child("exactly", "allocationMode"), resource.DeviceAllocationMode("other"), []resource.DeviceAllocationMode{resource.DeviceAllocationModeAll, resource.DeviceAllocationModeExactCount}).MarkCoveredByDeclarative(),
				field.Invalid(field.NewPath("spec", "devices", "requests").Index(4).Child("exactly", "count"), int64(2), "must not be specified when allocationMode is 'All'"),
				field.Duplicate(field.NewPath("spec", "devices", "requests").Index(5), "foo").MarkCoveredByDeclarative(),
			},
			claim: func() *resource.ResourceClaim {
				claim := testClaim(goodName, goodNS, validClaimSpec)

				// 依次构造：合法 All 模式、负数 Count、非法模式、All 模式带 Count、重名 request。
				goodReq := &claim.Spec.Devices.Requests[0]
				goodReq.Name = "foo"
				goodReq.Exactly.AllocationMode = resource.DeviceAllocationModeExactCount
				goodReq.Exactly.Count = 1

				req := goodReq.DeepCopy()
				req.Name += "2"
				req.Exactly.AllocationMode = resource.DeviceAllocationModeAll
				req.Exactly.Count = 0
				claim.Spec.Devices.Requests = append(claim.Spec.Devices.Requests, *req)

				req = goodReq.DeepCopy()
				req.Name += "3"
				req.Exactly.AllocationMode = resource.DeviceAllocationModeExactCount
				// Count 必须为正数，否则应报错。
				req.Exactly.Count = -1
				claim.Spec.Devices.Requests = append(claim.Spec.Devices.Requests, *req)

				req = goodReq.DeepCopy()
				req.Name += "4"
				// 未知分配模式，应被 NotSupported 拒绝。
				req.Exactly.AllocationMode = resource.DeviceAllocationMode("other")
				claim.Spec.Devices.Requests = append(claim.Spec.Devices.Requests, *req)

				req = goodReq.DeepCopy()
				req.Name += "5"
				// All 模式不允许指定 Count，这里故意设置非零值以触发错误。
				req.Exactly.AllocationMode = resource.DeviceAllocationModeAll
				req.Exactly.Count = 2
				claim.Spec.Devices.Requests = append(claim.Spec.Devices.Requests, *req)

				req = goodReq.DeepCopy()
				// 名称相同，因此应被判为重复项。
				goodReq.Name = "foo"
				claim.Spec.Devices.Requests = append(claim.Spec.Devices.Requests, *req)

				return claim
			}(),
		},
		"configuration": {
			wantFailures: field.ErrorList{
				field.Required(field.NewPath("spec", "devices", "config").Index(0).Child("opaque", "parameters"), ""),
				field.Invalid(field.NewPath("spec", "devices", "config").Index(1).Child("opaque", "parameters"), "<value omitted>", "error parsing data as JSON: unexpected end of JSON input"),
				field.Invalid(field.NewPath("spec", "devices", "config").Index(2).Child("opaque", "parameters"), "<value omitted>", "must be a valid JSON object"),
				field.Required(field.NewPath("spec", "devices", "config").Index(3).Child("opaque", "parameters"), ""),
				field.TooLong(field.NewPath("spec", "devices", "config").Index(5).Child("opaque", "parameters"), "" /* 该参数未被使用，仅占位。 */, resource.OpaqueParametersMaxLength),
			},
			claim: func() *resource.ResourceClaim {
				claim := testClaim(goodName, goodNS, validClaimSpec)
				claim.Spec.Devices.Config = []resource.DeviceClaimConfiguration{
					{
						DeviceConfiguration: resource.DeviceConfiguration{
							Opaque: &resource.OpaqueDeviceConfiguration{
								Driver: "dra.example.com",
								Parameters: runtime.RawExtension{
									Raw: []byte(``),
								},
							},
						},
					},
					{
						DeviceConfiguration: resource.DeviceConfiguration{
							Opaque: &resource.OpaqueDeviceConfiguration{
								Driver: "dra.example.com",
								Parameters: runtime.RawExtension{
									// 不完整的 JSON，用于验证 JSON 解析错误。
									Raw: []byte(`{`),
								},
							},
						},
					},
					{
						DeviceConfiguration: resource.DeviceConfiguration{
							Opaque: &resource.OpaqueDeviceConfiguration{
								Driver: "dra.example.com",
								Parameters: runtime.RawExtension{
									// 是合法 JSON 但不是对象，应被拒绝。
									Raw: []byte(`"hello-world"`),
								},
							},
						},
					},
					{
						DeviceConfiguration: resource.DeviceConfiguration{
							Opaque: &resource.OpaqueDeviceConfiguration{
								Driver: "dra.example.com",
								Parameters: runtime.RawExtension{
									// JSON null 不代表对象，应被拒绝。
									Raw: []byte(`null`),
								},
							},
						},
					},
					{
						DeviceConfiguration: resource.DeviceConfiguration{
							Opaque: &resource.OpaqueDeviceConfiguration{
								Driver: "dra.example.com",
								Parameters: runtime.RawExtension{
									Raw: []byte(`{"str": "` + strings.Repeat("x", resource.OpaqueParametersMaxLength-9-2) + `"}`),
								},
							},
						},
					},
					{
						DeviceConfiguration: resource.DeviceConfiguration{
							Opaque: &resource.OpaqueDeviceConfiguration{
								Driver: "dra.example.com",
								Parameters: runtime.RawExtension{
									// 比长度上限多一个字节，验证边界值检查。
									Raw: []byte(`{"str": "` + strings.Repeat("x", resource.OpaqueParametersMaxLength-9-2+1 /* too large by one */) + `"}`),
								},
							},
						},
					},
				}
				return claim
			}(),
		},
		"CEL-compile-errors": {
			wantFailures: field.ErrorList{
				field.Invalid(field.NewPath("spec", "devices", "requests").Index(1).Child("exactly", "selectors").Index(1).Child("cel", "expression"), `device.attributes[true].someBoolean`, "compilation failed: ERROR: <input>:1:18: found no matching overload for '_[_]' applied to '(map(string, map(string, any)), bool)'\n | device.attributes[true].someBoolean\n | .................^"),
			},
			claim: func() *resource.ResourceClaim {
				claim := testClaim(goodName, goodNS, validClaimSpec)
				claim.Spec.Devices.Requests = append(claim.Spec.Devices.Requests, *claim.Spec.Devices.Requests[0].DeepCopy())
				claim.Spec.Devices.Requests[1].Name += "-2"
				claim.Spec.Devices.Requests[1].Exactly.Selectors = []resource.DeviceSelector{
					{
						// 合法选择器，不应产生错误。
						CEL: &resource.CELDeviceSelector{
							Expression: `device.driver == "dra.example.com"`,
						},
					},
					{
						// 非法选择器，用于触发编译错误。
						CEL: &resource.CELDeviceSelector{
							Expression: `device.attributes[true].someBoolean`,
						},
					},
				}
				return claim
			}(),
		},
		"CEL-length": {
			wantFailures: field.ErrorList{
				field.TooLong(field.NewPath("spec", "devices", "requests").Index(1).Child("exactly", "selectors").Index(1).Child("cel", "expression"), "" /* 该参数未被使用，仅占位。 */, resource.CELSelectorExpressionMaxLength),
			},
			claim: func() *resource.ResourceClaim {
				claim := testClaim(goodName, goodNS, validClaimSpec)
				claim.Spec.Devices.Requests = append(claim.Spec.Devices.Requests, *claim.Spec.Devices.Requests[0].DeepCopy())
				claim.Spec.Devices.Requests[1].Name += "-2"
				expression := `device.driver == ""`
				claim.Spec.Devices.Requests[1].Exactly.Selectors = []resource.DeviceSelector{
					{
						// 合法选择器：长度恰好在上限之内。
						CEL: &resource.CELDeviceSelector{
							Expression: strings.ReplaceAll(expression, `""`, `"`+strings.Repeat("x", resource.CELSelectorExpressionMaxLength-len(expression))+`"`),
						},
					},
					{
						// 超出长度上限一个字符，应报错。
						CEL: &resource.CELDeviceSelector{
							Expression: strings.ReplaceAll(expression, `""`, `"`+strings.Repeat("x", resource.CELSelectorExpressionMaxLength-len(expression)+1)+`"`),
						},
					},
				}
				return claim
			}(),
		},
		"CEL-cost": {
			wantFailures: field.ErrorList{
				field.Forbidden(field.NewPath("spec", "devices", "requests").Index(0).Child("exactly", "selectors").Index(0).Child("cel", "expression"), "too complex, exceeds cost limit"),
			},
			claim: func() *resource.ResourceClaim {
				claim := testClaim(goodName, goodNS, validClaimSpec)
				claim.Spec.Devices.Requests[0].Exactly.Selectors = []resource.DeviceSelector{
					{
						CEL: &resource.CELDeviceSelector{
							// 取自 Kubernetes CEL 集成测试中的高成本表达式，用于触发 cost 上限。
							Expression: `[1, 2, 3, 4, 5, 6, 7, 8, 9, 10].all(x, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10].all(y, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10].all(z, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10].all(z2, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10].all(z3, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10].all(z4, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10].all(z5, int('1'.find('[0-9]*')) < 100)))))))`,
						},
					},
				}
				return claim
			}(),
		},
		"derived-attributes-valid": {
			claim: &resource.ResourceClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name:      goodName,
					Namespace: goodNS,
				},
				Spec: resource.ResourceClaimSpec{
					Devices: resource.DeviceClaim{
						Requests: []resource.DeviceRequest{
							{
								Name: goodName,
								Exactly: &resource.ExactDeviceRequest{
									DeviceClassName: goodName,
									AllocationMode:  resource.DeviceAllocationModeExactCount,
									Count:           1,
									DerivedAttributes: []resource.DeviceDerivedAttribute{
										{
											// 派生属性名带 derived/ 前缀，可被约束按名称引用。
											Name:       "derived/sharedNumaNode",
											Expression: `device.attributes["dra.example.com"]["numa"]`,
										},
									},
								},
							},
						},
						Constraints: []resource.DeviceConstraint{
							{
								Requests: []string{goodName},
								// 约束引用上面定义的派生属性，因此该属性是“被使用”的。
								MatchAttribute: ptr.To(resource.FullyQualifiedName("derived/sharedNumaNode")),
							},
						},
					},
				},
			},
		},
		"derived-attributes-valid-dotted-domain": {
			claim: &resource.ResourceClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name:      goodName,
					Namespace: goodNS,
				},
				Spec: resource.ResourceClaimSpec{
					Devices: resource.DeviceClaim{
						Requests: []resource.DeviceRequest{
							{
								Name: goodName,
								Exactly: &resource.ExactDeviceRequest{
									DeviceClassName: goodName,
									AllocationMode:  resource.DeviceAllocationModeExactCount,
									Count:           1,
									DerivedAttributes: []resource.DeviceDerivedAttribute{
										{
											Name:       "dra.example.com/sharedNumaNode",
											Expression: `device.attributes["dra.example.com"]["numa"]`,
										},
									},
								},
							},
						},
						Constraints: []resource.DeviceConstraint{
							{
								Requests:       []string{goodName},
								MatchAttribute: ptr.To(resource.FullyQualifiedName("dra.example.com/sharedNumaNode")),
							},
						},
					},
				},
			},
		},
		"derived-attributes-valid-global-constraint": {
			claim: &resource.ResourceClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name:      goodName,
					Namespace: goodNS,
				},
				Spec: resource.ResourceClaimSpec{
					Devices: resource.DeviceClaim{
						Requests: []resource.DeviceRequest{
							{
								Name: goodName,
								Exactly: &resource.ExactDeviceRequest{
									DeviceClassName: goodName,
									AllocationMode:  resource.DeviceAllocationModeExactCount,
									Count:           1,
									DerivedAttributes: []resource.DeviceDerivedAttribute{
										{
											Name:       "derived/sharedNumaNode",
											Expression: `device.attributes["dra.example.com"]["numa"]`,
										},
									},
								},
							},
						},
						Constraints: []resource.DeviceConstraint{
							{
								// requests 为空表示全局约束，作用于该 claim 的所有 request。
								MatchAttribute: ptr.To(resource.FullyQualifiedName("derived/sharedNumaNode")),
							},
						},
					},
				},
			},
		},
		"derived-attributes-valid-parent-constraint": {
			claim: &resource.ResourceClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name:      goodName,
					Namespace: goodNS,
				},
				Spec: resource.ResourceClaimSpec{
					Devices: resource.DeviceClaim{
						Requests: []resource.DeviceRequest{
							{
								Name: goodName,
								FirstAvailable: []resource.DeviceSubRequest{
									{
										Name:            "subreq",
										DeviceClassName: goodName,
										AllocationMode:  resource.DeviceAllocationModeExactCount,
										Count:           1,
										DerivedAttributes: []resource.DeviceDerivedAttribute{
											{
												Name:       "derived/sharedNumaNode",
												Expression: `device.attributes["dra.example.com"]["numa"]`,
											},
										},
									},
								},
							},
						},
						Constraints: []resource.DeviceConstraint{
							{
								Requests:       []string{goodName}, // 父级约束作用于该 request 下的所有子请求。
								MatchAttribute: ptr.To(resource.FullyQualifiedName("derived/sharedNumaNode")),
							},
						},
					},
				},
			},
		},
		"derived-attributes-unreferenced": {
			wantFailures: field.ErrorList{
				field.Invalid(field.NewPath("spec", "devices", "requests").Index(0).Child("exactly", "derivedAttributes").Index(0).Child("name"), resource.FullyQualifiedName("derived/sharedNumaNode"), "must be referenced by at least one matchAttribute or distinctAttribute constraint targeting this request"),
			},
			claim: func() *resource.ResourceClaim {
				claim := testClaim(goodName, goodNS, validClaimSpec)
				claim.Spec.Devices.Requests[0].Exactly.DerivedAttributes = []resource.DeviceDerivedAttribute{
					{
						// 校验要求每个派生属性至少被一条针对该 request 的约束引用，否则报错。
						Name:       resource.FullyQualifiedName("derived/sharedNumaNode"), // 未被任何约束引用的派生属性。
						Expression: `device.attributes["dra.example.com"]["numa"]`,
					},
				}
				return claim
			}(),
		},
		"derived-attributes-unreferenced-wrong-request-constraint": {
			wantFailures: field.ErrorList{
				field.Invalid(field.NewPath("spec", "devices", "requests").Index(0).Child("exactly", "derivedAttributes").Index(0).Child("name"), resource.FullyQualifiedName("derived/sharedNumaNode"), "must be referenced by at least one matchAttribute or distinctAttribute constraint targeting this request"),
			},
			claim: func() *resource.ResourceClaim {
				claim := testClaim(goodName, goodNS, validClaimSpec)
				claim.Spec.Devices.Requests = append(claim.Spec.Devices.Requests, resource.DeviceRequest{
					Name: goodName2,
					Exactly: &resource.ExactDeviceRequest{
						DeviceClassName: goodName,
						AllocationMode:  resource.DeviceAllocationModeExactCount,
						Count:           1,
					},
				})
				claim.Spec.Devices.Requests[0].Exactly.DerivedAttributes = []resource.DeviceDerivedAttribute{
					{
						Name:       resource.FullyQualifiedName("derived/sharedNumaNode"),
						Expression: `device.attributes["dra.example.com"]["numa"]`,
					},
				}
				claim.Spec.Devices.Constraints = []resource.DeviceConstraint{
					{
						// 约束的作用对象必须与被检查的 request 一致，否则不算引用。
						Requests:       []string{goodName2}, // 该约束指向 goodName2，而非 goodName，因此派生属性仍未被引用。
						MatchAttribute: ptr.To(resource.FullyQualifiedName("derived/sharedNumaNode")),
					},
				}
				return claim
			}(),
		},
		"derived-attributes-duplicate-name": {
			wantFailures: field.ErrorList{
				field.Duplicate(field.NewPath("spec", "devices", "requests").Index(0).Child("exactly", "derivedAttributes").Index(1), "derived/sharedNumaNode"),
			},
			claim: func() *resource.ResourceClaim {
				claim := testClaim(goodName, goodNS, validClaimSpec)
				claim.Spec.Devices.Requests[0].Exactly.DerivedAttributes = []resource.DeviceDerivedAttribute{
					{
						Name:       "derived/sharedNumaNode",
						Expression: `device.attributes["dra.example.com"]["numa"]`,
					},
					{
						// 同一 request 内派生属性名必须唯一，否则调度时无法确定取值。
						Name:       "derived/sharedNumaNode", // 名称重复。
						Expression: `device.attributes["dra.example.com"]["socket"]`,
					},
				}
				claim.Spec.Devices.Constraints = []resource.DeviceConstraint{
					{
						Requests:       []string{goodName},
						MatchAttribute: ptr.To(resource.FullyQualifiedName("derived/sharedNumaNode")),
					},
				}
				return claim
			}(),
		},
		"derived-attributes-CEL-compile-errors": {
			wantFailures: field.ErrorList{
				field.Invalid(field.NewPath("spec", "devices", "requests").Index(0).Child("exactly", "derivedAttributes").Index(0).Child("expression"), `device.attributes[true].someBoolean`, "compilation failed: ERROR: <input>:1:18: found no matching overload for '_[_]' applied to '(map(string, map(string, any)), bool)'\n | device.attributes[true].someBoolean\n | .................^"),
			},
			claim: func() *resource.ResourceClaim {
				claim := testClaim(goodName, goodNS, validClaimSpec)
				claim.Spec.Devices.Requests[0].Exactly.DerivedAttributes = []resource.DeviceDerivedAttribute{
					{
						Name: "derived/sharedNumaNode",
						// CEL 编译期即可发现类型错误，无需等到运行时。
						Expression: `device.attributes[true].someBoolean`, // map 的键类型非法：这里用了 boolean，应为 string。
					},
				}
				claim.Spec.Devices.Constraints = []resource.DeviceConstraint{
					{
						Requests:       []string{goodName},
						MatchAttribute: ptr.To(resource.FullyQualifiedName("derived/sharedNumaNode")),
					},
				}
				return claim
			}(),
		},
		"derived-attributes-CEL-invalid-type": {
			wantFailures: field.ErrorList{
				field.Invalid(field.NewPath("spec", "devices", "requests").Index(0).Child("exactly", "derivedAttributes").Index(0).Child("expression"), `device.attributes`, "must evaluate to a primitive scalar (string, integer, boolean, semver), not map(string, map(string, google.protobuf.Any))"),
			},
			claim: func() *resource.ResourceClaim {
				claim := testClaim(goodName, goodNS, validClaimSpec)
				claim.Spec.Devices.Requests[0].Exactly.DerivedAttributes = []resource.DeviceDerivedAttribute{
					{
						Name: "derived/sharedNumaNode",
						// 派生属性只能求值为基本标量（或开启 gate 时的列表），以便约束直接比较。
						Expression: `device.attributes`, // 返回类型非法：map 不是允许的基本标量或列表。
					},
				}
				claim.Spec.Devices.Constraints = []resource.DeviceConstraint{
					{
						Requests:       []string{goodName},
						MatchAttribute: ptr.To(resource.FullyQualifiedName("derived/sharedNumaNode")),
					},
				}
				return claim
			}(),
		},
		"derived-attributes-CEL-length": {
			wantFailures: field.ErrorList{
				field.TooLong(field.NewPath("spec", "devices", "requests").Index(0).Child("exactly", "derivedAttributes").Index(0).Child("expression"), "" /* 该参数未被使用，仅占位。 */, resource.CELSelectorExpressionMaxLength),
			},
			claim: func() *resource.ResourceClaim {
				claim := testClaim(goodName, goodNS, validClaimSpec)
				expression := `device.driver == ""`
				claim.Spec.Devices.Requests[0].Exactly.DerivedAttributes = []resource.DeviceDerivedAttribute{
					{
						Name: "derived/sharedNumaNode",
						// 用字符串替换构造精确长度，避免手工计算出错。
						// 表达式长度超过 1023 字符的上限。
						Expression: strings.ReplaceAll(expression, `""`, `"`+strings.Repeat("x", resource.CELSelectorExpressionMaxLength-len(expression)+1)+`"`),
					},
				}
				claim.Spec.Devices.Constraints = []resource.DeviceConstraint{
					{
						Requests:       []string{goodName},
						MatchAttribute: ptr.To(resource.FullyQualifiedName("derived/sharedNumaNode")),
					},
				}
				return claim
			}(),
		},

		// derived-attributes-CEL-cost-under-limit 验证开销约 50 万至 60 万的表达式仍然合法：
		// 它低于 1,000,000 的预算上限。
		"derived-attributes-CEL-cost-under-limit": {
			claim: func() *resource.ResourceClaim {
				claim := testClaim(goodName, goodNS, validClaimSpec)
				claim.Spec.Devices.Requests[0].Exactly.DerivedAttributes = []resource.DeviceDerivedAttribute{
					{
						Name: "derived/sharedNumaNode1",
						// 该表达式对长度为 10 的数组使用了 5 层嵌套 .all() 循环。
						// 每嵌套一层，开销估算乘以 10，单个表达式的复杂度约为
						// 50 万至 60 万，远低于 1,000,000 的预算上限。
						Expression: `[1, 2, 3, 4, 5, 6, 7, 8, 9, 10].all(x, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10].all(y, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10].all(z, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10].all(z2, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10].all(z3, int('1'.find('[0-9]*')) < 100)))))`,
					},
				}
				claim.Spec.Devices.Constraints = []resource.DeviceConstraint{
					{
						Requests:       []string{goodName},
						MatchAttribute: ptr.To(resource.FullyQualifiedName("derived/sharedNumaNode1")),
					},
				}
				return claim
			}(),
		},
		"derived-attributes-CEL-cost-per-expression-exceeds-limit": {
			wantFailures: field.ErrorList{
				field.Forbidden(field.NewPath("spec", "devices", "requests").Index(0).Child("exactly", "derivedAttributes").Index(0).Child("expression"), "too complex, exceeds cost limit"),
				field.Forbidden(field.NewPath("spec", "devices"), "too complex, total cost of derived attribute CEL expressions in the claim exceeds cost limit"),
			},
			claim: func() *resource.ResourceClaim {
				claim := testClaim(goodName, goodNS, validClaimSpec)
				claim.Spec.Devices.Requests[0].Exactly.DerivedAttributes = []resource.DeviceDerivedAttribute{
					{
						Name: "derived/sharedNumaNode1",
						// 该表达式使用 7 层嵌套 .all() 循环，超过单表达式 1,000,000 的开销预算。
						Expression: `[1, 2, 3, 4, 5, 6, 7, 8, 9, 10].all(x, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10].all(y, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10].all(z, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10].all(z2, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10].all(z3, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10].all(z4, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10].all(z5, int('1'.find('[0-9]*')) < 100)))))))`,
					},
				}
				claim.Spec.Devices.Constraints = []resource.DeviceConstraint{
					{
						Requests:       []string{goodName},
						MatchAttribute: ptr.To(resource.FullyQualifiedName("derived/sharedNumaNode1")),
					},
				}
				return claim
			}(),
		},
		// derived-attributes-CEL-cost-exceeds-limit 验证派生属性 CEL 表达式的开销上限是
		// 整个 DeviceClaim 共享的预算，而不是每个属性各自独立计算。
		// 这里定义了两个派生属性，每个表达式的开销单独都未超限，但两者合计超出了上限。
		"derived-attributes-CEL-cost-exceeds-limit": {
			wantFailures: field.ErrorList{
				field.Forbidden(field.NewPath("spec", "devices"), "too complex, total cost of derived attribute CEL expressions in the claim exceeds cost limit"),
			},
			claim: func() *resource.ResourceClaim {
				claim := testClaim(goodName, goodNS, validClaimSpec)
				claim.Spec.Devices.Requests[0].Exactly.DerivedAttributes = []resource.DeviceDerivedAttribute{
					{
						Name: "derived/sharedNumaNode1",
						// 该表达式对长度为 10 的数组使用了 5 层嵌套 .all() 循环。
						// 每嵌套一层，开销估算乘以 10，单个表达式的复杂度约为
						// 50 万至 60 万，低于 1,000,000 的预算上限。
						Expression: `[1, 2, 3, 4, 5, 6, 7, 8, 9, 10].all(x, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10].all(y, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10].all(z, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10].all(z2, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10].all(z3, int('1'.find('[0-9]*')) < 100)))))`,
					},
					{
						Name: "derived/sharedNumaNode2",
						// 与上一个表达式完全相同：单独使用合法，但与前一个表达式的开销合计后，
						// 整个 claim 内派生属性的总开销超过了 1,000,000 的上限。
						Expression: `[1, 2, 3, 4, 5, 6, 7, 8, 9, 10].all(x, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10].all(y, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10].all(z, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10].all(z2, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10].all(z3, int('1'.find('[0-9]*')) < 100)))))`,
					},
				}
				claim.Spec.Devices.Constraints = []resource.DeviceConstraint{
					{
						Requests:       []string{goodName},
						MatchAttribute: ptr.To(resource.FullyQualifiedName("derived/sharedNumaNode1")),
					},
					{
						Requests:       []string{goodName},
						MatchAttribute: ptr.To(resource.FullyQualifiedName("derived/sharedNumaNode2")),
					},
				}
				return claim
			}(),
		},
		"derived-attributes-list-type-disabled": {
			wantFailures: field.ErrorList{
				field.Invalid(field.NewPath("spec", "devices", "requests").Index(0).Child("exactly", "derivedAttributes").Index(0).Child("expression"), `[1, 2, 3]`, "must evaluate to a primitive scalar (string, integer, boolean, semver), not list(int)"),
			},
			claim: func() *resource.ResourceClaim {
				claim := testClaim(goodName, goodNS, validClaimSpec)
				claim.Spec.Devices.Requests[0].Exactly.DerivedAttributes = []resource.DeviceDerivedAttribute{
					{
						Name: "derived/sharedNumaNode",
						// 同一个表达式在开关不同时应得到不同结论，用来验证 gate 生效。
						Expression: `[1, 2, 3]`, // 列表返回类型：DRAListTypeAttributes feature gate 关闭时应被拒绝。
					},
				}
				claim.Spec.Devices.Constraints = []resource.DeviceConstraint{
					{
						Requests:       []string{goodName},
						MatchAttribute: ptr.To(resource.FullyQualifiedName("derived/sharedNumaNode")),
					},
				}
				return claim
			}(),
			listTypeAttributesFeatureGate: false,
		},
		"derived-attributes-list-type-enabled": {
			claim: func() *resource.ResourceClaim {
				claim := testClaim(goodName, goodNS, validClaimSpec)
				claim.Spec.Devices.Requests[0].Exactly.DerivedAttributes = []resource.DeviceDerivedAttribute{
					{
						Name:       "derived/sharedNumaNode",
						Expression: `[1, 2, 3]`, // 列表返回类型：DRAListTypeAttributes feature gate 开启时允许。
					},
				}
				claim.Spec.Devices.Constraints = []resource.DeviceConstraint{
					{
						Requests:       []string{goodName},
						MatchAttribute: ptr.To(resource.FullyQualifiedName("derived/sharedNumaNode")),
					},
				}
				return claim
			}(),
			listTypeAttributesFeatureGate: true,
		},
		// firstAvailable 表示按优先级尝试多个子请求，与 exactly 互斥。
		"prioritized-list-valid": {
			wantFailures: nil,
			claim: func() *resource.ResourceClaim {
				claim := testClaim(goodName, goodNS, validClaimSpecWithFirstAvailable)
				return claim
			}(),
		},
		"prioritized-list-both-first-available-and-exactly-set": {
			wantFailures: field.ErrorList{
				field.Invalid(field.NewPath("spec", "devices", "requests").Index(0), nil, "exactly one of `exactly` or `firstAvailable` is required, but multiple fields are set"),
			},
			claim: func() *resource.ResourceClaim {
				claim := testClaim(goodName, goodNS, validClaimSpecWithFirstAvailable)

				claim.Spec.Devices.Requests[0].Exactly = &resource.ExactDeviceRequest{
					DeviceClassName: goodName,
					Selectors:       validSelector,
					AllocationMode:  resource.DeviceAllocationModeExactCount,
					Count:           2,
					AdminAccess:     ptr.To(true),
				}
				return claim
			}(),
		},
		"first-available-with-zero-exactly-fails-old-validation": {
			wantFailures: field.ErrorList{
				field.Invalid(field.NewPath("spec", "devices", "requests").Index(0), nil, "exactly one of `exactly` or `firstAvailable` is required, but multiple fields are set"),
			},
			claim: func() *resource.ResourceClaim {
				claim := testClaim(goodName, goodNS, validClaimSpecWithFirstAvailable)
				// 即使是零值 Exactly，只要非空就意味着同时设置了两个互斥字段。
				claim.Spec.Devices.Requests[0].Exactly = &resource.ExactDeviceRequest{}
				return claim
			}(),
		},
		"prioritized-list-invalid-nested-request": {
			wantFailures: field.ErrorList{
				field.Invalid(field.NewPath("spec", "devices", "requests").Index(0).Child("firstAvailable").Index(0).Child("name"), badName, "a lowercase RFC 1123 label must consist of lower case alphanumeric characters or '-', and must start and end with an alphanumeric character (e.g. 'my-name',  or '123-abc', regex used for validation is '[a-z0-9]([-a-z0-9]*[a-z0-9])?')"),
				field.Required(field.NewPath("spec", "devices", "requests").Index(0).Child("firstAvailable").Index(0).Child("deviceClassName"), "").MarkCoveredByDeclarative(),
				field.NotSupported(field.NewPath("spec", "devices", "requests").Index(0).Child("firstAvailable").Index(0).Child("allocationMode"), resource.DeviceAllocationMode(""), []resource.DeviceAllocationMode{resource.DeviceAllocationModeAll, resource.DeviceAllocationModeExactCount}).MarkCoveredByDeclarative(),
			},
			claim: func() *resource.ResourceClaim {
				claim := testClaim(goodName, goodNS, validClaimSpecWithFirstAvailable)
				claim.Spec.Devices.Requests[0].FirstAvailable[0] = resource.DeviceSubRequest{
					Name: badName,
				}
				return claim
			}(),
		},
		"prioritized-list-nested-requests-same-name": {
			wantFailures: field.ErrorList{
				field.Duplicate(field.NewPath("spec", "devices", "requests").Index(0).Child("firstAvailable").Index(1), "foo").MarkCoveredByDeclarative(),
			},
			claim: func() *resource.ResourceClaim {
				claim := testClaim(goodName, goodNS, validClaimSpecWithFirstAvailable)
				claim.Spec.Devices.Requests[0].FirstAvailable[1].Name = goodName
				return claim
			}(),
		},
		"prioritized-list-too-many-subrequests": {
			wantFailures: field.ErrorList{
				field.TooMany(field.NewPath("spec", "devices", "requests").Index(0).Child("firstAvailable"), 9, 8).MarkCoveredByDeclarative(),
			},
			claim: func() *resource.ResourceClaim {
				claim := testClaim(goodName, goodNS, validClaimSpec)
				claim.Spec.Devices.Requests[0].Exactly = nil
				var subRequests []resource.DeviceSubRequest
				// 生成 9 个子请求，超过 firstAvailable 的 8 个上限。
				for i := 0; i <= 8; i++ {
					subRequests = append(subRequests, resource.DeviceSubRequest{
						Name:            fmt.Sprintf("subreq-%d", i),
						DeviceClassName: goodName,
						AllocationMode:  resource.DeviceAllocationModeExactCount,
						Count:           1,
					})
				}
				claim.Spec.Devices.Requests[0].FirstAvailable = subRequests
				return claim
			}(),
		},
		"prioritized-list-config-requests-with-subrequest-reference": {
			wantFailures: nil,
			claim: func() *resource.ResourceClaim {
				claim := testClaim(goodName, goodNS, validClaimSpecWithFirstAvailable)
				claim.Spec.Devices.Config = []resource.DeviceClaimConfiguration{
					{
						Requests: []string{"foo/bar"},
						DeviceConfiguration: resource.DeviceConfiguration{
							Opaque: &resource.OpaqueDeviceConfiguration{
								Driver: "dra.example.com",
								Parameters: runtime.RawExtension{
									Raw: []byte(`{"kind": "foo", "apiVersion": "dra.example.com/v1"}`),
								},
							},
						},
					},
				}
				return claim
			}(),
		},
		"prioritized-list-config-requests-with-parent-request-reference": {
			wantFailures: nil,
			claim: func() *resource.ResourceClaim {
				claim := testClaim(goodName, goodNS, validClaimSpecWithFirstAvailable)
				claim.Spec.Devices.Config = []resource.DeviceClaimConfiguration{
					{
						Requests: []string{"foo"},
						DeviceConfiguration: resource.DeviceConfiguration{
							Opaque: &resource.OpaqueDeviceConfiguration{
								Driver: "dra.example.com",
								Parameters: runtime.RawExtension{
									Raw: []byte(`{"kind": "foo", "apiVersion": "dra.example.com/v1"}`),
								},
							},
						},
					},
				}
				return claim
			}(),
		},
		"prioritized-list-config-requests-with-invalid-subrequest-reference": {
			wantFailures: field.ErrorList{field.Invalid(field.NewPath("spec", "devices", "config").Index(0).Child("requests").Index(0), "foo/baz", "must be the name of a request in the claim or the name of a request and a subrequest separated by '/'")},
			claim: func() *resource.ResourceClaim {
				claim := testClaim(goodName, goodNS, validClaimSpecWithFirstAvailable)
				claim.Spec.Devices.Config = []resource.DeviceClaimConfiguration{
					{
						Requests: []string{"foo/baz"},
						DeviceConfiguration: resource.DeviceConfiguration{
							Opaque: &resource.OpaqueDeviceConfiguration{
								Driver: "dra.example.com",
								Parameters: runtime.RawExtension{
									Raw: []byte(`{"kind": "foo", "apiVersion": "dra.example.com/v1"}`),
								},
							},
						},
					},
				}
				return claim
			}(),
		},
		"prioritized-list-constraints-requests-with-subrequest-reference": {
			wantFailures: nil,
			claim: func() *resource.ResourceClaim {
				claim := testClaim(goodName, goodNS, validClaimSpecWithFirstAvailable)
				claim.Spec.Devices.Constraints = []resource.DeviceConstraint{
					{
						Requests:       []string{"foo/bar"},
						MatchAttribute: ptr.To(resource.FullyQualifiedName("dra.example.com/driverVersion")),
					},
				}
				return claim
			}(),
		},
		"prioritized-list-constraints-requests-with-parent-request-reference": {
			wantFailures: nil,
			claim: func() *resource.ResourceClaim {
				claim := testClaim(goodName, goodNS, validClaimSpecWithFirstAvailable)
				claim.Spec.Devices.Constraints = []resource.DeviceConstraint{
					{
						Requests:       []string{"foo"},
						MatchAttribute: ptr.To(resource.FullyQualifiedName("dra.example.com/driverVersion")),
					},
				}
				return claim
			}(),
		},
		"prioritized-list-constraints-requests-with-invalid-subrequest-reference": {
			wantFailures: field.ErrorList{field.Invalid(field.NewPath("spec", "devices", "constraints").Index(0).Child("requests").Index(0), "foo/baz", "must be the name of a request in the claim or the name of a request and a subrequest separated by '/'")},
			claim: func() *resource.ResourceClaim {
				claim := testClaim(goodName, goodNS, validClaimSpecWithFirstAvailable)
				claim.Spec.Devices.Constraints = []resource.DeviceConstraint{
					{
						Requests:       []string{"foo/baz"},
						MatchAttribute: ptr.To(resource.FullyQualifiedName("dra.example.com/driverVersion")),
					},
				}
				return claim
			}(),
		},
		"tolerations": {
			wantFailures: func() field.ErrorList {
				var allErrs field.ErrorList
				fldPath := field.NewPath("spec", "devices", "requests").Index(0).Child("firstAvailable").Index(0).Child("tolerations")
				allErrs = append(allErrs,
					field.Required(fldPath.Index(0).Child("operator"), "").MarkCoveredByDeclarative(),
				)
				fldPath = field.NewPath("spec", "devices", "requests").Index(1).Child("exactly", "tolerations")
				allErrs = append(allErrs,
					field.Required(fldPath.Index(3).Child("operator"), "").MarkCoveredByDeclarative(),

					field.NotSupported(fldPath.Index(4).Child("operator"), resource.DeviceTolerationOperator("some-other-op"), []resource.DeviceTolerationOperator{resource.DeviceTolerationOpEqual, resource.DeviceTolerationOpExists}).MarkCoveredByDeclarative(),

					field.Invalid(fldPath.Index(5).Child("key"), badName, "name part must consist of alphanumeric characters, '-', '_' or '.', and must start and end with an alphanumeric character (e.g. 'MyName',  or 'my.name',  or '123-abc', regex used for validation is '([A-Za-z0-9][-A-Za-z0-9_.]*)?[A-Za-z0-9]')").MarkCoveredByDeclarative(),
					field.Invalid(fldPath.Index(5).Child("value"), badName, "a valid label must be an empty string or consist of alphanumeric characters, '-', '_' or '.', and must start and end with an alphanumeric character (e.g. 'MyValue',  or 'my_value',  or '12345', regex used for validation is '(([A-Za-z0-9][-A-Za-z0-9_.]*)?[A-Za-z0-9])?')"),
					field.NotSupported(fldPath.Index(5).Child("effect"), resource.DeviceTaintEffect("some-other-effect"), []resource.DeviceTaintEffect{resource.DeviceTaintEffectNoExecute, resource.DeviceTaintEffectNoSchedule, resource.DeviceTaintEffectNone}).MarkCoveredByDeclarative(),
				)
				return allErrs
			}(),
			claim: func() *resource.ResourceClaim {
				claim := testClaim(goodName, goodNS, validClaimSpecWithFirstAvailable)
				claim.Spec.Devices.Requests[0].FirstAvailable[0].Tolerations = []resource.DeviceToleration{
					{
						// 这里只放一个非法用例，用于校验字段路径是否正确；
						// validateDeviceToleration 的完整测试见下文。
						Operator: "",
					},
				}
				claim.Spec.Devices.Requests = append(claim.Spec.Devices.Requests, *validClaimSpec.Devices.Requests[0].DeepCopy())
				// 第二个 request 用于集中放置 toleration 的各类非法值。
				claim.Spec.Devices.Requests[1].Name += "-other"
				claim.Spec.Devices.Requests[1].Exactly.Tolerations = []resource.DeviceToleration{
					{
						// operator 为 Exists 时无需 key/value，是校验允许的最小组合。
						// 最小合法 toleration：匹配所有 taint。
						Operator: resource.DeviceTolerationOpExists,
					},
					{
						Key:      "example.com/taint",
						Operator: resource.DeviceTolerationOpExists,
						Effect:   resource.DeviceTaintEffectNoExecute,
					},
					{
						Key:      "example.com/taint",
						Operator: resource.DeviceTolerationOpEqual,
						Value:    "tainted",
						Effect:   resource.DeviceTaintEffectNoSchedule,
					},
					{
						// 非法：operator 为必填字段。
						Operator: "",
					},
					{
						Key:      goodName,
						Operator: "some-other-op",
					},
					{
						Key:      badName,
						Operator: resource.DeviceTolerationOpEqual,
						Value:    badName,
						Effect:   "some-other-effect",
					},
				}
				return claim
			}(),
		},
	}

	// feature gate 按场景显式设置，避免测试间的全局状态相互干扰。
	for name, scenario := range scenarios {
		t.Run(name, func(t *testing.T) {
			featuregatetesting.SetFeatureGatesDuringTest(t, utilfeature.DefaultFeatureGate, featuregatetesting.FeatureOverrides{
				features.DRAConsumableCapacity: scenario.consumableCapacityFeatureGate,
				features.DRAListTypeAttributes: scenario.listTypeAttributesFeatureGate,
			})
			errs := ValidateResourceClaim(scenario.claim)
			assertFailures(t, scenario.wantFailures, errs)
		})
	}
}

// TestValidateClaimUpdate 验证 spec 在更新时不可变，
// 以及历史遗留的超长配置在未被修改时仍然允许通过。
func TestValidateClaimUpdate(t *testing.T) {
	scenarios := map[string]struct {
		oldClaim     *resource.ResourceClaim
		update       func(claim *resource.ResourceClaim) *resource.ResourceClaim
		wantFailures field.ErrorList
	}{
		"valid-no-op-update": {
			oldClaim: validClaim,
			update:   func(claim *resource.ResourceClaim) *resource.ResourceClaim { return claim },
		},
		"invalid-update": {
			wantFailures: field.ErrorList{field.Invalid(field.NewPath("spec"), func() resource.ResourceClaimSpec {
				spec := validClaim.Spec.DeepCopy()
				spec.Devices.Requests[0].Exactly.DeviceClassName += "2"
				return *spec
			}(), "field is immutable")}.MarkCoveredByDeclarative(),
			oldClaim: validClaim,
			update: func(claim *resource.ResourceClaim) *resource.ResourceClaim {
				claim.Spec.Devices.Requests[0].Exactly.DeviceClassName += "2"
				return claim
			},
		},
		"too-large-config-valid-if-stored": {
			oldClaim: func() *resource.ResourceClaim {
				claim := validClaim.DeepCopy()
				claim.Spec.Devices.Config = []resource.DeviceClaimConfiguration{{
					DeviceConfiguration: resource.DeviceConfiguration{
						Opaque: &resource.OpaqueDeviceConfiguration{
							Driver:     goodName,
							Parameters: runtime.RawExtension{Raw: []byte(`{"str": "` + strings.Repeat("x", resource.OpaqueParametersMaxLength-9-2+1 /* too large by one */) + `"}`)},
						},
					},
				}}
				return claim
			}(),
			update: func(claim *resource.ResourceClaim) *resource.ResourceClaim {
				// 这是兼容性测试：旧对象可能携带现在非法的配置，未改动时不应被更新请求阻断。
				// 没有任何改动，应仍然合法。
				return claim
			},
		},
	}

	for name, scenario := range scenarios {
		t.Run(name, func(t *testing.T) {
			// 更新校验不应因为 ResourceVersion 这类元数据而失败。
			scenario.oldClaim.ResourceVersion = "1"
			errs := ValidateResourceClaimUpdate(scenario.update(scenario.oldClaim.DeepCopy()), scenario.oldClaim)
			assertFailures(t, scenario.wantFailures, errs)
		})
	}
}

// TestValidateClaimStatusUpdate 覆盖状态更新规则：分配结果不可变、预留引用、
// shareID 与设备状态合法性，以及多个 DRA feature gate 开关下的差异化行为。
func TestValidateClaimStatusUpdate(t *testing.T) {
	goodShareID := types.UID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
	goodShareID2 := types.UID("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb")
	badLengthShareID := types.UID("a")
	badFormatShareID := types.UID("Aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
	goodShareIDStr := string(goodShareID)
	goodShareID2Str := string(goodShareID2)
	badLengthShareIDStr := string(badLengthShareID)
	badFormatShareIDStr := string(badFormatShareID)
	validAllocatedClaim := validClaim.DeepCopy()
	validAllocatedClaim.Status = resource.ResourceClaimStatus{
		Allocation: &resource.AllocationResult{
			Devices: resource.DeviceAllocationResult{
				Results: []resource.DeviceRequestAllocationResult{{
					Request: goodName,
					Driver:  goodName,
					Pool:    goodName,
					Device:  goodName,
					// 1.35 起 AdminAccess 为必填；旧对象通过仿真版本兼容。
					AdminAccess: ptr.To(false), // 新分配必须显式设置该字段。
				}},
			},
		},
	}
	validAllocatedClaimOld := validAllocatedClaim.DeepCopy()
	validAllocatedClaimOld.Status.Allocation.Devices.Results[0].AdminAccess = nil // 1.31 版本不要求设置该字段。

	scenarios := map[string]struct {
		adminAccess                   bool
		deviceStatusFeatureGate       bool
		prioritizedListFeatureGate    bool
		consumableCapacityFeatureGate bool
		oldClaim                      *resource.ResourceClaim
		update                        func(claim *resource.ResourceClaim) *resource.ResourceClaim
		wantFailures                  field.ErrorList
	}{
		"valid-no-op-update": {
			oldClaim: validClaim,
			update:   func(claim *resource.ResourceClaim) *resource.ResourceClaim { return claim },
		},
		"valid-add-allocation-empty": {
			oldClaim: validClaim,
			update: func(claim *resource.ResourceClaim) *resource.ResourceClaim {
				claim.Status.Allocation = &resource.AllocationResult{}
				return claim
			},
		},
		"valid-add-allocation-non-empty": {
			oldClaim: validClaim,
			update: func(claim *resource.ResourceClaim) *resource.ResourceClaim {
				claim.Status.Allocation = &resource.AllocationResult{
					Devices: resource.DeviceAllocationResult{
						Results: []resource.DeviceRequestAllocationResult{{
							Request:     goodName,
							Driver:      goodName,
							Pool:        goodName,
							Device:      goodName,
							AdminAccess: ptr.To(false),
						}},
					},
				}
				return claim
			},
		},
		"invalid-add-allocation-bad-request": {
			wantFailures: field.ErrorList{
				field.Invalid(field.NewPath("status", "allocation", "devices", "results").Index(0).Child("request"), badName, "a lowercase RFC 1123 label must consist of lower case alphanumeric characters or '-', and must start and end with an alphanumeric character (e.g. 'my-name',  or '123-abc', regex used for validation is '[a-z0-9]([-a-z0-9]*[a-z0-9])?')"),
				field.Invalid(field.NewPath("status", "allocation", "devices", "results").Index(0).Child("request"), badName, "must be the name of a request in the claim or the name of a request and a subrequest separated by '/'"),
			},
			oldClaim: validClaim,
			update: func(claim *resource.ResourceClaim) *resource.ResourceClaim {
				claim.Status.Allocation = &resource.AllocationResult{
					Devices: resource.DeviceAllocationResult{
						Results: []resource.DeviceRequestAllocationResult{{
							Request:     badName,
							Driver:      goodName,
							Pool:        goodName,
							Device:      goodName,
							AdminAccess: ptr.To(false),
						}},
					},
				}
				return claim
			},
		},
		"okay-add-allocation-missing-admin-access": {
			adminAccess: false,
			oldClaim:    validClaim,
			update: func(claim *resource.ResourceClaim) *resource.ResourceClaim {
				claim.Status.Allocation = &resource.AllocationResult{
					Devices: resource.DeviceAllocationResult{
						Results: []resource.DeviceRequestAllocationResult{{
							Request: goodName,
							Driver:  goodName,
							Pool:    goodName,
							Device:  goodName,
							// 同一字段在新旧版本要求不同，是状态兼容性测试的核心。
							AdminAccess: nil, // 故意不设置，用于验证旧版本数据的兼容性。
						}},
					},
				}
				return claim
			},
		},
		"invalid-node-selector": {
			wantFailures: field.ErrorList{field.Required(field.NewPath("status", "allocation", "nodeSelector", "nodeSelectorTerms"), "must have at least one node selector term")},
			oldClaim:     validClaim,
			update: func(claim *resource.ResourceClaim) *resource.ResourceClaim {
				claim.Status.Allocation = &resource.AllocationResult{
					NodeSelector: &core.NodeSelector{
						// nodeSelector 不能为空，否则应报错。
					},
				}
				return claim
			},
		},
		"add-reservation": {
			oldClaim: validAllocatedClaim,
			update: func(claim *resource.ResourceClaim) *resource.ResourceClaim {
				for i := 0; i < resource.ResourceClaimReservedForMaxSize; i++ {
					claim.Status.ReservedFor = append(claim.Status.ReservedFor,
						resource.ResourceClaimConsumerReference{
							Resource: "pods",
							Name:     fmt.Sprintf("foo-%d", i),
							UID:      types.UID(fmt.Sprintf("%d", i)),
						})
				}
				return claim
			},
		},
		"add-reservation-old-claim": {
			oldClaim: validAllocatedClaimOld,
			update: func(claim *resource.ResourceClaim) *resource.ResourceClaim {
				for i := 0; i < resource.ResourceClaimReservedForMaxSize; i++ {
					claim.Status.ReservedFor = append(claim.Status.ReservedFor,
						resource.ResourceClaimConsumerReference{
							Resource: "pods",
							Name:     fmt.Sprintf("foo-%d", i),
							UID:      types.UID(fmt.Sprintf("%d", i)),
						})
				}
				return claim
			},
		},
		"add-reservation-and-allocation": {
			oldClaim: validClaim,
			update: func(claim *resource.ResourceClaim) *resource.ResourceClaim {
				claim.Status = *validAllocatedClaim.Status.DeepCopy()
				for i := 0; i < resource.ResourceClaimReservedForMaxSize; i++ {
					claim.Status.ReservedFor = append(claim.Status.ReservedFor,
						resource.ResourceClaimConsumerReference{
							Resource: "pods",
							Name:     fmt.Sprintf("foo-%d", i),
							UID:      types.UID(fmt.Sprintf("%d", i)),
						})
				}
				return claim
			},
		},
		"invalid-reserved-for-too-large": {
			wantFailures: field.ErrorList{field.TooMany(field.NewPath("status", "reservedFor"), resource.ResourceClaimReservedForMaxSize+1, resource.ResourceClaimReservedForMaxSize).MarkCoveredByDeclarative()},
			oldClaim:     validAllocatedClaim,
			update: func(claim *resource.ResourceClaim) *resource.ResourceClaim {
				for i := 0; i < resource.ResourceClaimReservedForMaxSize+1; i++ {
					claim.Status.ReservedFor = append(claim.Status.ReservedFor,
						resource.ResourceClaimConsumerReference{
							Resource: "pods",
							Name:     fmt.Sprintf("foo-%d", i),
							UID:      types.UID(fmt.Sprintf("%d", i)),
						})
				}
				return claim
			},
		},
		"invalid-reserved-for-duplicate": {
			wantFailures: field.ErrorList{field.Duplicate(field.NewPath("status", "reservedFor").Index(1), types.UID("1")).MarkCoveredByDeclarative()},
			oldClaim:     validAllocatedClaim,
			update: func(claim *resource.ResourceClaim) *resource.ResourceClaim {
				for i := 0; i < 2; i++ {
					claim.Status.ReservedFor = append(claim.Status.ReservedFor,
						resource.ResourceClaimConsumerReference{
							Resource: "pods",
							Name:     "foo",
							UID:      "1",
						})
				}
				return claim
			},
		},
		"invalid-reserved-for-no-allocation": {
			wantFailures: field.ErrorList{field.Forbidden(field.NewPath("status", "reservedFor"), "may not be specified when `allocated` is not set")},
			oldClaim:     validClaim,
			update: func(claim *resource.ResourceClaim) *resource.ResourceClaim {
				claim.Status.ReservedFor = []resource.ResourceClaimConsumerReference{
					{
						Resource: "pods",
						Name:     "foo",
						UID:      "1",
					},
				}
				return claim
			},
		},
		"invalid-reserved-for-no-resource": {
			wantFailures: field.ErrorList{field.Required(field.NewPath("status", "reservedFor").Index(0).Child("resource"), "")},
			oldClaim:     validAllocatedClaim,
			update: func(claim *resource.ResourceClaim) *resource.ResourceClaim {
				claim.Status.ReservedFor = []resource.ResourceClaimConsumerReference{
					{
						Name: "foo",
						UID:  "1",
					},
				}
				return claim
			},
		},
		"invalid-reserved-for-no-name": {
			wantFailures: field.ErrorList{field.Required(field.NewPath("status", "reservedFor").Index(0).Child("name"), "")},
			oldClaim:     validAllocatedClaim,
			update: func(claim *resource.ResourceClaim) *resource.ResourceClaim {
				claim.Status.ReservedFor = []resource.ResourceClaimConsumerReference{
					{
						Resource: "pods",
						UID:      "1",
					},
				}
				return claim
			},
		},
		"invalid-reserved-for-no-uid": {
			wantFailures: field.ErrorList{field.Required(field.NewPath("status", "reservedFor").Index(0).Child("uid"), "")},
			oldClaim:     validAllocatedClaim,
			update: func(claim *resource.ResourceClaim) *resource.ResourceClaim {
				claim.Status.ReservedFor = []resource.ResourceClaimConsumerReference{
					{
						Resource: "pods",
						Name:     "foo",
					},
				}
				return claim
			},
		},
		"invalid-reserved-deleted": {
			wantFailures: field.ErrorList{field.Forbidden(field.NewPath("status", "reservedFor"), "new entries may not be added while `deallocationRequested` or `deletionTimestamp` are set")},
			oldClaim: func() *resource.ResourceClaim {
				claim := validAllocatedClaim.DeepCopy()
				var deletionTimestamp metav1.Time
				claim.DeletionTimestamp = &deletionTimestamp
				return claim
			}(),
			update: func(claim *resource.ResourceClaim) *resource.ResourceClaim {
				claim.Status.ReservedFor = []resource.ResourceClaimConsumerReference{
					{
						Resource: "pods",
						Name:     "foo",
						UID:      "1",
					},
				}
				return claim
			},
		},
		"invalid-allocation-modification": {
			wantFailures: field.ErrorList{field.Invalid(field.NewPath("status.allocation"), func() *resource.AllocationResult {
				claim := validAllocatedClaim.DeepCopy()
				claim.Status.Allocation.Devices.Results[0].Driver += "-2"
				return claim.Status.Allocation
			}(), "field is immutable").MarkCoveredByDeclarative()},
			oldClaim: validAllocatedClaim,
			update: func(claim *resource.ResourceClaim) *resource.ResourceClaim {
				claim.Status.Allocation.Devices.Results[0].Driver += "-2"
				return claim
			},
		},
		"invalid-request-name": {
			wantFailures: field.ErrorList{
				field.Invalid(field.NewPath("status", "allocation", "devices", "config").Index(0).Child("requests").Index(1), badName, "a lowercase RFC 1123 label must consist of lower case alphanumeric characters or '-', and must start and end with an alphanumeric character (e.g. 'my-name',  or '123-abc', regex used for validation is '[a-z0-9]([-a-z0-9]*[a-z0-9])?')"),
				field.Invalid(field.NewPath("status", "allocation", "devices", "config").Index(0).Child("requests").Index(1), badName, "must be the name of a request in the claim or the name of a request and a subrequest separated by '/'"),
			},
			oldClaim: validClaim,
			update: func(claim *resource.ResourceClaim) *resource.ResourceClaim {
				claim = claim.DeepCopy()
				claim.Status.Allocation = validAllocatedClaim.Status.Allocation.DeepCopy()
				claim.Status.Allocation.Devices.Config = []resource.DeviceAllocationConfiguration{{
					Source:   resource.AllocationConfigSourceClaim,
					Requests: []string{claim.Spec.Devices.Requests[0].Name, badName},
					DeviceConfiguration: resource.DeviceConfiguration{
						Opaque: &resource.OpaqueDeviceConfiguration{
							Driver: "dra.example.com",
							Parameters: runtime.RawExtension{
								Raw: []byte(`{"kind": "foo", "apiVersion": "dra.example.com/v1"}`),
							},
						},
					},
				}}
				return claim
			},
		},
		"invalid-duplicate-request-name": {
			wantFailures: field.ErrorList{
				field.Duplicate(field.NewPath("status", "allocation", "devices", "config").Index(0).Child("requests").Index(1), "foo").MarkCoveredByDeclarative(),
			},
			oldClaim: validClaim,
			update: func(claim *resource.ResourceClaim) *resource.ResourceClaim {
				claim = claim.DeepCopy()
				claim.Status.Allocation = validAllocatedClaim.Status.Allocation.DeepCopy()
				claim.Status.Allocation.Devices.Config = []resource.DeviceAllocationConfiguration{{
					Source:   resource.AllocationConfigSourceClaim,
					Requests: []string{claim.Spec.Devices.Requests[0].Name, claim.Spec.Devices.Requests[0].Name},
					DeviceConfiguration: resource.DeviceConfiguration{
						Opaque: &resource.OpaqueDeviceConfiguration{
							Driver: "dra.example.com",
							Parameters: runtime.RawExtension{
								Raw: []byte(`{"kind": "foo", "apiVersion": "dra.example.com/v1"}`),
							},
						},
					},
				}}
				return claim
			},
		},
		"configuration": {
			wantFailures: field.ErrorList{
				field.Required(field.NewPath("status", "allocation", "devices", "config").Index(1).Child("source"), "").MarkCoveredByDeclarative(),
				field.NotSupported(field.NewPath("status", "allocation", "devices", "config").Index(2).Child("source"), resource.AllocationConfigSource("no-such-source"), []resource.AllocationConfigSource{resource.AllocationConfigSourceClaim, resource.AllocationConfigSourceClass}).MarkCoveredByDeclarative(),
				field.Required(field.NewPath("status", "allocation", "devices", "config").Index(3).Child("opaque"), ""),
				field.Required(field.NewPath("status", "allocation", "devices", "config").Index(4).Child("opaque", "driver"), "").MarkCoveredByDeclarative(),
				field.Required(field.NewPath("status", "allocation", "devices", "config").Index(4).Child("opaque", "parameters"), ""),
				field.TooLong(field.NewPath("status", "allocation", "devices", "config").Index(6).Child("opaque", "parameters"), "" /* 该参数未被使用，仅占位。 */, resource.OpaqueParametersMaxLength),
			},
			oldClaim: validClaim,
			update: func(claim *resource.ResourceClaim) *resource.ResourceClaim {
				claim = claim.DeepCopy()
				claim.Status.Allocation = validAllocatedClaim.Status.Allocation.DeepCopy()
				claim.Status.Allocation.Devices.Config = []resource.DeviceAllocationConfiguration{
					{
						Source: resource.AllocationConfigSourceClaim,
						DeviceConfiguration: resource.DeviceConfiguration{
							Opaque: &resource.OpaqueDeviceConfiguration{
								Driver: "dra.example.com",
								Parameters: runtime.RawExtension{
									Raw: []byte(`{"kind": "foo", "apiVersion": "dra.example.com/v1"}`),
								},
							},
						},
					},
					{
						Source: "", /* Empty! */
						DeviceConfiguration: resource.DeviceConfiguration{
							Opaque: &resource.OpaqueDeviceConfiguration{
								Driver: "dra.example.com",
								Parameters: runtime.RawExtension{
									Raw: []byte(`{"kind": "foo", "apiVersion": "dra.example.com/v1"}`),
								},
							},
						},
					},
					{
						Source: resource.AllocationConfigSource("no-such-source"),
						DeviceConfiguration: resource.DeviceConfiguration{
							Opaque: &resource.OpaqueDeviceConfiguration{
								Driver: "dra.example.com",
								Parameters: runtime.RawExtension{
									Raw: []byte(`{"kind": "foo", "apiVersion": "dra.example.com/v1"}`),
								},
							},
						},
					},
					{
						Source:              resource.AllocationConfigSourceClaim,
						DeviceConfiguration: resource.DeviceConfiguration{ /* Empty! */ },
					},
					{
						Source: resource.AllocationConfigSourceClaim,
						DeviceConfiguration: resource.DeviceConfiguration{
							Opaque: &resource.OpaqueDeviceConfiguration{ /* Empty! */ },
						},
					},
					{
						Source: resource.AllocationConfigSourceClaim,
						DeviceConfiguration: resource.DeviceConfiguration{
							Opaque: &resource.OpaqueDeviceConfiguration{
								Driver:     goodName,
								Parameters: runtime.RawExtension{Raw: []byte(`{"str": "` + strings.Repeat("x", resource.OpaqueParametersMaxLength-9-2) + `"}`)},
							},
						},
					},
					{
						Source: resource.AllocationConfigSourceClaim,
						DeviceConfiguration: resource.DeviceConfiguration{
							Opaque: &resource.OpaqueDeviceConfiguration{
								Driver:     goodName,
								Parameters: runtime.RawExtension{Raw: []byte(`{"str": "` + strings.Repeat("x", resource.OpaqueParametersMaxLength-9-2+1 /* too large by one */) + `"}`)},
							},
						},
					},
					// 其他非法的 resource.DeviceConfiguration 已在别处覆盖。
				}
				return claim
			},
		},
		"valid-configuration-update": {
			oldClaim: func() *resource.ResourceClaim {
				claim := validClaim.DeepCopy()
				claim.Status.Allocation = validAllocatedClaim.Status.Allocation.DeepCopy()
				claim.Status.Allocation.Devices.Config = []resource.DeviceAllocationConfiguration{
					{
						Source: resource.AllocationConfigSourceClaim,
						DeviceConfiguration: resource.DeviceConfiguration{
							Opaque: &resource.OpaqueDeviceConfiguration{
								Driver:     goodName,
								Parameters: runtime.RawExtension{Raw: []byte(`{"str": "` + strings.Repeat("x", resource.OpaqueParametersMaxLength-9-2+1 /* too large by one */) + `"}`)},
							},
						},
					},
				}
				return claim
			}(),
			update: func(claim *resource.ResourceClaim) *resource.ResourceClaim {
				// 没有任何改动，校验时旧值已超长也应视为合法。
				return claim
			},
		},
		"valid-network-device-status": {
			oldClaim: func() *resource.ResourceClaim { return validAllocatedClaim }(),
			update: func(claim *resource.ResourceClaim) *resource.ResourceClaim {
				claim.Status.Devices = []resource.AllocatedDeviceStatus{
					{
						Driver: goodName,
						Pool:   goodName,
						Device: goodName,
						Conditions: []metav1.Condition{
							{Type: "test-0", Status: metav1.ConditionTrue, Reason: "test_reason", LastTransitionTime: metav1.Now(), ObservedGeneration: 0},
							{Type: "test-1", Status: metav1.ConditionTrue, Reason: "test_reason", LastTransitionTime: metav1.Now(), ObservedGeneration: 0},
							{Type: "test-2", Status: metav1.ConditionTrue, Reason: "test_reason", LastTransitionTime: metav1.Now(), ObservedGeneration: 0},
							{Type: "test-3", Status: metav1.ConditionTrue, Reason: "test_reason", LastTransitionTime: metav1.Now(), ObservedGeneration: 0},
							{Type: "test-4", Status: metav1.ConditionTrue, Reason: "test_reason", LastTransitionTime: metav1.Now(), ObservedGeneration: 0},
							{Type: "test-5", Status: metav1.ConditionTrue, Reason: "test_reason", LastTransitionTime: metav1.Now(), ObservedGeneration: 0},
							{Type: "test-6", Status: metav1.ConditionTrue, Reason: "test_reason", LastTransitionTime: metav1.Now(), ObservedGeneration: 0},
							{Type: "test-7", Status: metav1.ConditionTrue, Reason: "test_reason", LastTransitionTime: metav1.Now(), ObservedGeneration: 0},
						},
						Data: &runtime.RawExtension{
							Raw: []byte(`{"kind": "foo", "apiVersion": "dra.example.com/v1"}`),
						},
						NetworkData: &resource.NetworkDeviceData{
							InterfaceName:   strings.Repeat("x", 256),
							HardwareAddress: strings.Repeat("x", 128),
							IPs: []string{
								"10.9.8.0/24",
								"2001:db8::/64",
								"10.9.8.1/24",
								"2001:db8::1/64",
								"10.9.8.2/24", "10.9.8.3/24", "10.9.8.4/24", "10.9.8.5/24", "10.9.8.6/24", "10.9.8.7/24",
								"10.9.8.8/24", "10.9.8.9/24", "10.9.8.10/24", "10.9.8.11/24", "10.9.8.12/24", "10.9.8.13/24",
							},
						},
					},
				}
				return claim
			},
			deviceStatusFeatureGate: true,
		},
		"valid-network-device-status-with-multi-byte-interface-name-and-hardware-address": {
			oldClaim: func() *resource.ResourceClaim { return validAllocatedClaim }(),
			update: func(claim *resource.ResourceClaim) *resource.ResourceClaim {
				claim.Status.Devices = []resource.AllocatedDeviceStatus{
					{
						Driver: goodName,
						Pool:   goodName,
						Device: goodName,
						Conditions: []metav1.Condition{
							{Type: "test-0", Status: metav1.ConditionTrue, Reason: "test_reason", LastTransitionTime: metav1.Now(), ObservedGeneration: 0},
							{Type: "test-1", Status: metav1.ConditionTrue, Reason: "test_reason", LastTransitionTime: metav1.Now(), ObservedGeneration: 0},
							{Type: "test-2", Status: metav1.ConditionTrue, Reason: "test_reason", LastTransitionTime: metav1.Now(), ObservedGeneration: 0},
							{Type: "test-3", Status: metav1.ConditionTrue, Reason: "test_reason", LastTransitionTime: metav1.Now(), ObservedGeneration: 0},
							{Type: "test-4", Status: metav1.ConditionTrue, Reason: "test_reason", LastTransitionTime: metav1.Now(), ObservedGeneration: 0},
							{Type: "test-5", Status: metav1.ConditionTrue, Reason: "test_reason", LastTransitionTime: metav1.Now(), ObservedGeneration: 0},
							{Type: "test-6", Status: metav1.ConditionTrue, Reason: "test_reason", LastTransitionTime: metav1.Now(), ObservedGeneration: 0},
							{Type: "test-7", Status: metav1.ConditionTrue, Reason: "test_reason", LastTransitionTime: metav1.Now(), ObservedGeneration: 0},
						},
						Data: &runtime.RawExtension{
							Raw: []byte(`{"kind": "foo", "apiVersion": "dra.example.com/v1"}`),
						},
						NetworkData: &resource.NetworkDeviceData{
							InterfaceName:   strings.Repeat("𝄞", 256/4), // 高音谱号字符恰好占 4 字节，重复 64 次正好为 256 字节，应合法。
							HardwareAddress: strings.Repeat("𝄞", 128/4), // 该字符占 4 字节，重复 32 次正好为 128 字节，应合法。
							IPs: []string{
								"10.9.8.0/24",
								"2001:db8::/64",
								"10.9.8.1/24",
								"2001:db8::1/64",
								"10.9.8.2/24", "10.9.8.3/24", "10.9.8.4/24", "10.9.8.5/24", "10.9.8.6/24", "10.9.8.7/24",
								"10.9.8.8/24", "10.9.8.9/24", "10.9.8.10/24", "10.9.8.11/24", "10.9.8.12/24", "10.9.8.13/24",
							},
						},
					},
				}
				return claim
			},
			deviceStatusFeatureGate: true,
		},
		"invalid-device-status-duplicate": {
			wantFailures: field.ErrorList{
				field.Duplicate(field.NewPath("status", "devices").Index(0).Child("networkData", "ips").Index(1), "2001:db8::1/64").MarkCoveredByDeclarative(),
				field.Duplicate(field.NewPath("status", "devices").Index(1), structured.MakeSharedDeviceID(structured.MakeDeviceID(goodName, goodName, goodName), nil)).MarkCoveredByDeclarative(),
			},
			oldClaim: func() *resource.ResourceClaim { return validAllocatedClaim }(),
			update: func(claim *resource.ResourceClaim) *resource.ResourceClaim {
				claim.Status.Devices = []resource.AllocatedDeviceStatus{
					{
						Driver: goodName,
						Pool:   goodName,
						Device: goodName,
						NetworkData: &resource.NetworkDeviceData{
							IPs: []string{
								"2001:db8::1/64",
								"2001:db8::1/64",
							},
						},
					},
					{
						Driver: goodName,
						Pool:   goodName,
						Device: goodName,
					},
				}
				return claim
			},
			deviceStatusFeatureGate: true,
		},
		"invalid-device-status-duplicate-with-share-id": {
			wantFailures: field.ErrorList{
				field.Duplicate(field.NewPath("status", "devices").Index(1),
					structured.MakeSharedDeviceID(structured.MakeDeviceID(goodName, goodName, goodName), ptr.To(goodShareID))).MarkCoveredByDeclarative(),
			},
			oldClaim: func() *resource.ResourceClaim {
				claim := validAllocatedClaim.DeepCopy()
				claim.Status.Allocation.Devices.Results[0].ShareID = ptr.To(goodShareID)
				return claim
			}(),
			update: func(claim *resource.ResourceClaim) *resource.ResourceClaim {
				claim.Status.Devices = []resource.AllocatedDeviceStatus{
					{
						Driver:  goodName,
						Pool:    goodName,
						Device:  goodName,
						ShareID: ptr.To(goodShareIDStr),
					},
					{
						Driver:  goodName,
						Pool:    goodName,
						Device:  goodName,
						ShareID: ptr.To(goodShareIDStr),
					},
				}
				return claim
			},
			deviceStatusFeatureGate:       true,
			consumableCapacityFeatureGate: true,
		},
		"invalid-network-device-status": {
			wantFailures: field.ErrorList{
				field.TooLong(field.NewPath("status", "devices").Index(0).Child("networkData", "interfaceName"), "", resource.NetworkDeviceDataInterfaceNameMaxLength).MarkCoveredByDeclarative(),
				field.TooLong(field.NewPath("status", "devices").Index(0).Child("networkData", "hardwareAddress"), "", resource.NetworkDeviceDataHardwareAddressMaxLength).MarkCoveredByDeclarative(),
				field.Invalid(field.NewPath("status", "devices").Index(0).Child("networkData", "ips").Index(0), "300.9.8.0/24", "must be a valid address in CIDR form, (e.g. 10.9.8.7/24 or 2001:db8::1/64)"),
				field.Invalid(field.NewPath("status", "devices").Index(0).Child("networkData", "ips").Index(1), "010.009.008.000/24", "must be in canonical form (\"10.9.8.0/24\")"),
				field.Invalid(field.NewPath("status", "devices").Index(0).Child("networkData", "ips").Index(2), "2001:0db8::1/64", "must be in canonical form (\"2001:db8::1/64\")"),
			},
			oldClaim: func() *resource.ResourceClaim { return validAllocatedClaim }(),
			update: func(claim *resource.ResourceClaim) *resource.ResourceClaim {
				claim.Status.Devices = []resource.AllocatedDeviceStatus{
					{
						Driver: goodName,
						Pool:   goodName,
						Device: goodName,
						NetworkData: &resource.NetworkDeviceData{
							InterfaceName:   strings.Repeat("x", resource.NetworkDeviceDataInterfaceNameMaxLength+1),
							HardwareAddress: strings.Repeat("x", resource.NetworkDeviceDataHardwareAddressMaxLength+1),
							IPs: []string{
								"300.9.8.0/24",
								"010.009.008.000/24",
								"2001:0db8::1/64",
							},
						},
					},
				}
				return claim
			},
			deviceStatusFeatureGate: true,
		},
		"invalid-network-device-status-with-multi-byte-interface-name-and-hardware-address": {
			wantFailures: field.ErrorList{
				field.TooLong(field.NewPath("status", "devices").Index(0).Child("networkData", "interfaceName"), "", resource.NetworkDeviceDataInterfaceNameMaxLength).MarkCoveredByDeclarative(),
				field.TooLong(field.NewPath("status", "devices").Index(0).Child("networkData", "hardwareAddress"), "", resource.NetworkDeviceDataHardwareAddressMaxLength).MarkCoveredByDeclarative(),
				field.Invalid(field.NewPath("status", "devices").Index(0).Child("networkData", "ips").Index(0), "300.9.8.0/24", "must be a valid address in CIDR form, (e.g. 10.9.8.7/24 or 2001:db8::1/64)"),
				field.Invalid(field.NewPath("status", "devices").Index(0).Child("networkData", "ips").Index(1), "010.009.008.000/24", "must be in canonical form (\"10.9.8.0/24\")"),
				field.Invalid(field.NewPath("status", "devices").Index(0).Child("networkData", "ips").Index(2), "2001:0db8::1/64", "must be in canonical form (\"2001:db8::1/64\")"),
			},
			oldClaim: func() *resource.ResourceClaim { return validAllocatedClaim }(),
			update: func(claim *resource.ResourceClaim) *resource.ResourceClaim {
				claim.Status.Devices = []resource.AllocatedDeviceStatus{
					{
						Driver: goodName,
						Pool:   goodName,
						Device: goodName,
						NetworkData: &resource.NetworkDeviceData{
							InterfaceName:   strings.Repeat("𝄞", resource.NetworkDeviceDataInterfaceNameMaxLength),   // 高音谱号占 4 字节，按字符数上限重复会使字节数超出上限 4 倍。
							HardwareAddress: strings.Repeat("𝄞", resource.NetworkDeviceDataHardwareAddressMaxLength), // 同理，按字符数上限重复会超出字节数上限 4 倍。
							IPs: []string{
								"300.9.8.0/24",
								"010.009.008.000/24",
								"2001:0db8::1/64",
							},
						},
					},
				}
				return claim
			},
			deviceStatusFeatureGate: true,
		},
		"invalid-data-device-status": {
			wantFailures: field.ErrorList{
				field.Invalid(field.NewPath("status", "devices").Index(0).Child("data"), "<value omitted>", "error parsing data as JSON: invalid character 'o' in literal false (expecting 'a')"),
			},
			oldClaim: func() *resource.ResourceClaim { return validAllocatedClaim }(),
			update: func(claim *resource.ResourceClaim) *resource.ResourceClaim {
				claim.Status.Devices = []resource.AllocatedDeviceStatus{
					{
						Driver: goodName,
						Pool:   goodName,
						Device: goodName,
						Data: &runtime.RawExtension{
							Raw: []byte(`foo`),
						},
					},
				}
				return claim
			},
			deviceStatusFeatureGate: true,
		},
		"invalid-data-device-status-limits": {
			wantFailures: field.ErrorList{
				field.TooMany(field.NewPath("status", "devices").Index(0).Child("conditions"), resource.AllocatedDeviceStatusMaxConditions+1, resource.AllocatedDeviceStatusMaxConditions),
				field.TooLong(field.NewPath("status", "devices").Index(0).Child("data"), "" /* 该参数未被使用，仅占位。 */, resource.AllocatedDeviceStatusDataMaxLength),
				field.TooMany(field.NewPath("status", "devices").Index(0).Child("networkData", "ips"), resource.NetworkDeviceDataMaxIPs+1, resource.NetworkDeviceDataMaxIPs).MarkCoveredByDeclarative(),
			},
			oldClaim: func() *resource.ResourceClaim { return validAllocatedClaim }(),
			update: func(claim *resource.ResourceClaim) *resource.ResourceClaim {
				claim.Status.Devices = []resource.AllocatedDeviceStatus{
					{
						Driver: goodName,
						Pool:   goodName,
						Device: goodName,
						Data:   &runtime.RawExtension{Raw: []byte(`{"str": "` + strings.Repeat("x", resource.AllocatedDeviceStatusDataMaxLength-9-2+1 /* too large by one */) + `"}`)},
						Conditions: []metav1.Condition{
							{Type: "test-0", Status: metav1.ConditionTrue, Reason: "test_reason", LastTransitionTime: metav1.Now(), ObservedGeneration: 0},
							{Type: "test-1", Status: metav1.ConditionTrue, Reason: "test_reason", LastTransitionTime: metav1.Now(), ObservedGeneration: 0},
							{Type: "test-2", Status: metav1.ConditionTrue, Reason: "test_reason", LastTransitionTime: metav1.Now(), ObservedGeneration: 0},
							{Type: "test-3", Status: metav1.ConditionTrue, Reason: "test_reason", LastTransitionTime: metav1.Now(), ObservedGeneration: 0},
							{Type: "test-4", Status: metav1.ConditionTrue, Reason: "test_reason", LastTransitionTime: metav1.Now(), ObservedGeneration: 0},
							{Type: "test-5", Status: metav1.ConditionTrue, Reason: "test_reason", LastTransitionTime: metav1.Now(), ObservedGeneration: 0},
							{Type: "test-6", Status: metav1.ConditionTrue, Reason: "test_reason", LastTransitionTime: metav1.Now(), ObservedGeneration: 0},
							{Type: "test-7", Status: metav1.ConditionTrue, Reason: "test_reason", LastTransitionTime: metav1.Now(), ObservedGeneration: 0},
							{Type: "test-8", Status: metav1.ConditionTrue, Reason: "test_reason", LastTransitionTime: metav1.Now(), ObservedGeneration: 0},
						},
						NetworkData: &resource.NetworkDeviceData{
							IPs: []string{
								"10.9.8.0/24", "10.9.8.1/24", "10.9.8.2/24", "10.9.8.3/24", "10.9.8.4/24", "10.9.8.5/24", "10.9.8.6/24", "10.9.8.7/24", "10.9.8.8/24",
								"10.9.8.9/24", "10.9.8.10/24", "10.9.8.11/24", "10.9.8.12/24", "10.9.8.13/24", "10.9.8.14/24", "10.9.8.15/24", "10.9.8.16/24",
							},
						},
					},
				}
				return claim
			},
			deviceStatusFeatureGate: true,
		},
		"invalid-device-status-no-device": {
			wantFailures: field.ErrorList{
				field.Invalid(field.NewPath("status", "devices").Index(0), structured.MakeSharedDeviceID(structured.MakeDeviceID("b", "a", "r"), nil), "must be an allocated device in the claim"),
			},
			oldClaim: func() *resource.ResourceClaim { return validAllocatedClaim }(),
			update: func(claim *resource.ResourceClaim) *resource.ResourceClaim {
				claim.Status.Devices = []resource.AllocatedDeviceStatus{
					{
						Driver: "b",
						Pool:   "a",
						Device: "r",
					},
				}
				return claim
			},
			deviceStatusFeatureGate: true,
		},
		"invalid-device-status-duplicate-disabled-feature-gate": {
			wantFailures: field.ErrorList{
				field.Duplicate(field.NewPath("status", "devices").Index(0).Child("networkData", "ips").Index(1), "2001:db8::1/64").MarkCoveredByDeclarative(),
				field.Duplicate(field.NewPath("status", "devices").Index(1), structured.MakeSharedDeviceID(structured.MakeDeviceID(goodName, goodName, goodName), nil)).MarkCoveredByDeclarative(),
			},
			oldClaim: func() *resource.ResourceClaim { return validAllocatedClaim }(),
			update: func(claim *resource.ResourceClaim) *resource.ResourceClaim {
				claim.Status.Devices = []resource.AllocatedDeviceStatus{
					{
						Driver: goodName,
						Pool:   goodName,
						Device: goodName,
						NetworkData: &resource.NetworkDeviceData{
							IPs: []string{
								"2001:db8::1/64",
								"2001:db8::1/64",
							},
						},
					},
					{
						Driver: goodName,
						Pool:   goodName,
						Device: goodName,
					},
				}
				return claim
			},
			deviceStatusFeatureGate: false,
		},
		"invalid-device-status-duplicate-with-share-id-disabled-feature-gate": {
			wantFailures: field.ErrorList{
				field.Duplicate(field.NewPath("status", "devices").Index(1),
					structured.MakeSharedDeviceID(structured.MakeDeviceID(goodName, goodName, goodName), ptr.To(goodShareID))).MarkCoveredByDeclarative(),
			},
			oldClaim: func() *resource.ResourceClaim {
				claim := validAllocatedClaim.DeepCopy()
				claim.Status.Allocation.Devices.Results[0].ShareID = ptr.To(goodShareID)
				return claim
			}(),
			update: func(claim *resource.ResourceClaim) *resource.ResourceClaim {
				claim.Status.Devices = []resource.AllocatedDeviceStatus{
					{
						Driver:  goodName,
						Pool:    goodName,
						Device:  goodName,
						ShareID: ptr.To(goodShareIDStr),
					},
					{
						Driver:  goodName,
						Pool:    goodName,
						Device:  goodName,
						ShareID: ptr.To(goodShareIDStr),
					},
				}
				return claim
			},
			deviceStatusFeatureGate:       false,
			consumableCapacityFeatureGate: false,
		},
		"invalid-network-device-status-disabled-feature-gate": {
			wantFailures: field.ErrorList{
				field.TooLong(field.NewPath("status", "devices").Index(0).Child("networkData", "interfaceName"), "", resource.NetworkDeviceDataInterfaceNameMaxLength).MarkCoveredByDeclarative(),
				field.TooLong(field.NewPath("status", "devices").Index(0).Child("networkData", "hardwareAddress"), "", resource.NetworkDeviceDataHardwareAddressMaxLength).MarkCoveredByDeclarative(),
				field.Invalid(field.NewPath("status", "devices").Index(0).Child("networkData", "ips").Index(0), "300.9.8.0/24", "must be a valid address in CIDR form, (e.g. 10.9.8.7/24 or 2001:db8::1/64)"),
			},
			oldClaim: func() *resource.ResourceClaim { return validAllocatedClaim }(),
			update: func(claim *resource.ResourceClaim) *resource.ResourceClaim {
				claim.Status.Devices = []resource.AllocatedDeviceStatus{
					{
						Driver: goodName,
						Pool:   goodName,
						Device: goodName,
						NetworkData: &resource.NetworkDeviceData{
							InterfaceName:   strings.Repeat("x", resource.NetworkDeviceDataInterfaceNameMaxLength+1),
							HardwareAddress: strings.Repeat("x", resource.NetworkDeviceDataHardwareAddressMaxLength+1),
							IPs: []string{
								"300.9.8.0/24",
							},
						},
					},
				}
				return claim
			},
			deviceStatusFeatureGate: false,
		},
		"invalid-data-device-status-disabled-feature-gate": {
			wantFailures: field.ErrorList{
				field.Invalid(field.NewPath("status", "devices").Index(0).Child("data"), "<value omitted>", "error parsing data as JSON: invalid character 'o' in literal false (expecting 'a')"),
			},
			oldClaim: func() *resource.ResourceClaim { return validAllocatedClaim }(),
			update: func(claim *resource.ResourceClaim) *resource.ResourceClaim {
				claim.Status.Devices = []resource.AllocatedDeviceStatus{
					{
						Driver: goodName,
						Pool:   goodName,
						Device: goodName,
						Data: &runtime.RawExtension{
							Raw: []byte(`foo`),
						},
					},
				}
				return claim
			},
			deviceStatusFeatureGate: false,
		},
		"invalid-data-device-status-limits-feature-gate": {
			wantFailures: field.ErrorList{
				field.TooMany(field.NewPath("status", "devices").Index(0).Child("conditions"), resource.AllocatedDeviceStatusMaxConditions+1, resource.AllocatedDeviceStatusMaxConditions),
				field.TooLong(field.NewPath("status", "devices").Index(0).Child("data"), "" /* 该参数未被使用，仅占位。 */, resource.AllocatedDeviceStatusDataMaxLength),
				field.TooMany(field.NewPath("status", "devices").Index(0).Child("networkData", "ips"), resource.NetworkDeviceDataMaxIPs+1, resource.NetworkDeviceDataMaxIPs).MarkCoveredByDeclarative(),
			},
			oldClaim: func() *resource.ResourceClaim { return validAllocatedClaim }(),
			update: func(claim *resource.ResourceClaim) *resource.ResourceClaim {
				claim.Status.Devices = []resource.AllocatedDeviceStatus{
					{
						Driver: goodName,
						Pool:   goodName,
						Device: goodName,
						Data:   &runtime.RawExtension{Raw: []byte(`{"str": "` + strings.Repeat("x", resource.AllocatedDeviceStatusDataMaxLength-9-2+1 /* too large by one */) + `"}`)},
						Conditions: []metav1.Condition{
							{Type: "test-0", Status: metav1.ConditionTrue, Reason: "test_reason", LastTransitionTime: metav1.Now(), ObservedGeneration: 0},
							{Type: "test-1", Status: metav1.ConditionTrue, Reason: "test_reason", LastTransitionTime: metav1.Now(), ObservedGeneration: 0},
							{Type: "test-2", Status: metav1.ConditionTrue, Reason: "test_reason", LastTransitionTime: metav1.Now(), ObservedGeneration: 0},
							{Type: "test-3", Status: metav1.ConditionTrue, Reason: "test_reason", LastTransitionTime: metav1.Now(), ObservedGeneration: 0},
							{Type: "test-4", Status: metav1.ConditionTrue, Reason: "test_reason", LastTransitionTime: metav1.Now(), ObservedGeneration: 0},
							{Type: "test-5", Status: metav1.ConditionTrue, Reason: "test_reason", LastTransitionTime: metav1.Now(), ObservedGeneration: 0},
							{Type: "test-6", Status: metav1.ConditionTrue, Reason: "test_reason", LastTransitionTime: metav1.Now(), ObservedGeneration: 0},
							{Type: "test-7", Status: metav1.ConditionTrue, Reason: "test_reason", LastTransitionTime: metav1.Now(), ObservedGeneration: 0},
							{Type: "test-8", Status: metav1.ConditionTrue, Reason: "test_reason", LastTransitionTime: metav1.Now(), ObservedGeneration: 0},
						},
						NetworkData: &resource.NetworkDeviceData{
							IPs: []string{
								"10.9.8.0/24", "10.9.8.1/24", "10.9.8.2/24", "10.9.8.3/24", "10.9.8.4/24", "10.9.8.5/24", "10.9.8.6/24", "10.9.8.7/24", "10.9.8.8/24",
								"10.9.8.9/24", "10.9.8.10/24", "10.9.8.11/24", "10.9.8.12/24", "10.9.8.13/24", "10.9.8.14/24", "10.9.8.15/24", "10.9.8.16/24",
							},
						},
					},
				}
				return claim
			},
			deviceStatusFeatureGate: false,
		},
		"invalid-device-status-no-device-disabled-feature-gate": {
			wantFailures: field.ErrorList{
				field.Invalid(field.NewPath("status", "devices").Index(0), structured.MakeSharedDeviceID(structured.MakeDeviceID("b", "a", "r"), nil), "must be an allocated device in the claim"),
			},
			oldClaim: func() *resource.ResourceClaim { return validAllocatedClaim }(),
			update: func(claim *resource.ResourceClaim) *resource.ResourceClaim {
				claim.Status.Devices = []resource.AllocatedDeviceStatus{
					{
						Driver: "b",
						Pool:   "a",
						Device: "r",
					},
				}
				return claim
			},
			deviceStatusFeatureGate: false,
		},
		"invalid-update-invalid-label-value": {
			wantFailures: field.ErrorList{
				field.Invalid(field.NewPath("status", "allocation", "nodeSelector", "nodeSelectorTerms").Index(0).Child("matchExpressions").Index(0).Child("values").Index(0), "-1", "a valid label must be an empty string or consist of alphanumeric characters, '-', '_' or '.', and must start and end with an alphanumeric character (e.g. 'MyValue',  or 'my_value',  or '12345', regex used for validation is '(([A-Za-z0-9][-A-Za-z0-9_.]*)?[A-Za-z0-9])?')"),
			},
			oldClaim: validClaim,
			update: func(claim *resource.ResourceClaim) *resource.ResourceClaim {
				claim = claim.DeepCopy()
				claim.Status.Allocation = validAllocatedClaim.Status.Allocation.DeepCopy()
				claim.Status.Allocation.NodeSelector = &core.NodeSelector{
					NodeSelectorTerms: []core.NodeSelectorTerm{{
						MatchExpressions: []core.NodeSelectorRequirement{{
							Key:      "foo",
							Operator: core.NodeSelectorOpIn,
							Values:   []string{"-1"},
						}},
					}},
				}
				return claim
			},
		},
		"valid-update-with-invalid-label-value": {
			oldClaim: func() *resource.ResourceClaim {
				claim := validAllocatedClaim.DeepCopy()
				claim.Status.Allocation = validAllocatedClaim.Status.Allocation.DeepCopy()
				claim.Status.Allocation.NodeSelector = &core.NodeSelector{
					NodeSelectorTerms: []core.NodeSelectorTerm{{
						MatchExpressions: []core.NodeSelectorRequirement{{
							Key:      "foo",
							Operator: core.NodeSelectorOpIn,
							Values:   []string{"-1"},
						}},
					}},
				}
				return claim
			}(),
			update: func(claim *resource.ResourceClaim) *resource.ResourceClaim {
				for i := 0; i < resource.ResourceClaimReservedForMaxSize; i++ {
					claim.Status.ReservedFor = append(claim.Status.ReservedFor,
						resource.ResourceClaimConsumerReference{
							Resource: "pods",
							Name:     fmt.Sprintf("foo-%d", i),
							UID:      types.UID(fmt.Sprintf("%d", i)),
						})
				}
				return claim
			},
		},
		"valid-add-allocation-with-sub-requests": {
			oldClaim: testClaim(goodName, goodNS, validClaimSpecWithFirstAvailable),
			update: func(claim *resource.ResourceClaim) *resource.ResourceClaim {
				claim.Status.Allocation = &resource.AllocationResult{
					Devices: resource.DeviceAllocationResult{
						Results: []resource.DeviceRequestAllocationResult{{
							Request:     fmt.Sprintf("%s/%s", goodName, goodName),
							Driver:      goodName,
							Pool:        goodName,
							Device:      goodName,
							AdminAccess: ptr.To(false),
						}},
					},
				}
				return claim
			},
			prioritizedListFeatureGate: true,
		},
		"invalid-add-allocation-with-sub-requests-invalid-format": {
			wantFailures: field.ErrorList{
				field.Invalid(field.NewPath("status", "allocation", "devices", "results").Index(0).Child("request"), badRequestFormat, "must be the name of a request in the claim or the name of a request and a subrequest separated by '/'"),
			},
			oldClaim: testClaim(goodName, goodNS, validClaimSpecWithFirstAvailable),
			update: func(claim *resource.ResourceClaim) *resource.ResourceClaim {
				claim.Status.Allocation = &resource.AllocationResult{
					Devices: resource.DeviceAllocationResult{
						Results: []resource.DeviceRequestAllocationResult{{
							Request:     badRequestFormat,
							Driver:      goodName,
							Pool:        goodName,
							Device:      goodName,
							AdminAccess: ptr.To(false),
						}},
					},
				}
				return claim
			},
			prioritizedListFeatureGate: true,
		},
		"invalid-add-allocation-with-sub-requests-no-corresponding-sub-request": {
			wantFailures: field.ErrorList{
				field.Invalid(field.NewPath("status", "allocation", "devices", "results").Index(0).Child("request"), "foo/baz", "must be the name of a request in the claim or the name of a request and a subrequest separated by '/'"),
			},
			oldClaim: testClaim(goodName, goodNS, validClaimSpecWithFirstAvailable),
			update: func(claim *resource.ResourceClaim) *resource.ResourceClaim {
				claim.Status.Allocation = &resource.AllocationResult{
					Devices: resource.DeviceAllocationResult{
						Results: []resource.DeviceRequestAllocationResult{{
							Request:     "foo/baz",
							Driver:      goodName,
							Pool:        goodName,
							Device:      goodName,
							AdminAccess: ptr.To(false),
						}},
					},
				}
				return claim
			},
			prioritizedListFeatureGate: true,
		},
		"invalid-add-allocation-with-sub-requests-invalid-request-names": {
			wantFailures: field.ErrorList{
				field.Invalid(field.NewPath("status", "allocation", "devices", "results").Index(0).Child("request"), badName, "a lowercase RFC 1123 label must consist of lower case alphanumeric characters or '-', and must start and end with an alphanumeric character (e.g. 'my-name',  or '123-abc', regex used for validation is '[a-z0-9]([-a-z0-9]*[a-z0-9])?')"),
				field.Invalid(field.NewPath("status", "allocation", "devices", "results").Index(0).Child("request"), badSubrequestName, "a lowercase RFC 1123 label must consist of lower case alphanumeric characters or '-', and must start and end with an alphanumeric character (e.g. 'my-name',  or '123-abc', regex used for validation is '[a-z0-9]([-a-z0-9]*[a-z0-9])?')"),
				field.Invalid(field.NewPath("status", "allocation", "devices", "results").Index(0).Child("request"), badFullSubrequestName, "must be the name of a request in the claim or the name of a request and a subrequest separated by '/'"),
			},
			oldClaim: testClaim(goodName, goodNS, validClaimSpecWithFirstAvailable),
			update: func(claim *resource.ResourceClaim) *resource.ResourceClaim {
				claim.Status.Allocation = &resource.AllocationResult{
					Devices: resource.DeviceAllocationResult{
						Results: []resource.DeviceRequestAllocationResult{{
							Request:     badFullSubrequestName,
							Driver:      goodName,
							Pool:        goodName,
							Device:      goodName,
							AdminAccess: ptr.To(false),
						}},
					},
				}
				return claim
			},
			prioritizedListFeatureGate: true,
		},
		"add-allocation-old-claim-with-prioritized-list": {
			wantFailures: nil,
			oldClaim:     testClaim(goodName, goodNS, validClaimSpecWithFirstAvailable),
			update: func(claim *resource.ResourceClaim) *resource.ResourceClaim {
				claim.Status.Allocation = &resource.AllocationResult{
					Devices: resource.DeviceAllocationResult{
						Results: []resource.DeviceRequestAllocationResult{{
							Request:     "foo/bar",
							Driver:      goodName,
							Pool:        goodName,
							Device:      goodName,
							AdminAccess: ptr.To(false),
						}},
					},
				}
				return claim
			},
			prioritizedListFeatureGate: false,
		},
		"good-binding-conditions": {
			oldClaim: validClaim,
			update: func(claim *resource.ResourceClaim) *resource.ResourceClaim {
				claim.Status.Allocation = &resource.AllocationResult{
					Devices: resource.DeviceAllocationResult{
						Results: []resource.DeviceRequestAllocationResult{{
							Request:                  goodName,
							Driver:                   goodName,
							Pool:                     goodName,
							Device:                   goodName,
							AdminAccess:              ptr.To(false),
							BindingConditions:        []string{"example.com/condition1", "condition2", "condition3", "condition4"},
							BindingFailureConditions: []string{"example.com/condition5", "condition6", "condition7", "condition8"},
						}},
					},
				}
				return claim
			},
		},
		"too-many-binding-conditions": {
			wantFailures: field.ErrorList{field.TooMany(field.NewPath("status", "allocation", "devices", "results").Index(0).Child("bindingConditions"), 5, 4).MarkCoveredByDeclarative()},
			oldClaim:     validClaim,
			update: func(claim *resource.ResourceClaim) *resource.ResourceClaim {
				claim.Status.Allocation = &resource.AllocationResult{
					Devices: resource.DeviceAllocationResult{
						Results: []resource.DeviceRequestAllocationResult{{
							Request:                  goodName,
							Driver:                   goodName,
							Pool:                     goodName,
							Device:                   goodName,
							AdminAccess:              ptr.To(false),
							BindingConditions:        []string{"condition1", "condition2", "condition3", "condition4", "condition5"},
							BindingFailureConditions: []string{"condition6", "condition7"},
						}},
					},
				}
				return claim
			},
		},
		"too-many-binding-failure-conditions": {
			wantFailures: field.ErrorList{field.TooMany(field.NewPath("status", "allocation", "devices", "results").Index(0).Child("bindingFailureConditions"), 5, 4).MarkCoveredByDeclarative()},
			oldClaim:     validClaim,
			update: func(claim *resource.ResourceClaim) *resource.ResourceClaim {
				claim.Status.Allocation = &resource.AllocationResult{
					Devices: resource.DeviceAllocationResult{
						Results: []resource.DeviceRequestAllocationResult{{
							Request:                  goodName,
							Driver:                   goodName,
							Pool:                     goodName,
							Device:                   goodName,
							AdminAccess:              ptr.To(false),
							BindingConditions:        []string{"condition1", "condition2"},
							BindingFailureConditions: []string{"condition3", "condition4", "condition5", "condition6", "condition7"},
						}},
					},
				}
				return claim
			},
		},
		"invalid-binding-conditions": {
			wantFailures: field.ErrorList{field.Invalid(field.NewPath("status", "allocation", "devices", "results").Index(0).Child("bindingConditions").Index(1), "condition2!", conditionValidationErrMessage)},
			oldClaim:     validClaim,
			update: func(claim *resource.ResourceClaim) *resource.ResourceClaim {
				claim.Status.Allocation = &resource.AllocationResult{
					Devices: resource.DeviceAllocationResult{
						Results: []resource.DeviceRequestAllocationResult{{
							Request:                  goodName,
							Driver:                   goodName,
							Pool:                     goodName,
							Device:                   goodName,
							AdminAccess:              ptr.To(false),
							BindingConditions:        []string{"condition1", "condition2!"},
							BindingFailureConditions: []string{"condition3", "condition4"},
						}},
					},
				}
				return claim
			},
		},
		"invalid-binding-failure-conditions": {
			wantFailures: field.ErrorList{field.Invalid(field.NewPath("status", "allocation", "devices", "results").Index(0).Child("bindingFailureConditions").Index(1), "condition4!", conditionValidationErrMessage)},
			oldClaim:     validClaim,
			update: func(claim *resource.ResourceClaim) *resource.ResourceClaim {
				claim.Status.Allocation = &resource.AllocationResult{
					Devices: resource.DeviceAllocationResult{
						Results: []resource.DeviceRequestAllocationResult{{
							Request:                  goodName,
							Driver:                   goodName,
							Pool:                     goodName,
							Device:                   goodName,
							AdminAccess:              ptr.To(false),
							BindingConditions:        []string{"condition1", "condition2"},
							BindingFailureConditions: []string{"condition3", "condition4!"},
						}},
					},
				}
				return claim
			},
		},
		"lacking-binding-conditions-claim-has-binding-failure-conditions": {
			wantFailures: field.ErrorList{field.Invalid(field.NewPath("status", "allocation", "devices", "results").Index(0).Child("bindingConditions"), []string(nil), "bindingConditions are required to use bindingFailureConditions")},
			oldClaim:     validClaim,
			update: func(claim *resource.ResourceClaim) *resource.ResourceClaim {
				claim.Status.Allocation = &resource.AllocationResult{
					Devices: resource.DeviceAllocationResult{
						Results: []resource.DeviceRequestAllocationResult{{
							Request:                  goodName,
							Driver:                   goodName,
							Pool:                     goodName,
							Device:                   goodName,
							AdminAccess:              ptr.To(false),
							BindingConditions:        nil,
							BindingFailureConditions: []string{"condition3", "condition4"},
						}},
					},
				}
				return claim
			},
		},
		"lacking-binding-failure-conditions-claim-has-binding-conditions": {
			wantFailures: field.ErrorList{field.Invalid(field.NewPath("status", "allocation", "devices", "results").Index(0).Child("bindingFailureConditions"), []string(nil), "bindingFailureConditions are required to use bindingConditions")},
			oldClaim:     validClaim,
			update: func(claim *resource.ResourceClaim) *resource.ResourceClaim {
				claim.Status.Allocation = &resource.AllocationResult{
					Devices: resource.DeviceAllocationResult{
						Results: []resource.DeviceRequestAllocationResult{{
							Request:                  goodName,
							Driver:                   goodName,
							Pool:                     goodName,
							Device:                   goodName,
							AdminAccess:              ptr.To(false),
							BindingConditions:        []string{"condition1", "condition2"},
							BindingFailureConditions: nil,
						}},
					},
				}
				return claim
			},
		},
		"valid-add-allocation-with-consumable-capacity-feature-on": {
			oldClaim: testClaim(goodName, goodNS, validClaimSpec),
			update: func(claim *resource.ResourceClaim) *resource.ResourceClaim {
				claim.Status.Allocation = &resource.AllocationResult{
					Devices: resource.DeviceAllocationResult{
						Results: []resource.DeviceRequestAllocationResult{{
							Request:     goodName,
							Driver:      goodName,
							Pool:        goodName,
							Device:      goodName,
							AdminAccess: ptr.To(false),
						}},
					},
				}
				return claim
			},
			consumableCapacityFeatureGate: true,
		},
		"valid-add-multi-alloc-devices": {
			oldClaim: testClaim(goodName, goodNS, func() resource.ResourceClaimSpec {
				spec := validClaimSpec.DeepCopy()
				spec.Devices.Requests[0].Exactly.Count = 2
				return *spec
			}()),
			update: func(claim *resource.ResourceClaim) *resource.ResourceClaim {
				claim.Status.Allocation = &resource.AllocationResult{
					Devices: resource.DeviceAllocationResult{
						Results: []resource.DeviceRequestAllocationResult{
							{
								Request:     goodName,
								Driver:      goodName,
								Pool:        goodName,
								Device:      goodName,
								ShareID:     ptr.To(goodShareID),
								AdminAccess: ptr.To(false),
							},
							{
								Request:     goodName,
								Driver:      goodName,
								Pool:        goodName,
								Device:      goodName,
								ShareID:     ptr.To(goodShareID2),
								AdminAccess: ptr.To(false),
							},
						},
					},
				}
				return claim
			},
			consumableCapacityFeatureGate: true,
		},
		"valid-add-multi-alloc-devices-with-device-status": {
			oldClaim: testClaim(goodName, goodNS, func() resource.ResourceClaimSpec {
				spec := validClaimSpec.DeepCopy()
				spec.Devices.Requests[0].Exactly.Count = 2
				return *spec
			}()),
			update: func(claim *resource.ResourceClaim) *resource.ResourceClaim {
				claim.Status.Allocation = &resource.AllocationResult{
					Devices: resource.DeviceAllocationResult{
						Results: []resource.DeviceRequestAllocationResult{
							{
								Request:     goodName,
								Driver:      goodName,
								Pool:        goodName,
								Device:      goodName,
								ShareID:     ptr.To(goodShareID),
								AdminAccess: ptr.To(false),
							},
							{
								Request:     goodName,
								Driver:      goodName,
								Pool:        goodName,
								Device:      goodName,
								ShareID:     ptr.To(goodShareID2),
								AdminAccess: ptr.To(false),
							},
						},
					},
				}
				claim.Status.Devices = []resource.AllocatedDeviceStatus{
					{
						Driver:  goodName,
						Pool:    goodName,
						Device:  goodName,
						ShareID: ptr.To(goodShareIDStr),
					},
					{
						Driver:  goodName,
						Pool:    goodName,
						Device:  goodName,
						ShareID: ptr.To(goodShareID2Str),
					},
				}
				return claim
			},
			consumableCapacityFeatureGate: true,
			deviceStatusFeatureGate:       true,
		},
		"valid-add-multi-alloc-devices-with-consumed-capacity": {
			oldClaim: testClaim(goodName, goodNS, validClaimSpec),
			update: func(claim *resource.ResourceClaim) *resource.ResourceClaim {
				claim.Status.Allocation = &resource.AllocationResult{
					Devices: resource.DeviceAllocationResult{
						Results: []resource.DeviceRequestAllocationResult{
							{
								Request:     goodName,
								Driver:      goodName,
								Pool:        goodName,
								Device:      goodName,
								ShareID:     ptr.To(goodShareID),
								AdminAccess: ptr.To(false),
								ConsumedCapacity: map[resource.QualifiedName]apiresource.Quantity{
									goodName: apiresource.MustParse("1"),
								},
							},
						},
					},
				}
				return claim
			},
			consumableCapacityFeatureGate: true,
		},
		"valid-add-enable-both-device-status-and-consumable-capacity-features": {
			oldClaim: testClaim(goodName, goodNS, validClaimSpec),
			update: func(claim *resource.ResourceClaim) *resource.ResourceClaim {
				claim.Status.Allocation = &resource.AllocationResult{
					Devices: resource.DeviceAllocationResult{
						Results: []resource.DeviceRequestAllocationResult{
							{
								Request:     goodName,
								Driver:      goodName,
								Pool:        goodName,
								Device:      goodName,
								ShareID:     ptr.To(goodShareID),
								AdminAccess: ptr.To(false),
							},
						},
					},
				}
				claim.Status.Devices = []resource.AllocatedDeviceStatus{
					{
						Driver:  goodName,
						Pool:    goodName,
						Device:  goodName,
						ShareID: ptr.To(goodShareIDStr),
					},
				}
				return claim
			},
			consumableCapacityFeatureGate: true,
			deviceStatusFeatureGate:       true,
		},
		"invalid-add-allocated-status-no-share-id": {
			wantFailures: field.ErrorList{
				field.Invalid(field.NewPath("status", "devices").Index(0), structured.MakeSharedDeviceID(structured.MakeDeviceID(goodName, goodName, goodName), nil), "must be an allocated device in the claim"),
			},
			oldClaim: testClaim(goodName, goodNS, validClaimSpec),
			update: func(claim *resource.ResourceClaim) *resource.ResourceClaim {
				claim.Status.Allocation = &resource.AllocationResult{
					Devices: resource.DeviceAllocationResult{
						Results: []resource.DeviceRequestAllocationResult{
							{
								Request:     goodName,
								Driver:      goodName,
								Pool:        goodName,
								Device:      goodName,
								ShareID:     ptr.To(goodShareID),
								AdminAccess: ptr.To(false),
							},
						},
					},
				}
				claim.Status.Devices = []resource.AllocatedDeviceStatus{
					{
						Driver: goodName,
						Pool:   goodName,
						Device: goodName,
					},
				}
				return claim
			},
			consumableCapacityFeatureGate: true,
			deviceStatusFeatureGate:       true,
		},
		"invalid-add-allocated-status-mismatch-share-id": {
			wantFailures: field.ErrorList{
				field.Invalid(field.NewPath("status", "devices").Index(0), structured.MakeSharedDeviceID(structured.MakeDeviceID(goodName, goodName, goodName), ptr.To(goodShareID2)), "must be an allocated device in the claim"),
			},
			oldClaim: testClaim(goodName, goodNS, validClaimSpec),
			update: func(claim *resource.ResourceClaim) *resource.ResourceClaim {
				claim.Status.Allocation = &resource.AllocationResult{
					Devices: resource.DeviceAllocationResult{
						Results: []resource.DeviceRequestAllocationResult{
							{
								Request:     goodName,
								Driver:      goodName,
								Pool:        goodName,
								Device:      goodName,
								ShareID:     ptr.To(goodShareID),
								AdminAccess: ptr.To(false),
							},
						},
					},
				}
				claim.Status.Devices = []resource.AllocatedDeviceStatus{
					{
						Driver:  goodName,
						Pool:    goodName,
						Device:  goodName,
						ShareID: &goodShareID2Str,
					},
				}
				return claim
			},
			consumableCapacityFeatureGate: true,
			deviceStatusFeatureGate:       true,
		},
		"invalid-add-allocated-status-invalid-share-id": {
			wantFailures: field.ErrorList{
				field.Invalid(field.NewPath("status", "devices").Index(0).Child("shareID"), badLengthShareIDStr, "error validating uid: invalid UUID length: 1").MarkCoveredByDeclarative(),
				field.Invalid(field.NewPath("status", "devices").Index(1).Child("shareID"), badFormatShareIDStr, "uid must be in RFC 4122 normalized form, `xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx` with lowercase hexadecimal characters").MarkCoveredByDeclarative(),
				field.Invalid(field.NewPath("status", "allocation", "devices", "results").Index(0).Child("shareID"), badLengthShareIDStr, "error validating uid: invalid UUID length: 1").MarkCoveredByDeclarative(),
				field.Invalid(field.NewPath("status", "allocation", "devices", "results").Index(1).Child("shareID"), badFormatShareIDStr, "uid must be in RFC 4122 normalized form, `xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx` with lowercase hexadecimal characters").MarkCoveredByDeclarative(),
			},
			oldClaim: testClaim(goodName, goodNS, validClaimSpec),
			update: func(claim *resource.ResourceClaim) *resource.ResourceClaim {
				claim.Status.Allocation = &resource.AllocationResult{
					Devices: resource.DeviceAllocationResult{
						Results: []resource.DeviceRequestAllocationResult{
							{
								Request:     goodName,
								Driver:      goodName,
								Pool:        goodName,
								Device:      goodName,
								ShareID:     ptr.To(badLengthShareID),
								AdminAccess: ptr.To(false),
							},
							{
								Request:     goodName,
								Driver:      goodName,
								Pool:        goodName,
								Device:      goodName,
								ShareID:     ptr.To(badFormatShareID),
								AdminAccess: ptr.To(false),
							},
						},
					},
				}
				claim.Status.Devices = []resource.AllocatedDeviceStatus{
					{
						Driver:  goodName,
						Pool:    goodName,
						Device:  goodName,
						ShareID: &badLengthShareIDStr,
					},
					{
						Driver:  goodName,
						Pool:    goodName,
						Device:  goodName,
						ShareID: &badFormatShareIDStr,
					},
				}
				return claim
			},
			consumableCapacityFeatureGate: true,
			deviceStatusFeatureGate:       true,
		},
		"valid-add-allocated-status-with-share-id-disabled-feature-gate": {
			oldClaim: testClaim(goodName, goodNS, validClaimSpec),
			update: func(claim *resource.ResourceClaim) *resource.ResourceClaim {
				claim.Status.Allocation = &resource.AllocationResult{
					Devices: resource.DeviceAllocationResult{
						Results: []resource.DeviceRequestAllocationResult{
							{
								Request:     goodName,
								Driver:      goodName,
								Pool:        goodName,
								Device:      goodName,
								ShareID:     ptr.To(goodShareID),
								AdminAccess: ptr.To(false),
							},
						},
					},
				}
				claim.Status.Devices = []resource.AllocatedDeviceStatus{
					{
						Driver:  goodName,
						Pool:    goodName,
						Device:  "foo",
						ShareID: ptr.To(goodShareIDStr),
					},
				}
				return claim
			},
			consumableCapacityFeatureGate: false,
			deviceStatusFeatureGate:       true,
		},
	}

	for name, scenario := range scenarios {
		t.Run(name, func(t *testing.T) {
			// 关闭 AdminAccess 时回退仿真版本到 1.35，模拟旧版本对象的校验行为。
			if !scenario.adminAccess {
				featuregatetesting.SetFeatureGateEmulationVersionDuringTest(t, utilfeature.DefaultFeatureGate, version.MustParse("1.35"))
			}
			featuregatetesting.SetFeatureGatesDuringTest(t, utilfeature.DefaultFeatureGate, featuregatetesting.FeatureOverrides{
				features.DRAAdminAccess:               scenario.adminAccess,
				features.DRAResourceClaimDeviceStatus: scenario.deviceStatusFeatureGate,
				features.DRAPrioritizedList:           scenario.prioritizedListFeatureGate,
				features.DRAConsumableCapacity:        scenario.consumableCapacityFeatureGate,
			})

			scenario.oldClaim.ResourceVersion = "1"
			errs := ValidateResourceClaimStatusUpdate(scenario.update(scenario.oldClaim.DeepCopy()), scenario.oldClaim)
			assertFailures(t, scenario.wantFailures, errs)
		})
	}
}
