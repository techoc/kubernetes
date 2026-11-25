/*
Copyright 2025 The Kubernetes Authors.

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

package v1beta1

import (
	"reflect"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	resourceapi "k8s.io/api/resource/v1"
	resourcev1beta1 "k8s.io/api/resource/v1beta1"
	"k8s.io/apimachinery/pkg/api/apitesting/fuzzer"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	apitest "k8s.io/dynamic-resource-allocation/api/internal/test"
	"sigs.k8s.io/randfill"
)

// TestConversion 验证 ResourceClaim 中 DeviceRequest 在 v1beta1 与最新版本之间的显式转换。
// 表驱动用例覆盖四个场景：无备选子请求时的双向转换（主请求字段与 Exactly 互转），
// 以及有 FirstAvailable 备选子请求时的双向转换（子请求字段原样保留）。
// expectErr 为空时断言 out 与 expectOut 深度相等，非空时断言错误信息包含期望子串。
func TestConversion(t *testing.T) {
	testcases := []struct {
		name      string
		in        runtime.Object
		out       runtime.Object
		expectOut runtime.Object
		expectErr string
	}{
		// 输入：v1beta1 顶层主请求字段；期望：v1 中折叠为 Exactly，其余字段保持不变。
		{
			name: "v1beta1 to latest without alternatives",
			in: &resourcev1beta1.ResourceClaim{
				Spec: resourcev1beta1.ResourceClaimSpec{
					Devices: resourcev1beta1.DeviceClaim{
						Requests: []resourcev1beta1.DeviceRequest{
							{
								Name:            "foo",
								DeviceClassName: "class-a",
								Selectors: []resourcev1beta1.DeviceSelector{
									{
										CEL: &resourcev1beta1.CELDeviceSelector{
											Expression: `device.attributes["driver-a"].exists`,
										},
									},
								},
								AllocationMode: resourcev1beta1.DeviceAllocationModeExactCount,
								Count:          2,
							},
						},
					},
				},
			},
			out: &resourceapi.ResourceClaim{},
			expectOut: &resourceapi.ResourceClaim{
				Spec: resourceapi.ResourceClaimSpec{
					Devices: resourceapi.DeviceClaim{
						Requests: []resourceapi.DeviceRequest{
							{
								Name: "foo",
								Exactly: &resourceapi.ExactDeviceRequest{
									DeviceClassName: "class-a",
									Selectors: []resourceapi.DeviceSelector{
										{
											CEL: &resourceapi.CELDeviceSelector{
												Expression: `device.attributes["driver-a"].exists`,
											},
										},
									},
									AllocationMode: resourceapi.DeviceAllocationModeExactCount,
									Count:          2,
								},
							},
						},
					},
				},
			},
		},
		// 输入：v1 的 Exactly；期望：v1beta1 中拆回顶层主请求字段。
		{
			name: "latest to v1beta1 without alternatives",
			in: &resourceapi.ResourceClaim{
				Spec: resourceapi.ResourceClaimSpec{
					Devices: resourceapi.DeviceClaim{
						Requests: []resourceapi.DeviceRequest{
							{
								Name: "foo",
								Exactly: &resourceapi.ExactDeviceRequest{
									DeviceClassName: "class-a",
									Selectors: []resourceapi.DeviceSelector{
										{
											CEL: &resourceapi.CELDeviceSelector{
												Expression: `device.attributes["driver-a"].exists`,
											},
										},
									},
									AllocationMode: resourceapi.DeviceAllocationModeExactCount,
									Count:          2,
								},
							},
						},
					},
				},
			},
			out: &resourcev1beta1.ResourceClaim{},
			expectOut: &resourcev1beta1.ResourceClaim{
				Spec: resourcev1beta1.ResourceClaimSpec{
					Devices: resourcev1beta1.DeviceClaim{
						Requests: []resourcev1beta1.DeviceRequest{
							{
								Name:            "foo",
								DeviceClassName: "class-a",
								Selectors: []resourcev1beta1.DeviceSelector{
									{
										CEL: &resourcev1beta1.CELDeviceSelector{
											Expression: `device.attributes["driver-a"].exists`,
										},
									},
								},
								AllocationMode: resourcev1beta1.DeviceAllocationModeExactCount,
								Count:          2,
							},
						},
					},
				},
			},
		},

		// 输入：v1beta1 的 FirstAvailable 子请求；期望：v1 中结构保持不变。
		{
			name: "v1beta1 to latest with alternatives",
			in: &resourcev1beta1.ResourceClaim{
				Spec: resourcev1beta1.ResourceClaimSpec{
					Devices: resourcev1beta1.DeviceClaim{
						Requests: []resourcev1beta1.DeviceRequest{
							{
								Name: "foo",
								FirstAvailable: []resourcev1beta1.DeviceSubRequest{
									{
										Name:            "sub-1",
										DeviceClassName: "class-a",
										Selectors: []resourcev1beta1.DeviceSelector{
											{
												CEL: &resourcev1beta1.CELDeviceSelector{
													Expression: `device.attributes["driver-a"].exists`,
												},
											},
										},
										AllocationMode: resourcev1beta1.DeviceAllocationModeExactCount,
										Count:          2,
									},
									{
										Name:            "sub-2",
										DeviceClassName: "class-a",
										Selectors: []resourcev1beta1.DeviceSelector{
											{
												CEL: &resourcev1beta1.CELDeviceSelector{
													Expression: `device.attributes["driver-a"].exists`,
												},
											},
										},
										AllocationMode: resourcev1beta1.DeviceAllocationModeExactCount,
										Count:          1,
									},
								},
							},
						},
					},
				},
			},
			out: &resourceapi.ResourceClaim{},
			expectOut: &resourceapi.ResourceClaim{
				Spec: resourceapi.ResourceClaimSpec{
					Devices: resourceapi.DeviceClaim{
						Requests: []resourceapi.DeviceRequest{
							{
								Name: "foo",
								FirstAvailable: []resourceapi.DeviceSubRequest{
									{
										Name:            "sub-1",
										DeviceClassName: "class-a",
										Selectors: []resourceapi.DeviceSelector{
											{
												CEL: &resourceapi.CELDeviceSelector{
													Expression: `device.attributes["driver-a"].exists`,
												},
											},
										},
										AllocationMode: resourceapi.DeviceAllocationModeExactCount,
										Count:          2,
									},
									{
										Name:            "sub-2",
										DeviceClassName: "class-a",
										Selectors: []resourceapi.DeviceSelector{
											{
												CEL: &resourceapi.CELDeviceSelector{
													Expression: `device.attributes["driver-a"].exists`,
												},
											},
										},
										AllocationMode: resourceapi.DeviceAllocationModeExactCount,
										Count:          1,
									},
								},
							},
						},
					},
				},
			},
		},
		// 输入：v1 的 FirstAvailable 子请求；期望：v1beta1 中结构保持不变。
		{
			name: "latest to v1beta1 with alternatives",
			in: &resourceapi.ResourceClaim{
				Spec: resourceapi.ResourceClaimSpec{
					Devices: resourceapi.DeviceClaim{
						Requests: []resourceapi.DeviceRequest{
							{
								Name: "foo",
								FirstAvailable: []resourceapi.DeviceSubRequest{
									{
										Name:            "sub-1",
										DeviceClassName: "class-a",
										Selectors: []resourceapi.DeviceSelector{
											{
												CEL: &resourceapi.CELDeviceSelector{
													Expression: `device.attributes["driver-a"].exists`,
												},
											},
										},
										AllocationMode: resourceapi.DeviceAllocationModeExactCount,
										Count:          2,
									},
									{
										Name:            "sub-2",
										DeviceClassName: "class-a",
										Selectors: []resourceapi.DeviceSelector{
											{
												CEL: &resourceapi.CELDeviceSelector{
													Expression: `device.attributes["driver-a"].exists`,
												},
											},
										},
										AllocationMode: resourceapi.DeviceAllocationModeExactCount,
										Count:          1,
									},
								},
							},
						},
					},
				},
			},
			out: &resourcev1beta1.ResourceClaim{},
			expectOut: &resourcev1beta1.ResourceClaim{
				Spec: resourcev1beta1.ResourceClaimSpec{
					Devices: resourcev1beta1.DeviceClaim{
						Requests: []resourcev1beta1.DeviceRequest{
							{
								Name: "foo",
								FirstAvailable: []resourcev1beta1.DeviceSubRequest{
									{
										Name:            "sub-1",
										DeviceClassName: "class-a",
										Selectors: []resourcev1beta1.DeviceSelector{
											{
												CEL: &resourcev1beta1.CELDeviceSelector{
													Expression: `device.attributes["driver-a"].exists`,
												},
											},
										},
										AllocationMode: resourcev1beta1.DeviceAllocationModeExactCount,
										Count:          2,
									},
									{
										Name:            "sub-2",
										DeviceClassName: "class-a",
										Selectors: []resourcev1beta1.DeviceSelector{
											{
												CEL: &resourcev1beta1.CELDeviceSelector{
													Expression: `device.attributes["driver-a"].exists`,
												},
											},
										},
										AllocationMode: resourcev1beta1.DeviceAllocationModeExactCount,
										Count:          1,
									},
								},
							},
						},
					},
				},
			},
		},
	}

	// 新建 scheme：注册 v1 类型，并注入本包提供的 v1beta1 <-> v1 转换函数。
	scheme := runtime.NewScheme()
	if err := resourceapi.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}

	if err := AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}

	// 逐用例执行：根据 expectErr 是否为空选择错误断言或结果断言。
	for i := range testcases {
		name := testcases[i].name
		tc := testcases[i]
		t.Run(name, func(t *testing.T) {
			// 转换失败时：未期望错误则直接失败，否则校验错误信息是否包含期望子串。
			err := scheme.Convert(tc.in, tc.out, nil)
			if err != nil {
				if len(tc.expectErr) == 0 {
					t.Fatalf("unexpected error %v", err)
				}
				if !strings.Contains(err.Error(), tc.expectErr) {
					t.Fatalf("expected error %s, got %v", tc.expectErr, err)
				}
				return
			}
			// 转换成功但期望错误，说明断言不完整。
			if len(tc.expectErr) > 0 {
				t.Fatalf("expected error %s, got none", tc.expectErr)
			}
			// 断言输出对象与期望对象完全一致，并用 cmp.Diff 输出差异。
			if !reflect.DeepEqual(tc.out, tc.expectOut) {
				t.Fatalf("unexpected result:\n %s", cmp.Diff(tc.expectOut, tc.out))
			}
		})
	}

}

// v1beta1FillFuncs 返回随机填充所需的自定义函数：默认填充可能产生因版本结构差异
// 而无法无损往返的对象，这里对这类字段做修正。
func v1beta1FillFuncs(codecs serializer.CodecFactory) []interface{} {
	return []interface{}{
		// 针对 Device 的填充器：转换目标总是得到非 nil 的 Basic，源对象也必须如此。
		func(d *resourcev1beta1.Device, c randfill.Continue) {
			// 先用默认规则填充所有字段。
			c.FillNoCustom(d)
			// 再补齐 Basic，避免往返后 nil 与非 nil 不一致。
			if d.Basic == nil {
				d.Basic = &resourcev1beta1.BasicDevice{}
			}
		},
	}
}

// TestConversionRoundTrip 验证生成与手写的转换代码能在 v1beta1 与 v1 之间无损往返：
// 借助自定义填充器生成随机对象，转换到 v1 后再转回，并断言与原对象语义相等。
// 注意：这只覆盖转换顶层 API 类型时被调用的转换代码；内嵌类型虽然也有各自的转换函数，
// 但不一定会在此测试中被调用。
func TestConversionRoundTrip(t *testing.T) {
	// 注册 v1、v1beta1 类型以及本包的转换函数。
	scheme := runtime.NewScheme()
	if err := resourceapi.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := resourcev1beta1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}

	// 使用自定义填充函数创建随机对象生成器，修正无法往返的字段。
	filler := apitest.NewFiller(t, scheme, fuzzer.FuzzerFuncs(v1beta1FillFuncs))

	// 对 v1beta1 中每个非 List 类型执行随机往返测试。
	apitest.ConversionRoundTrip(t, scheme, filler,
		schema.GroupVersion{Group: resourcev1beta1.GroupName, Version: "v1beta1"},
		schema.GroupVersion{Group: resourceapi.GroupName, Version: "v1"},
	)
}
