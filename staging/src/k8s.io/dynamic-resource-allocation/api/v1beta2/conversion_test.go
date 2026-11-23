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

package v1beta2

import (
	"reflect"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	resourceapi "k8s.io/api/resource/v1"
	resourcev1beta2 "k8s.io/api/resource/v1beta2"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	apitest "k8s.io/dynamic-resource-allocation/api/internal/test"
)

// TestConversion 验证 v1beta2 与最新版本（v1）之间的对象转换：
// 既检查字段（含 FirstAvailable 备选请求）是否正确映射，也检查预期错误的匹配。
func TestConversion(t *testing.T) {
	// 测试用例表：in 为输入对象，out 为转换目标，expectOut 为期望结果，expectErr 为期望的错误子串。
	testcases := []struct {
		name      string
		in        runtime.Object
		out       runtime.Object
		expectOut runtime.Object
		expectErr string
	}{
		{
			// 输入：v1beta2 ResourceClaim，用 Exactly 指定单个设备请求（类 class-a、CEL 选择器、精确计数 2）。
			// 断言：转换到 v1 后保留请求名、设备类名、选择器表达式、分配模式与数量。
			name: "v1beta2 to latest without alternatives",
			in: &resourcev1beta2.ResourceClaim{
				Spec: resourcev1beta2.ResourceClaimSpec{
					Devices: resourcev1beta2.DeviceClaim{
						Requests: []resourcev1beta2.DeviceRequest{
							{
								Name: "foo",
								Exactly: &resourcev1beta2.ExactDeviceRequest{
									DeviceClassName: "class-a",
									Selectors: []resourcev1beta2.DeviceSelector{
										{
											CEL: &resourcev1beta2.CELDeviceSelector{
												Expression: `device.attributes["driver-a"].exists`,
											},
										},
									},
									AllocationMode: resourcev1beta2.DeviceAllocationModeExactCount,
									Count:          2,
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
		{
			// 输入：与上例等价的 v1 ResourceClaim（Exactly 请求）。
			// 断言：反向转换回 v1beta2 后各字段保持一致。
			name: "latest to v1beta2 without alternatives",
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
			out: &resourcev1beta2.ResourceClaim{},
			expectOut: &resourcev1beta2.ResourceClaim{
				Spec: resourcev1beta2.ResourceClaimSpec{
					Devices: resourcev1beta2.DeviceClaim{
						Requests: []resourcev1beta2.DeviceRequest{
							{
								Name: "foo",
								Exactly: &resourcev1beta2.ExactDeviceRequest{
									DeviceClassName: "class-a",
									Selectors: []resourcev1beta2.DeviceSelector{
										{
											CEL: &resourcev1beta2.CELDeviceSelector{
												Expression: `device.attributes["driver-a"].exists`,
											},
										},
									},
									AllocationMode: resourcev1beta2.DeviceAllocationModeExactCount,
									Count:          2,
								},
							},
						},
					},
				},
			},
		},

		{
			// 输入：v1beta2 ResourceClaim，使用 FirstAvailable 给出两个备选子请求
			// （sub-1 计数 2、sub-2 计数 1，均带 CEL 选择器）。
			// 断言：转换为 v1 后两个子请求的名称、选择器与数量原样保留。
			name: "v1beta2 to latest with alternatives",
			in: &resourcev1beta2.ResourceClaim{
				Spec: resourcev1beta2.ResourceClaimSpec{
					Devices: resourcev1beta2.DeviceClaim{
						Requests: []resourcev1beta2.DeviceRequest{
							{
								Name: "foo",
								FirstAvailable: []resourcev1beta2.DeviceSubRequest{
									{
										Name:            "sub-1",
										DeviceClassName: "class-a",
										Selectors: []resourcev1beta2.DeviceSelector{
											{
												CEL: &resourcev1beta2.CELDeviceSelector{
													Expression: `device.attributes["driver-a"].exists`,
												},
											},
										},
										AllocationMode: resourcev1beta2.DeviceAllocationModeExactCount,
										Count:          2,
									},
									{
										Name:            "sub-2",
										DeviceClassName: "class-a",
										Selectors: []resourcev1beta2.DeviceSelector{
											{
												CEL: &resourcev1beta2.CELDeviceSelector{
													Expression: `device.attributes["driver-a"].exists`,
												},
											},
										},
										AllocationMode: resourcev1beta2.DeviceAllocationModeExactCount,
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
		{
			// 输入：与上例等价的 v1 ResourceClaim（FirstAvailable 两个备选子请求）。
			// 断言：反向转换回 v1beta2 后备选列表完整保留。
			name: "latest to v1beta2 with alternatives",
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
			out: &resourcev1beta2.ResourceClaim{},
			expectOut: &resourcev1beta2.ResourceClaim{
				Spec: resourcev1beta2.ResourceClaimSpec{
					Devices: resourcev1beta2.DeviceClaim{
						Requests: []resourcev1beta2.DeviceRequest{
							{
								Name: "foo",
								FirstAvailable: []resourcev1beta2.DeviceSubRequest{
									{
										Name:            "sub-1",
										DeviceClassName: "class-a",
										Selectors: []resourcev1beta2.DeviceSelector{
											{
												CEL: &resourcev1beta2.CELDeviceSelector{
													Expression: `device.attributes["driver-a"].exists`,
												},
											},
										},
										AllocationMode: resourcev1beta2.DeviceAllocationModeExactCount,
										Count:          2,
									},
									{
										Name:            "sub-2",
										DeviceClassName: "class-a",
										Selectors: []resourcev1beta2.DeviceSelector{
											{
												CEL: &resourcev1beta2.CELDeviceSelector{
													Expression: `device.attributes["driver-a"].exists`,
												},
											},
										},
										AllocationMode: resourcev1beta2.DeviceAllocationModeExactCount,
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

	// 构造 scheme 并分别注册最新版本与 v1beta2 类型，使 Convert 能找到两端的转换函数。
	scheme := runtime.NewScheme()
	// 注册最新版本（v1）的类型。
	if err := resourceapi.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}

	// 注册本包提供的 v1beta2 类型转换。
	if err := AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}

	// 逐个执行测试用例；每个用例作为独立子测试运行，便于定位失败。
	for i := range testcases {
		name := testcases[i].name
		tc := testcases[i]
		t.Run(name, func(t *testing.T) {
			// 调用 scheme.Convert，由输入/输出对象的实际类型决定使用哪个版本的转换函数。
			err := scheme.Convert(tc.in, tc.out, nil)
			if err != nil {
				// 转换报错但用例未预期错误，直接失败。
				if len(tc.expectErr) == 0 {
					t.Fatalf("unexpected error %v", err)
				}
				// 期望的错误文本必须是实际错误的子串。
				if !strings.Contains(err.Error(), tc.expectErr) {
					t.Fatalf("expected error %s, got %v", tc.expectErr, err)
				}
				// 错误符合预期，本用例结束。
				return
			}
			// 转换成功但用例预期报错，属于失败。
			if len(tc.expectErr) > 0 {
				t.Fatalf("expected error %s, got none", tc.expectErr)
			}
			// 深度比较实际输出与期望输出，不一致时输出可读的 diff。
			if !reflect.DeepEqual(tc.out, tc.expectOut) {
				t.Fatalf("unexpected result:\n %s", cmp.Diff(tc.expectOut, tc.out))
			}
		})
	}

}

// TestConversionRoundTrip verifies that the automatically generated and
// hand-written conversion code for the DRA API correctly round-trips
// between v1beta2 and v1. For each non-list type registered in v1beta2,
// a fuzzed object is converted to v1 and back, and the result must be
// equal to the original.
//
// Note that this only covers conversion code which is called
// while converting the top-level API types. Types embedded
// inside those have their own conversion functions, but those
// are not necessarily called.
// TestConversionRoundTrip 验证自动生成的转换代码与手写转换代码能在 v1beta2 与 v1 之间正确往返：
// 对 v1beta2 中注册的每个非列表类型，先用随机数据填充，再转换到 v1 并转回，
// 结果必须与原始对象相等。
// 注意：此测试只覆盖转换顶层 API 类型时调用到的代码；
// 内嵌类型虽有各自的转换函数，但未必会被调用。
func TestConversionRoundTrip(t *testing.T) {
	// 新建 scheme，并注册 v1、v1beta2 以及本包的转换函数。
	scheme := runtime.NewScheme()
	if err := resourceapi.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := resourcev1beta2.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}

	// 构造随机对象填充器，用于生成往返测试的原始对象。
	filler := apitest.NewFiller(t, scheme, nil)

	// 在 v1beta2 与 v1 两个 GroupVersion 之间执行往返转换测试。
	apitest.ConversionRoundTrip(t, scheme, filler,
		schema.GroupVersion{Group: resourcev1beta2.GroupName, Version: "v1beta2"},
		schema.GroupVersion{Group: resourceapi.GroupName, Version: "v1"},
	)
}
