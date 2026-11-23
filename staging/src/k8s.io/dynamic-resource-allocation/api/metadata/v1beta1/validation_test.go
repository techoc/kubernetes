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

package v1beta1

import (
	"context"
	"fmt"
	"strings"
	"testing"

	resourceapi "k8s.io/api/resource/v1"
	"k8s.io/apimachinery/pkg/api/operation"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// TestValidateDeviceMetadata 验证 DeviceMetadata 的声明式校验规则。
// 测试先构造一个合法的基准对象，再通过 mutate 注入各类非法改动；断言要么没有错误，
// 要么恰好产生一个错误，且错误字段路径与 wantField 一致。
func TestValidateDeviceMetadata(t *testing.T) {
	// 合法的 Pod 侧 claim 名称，供基准对象使用。
	podClaimName := "claim"
	// 用于填充字符串属性的合法值。
	stringValue := "LATEST-GPU-MODEL"
	// 基准合法对象：包含 ObjectMeta、PodClaimName、一个请求和一台带属性的设备。
	valid := DeviceMetadata{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "claim",
			Namespace:  "default",
			Generation: 1,
		},
		PodClaimName: &podClaimName,
		Requests: []DeviceMetadataRequest{{
			Name: "request/subrequest",
			Devices: []Device{{
				Driver: "gpu.example.com",
				Pool:   "worker-0",
				Name:   "gpu-0",
				Attributes: map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
					"model": {StringValue: &stringValue},
				},
			}},
		}},
	}

	// 构造 33 个属性，超过 +k8s:maxProperties=32 的上限，用于触发属性数量错误。
	tooManyAttributes := make(map[resourceapi.QualifiedName]resourceapi.DeviceAttribute, 33)
	// 生成 attribute0 到 attribute32 共 33 个字符串属性。
	for i := range 33 {
		tooManyAttributes[resourceapi.QualifiedName(fmt.Sprintf("attribute%d", i))] = resourceapi.DeviceAttribute{
			StringValue: &stringValue,
		}
	}

	// 表驱动用例：mutate 为 nil 表示不修改基准对象；wantField 为空表示期望校验通过。
	testcases := map[string]struct {
		mutate    func(*DeviceMetadata)
		wantField string
	}{
		// 不做任何修改的合法对象，期望零错误。
		"valid": {},
		// 输入：PodClaimName 改为含大写的非法短名称；断言字段 podClaimName。
		"invalid pod claim name": {
			mutate: func(metadata *DeviceMetadata) {
				invalidName := "INVALID"
				metadata.PodClaimName = &invalidName
			},
			wantField: "podClaimName",
		},
		// 输入：请求名置空，违反必填规则；断言字段 requests[0].name。
		"request name required": {
			mutate: func(metadata *DeviceMetadata) {
				metadata.Requests[0].Name = ""
			},
			wantField: "requests[0].name",
		},
		// 输入：驱动名置空，违反必填规则；断言字段 requests[0].devices[0].driver。
		"driver required": {
			mutate: func(metadata *DeviceMetadata) {
				metadata.Requests[0].Devices[0].Driver = ""
			},
			wantField: "requests[0].devices[0].driver",
		},
		// 输入：64 个字符的驱动名，超过 63 字符上限；断言同一字段。
		"driver too long": {
			mutate: func(metadata *DeviceMetadata) {
				metadata.Requests[0].Devices[0].Driver = strings.Repeat("a", 64)
			},
			wantField: "requests[0].devices[0].driver",
		},
		// 输入：资源池名以斜杠结尾，不符合 DNS 子域格式；断言字段 pool。
		"invalid pool": {
			mutate: func(metadata *DeviceMetadata) {
				metadata.Requests[0].Devices[0].Pool = "worker-0/"
			},
			wantField: "requests[0].devices[0].pool",
		},
		// 输入：设备名含大写，违反 DNS label 格式；断言字段 name。
		"invalid device name": {
			mutate: func(metadata *DeviceMetadata) {
				metadata.Requests[0].Devices[0].Name = "GPU-0"
			},
			wantField: "requests[0].devices[0].name",
		},
		// 输入：替换为 33 个属性，超过数量上限；断言字段 attributes。
		"too many attributes": {
			mutate: func(metadata *DeviceMetadata) {
				metadata.Requests[0].Devices[0].Attributes = tooManyAttributes
			},
			wantField: "requests[0].devices[0].attributes",
		},
	}

	// 逐用例执行：深拷贝基准对象后应用 mutate，避免用例间相互污染。
	for name, tc := range testcases {
		t.Run(name, func(t *testing.T) {
			metadata := valid.DeepCopy()
			if tc.mutate != nil {
				tc.mutate(metadata)
			}

			// 调用生成的校验入口；oldObj 传 nil 表示创建场景，无需做更新时的差异比较。
			errs := Validate_DeviceMetadata(context.Background(), operation.Operation{}, nil, metadata, nil)
			if tc.wantField == "" {
				// 期望通过：断言校验错误列表为空。
				if len(errs) != 0 {
					t.Fatalf("expected no validation errors, got %v", errs)
				}
				return
			}
			// 期望失败：断言恰好一个错误，避免遗漏其他字段问题。
			if len(errs) != 1 {
				t.Fatalf("expected one validation error for %q, got %v", tc.wantField, errs)
			}
			// 断言错误精确落在期望的字段路径上。
			if errs[0].Field != tc.wantField {
				t.Errorf("expected validation error for %q, got %q", tc.wantField, errs[0].Field)
			}
		})
	}
}
