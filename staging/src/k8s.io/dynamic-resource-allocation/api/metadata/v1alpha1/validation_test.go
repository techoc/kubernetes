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

package v1alpha1

import (
	"context"
	"fmt"
	"strings"
	"testing"

	resourceapi "k8s.io/api/resource/v1"
	"k8s.io/apimachinery/pkg/api/operation"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// TestValidateDeviceMetadata 测试生成的 Validate_DeviceMetadata 校验函数。
// 测试意图：验证合法对象通过校验，各类非法字段被准确报错且定位到正确的字段路径。
// 输入：先构造一个完整的合法 DeviceMetadata 作为基线，再准备若干 mutate 函数产生非法变体；
// 断言：调用 Validate_DeviceMetadata 后，合法用例应无错误；非法用例应恰好产生一个错误，
// 且错误的 Field 与用例声明的 wantField 一致。
func TestValidateDeviceMetadata(t *testing.T) {
	// 构造合法基线对象：包含合法的 PodClaimName、request 名称（含子请求）、驱动/池/设备名称以及一个属性。
	podClaimName := "claim"
	stringValue := "LATEST-GPU-MODEL"
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

	// 构造 33 个属性的 map，超过 Attributes 字段 "最多 32 个属性" 的校验上限。
	tooManyAttributes := make(map[resourceapi.QualifiedName]resourceapi.DeviceAttribute, 33)
	for i := range 33 {
		tooManyAttributes[resourceapi.QualifiedName(fmt.Sprintf("attribute%d", i))] = resourceapi.DeviceAttribute{
			StringValue: &stringValue,
		}
	}

	// 测试用例表：mutate 为 nil 表示合法用例；wantField 为空表示期望无错误，
	// 否则表示期望报错的字段路径。各用例覆盖短名称格式、必填、长度上限、资源池格式、设备名格式和属性数量上限。
	testcases := map[string]struct {
		mutate    func(*DeviceMetadata)
		wantField string
	}{
		"valid": {},
		"invalid pod claim name": {
			mutate: func(metadata *DeviceMetadata) {
				invalidName := "INVALID"
				metadata.PodClaimName = &invalidName
			},
			wantField: "podClaimName",
		},
		"request name required": {
			mutate: func(metadata *DeviceMetadata) {
				metadata.Requests[0].Name = ""
			},
			wantField: "requests[0].name",
		},
		"driver required": {
			mutate: func(metadata *DeviceMetadata) {
				metadata.Requests[0].Devices[0].Driver = ""
			},
			wantField: "requests[0].devices[0].driver",
		},
		"driver too long": {
			mutate: func(metadata *DeviceMetadata) {
				metadata.Requests[0].Devices[0].Driver = strings.Repeat("a", 64)
			},
			wantField: "requests[0].devices[0].driver",
		},
		"invalid pool": {
			mutate: func(metadata *DeviceMetadata) {
				metadata.Requests[0].Devices[0].Pool = "worker-0/"
			},
			wantField: "requests[0].devices[0].pool",
		},
		"invalid device name": {
			mutate: func(metadata *DeviceMetadata) {
				metadata.Requests[0].Devices[0].Name = "GPU-0"
			},
			wantField: "requests[0].devices[0].name",
		},
		"too many attributes": {
			mutate: func(metadata *DeviceMetadata) {
				metadata.Requests[0].Devices[0].Attributes = tooManyAttributes
			},
			wantField: "requests[0].devices[0].attributes",
		},
	}

	for name, tc := range testcases {
		t.Run(name, func(t *testing.T) {
			// 每个子测试从合法基线深拷贝一份，避免用例之间的修改相互污染。
			metadata := valid.DeepCopy()
			if tc.mutate != nil {
				tc.mutate(metadata)
			}

			// 用默认操作类型校验当前对象；oldObj 传 nil 表示新建场景。
			errs := Validate_DeviceMetadata(context.Background(), operation.Operation{}, nil, metadata, nil)
			if tc.wantField == "" {
				// 合法用例：断言没有任何校验错误。
				if len(errs) != 0 {
					t.Fatalf("expected no validation errors, got %v", errs)
				}
				return
			}
			// 非法用例：断言恰好产生一个错误，并校验错误落在预期的字段路径上。
			if len(errs) != 1 {
				t.Fatalf("expected one validation error for %q, got %v", tc.wantField, errs)
			}
			if errs[0].Field != tc.wantField {
				t.Errorf("expected validation error for %q, got %q", tc.wantField, errs[0].Field)
			}
		})
	}
}
