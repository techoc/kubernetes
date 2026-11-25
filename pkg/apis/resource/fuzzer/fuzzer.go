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

package fuzzer

import (
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	runtimeserializer "k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/kubernetes/pkg/apis/resource"
	"sigs.k8s.io/randfill"
)

// Funcs 包含 resource API 组的模糊测试函数。
//
// 这里刻意让某些字段保持为空，交由默认值填充逻辑补全；
// 如果模糊测试随意填充这些字段，往返测试（roundtrip）可能因默认值差异而失败。
var Funcs = func(codecs runtimeserializer.CodecFactory) []interface{} {
	return []interface{}{
		func(r *resource.ExactDeviceRequest, c randfill.Continue) {
			c.FillNoCustom(r) // 先填充自身字段，避免递归调用此函数

			// AllocationMode 为空时随机选取合法枚举值，
			// 否则生成的值可能不是默认值且无法通过往返测试。
			if r.AllocationMode == "" {
				r.AllocationMode = []resource.DeviceAllocationMode{
					resource.DeviceAllocationModeAll,
					resource.DeviceAllocationModeExactCount,
				}[c.Int31n(2)]
			}
		},
		func(r *resource.DeviceSubRequest, c randfill.Continue) {
			c.FillNoCustom(r) // 对本结构体做模糊填充，但不再递归调用本函数

			if r.AllocationMode == "" {
				r.AllocationMode = []resource.DeviceAllocationMode{
					resource.DeviceAllocationModeAll,
					resource.DeviceAllocationModeExactCount,
				}[c.Int31n(2)]
			}
		},
		func(r *resource.DeviceAllocationConfiguration, c randfill.Continue) {
			c.FillNoCustom(r)
			if r.Source == "" {
				r.Source = []resource.AllocationConfigSource{
					resource.AllocationConfigSourceClass,
					resource.AllocationConfigSourceClaim,
				}[c.Int31n(2)]
			}
		},
		func(r *resource.DeviceToleration, c randfill.Continue) {
			c.FillNoCustom(r)
			if r.Operator == "" {
				r.Operator = []resource.DeviceTolerationOperator{
					resource.DeviceTolerationOpEqual,
					resource.DeviceTolerationOpExists,
				}[c.Int31n(2)]
			}
		},
		func(r *resource.DeviceTaint, c randfill.Continue) {
			c.FillNoCustom(r)
			if r.TimeAdded == nil {
				// 当前时间是随机的。
				// 截断到秒级，因为亚秒级精度
				// 无法在往返转换中保持不变。
				r.TimeAdded = &metav1.Time{Time: time.Now().Truncate(time.Second)}
			}
		},
		func(r *resource.OpaqueDeviceConfiguration, c randfill.Continue) {
			c.FillNoCustom(r)
			// 与 fuzzer 对 runtime.Object 生成的默认内容保持一致。
			//
			// 这样做是必要的，因为随机生成的内容
			// 可能是合法 JSON，而它在重新编码时会发生变化。
			r.Parameters = runtime.RawExtension{Raw: []byte(`{"apiVersion":"unknown.group/unknown","kind":"Something","someKey":"someValue"}`)}
		},
		func(r *resource.AllocatedDeviceStatus, c randfill.Continue) {
			c.FillNoCustom(r)
			// 与 fuzzer 对 runtime.Object 生成的默认内容保持一致。
			//
			// 这样做是必要的，因为随机生成的内容
			// 可能是合法 JSON，而它在重新编码时会发生变化。
			r.Data = &runtime.RawExtension{Raw: []byte(`{"apiVersion":"unknown.group/unknown","kind":"Something","someKey":"someValue"}`)}
		},
		func(r *resource.ResourceSliceSpec, c randfill.Continue) {
			c.FillNoCustom(r)
			// 将 AllNodes 设置为 false 是不允许的。它必须
			// 为 true 或 nil。
			if r.AllNodes != nil && !*r.AllNodes {
				r.AllNodes = nil
			}
			if r.NodeName != nil && *r.NodeName == "" {
				r.NodeName = nil
			}
		},
		func(r *resource.ResourcePoolStatusRequestStatus, c randfill.Continue) {
			c.FillNoCustom(r)
			// 空切片经过 JSON 往返后会变为 nil。
			// 确保它们要么为 nil，要么至少包含一个元素。
			if len(r.Pools) == 0 {
				r.Pools = nil
			}
			if len(r.Conditions) == 0 {
				r.Conditions = nil
			}
		},
		func(r *resource.ResourcePoolStatusRequestSpec, c randfill.Continue) {
			c.FillNoCustom(r)
			// Limit 的默认值为 100，因此确保其被设置，
			// 以避免往返过程中触发默认值设置。
			if r.Limit == nil {
				limit := int32(c.Int31n(1000) + 1)
				r.Limit = &limit
			}
		},
	}
}
