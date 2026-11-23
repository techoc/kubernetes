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

package api

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog/v2"
)

// slice 是用于 klog 格式化测试的示例 ResourceSlice 对象，设置了 Kind、名称、驱动与一个设备作为输入。
var slice = ResourceSlice{
	TypeMeta: metav1.TypeMeta{
		Kind: "ResourceSlice",
	},
	ObjectMeta: metav1.ObjectMeta{
		Name: "slice",
	},
	Spec: ResourceSliceSpec{
		// Driver 为唯一的驱动名。
		Driver: MakeUniqueString("driver-name"),
		// Devices 只含一个设备，用于验证嵌套结构的格式化输出。
		Devices: []Device{{
			Name: MakeUniqueString("device-name"),
		}},
	},
}

// TestKlog 用 klog.Format 格式化示例 ResourceSlice，验证其输出可读且不 panic。
func TestKlog(t *testing.T) {
	// 将格式化结果写入测试日志，供人工检查输出。
	t.Logf("slice:\n%s", klog.Format(slice))
}
