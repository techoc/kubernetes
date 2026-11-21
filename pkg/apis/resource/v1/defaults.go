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

package v1

import (
	"time"

	resourceapi "k8s.io/api/resource/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// addDefaultingFuncs 向 scheme 注册默认值设置函数。
func addDefaultingFuncs(scheme *runtime.Scheme) error {
	return RegisterDefaults(scheme)
}

// SetDefaults_ExactDeviceRequest 为精确数量设备请求填充默认值：
// 未指定分配模式时按精确数量处理，数量为 0 时按 1 处理。
func SetDefaults_ExactDeviceRequest(obj *resourceapi.ExactDeviceRequest) {
	// 分配模式为空时默认按精确数量分配，保持与历史行为一致。
	if obj.AllocationMode == "" {
		obj.AllocationMode = resourceapi.DeviceAllocationModeExactCount
	}

	// 精确数量模式下 count 为 0 没有意义，默认请求 1 个设备。
	if obj.AllocationMode == resourceapi.DeviceAllocationModeExactCount && obj.Count == 0 {
		obj.Count = 1
	}
}

// SetDefaults_DeviceSubRequest 为设备子请求填充默认值，规则与 ExactDeviceRequest 相同。
func SetDefaults_DeviceSubRequest(obj *resourceapi.DeviceSubRequest) {
	if obj.AllocationMode == "" {
		obj.AllocationMode = resourceapi.DeviceAllocationModeExactCount
	}

	if obj.AllocationMode == resourceapi.DeviceAllocationModeExactCount && obj.Count == 0 {
		obj.Count = 1
	}
}

// SetDefaults_DeviceTaint 在设备污点未设置添加时间时填充当前时间。
// 截断到秒级以对齐 metav1.Time 的序列化精度。
func SetDefaults_DeviceTaint(obj *resourceapi.DeviceTaint) {
	if obj.TimeAdded == nil {
		obj.TimeAdded = &metav1.Time{Time: time.Now().Truncate(time.Second)}
	}
}
