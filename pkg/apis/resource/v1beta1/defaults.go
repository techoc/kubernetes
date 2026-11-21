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

package v1beta1

import (
	"time"

	resourceapi "k8s.io/api/resource/v1beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// 把本版本的默认值注册函数挂到 scheme 上。
func addDefaultingFuncs(scheme *runtime.Scheme) error {
	return RegisterDefaults(scheme)
}

// 为 v1beta1 的 DeviceRequest 设置默认值。
func SetDefaults_DeviceRequest(obj *resourceapi.DeviceRequest) {
	// 如果未设置 deviceClassName，则该请求应携带子请求，
	// 此时不应设置 allocationMode 和 count 字段，
	// 因此这里直接返回，不做默认值处理。
	if obj.DeviceClassName == "" {
		return
	}
	// AllocationMode 没有使用声明式默认值（+default），原因如下：
	// v1beta1 使用了定制的联合（union）结构；若无条件设置默认值，
	// 会使合法的 FirstAvailable 请求（该字段必须留空）变成非法。
	// 因此改用这里的手写默认值逻辑。
	if obj.AllocationMode == "" {
		obj.AllocationMode = resourceapi.DeviceAllocationModeExactCount
	}

	if obj.AllocationMode == resourceapi.DeviceAllocationModeExactCount && obj.Count == 0 {
		obj.Count = 1
	}
}

// 为 v1beta1 的 DeviceSubRequest 设置默认值：未指定分配模式时按精确数量分配，数量为 0 时取 1。
func SetDefaults_DeviceSubRequest(obj *resourceapi.DeviceSubRequest) {
	if obj.AllocationMode == "" {
		obj.AllocationMode = resourceapi.DeviceAllocationModeExactCount
	}

	if obj.AllocationMode == resourceapi.DeviceAllocationModeExactCount && obj.Count == 0 {
		obj.Count = 1
	}
}

// 为 DeviceTaint 设置默认值：未指定时间时使用当前时间（精确到秒）。
func SetDefaults_DeviceTaint(obj *resourceapi.DeviceTaint) {
	if obj.TimeAdded == nil {
		obj.TimeAdded = &metav1.Time{Time: time.Now().Truncate(time.Second)}
	}
}
