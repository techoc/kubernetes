/*
Copyright 2017 The Kubernetes Authors.

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

package core

import (
	"k8s.io/apimachinery/pkg/api/resource"
)

// String 把资源名按其底层字符串返回。
func (rn ResourceName) String() string {
	return string(rn)
}

// CPU 返回 CPU 限制值（如果已指定）。
func (rl *ResourceList) CPU() *resource.Quantity {
	return rl.Name(ResourceCPU, resource.DecimalSI)
}

// Memory 返回内存限制值（如果已指定）。
func (rl *ResourceList) Memory() *resource.Quantity {
	return rl.Name(ResourceMemory, resource.BinarySI)
}

// Storage 返回存储限制值（如果已指定）。
func (rl *ResourceList) Storage() *resource.Quantity {
	return rl.Name(ResourceStorage, resource.BinarySI)
}

// Pods 返回 Pod 数量限制（如果已指定）。
func (rl *ResourceList) Pods() *resource.Quantity {
	return rl.Name(ResourcePods, resource.DecimalSI)
}

// StorageEphemeral 返回临时存储卷的配额（如果已指定）。
func (rl *ResourceList) StorageEphemeral() *resource.Quantity {
	return rl.Name(ResourceEphemeralStorage, resource.BinarySI)
}

// Name 按名称返回对应的资源量；未指定时返回一个使用默认格式的零值 Quantity。
// 这里返回零值而非 nil，调用方无需再判空即可安全读取/比较。
func (rl *ResourceList) Name(name ResourceName, defaultFormat resource.Format) *resource.Quantity {
	if val, ok := (*rl)[name]; ok {
		return &val
	}
	return &resource.Quantity{Format: defaultFormat}
}
