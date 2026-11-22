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

package v1

import (
	"k8s.io/apimachinery/pkg/api/resource"
)

// String 返回 ResourceName 的字符串形式。
func (rn ResourceName) String() string {
	return string(rn)
}

// Cpu 返回 CPU 数量（如果已指定）。
func (rl *ResourceList) Cpu() *resource.Quantity {
	return rl.Name(ResourceCPU, resource.DecimalSI)
}

// Memory 返回内存数量（如果已指定）。
func (rl *ResourceList) Memory() *resource.Quantity {
	return rl.Name(ResourceMemory, resource.BinarySI)
}

// Storage 返回存储数量（如果已指定）。
func (rl *ResourceList) Storage() *resource.Quantity {
	return rl.Name(ResourceStorage, resource.BinarySI)
}

// Pods 返回 Pod 数量（如果已指定）。
func (rl *ResourceList) Pods() *resource.Quantity {
	return rl.Name(ResourcePods, resource.DecimalSI)
}

// StorageEphemeral 返回临时存储数量（如果已指定）。
func (rl *ResourceList) StorageEphemeral() *resource.Quantity {
	return rl.Name(ResourceEphemeralStorage, resource.BinarySI)
}

// Name 返回指定名称的资源数量；若未指定，返回默认格式的零值 Quantity。
func (rl *ResourceList) Name(name ResourceName, defaultFormat resource.Format) *resource.Quantity {
	if val, ok := (*rl)[name]; ok {
		return &val
	}
	// 不同资源默认单位不同（CPU 十进制、内存/存储二进制），零值也要带上正确格式，避免调用方拿到格式错误的 Quantity。
	return &resource.Quantity{Format: defaultFormat}
}
