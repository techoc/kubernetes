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

// Package experimental 提供 DRA 实验版结构化分配器的核心实现。
// 本文件为实验版分配器接入共享的分配器测试套件。
package experimental

import (
	"context"
	"testing"

	resourceapi "k8s.io/api/resource/v1"
	"k8s.io/dynamic-resource-allocation/cel"
	"k8s.io/dynamic-resource-allocation/structured/internal"
	"k8s.io/dynamic-resource-allocation/structured/internal/allocatortesting"
)

// TestAllocator 以实验版分配器实现运行共享的分配器测试套件（基础用例与字典序用例），验证其功能与排序行为符合 Structured DRA 的通用契约。
func TestAllocator(t *testing.T) {
	// 构造分配器工厂函数，把 NewAllocator 适配为测试套件期望的 internal.Allocator 工厂签名；输入包括上下文、特性集合、已分配状态、类查询器、slice 列表与 CEL 缓存。
	newAllocator := func(
		ctx context.Context,
		features Features,
		allocatedState AllocatedState,
		classLister DeviceClassLister,
		slices []*resourceapi.ResourceSlice,
		celCache *cel.Cache,
	) (internal.Allocator, error) {
		return NewAllocator(ctx, features, allocatedState, classLister, slices, celCache)
	}
	// 运行通用分配器测试：对每个用例调用工厂创建分配器，断言返回的分配结果与预期一致。
	allocatortesting.TestAllocator(t,
		SupportedFeatures,
		newAllocator,
	)
	// 运行字典序分配测试：断言分配结果按设备名的字典序稳定选择。
	allocatortesting.TestLexicographicalAllocator(t,
		SupportedFeatures,
		newAllocator,
	)
}
