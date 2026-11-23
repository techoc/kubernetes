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

package stable

import (
	"context"
	"testing"

	resourceapi "k8s.io/api/resource/v1"
	"k8s.io/dynamic-resource-allocation/cel"
	"k8s.io/dynamic-resource-allocation/structured/internal"
	"k8s.io/dynamic-resource-allocation/structured/internal/allocatortesting"
)

// TestAllocator 通过共享的 allocatortesting 测试套件验证 stable 分配器实现。
// 输入为测试框架 t；断言由共享套件在内部完成，覆盖各种分配场景。
func TestAllocator(t *testing.T) {
	// 复用通用测试套件，避免为不同特性变体重复编写分配用例。
	allocatortesting.TestAllocator(t,
		// 声明本变体支持的全部特性，测试套件据此生成相应场景。
		SupportedFeatures,
		// 测试套件用此工厂函数为每组输入构造一个分配器实例。
		func(
			// ctx 为本次分配的上下文，用于取消与日志。
			ctx context.Context,
			// features 为本场景启用的特性集合。
			features Features,
			// allocatedState 提供此前已完成分配的设备集合。
			allocatedState internal.AllocatedState,
			// classLister 用于按名称查询设备类。
			classLister DeviceClassLister,
			// slices 为可供分配的资源切片列表。
			slices []*resourceapi.ResourceSlice,
			// celCache 复用已编译的 CEL 表达式。
			celCache *cel.Cache,
		) (internal.Allocator, error) {
			// 构造稳定版分配器，并将已分配设备集合传给 NewAllocator。
			return NewAllocator(ctx, features, allocatedState.AllocatedDevices, classLister, slices, celCache)
		},
	)
}
