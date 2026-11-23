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

package incubating

import (
	"context"
	"testing"

	resourceapi "k8s.io/api/resource/v1"
	"k8s.io/dynamic-resource-allocation/cel"
	"k8s.io/dynamic-resource-allocation/structured/internal"
	"k8s.io/dynamic-resource-allocation/structured/internal/allocatortesting"
)

// TestAllocator 使用 incubating 变体运行分配器通用测试套件，验证启用全部孵化特性时的基础分配行为。
func TestAllocator(t *testing.T) {
	// newAllocator 将 NewAllocator 适配为测试套件期望的构造函数签名。
	newAllocator := func(
		ctx context.Context,
		features Features,
		allocatedState AllocatedState,
		classLister DeviceClassLister,
		slices []*resourceapi.ResourceSlice,
		celCache *cel.Cache,
	) (internal.Allocator, error) {
		// 直接构造 incubating 变体的分配器实例。
		return NewAllocator(ctx, features, allocatedState, classLister, slices, celCache)
	}
	// 运行基础分配器测试，覆盖设备选择、约束、共享设备与容量等通用场景。
	allocatortesting.TestAllocator(t,
		SupportedFeatures,
		newAllocator,
	)
	// 运行依赖字典序排序的测试，验证池与候选设备的选择顺序符合预期。
	allocatortesting.TestLexicographicalAllocator(t,
		SupportedFeatures,
		newAllocator,
	)
}
