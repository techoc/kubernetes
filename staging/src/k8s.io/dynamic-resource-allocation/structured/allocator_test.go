/*
Copyright 2024 The Kubernetes Authors.

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

package structured

import (
	"context"
	"fmt"
	"testing"

	resourceapi "k8s.io/api/resource/v1"
	"k8s.io/dynamic-resource-allocation/cel"
	"k8s.io/dynamic-resource-allocation/structured/internal"
	"k8s.io/dynamic-resource-allocation/structured/internal/allocatortesting"
)

// TestAllocator 借助共享的 allocatortesting 测试套件，验证 structured 包的分配器实现。
// 输入：启用全部 DRA 特性的配置、一个用于构造分配器的工厂闭包。
// 断言：工厂构造出的分配器必须实现 internal.Allocator 接口，以便测试访问其内部 channel。
func TestAllocator(t *testing.T) {
	// 复用所有分配器实现共享的测试套件，并使用全部特性。
	allocatortesting.TestAllocator(t,
		internal.FeaturesAll,
		// 输入：启用所有 DRA 特性。
		// 输入：分配器工厂闭包，供测试套件构造被测对象。
		func(
			ctx context.Context,
			features Features,
			allocatedState AllocatedState,
			classLister DeviceClassLister,
			slices []*resourceapi.ResourceSlice,
			celCache *cel.Cache,
		) (allocatortesting.Allocator, error) {
			// 调用被测包的 NewAllocator 构造分配器。
			allocator, err := NewAllocator(ctx, features, allocatedState, classLister, slices, celCache)
			if err != nil {
				return nil, err
			}
			// Return the allocator with the internal interface so the tests can
			// check the channel for the allocator.
			// 以 internal.Allocator 接口返回实例，便于测试检查分配器的 channel（实现通道标识）。
			// 断言：分配器必须实现 internal.Allocator 接口。
			internalAllocator, ok := allocator.(internal.Allocator)
			if !ok {
				// 类型断言失败时给出明确的错误信息。
				return nil, fmt.Errorf("allocator doesn't implement internal interface")
			}
			// 断言通过，返回内部接口实例。
			return internalAllocator, nil
		})
}
