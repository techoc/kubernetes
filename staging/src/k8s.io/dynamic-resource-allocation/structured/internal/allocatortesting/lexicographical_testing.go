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

package allocatortesting

import (
	"context"
	"testing"

	resourceapi "k8s.io/api/resource/v1"
	"k8s.io/dynamic-resource-allocation/cel"
)

// TestLexicographicalAllocator runs tests which depend on lexicographical sorting.
// TestLexicographicalAllocator 运行一组依赖"字典序排序"行为的分配测试。
// 这些用例验证：当输入的 ResourceSlice / DevicePool / Device 顺序被打乱时，
// 分配器仍按名称的字典序（而非输入顺序）选择设备，从而保证不同实现产生一致结果。
// supportedFeatures 与 newAllocator 由具体的分配器实现传入。
func TestLexicographicalAllocator(t *testing.T,
	supportedFeatures Features,
	newAllocator func(
		ctx context.Context,
		features Features,
		allocateState AllocatedState,
		classLister DeviceClassLister,
		slices []*resourceapi.ResourceSlice,
		celCache *cel.Cache,
	) (Allocator, error)) {
	// 定义测试用例集合：每个用例打乱输入顺序，期望按字典序选出设备。
	testcases := map[string]AllocatorTestCase{
		// 场景一：池的字典序。输入顺序为 pool3、pool2、pool1，
		// 期望先选 pool1 再选 pool2，证明按池名字典序而非输入顺序分配。
		"lexicographical-sorting-pools": {
			claimsToAllocate: objects(claimWithRequests(claim0, nil, request(req0, classA, 2))),
			classes:          objects(class(classA, driverA)),
			slices: unwrapResourceSlices(
				// pool-3 before pool-2 before pool-1 in input.
				sliceWithOneDevice(slice1, node1, resourcePool(pool3, 1), driverA),
				sliceWithOneDevice(slice2, node1, resourcePool(pool2, 1), driverA),
				sliceWithOneDevice(slice3, node1, resourcePool(pool1, 1), driverA),
			),
			node: node(node1, region1),

			expectResults: []any{allocationResult(
				localNodeSelector(node1),
				deviceAllocationResult(req0, driverA, pool1, device1, false),
				deviceAllocationResult(req0, driverA, pool2, device1, false),
			)},
		},
		// 场景二：带 binding conditions 的池排序。
		// 期望没有 binding conditions 的池（pool2/pool3）排在带条件的 pool1 之前，
		// 且带条件的设备被选中时其条件要原样反映到结果中。
		"lexicographical-sorting-pools-with-binding-conditions": {
			features: Features{
				DeviceBindingAndStatus: true,
			},
			claimsToAllocate: objects(claimWithRequests(claim0, nil, request(req0, classA, 2))),
			classes:          objects(class(classA, driverA)),
			slices: unwrapResourceSlices(
				// pools with binding conditions will be considered after pools without binding conditions.
				sliceWithDevices(slice1, node1, resourcePool(pool1, 1), driverA,
					device(device1, nil, nil).withBindingConditions([]string{"IsPrepare"}, []string{"BindingFailed"}),
				),
				sliceWithDevices(slice3, node1, resourcePool(pool3, 1), driverA, device(device3, nil, nil)),
				sliceWithDevices(slice2, node1, resourcePool(pool2, 1), driverA, device(device2, nil, nil)),
			),
			node: node(node1, region1),

			expectResults: []any{allocationResult(
				localNodeSelector(node1),
				deviceAllocationResult(req0, driverA, pool2, device2, false),
				deviceAllocationResult(req0, driverA, pool3, device3, false),
			)},
		},
		// 场景三：同一池内 ResourceSlice 的字典序。
		// 输入顺序为 slice3、slice2、slice1，期望先选 slice1 再选 slice2 中的设备。
		"lexicographical-sorting-slices": {
			claimsToAllocate: objects(claimWithRequests(claim0, nil, request(req0, classA, 2))),
			classes:          objects(class(classA, driverA)),
			slices: unwrapResourceSlices(
				// slice-3 before slice-2 before slice-1 in input.
				sliceWithDevices(slice3, node1, resourcePool(pool1, 3), driverA, device(device3, nil, nil)),
				sliceWithDevices(slice2, node1, resourcePool(pool1, 3), driverA, device(device2, nil, nil)),
				sliceWithDevices(slice1, node1, resourcePool(pool1, 3), driverA, device(device1, nil, nil)),
			),
			node: node(node1, region1),

			expectResults: []any{allocationResult(
				localNodeSelector(node1),
				deviceAllocationResult(req0, driverA, pool1, device1, false),
				deviceAllocationResult(req0, driverA, pool1, device2, false),
			)},
		},
		// 场景四：带 binding conditions 的 slice 排序。
		// 期望先选无条件的 slice2/slice3 中的设备；当选中带条件的 device1 时，
		// 结果中必须携带其 bindingConditions / bindingFailureConditions。
		"lexicographical-sorting-slices-with-binding-conditions": {
			features: Features{
				DeviceBindingAndStatus: true,
			},
			claimsToAllocate: objects(claimWithRequests(claim0, nil, request(req0, classA, 2))),
			classes:          objects(class(classA, driverA)),
			slices: unwrapResourceSlices(
				sliceWithDevices(slice1, node1, resourcePool(pool1, 3), driverA,
					device(device1, nil, nil).withBindingConditions([]string{"IsPrepare"}, []string{"BindingFailed"}),
				),
				sliceWithDevices(slice3, node1, resourcePool(pool1, 3), driverA, device(device3, nil, nil)),
				sliceWithDevices(slice2, node1, resourcePool(pool1, 3), driverA, device(device2, nil, nil)),
			),
			node: node(node1, region1),

			expectResults: []any{
				resourceapi.AllocationResult{
					Devices: resourceapi.DeviceAllocationResult{
						Results: []resourceapi.DeviceRequestAllocationResult{
							deviceRequestAllocationResultWithBindingConditions(req0, driverA, pool1, device1, []string{"IsPrepare"}, []string{"BindingFailed"}),
							deviceAllocationResult(req0, driverA, pool1, device2, false),
						},
					},
					NodeSelector: localNodeSelector(node1),
				},
			},
		},
	}

	// 复用通用测试运行器执行所有用例。
	RunTestAllocator(t, supportedFeatures, newAllocator, testcases)
}
