/*
Copyright The Kubernetes Authors.

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

package resourcepoolstatusrequest

import (
	"fmt"
	"maps"
	"strings"
	"testing"
	"time"

	resourcev1 "k8s.io/api/resource/v1"
	resourcev1alpha3 "k8s.io/api/resource/v1alpha3"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"
	featuregatetesting "k8s.io/component-base/featuregate/testing"
	"k8s.io/klog/v2/ktesting"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/utils/ptr"
)

// TestCalculatePoolStatus 验证不同输入下的池状态计算：过滤、合并、截断与不完整池判定。
func TestCalculatePoolStatus(t *testing.T) {
	testCases := []struct {
		// name 用例名称，用于定位失败场景。
		name string
		// request 待计算状态的请求，携带驱动与池名等过滤条件。
		request *resourcev1alpha3.ResourcePoolStatusRequest
		// slices 注入 informer 的 ResourceSlice 列表。
		slices []*resourcev1.ResourceSlice
		// claims 注入 informer 的 ResourceClaim 列表，用于统计已分配设备。
		claims []*resourcev1.ResourceClaim
		// expectedPools 期望返回的池数量。
		expectedPools int
		// expectedTotal 期望的设备总数。
		expectedTotal int32
		// expectedAlloc 期望的已分配设备数。
		expectedAlloc      int32
		expectedPoolCount  int32   // 期望的 PoolCount 值（限制前的匹配池总数）
		expectedTruncated  bool    // 为 true 时表示列表被截断，即 len(Pools) < PoolCount
		checkNodeName      bool    // 为 true 时校验首个池的 NodeName 与期望值一致
		expectedNodeName   *string // 首个池期望的 NodeName，nil 表示不应设置
		checkIncomplete    bool    // 为 true 时校验不完整池的行为
		expectedIncomplete int     // 期望设置了 ValidationError 的池数量
	}{
		{
			// 基线用例：单个完整池、无 claim，校验总数与零分配。
			name: "single-pool-no-allocations",
			request: &resourcev1alpha3.ResourcePoolStatusRequest{
				ObjectMeta: metav1.ObjectMeta{Name: "test-request"},
				Spec: resourcev1alpha3.ResourcePoolStatusRequestSpec{
					Driver: "test.example.com",
				},
			},
			slices: []*resourcev1.ResourceSlice{
				makeSlice("slice-1", "test.example.com", "pool-1", "node-1", 4),
			},
			claims:        []*resourcev1.ResourceClaim{},
			expectedPools: 1,
			expectedTotal: 4,
			expectedAlloc: 0,
		},
		{
			// 两个 claim 分别占用不同设备，分配数应为 2。
			name: "single-pool-with-allocations",
			request: &resourcev1alpha3.ResourcePoolStatusRequest{
				ObjectMeta: metav1.ObjectMeta{Name: "test-request"},
				Spec: resourcev1alpha3.ResourcePoolStatusRequestSpec{
					Driver: "test.example.com",
				},
			},
			slices: []*resourcev1.ResourceSlice{
				makeSlice("slice-1", "test.example.com", "pool-1", "node-1", 4),
			},
			claims: []*resourcev1.ResourceClaim{
				makeAllocatedClaim("claim-1", "default", "test.example.com", "pool-1", "device-0"),
				makeAllocatedClaim("claim-2", "default", "test.example.com", "pool-1", "device-1"),
			},
			expectedPools: 1,
			expectedTotal: 4,
			expectedAlloc: 2,
		},
		{
			// 不同池的设备数应合并统计，池数按池去重。
			name: "multiple-pools",
			request: &resourcev1alpha3.ResourcePoolStatusRequest{
				ObjectMeta: metav1.ObjectMeta{Name: "test-request"},
				Spec: resourcev1alpha3.ResourcePoolStatusRequestSpec{
					Driver: "test.example.com",
				},
			},
			slices: []*resourcev1.ResourceSlice{
				makeSlice("slice-1", "test.example.com", "pool-1", "node-1", 4),
				makeSlice("slice-2", "test.example.com", "pool-2", "node-2", 8),
			},
			claims:        []*resourcev1.ResourceClaim{},
			expectedPools: 2,
			expectedTotal: 12,
			expectedAlloc: 0,
		},
		{
			// 请求只关心指定驱动，另一个驱动的池必须被过滤掉。
			name: "filter-by-driver",
			request: &resourcev1alpha3.ResourcePoolStatusRequest{
				ObjectMeta: metav1.ObjectMeta{Name: "test-request"},
				Spec: resourcev1alpha3.ResourcePoolStatusRequestSpec{
					Driver: "test.example.com",
				},
			},
			slices: []*resourcev1.ResourceSlice{
				makeSlice("slice-1", "test.example.com", "pool-1", "node-1", 4),
				makeSlice("slice-2", "other.example.com", "pool-2", "node-2", 8),
			},
			claims:        []*resourcev1.ResourceClaim{},
			expectedPools: 1,
			expectedTotal: 4,
			expectedAlloc: 0,
		},
		{
			// 指定 PoolName 后只应返回同名池。
			name: "filter-by-pool-name",
			request: &resourcev1alpha3.ResourcePoolStatusRequest{
				ObjectMeta: metav1.ObjectMeta{Name: "test-request"},
				Spec: resourcev1alpha3.ResourcePoolStatusRequestSpec{
					Driver:   "test.example.com",
					PoolName: new("pool-1"),
				},
			},
			slices: []*resourcev1.ResourceSlice{
				makeSlice("slice-1", "test.example.com", "pool-1", "node-1", 4),
				makeSlice("slice-2", "test.example.com", "pool-2", "node-2", 8),
			},
			claims:        []*resourcev1.ResourceClaim{},
			expectedPools: 1,
			expectedTotal: 4,
			expectedAlloc: 0,
		},
		{
			// 同一池的多个 slice 应合并为一个池条目，计数相加。
			name: "multiple-slices-same-pool",
			request: &resourcev1alpha3.ResourcePoolStatusRequest{
				ObjectMeta: metav1.ObjectMeta{Name: "test-request"},
				Spec: resourcev1alpha3.ResourcePoolStatusRequestSpec{
					Driver: "test.example.com",
				},
			},
			slices: []*resourcev1.ResourceSlice{
				makeSlice("slice-1", "test.example.com", "pool-1", "node-1", 4),
				makeSlice("slice-2", "test.example.com", "pool-1", "node-1", 4),
			},
			claims:        []*resourcev1.ResourceClaim{},
			expectedPools: 1,
			expectedTotal: 8,
			expectedAlloc: 0,
		},
		{
			// Limit=1 时只返回一个池，但 PoolCount 保留匹配总数以告知调用方被截断。
			name: "limit-pools",
			request: &resourcev1alpha3.ResourcePoolStatusRequest{
				ObjectMeta: metav1.ObjectMeta{Name: "test-request"},
				Spec: resourcev1alpha3.ResourcePoolStatusRequestSpec{
					Driver: "test.example.com",
					Limit:  new(int32(1)),
				},
			},
			slices: []*resourcev1.ResourceSlice{
				makeSlice("slice-1", "test.example.com", "pool-1", "node-1", 4),
				makeSlice("slice-2", "test.example.com", "pool-2", "node-2", 8),
			},
			claims:            []*resourcev1.ResourceClaim{},
			expectedPools:     1,
			expectedPoolCount: 2,
			expectedTruncated: true,
		},
		{
			// 没有匹配 slice 时返回空列表而非报错。
			name: "no-matching-pools",
			request: &resourcev1alpha3.ResourcePoolStatusRequest{
				ObjectMeta: metav1.ObjectMeta{Name: "test-request"},
				Spec: resourcev1alpha3.ResourcePoolStatusRequestSpec{
					Driver: "nonexistent.example.com",
				},
			},
			slices: []*resourcev1.ResourceSlice{
				makeSlice("slice-1", "test.example.com", "pool-1", "node-1", 4),
			},
			claims:        []*resourcev1.ResourceClaim{},
			expectedPools: 0,
			expectedTotal: 0,
			expectedAlloc: 0,
		},
		{
			// 同一池存在多代 slice 时只取最新代，旧代必须丢弃。
			name: "older-generation-slices-ignored",
			request: &resourcev1alpha3.ResourcePoolStatusRequest{
				ObjectMeta: metav1.ObjectMeta{Name: "test-request"},
				Spec: resourcev1alpha3.ResourcePoolStatusRequestSpec{
					Driver: "test.example.com",
				},
			},
			slices: []*resourcev1.ResourceSlice{
				// 旧代 slice，应当被忽略
				makeSliceWithGeneration("slice-old", "test.example.com", "pool-1", "node-1", 8, 1),
				// 新一代 slice，应当被计入
				makeSliceWithGeneration("slice-new-1", "test.example.com", "pool-1", "node-1", 4, 2),
				makeSliceWithGeneration("slice-new-2", "test.example.com", "pool-1", "node-1", 4, 2),
			},
			claims:        []*resourcev1.ResourceClaim{},
			expectedPools: 1,
			expectedTotal: 8, // 只计入第 2 代的 4+4，而非第 1 代的 8
			expectedAlloc: 0,
		},
		{
			// slice 数量不足时池被标记为不完整，携带 ValidationError 且不计设备。
			name: "incomplete-pool-has-validation-error",
			request: &resourcev1alpha3.ResourcePoolStatusRequest{
				ObjectMeta: metav1.ObjectMeta{Name: "test-request"},
				Spec: resourcev1alpha3.ResourcePoolStatusRequestSpec{
					Driver: "test.example.com",
				},
			},
			slices: []*resourcev1.ResourceSlice{
				// 池期望 5 个 slice，但只发布了 3 个
				makeSliceWithExpectedCount("slice-1", "test.example.com", "pool-1", "node-1", 4, 1, 5),
				makeSliceWithExpectedCount("slice-2", "test.example.com", "pool-1", "node-1", 4, 1, 5),
				makeSliceWithExpectedCount("slice-3", "test.example.com", "pool-1", "node-1", 4, 1, 5),
			},
			claims:             []*resourcev1.ResourceClaim{},
			expectedPools:      1,
			checkIncomplete:    true,
			expectedIncomplete: 1,
		},
		{
			// 完整池与不完整池共存，只有完整池计入设备总数。
			name: "mix-of-complete-and-incomplete-pools",
			request: &resourcev1alpha3.ResourcePoolStatusRequest{
				ObjectMeta: metav1.ObjectMeta{Name: "test-request"},
				Spec: resourcev1alpha3.ResourcePoolStatusRequestSpec{
					Driver: "test.example.com",
				},
			},
			slices: []*resourcev1.ResourceSlice{
				// pool-1：完整（1/1 个 slice）
				makeSliceWithExpectedCount("slice-1", "test.example.com", "pool-1", "node-1", 4, 1, 1),
				// pool-2：不完整（2/3 个 slice）
				makeSliceWithExpectedCount("slice-2a", "test.example.com", "pool-2", "node-2", 4, 1, 3),
				makeSliceWithExpectedCount("slice-2b", "test.example.com", "pool-2", "node-2", 4, 1, 3),
			},
			claims:             []*resourcev1.ResourceClaim{},
			expectedPools:      2,
			expectedTotal:      4, // 只有完整的 pool-1 计入设备总数
			expectedAlloc:      0,
			checkIncomplete:    true,
			expectedIncomplete: 1,
		},
		{
			// 池内所有 slice 同属一个节点时，PoolStatus 应带上 NodeName。
			name: "same-node-pool-sets-nodename",
			request: &resourcev1alpha3.ResourcePoolStatusRequest{
				ObjectMeta: metav1.ObjectMeta{Name: "test-request"},
				Spec: resourcev1alpha3.ResourcePoolStatusRequestSpec{
					Driver: "test.example.com",
				},
			},
			slices: []*resourcev1.ResourceSlice{
				makeSlice("slice-1", "test.example.com", "pool-1", "node-1", 4),
				makeSlice("slice-2", "test.example.com", "pool-1", "node-1", 4),
			},
			claims:           []*resourcev1.ResourceClaim{},
			expectedPools:    1,
			expectedTotal:    8,
			expectedAlloc:    0,
			checkNodeName:    true,
			expectedNodeName: new("node-1"),
		},
		{
			// 池跨多个节点时 NodeName 必须清空，避免误导调用方。
			name: "mixed-node-pool-clears-nodename",
			request: &resourcev1alpha3.ResourcePoolStatusRequest{
				ObjectMeta: metav1.ObjectMeta{Name: "test-request"},
				Spec: resourcev1alpha3.ResourcePoolStatusRequestSpec{
					Driver: "test.example.com",
				},
			},
			slices: []*resourcev1.ResourceSlice{
				makeSlice("slice-1", "test.example.com", "pool-1", "node-1", 4),
				makeSlice("slice-2", "test.example.com", "pool-1", "node-2", 4),
			},
			claims:           []*resourcev1.ResourceClaim{},
			expectedPools:    1,
			expectedTotal:    8,
			expectedAlloc:    0,
			checkNodeName:    true,
			expectedNodeName: nil,
		},
		{
			// slice 未指定节点时也不应推断出 NodeName。
			name: "nil-nodename-slices-no-nodename",
			request: &resourcev1alpha3.ResourcePoolStatusRequest{
				ObjectMeta: metav1.ObjectMeta{Name: "test-request"},
				Spec: resourcev1alpha3.ResourcePoolStatusRequestSpec{
					Driver: "test.example.com",
				},
			},
			slices: []*resourcev1.ResourceSlice{
				makeSliceWithoutNode("slice-1", "test.example.com", "pool-1", 4),
				makeSliceWithoutNode("slice-2", "test.example.com", "pool-1", 4),
			},
			claims:           []*resourcev1.ResourceClaim{},
			expectedPools:    1,
			expectedTotal:    8,
			expectedAlloc:    0,
			checkNodeName:    true,
			expectedNodeName: nil,
		},
		{
			// 所有 slice 都发布齐全时不产生 ValidationError。
			name: "all-pools-complete-no-validation-errors",
			request: &resourcev1alpha3.ResourcePoolStatusRequest{
				ObjectMeta: metav1.ObjectMeta{Name: "test-request"},
				Spec: resourcev1alpha3.ResourcePoolStatusRequestSpec{
					Driver: "test.example.com",
				},
			},
			slices: []*resourcev1.ResourceSlice{
				// pool-1 的 slice 已全部就绪（2/2）。
				makeSliceWithExpectedCount("slice-1a", "test.example.com", "pool-1", "node-1", 4, 1, 2),
				makeSliceWithExpectedCount("slice-1b", "test.example.com", "pool-1", "node-1", 4, 1, 2),
			},
			claims:             []*resourcev1.ResourceClaim{},
			expectedPools:      1,
			expectedTotal:      8,
			expectedAlloc:      0,
			checkIncomplete:    true,
			expectedIncomplete: 0,
		},
	}

	// 直接调用计算方法而非启动控制器循环，避免异步处理干扰断言。
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, ctx := ktesting.NewTestContext(t)

			// 创建 fake 客户端与 informer
			fakeClient := fake.NewClientset()
			informerFactory := informers.NewSharedInformerFactory(fakeClient, 0)

			// 创建控制器
			controller, err := NewController(ctx, fakeClient,
				informerFactory.Resource().V1alpha3().ResourcePoolStatusRequests(),
				informerFactory.Resource().V1().ResourceSlices(),
				informerFactory.Resource().V1().ResourceClaims(),
				informerFactory.Resource().V1().DeviceTaintRules(),
			)
			if err != nil {
				t.Fatalf("Failed to create controller: %v", err)
			}

			// 将 slice 加入 informer store
			for _, slice := range tc.slices {
				err := informerFactory.Resource().V1().ResourceSlices().Informer().GetStore().Add(slice)
				if err != nil {
					t.Fatalf("Failed to add slice to informer: %v", err)
				}
			}

			// 将 claim 加入 informer store
			for _, claim := range tc.claims {
				err := informerFactory.Resource().V1().ResourceClaims().Informer().GetStore().Add(claim)
				if err != nil {
					t.Fatalf("Failed to add claim to informer: %v", err)
				}
			}

			// 直接通过 lister 计算池状态
			status := controller.calculatePoolStatus(ctx, tc.request)

			// 校验池数量
			if len(status.Pools) != tc.expectedPools {
				t.Errorf("Expected %d pools, got %d", tc.expectedPools, len(status.Pools))
			}

			// 校验 PoolCount 字段
			if tc.expectedPoolCount > 0 {
				if status.PoolCount == nil {
					t.Errorf("Expected PoolCount to be %d, got nil", tc.expectedPoolCount)
				} else if *status.PoolCount != tc.expectedPoolCount {
					t.Errorf("Expected PoolCount %d, got %d", tc.expectedPoolCount, *status.PoolCount)
				}
			}

			// 校验截断：len(Pools) < PoolCount 即表示结果被截断
			if tc.expectedTruncated {
				if status.PoolCount == nil || int32(len(status.Pools)) >= *status.PoolCount {
					t.Errorf("Expected truncation (len(Pools)=%d < PoolCount), but PoolCount=%v", len(status.Pools), status.PoolCount)
				}
			}

			// 只统计完整池的设备数（即没有 ValidationError 的池）
			if tc.expectedPools > 0 && !tc.expectedTruncated {
				var totalDevices, allocatedDevices int32
				for _, pool := range status.Pools {
					if pool.TotalDevices != nil {
						totalDevices += *pool.TotalDevices
					}
					if pool.AllocatedDevices != nil {
						allocatedDevices += *pool.AllocatedDevices
					}
				}
				if totalDevices != tc.expectedTotal {
					t.Errorf("Expected total devices %d, got %d", tc.expectedTotal, totalDevices)
				}
				if allocatedDevices != tc.expectedAlloc {
					t.Errorf("Expected allocated devices %d, got %d", tc.expectedAlloc, allocatedDevices)
				}
			}

			// 用例指定期望值时校验 NodeName
			if tc.checkNodeName && len(status.Pools) == 1 {
				pool := status.Pools[0]
				if tc.expectedNodeName != nil {
					if pool.NodeName == nil {
						t.Errorf("Expected NodeName %q, got nil", *tc.expectedNodeName)
					} else if *pool.NodeName != *tc.expectedNodeName {
						t.Errorf("Expected NodeName %q, got %q", *tc.expectedNodeName, *pool.NodeName)
					}
				} else if pool.NodeName != nil {
					t.Errorf("Expected NodeName to be nil, got %q", *pool.NodeName)
				}
			}

			// 校验不完整池数量（设置了 ValidationError 的池）
			if tc.checkIncomplete {
				incompleteCount := 0
				for _, pool := range status.Pools {
					if pool.ValidationError != nil {
						incompleteCount++
						// 不完整池不应填写设备计数和 ResourceSliceCount
						if pool.TotalDevices != nil {
							t.Errorf("Incomplete pool %s/%s should have nil TotalDevices, got %d", pool.Driver, pool.PoolName, *pool.TotalDevices)
						}
						if pool.AllocatedDevices != nil {
							t.Errorf("Incomplete pool %s/%s should have nil AllocatedDevices, got %d", pool.Driver, pool.PoolName, *pool.AllocatedDevices)
						}
						if pool.ResourceSliceCount != nil {
							t.Errorf("Incomplete pool %s/%s should have nil ResourceSliceCount, got %d", pool.Driver, pool.PoolName, *pool.ResourceSliceCount)
						}
					}
				}
				if incompleteCount != tc.expectedIncomplete {
					t.Errorf("Expected %d incomplete pools (with ValidationError), got %d", tc.expectedIncomplete, incompleteCount)
				}
			}

			// 校验 Conditions 已设置
			if len(status.Conditions) == 0 {
				t.Error("Expected Conditions to be set")
			}
		})
	}
}

// TestSyncRequest 验证同步请求后会通过 status 子资源写回状态。
func TestSyncRequest(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)

	// 创建 fake 客户端与 informer
	request := &resourcev1alpha3.ResourcePoolStatusRequest{
		ObjectMeta: metav1.ObjectMeta{Name: "test-request"},
		Spec: resourcev1alpha3.ResourcePoolStatusRequestSpec{
			Driver: "test.example.com",
		},
	}

	fakeClient := fake.NewClientset(request)
	informerFactory := informers.NewSharedInformerFactory(fakeClient, 0)

	// 创建控制器
	controller, err := NewController(ctx, fakeClient,
		informerFactory.Resource().V1alpha3().ResourcePoolStatusRequests(),
		informerFactory.Resource().V1().ResourceSlices(),
		informerFactory.Resource().V1().ResourceClaims(),
		informerFactory.Resource().V1().DeviceTaintRules(),
	)
	if err != nil {
		t.Fatalf("Failed to create controller: %v", err)
	}

	// 将请求加入 informer
	err = informerFactory.Resource().V1alpha3().ResourcePoolStatusRequests().Informer().GetStore().Add(request)
	if err != nil {
		t.Fatalf("Failed to add request to informer: %v", err)
	}

	// 添加一个 slice
	slice := makeSlice("slice-1", "test.example.com", "pool-1", "node-1", 4)
	err = informerFactory.Resource().V1().ResourceSlices().Informer().GetStore().Add(slice)
	if err != nil {
		t.Fatalf("Failed to add slice to informer: %v", err)
	}

	// 同步该请求
	err = controller.syncRequest(ctx, "test-request")
	if err != nil {
		t.Fatalf("syncRequest failed: %v", err)
	}

	// 校验状态已更新（fake 客户端应收到 status 子资源更新）
	actions := fakeClient.Actions()
	var foundUpdate bool
	for _, action := range actions {
		if action.GetVerb() == "update" && action.GetSubresource() == "status" {
			foundUpdate = true
			break
		}
	}
	if !foundUpdate {
		t.Error("Expected status update action, but none found")
	}
}

// TestSyncRequestRequeuesIncompletePool 验证不完整池通过返回错误触发重新入队，且重试耗尽后仍不写状态。
func TestSyncRequestRequeuesIncompletePool(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)

	request := &resourcev1alpha3.ResourcePoolStatusRequest{
		ObjectMeta: metav1.ObjectMeta{Name: "test-request"},
		Spec: resourcev1alpha3.ResourcePoolStatusRequestSpec{
			Driver: "test.example.com",
		},
	}

	fakeClient := fake.NewClientset(request)
	informerFactory := informers.NewSharedInformerFactory(fakeClient, 0)

	controller, err := NewController(ctx, fakeClient,
		informerFactory.Resource().V1alpha3().ResourcePoolStatusRequests(),
		informerFactory.Resource().V1().ResourceSlices(),
		informerFactory.Resource().V1().ResourceClaims(),
		informerFactory.Resource().V1().DeviceTaintRules(),
	)
	if err != nil {
		t.Fatalf("Failed to create controller: %v", err)
	}

	// 将请求加入 informer
	err = informerFactory.Resource().V1alpha3().ResourcePoolStatusRequests().Informer().GetStore().Add(request)
	if err != nil {
		t.Fatalf("Failed to add request to informer: %v", err)
	}

	// 添加一个不完整池（期望 3 个 slice，只发布了 1 个）
	slice := makeSliceWithExpectedCount("slice-1", "test.example.com", "pool-1", "node-1", 4, 1, 3)
	err = informerFactory.Resource().V1().ResourceSlices().Informer().GetStore().Add(slice)
	if err != nil {
		t.Fatalf("Failed to add slice to informer: %v", err)
	}

	// 不完整池必须始终由 syncRequest 返回错误，
	// 重试计数与丢弃由 processNextWorkItem 负责。
	err = controller.syncRequest(ctx, "test-request")
	if err == nil {
		t.Fatal("Expected syncRequest to return error for incomplete pool requeue, got nil")
	}

	// 校验没有更新状态
	for _, action := range fakeClient.Actions() {
		if action.GetVerb() == "update" && action.GetSubresource() == "status" {
			t.Error("Should not update status when requeueing for incomplete pools")
		}
	}

	// 即使重试耗尽，syncRequest 仍返回错误；
	// 是否丢弃键由 processNextWorkItem 决定。
	for range maxRetries {
		controller.workqueue.AddRateLimited("test-request")
		key, _ := controller.workqueue.Get()
		controller.workqueue.Done(key)
	}

	fakeClient.ClearActions()
	err = controller.syncRequest(ctx, "test-request")
	if err == nil {
		t.Fatal("Expected syncRequest to still return error for incomplete pools after retries exhausted")
	}
}

// TestSyncRequestWritesStructuralViewError 验证结构性错误直接写入状态，且不再入队重试。
func TestSyncRequestWritesStructuralViewError(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)

	request := &resourcev1alpha3.ResourcePoolStatusRequest{
		ObjectMeta: metav1.ObjectMeta{Name: "test-request"},
		Spec: resourcev1alpha3.ResourcePoolStatusRequestSpec{
			Driver: "test.example.com",
		},
	}

	fakeClient := fake.NewClientset(request)
	informerFactory := informers.NewSharedInformerFactory(fakeClient, 0)

	controller, err := NewController(ctx, fakeClient,
		informerFactory.Resource().V1alpha3().ResourcePoolStatusRequests(),
		informerFactory.Resource().V1().ResourceSlices(),
		informerFactory.Resource().V1().ResourceClaims(),
		informerFactory.Resource().V1().DeviceTaintRules(),
	)
	if err != nil {
		t.Fatalf("Failed to create controller: %v", err)
	}

	if err := informerFactory.Resource().V1alpha3().ResourcePoolStatusRequests().Informer().GetStore().Add(request); err != nil {
		t.Fatalf("Failed to add request to informer: %v", err)
	}

	// 构造一个完整池（1/1 个 slice）并注入永久结构性错误：
	// 它声明了 partitionTypeAttribute 却没有发布 sharedCounters。
	slice := makeSlice("slice-1", "test.example.com", "pool-1", "node-1", 2)
	attr := resourcev1.FullyQualifiedName("test.example.com/profile")
	slice.Spec.PartitionTypeAttribute = &attr
	if err := informerFactory.Resource().V1().ResourceSlices().Informer().GetStore().Add(slice); err != nil {
		t.Fatalf("Failed to add slice to informer: %v", err)
	}

	// 结构性错误是永久的：syncRequest 不得重新入队，而必须
	// 写入状态，让错误对用户可见。
	if err := controller.syncRequest(ctx, "test-request"); err != nil {
		t.Fatalf("syncRequest requeued on a permanent structural error: %v", err)
	}

	updated, err := fakeClient.ResourceV1alpha3().ResourcePoolStatusRequests().Get(ctx, "test-request", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Failed to get request: %v", err)
	}
	if updated.Status == nil {
		t.Fatal("Expected status to be written for a structural view error, got nil")
	}
	if len(updated.Status.Pools) != 1 {
		t.Fatalf("Expected 1 pool, got %d", len(updated.Status.Pools))
	}
	ve := updated.Status.Pools[0].ValidationError
	if ve == nil {
		t.Fatal("Expected pool ValidationError to be set")
	}
	if !strings.HasPrefix(*ve, prefixPartitionTypeMissing) {
		t.Errorf("Expected validationError with prefix %q, got %q", prefixPartitionTypeMissing, *ve)
	}
}

// TestSkipProcessedRequest 验证已处理过的请求被跳过，不会再次更新状态。
func TestSkipProcessedRequest(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)

	now := metav1.Now()
	// 构造已处理过的请求（Status 非空且带有 Conditions）
	request := &resourcev1alpha3.ResourcePoolStatusRequest{
		ObjectMeta: metav1.ObjectMeta{Name: "test-request"},
		Spec: resourcev1alpha3.ResourcePoolStatusRequestSpec{
			Driver: "test.example.com",
		},
		Status: &resourcev1alpha3.ResourcePoolStatusRequestStatus{
			PoolCount: new(int32(0)),
			Conditions: []metav1.Condition{
				{
					Type:               resourcev1alpha3.ResourcePoolStatusRequestConditionComplete,
					Status:             metav1.ConditionTrue,
					LastTransitionTime: now,
					Reason:             "CalculationComplete",
					Message:            "Calculated status for 0 pools",
				},
			},
		},
	}

	fakeClient := fake.NewClientset(request)
	informerFactory := informers.NewSharedInformerFactory(fakeClient, 0)

	controller, err := NewController(ctx, fakeClient,
		informerFactory.Resource().V1alpha3().ResourcePoolStatusRequests(),
		informerFactory.Resource().V1().ResourceSlices(),
		informerFactory.Resource().V1().ResourceClaims(),
		informerFactory.Resource().V1().DeviceTaintRules(),
	)
	if err != nil {
		t.Fatalf("Failed to create controller: %v", err)
	}

	err = informerFactory.Resource().V1alpha3().ResourcePoolStatusRequests().Informer().GetStore().Add(request)
	if err != nil {
		t.Fatalf("Failed to add request to informer: %v", err)
	}

	// 同步应跳过已处理的请求
	err = controller.syncRequest(ctx, "test-request")
	if err != nil {
		t.Fatalf("syncRequest failed: %v", err)
	}

	// 校验没有执行更新
	actions := fakeClient.Actions()
	for _, action := range actions {
		if action.GetVerb() == "update" && action.GetSubresource() == "status" {
			t.Error("Should not update status for already-processed request")
		}
	}
}

// 辅助函数

// makeSlice 构造单个 slice，默认第 1 代且资源计数为 1。
func makeSlice(name, driver, pool, node string, deviceCount int) *resourcev1.ResourceSlice {
	return makeSliceWithGenerationAndCount(name, driver, pool, node, deviceCount, 1, 1)
}

// makeSliceWithExpectedCount 构造声明了预期 slice 数量的 slice，用于不完整池场景。
func makeSliceWithExpectedCount(name, driver, pool, node string, deviceCount int, generation int64, resourceSliceCount int64) *resourcev1.ResourceSlice {
	return makeSliceWithGenerationAndCount(name, driver, pool, node, deviceCount, generation, resourceSliceCount)
}

// makeSliceWithGeneration 构造指定代际的 slice。
func makeSliceWithGeneration(name, driver, pool, node string, deviceCount int, generation int64) *resourcev1.ResourceSlice {
	return makeSliceWithGenerationAndCount(name, driver, pool, node, deviceCount, generation, 1)
}

// makeSliceWithGenerationAndCount 构造指定代际、设备数和资源计数的 slice。
func makeSliceWithGenerationAndCount(name, driver, pool, node string, deviceCount int, generation int64, resourceSliceCount int64) *resourcev1.ResourceSlice {
	devices := make([]resourcev1.Device, deviceCount)
	for i := range deviceCount {
		devices[i] = resourcev1.Device{
			Name: fmt.Sprintf("device-%d", i),
		}
	}

	return &resourcev1.ResourceSlice{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
		Spec: resourcev1.ResourceSliceSpec{
			Driver:   driver,
			NodeName: &node,
			Pool: resourcev1.ResourcePool{
				Name:               pool,
				Generation:         generation,
				ResourceSliceCount: resourceSliceCount,
			},
			Devices: devices,
		},
	}
}

// makeAllocatedClaim 构造指向指定设备的一次普通分配。
func makeAllocatedClaim(name, namespace, driver, pool, device string) *resourcev1.ResourceClaim {
	return &resourcev1.ResourceClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: resourcev1.ResourceClaimSpec{},
		Status: resourcev1.ResourceClaimStatus{
			Allocation: &resourcev1.AllocationResult{
				Devices: resourcev1.DeviceAllocationResult{
					Results: []resourcev1.DeviceRequestAllocationResult{
						{
							Driver: driver,
							Pool:   pool,
							Device: device,
						},
					},
				},
			},
		},
	}
}

// makeAdminAccessClaim 构造 AdminAccess 分配，用于验证观察者不计入分配统计。
func makeAdminAccessClaim(name, namespace, driver, pool, device string) *resourcev1.ResourceClaim {
	claim := makeAllocatedClaim(name, namespace, driver, pool, device)
	adminAccess := true
	claim.Status.Allocation.Devices.Results[0].AdminAccess = &adminAccess
	return claim
}

// makeSliceWithTaintedDevices 给前 taintedCount 个设备打上指定 effect 的污点。
func makeSliceWithTaintedDevices(name, driver, pool, node string, deviceCount, taintedCount int, effect resourcev1.DeviceTaintEffect) *resourcev1.ResourceSlice {
	slice := makeSlice(name, driver, pool, node, deviceCount)
	for i := 0; i < taintedCount && i < deviceCount; i++ {
		slice.Spec.Devices[i].Taints = []resourcev1.DeviceTaint{{
			Key:    "example.com/maintenance",
			Effect: effect,
		}}
	}
	return slice
}

// makeRequest 构造指定驱动的 ResourcePoolStatusRequest。
func makeRequest(driver string) *resourcev1alpha3.ResourcePoolStatusRequest {
	return &resourcev1alpha3.ResourcePoolStatusRequest{
		ObjectMeta: metav1.ObjectMeta{Name: "test-request"},
		Spec:       resourcev1alpha3.ResourcePoolStatusRequestSpec{Driver: driver},
	}
}

// runCalculatePoolStatus 填充 informer store 后执行池状态计算。
func runCalculatePoolStatus(t *testing.T, request *resourcev1alpha3.ResourcePoolStatusRequest, slices []*resourcev1.ResourceSlice, claims []*resourcev1.ResourceClaim, rules ...*resourcev1.DeviceTaintRule) resourcev1alpha3.ResourcePoolStatusRequestStatus {
	t.Helper()
	_, ctx := ktesting.NewTestContext(t)
	fakeClient := fake.NewClientset()
	informerFactory := informers.NewSharedInformerFactory(fakeClient, 0)
	controller, err := NewController(ctx, fakeClient,
		informerFactory.Resource().V1alpha3().ResourcePoolStatusRequests(),
		informerFactory.Resource().V1().ResourceSlices(),
		informerFactory.Resource().V1().ResourceClaims(),
		informerFactory.Resource().V1().DeviceTaintRules(),
	)
	if err != nil {
		t.Fatalf("Failed to create controller: %v", err)
	}
	for _, slice := range slices {
		if err := informerFactory.Resource().V1().ResourceSlices().Informer().GetStore().Add(slice); err != nil {
			t.Fatalf("Failed to add slice: %v", err)
		}
	}
	for _, claim := range claims {
		if err := informerFactory.Resource().V1().ResourceClaims().Informer().GetStore().Add(claim); err != nil {
			t.Fatalf("Failed to add claim: %v", err)
		}
	}
	for _, rule := range rules {
		if err := informerFactory.Resource().V1().DeviceTaintRules().Informer().GetStore().Add(rule); err != nil {
			t.Fatalf("Failed to add taint rule: %v", err)
		}
	}
	return controller.calculatePoolStatus(ctx, request)
}

// makeDeviceTaintRule 通过外部规则给池中指定设备打污点。
func makeDeviceTaintRule(name, driver, pool, device string, effect resourcev1.DeviceTaintEffect) *resourcev1.DeviceTaintRule {
	return &resourcev1.DeviceTaintRule{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: resourcev1.DeviceTaintRuleSpec{
			DeviceSelector: &resourcev1.DeviceTaintSelector{
				Driver: &driver,
				Pool:   &pool,
				Device: &device,
			},
			Taint: resourcev1.DeviceTaint{
				Key:    "example.com/maintenance",
				Effect: effect,
			},
		},
	}
}

// requireSinglePool 断言状态中恰好有一个池并返回它。
func requireSinglePool(t *testing.T, status resourcev1alpha3.ResourcePoolStatusRequestStatus) resourcev1alpha3.PoolStatus {
	t.Helper()
	if len(status.Pools) != 1 {
		t.Fatalf("expected 1 pool, got %d", len(status.Pools))
	}
	return status.Pools[0]
}

// derefInt32 对 nil 返回 -1，以便未设置的计数在断言时明显失败。
func derefInt32(p *int32) int32 {
	if p == nil {
		return -1
	}
	return *p
}

// TestCalculatePoolStatus_DeviceCounts 验证设备计数：重复分配、AdminAccess、污点与污点规则。
func TestCalculatePoolStatus_DeviceCounts(t *testing.T) {
	driver := "test.example.com"
	// 按指定污点效果构造含两个被污染设备的 slice。
	plainSlices := []*resourcev1.ResourceSlice{makeSlice("slice-1", driver, "pool-1", "node-1", 4)}
	taintedSlices := func(effect resourcev1.DeviceTaintEffect) []*resourcev1.ResourceSlice {
		return []*resourcev1.ResourceSlice{makeSliceWithTaintedDevices("slice-1", driver, "pool-1", "node-1", 5, 2, effect)}
	}
	taintRule := makeDeviceTaintRule("rule-1", driver, "pool-1", "device-0", resourcev1.DeviceTaintEffectNoSchedule)

	testCases := map[string]struct {
		// slices 注入的 ResourceSlice 列表。
		slices []*resourcev1.ResourceSlice
		// claims 注入的 ResourceClaim 列表。
		claims []*resourcev1.ResourceClaim
		// rules 注入的 DeviceTaintRule 列表。
		rules []*resourcev1.DeviceTaintRule
		// enableTaintRules 是否开启设备污点规则特性门控。
		enableTaintRules bool
		// wantTotal 期望的设备总数。
		wantTotal int32
		// wantAllocated 期望的已分配设备数。
		wantAllocated int32
		// wantAvailable 期望的可用设备数。
		wantAvailable int32
		// wantUnavailable 期望的不可用设备数。
		wantUnavailable int32
	}{
		// 三个 claim 指向同一物理设备，只能计为一次分配。
		"repeated claims on one device count once": {
			slices: plainSlices,
			claims: []*resourcev1.ResourceClaim{
				makeAllocatedClaim("claim-1", "default", driver, "pool-1", "device-0"),
				makeAllocatedClaim("claim-2", "default", driver, "pool-1", "device-0"),
				makeAllocatedClaim("claim-3", "default", driver, "pool-1", "device-0"),
			},
			wantTotal: 4, wantAllocated: 1, wantAvailable: 3,
		},
		// AdminAccess 分配只是观察者，不应改变任何计数。
		"adminAccess claim is not an allocation": {
			slices: plainSlices,
			claims: []*resourcev1.ResourceClaim{
				makeAllocatedClaim("claim-1", "default", driver, "pool-1", "device-0"),
				makeAdminAccessClaim("admin-1", "default", driver, "pool-1", "device-1"),
			},
			wantTotal: 4, wantAllocated: 1, wantAvailable: 3,
		},
		// NoSchedule 污点使设备不可用，总数不变但可用数减少。
		"noSchedule taint is unavailable": {
			slices:    taintedSlices(resourcev1.DeviceTaintEffectNoSchedule),
			wantTotal: 5, wantAvailable: 3, wantUnavailable: 2,
		},
		// NoExecute 与 NoSchedule 一样计入不可用。
		"noExecute taint is unavailable": {
			slices:    taintedSlices(resourcev1.DeviceTaintEffectNoExecute),
			wantTotal: 5, wantAvailable: 3, wantUnavailable: 2,
		},
		// None 效果的污点不会让设备不可用，其计数
		// 与未打污点的设备相同；未知效果也按 None 处理。
		"none taint is available": {
			slices:    taintedSlices(resourcev1.DeviceTaintEffectNone),
			wantTotal: 5, wantAvailable: 5, wantUnavailable: 0,
		},
		// 匹配的 DeviceTaintRule 仅在特性门控开启时生效。
		"taint rule applies when gate is enabled": {
			slices:           plainSlices,
			rules:            []*resourcev1.DeviceTaintRule{taintRule},
			enableTaintRules: true,
			wantTotal:        4, wantAvailable: 3, wantUnavailable: 1,
		},
		// 门控关闭时规则必须被忽略，否则会改变设备可用性统计。
		"taint rule ignored when gate is disabled": {
			slices:    plainSlices,
			rules:     []*resourcev1.DeviceTaintRule{taintRule},
			wantTotal: 4, wantAvailable: 4, wantUnavailable: 0,
		},
	}

	// 每个子用例单独设置门控，避免开关状态残留影响其他场景。
	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.DRADeviceTaintRules, tc.enableTaintRules)
			pool := requireSinglePool(t, runCalculatePoolStatus(t, makeRequest(driver), tc.slices, tc.claims, tc.rules...))
			if got := derefInt32(pool.TotalDevices); got != tc.wantTotal {
				t.Errorf("TotalDevices = %d, want %d", got, tc.wantTotal)
			}
			if got := derefInt32(pool.AllocatedDevices); got != tc.wantAllocated {
				t.Errorf("AllocatedDevices = %d, want %d", got, tc.wantAllocated)
			}
			if got := derefInt32(pool.AvailableDevices); got != tc.wantAvailable {
				t.Errorf("AvailableDevices = %d, want %d", got, tc.wantAvailable)
			}
			if got := derefInt32(pool.UnavailableDevices); got != tc.wantUnavailable {
				t.Errorf("UnavailableDevices = %d, want %d", got, tc.wantUnavailable)
			}
		})
	}
}

// TestIsOlderThan 验证给定时间是否早于持续时间窗口的起点。
func TestIsOlderThan(t *testing.T) {
	testCases := []struct {
		// name 用例名称。
		name string
		// time 待判断的时间，零值表示从未更新。
		time time.Time
		// duration 判断窗口。
		duration time.Duration
		// expected 是否应判定为过期。
		expected bool
	}{
		{
			// 零值表示时间未初始化，不能误判为过期。
			name:     "zero-time-is-not-older",
			time:     time.Time{},
			duration: time.Hour,
			expected: false,
		},
		{
			// 窗口内的时间不应过期。
			name:     "recent-time-is-not-older",
			time:     time.Now().Add(-30 * time.Minute),
			duration: time.Hour,
			expected: false,
		},
		{
			// 超出窗口的时间应判定为过期。
			name:     "old-time-is-older",
			time:     time.Now().Add(-2 * time.Hour),
			duration: time.Hour,
			expected: true,
		},
		{
			// 恰好还差一分钟才到边界，不能提前判定过期。
			name:     "just-under-boundary-is-not-older",
			time:     time.Now().Add(-time.Hour + time.Minute),
			duration: time.Hour,
			expected: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// 直接调用纯函数，不需要控制器或客户端。
			result := isOlderThan(tc.time, tc.duration)
			if result != tc.expected {
				t.Errorf("isOlderThan(%v, %v) = %v, expected %v", tc.time, tc.duration, result, tc.expected)
			}
		})
	}
}

// TestShouldDeleteRequest 验证已完成与待处理请求在不同过期时间下的删除判定。
func TestShouldDeleteRequest(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)

	fakeClient := fake.NewClientset()
	informerFactory := informers.NewSharedInformerFactory(fakeClient, 0)

	controller, err := NewController(ctx, fakeClient,
		informerFactory.Resource().V1alpha3().ResourcePoolStatusRequests(),
		informerFactory.Resource().V1().ResourceSlices(),
		informerFactory.Resource().V1().ResourceClaims(),
		informerFactory.Resource().V1().DeviceTaintRules(),
	)
	if err != nil {
		t.Fatalf("Failed to create controller: %v", err)
	}

	testCases := []struct {
		// name 用例名称。
		name string
		// request 待判定的资源池状态请求。
		request *resourcev1alpha3.ResourcePoolStatusRequest
		// expectedDelete 是否应被删除。
		expectedDelete bool
	}{
		{
			// 已完成请求以 Complete 条件时间为准，未超过 1 小时不删除。
			name: "completed-request-not-expired",
			request: &resourcev1alpha3.ResourcePoolStatusRequest{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "test-request",
					CreationTimestamp: metav1.Now(),
				},
				Spec: resourcev1alpha3.ResourcePoolStatusRequestSpec{
					Driver: "test.example.com",
				},
				Status: &resourcev1alpha3.ResourcePoolStatusRequestStatus{
					PoolCount: new(int32(0)),
					Conditions: []metav1.Condition{
						{
							Type:               resourcev1alpha3.ResourcePoolStatusRequestConditionComplete,
							Status:             metav1.ConditionTrue,
							LastTransitionTime: metav1.Now(),
							Reason:             "CalculationComplete",
							Message:            "Calculated status for 0 pools",
						},
					},
				},
			},
			expectedDelete: false,
		},
		{
			// 条件时间超过 completedRequestTTL，应删除。
			name: "completed-request-expired",
			request: &resourcev1alpha3.ResourcePoolStatusRequest{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "test-request",
					CreationTimestamp: metav1.NewTime(time.Now().Add(-2 * time.Hour)),
				},
				Spec: resourcev1alpha3.ResourcePoolStatusRequestSpec{
					Driver: "test.example.com",
				},
				Status: &resourcev1alpha3.ResourcePoolStatusRequestStatus{
					PoolCount: new(int32(0)),
					Conditions: []metav1.Condition{
						{
							Type:               resourcev1alpha3.ResourcePoolStatusRequestConditionComplete,
							Status:             metav1.ConditionTrue,
							LastTransitionTime: metav1.NewTime(time.Now().Add(-2 * time.Hour)),
							Reason:             "CalculationComplete",
							Message:            "Calculated status for 0 pools",
						},
					},
				},
			},
			expectedDelete: true,
		},
		{
			// 未完成请求按创建时间算，短 TTL 内保留。
			name: "pending-request-not-expired",
			request: &resourcev1alpha3.ResourcePoolStatusRequest{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "test-request",
					CreationTimestamp: metav1.Now(),
				},
				Spec: resourcev1alpha3.ResourcePoolStatusRequestSpec{
					Driver: "test.example.com",
				},
				// 无 Status：请求仍在等待处理
			},
			expectedDelete: false,
		},
		{
			// 未完成且创建时间超过 pendingRequestTTL，兜底删除。
			name: "pending-request-expired",
			request: &resourcev1alpha3.ResourcePoolStatusRequest{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "test-request",
					CreationTimestamp: metav1.NewTime(time.Now().Add(-25 * time.Hour)),
				},
				Spec: resourcev1alpha3.ResourcePoolStatusRequestSpec{
					Driver: "test.example.com",
				},
				// 无 Status：待处理请求已过期
			},
			expectedDelete: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := controller.shouldDeleteRequest(tc.request)
			if result != tc.expectedDelete {
				t.Errorf("shouldDeleteRequest() = %v, expected %v", result, tc.expectedDelete)
			}
		})
	}
}

// TestCleanupExpiredRequests 验证清理只删除过期请求，保留未过期请求。
func TestCleanupExpiredRequests(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)

	// 构造两个请求：一个已过期，一个未过期
	expiredRequest := &resourcev1alpha3.ResourcePoolStatusRequest{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "expired-request",
			CreationTimestamp: metav1.NewTime(time.Now().Add(-2 * time.Hour)),
		},
		Spec: resourcev1alpha3.ResourcePoolStatusRequestSpec{
			Driver: "test.example.com",
		},
		Status: &resourcev1alpha3.ResourcePoolStatusRequestStatus{
			PoolCount: new(int32(0)),
			Conditions: []metav1.Condition{
				{
					Type:               resourcev1alpha3.ResourcePoolStatusRequestConditionComplete,
					Status:             metav1.ConditionTrue,
					LastTransitionTime: metav1.NewTime(time.Now().Add(-2 * time.Hour)),
					Reason:             "CalculationComplete",
					Message:            "Calculated status for 0 pools",
				},
			},
		},
	}

	activeRequest := &resourcev1alpha3.ResourcePoolStatusRequest{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "active-request",
			CreationTimestamp: metav1.Now(),
		},
		Spec: resourcev1alpha3.ResourcePoolStatusRequestSpec{
			Driver: "test.example.com",
		},
		Status: &resourcev1alpha3.ResourcePoolStatusRequestStatus{
			PoolCount: new(int32(0)),
			Conditions: []metav1.Condition{
				{
					Type:               resourcev1alpha3.ResourcePoolStatusRequestConditionComplete,
					Status:             metav1.ConditionTrue,
					LastTransitionTime: metav1.Now(),
					Reason:             "CalculationComplete",
					Message:            "Calculated status for 0 pools",
				},
			},
		},
	}

	fakeClient := fake.NewClientset(expiredRequest, activeRequest)
	informerFactory := informers.NewSharedInformerFactory(fakeClient, 0)

	controller, err := NewController(ctx, fakeClient,
		informerFactory.Resource().V1alpha3().ResourcePoolStatusRequests(),
		informerFactory.Resource().V1().ResourceSlices(),
		informerFactory.Resource().V1().ResourceClaims(),
		informerFactory.Resource().V1().DeviceTaintRules(),
	)
	if err != nil {
		t.Fatalf("Failed to create controller: %v", err)
	}

	// 将请求加入 informer store
	err = informerFactory.Resource().V1alpha3().ResourcePoolStatusRequests().Informer().GetStore().Add(expiredRequest)
	if err != nil {
		t.Fatalf("Failed to add expired request to informer: %v", err)
	}
	err = informerFactory.Resource().V1alpha3().ResourcePoolStatusRequests().Informer().GetStore().Add(activeRequest)
	if err != nil {
		t.Fatalf("Failed to add active request to informer: %v", err)
	}

	// 执行清理
	controller.cleanupExpiredRequests(ctx)

	// 断言删除动作只针对过期请求；未过期请求必须保留，否则会误删活跃状态。
	actions := fakeClient.Actions()
	var deletedExpired, deletedActive bool
	for _, action := range actions {
		if action.GetVerb() == "delete" {
			deleteAction, ok := action.(interface{ GetName() string })
			if ok {
				if deleteAction.GetName() == "expired-request" {
					deletedExpired = true
				}
				if deleteAction.GetName() == "active-request" {
					deletedActive = true
				}
			}
		}
	}

	if !deletedExpired {
		t.Error("Expected expired-request to be deleted")
	}
	if deletedActive {
		t.Error("Did not expect active-request to be deleted")
	}
}

// makeSliceWithoutNode 构造未设置 NodeName 的 slice。
func makeSliceWithoutNode(name, driver, pool string, deviceCount int) *resourcev1.ResourceSlice {
	devices := make([]resourcev1.Device, deviceCount)
	for i := range deviceCount {
		devices[i] = resourcev1.Device{
			Name: fmt.Sprintf("device-%d", i),
		}
	}

	return &resourcev1.ResourceSlice{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
		Spec: resourcev1.ResourceSliceSpec{
			Driver: driver,
			Pool: resourcev1.ResourcePool{
				Name:               pool,
				Generation:         1,
				ResourceSliceCount: 1,
			},
			Devices: devices,
		},
	}
}

// new 是创建任意值指针的泛型辅助函数。
func new[T any](v T) *T {
	return &v
}

// makePartitionCounterSlice 构造可分区池中只含计数器的 slice：
// 它发布共享计数器集合并声明池的分区属性。
func makePartitionCounterSlice(name, driver, pool, node string, sliceCount int64) *resourcev1.ResourceSlice {
	s := makeSliceWithGenerationAndCount(name, driver, pool, node, 0, 1, sliceCount)
	s.Spec.Devices = nil
	s.Spec.SharedCounters = []resourcev1.CounterSet{counterSet("gpu-0", map[string]string{"memory": "80Gi"})}
	s.Spec.PartitionTypeAttribute = ptr.To(resourcev1.FullyQualifiedName(driver + "/profile"))
	return s
}

// makePartitionDeviceSlice 构造可分区池中承载设备的 slice：
// 包含一个 Full（80Gi）和两个 Half（40Gi）分区，
// 均引用同一计数器集合。
func makePartitionDeviceSlice(name, driver, pool, node string, sliceCount int64) *resourcev1.ResourceSlice {
	s := makeSliceWithGenerationAndCount(name, driver, pool, node, 0, 1, sliceCount)
	s.Spec.PartitionTypeAttribute = ptr.To(resourcev1.FullyQualifiedName(driver + "/profile"))
	s.Spec.Devices = []resourcev1.Device{
		partitionSliceDevice("full-0", "Full", "80Gi"),
		partitionSliceDevice("half-0", "Half", "40Gi"),
		partitionSliceDevice("half-1", "Half", "40Gi"),
	}
	return s
}

// partitionSliceDevice 构造带分区属性与计数器消耗的设备。
func partitionSliceDevice(name, profile, cost string) resourcev1.Device {
	return resourcev1.Device{
		Name: name,
		Attributes: map[resourcev1.QualifiedName]resourcev1.DeviceAttribute{
			"profile": {StringValue: ptr.To(profile)},
		},
		ConsumesCounters: []resourcev1.DeviceCounterConsumption{consumes("gpu-0", map[string]string{"memory": cost})},
	}
}

// makePartitionClassSlice 为同一池构造第二个设备 slice，按另一属性
// （driver/class）分组，并复用同一计数器集合；
// 用于模拟混合多种分区属性的池。
func makePartitionClassSlice(name, driver, pool, node string, sliceCount int64) *resourcev1.ResourceSlice {
	s := makeSliceWithGenerationAndCount(name, driver, pool, node, 0, 1, sliceCount)
	s.Spec.PartitionTypeAttribute = ptr.To(resourcev1.FullyQualifiedName(driver + "/class"))
	s.Spec.Devices = []resourcev1.Device{{
		Name: "a-0",
		Attributes: map[resourcev1.QualifiedName]resourcev1.DeviceAttribute{
			"class": {StringValue: ptr.To("A")},
		},
		ConsumesCounters: []resourcev1.DeviceCounterConsumption{consumes("gpu-0", map[string]string{"memory": "40Gi"})},
	}}
	return s
}

// 端到端验证双 slice 可分区池的属性解析，
// 覆盖 slice 聚合与池声明优先于请求默认值的规则。
func TestCalculatePoolStatus_PartitionSummary(t *testing.T) {
	driver := "gpu.example.com"

	profileAttr := driver + "/profile"
	classAttr := driver + "/class"
	// key 用属性与类型标识汇总项，
	// 使断言同时覆盖上报的 Attribute 和计数。
	key := func(attr, typ string) string { return attr + "\t" + typ }

	testCases := map[string]struct {
		// declarePoolAttr 为 true 时保留 slice 上驱动自身的属性。
		declarePoolAttr bool
		// requestAttr 请求侧指定的默认分区属性。
		requestAttr *string
		// mixedClass 增加一个按不同属性分组的设备 slice。
		mixedClass bool
		// want 期望的 {属性, 类型} 到 {总数, 可分配数} 的映射。
		want map[string][2]int32
	}{
		// 池自身声明的分区属性应直接用于汇总。
		"pool declares the attribute": {
			declarePoolAttr: true,
			want:            map[string][2]int32{key(profileAttr, "Full"): {1, 1}, key(profileAttr, "Half"): {2, 2}},
		},
		// slice 声明的是 driver/profile；请求指定了不同的属性，
		// 但没有任何设备携带该属性：若请求优先，所有设备都会
		// 显示为无类型，因此池自身的声明必须优先。
		"pool declaration beats the request default": {
			declarePoolAttr: true,
			requestAttr:     ptr.To(driver + "/other"),
			want:            map[string][2]int32{key(profileAttr, "Full"): {1, 1}, key(profileAttr, "Half"): {2, 2}},
		},
		// 池没有声明时，才回退到请求指定的默认属性。
		"request default applies when the pool declares none": {
			requestAttr: ptr.To(profileAttr),
			want:        map[string][2]int32{key(profileAttr, "Full"): {1, 1}, key(profileAttr, "Half"): {2, 2}},
		},
		// 声明不同属性的 slice 并不冲突：每组
		// 都在各自属性下独立上报。
		"pool mixes two grouping attributes": {
			declarePoolAttr: true,
			mixedClass:      true,
			want: map[string][2]int32{
				key(profileAttr, "Full"): {1, 1},
				key(profileAttr, "Half"): {2, 2},
				key(classAttr, "A"):      {1, 1},
			},
		},
	}

	// 逐用例校验分区汇总，确认属性优先级不影响计数。
	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			sliceCount := int64(2)
			if tc.mixedClass {
				sliceCount = 3
			}
			counters := makePartitionCounterSlice("counters", driver, "pool-0", "node-0", sliceCount)
			devices := makePartitionDeviceSlice("devices", driver, "pool-0", "node-0", sliceCount)
			all := []*resourcev1.ResourceSlice{counters, devices}
			if tc.mixedClass {
				all = append(all, makePartitionClassSlice("class-devices", driver, "pool-0", "node-0", sliceCount))
			}
			if !tc.declarePoolAttr {
				counters.Spec.PartitionTypeAttribute = nil
				devices.Spec.PartitionTypeAttribute = nil
			}
			request := makeRequest(driver)
			request.Spec.DefaultPartitionTypeAttribute = tc.requestAttr

			pool := requireSinglePool(t, runCalculatePoolStatus(t, request, all, nil))
			if pool.ValidationError != nil {
				t.Fatalf("unexpected validationError: %s", *pool.ValidationError)
			}
			got := map[string][2]int32{}
			for _, p := range pool.PartitionSummary {
				got[key(p.Attribute, p.Type)] = [2]int32{ptr.Deref(p.Total, 0), ptr.Deref(p.Allocatable, 0)}
			}
			if !maps.Equal(got, tc.want) {
				t.Errorf("partitionSummary {total,allocatable} = %v, want %v", got, tc.want)
			}
		})
	}
}

// 端到端验证可共享设备聚合 claim 中的已消耗容量，
// 并按名称独立上报每个容量键。
func TestCalculatePoolStatus_ShareableSummary(t *testing.T) {
	driver := "gpu.example.com"

	testCases := map[string]struct {
		// capacity 设备上声明的容量。
		capacity map[resourcev1.QualifiedName]resourcev1.DeviceCapacity
		// consumed 分配结果中声明的已消耗容量。
		consumed map[resourcev1.QualifiedName]resource.Quantity
		// wantFully 期望的完全可用设备数。
		wantFully int32
		// wantPartially 期望的部分可用设备数。
		wantPartially int32
		// want 按容量名称映射 {总量, 已消耗, 可用}。
		want map[string][3]string
	}{
		// 单一容量：total - consumed = available。
		"single capacity": {
			capacity:      map[resourcev1.QualifiedName]resourcev1.DeviceCapacity{"memory": {Value: qty("40Gi")}},
			consumed:      map[resourcev1.QualifiedName]resource.Quantity{"memory": qty("10Gi")},
			wantPartially: 1,
			want:          map[string][3]string{"memory": {"40Gi", "10Gi", "30Gi"}},
		},
		// 多个容量键应各自独立汇总，互不影响。
		"multiple capacities": {
			capacity: map[resourcev1.QualifiedName]resourcev1.DeviceCapacity{
				"memory": {Value: qty("40Gi")},
				"cores":  {Value: qty("8")},
			},
			consumed: map[resourcev1.QualifiedName]resource.Quantity{
				"memory": qty("10Gi"),
				"cores":  qty("2"),
			},
			wantPartially: 1,
			want: map[string][3]string{
				"memory": {"40Gi", "10Gi", "30Gi"},
				"cores":  {"8", "2", "6"},
			},
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			// 构造允许重复分配的设备，使 claim 消耗容量而非独占设备。
			slice := makeSlice("shareable", driver, "pool-0", "node-0", 1)
			slice.Spec.Devices[0].AllowMultipleAllocations = ptr.To(true)
			slice.Spec.Devices[0].Capacity = tc.capacity
			claim := makeAllocatedClaim("c0", "ns", driver, "pool-0", "device-0")
			claim.Status.Allocation.Devices.Results[0].ConsumedCapacity = tc.consumed

			pool := requireSinglePool(t, runCalculatePoolStatus(t, makeRequest(driver), []*resourcev1.ResourceSlice{slice}, []*resourcev1.ResourceClaim{claim}))
			if pool.ShareableSummary == nil {
				t.Fatal("expected a shareableSummary")
			}
			sh := pool.ShareableSummary
			if gotFully, gotPartially := ptr.Deref(sh.FullyAvailableDevices, 0), ptr.Deref(sh.PartiallyAvailableDevices, 0); gotFully != tc.wantFully || gotPartially != tc.wantPartially {
				t.Errorf("full/partial devices = %d/%d, want %d/%d", gotFully, gotPartially, tc.wantFully, tc.wantPartially)
			}
			got := map[string][3]string{}
			for _, c := range sh.Capacity {
				got[c.Name] = [3]string{c.Total.String(), c.Consumed.String(), c.Available.String()}
			}
			if !maps.Equal(got, tc.want) {
				t.Errorf("capacity {total,consumed,available} = %v, want %v", got, tc.want)
			}
		})
	}
}
