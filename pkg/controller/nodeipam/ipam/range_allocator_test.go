/*
Copyright 2016 The Kubernetes Authors.

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

package ipam

import (
	"net"
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/kubernetes/pkg/controller/nodeipam/ipam/test"
	"k8s.io/kubernetes/pkg/controller/testutil"
	"k8s.io/kubernetes/test/utils/ktesting"
	netutils "k8s.io/utils/net"
)

// testCase 描述 CIDR 分配器的一个测试用例。
type testCase struct {
	// description 为用例名称，同时用作子测试名，便于定位失败场景。
	description string
	// fakeNodeHandler 提供模拟的节点存储，并承载分配器的节点更新结果。
	fakeNodeHandler *testutil.FakeNodeHandler
	// allocatorParams 为创建分配器所需的集群 CIDR、Service CIDR 及节点掩码配置。
	allocatorParams CIDRAllocatorParams
	// expectedAllocatedCIDR 的键是 CIDR 集合的索引，值是该集合期望分配到的 CIDR。
	// 键是已分配 CIDR 集合的索引。
	expectedAllocatedCIDR map[int]string
	// allocatedCIDRs 按 CIDR 集合索引记录需要预先占用的 CIDR，模拟已被其他节点使用的地址。
	allocatedCIDRs map[int][]string
	// ctrlCreateFail 表示创建分配器是否应当失败，用于校验非法配置。
	// 控制器创建是否应当失败。
	ctrlCreateFail bool
}

// TestOccupyPreExistingCIDR 验证分配器初始化时能正确识别节点上已存在的 PodCIDR。
// 合法配置应创建成功，非法配置（如节点 CIDR 不在集群范围内）应创建失败。
func TestOccupyPreExistingCIDR(t *testing.T) {
	// 所有用例都只操作单个节点，便于集中校验占用逻辑。
	testCases := []testCase{
		// 成功场景：单栈集群，节点尚未分配 CIDR，分配器应正常创建。
		{
			description: "success, single stack no node allocation",
			fakeNodeHandler: &testutil.FakeNodeHandler{
				Existing: []*v1.Node{
					{
						ObjectMeta: metav1.ObjectMeta{
							Name: "node0",
						},
					},
				},
				Clientset: fake.NewSimpleClientset(),
			},
			allocatorParams: CIDRAllocatorParams{
				ClusterCIDRs: func() []*net.IPNet {
					_, clusterCIDRv4, _ := netutils.ParseCIDRSloppy("10.10.0.0/16")
					return []*net.IPNet{clusterCIDRv4}
				}(),
				ServiceCIDR:          nil,
				SecondaryServiceCIDR: nil,
				NodeCIDRMaskSizes:    []int{24},
			},
			allocatedCIDRs:        nil,
			expectedAllocatedCIDR: nil,
			ctrlCreateFail:        false,
		},
		// 成功场景：双栈集群，节点尚未分配 CIDR，分配器应正常创建。
		{
			description: "success, dual stack no node allocation",
			fakeNodeHandler: &testutil.FakeNodeHandler{
				Existing: []*v1.Node{
					{
						ObjectMeta: metav1.ObjectMeta{
							Name: "node0",
						},
					},
				},
				Clientset: fake.NewSimpleClientset(),
			},
			allocatorParams: CIDRAllocatorParams{
				ClusterCIDRs: func() []*net.IPNet {
					_, clusterCIDRv4, _ := netutils.ParseCIDRSloppy("10.10.0.0/16")
					_, clusterCIDRv6, _ := netutils.ParseCIDRSloppy("ace:cab:deca::/8")
					return []*net.IPNet{clusterCIDRv4, clusterCIDRv6}
				}(),
				ServiceCIDR:          nil,
				SecondaryServiceCIDR: nil,
				NodeCIDRMaskSizes:    []int{24, 24},
			},
			allocatedCIDRs:        nil,
			expectedAllocatedCIDR: nil,
			ctrlCreateFail:        false,
		},
		// 成功场景：单栈集群，节点已分配且位于集群范围内的 CIDR，应被正确占用。
		{
			description: "success, single stack correct node allocation",
			fakeNodeHandler: &testutil.FakeNodeHandler{
				Existing: []*v1.Node{
					{
						ObjectMeta: metav1.ObjectMeta{
							Name: "node0",
						},
						Spec: v1.NodeSpec{
							PodCIDRs: []string{"10.10.0.1/24"},
						},
					},
				},
				Clientset: fake.NewSimpleClientset(),
			},
			allocatorParams: CIDRAllocatorParams{
				ClusterCIDRs: func() []*net.IPNet {
					_, clusterCIDRv4, _ := netutils.ParseCIDRSloppy("10.10.0.0/16")
					return []*net.IPNet{clusterCIDRv4}
				}(),
				ServiceCIDR:          nil,
				SecondaryServiceCIDR: nil,
				NodeCIDRMaskSizes:    []int{24},
			},
			allocatedCIDRs:        nil,
			expectedAllocatedCIDR: nil,
			ctrlCreateFail:        false,
		},
		// 成功场景：双栈集群，节点的 IPv4 与 IPv6 CIDR 均合法，应被同时占用。
		{
			description: "success, dual stack both allocated correctly",
			fakeNodeHandler: &testutil.FakeNodeHandler{
				Existing: []*v1.Node{
					{
						ObjectMeta: metav1.ObjectMeta{
							Name: "node0",
						},
						Spec: v1.NodeSpec{
							PodCIDRs: []string{"10.10.0.1/24", "a00::/86"},
						},
					},
				},
				Clientset: fake.NewSimpleClientset(),
			},
			allocatorParams: CIDRAllocatorParams{
				ClusterCIDRs: func() []*net.IPNet {
					_, clusterCIDRv4, _ := netutils.ParseCIDRSloppy("10.10.0.0/16")
					_, clusterCIDRv6, _ := netutils.ParseCIDRSloppy("ace:cab:deca::/8")
					return []*net.IPNet{clusterCIDRv4, clusterCIDRv6}
				}(),
				ServiceCIDR:          nil,
				SecondaryServiceCIDR: nil,
				NodeCIDRMaskSizes:    []int{24, 24},
			},
			allocatedCIDRs:        nil,
			expectedAllocatedCIDR: nil,
			ctrlCreateFail:        false,
		},
		// 以下是预期创建失败的用例。
		// 失败场景：单栈集群，节点 CIDR 不在集群范围内，分配器创建应失败。
		{
			description: "fail, single stack incorrect node allocation",
			fakeNodeHandler: &testutil.FakeNodeHandler{
				Existing: []*v1.Node{
					{
						ObjectMeta: metav1.ObjectMeta{
							Name: "node0",
						},
						Spec: v1.NodeSpec{
							PodCIDRs: []string{"172.10.0.1/24"},
						},
					},
				},
				Clientset: fake.NewSimpleClientset(),
			},
			allocatorParams: CIDRAllocatorParams{
				ClusterCIDRs: func() []*net.IPNet {
					_, clusterCIDRv4, _ := netutils.ParseCIDRSloppy("10.10.0.0/16")
					return []*net.IPNet{clusterCIDRv4}
				}(),
				ServiceCIDR:          nil,
				SecondaryServiceCIDR: nil,
				NodeCIDRMaskSizes:    []int{24},
			},
			allocatedCIDRs:        nil,
			expectedAllocatedCIDR: nil,
			ctrlCreateFail:        true,
		},
		// 失败场景：双栈节点使用了集群中不存在的 IPv6 CIDR，但只配置单栈集群，创建应失败。
		{
			description: "fail, dualstack node allocating from non existing cidr",

			fakeNodeHandler: &testutil.FakeNodeHandler{
				Existing: []*v1.Node{
					{
						ObjectMeta: metav1.ObjectMeta{
							Name: "node0",
						},
						Spec: v1.NodeSpec{
							PodCIDRs: []string{"10.10.0.1/24", "a00::/86"},
						},
					},
				},
				Clientset: fake.NewSimpleClientset(),
			},
			allocatorParams: CIDRAllocatorParams{
				ClusterCIDRs: func() []*net.IPNet {
					_, clusterCIDRv4, _ := netutils.ParseCIDRSloppy("10.10.0.0/16")
					return []*net.IPNet{clusterCIDRv4}
				}(),
				ServiceCIDR:          nil,
				SecondaryServiceCIDR: nil,
				NodeCIDRMaskSizes:    []int{24},
			},
			allocatedCIDRs:        nil,
			expectedAllocatedCIDR: nil,
			ctrlCreateFail:        true,
		},
		// 失败场景：双栈节点的 IPv4 CIDR 不在集群范围内，分配器创建应失败。
		{
			description: "fail, dualstack node allocating bad v4",

			fakeNodeHandler: &testutil.FakeNodeHandler{
				Existing: []*v1.Node{
					{
						ObjectMeta: metav1.ObjectMeta{
							Name: "node0",
						},
						Spec: v1.NodeSpec{
							PodCIDRs: []string{"172.10.0.1/24", "a00::/86"},
						},
					},
				},
				Clientset: fake.NewSimpleClientset(),
			},
			allocatorParams: CIDRAllocatorParams{
				ClusterCIDRs: func() []*net.IPNet {
					_, clusterCIDRv4, _ := netutils.ParseCIDRSloppy("10.10.0.0/16")
					_, clusterCIDRv6, _ := netutils.ParseCIDRSloppy("ace:cab:deca::/8")
					return []*net.IPNet{clusterCIDRv4, clusterCIDRv6}
				}(),
				ServiceCIDR:          nil,
				SecondaryServiceCIDR: nil,
				NodeCIDRMaskSizes:    []int{24, 24},
			},
			allocatedCIDRs:        nil,
			expectedAllocatedCIDR: nil,
			ctrlCreateFail:        true,
		},
		// 失败场景：双栈节点的 IPv6 CIDR 不在集群范围内，分配器创建应失败。
		{
			description: "fail, dualstack node allocating bad v6",

			fakeNodeHandler: &testutil.FakeNodeHandler{
				Existing: []*v1.Node{
					{
						ObjectMeta: metav1.ObjectMeta{
							Name: "node0",
						},
						Spec: v1.NodeSpec{
							PodCIDRs: []string{"10.10.0.1/24", "cdd::/86"},
						},
					},
				},
				Clientset: fake.NewSimpleClientset(),
			},
			allocatorParams: CIDRAllocatorParams{
				ClusterCIDRs: func() []*net.IPNet {
					_, clusterCIDRv4, _ := netutils.ParseCIDRSloppy("10.10.0.0/16")
					_, clusterCIDRv6, _ := netutils.ParseCIDRSloppy("ace:cab:deca::/8")
					return []*net.IPNet{clusterCIDRv4, clusterCIDRv6}
				}(),
				ServiceCIDR:          nil,
				SecondaryServiceCIDR: nil,
				NodeCIDRMaskSizes:    []int{24, 24},
			},
			allocatedCIDRs:        nil,
			expectedAllocatedCIDR: nil,
			ctrlCreateFail:        true,
		},
	}

	// 统一执行表驱动用例：只校验分配器能否按预期创建。
	tCtx := ktesting.Init(t)
	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			// 用模拟节点处理器创建分配器，触发对已有 CIDR 的占用检查。
			fakeNodeInformer := test.FakeNodeInformer(tc.fakeNodeHandler)
			nodeList, _ := tc.fakeNodeHandler.List(tCtx, metav1.ListOptions{})
			_, err := NewCIDRRangeAllocator(tCtx, tc.fakeNodeHandler, fakeNodeInformer, tc.allocatorParams, nodeList)
			// 预期失败却成功，说明非法的已有 CIDR 未被识别。
			if err == nil && tc.ctrlCreateFail {
				t.Fatalf("creating range allocator was expected to fail, but it did not")
			}
			// 预期成功却失败，说明合法的已有 CIDR 被误判为非法。
			if err != nil && !tc.ctrlCreateFail {
				t.Fatalf("creating range allocator was expected to succeed, but it did not")
			}
		})
	}
}

// TestAllocateOrOccupyCIDRSuccess 验证分配器能为未分配 CIDR 的节点成功分配地址，
// 并正确跳过 Service CIDR、已占用 CIDR，以及处理双栈场景。
func TestAllocateOrOccupyCIDRSuccess(t *testing.T) {
	// 该测试会修改全局变量 nodePollInterval，不能并行执行；结束后恢复原值。
	oldNodePollInterval := nodePollInterval
	nodePollInterval = test.NodePollInterval
	defer func() {
		nodePollInterval = oldNodePollInterval
	}()

	// 所有用例中只有一个节点尚未分配 CIDR，便于断言更新次数。
	testCases := []testCase{
		// 无 Service CIDR 时，应从集群 CIDR 段的第一个可用子网开始分配。
		{
			description: "When there's no ServiceCIDR return first CIDR in range",
			fakeNodeHandler: &testutil.FakeNodeHandler{
				Existing: []*v1.Node{
					{
						ObjectMeta: metav1.ObjectMeta{
							Name: "node0",
						},
					},
				},
				Clientset: fake.NewSimpleClientset(),
			},
			allocatorParams: CIDRAllocatorParams{
				ClusterCIDRs: func() []*net.IPNet {
					_, clusterCIDR, _ := netutils.ParseCIDRSloppy("127.123.234.0/24")
					return []*net.IPNet{clusterCIDR}
				}(),
				ServiceCIDR:          nil,
				SecondaryServiceCIDR: nil,
				NodeCIDRMaskSizes:    []int{30},
			},
			expectedAllocatedCIDR: map[int]string{
				0: "127.123.234.0/30",
			},
		},
		// 配置 Service CIDR 后，应跳过其占用的地址段，从其后第一个可用子网分配。
		{
			description: "Correctly filter out ServiceCIDR",
			fakeNodeHandler: &testutil.FakeNodeHandler{
				Existing: []*v1.Node{
					{
						ObjectMeta: metav1.ObjectMeta{
							Name: "node0",
						},
					},
				},
				Clientset: fake.NewSimpleClientset(),
			},
			allocatorParams: CIDRAllocatorParams{
				ClusterCIDRs: func() []*net.IPNet {
					_, clusterCIDR, _ := netutils.ParseCIDRSloppy("127.123.234.0/24")
					return []*net.IPNet{clusterCIDR}
				}(),
				ServiceCIDR: func() *net.IPNet {
					_, serviceCIDR, _ := netutils.ParseCIDRSloppy("127.123.234.0/26")
					return serviceCIDR
				}(),
				SecondaryServiceCIDR: nil,
				NodeCIDRMaskSizes:    []int{30},
			},
			// 应从 Service CIDR 地址段之后的第一个 /30 子网开始分配。
			expectedAllocatedCIDR: map[int]string{
				0: "127.123.234.64/30",
			},
		},
		// 已占用 CIDR 不再重复分配，即使地址段中间存在空洞也应先填补空洞。
		{
			description: "Correctly ignore already allocated CIDRs",
			fakeNodeHandler: &testutil.FakeNodeHandler{
				Existing: []*v1.Node{
					{
						ObjectMeta: metav1.ObjectMeta{
							Name: "node0",
						},
					},
				},
				Clientset: fake.NewSimpleClientset(),
			},
			allocatorParams: CIDRAllocatorParams{
				ClusterCIDRs: func() []*net.IPNet {
					_, clusterCIDR, _ := netutils.ParseCIDRSloppy("127.123.234.0/24")
					return []*net.IPNet{clusterCIDR}
				}(),
				ServiceCIDR: func() *net.IPNet {
					_, serviceCIDR, _ := netutils.ParseCIDRSloppy("127.123.234.0/26")
					return serviceCIDR
				}(),
				SecondaryServiceCIDR: nil,
				NodeCIDRMaskSizes:    []int{30},
			},
			allocatedCIDRs: map[int][]string{
				0: {"127.123.234.64/30", "127.123.234.68/30", "127.123.234.72/30", "127.123.234.80/30"},
			},
			expectedAllocatedCIDR: map[int]string{
				0: "127.123.234.76/30",
			},
		},
		// 双栈场景：集群 CIDR 按 IPv4 在前、IPv6 在后的顺序配置，应分别分配对应地址族的子网。
		{
			description: "Dualstack CIDRs v4,v6",
			fakeNodeHandler: &testutil.FakeNodeHandler{
				Existing: []*v1.Node{
					{
						ObjectMeta: metav1.ObjectMeta{
							Name: "node0",
						},
					},
				},
				Clientset: fake.NewSimpleClientset(),
			},
			allocatorParams: CIDRAllocatorParams{
				ClusterCIDRs: func() []*net.IPNet {
					_, clusterCIDRv4, _ := netutils.ParseCIDRSloppy("127.123.234.0/8")
					_, clusterCIDRv6, _ := netutils.ParseCIDRSloppy("ace:cab:deca::/84")
					return []*net.IPNet{clusterCIDRv4, clusterCIDRv6}
				}(),
				ServiceCIDR: func() *net.IPNet {
					_, serviceCIDR, _ := netutils.ParseCIDRSloppy("127.123.234.0/26")
					return serviceCIDR
				}(),
				SecondaryServiceCIDR: nil,
				NodeCIDRMaskSizes:    []int{24, 98},
			},
		},
		// 双栈场景：集群 CIDR 按 IPv6 在前、IPv4 在后的顺序配置，应分别分配对应地址族的子网。
		{
			description: "Dualstack CIDRs v6,v4",
			fakeNodeHandler: &testutil.FakeNodeHandler{
				Existing: []*v1.Node{
					{
						ObjectMeta: metav1.ObjectMeta{
							Name: "node0",
						},
					},
				},
				Clientset: fake.NewSimpleClientset(),
			},
			allocatorParams: CIDRAllocatorParams{
				ClusterCIDRs: func() []*net.IPNet {
					_, clusterCIDRv4, _ := netutils.ParseCIDRSloppy("127.123.234.0/8")
					_, clusterCIDRv6, _ := netutils.ParseCIDRSloppy("ace:cab:deca::/84")
					return []*net.IPNet{clusterCIDRv6, clusterCIDRv4}
				}(),
				ServiceCIDR: func() *net.IPNet {
					_, serviceCIDR, _ := netutils.ParseCIDRSloppy("127.123.234.0/26")
					return serviceCIDR
				}(),
				SecondaryServiceCIDR: nil,
				NodeCIDRMaskSizes:    []int{98, 24},
			},
		},
		// 配置了超过两个集群 CIDR 时，应只使用每个地址族的第一个 CIDR 进行分配。
		{
			description: "Dualstack CIDRs, more than two",
			fakeNodeHandler: &testutil.FakeNodeHandler{
				Existing: []*v1.Node{
					{
						ObjectMeta: metav1.ObjectMeta{
							Name: "node0",
						},
					},
				},
				Clientset: fake.NewSimpleClientset(),
			},
			allocatorParams: CIDRAllocatorParams{
				ClusterCIDRs: func() []*net.IPNet {
					_, clusterCIDRv4, _ := netutils.ParseCIDRSloppy("127.123.234.0/8")
					_, clusterCIDRv6, _ := netutils.ParseCIDRSloppy("ace:cab:deca::/84")
					_, clusterCIDRv4_2, _ := netutils.ParseCIDRSloppy("10.0.0.0/8")
					return []*net.IPNet{clusterCIDRv4, clusterCIDRv6, clusterCIDRv4_2}
				}(),
				ServiceCIDR: func() *net.IPNet {
					_, serviceCIDR, _ := netutils.ParseCIDRSloppy("127.123.234.0/26")
					return serviceCIDR
				}(),
				SecondaryServiceCIDR: nil,
				NodeCIDRMaskSizes:    []int{24, 98, 24},
			},
		},
		// 已分配 CIDR 的多个节点不应导致重复计数，空洞子网仍应被优先分配。
		{
			description: "no double counting",
			fakeNodeHandler: &testutil.FakeNodeHandler{
				Existing: []*v1.Node{
					{
						ObjectMeta: metav1.ObjectMeta{
							Name: "node0",
						},
						Spec: v1.NodeSpec{
							PodCIDRs: []string{"10.10.0.0/24"},
						},
					},
					{
						ObjectMeta: metav1.ObjectMeta{
							Name: "node1",
						},
						Spec: v1.NodeSpec{
							PodCIDRs: []string{"10.10.2.0/24"},
						},
					},
					{
						ObjectMeta: metav1.ObjectMeta{
							Name: "node2",
						},
					},
				},
				Clientset: fake.NewSimpleClientset(),
			},
			allocatorParams: CIDRAllocatorParams{
				ClusterCIDRs: func() []*net.IPNet {
					_, clusterCIDR, _ := netutils.ParseCIDRSloppy("10.10.0.0/22")
					return []*net.IPNet{clusterCIDR}
				}(),
				ServiceCIDR:          nil,
				SecondaryServiceCIDR: nil,
				NodeCIDRMaskSizes:    []int{24},
			},
			expectedAllocatedCIDR: map[int]string{
				0: "10.10.1.0/24",
			},
		},
	}

	// 单个用例的执行函数：创建分配器、预占 CIDR、触发分配并校验结果。
	_, tCtx := ktesting.NewTestContext(t)
	testFunc := func(tc testCase) {
		// 每个用例使用独立的 informer 与节点列表，避免相互干扰。
		fakeNodeInformer := test.FakeNodeInformer(tc.fakeNodeHandler)
		nodeList, _ := tc.fakeNodeHandler.List(tCtx, metav1.ListOptions{})
		// 用模拟节点处理器初始化分配器。
		allocator, err := NewCIDRRangeAllocator(tCtx, tc.fakeNodeHandler, fakeNodeInformer, tc.allocatorParams, nodeList)
		if err != nil {
			t.Errorf("%v: failed to create CIDRRangeAllocator with error %v", tc.description, err)
			return
		}
		rangeAllocator, ok := allocator.(*rangeAllocator)
		if !ok {
			t.Logf("%v: found non-default implementation of CIDRAllocator, skipping white-box test...", tc.description)
			return
		}
		rangeAllocator.nodesSynced = test.AlwaysReady
		rangeAllocator.recorder = testutil.NewFakeRecorder()
		go allocator.Run(tCtx)

		// 白盒测试：直接占用指定的 CIDR，模拟这些地址已被集群中其他节点使用。
		for idx, allocatedList := range tc.allocatedCIDRs {
			for _, allocated := range allocatedList {
				_, cidr, err := netutils.ParseCIDRSloppy(allocated)
				if err != nil {
					t.Fatalf("%v: unexpected error when parsing CIDR %v: %v", tc.description, allocated, err)
				}
				if err = rangeAllocator.cidrSets[idx].Occupy(cidr); err != nil {
					t.Fatalf("%v: unexpected error when occupying CIDR %v: %v", tc.description, allocated, err)
				}
			}
		}

		// 统计需要分配 CIDR 的节点数，作为期望的更新次数。
		updateCount := 0
		for _, node := range tc.fakeNodeHandler.Existing {
			if node.Spec.PodCIDRs == nil {
				updateCount++
			}
			if err := allocator.AllocateOrOccupyCIDR(tCtx, node); err != nil {
				t.Errorf("%v: unexpected error in AllocateOrOccupyCIDR: %v", tc.description, err)
			}
		}
		// 所有用例都设计为只更新一个节点，否则说明用例自身设置有误。
		if updateCount != 1 {
			t.Fatalf("test error: all tests must update exactly one node")
		}
		// 分配是异步写回节点的，需等待更新完成再校验。
		if err := test.WaitForUpdatedNodeWithTimeout(tc.fakeNodeHandler, updateCount, wait.ForeverTestTimeout); err != nil {
			t.Fatalf("%v: timeout while waiting for Node update: %v", tc.description, err)
		}

		if len(tc.expectedAllocatedCIDR) == 0 {
			// 未声明期望值，说明本用例只关注“分配成功”。
			return
		}
		for _, updatedNode := range tc.fakeNodeHandler.GetUpdatedNodesCopy() {
			if len(updatedNode.Spec.PodCIDRs) == 0 {
				// 尚未分配到 CIDR 的节点跳过。
				continue
			}
			// 逐地址族比对期望的 PodCIDR。
			for podCIDRIdx, expectedPodCIDR := range tc.expectedAllocatedCIDR {
				if updatedNode.Spec.PodCIDRs[podCIDRIdx] != expectedPodCIDR {
					t.Errorf("%v: Unable to find allocated CIDR %v, found updated Nodes with CIDRs: %v", tc.description, expectedPodCIDR, updatedNode.Spec.PodCIDRs)
					break
				}
			}
		}
	}

	// 逐个运行表驱动用例（不使用子测试，因为全局变量已被修改）。
	for _, tc := range testCases {
		testFunc(tc)
	}
}

// TestAllocateOrOccupyCIDRFailure 验证集群 CIDR 范围内没有可用子网时，分配应失败且不更新节点。
func TestAllocateOrOccupyCIDRFailure(t *testing.T) {
	testCases := []testCase{
		// 集群 CIDR 的全部 /30 子网均被预占，后续分配应失败。
		{
			description: "When there's no ServiceCIDR return first CIDR in range",
			fakeNodeHandler: &testutil.FakeNodeHandler{
				Existing: []*v1.Node{
					{
						ObjectMeta: metav1.ObjectMeta{
							Name: "node0",
						},
					},
				},
				Clientset: fake.NewSimpleClientset(),
			},
			allocatorParams: CIDRAllocatorParams{
				ClusterCIDRs: func() []*net.IPNet {
					_, clusterCIDR, _ := netutils.ParseCIDRSloppy("127.123.234.0/28")
					return []*net.IPNet{clusterCIDR}
				}(),
				ServiceCIDR:          nil,
				SecondaryServiceCIDR: nil,
				NodeCIDRMaskSizes:    []int{30},
			},
			allocatedCIDRs: map[int][]string{
				0: {"127.123.234.0/30", "127.123.234.4/30", "127.123.234.8/30", "127.123.234.12/30"},
			},
		},
	}
	_, tCtx := ktesting.NewTestContext(t)
	testFunc := func(tc testCase) {
		// 创建分配器；这里未传入节点列表，后续通过白盒方式预占 CIDR。
		allocator, err := NewCIDRRangeAllocator(tCtx, tc.fakeNodeHandler, test.FakeNodeInformer(tc.fakeNodeHandler), tc.allocatorParams, nil)
		if err != nil {
			t.Logf("%v: failed to create CIDRRangeAllocator with error %v", tc.description, err)
		}
		rangeAllocator, ok := allocator.(*rangeAllocator)
		if !ok {
			t.Logf("%v: found non-default implementation of CIDRAllocator, skipping white-box test...", tc.description)
			return
		}
		rangeAllocator.nodesSynced = test.AlwaysReady
		rangeAllocator.recorder = testutil.NewFakeRecorder()
		go allocator.Run(tCtx)

		// 白盒测试：预占集群 CIDR 内的全部子网，制造无可用地址的局面。
		for setIdx, allocatedList := range tc.allocatedCIDRs {
			for _, allocated := range allocatedList {
				_, cidr, err := netutils.ParseCIDRSloppy(allocated)
				if err != nil {
					t.Fatalf("%v: unexpected error when parsing CIDR %v: %v", tc.description, cidr, err)
				}
				err = rangeAllocator.cidrSets[setIdx].Occupy(cidr)
				if err != nil {
					t.Fatalf("%v: unexpected error when occupying CIDR %v: %v", tc.description, cidr, err)
				}
			}
		}
		// 地址耗尽时必须返回错误。
		if err := allocator.AllocateOrOccupyCIDR(tCtx, tc.fakeNodeHandler.Existing[0]); err == nil {
			t.Errorf("%v: unexpected success in AllocateOrOccupyCIDR: %v", tc.description, err)
		}
		// 不应有节点更新；用短暂等待确认没有异步写回发生。
		time.Sleep(time.Second)
		if len(tc.fakeNodeHandler.GetUpdatedNodesCopy()) != 0 {
			t.Fatalf("%v: unexpected update of nodes: %v", tc.description, tc.fakeNodeHandler.GetUpdatedNodesCopy())
		}
		if len(tc.expectedAllocatedCIDR) == 0 {
			// 未声明期望值，说明本用例只关注“分配失败且无更新”。
			return
		}
		for _, updatedNode := range tc.fakeNodeHandler.GetUpdatedNodesCopy() {
			if len(updatedNode.Spec.PodCIDRs) == 0 {
				// 尚未分配到 CIDR 的节点跳过。
				continue
			}
			// 确认这些 CIDR 没有被错误地分配给节点。
			for podCIDRIdx, expectedPodCIDR := range tc.expectedAllocatedCIDR {
				if updatedNode.Spec.PodCIDRs[podCIDRIdx] == expectedPodCIDR {
					t.Errorf("%v: found cidr %v that should not be allocated on node with CIDRs:%v", tc.description, expectedPodCIDR, updatedNode.Spec.PodCIDRs)
					break
				}
			}
		}
	}
	for _, tc := range testCases {
		testFunc(tc)
	}
}

// releaseTestCase 描述 CIDR 释放与回收的一个测试用例。
type releaseTestCase struct {
	// description 为用例名称，同时用作失败信息中的标识。
	description string
	// fakeNodeHandler 提供模拟节点存储，并接收分配器的更新结果。
	fakeNodeHandler *testutil.FakeNodeHandler
	// allocatorParams 为创建分配器所需的配置。
	allocatorParams CIDRAllocatorParams
	// expectedAllocatedCIDRFirstRound 为释放前第一轮分配期望得到的 CIDR。
	expectedAllocatedCIDRFirstRound map[int]string
	// expectedAllocatedCIDRSecondRound 为释放后第二轮分配期望得到的 CIDR。
	expectedAllocatedCIDRSecondRound map[int]string
	// allocatedCIDRs 按 CIDR 集合索引记录需要预先占用的 CIDR。
	allocatedCIDRs map[int][]string
	// cidrsToRelease 为需要模拟释放的 PodCIDR 列表。
	cidrsToRelease [][]string
}

// TestReleaseCIDRSuccess 验证 CIDR 释放后能被重新分配：
// 既覆盖“释放后原本不可用的地址变为可用”，也覆盖“释放的地址被回收复用”。
func TestReleaseCIDRSuccess(t *testing.T) {
	// 该测试会修改全局变量 nodePollInterval，不能并行执行；结束后恢复原值。
	oldNodePollInterval := nodePollInterval
	nodePollInterval = test.NodePollInterval
	defer func() {
		nodePollInterval = oldNodePollInterval
	}()

	testCases := []releaseTestCase{
		// 释放预先占用的 CIDR 后，该地址应变为可用并在下一轮被分配。
		{
			description: "Correctly release preallocated CIDR",
			fakeNodeHandler: &testutil.FakeNodeHandler{
				Existing: []*v1.Node{
					{
						ObjectMeta: metav1.ObjectMeta{
							Name: "node0",
						},
					},
				},
				Clientset: fake.NewSimpleClientset(),
			},
			allocatorParams: CIDRAllocatorParams{
				ClusterCIDRs: func() []*net.IPNet {
					_, clusterCIDR, _ := netutils.ParseCIDRSloppy("127.123.234.0/28")
					return []*net.IPNet{clusterCIDR}
				}(),
				ServiceCIDR:          nil,
				SecondaryServiceCIDR: nil,
				NodeCIDRMaskSizes:    []int{30},
			},
			allocatedCIDRs: map[int][]string{
				0: {"127.123.234.0/30", "127.123.234.4/30", "127.123.234.8/30", "127.123.234.12/30"},
			},
			expectedAllocatedCIDRFirstRound: nil,
			cidrsToRelease: [][]string{
				{"127.123.234.4/30"},
			},
			expectedAllocatedCIDRSecondRound: map[int]string{
				0: "127.123.234.4/30",
			},
		},
		// 节点释放已分配的 CIDR 后，该地址应被回收并在下一轮重新分配。
		{
			description: "Correctly recycle CIDR",
			fakeNodeHandler: &testutil.FakeNodeHandler{
				Existing: []*v1.Node{
					{
						ObjectMeta: metav1.ObjectMeta{
							Name: "node0",
						},
					},
				},
				Clientset: fake.NewSimpleClientset(),
			},
			allocatorParams: CIDRAllocatorParams{
				ClusterCIDRs: func() []*net.IPNet {
					_, clusterCIDR, _ := netutils.ParseCIDRSloppy("127.123.234.0/28")
					return []*net.IPNet{clusterCIDR}
				}(),
				ServiceCIDR:          nil,
				SecondaryServiceCIDR: nil,
				NodeCIDRMaskSizes:    []int{30},
			},
			allocatedCIDRs: map[int][]string{
				0: {"127.123.234.4/30", "127.123.234.8/30", "127.123.234.12/30"},
			},
			expectedAllocatedCIDRFirstRound: map[int]string{
				0: "127.123.234.0/30",
			},
			cidrsToRelease: [][]string{
				{"127.123.234.0/30"},
			},
			expectedAllocatedCIDRSecondRound: map[int]string{
				0: "127.123.234.0/30",
			},
		},
	}
	logger, tCtx := ktesting.NewTestContext(t)
	testFunc := func(tc releaseTestCase) {
		// 创建分配器，随后通过白盒方式预占 CIDR。
		allocator, _ := NewCIDRRangeAllocator(tCtx, tc.fakeNodeHandler, test.FakeNodeInformer(tc.fakeNodeHandler), tc.allocatorParams, nil)
		rangeAllocator, ok := allocator.(*rangeAllocator)
		if !ok {
			t.Logf("%v: found non-default implementation of CIDRAllocator, skipping white-box test...", tc.description)
			return
		}
		rangeAllocator.nodesSynced = test.AlwaysReady
		rangeAllocator.recorder = testutil.NewFakeRecorder()
		go allocator.Run(tCtx)

		// 白盒测试：直接占用指定 CIDR，构造地址被预占的初始状态。
		for setIdx, allocatedList := range tc.allocatedCIDRs {
			for _, allocated := range allocatedList {
				_, cidr, err := netutils.ParseCIDRSloppy(allocated)
				if err != nil {
					t.Fatalf("%v: unexpected error when parsing CIDR %v: %v", tc.description, allocated, err)
				}
				err = rangeAllocator.cidrSets[setIdx].Occupy(cidr)
				if err != nil {
					t.Fatalf("%v: unexpected error when occupying CIDR %v: %v", tc.description, allocated, err)
				}
			}
		}

		// 第一轮分配：根据期望值判断应成功还是因地址耗尽而失败。
		err := allocator.AllocateOrOccupyCIDR(tCtx, tc.fakeNodeHandler.Existing[0])
		if len(tc.expectedAllocatedCIDRFirstRound) != 0 {
			if err != nil {
				t.Fatalf("%v: unexpected error in AllocateOrOccupyCIDR: %v", tc.description, err)
			}
			if err := test.WaitForUpdatedNodeWithTimeout(tc.fakeNodeHandler, 1, wait.ForeverTestTimeout); err != nil {
				t.Fatalf("%v: timeout while waiting for Node update: %v", tc.description, err)
			}
			// 第一轮期望失败，因为可用地址已被全部预占。
		} else {
			if err == nil {
				t.Fatalf("%v: unexpected success in AllocateOrOccupyCIDR: %v", tc.description, err)
			}
			// 失败不应触发节点更新，等待确认没有异步写回。
			time.Sleep(time.Second)
			if len(tc.fakeNodeHandler.GetUpdatedNodesCopy()) != 0 {
				t.Fatalf("%v: unexpected update of nodes: %v", tc.description, tc.fakeNodeHandler.GetUpdatedNodesCopy())
			}
		}
		// 模拟节点删除：构造只带待释放 CIDR 的节点对象并触发释放。
		for _, cidrToRelease := range tc.cidrsToRelease {
			nodeToRelease := v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name: "node0",
				},
			}
			nodeToRelease.Spec.PodCIDRs = cidrToRelease
			err = allocator.ReleaseCIDR(logger, &nodeToRelease)
			if err != nil {
				t.Fatalf("%v: unexpected error in ReleaseCIDR: %v", tc.description, err)
			}
		}
		if err = allocator.AllocateOrOccupyCIDR(tCtx, tc.fakeNodeHandler.Existing[0]); err != nil {
			t.Fatalf("%v: unexpected error in AllocateOrOccupyCIDR: %v", tc.description, err)
		}
		if err := test.WaitForUpdatedNodeWithTimeout(tc.fakeNodeHandler, 1, wait.ForeverTestTimeout); err != nil {
			t.Fatalf("%v: timeout while waiting for Node update: %v", tc.description, err)
		}

		if len(tc.expectedAllocatedCIDRSecondRound) == 0 {
			// 未声明期望值，说明本用例到释放成功即可结束。
			return
		}
		for _, updatedNode := range tc.fakeNodeHandler.GetUpdatedNodesCopy() {
			if len(updatedNode.Spec.PodCIDRs) == 0 {
				// 尚未分配到 CIDR 的节点跳过。
				continue
			}
			// 逐地址族比对第二轮分配的 CIDR。
			for podCIDRIdx, expectedPodCIDR := range tc.expectedAllocatedCIDRSecondRound {
				if updatedNode.Spec.PodCIDRs[podCIDRIdx] != expectedPodCIDR {
					t.Errorf("%v: found cidr %v that should not be allocated on node with CIDRs:%v", tc.description, expectedPodCIDR, updatedNode.Spec.PodCIDRs)
					break
				}
			}
		}
	}

	for _, tc := range testCases {
		testFunc(tc)
	}
}

// TestNodeDeletionReleaseCIDR 验证节点删除时的 CIDR 释放行为：
// 仅当节点已从存储中彻底消失时才释放其 CIDR，处于删除中或仍存在的节点不释放。
func TestNodeDeletionReleaseCIDR(t *testing.T) {
	// 解析集群 CIDR 与节点已分配的 CIDR，作为所有用例的公共输入。
	_, clusterCIDRv4, err := netutils.ParseCIDRSloppy("10.10.0.0/16")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	_, allocatedCIDR, err := netutils.ParseCIDRSloppy("10.10.0.0/24")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	testCases := []struct {
		description string
		// nodeKey 为需要执行节点同步的节点名。
		nodeKey string
		// existingNodes 为模拟存储中的节点集合。
		existingNodes []*v1.Node
		// shouldReleaseCIDR 表示该场景是否期望 CIDR 被释放。
		shouldReleaseCIDR bool
	}{
		// 普通存活节点：同步时不应释放其 CIDR。
		{
			description: "Regular node not under deletion",
			nodeKey:     "node0",
			existingNodes: []*v1.Node{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "node0",
					},
					Spec: v1.NodeSpec{
						PodCIDR:  allocatedCIDR.String(),
						PodCIDRs: []string{allocatedCIDR.String()}},
				},
			},
			shouldReleaseCIDR: false,
		},
		// 节点带有删除时间戳但尚未真正删除：同步时仍不应释放其 CIDR。
		{
			description: "Node under deletion",
			nodeKey:     "node0",
			existingNodes: []*v1.Node{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:              "node0",
						DeletionTimestamp: &metav1.Time{Time: time.Now()},
					},
					Spec: v1.NodeSpec{
						PodCIDR:  allocatedCIDR.String(),
						PodCIDRs: []string{allocatedCIDR.String()}},
				},
			},
			shouldReleaseCIDR: false,
		},
		// 节点已从存储中删除：同步时应释放其 CIDR，使其可被再次分配。
		{
			description:       "Node deleted",
			nodeKey:           "node0",
			existingNodes:     []*v1.Node{},
			shouldReleaseCIDR: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			allocatorParams := CIDRAllocatorParams{
				ClusterCIDRs: []*net.IPNet{clusterCIDRv4}, ServiceCIDR: nil,
				SecondaryServiceCIDR: nil,
				NodeCIDRMaskSizes:    []int{24},
			}

			// 每个子测试使用独立的模拟存储，避免节点状态相互污染。
			fakeNodeHandler := &testutil.FakeNodeHandler{
				Existing:  tc.existingNodes,
				Clientset: fake.NewSimpleClientset(),
			}
			_, tCtx := ktesting.NewTestContext(t)

			fakeNodeInformer := test.FakeNodeInformer(fakeNodeHandler)
			nodeList, err := fakeNodeHandler.List(tCtx, metav1.ListOptions{})
			if err != nil {
				t.Fatalf("Failed to get list of nodes %v", err)
			}
			allocator, err := NewCIDRRangeAllocator(tCtx, fakeNodeHandler, fakeNodeInformer, allocatorParams, nodeList)
			if err != nil {
				t.Fatalf("failed to create CIDRRangeAllocator: %v", err)
			}
			rangeAllocator, ok := allocator.(*rangeAllocator)
			if !ok {
				t.Fatalf("found non-default implementation of CIDRAllocator")
			}
			rangeAllocator.nodesSynced = test.AlwaysReady
			rangeAllocator.recorder = testutil.NewFakeRecorder()

			if err := rangeAllocator.syncNode(tCtx, tc.nodeKey); err != nil {
				t.Fatalf("failed to run rangeAllocator.syncNode")
			}

			if len(rangeAllocator.cidrSets) != 1 {
				t.Fatalf("Expected a single cidrSet, but got %d", len(rangeAllocator.cidrSets))
			}

			// 通过下一次分配的结果间接判断 CIDR 是否被释放：若已释放，应能重新分配到原来那个 CIDR；否则只能分配到下一个。
			nextCIDR, err := rangeAllocator.cidrSets[0].AllocateNext()
			if err != nil {
				t.Fatalf("unexpected error trying to allocate next CIDR: %v", err)
			}
			// 已被节点占用的是 10.0.0.0/24。
			expectedCIDR := "10.10.1.0/24"
			if tc.shouldReleaseCIDR {
				// CIDR 已释放，期望复用原地址（即 10.0.0.0/24）。
				expectedCIDR = allocatedCIDR.String()
			}
			if nextCIDR.String() != expectedCIDR {
				t.Fatalf("Expected CIDR %s to be allocated next, but got: %v", expectedCIDR, nextCIDR.String())
			}
		})
	}
}
