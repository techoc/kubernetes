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

package proxy

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	clientsetfake "k8s.io/client-go/kubernetes/fake"
	"k8s.io/kubernetes/test/utils/ktesting"
	netutils "k8s.io/utils/net"
	"k8s.io/utils/ptr"
)

const (
	testNodeName = "test-node"
)

// nodeTweak 定义 Node 对象的修改函数，便于测试用例按需拼装节点。
type nodeTweak func(n *v1.Node)

// makeNode 创建一个测试用 Node，并依次应用所有 tweaks。
func makeNode(tweaks ...nodeTweak) *v1.Node {
	n := &v1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: testNodeName,
		},
	}
	for _, tw := range tweaks {
		tw(n)
	}
	return n
}

// tweakNodeIPs 返回一个向 Node 状态追加 InternalIP 地址的修改函数。
func tweakNodeIPs(nodeIPs ...string) nodeTweak {
	return func(n *v1.Node) {
		for _, ip := range nodeIPs {
			n.Status.Addresses = append(n.Status.Addresses, v1.NodeAddress{Type: v1.NodeInternalIP, Address: ip})
		}
	}
}

// tweakPodCIDRs 返回一个向 Node 规格追加 PodCIDR 的修改函数。
func tweakPodCIDRs(podCIDRs ...string) nodeTweak {
	return func(n *v1.Node) {
		n.Spec.PodCIDRs = append(n.Spec.PodCIDRs, podCIDRs...)
	}
}

// tweakResourceVersion 返回一个设置 Node ResourceVersion 的修改函数。
func tweakResourceVersion(resourceVersion string) nodeTweak {
	return func(n *v1.Node) {
		n.ResourceVersion = resourceVersion
	}
}

// TestNewNodeManager 验证 Node 与 PodCIDR 不同就绪状态下 newNodeManager 的等待、超时与错误处理。
func TestNewNodeManager(t *testing.T) {
	testCases := []struct {
		name             string
		watchPodCIDRs    bool
		nodeUpdates      []func(context.Context, clientset.Interface)
		expectedNodeIPs  []net.IP
		expectedPodCIDRs []string
		expectedError    string
	}{
		{
			name: "node object doesn't exist",
			// 等待条件始终未满足：超时后忽略错误。
			expectedNodeIPs: nil,
		},
		{
			name:          "node object doesn't exist, with watchPodCIDRs",
			watchPodCIDRs: true,
			// 断言 watchPodCIDRs 时 newNodeManager 返回的错误。
			expectedError: "timeout waiting for node \"test-node\" to exist",
		},
		{
			name: "node object exist without NodeIP",
			nodeUpdates: []func(ctx context.Context, client clientset.Interface){
				// 按时间顺序模拟一次外部事件，逐步推进节点状态。
				func(ctx context.Context, client clientset.Interface) {
					// 初始时节点对象尚不存在。
				},

				// 按时间顺序模拟一次外部事件，逐步推进节点状态。
				func(ctx context.Context, client clientset.Interface) {
					// 此时节点已创建但还没有 NodeIP 或 PodCIDR。
					_, _ = client.CoreV1().Nodes().Create(ctx, makeNode(), metav1.CreateOptions{})
				},
			},
			// 等待条件始终未满足：超时后忽略错误。
			expectedNodeIPs: nil,
		},
		{
			name: "node object exist with NodeIP",
			nodeUpdates: []func(ctx context.Context, client clientset.Interface){
				// 按时间顺序模拟一次外部事件，逐步推进节点状态。
				func(ctx context.Context, client clientset.Interface) {
					// 初始时节点对象尚不存在。
				},

				// 按时间顺序模拟一次外部事件，逐步推进节点状态。
				func(ctx context.Context, client clientset.Interface) {
					// 此时节点已创建但还没有 NodeIP 或 PodCIDR。
					_, _ = client.CoreV1().Nodes().Create(ctx, makeNode(), metav1.CreateOptions{})
				},

				// 按时间顺序模拟一次外部事件，逐步推进节点状态。
				func(ctx context.Context, client clientset.Interface) {
					// 节点更新后带上 NodeIP。
					_, _ = client.CoreV1().Nodes().Update(ctx, makeNode(
						tweakNodeIPs("192.168.1.10"),
					), metav1.UpdateOptions{})
				},
			},
			expectedNodeIPs: []net.IP{netutils.ParseIPSloppy("192.168.1.10")},
		},
		{
			name:          "watchPodCIDRs and node object exist without PodCIDRs",
			watchPodCIDRs: true,
			nodeUpdates: []func(ctx context.Context, client clientset.Interface){
				// 按时间顺序模拟一次外部事件，逐步推进节点状态。
				func(ctx context.Context, client clientset.Interface) {
					// 初始时节点对象尚不存在。
				},

				// 按时间顺序模拟一次外部事件，逐步推进节点状态。
				func(ctx context.Context, client clientset.Interface) {
					// 此时节点已创建但还没有 NodeIP 或 PodCIDR。
					_, _ = client.CoreV1().Nodes().Create(ctx, makeNode(), metav1.CreateOptions{})
				},
				// 按时间顺序模拟一次外部事件，逐步推进节点状态。
				func(ctx context.Context, client clientset.Interface) {
					// 节点更新后带上 NodeIP。
					_, _ = client.CoreV1().Nodes().Update(ctx, makeNode(
						tweakNodeIPs("192.168.1.10"),
					), metav1.UpdateOptions{})
				},
			},
			// 断言 watchPodCIDRs 时 newNodeManager 返回的错误。
			expectedError: "timeout waiting for PodCIDR allocation on node \"test-node\"",
		},
		{
			name:          "watchPodCIDRs and node object exist with NodeIP and PodCIDR",
			watchPodCIDRs: true,
			nodeUpdates: []func(ctx context.Context, client clientset.Interface){
				// 按时间顺序模拟一次外部事件，逐步推进节点状态。
				func(ctx context.Context, client clientset.Interface) {
					// 初始时节点对象尚不存在。
				},

				// 按时间顺序模拟一次外部事件，逐步推进节点状态。
				func(ctx context.Context, client clientset.Interface) {
					// 此时节点已创建但还没有 NodeIP 或 PodCIDR。
					_, _ = client.CoreV1().Nodes().Create(ctx, makeNode(), metav1.CreateOptions{})
				},

				// 按时间顺序模拟一次外部事件，逐步推进节点状态。
				func(ctx context.Context, client clientset.Interface) {
					// 节点更新后带上 NodeIP。
					_, _ = client.CoreV1().Nodes().Update(ctx, makeNode(
						tweakNodeIPs("192.168.1.10"),
					), metav1.UpdateOptions{})
				},
				// 按时间顺序模拟一次外部事件，逐步推进节点状态。
				func(ctx context.Context, client clientset.Interface) {
					// 节点更新后分配了 PodCIDR。
					_, _ = client.CoreV1().Nodes().Update(ctx, makeNode(
						tweakNodeIPs("192.168.1.1"),
						tweakPodCIDRs("10.0.0.0/24"),
					), metav1.UpdateOptions{})
				},
			},
			expectedNodeIPs:  []net.IP{netutils.ParseIPSloppy("192.168.1.1")},
			expectedPodCIDRs: []string{"10.0.0.0/24"},
		},
		{
			name:          "watchPodCIDRs and node object exist without NodeIP and with PodCIDR",
			watchPodCIDRs: true,
			nodeUpdates: []func(ctx context.Context, client clientset.Interface){
				// 按时间顺序模拟一次外部事件，逐步推进节点状态。
				func(ctx context.Context, client clientset.Interface) {
					// 初始时节点对象尚不存在。
				},

				// 按时间顺序模拟一次外部事件，逐步推进节点状态。
				func(ctx context.Context, client clientset.Interface) {
					// 此时节点已创建但还没有 NodeIP 或 PodCIDR。
					_, _ = client.CoreV1().Nodes().Create(ctx, makeNode(), metav1.CreateOptions{})
				},
				// 按时间顺序模拟一次外部事件，逐步推进节点状态。
				func(ctx context.Context, client clientset.Interface) {
					// 节点更新后分配了 PodCIDR。
					_, _ = client.CoreV1().Nodes().Update(ctx, makeNode(
						tweakPodCIDRs("10.0.0.0/24"),
					), metav1.UpdateOptions{})
				},
			},
			// 没有 NodeIP 会超时，因此这里忽略错误，只检查已观察到的 PodCIDR。
			expectedNodeIPs:  nil,
			expectedPodCIDRs: []string{"10.0.0.0/24"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, ctx := ktesting.NewTestContext(t)
			client := clientsetfake.NewClientset()

			// 在 goroutine 中按序触发节点更新，每次间隔 15ms，
			// 保证 manager 的 10ms 轮询能观察到每一次变化。
			go func() {
				// 先等 manager 完成初始化并开始监听，否则早期更新可能被错过。
				time.Sleep(100 * time.Millisecond)

				for _, update := range tc.nodeUpdates {
					update(ctx, client)
					// 留足 15ms，确保 manager 的 10ms 轮询能观察到本次更新。
					time.Sleep(15 * time.Millisecond)
				}
			}()
			// 初始化 manager 时用 10ms 轮询间隔和 1s 超时，使测试快速结束。
			nodeManager, err := newNodeManager(ctx, client, time.Second, testNodeName, tc.watchPodCIDRs, func(i int) {}, 10*time.Millisecond, time.Second, time.Second)
			if len(tc.expectedError) > 0 {
				require.Nil(t, nodeManager)
				require.ErrorContains(t, err, tc.expectedError)
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.expectedNodeIPs, nodeManager.NodeIPs())
				require.Equal(t, tc.expectedPodCIDRs, nodeManager.PodCIDRs())
			}
		})
	}
}

// TestNodeManagerOnNodeChange 验证 Node 的 IP 或 PodCIDR 变化是否按预期触发退出。
func TestNodeManagerOnNodeChange(t *testing.T) {
	tests := []struct {
		name             string
		initialNodeIPs   []string
		initialPodCIDRs  []string
		updatedNodeIPs   []string
		updatedPodCIDRs  []string
		watchPodCIDRs    bool
		expectedExitCode *int
	}{
		{
			name:             "node updated with same NodeIPs",
			initialNodeIPs:   []string{"192.168.1.1", "fd00:1:2:3::1"},
			updatedNodeIPs:   []string{"192.168.1.1", "fd00:1:2:3::1"},
			expectedExitCode: nil,
		},
		{
			name:             "node updated with different NodeIPs",
			initialNodeIPs:   []string{"192.168.1.1", "fd00:1:2:3::1"},
			updatedNodeIPs:   []string{"10.0.1.1", "fd00:3:2:1::2"},
			expectedExitCode: ptr.To(1),
		},
		{
			name:             "watchPodCIDR and node updated with same PodCIDRs",
			initialNodeIPs:   []string{"192.168.1.1", "fd00:1:2:3::1"},
			initialPodCIDRs:  []string{"10.0.0.0/8", "fd01:2345::/64"},
			updatedNodeIPs:   []string{"192.168.1.1", "fd00:1:2:3::1"},
			updatedPodCIDRs:  []string{"10.0.0.0/8", "fd01:2345::/64"},
			watchPodCIDRs:    true,
			expectedExitCode: nil,
		},
		{
			name:             "watchPodCIDR and node updated with different PodCIDRs",
			initialNodeIPs:   []string{"192.168.1.1", "fd00:1:2:3::1"},
			initialPodCIDRs:  []string{"10.0.0.0/8", "fd01:2345::/64"},
			updatedNodeIPs:   []string{"192.168.1.1", "fd00:1:2:3::1"},
			updatedPodCIDRs:  []string{"172.16.10.0/24", "fd01:5422::/64"},
			watchPodCIDRs:    true,
			expectedExitCode: ptr.To(1),
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, ctx := ktesting.NewTestContext(t)
			var exitCode *int
			exitFunc := func(code int) {
				exitCode = &code
			}

			client := clientsetfake.NewClientset()
			_, err := client.CoreV1().Nodes().Create(ctx, makeNode(
				tweakNodeIPs(tc.initialNodeIPs...),
				tweakPodCIDRs(tc.initialPodCIDRs...),
			), metav1.CreateOptions{})
			require.NoError(t, err)

			nodeManager, err := newNodeManager(ctx, client, 30*time.Second, testNodeName, tc.watchPodCIDRs, exitFunc, 10*time.Millisecond, time.Second, time.Second)
			require.NoError(t, err)

			nodeManager.OnNodeChange(makeNode(tweakNodeIPs(tc.updatedNodeIPs...), tweakPodCIDRs(tc.updatedPodCIDRs...)))
			require.Equal(t, tc.expectedExitCode, exitCode)
		})
	}
}

// TestNodeManagerOnNodeDelete 验证节点被删除时会调用退出函数。
func TestNodeManagerOnNodeDelete(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)
	var exitCode *int
	exitFunc := func(code int) {
		exitCode = &code
	}
	client := clientsetfake.NewClientset()
	_, _ = client.CoreV1().Nodes().Create(ctx, makeNode(tweakNodeIPs("192.168.1.1")), metav1.CreateOptions{})
	nodeManager, err := newNodeManager(ctx, client, 30*time.Second, testNodeName, false, exitFunc, 10*time.Millisecond, time.Second, time.Second)
	require.NoError(t, err)

	nodeManager.OnNodeDelete(makeNode())
	require.Equal(t, ptr.To(1), exitCode)
}

// TestNodeManagerNode 验证 Node 访问器返回的是最新版本的节点对象。
func TestNodeManagerNode(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)

	client := clientsetfake.NewClientset()
	_, _ = client.CoreV1().Nodes().Create(ctx, makeNode(
		tweakNodeIPs("192.168.1.1"),
		tweakResourceVersion("1")),
		metav1.CreateOptions{})

	nodeManager, err := newNodeManager(ctx, client, 30*time.Second, testNodeName, false, func(i int) {}, time.Nanosecond, time.Nanosecond, time.Nanosecond)
	require.NoError(t, err)
	require.Equal(t, "1", nodeManager.Node().ResourceVersion)

	nodeManager.OnNodeChange(makeNode(tweakResourceVersion("2")))
	require.NoError(t, err)
	require.Equal(t, "2", nodeManager.Node().ResourceVersion)
}
