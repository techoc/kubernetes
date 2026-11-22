/*
Copyright 2019 The Kubernetes Authors.

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

package nodeports

import (
	"context"
	"strconv"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"

	v1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2/ktesting"
	_ "k8s.io/klog/v2/ktesting/init"
	fwk "k8s.io/kube-scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/feature"
	st "k8s.io/kubernetes/pkg/scheduler/testing"
)

// newPod 用 "协议/IP/端口" 形式的描述串构造带 hostPort 的测试 Pod。
func newPod(host string, hostPortInfos ...string) *v1.Pod {
	networkPorts := []v1.ContainerPort{}
	for _, portInfo := range hostPortInfos {
		splited := strings.Split(portInfo, "/")
		hostPort, _ := strconv.Atoi(splited[2])

		networkPorts = append(networkPorts, v1.ContainerPort{
			HostIP:   splited[1],
			HostPort: int32(hostPort),
			Protocol: v1.Protocol(splited[0]),
		})
	}
	return st.MakePod().Node(host).ContainerPort(networkPorts).Obj()
}

// newNodeInfo 构造一个 NodeInfo，所含 Pod 携带指定的 hostPort。
func newNodeInfo(hostPortInfos ...string) fwk.NodeInfo {
	return framework.NewNodeInfo(newPod("p1", hostPortInfos...))
}

// TestNodePorts 覆盖 NodePorts 插件的 PreFilter 与 Filter 行为：
// 无 hostPort 时跳过；同协议同 IP（含 0.0.0.0 通配）同端口冲突；
// 非 sidecar initContainer 的 hostPort 忽略，sidecar 的仍参与冲突检测。
func TestNodePorts(t *testing.T) {
	tests := []struct {
		pod                 *v1.Pod
		nodeInfo            fwk.NodeInfo
		name                string
		wantPreFilterStatus *fwk.Status
		wantFilterStatus    *fwk.Status
	}{
		{
			pod:                 &v1.Pod{},
			nodeInfo:            framework.NewNodeInfo(),
			name:                "skip filter",
			wantPreFilterStatus: fwk.NewStatus(fwk.Skip),
		},
		{
			pod: newPod("m1", "UDP/127.0.0.1/8080"),
			nodeInfo: framework.NewNodeInfo(
				newPod("m1", "UDP/127.0.0.1/9090")),
			name: "other port",
		},
		{
			pod: newPod("m1", "UDP/127.0.0.1/8080"),
			nodeInfo: framework.NewNodeInfo(
				newPod("m1", "UDP/127.0.0.1/8080")),
			name:             "same udp port",
			wantFilterStatus: fwk.NewStatus(fwk.Unschedulable, ErrReason),
		},
		{
			pod: newPod("m1", "TCP/127.0.0.1/8080"),
			nodeInfo: framework.NewNodeInfo(
				newPod("m1", "TCP/127.0.0.1/8080")),
			name:             "same tcp port",
			wantFilterStatus: fwk.NewStatus(fwk.Unschedulable, ErrReason),
		},
		{
			pod: newPod("m1", "TCP/127.0.0.1/8080"),
			nodeInfo: framework.NewNodeInfo(
				newPod("m1", "TCP/127.0.0.2/8080")),
			name: "different host ip",
		},
		{
			pod: newPod("m1", "UDP/127.0.0.1/8080"),
			nodeInfo: framework.NewNodeInfo(
				newPod("m1", "TCP/127.0.0.1/8080")),
			name: "different protocol",
		},
		{
			pod: newPod("m1", "UDP/127.0.0.1/8000", "UDP/127.0.0.1/8080"),
			nodeInfo: framework.NewNodeInfo(
				newPod("m1", "UDP/127.0.0.1/8080")),
			name:             "second udp port conflict",
			wantFilterStatus: fwk.NewStatus(fwk.Unschedulable, ErrReason),
		},
		{
			pod: newPod("m1", "TCP/127.0.0.1/8001", "UDP/127.0.0.1/8080"),
			nodeInfo: framework.NewNodeInfo(
				newPod("m1", "TCP/127.0.0.1/8001", "UDP/127.0.0.1/8081")),
			name:             "first tcp port conflict",
			wantFilterStatus: fwk.NewStatus(fwk.Unschedulable, ErrReason),
		},
		{
			pod: newPod("m1", "TCP/0.0.0.0/8001"),
			nodeInfo: framework.NewNodeInfo(
				newPod("m1", "TCP/127.0.0.1/8001")),
			name:             "first tcp port conflict due to 0.0.0.0 hostIP",
			wantFilterStatus: fwk.NewStatus(fwk.Unschedulable, ErrReason),
		},
		{
			pod: newPod("m1", "TCP/10.0.10.10/8001", "TCP/0.0.0.0/8001"),
			nodeInfo: framework.NewNodeInfo(
				newPod("m1", "TCP/127.0.0.1/8001")),
			name:             "TCP hostPort conflict due to 0.0.0.0 hostIP",
			wantFilterStatus: fwk.NewStatus(fwk.Unschedulable, ErrReason),
		},
		{
			pod: newPod("m1", "TCP/127.0.0.1/8001"),
			nodeInfo: framework.NewNodeInfo(
				newPod("m1", "TCP/0.0.0.0/8001")),
			name:             "second tcp port conflict to 0.0.0.0 hostIP",
			wantFilterStatus: fwk.NewStatus(fwk.Unschedulable, ErrReason),
		},
		{
			pod: newPod("m1", "UDP/127.0.0.1/8001"),
			nodeInfo: framework.NewNodeInfo(
				newPod("m1", "TCP/0.0.0.0/8001")),
			name: "second different protocol",
		},
		{
			pod: newPod("m1", "UDP/127.0.0.1/8001"),
			nodeInfo: framework.NewNodeInfo(
				newPod("m1", "TCP/0.0.0.0/8001", "UDP/0.0.0.0/8001")),
			name:             "UDP hostPort conflict due to 0.0.0.0 hostIP",
			wantFilterStatus: fwk.NewStatus(fwk.Unschedulable, ErrReason),
		},
		{
			pod: st.MakePod().
				InitContainerPort(false /* sidecar */, []v1.ContainerPort{
					{
						ContainerPort: 8001,
						HostPort:      8001,
						Protocol:      v1.ProtocolTCP,
					},
				}).Obj(),
			nodeInfo: framework.NewNodeInfo(
				newPod("m1", "TCP/0.0.0.0/8001")),
			name:                "non-sidecar initContainer using hostPort",
			wantPreFilterStatus: fwk.NewStatus(fwk.Skip),
		},
		{
			pod: st.MakePod().
				InitContainerPort(true /* sidecar */, []v1.ContainerPort{
					{
						ContainerPort: 8001,
						HostPort:      8001,
						Protocol:      v1.ProtocolTCP,
					},
				}).Obj(),
			nodeInfo: framework.NewNodeInfo(
				newPod("m1", "TCP/0.0.0.0/8001")),
			name:             "TCP hostPort conflict from sidecar initContainer",
			wantFilterStatus: fwk.NewStatus(fwk.Unschedulable, ErrReason),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, ctx := ktesting.NewTestContext(t)
			p, err := New(ctx, nil, nil, feature.Features{})
			if err != nil {
				t.Fatalf("creating plugin: %v", err)
			}
			cycleState := framework.NewCycleState()
			_, preFilterStatus := p.(fwk.PreFilterPlugin).PreFilter(ctx, cycleState, test.pod, nil)
			if diff := cmp.Diff(test.wantPreFilterStatus, preFilterStatus); diff != "" {
				t.Errorf("preFilter status does not match (-want,+got): %s", diff)
			}
			if preFilterStatus.IsSkip() {
				return
			}
			if !preFilterStatus.IsSuccess() {
				t.Errorf("prefilter failed with status: %v", preFilterStatus)
			}
			gotStatus := p.(fwk.FilterPlugin).Filter(ctx, cycleState, test.pod, test.nodeInfo)
			if diff := cmp.Diff(test.wantFilterStatus, gotStatus); diff != "" {
				t.Errorf("filter status does not match (-want, +got): %s", diff)
			}
		})
	}
}

// TestPreFilterDisabled 验证未经过 PreFilter 直接调用 Filter 时返回 ErrNotFound：
// 端口信息由 PreFilter 写入 cycleState，缺失则无法判断。
func TestPreFilterDisabled(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)
	pod := &v1.Pod{}
	nodeInfo := framework.NewNodeInfo()
	node := v1.Node{}
	nodeInfo.SetNode(&node)
	p, err := New(ctx, nil, nil, feature.Features{})
	if err != nil {
		t.Fatalf("creating plugin: %v", err)
	}
	cycleState := framework.NewCycleState()
	gotStatus := p.(fwk.FilterPlugin).Filter(ctx, cycleState, pod, nodeInfo)
	wantStatus := fwk.AsStatus(fwk.ErrNotFound)
	if diff := cmp.Diff(wantStatus, gotStatus); diff != "" {
		t.Errorf("status does not match (-want,+got):\n%s", diff)
	}
}

// Test_isSchedulableAfterAssignedPodDeleted 验证已分配 Pod 删除事件带来的队列提示：
// 仅当被删 Pod 确实占用了本 Pod 需要的 hostPort 时才提示 Queue。
func Test_isSchedulableAfterAssignedPodDeleted(t *testing.T) {
	podWithHostPort := st.MakePod().HostPort(8080)

	testcases := map[string]struct {
		pod          *v1.Pod
		oldObj       interface{}
		expectedHint fwk.QueueingHint
		expectedErr  bool
	}{
		"backoff-wrong-old-object": {
			pod:          podWithHostPort.Obj(),
			oldObj:       "not-a-pod",
			expectedHint: fwk.Queue,
			expectedErr:  true,
		},
		"skip-queue-on-unscheduled": {
			pod:          podWithHostPort.Obj(),
			oldObj:       st.MakePod().Obj(),
			expectedHint: fwk.QueueSkip,
		},
		"skip-queue-on-non-hostport": {
			pod:          podWithHostPort.Obj(),
			oldObj:       st.MakePod().Node("fake-node").Obj(),
			expectedHint: fwk.QueueSkip,
		},
		"skip-queue-on-unrelated-hostport": {
			pod:          podWithHostPort.Obj(),
			oldObj:       st.MakePod().Node("fake-node").HostPort(8081).Obj(),
			expectedHint: fwk.QueueSkip,
		},
		"queue-on-released-hostport": {
			pod:          podWithHostPort.Obj(),
			oldObj:       st.MakePod().Node("fake-node").HostPort(8080).Obj(),
			expectedHint: fwk.Queue,
		},
	}

	for name, tc := range testcases {
		t.Run(name, func(t *testing.T) {
			logger, ctx := ktesting.NewTestContext(t)
			p, err := New(ctx, nil, nil, feature.Features{})
			if err != nil {
				t.Fatalf("creating plugin: %v", err)
			}
			actualHint, err := p.(*NodePorts).isSchedulableAfterAssignedPodDeleted(logger, tc.pod, tc.oldObj, nil)
			if tc.expectedErr {
				if err == nil {
					t.Fatalf("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if diff := cmp.Diff(tc.expectedHint, actualHint); diff != "" {
				t.Errorf("unexpected hint (-want, +got):\n%s", diff)
			}
		})
	}
}

// TestFits 与 k8s.io/kube-scheduler/framework/types_test.go 中的
// TestHostPortInfo_Check 类似，但只覆盖较少的用例。
// 后续可考虑用假的 HostPortInfo 直接验证 Fits 的逻辑，而非 CheckConflict 的逻辑。
func TestFits(t *testing.T) {
	tests := []struct {
		desc     string
		pod      *v1.Pod
		existing fwk.NodeInfo
		expect   bool
	}{
		{
			desc:     "non-conflicting ports",
			pod:      newPod("p", "TCP/127.0.0.1/80"),
			existing: newNodeInfo("TCP/127.0.0.1/9090"),
			expect:   true,
		},
		{
			desc: "multiple non-conflicting ports",
			pod: newPod("p",
				"TCP/127.0.0.1/80",
				"TCP/127.0.1.1/80",
				"TCP/127.0.1.1/90",
				"TCP/127.0.1.1/100"),
			existing: newNodeInfo(
				"TCP/127.0.0.1/9090",
				"TCP/127.0.0.1/9191",
				"TCP/127.0.1.1/8080"),
			expect: true,
		},
		{
			desc:     "same ports on different protocols",
			pod:      newPod("m1", "TCP/127.0.0.1/80"),
			existing: newNodeInfo("UDP/127.0.0.1/80"),
			expect:   true,
		},
		{
			desc:     "conflicting ports",
			pod:      newPod("m1", "TCP/127.0.0.1/80"),
			existing: newNodeInfo("TCP/127.0.0.1/80"),
			expect:   false,
		},
		{
			desc: "multiple ports, some conflicting",
			pod: newPod("p",
				"TCP/127.0.0.1/80",
				"TCP/127.0.1.1/80",
				"TCP/127.0.1.1/90",
				"TCP/127.0.1.1/100"),
			existing: newNodeInfo(
				"TCP/127.0.0.1/9090",
				"TCP/127.0.1.1/90",
				"TCP/127.0.1.1/8080"),
			expect: false,
		},
	}
	for _, test := range tests {
		t.Run(test.desc, func(t *testing.T) {
			if Fits(test.pod, test.existing) != test.expect {
				t.Errorf("expected %t; got %t", test.expect, !test.expect)
			}
		})
	}
}

// TestNodePorts_DeferredResizeSkipped 验证处于延迟扩容（Deferred resize）状态的 Pod
// 跳过 PreFilter 且 Filter 直接成功：扩容调度抢占路径不应被端口检查拦住。
func TestNodePorts_DeferredResizeSkipped(t *testing.T) {
	ctx := context.Background()
	pod := st.MakePod().Name("p").UID("p").Condition(v1.PodResizePending, v1.ConditionTrue, v1.PodReasonDeferred).Obj()
	nodeInfo := framework.NewNodeInfo()
	nodeInfo.SetNode(st.MakeNode().Name("node1").Obj())

	pl := &NodePorts{enableInPlacePodVerticalScalingSchedulerPreemption: true}

	if preRes, preStatus := pl.PreFilter(ctx, nil, pod, nil); preStatus.Code() != fwk.Skip || preRes != nil {
		t.Errorf("PreFilter: got (res: %v, status: %v), want (nil, Skip)", preRes, preStatus.Code())
	}

	if filterStatus := pl.Filter(ctx, nil, pod, nodeInfo); filterStatus.Code() != fwk.Success {
		t.Errorf("Filter: got status %v, want Success (nil)", filterStatus.Code())
	}
}
