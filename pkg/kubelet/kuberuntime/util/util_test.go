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

package util

import (
	"testing"

	"github.com/stretchr/testify/require"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/version"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	featuregatetesting "k8s.io/component-base/featuregate/testing"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
	pkgfeatures "k8s.io/kubernetes/pkg/features"
	kubecontainer "k8s.io/kubernetes/pkg/kubelet/container"
	kubecontainertest "k8s.io/kubernetes/pkg/kubelet/container/testing"
	kubetypes "k8s.io/kubernetes/pkg/kubelet/types"
	"k8s.io/kubernetes/test/utils/ktesting"
)

// TestPodSandboxChanged 表驱动验证沙箱变更判定：无沙箱、多个就绪沙箱、
// 沙箱未就绪、网络命名空间模式不匹配、缺少 IP，以及一切正常的情况。
func TestPodSandboxChanged(t *testing.T) {
	for desc, test := range map[string]struct {
		pod               *v1.Pod
		status            *kubecontainer.PodStatus
		expectedChanged   bool
		expectedAttempt   uint32
		expectedSandboxID string
		expectedReason    string
	}{
		"Pod with no existing sandboxes": {
			pod:               &v1.Pod{},
			status:            &kubecontainer.PodStatus{},
			expectedChanged:   true,
			expectedAttempt:   0,
			expectedSandboxID: "",
			expectedReason:    kubetypes.PodSandboxNotReadyMsgNoPodSandbox,
		},
		"Pod with multiple ready sandbox statuses": {
			pod: &v1.Pod{},
			status: &kubecontainer.PodStatus{
				SandboxStatuses: []*runtimeapi.PodSandboxStatus{
					{
						Id:       "sandboxID2",
						Metadata: &runtimeapi.PodSandboxMetadata{Attempt: uint32(1)},
						State:    runtimeapi.PodSandboxState_SANDBOX_READY,
					},
					{
						Id:       "sandboxID1",
						Metadata: &runtimeapi.PodSandboxMetadata{Attempt: uint32(0)},
						State:    runtimeapi.PodSandboxState_SANDBOX_READY,
					},
				},
			},
			expectedChanged:   true,
			expectedAttempt:   2,
			expectedSandboxID: "sandboxID2",
			expectedReason:    kubetypes.PodSandboxNotReadyMsgMultipleSandboxes,
		},
		"Pod with no ready sandbox statuses": {
			pod: &v1.Pod{},
			status: &kubecontainer.PodStatus{
				SandboxStatuses: []*runtimeapi.PodSandboxStatus{
					{
						Id:       "sandboxID2",
						Metadata: &runtimeapi.PodSandboxMetadata{Attempt: uint32(1)},
						State:    runtimeapi.PodSandboxState_SANDBOX_NOTREADY,
					},
					{
						Id:       "sandboxID1",
						Metadata: &runtimeapi.PodSandboxMetadata{Attempt: uint32(0)},
						State:    runtimeapi.PodSandboxState_SANDBOX_NOTREADY,
					},
				},
			},
			expectedChanged:   true,
			expectedAttempt:   2,
			expectedSandboxID: "sandboxID2",
			expectedReason:    kubetypes.PodSandboxNotReadyMsgSandboxNotReady,
		},
		"Pod with ready sandbox status but network namespace mismatch": {
			pod: &v1.Pod{
				Spec: v1.PodSpec{
					HostNetwork: true,
				},
			},
			status: &kubecontainer.PodStatus{
				SandboxStatuses: []*runtimeapi.PodSandboxStatus{
					{
						Id: "sandboxID1",
						Linux: &runtimeapi.LinuxPodSandboxStatus{
							Namespaces: &runtimeapi.Namespace{
								Options: &runtimeapi.NamespaceOption{
									Network: runtimeapi.NamespaceMode_POD,
								},
							},
						},
						Metadata: &runtimeapi.PodSandboxMetadata{Attempt: uint32(0)},
						State:    runtimeapi.PodSandboxState_SANDBOX_READY,
					},
				},
			},
			expectedChanged:   true,
			expectedAttempt:   1,
			expectedSandboxID: "",
			expectedReason:    kubetypes.PodSandboxNotReadyMsgNetworkNamespaceMode,
		},
		"Pod with ready sandbox status but no IP": {
			pod: &v1.Pod{
				Spec: v1.PodSpec{
					HostNetwork: false,
				},
			},
			status: &kubecontainer.PodStatus{
				SandboxStatuses: []*runtimeapi.PodSandboxStatus{
					{
						Id: "sandboxID1",
						Network: &runtimeapi.PodSandboxNetworkStatus{
							Ip: "",
						},
						Metadata: &runtimeapi.PodSandboxMetadata{Attempt: uint32(0)},
						State:    runtimeapi.PodSandboxState_SANDBOX_READY,
					},
				},
			},
			expectedChanged:   true,
			expectedAttempt:   1,
			expectedSandboxID: "sandboxID1",
			expectedReason:    kubetypes.PodSandboxNotReadyMsgNoIPAddress,
		},
		"Pod with ready sandbox status with IP": {
			pod: &v1.Pod{
				Spec: v1.PodSpec{
					HostNetwork: false,
				},
			},
			status: &kubecontainer.PodStatus{
				SandboxStatuses: []*runtimeapi.PodSandboxStatus{
					{
						Id: "sandboxID1",
						Network: &runtimeapi.PodSandboxNetworkStatus{
							Ip: "10.0.0.10",
						},
						Metadata: &runtimeapi.PodSandboxMetadata{Attempt: uint32(0)},
						State:    runtimeapi.PodSandboxState_SANDBOX_READY,
					},
				},
			},
			expectedChanged:   false,
			expectedAttempt:   0,
			expectedSandboxID: "sandboxID1",
			expectedReason:    "",
		},
	} {
		t.Run(desc, func(t *testing.T) {
			// 返回值依次为：是否需要新建沙箱、下一次尝试序号、选中的沙箱 ID、未就绪原因。
			changed, attempt, id, reason := PodSandboxChanged(test.pod, test.status)
			require.Equal(t, test.expectedChanged, changed)
			require.Equal(t, test.expectedAttempt, attempt)
			require.Equal(t, test.expectedSandboxID, id)
			require.Equal(t, test.expectedReason, reason)
		})
	}
}

// fakeRuntimeHandlerResolver 是空实现的 runtime handler 解析器，测试中不需要真实 handler。
type fakeRuntimeHandlerResolver struct{}

// LookupRuntimeHandler 始终返回空 handler，满足接口即可。
func (*fakeRuntimeHandlerResolver) LookupRuntimeHandler(s *string) (string, error) {
	return "", nil
}

// TestNamespacesForPod 验证 Pod 规格到 CRI NamespaceOption 的映射：
// 默认命名空间、host 命名空间、共享进程命名空间、user namespace，以及特性关闭时的报错。
func TestNamespacesForPod(t *testing.T) {
	tCtx := ktesting.Init(t)
	usernsIDs := &runtimeapi.IDMapping{
		HostId:      65536,
		ContainerId: 0,
		Length:      65536,
	}

	for desc, test := range map[string]struct {
		input           *v1.Pod
		runtimeHandlers map[string]kubecontainer.RuntimeHandler
		usernsEnabled   bool
		expected        *runtimeapi.NamespaceOption
		expErr          bool
	}{
		"nil pod -> default v1 namespaces": {
			input: nil,
			expected: &runtimeapi.NamespaceOption{
				Ipc:     runtimeapi.NamespaceMode_POD,
				Network: runtimeapi.NamespaceMode_POD,
				Pid:     runtimeapi.NamespaceMode_CONTAINER,
			},
		},
		"v1.Pod default namespaces": {
			input: &v1.Pod{},
			expected: &runtimeapi.NamespaceOption{
				Ipc:     runtimeapi.NamespaceMode_POD,
				Network: runtimeapi.NamespaceMode_POD,
				Pid:     runtimeapi.NamespaceMode_CONTAINER,
			},
		},
		"Host Namespaces": {
			input: &v1.Pod{
				Spec: v1.PodSpec{
					HostIPC:     true,
					HostNetwork: true,
					HostPID:     true,
				},
			},
			expected: &runtimeapi.NamespaceOption{
				Ipc:     runtimeapi.NamespaceMode_NODE,
				Network: runtimeapi.NamespaceMode_NODE,
				Pid:     runtimeapi.NamespaceMode_NODE,
			},
		},
		"Shared Process Namespace (feature enabled)": {
			input: &v1.Pod{
				Spec: v1.PodSpec{
					ShareProcessNamespace: &[]bool{true}[0],
				},
			},
			expected: &runtimeapi.NamespaceOption{
				Ipc:     runtimeapi.NamespaceMode_POD,
				Network: runtimeapi.NamespaceMode_POD,
				Pid:     runtimeapi.NamespaceMode_POD,
			},
		},
		"Shared Process Namespace, redundant flag (feature enabled)": {
			input: &v1.Pod{
				Spec: v1.PodSpec{
					ShareProcessNamespace: &[]bool{false}[0],
				},
			},
			expected: &runtimeapi.NamespaceOption{
				Ipc:     runtimeapi.NamespaceMode_POD,
				Network: runtimeapi.NamespaceMode_POD,
				Pid:     runtimeapi.NamespaceMode_CONTAINER,
			},
		},
		"hostUsers: false and feature enabled": {
			input: &v1.Pod{
				Spec: v1.PodSpec{
					HostUsers: &[]bool{false}[0],
				},
			},
			usernsEnabled: true,
			runtimeHandlers: map[string]kubecontainer.RuntimeHandler{
				"": {
					SupportsUserNamespaces: true,
				},
			},
			expected: &runtimeapi.NamespaceOption{
				Ipc:     runtimeapi.NamespaceMode_POD,
				Network: runtimeapi.NamespaceMode_POD,
				Pid:     runtimeapi.NamespaceMode_CONTAINER,
				UsernsOptions: &runtimeapi.UserNamespace{
					Mode: runtimeapi.NamespaceMode_POD,
					Uids: []*runtimeapi.IDMapping{usernsIDs},
					Gids: []*runtimeapi.IDMapping{usernsIDs},
				},
			},
		},
		// 特性关闭时 hostUsers 不允许设置任何值，否则应报错。
		"hostUsers: false and feature disabled --> error": {
			input: &v1.Pod{
				Spec: v1.PodSpec{
					HostUsers: &[]bool{false}[0],
				},
			},
			usernsEnabled: false,
			expErr:        true,
		},
		// 特性关闭时 hostUsers 不允许设置任何值，否则应报错。
		"hostUsers: true and feature disabled --> error": {
			input: &v1.Pod{
				Spec: v1.PodSpec{
					HostUsers: &[]bool{true}[0],
				},
			},
			usernsEnabled: false,
			expErr:        true,
		},
		"error if runtime handler not found": {
			input: &v1.Pod{
				Spec: v1.PodSpec{
					HostUsers: &[]bool{false}[0],
				},
			},
			usernsEnabled: true,
			runtimeHandlers: map[string]kubecontainer.RuntimeHandler{
				"other": {},
			},
			expErr: true,
		},
		"error if runtime handler does not support userns": {
			input: &v1.Pod{
				Spec: v1.PodSpec{
					HostUsers: &[]bool{false}[0],
				},
			},
			usernsEnabled: true,
			expErr:        true,
		},
	} {
		t.Run(desc, func(t *testing.T) {
			if !test.usernsEnabled {
				// 先把模拟版本固定到 1.35，才能在该测试中关闭已 GA 的特性门控。
				featuregatetesting.SetFeatureGateEmulationVersionDuringTest(t, utilfeature.DefaultFeatureGate, version.MustParse("1.35"))
				featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, pkgfeatures.UserNamespacesSupport, false)
			}

			// fake runtime helper 提供测试所需的 runtime handler 集合。
			fakeRuntimeHelper := kubecontainertest.FakeRuntimeHelper{
				RuntimeHandlers: test.runtimeHandlers,
			}
			// 把 Pod 规格转换为 CRI 命名空间选项：Pod 级、节点级或容器级。
			actual, err := NamespacesForPod(tCtx, test.input, &fakeRuntimeHelper, &fakeRuntimeHandlerResolver{})
			if test.expErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, test.expected, actual)
			}
		})
	}
}
