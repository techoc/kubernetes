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
	"context"

	v1 "k8s.io/api/core/v1"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
	"k8s.io/klog/v2"
	kubecontainer "k8s.io/kubernetes/pkg/kubelet/container"
	kubetypes "k8s.io/kubernetes/pkg/kubelet/types"
)

// PodSandboxChanged 判断 Pod 的 sandbox 是否需要重建，
// 返回（是否变化、新的 attempt 序号、原 sandboxID（若存在）、变化原因）。
func PodSandboxChanged(pod *v1.Pod, podStatus *kubecontainer.PodStatus) (bool, uint32, string, string) {
	ctx := context.TODO() // 该上下文未来会作为参数传入。
	logger := klog.FromContext(ctx)
	// 找不到 sandbox 时必须新建。
	if len(podStatus.SandboxStatuses) == 0 {
		logger.V(2).Info("No sandbox for pod can be found. Need to start a new one", "pod", klog.KObj(pod))
		return true, 0, "", kubetypes.PodSandboxNotReadyMsgNoPodSandbox
	}

	// 统计处于 Ready 状态的 sandbox 数量。
	readySandboxCount := 0
	for _, s := range podStatus.SandboxStatuses {
		if s.State == runtimeapi.PodSandboxState_SANDBOX_READY {
			readySandboxCount++
		}
	}

	// 存在多个 Ready sandbox，或最新的 sandbox 未 Ready 时，需要重建。
	sandboxStatus := podStatus.SandboxStatuses[0]
	if readySandboxCount > 1 {
		logger.V(2).Info("Multiple sandboxes are ready for Pod. Need to reconcile them", "pod", klog.KObj(pod))
		return true, sandboxStatus.Metadata.Attempt + 1, sandboxStatus.Id, kubetypes.PodSandboxNotReadyMsgMultipleSandboxes
	}
	if sandboxStatus.State != runtimeapi.PodSandboxState_SANDBOX_READY {
		logger.V(2).Info("No ready sandbox for pod can be found. Need to start a new one", "pod", klog.KObj(pod))
		return true, sandboxStatus.Metadata.Attempt + 1, sandboxStatus.Id, kubetypes.PodSandboxNotReadyMsgSandboxNotReady
	}

	// 网络命名空间模式变化时需要重建 sandbox。
	if sandboxStatus.GetLinux().GetNamespaces().GetOptions().GetNetwork() != NetworkNamespaceForPod(pod) {
		logger.V(2).Info("Sandbox for pod has changed. Need to start a new one", "pod", klog.KObj(pod))
		return true, sandboxStatus.Metadata.Attempt + 1, "", kubetypes.PodSandboxNotReadyMsgNetworkNamespaceMode
	}

	// sandbox 没有 IP 地址时需要重建。
	if !kubecontainer.IsHostNetworkPod(pod) && sandboxStatus.Network != nil && sandboxStatus.Network.Ip == "" {
		logger.V(2).Info("Sandbox for pod has no IP address. Need to start a new one", "pod", klog.KObj(pod))
		return true, sandboxStatus.Metadata.Attempt + 1, sandboxStatus.Id, kubetypes.PodSandboxNotReadyMsgNoIPAddress
	}

	return false, sandboxStatus.Metadata.Attempt, sandboxStatus.Id, ""
}

// IpcNamespaceForPod 返回 Pod 的 IPC 命名空间模式。
func IpcNamespaceForPod(pod *v1.Pod) runtimeapi.NamespaceMode {
	if pod != nil && pod.Spec.HostIPC {
		return runtimeapi.NamespaceMode_NODE
	}
	return runtimeapi.NamespaceMode_POD
}

// NetworkNamespaceForPod 返回 Pod 的网络命名空间模式。
func NetworkNamespaceForPod(pod *v1.Pod) runtimeapi.NamespaceMode {
	if pod != nil && pod.Spec.HostNetwork {
		return runtimeapi.NamespaceMode_NODE
	}
	return runtimeapi.NamespaceMode_POD
}

// PidNamespaceForPod 返回 Pod 的 PID 命名空间模式。
func PidNamespaceForPod(pod *v1.Pod) runtimeapi.NamespaceMode {
	if pod != nil {
		if pod.Spec.HostPID {
			return runtimeapi.NamespaceMode_NODE
		}
		if pod.Spec.ShareProcessNamespace != nil && *pod.Spec.ShareProcessNamespace {
			return runtimeapi.NamespaceMode_POD
		}
	}
	// 注意：v1.Pod 中 PID 的零值并非期望的默认模式。
	return runtimeapi.NamespaceMode_CONTAINER
}

// RuntimeHandlerResolver 由 *runtimeclass.Manager 实现。
type RuntimeHandlerResolver interface {
	// LookupRuntimeHandler 解析 RuntimeClass 名称对应的运行时处理器。
	LookupRuntimeHandler(runtimeClassName *string) (string, error)
}

// NamespacesForPod 返回给定 Pod 的 runtimeapi.NamespaceOption。
// 传入空或 nil 的 Pod 可获得 v1.Pod 的命名空间默认值。
func NamespacesForPod(ctx context.Context, pod *v1.Pod, runtimeHelper kubecontainer.RuntimeHelper, rcManager RuntimeHandlerResolver) (*runtimeapi.NamespaceOption, error) {
	logger := klog.FromContext(ctx)
	runtimeHandler := ""
	// RuntimeClass 可能指定不同的运行时处理器。
	if pod != nil && rcManager != nil {
		var err error
		runtimeHandler, err = rcManager.LookupRuntimeHandler(pod.Spec.RuntimeClassName)
		if err != nil {
			return nil, err
		}
	}
	// 由运行时助手决定是否需要创建用户命名空间映射。
	userNs, err := runtimeHelper.GetOrCreateUserNamespaceMappings(logger, pod, runtimeHandler)
	if err != nil {
		return nil, err
	}

	return &runtimeapi.NamespaceOption{
		Ipc:           IpcNamespaceForPod(pod),
		Network:       NetworkNamespaceForPod(pod),
		Pid:           PidNamespaceForPod(pod),
		UsernsOptions: userNs,
	}, nil
}
