/*
Copyright 2017 The Kubernetes Authors.

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

package testing

import (
	"context"
	"fmt"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	kubetypes "k8s.io/apimachinery/pkg/types"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
	"k8s.io/klog/v2"
	statsapi "k8s.io/kubelet/pkg/apis/stats/v1alpha1"
	"k8s.io/kubernetes/pkg/features"
	kubecontainer "k8s.io/kubernetes/pkg/kubelet/container"
)

// FakeRuntimeHelper 是供测试使用的 RuntimeHelper 实现。
type FakeRuntimeHelper struct {
	DNSServers      []string
	DNSSearches     []string
	DNSOptions      []string
	HostName        string
	HostDomain      string
	PodContainerDir string
	RuntimeHandlers map[string]kubecontainer.RuntimeHandler
	Err             error
	PodStats        map[kubetypes.UID]*statsapi.PodStats
}

// GenerateRunContainerOptions 返回最小化的容器启动选项，仅在配置了 termination message 路径时设置 Pod 容器目录。
func (f *FakeRuntimeHelper) GenerateRunContainerOptions(_ context.Context, pod *v1.Pod, container *v1.Container, podIP string, podIPs []string, imageVolumes kubecontainer.ImageVolumes) (*kubecontainer.RunContainerOptions, func(), error) {
	var opts kubecontainer.RunContainerOptions
	// 只有需要写 termination message 时才指定容器目录。
	if len(container.TerminationMessagePath) != 0 {
		opts.PodContainerDir = f.PodContainerDir
	}
	return &opts, nil, nil
}

// GetPodCgroupParent 返回空字符串，测试中不关心 cgroup 父路径。
func (f *FakeRuntimeHelper) GetPodCgroupParent(pod *v1.Pod) string {
	return ""
}

// GetPodDNS 返回预置的 DNS 配置与错误。
func (f *FakeRuntimeHelper) GetPodDNS(_ context.Context, pod *v1.Pod) (*runtimeapi.DNSConfig, error) {
	return &runtimeapi.DNSConfig{
		Servers:  f.DNSServers,
		Searches: f.DNSSearches,
		Options:  f.DNSOptions}, f.Err
}

// GeneratePodHostNameAndDomain 返回预置的主机名与域名；docker runtime 不会调用它。
func (f *FakeRuntimeHelper) GeneratePodHostNameAndDomain(logger klog.Logger, _ *v1.Pod) (string, string, error) {
	return f.HostName, f.HostDomain, f.Err
}

// GetPodDir 返回按 UID 拼出的固定 Pod 目录。
func (f *FakeRuntimeHelper) GetPodDir(podUID kubetypes.UID) string {
	return "/poddir/" + string(podUID)
}

// GetExtraSupplementalGroupsForPod 返回 nil，测试中不提供额外补充组。
func (f *FakeRuntimeHelper) GetExtraSupplementalGroupsForPod(pod *v1.Pod) []int64 {
	return nil
}

// GetOrCreateUserNamespaceMappings 根据 HostUsers 字段和特性门控模拟用户命名空间映射的创建。
func (f *FakeRuntimeHelper) GetOrCreateUserNamespaceMappings(logger klog.Logger, pod *v1.Pod, runtimeHandler string) (*runtimeapi.UserNamespace, error) {
	featureEnabled := utilfeature.DefaultFeatureGate.Enabled(features.UserNamespacesSupport)
	// 未显式设置 HostUsers 时沿用运行时默认行为。
	if pod == nil || pod.Spec.HostUsers == nil {
		return nil, nil
	}
	// HostUsers 已显式设置，但门控未开启时不允许使用该字段。
	if !featureEnabled {
		return nil, fmt.Errorf("the feature gate %q is disabled: can't set spec.HostUsers", features.UserNamespacesSupport)
	}
	// hostUsers=true 表示与宿主共享用户，无需映射。
	if *pod.Spec.HostUsers {
		return nil, nil
	}

	// 从这里开始：hostUsers=false 且特性门控已开启。

	// Pod 请求了用户命名空间，而运行时处理器不支持时必须报错。
	if h, ok := f.RuntimeHandlers[runtimeHandler]; !ok {
		return nil, fmt.Errorf("RuntimeClass handler %q not found", runtimeHandler)
	} else if !h.SupportsUserNamespaces {
		return nil, fmt.Errorf("RuntimeClass handler %q does not support user namespaces", runtimeHandler)
	}

	// 固定映射 65536 个 ID，便于测试断言。
	ids := &runtimeapi.IDMapping{
		HostId:      65536,
		ContainerId: 0,
		Length:      65536,
	}

	return &runtimeapi.UserNamespace{
		Mode: runtimeapi.NamespaceMode_POD,
		Uids: []*runtimeapi.IDMapping{ids},
		Gids: []*runtimeapi.IDMapping{ids},
	}, nil
}

// PrepareDynamicResources 是空实现，始终返回成功。
func (f *FakeRuntimeHelper) PrepareDynamicResources(ctx context.Context, pod *v1.Pod) error {
	return nil
}

// UnprepareDynamicResources 是空实现，始终返回成功。
func (f *FakeRuntimeHelper) UnprepareDynamicResources(ctx context.Context, pod *v1.Pod) error {
	return nil
}

// RequestPodReinspect 是空实现。
func (f *FakeRuntimeHelper) RequestPodReinspect(_ kubetypes.UID) {
	// 未实现。
}

// RequestPodRelist 是空实现。
func (f *FakeRuntimeHelper) RequestPodRelist(_ kubetypes.UID) {
	// 未实现。
}

// PodCPUAndMemoryStats 返回预置的 Pod 统计信息，找不到时返回错误。
func (f *FakeRuntimeHelper) PodCPUAndMemoryStats(_ context.Context, pod *v1.Pod, _ *kubecontainer.PodStatus) (*statsapi.PodStats, error) {
	if stats, ok := f.PodStats[pod.UID]; ok {
		return stats, nil
	}
	return nil, fmt.Errorf("stats for pod %q not found", pod.UID)
}

// OnPodSandboxReady 是空实现。
func (f *FakeRuntimeHelper) OnPodSandboxReady(_ context.Context, _ *v1.Pod) error {
	// 未实现。
	return nil
}

// ResizeEphemeralVolume 未实现，测试中直接返回成功。
func (f *FakeRuntimeHelper) ResizeEphemeralVolume(pod *v1.Pod, volumeName string, newSize *resource.Quantity) error {
	return nil
}
