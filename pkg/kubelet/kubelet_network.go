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

package kubelet

import (
	"context"
	"fmt"

	v1 "k8s.io/api/core/v1"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
	"k8s.io/klog/v2"
)

// updatePodCIDR 在与当前 CIDR 不同时更新运行时状态中的 Pod CIDR。
// 返回 Pod CIDR 是否实际发生变化。
func (kl *Kubelet) updatePodCIDR(ctx context.Context, cidr string) (bool, error) {
	logger := klog.FromContext(ctx)
	// 串行化 CIDR 更新，避免并发更新导致运行时与状态不一致。
	kl.updatePodCIDRMux.Lock()
	defer kl.updatePodCIDRMux.Unlock()

	podCIDR := kl.runtimeState.podCIDR()

	if podCIDR == cidr {
		return false, nil
	}

	// 调用链：kubelet -> 通用运行时 -> runtime shim -> 网络插件。
	// docker/非 CRI 实现会透传 UpdatePodCIDR。
	if err := kl.getRuntime().UpdatePodCIDR(ctx, cidr); err != nil {
		// 更新失败时理论上 CIDR 不会改变，但为稳妥起见仍返回 true，
		// 以便调用方触发后续同步。
		return true, fmt.Errorf("failed to update pod CIDR: %v", err)
	}
	logger.Info("Updating Pod CIDR", "originalPodCIDR", podCIDR, "newPodCIDR", cidr)
	kl.runtimeState.setPodCIDR(cidr)
	return true, nil
}

// GetPodDNS 返回 Pod 的 DNS 设置。
// 该函数由 kubecontainer.RuntimeHelper 接口定义，因此必须实现。
func (kl *Kubelet) GetPodDNS(ctx context.Context, pod *v1.Pod) (*runtimeapi.DNSConfig, error) {
	return kl.dnsConfigurer.GetPodDNS(ctx, pod)
}
