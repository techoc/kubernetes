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

package util

import (
	"fmt"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/kubernetes/pkg/util/filesystem"
)

// FromApiserverCache 修改 opts，使 GET 请求由 apiserver 缓存返回而不是读取 etcd。
func FromApiserverCache(opts *metav1.GetOptions) {
	// ResourceVersion=0 让 apiserver 从缓存返回结果，避免直接读 etcd。
	opts.ResourceVersion = "0"
}

// IsUnixDomainSocket 是判断 Unix domain socket 的函数别名。
var IsUnixDomainSocket = filesystem.IsUnixDomainSocket

// GetNodenameForKernel 返回要写入 Pod hostname 字段（utsname 的 nodename）的值。
func GetNodenameForKernel(hostname string, hostDomainName string, setHostnameAsFQDN *bool) (string, error) {
	kernelHostname := hostname
	// FQDN 最长 64 字符，才能放入 Linux 内核的 nodename 字段（规格为 64 字符加结尾空字符）。
	const fqdnMaxLen = 64
	if len(hostDomainName) > 0 && setHostnameAsFQDN != nil && *setHostnameAsFQDN {
		fqdn := fmt.Sprintf("%s.%s", hostname, hostDomainName)
		// FQDN 长度不能超过 fqdnMaxLen。
		if len(fqdn) > fqdnMaxLen {
			return "", fmt.Errorf("failed to construct FQDN from pod hostname and cluster domain, FQDN %s is too long (%d characters is the max, %d characters requested)", fqdn, fqdnMaxLen, len(fqdn))
		}
		kernelHostname = fqdn
	}
	return kernelHostname, nil
}

// GetContainerByIndex 校验并从 containers 中提取索引 idx 处的容器，
// 同时与 statuses 对应位置进行比对。有效时返回 true。
func GetContainerByIndex(containers []v1.Container, statuses []v1.ContainerStatus, idx int) (v1.Container, bool) {
	if idx < 0 || idx >= len(containers) || idx >= len(statuses) {
		return v1.Container{}, false
	}
	if statuses[idx].Name != containers[idx].Name {
		return v1.Container{}, false
	}
	return containers[idx], true
}

// ResourceOpts 汇总 Pod 级与容器级资源需求。
type ResourceOpts struct {
	// PodResources 是 Pod 级资源需求。
	PodResources *v1.ResourceRequirements
	// ContainerResources 是容器级资源需求。
	ContainerResources *v1.ResourceRequirements
}

// GetLimits 计算容器的资源限制。
// 优先使用容器显式设置的限制；若容器级 CPU 或内存限制未设置（即为零），
// 则使用 Pod 级资源需求中的对应限制作为默认值。
// TODO(ndixita): 整合资源限制逻辑。CPU/内存限制的计算在此处与
// pkg/kubelet/kuberuntime/kuberuntime_container_linux.go（getCPULimits/getMemoryLimits）
// 重复，应重构为共享的工具函数。
func GetLimits(res *ResourceOpts) v1.ResourceList {
	if res == nil {
		return v1.ResourceList{}
	}

	// limitsOrEmpty 安全地深拷贝容器 limits，nil 时返回空映射。
	limitsOrEmpty := func(containerResources *v1.ResourceRequirements) v1.ResourceList {
		if containerResources == nil || containerResources.Limits == nil {
			return v1.ResourceList{}
		}
		return containerResources.Limits.DeepCopy()
	}

	containerLimits := limitsOrEmpty(res.ContainerResources)

	// 未设置 Pod 级限制时直接返回容器级限制。
	if res.PodResources == nil || len(res.PodResources.Limits) == 0 {
		// 未设置 Pod 级限制时直接返回容器级限制。
		return containerLimits
	}

	podLevelLimits := res.PodResources.Limits

	// 容器级 CPU 限制未设置时，使用 Pod 级 CPU 限制。
	// 容器级 CPU 限制为零时使用 Pod 级限制。
	if containerLimits.Cpu().IsZero() && !podLevelLimits.Cpu().IsZero() {
		containerLimits[v1.ResourceCPU] = podLevelLimits[v1.ResourceCPU].DeepCopy()
	}

	// 容器级内存限制未设置时，使用 Pod 级内存限制。
	// 容器级内存限制为零时使用 Pod 级限制。
	if containerLimits.Memory().IsZero() && !podLevelLimits.Memory().IsZero() {
		containerLimits[v1.ResourceMemory] = podLevelLimits[v1.ResourceMemory].DeepCopy()
	}
	return containerLimits
}
