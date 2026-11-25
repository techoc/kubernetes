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

package cache

import (
	"sync"

	compbasemetrics "k8s.io/component-base/metrics"
	"k8s.io/component-base/metrics/legacyregistry"
)

// SELinux 卷挂载相关指标。
var (
	// TODO: 为这些指标补充插件名称和访问模式标签。
	// seLinuxContainerContextErrors 统计无法为容器计算 SELinux 上下文的错误数。
	seLinuxContainerContextErrors = compbasemetrics.NewGaugeVec(
		&compbasemetrics.GaugeOpts{
			Name:           "volume_manager_selinux_container_errors_total",
			Help:           "Number of errors when kubelet cannot compute SELinux context for a container. Kubelet can't start such a Pod then and it will retry, therefore value of this metric may not represent the actual nr. of containers.",
			StabilityLevel: compbasemetrics.ALPHA,
		},
		[]string{"access_mode"},
	)
	// seLinuxContainerContextWarnings 统计被忽略的容器 SELinux 上下文错误数。
	seLinuxContainerContextWarnings = compbasemetrics.NewGaugeVec(
		&compbasemetrics.GaugeOpts{
			Name:           "volume_manager_selinux_container_warnings_total",
			StabilityLevel: compbasemetrics.ALPHA,
			Help:           "Number of errors when kubelet cannot compute SELinux context for a container that are ignored. They will become real errors when SELinuxMountReadWriteOncePod feature is expanded to all volume access modes.",
		},
		[]string{"access_mode"},
	)
	// seLinuxPodContextMismatchErrors 统计同一卷在不同容器间 SELinux 上下文不一致的错误数。
	seLinuxPodContextMismatchErrors = compbasemetrics.NewGaugeVec(
		&compbasemetrics.GaugeOpts{
			Name:           "volume_manager_selinux_pod_context_mismatch_errors_total",
			Help:           "Number of errors when a Pod defines different SELinux contexts for its containers that use the same volume. Kubelet can't start such a Pod then and it will retry, therefore value of this metric may not represent the actual nr. of Pods.",
			StabilityLevel: compbasemetrics.ALPHA,
		},
		[]string{"access_mode"},
	)
	// seLinuxPodContextMismatchWarnings 统计上述上下文不一致但暂被忽略的警告数。
	seLinuxPodContextMismatchWarnings = compbasemetrics.NewGaugeVec(
		&compbasemetrics.GaugeOpts{
			Name:           "volume_manager_selinux_pod_context_mismatch_warnings_total",
			Help:           "Number of errors when a Pod defines different SELinux contexts for its containers that use the same volume. They are not errors yet, but they will become real errors when SELinuxMountReadWriteOncePod feature is expanded to all volume access modes.",
			StabilityLevel: compbasemetrics.ALPHA,
		},
		[]string{"access_mode"},
	)
	// seLinuxVolumeContextMismatchErrors 统计卷已按不同 SELinux 上下文挂载的错误数。
	seLinuxVolumeContextMismatchErrors = compbasemetrics.NewGaugeVec(
		&compbasemetrics.GaugeOpts{
			Name:           "volume_manager_selinux_volume_context_mismatch_errors_total",
			Help:           "Number of errors when a Pod uses a volume that is already mounted with a different SELinux context than the Pod needs. Kubelet can't start such a Pod then and it will retry, therefore value of this metric may not represent the actual nr. of Pods.",
			StabilityLevel: compbasemetrics.ALPHA,
		},
		[]string{"volume_plugin", "access_mode"},
	)
	// seLinuxVolumeContextMismatchWarnings 统计上述卷上下文不一致但暂被忽略的警告数。
	seLinuxVolumeContextMismatchWarnings = compbasemetrics.NewGaugeVec(
		&compbasemetrics.GaugeOpts{
			Name:           "volume_manager_selinux_volume_context_mismatch_warnings_total",
			Help:           "Number of errors when a Pod uses a volume that is already mounted with a different SELinux context than the Pod needs. They are not errors yet, but they will become real errors when SELinuxMountReadWriteOncePod feature is expanded to all volume access modes.",
			StabilityLevel: compbasemetrics.ALPHA,
		},
		[]string{"volume_plugin", "access_mode"},
	)
	// seLinuxVolumesAdmitted 统计 SELinux 上下文正常、可使用 mount -o context 挂载的卷数。
	seLinuxVolumesAdmitted = compbasemetrics.NewGaugeVec(
		&compbasemetrics.GaugeOpts{
			Name:           "volume_manager_selinux_volumes_admitted_total",
			Help:           "Number of volumes whose SELinux context was fine and will be mounted with mount -o context option.",
			StabilityLevel: compbasemetrics.ALPHA,
		},
		[]string{"volume_plugin", "access_mode"},
	)

	// registerMetrics 保证指标只注册一次。
	registerMetrics sync.Once
)

// registerSELinuxMetrics 把 SELinux 相关指标注册到 legacy registry。
func registerSELinuxMetrics() {
	registerMetrics.Do(func() {
		legacyregistry.MustRegister(seLinuxContainerContextErrors)
		legacyregistry.MustRegister(seLinuxContainerContextWarnings)
		legacyregistry.MustRegister(seLinuxPodContextMismatchErrors)
		legacyregistry.MustRegister(seLinuxPodContextMismatchWarnings)
		legacyregistry.MustRegister(seLinuxVolumeContextMismatchErrors)
		legacyregistry.MustRegister(seLinuxVolumeContextMismatchWarnings)
		legacyregistry.MustRegister(seLinuxVolumesAdmitted)
	})
}
