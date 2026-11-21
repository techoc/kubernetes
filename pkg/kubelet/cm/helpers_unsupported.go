//go:build !linux

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

package cm

import (
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"
	kubeletconfig "k8s.io/kubernetes/pkg/kubelet/apis/config"
)

const (
	// 非 Linux 平台没有 cgroup 配额/份额概念，以下常量全部返回 0。
	MinShares = 0
	MaxShares = 0

	SharesPerCPU  = 0
	MilliCPUToCPU = 0

	QuotaPeriod      = 0
	MinQuotaPeriod   = 0
	MinMilliCPULimit = 0
)

// MilliCPUToQuota 在非 Linux 平台返回 0。
func MilliCPUToQuota(milliCPU, period int64) int64 {
	return 0
}

// MilliCPUToShares 在非 Linux 平台返回 0。
func MilliCPUToShares(milliCPU int64) uint64 {
	return 0
}

// ResourceConfigForPod 在非 Linux 平台返回 nil。
func ResourceConfigForPod(pod *v1.Pod, enforceCPULimit bool, cpuPeriod uint64, enforceMemoryQoS bool, memoryReservationPolicy kubeletconfig.MemoryReservationPolicy) *ResourceConfig {
	return nil
}

// GetCgroupSubsystems 在非 Linux 平台返回 nil。
func GetCgroupSubsystems() (*CgroupSubsystems, error) {
	return nil, nil
}

// getCgroupProcs 在非 Linux 平台返回 nil。
func getCgroupProcs(dir string) ([]int, error) {
	return nil, nil
}

// GetPodCgroupNameSuffix 在非 Linux 平台返回空字符串。
func GetPodCgroupNameSuffix(podUID types.UID) string {
	return ""
}

// NodeAllocatableRoot 在非 Linux 平台返回空字符串。
func NodeAllocatableRoot(cgroupRoot string, cgroupsPerQOS bool, cgroupDriver string) string {
	return ""
}

// GetKubeletContainer 在非 Linux 平台返回空字符串。
func GetKubeletContainer(logger klog.Logger, kubeletCgroups string) (string, error) {
	return "", nil
}

// CPURequestsFromConfig 在非 Linux 平台返回 nil。
func CPURequestsFromConfig(podConfig *ResourceConfig) *resource.Quantity {
	return nil
}

// CPUSharesEqualAfterV2RoundTrip 在非 Linux 平台始终返回 false。
func CPUSharesEqualAfterV2RoundTrip(allocatedShares, readbackShares uint64) bool {
	return false
}

// CPULimitsFromConfig 在非 Linux 平台返回 nil。
func CPULimitsFromConfig(podConfig *ResourceConfig) *resource.Quantity {
	return nil
}

// MemoryLimitsFromConfig 在非 Linux 平台返回 nil。
func MemoryLimitsFromConfig(podConfig *ResourceConfig) *resource.Quantity {
	return nil
}
