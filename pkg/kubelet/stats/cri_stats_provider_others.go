//go:build !linux && !windows

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

package stats

import (
	cadvisorapi "github.com/google/cadvisor/lib/model"

	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
	"k8s.io/klog/v2"
	statsapi "k8s.io/kubelet/pkg/apis/stats/v1alpha1"
)

// listContainerNetworkStats 返回所有运行中容器的网络统计；
// 非 Windows 平台应返回 (nil, nil)。
func (p *criStatsProvider) listContainerNetworkStats(klog.Logger) (map[string]*statsapi.NetworkStats, error) {
	return nil, nil
}

// addCRIPodContainerStats 在非 Windows 平台无需补充容器统计，为空实现。
func (p *criStatsProvider) addCRIPodContainerStats(
	klog.Logger,
	*runtimeapi.PodSandboxStats,
	*statsapi.PodStats,
	map[string]*cadvisorapi.FsInfo,
	map[string]*runtimeapi.Container,
	*runtimeapi.PodSandbox,
	*cadvisorapi.FsInfo,
	bool) error {
	return nil
}

// addCRIPodNetworkStats 在非 Windows 平台为空实现。
func addCRIPodNetworkStats(ps *statsapi.PodStats, criPodStat *runtimeapi.PodSandboxStats) {
}

// addCRIPodMemoryStats 在非 Windows 平台为空实现。
func addCRIPodMemoryStats(ps *statsapi.PodStats, criPodStat *runtimeapi.PodSandboxStats) {
}

// addCRIPodCPUStats 在非 Windows 平台为空实现。
func addCRIPodCPUStats(ps *statsapi.PodStats, criPodStat *runtimeapi.PodSandboxStats) {
}

// addCRIPodProcessStats 在非 Windows 平台为空实现。
func addCRIPodProcessStats(ps *statsapi.PodStats, criPodStat *runtimeapi.PodSandboxStats) {
}

// addCRIPodIOStats 在非 Windows 平台为空实现。
func addCRIPodIOStats(ps *statsapi.PodStats, criPodStat *runtimeapi.PodSandboxStats) {
}

// addCRIPodContainerCPUAndMemoryStats 在非 Windows 平台为空实现。
func (p *criStatsProvider) addCRIPodContainerCPUAndMemoryStats(
	*runtimeapi.PodSandboxStats,
	*statsapi.PodStats,
	map[string]*runtimeapi.Container) {
}
