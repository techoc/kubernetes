//go:build !windows

/*
Copyright 2018 The Kubernetes Authors.

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
	"context"
	"errors"

	"k8s.io/klog/v2"

	cadvisormemory "github.com/google/cadvisor/lib/cache/memory"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	statsapi "k8s.io/kubelet/pkg/apis/stats/v1alpha1"
	"k8s.io/kubernetes/pkg/kubelet/cm"
)

// GetSystemContainersStats 返回各系统容器的统计信息。
func (sp *summaryProviderImpl) GetSystemContainersStats(ctx context.Context, nodeConfig cm.NodeConfig, podStats []statsapi.PodStats, updateStats bool) (stats []statsapi.ContainerStats) {
	logger := klog.FromContext(ctx)
	// 收集 kubelet、runtime、misc 与 pods 四类系统容器的 cgroup 名称。
	systemContainers := map[string]struct {
		name             string
		forceStatsUpdate bool
		startTime        metav1.Time
	}{
		statsapi.SystemContainerKubelet: {name: nodeConfig.KubeletCgroupsName, forceStatsUpdate: false, startTime: sp.kubeletCreationTime},
		statsapi.SystemContainerRuntime: {name: nodeConfig.RuntimeCgroupsName, forceStatsUpdate: false},
		statsapi.SystemContainerMisc:    {name: nodeConfig.SystemCgroupsName, forceStatsUpdate: false},
		statsapi.SystemContainerPods:    {name: sp.provider.GetPodCgroupRoot(), forceStatsUpdate: updateStats},
	}
	for sys, cont := range systemContainers {
		// cgroup 名称未定义时跳过（并非所有系统容器都必需）。
		if cont.name == "" {
			continue
		}
		s, _, err := sp.provider.GetCgroupStats(ctx, cont.name, cont.forceStatsUpdate)
		if err != nil {
			logger.Error(err, "Failed to get system container stats", "containerName", cont.name)
			continue
		}
		// 系统容器没有关联的文件系统，清空相关统计。
		s.Logs, s.Rootfs = nil, nil
		s.Name = sys

		// 若已知系统容器的启动时间，优先使用它而不是 cAdvisor 提供的时间。
		if !cont.startTime.IsZero() {
			s.StartTime = cont.startTime
		}
		stats = append(stats, *s)
	}

	return stats
}

// GetSystemContainersCPUAndMemoryStats 返回各系统容器的 CPU 与内存统计信息。
func (sp *summaryProviderImpl) GetSystemContainersCPUAndMemoryStats(ctx context.Context, nodeConfig cm.NodeConfig, podStats []statsapi.PodStats, updateStats bool) (stats []statsapi.ContainerStats) {
	logger := klog.FromContext(ctx)
	// 与 GetSystemContainersStats 使用相同的系统容器集合。
	systemContainers := map[string]struct {
		name             string
		forceStatsUpdate bool
		startTime        metav1.Time
	}{
		statsapi.SystemContainerKubelet: {name: nodeConfig.KubeletCgroupsName, forceStatsUpdate: false, startTime: sp.kubeletCreationTime},
		statsapi.SystemContainerRuntime: {name: nodeConfig.RuntimeCgroupsName, forceStatsUpdate: false},
		statsapi.SystemContainerMisc:    {name: nodeConfig.SystemCgroupsName, forceStatsUpdate: false},
		statsapi.SystemContainerPods:    {name: sp.provider.GetPodCgroupRoot(), forceStatsUpdate: updateStats},
	}
	for sys, cont := range systemContainers {
		// cgroup 名称未定义时跳过（并非所有系统容器都必需）。
		if cont.name == "" {
			continue
		}
		s, err := sp.provider.GetCgroupCPUAndMemoryStats(cont.name, cont.forceStatsUpdate)
		if err != nil {
			// 内存缓存中找不到统计信息属于正常情况，仅记录低级别日志。
			if errors.Is(err, cadvisormemory.ErrDataNotFound) {
				logger.V(4).Info("cgroup stats not found in memory cache", "containerName", cont.name)
			} else {
				logger.Error(err, "Failed to get system container stats", "containerName", cont.name)
			}
			continue
		}
		s.Name = sys

		// 若已知系统容器的启动时间，优先使用它而不是 cAdvisor 提供的时间。
		if !cont.startTime.IsZero() {
			s.StartTime = cont.startTime
		}
		stats = append(stats, *s)
	}

	return stats
}
