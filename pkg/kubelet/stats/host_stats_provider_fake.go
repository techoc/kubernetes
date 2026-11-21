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

package stats

import (
	"fmt"
	"path/filepath"

	cadvisorapi "github.com/google/cadvisor/lib/model"
	"k8s.io/apimachinery/pkg/types"
	statsapi "k8s.io/kubelet/pkg/apis/stats/v1alpha1"
	kubecontainer "k8s.io/kubernetes/pkg/kubelet/container"
	"k8s.io/kubernetes/pkg/kubelet/kuberuntime"
	"k8s.io/kubernetes/pkg/volume"
)

// fakeHostStatsProvider 是供测试使用的 HostStatsProvider 实现。
type fakeHostStatsProvider struct {
	// fakeStats 是路径到预置卷指标的映射。
	fakeStats map[string]*volume.Metrics
	// osInterface 用于读取日志目录。
	osInterface kubecontainer.OSInterface
}

// NewFakeHostStatsProvider 创建不带预置数据的 fake 提供者。
func NewFakeHostStatsProvider(osInterface kubecontainer.OSInterface) HostStatsProvider {
	return &fakeHostStatsProvider{
		osInterface: osInterface,
	}
}

// NewFakeHostStatsProviderWithData 创建带预置数据的 fake 提供者。
func NewFakeHostStatsProviderWithData(fakeStats map[string]*volume.Metrics, osInterface kubecontainer.OSInterface) HostStatsProvider {
	return &fakeHostStatsProvider{
		fakeStats:   fakeStats,
		osInterface: osInterface,
	}
}

// getPodLogStats 汇总 Pod 日志目录下各文件的用量。
func (f *fakeHostStatsProvider) getPodLogStats(podNamespace, podName string, podUID types.UID, rootFsInfo *cadvisorapi.FsInfo) (*statsapi.FsStats, error) {
	path := kuberuntime.BuildPodLogsDirectory("/var/log/kube/pods/", podNamespace, podName, podUID)
	files, err := f.osInterface.ReadDir(path)
	if err != nil {
		return nil, err
	}
	var results []volume.MetricsProvider
	// 跳过子目录，只处理文件。
	for _, file := range files {
		if file.IsDir() {
			continue
		}
		// 只统计 Pod 日志目录下的普通文件。
		fpath := filepath.Join(path, file.Name())
		results = append(results, NewFakeMetricsDu(fpath, f.fakeStats[fpath]))
	}
	return fakeMetricsProvidersToStats(results, rootFsInfo)
}

// getPodContainerLogStats 返回指定容器日志文件的用量。
func (f *fakeHostStatsProvider) getPodContainerLogStats(podNamespace, podName string, podUID types.UID, containerName string, rootFsInfo *cadvisorapi.FsInfo) (*statsapi.FsStats, error) {
	path := kuberuntime.BuildContainerLogsDirectory("/var/log/kube/pods/", podNamespace, podName, podUID, containerName)
	metricsProvider := NewFakeMetricsDu(path, f.fakeStats[path])
	return fakeMetricsProvidersToStats([]volume.MetricsProvider{metricsProvider}, rootFsInfo)
}

// getPodEtcHostsStats 未实现，始终返回错误。
func (f *fakeHostStatsProvider) getPodEtcHostsStats(podUID types.UID, rootFsInfo *cadvisorapi.FsInfo) (*statsapi.FsStats, error) {
	return nil, fmt.Errorf("not implemented")
}

// fakeMetricsProvidersToStats 把多个路径的指标累加为一份 FsStats。
func fakeMetricsProvidersToStats(metricsProviders []volume.MetricsProvider, rootFsInfo *cadvisorapi.FsInfo) (*statsapi.FsStats, error) {
	// 以 rootfs 统计为基准，累加各路径的用量。
	result := rootFsInfoToFsStats(rootFsInfo)
	for i, metricsProvider := range metricsProviders {
		hostMetrics, err := metricsProvider.GetMetrics()
		if err != nil {
			return nil, fmt.Errorf("failed to get stats for item %d: %v", i, err)
		}
		// 累加已用字节与 inode 数，并取最新时间戳。
		usedBytes := uint64(hostMetrics.Used.Value())
		inodesUsed := uint64(hostMetrics.InodesUsed.Value())
		result.UsedBytes = addUsage(result.UsedBytes, &usedBytes)
		result.InodesUsed = addUsage(result.InodesUsed, &inodesUsed)
		result.Time = maxUpdateTime(&result.Time, &hostMetrics.Time)
	}
	return result, nil
}

// fakeMetricsDu 返回预置的卷指标。
type fakeMetricsDu struct {
	// fakeStats 是预置的卷指标。
	fakeStats *volume.Metrics
}

// NewFakeMetricsDu 创建返回预置指标的 MetricsProvider。
func NewFakeMetricsDu(path string, stats *volume.Metrics) volume.MetricsProvider {
	return &fakeMetricsDu{fakeStats: stats}
}

// GetMetrics 返回预置的卷指标。
func (f *fakeMetricsDu) GetMetrics() (*volume.Metrics, error) {
	if f.fakeStats == nil {
		return nil, fmt.Errorf("no stats provided")
	}
	return f.fakeStats, nil
}
