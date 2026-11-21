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
	"fmt"
	"os"
	"path/filepath"

	cadvisorapi "github.com/google/cadvisor/lib/model"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	statsapi "k8s.io/kubelet/pkg/apis/stats/v1alpha1"
	kubecontainer "k8s.io/kubernetes/pkg/kubelet/container"
	"k8s.io/kubernetes/pkg/kubelet/kuberuntime"
	"k8s.io/kubernetes/pkg/volume"
)

// PodEtcHostsPathFunc 根据 Pod UID 获取 etc-hosts 路径。
type PodEtcHostsPathFunc func(podUID types.UID) string

// metricsProviderByPath 把文件路径映射到对应的指标 provider。
type metricsProviderByPath map[string]volume.MetricsProvider

// HostStatsProvider 定义提供 Pod 相关宿主机统计的接口。
type HostStatsProvider interface {
	// 获取 Pod 日志占用的统计。
	getPodLogStats(podNamespace, podName string, podUID types.UID, rootFsInfo *cadvisorapi.FsInfo) (*statsapi.FsStats, error)
	// 获取容器日志占用的统计。
	getPodContainerLogStats(podNamespace, podName string, podUID types.UID, containerName string, rootFsInfo *cadvisorapi.FsInfo) (*statsapi.FsStats, error)
	// 获取 Pod etc-hosts 占用的统计。
	getPodEtcHostsStats(podUID types.UID, rootFsInfo *cadvisorapi.FsInfo) (*statsapi.FsStats, error)
}

// hostStatsProvider 是 HostStatsProvider 的默认实现。
type hostStatsProvider struct {
	// 系统调用接口。
	osInterface kubecontainer.OSInterface
	// 按 UID 获取 Pod etc-hosts 路径。
	podEtcHostsPathFunc PodEtcHostsPathFunc
	// Pod 日志根目录。
	podLogsDirectory string
}

// NewHostStatsProvider 创建 HostStatsProvider 实例。
func NewHostStatsProvider(osInterface kubecontainer.OSInterface, podEtcHostsPathFunc PodEtcHostsPathFunc, podLogsDirectory string) HostStatsProvider {
	return hostStatsProvider{
		osInterface:         osInterface,
		podEtcHostsPathFunc: podEtcHostsPathFunc,
		podLogsDirectory:    podLogsDirectory,
	}
}

// getPodLogStats 汇总 Pod 日志目录的磁盘占用。
func (h hostStatsProvider) getPodLogStats(podNamespace, podName string, podUID types.UID, rootFsInfo *cadvisorapi.FsInfo) (*statsapi.FsStats, error) {
	metricsByPath, err := h.podLogMetrics(podNamespace, podName, podUID)
	if err != nil {
		return nil, err
	}
	return metricsByPathToFsStats(metricsByPath, rootFsInfo)
}

// getPodContainerLogStats 获取容器的日志统计。
func (h hostStatsProvider) getPodContainerLogStats(podNamespace, podName string, podUID types.UID, containerName string, rootFsInfo *cadvisorapi.FsInfo) (*statsapi.FsStats, error) {
	metricsByPath, err := h.podContainerLogMetrics(podNamespace, podName, podUID, containerName)
	if err != nil {
		return nil, err
	}
	return metricsByPathToFsStats(metricsByPath, rootFsInfo)
}

// getPodEtcHostsStats 获取 Pod etc-hosts 文件的统计。
func (h hostStatsProvider) getPodEtcHostsStats(podUID types.UID, rootFsInfo *cadvisorapi.FsInfo) (*statsapi.FsStats, error) {
	// 某些运行时（Windows + Docker）不支持 etc-hosts 文件。
	podEtcHostsPath := h.podEtcHostsPathFunc(podUID)
	// 部分 Pod 显式挂载了 /etc/hosts，kubelet 不会为其创建 etc-hosts 文件；
	// 文件不存在时返回 nil 而非报错。
	if _, err := os.Stat(podEtcHostsPath); os.IsNotExist(err) {
		return nil, nil
	}

	metrics := volume.NewMetricsDu(podEtcHostsPath)
	hostMetrics, err := metrics.GetMetrics()
	if err != nil {
		return nil, fmt.Errorf("failed to get stats %v", err)
	}
	result := rootFsInfoToFsStats(rootFsInfo)
	usedBytes := uint64(hostMetrics.Used.Value())
	inodesUsed := uint64(hostMetrics.InodesUsed.Value())
	result.UsedBytes = addUsage(result.UsedBytes, &usedBytes)
	result.InodesUsed = addUsage(result.InodesUsed, &inodesUsed)
	result.Time = maxUpdateTime(&result.Time, &hostMetrics.Time)
	return result, nil
}

// podLogMetrics 返回 Pod 日志目录下各文件的指标 provider。
func (h hostStatsProvider) podLogMetrics(podNamespace, podName string, podUID types.UID) (metricsProviderByPath, error) {
	podLogsDirectoryPath := kuberuntime.BuildPodLogsDirectory(h.podLogsDirectory, podNamespace, podName, podUID)
	return h.fileMetricsByDir(podLogsDirectoryPath)
}

// podContainerLogMetrics 返回容器日志目录下各文件的指标 provider。
func (h hostStatsProvider) podContainerLogMetrics(podNamespace, podName string, podUID types.UID, containerName string) (metricsProviderByPath, error) {
	podContainerLogsDirectoryPath := kuberuntime.BuildContainerLogsDirectory(h.podLogsDirectory, podNamespace, podName, podUID, containerName)
	return h.fileMetricsByDir(podContainerLogsDirectoryPath)
}

// fileMetricsByDir 返回指定目录下每个文件的指标 provider。
func (h hostStatsProvider) fileMetricsByDir(dirname string) (metricsProviderByPath, error) {
	files, err := h.osInterface.ReadDir(dirname)
	if err != nil {
		return nil, err
	}
	results := metricsProviderByPath{}
	for _, f := range files {
		if f.IsDir() {
			continue
		}
		// 只统计目录下的普通文件，跳过子目录。
		fpath := filepath.Join(dirname, f.Name())
		results[fpath] = volume.NewMetricsDu(fpath)
	}
	return results, nil
}

// metricsByPathToFsStats 把按路径组织的指标汇总为 fs stats。
func metricsByPathToFsStats(metricsByPath metricsProviderByPath, rootFsInfo *cadvisorapi.FsInfo) (*statsapi.FsStats, error) {
	result := rootFsInfoToFsStats(rootFsInfo)
	for fpath, metrics := range metricsByPath {
		hostMetrics, err := metrics.GetMetrics()
		if err != nil {
			return nil, fmt.Errorf("failed to get fsstats for %q: %v", fpath, err)
		}
		usedBytes := uint64(hostMetrics.Used.Value())
		inodesUsed := uint64(hostMetrics.InodesUsed.Value())
		result.UsedBytes = addUsage(result.UsedBytes, &usedBytes)
		result.InodesUsed = addUsage(result.InodesUsed, &inodesUsed)
		result.Time = maxUpdateTime(&result.Time, &hostMetrics.Time)
	}
	return result, nil
}

// rootFsInfoToFsStats 把 cadvisor 的 rootFsInfo 转换为 statsapi.FsStats。
func rootFsInfoToFsStats(rootFsInfo *cadvisorapi.FsInfo) *statsapi.FsStats {
	return &statsapi.FsStats{
		Time:           metav1.NewTime(rootFsInfo.Timestamp),
		AvailableBytes: &rootFsInfo.Available,
		CapacityBytes:  &rootFsInfo.Capacity,
		InodesFree:     rootFsInfo.InodesFree,
		Inodes:         rootFsInfo.Inodes,
	}
}
