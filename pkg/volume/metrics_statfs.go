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

package volume

import (
	"time"

	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	servermetrics "k8s.io/kubernetes/pkg/kubelet/server/metrics"
	"k8s.io/kubernetes/pkg/volume/util/fs"
)

// 编译时断言 metricsStatFS 实现 MetricsProvider 接口。
var _ MetricsProvider = &metricsStatFS{}

// metricsStatFS 通过 statfs 系统调用收集卷路径的文件系统信息，
// 计算卷的已用空间和可用空间。
type metricsStatFS struct {
	// 卷挂载到的目录路径。
	path string
}

// NewMetricsStatFS 以卷路径创建一个 metricsStatFS 指标提供者。
func NewMetricsStatFS(path string) MetricsProvider {
	return &metricsStatFS{path}
}

// GetMetrics 计算卷使用量和设备剩余空间。
// 通过 fs.Info 获取卷路径的文件系统统计信息（容量、已用、可用及 inode 数据）。
// 指标采集耗时由 servermetrics 记录，便于观测 statfs 调用开销。
// 参见 MetricsProvider.GetMetrics。
func (md *metricsStatFS) GetMetrics() (*Metrics, error) {
	startTime := time.Now()
	defer servermetrics.CollectVolumeStatCalDuration("statfs", startTime)

	metrics := &Metrics{Time: metav1.Now()}
	if md.path == "" {
		return metrics, NewNoPathDefinedError()
	}

	err := md.getFsInfo(metrics)
	if err != nil {
		return metrics, err
	}

	return metrics, nil
}

// getFsInfo 从文件系统信息填充 metrics 的容量、已用、可用及 inode 字段。
// 容量统一按 BinarySI（KiB/MiB 等）计量，与 kubectl 展示单位保持一致。
func (md *metricsStatFS) getFsInfo(metrics *Metrics) error {
	available, capacity, usage, inodes, inodesFree, inodesUsed, err := fs.Info(md.path)
	if err != nil {
		return NewFsInfoFailedError(err)
	}
	metrics.Available = resource.NewQuantity(available, resource.BinarySI)
	metrics.Capacity = resource.NewQuantity(capacity, resource.BinarySI)
	metrics.Used = resource.NewQuantity(usage, resource.BinarySI)
	metrics.Inodes = resource.NewQuantity(inodes, resource.BinarySI)
	metrics.InodesFree = resource.NewQuantity(inodesFree, resource.BinarySI)
	metrics.InodesUsed = resource.NewQuantity(inodesUsed, resource.BinarySI)
	return nil
}
