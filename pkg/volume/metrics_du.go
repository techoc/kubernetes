/*
Copyright 2014 The Kubernetes Authors.

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
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/kubernetes/pkg/volume/util/fs"
)

var _ MetricsProvider = &metricsDu{}

// metricsDu 通过 fs.DiskUsage() 及卷路径的文件系统信息计算卷已用与可用空间的 MetricsProvider。
type metricsDu struct {
	// 卷挂载到的目录路径。
	path string
}

// NewMetricsDu 使用卷路径创建一个 metricsDu。
func NewMetricsDu(path string) MetricsProvider {
	return &metricsDu{path}
}

// GetMetrics 通过执行 "du" 并收集卷路径的文件系统信息，计算卷用量与设备空闲空间。
// 参见 MetricsProvider.GetMetrics。
func (md *metricsDu) GetMetrics() (*Metrics, error) {
	metrics := &Metrics{Time: metav1.Now()}
	if md.path == "" {
		return metrics, NewNoPathDefinedError()
	}

	err := md.getDiskUsage(metrics)
	if err != nil {
		return metrics, err
	}

	err = md.getFsInfo(metrics)
	if err != nil {
		return metrics, err
	}

	return metrics, nil
}

// getDiskUsage 将 fs.DiskUsage 的结果写入 metrics.Used 与 metrics.InodesUsed。
func (md *metricsDu) getDiskUsage(metrics *Metrics) error {
	usage, err := fs.DiskUsage(md.path)
	if err != nil {
		return err
	}
	metrics.Used = resource.NewQuantity(usage.Bytes, resource.BinarySI)
	metrics.InodesUsed = resource.NewQuantity(usage.Inodes, resource.BinarySI)
	return nil
}

// getFsInfo 将文件系统信息写入 metrics.Capacity 与 metrics.Available。
func (md *metricsDu) getFsInfo(metrics *Metrics) error {
	available, capacity, _, inodes, inodesFree, _, err := fs.Info(md.path)
	if err != nil {
		return NewFsInfoFailedError(err)
	}
	metrics.Available = resource.NewQuantity(available, resource.BinarySI)
	metrics.Capacity = resource.NewQuantity(capacity, resource.BinarySI)
	metrics.Inodes = resource.NewQuantity(inodes, resource.BinarySI)
	metrics.InodesFree = resource.NewQuantity(inodesFree, resource.BinarySI)
	return nil
}
