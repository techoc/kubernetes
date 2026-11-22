/*
Copyright 2021 The Kubernetes Authors.

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
	"fmt"
	"io"
	"os"
	"runtime"
	"time"

	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	servermetrics "k8s.io/kubernetes/pkg/kubelet/server/metrics"
)

var _ MetricsProvider = &metricsBlock{}

// metricsBlock 通过块设备节点探测 BlockMode 卷大小的 MetricsProvider。
type metricsBlock struct {
	// 卷所挂载到的设备节点。
	device string
}

// NewMetricsBlock 使用卷的设备节点创建一个 metricsBlock。
func NewMetricsBlock(device string) MetricsProvider {
	return &metricsBlock{device}
}

// GetMetrics 探测卷所挂载设备节点上 BlockMode 卷的大小。
// 参见 MetricsProvider.GetMetrics。
//
// 标准工具只能探测设备容量；存储系统可能通过专有 API 提供更丰富的信息。
func (mb *metricsBlock) GetMetrics() (*Metrics, error) {
	startTime := time.Now()
	defer servermetrics.CollectVolumeStatCalDuration("block", startTime)

	// TODO: Windows does not yet support VolumeMode=Block
	if runtime.GOOS == "windows" {
		return nil, NewNotImplementedError("Windows does not support Block volumes")
	}

	metrics := &Metrics{Time: metav1.Now()}
	if mb.device == "" {
		return metrics, NewNoPathDefinedError()
	}

	err := mb.getBlockInfo(metrics)
	if err != nil {
		return metrics, err
	}

	return metrics, nil
}

// getBlockInfo 打开设备并定位到末尾，由此得到 metrics.Capacity。
func (mb *metricsBlock) getBlockInfo(metrics *Metrics) error {
	dev, err := os.Open(mb.device)
	if err != nil {
		return fmt.Errorf("unable to open device %q: %w", mb.device, err)
	}
	defer dev.Close()

	end, err := dev.Seek(0, io.SeekEnd)
	if err != nil {
		return fmt.Errorf("failed to detect size of %q: %w", mb.device, err)
	}

	metrics.Capacity = resource.NewQuantity(end, resource.BinarySI)

	return nil
}
