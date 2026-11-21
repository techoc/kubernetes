/*
Copyright The Kubernetes Authors.

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

package volumehealth

import (
	"sync"

	"k8s.io/component-base/metrics"
	"k8s.io/component-base/metrics/legacyregistry"
)

var (
	registerMetrics sync.Once

	csiNodeStorageHealthStatus = metrics.NewGaugeVec(
		&metrics.GaugeOpts{
			Name:           "csi_node_storage_health_status",
			Help:           "CSI node storage backend health conditions currently reported (1 = present).",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"driver_name", "status", "reason"},
	)
)

// registerHealthMetrics 确保 CSI 存储健康指标只注册一次。
func registerHealthMetrics() {
	registerMetrics.Do(func() {
		legacyregistry.MustRegister(csiNodeStorageHealthStatus)
	})
}

// setStorageHealthGauges 先清除该 driver 的旧序列，再为当前存在的健康条件置 1。
func setStorageHealthGauges(driverName string, conditions []storageHealthKey) {
	// 调用方传入当前完整的条件集合：先删除该 driver 的旧序列，
	// 再仅把存在的条件置为 1。恢复情况通过下一次更新中条件缺失来体现。
	csiNodeStorageHealthStatus.DeletePartialMatch(map[string]string{"driver_name": driverName})
	for _, c := range conditions {
		csiNodeStorageHealthStatus.WithLabelValues(driverName, c.status, c.reason).Set(1)
	}
}

// storageHealthKey 描述一条存储健康条件（状态 + 原因）。
type storageHealthKey struct {
	status string
	reason string
}
