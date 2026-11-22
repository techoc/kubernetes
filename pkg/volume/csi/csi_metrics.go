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

package csi

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/grpc"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	servermetrics "k8s.io/kubernetes/pkg/kubelet/server/metrics"
	"k8s.io/kubernetes/pkg/volume"
	volumeutil "k8s.io/kubernetes/pkg/volume/util"
	volumetypes "k8s.io/kubernetes/pkg/volume/util/types"
)

var _ volume.MetricsProvider = &metricsCsi{}

// metricsCsi 实现 MetricsProvider，通过卷路径计算卷的已用空间、空闲空间和容量信息。
type metricsCsi struct {
	// 卷挂载到的目录路径。
	targetPath string

	// 卷句柄或 ID。
	volumeID string

	// 带缓存的 csiClient。
	csiClientGetter
}

// NewMetricsCsi 根据卷 ID、路径和驱动名称创建一个新的 metricsCsi。
func NewMetricsCsi(volumeID string, targetPath string, driverName csiDriverName) volume.MetricsProvider {
	mc := &metricsCsi{volumeID: volumeID, targetPath: targetPath}
	mc.csiClientGetter.driverName = driverName
	return mc
}

// GetMetrics 通过 CSI 的 NodeGetVolumeStats 获取卷的使用量、容量等指标。
func (mc *metricsCsi) GetMetrics() (*volume.Metrics, error) {
	startTime := time.Now()
	defer servermetrics.CollectVolumeStatCalDuration(string(mc.csiClientGetter.driverName), startTime)
	currentTime := metav1.Now()
	ctx, cancel := context.WithTimeout(context.Background(), csiTimeout)
	defer cancel()
	// 获取 CSI 客户端。
	csiClient, err := mc.csiClientGetter.Get()
	if err != nil {
		// CSI 驱动暂不可用按瞬时错误处理。
		// 参见 https://github.com/kubernetes/kubernetes/issues/120268
		return nil, volumetypes.NewTransientOperationFailure(err.Error())
	}
	// 检查是否设置了 "GET_VOLUME_STATS" 能力。
	volumeStatsSet, err := csiClient.NodeSupportsVolumeStats(ctx)
	if err != nil {
		return nil, err
	}

	// 插件不支持卷统计时直接返回。
	if !volumeStatsSet {
		return nil, volume.NewNotSupportedErrorWithDriverName(
			string(mc.csiClientGetter.driverName))
	}
	// 获取卷状态。
	metrics, err := csiClient.NodeGetVolumeStats(ctx, mc.volumeID, mc.targetPath)
	if err != nil {
		return nil, err
	}
	if metrics == nil {
		return nil, fmt.Errorf("csi.NodeGetVolumeStats returned nil metrics for volume %s", mc.volumeID)
	}
	// 记录采集时间。
	metrics.Time = currentTime
	return metrics, nil
}

// MetricsManager 管理 CSI 操作的指标记录。
type MetricsManager struct {
	driverName string
}

// NewCSIMetricsManager 创建一个 CSIMetricsManager 对象。
func NewCSIMetricsManager(driverName string) *MetricsManager {
	volumeutil.RegisterMetrics()
	cmm := MetricsManager{
		driverName: driverName,
	}
	return &cmm
}

type additionalInfo struct {
	Migrated string
}
type additionalInfoKeyType struct{}

var additionalInfoKey additionalInfoKeyType

// RecordMetricsInterceptor 是用于记录 CSI 操作的 gRPC 拦截器。
func (cmm *MetricsManager) RecordMetricsInterceptor(
	ctx context.Context,
	method string,
	req, reply interface{},
	cc *grpc.ClientConn,
	invoker grpc.UnaryInvoker,
	opts ...grpc.CallOption) error {
	start := time.Now()
	err := invoker(ctx, method, req, reply, cc, opts...)
	duration := time.Since(start)
	// 判断是否为 CSI 迁移操作。
	additionalInfoVal := ctx.Value(additionalInfoKey)
	migrated := "false"
	if additionalInfoVal != nil {
		additionalInfoVal, ok := additionalInfoVal.(additionalInfo)
		if !ok {
			return err
		}
		migrated = additionalInfoVal.Migrated
	}
	// 记录该操作的耗时指标。
	volumeutil.RecordCSIOperationLatencyMetrics(cmm.driverName, method, err, duration, migrated)

	return err
}
