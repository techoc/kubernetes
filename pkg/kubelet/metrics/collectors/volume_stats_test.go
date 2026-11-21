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

package collectors

import (
	"context"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/component-base/metrics/testutil"
	statsapi "k8s.io/kubelet/pkg/apis/stats/v1alpha1"
	statstest "k8s.io/kubernetes/pkg/kubelet/server/stats/testing"
	"k8s.io/utils/ptr"
)

// TestVolumeStatsCollector 验证卷统计收集器把 Pod 卷指标导出为 Prometheus 指标，
// 并按 PVC 聚合。
func TestVolumeStatsCollector(t *testing.T) {
	ctx := context.TODO()
	// 固定的指标类型与帮助文本；把它拼接到每个期望输出前，
	// 调整时只需修改这一处。
	const metadata = `
		# HELP kubelet_volume_stats_available_bytes [ALPHA] Number of available bytes in the volume
		# TYPE kubelet_volume_stats_available_bytes gauge
		# HELP kubelet_volume_stats_capacity_bytes [ALPHA] Capacity in bytes of the volume
		# TYPE kubelet_volume_stats_capacity_bytes gauge
		# HELP kubelet_volume_stats_inodes [ALPHA] Maximum number of inodes in the volume
		# TYPE kubelet_volume_stats_inodes gauge
		# HELP kubelet_volume_stats_inodes_free [ALPHA] Number of free inodes in the volume
		# TYPE kubelet_volume_stats_inodes_free gauge
		# HELP kubelet_volume_stats_inodes_used [ALPHA] Number of used inodes in the volume
		# TYPE kubelet_volume_stats_inodes_used gauge
		# HELP kubelet_volume_stats_used_bytes [ALPHA] Number of used bytes in the volume
		# TYPE kubelet_volume_stats_used_bytes gauge
	`

	var (
		// 两个 Pod 引用同一个 PVC，用于验证指标按 PVC 去重聚合。
		podStats = []statsapi.PodStats{
			{
				PodRef:    statsapi.PodReference{Name: "test-pod", Namespace: "test-namespace", UID: "UID_test-pod"},
				StartTime: metav1.Now(),
				VolumeStats: []statsapi.VolumeStats{
					{
						FsStats: statsapi.FsStats{
							Time:           metav1.Now(),
							AvailableBytes: ptr.To[uint64](5.663154176e+09),
							CapacityBytes:  ptr.To[uint64](1.0434699264e+10),
							UsedBytes:      ptr.To[uint64](4.21789696e+09),
							InodesFree:     ptr.To[uint64](655344),
							Inodes:         ptr.To[uint64](655360),
							InodesUsed:     ptr.To[uint64](16),
						},
						Name:   "test",
						PVCRef: nil,
					},
					{
						FsStats: statsapi.FsStats{
							Time:           metav1.Now(),
							AvailableBytes: ptr.To[uint64](5.663154176e+09),
							CapacityBytes:  ptr.To[uint64](1.0434699264e+10),
							UsedBytes:      ptr.To[uint64](4.21789696e+09),
							InodesFree:     ptr.To[uint64](655344),
							Inodes:         ptr.To[uint64](655360),
							InodesUsed:     ptr.To[uint64](16),
						},
						Name: "test",
						PVCRef: &statsapi.PVCReference{
							Name:      "testpvc",
							Namespace: "testns",
						},
					},
				},
			},
			{
				// 另一个 Pod 引用同一个 PVC（test-namespace/testpvc）。
				PodRef:    statsapi.PodReference{Name: "test-pod-2", Namespace: "test-namespace", UID: "UID_test-pod"},
				StartTime: metav1.Now(),
				VolumeStats: []statsapi.VolumeStats{
					{
						FsStats: statsapi.FsStats{
							Time:           metav1.Now(),
							AvailableBytes: ptr.To[uint64](5.663154176e+09),
							CapacityBytes:  ptr.To[uint64](1.0434699264e+10),
							UsedBytes:      ptr.To[uint64](4.21789696e+09),
							InodesFree:     ptr.To[uint64](655344),
							Inodes:         ptr.To[uint64](655360),
							InodesUsed:     ptr.To[uint64](16),
						},
						Name: "test",
						PVCRef: &statsapi.PVCReference{
							Name:      "testpvc",
							Namespace: "testns",
						},
					},
				},
			},
		}

		want = metadata + `
			kubelet_volume_stats_available_bytes{namespace="testns",persistentvolumeclaim="testpvc"} 5.663154176e+09
			kubelet_volume_stats_capacity_bytes{namespace="testns",persistentvolumeclaim="testpvc"} 1.0434699264e+10
			kubelet_volume_stats_inodes{namespace="testns",persistentvolumeclaim="testpvc"} 655360
			kubelet_volume_stats_inodes_free{namespace="testns",persistentvolumeclaim="testpvc"} 655344
			kubelet_volume_stats_inodes_used{namespace="testns",persistentvolumeclaim="testpvc"} 16
			kubelet_volume_stats_used_bytes{namespace="testns",persistentvolumeclaim="testpvc"} 4.21789696e+09
			`

		metrics = []string{
			"kubelet_volume_stats_available_bytes",
			"kubelet_volume_stats_capacity_bytes",
			"kubelet_volume_stats_inodes",
			"kubelet_volume_stats_inodes_free",
			"kubelet_volume_stats_inodes_used",
			"kubelet_volume_stats_used_bytes",
		}
	)

	// 使用 mock 统计提供者，允许任意调用。
	mockStatsProvider := statstest.NewMockProvider(t)

	mockStatsProvider.EXPECT().ListPodStats(ctx).Return(podStats, nil).Maybe()
	mockStatsProvider.EXPECT().ListPodStatsAndUpdateCPUNanoCoreUsage(ctx).Return(podStats, nil).Maybe()
	// 采集并比较输出，确保指标内容与期望一致。
	if err := testutil.CustomCollectAndCompare(&volumeStatsCollector{statsProvider: mockStatsProvider}, strings.NewReader(want), metrics...); err != nil {
		t.Errorf("unexpected collecting result:\n%s", err)
	}
}

// TestVolumeStatsCollectorWithNullVolumeStatus 验证存在无 PVC 引用的卷时，
// 收集器只导出有 PVC 的卷指标。
func TestVolumeStatsCollectorWithNullVolumeStatus(t *testing.T) {
	ctx := context.TODO()
	// 固定的指标类型与帮助文本；把它拼接到每个期望输出前，
	// 调整时只需修改这一处。
	const metadata = `
		# HELP kubelet_volume_stats_available_bytes [ALPHA] Number of available bytes in the volume
		# TYPE kubelet_volume_stats_available_bytes gauge
		# HELP kubelet_volume_stats_capacity_bytes [ALPHA] Capacity in bytes of the volume
		# TYPE kubelet_volume_stats_capacity_bytes gauge
		# HELP kubelet_volume_stats_inodes [ALPHA] Maximum number of inodes in the volume
		# TYPE kubelet_volume_stats_inodes gauge
		# HELP kubelet_volume_stats_inodes_free [ALPHA] Number of free inodes in the volume
		# TYPE kubelet_volume_stats_inodes_free gauge
		# HELP kubelet_volume_stats_inodes_used [ALPHA] Number of used inodes in the volume
		# TYPE kubelet_volume_stats_inodes_used gauge
		# HELP kubelet_volume_stats_used_bytes [ALPHA] Number of used bytes in the volume
		# TYPE kubelet_volume_stats_used_bytes gauge
	`

	var (
		// 只有一个 Pod，其中一个卷没有 PVC 引用。
		podStats = []statsapi.PodStats{
			{
				PodRef:    statsapi.PodReference{Name: "test-pod", Namespace: "test-namespace", UID: "UID_test-pod"},
				StartTime: metav1.Now(),
				VolumeStats: []statsapi.VolumeStats{
					{
						FsStats: statsapi.FsStats{
							Time:           metav1.Now(),
							AvailableBytes: ptr.To[uint64](5.663154176e+09),
							CapacityBytes:  ptr.To[uint64](1.0434699264e+10),
							UsedBytes:      ptr.To[uint64](4.21789696e+09),
							InodesFree:     ptr.To[uint64](655344),
							Inodes:         ptr.To[uint64](655360),
							InodesUsed:     ptr.To[uint64](16),
						},
						Name:   "test",
						PVCRef: nil,
					},
					{
						FsStats: statsapi.FsStats{
							Time:           metav1.Now(),
							AvailableBytes: ptr.To[uint64](5.663154176e+09),
							CapacityBytes:  ptr.To[uint64](1.0434699264e+10),
							UsedBytes:      ptr.To[uint64](4.21789696e+09),
							InodesFree:     ptr.To[uint64](655344),
							Inodes:         ptr.To[uint64](655360),
							InodesUsed:     ptr.To[uint64](16),
						},
						Name: "test",
						PVCRef: &statsapi.PVCReference{
							Name:      "testpvc",
							Namespace: "testns",
						},
					},
				},
			},
		}

		want = metadata + `
			kubelet_volume_stats_available_bytes{namespace="testns",persistentvolumeclaim="testpvc"} 5.663154176e+09
			kubelet_volume_stats_capacity_bytes{namespace="testns",persistentvolumeclaim="testpvc"} 1.0434699264e+10
			kubelet_volume_stats_inodes{namespace="testns",persistentvolumeclaim="testpvc"} 655360
			kubelet_volume_stats_inodes_free{namespace="testns",persistentvolumeclaim="testpvc"} 655344
			kubelet_volume_stats_inodes_used{namespace="testns",persistentvolumeclaim="testpvc"} 16
			kubelet_volume_stats_used_bytes{namespace="testns",persistentvolumeclaim="testpvc"} 4.21789696e+09
			`

		metrics = []string{
			"kubelet_volume_stats_available_bytes",
			"kubelet_volume_stats_capacity_bytes",
			"kubelet_volume_stats_inodes",
			"kubelet_volume_stats_inodes_free",
			"kubelet_volume_stats_inodes_used",
			"kubelet_volume_stats_used_bytes",
		}
	)

	// 使用 mock 统计提供者。
	mockStatsProvider := statstest.NewMockProvider(t)

	mockStatsProvider.EXPECT().ListPodStats(ctx).Return(podStats, nil).Maybe()
	mockStatsProvider.EXPECT().ListPodStatsAndUpdateCPUNanoCoreUsage(ctx).Return(podStats, nil).Maybe()
	// 采集并比较输出，确保只导出有 PVC 引用的卷指标。
	if err := testutil.CustomCollectAndCompare(&volumeStatsCollector{statsProvider: mockStatsProvider}, strings.NewReader(want), metrics...); err != nil {
		t.Errorf("unexpected collecting result:\n%s", err)
	}
}
