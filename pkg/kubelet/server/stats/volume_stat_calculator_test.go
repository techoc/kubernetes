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
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	k8sv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	kubestats "k8s.io/kubelet/pkg/apis/stats/v1alpha1"
	statstest "k8s.io/kubernetes/pkg/kubelet/server/stats/testing"
	"k8s.io/kubernetes/pkg/volume"
	"k8s.io/kubernetes/test/utils/ktesting"
)

const (
	// 测试使用的固定命名空间、Pod 名、容量与卷名。
	namespace0  = "test0"
	pName0      = "pod0"
	capacity    = int64(10000000)
	available   = int64(5000000)
	inodesTotal = int64(2000)
	inodesFree  = int64(1000)

	vol0          = "vol0"
	vol1          = "vol1"
	vol2          = "vol2"
	vol3          = "vol3"
	pvcClaimName0 = "pvc-fake0"
	pvcClaimName1 = "pvc-fake1"
)

var (
	// ErrorWatchTimeout 表示等待卷事件超时。
	ErrorWatchTimeout = errors.New("watch event timeout")
	// podVolumes 与 fakePod 是测试使用的 Pod 卷配置。
	podVolumes = []k8sv1.Volume{
		{
			Name: vol0,
			VolumeSource: k8sv1.VolumeSource{
				GCEPersistentDisk: &k8sv1.GCEPersistentDiskVolumeSource{
					PDName: "fake-device1",
				},
			},
		},
		{
			Name: vol1,
			VolumeSource: k8sv1.VolumeSource{
				PersistentVolumeClaim: &k8sv1.PersistentVolumeClaimVolumeSource{
					ClaimName: pvcClaimName0,
				},
			},
		},
		{
			Name: vol2,
			VolumeSource: k8sv1.VolumeSource{
				PersistentVolumeClaim: &k8sv1.PersistentVolumeClaimVolumeSource{
					ClaimName: pvcClaimName1,
				},
			},
		},
		{
			Name: vol3,
			VolumeSource: k8sv1.VolumeSource{
				Ephemeral: &k8sv1.EphemeralVolumeSource{},
			},
		},
	}

	fakePod = &k8sv1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      pName0,
			Namespace: namespace0,
			UID:       "UID" + pName0,
		},
		Spec: k8sv1.PodSpec{
			Volumes: podVolumes,
		},
	}
)

// TestPVCRef 验证卷统计计算能为各类卷正确关联 PVC 引用。
func TestPVCRef(t *testing.T) {
	ktesting.Init(t).SyncTest("", testPVCRef)
}

// testPVCRef 是 TestPVCRef 的实际测试逻辑。
func testPVCRef(tCtx ktesting.TContext) {
	t := tCtx.TB()
	logger := tCtx.Logger()
	// 设置 mock 统计提供者，声明 Pod 的普通卷与块设备卷。
	mockStats := statstest.NewMockProvider(t)
	volumes := map[string]volume.Volume{vol0: &fakeVolume{}, vol1: &fakeVolume{}, vol3: &fakeVolume{}}
	mockStats.EXPECT().ListVolumesForPod(fakePod.UID).Return(volumes, true)
	blockVolumes := map[string]volume.BlockVolume{vol2: &fakeBlockVolume{}}
	mockStats.EXPECT().ListBlockVolumesForPod(fakePod.UID).Return(blockVolumes, true)

	// 使用带缓冲的通道收集假事件记录器产生的事件。
	eventStore := make(chan string, 1)
	fakeEventRecorder := record.FakeRecorder{
		Events: eventStore,
	}

	// 计算 Pod 的卷统计。
	statsCalculator := newVolumeStatCalculator(mockStats, time.Minute, fakePod, &fakeEventRecorder)
	statsCalculator.calcAndStoreStats(logger)
	vs, _ := statsCalculator.GetLatest()

	// 普通卷、块设备卷与临时卷合计 4 个。
	assert.Len(t, append(vs.EphemeralVolumes, vs.PersistentVolumes...), 4)
	// 校验 vol0 没有 PVC 引用。
	assert.Contains(t, append(vs.EphemeralVolumes, vs.PersistentVolumes...), kubestats.VolumeStats{
		Name:    vol0,
		FsStats: expectedFSStats(),
	})
	// 校验 vol1 关联了 PVC 引用。
	assert.Contains(t, append(vs.EphemeralVolumes, vs.PersistentVolumes...), kubestats.VolumeStats{
		Name: vol1,
		PVCRef: &kubestats.PVCReference{
			Name:      pvcClaimName0,
			Namespace: namespace0,
		},
		FsStats: expectedFSStats(),
	})
	// 校验 vol2（块设备卷）关联了 PVC 引用。
	assert.Contains(t, append(vs.EphemeralVolumes, vs.PersistentVolumes...), kubestats.VolumeStats{
		Name: vol2,
		PVCRef: &kubestats.PVCReference{
			Name:      pvcClaimName1,
			Namespace: namespace0,
		},
		FsStats: expectedBlockStats(),
	})
	// 校验 vol3（临时卷）使用自动生成的 PVC 名称。
	assert.Contains(t, append(vs.EphemeralVolumes, vs.PersistentVolumes...), kubestats.VolumeStats{
		Name: vol3,
		PVCRef: &kubestats.PVCReference{
			Name:      pName0 + "-" + vol3,
			Namespace: namespace0,
		},
		FsStats: expectedFSStats(),
	})
}

// TestNormalVolumeEvent 验证正常情况下不会产生卷事件。
func TestNormalVolumeEvent(t *testing.T) {
	ktesting.Init(t).SyncTest("", testNormalVolumeEvent)
}

// testNormalVolumeEvent 是 TestNormalVolumeEvent 的实际测试逻辑。
func testNormalVolumeEvent(tCtx ktesting.TContext) {
	t := tCtx.TB()
	logger := tCtx.Logger()
	// 设置 mock 统计提供者，声明 Pod 的普通卷与块设备卷。
	mockStats := statstest.NewMockProvider(t)

	volumes := map[string]volume.Volume{vol0: &fakeVolume{}, vol1: &fakeVolume{}}
	mockStats.EXPECT().ListVolumesForPod(fakePod.UID).Return(volumes, true)
	blockVolumes := map[string]volume.BlockVolume{vol2: &fakeBlockVolume{}}
	mockStats.EXPECT().ListBlockVolumesForPod(fakePod.UID).Return(blockVolumes, true)

	eventStore := make(chan string, 2)
	fakeEventRecorder := record.FakeRecorder{
		Events: eventStore,
	}

	// Calculate stats for pod
	statsCalculator := newVolumeStatCalculator(mockStats, time.Minute, fakePod, &fakeEventRecorder)
	statsCalculator.calcAndStoreStats(logger)

	// 正常情况下不应有事件产生，等待应当超时。
	event, err := WatchEvent(eventStore)
	assert.Error(t, err)
	assert.Equal(t, "", event)
}

// WatchEvent 等待一个事件字符串，超时返回 ErrorWatchTimeout。
func WatchEvent(eventChan <-chan string) (string, error) {
	select {
	case event := <-eventChan:
		return event, nil
	// 最多等待 5 秒，避免测试永久阻塞。
	case <-time.After(5 * time.Second):
		return "", ErrorWatchTimeout
	}
}

// 假卷/指标提供者实现。
var _ volume.Volume = &fakeVolume{}

// fakeVolume 是只返回固定指标的普通卷实现。
type fakeVolume struct{}

// GetPath 返回空路径，测试中不使用。
func (v *fakeVolume) GetPath() string { return "" }

// GetMetrics 返回固定的普通卷指标。
func (v *fakeVolume) GetMetrics() (*volume.Metrics, error) {
	return expectedMetrics(), nil
}

// expectedMetrics 构造测试期望的普通卷指标。
func expectedMetrics() *volume.Metrics {
	vMetrics := &volume.Metrics{
		Available:  resource.NewQuantity(available, resource.BinarySI),
		Capacity:   resource.NewQuantity(capacity, resource.BinarySI),
		Used:       resource.NewQuantity(available-capacity, resource.BinarySI),
		Inodes:     resource.NewQuantity(inodesTotal, resource.BinarySI),
		InodesFree: resource.NewQuantity(inodesFree, resource.BinarySI),
		InodesUsed: resource.NewQuantity(inodesTotal-inodesFree, resource.BinarySI),
	}

	return vMetrics
}

// expectedFSStats 将期望指标转换为文件系统统计结构。
func expectedFSStats() kubestats.FsStats {
	metric := expectedMetrics()
	available := uint64(metric.Available.Value())
	capacity := uint64(metric.Capacity.Value())
	used := uint64(metric.Used.Value())
	inodes := uint64(metric.Inodes.Value())
	inodesFree := uint64(metric.InodesFree.Value())
	inodesUsed := uint64(metric.InodesUsed.Value())
	return kubestats.FsStats{
		AvailableBytes: &available,
		CapacityBytes:  &capacity,
		UsedBytes:      &used,
		Inodes:         &inodes,
		InodesFree:     &inodesFree,
		InodesUsed:     &inodesUsed,
	}
}

// 假块设备卷/指标提供者；块设备没有 inode 概念。
var _ volume.BlockVolume = &fakeBlockVolume{}

// fakeBlockVolume 是只返回固定指标的块设备卷实现。
type fakeBlockVolume struct{}

// GetGlobalMapPath 返回空路径，测试中不使用。
func (v *fakeBlockVolume) GetGlobalMapPath(*volume.Spec) (string, error) { return "", nil }

// GetPodDeviceMapPath 返回空路径，测试中不使用。
func (v *fakeBlockVolume) GetPodDeviceMapPath() (string, string) { return "", "" }

// SupportsMetrics 表示该假块设备卷支持指标采集。
func (v *fakeBlockVolume) SupportsMetrics() bool { return true }

// GetMetrics 返回固定的块设备卷指标。
func (v *fakeBlockVolume) GetMetrics() (*volume.Metrics, error) {
	return expectedBlockMetrics(), nil
}

// expectedBlockMetrics 构造测试期望的块设备卷指标。
func expectedBlockMetrics() *volume.Metrics {
	vMetrics := &volume.Metrics{
		Available: resource.NewQuantity(available, resource.BinarySI),
		Capacity:  resource.NewQuantity(capacity, resource.BinarySI),
		Used:      resource.NewQuantity(available-capacity, resource.BinarySI),
	}

	return vMetrics
}

// expectedBlockStats 将块设备指标转换为文件系统统计结构，inode 字段全为零。
func expectedBlockStats() kubestats.FsStats {
	metric := expectedBlockMetrics()
	available := uint64(metric.Available.Value())
	capacity := uint64(metric.Capacity.Value())
	used := uint64(metric.Used.Value())
	null := uint64(0)
	return kubestats.FsStats{
		AvailableBytes: &available,
		CapacityBytes:  &capacity,
		UsedBytes:      &used,
		Inodes:         &null,
		InodesFree:     &null,
		InodesUsed:     &null,
	}
}
