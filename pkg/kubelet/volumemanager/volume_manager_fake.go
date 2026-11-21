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

package volumemanager

import (
	"context"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/kubernetes/pkg/kubelet/config"
	"k8s.io/kubernetes/pkg/kubelet/container"
	"k8s.io/kubernetes/pkg/volume/util/types"
)

// FakeVolumeManager 是仅记录调用的测试实现。
type FakeVolumeManager struct {
	volumes                   map[v1.UniqueVolumeName]bool
	reportedInUse             map[v1.UniqueVolumeName]bool
	unmountDelay              time.Duration
	unmountError              error
	volumeAttachLimitExceeded bool
}

var _ VolumeManager = &FakeVolumeManager{}

// NewFakeVolumeManager 创建 VolumeManager 的测试实例。
func NewFakeVolumeManager(initialVolumes []v1.UniqueVolumeName, unmountDelay time.Duration, unmountError error, volumeAttachLimitExceeded bool) *FakeVolumeManager {
	volumes := map[v1.UniqueVolumeName]bool{}
	for _, v := range initialVolumes {
		// 初始卷都视为已挂载。
		volumes[v] = true
	}
	return &FakeVolumeManager{
		volumes:                   volumes,
		reportedInUse:             map[v1.UniqueVolumeName]bool{},
		unmountDelay:              unmountDelay,
		unmountError:              unmountError,
		volumeAttachLimitExceeded: volumeAttachLimitExceeded,
	}
}

// Run 未实现。
func (f *FakeVolumeManager) Run(ctx context.Context, sourcesReady config.SourcesReady) {
}

// WaitForAttachAndMount 默认返回成功，可按需模拟挂载数超限错误。
func (f *FakeVolumeManager) WaitForAttachAndMount(ctx context.Context, pod *v1.Pod) error {
	// 模拟卷挂载数量超过限制的错误。
	if f.volumeAttachLimitExceeded {
		return &VolumeAttachLimitExceededError{}
	}
	return nil
}

// WaitForUnmount 未实现，直接返回成功。
func (f *FakeVolumeManager) WaitForUnmount(ctx context.Context, pod *v1.Pod) error {
	return nil
}

// WaitForAllPodsUnmount 按配置的延迟模拟卸载耗时，并尊重上下文取消。
func (f *FakeVolumeManager) WaitForAllPodsUnmount(ctx context.Context, pods []*v1.Pod) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(f.unmountDelay):
		return f.unmountError
	}
}

// GetMountedVolumesForPod 未实现，返回 nil。
func (f *FakeVolumeManager) GetMountedVolumesForPod(podName types.UniquePodName) container.VolumeMap {
	return nil
}

// HasPossiblyMountedVolumesForPod 未实现，始终返回 false。
func (f *FakeVolumeManager) HasPossiblyMountedVolumesForPod(podName types.UniquePodName) bool {
	return false
}

// GetExtraSupplementalGroupsForPod 未实现，返回 nil。
func (f *FakeVolumeManager) GetExtraSupplementalGroupsForPod(pod *v1.Pod) []int64 {
	return nil
}

// GetVolumesInUse 返回初始卷列表。
func (f *FakeVolumeManager) GetVolumesInUse() []v1.UniqueVolumeName {
	inuse := []v1.UniqueVolumeName{}
	for v := range f.volumes {
		inuse = append(inuse, v)
	}
	return inuse
}

// ReconcilerStatesHasBeenSynced 未实现，始终返回 true。
func (f *FakeVolumeManager) ReconcilerStatesHasBeenSynced() bool {
	return true
}

// VolumeIsAttached 未实现，始终返回 false。
func (f *FakeVolumeManager) VolumeIsAttached(volumeName v1.UniqueVolumeName) bool {
	return false
}

// MarkVolumesAsReportedInUse 把给定卷记入 reportedInUse。
func (f *FakeVolumeManager) MarkVolumesAsReportedInUse(volumesReportedAsInUse []v1.UniqueVolumeName) {
	for _, reportedVolume := range volumesReportedAsInUse {
		// 只记录确实由本管理器跟踪的卷。
		if _, ok := f.volumes[reportedVolume]; ok {
			f.reportedInUse[reportedVolume] = true
		}
	}
}

// GetVolumesReportedInUse 仅供测试，返回 reportedInUse 中的卷列表。
func (f *FakeVolumeManager) GetVolumesReportedInUse() []v1.UniqueVolumeName {
	inuse := []v1.UniqueVolumeName{}
	for reportedVolume := range f.reportedInUse {
		inuse = append(inuse, reportedVolume)
	}
	return inuse
}

// ResizeEphemeralVolume 未实现，直接返回成功。
func (f *FakeVolumeManager) ResizeEphemeralVolume(pod *v1.Pod, volumeName string, newSize *resource.Quantity) error {
	return nil
}
