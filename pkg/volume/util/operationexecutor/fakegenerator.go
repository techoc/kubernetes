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

package operationexecutor

import (
	"time"

	"k8s.io/klog/v2"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/kubernetes/pkg/volume"
	"k8s.io/kubernetes/pkg/volume/util/hostutil"
	volumetypes "k8s.io/kubernetes/pkg/volume/util/types"
)

// fakeOGCounter 是一个简单的 OperationGenerator，用于统计各函数被调用的次数。
type fakeOGCounter struct {
	// calledFuncs 记录函数名及调用次数。
	calledFuncs map[string]int
	opFunc      func() volumetypes.OperationContext
}

var _ OperationGenerator = &fakeOGCounter{}

// NewFakeOGCounter 返回一个 OperationGenerator，用于记录函数调用。
func NewFakeOGCounter(opFunc func() volumetypes.OperationContext) OperationGenerator {
	return &fakeOGCounter{
		calledFuncs: map[string]int{},
		opFunc:      opFunc,
	}
}

// GenerateMountVolumeFunc 记录 GenerateMountVolumeFunc 的调用。
func (f *fakeOGCounter) GenerateMountVolumeFunc(waitForAttachTimeout time.Duration, volumeToMount VolumeToMount, actualStateOfWorldMounterUpdater ActualStateOfWorldMounterUpdater, isRemount bool) volumetypes.GeneratedOperations {
	return f.recordFuncCall("GenerateMountVolumeFunc")
}

// GenerateUnmountVolumeFunc 记录 GenerateUnmountVolumeFunc 的调用。
func (f *fakeOGCounter) GenerateUnmountVolumeFunc(volumeToUnmount MountedVolume, actualStateOfWorld ActualStateOfWorldMounterUpdater, podsDir string) (volumetypes.GeneratedOperations, error) {
	return f.recordFuncCall("GenerateUnmountVolumeFunc"), nil
}

// GenerateAttachVolumeFunc 记录 GenerateAttachVolumeFunc 的调用。
func (f *fakeOGCounter) GenerateAttachVolumeFunc(logger klog.Logger, volumeToAttach VolumeToAttach, actualStateOfWorld ActualStateOfWorldAttacherUpdater) volumetypes.GeneratedOperations {
	return f.recordFuncCall("GenerateAttachVolumeFunc")
}

// GenerateDetachVolumeFunc 记录 GenerateDetachVolumeFunc 的调用。
func (f *fakeOGCounter) GenerateDetachVolumeFunc(logger klog.Logger, volumeToDetach AttachedVolume, verifySafeToDetach bool, actualStateOfWorld ActualStateOfWorldAttacherUpdater) (volumetypes.GeneratedOperations, error) {
	return f.recordFuncCall("GenerateDetachVolumeFunc"), nil
}

// GenerateVolumesAreAttachedFunc 记录 GenerateVolumesAreAttachedFunc 的调用。
func (f *fakeOGCounter) GenerateVolumesAreAttachedFunc(attachedVolumes []AttachedVolume, nodeName types.NodeName, actualStateOfWorld ActualStateOfWorldAttacherUpdater) (volumetypes.GeneratedOperations, error) {
	return f.recordFuncCall("GenerateVolumesAreAttachedFunc"), nil
}

// GenerateUnmountDeviceFunc 记录 GenerateUnmountDeviceFunc 的调用。
func (f *fakeOGCounter) GenerateUnmountDeviceFunc(deviceToDetach AttachedVolume, actualStateOfWorld ActualStateOfWorldMounterUpdater, hu hostutil.HostUtils) (volumetypes.GeneratedOperations, error) {
	return f.recordFuncCall("GenerateUnmountDeviceFunc"), nil
}

// GenerateVerifyControllerAttachedVolumeFunc 记录 GenerateVerifyControllerAttachedVolumeFunc 的调用。
func (f *fakeOGCounter) GenerateVerifyControllerAttachedVolumeFunc(logger klog.Logger, volumeToMount VolumeToMount, nodeName types.NodeName, actualStateOfWorld ActualStateOfWorldAttacherUpdater) (volumetypes.GeneratedOperations, error) {
	return f.recordFuncCall("GenerateVerifyControllerAttachedVolumeFunc"), nil
}

// GenerateMapVolumeFunc 记录 GenerateMapVolumeFunc 的调用。
func (f *fakeOGCounter) GenerateMapVolumeFunc(waitForAttachTimeout time.Duration, volumeToMount VolumeToMount, actualStateOfWorldMounterUpdater ActualStateOfWorldMounterUpdater) (volumetypes.GeneratedOperations, error) {
	return f.recordFuncCall("GenerateMapVolumeFunc"), nil
}

// GenerateUnmapVolumeFunc 记录 GenerateUnmapVolumeFunc 的调用。
func (f *fakeOGCounter) GenerateUnmapVolumeFunc(volumeToUnmount MountedVolume, actualStateOfWorld ActualStateOfWorldMounterUpdater) (volumetypes.GeneratedOperations, error) {
	return f.recordFuncCall("GenerateUnmapVolumeFunc"), nil
}

// GenerateUnmapDeviceFunc 记录 GenerateUnmapDeviceFunc 的调用。
func (f *fakeOGCounter) GenerateUnmapDeviceFunc(deviceToDetach AttachedVolume, actualStateOfWorld ActualStateOfWorldMounterUpdater, hu hostutil.HostUtils) (volumetypes.GeneratedOperations, error) {
	return f.recordFuncCall("GenerateUnmapDeviceFunc"), nil
}

// GetVolumePluginMgr 返回 nil；该 fake 不依赖插件管理器。
func (f *fakeOGCounter) GetVolumePluginMgr() *volume.VolumePluginMgr {
	return nil
}

// GenerateExpandVolumeFunc 记录 GenerateExpandVolumeFunc 的调用。
func (f *fakeOGCounter) GenerateExpandVolumeFunc(*v1.PersistentVolumeClaim, *v1.PersistentVolume) (volumetypes.GeneratedOperations, error) {
	return f.recordFuncCall("GenerateExpandVolumeFunc"), nil
}

// GenerateExpandAndRecoverVolumeFunc 记录扩容及恢复调用的复用计数（沿用 ExpandVolumeFunc 名称）。
func (f *fakeOGCounter) GenerateExpandAndRecoverVolumeFunc(*v1.PersistentVolumeClaim, *v1.PersistentVolume, string) (volumetypes.GeneratedOperations, error) {
	return f.recordFuncCall("GenerateExpandVolumeFunc"), nil
}

// GenerateExpandInUseVolumeFunc 记录 GenerateExpandInUseVolumeFunc 的调用。
func (f *fakeOGCounter) GenerateExpandInUseVolumeFunc(volumeToMount VolumeToMount, actualStateOfWorld ActualStateOfWorldMounterUpdater, currentSize resource.Quantity) (volumetypes.GeneratedOperations, error) {
	return f.recordFuncCall("GenerateExpandInUseVolumeFunc"), nil
}

// recordFuncCall 累加指定函数的调用次数，并返回以其命名的 GeneratedOperations。
func (f *fakeOGCounter) recordFuncCall(name string) volumetypes.GeneratedOperations {
	if _, ok := f.calledFuncs[name]; ok {
		f.calledFuncs[name]++
	}
	ops := volumetypes.GeneratedOperations{
		OperationName: name,
		OperationFunc: f.opFunc,
	}
	return ops
}
