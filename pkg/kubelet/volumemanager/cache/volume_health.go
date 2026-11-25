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

package cache

import (
	v1 "k8s.io/api/core/v1"
	"k8s.io/kubernetes/pkg/volume"
	"k8s.io/kubernetes/pkg/volume/csi"
	"k8s.io/kubernetes/pkg/volume/util/operationexecutor"
	volumetypes "k8s.io/kubernetes/pkg/volume/util/types"
)

// VolumeToHealthCheck 描述一个应进行健康探测的卷。
type VolumeToHealthCheck struct {
	// VolumeName 是卷在 kubelet 内的唯一名称。
	VolumeName v1.UniqueVolumeName
	// PodName 是使用该卷的 Pod 唯一名称。
	PodName volumetypes.UniquePodName
	// Pod 是使用该卷的 Pod 对象。
	Pod             *v1.Pod
	OuterVolumeName string // Pod.Spec.Volumes[].Name
	// DriverName 是 CSI 驱动名称。
	DriverName string
	// CSIVolumeHandle 是 CSI 卷句柄。
	CSIVolumeHandle string
	// StagingPath 是卷的 staging 路径。
	StagingPath string
	// PublishPath 是卷在 Pod 内的挂载路径。
	PublishPath string
	// VolumeSpec 是卷的规格。
	VolumeSpec *volume.Spec
}

// GetVolumesToReportHealth 返回 DSW 中已尝试挂载的 CSI 卷
// （或在 ASW 中已挂载/状态不确定，同样意味着已尝试）。若挂载从未成功，路径可能为空。
func GetVolumesToReportHealth(dsw DesiredStateOfWorld, asw ActualStateOfWorld) []VolumeToHealthCheck {
	volumesToMount := dsw.GetVolumesToMount()
	result := make([]VolumeToHealthCheck, 0, len(volumesToMount))

	for _, vtm := range volumesToMount {
		// 只有 CSI 卷支持卷健康探测。
		if !isCSIVolumeSpec(vtm.VolumeSpec) {
			continue
		}

		// 从未尝试挂载的卷没有可报告的健康信息。
		attempted := asw.IsVolumeMountAttempted(vtm.PodName, vtm.VolumeName)
		mountState := asw.GetVolumeMountState(vtm.VolumeName, vtm.PodName)
		if !attempted && mountState == operationexecutor.VolumeNotMounted {
			continue
		}

		// 取不到驱动名的卷无法上报健康状态，跳过。
		driverName, err := csi.GetCSIDriverName(vtm.VolumeSpec)
		if err != nil {
			continue
		}

		handle, ok := csiVolumeHandle(vtm.VolumeSpec)
		if !ok {
			// 暂无 PV 句柄的临时 CSI 卷暂时跳过。
			continue
		}

		// 取 Pod 中第一个卷名作为对外展示的名称。
		outerVolumeName := ""
		if len(vtm.OuterVolumeSpecNames) > 0 {
			outerVolumeName = vtm.OuterVolumeSpecNames[0]
		}

		stagingPath, publishPath := volumeHealthPaths(asw, vtm.PodName, vtm.VolumeName)

		result = append(result, VolumeToHealthCheck{
			VolumeName:      vtm.VolumeName,
			PodName:         vtm.PodName,
			Pod:             vtm.Pod,
			OuterVolumeName: outerVolumeName,
			DriverName:      driverName,
			CSIVolumeHandle: handle,
			StagingPath:     stagingPath,
			PublishPath:     publishPath,
			VolumeSpec:      vtm.VolumeSpec,
		})
	}

	return result
}

// isCSIVolumeSpec 判断卷规格是否对应 CSI 卷（PV 或内联卷）。
func isCSIVolumeSpec(spec *volume.Spec) bool {
	if spec == nil {
		return false
	}
	if spec.PersistentVolume != nil && spec.PersistentVolume.Spec.CSI != nil {
		return true
	}
	if spec.Volume != nil && spec.Volume.CSI != nil {
		return true
	}
	return false
}

// csiVolumeHandle 返回 PV 对应的 CSI 卷句柄。
func csiVolumeHandle(spec *volume.Spec) (string, bool) {
	if spec == nil {
		return "", false
	}
	if spec.PersistentVolume != nil && spec.PersistentVolume.Spec.CSI != nil {
		handle := spec.PersistentVolume.Spec.CSI.VolumeHandle
		if handle == "" {
			return "", false
		}
		return handle, true
	}
	// 临时 CSI 卷的句柄在挂载时才生成，暂时跳过。
	return "", false
}

// volumeHealthPaths 从 ASW 中查找卷的 staging 与 publish 路径。
func volumeHealthPaths(asw ActualStateOfWorld, podName volumetypes.UniquePodName, volumeName v1.UniqueVolumeName) (stagingPath, publishPath string) {
	for _, mv := range asw.GetPossiblyMountedVolumesForPod(podName) {
		if mv.VolumeName != volumeName {
			continue
		}
		stagingPath = mv.DeviceMountPath
		// 只有已挂载的卷才有 Pod 内的发布路径。
		if mv.Mounter != nil {
			publishPath = mv.Mounter.GetPath()
		}
		return stagingPath, publishPath
	}
	// 未挂载但已 attach 的卷仍可提供 staging 路径。
	if attached, ok := asw.GetAttachedVolume(volumeName); ok {
		stagingPath = attached.DeviceMountPath
	}
	return stagingPath, publishPath
}
