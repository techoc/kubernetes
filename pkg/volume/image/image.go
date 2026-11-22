/*
Copyright 2024 The Kubernetes Authors.

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

package image

import (
	"fmt"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/kubernetes/pkg/volume"
)

// imagePlugin 是镜像卷插件的桩实现，仅提供卷管理器所需的最小功能。
// 真正的卷源实现位于 kubelet 代码中，受 ImageVolume 特性开关控制。
// 参见：https://kep.k8s.io/4639
type imagePlugin struct {
	volume.MetricsNil
}

var _ volume.VolumePlugin = &imagePlugin{}
var _ volume.Mounter = &imagePlugin{}
var _ volume.Unmounter = &imagePlugin{}
var _ volume.Volume = &imagePlugin{}

const pluginName = "kubernetes.io/image"

// ProbeVolumePlugins 返回镜像卷插件实例列表。
func ProbeVolumePlugins() []volume.VolumePlugin {
	p := &imagePlugin{}
	return []volume.VolumePlugin{p}
}

// Init 无需初始化，直接返回成功。
func (o *imagePlugin) Init(volume.VolumeHost) error { return nil }

// GetPluginName 返回插件名称。
func (o *imagePlugin) GetPluginName() string { return pluginName }

// GetVolumeName 返回卷规格中引用的镜像名称。
func (o *imagePlugin) GetVolumeName(spec *volume.Spec) (string, error) {
	volumeSource := getVolumeSource(spec)
	if volumeSource == nil {
		return "", fmt.Errorf("the volumeSpec does not reference an Image volume type")
	}

	return volumeSource.Reference, nil
}

// CanSupport 判断卷规格是否引用了镜像卷源。
func (o *imagePlugin) CanSupport(spec *volume.Spec) bool {
	return spec != nil && spec.Volume != nil && spec.Volume.Image != nil
}

// NewMounter 返回插件自身作为 Mounter。
func (o *imagePlugin) NewMounter(spec *volume.Spec, podRef *v1.Pod) (volume.Mounter, error) {
	return o, nil
}

// NewUnmounter 返回插件自身作为 Unmounter。
func (o *imagePlugin) NewUnmounter(name string, podUID types.UID) (volume.Unmounter, error) {
	return o, nil
}

// ConstructVolumeSpec 返回空的重建结果：镜像卷无需从挂载点重建规格。
func (o *imagePlugin) ConstructVolumeSpec(volumeName, mountPath string) (volume.ReconstructedVolume, error) {
	return volume.ReconstructedVolume{}, nil
}

// GetAttributes 返回卷属性：镜像卷只读、不受 kubelet 管理、不重打 SELinux 标签。
func (o *imagePlugin) GetAttributes() volume.Attributes {
	return volume.Attributes{
		ReadOnly:       true,
		Managed:        false,
		SELinuxRelabel: false,
	}
}

// GetPath 返回空路径：实际的镜像挂载由 kubelet 处理。
func (o *imagePlugin) GetPath() string { return "" }

// RequiresFSResize 返回 false：镜像卷无需文件系统扩容。
func (o *imagePlugin) RequiresFSResize() bool { return false }

// RequiresRemount 返回 false：镜像卷无需重挂载。
func (o *imagePlugin) RequiresRemount(spec *volume.Spec) bool { return false }

// SetUp 是空操作，实际的卷准备由 kubelet 完成。
func (o *imagePlugin) SetUp(mounterArgs volume.MounterArgs) error { return nil }

// SetUpAt 是空操作，实际的卷准备由 kubelet 完成。
func (o *imagePlugin) SetUpAt(dir string, mounterArgs volume.MounterArgs) error { return nil }

// SupportsMountOption 返回 false：镜像卷不支持挂载选项。
func (o *imagePlugin) SupportsMountOption() bool { return false }

// SupportsSELinuxContextMount 返回 false：不支持 SELinux 上下文挂载。
func (o *imagePlugin) SupportsSELinuxContextMount(spec *volume.Spec) (bool, error) { return false, nil }

// TearDown 是空操作，实际的卸载由 kubelet 完成。
func (o *imagePlugin) TearDown() error { return nil }

// TearDownAt 是空操作，实际的卸载由 kubelet 完成。
func (o *imagePlugin) TearDownAt(string) error { return nil }

// getVolumeSource 从卷规格中提取镜像卷源，若不是镜像卷则返回 nil。
func getVolumeSource(spec *volume.Spec) *v1.ImageVolumeSource {
	if spec == nil || spec.Volume == nil {
		return nil
	}
	return spec.Volume.Image
}
