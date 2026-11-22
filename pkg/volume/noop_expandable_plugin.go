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

package volume

import (
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/types"
)

// noopExpandableVolumePluginInstance 是一个空实现的可扩容插件，仅用于占位。
type noopExpandableVolumePluginInstance struct {
	spec *Spec
}

var _ ExpandableVolumePlugin = &noopExpandableVolumePluginInstance{}

// ExpandVolumeDevice 直接返回目标大小，不做任何设备层操作。
func (n *noopExpandableVolumePluginInstance) ExpandVolumeDevice(spec *Spec, newSize resource.Quantity, oldSize resource.Quantity) (resource.Quantity, error) {
	return newSize, nil
}

// Init 无需初始化，直接返回成功。
func (n *noopExpandableVolumePluginInstance) Init(host VolumeHost) error {
	return nil
}

// GetPluginName 返回卷规格声明的插件名称。
func (n *noopExpandableVolumePluginInstance) GetPluginName() string {
	return n.spec.KubeletExpandablePluginName()
}

// GetVolumeName 返回卷规格的名称。
func (n *noopExpandableVolumePluginInstance) GetVolumeName(spec *Spec) (string, error) {
	return n.spec.Name(), nil
}

// CanSupport 始终返回 true：该占位插件可接受任意卷规格。
func (n *noopExpandableVolumePluginInstance) CanSupport(spec *Spec) bool {
	return true
}

// RequiresRemount 始终返回 false：无需重挂载。
func (n *noopExpandableVolumePluginInstance) RequiresRemount(spec *Spec) bool {
	return false
}

// NewMounter 返回空的 Mounter：该插件不提供实际挂载能力。
func (n *noopExpandableVolumePluginInstance) NewMounter(spec *Spec, podRef *v1.Pod) (Mounter, error) {
	return nil, nil
}

// NewUnmounter 返回空的 Unmounter：该插件不提供实际卸载能力。
func (n *noopExpandableVolumePluginInstance) NewUnmounter(name string, podUID types.UID) (Unmounter, error) {
	return nil, nil
}

// ConstructVolumeSpec 返回携带原始卷规格的重建结果。
func (n *noopExpandableVolumePluginInstance) ConstructVolumeSpec(volumeName, mountPath string) (ReconstructedVolume, error) {
	return ReconstructedVolume{Spec: n.spec}, nil
}

// SupportsMountOption 返回 true，声明支持挂载选项。
func (n *noopExpandableVolumePluginInstance) SupportsMountOption() bool {
	return true
}

// RequiresFSResize 返回 true，要求执行文件系统扩容。
func (n *noopExpandableVolumePluginInstance) RequiresFSResize() bool {
	return true
}

// SupportsSELinuxContextMount 返回 false：不支持 SELinux 上下文挂载。
func (n *noopExpandableVolumePluginInstance) SupportsSELinuxContextMount(spec *Spec) (bool, error) {
	return false, nil
}
