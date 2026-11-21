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

package testing

import (
	"fmt"
	"sync"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/kubernetes/pkg/volume"
)

// TestPluginName 是测试插件注册到卷插件管理器时使用的唯一名称。
const TestPluginName = "kubernetes.io/testPlugin"

// TestPlugin 是一个最小化的卷插件假实现，可记录挂载/卸载调用供测试断言。
type TestPlugin struct {
	// SupportCSIVolume 控制是否声明支持 CSI 卷；只模拟能力声明，不模拟真实 CSI 操作。
	SupportCSIVolume bool
	// ErrorEncountered 记录是否出现非法调用，测试结束后可统一断言。
	ErrorEncountered  bool
	attachedVolumeMap map[string][]string
	detachedVolumeMap map[string][]string
	pluginLock        *sync.RWMutex
}

// Init 满足 VolumePlugin 接口；测试插件无需初始化。
func (plugin *TestPlugin) Init(host volume.VolumeHost) error {
	return nil
}

// GetPluginName 返回测试插件的固定名称。
func (plugin *TestPlugin) GetPluginName() string {
	return TestPluginName
}

// GetVolumeName 根据 spec 类型提取卷名；遇到未知来源时记录错误并返回错误。
func (plugin *TestPlugin) GetVolumeName(spec *volume.Spec) (string, error) {
	plugin.pluginLock.Lock()
	defer plugin.pluginLock.Unlock()
	if spec == nil {
		plugin.ErrorEncountered = true
		return "", fmt.Errorf("GetVolumeName called with nil volume spec")
	}
	if spec.Volume != nil {
		return spec.Name(), nil
	} else if spec.PersistentVolume != nil {
		if spec.PersistentVolume.Spec.PersistentVolumeSource.GCEPersistentDisk != nil {
			return spec.PersistentVolume.Spec.PersistentVolumeSource.GCEPersistentDisk.PDName, nil
		} else if spec.PersistentVolume.Spec.PersistentVolumeSource.NFS != nil {
			return spec.PersistentVolume.Spec.PersistentVolumeSource.NFS.Server, nil
		} else if spec.PersistentVolume.Spec.PersistentVolumeSource.RBD != nil {
			return spec.PersistentVolume.Spec.PersistentVolumeSource.RBD.RBDImage, nil
		} else if spec.PersistentVolume.Spec.PersistentVolumeSource.CSI != nil {
			csi := spec.PersistentVolume.Spec.PersistentVolumeSource.CSI
			return fmt.Sprintf("%s^%s", csi.Driver, csi.VolumeHandle), nil
		}
		return "", fmt.Errorf("GetVolumeName called with unexpected PersistentVolume: %v", spec)
	} else {
		return "", nil
	}
}

// CanSupport 报告插件是否支持给定卷；CSI 卷的支持与否由 SupportCSIVolume 开关决定。
func (plugin *TestPlugin) CanSupport(spec *volume.Spec) bool {
	plugin.pluginLock.Lock()
	defer plugin.pluginLock.Unlock()
	if spec == nil {
		plugin.ErrorEncountered = true
	} else {
		if spec.Volume != nil && spec.Volume.CSI != nil {
			return plugin.SupportCSIVolume
		}
		if spec.PersistentVolume != nil && spec.PersistentVolume.Spec.CSI != nil {
			return plugin.SupportCSIVolume
		}
	}
	return true
}

// RequiresRemount 表示测试插件从不要求重挂。
func (plugin *TestPlugin) RequiresRemount(spec *volume.Spec) bool {
	return false
}

// NewMounter 返回空挂载器；传入 nil spec 时记录错误，以便测试验证调用方参数。
func (plugin *TestPlugin) NewMounter(spec *volume.Spec, podRef *v1.Pod) (volume.Mounter, error) {
	plugin.pluginLock.Lock()
	defer plugin.pluginLock.Unlock()
	if spec == nil {
		plugin.ErrorEncountered = true
	}
	return nil, nil
}

// NewUnmounter 返回空卸载器。
func (plugin *TestPlugin) NewUnmounter(name string, podUID types.UID) (volume.Unmounter, error) {
	return nil, nil
}

// VerifyExhaustedResource 表示测试插件不检查资源耗尽。
func (plugin *TestPlugin) VerifyExhaustedResource(spec *volume.Spec) bool {
	return false
}

// ConstructVolumeSpec 构造一个带有 GCE PD 来源的重建卷 spec，供挂载路径相关测试使用。
func (plugin *TestPlugin) ConstructVolumeSpec(volumeName, mountPath string) (volume.ReconstructedVolume, error) {
	fakeVolume := &v1.Volume{
		Name: volumeName,
		VolumeSource: v1.VolumeSource{
			GCEPersistentDisk: &v1.GCEPersistentDiskVolumeSource{
				PDName:   "pdName",
				FSType:   "ext4",
				ReadOnly: false,
			},
		},
	}
	return volume.ReconstructedVolume{
		Spec: volume.NewSpecFromVolume(fakeVolume),
	}, nil
}

// NewAttacher 创建共享插件状态的 attacher，使挂载记录与错误标志对测试可见。
func (plugin *TestPlugin) NewAttacher() (volume.Attacher, error) {
	attacher := testPluginAttacher{
		ErrorEncountered:  &plugin.ErrorEncountered,
		attachedVolumeMap: plugin.attachedVolumeMap,
		pluginLock:        plugin.pluginLock,
	}
	return &attacher, nil
}

// NewDeviceMounter 复用 attacher 实现设备挂载接口。
func (plugin *TestPlugin) NewDeviceMounter() (volume.DeviceMounter, error) {
	return plugin.NewAttacher()
}

// NewDetacher 创建共享插件状态的 detacher。
func (plugin *TestPlugin) NewDetacher() (volume.Detacher, error) {
	detacher := testPluginDetacher{
		detachedVolumeMap: plugin.detachedVolumeMap,
		pluginLock:        plugin.pluginLock,
	}
	return &detacher, nil
}

// CanAttach 表示测试插件始终允许挂载。
func (plugin *TestPlugin) CanAttach(spec *volume.Spec) (bool, error) {
	return true, nil
}

// CanDeviceMount 表示测试插件始终允许设备挂载。
func (plugin *TestPlugin) CanDeviceMount(spec *volume.Spec) (bool, error) {
	return true, nil
}

// NewDeviceUnmounter 复用 detacher 实现设备卸载接口。
func (plugin *TestPlugin) NewDeviceUnmounter() (volume.DeviceUnmounter, error) {
	return plugin.NewDetacher()
}

// GetDeviceMountRefs 返回空引用列表，模拟无其他引用者。
func (plugin *TestPlugin) GetDeviceMountRefs(deviceMountPath string) ([]string, error) {
	return []string{}, nil
}

// SupportsMountOption 表示测试插件不支持挂载选项。
func (plugin *TestPlugin) SupportsMountOption() bool {
	return false
}

// SupportsBulkVolumeVerification 表示不支持批量卷校验。
func (plugin *TestPlugin) SupportsBulkVolumeVerification() bool {
	return false
}

// SupportsSELinuxContextMount 表示不支持 SELinux 上下文挂载。
func (plugin *TestPlugin) SupportsSELinuxContextMount(spec *volume.Spec) (bool, error) {
	return false, nil
}

// GetErrorEncountered 返回测试期间是否发生过非法调用；读取受锁保护。
func (plugin *TestPlugin) GetErrorEncountered() bool {
	plugin.pluginLock.RLock()
	defer plugin.pluginLock.RUnlock()
	return plugin.ErrorEncountered
}

// GetAttachedVolumes 返回已挂载卷的快照；复制切片避免调用方修改内部状态。
func (plugin *TestPlugin) GetAttachedVolumes() map[string][]string {
	plugin.pluginLock.RLock()
	defer plugin.pluginLock.RUnlock()
	ret := make(map[string][]string)
	for nodeName, volumeList := range plugin.attachedVolumeMap {
		ret[nodeName] = make([]string, len(volumeList))
		copy(ret[nodeName], volumeList)
	}
	return ret
}

// GetDetachedVolumes 返回已卸载卷的快照；复制切片避免数据竞争。
func (plugin *TestPlugin) GetDetachedVolumes() map[string][]string {
	plugin.pluginLock.RLock()
	defer plugin.pluginLock.RUnlock()
	ret := make(map[string][]string)
	for nodeName, volumeList := range plugin.detachedVolumeMap {
		ret[nodeName] = make([]string, len(volumeList))
		copy(ret[nodeName], volumeList)
	}
	return ret
}

// CreateTestPlugin 创建一个配置好 CSI 支持开关的测试插件实例。
func CreateTestPlugin(supportCSIVolume bool) []volume.VolumePlugin {
	// 多个方法共享这些 map 和锁，因此必须在构造时初始化，不能留零值。
	attachedVolumes := make(map[string][]string)
	detachedVolumes := make(map[string][]string)
	return []volume.VolumePlugin{&TestPlugin{
		SupportCSIVolume:  supportCSIVolume,
		ErrorEncountered:  false,
		attachedVolumeMap: attachedVolumes,
		detachedVolumeMap: detachedVolumes,
		pluginLock:        &sync.RWMutex{},
	}}
}

// testPluginAttacher 记录挂载调用并通过指针共享插件的错误标志。
type testPluginAttacher struct {
	ErrorEncountered  *bool
	attachedVolumeMap map[string][]string
	pluginLock        *sync.RWMutex
}

// Attach 记录一次挂载调用；nil spec 会被标记以便测试断言。
func (attacher *testPluginAttacher) Attach(spec *volume.Spec, nodeName types.NodeName) (string, error) {
	attacher.pluginLock.Lock()
	defer attacher.pluginLock.Unlock()
	if spec == nil {
		*attacher.ErrorEncountered = true
		return "", fmt.Errorf("Attach called with nil volume spec")
	}
	attacher.attachedVolumeMap[string(nodeName)] = append(attacher.attachedVolumeMap[string(nodeName)], spec.Name())
	return spec.Name(), nil
}

// VolumesAreAttached 返回 nil，表示测试中不进行实际挂载状态校验。
func (attacher *testPluginAttacher) VolumesAreAttached(specs []*volume.Spec, nodeName types.NodeName) (map[*volume.Spec]bool, error) {
	return nil, nil
}

// WaitForAttach 返回一个由设备路径和卷名拼接的假设备路径。
func (attacher *testPluginAttacher) WaitForAttach(spec *volume.Spec, devicePath string, pod *v1.Pod, timeout time.Duration) (string, error) {
	attacher.pluginLock.Lock()
	defer attacher.pluginLock.Unlock()
	if spec == nil {
		*attacher.ErrorEncountered = true
		return "", fmt.Errorf("WaitForAttach called with nil volume spec")
	}
	fakePath := fmt.Sprintf("%s/%s", devicePath, spec.Name())
	return fakePath, nil
}

// GetDeviceMountPath 返回空路径；测试只关心调用是否发生。
func (attacher *testPluginAttacher) GetDeviceMountPath(spec *volume.Spec) (string, error) {
	attacher.pluginLock.Lock()
	defer attacher.pluginLock.Unlock()
	if spec == nil {
		*attacher.ErrorEncountered = true
		return "", fmt.Errorf("GetDeviceMountPath called with nil volume spec")
	}
	return "", nil
}

// MountDevice 记录设备挂载调用，nil spec 时标记错误。
func (attacher *testPluginAttacher) MountDevice(spec *volume.Spec, devicePath string, deviceMountPath string, _ volume.DeviceMounterArgs) error {
	attacher.pluginLock.Lock()
	defer attacher.pluginLock.Unlock()
	if spec == nil {
		*attacher.ErrorEncountered = true
		return fmt.Errorf("MountDevice called with nil volume spec")
	}
	return nil
}

// testPluginDetacher 记录卸载调用，供测试验证控制器发出的卸载操作。
type testPluginDetacher struct {
	detachedVolumeMap map[string][]string
	pluginLock        *sync.RWMutex
}

// Detach 记录一次卸载调用。
func (detacher *testPluginDetacher) Detach(volumeName string, nodeName types.NodeName) error {
	detacher.pluginLock.Lock()
	defer detacher.pluginLock.Unlock()
	detacher.detachedVolumeMap[string(nodeName)] = append(detacher.detachedVolumeMap[string(nodeName)], volumeName)
	return nil
}

// UnmountDevice 为空实现。
func (detacher *testPluginDetacher) UnmountDevice(deviceMountPath string) error {
	return nil
}
