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

package flexvolume

import (
	"fmt"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"

	api "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/volume"
	"k8s.io/kubernetes/pkg/volume/util"
	"k8s.io/mount-utils"
	"k8s.io/utils/exec"
	utilstrings "k8s.io/utils/strings"
)

const (
	flexVolumePluginName = "kubernetes.io/flexvolume"
)

// flexVolumePlugin 表示一个 flexvolume 驱动插件实例。
type flexVolumePlugin struct {
	driverName string
	execPath   string
	host       volume.VolumeHost
	runner     exec.Interface

	sync.Mutex
	unsupportedCommands []string
	capabilities        DriverCapabilities
}

// flexVolumeAttachablePlugin 包装支持 attach/detach 的 flexvolume 插件。
type flexVolumeAttachablePlugin struct {
	*flexVolumePlugin
}

var _ volume.AttachableVolumePlugin = &flexVolumeAttachablePlugin{}
var _ volume.PersistentVolumePlugin = &flexVolumePlugin{}
var _ volume.NodeExpandableVolumePlugin = &flexVolumePlugin{}
var _ volume.ExpandableVolumePlugin = &flexVolumePlugin{}

var _ volume.DeviceMountableVolumePlugin = &flexVolumeAttachablePlugin{}

// PluginFactory 创建 flexvolume 插件。
type PluginFactory interface {
	NewFlexVolumePlugin(pluginDir, driverName string, runner exec.Interface) (volume.VolumePlugin, error)
}

// pluginFactory 是 PluginFactory 的默认实现。
type pluginFactory struct{}

// NewFlexVolumePlugin 创建 flexvolume 插件，并调用驱动的 init 命令探测其能力。
func (pluginFactory) NewFlexVolumePlugin(pluginDir, name string, runner exec.Interface) (volume.VolumePlugin, error) {
	execPath := filepath.Join(pluginDir, name)

	driverName := utilstrings.UnescapeQualifiedName(name)

	flexPlugin := &flexVolumePlugin{
		driverName:          driverName,
		execPath:            execPath,
		runner:              runner,
		unsupportedCommands: []string{},
	}

	// 初始化插件并探测其能力。
	call := flexPlugin.NewDriverCall(initCmd)
	ds, err := call.Run()
	if err != nil {
		return nil, err
	}
	flexPlugin.capabilities = *ds.Capabilities

	if flexPlugin.capabilities.Attach {
		// 插件支持 attach/detach，返回可附加插件类型。
		return &flexVolumeAttachablePlugin{flexVolumePlugin: flexPlugin}, nil
	}
	return flexPlugin, nil
}

// Init 实现 volume.VolumePlugin 接口，记录 VolumeHost。
func (plugin *flexVolumePlugin) Init(host volume.VolumeHost) error {
	plugin.host = host
	// 直接返回成功：init() 调用的错误已由 NewFlexVolumePlugin() 处理。
	return nil
}

// VerifyExhaustedResource 返回是否存在资源耗尽的情况；flexvolume 不做检查。
func (plugin *flexVolumePlugin) VerifyExhaustedResource(spec *volume.Spec) bool {
	return false
}

// getExecutable 返回驱动可执行文件的路径。
func (plugin *flexVolumePlugin) getExecutable() string {
	parts := strings.Split(plugin.driverName, "/")
	execName := parts[len(parts)-1]
	execPath := filepath.Join(plugin.execPath, execName)
	if runtime.GOOS == "windows" {
		execPath = util.GetWindowsPath(execPath)
	}
	return execPath
}

// GetPluginName 实现 volume.VolumePlugin 接口，返回插件名称。
func (plugin *flexVolumePlugin) GetPluginName() string {
	return plugin.driverName
}

// GetVolumeName 实现 volume.VolumePlugin 接口，返回卷名称。
func (plugin *flexVolumePlugin) GetVolumeName(spec *volume.Spec) (string, error) {
	call := plugin.NewDriverCall(getVolumeNameCmd)
	call.AppendSpec(spec, plugin.host, nil)

	_, err := call.Run()
	if isCmdNotSupportedErr(err) {
		return (*pluginDefaults)(plugin).GetVolumeName(spec)
	} else if err != nil {
		return "", err
	}

	name, err := (*pluginDefaults)(plugin).GetVolumeName(spec)
	if err != nil {
		return "", err
	}

	klog.V(4).Info(logPrefix(plugin), "GetVolumeName is not supported yet. Defaulting to PV or volume name: ", name)

	return name, nil
}

// CanSupport 实现 volume.VolumePlugin 接口，判断插件是否支持给定卷规格。
func (plugin *flexVolumePlugin) CanSupport(spec *volume.Spec) bool {
	sourceDriver, err := getDriver(spec)
	if err != nil {
		return false
	}
	return sourceDriver == plugin.driverName
}

// RequiresRemount 实现 volume.VolumePlugin 接口；flexvolume 不需要周期性重挂。
func (plugin *flexVolumePlugin) RequiresRemount(spec *volume.Spec) bool {
	return false
}

// GetAccessModes 返回该插件允许的 access mode。
func (plugin *flexVolumePlugin) GetAccessModes() []api.PersistentVolumeAccessMode {
	return []api.PersistentVolumeAccessMode{
		api.ReadWriteOnce,
		api.ReadOnlyMany,
	}
}

// NewMounter 实现 volume.VolumePlugin 接口，为指定卷和 Pod 创建挂载器。
func (plugin *flexVolumePlugin) NewMounter(spec *volume.Spec, pod *api.Pod) (volume.Mounter, error) {
	return plugin.newMounterInternal(spec, pod, plugin.host.GetMounter(), plugin.runner)
}

// newMounterInternal 是构造挂载器的内部实现。
func (plugin *flexVolumePlugin) newMounterInternal(spec *volume.Spec, pod *api.Pod, mounter mount.Interface, runner exec.Interface) (volume.Mounter, error) {
	sourceDriver, err := getDriver(spec)
	if err != nil {
		return nil, err
	}

	readOnly, err := getReadOnly(spec)
	if err != nil {
		return nil, err
	}

	var metricsProvider volume.MetricsProvider
	if plugin.capabilities.SupportsMetrics {
		metricsProvider = volume.NewMetricsStatFS(plugin.host.GetPodVolumeDir(
			pod.UID, utilstrings.EscapeQualifiedName(sourceDriver), spec.Name()))
	} else {
		metricsProvider = &volume.MetricsNil{}
	}

	return &flexVolumeMounter{
		flexVolume: &flexVolume{
			driverName:            sourceDriver,
			execPath:              plugin.getExecutable(),
			mounter:               mounter,
			plugin:                plugin,
			podName:               pod.Name,
			podUID:                pod.UID,
			podNamespace:          pod.Namespace,
			podServiceAccountName: pod.Spec.ServiceAccountName,
			volName:               spec.Name(),
			MetricsProvider:       metricsProvider,
		},
		runner:   runner,
		spec:     spec,
		readOnly: readOnly,
	}, nil
}

// NewUnmounter 实现 volume.VolumePlugin 接口，为指定卷创建卸载器。
func (plugin *flexVolumePlugin) NewUnmounter(volName string, podUID types.UID) (volume.Unmounter, error) {
	return plugin.newUnmounterInternal(volName, podUID, plugin.host.GetMounter(), plugin.runner)
}

// newUnmounterInternal 是构造卸载器的内部实现。
func (plugin *flexVolumePlugin) newUnmounterInternal(volName string, podUID types.UID, mounter mount.Interface, runner exec.Interface) (volume.Unmounter, error) {
	var metricsProvider volume.MetricsProvider
	if plugin.capabilities.SupportsMetrics {
		metricsProvider = volume.NewMetricsStatFS(plugin.host.GetPodVolumeDir(
			podUID, utilstrings.EscapeQualifiedName(plugin.driverName), volName))
	} else {
		metricsProvider = &volume.MetricsNil{}
	}

	return &flexVolumeUnmounter{
		flexVolume: &flexVolume{
			driverName:      plugin.driverName,
			execPath:        plugin.getExecutable(),
			mounter:         mounter,
			plugin:          plugin,
			podUID:          podUID,
			volName:         volName,
			MetricsProvider: metricsProvider,
		},
		runner: runner,
	}, nil
}

// NewAttacher 实现 volume.AttachableVolumePlugin 接口，创建附加器。
func (plugin *flexVolumeAttachablePlugin) NewAttacher() (volume.Attacher, error) {
	return &flexVolumeAttacher{plugin}, nil
}

// NewDeviceMounter 创建设备挂载器；flexvolume 直接复用附加器。
func (plugin *flexVolumeAttachablePlugin) NewDeviceMounter() (volume.DeviceMounter, error) {
	return plugin.NewAttacher()
}

// NewDetacher 实现 volume.AttachableVolumePlugin 接口，创建分离器。
func (plugin *flexVolumeAttachablePlugin) NewDetacher() (volume.Detacher, error) {
	return &flexVolumeDetacher{plugin}, nil
}

// NewDeviceUnmounter 创建设备卸载器；flexvolume 直接复用分离器。
func (plugin *flexVolumeAttachablePlugin) NewDeviceUnmounter() (volume.DeviceUnmounter, error) {
	return plugin.NewDetacher()
}

// CanAttach 判断是否可以附加指定卷；flexvolume 始终返回 true。
func (plugin *flexVolumeAttachablePlugin) CanAttach(spec *volume.Spec) (bool, error) {
	return true, nil
}

// CanDeviceMount 判断是否可以挂载指定设备；flexvolume 始终返回 true。
func (plugin *flexVolumeAttachablePlugin) CanDeviceMount(spec *volume.Spec) (bool, error) {
	return true, nil
}

// ConstructVolumeSpec 实现 volume.AttachableVolumePlugin 接口，从卷名和挂载路径重建卷规格。
func (plugin *flexVolumePlugin) ConstructVolumeSpec(volumeName, mountPath string) (volume.ReconstructedVolume, error) {
	flexVolume := &api.Volume{
		Name: volumeName,
		VolumeSource: api.VolumeSource{
			FlexVolume: &api.FlexVolumeSource{
				Driver: plugin.driverName,
			},
		},
	}
	return volume.ReconstructedVolume{
		Spec: volume.NewSpecFromVolume(flexVolume),
	}, nil
}

// SupportsMountOption 判断插件是否支持传入 mount 选项；flexvolume 不支持。
func (plugin *flexVolumePlugin) SupportsMountOption() bool {
	return false
}

// unsupported 将给定命令记录为不支持，避免后续重复调用。
func (plugin *flexVolumePlugin) unsupported(commands ...string) {
	plugin.Lock()
	defer plugin.Unlock()
	plugin.unsupportedCommands = append(plugin.unsupportedCommands, commands...)
}

// SupportsSELinuxContextMount 判断插件是否支持在挂载时设置 SELinux context。
func (plugin *flexVolumePlugin) SupportsSELinuxContextMount(spec *volume.Spec) (bool, error) {
	return false, nil
}

// isUnsupported 判断给定命令是否已知不受支持。
func (plugin *flexVolumePlugin) isUnsupported(command string) bool {
	plugin.Lock()
	defer plugin.Unlock()
	return slices.Contains(plugin.unsupportedCommands, command)
}

// GetDeviceMountRefs 返回设备挂载路径的所有挂载引用。
func (plugin *flexVolumePlugin) GetDeviceMountRefs(deviceMountPath string) ([]string, error) {
	mounter := plugin.host.GetMounter()
	return mounter.GetMountRefs(deviceMountPath)
}

// getDeviceMountPath 返回该卷在插件目录下的设备挂载路径。
func (plugin *flexVolumePlugin) getDeviceMountPath(spec *volume.Spec) (string, error) {
	volumeName, err := plugin.GetVolumeName(spec)
	if err != nil {
		return "", fmt.Errorf("GetVolumeName failed from getDeviceMountPath: %s", err)
	}

	mountsDir := filepath.Join(plugin.host.GetPluginDir(flexVolumePluginName), plugin.driverName, "mounts")
	return filepath.Join(mountsDir, volumeName), nil
}

// RequiresFSResize 返回插件是否需要由 kubelet 执行文件系统扩容。
func (plugin *flexVolumePlugin) RequiresFSResize() bool {
	return plugin.capabilities.RequiresFSResize
}
