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

package local

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	"k8s.io/klog/v2"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"k8s.io/kubernetes/pkg/kubelet/events"
	"k8s.io/kubernetes/pkg/volume"
	"k8s.io/kubernetes/pkg/volume/util"
	"k8s.io/kubernetes/pkg/volume/util/hostutil"
	"k8s.io/kubernetes/pkg/volume/validation"
	"k8s.io/mount-utils"
	utilexec "k8s.io/utils/exec"
	"k8s.io/utils/keymutex"
	utilstrings "k8s.io/utils/strings"
)

const (
	defaultFSType = "ext4"
)

// ProbeVolumePlugins 是卷插件的主要入口。
func ProbeVolumePlugins() []volume.VolumePlugin {
	return []volume.VolumePlugin{&localVolumePlugin{}}
}

// localVolumePlugin 实现 local 卷的卷插件接口。
type localVolumePlugin struct {
	host        volume.VolumeHost
	volumeLocks keymutex.KeyMutex
	recorder    record.EventRecorder
}

var _ volume.VolumePlugin = &localVolumePlugin{}
var _ volume.PersistentVolumePlugin = &localVolumePlugin{}
var _ volume.BlockVolumePlugin = &localVolumePlugin{}
var _ volume.NodeExpandableVolumePlugin = &localVolumePlugin{}

const (
	localVolumePluginName = "kubernetes.io/local-volume"
)

// Init 初始化插件，保存 VolumeHost、卷锁与事件记录器。
func (plugin *localVolumePlugin) Init(host volume.VolumeHost) error {
	plugin.host = host
	plugin.volumeLocks = keymutex.NewHashed(0)
	plugin.recorder = host.GetEventRecorder()
	return nil
}

// GetPluginName 返回插件名称。
func (plugin *localVolumePlugin) GetPluginName() string {
	return localVolumePluginName
}

// GetVolumeName 返回卷名；local 卷仅支持作为 PersistentVolumeSource，故 PV 名即唯一标识。
func (plugin *localVolumePlugin) GetVolumeName(spec *volume.Spec) (string, error) {
	// local 卷仅支持作为 PersistentVolumeSource，因此 PV 名称是唯一的。
	return spec.Name(), nil
}

// CanSupport 判断给定规格是否为 local PersistentVolume。
func (plugin *localVolumePlugin) CanSupport(spec *volume.Spec) bool {
	// local 卷仅支持作为 PersistentVolumeSource。
	return (spec.PersistentVolume != nil && spec.PersistentVolume.Spec.Local != nil)
}

// RequiresRemount 返回是否需要重挂载；local 卷不需要。
func (plugin *localVolumePlugin) RequiresRemount(spec *volume.Spec) bool {
	return false
}

// SupportsMountOption 返回是否支持挂载选项。
func (plugin *localVolumePlugin) SupportsMountOption() bool {
	return true
}

// SupportsSELinuxContextMount 返回是否支持 SELinux 上下文挂载；local 卷不支持。
func (plugin *localVolumePlugin) SupportsSELinuxContextMount(spec *volume.Spec) (bool, error) {
	return false, nil
}

// GetAccessModes 返回支持的访问模式。
func (plugin *localVolumePlugin) GetAccessModes() []v1.PersistentVolumeAccessMode {
	// 当前 AccessMode 的语义是能 attach 的节点数，而非能挂载的 Pod 数。
	return []v1.PersistentVolumeAccessMode{
		v1.ReadWriteOnce,
	}
}

// getVolumeSource 从规格中提取 local 卷源及只读标记。
func getVolumeSource(spec *volume.Spec) (*v1.LocalVolumeSource, bool, error) {
	if spec.PersistentVolume != nil && spec.PersistentVolume.Spec.Local != nil {
		return spec.PersistentVolume.Spec.Local, spec.ReadOnly, nil
	}

	return nil, false, fmt.Errorf("Spec does not reference a Local volume type")
}

// NewMounter 为 local 卷创建文件系统 mounter。
func (plugin *localVolumePlugin) NewMounter(spec *volume.Spec, pod *v1.Pod) (volume.Mounter, error) {
	_, readOnly, err := getVolumeSource(spec)
	if err != nil {
		return nil, err
	}

	globalLocalPath, err := plugin.getGlobalLocalPath(spec)
	if err != nil {
		return nil, err
	}

	kvh, ok := plugin.host.(volume.KubeletVolumeHost)
	if !ok {
		return nil, fmt.Errorf("plugin volume host does not implement KubeletVolumeHost interface")
	}

	return &localVolumeMounter{
		localVolume: &localVolume{
			pod:             pod,
			podUID:          pod.UID,
			volName:         spec.Name(),
			mounter:         plugin.host.GetMounter(),
			hostUtil:        kvh.GetHostUtil(),
			plugin:          plugin,
			globalPath:      globalLocalPath,
			MetricsProvider: volume.NewMetricsStatFS(plugin.host.GetPodVolumeDir(pod.UID, utilstrings.EscapeQualifiedName(localVolumePluginName), spec.Name())),
		},
		mountOptions: util.MountOptionFromSpec(spec),
		readOnly:     readOnly,
	}, nil

}

// NewUnmounter 为 local 卷创建 unmounter。
func (plugin *localVolumePlugin) NewUnmounter(volName string, podUID types.UID) (volume.Unmounter, error) {
	return &localVolumeUnmounter{
		localVolume: &localVolume{
			podUID:  podUID,
			volName: volName,
			mounter: plugin.host.GetMounter(),
			plugin:  plugin,
		},
	}, nil
}

// NewBlockVolumeMapper 为 local 块卷创建 mapper。
func (plugin *localVolumePlugin) NewBlockVolumeMapper(spec *volume.Spec, pod *v1.Pod) (volume.BlockVolumeMapper, error) {
	volumeSource, readOnly, err := getVolumeSource(spec)
	if err != nil {
		return nil, err
	}

	mapper := &localVolumeMapper{
		localVolume: &localVolume{
			podUID:     pod.UID,
			volName:    spec.Name(),
			globalPath: volumeSource.Path,
			plugin:     plugin,
		},
		readOnly: readOnly,
	}

	blockPath, err := mapper.GetGlobalMapPath(spec)
	if err != nil {
		return nil, fmt.Errorf("failed to get device path: %v", err)
	}
	mapper.MetricsProvider = volume.NewMetricsBlock(filepath.Join(blockPath, string(pod.UID)))

	return mapper, nil
}

// NewBlockVolumeUnmapper 为 local 块卷创建 unmapper。
func (plugin *localVolumePlugin) NewBlockVolumeUnmapper(volName string,
	podUID types.UID) (volume.BlockVolumeUnmapper, error) {
	return &localVolumeUnmapper{
		localVolume: &localVolume{
			podUID:  podUID,
			volName: volName,
			plugin:  plugin,
		},
	}, nil
}

// TODO: 检查无路径、无拓扑约束的情况是否可接受。
func (plugin *localVolumePlugin) ConstructVolumeSpec(volumeName, mountPath string) (volume.ReconstructedVolume, error) {
	fs := v1.PersistentVolumeFilesystem
	// 重建卷的主要目的是清理未使用的挂载点和目录。
	// 对于以目录为来源的文件系统卷，无需清理全局挂载路径，空路径即可。
	// 对于以块设备为来源的文件系统卷，若全局挂载路径存在，应解析出其设备路径。
	var path string
	mounter := plugin.host.GetMounter()
	refs, err := mounter.GetMountRefs(mountPath)
	if err != nil {
		return volume.ReconstructedVolume{}, err
	}
	baseMountPath := plugin.generateBlockDeviceBaseGlobalPath()
	for _, ref := range refs {
		if mount.PathWithinBase(ref, baseMountPath) {
			// 若存在块设备的全局挂载，说明卷来源是块设备。
			// 使用符号链接时，解析出的设备路径可能与 local PV 对象中的路径不完全一致，
			// 但它是真实的来源，可用于重建卷。
			path, _, err = mount.GetDeviceNameFromMount(mounter, ref)
			if err != nil {
				return volume.ReconstructedVolume{}, err
			}
			klog.V(4).Infof("local: reconstructing volume %q (pod volume mount: %q) with device %q", volumeName, mountPath, path)
			break
		}
	}
	localVolume := &v1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: volumeName,
		},
		Spec: v1.PersistentVolumeSpec{
			PersistentVolumeSource: v1.PersistentVolumeSource{
				Local: &v1.LocalVolumeSource{
					Path: path,
				},
			},
			VolumeMode: &fs,
		},
	}
	return volume.ReconstructedVolume{
		Spec: volume.NewSpecFromPersistentVolume(localVolume, false),
	}, nil
}

// ConstructBlockVolumeSpec 重建 local 块卷的规格。
func (plugin *localVolumePlugin) ConstructBlockVolumeSpec(podUID types.UID, volumeName,
	mapPath string) (*volume.Spec, error) {
	block := v1.PersistentVolumeBlock

	localVolume := &v1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: volumeName,
		},
		Spec: v1.PersistentVolumeSpec{
			PersistentVolumeSource: v1.PersistentVolumeSource{
				Local: &v1.LocalVolumeSource{
					// 无需从主机断开本地设备，因此路径留空。
					Path: "",
				},
			},
			VolumeMode: &block,
		},
	}

	return volume.NewSpecFromPersistentVolume(localVolume, false), nil
}

// generateBlockDeviceBaseGlobalPath 返回块设备全局挂载路径的根目录。
func (plugin *localVolumePlugin) generateBlockDeviceBaseGlobalPath() string {
	return filepath.Join(plugin.host.GetPluginDir(localVolumePluginName), util.MountsInGlobalPDPath)
}

// getGlobalLocalPath 返回 local 卷在主机上的全局路径。
func (plugin *localVolumePlugin) getGlobalLocalPath(spec *volume.Spec) (string, error) {
	if spec.PersistentVolume.Spec.Local == nil || len(spec.PersistentVolume.Spec.Local.Path) == 0 {
		return "", fmt.Errorf("local volume source is nil or local path is not set")
	}

	kvh, ok := plugin.host.(volume.KubeletVolumeHost)
	if !ok {
		return "", fmt.Errorf("plugin volume host does not implement KubeletVolumeHost interface")
	}

	fileType, err := kvh.GetHostUtil().GetFileType(spec.PersistentVolume.Spec.Local.Path)
	if err != nil {
		return "", err
	}
	switch fileType {
	case hostutil.FileTypeDirectory:
		return spec.PersistentVolume.Spec.Local.Path, nil
	case hostutil.FileTypeBlockDev:
		return filepath.Join(plugin.generateBlockDeviceBaseGlobalPath(), spec.Name()), nil
	default:
		return "", fmt.Errorf("only directory and block device are supported")
	}
}

var _ volume.DeviceMountableVolumePlugin = &localVolumePlugin{}

// deviceMounter 实现 local 卷的设备挂载与卸载。
type deviceMounter struct {
	plugin   *localVolumePlugin
	mounter  *mount.SafeFormatAndMount
	hostUtil hostutil.HostUtils
}

var _ volume.DeviceMounter = &deviceMounter{}

// CanDeviceMount 表示始终可进行设备挂载。
func (plugin *localVolumePlugin) CanDeviceMount(spec *volume.Spec) (bool, error) {
	return true, nil
}

// NewDeviceMounter 创建一个 deviceMounter。
func (plugin *localVolumePlugin) NewDeviceMounter() (volume.DeviceMounter, error) {
	kvh, ok := plugin.host.(volume.KubeletVolumeHost)
	if !ok {
		return nil, fmt.Errorf("plugin volume host does not implement KubeletVolumeHost interface")
	}
	return plugin.newDeviceMounterInternal(kvh, plugin.host.GetMounter(), utilexec.New())
}

// newDeviceMounterInternal 构造 deviceMounter，便于测试时注入 mounter 与 exec。
func (plugin *localVolumePlugin) newDeviceMounterInternal(kvh volume.KubeletVolumeHost, mounter mount.Interface, exec utilexec.Interface) (volume.DeviceMounter, error) {
	return &deviceMounter{
		plugin:   plugin,
		mounter:  mount.NewSafeFormatAndMount(mounter, exec),
		hostUtil: kvh.GetHostUtil(),
	}, nil
}

// mountLocalBlockDevice 在必要时格式化并将本地块设备挂载到 deviceMountPath。
func (dm *deviceMounter) mountLocalBlockDevice(spec *volume.Spec, devicePath string, deviceMountPath string) error {
	klog.V(4).Infof("local: mounting device %s to %s", devicePath, deviceMountPath)
	notMnt, err := dm.mounter.IsLikelyNotMountPoint(deviceMountPath)
	if err != nil {
		if os.IsNotExist(err) {
			if err := os.MkdirAll(deviceMountPath, 0750); err != nil {
				return err
			}
			notMnt = true
		} else {
			return err
		}
	}
	if !notMnt {
		return nil
	}
	fstype, err := getVolumeSourceFSType(spec)
	if err != nil {
		return err
	}

	ro, err := getVolumeSourceReadOnly(spec)
	if err != nil {
		return err
	}
	options := []string{}
	if ro {
		options = append(options, "ro")
	}
	mountOptions := util.MountOptionFromSpec(spec, options...)
	err = dm.mounter.FormatAndMount(devicePath, deviceMountPath, fstype, mountOptions)
	if err != nil {
		if rmErr := os.Remove(deviceMountPath); rmErr != nil {
			klog.Warningf("local: failed to remove %s: %v", deviceMountPath, rmErr)
		}
		return fmt.Errorf("local: failed to mount device %s at %s (fstype: %s), error %w", devicePath, deviceMountPath, fstype, err)
	}
	klog.V(3).Infof("local: successfully mount device %s at %s (fstype: %s)", devicePath, deviceMountPath, fstype)
	return nil
}

// MountDevice 按卷来源类型挂载设备；目录来源直接返回，块设备来源执行实际挂载。
func (dm *deviceMounter) MountDevice(spec *volume.Spec, devicePath string, deviceMountPath string, _ volume.DeviceMounterArgs) error {
	if spec.PersistentVolume.Spec.Local == nil || len(spec.PersistentVolume.Spec.Local.Path) == 0 {
		return fmt.Errorf("local volume source is nil or local path is not set")
	}
	fileType, err := dm.hostUtil.GetFileType(spec.PersistentVolume.Spec.Local.Path)
	if err != nil {
		return err
	}

	switch fileType {
	case hostutil.FileTypeBlockDev:
		// local 插件未实现 AttachableVolumePlugin，因此直接使用 PV 规格中的路径作为设备路径。
		return dm.mountLocalBlockDevice(spec, spec.PersistentVolume.Spec.Local.Path, deviceMountPath)
	case hostutil.FileTypeDirectory:
		// 路径本身已是文件系统目录，无需挂载，直接返回。
		return nil
	default:
		return fmt.Errorf("only directory and block device are supported")
	}
}

// RequiresFSResize 返回是否需要文件系统扩容。
func (plugin *localVolumePlugin) RequiresFSResize() bool {
	return true
}

// NodeExpand 对 local 卷执行节点扩容；目录来源为空操作。
func (plugin *localVolumePlugin) NodeExpand(resizeOptions volume.NodeResizeOptions) (bool, error) {
	fsVolume, err := util.CheckVolumeModeFilesystem(resizeOptions.VolumeSpec)
	if err != nil {
		return false, fmt.Errorf("error checking VolumeMode: %v", err)
	}
	if !fsVolume {
		return true, nil
	}

	localDevicePath := resizeOptions.VolumeSpec.PersistentVolume.Spec.Local.Path

	kvh, ok := plugin.host.(volume.KubeletVolumeHost)
	if !ok {
		return false, fmt.Errorf("plugin volume host does not implement KubeletVolumeHost interface")
	}

	fileType, err := kvh.GetHostUtil().GetFileType(localDevicePath)
	if err != nil {
		return false, err
	}

	switch fileType {
	case hostutil.FileTypeBlockDev:
		_, err = util.GenericResizeFS(plugin.host, localDevicePath, resizeOptions.DeviceMountPath)
		if err != nil {
			return false, err
		}
		return true, nil
	case hostutil.FileTypeDirectory:
		// 路径本身已是文件系统目录，直接返回，避免阻碍挂载成功。
		klog.InfoS("Expansion of directory based local volumes is NO-OP", "localVolumePath", localDevicePath)
		return true, nil
	default:
		return false, fmt.Errorf("only directory and block device are supported")
	}
}

// getVolumeSourceFSType 返回 local 卷的文件系统类型，未指定时默认为 ext4。
func getVolumeSourceFSType(spec *volume.Spec) (string, error) {
	if spec.PersistentVolume != nil &&
		spec.PersistentVolume.Spec.Local != nil {
		if spec.PersistentVolume.Spec.Local.FSType != nil {
			return *spec.PersistentVolume.Spec.Local.FSType, nil
		}
		// local PV 规格未指定 FSType 时使用默认值 "ext4"。
		return defaultFSType, nil
	}

	return "", fmt.Errorf("spec does not reference a Local volume type")
}

// getVolumeSourceReadOnly 返回 local 卷的只读标记。
func getVolumeSourceReadOnly(spec *volume.Spec) (bool, error) {
	if spec.PersistentVolume != nil &&
		spec.PersistentVolume.Spec.Local != nil {
		// 作为 PV 使用的 local 卷，其只读标记通过挂载该 PV 的 persistent-claim 卷间接获得。
		return spec.ReadOnly, nil
	}

	return false, fmt.Errorf("spec does not reference a Local volume type")
}

// GetDeviceMountPath 返回设备的全局挂载路径。
func (dm *deviceMounter) GetDeviceMountPath(spec *volume.Spec) (string, error) {
	return dm.plugin.getGlobalLocalPath(spec)
}

// NewDeviceUnmounter 创建设备卸载器。
func (plugin *localVolumePlugin) NewDeviceUnmounter() (volume.DeviceUnmounter, error) {
	return plugin.newDeviceUnmounterInternal(plugin.host.GetMounter(), utilexec.New())
}

// newDeviceUnmounterInternal 构造设备卸载器，便于测试时注入依赖。
func (plugin *localVolumePlugin) newDeviceUnmounterInternal(mounter mount.Interface, exec utilexec.Interface) (volume.DeviceUnmounter, error) {
	return &deviceMounter{
		plugin:  plugin,
		mounter: mount.NewSafeFormatAndMount(mounter, exec),
	}, nil
}

// GetDeviceMountRefs 返回设备挂载点的引用列表。
func (plugin *localVolumePlugin) GetDeviceMountRefs(deviceMountPath string) ([]string, error) {
	mounter := plugin.host.GetMounter()
	return mounter.GetMountRefs(deviceMountPath)
}

var _ volume.DeviceUnmounter = &deviceMounter{}

// UnmountDevice 仅在 deviceMountPath 位于块设备全局挂载根目录下时执行卸载。
func (dm *deviceMounter) UnmountDevice(deviceMountPath string) error {
	// 块设备卷的 deviceMountPath 形如 /var/lib/kubelet/plugins/kubernetes.io/local-volume/mounts/<PV 名>；
	// 目录来源卷则直接使用 pvSpec.Local.Path。这里只需卸载块设备，故检查路径是否位于上述挂载根目录内。
	basemountPath := dm.plugin.generateBlockDeviceBaseGlobalPath()
	if mount.PathWithinBase(deviceMountPath, basemountPath) {
		return mount.CleanupMountPoint(deviceMountPath, dm.mounter, false)
	}

	return nil
}

// localVolume 表示节点上的本地目录，globalPath 处的目录会被 bind mount 到 Pod 目录。
type localVolume struct {
	volName string
	pod     *v1.Pod
	podUID  types.UID
	// 卷在主机上的全局路径。
	globalPath string
	// 提供将全局路径挂载到 Pod 本地路径的系统调用。
	mounter  mount.Interface
	hostUtil hostutil.HostUtils
	plugin   *localVolumePlugin
	volume.MetricsProvider
}

// GetPath 返回卷在 Pod 目录中的路径。
func (l *localVolume) GetPath() string {
	return l.plugin.host.GetPodVolumeDir(l.podUID, utilstrings.EscapeQualifiedName(localVolumePluginName), l.volName)
}

// localVolumeMounter 实现 local 卷的文件系统挂载。
type localVolumeMounter struct {
	*localVolume
	readOnly     bool
	mountOptions []string
}

var _ volume.Mounter = &localVolumeMounter{}

// GetAttributes 返回卷属性，包括只读标记与 SELinux 重标记能力。
func (m *localVolumeMounter) GetAttributes() volume.Attributes {
	return volume.Attributes{
		ReadOnly:       m.readOnly,
		Managed:        !m.readOnly,
		SELinuxRelabel: true,
	}
}

// SetUp 将本地目录 bind mount 到卷路径。
func (m *localVolumeMounter) SetUp(mounterArgs volume.MounterArgs) error {
	return m.SetUpAt(m.GetPath(), mounterArgs)
}

// SetUpAt 将本地目录 bind mount 到指定路径，并设置卷的所有权。
func (m *localVolumeMounter) SetUpAt(dir string, mounterArgs volume.MounterArgs) error {
	m.plugin.volumeLocks.LockKey(m.globalPath)
	defer m.plugin.volumeLocks.UnlockKey(m.globalPath)

	if m.globalPath == "" {
		return fmt.Errorf("LocalVolume volume %q path is empty", m.volName)
	}

	err := validation.ValidatePathNoBacksteps(m.globalPath)
	if err != nil {
		return fmt.Errorf("invalid path: %s %v", m.globalPath, err)
	}

	notMnt, err := mount.IsNotMountPoint(m.mounter, dir)
	klog.V(4).Infof("LocalVolume mount setup: PodDir(%s) VolDir(%s) Mounted(%t) Error(%v), ReadOnly(%t)", dir, m.globalPath, !notMnt, err, m.readOnly)
	if err != nil && !os.IsNotExist(err) {
		klog.Errorf("cannot validate mount point: %s %v", dir, err)
		return err
	}

	if !notMnt {
		return nil
	}
	refs, err := m.mounter.GetMountRefs(m.globalPath)
	if mounterArgs.FsGroup != nil {
		if err != nil {
			klog.Errorf("cannot collect mounting information: %s %v", m.globalPath, err)
			return err
		}

		// 只统计来自其他 Pod 的挂载。
		refs = m.filterPodMounts(refs)
		if len(refs) > 0 {
			fsGroupNew := int64(*mounterArgs.FsGroup)
			_, fsGroupOld, err := m.hostUtil.GetOwner(m.globalPath)
			if err != nil {
				return fmt.Errorf("failed to check fsGroup for %s (%v)", m.globalPath, err)
			}
			if fsGroupNew != fsGroupOld {
				m.plugin.recorder.Eventf(m.pod, v1.EventTypeWarning, events.WarnAlreadyMountedVolume, "The requested fsGroup is %d, but the volume %s has GID %d. The volume may not be shareable.", fsGroupNew, m.volName, fsGroupOld)
			}
		}

	}

	if runtime.GOOS != "windows" {
		// Windows 不执行下面的 MkdirAll，因为 "bind mount" 逻辑在 mount_windows.go 中另有实现。
		if err := os.MkdirAll(dir, 0750); err != nil {
			klog.Errorf("mkdir failed on disk %s (%v)", dir, err)
			return err
		}
	}
	// 对完整路径执行 bind mount，使同一卷可在多处重复挂载。
	options := []string{"bind"}
	if m.readOnly {
		options = append(options, "ro")
	}
	mountOptions := util.JoinMountOptions(options, m.mountOptions)

	klog.V(4).Infof("attempting to mount %s", dir)
	globalPath := util.MakeAbsolutePath(runtime.GOOS, m.globalPath)
	err = m.mounter.MountSensitiveWithoutSystemd(globalPath, dir, "", mountOptions, nil)
	if err != nil {
		klog.Errorf("Mount of volume %s failed: %v", dir, err)
		notMnt, mntErr := mount.IsNotMountPoint(m.mounter, dir)
		if mntErr != nil {
			klog.Errorf("IsNotMountPoint check failed: %v", mntErr)
			return err
		}
		if !notMnt {
			if mntErr = m.mounter.Unmount(dir); mntErr != nil {
				klog.Errorf("Failed to unmount: %v", mntErr)
				return err
			}
			notMnt, mntErr = mount.IsNotMountPoint(m.mounter, dir)
			if mntErr != nil {
				klog.Errorf("IsNotMountPoint check failed: %v", mntErr)
				return err
			}
			if !notMnt {
				// 属于异常情况，留待下一个 sync 循环重试。
				klog.Errorf("%s is still mounted, despite call to unmount().  Will try again next sync loop.", dir)
				return err
			}
		}
		if rmErr := os.Remove(dir); rmErr != nil {
			klog.Warningf("failed to remove %s: %v", dir, rmErr)
		}
		return err
	}
	if !m.readOnly {
		// 卷所有者仅在首次挂载时写入。
		if len(refs) == 0 {
			ownershipChanger := volume.NewVolumeOwnership(m, dir, mounterArgs.FsGroup, mounterArgs.FSGroupChangePolicy, util.FSGroupCompleteHook(m.plugin, nil))
			ownershipChanger.AddProgressNotifier(m.pod, mounterArgs.Recorder)
			return ownershipChanger.ChangePermissions()
		}
	}
	return nil
}

// filterPodMounts 只保留位于 kubelet Pod 目录内的挂载路径。
func (m *localVolumeMounter) filterPodMounts(refs []string) []string {
	filtered := []string{}
	for _, r := range refs {
		if strings.HasPrefix(r, m.plugin.host.GetPodsDir()+string(os.PathSeparator)) {
			filtered = append(filtered, r)
		}
	}
	return filtered
}

// localVolumeUnmounter 实现 local 卷的卸载。
type localVolumeUnmounter struct {
	*localVolume
}

var _ volume.Unmounter = &localVolumeUnmounter{}

// TearDown 解除 bind mount。
func (u *localVolumeUnmounter) TearDown() error {
	return u.TearDownAt(u.GetPath())
}

// TearDownAt 解除指定路径上的 bind mount。
func (u *localVolumeUnmounter) TearDownAt(dir string) error {
	klog.V(4).Infof("Unmounting volume %q at path %q\n", u.volName, dir)
	return mount.CleanupMountPoint(dir, u.mounter, true) /* extensiveMountPointCheck = true */
}

// localVolumeMapper 为 local 卷实现 BlockVolumeMapper 接口。
type localVolumeMapper struct {
	*localVolume
	readOnly bool
}

var _ volume.BlockVolumeMapper = &localVolumeMapper{}
var _ volume.CustomBlockVolumeMapper = &localVolumeMapper{}

// SetUpDevice 按插件特定的方式在节点上准备卷；local 卷无需额外操作。
func (m *localVolumeMapper) SetUpDevice() (string, error) {
	return "", nil
}

// MapPodDevice 返回 local PV 对应的物理设备路径。
func (m *localVolumeMapper) MapPodDevice() (string, error) {
	globalPath := util.MakeAbsolutePath(runtime.GOOS, m.globalPath)
	klog.V(4).Infof("MapPodDevice returning path %s", globalPath)
	return globalPath, nil
}

// GetStagingPath 返回空字符串；local 块卷无需 staging 路径。
func (m *localVolumeMapper) GetStagingPath() string {
	return ""
}

// SupportsMetrics 返回 true，因为该 mapper 会初始化 MetricsProvider。
func (m *localVolumeMapper) SupportsMetrics() bool {
	return true
}

// localVolumeUnmapper 为 local 卷实现 BlockVolumeUnmapper 接口。
type localVolumeUnmapper struct {
	*localVolume
	volume.MetricsNil
}

var _ volume.BlockVolumeUnmapper = &localVolumeUnmapper{}

// GetGlobalMapPath 返回全局映射路径。
// 路径格式：plugins/kubernetes.io/kubernetes.io/local-volume/volumeDevices/{volumeName}
func (l *localVolume) GetGlobalMapPath(spec *volume.Spec) (string, error) {
	return filepath.Join(l.plugin.host.GetVolumeDevicePluginDir(utilstrings.EscapeQualifiedName(localVolumePluginName)),
		l.volName), nil
}

// GetPodDeviceMapPath 返回 Pod 设备映射路径与卷名。
// 路径格式：pods/{podUid}/volumeDevices/kubernetes.io~local-volume
// 卷名示例：local-pv-ff0d6d4
func (l *localVolume) GetPodDeviceMapPath() (string, string) {
	return l.plugin.host.GetPodVolumeDeviceDir(l.podUID,
		utilstrings.EscapeQualifiedName(localVolumePluginName)), l.volName
}
