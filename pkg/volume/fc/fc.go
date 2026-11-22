/*
Copyright 2015 The Kubernetes Authors.

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

// Package fc 实现 Fibre Channel（fc）卷的插件逻辑。
package fc

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/mount-utils"
	utilexec "k8s.io/utils/exec"
	"k8s.io/utils/io"
	utilstrings "k8s.io/utils/strings"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/kubernetes/pkg/volume"
	"k8s.io/kubernetes/pkg/volume/util"
	"k8s.io/kubernetes/pkg/volume/util/volumepathhandler"
)

// ProbeVolumePlugins 是卷插件的主要入口点。
func ProbeVolumePlugins() []volume.VolumePlugin {
	return []volume.VolumePlugin{&fcPlugin{nil}}
}

type fcPlugin struct {
	host volume.VolumeHost
}

var _ volume.VolumePlugin = &fcPlugin{}
var _ volume.PersistentVolumePlugin = &fcPlugin{}
var _ volume.BlockVolumePlugin = &fcPlugin{}

const (
	fcPluginName = "kubernetes.io/fc"
)

// Init 初始化插件并保存 VolumeHost。
func (plugin *fcPlugin) Init(host volume.VolumeHost) error {
	plugin.host = host
	return nil
}

// GetPluginName 返回插件名称。
func (plugin *fcPlugin) GetPluginName() string {
	return fcPluginName
}

// GetVolumeName 返回卷的唯一标识（wwn+lun 或 wwid）。
func (plugin *fcPlugin) GetVolumeName(spec *volume.Spec) (string, error) {
	volumeSource, _, err := getVolumeSource(spec)
	if err != nil {
		return "", err
	}

	// API server 已提前校验这些参数，但 attach/detach controller 创建 volumespec 时未做校验。
	// 参数可能为 nil 或空，这里再检查一次以避免意外情况。
	if len(volumeSource.TargetWWNs) != 0 && volumeSource.Lun != nil {
		// TargetWWNs 是 FibreChannel 目标全球名称。
		return fmt.Sprintf("%v:%v", volumeSource.TargetWWNs, *volumeSource.Lun), nil
	} else if len(volumeSource.WWIDs) != 0 {
		// WWIDs 是 FibreChannel 全球标识符。
		return fmt.Sprintf("%v", volumeSource.WWIDs), nil
	}

	return "", err
}

// CanSupport 判断 spec 是否为 FC 卷。
func (plugin *fcPlugin) CanSupport(spec *volume.Spec) bool {
	return (spec.Volume != nil && spec.Volume.FC != nil) || (spec.PersistentVolume != nil && spec.PersistentVolume.Spec.FC != nil)
}

// RequiresRemount 返回该插件是否需要周期性重挂载。
func (plugin *fcPlugin) RequiresRemount(spec *volume.Spec) bool {
	return false
}

// SupportsMountOption 返回该插件是否支持挂载选项。
func (plugin *fcPlugin) SupportsMountOption() bool {
	return true
}

// SupportsSELinuxContextMount 返回该插件是否支持通过 -o context 挂载 SELinux 上下文。
func (plugin *fcPlugin) SupportsSELinuxContextMount(spec *volume.Spec) (bool, error) {
	return true, nil
}

// GetAccessModes 返回该插件支持的访问模式。
func (plugin *fcPlugin) GetAccessModes() []v1.PersistentVolumeAccessMode {
	return []v1.PersistentVolumeAccessMode{
		v1.ReadWriteOnce,
		v1.ReadOnlyMany,
	}
}

// NewMounter 创建一个文件系统卷的 Mounter。
func (plugin *fcPlugin) NewMounter(spec *volume.Spec, pod *v1.Pod) (volume.Mounter, error) {
	// 在此注入真实实现，测试通过内部函数进行。
	return plugin.newMounterInternal(spec, pod.UID, &fcUtil{}, plugin.host.GetMounter(), utilexec.New())
}

func (plugin *fcPlugin) newMounterInternal(spec *volume.Spec, podUID types.UID, manager diskManager, mounter mount.Interface, exec utilexec.Interface) (volume.Mounter, error) {
	// pod 中直接使用的 fc 卷其 ReadOnly 由 pod 作者设置；
	// 作为 PersistentVolume 使用的 fc 卷则通过挂载 PV 的 persistent-claim 卷间接获得 ReadOnly。
	fc, readOnly, err := getVolumeSource(spec)
	if err != nil {
		return nil, err
	}

	wwns, lun, wwids, err := getWwnsLunWwids(fc)
	if err != nil {
		return nil, fmt.Errorf("fc: no fc disk information found. failed to make a new mounter")
	}
	fcDisk := &fcDisk{
		podUID:  podUID,
		volName: spec.Name(),
		wwns:    wwns,
		lun:     lun,
		wwids:   wwids,
		manager: manager,
		io:      &osIOHandler{},
		plugin:  plugin,
	}

	volumeMode, err := util.GetVolumeMode(spec)
	if err != nil {
		return nil, err
	}

	klog.V(5).Infof("fc: newMounterInternal volumeMode %s", volumeMode)
	return &fcDiskMounter{
		fcDisk:       fcDisk,
		fsType:       fc.FSType,
		volumeMode:   volumeMode,
		readOnly:     readOnly,
		mounter:      &mount.SafeFormatAndMount{Interface: mounter, Exec: exec},
		deviceUtil:   util.NewDeviceHandler(util.NewIOHandler()),
		mountOptions: util.MountOptionFromSpec(spec),
	}, nil
}

// NewBlockVolumeMapper 创建块设备的 Mapper。
func (plugin *fcPlugin) NewBlockVolumeMapper(spec *volume.Spec, pod *v1.Pod) (volume.BlockVolumeMapper, error) {
	// 若经由 GenerateUnmapDeviceFunc() 调用，pod 为 nil。
	// 此时 uid 不会被使用，传入空字符串作为占位即可。
	var uid types.UID
	if pod != nil {
		uid = pod.UID
	}
	return plugin.newBlockVolumeMapperInternal(spec, uid, &fcUtil{}, plugin.host.GetMounter(), utilexec.New())
}

func (plugin *fcPlugin) newBlockVolumeMapperInternal(spec *volume.Spec, podUID types.UID, manager diskManager, mounter mount.Interface, exec utilexec.Interface) (volume.BlockVolumeMapper, error) {
	fc, readOnly, err := getVolumeSource(spec)
	if err != nil {
		return nil, err
	}

	wwns, lun, wwids, err := getWwnsLunWwids(fc)
	if err != nil {
		return nil, fmt.Errorf("fc: no fc disk information found. failed to make a new mapper")
	}

	mapper := &fcDiskMapper{
		fcDisk: &fcDisk{
			podUID:  podUID,
			volName: spec.Name(),
			wwns:    wwns,
			lun:     lun,
			wwids:   wwids,
			manager: manager,
			io:      &osIOHandler{},
			plugin:  plugin},
		readOnly:   readOnly,
		mounter:    &mount.SafeFormatAndMount{Interface: mounter, Exec: exec},
		deviceUtil: util.NewDeviceHandler(util.NewIOHandler()),
	}

	blockPath, err := mapper.GetGlobalMapPath(spec)
	if err != nil {
		return nil, fmt.Errorf("failed to get device path: %v", err)
	}
	mapper.MetricsProvider = volume.NewMetricsBlock(filepath.Join(blockPath, string(podUID)))

	return mapper, nil
}

// NewUnmounter 创建文件系统卷的 Unmounter。
func (plugin *fcPlugin) NewUnmounter(volName string, podUID types.UID) (volume.Unmounter, error) {
	// 在此注入真实实现，测试通过内部函数进行。
	return plugin.newUnmounterInternal(volName, podUID, &fcUtil{}, plugin.host.GetMounter(), utilexec.New())
}

func (plugin *fcPlugin) newUnmounterInternal(volName string, podUID types.UID, manager diskManager, mounter mount.Interface, exec utilexec.Interface) (volume.Unmounter, error) {
	return &fcDiskUnmounter{
		fcDisk: &fcDisk{
			podUID:  podUID,
			volName: volName,
			manager: manager,
			plugin:  plugin,
			io:      &osIOHandler{},
		},
		mounter:    mounter,
		deviceUtil: util.NewDeviceHandler(util.NewIOHandler()),
		exec:       exec,
	}, nil
}

// NewBlockVolumeUnmapper 创建块设备的 Unmapper。
func (plugin *fcPlugin) NewBlockVolumeUnmapper(volName string, podUID types.UID) (volume.BlockVolumeUnmapper, error) {
	return plugin.newUnmapperInternal(volName, podUID, &fcUtil{}, utilexec.New())
}

func (plugin *fcPlugin) newUnmapperInternal(volName string, podUID types.UID, manager diskManager, exec utilexec.Interface) (volume.BlockVolumeUnmapper, error) {
	return &fcDiskUnmapper{
		fcDisk: &fcDisk{
			podUID:  podUID,
			volName: volName,
			manager: manager,
			plugin:  plugin,
			io:      &osIOHandler{},
		},
		exec:       exec,
		deviceUtil: util.NewDeviceHandler(util.NewIOHandler()),
	}, nil
}

// ConstructVolumeSpec 从 pod 卷目录中的挂载信息重建 volume.Spec。
func (plugin *fcPlugin) ConstructVolumeSpec(volumeName, mountPath string) (volume.ReconstructedVolume, error) {
	// 从 pod 卷目录（mountPath）中找出 globalPDPath。
	// 示例：
	//   mountPath:     pods/{podUid}/volumes/kubernetes.io~fc/{volumeName}
	//   globalPDPath : plugins/kubernetes.io/fc/50060e801049cfd1-lun-0
	var globalPDPath string

	mounter := plugin.host.GetMounter()
	// 必须尽力获取卷的全局挂载点：若在此返回错误，全局挂载仍会保留，
	// 而卷却被标记为未使用，进而可能在多个节点上被挂载，导致卷损坏。
	paths, err := util.GetReliableMountRefs(mounter, mountPath)
	if io.IsInconsistentReadError(err) {
		klog.Errorf("Failed to read mount refs from /proc/mounts for %s: %s", mountPath, err)
		klog.Errorf("Kubelet cannot unmount volume at %s, please unmount it manually", mountPath)
		return volume.ReconstructedVolume{}, err
	}
	if err != nil {
		return volume.ReconstructedVolume{}, err
	}
	for _, path := range paths {
		if strings.Contains(path, plugin.host.GetPluginDir(fcPluginName)) {
			globalPDPath = path
			break
		}
	}
	// 未能取得 globalPDPath。
	if len(globalPDPath) == 0 {
		return volume.ReconstructedVolume{}, fmt.Errorf("couldn't fetch globalPDPath. failed to obtain volume spec")
	}

	wwns, lun, wwids, err := parsePDName(globalPDPath)
	if err != nil {
		return volume.ReconstructedVolume{}, fmt.Errorf("failed to retrieve volume plugin information from globalPDPath: %s", err)
	}
	// 用 wwn+lun 或 wwid 创建卷。
	fcVolume := &v1.Volume{
		Name: volumeName,
		VolumeSource: v1.VolumeSource{
			FC: &v1.FCVolumeSource{WWIDs: wwids, Lun: &lun, TargetWWNs: wwns},
		},
	}

	var mountContext string
	if utilfeature.DefaultFeatureGate.Enabled(features.SELinuxMountReadWriteOncePod) {
		kvh, ok := plugin.host.(volume.KubeletVolumeHost)
		if !ok {
			return volume.ReconstructedVolume{}, fmt.Errorf("plugin volume host does not implement KubeletVolumeHost interface")
		}
		hu := kvh.GetHostUtil()
		mountContext, err = hu.GetSELinuxMountContext(mountPath)
		if err != nil {
			return volume.ReconstructedVolume{}, err
		}
	}

	klog.V(5).Infof("ConstructVolumeSpec: TargetWWNs: %v, Lun: %v, WWIDs: %v",
		fcVolume.VolumeSource.FC.TargetWWNs, *fcVolume.VolumeSource.FC.Lun, fcVolume.VolumeSource.FC.WWIDs)
	return volume.ReconstructedVolume{
		Spec:                volume.NewSpecFromVolume(fcVolume),
		SELinuxMountContext: mountContext,
	}, nil
}

// ConstructBlockVolumeSpec 通过以下步骤创建新的 volume.Spec。
//   - 在卷插件目录下查找以 {pod uuid} 命名的文件。
//   - 找到后，从 globalMapPathUUID 中还原 volumePluginDependentPath。
//   - 取得 volumePluginDependentPath 后，将卷信息写入 VolumeSource。
//
// 示例：
//
//	mapPath: pods/{podUid}/{DefaultKubeletVolumeDevicesDirName}/{escapeQualifiedPluginName}/{volumeName}
//	globalMapPathUUID : plugins/kubernetes.io/{PluginName}/{DefaultKubeletVolumeDevicesDirName}/{volumePluginDependentPath}/{pod uuid}
func (plugin *fcPlugin) ConstructBlockVolumeSpec(podUID types.UID, volumeName, mapPath string) (*volume.Spec, error) {
	pluginDir := plugin.host.GetVolumeDevicePluginDir(fcPluginName)
	blkutil := volumepathhandler.NewBlockVolumePathHandler()
	globalMapPathUUID, err := blkutil.FindGlobalMapPathUUIDFromPod(pluginDir, mapPath, podUID)
	if err != nil {
		return nil, err
	}
	klog.V(5).Infof("globalMapPathUUID: %v, err: %v", globalMapPathUUID, err)

	// 从 globalMapPathUUID 还原 globalPDPath。
	// globalMapPathUUID 示例：
	//   wwn+lun: plugins/kubernetes.io/fc/volumeDevices/50060e801049cfd1-lun-0/{pod uuid}
	//   wwid: plugins/kubernetes.io/fc/volumeDevices/3600508b400105e210000900000490000/{pod uuid}
	globalPDPath := filepath.Dir(globalMapPathUUID)
	// 用 wwn+lun 或 wwid 创建卷。
	wwns, lun, wwids, err := parsePDName(globalPDPath)
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve volume plugin information from globalPDPath: %s", err)
	}
	fcPV := createPersistentVolumeFromFCVolumeSource(volumeName,
		v1.FCVolumeSource{TargetWWNs: wwns, Lun: &lun, WWIDs: wwids})
	klog.V(5).Infof("ConstructBlockVolumeSpec: TargetWWNs: %v, Lun: %v, WWIDs: %v",
		fcPV.Spec.PersistentVolumeSource.FC.TargetWWNs,
		*fcPV.Spec.PersistentVolumeSource.FC.Lun,
		fcPV.Spec.PersistentVolumeSource.FC.WWIDs)

	return volume.NewSpecFromPersistentVolume(fcPV, false), nil
}

type fcDisk struct {
	volName string
	podUID  types.UID
	wwns    []string
	lun     string
	wwids   []string
	plugin  *fcPlugin
	// 向 provider 发起 attach/detach 磁盘调用的工具接口。
	manager diskManager
	// io 处理接口。
	io ioHandler
	volume.MetricsNil
}

// GetPath 返回卷在 pod 目录中的挂载路径。
func (fc *fcDisk) GetPath() string {
	// 现在可以安全使用 PodVolumeDir：卷的 teardown 发生在 pod 清理之前。
	return fc.plugin.host.GetPodVolumeDir(fc.podUID, utilstrings.EscapeQualifiedName(fcPluginName), fc.volName)
}

// fcGlobalMapPath 返回块设备的全局映射路径。
func (fc *fcDisk) fcGlobalMapPath(spec *volume.Spec) (string, error) {
	mounter, err := volumeSpecToMounter(spec, fc.plugin.host)
	if err != nil {
		klog.Warningf("failed to get fc mounter: %v", err)
		return "", err
	}
	return fc.manager.MakeGlobalVDPDName(*mounter.fcDisk), nil
}

// fcPodDeviceMapPath 返回 pod 设备映射目录及卷名。
func (fc *fcDisk) fcPodDeviceMapPath() (string, string) {
	return fc.plugin.host.GetPodVolumeDeviceDir(fc.podUID, utilstrings.EscapeQualifiedName(fcPluginName)), fc.volName
}

type fcDiskMounter struct {
	*fcDisk
	readOnly                  bool
	fsType                    string
	volumeMode                v1.PersistentVolumeMode
	mounter                   *mount.SafeFormatAndMount
	deviceUtil                util.DeviceUtil
	mountOptions              []string
	mountedWithSELinuxContext bool
}

var _ volume.Mounter = &fcDiskMounter{}

// GetAttributes 返回卷的属性，如只读、是否由 kubelet 管理。
func (b *fcDiskMounter) GetAttributes() volume.Attributes {
	return volume.Attributes{
		ReadOnly:       b.readOnly,
		Managed:        !b.readOnly,
		SELinuxRelabel: !b.mountedWithSELinuxContext,
	}
}

// SetUp 将卷挂载到默认路径。
func (b *fcDiskMounter) SetUp(mounterArgs volume.MounterArgs) error {
	return b.SetUpAt(b.GetPath(), mounterArgs)
}

// SetUpAt 将卷挂载到指定目录。
func (b *fcDiskMounter) SetUpAt(dir string, mounterArgs volume.MounterArgs) error {
	// diskSetUp 会检查挂载点，避免重复调用。
	err := diskSetUp(b.manager, *b, dir, b.mounter, mounterArgs.FsGroup, mounterArgs.FSGroupChangePolicy)
	if err != nil {
		klog.Errorf("fc: failed to setup")
	}

	if utilfeature.DefaultFeatureGate.Enabled(features.SELinuxMountReadWriteOncePod) {
		// 卷在 MountDevice 中必然已通过 -o context 挂载。
		b.mountedWithSELinuxContext = mounterArgs.SELinuxLabel != ""
	}
	return err
}

type fcDiskUnmounter struct {
	*fcDisk
	mounter    mount.Interface
	deviceUtil util.DeviceUtil
	exec       utilexec.Interface
}

var _ volume.Unmounter = &fcDiskUnmounter{}

// TearDown 卸载 bind mount；仅当该磁盘在 kubelet 上是最后一个引用时才分离磁盘。
func (c *fcDiskUnmounter) TearDown() error {
	return c.TearDownAt(c.GetPath())
}

// TearDownAt 卸载指定目录处的挂载点。
func (c *fcDiskUnmounter) TearDownAt(dir string) error {
	return mount.CleanupMountPoint(dir, c.mounter, false)
}

// 以下是块设备支持的实现。
// fcDiskMapper 负责 FC 块设备的映射。
type fcDiskMapper struct {
	*fcDisk
	volume.MetricsProvider
	readOnly   bool
	mounter    mount.Interface
	deviceUtil util.DeviceUtil
}

var _ volume.BlockVolumeMapper = &fcDiskMapper{}

type fcDiskUnmapper struct {
	*fcDisk
	deviceUtil util.DeviceUtil
	exec       utilexec.Interface
}

var _ volume.BlockVolumeUnmapper = &fcDiskUnmapper{}
var _ volume.CustomBlockVolumeUnmapper = &fcDiskUnmapper{}

// TearDownDevice 分离块设备并清理其映射路径。
func (c *fcDiskUnmapper) TearDownDevice(mapPath, devicePath string) error {
	err := c.manager.DetachBlockFCDisk(*c, mapPath, devicePath)
	if err != nil {
		return fmt.Errorf("fc: failed to detach disk: %s\nError: %v", mapPath, err)
	}
	klog.V(4).Infof("fc: %s is unmounted, deleting the directory", mapPath)
	if err = os.RemoveAll(mapPath); err != nil {
		return fmt.Errorf("fc: failed to delete the directory: %s\nError: %v", mapPath, err)
	}
	klog.V(4).Infof("fc: successfully detached disk: %s", mapPath)
	return nil
}

// UnmapPodDevice 取消 pod 设备映射；FC 无需额外操作。
func (c *fcDiskUnmapper) UnmapPodDevice() error {
	return nil
}

// GetGlobalMapPath 返回全局映射路径及错误。
// path: plugins/kubernetes.io/{PluginName}/volumeDevices/{WWID}/{podUid}
func (fc *fcDisk) GetGlobalMapPath(spec *volume.Spec) (string, error) {
	return fc.fcGlobalMapPath(spec)
}

// GetPodDeviceMapPath 返回 pod 设备映射路径及卷名。
// path: pods/{podUid}/volumeDevices/kubernetes.io~fc
// volumeName: pv0001
func (fc *fcDisk) GetPodDeviceMapPath() (string, string) {
	return fc.fcPodDeviceMapPath()
}

// getVolumeSource 从 spec 中提取 FC 卷源及 ReadOnly 标志。
func getVolumeSource(spec *volume.Spec) (*v1.FCVolumeSource, bool, error) {
	// pod 中直接使用的 fc 卷其 ReadOnly 由 pod 作者设置；
	// 作为 PersistentVolume 使用的 fc 卷则通过挂载 PV 的 persistent-claim 卷间接获得 ReadOnly。
	if spec.Volume != nil && spec.Volume.FC != nil {
		return spec.Volume.FC, spec.Volume.FC.ReadOnly, nil
	} else if spec.PersistentVolume != nil &&
		spec.PersistentVolume.Spec.FC != nil {
		return spec.PersistentVolume.Spec.FC, spec.ReadOnly, nil
	}

	return nil, false, fmt.Errorf("Spec does not reference a FibreChannel volume type")
}

// createPersistentVolumeFromFCVolumeSource 根据 FC 卷源构造块模式的 PersistentVolume。
func createPersistentVolumeFromFCVolumeSource(volumeName string, fc v1.FCVolumeSource) *v1.PersistentVolume {
	block := v1.PersistentVolumeBlock
	return &v1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: volumeName,
		},
		Spec: v1.PersistentVolumeSpec{
			PersistentVolumeSource: v1.PersistentVolumeSource{
				FC: &fc,
			},
			VolumeMode: &block,
		},
	}
}

// getWwnsLunWwids 从 FC 卷源中提取 wwns、lun 和 wwids。
func getWwnsLunWwids(fc *v1.FCVolumeSource) ([]string, string, []string, error) {
	var lun string
	var wwids []string
	if fc.Lun != nil && len(fc.TargetWWNs) != 0 {
		lun = strconv.Itoa(int(*fc.Lun))
		return fc.TargetWWNs, lun, wwids, nil
	}
	if len(fc.WWIDs) != 0 {
		for _, wwid := range fc.WWIDs {
			wwids = append(wwids, strings.Replace(wwid, " ", "_", -1))
		}
		return fc.TargetWWNs, lun, wwids, nil
	}
	return nil, "", nil, fmt.Errorf("fc: no fc disk information found")
}
