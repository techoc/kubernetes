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

package iscsi

import (
	"context"
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
	"k8s.io/utils/keymutex"
	utilstrings "k8s.io/utils/strings"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/kubernetes/pkg/volume"
	ioutil "k8s.io/kubernetes/pkg/volume/util"
	"k8s.io/kubernetes/pkg/volume/util/volumepathhandler"
)

// ProbeVolumePlugins 是卷插件的主要入口点。
func ProbeVolumePlugins() []volume.VolumePlugin {
	return []volume.VolumePlugin{&iscsiPlugin{}}
}

// iscsiPlugin 实现 iSCSI 卷插件。
type iscsiPlugin struct {
	host        volume.VolumeHost
	targetLocks keymutex.KeyMutex
}

var _ volume.VolumePlugin = &iscsiPlugin{}
var _ volume.PersistentVolumePlugin = &iscsiPlugin{}
var _ volume.BlockVolumePlugin = &iscsiPlugin{}

const (
	iscsiPluginName = "kubernetes.io/iscsi"
)

// Init 初始化插件：保存卷宿主并创建按目标加锁的锁表。
func (plugin *iscsiPlugin) Init(host volume.VolumeHost) error {
	plugin.host = host
	plugin.targetLocks = keymutex.NewHashed(0)
	return nil
}

// GetPluginName 返回插件名称。
func (plugin *iscsiPlugin) GetPluginName() string {
	return iscsiPluginName
}

// GetVolumeName 返回 "portal:iqn:lun" 形式的卷唯一名称。
func (plugin *iscsiPlugin) GetVolumeName(spec *volume.Spec) (string, error) {
	tp, _, iqn, lun, err := getISCSITargetInfo(spec)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("%v:%v:%v", tp, iqn, lun), nil
}

// CanSupport 判断卷规格是否为 iSCSI 类型（内联卷或持久卷均可）。
func (plugin *iscsiPlugin) CanSupport(spec *volume.Spec) bool {
	return (spec.Volume != nil && spec.Volume.ISCSI != nil) || (spec.PersistentVolume != nil && spec.PersistentVolume.Spec.ISCSI != nil)
}

// RequiresRemount 返回 false：iSCSI 卷无需重挂载。
func (plugin *iscsiPlugin) RequiresRemount(spec *volume.Spec) bool {
	return false
}

// SupportsMountOption 返回 true：支持自定义挂载选项。
func (plugin *iscsiPlugin) SupportsMountOption() bool {
	return true
}

// SupportsSELinuxContextMount 返回 true：支持 SELinux 上下文挂载。
func (plugin *iscsiPlugin) SupportsSELinuxContextMount(spec *volume.Spec) (bool, error) {
	return true, nil
}

// GetAccessModes 返回 iSCSI 卷支持的访问模式。
func (plugin *iscsiPlugin) GetAccessModes() []v1.PersistentVolumeAccessMode {
	return []v1.PersistentVolumeAccessMode{
		v1.ReadWriteOnce,
		v1.ReadOnlyMany,
	}
}

// NewMounter 根据卷规格和 Pod 创建 iSCSI 挂载器。
func (plugin *iscsiPlugin) NewMounter(spec *volume.Spec, pod *v1.Pod) (volume.Mounter, error) {
	if pod == nil {
		return nil, fmt.Errorf("nil pod")
	}
	secret, err := createSecretMap(spec, plugin, pod.Namespace)
	if err != nil {
		return nil, err
	}
	return plugin.newMounterInternal(spec, pod.UID, &ISCSIUtil{}, plugin.host.GetMounter(), utilexec.New(), secret)
}

// newMounterInternal 使用指定的磁盘管理器、挂载器和执行器创建挂载器。
func (plugin *iscsiPlugin) newMounterInternal(spec *volume.Spec, podUID types.UID, manager diskManager, mounter mount.Interface, exec utilexec.Interface, secret map[string]string) (volume.Mounter, error) {
	readOnly, fsType, err := getISCSIVolumeInfo(spec)
	if err != nil {
		return nil, err
	}
	iscsiDisk, err := createISCSIDisk(spec, podUID, plugin, manager, secret)
	if err != nil {
		return nil, err
	}

	if iscsiDisk != nil {

		// 添加卷指标统计
		iscsiDisk.MetricsProvider = volume.NewMetricsStatFS(iscsiDisk.GetPath())
	}
	return &iscsiDiskMounter{
		iscsiDisk:    iscsiDisk,
		fsType:       fsType,
		readOnly:     readOnly,
		mounter:      &mount.SafeFormatAndMount{Interface: mounter, Exec: exec},
		exec:         exec,
		deviceUtil:   ioutil.NewDeviceHandler(ioutil.NewIOHandler()),
		mountOptions: ioutil.MountOptionFromSpec(spec),
	}, nil
}

// NewBlockVolumeMapper 根据 API 规格创建块设备卷映射器。
func (plugin *iscsiPlugin) NewBlockVolumeMapper(spec *volume.Spec, pod *v1.Pod) (volume.BlockVolumeMapper, error) {
	// 经 GenerateUnmapDeviceFunc() 调用时 pod 为 nil，此时用不到 uid，传空值占位。
	var uid types.UID
	var secret map[string]string
	var err error
	if pod != nil {
		uid = pod.UID
		secret, err = createSecretMap(spec, plugin, pod.Namespace)
		if err != nil {
			return nil, err
		}
	}
	return plugin.newBlockVolumeMapperInternal(spec, uid, &ISCSIUtil{}, plugin.host.GetMounter(), utilexec.New(), secret)
}

// newBlockVolumeMapperInternal 使用指定的磁盘管理器、挂载器和执行器创建块设备映射器。
func (plugin *iscsiPlugin) newBlockVolumeMapperInternal(spec *volume.Spec, podUID types.UID, manager diskManager, mounter mount.Interface, exec utilexec.Interface, secret map[string]string) (volume.BlockVolumeMapper, error) {
	readOnly, _, err := getISCSIVolumeInfo(spec)
	if err != nil {
		return nil, err
	}
	iscsiDisk, err := createISCSIDisk(spec, podUID, plugin, manager, secret)
	if err != nil {
		return nil, err
	}
	mapper := &iscsiDiskMapper{
		iscsiDisk:  iscsiDisk,
		readOnly:   readOnly,
		exec:       exec,
		deviceUtil: ioutil.NewDeviceHandler(ioutil.NewIOHandler()),
	}

	blockPath, err := mapper.GetGlobalMapPath(spec)
	if err != nil {
		return nil, fmt.Errorf("failed to get device path: %v", err)
	}
	mapper.MetricsProvider = volume.NewMetricsBlock(filepath.Join(blockPath, string(podUID)))

	return mapper, nil
}

// NewUnmounter 根据卷名和 Pod UID 创建卸载器。
func (plugin *iscsiPlugin) NewUnmounter(volName string, podUID types.UID) (volume.Unmounter, error) {
	return plugin.newUnmounterInternal(volName, podUID, &ISCSIUtil{}, plugin.host.GetMounter(), utilexec.New())
}

// newUnmounterInternal 使用指定的磁盘管理器、挂载器和执行器创建卸载器。
func (plugin *iscsiPlugin) newUnmounterInternal(volName string, podUID types.UID, manager diskManager, mounter mount.Interface, exec utilexec.Interface) (volume.Unmounter, error) {
	return &iscsiDiskUnmounter{
		iscsiDisk: &iscsiDisk{
			podUID:          podUID,
			VolName:         volName,
			manager:         manager,
			plugin:          plugin,
			MetricsProvider: volume.NewMetricsStatFS(plugin.host.GetPodVolumeDir(podUID, utilstrings.EscapeQualifiedName(iscsiPluginName), volName)),
		},
		mounter:    mounter,
		exec:       exec,
		deviceUtil: ioutil.NewDeviceHandler(ioutil.NewIOHandler()),
	}, nil
}

// NewBlockVolumeUnmapper 根据可恢复状态创建块设备卷解映射器。
func (plugin *iscsiPlugin) NewBlockVolumeUnmapper(volName string, podUID types.UID) (volume.BlockVolumeUnmapper, error) {
	return plugin.newUnmapperInternal(volName, podUID, &ISCSIUtil{}, utilexec.New())
}

// newUnmapperInternal 使用指定的磁盘管理器和执行器创建解映射器。
func (plugin *iscsiPlugin) newUnmapperInternal(volName string, podUID types.UID, manager diskManager, exec utilexec.Interface) (volume.BlockVolumeUnmapper, error) {
	return &iscsiDiskUnmapper{
		iscsiDisk: &iscsiDisk{
			podUID:  podUID,
			VolName: volName,
			manager: manager,
			plugin:  plugin,
		},
		exec:       exec,
		deviceUtil: ioutil.NewDeviceHandler(ioutil.NewIOHandler()),
	}, nil
}

// ConstructVolumeSpec 从 Pod 卷挂载点重建 iSCSI 卷规格。
func (plugin *iscsiPlugin) ConstructVolumeSpec(volumeName, mountPath string) (volume.ReconstructedVolume, error) {
	// 从 Pod 卷目录（mountPath）反向查找全局挂载路径
	var globalPDPath string
	mounter := plugin.host.GetMounter()
	// 必须尽力找到全局挂载点：否则全局挂载仍保留，卷却被标记为未使用，
	// 随后可能被挂载到多个节点，导致数据损坏。
	paths, err := ioutil.GetReliableMountRefs(mounter, mountPath)
	if io.IsInconsistentReadError(err) {
		klog.Errorf("Failed to read mount refs from /proc/mounts for %s: %s", mountPath, err)
		klog.Errorf("Kubelet cannot unmount volume at %s, please unmount it and all mounts of the same device manually.", mountPath)
		return volume.ReconstructedVolume{}, err
	}
	if err != nil {
		return volume.ReconstructedVolume{}, err
	}

	for _, path := range paths {
		if strings.Contains(path, plugin.host.GetPluginDir(iscsiPluginName)) {
			globalPDPath = path
			break
		}
	}
	// 未找到全局挂载路径
	if len(globalPDPath) == 0 {
		return volume.ReconstructedVolume{}, fmt.Errorf("couldn't fetch globalPDPath. failed to obtain volume spec")
	}

	// 从全局挂载路径中解析 iSCSI 磁盘配置
	device, _, err := extractDeviceAndPrefix(globalPDPath)
	if err != nil {
		return volume.ReconstructedVolume{}, err
	}
	bkpPortal, iqn, err := extractPortalAndIqn(device)
	if err != nil {
		return volume.ReconstructedVolume{}, err
	}
	arr := strings.Split(device, "-lun-")
	if len(arr) < 2 {
		return volume.ReconstructedVolume{}, fmt.Errorf("failed to retrieve lun from globalPDPath: %v", globalPDPath)
	}
	lun, err := strconv.Atoi(arr[1])
	if err != nil {
		return volume.ReconstructedVolume{}, err
	}
	iface, _ := extractIface(globalPDPath)
	iscsiVolume := &v1.Volume{
		Name: volumeName,
		VolumeSource: v1.VolumeSource{
			ISCSI: &v1.ISCSIVolumeSource{
				TargetPortal:   bkpPortal,
				IQN:            iqn,
				Lun:            int32(lun),
				ISCSIInterface: iface,
			},
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

	return volume.ReconstructedVolume{
		Spec:                volume.NewSpecFromVolume(iscsiVolume),
		SELinuxMountContext: mountContext,
	}, nil
}

// ConstructBlockVolumeSpec 从块设备卷的映射路径重建卷规格。
func (plugin *iscsiPlugin) ConstructBlockVolumeSpec(podUID types.UID, volumeName, mapPath string) (*volume.Spec, error) {
	pluginDir := plugin.host.GetVolumeDevicePluginDir(iscsiPluginName)
	blkutil := volumepathhandler.NewBlockVolumePathHandler()
	globalMapPathUUID, err := blkutil.FindGlobalMapPathUUIDFromPod(pluginDir, mapPath, podUID)
	if err != nil {
		return nil, err
	}
	klog.V(5).Infof("globalMapPathUUID: %v, err: %v", globalMapPathUUID, err)
	// 从 globalMapPathUUID 中提取卷信息，示例：
	// plugins/kubernetes.io/{PluginName}/{DefaultKubeletVolumeDevicesDirName}/{volumePluginDependentPath}/{pod uuid}
	// plugins/kubernetes.io/iscsi/volumeDevices/iface-default/192.168.0.10:3260-iqn.2017-05.com.example:test-lun-0/{pod uuid}
	globalMapPath := filepath.Dir(globalMapPathUUID)
	return getVolumeSpecFromGlobalMapPath(volumeName, globalMapPath)
}

// iscsiDisk 保存 iSCSI 卷的目标配置与运行时状态。
type iscsiDisk struct {
	VolName       string
	podUID        types.UID
	Portals       []string
	Iqn           string
	Lun           string
	InitIface     string
	Iface         string
	chapDiscovery bool
	chapSession   bool
	secret        map[string]string `datapolicy:"token"`
	InitiatorName string
	plugin        *iscsiPlugin
	// 提供磁盘附加/分离操作的工具接口。
	manager diskManager
	volume.MetricsProvider
}

// GetPath 返回该卷在 Pod 目录下的挂载路径。
func (iscsi *iscsiDisk) GetPath() string {
	name := iscsiPluginName
	// 卷拆解发生在 Pod 清理之前，此时使用 PodVolumeDir 是安全的
	return iscsi.plugin.host.GetPodVolumeDir(iscsi.podUID, utilstrings.EscapeQualifiedName(name), iscsi.VolName)
}

// iscsiGlobalMapPath 返回块设备卷的全局映射路径。
func (iscsi *iscsiDisk) iscsiGlobalMapPath(spec *volume.Spec) (string, error) {
	mounter, err := volumeSpecToMounter(spec, iscsi.plugin.host, iscsi.plugin.targetLocks, nil /* pod */)
	if err != nil {
		klog.Warningf("failed to get iscsi mounter: %v", err)
		return "", err
	}
	return iscsi.manager.MakeGlobalVDPDName(*mounter.iscsiDisk), nil
}

// iscsiPodDeviceMapPath 返回 Pod 内块设备映射目录及卷名。
func (iscsi *iscsiDisk) iscsiPodDeviceMapPath() (string, string) {
	name := iscsiPluginName
	return iscsi.plugin.host.GetPodVolumeDeviceDir(iscsi.podUID, utilstrings.EscapeQualifiedName(name)), iscsi.VolName
}

// iscsiDiskMounter 实现文件系统模式 iSCSI 卷的挂载。
type iscsiDiskMounter struct {
	*iscsiDisk
	readOnly                  bool
	fsType                    string
	volumeMode                v1.PersistentVolumeMode
	mounter                   *mount.SafeFormatAndMount
	exec                      utilexec.Interface
	deviceUtil                ioutil.DeviceUtil
	mountOptions              []string
	mountedWithSELinuxContext bool
}

var _ volume.Mounter = &iscsiDiskMounter{}

// GetAttributes 返回卷属性，是否重打 SELinux 标签取决于挂载时是否使用了 context 选项。
func (b *iscsiDiskMounter) GetAttributes() volume.Attributes {
	return volume.Attributes{
		ReadOnly:       b.readOnly,
		Managed:        !b.readOnly,
		SELinuxRelabel: !b.mountedWithSELinuxContext,
	}
}

// SetUp 将卷挂载到默认路径。
func (b *iscsiDiskMounter) SetUp(mounterArgs volume.MounterArgs) error {
	return b.SetUpAt(b.GetPath(), mounterArgs)
}

// SetUpAt 将卷挂载到指定目录。
func (b *iscsiDiskMounter) SetUpAt(dir string, mounterArgs volume.MounterArgs) error {
	// diskSetUp 会检查挂载点，避免重复挂载
	err := diskSetUp(b.manager, *b, dir, b.mounter, mounterArgs)
	if err != nil {
		klog.Errorf("iscsi: failed to setup")
	}

	if utilfeature.DefaultFeatureGate.Enabled(features.SELinuxMountReadWriteOncePod) {
		// 卷在 MountDevice 阶段应该已带 -o context 挂载。
		// TODO: 是否在 GetAttributes() 中从挂载表解析以确认？
		b.mountedWithSELinuxContext = mounterArgs.SELinuxLabel != ""
	}
	return err
}

// iscsiDiskUnmounter 实现文件系统模式 iSCSI 卷的卸载。
type iscsiDiskUnmounter struct {
	*iscsiDisk
	mounter    mount.Interface
	exec       utilexec.Interface
	deviceUtil ioutil.DeviceUtil
}

var _ volume.Unmounter = &iscsiDiskUnmounter{}

// TearDown 解除绑定挂载；仅当该磁盘在 kubelet 上已无其他引用时才分离设备。
func (c *iscsiDiskUnmounter) TearDown() error {
	return c.TearDownAt(c.GetPath())
}

// TearDownAt 清理指定目录的挂载点。
func (c *iscsiDiskUnmounter) TearDownAt(dir string) error {
	return mount.CleanupMountPoint(dir, c.mounter, false)
}

// iscsiDiskMapper 实现块设备模式 iSCSI 卷的映射。
type iscsiDiskMapper struct {
	*iscsiDisk
	readOnly   bool
	exec       utilexec.Interface
	deviceUtil ioutil.DeviceUtil
}

var _ volume.BlockVolumeMapper = &iscsiDiskMapper{}

// iscsiDiskUnmapper 实现块设备模式 iSCSI 卷的解映射。
type iscsiDiskUnmapper struct {
	*iscsiDisk
	exec       utilexec.Interface
	deviceUtil ioutil.DeviceUtil
	volume.MetricsNil
}

// SupportsMetrics 返回 true：映射器已初始化 MetricsProvider。
func (idm *iscsiDiskMapper) SupportsMetrics() bool {
	return true
}

var _ volume.BlockVolumeUnmapper = &iscsiDiskUnmapper{}
var _ volume.CustomBlockVolumeUnmapper = &iscsiDiskUnmapper{}

// 尽管 iSCSI 插件有 attacher/detacher 实现，但仍需在 TearDownDevice() 中分离卷：
// 这是卷拆解流程中唯一在 kubelet 节点上执行操作的机会。
func (c *iscsiDiskUnmapper) TearDownDevice(mapPath, _ string) error {
	err := c.manager.DetachBlockISCSIDisk(*c, mapPath)
	if err != nil {
		return fmt.Errorf("iscsi: failed to detach disk: %s\nError: %v", mapPath, err)
	}
	klog.V(4).Infof("iscsi: %q is unmounted, deleting the directory", mapPath)
	err = os.RemoveAll(mapPath)
	if err != nil {
		return fmt.Errorf("iscsi: failed to delete the directory: %s\nError: %v", mapPath, err)
	}
	klog.V(4).Infof("iscsi: successfully detached disk: %s", mapPath)
	return nil
}

// UnmapPodDevice 为空操作：iSCSI 在 TearDownDevice 中已完成分离。
func (c *iscsiDiskUnmapper) UnmapPodDevice() error {
	return nil
}

// GetGlobalMapPath 返回块设备卷的全局映射路径：
// plugins/kubernetes.io/{PluginName}/volumeDevices/{ifaceName}/{portal-some_iqn-lun-lun_id}
func (iscsi *iscsiDisk) GetGlobalMapPath(spec *volume.Spec) (string, error) {
	return iscsi.iscsiGlobalMapPath(spec)
}

// GetPodDeviceMapPath 返回 Pod 内的设备映射目录和卷名：
// 路径 pods/{podUid}/volumeDevices/kubernetes.io~iscsi，卷名如 pv0001。
func (iscsi *iscsiDisk) GetPodDeviceMapPath() (string, string) {
	return iscsi.iscsiPodDeviceMapPath()
}

// portalMounter 给不含端口的 portal 补上默认端口 3260。
func portalMounter(portal string) string {
	if !strings.Contains(portal, ":") {
		portal = portal + ":3260"
	}
	return portal
}

// getISCSIVolumeInfo 提取 iSCSI 卷的只读标志和文件系统类型。
func getISCSIVolumeInfo(spec *volume.Spec) (bool, string, error) {
	// 内联卷的只读标志在卷规格中；持久卷的标志通过 PVC 卷源间接传入 spec.ReadOnly
	if spec.Volume != nil && spec.Volume.ISCSI != nil {
		return spec.Volume.ISCSI.ReadOnly, spec.Volume.ISCSI.FSType, nil
	} else if spec.PersistentVolume != nil &&
		spec.PersistentVolume.Spec.ISCSI != nil {
		return spec.ReadOnly, spec.PersistentVolume.Spec.ISCSI.FSType, nil
	}

	return false, "", fmt.Errorf("Spec does not reference an ISCSI volume type")
}

// getISCSITargetInfo 提取 iSCSI 目标信息：主 portal、备选 portal、IQN 和 lun。
func getISCSITargetInfo(spec *volume.Spec) (string, []string, string, int32, error) {
	if spec.Volume != nil && spec.Volume.ISCSI != nil {
		return spec.Volume.ISCSI.TargetPortal, spec.Volume.ISCSI.Portals, spec.Volume.ISCSI.IQN, spec.Volume.ISCSI.Lun, nil
	} else if spec.PersistentVolume != nil &&
		spec.PersistentVolume.Spec.ISCSI != nil {
		return spec.PersistentVolume.Spec.ISCSI.TargetPortal, spec.PersistentVolume.Spec.ISCSI.Portals, spec.PersistentVolume.Spec.ISCSI.IQN, spec.PersistentVolume.Spec.ISCSI.Lun, nil
	}

	return "", nil, "", 0, fmt.Errorf("Spec does not reference an ISCSI volume type")
}

// getISCSIInitiatorInfo 提取 iSCSI 发起端信息：接口名和发起端名称。
func getISCSIInitiatorInfo(spec *volume.Spec) (string, *string, error) {
	if spec.Volume != nil && spec.Volume.ISCSI != nil {
		return spec.Volume.ISCSI.ISCSIInterface, spec.Volume.ISCSI.InitiatorName, nil
	} else if spec.PersistentVolume != nil &&
		spec.PersistentVolume.Spec.ISCSI != nil {
		return spec.PersistentVolume.Spec.ISCSI.ISCSIInterface, spec.PersistentVolume.Spec.ISCSI.InitiatorName, nil
	}

	return "", nil, fmt.Errorf("Spec does not reference an ISCSI volume type")
}

// getISCSIDiscoveryCHAPInfo 提取是否启用发现阶段 CHAP 认证。
func getISCSIDiscoveryCHAPInfo(spec *volume.Spec) (bool, error) {
	if spec.Volume != nil && spec.Volume.ISCSI != nil {
		return spec.Volume.ISCSI.DiscoveryCHAPAuth, nil
	} else if spec.PersistentVolume != nil &&
		spec.PersistentVolume.Spec.ISCSI != nil {
		return spec.PersistentVolume.Spec.ISCSI.DiscoveryCHAPAuth, nil
	}

	return false, fmt.Errorf("Spec does not reference an ISCSI volume type")
}

// getISCSISessionCHAPInfo 提取是否启用会话阶段 CHAP 认证。
func getISCSISessionCHAPInfo(spec *volume.Spec) (bool, error) {
	if spec.Volume != nil && spec.Volume.ISCSI != nil {
		return spec.Volume.ISCSI.SessionCHAPAuth, nil
	} else if spec.PersistentVolume != nil &&
		spec.PersistentVolume.Spec.ISCSI != nil {
		return spec.PersistentVolume.Spec.ISCSI.SessionCHAPAuth, nil
	}

	return false, fmt.Errorf("Spec does not reference an ISCSI volume type")
}

// getISCSISecretNameAndNamespace 提取 CHAP 认证 Secret 的名称和命名空间。
func getISCSISecretNameAndNamespace(spec *volume.Spec, defaultSecretNamespace string) (string, string, error) {
	if spec.Volume != nil && spec.Volume.ISCSI != nil {
		if spec.Volume.ISCSI.SecretRef != nil {
			return spec.Volume.ISCSI.SecretRef.Name, defaultSecretNamespace, nil
		}
		return "", "", nil
	} else if spec.PersistentVolume != nil &&
		spec.PersistentVolume.Spec.ISCSI != nil {
		secretRef := spec.PersistentVolume.Spec.ISCSI.SecretRef
		secretNs := defaultSecretNamespace
		if secretRef != nil {
			if len(secretRef.Namespace) != 0 {
				secretNs = secretRef.Namespace
			}
			return secretRef.Name, secretNs, nil
		}
		return "", "", nil
	}

	return "", "", fmt.Errorf("Spec does not reference an ISCSI volume type")
}

// createISCSIDisk 根据卷规格和 CHAP Secret 构造 iSCSI 磁盘对象。
func createISCSIDisk(spec *volume.Spec, podUID types.UID, plugin *iscsiPlugin, manager diskManager, secret map[string]string) (*iscsiDisk, error) {
	tp, portals, iqn, lunStr, err := getISCSITargetInfo(spec)
	if err != nil {
		return nil, err
	}

	lun := strconv.Itoa(int(lunStr))
	portal := portalMounter(tp)
	var bkportal []string
	bkportal = append(bkportal, portal)
	for _, p := range portals {
		bkportal = append(bkportal, portalMounter(string(p)))
	}

	iface, initiatorNamePtr, err := getISCSIInitiatorInfo(spec)
	if err != nil {
		return nil, err
	}

	var initiatorName string
	if initiatorNamePtr != nil {
		initiatorName = *initiatorNamePtr
	}
	chapDiscovery, err := getISCSIDiscoveryCHAPInfo(spec)
	if err != nil {
		return nil, err
	}
	chapSession, err := getISCSISessionCHAPInfo(spec)
	if err != nil {
		return nil, err
	}

	initIface := iface
	if initiatorName != "" {
		iface = bkportal[0] + ":" + spec.Name()
	}

	return &iscsiDisk{
		podUID:        podUID,
		VolName:       spec.Name(),
		Portals:       bkportal,
		Iqn:           iqn,
		Lun:           lun,
		InitIface:     initIface,
		Iface:         iface,
		chapDiscovery: chapDiscovery,
		chapSession:   chapSession,
		secret:        secret,
		InitiatorName: initiatorName,
		manager:       manager,
		plugin:        plugin}, nil
}

// createSecretMap 从 API 读取 CHAP Secret，若未启用认证则返回 nil。
func createSecretMap(spec *volume.Spec, plugin *iscsiPlugin, namespace string) (map[string]string, error) {
	var secret map[string]string
	chapDiscover, err := getISCSIDiscoveryCHAPInfo(spec)
	if err != nil {
		return nil, err
	}
	chapSession, err := getISCSISessionCHAPInfo(spec)
	if err != nil {
		return nil, err
	}
	if chapDiscover || chapSession {
		secretName, secretNamespace, err := getISCSISecretNameAndNamespace(spec, namespace)
		if err != nil {
			return nil, err
		}

		if len(secretName) > 0 && len(secretNamespace) > 0 {
			// 指定了 Secret 时从 API 读取
			kubeClient := plugin.host.GetKubeClient()
			if kubeClient == nil {
				return nil, fmt.Errorf("cannot get kube client")
			}
			secretObj, err := kubeClient.CoreV1().Secrets(secretNamespace).Get(context.TODO(), secretName, metav1.GetOptions{})
			if err != nil {
				err = fmt.Errorf("couldn't get secret %v/%v error: %w", secretNamespace, secretName, err)
				return nil, err
			}
			secret = make(map[string]string)
			for name, data := range secretObj.Data {
				klog.V(4).Infof("retrieving CHAP secret name: %s", name)
				secret[name] = string(data)
			}
		}
	}
	return secret, err
}

// createPersistentVolumeFromISCSIPVSource 用给定的 iSCSI 卷源构造块设备模式的持久卷。
func createPersistentVolumeFromISCSIPVSource(volumeName string, iscsi v1.ISCSIPersistentVolumeSource) *v1.PersistentVolume {
	block := v1.PersistentVolumeBlock
	return &v1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: volumeName,
		},
		Spec: v1.PersistentVolumeSpec{
			PersistentVolumeSource: v1.PersistentVolumeSource{
				ISCSI: &iscsi,
			},
			VolumeMode: &block,
		},
	}
}

// getVolumeSpecFromGlobalMapPath 从块设备卷的全局映射路径重建卷规格。
func getVolumeSpecFromGlobalMapPath(volumeName, globalMapPath string) (*volume.Spec, error) {
	// 从 globalMapPath 中提取卷规格信息，示例：
	// plugins/kubernetes.io/{PluginName}/{DefaultKubeletVolumeDevicesDirName}/{volumePluginDependentPath}
	// plugins/kubernetes.io/iscsi/volumeDevices/iface-default/192.168.0.10:3260-iqn.2017-05.com.example:test-lun-0

	// 其中 device 部分为 192.168.0.10:3260-iqn.2017-05.com.example:test-lun-0
	device, _, err := extractDeviceAndPrefix(globalMapPath)
	if err != nil {
		return nil, err
	}
	bkpPortal, iqn, err := extractPortalAndIqn(device)
	if err != nil {
		return nil, err
	}
	arr := strings.Split(device, "-lun-")
	if len(arr) < 2 {
		return nil, fmt.Errorf("failed to retrieve lun from globalMapPath: %v", globalMapPath)
	}
	lun, err := strconv.Atoi(arr[1])
	if err != nil {
		return nil, err
	}
	iface, found := extractIface(globalMapPath)
	if !found {
		return nil, fmt.Errorf("failed to retrieve iface from globalMapPath: %v", globalMapPath)
	}
	iscsiPV := createPersistentVolumeFromISCSIPVSource(volumeName,
		v1.ISCSIPersistentVolumeSource{
			TargetPortal:   bkpPortal,
			IQN:            iqn,
			Lun:            int32(lun),
			ISCSIInterface: iface,
		},
	)
	klog.V(5).Infof("ConstructBlockVolumeSpec: TargetPortal: %v, IQN: %v, Lun: %v, ISCSIInterface: %v",
		iscsiPV.Spec.PersistentVolumeSource.ISCSI.TargetPortal,
		iscsiPV.Spec.PersistentVolumeSource.ISCSI.IQN,
		iscsiPV.Spec.PersistentVolumeSource.ISCSI.Lun,
		iscsiPV.Spec.PersistentVolumeSource.ISCSI.ISCSIInterface,
	)
	return volume.NewSpecFromPersistentVolume(iscsiPV, false), nil
}
