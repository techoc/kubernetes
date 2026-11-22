/*
Copyright 2014 The Kubernetes Authors.

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

package nfs

import (
	"fmt"
	"os"
	"time"

	netutil "k8s.io/utils/net"

	"k8s.io/klog/v2"
	"k8s.io/mount-utils"
	utilstrings "k8s.io/utils/strings"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/kubernetes/pkg/volume"
	"k8s.io/kubernetes/pkg/volume/util"
	"k8s.io/kubernetes/pkg/volume/util/recyclerclient"
)

// getPath 返回指定 Pod 下 NFS 卷的挂载目录。
func getPath(uid types.UID, volName string, host volume.VolumeHost) string {
	return host.GetPodVolumeDir(uid, utilstrings.EscapeQualifiedName(nfsPluginName), volName)
}

// ProbeVolumePlugins 是卷插件的主要入口点。
// volumeConfig 参数用于配置回收器行为，采用指针类型以便传入 nil。
// nfsPlugin 保存该配置，并在需要时传给创建 NFS 回收器的函数。
// 涉及回收逻辑的测试应改用 ProbeRecyclablePlugins() 覆盖默认行为。
func ProbeVolumePlugins(volumeConfig volume.VolumeConfig) []volume.VolumePlugin {
	return []volume.VolumePlugin{
		&nfsPlugin{
			host:   nil,
			config: volumeConfig,
		},
	}
}

// nfsPlugin 实现 NFS 卷插件。
type nfsPlugin struct {
	host   volume.VolumeHost
	config volume.VolumeConfig
}

var _ volume.VolumePlugin = &nfsPlugin{}
var _ volume.PersistentVolumePlugin = &nfsPlugin{}
var _ volume.RecyclableVolumePlugin = &nfsPlugin{}

const (
	nfsPluginName  = "kubernetes.io/nfs"
	unMountTimeout = time.Minute
)

// Init 初始化插件并保存卷宿主。
func (plugin *nfsPlugin) Init(host volume.VolumeHost) error {
	plugin.host = host
	return nil
}

// GetPluginName 返回插件名称。
func (plugin *nfsPlugin) GetPluginName() string {
	return nfsPluginName
}

// GetVolumeName 返回 "服务器/导出路径" 形式的卷唯一名称。
func (plugin *nfsPlugin) GetVolumeName(spec *volume.Spec) (string, error) {
	volumeSource, _, err := getVolumeSource(spec)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf(
		"%v/%v",
		volumeSource.Server,
		volumeSource.Path), nil
}

// CanSupport 判断卷规格是否为 NFS 类型（内联卷或持久卷均可）。
func (plugin *nfsPlugin) CanSupport(spec *volume.Spec) bool {
	return (spec.PersistentVolume != nil && spec.PersistentVolume.Spec.NFS != nil) ||
		(spec.Volume != nil && spec.Volume.NFS != nil)
}

// RequiresRemount 返回 false：NFS 卷无需重挂载。
func (plugin *nfsPlugin) RequiresRemount(spec *volume.Spec) bool {
	return false
}

// SupportsMountOption 返回 true：NFS 卷支持自定义挂载选项。
func (plugin *nfsPlugin) SupportsMountOption() bool {
	return true
}

// SupportsSELinuxContextMount 返回 false：不支持 SELinux 上下文挂载。
func (plugin *nfsPlugin) SupportsSELinuxContextMount(spec *volume.Spec) (bool, error) {
	return false, nil
}

// GetAccessModes 返回 NFS 卷支持的访问模式。
func (plugin *nfsPlugin) GetAccessModes() []v1.PersistentVolumeAccessMode {
	return []v1.PersistentVolumeAccessMode{
		v1.ReadWriteOnce,
		v1.ReadOnlyMany,
		v1.ReadWriteMany,
	}
}

// NewMounter 根据卷规格创建 NFS 挂载器。
func (plugin *nfsPlugin) NewMounter(spec *volume.Spec, pod *v1.Pod) (volume.Mounter, error) {
	return plugin.newMounterInternal(spec, pod, plugin.host.GetMounter())
}

// newMounterInternal 使用指定的 mounter 创建 NFS 挂载器。
func (plugin *nfsPlugin) newMounterInternal(spec *volume.Spec, pod *v1.Pod, mounter mount.Interface) (volume.Mounter, error) {
	source, readOnly, err := getVolumeSource(spec)
	if err != nil {
		return nil, err
	}
	return &nfsMounter{
		nfs: &nfs{
			volName:         spec.Name(),
			mounter:         mounter,
			pod:             pod,
			plugin:          plugin,
			MetricsProvider: volume.NewMetricsStatFS(getPath(pod.UID, spec.Name(), plugin.host)),
		},
		server:       getServerFromSource(source),
		exportPath:   source.Path,
		readOnly:     readOnly,
		mountOptions: util.MountOptionFromSpec(spec),
	}, nil
}

// NewUnmounter 根据卷名和 Pod UID 创建 NFS 卸载器。
func (plugin *nfsPlugin) NewUnmounter(volName string, podUID types.UID) (volume.Unmounter, error) {
	return plugin.newUnmounterInternal(volName, podUID, plugin.host.GetMounter())
}

// newUnmounterInternal 使用指定的 mounter 创建 NFS 卸载器。
func (plugin *nfsPlugin) newUnmounterInternal(volName string, podUID types.UID, mounter mount.Interface) (volume.Unmounter, error) {
	return &nfsUnmounter{&nfs{
		volName:         volName,
		mounter:         mounter,
		pod:             &v1.Pod{ObjectMeta: metav1.ObjectMeta{UID: podUID}},
		plugin:          plugin,
		MetricsProvider: volume.NewMetricsStatFS(getPath(podUID, volName, plugin.host)),
	}}, nil
}

// Recycle 清理 NFS 卷，阻塞直到回收 Pod 完成或出错。
func (plugin *nfsPlugin) Recycle(pvName string, spec *volume.Spec, eventRecorder recyclerclient.RecycleEventRecorder) error {
	if spec.PersistentVolume == nil || spec.PersistentVolume.Spec.NFS == nil {
		return fmt.Errorf("spec.PersistentVolumeSource.NFS is nil")
	}

	pod := plugin.config.RecyclerPodTemplate
	timeout := util.CalculateTimeoutForVolume(plugin.config.RecyclerMinimumTimeout, plugin.config.RecyclerTimeoutIncrement, spec.PersistentVolume)
	// 用计算出的超时和固定前缀覆盖模板中的默认值
	pod.Spec.ActiveDeadlineSeconds = &timeout
	pod.GenerateName = "pv-recycler-nfs-"
	pod.Spec.Volumes[0].VolumeSource = v1.VolumeSource{
		NFS: &v1.NFSVolumeSource{
			Server: spec.PersistentVolume.Spec.NFS.Server,
			Path:   spec.PersistentVolume.Spec.NFS.Path,
		},
	}
	return recyclerclient.RecycleVolumeByWatchingPodUntilCompletion(pvName, pod, plugin.host.GetKubeClient(), eventRecorder)
}

// ConstructVolumeSpec 从挂载点信息重建 NFS 卷规格。
func (plugin *nfsPlugin) ConstructVolumeSpec(volumeName, mountPath string) (volume.ReconstructedVolume, error) {
	nfsVolume := &v1.Volume{
		Name: volumeName,
		VolumeSource: v1.VolumeSource{
			NFS: &v1.NFSVolumeSource{
				Path: volumeName,
			},
		},
	}
	return volume.ReconstructedVolume{
		Spec: volume.NewSpecFromVolume(nfsVolume),
	}, nil
}

// nfs 表示 NFS 导出目录在宿主机上的一次裸挂载。
type nfs struct {
	volName string
	pod     *v1.Pod
	mounter mount.Interface
	plugin  *nfsPlugin
	volume.MetricsProvider
}

// GetPath 返回该卷在 Pod 目录下的挂载路径。
func (nfsVolume *nfs) GetPath() string {
	name := nfsPluginName
	return nfsVolume.plugin.host.GetPodVolumeDir(nfsVolume.pod.UID, utilstrings.EscapeQualifiedName(name), nfsVolume.volName)
}

// nfsMounter 实现 NFS 卷的挂载。
type nfsMounter struct {
	*nfs
	server       string
	exportPath   string
	readOnly     bool
	mountOptions []string
}

var _ volume.Mounter = &nfsMounter{}

// GetAttributes 返回卷属性，只读性由规格中的 readOnly 决定。
func (nfsMounter *nfsMounter) GetAttributes() volume.Attributes {
	return volume.Attributes{
		ReadOnly:       nfsMounter.readOnly,
		Managed:        false,
		SELinuxRelabel: false,
	}
}

// SetUp 将 NFS 导出挂载到卷路径上。
func (nfsMounter *nfsMounter) SetUp(mounterArgs volume.MounterArgs) error {
	return nfsMounter.SetUpAt(nfsMounter.GetPath(), mounterArgs)
}

// SetUpAt 将 NFS 导出挂载到指定目录。
func (nfsMounter *nfsMounter) SetUpAt(dir string, mounterArgs volume.MounterArgs) error {
	notMnt, err := mount.IsNotMountPoint(nfsMounter.mounter, dir)
	klog.V(4).Infof("NFS mount set up: %s %v %v", dir, !notMnt, err)
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	if !notMnt {
		return nil
	}
	if err := os.MkdirAll(dir, 0750); err != nil {
		return err
	}
	source := fmt.Sprintf("%s:%s", nfsMounter.server, nfsMounter.exportPath)
	options := []string{}
	if nfsMounter.readOnly {
		options = append(options, "ro")
	}
	mountOptions := util.JoinMountOptions(nfsMounter.mountOptions, options)
	err = nfsMounter.mounter.MountSensitiveWithoutSystemd(source, dir, "nfs", mountOptions, nil)
	if err != nil {
		notMnt, mntErr := mount.IsNotMountPoint(nfsMounter.mounter, dir)
		if mntErr != nil {
			klog.Errorf("IsNotMountPoint check failed: %v", mntErr)
			return err
		}
		if !notMnt {
			if mntErr = nfsMounter.mounter.Unmount(dir); mntErr != nil {
				klog.Errorf("Failed to unmount: %v", mntErr)
				return err
			}
			notMnt, mntErr := mount.IsNotMountPoint(nfsMounter.mounter, dir)
			if mntErr != nil {
				klog.Errorf("IsNotMountPoint check failed: %v", mntErr)
				return err
			}
			if !notMnt {
				// 理论上不应出现：卸载后仍能检测到挂载点，留给下个同步周期重试。
				klog.Errorf("%s is still mounted, despite call to unmount().  Will try again next sync loop.", dir)
				return err
			}
		}
		os.Remove(dir)
		return err
	}
	return nil
}

var _ volume.Unmounter = &nfsUnmounter{}

// nfsUnmounter 实现 NFS 卷的卸载。
type nfsUnmounter struct {
	*nfs
}

// TearDown 卸载卷的默认挂载路径。
func (c *nfsUnmounter) TearDown() error {
	return c.TearDownAt(c.GetPath())
}

// TearDownAt 卸载指定目录上的 NFS 挂载。
func (c *nfsUnmounter) TearDownAt(dir string) error {
	// 使用检查 /proc/mounts 的彻底检测方式。NFS 服务端可能启用了 root_squash，
	// kubelet 无法在挂载点执行 lstat/stat，因此不能用更快的 IsLikelyNotMountPoint。
	forceUnmounter, ok := c.mounter.(mount.MounterForceUnmounter)
	if ok {
		klog.V(4).Infof("Using force unmounter interface")
		return mount.CleanupMountWithForce(dir, forceUnmounter, true /* extensiveMountPointCheck */, unMountTimeout)
	}
	return mount.CleanupMountPoint(dir, c.mounter, true /* extensiveMountPointCheck */)
}

// getVolumeSource 从卷规格中提取 NFS 卷源及只读标志，非 NFS 卷返回错误。
func getVolumeSource(spec *volume.Spec) (*v1.NFSVolumeSource, bool, error) {
	if spec.Volume != nil && spec.Volume.NFS != nil {
		return spec.Volume.NFS, spec.Volume.NFS.ReadOnly, nil
	} else if spec.PersistentVolume != nil &&
		spec.PersistentVolume.Spec.NFS != nil {
		return spec.PersistentVolume.Spec.NFS, spec.ReadOnly, nil
	}

	return nil, false, fmt.Errorf("Spec does not reference a NFS volume type")
}

// getServerFromSource 将 IPv6 地址用方括号包裹，以便拼入 NFS 挂载源。
func getServerFromSource(source *v1.NFSVolumeSource) string {
	if netutil.IsIPv6String(source.Server) {
		return fmt.Sprintf("[%s]", source.Server)
	}
	return source.Server
}
