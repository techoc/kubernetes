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

package downwardapi

import (
	"fmt"
	"path/filepath"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/api/v1/resource"
	"k8s.io/kubernetes/pkg/fieldpath"
	"k8s.io/kubernetes/pkg/volume"
	volumeutil "k8s.io/kubernetes/pkg/volume/util"
	utilstrings "k8s.io/utils/strings"
)

// ProbeVolumePlugins 是包内插件探测的入口，返回 downwardAPI 卷插件实例。
func ProbeVolumePlugins() []volume.VolumePlugin {
	return []volume.VolumePlugin{&downwardAPIPlugin{}}
}

// downwardAPI 插件名称。
const (
	downwardAPIPluginName = "kubernetes.io/downward-api"
)

// downwardAPIPlugin 实现 VolumePlugin 接口，暴露 Pod 元数据给容器。
type downwardAPIPlugin struct {
	host volume.VolumeHost
}

// 编译时断言 downwardAPIPlugin 实现 volume.VolumePlugin 接口。
var _ volume.VolumePlugin = &downwardAPIPlugin{}

// getPath 返回 downwardAPI 卷在宿主机上的挂载目录。
func getPath(uid types.UID, volName string, host volume.VolumeHost) string {
	return host.GetPodVolumeDir(uid, utilstrings.EscapeQualifiedName(downwardAPIPluginName), volName)
}

// wrappedVolumeSpec 返回内存 EmptyDir 规格，作为 downwardAPI 卷的底层承载卷。
func wrappedVolumeSpec() volume.Spec {
	return volume.Spec{
		Volume: &v1.Volume{VolumeSource: v1.VolumeSource{EmptyDir: &v1.EmptyDirVolumeSource{Medium: v1.StorageMediumMemory}}},
	}
}

// Init 初始化插件，保存 VolumeHost 供后续使用。
func (plugin *downwardAPIPlugin) Init(host volume.VolumeHost) error {
	plugin.host = host
	return nil
}

// GetPluginName 返回插件名称。
func (plugin *downwardAPIPlugin) GetPluginName() string {
	return downwardAPIPluginName
}

// GetVolumeName 校验规格引用的是 downwardAPI 卷，并返回用户定义的卷名。
func (plugin *downwardAPIPlugin) GetVolumeName(spec *volume.Spec) (string, error) {
	volumeSource, _ := getVolumeSource(spec)
	if volumeSource == nil {
		return "", fmt.Errorf("Spec does not reference a DownwardAPI volume type")
	}

	// 临时卷没有持久标识，返回用户定义的卷名即可。
	return spec.Name(), nil
}

// CanSupport 判断规格是否为 downwardAPI 卷。
func (plugin *downwardAPIPlugin) CanSupport(spec *volume.Spec) bool {
	return spec.Volume != nil && spec.Volume.DownwardAPI != nil
}

// RequiresRemount 始终返回 true：downwardAPI 内容需随 kubelet 同步周期刷新。
func (plugin *downwardAPIPlugin) RequiresRemount(spec *volume.Spec) bool {
	return true
}

// SupportsMountOption 返回 false，downwardAPI 卷不支持挂载选项。
func (plugin *downwardAPIPlugin) SupportsMountOption() bool {
	return false
}

// SupportsSELinuxContextMount 返回 false，不支持通过挂载选项设置 SELinux 上下文。
func (plugin *downwardAPIPlugin) SupportsSELinuxContextMount(spec *volume.Spec) (bool, error) {
	return false, nil
}

// NewMounter 为 Pod 创建 downwardAPI 卷的 Mounter，并挂载缓存的用量指标提供者。
func (plugin *downwardAPIPlugin) NewMounter(spec *volume.Spec, pod *v1.Pod) (volume.Mounter, error) {
	v := &downwardAPIVolume{
		volName:         spec.Name(),
		items:           spec.Volume.DownwardAPI.Items,
		pod:             pod,
		podUID:          pod.UID,
		plugin:          plugin,
		MetricsProvider: volume.NewCachedMetrics(volume.NewMetricsDu(getPath(pod.UID, spec.Name(), plugin.host))),
	}
	return &downwardAPIVolumeMounter{
		downwardAPIVolume: v,
		source:            *spec.Volume.DownwardAPI,
	}, nil
}

// NewUnmounter 根据卷名和 Pod UID 创建 downwardAPI 卷的 Unmounter。
func (plugin *downwardAPIPlugin) NewUnmounter(volName string, podUID types.UID) (volume.Unmounter, error) {
	return &downwardAPIVolumeUnmounter{
		&downwardAPIVolume{
			volName:         volName,
			podUID:          podUID,
			plugin:          plugin,
			MetricsProvider: volume.NewCachedMetrics(volume.NewMetricsDu(getPath(podUID, volName, plugin.host))),
		},
	}, nil
}

// ConstructVolumeSpec 根据卷名重建 downwardAPI 卷规格（仅用于卸载等场景）。
func (plugin *downwardAPIPlugin) ConstructVolumeSpec(volumeName, mountPath string) (volume.ReconstructedVolume, error) {
	downwardAPIVolume := &v1.Volume{
		Name: volumeName,
		VolumeSource: v1.VolumeSource{
			DownwardAPI: &v1.DownwardAPIVolumeSource{},
		},
	}
	return volume.ReconstructedVolume{
		Spec: volume.NewSpecFromVolume(downwardAPIVolume),
	}, nil
}

// downwardAPIVolume 保存 downwardAPI 卷的基本信息，用于生成文件并计算挂载路径。
type downwardAPIVolume struct {
	volName string
	items   []v1.DownwardAPIVolumeFile
	pod     *v1.Pod
	// TODO: remove this redundancy as soon NewUnmounter func will have *v1.POD and not only types.UID
	// podUID 与 pod.UID 重复，待 NewUnmounter 接收 *v1.Pod 后可移除。
	podUID types.UID
	plugin *downwardAPIPlugin
	volume.MetricsProvider
}

// downwardAPIVolumeMounter 从 Pod 获取 downwardAPI 信息并写入文件。
type downwardAPIVolumeMounter struct {
	*downwardAPIVolume
	source v1.DownwardAPIVolumeSource
}

// 编译时断言 downwardAPIVolumeMounter 实现 volume.Mounter 接口。
var _ volume.Mounter = &downwardAPIVolumeMounter{}

// GetAttributes 返回卷属性：downwardAPI 卷始终为只读且受 kubelet 管理。
func (d *downwardAPIVolume) GetAttributes() volume.Attributes {
	return volume.Attributes{
		ReadOnly:       true,
		Managed:        true,
		SELinuxRelabel: true,
	}
}

// SetUp 设置卷。
// 该函数故意不做幂等处理：数据需要周期性刷新，由 kubelet 内部同步周期驱动。
// TODO: Add volume specific ticker and refresh loop
func (b *downwardAPIVolumeMounter) SetUp(mounterArgs volume.MounterArgs) error {
	return b.SetUpAt(b.GetPath(), mounterArgs)
}

// SetUpAt 在指定目录设置 downwardAPI 卷：底层用内存 EmptyDir 承载，
// 收集字段数据后通过 AtomicWriter 原子写入；失败时清理目录。
func (b *downwardAPIVolumeMounter) SetUpAt(dir string, mounterArgs volume.MounterArgs) error {
	klog.V(3).Infof("Setting up a downwardAPI volume %v for pod %v/%v at %v", b.volName, b.pod.Namespace, b.pod.Name, dir)
	// 包装 EmptyDir，依赖其幂等性避免重复挂载。
	wrapped, err := b.plugin.host.NewWrapperMounter(b.volName, wrappedVolumeSpec(), b.pod)
	if err != nil {
		klog.Errorf("Couldn't setup downwardAPI volume %v for pod %v/%v: %s", b.volName, b.pod.Namespace, b.pod.Name, err.Error())
		return err
	}

	data, err := CollectData(b.source.Items, b.pod, b.plugin.host, b.source.DefaultMode, b.source.DefaultUser)
	if err != nil {
		klog.Errorf("Error preparing data for downwardAPI volume %v for pod %v/%v: %s", b.volName, b.pod.Namespace, b.pod.Name, err.Error())
		return err
	}

	setupSuccess := false
	if err := wrapped.SetUpAt(dir, mounterArgs); err != nil {
		klog.Errorf("Unable to setup downwardAPI volume %v for pod %v/%v: %s", b.volName, b.pod.Namespace, b.pod.Name, err.Error())
		return err
	}

	if err := volumeutil.MakeNestedMountpoints(b.volName, dir, *b.pod); err != nil {
		return err
	}

	defer func() {
		// 设置失败时清理目录，避免残留部分写入的卷。
		if !setupSuccess {
			unmounter, unmountCreateErr := b.plugin.NewUnmounter(b.volName, b.podUID)
			if unmountCreateErr != nil {
				klog.Errorf("error cleaning up mount %s after failure. Create unmounter failed with %v", b.volName, unmountCreateErr)
				return
			}
			tearDownErr := unmounter.TearDown()
			if tearDownErr != nil {
				klog.Errorf("error tearing down volume %s: %v", b.volName, tearDownErr)
			}
		}
	}()

	writerContext := fmt.Sprintf("pod %v/%v volume %v", b.pod.Namespace, b.pod.Name, b.volName)
	writer, err := volumeutil.NewAtomicWriter(dir, writerContext)
	if err != nil {
		klog.Errorf("Error creating atomic writer: %v", err)
		return err
	}

	setPerms := func(_ string) error {
		// 首次写入时新文件可能出现在时间戳子目录之外，
		// 因此对整个卷而非仅时间戳目录调整权限。
		ownershipChanger := volume.NewVolumeOwnership(b, dir, mounterArgs.FsGroup, nil /*fsGroupChangePolicy*/, volumeutil.FSGroupCompleteHook(b.plugin, nil))
		return ownershipChanger.ChangePermissions()
	}
	err = writer.Write(data, setPerms)
	if err != nil {
		klog.Errorf("Error writing payload to dir: %v", err)
		return err
	}

	setupSuccess = true
	return nil
}

// CollectData 收集请求的 downwardAPI 数据到 map 中。
// map 的键为要写入的文件名，值为字段内容（已排序）。
//
// 注意：导出该函数是为了供 projection 卷驱动调用。
func CollectData(items []v1.DownwardAPIVolumeFile, pod *v1.Pod, host volume.VolumeHost, defaultMode *int32, defaultUser *int64) (map[string]volumeutil.FileProjection, error) {
	if defaultMode == nil {
		return nil, fmt.Errorf("no defaultMode used, not even the default value for it")
	}

	errlist := []error{}
	data := make(map[string]volumeutil.FileProjection)
	for _, fileInfo := range items {
		var fileProjection volumeutil.FileProjection
		fPath := filepath.Clean(fileInfo.Path)

		fileProjection.FsUser = volumeutil.ResolvesFsUser(defaultUser, fileInfo.User)
		if fileInfo.Mode != nil {
			fileProjection.Mode = *fileInfo.Mode
		} else {
			fileProjection.Mode = *defaultMode
		}
		if fileInfo.FieldRef != nil {
			// TODO: unify with Kubelet.podFieldSelectorRuntimeValue
			// 字段路径提取逻辑应与 kubelet 的字段选择器统一，避免行为漂移。
			if values, err := fieldpath.ExtractFieldPathAsString(pod, fileInfo.FieldRef.FieldPath); err != nil {
				klog.Errorf("Unable to extract field %s: %s", fileInfo.FieldRef.FieldPath, err.Error())
				errlist = append(errlist, err)
			} else {
				fileProjection.Data = []byte(values)
			}
		} else if fileInfo.ResourceFieldRef != nil {
			containerName := fileInfo.ResourceFieldRef.ContainerName
			nodeAllocatable, err := host.GetNodeAllocatable()
			if err != nil {
				errlist = append(errlist, err)
			} else if values, err := resource.ExtractResourceValueByContainerNameAndNodeAllocatable(fileInfo.ResourceFieldRef, pod, containerName, nodeAllocatable); err != nil {
				klog.Errorf("Unable to extract field %s: %s", fileInfo.ResourceFieldRef.Resource, err.Error())
				errlist = append(errlist, err)
			} else {
				fileProjection.Data = []byte(values)
			}
		}

		data[fPath] = fileProjection
	}
	return data, utilerrors.NewAggregate(errlist)
}

// GetPath 返回卷在宿主机上的挂载目录。
func (d *downwardAPIVolume) GetPath() string {
	return d.plugin.host.GetPodVolumeDir(d.podUID, utilstrings.EscapeQualifiedName(downwardAPIPluginName), d.volName)
}

// downwardAPIVolumeUnmounter 负责清理 downwardAPI 卷。
type downwardAPIVolumeUnmounter struct {
	*downwardAPIVolume
}

// 编译时断言 downwardAPIVolumeUnmounter 实现 volume.Unmounter 接口。
var _ volume.Unmounter = &downwardAPIVolumeUnmounter{}

// TearDown 卸载默认路径下的 downwardAPI 卷。
func (c *downwardAPIVolumeUnmounter) TearDown() error {
	return c.TearDownAt(c.GetPath())
}

// TearDownAt 通过底层 EmptyDir 卸载指定目录。
func (c *downwardAPIVolumeUnmounter) TearDownAt(dir string) error {
	return volumeutil.UnmountViaEmptyDir(dir, c.plugin.host, c.volName, wrappedVolumeSpec(), c.podUID)
}

// getVolumeSource 从规格中提取 downwardAPI 卷源和只读标志。
func getVolumeSource(spec *volume.Spec) (*v1.DownwardAPIVolumeSource, bool) {
	var readOnly bool
	var volumeSource *v1.DownwardAPIVolumeSource

	if spec.Volume != nil && spec.Volume.DownwardAPI != nil {
		volumeSource = spec.Volume.DownwardAPI
		readOnly = spec.ReadOnly
	}

	return volumeSource, readOnly
}
