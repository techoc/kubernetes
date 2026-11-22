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

// Package configmap 实现 ConfigMap 卷插件，将 API Server 中的 ConfigMap 数据以文件形式挂载到 Pod 中。
// 卷内容通过 AtomicWriter 原子写入，kubelet 定期同步时按需更新，无需重启容器。
package configmap

import (
	"fmt"

	"k8s.io/klog/v2"
	"k8s.io/mount-utils"
	utilstrings "k8s.io/utils/strings"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/pkg/volume"
	volumeutil "k8s.io/kubernetes/pkg/volume/util"
)

// ProbeVolumePlugins 是包内卷插件探测的入口，返回可供 kubelet 注册的插件列表。
func ProbeVolumePlugins() []volume.VolumePlugin {
	return []volume.VolumePlugin{&configMapPlugin{}}
}

const (
	configMapPluginName = "kubernetes.io/configmap"
)

// configMapPlugin 实现 VolumePlugin 接口，负责创建 ConfigMap 卷的 mounter 和 unmounter。
type configMapPlugin struct {
	host         volume.VolumeHost
	getConfigMap func(namespace, name string) (*v1.ConfigMap, error)
}

var _ volume.VolumePlugin = &configMapPlugin{}

func getPath(uid types.UID, volName string, host volume.VolumeHost) string {
	return host.GetPodVolumeDir(uid, utilstrings.EscapeQualifiedName(configMapPluginName), volName)
}

// Init 初始化插件，保存 VolumeHost 并获取 ConfigMap 查询函数。
func (plugin *configMapPlugin) Init(host volume.VolumeHost) error {
	plugin.host = host
	plugin.getConfigMap = host.GetConfigMapFunc()
	return nil
}

// GetPluginName 返回插件名称。
func (plugin *configMapPlugin) GetPluginName() string {
	return configMapPluginName
}

// GetVolumeName 返回卷的唯一标识，格式为 "<卷名>/<ConfigMap 名>"。
func (plugin *configMapPlugin) GetVolumeName(spec *volume.Spec) (string, error) {
	volumeSource, _ := getVolumeSource(spec)
	if volumeSource == nil {
		return "", fmt.Errorf("Spec does not reference a ConfigMap volume type")
	}

	return fmt.Sprintf(
		"%v/%v",
		spec.Name(),
		volumeSource.Name), nil
}

// CanSupport 判断卷规格是否引用 ConfigMap 卷源。
func (plugin *configMapPlugin) CanSupport(spec *volume.Spec) bool {
	return spec.Volume != nil && spec.Volume.ConfigMap != nil
}

// RequiresRemount 返回 true，ConfigMap 内容变化时需要重新挂载以同步数据。
func (plugin *configMapPlugin) RequiresRemount(spec *volume.Spec) bool {
	return true
}

// SupportsMountOption 返回 false，该插件不支持挂载选项。
func (plugin *configMapPlugin) SupportsMountOption() bool {
	return false
}

// SupportsSELinuxContextMount 返回 false，该插件不支持 SELinux 上下文挂载。
func (plugin *configMapPlugin) SupportsSELinuxContextMount(spec *volume.Spec) (bool, error) {
	return false, nil
}

// NewMounter 为 Pod 的 ConfigMap 卷创建 mounter。
func (plugin *configMapPlugin) NewMounter(spec *volume.Spec, pod *v1.Pod) (volume.Mounter, error) {
	return &configMapVolumeMounter{
		configMapVolume: &configMapVolume{
			spec.Name(),
			pod.UID,
			plugin,
			plugin.host.GetMounter(),
			volume.NewCachedMetrics(volume.NewMetricsDu(getPath(pod.UID, spec.Name(), plugin.host))),
		},
		source:       *spec.Volume.ConfigMap,
		pod:          *pod,
		getConfigMap: plugin.getConfigMap,
	}, nil
}

// NewUnmounter 为指定 Pod 的 ConfigMap 卷创建 unmounter。
func (plugin *configMapPlugin) NewUnmounter(volName string, podUID types.UID) (volume.Unmounter, error) {
	return &configMapVolumeUnmounter{
		&configMapVolume{
			volName,
			podUID,
			plugin,
			plugin.host.GetMounter(),
			volume.NewCachedMetrics(volume.NewMetricsDu(getPath(podUID, volName, plugin.host))),
		},
	}, nil
}

// ConstructVolumeSpec 根据已挂载的卷名重建 ConfigMap 卷规格，用于节点重启后的卷恢复场景。
func (plugin *configMapPlugin) ConstructVolumeSpec(volumeName, mountPath string) (volume.ReconstructedVolume, error) {
	configMapVolume := &v1.Volume{
		Name: volumeName,
		VolumeSource: v1.VolumeSource{
			ConfigMap: &v1.ConfigMapVolumeSource{},
		},
	}
	return volume.ReconstructedVolume{
		Spec: volume.NewSpecFromVolume(configMapVolume),
	}, nil
}

// configMapVolume 保存 ConfigMap 卷的公共状态：卷名、所属 Pod、插件、mounter 和指标。
type configMapVolume struct {
	volName string
	podUID  types.UID
	plugin  *configMapPlugin
	mounter mount.Interface
	volume.MetricsProvider
}

var _ volume.Volume = &configMapVolume{}

// GetPath 返回该卷在主机上的挂载路径。
func (sv *configMapVolume) GetPath() string {
	return sv.plugin.host.GetPodVolumeDir(sv.podUID, utilstrings.EscapeQualifiedName(configMapPluginName), sv.volName)
}

// configMapVolumeMounter 从 API Server 获取 ConfigMap 数据，并以文件形式写入主机上的卷目录。
type configMapVolumeMounter struct {
	*configMapVolume

	source       v1.ConfigMapVolumeSource
	pod          v1.Pod
	getConfigMap func(namespace, name string) (*v1.ConfigMap, error)
}

var _ volume.Mounter = &configMapVolumeMounter{}

// GetAttributes 返回卷属性：只读、由 kubelet 管理、需要 SELinux 重新打标。
func (sv *configMapVolume) GetAttributes() volume.Attributes {
	return volume.Attributes{
		ReadOnly:       true,
		Managed:        true,
		SELinuxRelabel: true,
	}
}

func wrappedVolumeSpec() volume.Spec {
	// 这是插件内部包装所用的卷规格：借用 EmptyDir 完成实际的目录创建和生命周期管理。
	return volume.Spec{
		// 理想情况下应使用 tmpfs 而非本地磁盘，但目前无法将 tmpfs 的内存正确计入对应的 cgroup；
		// 待内存计费问题解决后再切换为 tmpfs。
		Volume: &v1.Volume{VolumeSource: v1.VolumeSource{EmptyDir: &v1.EmptyDirVolumeSource{}}},
	}
}

// SetUp 在默认路径上挂载 ConfigMap 卷。
func (b *configMapVolumeMounter) SetUp(mounterArgs volume.MounterArgs) error {
	return b.SetUpAt(b.GetPath(), mounterArgs)
}

// SetUpAt 在指定目录挂载 ConfigMap 卷：先由 EmptyDir 准备好目录，再将 ConfigMap 数据原子写入。
func (b *configMapVolumeMounter) SetUpAt(dir string, mounterArgs volume.MounterArgs) error {
	klog.V(3).Infof("Setting up volume %v for pod %v at %v", b.volName, b.pod.UID, dir)

	// 包装 EmptyDir，由它完成底层目录的创建和挂载。
	wrapped, err := b.plugin.host.NewWrapperMounter(b.volName, wrappedVolumeSpec(), &b.pod)
	if err != nil {
		return err
	}

	optional := b.source.Optional != nil && *b.source.Optional
	configMap, err := b.getConfigMap(b.pod.Namespace, b.source.Name)
	if err != nil {
		// 只有 ConfigMap 不存在且声明为 optional 时才继续，否则必须报错。
		if !(errors.IsNotFound(err) && optional) {
			klog.Errorf("Couldn't get configMap %v/%v: %v", b.pod.Namespace, b.source.Name, err)
			return err
		}
		configMap = &v1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: b.pod.Namespace,
				Name:      b.source.Name,
			},
		}
	}

	totalBytes := totalBytes(configMap)
	klog.V(3).Infof("Received configMap %v/%v containing (%v) pieces of data, %v total bytes",
		b.pod.Namespace,
		b.source.Name,
		len(configMap.Data)+len(configMap.BinaryData),
		totalBytes)

	payload, err := MakePayload(b.source.Items, configMap, b.source.DefaultMode, b.source.DefaultUser, optional)
	if err != nil {
		return err
	}

	setupSuccess := false
	if err := wrapped.SetUpAt(dir, mounterArgs); err != nil {
		return err
	}
	if err := volumeutil.MakeNestedMountpoints(b.volName, dir, b.pod); err != nil {
		return err
	}

	defer func() {
		// 安装失败时清理已创建的目录，避免留下残留挂载点。
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
		// 首次写入时新文件可能创建在时间戳子目录之外，因此要对整个卷改权限，而不只是时间戳目录。
		ownerShipChanger := volume.NewVolumeOwnership(b, dir, mounterArgs.FsGroup, nil /*fsGroupChangePolicy*/, volumeutil.FSGroupCompleteHook(b.plugin, nil))
		return ownerShipChanger.ChangePermissions()
	}
	err = writer.Write(payload, setPerms)
	if err != nil {
		klog.Errorf("Error writing payload to dir: %v", err)
		return err
	}

	setupSuccess = true
	return nil
}

// MakePayload 将 ConfigMap 数据按 items 映射转换为待写入的文件投影。
// 导出该函数以便 projection 卷驱动复用。
func MakePayload(mappings []v1.KeyToPath, configMap *v1.ConfigMap, defaultMode *int32, defaultUser *int64, optional bool) (map[string]volumeutil.FileProjection, error) {
	if defaultMode == nil {
		return nil, fmt.Errorf("no defaultMode used, not even the default value for it")
	}

	payload := make(map[string]volumeutil.FileProjection, (len(configMap.Data) + len(configMap.BinaryData)))
	var fileProjection volumeutil.FileProjection

	if len(mappings) == 0 {
		// 未指定 items 时，将 Data 和 BinaryData 中的所有键直接投射为同名文件。
		for name, data := range configMap.Data {
			fileProjection.Data = []byte(data)
			fileProjection.Mode = *defaultMode
			if utilfeature.DefaultFeatureGate.Enabled(features.AtomicWriteVolumeUserFields) {
				fileProjection.FsUser = defaultUser
			}
			payload[name] = fileProjection
		}
		for name, data := range configMap.BinaryData {
			fileProjection.Data = data
			fileProjection.Mode = *defaultMode
			if utilfeature.DefaultFeatureGate.Enabled(features.AtomicWriteVolumeUserFields) {
				fileProjection.FsUser = defaultUser
			}
			payload[name] = fileProjection
		}
	} else {
		// 指定 items 时按 key -> path 映射投射；optional 下缺失的键直接跳过。
		for _, ktp := range mappings {
			if stringData, ok := configMap.Data[ktp.Key]; ok {
				fileProjection.Data = []byte(stringData)
			} else if binaryData, ok := configMap.BinaryData[ktp.Key]; ok {
				fileProjection.Data = binaryData
			} else {
				if optional {
					continue
				}
				return nil, fmt.Errorf("configmap references non-existent config key: %s", ktp.Key)
			}

			fileProjection.FsUser = volumeutil.ResolvesFsUser(defaultUser, ktp.User)
			if ktp.Mode != nil {
				fileProjection.Mode = *ktp.Mode
			} else {
				fileProjection.Mode = *defaultMode
			}

			payload[ktp.Path] = fileProjection
		}
	}

	return payload, nil
}

// totalBytes 统计 ConfigMap 中字符串数据和二进制数据的总字节数，仅用于日志输出。
func totalBytes(configMap *v1.ConfigMap) int {
	totalSize := 0
	for _, value := range configMap.Data {
		totalSize += len(value)
	}
	for _, value := range configMap.BinaryData {
		totalSize += len(value)
	}

	return totalSize
}

// configMapVolumeUnmounter 负责清理 ConfigMap 卷挂载。
type configMapVolumeUnmounter struct {
	*configMapVolume
}

var _ volume.Unmounter = &configMapVolumeUnmounter{}

// TearDown 在默认路径上卸载 ConfigMap 卷。
func (c *configMapVolumeUnmounter) TearDown() error {
	return c.TearDownAt(c.GetPath())
}

// TearDownAt 卸载指定目录的 ConfigMap 卷，交由包装的 EmptyDir 完成清理。
func (c *configMapVolumeUnmounter) TearDownAt(dir string) error {
	return volumeutil.UnmountViaEmptyDir(dir, c.plugin.host, c.volName, wrappedVolumeSpec(), c.podUID)
}

// getVolumeSource 从卷规格中提取 ConfigMap 卷源及只读标记。
func getVolumeSource(spec *volume.Spec) (*v1.ConfigMapVolumeSource, bool) {
	var readOnly bool
	var volumeSource *v1.ConfigMapVolumeSource

	if spec.Volume != nil && spec.Volume.ConfigMap != nil {
		volumeSource = spec.Volume.ConfigMap
		readOnly = spec.ReadOnly
	}

	return volumeSource, readOnly
}
