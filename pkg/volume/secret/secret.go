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

// Package secret 实现 Secret 卷插件，将 API Server 中的 Secret 数据以文件形式挂载到 Pod 中。
// 底层使用内存介质（tmpfs）的 EmptyDir 包装卷，避免敏感数据落盘，内容通过 AtomicWriter 原子写入。
package secret

import (
	"errors"
	"fmt"

	"k8s.io/klog/v2"
	"k8s.io/mount-utils"
	utilstrings "k8s.io/utils/strings"

	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/pkg/volume"
	volumeutil "k8s.io/kubernetes/pkg/volume/util"
)

// ProbeVolumePlugins 是包内卷插件探测的入口，返回可供 kubelet 注册的插件列表。
func ProbeVolumePlugins() []volume.VolumePlugin {
	return []volume.VolumePlugin{&secretPlugin{}}
}

const (
	secretPluginName = "kubernetes.io/secret"
)

// secretPlugin 实现 VolumePlugin 接口，负责创建 Secret 卷的 mounter 和 unmounter。
type secretPlugin struct {
	host      volume.VolumeHost
	getSecret func(namespace, name string) (*v1.Secret, error)
}

var _ volume.VolumePlugin = &secretPlugin{}

func wrappedVolumeSpec() volume.Spec {
	// 使用内存介质的 EmptyDir 作为包装卷，Secret 内容只存在于 tmpfs 中，不写入节点磁盘。
	return volume.Spec{
		Volume: &v1.Volume{VolumeSource: v1.VolumeSource{EmptyDir: &v1.EmptyDirVolumeSource{Medium: v1.StorageMediumMemory}}},
	}
}

func getPath(uid types.UID, volName string, host volume.VolumeHost) string {
	return host.GetPodVolumeDir(uid, utilstrings.EscapeQualifiedName(secretPluginName), volName)
}

// Init 初始化插件，保存 VolumeHost 并获取 Secret 查询函数。
func (plugin *secretPlugin) Init(host volume.VolumeHost) error {
	plugin.host = host
	plugin.getSecret = host.GetSecretFunc()
	return nil
}

// GetPluginName 返回插件名称。
func (plugin *secretPlugin) GetPluginName() string {
	return secretPluginName
}

// GetVolumeName 返回卷的唯一标识，这里直接使用 Secret 名称。
func (plugin *secretPlugin) GetVolumeName(spec *volume.Spec) (string, error) {
	volumeSource, _ := getVolumeSource(spec)
	if volumeSource == nil {
		return "", fmt.Errorf("Spec does not reference a Secret volume type")
	}

	return volumeSource.SecretName, nil
}

// CanSupport 判断卷规格是否引用 Secret 卷源。
func (plugin *secretPlugin) CanSupport(spec *volume.Spec) bool {
	return spec.Volume != nil && spec.Volume.Secret != nil
}

// RequiresRemount 返回 true，Secret 内容变化时需要重新挂载以同步数据。
func (plugin *secretPlugin) RequiresRemount(spec *volume.Spec) bool {
	return true
}

// SupportsMountOption 返回 false，该插件不支持挂载选项。
func (plugin *secretPlugin) SupportsMountOption() bool {
	return false
}

// SupportsSELinuxContextMount 返回 false，该插件不支持 SELinux 上下文挂载。
func (plugin *secretPlugin) SupportsSELinuxContextMount(spec *volume.Spec) (bool, error) {
	return false, nil
}

// NewMounter 为 Pod 的 Secret 卷创建 mounter。
func (plugin *secretPlugin) NewMounter(spec *volume.Spec, pod *v1.Pod) (volume.Mounter, error) {
	return &secretVolumeMounter{
		secretVolume: &secretVolume{
			spec.Name(),
			pod.UID,
			plugin,
			plugin.host.GetMounter(),
			volume.NewCachedMetrics(volume.NewMetricsDu(getPath(pod.UID, spec.Name(), plugin.host))),
		},
		source:    *spec.Volume.Secret,
		pod:       *pod,
		getSecret: plugin.getSecret,
	}, nil
}

// NewUnmounter 为指定 Pod 的 Secret 卷创建 unmounter。
func (plugin *secretPlugin) NewUnmounter(volName string, podUID types.UID) (volume.Unmounter, error) {
	return &secretVolumeUnmounter{
		&secretVolume{
			volName,
			podUID,
			plugin,
			plugin.host.GetMounter(),
			volume.NewCachedMetrics(volume.NewMetricsDu(getPath(podUID, volName, plugin.host))),
		},
	}, nil
}

// ConstructVolumeSpec 根据已挂载的卷名重建 Secret 卷规格，用于节点重启后的卷恢复场景。
func (plugin *secretPlugin) ConstructVolumeSpec(volName, mountPath string) (volume.ReconstructedVolume, error) {
	secretVolume := &v1.Volume{
		Name: volName,
		VolumeSource: v1.VolumeSource{
			Secret: &v1.SecretVolumeSource{
				SecretName: volName,
			},
		},
	}
	return volume.ReconstructedVolume{
		Spec: volume.NewSpecFromVolume(secretVolume),
	}, nil
}

// secretVolume 保存 Secret 卷的公共状态：卷名、所属 Pod、插件、mounter 和指标。
type secretVolume struct {
	volName string
	podUID  types.UID
	plugin  *secretPlugin
	mounter mount.Interface
	volume.MetricsProvider
}

var _ volume.Volume = &secretVolume{}

// GetPath 返回该卷在主机上的挂载路径。
func (sv *secretVolume) GetPath() string {
	return getPath(sv.podUID, sv.volName, sv.plugin.host)
}

// secretVolumeMounter 从 API Server 获取 Secret，并以文件形式写入主机上的卷目录。
type secretVolumeMounter struct {
	*secretVolume

	source    v1.SecretVolumeSource
	pod       v1.Pod
	getSecret func(namespace, name string) (*v1.Secret, error)
}

var _ volume.Mounter = &secretVolumeMounter{}

// GetAttributes 返回卷属性：只读、由 kubelet 管理、需要 SELinux 重新打标。
func (sv *secretVolume) GetAttributes() volume.Attributes {
	return volume.Attributes{
		ReadOnly:       true,
		Managed:        true,
		SELinuxRelabel: true,
	}
}

// SetUp 在默认路径上挂载 Secret 卷。
func (b *secretVolumeMounter) SetUp(mounterArgs volume.MounterArgs) error {
	return b.SetUpAt(b.GetPath(), mounterArgs)
}

// SetUpAt 在指定目录挂载 Secret 卷：先由内存 EmptyDir 准备好目录，再将 Secret 数据原子写入。
func (b *secretVolumeMounter) SetUpAt(dir string, mounterArgs volume.MounterArgs) error {
	klog.V(3).Infof("Setting up volume %v for pod %v at %v", b.volName, b.pod.UID, dir)

	// 包装 EmptyDir，由它完成底层目录的创建和挂载。
	wrapped, err := b.plugin.host.NewWrapperMounter(b.volName, wrappedVolumeSpec(), &b.pod)
	if err != nil {
		return err
	}

	optional := b.source.Optional != nil && *b.source.Optional
	secret, err := b.getSecret(b.pod.Namespace, b.source.SecretName)
	if err != nil {
		// 只有 Secret 不存在且声明为 optional 时才继续，否则必须报错。
		if !(apierrors.IsNotFound(err) && optional) {
			klog.Errorf("Couldn't get secret %v/%v: %v", b.pod.Namespace, b.source.SecretName, err)
			return err
		}
		secret = &v1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: b.pod.Namespace,
				Name:      b.source.SecretName,
			},
		}
	}

	totalBytes := totalSecretBytes(secret)
	klog.V(3).Infof("Received secret %v/%v containing (%v) pieces of data, %v total bytes",
		b.pod.Namespace,
		b.source.SecretName,
		len(secret.Data),
		totalBytes)

	payload, err := MakePayload(b.source.Items, secret, b.source.DefaultMode, b.source.DefaultUser, optional)
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
		ownershipChanger := volume.NewVolumeOwnership(b, dir, mounterArgs.FsGroup, nil /*fsGroupChangePolicy*/, volumeutil.FSGroupCompleteHook(b.plugin, nil))
		return ownershipChanger.ChangePermissions()
	}
	err = writer.Write(payload, setPerms)
	if err != nil {
		klog.Errorf("Error writing payload to dir: %v", err)
		return err
	}

	setupSuccess = true
	return nil
}

// MakePayload 将 Secret 数据按 items 映射转换为待写入的文件投影。
// 导出该函数以便 projection 卷驱动复用。
func MakePayload(mappings []v1.KeyToPath, secret *v1.Secret, defaultMode *int32, defaultUser *int64, optional bool) (map[string]volumeutil.FileProjection, error) {
	if defaultMode == nil {
		return nil, fmt.Errorf("no defaultMode used, not even the default value for it")
	}

	payload := make(map[string]volumeutil.FileProjection, len(secret.Data))
	var fileProjection volumeutil.FileProjection

	if len(mappings) == 0 {
		// 未指定 items 时，将所有键直接投射为同名文件。
		for name, data := range secret.Data {
			fileProjection.Data = []byte(data)
			fileProjection.Mode = *defaultMode
			if utilfeature.DefaultFeatureGate.Enabled(features.AtomicWriteVolumeUserFields) {
				fileProjection.FsUser = defaultUser
			}
			payload[name] = fileProjection
		}
	} else {
		// 指定 items 时按 key -> path 映射投射；optional 下缺失的键直接跳过。
		for _, ktp := range mappings {
			content, ok := secret.Data[ktp.Key]
			if !ok {
				if optional {
					continue
				}
				errMsg := fmt.Sprintf("references non-existent secret key: %s", ktp.Key)
				klog.Error(errMsg)
				return nil, errors.New(errMsg)
			}

			fileProjection.Data = []byte(content)
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

// totalSecretBytes 统计 Secret 数据的总字节数，仅用于日志输出。
func totalSecretBytes(secret *v1.Secret) int {
	totalSize := 0
	for _, bytes := range secret.Data {
		totalSize += len(bytes)
	}

	return totalSize
}

// secretVolumeUnmounter 负责清理 Secret 卷挂载。
type secretVolumeUnmounter struct {
	*secretVolume
}

var _ volume.Unmounter = &secretVolumeUnmounter{}

// TearDown 在默认路径上卸载 Secret 卷。
func (c *secretVolumeUnmounter) TearDown() error {
	return c.TearDownAt(c.GetPath())
}

// TearDownAt 卸载指定目录的 Secret 卷，交由包装的 EmptyDir 完成清理。
func (c *secretVolumeUnmounter) TearDownAt(dir string) error {
	return volumeutil.UnmountViaEmptyDir(dir, c.plugin.host, c.volName, wrappedVolumeSpec(), c.podUID)
}

// getVolumeSource 从卷规格中提取 Secret 卷源及只读标记。
func getVolumeSource(spec *volume.Spec) (*v1.SecretVolumeSource, bool) {
	var readOnly bool
	var volumeSource *v1.SecretVolumeSource

	if spec.Volume != nil && spec.Volume.Secret != nil {
		volumeSource = spec.Volume.Secret
		readOnly = spec.ReadOnly
	}

	return volumeSource, readOnly
}
