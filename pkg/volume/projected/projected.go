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

package projected

import (
	"bytes"
	"context"
	"fmt"

	authenticationv1 "k8s.io/api/authentication/v1"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/pkg/volume"
	"k8s.io/kubernetes/pkg/volume/configmap"
	"k8s.io/kubernetes/pkg/volume/downwardapi"
	"k8s.io/kubernetes/pkg/volume/secret"
	volumeutil "k8s.io/kubernetes/pkg/volume/util"
	utilstrings "k8s.io/utils/strings"
)

// ProbeVolumePlugins 是包内插件探测的入口，返回 projected 卷插件实例。
func ProbeVolumePlugins() []volume.VolumePlugin {
	return []volume.VolumePlugin{&projectedPlugin{}}
}

// projected 插件名称。
const (
	projectedPluginName = "kubernetes.io/projected"
)

// projectedPlugin 实现 projected 卷插件，将多种数据源投影到同一目录。
// 通过函数字段注入 API 访问，便于测试替换。
type projectedPlugin struct {
	host                      volume.VolumeHost
	kvHost                    volume.KubeletVolumeHost
	getSecret                 func(namespace, name string) (*v1.Secret, error)
	getConfigMap              func(namespace, name string) (*v1.ConfigMap, error)
	getServiceAccountToken    func(namespace, name string, tr *authenticationv1.TokenRequest) (*authenticationv1.TokenRequest, error)
	deleteServiceAccountToken func(podUID types.UID)
}

// 编译时断言 projectedPlugin 实现 volume.VolumePlugin 接口。
var _ volume.VolumePlugin = &projectedPlugin{}

// wrappedVolumeSpec 返回内存 EmptyDir 规格，作为 projected 卷的底层承载卷。
func wrappedVolumeSpec() volume.Spec {
	return volume.Spec{
		Volume: &v1.Volume{
			VolumeSource: v1.VolumeSource{
				EmptyDir: &v1.EmptyDirVolumeSource{Medium: v1.StorageMediumMemory},
			},
		},
	}
}

// getPath 返回 projected 卷在宿主机上的挂载目录。
func getPath(uid types.UID, volName string, host volume.VolumeHost) string {
	return host.GetPodVolumeDir(uid, utilstrings.EscapeQualifiedName(projectedPluginName), volName)
}

// Init 初始化插件，注入 VolumeHost 并缓存各类 API 访问函数。
func (plugin *projectedPlugin) Init(host volume.VolumeHost) error {
	plugin.host = host
	plugin.kvHost = host.(volume.KubeletVolumeHost)
	plugin.getSecret = host.GetSecretFunc()
	plugin.getConfigMap = host.GetConfigMapFunc()
	plugin.getServiceAccountToken = host.GetServiceAccountTokenFunc()
	plugin.deleteServiceAccountToken = host.DeleteServiceAccountTokenFunc()
	return nil
}

// GetPluginName 返回插件名称。
func (plugin *projectedPlugin) GetPluginName() string {
	return projectedPluginName
}

// GetVolumeName 校验规格引用的是 projected 卷，并返回用户定义的卷名。
func (plugin *projectedPlugin) GetVolumeName(spec *volume.Spec) (string, error) {
	_, _, err := getVolumeSource(spec)
	if err != nil {
		return "", err
	}

	return spec.Name(), nil
}

// CanSupport 判断规格是否为 projected 卷。
func (plugin *projectedPlugin) CanSupport(spec *volume.Spec) bool {
	return spec.Volume != nil && spec.Volume.Projected != nil
}

// RequiresRemount 始终返回 true：projected 卷内容需随 kubelet 同步周期刷新。
func (plugin *projectedPlugin) RequiresRemount(spec *volume.Spec) bool {
	return true
}

// SupportsMountOption 返回 false，projected 卷不支持挂载选项。
func (plugin *projectedPlugin) SupportsMountOption() bool {
	return false
}

// SupportsSELinuxContextMount 返回 false，不支持通过挂载选项设置 SELinux 上下文。
func (plugin *projectedPlugin) SupportsSELinuxContextMount(spec *volume.Spec) (bool, error) {
	return false, nil
}

// NewMounter 为 Pod 创建 projected 卷的 Mounter，并挂载缓存的用量指标提供者。
func (plugin *projectedPlugin) NewMounter(spec *volume.Spec, pod *v1.Pod) (volume.Mounter, error) {
	return &projectedVolumeMounter{
		projectedVolume: &projectedVolume{
			volName:         spec.Name(),
			sources:         spec.Volume.Projected.Sources,
			podUID:          pod.UID,
			plugin:          plugin,
			MetricsProvider: volume.NewCachedMetrics(volume.NewMetricsDu(getPath(pod.UID, spec.Name(), plugin.host))),
		},
		source: *spec.Volume.Projected,
		pod:    pod,
	}, nil
}

// NewUnmounter 根据卷名和 Pod UID 创建 projected 卷的 Unmounter。
func (plugin *projectedPlugin) NewUnmounter(volName string, podUID types.UID) (volume.Unmounter, error) {
	return &projectedVolumeUnmounter{
		&projectedVolume{
			volName:         volName,
			podUID:          podUID,
			plugin:          plugin,
			MetricsProvider: volume.NewCachedMetrics(volume.NewMetricsDu(getPath(podUID, volName, plugin.host))),
		},
	}, nil
}

// ConstructVolumeSpec 根据卷名重建 projected 卷规格（仅用于卸载等无需数据源的场景）。
func (plugin *projectedPlugin) ConstructVolumeSpec(volumeName, mountPath string) (volume.ReconstructedVolume, error) {
	projectedVolume := &v1.Volume{
		Name: volumeName,
		VolumeSource: v1.VolumeSource{
			Projected: &v1.ProjectedVolumeSource{},
		},
	}

	return volume.ReconstructedVolume{
		Spec: volume.NewSpecFromVolume(projectedVolume),
	}, nil
}

// projectedVolume 表示投影卷实例，持有卷名、数据源列表和所属 Pod。
type projectedVolume struct {
	volName string
	sources []v1.VolumeProjection
	podUID  types.UID
	plugin  *projectedPlugin
	volume.MetricsProvider
}

// 编译时断言 projectedVolume 实现 volume.Volume 接口。
var _ volume.Volume = &projectedVolume{}

// GetPath 返回卷在宿主机上的挂载目录。
func (sv *projectedVolume) GetPath() string {
	return getPath(sv.podUID, sv.volName, sv.plugin.host)
}

// projectedVolumeMounter 负责准备并写入投影数据源。
type projectedVolumeMounter struct {
	*projectedVolume

	source v1.ProjectedVolumeSource
	pod    *v1.Pod
}

// 编译时断言 projectedVolumeMounter 实现 volume.Mounter 接口。
var _ volume.Mounter = &projectedVolumeMounter{}

// GetAttributes 返回卷属性：只读、受 kubelet 管理、需重打 SELinux 标签。
func (sv *projectedVolume) GetAttributes() volume.Attributes {
	return volume.Attributes{
		ReadOnly:       true,
		Managed:        true,
		SELinuxRelabel: true,
	}

}

// SetUp 在默认挂载目录设置 projected 卷。
func (s *projectedVolumeMounter) SetUp(mounterArgs volume.MounterArgs) error {
	return s.SetUpAt(s.GetPath(), mounterArgs)
}

// SetUpAt 在指定目录设置 projected 卷。
// 底层用内存 EmptyDir 承载，再通过 AtomicWriter 原子写入各数据源；
// 任一环节失败时通过 defer 卸载并清理目录，避免残留半成品。
func (s *projectedVolumeMounter) SetUpAt(dir string, mounterArgs volume.MounterArgs) error {
	klog.V(3).Infof("Setting up volume %v for pod %v at %v", s.volName, s.pod.UID, dir)

	wrapped, err := s.plugin.host.NewWrapperMounter(s.volName, wrappedVolumeSpec(), s.pod)
	if err != nil {
		return err
	}

	data, err := s.collectData(mounterArgs)
	if err != nil {
		klog.Errorf("Error preparing data for projected volume %v for pod %v/%v: %s", s.volName, s.pod.Namespace, s.pod.Name, err.Error())
		return err
	}

	setupSuccess := false
	if err := wrapped.SetUpAt(dir, mounterArgs); err != nil {
		return err
	}

	if err := volumeutil.MakeNestedMountpoints(s.volName, dir, *s.pod); err != nil {
		return err
	}

	defer func() {
		// 设置失败时清理目录，避免留下部分写入的卷。
		if !setupSuccess {
			unmounter, unmountCreateErr := s.plugin.NewUnmounter(s.volName, s.podUID)
			if unmountCreateErr != nil {
				klog.Errorf("error cleaning up mount %s after failure. Create unmounter failed with %v", s.volName, unmountCreateErr)
				return
			}
			tearDownErr := unmounter.TearDown()
			if tearDownErr != nil {
				klog.Errorf("error tearing down volume %s: %v", s.volName, tearDownErr)
			}
		}
	}()

	writerContext := fmt.Sprintf("pod %v/%v volume %v", s.pod.Namespace, s.pod.Name, s.volName)
	writer, err := volumeutil.NewAtomicWriter(dir, writerContext)
	if err != nil {
		klog.Errorf("Error creating atomic writer: %v", err)
		return err
	}

	setPerms := func(_ string) error {
		// 首次写入时新文件可能出现在时间戳子目录之外，
		// 因此对整个卷而非仅时间戳目录调整权限。
		ownershipChanger := volume.NewVolumeOwnership(s, dir, mounterArgs.FsGroup, nil /*fsGroupChangePolicy*/, volumeutil.FSGroupCompleteHook(s.plugin, nil))
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

// collectData 收集所有投影数据源，生成文件路径到文件内容的映射。
// 单个数据源失败记入错误列表后继续处理其余数据源，最后聚合返回。
func (s *projectedVolumeMounter) collectData(mounterArgs volume.MounterArgs) (map[string]volumeutil.FileProjection, error) {
	if s.source.DefaultMode == nil {
		return nil, fmt.Errorf("no defaultMode used, not even the default value for it")
	}

	kubeClient := s.plugin.host.GetKubeClient()
	if kubeClient == nil {
		return nil, fmt.Errorf("cannot setup projected volume %v because kube client is not configured", s.volName)
	}

	errlist := []error{}
	payload := make(map[string]volumeutil.FileProjection)
	for sourceIndex, source := range s.source.Sources {
		switch {
		case source.Secret != nil:
			optional := source.Secret.Optional != nil && *source.Secret.Optional
			secretapi, err := s.plugin.getSecret(s.pod.Namespace, source.Secret.Name)
			if err != nil {
				// 仅当数据源标记为 optional 且 Secret 不存在时才忽略，
				// 其余错误（含权限问题）都需上报。
				if !(errors.IsNotFound(err) && optional) {
					klog.Errorf("Couldn't get secret %v/%v: %v", s.pod.Namespace, source.Secret.Name, err)
					errlist = append(errlist, err)
					continue
				}
				secretapi = &v1.Secret{
					ObjectMeta: metav1.ObjectMeta{
						Namespace: s.pod.Namespace,
						Name:      source.Secret.Name,
					},
				}
			}
			secretPayload, err := secret.MakePayload(source.Secret.Items, secretapi, s.source.DefaultMode, s.source.DefaultUser, optional)
			if err != nil {
				klog.Errorf("Couldn't get secret payload %v/%v: %v", s.pod.Namespace, source.Secret.Name, err)
				errlist = append(errlist, err)
				continue
			}
			for k, v := range secretPayload {
				payload[k] = v
			}
		case source.ConfigMap != nil:
			optional := source.ConfigMap.Optional != nil && *source.ConfigMap.Optional
			configMap, err := s.plugin.getConfigMap(s.pod.Namespace, source.ConfigMap.Name)
			if err != nil {
				// 与 Secret 一致：仅 optional 且不存在时可忽略。
				if !(errors.IsNotFound(err) && optional) {
					klog.Errorf("Couldn't get configMap %v/%v: %v", s.pod.Namespace, source.ConfigMap.Name, err)
					errlist = append(errlist, err)
					continue
				}
				configMap = &v1.ConfigMap{
					ObjectMeta: metav1.ObjectMeta{
						Namespace: s.pod.Namespace,
						Name:      source.ConfigMap.Name,
					},
				}
			}
			configMapPayload, err := configmap.MakePayload(source.ConfigMap.Items, configMap, s.source.DefaultMode, s.source.DefaultUser, optional)
			if err != nil {
				klog.Errorf("Couldn't get configMap payload %v/%v: %v", s.pod.Namespace, source.ConfigMap.Name, err)
				errlist = append(errlist, err)
				continue
			}
			for k, v := range configMapPayload {
				payload[k] = v
			}
		case source.DownwardAPI != nil:
			downwardAPIPayload, err := downwardapi.CollectData(source.DownwardAPI.Items, s.pod, s.plugin.host, s.source.DefaultMode, s.source.DefaultUser)
			if err != nil {
				errlist = append(errlist, err)
				continue
			}
			for k, v := range downwardAPIPayload {
				payload[k] = v
			}
		case source.ServiceAccountToken != nil:
			tp := source.ServiceAccountToken
			fsUser := resolveFsUser(mounterArgs, s.source.DefaultUser, tp.User)

			// 指定 FsUser 或 FsGroup 时先以 0600 写入，
			// 再由 SetVolumeOwnership 按需调整为 0640。
			mode := *s.source.DefaultMode
			if fsUser != nil || mounterArgs.FsGroup != nil {
				mode = 0600
			}

			var auds []string
			if len(tp.Audience) != 0 {
				auds = []string{tp.Audience}
			}
			tr, err := s.plugin.getServiceAccountToken(s.pod.Namespace, s.pod.Spec.ServiceAccountName, &authenticationv1.TokenRequest{
				Spec: authenticationv1.TokenRequestSpec{
					Audiences:         auds,
					ExpirationSeconds: tp.ExpirationSeconds,
					BoundObjectRef: &authenticationv1.BoundObjectReference{
						APIVersion: "v1",
						Kind:       "Pod",
						Name:       s.pod.Name,
						UID:        s.pod.UID,
					},
				},
			})
			if err != nil {
				errlist = append(errlist, err)
				continue
			}
			payload[tp.Path] = volumeutil.FileProjection{
				Data:   []byte(tr.Status.Token),
				Mode:   mode,
				FsUser: fsUser,
			}
		case source.ClusterTrustBundle != nil:
			allowEmpty := false
			if source.ClusterTrustBundle.Optional != nil && *source.ClusterTrustBundle.Optional {
				allowEmpty = true
			}

			var trustAnchors []byte
			if source.ClusterTrustBundle.Name != nil {
				var err error
				trustAnchors, err = s.plugin.kvHost.GetTrustAnchorsByName(context.TODO(), *source.ClusterTrustBundle.Name, allowEmpty)
				if err != nil {
					errlist = append(errlist, err)
					continue
				}
			} else if source.ClusterTrustBundle.SignerName != nil {
				var err error
				trustAnchors, err = s.plugin.kvHost.GetTrustAnchorsBySigner(context.TODO(), *source.ClusterTrustBundle.SignerName, source.ClusterTrustBundle.LabelSelector, allowEmpty)
				if err != nil {
					errlist = append(errlist, err)
					continue
				}
			} else {
				errlist = append(errlist, fmt.Errorf("ClusterTrustBundle projection requires either name or signerName to be set"))
				continue
			}

			fsUser := resolveFsUser(mounterArgs, s.source.DefaultUser, source.ClusterTrustBundle.User)
			// 含用户/组要求时收紧为 0600，由后续所有权调整放宽。
			mode := *s.source.DefaultMode
			if fsUser != nil || mounterArgs.FsGroup != nil {
				mode = 0600
			}

			payload[source.ClusterTrustBundle.Path] = volumeutil.FileProjection{
				Data:   trustAnchors,
				Mode:   mode,
				FsUser: fsUser,
			}
		case source.PodCertificate != nil:
			key, certificates, err := s.plugin.kvHost.GetPodCertificateCredentialBundle(context.TODO(), s.pod.ObjectMeta.Namespace, s.pod.ObjectMeta.Name, string(s.pod.ObjectMeta.UID), s.volName, sourceIndex)
			if err != nil {
				errlist = append(errlist, err)
				continue
			}

			fsUser := resolveFsUser(mounterArgs, s.source.DefaultUser, source.PodCertificate.User)
			// 私钥文件须限制权限，后续再按需调整所有权。
			mode := *s.source.DefaultMode
			if fsUser != nil || mounterArgs.FsGroup != nil {
				mode = 0600
			}

			if source.PodCertificate.CredentialBundlePath != "" {
				credentialBundle := bytes.Buffer{}
				credentialBundle.Write(key)
				credentialBundle.Write(certificates)
				payload[source.PodCertificate.CredentialBundlePath] = volumeutil.FileProjection{
					Data:   credentialBundle.Bytes(),
					Mode:   mode,
					FsUser: fsUser,
				}
			}
			if source.PodCertificate.KeyPath != "" {
				payload[source.PodCertificate.KeyPath] = volumeutil.FileProjection{
					Data:   key,
					Mode:   mode,
					FsUser: fsUser,
				}
			}
			if source.PodCertificate.CertificateChainPath != "" {
				payload[source.PodCertificate.CertificateChainPath] = volumeutil.FileProjection{
					Data:   certificates,
					Mode:   mode,
					FsUser: fsUser,
				}
			}

		}
	}
	return payload, utilerrors.NewAggregate(errlist)
}

// projectedVolumeUnmounter 负责清理 projected 卷的挂载目录。
type projectedVolumeUnmounter struct {
	*projectedVolume
}

// 编译时断言 projectedVolumeUnmounter 实现 volume.Unmounter 接口。
var _ volume.Unmounter = &projectedVolumeUnmounter{}

// TearDown 卸载默认路径下的 projected 卷。
func (c *projectedVolumeUnmounter) TearDown() error {
	return c.TearDownAt(c.GetPath())
}

// TearDownAt 通过底层 EmptyDir 卸载指定目录，并删除该 Pod 的 ServiceAccount token。
func (c *projectedVolumeUnmounter) TearDownAt(dir string) error {
	klog.V(3).Infof("Tearing down volume %v for pod %v at %v", c.volName, c.podUID, dir)

	wrapped, err := c.plugin.host.NewWrapperUnmounter(c.volName, wrappedVolumeSpec(), c.podUID)
	if err != nil {
		return err
	}
	if err = wrapped.TearDownAt(dir); err != nil {
		return err
	}

	c.plugin.deleteServiceAccountToken(c.podUID)

	return nil
}

// getVolumeSource 从规格中提取 projected 卷源和只读标志，非 projected 卷返回错误。
func getVolumeSource(spec *volume.Spec) (*v1.ProjectedVolumeSource, bool, error) {
	if spec.Volume != nil && spec.Volume.Projected != nil {
		return spec.Volume.Projected, spec.ReadOnly, nil
	}

	return nil, false, fmt.Errorf("spec does not reference a projected volume type")
}

// resolveFsUser 按 KEP-5936 描述的回退机制确定文件所有者 UID。
// 特性门控关闭时直接返回 mounterArgs.FsUser；
// 开启时依次用卷默认用户、数据源用户覆盖，后者优先级更高。
func resolveFsUser(mounterArgs volume.MounterArgs, defaultUser, itemUser *int64) *int64 {
	fsUser := mounterArgs.FsUser

	if utilfeature.DefaultFeatureGate.Enabled(features.AtomicWriteVolumeUserFields) {
		if defaultUser != nil {
			fsUser = defaultUser
		}
		if itemUser != nil {
			fsUser = itemUser
		}
	}
	return fsUser
}
