/*
Copyright 2016 The Kubernetes Authors.

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

package kubelet

import (
	"context"
	"fmt"
	"runtime"

	"k8s.io/klog/v2"
	"k8s.io/mount-utils"

	authenticationv1 "k8s.io/api/authentication/v1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/informers"
	clientset "k8s.io/client-go/kubernetes"
	storagelisters "k8s.io/client-go/listers/storage/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/kubernetes/pkg/kubelet/clustertrustbundle"
	"k8s.io/kubernetes/pkg/kubelet/configmap"
	"k8s.io/kubernetes/pkg/kubelet/podcertificate"
	"k8s.io/kubernetes/pkg/kubelet/secret"
	"k8s.io/kubernetes/pkg/kubelet/token"
	"k8s.io/kubernetes/pkg/volume"
	"k8s.io/kubernetes/pkg/volume/util"
	"k8s.io/kubernetes/pkg/volume/util/hostutil"
	"k8s.io/kubernetes/pkg/volume/util/subpath"
)

// NewInitializedVolumePluginMgr 创建并初始化 VolumePluginMgr，
// 注入 kubelet 对 VolumeHost 接口的实现。
// kubelet 提供 kubelet 特有参数，plugins 用于初始化插件管理器。
func NewInitializedVolumePluginMgr(
	logger klog.Logger,
	kubelet *Kubelet,
	secretManager secret.Manager,
	configMapManager configmap.Manager,
	tokenManager *token.Manager,
	clusterTrustBundleManager clustertrustbundle.Manager,
	plugins []volume.VolumePlugin,
	prober volume.DynamicPluginProber) (*volume.VolumePluginMgr, error) {

	// 必须在 InitPlugins 之前初始化 CSIDriver lister。
	var informerFactory informers.SharedInformerFactory
	var csiDriverLister storagelisters.CSIDriverLister
	var csiDriversSynced cache.InformerSynced
	const resyncPeriod = 0
	// kubeClient 为 nil（standalone 模式）时跳过 informer 初始化。
	if kubelet.kubeClient != nil {
		informerFactory = informers.NewSharedInformerFactory(kubelet.kubeClient, resyncPeriod)
		csiDriverInformer := informerFactory.Storage().V1().CSIDrivers()
		csiDriverLister = csiDriverInformer.Lister()
		csiDriversSynced = csiDriverInformer.Informer().HasSynced

	} else {
		logger.Info("KubeClient is nil. Skip initialization of CSIDriverLister")
	}

	kvh := &kubeletVolumeHost{
		kubelet:                   kubelet,
		volumePluginMgr:           volume.VolumePluginMgr{},
		secretManager:             secretManager,
		configMapManager:          configMapManager,
		tokenManager:              tokenManager,
		clusterTrustBundleManager: clusterTrustBundleManager,
		podCertificateManager:     kubelet.podCertificateManager,
		informerFactory:           informerFactory,
		csiDriverLister:           csiDriverLister,
		csiDriversSynced:          csiDriversSynced,
	}

	if err := kvh.volumePluginMgr.InitPlugins(plugins, prober, kvh); err != nil {
		return nil, fmt.Errorf(
			"could not initialize volume plugins for KubeletVolumePluginMgr: %v",
			err)
	}

	return &kvh.volumePluginMgr, nil
}

// 编译期断言：kubeletVolumeHost 实现了 VolumeHost 与 KubeletVolumeHost 接口。
var _ volume.VolumeHost = &kubeletVolumeHost{}
var _ volume.KubeletVolumeHost = &kubeletVolumeHost{}

// GetPluginDir 返回插件目录。
func (kvh *kubeletVolumeHost) GetPluginDir(pluginName string) string {
	return kvh.kubelet.getPluginDir(pluginName)
}

// kubeletVolumeHost 是 VolumeHost 的 kubelet 实现，向卷插件暴露 kubelet 能力。
type kubeletVolumeHost struct {
	kubelet                   *Kubelet
	volumePluginMgr           volume.VolumePluginMgr
	secretManager             secret.Manager
	tokenManager              *token.Manager
	configMapManager          configmap.Manager
	clusterTrustBundleManager clustertrustbundle.Manager
	podCertificateManager     podcertificate.Manager
	informerFactory           informers.SharedInformerFactory
	csiDriverLister           storagelisters.CSIDriverLister
	csiDriversSynced          cache.InformerSynced
}

// SetKubeletError 把卷相关的存储错误上报到 kubelet 运行时状态。
func (kvh *kubeletVolumeHost) SetKubeletError(err error) {
	kvh.kubelet.runtimeState.setStorageState(err)
}

// GetVolumeDevicePluginDir 返回卷设备插件目录。
func (kvh *kubeletVolumeHost) GetVolumeDevicePluginDir(pluginName string) string {
	return kvh.kubelet.getVolumeDevicePluginDir(pluginName)
}

// GetPodsDir 返回 Pod 根目录。
func (kvh *kubeletVolumeHost) GetPodsDir() string {
	return kvh.kubelet.getPodsDir()
}

// GetPodVolumeDir 返回 Pod 卷目录；Windows 上转换为 Windows 路径格式。
func (kvh *kubeletVolumeHost) GetPodVolumeDir(podUID types.UID, pluginName string, volumeName string) string {
	dir := kvh.kubelet.getPodVolumeDir(podUID, pluginName, volumeName)
	if runtime.GOOS == "windows" {
		dir = util.GetWindowsPath(dir)
	}
	return dir
}

// GetPodVolumeDeviceDir 返回 Pod 卷设备目录。
func (kvh *kubeletVolumeHost) GetPodVolumeDeviceDir(podUID types.UID, pluginName string) string {
	return kvh.kubelet.getPodVolumeDeviceDir(podUID, pluginName)
}

// GetPodPluginDir 返回 Pod 插件目录。
func (kvh *kubeletVolumeHost) GetPodPluginDir(podUID types.UID, pluginName string) string {
	return kvh.kubelet.getPodPluginDir(podUID, pluginName)
}

// GetKubeClient 返回 kubelet 使用的 API client。
func (kvh *kubeletVolumeHost) GetKubeClient() clientset.Interface {
	return kvh.kubelet.kubeClient
}

// GetSubpather 返回 subpath 操作接口。
func (kvh *kubeletVolumeHost) GetSubpather() subpath.Interface {
	return kvh.kubelet.subpather
}

// GetHostUtil 返回主机文件系统工具。
func (kvh *kubeletVolumeHost) GetHostUtil() hostutil.HostUtils {
	return kvh.kubelet.hostutil
}

// GetInformerFactory 返回共享 informer 工厂；standalone 模式下可能为 nil。
func (kvh *kubeletVolumeHost) GetInformerFactory() informers.SharedInformerFactory {
	return kvh.informerFactory
}

// CSIDriverLister 返回 CSIDriver lister。
func (kvh *kubeletVolumeHost) CSIDriverLister() storagelisters.CSIDriverLister {
	return kvh.csiDriverLister
}

// CSIDriversSynced 返回 CSIDriver informer 的同步函数。
func (kvh *kubeletVolumeHost) CSIDriversSynced() cache.InformerSynced {
	return kvh.csiDriversSynced
}

// WaitForCacheSync 等待 CSIDriverLister 缓存同步完成。
func (kvh *kubeletVolumeHost) WaitForCacheSync() error {
	// 暂时使用 context.TODO()，因为当前没有合适的 context 可传入；
	// 后续重构该函数接收 context 参数时应替换掉。
	logger := klog.FromContext(context.TODO())
	if kvh.csiDriversSynced == nil {
		logger.Error(nil, "CsiDriversSynced not found on KubeletVolumeHost")
		return fmt.Errorf("csiDriversSynced not found on KubeletVolumeHost")
	}

	synced := []cache.InformerSynced{kvh.csiDriversSynced}
	if !cache.WaitForCacheSync(wait.NeverStop, synced...) {
		logger.Info("Failed to wait for cache sync for CSIDriverLister")
		return fmt.Errorf("failed to wait for cache sync for CSIDriverLister")
	}

	return nil
}

// NewWrapperMounter 为包装卷创建 Mounter，卷名统一加 wrapped_ 前缀。
func (kvh *kubeletVolumeHost) NewWrapperMounter(
	volName string,
	spec volume.Spec,
	pod *v1.Pod) (volume.Mounter, error) {
	// 暂时使用 context.TODO()，因为当前没有合适的 context 可传入；
	// 后续重构该函数接收 context 参数时应替换掉。
	logger := klog.FromContext(context.TODO())

	// 包装卷名固定为 wrapped_{原卷名}，避免与原卷冲突。
	wrapperVolumeName := "wrapped_" + volName
	if spec.Volume != nil {
		spec.Volume.Name = wrapperVolumeName
	}

	return kvh.kubelet.newVolumeMounterFromPlugins(logger, &spec, pod)
}

// NewWrapperUnmounter 为包装卷创建 Unmounter。
func (kvh *kubeletVolumeHost) NewWrapperUnmounter(volName string, spec volume.Spec, podUID types.UID) (volume.Unmounter, error) {
	// 包装卷名固定为 wrapped_{原卷名}，避免与原卷冲突。
	wrapperVolumeName := "wrapped_" + volName
	if spec.Volume != nil {
		spec.Volume.Name = wrapperVolumeName
	}

	plugin, err := kvh.kubelet.volumePluginMgr.FindPluginBySpec(&spec)
	if err != nil {
		return nil, err
	}

	return plugin.NewUnmounter(spec.Name(), podUID)
}

// GetMounter 返回 kubelet 使用的 mount 接口。
func (kvh *kubeletVolumeHost) GetMounter() mount.Interface {
	return kvh.kubelet.mounter
}

// GetNodeAllocatable 返回节点可分配资源。
func (kvh *kubeletVolumeHost) GetNodeAllocatable() (v1.ResourceList, error) {
	// TODO: VolumeHost 接口方法支持上下文参数后，传入正确的上下文。
	node, err := kvh.kubelet.getNodeAnyWay(context.TODO())
	if err != nil {
		return nil, fmt.Errorf("error retrieving node: %v", err)
	}
	return node.Status.Allocatable, nil
}

// GetSecretFunc 返回 Secret 读取函数；standalone 模式下返回错误函数。
func (kvh *kubeletVolumeHost) GetSecretFunc() func(namespace, name string) (*v1.Secret, error) {
	if kvh.secretManager != nil {
		return kvh.secretManager.GetSecret
	}
	return func(namespace, name string) (*v1.Secret, error) {
		return nil, fmt.Errorf("not supported due to running kubelet in standalone mode")
	}
}

// GetConfigMapFunc 返回 ConfigMap 读取函数；standalone 模式下返回错误函数。
func (kvh *kubeletVolumeHost) GetConfigMapFunc() func(namespace, name string) (*v1.ConfigMap, error) {
	if kvh.configMapManager != nil {
		return kvh.configMapManager.GetConfigMap
	}
	return func(namespace, name string) (*v1.ConfigMap, error) {
		return nil, fmt.Errorf("not supported due to running kubelet in standalone mode")
	}
}

// GetServiceAccountTokenFunc 返回 ServiceAccount token 申请函数。
func (kvh *kubeletVolumeHost) GetServiceAccountTokenFunc() func(namespace, name string, tr *authenticationv1.TokenRequest) (*authenticationv1.TokenRequest, error) {
	return kvh.tokenManager.GetServiceAccountToken
}

// DeleteServiceAccountTokenFunc 返回 token 清理函数。
func (kvh *kubeletVolumeHost) DeleteServiceAccountTokenFunc() func(podUID types.UID) {
	return kvh.tokenManager.DeleteServiceAccountToken
}

// GetTrustAnchorsByName 按名称读取 ClusterTrustBundle。
func (kvh *kubeletVolumeHost) GetTrustAnchorsByName(ctx context.Context, name string, allowMissing bool) ([]byte, error) {
	return kvh.clusterTrustBundleManager.GetTrustAnchorsByName(ctx, name, allowMissing)
}

// GetTrustAnchorsBySigner 按签名者和标签选择器读取 ClusterTrustBundle。
func (kvh *kubeletVolumeHost) GetTrustAnchorsBySigner(ctx context.Context, signerName string, labelSelector *metav1.LabelSelector, allowMissing bool) ([]byte, error) {
	return kvh.clusterTrustBundleManager.GetTrustAnchorsBySigner(ctx, signerName, labelSelector, allowMissing)
}

// GetPodCertificateCredentialBundle 返回 Pod 证书的凭据包。
func (kvh *kubeletVolumeHost) GetPodCertificateCredentialBundle(ctx context.Context, namespace, podName, podUID, volumeName string, sourceIndex int) ([]byte, []byte, error) {
	return kvh.podCertificateManager.GetPodCertificateCredentialBundle(ctx, namespace, podName, podUID, volumeName, sourceIndex)
}

// GetNodeLabels 返回节点标签。
func (kvh *kubeletVolumeHost) GetNodeLabels() (map[string]string, error) {
	// TODO: VolumeHost 接口方法支持上下文参数后，传入正确的上下文。
	node, err := kvh.kubelet.GetNode(context.TODO())
	if err != nil {
		return nil, fmt.Errorf("error retrieving node: %v", err)
	}
	return node.Labels, nil
}

// GetAttachedVolumesFromNodeStatus 从节点状态构建卷名到设备路径的映射。
func (kvh *kubeletVolumeHost) GetAttachedVolumesFromNodeStatus() (map[v1.UniqueVolumeName]string, error) {
	// TODO: VolumeHost 接口方法支持上下文参数后，传入正确的上下文。
	node, err := kvh.kubelet.GetNode(context.TODO())
	if err != nil {
		return nil, fmt.Errorf("error retrieving node: %v", err)
	}
	attachedVolumes := node.Status.VolumesAttached
	result := map[v1.UniqueVolumeName]string{}
	for i := range attachedVolumes {
		attachedVolume := attachedVolumes[i]
		result[attachedVolume.Name] = attachedVolume.DevicePath
	}
	return result, nil
}

// GetNodeName 返回 kubelet 节点名。
func (kvh *kubeletVolumeHost) GetNodeName() types.NodeName {
	return kvh.kubelet.nodeName
}

// GetEventRecorder 返回事件记录器。
func (kvh *kubeletVolumeHost) GetEventRecorder() record.EventRecorder {
	return kvh.kubelet.recorder
}
