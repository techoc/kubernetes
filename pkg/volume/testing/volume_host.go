/*
Copyright 2020 The Kubernetes Authors.

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

package testing

import (
	"bytes"
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	authenticationv1 "k8s.io/api/authentication/v1"
	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/informers"
	clientset "k8s.io/client-go/kubernetes"
	storagelistersv1 "k8s.io/client-go/listers/storage/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	csilibplugins "k8s.io/csi-translation-lib/plugins"
	. "k8s.io/kubernetes/pkg/volume"
	"k8s.io/kubernetes/pkg/volume/util/hostutil"
	"k8s.io/kubernetes/pkg/volume/util/subpath"
	"k8s.io/mount-utils"
)

// FakeVolumeHost 扩展 VolumeHost，额外暴露插件管理器供测试检查。
type FakeVolumeHost interface {
	VolumeHost

	GetPluginMgr() *VolumePluginMgr
}

// fakeVolumeHost 是用于测试卷插件的通用假 VolumeHost 实现。
// TODO: 将 fakeKubeletVolumeHost 和 fakeAttachDetachVolumeHost 特有的字段拆分出去。
type fakeVolumeHost struct {
	rootDir                string
	kubeClient             clientset.Interface
	pluginMgr              *VolumePluginMgr
	mounter                mount.Interface
	hostUtil               hostutil.HostUtils
	nodeLabels             map[string]string
	nodeName               string
	subpather              subpath.Interface
	node                   *v1.Node
	csiDriverLister        storagelistersv1.CSIDriverLister
	volumeAttachmentLister storagelistersv1.VolumeAttachmentLister
	informerFactory        informers.SharedInformerFactory
	kubeletErr             error
	mux                    sync.Mutex
}

var _ VolumeHost = &fakeVolumeHost{}
var _ FakeVolumeHost = &fakeVolumeHost{}

// NewFakeVolumeHost 创建使用默认节点名和客户端的假 VolumeHost。
func NewFakeVolumeHost(t *testing.T, rootDir string, kubeClient clientset.Interface, plugins []VolumePlugin) FakeVolumeHost {
	return newFakeVolumeHost(t, rootDir, kubeClient, plugins, nil, "", nil, nil)
}

// NewFakeVolumeHostWithCloudProvider 创建假 VolumeHost；云 provider 相关行为已弃用，参数仅为兼容保留。
func NewFakeVolumeHostWithCloudProvider(t *testing.T, rootDir string, kubeClient clientset.Interface, plugins []VolumePlugin) FakeVolumeHost {
	return newFakeVolumeHost(t, rootDir, kubeClient, plugins, nil, "", nil, nil)
}

// NewFakeVolumeHostWithCSINodeName 创建带指定节点名和 CSI lister 的假 VolumeHost。
func NewFakeVolumeHostWithCSINodeName(t *testing.T, rootDir string, kubeClient clientset.Interface, plugins []VolumePlugin, nodeName string, driverLister storagelistersv1.CSIDriverLister, volumeAttachLister storagelistersv1.VolumeAttachmentLister) FakeVolumeHost {
	return newFakeVolumeHost(t, rootDir, kubeClient, plugins, nil, nodeName, driverLister, volumeAttachLister)
}

// newFakeVolumeHost 组装假 VolumeHost 并初始化插件，等待 kubelet 错误清空后再返回。
func newFakeVolumeHost(t *testing.T, rootDir string, kubeClient clientset.Interface, plugins []VolumePlugin, pathToTypeMap map[string]hostutil.FileType, nodeName string, driverLister storagelistersv1.CSIDriverLister, volumeAttachLister storagelistersv1.VolumeAttachmentLister) FakeVolumeHost {
	host := &fakeVolumeHost{rootDir: rootDir, kubeClient: kubeClient, nodeName: nodeName, csiDriverLister: driverLister, volumeAttachmentLister: volumeAttachLister}
	host.mounter = mount.NewFakeMounter(nil)
	host.hostUtil = hostutil.NewFakeHostUtil(pathToTypeMap)
	host.pluginMgr = &VolumePluginMgr{}
	if err := host.pluginMgr.InitPlugins(plugins, nil /* prober */, host); err != nil {
		t.Fatalf("Failed to init plugins while creating fake volume host: %v", err)
	}
	host.subpather = &subpath.FakeSubpath{}
	host.informerFactory = informers.NewSharedInformerFactory(kubeClient, time.Minute)
	// 等 InitPlugins 完成再返回，避免测试看到尚未就绪的插件管理器。
	if err := host.WaitForKubeletErrNil(); err != nil {
		t.Fatalf("Failed to wait for kubelet err to be nil while creating fake volume host: %v", err)
	}
	return host
}

// GetPluginDir 返回指定 Pod 的插件目录。
func (f *fakeVolumeHost) GetPluginDir(podUID string) string {
	return filepath.Join(f.rootDir, "plugins", podUID)
}

// GetVolumeDevicePluginDir 返回指定插件的卷设备目录。
func (f *fakeVolumeHost) GetVolumeDevicePluginDir(pluginName string) string {
	return filepath.Join(f.rootDir, "plugins", pluginName, "volumeDevices")
}

// GetPodsDir 返回存放所有 Pod 目录的根路径。
func (f *fakeVolumeHost) GetPodsDir() string {
	return filepath.Join(f.rootDir, "pods")
}

// GetPodVolumeDir 返回指定 Pod 下某个插件卷的挂载目录。
func (f *fakeVolumeHost) GetPodVolumeDir(podUID types.UID, pluginName, volumeName string) string {
	return filepath.Join(f.rootDir, "pods", string(podUID), "volumes", pluginName, volumeName)
}

// GetPodVolumeDeviceDir 返回指定 Pod 下某个插件的卷设备目录。
func (f *fakeVolumeHost) GetPodVolumeDeviceDir(podUID types.UID, pluginName string) string {
	return filepath.Join(f.rootDir, "pods", string(podUID), "volumeDevices", pluginName)
}

// GetPodPluginDir 返回指定 Pod 下某个插件的私有目录。
func (f *fakeVolumeHost) GetPodPluginDir(podUID types.UID, pluginName string) string {
	return filepath.Join(f.rootDir, "pods", string(podUID), "plugins", pluginName)
}

// GetKubeClient 返回供插件使用的假 Kubernetes 客户端。
func (f *fakeVolumeHost) GetKubeClient() clientset.Interface {
	return f.kubeClient
}

// GetMounter 返回假挂载器，测试中不触碰真实系统挂载。
func (f *fakeVolumeHost) GetMounter() mount.Interface {
	return f.mounter
}

// GetSubpather 返回假 subpath 接口实现。
func (f *fakeVolumeHost) GetSubpather() subpath.Interface {
	return f.subpather
}

// GetPluginMgr 返回插件管理器，供测试断言插件注册状态。
func (f *fakeVolumeHost) GetPluginMgr() *VolumePluginMgr {
	return f.pluginMgr
}

// GetAttachedVolumesFromNodeStatus 返回空映射，假主机上没有节点状态可查询。
func (f *fakeVolumeHost) GetAttachedVolumesFromNodeStatus() (map[v1.UniqueVolumeName]string, error) {
	return map[v1.UniqueVolumeName]string{}, nil
}

// NewWrapperMounter 创建包装卷的 Mounter，卷名加 wrapped_ 前缀以区分原卷。
func (f *fakeVolumeHost) NewWrapperMounter(volName string, spec Spec, pod *v1.Pod) (Mounter, error) {
	// 包装卷使用 wrapped_ 前缀命名，避免与原卷混淆。
	wrapperVolumeName := "wrapped_" + volName
	if spec.Volume != nil {
		spec.Volume.Name = wrapperVolumeName
	}
	plug, err := f.pluginMgr.FindPluginBySpec(&spec)
	if err != nil {
		return nil, err
	}
	return plug.NewMounter(&spec, pod)
}

// NewWrapperUnmounter 创建包装卷的 Unmounter，命名规则与 NewWrapperMounter 一致。
func (f *fakeVolumeHost) NewWrapperUnmounter(volName string, spec Spec, podUID types.UID) (Unmounter, error) {
	// 与 NewWrapperMounter 使用相同的 wrapped_ 前缀，确保查到同一个插件。
	wrapperVolumeName := "wrapped_" + volName
	if spec.Volume != nil {
		spec.Volume.Name = wrapperVolumeName
	}
	plug, err := f.pluginMgr.FindPluginBySpec(&spec)
	if err != nil {
		return nil, err
	}
	return plug.NewUnmounter(spec.Name(), podUID)
}

// GetNodeAllocatable 返回空的可分配资源列表，测试中不依赖真实节点容量。
func (f *fakeVolumeHost) GetNodeAllocatable() (v1.ResourceList, error) {
	return v1.ResourceList{}, nil
}

// GetSecretFunc 返回从假客户端读取 Secret 的函数。
func (f *fakeVolumeHost) GetSecretFunc() func(namespace, name string) (*v1.Secret, error) {
	return func(namespace, name string) (*v1.Secret, error) {
		return f.kubeClient.CoreV1().Secrets(namespace).Get(context.TODO(), name, metav1.GetOptions{})
	}
}

// GetConfigMapFunc 返回从假客户端读取 ConfigMap 的函数。
func (f *fakeVolumeHost) GetConfigMapFunc() func(namespace, name string) (*v1.ConfigMap, error) {
	return func(namespace, name string) (*v1.ConfigMap, error) {
		return f.kubeClient.CoreV1().ConfigMaps(namespace).Get(context.TODO(), name, metav1.GetOptions{})
	}
}

// GetServiceAccountTokenFunc 返回通过假客户端申请 ServiceAccount Token 的函数。
func (f *fakeVolumeHost) GetServiceAccountTokenFunc() func(string, string, *authenticationv1.TokenRequest) (*authenticationv1.TokenRequest, error) {
	return func(namespace, name string, tr *authenticationv1.TokenRequest) (*authenticationv1.TokenRequest, error) {
		return f.kubeClient.CoreV1().ServiceAccounts(namespace).CreateToken(context.TODO(), name, tr, metav1.CreateOptions{})
	}
}

// DeleteServiceAccountTokenFunc 返回空的 Token 清理函数，假环境无需撤销 Token。
func (f *fakeVolumeHost) DeleteServiceAccountTokenFunc() func(types.UID) {
	return func(types.UID) {}
}

// GetNodeLabels 返回测试用节点标签，未设置时填充默认值。
func (f *fakeVolumeHost) GetNodeLabels() (map[string]string, error) {
	if f.nodeLabels == nil {
		f.nodeLabels = map[string]string{"test-label": "test-value"}
	}
	return f.nodeLabels, nil
}

// GetNodeName 返回假主机的节点名。
func (f *fakeVolumeHost) GetNodeName() types.NodeName {
	return types.NodeName(f.nodeName)
}

// GetEventRecorder 返回 nil，测试中不记录事件。
func (f *fakeVolumeHost) GetEventRecorder() record.EventRecorder {
	return nil
}

// WaitForKubeletErrNil 轮询等待 kubeletErr 清空，说明插件初始化已完成。
func (f *fakeVolumeHost) WaitForKubeletErrNil() error {
	return wait.PollImmediate(10*time.Millisecond, 10*time.Second, func() (bool, error) {
		f.mux.Lock()
		defer f.mux.Unlock()
		return f.kubeletErr == nil, nil
	})
}

// fakeAttachDetachVolumeHost 是满足 AttachDetachVolumeHost 接口的假实现，嵌入通用 fakeVolumeHost 复用其能力。
type fakeAttachDetachVolumeHost struct {
	fakeVolumeHost
}

var _ AttachDetachVolumeHost = &fakeAttachDetachVolumeHost{}
var _ FakeVolumeHost = &fakeAttachDetachVolumeHost{}

// NewFakeAttachDetachVolumeHostWithCSINodeName 创建带节点名和 CSI lister 的假 AttachDetachVolumeHost。
func NewFakeAttachDetachVolumeHostWithCSINodeName(t *testing.T, rootDir string, kubeClient clientset.Interface, plugins []VolumePlugin, nodeName string, driverLister storagelistersv1.CSIDriverLister, volumeAttachLister storagelistersv1.VolumeAttachmentLister) FakeVolumeHost {
	return newFakeAttachDetachVolumeHost(t, rootDir, kubeClient, plugins, nil, nodeName, driverLister, volumeAttachLister)
}

// newFakeAttachDetachVolumeHost 组装假 AttachDetachVolumeHost 并等待插件初始化完成。
func newFakeAttachDetachVolumeHost(t *testing.T, rootDir string, kubeClient clientset.Interface, plugins []VolumePlugin, pathToTypeMap map[string]hostutil.FileType, nodeName string, driverLister storagelistersv1.CSIDriverLister, volumeAttachLister storagelistersv1.VolumeAttachmentLister) FakeVolumeHost {
	host := &fakeAttachDetachVolumeHost{}
	host.rootDir = rootDir
	host.kubeClient = kubeClient
	host.nodeName = nodeName
	host.csiDriverLister = driverLister
	host.volumeAttachmentLister = volumeAttachLister
	host.mounter = mount.NewFakeMounter(nil)
	host.hostUtil = hostutil.NewFakeHostUtil(pathToTypeMap)
	host.pluginMgr = &VolumePluginMgr{}
	if err := host.pluginMgr.InitPlugins(plugins, nil /* prober */, host); err != nil {
		t.Fatalf("Failed to init plugins while creating fake volume host: %v", err)
	}
	host.subpather = &subpath.FakeSubpath{}
	host.informerFactory = informers.NewSharedInformerFactory(kubeClient, time.Minute)
	// 等 InitPlugins 完成再返回，避免测试看到尚未就绪的插件管理器。
	if err := host.WaitForKubeletErrNil(); err != nil {
		t.Fatalf("Failed to wait for kubelet err to be nil while creating fake volume host: %v", err)
	}
	return host
}

// CSINodeLister 返回包含一个合成 CSINode 的 lister，并标记 GCE PD 驱动已迁移。
func (f *fakeAttachDetachVolumeHost) CSINodeLister() storagelistersv1.CSINodeLister {
	csiNode := &storagev1.CSINode{
		ObjectMeta: metav1.ObjectMeta{Name: f.nodeName},
		Spec: storagev1.CSINodeSpec{
			Drivers: []storagev1.CSINodeDriver{},
		},
	}
	enableMigrationOnNode(csiNode, csilibplugins.GCEPDInTreePluginName)
	return getFakeCSINodeLister(csiNode)
}

// enableMigrationOnNode 在 CSINode 注解中标记指定 in-tree 插件已迁移到 CSI。
func enableMigrationOnNode(csiNode *storagev1.CSINode, pluginName string) {
	nodeInfoAnnotations := csiNode.GetAnnotations()
	if nodeInfoAnnotations == nil {
		nodeInfoAnnotations = map[string]string{}
	}

	newAnnotationSet := sets.New[string]()
	newAnnotationSet.Insert(pluginName)
	nas := strings.Join(sets.List(newAnnotationSet), ",")
	nodeInfoAnnotations[v1.MigratedPluginsAnnotationKey] = nas

	csiNode.Annotations = nodeInfoAnnotations
}

// CSIDriverLister 返回构造时传入的 CSIDriver lister。
func (f *fakeAttachDetachVolumeHost) CSIDriverLister() storagelistersv1.CSIDriverLister {
	return f.csiDriverLister
}

// VolumeAttachmentLister 返回构造时传入的卷附件 lister。
func (f *fakeAttachDetachVolumeHost) VolumeAttachmentLister() storagelistersv1.VolumeAttachmentLister {
	return f.volumeAttachmentLister
}

// IsAttachDetachController 标明该假主机代表 AttachDetach 控制器。
func (f *fakeAttachDetachVolumeHost) IsAttachDetachController() bool {
	return true
}

// fakeKubeletVolumeHost 是满足 KubeletVolumeHost 接口的假实现，嵌入通用 fakeVolumeHost 复用其能力。
type fakeKubeletVolumeHost struct {
	fakeVolumeHost
}

var _ KubeletVolumeHost = &fakeKubeletVolumeHost{}
var _ FakeVolumeHost = &fakeKubeletVolumeHost{}

// NewFakeKubeletVolumeHost 创建使用默认节点名的假 KubeletVolumeHost。
func NewFakeKubeletVolumeHost(t *testing.T, rootDir string, kubeClient clientset.Interface, plugins []VolumePlugin) *fakeKubeletVolumeHost {
	return newFakeKubeletVolumeHost(t, rootDir, kubeClient, plugins, nil, "", nil, nil)
}

// NewFakeKubeletVolumeHostWithCSINodeName 创建带节点名和 CSI lister 的假 KubeletVolumeHost。
func NewFakeKubeletVolumeHostWithCSINodeName(t *testing.T, rootDir string, kubeClient clientset.Interface, plugins []VolumePlugin, nodeName string, driverLister storagelistersv1.CSIDriverLister, volumeAttachLister storagelistersv1.VolumeAttachmentLister) *fakeKubeletVolumeHost {
	return newFakeKubeletVolumeHost(t, rootDir, kubeClient, plugins, nil, nodeName, driverLister, volumeAttachLister)
}

// NewFakeKubeletVolumeHostWithMounterFSType 创建可指定路径到文件类型映射的假 KubeletVolumeHost。
func NewFakeKubeletVolumeHostWithMounterFSType(t *testing.T, rootDir string, kubeClient clientset.Interface, plugins []VolumePlugin, pathToTypeMap map[string]hostutil.FileType) *fakeKubeletVolumeHost {
	return newFakeKubeletVolumeHost(t, rootDir, kubeClient, plugins, pathToTypeMap, "", nil, nil)
}

// newFakeKubeletVolumeHost 组装假 KubeletVolumeHost 并等待插件初始化完成。
func newFakeKubeletVolumeHost(t *testing.T, rootDir string, kubeClient clientset.Interface, plugins []VolumePlugin, pathToTypeMap map[string]hostutil.FileType, nodeName string, driverLister storagelistersv1.CSIDriverLister, volumeAttachLister storagelistersv1.VolumeAttachmentLister) *fakeKubeletVolumeHost {
	host := &fakeKubeletVolumeHost{}
	host.rootDir = rootDir
	host.kubeClient = kubeClient
	host.nodeName = nodeName
	host.csiDriverLister = driverLister
	host.volumeAttachmentLister = volumeAttachLister
	host.mounter = mount.NewFakeMounter(nil)
	host.hostUtil = hostutil.NewFakeHostUtil(pathToTypeMap)
	host.pluginMgr = &VolumePluginMgr{}
	if err := host.pluginMgr.InitPlugins(plugins, nil /* prober */, host); err != nil {
		t.Fatalf("Failed to init plugins while creating fake volume host: %v", err)
	}
	host.subpather = &subpath.FakeSubpath{}
	host.informerFactory = informers.NewSharedInformerFactory(kubeClient, time.Minute)
	// 等 InitPlugins 完成再返回，避免测试看到尚未就绪的插件管理器。
	if err := host.WaitForKubeletErrNil(); err != nil {
		t.Fatalf("Failed to wait for kubelet err to be nil while creating fake volume host: %v", err)
	}
	return host
}

// WithNode 设置假主机的 Node 对象，返回自身以便链式调用。
func (f *fakeKubeletVolumeHost) WithNode(node *v1.Node) *fakeKubeletVolumeHost {
	f.node = node
	return f
}

// CSINodeLister 是基于切片实现的假 CSINodeLister。
type CSINodeLister []storagev1.CSINode

// Get 按名称返回对应的假 CSINode 对象。
func (n CSINodeLister) Get(name string) (*storagev1.CSINode, error) {
	for _, cn := range n {
		if cn.Name == name {
			return &cn, nil
		}
	}
	return nil, fmt.Errorf("csiNode %q not found", name)
}

// List 未实现，测试中不需要列出所有 CSINode。
func (n CSINodeLister) List(selector labels.Selector) (ret []*storagev1.CSINode, err error) {
	return nil, fmt.Errorf("not implemented")
}

// getFakeCSINodeLister 用 CSINode 的深拷贝构造 lister，避免测试改动影响原对象。
func getFakeCSINodeLister(csiNode *storagev1.CSINode) CSINodeLister {
	csiNodeLister := CSINodeLister{}
	if csiNode != nil {
		csiNodeLister = append(csiNodeLister, *csiNode.DeepCopy())
	}
	return csiNodeLister
}

// SetKubeletError 设置模拟的 kubelet 错误，供等待逻辑测试使用。
func (f *fakeKubeletVolumeHost) SetKubeletError(err error) {
	f.mux.Lock()
	defer f.mux.Unlock()
	f.kubeletErr = err
}

// GetInformerFactory 返回测试用的共享 informer 工厂。
func (f *fakeKubeletVolumeHost) GetInformerFactory() informers.SharedInformerFactory {
	return f.informerFactory
}

// GetAttachedVolumesFromNodeStatus 从假 Node 状态中提取已挂载卷及其设备路径。
func (f *fakeKubeletVolumeHost) GetAttachedVolumesFromNodeStatus() (map[v1.UniqueVolumeName]string, error) {
	result := map[v1.UniqueVolumeName]string{}
	if f.node != nil {
		for _, av := range f.node.Status.VolumesAttached {
			result[av.Name] = av.DevicePath
		}
	}

	return result, nil
}

// CSIDriverLister 返回构造时传入的 CSIDriver lister。
func (f *fakeKubeletVolumeHost) CSIDriverLister() storagelistersv1.CSIDriverLister {
	return f.csiDriverLister
}

// CSIDriversSynced 返回 nil，测试中不依赖 informer 同步信号。
func (f *fakeKubeletVolumeHost) CSIDriversSynced() cache.InformerSynced {
	// 测试无需等待 CSIDriver informer 同步。
	return nil
}

// WaitForCacheSync 返回 nil，假环境无需等待缓存同步。
func (f *fakeKubeletVolumeHost) WaitForCacheSync() error {
	return nil
}

// GetHostUtil 返回假主机工具集，用于模拟文件系统查询。
func (f *fakeKubeletVolumeHost) GetHostUtil() hostutil.HostUtils {
	return f.hostUtil
}

// GetTrustAnchorsByName 从假客户端读取指定名称的 ClusterTrustBundle 作为信任锚点。
func (f *fakeKubeletVolumeHost) GetTrustAnchorsByName(ctx context.Context, name string, allowMissing bool) ([]byte, error) {
	ctb, err := f.kubeClient.CertificatesV1().ClusterTrustBundles().Get(context.Background(), name, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("while getting ClusterTrustBundle %s: %w", name, err)
	}

	return []byte(ctb.Spec.TrustBundle), nil
}

// GetTrustAnchorsBySigner 把所有 ClusterTrustBundle 简单拼接为信任锚点。
// 注意：真实实现需要去重和排序，这里刻意不做。
func (f *fakeKubeletVolumeHost) GetTrustAnchorsBySigner(ctx context.Context, signerName string, labelSelector *metav1.LabelSelector, allowMissing bool) ([]byte, error) {
	ctbList, err := f.kubeClient.CertificatesV1().ClusterTrustBundles().List(context.Background(), metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("while listing all ClusterTrustBundles: %w", err)
	}

	fullSet := bytes.Buffer{}
	for i, ctb := range ctbList.Items {
		fullSet.WriteString(ctb.Spec.TrustBundle)
		if i != len(ctbList.Items)-1 {
			fullSet.WriteString("\n")
		}
	}

	return fullSet.Bytes(), nil
}

// GetPodCertificateCredentialBundle 返回固定的假私钥和证书，供 Pod 证书卷测试使用。
func (f *fakeKubeletVolumeHost) GetPodCertificateCredentialBundle(ctx context.Context, namespace, podName, podUID, volumeName string, sourceIndex int) ([]byte, []byte, error) {
	return []byte("key\n"), []byte("cert\n"), nil
}
