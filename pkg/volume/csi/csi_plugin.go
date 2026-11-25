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

package csi

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"k8s.io/klog/v2"

	authenticationv1 "k8s.io/api/authentication/v1"
	api "k8s.io/api/core/v1"
	storage "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	utilversion "k8s.io/apimachinery/pkg/util/version"
	"k8s.io/apimachinery/pkg/util/wait"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	clientset "k8s.io/client-go/kubernetes"
	storagelisters "k8s.io/client-go/listers/storage/v1"
	"k8s.io/client-go/tools/cache"
	csitranslationplugins "k8s.io/csi-translation-lib/plugins"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/pkg/kubelet/util"
	"k8s.io/kubernetes/pkg/volume"
	"k8s.io/kubernetes/pkg/volume/csi/nodeinfomanager"
	volumetypes "k8s.io/kubernetes/pkg/volume/util/types"
)

const (
	// CSIPluginName 是树内 CSI 插件的名称。
	CSIPluginName = "kubernetes.io/csi"

	csiTimeout      = 2 * time.Minute
	volNameSep      = "^"
	volDataFileName = "vol_data.json"
	fsTypeBlockName = "block"

	// CsiResyncPeriod 是默认的重新同步周期。
	// TODO: 将重新同步周期调整为更有意义的值。
	CsiResyncPeriod = time.Minute
)

// csiPlugin 实现 CSI 卷插件，桥接 kubelet 与外部 CSI 驱动。
type csiPlugin struct {
	host                      volume.VolumeHost
	csiDriverLister           storagelisters.CSIDriverLister
	csiDriverInformer         cache.SharedIndexInformer
	serviceAccountTokenGetter func(namespace, name string, tr *authenticationv1.TokenRequest) (*authenticationv1.TokenRequest, error)
	volumeAttachmentLister    storagelisters.VolumeAttachmentLister
}

// ProbeVolumePlugins 返回已实现的 CSI 插件实例列表。
func ProbeVolumePlugins() []volume.VolumePlugin {
	p := &csiPlugin{
		host: nil,
	}
	return []volume.VolumePlugin{p}
}

// 编译时断言 csiPlugin 实现 VolumePlugin 接口。
var _ volume.VolumePlugin = &csiPlugin{}

// RegistrationHandler 实现 pluginwatcher API 所需的驱动注册回调。
type RegistrationHandler struct {
	csiPlugin *csiPlugin
}

// TODO (verult)：考虑使用结构体替代全局变量。
// csiDrivers 记录节点上所有已注册的 CSI 驱动及其对应的 socket 端点。
var csiDrivers = &DriversStore{}

// 节点信息管理器，负责维护 CSINode 对象。
var nim nodeinfomanager.Interface

// 用于动态更新 CSINode 可分配卷数量的 updater。
var csiNodeUpdaterVar *csiNodeUpdater

// PluginHandler 是传给 kubelet pluginwatcher 模块的插件注册处理器。
var PluginHandler = &RegistrationHandler{}

// ValidatePlugin 在 kubelet 的 pluginwatcher 发现 CSI 驱动注册器新建 socket 时调用，
// 用于校验驱动声明的版本是否受支持。
func (h *RegistrationHandler) ValidatePlugin(ctx context.Context, pluginName string, endpoint string, versions []string) error {
	klog.Info(log("Trying to validate a new CSI Driver with name: %s endpoint: %s versions: %s",
		pluginName, endpoint, strings.Join(versions, ",")))

	_, err := h.validateVersions("ValidatePlugin", pluginName, endpoint, versions)
	if err != nil {
		return fmt.Errorf("validation failed for CSI Driver %s at endpoint %s: %v", pluginName, endpoint, err)
	}

	return err
}

// RegisterPlugin 在驱动可注册时调用：保存端点、获取节点信息并初始化 CSINode。
// 任一步失败都会注销驱动，避免留下不完整注册状态。
func (h *RegistrationHandler) RegisterPlugin(ctx context.Context, pluginName string, endpoint string, versions []string, pluginClientTimeout *time.Duration) error {
	klog.Info(log("Register new plugin with name: %s at endpoint: %s", pluginName, endpoint))

	highestSupportedVersion, err := h.validateVersions("RegisterPlugin", pluginName, endpoint, versions)
	if err != nil {
		return err
	}

	// 以驱动名为键保存新注册驱动的端点，
	// 其他 CSI 组件即可按名称找到对应的 socket。
	csiDrivers.Set(pluginName, Driver{
		endpoint:                endpoint,
		highestSupportedVersion: highestSupportedVersion,
	})

	// 从驱动获取节点信息（节点 ID、每节点卷数上限、可访问拓扑）。
	csi, err := newCsiDriverClient(csiDriverName(pluginName))
	if err != nil {
		return err
	}

	var timeout time.Duration
	if pluginClientTimeout == nil {
		timeout = csiTimeout
	} else {
		timeout = *pluginClientTimeout
	}

	newCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	driverNodeID, maxVolumePerNode, accessibleTopology, err := csi.NodeGetInfo(newCtx)
	if err != nil {
		if unregErr := unregisterDriver(pluginName); unregErr != nil {
			klog.Error(log("registrationHandler.RegisterPlugin failed to unregister plugin due to previous error: %v", unregErr))
		}
		return err
	}

	err = nim.InstallCSIDriver(pluginName, driverNodeID, maxVolumePerNode, accessibleTopology)
	if err != nil {
		if unregErr := unregisterDriver(pluginName); unregErr != nil {
			klog.Error(log("registrationHandler.RegisterPlugin failed to unregister plugin due to previous error: %v", unregErr))
		}
		return err
	}

	if csiNodeUpdaterVar != nil {
		csiNodeUpdaterVar.syncDriverUpdater(pluginName)
	}

	return nil
}

// updateCSIDriver 重新向驱动查询节点信息并更新 CSINode 中的记录。
func updateCSIDriver(pluginName string) error {
	csi, err := newCsiDriverClient(csiDriverName(pluginName))
	if err != nil {
		return fmt.Errorf("failed to create CSI client for driver %q: %w", pluginName, err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), csiTimeout)
	defer cancel()

	driverNodeID, maxVolumePerNode, accessibleTopology, err := csi.NodeGetInfo(ctx)
	if err != nil {
		return fmt.Errorf("failed to get NodeGetInfo from driver %q: %w", pluginName, err)
	}

	if err := nim.UpdateCSIDriver(pluginName, driverNodeID, maxVolumePerNode, accessibleTopology); err != nil {
		return fmt.Errorf("failed to update driver %q: %w", pluginName, err)
	}
	return nil
}

// VerifyExhaustedResource 检查卷的 VolumeAttachment 是否报告资源耗尽错误。
// 检测到时刷新驱动信息并返回 true，供上层触发重新调度。
func (p *csiPlugin) VerifyExhaustedResource(spec *volume.Spec) bool {
	if spec == nil || spec.PersistentVolume == nil || spec.PersistentVolume.Spec.CSI == nil {
		klog.ErrorS(nil, "Invalid volume spec for CSI")
		return false
	}

	pluginName := spec.PersistentVolume.Spec.CSI.Driver

	driver, err := p.getCSIDriver(pluginName)
	if err != nil {
		klog.ErrorS(err, "Failed to retrieve CSIDriver", "pluginName", pluginName)
		return false
	}

	period := getNodeAllocatableUpdatePeriod(driver)
	if period == 0 {
		return false
	}

	volumeHandle := spec.PersistentVolume.Spec.CSI.VolumeHandle
	attachmentName := getAttachmentName(volumeHandle, pluginName, string(p.host.GetNodeName()))
	kubeClient := p.host.GetKubeClient()

	ctx, cancel := context.WithTimeout(context.Background(), csiTimeout)
	defer cancel()

	attachment, err := kubeClient.StorageV1().VolumeAttachments().Get(ctx, attachmentName, meta.GetOptions{})
	if err != nil {
		klog.ErrorS(err, "Failed to get volume attachment", "attachmentName", attachmentName)
		return false
	}

	if isResourceExhaustError(attachment) {
		klog.V(4).InfoS("Detected ResourceExhausted error for volume", "pluginName", pluginName, "volumeHandle", volumeHandle)
		if err := updateCSIDriver(pluginName); err != nil {
			klog.ErrorS(err, "Failed to update CSIDriver", "pluginName", pluginName)
		}
		return true
	}
	return false
}

// isResourceExhaustError 判断卷附加错误码是否为 gRPC ResourceExhausted。
func isResourceExhaustError(attachment *storage.VolumeAttachment) bool {
	if attachment == nil || attachment.Status.AttachError == nil {
		return false
	}
	return attachment.Status.AttachError.ErrorCode != nil &&
		*attachment.Status.AttachError.ErrorCode == int32(codes.ResourceExhausted)
}

// validateVersions 校验驱动声明的版本并返回其中受支持的最高版本。
// 若同名驱动已以更高版本注册，则拒绝本次注册。
func (h *RegistrationHandler) validateVersions(callerName, pluginName string, endpoint string, versions []string) (*utilversion.Version, error) {
	if len(versions) == 0 {
		return nil, errors.New(log("%s for CSI driver %q failed. Plugin returned an empty list for supported versions", callerName, pluginName))
	}

	// 校验版本：CSI 目前只有 0.x 和 1.x（参见 https://github.com/container-storage-interface/spec/releases）。
	// 因此声明 2.x 及以上的驱动会被当作不支持而忽略。
	// 1.x 后续版本应向后兼容，本版本 Kubernetes 可配合任意 1.x（或 0.x）驱动；
	// 但可能不兼容 2.x，因为 2.x 无需向后兼容 1.x。
	// 按 Kubernetes v1.13 的弃用策略，自 v1.17 起不再支持 CSI v0.x。
	newDriverHighestVersion, err := utilversion.HighestSupportedVersion(versions)
	if err != nil {
		return nil, errors.New(log("%s for CSI driver %q failed. None of the versions specified %q are supported. err=%v", callerName, pluginName, versions, err))
	}

	existingDriver, driverExists := csiDrivers.Get(pluginName)
	if driverExists {
		if !existingDriver.highestSupportedVersion.LessThan(newDriverHighestVersion) {
			return nil, errors.New(log("%s for CSI driver %q failed. Another driver with the same name is already registered with a higher supported version: %q", callerName, pluginName, existingDriver.highestSupportedVersion))
		}
	}

	return newDriverHighestVersion, nil
}

// DeRegisterPlugin 在插件移除 socket 时调用，表示驱动已不可用。
func (h *RegistrationHandler) DeRegisterPlugin(ctx context.Context, pluginName, endpoint string) {
	klog.Info(log("registrationHandler.DeRegisterPlugin request for plugin %s, endpoint %s", pluginName, endpoint))
	if err := unregisterDriver(pluginName); err != nil {
		klog.Error(log("registrationHandler.DeRegisterPlugin failed: %v", err))
	}

	if csiNodeUpdaterVar != nil {
		csiNodeUpdaterVar.syncDriverUpdater(pluginName)
	}
}

// Init 初始化插件：注入 VolumeHost、配置 lister 与 token 获取函数，并初始化 CSINode。
func (p *csiPlugin) Init(host volume.VolumeHost) error {
	p.host = host

	csiClient := host.GetKubeClient()
	if csiClient == nil {
		klog.Warning(log("kubeclient not set, assuming standalone kubelet"))
	} else {
		// 从 host 取 CSIDriverLister 和 volumeAttachmentLister。
		seLinuxHost, ok := host.(volume.CSIDriverVolumeHost)
		if ok {
			p.csiDriverLister = seLinuxHost.CSIDriverLister()
			if p.csiDriverLister == nil {
				klog.Error(log("CSIDriverLister not found on CSIDriverVolumeHost"))
			}
		}
		adcHost, ok := host.(volume.AttachDetachVolumeHost)
		if ok {
			p.volumeAttachmentLister = adcHost.VolumeAttachmentLister()
			if p.volumeAttachmentLister == nil {
				klog.Error(log("VolumeAttachmentLister not found on AttachDetachVolumeHost"))
			}
		}
		kletHost, ok := host.(volume.KubeletVolumeHost)
		if ok {
			p.csiDriverLister = kletHost.CSIDriverLister()
			if p.csiDriverLister == nil {
				klog.Error(log("CSIDriverLister not found on KubeletVolumeHost"))
			}
			p.serviceAccountTokenGetter = host.GetServiceAccountTokenFunc()
			if p.serviceAccountTokenGetter == nil {
				klog.Error(log("ServiceAccountTokenGetter not found on KubeletVolumeHost"))
			}
			// kubelet 上下文不使用 volumeAttachmentLister（附加由 controller 负责）。
			p.volumeAttachmentLister = nil

			informerFactory := kletHost.GetInformerFactory()
			if informerFactory == nil {
				klog.Error(log("InformerFactory not found on KubeletVolumeHost"))
			} else {
				p.csiDriverInformer = informerFactory.Storage().V1().CSIDrivers().Informer()
			}
		}
	}

	var migratedPlugins = map[string](func() bool){
		csitranslationplugins.GCEPDInTreePluginName: func() bool {
			return true
		},
		csitranslationplugins.AWSEBSInTreePluginName: func() bool {
			return true
		},
		csitranslationplugins.CinderInTreePluginName: func() bool {
			return true
		},
		csitranslationplugins.AzureDiskInTreePluginName: func() bool {
			return true
		},
		csitranslationplugins.AzureFileInTreePluginName: func() bool {
			return true
		},
		csitranslationplugins.VSphereInTreePluginName: func() bool {
			return true
		},
		csitranslationplugins.PortworxVolumePluginName: func() bool {
			return true
		},
	}

	// 初始化节点信息管理器和插件处理器。
	nim = nodeinfomanager.NewNodeInfoManager(host.GetNodeName(), host, migratedPlugins)
	PluginHandler.csiPlugin = p

	// 在 CSINode 安装并初始化完成前阻止 kubelet 上报 Ready 状态。
	if err := initializeCSINode(host, p.csiDriverInformer); err != nil {
		return errors.New(log("failed to initialize CSINode: %v", err))
	}
	return nil
}

// initializeCSINode 在后台初始化 CSINode：等待 APIServer 可用后初始化已迁移驱动，
// 完成后才允许 kubelet 上报 Ready；非 kubelet 或单机模式直接跳过。
func initializeCSINode(host volume.VolumeHost, csiDriverInformer cache.SharedIndexInformer) error {
	kvh, ok := host.(volume.KubeletVolumeHost)
	if !ok {
		klog.V(4).Info("Cast from VolumeHost to KubeletVolumeHost failed. Skipping CSINode initialization, not running on kubelet")
		return nil
	}
	kubeClient := host.GetKubeClient()
	if kubeClient == nil {
		// 单机模式无 APIServer，跳过 CSINode 初始化。
		klog.Warning("Skipping CSINode initialization, kubelet running in standalone mode")
		return nil
	}

	kvh.SetKubeletError(errors.New("CSINode is not yet initialized"))

	go func() {
		defer utilruntime.HandleCrash()

		// 先无限等待，直到能与 APIServer 通信。
		nodeName := host.GetNodeName()
		err := waitForAPIServerForever(kubeClient, nodeName)
		if err != nil {
			klog.Fatalf("Failed to initialize CSINode while waiting for API server to report ok: %v", err)
		}

		// 退避参数按总计约 140 秒重试调优；超过最大次数即失败并重启 kubelet。
		initBackoff := wait.Backoff{
			Steps:    6,
			Duration: 30 * time.Millisecond,
			Factor:   8.0,
			Jitter:   0.1,
		}
		err = wait.ExponentialBackoff(initBackoff, func() (bool, error) {
			klog.V(4).Infof("Initializing migrated drivers on CSINode")
			err := nim.InitializeCSINodeWithAnnotation()
			if err != nil {
				kvh.SetKubeletError(fmt.Errorf("failed to initialize CSINode: %v", err))
				klog.Errorf("Failed to initialize CSINode: %v", err)
				return false, nil
			}

			// 驱动初始化成功，允许 kubelet 上报 Ready。
			kvh.SetKubeletError(nil)
			return true, nil
		})
		if err != nil {
			// 当 CSIMigration 及各 CSIMigrationX 永久启用两个版本后，
			// apiserver/controller 可假定 kubelet 对所有迁移插件都使用 CSI，
			// 届时可移除 kubelet 中的 CSINode 初始化代码。
			// 目前选择终止 kubelet 进程，重启后重试初始化。
			klog.Fatalf("Failed to initialize CSINode after retrying: %v", err)
		}
	}()

	if utilfeature.DefaultFeatureGate.Enabled(features.MutableCSINodeAllocatableCount) && csiNodeUpdaterVar == nil {
		if csiDriverInformer != nil {
			var err error
			csiNodeUpdaterVar, err = NewCSINodeUpdater(csiDriverInformer)
			if err != nil {
				klog.ErrorS(err, "Failed to create CSINodeUpdater")
			} else {
				go csiNodeUpdaterVar.Run()
			}
		}
	}
	return nil
}

// GetPluginName 返回插件名称。
func (p *csiPlugin) GetPluginName() string {
	return CSIPluginName
}

// GetVolumeName 返回 "驱动名<分隔符>卷句柄" 的拼接字符串。
// Detach() 会从中解析出驱动名和卷名。
func (p *csiPlugin) GetVolumeName(spec *volume.Spec) (string, error) {
	csi, err := getPVSourceFromSpec(spec)
	if err != nil {
		return "", errors.New(log("plugin.GetVolumeName failed to extract volume source from spec: %v", err))
	}

	// 返回 driverName<分隔符>volumeHandle。
	return fmt.Sprintf("%s%s%s", csi.Driver, volNameSep, csi.VolumeHandle), nil
}

// CanSupport 判断规格是否引用 CSI 卷（内联卷或 PV）。
func (p *csiPlugin) CanSupport(spec *volume.Spec) bool {
	// TODO (vladimirvivien)：CanSupport 还应检查卷源指定的驱动是否已注册可用；
	// 当前仅判断卷源类型。
	if spec == nil {
		return false
	}
	return (spec.PersistentVolume != nil && spec.PersistentVolume.Spec.CSI != nil) ||
		(spec.Volume != nil && spec.Volume.CSI != nil)
}

// RequiresRemount 根据 CSIDriver.Spec.RequiresRepublish 判断是否需要重新发布卷。
func (p *csiPlugin) RequiresRemount(spec *volume.Spec) bool {
	if p.csiDriverLister == nil {
		return false
	}
	driverName, err := GetCSIDriverName(spec)
	if err != nil {
		klog.V(5).Info(log("Failed to mark %q as republish required, err: %v", spec.Name(), err))
		return false
	}
	csiDriver, err := p.getCSIDriver(driverName)
	if err != nil {
		klog.V(5).Info(log("Failed to mark %q as republish required, err: %v", spec.Name(), err))
		return false
	}
	return *csiDriver.Spec.RequiresRepublish
}

// NewMounter 为 Pod 创建 CSI 卷的 Mounter。
// 从卷规格提取驱动名和卷句柄，内联卷的句柄由 Pod UID 和卷名合成。
func (p *csiPlugin) NewMounter(
	spec *volume.Spec,
	pod *api.Pod) (volume.Mounter, error) {

	volSrc, pvSrc, err := getSourceFromSpec(spec)
	if err != nil {
		return nil, err
	}

	var (
		driverName   string
		volumeHandle string
		readOnly     bool
	)

	switch {
	case volSrc != nil:
		volumeHandle = makeVolumeHandle(string(pod.UID), spec.Name())
		driverName = volSrc.Driver
		if volSrc.ReadOnly != nil {
			readOnly = *volSrc.ReadOnly
		}
	case pvSrc != nil:
		driverName = pvSrc.Driver
		volumeHandle = pvSrc.VolumeHandle
		readOnly = spec.ReadOnly
	default:
		return nil, errors.New(log("volume source not found in volume.Spec"))
	}

	volumeLifecycleMode, err := p.getVolumeLifecycleMode(spec)
	if err != nil {
		return nil, err
	}

	k8s := p.host.GetKubeClient()
	if k8s == nil {
		return nil, errors.New(log("failed to get a kubernetes client"))
	}

	kvh, ok := p.host.(volume.KubeletVolumeHost)
	if !ok {
		return nil, errors.New(log("cast from VolumeHost to KubeletVolumeHost failed"))
	}

	mounter := &csiMountMgr{
		plugin:              p,
		k8s:                 k8s,
		spec:                spec,
		pod:                 pod,
		podUID:              pod.UID,
		driverName:          csiDriverName(driverName),
		volumeLifecycleMode: volumeLifecycleMode,
		volumeID:            volumeHandle,
		specVolumeID:        spec.Name(),
		readOnly:            readOnly,
		kubeVolHost:         kvh,
	}
	mounter.csiClientGetter.driverName = csiDriverName(driverName)

	dir := mounter.GetPath()
	mounter.MetricsProvider = NewMetricsCsi(volumeHandle, dir, csiDriverName(driverName))
	klog.V(4).Info(log("mounter created successfully"))
	return mounter, nil
}

// NewUnmounter 从磁盘上的卷数据文件恢复驱动名和卷句柄，创建 Unmounter。
func (p *csiPlugin) NewUnmounter(specName string, podUID types.UID) (volume.Unmounter, error) {
	klog.V(4).Info(log("setting up unmounter for [name=%v, podUID=%v]", specName, podUID))

	kvh, ok := p.host.(volume.KubeletVolumeHost)
	if !ok {
		return nil, errors.New(log("cast from VolumeHost to KubeletVolumeHost failed"))
	}

	unmounter := &csiMountMgr{
		plugin:       p,
		podUID:       podUID,
		specVolumeID: specName,
		kubeVolHost:  kvh,
	}

	// 从文件加载卸载所需的卷信息。
	dir := unmounter.GetPath()
	dataDir := filepath.Dir(dir) // 去掉末尾的 /mount 目录
	data, err := loadVolumeData(dataDir, volDataFileName)
	if err != nil {
		return nil, errors.New(log("unmounter failed to load volume data file [%s]: %v", dir, err))
	}
	unmounter.driverName = csiDriverName(data[volDataKey.driverName])
	unmounter.volumeID = data[volDataKey.volHandle]
	unmounter.csiClientGetter.driverName = unmounter.driverName

	return unmounter, nil
}

// ConstructVolumeSpec 根据挂载目录中的卷数据重建卷规格，区分临时卷和持久卷。
func (p *csiPlugin) ConstructVolumeSpec(volumeName, mountPath string) (volume.ReconstructedVolume, error) {
	klog.V(4).Info(log("plugin.ConstructVolumeSpec [pv.Name=%v, path=%v]", volumeName, mountPath))

	volData, err := loadVolumeData(mountPath, volDataFileName)
	if err != nil {
		return volume.ReconstructedVolume{}, errors.New(log("plugin.ConstructVolumeSpec failed loading volume data using [%s]: %v", mountPath, err))
	}
	klog.V(4).Info(log("plugin.ConstructVolumeSpec extracted [%#v]", volData))

	var ret volume.ReconstructedVolume
	if utilfeature.DefaultFeatureGate.Enabled(features.SELinuxMountReadWriteOncePod) {
		ret.SELinuxMountContext = volData[volDataKey.seLinuxMountContext]
	}

	// 临时卷用 constructVolSourceSpec 重建内联卷源；
	// 持久卷用 constructPVSourceSpec 重建 PV 卷源。
	if storage.VolumeLifecycleMode(volData[volDataKey.volumeLifecycleMode]) == storage.VolumeLifecycleEphemeral {
		ret.Spec = p.constructVolSourceSpec(volData[volDataKey.specVolID], volData[volDataKey.driverName])
		return ret, nil
	}

	ret.Spec = p.constructPVSourceSpec(volData[volDataKey.specVolID], volData[volDataKey.driverName], volData[volDataKey.volHandle])
	return ret, nil
}

// constructVolSourceSpec 构造带 CSIVolumeSource 的卷规格（临时卷）。
func (p *csiPlugin) constructVolSourceSpec(volSpecName, driverName string) *volume.Spec {
	vol := &api.Volume{
		Name: volSpecName,
		VolumeSource: api.VolumeSource{
			CSI: &api.CSIVolumeSource{
				Driver: driverName,
			},
		},
	}
	return volume.NewSpecFromVolume(vol)
}

// constructPVSourceSpec 构造带 CSIPersistentVolumeSource 的卷规格（持久卷）。
func (p *csiPlugin) constructPVSourceSpec(volSpecName, driverName, volumeHandle string) *volume.Spec {
	fsMode := api.PersistentVolumeFilesystem
	pv := &api.PersistentVolume{
		ObjectMeta: meta.ObjectMeta{
			Name: volSpecName,
		},
		Spec: api.PersistentVolumeSpec{
			PersistentVolumeSource: api.PersistentVolumeSource{
				CSI: &api.CSIPersistentVolumeSource{
					Driver:       driverName,
					VolumeHandle: volumeHandle,
				},
			},
			VolumeMode: &fsMode,
		},
	}
	return volume.NewSpecFromPersistentVolume(pv, false)
}

// SupportsMountOption 返回 true，CSI 插件支持挂载选项。
func (p *csiPlugin) SupportsMountOption() bool {
	// TODO (vladimirvivien)：改用 CSI VolumeCapability.MountVolume.mount_flags 探测驱动能力。
	// (bswartz) 在 CSI 规范支持探测之前，只能由插件直接声明是否支持挂载选项。
	// 因此当前保守返回 true。
	return true
}

// SupportsSELinuxContextMount 根据 CSIDriver.Spec.SELinuxMount 判断驱动是否支持 SELinux 挂载上下文。
func (p *csiPlugin) SupportsSELinuxContextMount(spec *volume.Spec) (bool, error) {
	if utilfeature.DefaultFeatureGate.Enabled(features.SELinuxMountReadWriteOncePod) {
		driver, err := GetCSIDriverName(spec)
		if err != nil {
			return false, err
		}
		csiDriver, err := p.getCSIDriver(driver)
		if err != nil {
			if apierrors.IsNotFound(err) {
				return false, nil
			}
			return false, err
		}
		if csiDriver.Spec.SELinuxMount != nil {
			return *csiDriver.Spec.SELinuxMount, nil
		}
		return false, nil
	}
	return false, nil
}

// 编译时断言 csiPlugin 实现卷附加和设备挂载相关接口。
var _ volume.AttachableVolumePlugin = &csiPlugin{}

var _ volume.DeviceMountableVolumePlugin = &csiPlugin{}

// NewAttacher 创建 CSI 卷附加器。
func (p *csiPlugin) NewAttacher() (volume.Attacher, error) {
	return p.newAttacherDetacher()
}

// NewDeviceMounter 创建设备挂载器，与 Attacher 共用实现。
func (p *csiPlugin) NewDeviceMounter() (volume.DeviceMounter, error) {
	return p.NewAttacher()
}

// NewDetacher 创建 CSI 卷分离器。
func (p *csiPlugin) NewDetacher() (volume.Detacher, error) {
	return p.newAttacherDetacher()
}

// CanAttach 判断卷是否需要附加：临时卷不需要，
// 持久卷依据 CSIDriver.Spec.AttachRequired 决定。
func (p *csiPlugin) CanAttach(spec *volume.Spec) (bool, error) {
	volumeLifecycleMode, err := p.getVolumeLifecycleMode(spec)
	if err != nil {
		return false, err
	}

	if volumeLifecycleMode == storage.VolumeLifecycleEphemeral {
		klog.V(5).Info(log("plugin.CanAttach = false, ephemeral mode detected for spec %v", spec.Name()))
		return false, nil
	}

	pvSrc, err := getCSISourceFromSpec(spec)
	if err != nil {
		return false, err
	}

	driverName := pvSrc.Driver

	skipAttach, err := p.skipAttach(driverName)
	if err != nil {
		return false, err
	}

	return !skipAttach, nil
}

// CanDeviceMount 判断规格是否支持设备挂载：临时卷不支持，持久卷支持。
func (p *csiPlugin) CanDeviceMount(spec *volume.Spec) (bool, error) {
	volumeLifecycleMode, err := p.getVolumeLifecycleMode(spec)
	if err != nil {
		return false, err
	}

	if volumeLifecycleMode == storage.VolumeLifecycleEphemeral {
		klog.V(5).Info(log("plugin.CanDeviceMount skipped ephemeral mode detected for spec %v", spec.Name()))
		return false, nil
	}

	// 持久卷支持设备挂载。
	return true, nil
}

// NewDeviceUnmounter 创建设备卸载器，与 Detacher 共用实现。
func (p *csiPlugin) NewDeviceUnmounter() (volume.DeviceUnmounter, error) {
	return p.NewDetacher()
}

// GetDeviceMountRefs 返回设备挂载点的引用列表。
func (p *csiPlugin) GetDeviceMountRefs(deviceMountPath string) ([]string, error) {
	m := p.host.GetMounter()
	return m.GetMountRefs(deviceMountPath)
}

// 编译时断言 csiPlugin 实现 BlockVolumePlugin 接口。
var _ volume.BlockVolumePlugin = &csiPlugin{}

// NewBlockVolumeMapper 为 Pod 创建块设备卷映射器，并持久化卷信息供卸载时恢复。
func (p *csiPlugin) NewBlockVolumeMapper(spec *volume.Spec, podRef *api.Pod) (volume.BlockVolumeMapper, error) {
	pvSource, err := getCSISourceFromSpec(spec)
	if err != nil {
		return nil, err
	}
	readOnly, err := getReadOnlyFromSpec(spec)
	if err != nil {
		return nil, err
	}

	klog.V(4).Info(log("setting up block mapper for [volume=%v,driver=%v]", pvSource.VolumeHandle, pvSource.Driver))

	k8s := p.host.GetKubeClient()
	if k8s == nil {
		return nil, errors.New(log("failed to get a kubernetes client"))
	}

	mapper := &csiBlockMapper{
		k8s:        k8s,
		plugin:     p,
		volumeID:   pvSource.VolumeHandle,
		driverName: csiDriverName(pvSource.Driver),
		readOnly:   readOnly,
		spec:       spec,
		specName:   spec.Name(),
		pod:        podRef,
		podUID:     podRef.UID,
	}
	mapper.csiClientGetter.driverName = csiDriverName(pvSource.Driver)

	// 在 Pod 目录下保存卷信息。
	dataDir := getVolumeDeviceDataDir(spec.Name(), p.host)

	if err := os.MkdirAll(dataDir, 0750); err != nil {
		return nil, errors.New(log("failed to create data dir %s:  %v", dataDir, err))
	}
	klog.V(4).Info(log("created path successfully [%s]", dataDir))

	blockPath, err := mapper.GetGlobalMapPath(spec)
	if err != nil {
		return nil, errors.New(log("failed to get device path: %v", err))
	}

	mapper.MetricsProvider = NewMetricsCsi(pvSource.VolumeHandle, blockPath+"/"+string(podRef.UID), csiDriverName(pvSource.Driver))

	// 持久化卷信息，供 teardown 阶段使用。
	node := string(p.host.GetNodeName())
	attachID := getAttachmentName(pvSource.VolumeHandle, pvSource.Driver, node)
	volData := map[string]string{
		volDataKey.specVolID:    spec.Name(),
		volDataKey.volHandle:    pvSource.VolumeHandle,
		volDataKey.driverName:   pvSource.Driver,
		volDataKey.nodeName:     node,
		volDataKey.attachmentID: attachID,
	}

	err = saveVolumeData(dataDir, volDataFileName, volData)
	defer func() {
		// 仅在出错且操作被视为终结性失败时删除目录，
		// 避免清理可被后续重试复用的状态。
		if err != nil && volumetypes.IsOperationFinishedError(err) {
			// 尝试清理挂载目录。
			if err = removeMountDir(p, dataDir); err != nil {
				klog.Error(log("attacher.MountDevice failed to remove mount dir after error [%s]: %v", dataDir, err))
			}
		}
	}()
	if err != nil {
		errorMsg := log("csi.NewBlockVolumeMapper failed to save volume info data: %v", err)
		klog.Error(errorMsg)
		return nil, errors.New(errorMsg)
	}

	return mapper, nil
}

// NewBlockVolumeUnmapper 从磁盘卷数据恢复驱动信息，创建块设备卷解除映射器。
func (p *csiPlugin) NewBlockVolumeUnmapper(volName string, podUID types.UID) (volume.BlockVolumeUnmapper, error) {
	klog.V(4).Info(log("setting up block unmapper for [Spec=%v, podUID=%v]", volName, podUID))
	unmapper := &csiBlockMapper{
		plugin:   p,
		podUID:   podUID,
		specName: volName,
	}

	// 从文件加载卷信息。
	dataDir := getVolumeDeviceDataDir(unmapper.specName, p.host)
	data, err := loadVolumeData(dataDir, volDataFileName)
	if err != nil {
		return nil, errors.New(log("unmapper failed to load volume data file [%s]: %v", dataDir, err))
	}
	unmapper.driverName = csiDriverName(data[volDataKey.driverName])
	unmapper.volumeID = data[volDataKey.volHandle]
	unmapper.csiClientGetter.driverName = unmapper.driverName

	return unmapper, nil
}

// ConstructBlockVolumeSpec 根据持久化的卷数据重建块设备卷的 PV 规格。
func (p *csiPlugin) ConstructBlockVolumeSpec(podUID types.UID, specVolName, mapPath string) (*volume.Spec, error) {
	klog.V(4).Infof("plugin.ConstructBlockVolumeSpec [podUID=%s, specVolName=%s, path=%s]", string(podUID), specVolName, mapPath)

	dataDir := getVolumeDeviceDataDir(specVolName, p.host)
	volData, err := loadVolumeData(dataDir, volDataFileName)
	if err != nil {
		return nil, errors.New(log("plugin.ConstructBlockVolumeSpec failed loading volume data using [%s]: %v", mapPath, err))
	}

	klog.V(4).Info(log("plugin.ConstructBlockVolumeSpec extracted [%#v]", volData))

	blockMode := api.PersistentVolumeBlock
	pv := &api.PersistentVolume{
		ObjectMeta: meta.ObjectMeta{
			Name: volData[volDataKey.specVolID],
		},
		Spec: api.PersistentVolumeSpec{
			PersistentVolumeSource: api.PersistentVolumeSource{
				CSI: &api.CSIPersistentVolumeSource{
					Driver:       volData[volDataKey.driverName],
					VolumeHandle: volData[volDataKey.volHandle],
				},
			},
			VolumeMode: &blockMode,
		},
	}

	return volume.NewSpecFromPersistentVolume(pv, false), nil
}

// skipAttach 查询 CSIDriver 对象，判断驱动是否需要附加卷操作。
// CSIDriver 不存在时保守返回 false（执行附加）。
func (p *csiPlugin) skipAttach(driver string) (bool, error) {
	csiDriver, err := p.getCSIDriver(driver)
	if err != nil {
		if apierrors.IsNotFound(err) {
			// CSIDriver 不存在时不能跳过附加。
			return false, nil
		}
		return false, err
	}
	if csiDriver.Spec.AttachRequired != nil && *csiDriver.Spec.AttachRequired == false {
		return true, nil
	}
	return false, nil
}

// getCSIDriver 从 lister 获取指定驱动对象，读取前先等待缓存同步完成。
func (p *csiPlugin) getCSIDriver(driver string) (*storage.CSIDriver, error) {
	kletHost, ok := p.host.(volume.KubeletVolumeHost)
	if ok {
		if err := kletHost.WaitForCacheSync(); err != nil {
			return nil, err
		}
	}

	if p.csiDriverLister == nil {
		return nil, errors.New("CSIDriver lister does not exist")
	}
	csiDriver, err := p.csiDriverLister.Get(driver)
	return csiDriver, err
}

// getVolumeLifecycleMode 返回规格的卷生命周期模式：persistent 或 ephemeral。
// 无法确定时默认 persistent；模式无法解析为两者之一时返回错误。
// 参见 https://github.com/kubernetes/enhancements/blob/master/keps/sig-storage/596-csi-inline-volumes/README.md
func (p *csiPlugin) getVolumeLifecycleMode(spec *volume.Spec) (storage.VolumeLifecycleMode, error) {
	// 1) volume.Spec.Volume.CSI 非空 -> ephemeral
	// 2) volume.Spec.PersistentVolume.Spec.CSI 非空 -> persistent
	volSrc, _, err := getSourceFromSpec(spec)
	if err != nil {
		return "", err
	}

	if volSrc != nil {
		return storage.VolumeLifecycleEphemeral, nil
	}
	return storage.VolumeLifecyclePersistent, nil
}

// getPublishContext 从 VolumeAttachment 中获取驱动的发布上下文。
// skipAttach 的驱动没有附加对象，返回 nil。
func (p *csiPlugin) getPublishContext(client clientset.Interface, handle, driver, nodeName string) (map[string]string, error) {
	skip, err := p.skipAttach(driver)
	if err != nil {
		return nil, err
	}
	if skip {
		return nil, nil
	}

	attachID := getAttachmentName(handle, driver, nodeName)

	// 按附加 ID 查找 VolumeAttachment。
	attachment, err := client.StorageV1().VolumeAttachments().Get(context.TODO(), attachID, meta.GetOptions{})
	if err != nil {
		// 该错误已包含足够上下文，直接返回不再包装。
		return nil, err
	}

	if attachment == nil {
		err = errors.New("no existing VolumeAttachment found")
		return nil, err
	}
	return attachment.Status.AttachmentMetadata, nil
}

// newAttacherDetacher 创建附加器/分离器共用的 csiAttacher 实例。
func (p *csiPlugin) newAttacherDetacher() (*csiAttacher, error) {
	k8s := p.host.GetKubeClient()
	if k8s == nil {
		return nil, errors.New(log("unable to get kubernetes client from host"))
	}

	return &csiAttacher{
		plugin:       p,
		k8s:          k8s,
		watchTimeout: csiTimeout,
	}, nil
}

// podInfoEnabled 检查 CSIDriver 是否启用了 Pod 信息上报（PodInfoOnMount）。
func (p *csiPlugin) podInfoEnabled(driverName string) (bool, error) {
	csiDriver, err := p.getCSIDriver(driverName)
	if err != nil {
		if apierrors.IsNotFound(err) {
			klog.V(4).Info(log("CSIDriver %q not found, not adding pod information", driverName))
			return false, nil
		}
		return false, err
	}

	// PodInfoOnMount 未设置或为 false 时不传递 Pod 属性。
	if csiDriver.Spec.PodInfoOnMount == nil || *csiDriver.Spec.PodInfoOnMount == false {
		klog.V(4).Info(log("CSIDriver %q does not require pod information", driverName))
		return false, nil
	}
	return true, nil
}

// unregisterDriver 从驱动表中删除驱动并卸载其节点信息。
func unregisterDriver(driverName string) error {
	csiDrivers.Delete(driverName)

	if err := nim.UninstallCSIDriver(driverName); err != nil {
		return errors.New(log("Error uninstalling CSI driver: %v", err))
	}

	return nil
}

// waitForAPIServerForever 无限重试读取 CSINode，作为 APIServer 健康可用的信号。
func waitForAPIServerForever(client clientset.Interface, nodeName types.NodeName) error {
	var lastErr error
	// 读取后立即丢弃对象，避免陈旧数据风险，同时减轻 APIServer 和 etcd 负载。
	opts := meta.GetOptions{}
	util.FromApiserverCache(&opts)
	err := wait.PollImmediateInfinite(time.Second, func() (bool, error) {
		// 读取 CSINode 同时验证：1）kubelet 能访问 APIServer；2）具备足够权限。
		// TLS 引导阶段 kubelet 权限可能受限。
		// https://kubernetes.io/docs/reference/access-authn-authz/kubelet-tls-bootstrapping/
		_, lastErr = client.StorageV1().CSINodes().Get(context.TODO(), string(nodeName), opts)
		if lastErr == nil || apierrors.IsNotFound(lastErr) {
			// 已能访问 APIServer（NotFound 也算成功连通）。
			return true, nil
		}
		klog.V(2).Infof("Failed to contact API server when waiting for CSINode publishing: %s", lastErr)
		return false, nil
	})
	if err != nil {
		// 理论上不可达，仅作兜底。
		return fmt.Errorf("%v: %v", err, lastErr)
	}

	return nil
}
