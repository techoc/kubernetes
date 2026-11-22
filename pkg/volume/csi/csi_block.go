/*
Copyright 2018 The Kubernetes Authors.

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

/*
This file defines block volume related methods for CSI driver.
CSI driver is responsible for staging/publishing volumes to their staging/publish paths.
Mapping and unmapping of a device in a publish path to its global map path and its
pod device map path are done by operation_executor through MapBlockVolume/UnmapBlockVolume
(MapBlockVolume and UnmapBlockVolume take care for lock, symlink, and bind mount).

Summary of block volume related CSI driver's methods are as follows:
 - GetGlobalMapPath returns a global map path,
 - GetPodDeviceMapPath returns a pod device map path and filename,
 - SetUpDevice calls CSI's NodeStageVolume and stage a volume to its staging path,
 - MapPodDevice calls CSI's NodePublishVolume and publish a volume to its publish path,
 - UnmapPodDevice calls CSI's NodeUnpublishVolume and unpublish a volume from its publish path,
 - TearDownDevice calls CSI's NodeUnstageVolume and unstage a volume from its staging path.

These methods are called by below sequences:
 - operation_executor.MountVolume
   - csi.GetGlobalMapPath
   - csi.SetupDevice
     - NodeStageVolume
   - ASW.MarkDeviceAsMounted
   - csi.GetPodDeviceMapPath
   - csi.MapPodDevice
     - NodePublishVolume
   - util.MapBlockVolume
   - ASW.MarkVolumeAsMounted

 - operation_executor.UnmountVolume
   - csi.GetPodDeviceMapPath
   - util.UnmapBlockVolume
   - csi.UnmapPodDevice
     - NodeUnpublishVolume
   - ASW.MarkVolumeAsUnmounted

 - operation_executor.UnmountDevice
   - csi.TearDownDevice
     - NodeUnstageVolume
   - ASW.MarkDeviceAsUnmounted

After successful MountVolume for block volume, directory structure will be like below:
  /dev/loopX ... Descriptor lock(Loopback device to mapFile under global map path)
  /var/lib/kubelet/plugins/kubernetes.io/csi/volumeDevices/{specName}/dev/ ... Global map path
  /var/lib/kubelet/plugins/kubernetes.io/csi/volumeDevices/{specName}/dev/{podUID} ... MapFile(Bind mount to publish Path)
  /var/lib/kubelet/plugins/kubernetes.io/csi/volumeDevices/staging/{specName} ... Staging path
  /var/lib/kubelet/plugins/kubernetes.io/csi/volumeDevices/publish/{specName}/{podUID} ... Publish path
  /var/lib/kubelet/pods/{podUID}/volumeDevices/kubernetes.io~csi/ ... Pod device map path
  /var/lib/kubelet/pods/{podUID}/volumeDevices/kubernetes.io~csi/{specName} ... MapFile(Symlink to publish path)
*/

package csi

import (
	"context"
	"errors"
	"os"
	"path/filepath"

	v1 "k8s.io/api/core/v1"
	storage "k8s.io/api/storage/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/util/removeall"
	"k8s.io/kubernetes/pkg/volume"
	volumetypes "k8s.io/kubernetes/pkg/volume/util/types"
	utilstrings "k8s.io/utils/strings"
)

// csiBlockMapper 实现 CSI 块卷的映射器与取消映射器。
type csiBlockMapper struct {
	csiClientGetter
	k8s        kubernetes.Interface
	plugin     *csiPlugin
	driverName csiDriverName
	specName   string
	volumeID   string
	readOnly   bool
	spec       *volume.Spec
	pod        *v1.Pod
	podUID     types.UID
	volume.MetricsProvider
}

var _ volume.BlockVolumeMapper = &csiBlockMapper{}
var _ volume.CustomBlockVolumeMapper = &csiBlockMapper{}

// GetGlobalMapPath 返回节点上设备文件的全局映射路径，该设备文件会被符号链接到此路径。
// 示例：plugins/kubernetes.io/csi/volumeDevices/{specName}/dev
func (m *csiBlockMapper) GetGlobalMapPath(spec *volume.Spec) (string, error) {
	dir := getVolumeDevicePluginDir(m.specName, m.plugin.host)
	klog.V(4).Info(log("blockMapper.GetGlobalMapPath = %s", dir))
	return dir, nil
}

// GetStagingPath 返回节点上供 NodeStageVolume/NodeUnstageVolume 使用的目录 staging 路径。
// 示例：plugins/kubernetes.io/csi/volumeDevices/staging/{specName}
func (m *csiBlockMapper) GetStagingPath() string {
	return filepath.Join(m.plugin.host.GetVolumeDevicePluginDir(CSIPluginName), "staging", m.specName)
}

// SupportsMetrics 对 csiBlockMapper 始终返回 true，因为它会初始化 MetricsProvider。
func (m *csiBlockMapper) SupportsMetrics() bool {
	return true
}

// getPublishDir 返回卷 publish 到各 Pod 的父目录路径。
// 示例：plugins/kubernetes.io/csi/volumeDevices/publish/{specName}
func (m *csiBlockMapper) getPublishDir() string {
	return filepath.Join(m.plugin.host.GetVolumeDevicePluginDir(CSIPluginName), "publish", m.specName)
}

// getPublishPath 返回节点上供 NodePublishVolume/NodeUnpublishVolume 使用的文件 publish 路径。
// 示例：plugins/kubernetes.io/csi/volumeDevices/publish/{specName}/{podUID}
func (m *csiBlockMapper) getPublishPath() string {
	return filepath.Join(m.getPublishDir(), string(m.podUID))
}

// GetPodDeviceMapPath 返回 Pod 中要映射到卷的设备文件路径。
// 返回：pods/{podUID}/volumeDevices/kubernetes.io~csi 与 {specName}
func (m *csiBlockMapper) GetPodDeviceMapPath() (string, string) {
	path := m.plugin.host.GetPodVolumeDeviceDir(m.podUID, utilstrings.EscapeQualifiedName(CSIPluginName))
	klog.V(4).Info(log("blockMapper.GetPodDeviceMapPath [path=%s; name=%s]", path, m.specName))
	return path, m.specName
}

// stageVolumeForBlock 将块卷 stage 到 stagingPath
func (m *csiBlockMapper) stageVolumeForBlock(
	ctx context.Context,
	csi csiClient,
	accessMode v1.PersistentVolumeAccessMode,
	csiSource *v1.CSIPersistentVolumeSource,
	attachment *storage.VolumeAttachment,
) (string, error) {
	klog.V(4).Info(log("blockMapper.stageVolumeForBlock called"))

	stagingPath := m.GetStagingPath()
	klog.V(4).Info(log("blockMapper.stageVolumeForBlock stagingPath set [%s]", stagingPath))

	// 检查驱动是否设置了 "STAGE_UNSTAGE_VOLUME" 能力
	stageUnstageSet, err := csi.NodeSupportsStageUnstage(ctx)
	if err != nil {
		return "", errors.New(log("blockMapper.stageVolumeForBlock failed to check STAGE_UNSTAGE_VOLUME capability: %v", err))
	}
	if !stageUnstageSet {
		klog.Info(log("blockMapper.stageVolumeForBlock STAGE_UNSTAGE_VOLUME capability not set. Skipping MountDevice..."))
		return "", nil
	}
	publishVolumeInfo := map[string]string{}
	if attachment != nil {
		publishVolumeInfo = attachment.Status.AttachmentMetadata
	}
	nodeStageSecrets := map[string]string{}
	if csiSource.NodeStageSecretRef != nil {
		nodeStageSecrets, err = getCredentialsFromSecret(m.k8s, csiSource.NodeStageSecretRef)
		if err != nil {
			return "", volumetypes.NewTransientOperationFailure(log("failed to get NodeStageSecretRef %s/%s: %v",
				csiSource.NodeStageSecretRef.Namespace, csiSource.NodeStageSecretRef.Name, err))
		}
	}

	// 调用 NodeStageVolume 前先创建 stagingPath 目录
	if err := os.MkdirAll(stagingPath, 0750); err != nil {
		return "", errors.New(log("blockMapper.stageVolumeForBlock failed to create dir %s: %v", stagingPath, err))
	}
	klog.V(4).Info(log("blockMapper.stageVolumeForBlock created stagingPath directory successfully [%s]", stagingPath))

	// 请求将块卷 stage 到 stagingPath。
	// 驱动应实现为在 stagingPath 上创建自身特定的资源，
	// 并把块卷附加到节点。
	err = csi.NodeStageVolume(ctx,
		csiSource.VolumeHandle,
		publishVolumeInfo,
		stagingPath,
		fsTypeBlockName,
		accessMode,
		nodeStageSecrets,
		csiSource.VolumeAttributes,
		nil, /* MountOptions */
		nil /* fsGroup */)

	if err != nil {
		return "", err
	}

	klog.V(4).Info(log("blockMapper.stageVolumeForBlock successfully requested NodeStageVolume [%s]", stagingPath))
	return stagingPath, nil
}

// publishVolumeForBlock 将块卷 publish 到 publishPath
func (m *csiBlockMapper) publishVolumeForBlock(
	ctx context.Context,
	csi csiClient,
	accessMode v1.PersistentVolumeAccessMode,
	csiSource *v1.CSIPersistentVolumeSource,
	attachment *storage.VolumeAttachment,
) (string, error) {
	klog.V(4).Info(log("blockMapper.publishVolumeForBlock called"))

	publishVolumeInfo := map[string]string{}
	if attachment != nil {
		publishVolumeInfo = attachment.Status.AttachmentMetadata
	}

	// 将 Pod 信息注入 volume_attributes
	volAttribs := csiSource.VolumeAttributes
	podInfoEnabled, err := m.plugin.podInfoEnabled(string(m.driverName))
	if err != nil {
		return "", volumetypes.NewTransientOperationFailure(log("blockMapper.publishVolumeForBlock failed to assemble volume attributes: %v", err))
	}
	volumeLifecycleMode, err := m.plugin.getVolumeLifecycleMode(m.spec)
	if err != nil {
		return "", volumetypes.NewTransientOperationFailure(log("blockMapper.publishVolumeForBlock failed to get VolumeLifecycleMode: %v", err))
	}
	if podInfoEnabled {
		volAttribs = mergeMap(volAttribs, getPodInfoAttrs(m.pod, volumeLifecycleMode))
	}

	nodePublishSecrets := map[string]string{}
	if csiSource.NodePublishSecretRef != nil {
		nodePublishSecrets, err = getCredentialsFromSecret(m.k8s, csiSource.NodePublishSecretRef)
		if err != nil {
			return "", volumetypes.NewTransientOperationFailure(log("blockMapper.publishVolumeForBlock failed to get NodePublishSecretRef %s/%s: %v",
				csiSource.NodePublishSecretRef.Namespace, csiSource.NodePublishSecretRef.Name, err))
		}
	}

	publishPath := m.getPublishPath()
	// 调用 NodePublishVolume 前先创建 publishPath 的父目录
	publishDir := filepath.Dir(publishPath)
	if err := os.MkdirAll(publishDir, 0750); err != nil {
		return "", errors.New(log("blockMapper.publishVolumeForBlock failed to create dir %s:  %v", publishDir, err))
	}
	klog.V(4).Info(log("blockMapper.publishVolumeForBlock created directory for publishPath successfully [%s]", publishDir))

	// 请求将块卷 publish 到 publishPath。
	// 驱动应通过把设备文件 bind mount 到 publishPath，或在 publishPath 上创建设备文件，
	// 将块卷放置到 publishPath。
	// publishPath 的父目录由 k8s 创建，但 publishPath 本身由驱动负责创建。
	// 若驱动未实现 NodeStageVolume，可在此处完成块卷到节点的附加。
	err = csi.NodePublishVolume(
		ctx,
		m.volumeID,
		m.readOnly,
		m.GetStagingPath(),
		publishPath,
		accessMode,
		publishVolumeInfo,
		volAttribs,
		nodePublishSecrets,
		fsTypeBlockName,
		[]string{}, /* mountOptions */
		nil,        /* fsGroup */
	)

	if err != nil {
		return "", err
	}

	return publishPath, nil
}

// SetUpDevice 确保设备已附加，并返回设备所在路径。
func (m *csiBlockMapper) SetUpDevice() (string, error) {
	klog.V(4).Info(log("blockMapper.SetUpDevice called"))

	// 从卷规格中获取 CSI 卷来源。
	if m.spec == nil {
		return "", errors.New(log("blockMapper.SetUpDevice spec is nil"))
	}

	csiSource, err := getCSISourceFromSpec(m.spec)
	if err != nil {
		return "", errors.New(log("blockMapper.SetUpDevice failed to get CSI persistent source: %v", err))
	}

	driverName := csiSource.Driver
	skip, err := m.plugin.skipAttach(driverName)
	if err != nil {
		return "", errors.New(log("blockMapper.SetupDevice failed to check CSIDriver for %s: %v", driverName, err))
	}

	var attachment *storage.VolumeAttachment
	if !skip {
		// 按 VolumeAttachment.Spec.Source.PersistentVolumeName 查找 VolumeAttachment。
		nodeName := string(m.plugin.host.GetNodeName())
		attachID := getAttachmentName(csiSource.VolumeHandle, csiSource.Driver, nodeName)
		attachment, err = m.k8s.StorageV1().VolumeAttachments().Get(context.TODO(), attachID, meta.GetOptions{})
		if err != nil {
			return "", volumetypes.NewTransientOperationFailure(log("blockMapper.SetupDevice failed to get volume attachment [id=%v]: %v", attachID, err))
		}
	}

	// TODO (vladimirvivien) 实现 k8s 与 CSI 之间更完善的 AccessModes 映射
	accessMode := v1.ReadWriteOnce
	if m.spec.PersistentVolume.Spec.AccessModes != nil {
		accessMode = m.spec.PersistentVolume.Spec.AccessModes[0]
	}

	ctx, cancel := context.WithTimeout(context.Background(), csiTimeout)
	defer cancel()

	csiClient, err := m.csiClientGetter.Get()
	if err != nil {
		// CSI 驱动暂不可用按瞬时错误处理。
		// 参见 https://github.com/kubernetes/kubernetes/issues/120268
		return "", volumetypes.NewTransientOperationFailure(log("blockMapper.SetUpDevice failed to get CSI client: %v", err))
	}

	// 调用 NodeStageVolume。
	stagingPath, err := m.stageVolumeForBlock(ctx, csiClient, accessMode, csiSource, attachment)
	if err != nil {
		if volumetypes.IsOperationFinishedError(err) {
			cleanupErr := m.cleanupOrphanDeviceFiles()
			if cleanupErr != nil {
				// 清理失败不算严重错误，用 V(4) 记录。
				klog.V(4).Infof("Failed to clean up block volume directory %s", cleanupErr)
			}
		}
		return "", err
	}

	return stagingPath, nil
}

// MapPodDevice 调用 CSI 的 NodePublishVolume，将块卷 publish 到 Pod 的 publish 路径。
func (m *csiBlockMapper) MapPodDevice() (string, error) {
	klog.V(4).Info(log("blockMapper.MapPodDevice called"))

	// 从卷规格中获取 CSI 卷来源。
	if m.spec == nil {
		return "", errors.New(log("blockMapper.MapPodDevice spec is nil"))
	}

	csiSource, err := getCSISourceFromSpec(m.spec)
	if err != nil {
		return "", errors.New(log("blockMapper.MapPodDevice failed to get CSI persistent source: %v", err))
	}

	driverName := csiSource.Driver
	skip, err := m.plugin.skipAttach(driverName)
	if err != nil {
		return "", errors.New(log("blockMapper.MapPodDevice failed to check CSIDriver for %s: %v", driverName, err))
	}

	var attachment *storage.VolumeAttachment
	if !skip {
		// 按 VolumeAttachment.Spec.Source.PersistentVolumeName 查找 VolumeAttachment。
		nodeName := string(m.plugin.host.GetNodeName())
		attachID := getAttachmentName(csiSource.VolumeHandle, csiSource.Driver, nodeName)
		attachment, err = m.k8s.StorageV1().VolumeAttachments().Get(context.TODO(), attachID, meta.GetOptions{})
		if err != nil {
			return "", volumetypes.NewTransientOperationFailure(log("blockMapper.MapPodDevice failed to get volume attachment [id=%v]: %v", attachID, err))
		}
	}

	// TODO (vladimirvivien) 实现 k8s 与 CSI 之间更完善的 AccessModes 映射
	accessMode := v1.ReadWriteOnce
	if m.spec.PersistentVolume.Spec.AccessModes != nil {
		accessMode = m.spec.PersistentVolume.Spec.AccessModes[0]
	}

	ctx, cancel := createCSIOperationContext(m.spec, csiTimeout)
	defer cancel()

	csiClient, err := m.csiClientGetter.Get()
	if err != nil {
		// CSI 驱动暂不可用按瞬时错误处理。
		// 参见 https://github.com/kubernetes/kubernetes/issues/120268
		return "", volumetypes.NewTransientOperationFailure(log("blockMapper.MapPodDevice failed to get CSI client: %v", err))
	}

	// 调用 NodePublishVolume。
	publishPath, err := m.publishVolumeForBlock(ctx, csiClient, accessMode, csiSource, attachment)
	if err != nil {
		return "", err
	}

	return publishPath, nil
}

var _ volume.BlockVolumeUnmapper = &csiBlockMapper{}
var _ volume.CustomBlockVolumeUnmapper = &csiBlockMapper{}

// unpublishVolumeForBlock 将块卷从 publishPath 取消 publish
func (m *csiBlockMapper) unpublishVolumeForBlock(ctx context.Context, csi csiClient, publishPath string) error {
	// 请求将块卷从 publishPath 取消 publish。
	// 驱动应通过解挂 bind mount 的设备文件，或删除设备文件，
	// 将块卷从 publishPath 移除。
	// publishPath 本身由驱动负责删除。
	// 若驱动未实现 NodeUnstageVolume，可在此处完成块卷从节点的分离。
	if err := csi.NodeUnpublishVolume(ctx, m.volumeID, publishPath); err != nil {
		return errors.New(log("blockMapper.unpublishVolumeForBlock failed: %v", err))
	}
	klog.V(4).Info(log("blockMapper.unpublishVolumeForBlock NodeUnpublished successfully [%s]", publishPath))

	return nil
}

// unstageVolumeForBlock 将块卷从 stagingPath 取消 stage
func (m *csiBlockMapper) unstageVolumeForBlock(ctx context.Context, csi csiClient, stagingPath string) error {
	// 检查驱动是否设置了 "STAGE_UNSTAGE_VOLUME" 能力
	stageUnstageSet, err := csi.NodeSupportsStageUnstage(ctx)
	if err != nil {
		return errors.New(log("blockMapper.unstageVolumeForBlock failed to check STAGE_UNSTAGE_VOLUME capability: %v", err))
	}
	if !stageUnstageSet {
		klog.Info(log("blockMapper.unstageVolumeForBlock STAGE_UNSTAGE_VOLUME capability not set. Skipping unstageVolumeForBlock ..."))
		return nil
	}

	// 请求将块卷从 stagingPath 取消 stage。
	// 驱动应移除 stagingPath 中自身特定的资源，
	// 并把块卷从节点分离。
	if err := csi.NodeUnstageVolume(ctx, m.volumeID, stagingPath); err != nil {
		return errors.New(log("blockMapper.unstageVolumeForBlock failed: %v", err))
	}
	klog.V(4).Info(log("blockMapper.unstageVolumeForBlock NodeUnstageVolume successfully [%s]", stagingPath))

	// 删除 stagingPath 目录及其内容。
	if err := os.RemoveAll(stagingPath); err != nil {
		return errors.New(log("blockMapper.unstageVolumeForBlock failed to remove staging path after NodeUnstageVolume() error [%s]: %v", stagingPath, err))
	}

	return nil
}

// TearDownDevice 清理 SetUpDevice 留下的痕迹。
func (m *csiBlockMapper) TearDownDevice(globalMapPath, devicePath string) error {
	ctx, cancel := createCSIOperationContext(m.spec, csiTimeout)
	defer cancel()

	csiClient, err := m.csiClientGetter.Get()
	if err != nil {
		// CSI 驱动暂不可用按瞬时错误处理。
		// 参见 https://github.com/kubernetes/kubernetes/issues/120268
		return volumetypes.NewTransientOperationFailure(log("blockMapper.TearDownDevice failed to get CSI client: %v", err))
	}

	// 调用 NodeUnstageVolume。
	stagingPath := m.GetStagingPath()
	if _, err := os.Stat(stagingPath); err != nil {
		if os.IsNotExist(err) {
			klog.V(4).Info(log("blockMapper.TearDownDevice stagingPath(%s) has already been deleted, skip calling NodeUnstageVolume", stagingPath))
		} else {
			return err
		}
	} else {
		err := m.unstageVolumeForBlock(ctx, csiClient, stagingPath)
		if err != nil {
			return err
		}
	}
	if err = m.cleanupOrphanDeviceFiles(); err != nil {
		// 清理失败不算严重错误，用 V(4) 记录。
		klog.V(4).Infof("Failed to clean up block volume directory %s", err)
	}

	return nil
}

// 取消 stage 块卷时清理所有孤立的文件/目录。
// 此时可以确定已无 Pod 使用该卷，
// 所有文件确为孤立文件。
func (m *csiBlockMapper) cleanupOrphanDeviceFiles() error {
	// 清理 NodePublish 留下的产物。
	// publishDir 为 xxx/plugins/kubernetes.io/csi/volumeDevices/publish/<volume name>。
	// 每次 PublishVolume() 都会在其中创建子目录；此时应都已取消 publish，
	// 目录应为空。
	publishDir := m.getPublishDir()
	if err := os.Remove(publishDir); err != nil && !os.IsNotExist(err) {
		return errors.New(log("failed to remove publish directory [%s]: %v", publishDir, err))
	}

	// 清理 NodeStage 留下的产物。
	// stagingPath 为 xxx/plugins/kubernetes.io/csi/volumeDevices/staging/<volume name>。
	stagingPath := m.GetStagingPath()
	if err := os.Remove(stagingPath); err != nil && !os.IsNotExist(err) {
		return errors.New(log("failed to delete volume staging path [%s]: %v", stagingPath, err))
	}

	// 删除 xxx/plugins/kubernetes.io/csi/volumeDevices/<volume name> 下的所有内容。
	// 此时其中只剩 "data/vol_data.json" 和空的 "dev/"。
	volumeDir := getVolumePluginDir(m.specName, m.plugin.host)
	mounter := m.plugin.host.GetMounter()
	if err := removeall.RemoveAllOneFilesystem(mounter, volumeDir); err != nil {
		return err
	}

	return nil
}

// UnmapPodDevice 解除块设备路径的映射。
func (m *csiBlockMapper) UnmapPodDevice() error {
	publishPath := m.getPublishPath()

	csiClient, err := m.csiClientGetter.Get()
	if err != nil {
		// CSI 驱动暂不可用按瞬时错误处理。
		// 参见 https://github.com/kubernetes/kubernetes/issues/120268
		return volumetypes.NewTransientOperationFailure(log("blockMapper.UnmapPodDevice failed to get CSI client: %v", err))
	}

	ctx, cancel := createCSIOperationContext(m.spec, csiTimeout)
	defer cancel()

	// 调用 NodeUnpublishVolume。
	// 即使 publishPath 不存在也要调用：之前的 NodePublish 可能已超时，
	// 需要由 Kubernetes 确保该操作最终完成。
	return m.unpublishVolumeForBlock(ctx, csiClient, publishPath)
}
