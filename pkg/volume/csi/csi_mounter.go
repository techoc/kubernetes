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
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"

	authenticationv1 "k8s.io/api/authentication/v1"
	api "k8s.io/api/core/v1"
	storage "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/pkg/volume"
	"k8s.io/kubernetes/pkg/volume/util"
	volumetypes "k8s.io/kubernetes/pkg/volume/util/types"
	utilstrings "k8s.io/utils/strings"
)

// TODO (vladimirvivien) 后续移到集中位置
var (
	volDataKey = struct {
		specVolID,
		volHandle,
		driverName,
		nodeName,
		attachmentID,
		volumeLifecycleMode,
		seLinuxMountContext string
	}{
		"specVolID",
		"volumeHandle",
		"driverName",
		"nodeName",
		"attachmentID",
		"volumeLifecycleMode",
		"seLinuxMountContext",
	}
)

// csiMountMgr 管理 CSI 卷的挂载与卸载。
type csiMountMgr struct {
	csiClientGetter
	k8s                 kubernetes.Interface
	plugin              *csiPlugin
	driverName          csiDriverName
	volumeLifecycleMode storage.VolumeLifecycleMode
	volumeID            string
	specVolumeID        string
	readOnly            bool
	needSELinuxRelabel  bool
	spec                *volume.Spec
	pod                 *api.Pod
	podUID              types.UID
	publishContext      map[string]string
	kubeVolHost         volume.KubeletVolumeHost
	volume.MetricsProvider
}

// 实现 volume.Volume 接口的方法。
var _ volume.Volume = &csiMountMgr{}

// GetPath 返回 CSI 卷的挂载路径。
func (c *csiMountMgr) GetPath() string {
	dir := GetCSIMounterPath(filepath.Join(getTargetPath(c.podUID, c.specVolumeID, c.plugin.host)))
	klog.V(4).Info(log("mounter.GetPath generated [%s]", dir))
	return dir
}

// getTargetPath 返回卷在 Pod 目录下的目标路径。
func getTargetPath(uid types.UID, specVolumeID string, host volume.VolumeHost) string {
	specVolID := utilstrings.EscapeQualifiedName(specVolumeID)
	return host.GetPodVolumeDir(uid, utilstrings.EscapeQualifiedName(CSIPluginName), specVolID)
}

// 实现 volume.Mounter 接口的方法。
var _ volume.Mounter = &csiMountMgr{}

// SetUp 在卷的默认路径上挂载卷。
func (c *csiMountMgr) SetUp(mounterArgs volume.MounterArgs) error {
	return c.SetUpAt(c.GetPath(), mounterArgs)
}

// SetUpAt 在指定目录上挂载卷，并完成 CSI NodePublishVolume 调用。
func (c *csiMountMgr) SetUpAt(dir string, mounterArgs volume.MounterArgs) error {
	klog.V(4).Info(log("Mounter.SetUpAt(%s)", dir))

	csi, err := c.csiClientGetter.Get()
	if err != nil {
		// CSI 驱动暂不可用按瞬时错误处理。
		// 参见 https://github.com/kubernetes/kubernetes/issues/120268
		return volumetypes.NewTransientOperationFailure(log("mounter.SetUpAt failed to get CSI client: %v", err))
	}

	ctx, cancel := createCSIOperationContext(c.spec, csiTimeout)
	defer cancel()

	volSrc, pvSrc, err := getSourceFromSpec(c.spec)
	if err != nil {
		return errors.New(log("mounter.SetupAt failed to get CSI persistent source: %v", err))
	}

	// 检查 CSIDriver.Spec.Mode，确保 CSI 驱动
	// 支持当前的 volumeLifecycleMode。
	if err := c.supportsVolumeLifecycleMode(); err != nil {
		return volumetypes.NewTransientOperationFailure(log("mounter.SetupAt failed to check volume lifecycle mode: %s", err))
	}

	fsGroupPolicy, err := c.getFSGroupPolicy()
	if err != nil {
		return volumetypes.NewTransientOperationFailure(log("mounter.SetupAt failed to check fsGroup policy: %s", err))
	}

	driverName := c.driverName
	volumeHandle := c.volumeID
	readOnly := c.readOnly
	accessMode := api.ReadWriteOnce

	var (
		fsType             string
		volAttribs         map[string]string
		nodePublishSecrets map[string]string
		publishContext     map[string]string
		mountOptions       []string
		deviceMountPath    string
		secretRef          *api.SecretReference
	)

	switch {
	case volSrc != nil:
		if c.volumeLifecycleMode != storage.VolumeLifecycleEphemeral {
			return fmt.Errorf("unexpected volume mode: %s", c.volumeLifecycleMode)
		}
		if volSrc.FSType != nil {
			fsType = *volSrc.FSType
		}

		volAttribs = volSrc.VolumeAttributes

		if volSrc.NodePublishSecretRef != nil {
			secretName := volSrc.NodePublishSecretRef.Name
			ns := c.pod.Namespace
			secretRef = &api.SecretReference{Name: secretName, Namespace: ns}
		}
	case pvSrc != nil:
		if c.volumeLifecycleMode != storage.VolumeLifecyclePersistent {
			return fmt.Errorf("unexpected driver mode: %s", c.volumeLifecycleMode)
		}

		fsType = pvSrc.FSType

		volAttribs = pvSrc.VolumeAttributes

		if pvSrc.NodePublishSecretRef != nil {
			secretRef = pvSrc.NodePublishSecretRef
		}

		// TODO (vladimirvivien) 实现 k8s 与 CSI 之间更完善的 AccessModes 映射
		if c.spec.PersistentVolume.Spec.AccessModes != nil {
			accessMode = c.spec.PersistentVolume.Spec.AccessModes[0]
		}

		mountOptions = c.spec.PersistentVolume.Spec.MountOptions

		// 检查驱动是否设置了 STAGE_UNSTAGE_VOLUME，若设置则填充 deviceMountPath
		stageUnstageSet, err := csi.NodeSupportsStageUnstage(ctx)
		if err != nil {
			return errors.New(log("mounter.SetUpAt failed to check for STAGE_UNSTAGE_VOLUME capability: %v", err))
		}

		if stageUnstageSet {
			deviceMountPath, err = makeDeviceMountPath(c.plugin, c.spec)
			if err != nil {
				return errors.New(log("mounter.SetUpAt failed to make device mount path: %v", err))
			}
		}

		// 按 VolumeAttachment.Spec.Source.PersistentVolumeName 查找 VolumeAttachment
		if c.publishContext == nil {
			nodeName := string(c.plugin.host.GetNodeName())
			c.publishContext, err = c.plugin.getPublishContext(c.k8s, volumeHandle, string(driverName), nodeName)
			if err != nil {
				// 获取 publish context 时可能遇到瞬时错误
				return volumetypes.NewTransientOperationFailure(log("mounter.SetUpAt failed to fetch publishContext: %v", err))
			}
			publishContext = c.publishContext
		}

	default:
		return fmt.Errorf("volume source not found in volume.Spec")
	}

	// 调用 NodePublish 前先创建 target_dir
	parentDir := filepath.Dir(dir)
	if err := os.MkdirAll(parentDir, 0750); err != nil {
		return errors.New(log("mounter.SetUpAt failed to create dir %#v:  %v", parentDir, err))
	}
	klog.V(4).Info(log("created target path successfully [%s]", parentDir))

	nodePublishSecrets = map[string]string{}
	if secretRef != nil {
		nodePublishSecrets, err = getCredentialsFromSecret(c.k8s, secretRef)
		if err != nil {
			return volumetypes.NewTransientOperationFailure(fmt.Sprintf("fetching NodePublishSecretRef %s/%s failed: %v",
				secretRef.Namespace, secretRef.Name, err))
		}

	}

	// 将 Pod 信息注入 volume_attributes
	podInfoEnabled, err := c.plugin.podInfoEnabled(string(c.driverName))
	if err != nil {
		return volumetypes.NewTransientOperationFailure(log("mounter.SetUpAt failed to assemble volume attributes: %v", err))
	}
	if podInfoEnabled {
		volAttribs = mergeMap(volAttribs, getPodInfoAttrs(c.pod, c.volumeLifecycleMode))
	}

	serviceAccountTokenAttrs, serviceAccountTokenInSecrets, err := c.podServiceAccountTokenAttrs()
	if err != nil {
		return volumetypes.NewTransientOperationFailure(log("mounter.SetUpAt failed to get service accoount token attributes: %v", err))
	}

	// 将 ServiceAccount token 信息注入：
	// 1. volume_attributes（默认行为）
	// 2. node_publish_secrets（当 CSIDriver 规格中 ServiceAccountTokenInSecrets 为 true 时）
	if serviceAccountTokenInSecrets {
		nodePublishSecrets = mergeMap(nodePublishSecrets, serviceAccountTokenAttrs)
	} else {
		volAttribs = mergeMap(volAttribs, serviceAccountTokenAttrs)
	}

	driverSupportsCSIVolumeMountGroup := false
	var nodePublishFSGroupArg *int64
	driverSupportsCSIVolumeMountGroup, err = csi.NodeSupportsVolumeMountGroup(ctx)
	if err != nil {
		return volumetypes.NewTransientOperationFailure(log("mounter.SetUpAt failed to determine if the node service has VOLUME_MOUNT_GROUP capability: %v", err))
	}

	if driverSupportsCSIVolumeMountGroup {
		klog.V(3).Infof("Driver %s supports applying FSGroup (has VOLUME_MOUNT_GROUP node capability). Delegating FSGroup application to the driver through NodePublishVolume.", c.driverName)
		nodePublishFSGroupArg = mounterArgs.FsGroup
	}

	var selinuxLabelMount bool
	if utilfeature.DefaultFeatureGate.Enabled(features.SELinuxMountReadWriteOncePod) {
		support, err := c.plugin.SupportsSELinuxContextMount(c.spec)
		if err != nil {
			return errors.New(log("failed to query for SELinuxMount support: %s", err))
		}
		if support && mounterArgs.SELinuxLabel != "" {
			mountOptions = util.AddSELinuxMountOption(mountOptions, mounterArgs.SELinuxLabel)
			selinuxLabelMount = true
		}
	}

	// 将卷信息保存到 Pod 目录，
	// 供卸载时读取。
	nodeName := string(c.plugin.host.GetNodeName())
	volData := map[string]string{
		volDataKey.specVolID:           c.spec.Name(),
		volDataKey.volHandle:           volumeHandle,
		volDataKey.driverName:          string(c.driverName),
		volDataKey.nodeName:            nodeName,
		volDataKey.volumeLifecycleMode: string(c.volumeLifecycleMode),
		volDataKey.attachmentID:        getAttachmentName(volumeHandle, string(c.driverName), nodeName),
	}

	if utilfeature.DefaultFeatureGate.Enabled(features.SELinuxMountReadWriteOncePod) && selinuxLabelMount {
		volData[volDataKey.seLinuxMountContext] = mounterArgs.SELinuxLabel
	}

	err = saveVolumeData(parentDir, volDataFileName, volData)
	if err != nil {
		errorMsg := log("mounter.SetUpAt failed to save volume info data: %v", err)
		klog.Error(errorMsg)
		if removeerr := removeMountDir(c.plugin, dir); removeerr != nil {
			klog.Error(log("mounter.SetUpAt failed to remove mount dir after error [%s]: %v", dir, removeerr))
		}
		return err
	}

	csiRPCError := csi.NodePublishVolume(
		ctx,
		volumeHandle,
		readOnly,
		deviceMountPath,
		dir,
		accessMode,
		publishContext,
		volAttribs,
		nodePublishSecrets,
		fsType,
		mountOptions,
		nodePublishFSGroupArg,
	)

	if csiRPCError != nil {
		// 操作明确失败时可以删除挂载目录。
		// 重挂场景（如 CSIDriver.spec.requiresRepublish=true）要跳过：卷
		// 已经 publish 过，Pod 正在使用已有的 bind mount，
		// 此时删除挂载目录会让 Pod 看到过期内容，
		// 后续成功的 republish 也无法修复（#121271）。
		if volumetypes.IsOperationFinishedError(csiRPCError) && !mounterArgs.ReconstructedVolume && !mounterArgs.IsRemount {
			if removeMountDirErr := removeMountDir(c.plugin, dir); removeMountDirErr != nil {
				klog.Error(log("mounter.SetupAt failed to remove mount dir after a NodePublish() error [%s]: %v", dir, removeMountDirErr))
			}
		}
		return csiRPCError
	}

	if !selinuxLabelMount {
		c.needSELinuxRelabel, err = c.kubeVolHost.GetHostUtil().GetSELinuxSupport(dir)
		if err != nil {
			// 卷已挂载，返回 UncertainProgressError，以便用户删除 Pod 时 kubelet 会执行卸载。
			return volumetypes.NewUncertainProgressError(fmt.Sprintf("error checking for SELinux support: %s", err))
		}
	}

	if !driverSupportsCSIVolumeMountGroup && c.supportsFSGroup(fsType, mounterArgs.FsGroup, fsGroupPolicy) {
		// 驱动不支持设置 FSGroup，需由 kubelet 代为应用。
		var ownershipChanger volume.VolumeOwnershipChanger
		if mounterArgs.VolumeOwnershipApplicator != nil {
			ownershipChanger = mounterArgs.VolumeOwnershipApplicator
		} else {
			ownershipChanger = volume.NewVolumeOwnership(c, dir, mounterArgs.FsGroup, mounterArgs.FSGroupChangePolicy, util.FSGroupCompleteHook(c.plugin, c.spec))
		}
		// 完整插件名有助于区分 CSI 插件中的不同驱动。
		ownershipChanger.AddProgressNotifier(c.pod, mounterArgs.Recorder)
		err = ownershipChanger.ChangePermissions()
		if err != nil {
			// 此时挂载已成功，但：
			//   1. 因权限无效导致 Pod 无法使用该卷，必须返回错误；
			//   2. 因挂载已成功，必须将卷记录为状态不确定的已挂载，
			//      以便后续清理。
			return volumetypes.NewUncertainProgressError(fmt.Sprintf("applyFSGroup failed for vol %s: %v", c.volumeID, err))
		}
		klog.V(4).Info(log("mounter.SetupAt fsGroup [%d] applied successfully to %s", *mounterArgs.FsGroup, c.volumeID))
	}

	klog.V(4).Info(log("mounter.SetUp successfully requested NodePublish [%s]", dir))
	return nil
}

// podServiceAccountTokenAttrs 获取 CSIDriver 所需的 ServiceAccount token 属性。
func (c *csiMountMgr) podServiceAccountTokenAttrs() (map[string]string, bool, error) {
	if c.plugin.serviceAccountTokenGetter == nil {
		return nil, false, errors.New("ServiceAccountTokenGetter is nil")
	}

	csiDriver, err := c.plugin.csiDriverLister.Get(string(c.driverName))
	if err != nil {
		if apierrors.IsNotFound(err) {
			klog.V(5).Info(log("CSIDriver %q not found, not adding service account token information", c.driverName))
			return nil, false, nil
		}
		return nil, false, err
	}

	if len(csiDriver.Spec.TokenRequests) == 0 {
		return nil, false, nil
	}

	outputs := map[string]authenticationv1.TokenRequestStatus{}
	for _, tokenRequest := range csiDriver.Spec.TokenRequests {
		audience := tokenRequest.Audience
		audiences := []string{audience}
		if audience == "" {
			audiences = []string{}
		}
		tr, err := c.plugin.serviceAccountTokenGetter(c.pod.Namespace, c.pod.Spec.ServiceAccountName, &authenticationv1.TokenRequest{
			Spec: authenticationv1.TokenRequestSpec{
				Audiences:         audiences,
				ExpirationSeconds: tokenRequest.ExpirationSeconds,
				BoundObjectRef: &authenticationv1.BoundObjectReference{
					APIVersion: "v1",
					Kind:       "Pod",
					Name:       c.pod.Name,
					UID:        c.pod.UID,
				},
			},
		})
		if err != nil {
			return nil, false, err
		}

		outputs[audience] = tr.Status
	}

	klog.V(4).Info(log("Fetched service account token attrs for CSIDriver %q", c.driverName))
	tokens, err := json.Marshal(outputs)
	if err != nil {
		return nil, false, err
	}

	var serviceAccountTokenInSecrets bool
	if csiDriver.Spec.ServiceAccountTokenInSecrets != nil && *csiDriver.Spec.ServiceAccountTokenInSecrets {
		serviceAccountTokenInSecrets = true
	}

	return map[string]string{
		"csi.storage.k8s.io/serviceAccount.tokens": string(tokens),
	}, serviceAccountTokenInSecrets, nil
}

// GetAttributes 返回卷的挂载属性。
func (c *csiMountMgr) GetAttributes() volume.Attributes {
	return volume.Attributes{
		ReadOnly:       c.readOnly,
		Managed:        !c.readOnly,
		SELinuxRelabel: c.needSELinuxRelabel,
	}
}

// 实现 volume.Unmounter 接口的方法。
var _ volume.Unmounter = &csiMountMgr{}

// TearDown 卸载默认路径上的卷。
func (c *csiMountMgr) TearDown() error {
	return c.TearDownAt(c.GetPath())
}

// TearDownAt 卸载指定目录上的卷，并清理 kubelet 创建的数据文件。
func (c *csiMountMgr) TearDownAt(dir string) error {
	klog.V(4).Info(log("Unmounter.TearDownAt(%s)", dir))

	volID := c.volumeID
	csi, err := c.csiClientGetter.Get()
	if err != nil {
		// CSI 驱动暂不可用按瞬时错误处理。
		// 参见 https://github.com/kubernetes/kubernetes/issues/120268
		return volumetypes.NewTransientOperationFailure(log("Unmounter.TearDownAt failed to get CSI client: %v", err))
	}

	// c.spec 为 nil，无法判断是否为迁移操作。
	ctx, cancel := createCSIOperationContext(c.spec, csiTimeout)
	defer cancel()

	if err := csi.NodeUnpublishVolume(ctx, volID, dir); err != nil {
		return errors.New(log("Unmounter.TearDownAt failed: %v", err))
	}

	// 根据 CSI 规范，NodePublish RPC 中提供的 target_path
	// （此处即 `dir`）必须由 CSI 插件负责删除。
	//
	// kubelet 只应负责删除自己创建的 JSON 数据文件
	// 及其父目录。
	//
	// 但某些 CSI 插件可能存在缺陷、不遵循规范，
	// 因此当 target_path 已卸载且为空时，这里仍需将其删除。
	if err := removeMountDir(c.plugin, dir); err != nil {
		return errors.New(log("Unmounter.TearDownAt failed to clean mount dir [%s]: %v", dir, err))
	}
	klog.V(4).Info(log("Unmounter.TearDownAt successfully unmounted dir [%s]", dir))

	return nil
}

// supportsFSGroup 判断是否应由 kubelet 应用 FSGroup。
func (c *csiMountMgr) supportsFSGroup(fsType string, fsGroup *int64, driverPolicy storage.FSGroupPolicy) bool {
	if fsGroup == nil || driverPolicy == storage.NoneFSGroupPolicy || c.readOnly {
		return false
	}

	if driverPolicy == storage.FileFSGroupPolicy {
		return true
	}

	if fsType == "" {
		klog.V(4).Info(log("mounter.SetupAt WARNING: skipping fsGroup, fsType not provided"))
		return false
	}

	if c.spec.PersistentVolume != nil {
		if c.spec.PersistentVolume.Spec.AccessModes == nil {
			klog.V(4).Info(log("mounter.SetupAt WARNING: skipping fsGroup, access modes not provided"))
			return false
		}
		if !hasReadWriteOnce(c.spec.PersistentVolume.Spec.AccessModes) {
			klog.V(4).Info(log("mounter.SetupAt WARNING: skipping fsGroup, only support ReadWriteOnce access mode"))
			return false
		}
		return true
	} else if c.spec.Volume != nil && c.spec.Volume.CSI != nil {
		// 内联 CSI 卷在 SetUpAt 中始终以 RWO access mode 挂载
		return true
	}

	klog.V(4).Info(log("mounter.SetupAt WARNING: skipping fsGroup, unsupported volume type"))
	return false
}

// getFSGroupPolicy 返回 CSI 驱动的 FSGroup 策略。
// 返回错误表示不支持，并说明原因。
func (c *csiMountMgr) getFSGroupPolicy() (storage.FSGroupPolicy, error) {
	// 获取 CSIDriver。驱动不存在不算错误
	// （CSIDriver 是可选对象）。
	var csiDriver *storage.CSIDriver
	driver := string(c.driverName)
	if c.plugin.csiDriverLister != nil {
		c, err := c.plugin.getCSIDriver(driver)
		if err != nil && !apierrors.IsNotFound(err) {
			// 内部错误。
			return storage.ReadWriteOnceWithFSTypeFSGroupPolicy, err
		}
		csiDriver = c
	}

	// 未定义 csiDriver 时返回默认行为。
	if csiDriver == nil {
		return storage.ReadWriteOnceWithFSTypeFSGroupPolicy, nil
	}
	// csiDriver 存在但未定义 fsGroupPolicy 时返回错误。
	if csiDriver.Spec.FSGroupPolicy == nil || *csiDriver.Spec.FSGroupPolicy == "" {
		return storage.ReadWriteOnceWithFSTypeFSGroupPolicy, errors.New(log("expected valid fsGroupPolicy, received nil value or empty string"))
	}
	return *csiDriver.Spec.FSGroupPolicy, nil
}

// supportsVolumeLifecycleMode 检查 CSI 驱动是否支持给定卷生命周期模式。
// 返回错误表示不支持，并说明原因。
func (c *csiMountMgr) supportsVolumeLifecycleMode() error {
	// 获取 CSIDriver。驱动不存在不算错误
	// （CSIDriver 是可选对象），但此时只支持持久卷。
	var csiDriver *storage.CSIDriver
	driver := string(c.driverName)
	if c.plugin.csiDriverLister != nil {
		c, err := c.plugin.getCSIDriver(driver)
		if err != nil && !apierrors.IsNotFound(err) {
			// 内部错误。
			return err
		}
		csiDriver = c
	}

	// 如何响应取决于是否掌握驱动
	// 与卷模式的信息。
	switch {
	case csiDriver == nil && c.volumeLifecycleMode == storage.VolumeLifecyclePersistent:
		// 没有驱动信息，但对持久卷而言可以接受（且仅限持久卷）。
		return nil
	case csiDriver == nil:
		return fmt.Errorf("volume mode %q not supported by driver %s (no CSIDriver object)", c.volumeLifecycleMode, driver)
	case containsVolumeMode(csiDriver.Spec.VolumeLifecycleModes, c.volumeLifecycleMode):
		// 模式被显式列出。
		return nil
	default:
		return fmt.Errorf("volume mode %q not supported by driver %s (only supports %q)", c.volumeLifecycleMode, driver, csiDriver.Spec.VolumeLifecycleModes)
	}
}

// containsVolumeMode 判断给定卷模式是否在列表中。
func containsVolumeMode(modes []storage.VolumeLifecycleMode, mode storage.VolumeLifecycleMode) bool {
	return slices.Contains(modes, mode)
}

// isDirMounted 返回 IsLikelyNotMountPoint 检查结果的取反值
func isDirMounted(plug *csiPlugin, dir string) (bool, error) {
	mounter := plug.host.GetMounter()
	notMnt, err := mounter.IsLikelyNotMountPoint(dir)
	if err != nil && !os.IsNotExist(err) {
		klog.Error(log("isDirMounted IsLikelyNotMountPoint test failed for dir [%v]", dir))
		return false, err
	}
	return !notMnt, nil
}

// removeMountDir 在目录未挂载时清理挂载目录，并删除其中的卷数据文件
func removeMountDir(plug *csiPlugin, mountPath string) error {
	klog.V(4).Info(log("removing mount path [%s]", mountPath))

	mnt, err := isDirMounted(plug, mountPath)
	if err != nil {
		return err
	}
	if !mnt {
		klog.V(4).Info(log("dir not mounted, deleting it [%s]", mountPath))
		if err := os.Remove(mountPath); err != nil && !os.IsNotExist(err) {
			return errors.New(log("failed to remove dir [%s]: %v", mountPath, err))
		}
		// 同时删除卷数据文件。
		volPath := filepath.Dir(mountPath)
		dataFile := filepath.Join(volPath, volDataFileName)
		klog.V(4).Info(log("also deleting volume info data file [%s]", dataFile))
		if err := os.Remove(dataFile); err != nil && !os.IsNotExist(err) {
			return errors.New(log("failed to delete volume data file [%s]: %v", dataFile, err))
		}
		// 删除卷路径。
		klog.V(4).Info(log("deleting volume path [%s]", volPath))
		if err := os.Remove(volPath); err != nil && !os.IsNotExist(err) {
			return errors.New(log("failed to delete volume path [%s]: %v", volPath, err))
		}
	}
	return nil
}

// makeVolumeHandle 返回 csi-<sha256(podUID,volSourceSpecName)> 形式的句柄
func makeVolumeHandle(podUID, volSourceSpecName string) string {
	result := sha256.Sum256([]byte(fmt.Sprintf("%s%s", podUID, volSourceSpecName)))
	return fmt.Sprintf("csi-%x", result)
}

// mergeMap 将 second 的键值合并入 first（同键以 second 为准）。
func mergeMap(first, second map[string]string) map[string]string {
	if first == nil {
		return second
	}
	for k, v := range second {
		first[k] = v
	}
	return first
}
