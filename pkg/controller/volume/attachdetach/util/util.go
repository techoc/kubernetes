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

package util

import (
	"errors"
	"fmt"
	"strings"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/component-helpers/storage/ephemeral"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/controller/volume/attachdetach/cache"
	"k8s.io/kubernetes/pkg/volume"
	"k8s.io/kubernetes/pkg/volume/csimigration"
	"k8s.io/kubernetes/pkg/volume/util"
)

// createInTreeVolumeSpec 从 Pod 的卷定义构建 in-tree 插件的 volume.Spec：
// 对 inline 卷直接构造，对 PVC 卷则解引用到其绑定的 PV 再构造。
func createInTreeVolumeSpec(logger klog.Logger, podVolume *v1.Volume, pod *v1.Pod, vpm *volume.VolumePluginMgr, pvcLister corelisters.PersistentVolumeClaimLister, pvLister corelisters.PersistentVolumeLister, csiMigratedPluginManager csimigration.PluginManager, csiTranslator csimigration.InTreeToCSITranslator) (*volume.Spec, string, error) {
	claimName := ""
	readOnly := false
	if pvcSource := podVolume.VolumeSource.PersistentVolumeClaim; pvcSource != nil {
		claimName = pvcSource.ClaimName
		readOnly = pvcSource.ReadOnly
	}
	isEphemeral := podVolume.VolumeSource.Ephemeral != nil
	if isEphemeral {
		claimName = ephemeral.VolumeClaimName(pod, podVolume)
	}
	if claimName == "" {
		// inline 卷，无需解引用 PVC/PV。
		return volume.NewSpecFromVolume(podVolume), "", nil
	}
	// 该卷基于 PVC，需要解引用出 PVC 及其绑定的 PV。
	logger.V(10).Info("Found PVC", "PVC", klog.KRef(pod.Namespace, claimName))

	// 从缓存中取出声明背后真正的 PV。
	pvc, err := getPVCFromCache(pod.Namespace, claimName, pvcLister)
	if err != nil {
		return nil, claimName, fmt.Errorf(
			"error processing PVC %q/%q: %w",
			pod.Namespace,
			claimName,
			err)
	}
	if isEphemeral {
		// 临时卷的 PVC 由控制器按 Pod 创建，需确认其确实属于该 Pod。
		if err := ephemeral.VolumeIsForPod(pod, pvc); err != nil {
			return nil, claimName, err
		}
	}

	pvName, pvcUID := pvc.Spec.VolumeName, pvc.UID
	logger.V(10).Info("Found bound PV for PVC", "PVC", klog.KRef(pod.Namespace, claimName), "pvcUID", pvcUID, "PV", klog.KRef("", pvName))

	// 根据 PV 对象构造实际的 volume.Spec。
	volumeSpec, err := getPVSpecFromCache(
		pvName, readOnly, pvcUID, pvLister)
	if err != nil {
		return nil, claimName, fmt.Errorf(
			"error processing PVC %q/%q: %w",
			pod.Namespace,
			claimName,
			err)
	}

	logger.V(10).Info("Extracted volumeSpec from bound PV and PVC", "PVC", klog.KRef(pod.Namespace, claimName), "pvcUID", pvcUID, "PV", klog.KRef("", pvName), "volumeSpecName", volumeSpec.Name())
	return volumeSpec, claimName, nil
}

// CreateVolumeSpec 为指定卷创建可修改的 volume.Spec；必要时解引用 PVC 得到 PV。
// 当 in-tree 插件满足节点上的 CSI 迁移条件时，会将 spec 转换为对应的 CSI 插件 spec。
func CreateVolumeSpec(logger klog.Logger, podVolume v1.Volume, pod *v1.Pod, vpm *volume.VolumePluginMgr, pvcLister corelisters.PersistentVolumeClaimLister, pvLister corelisters.PersistentVolumeLister, csiMigratedPluginManager csimigration.PluginManager, csiTranslator csimigration.InTreeToCSITranslator) (*volume.Spec, error) {
	volumeSpec, claimName, err := createInTreeVolumeSpec(logger, &podVolume, pod, vpm, pvcLister, pvLister, csiMigratedPluginManager, csiTranslator)
	if err != nil {
		return nil, err
	}
	volumeSpec, err = translateInTreeSpecToCSIIfNeeded(logger, volumeSpec, vpm, csiMigratedPluginManager, csiTranslator, pod.Namespace)
	if err != nil {
		return nil, fmt.Errorf(
			"error performing CSI migration checks and translation for PVC %q/%q: %w",
			pod.Namespace,
			claimName,
			err)
	}
	return volumeSpec, nil
}

// CreateVolumeSpecWithNodeMigration 为指定卷创建可修改的 volume.Spec，必要时解引用 PVC 得到 PV。
// 引用 in-tree 插件 spec 的 volume.Spec 会在节点上满足该 in-tree 插件的 CSI 迁移
// 全部条件时，被转换为指向已迁移 CSI 插件的 spec。
func CreateVolumeSpecWithNodeMigration(logger klog.Logger, podVolume v1.Volume, pod *v1.Pod, nodeName types.NodeName, vpm *volume.VolumePluginMgr, pvcLister corelisters.PersistentVolumeClaimLister, pvLister corelisters.PersistentVolumeLister, csiMigratedPluginManager csimigration.PluginManager, csiTranslator csimigration.InTreeToCSITranslator) (*volume.Spec, error) {
	volumeSpec, claimName, err := createInTreeVolumeSpec(logger, &podVolume, pod, vpm, pvcLister, pvLister, csiMigratedPluginManager, csiTranslator)
	if err != nil {
		return nil, err
	}
	volumeSpec, err = translateInTreeSpecToCSIOnNodeIfNeeded(logger, volumeSpec, nodeName, vpm, csiMigratedPluginManager, csiTranslator, pod.Namespace)
	if err != nil {
		return nil, fmt.Errorf(
			"error performing CSI migration checks and translation for PVC %q/%q: %w",
			pod.Namespace,
			claimName,
			err)
	}
	return volumeSpec, nil
}

// getPVCFromCache 从共享的内部 PVC 存储中按给定的命名空间和名称获取 PVC 对象。
// 缓存中不存在对应 PVC 时返回错误；PVC 状态不是 "Bound" 时也返回错误。
func getPVCFromCache(namespace string, name string, pvcLister corelisters.PersistentVolumeClaimLister) (*v1.PersistentVolumeClaim, error) {
	pvc, err := pvcLister.PersistentVolumeClaims(namespace).Get(name)
	if err != nil {
		return nil, fmt.Errorf("failed to find PVC %s/%s in PVCInformer cache: %v", namespace, name, err)
	}

	// 未绑定或 VolumeName 为空的 PVC 还不能解析出 PV，拒绝继续处理。
	if pvc.Status.Phase != v1.ClaimBound || pvc.Spec.VolumeName == "" {
		return nil, fmt.Errorf(
			"PVC %s/%s has non-bound phase (%q) or empty pvc.Spec.VolumeName (%q)",
			namespace,
			name,
			pvc.Status.Phase,
			pvc.Spec.VolumeName)
	}

	return pvc, nil
}

// getPVSpecFromCache 从共享的内部 PV 存储中按名称获取 PV 对象，返回表示它的 volume.Spec。
// 缓存中不存在对应 PV 时返回错误。
// 该方法会深拷贝 PV 对象，调用方使用返回的 volume.Spec 时无需担心对象被意外修改。
func getPVSpecFromCache(name string, pvcReadOnly bool, expectedClaimUID types.UID, pvLister corelisters.PersistentVolumeLister) (*volume.Spec, error) {
	pv, err := pvLister.Get(name)
	if err != nil {
		return nil, fmt.Errorf("failed to find PV %q in PVInformer cache: %v", name, err)
	}
	if pv.Spec.ClaimRef == nil {
		return nil, fmt.Errorf(
			"found PV object %q but it has a nil pv.Spec.ClaimRef indicating it is not yet bound to the claim",
			name)
	}

	// ClaimRef.UID 不匹配说明该 PV 已不属于此 PVC，避免误用过期绑定关系。
	if pv.Spec.ClaimRef.UID != expectedClaimUID {
		return nil, fmt.Errorf(
			"found PV object %q but its pv.Spec.ClaimRef.UID (%q) does not point to claim.UID (%q)",
			name,
			pv.Spec.ClaimRef.UID,
			expectedClaimUID)
	}

	// 不直接返回 Informer 中的对象：存储是共享的，可能被其他消费者修改。
	clonedPV := pv.DeepCopy()

	return volume.NewSpecFromPersistentVolume(clonedPV, pvcReadOnly), nil
}

// DetermineVolumeAction 返回 true 表示需要把卷和 Pod 加入 dswp，
// 返回 false 表示需要把它们从 dswp 中移除。
func DetermineVolumeAction(pod *v1.Pod, defaultAction bool) bool {
	if pod == nil || len(pod.Spec.Volumes) <= 0 {
		return defaultAction
	}

	// 已终止的 Pod 不再需要挂载卷，应移除其状态记录。
	if util.IsPodTerminated(pod, pod.Status) {
		return false
	}
	return defaultAction
}

// ProcessPodVolumes 处理给定 Pod 中的卷：addVolumes 为 true 时加入期望状态，否则移除。
func ProcessPodVolumes(logger klog.Logger, pod *v1.Pod, addVolumes bool, desiredStateOfWorld cache.DesiredStateOfWorld, volumePluginMgr *volume.VolumePluginMgr, pvcLister corelisters.PersistentVolumeClaimLister, pvLister corelisters.PersistentVolumeLister, csiMigratedPluginManager csimigration.PluginManager, csiTranslator csimigration.InTreeToCSITranslator) {
	if pod == nil {
		return
	}
	if len(pod.Spec.Volumes) <= 0 {
		logger.V(10).Info("Skipping processing of pod, it has no volumes", "pod", klog.KObj(pod))
		return
	}

	nodeName := types.NodeName(pod.Spec.NodeName)
	if nodeName == "" {
		logger.V(10).Info("Skipping processing of pod, it is not scheduled to a node", "pod", klog.KObj(pod))
		return
	} else if !desiredStateOfWorld.NodeExists(nodeName) {
		// Pod 被调度到的节点尚不在期望状态数据结构中，说明该节点还未被控制器管理，
		// 因此忽略该 Pod。
		logger.V(4).Info("Skipping processing of pod, it is scheduled to node which is not managed by the controller", "node", klog.KRef("", string(nodeName)), "pod", klog.KObj(pod))
		return
	}

	// 逐个处理 Pod 中定义的卷。
	for _, podVolume := range pod.Spec.Volumes {
		volumeSpec, err := CreateVolumeSpecWithNodeMigration(logger, podVolume, pod, nodeName, volumePluginMgr, pvcLister, pvLister, csiMigratedPluginManager, csiTranslator)
		if err != nil {
			logger.V(10).Info("Error processing volume for pod", "pod", klog.KObj(pod), "volumeName", podVolume.Name, "err", err)
			continue
		}

		attachableVolumePlugin, err :=
			volumePluginMgr.FindAttachablePluginBySpec(volumeSpec)
		if err != nil || attachableVolumePlugin == nil {
			// 只有实现 attacher 接口的插件才由该控制器处理。
			logger.V(10).Info("Skipping volume for pod, it does not implement attacher interface", "pod", klog.KObj(pod), "volumeName", podVolume.Name, "err", err)
			continue
		}

		uniquePodName := util.GetUniquePodName(pod)
		if addVolumes {
			// 将卷加入期望状态。
			_, err := desiredStateOfWorld.AddPod(
				uniquePodName, pod, volumeSpec, nodeName)
			if err != nil {
				logger.V(10).Info("Failed to add volume for pod to desiredStateOfWorld", "pod", klog.KObj(pod), "volumeName", podVolume.Name, "err", err)
			}

		} else {
			// 从期望状态中移除卷。
			uniqueVolumeName, err := util.GetUniqueVolumeNameFromSpec(
				attachableVolumePlugin, volumeSpec)
			if err != nil {
				logger.V(10).Info("Failed to delete volume for pod from desiredStateOfWorld. GetUniqueVolumeNameFromSpec failed", "pod", klog.KObj(pod), "volumeName", podVolume.Name, "err", err)
				continue
			}
			desiredStateOfWorld.DeletePod(
				uniquePodName, uniqueVolumeName, nodeName)
		}
	}
}

// translateInTreeSpecToCSIOnNodeIfNeeded 在卷可迁移且指定节点支持迁移时，
// 将 in-tree 卷 spec 转换为对应的 CSI 卷 spec。
func translateInTreeSpecToCSIOnNodeIfNeeded(logger klog.Logger, spec *volume.Spec, nodeName types.NodeName, vpm *volume.VolumePluginMgr, csiMigratedPluginManager csimigration.PluginManager, csiTranslator csimigration.InTreeToCSITranslator, podNamespace string) (*volume.Spec, error) {
	translatedSpec := spec
	migratable, err := csiMigratedPluginManager.IsMigratable(spec)
	if err != nil {
		return nil, err
	}
	if !migratable {
		// spec 本身不可迁移时直接返回，避免再做多余的节点检查。
		return spec, nil
	}
	migrationSupportedOnNode, err := isCSIMigrationSupportedOnNode(nodeName, spec, vpm, csiMigratedPluginManager)
	if err != nil {
		return nil, err
	}
	if migratable && migrationSupportedOnNode {
		translatedSpec, err = csimigration.TranslateInTreeSpecToCSI(logger, spec, podNamespace, csiTranslator)
		if err != nil {
			return nil, err
		}
	}
	return translatedSpec, nil
}

// translateInTreeSpecToCSIIfNeeded 在卷可迁移时将其 in-tree spec 转换为 CSI spec。
func translateInTreeSpecToCSIIfNeeded(logger klog.Logger, spec *volume.Spec, vpm *volume.VolumePluginMgr, csiMigratedPluginManager csimigration.PluginManager, csiTranslator csimigration.InTreeToCSITranslator, podNamespace string) (*volume.Spec, error) {
	migratable, err := csiMigratedPluginManager.IsMigratable(spec)
	if err != nil {
		return nil, err
	}
	if !migratable {
		// spec 本身不可迁移时直接返回，避免再做多余的节点检查。
		return spec, nil
	}
	translatedSpec, err := csimigration.TranslateInTreeSpecToCSI(logger, spec, podNamespace, csiTranslator)
	if err != nil {
		return nil, err
	}
	return translatedSpec, nil
}

// isCSIMigrationSupportedOnNode 判断给定节点上的 in-tree 插件是否已完成 CSI 迁移。
func isCSIMigrationSupportedOnNode(nodeName types.NodeName, spec *volume.Spec, vpm *volume.VolumePluginMgr, csiMigratedPluginManager csimigration.PluginManager) (bool, error) {
	pluginName, err := csiMigratedPluginManager.GetInTreePluginNameFromSpec(spec.PersistentVolume, spec.Volume)
	if err != nil {
		return false, err
	}

	if len(pluginName) == 0 {
		// 无法从转换目录中查到插件名，视为未迁移。
		return false, nil
	}

	if csiMigratedPluginManager.IsMigrationCompleteForPlugin(pluginName) {
		// 插件启用"迁移完成"标记后，假定所有节点都已安装并配置好 CSI 插件；
		// 即使管理端与节点存在版本差异，也认为支持 CSI 迁移。
		return true, nil
	}

	if len(nodeName) == 0 {
		return false, errors.New("nodeName is empty")
	}

	kubeClient := vpm.Host.GetKubeClient()
	if kubeClient == nil {
		// 无法进行控制器/kubelet 版本差异检查时，退化为只检查特性开关。
		// 这种情况可能出现在独立运行的（headless）kubelet 中。
		return true, nil
	}

	adcHost, ok := vpm.Host.(volume.AttachDetachVolumeHost)
	if !ok {
		// 同样退化为只检查特性开关：例如 kubelet 上设置了
		// "enableControllerAttachDetach" 为 true 的情况。
		return true, nil
	}

	if adcHost.CSINodeLister() == nil {
		return false, errors.New("could not find CSINodeLister in attachDetachController")
	}

	csiNode, err := adcHost.CSINodeLister().Get(string(nodeName))
	if err != nil {
		return false, err
	}

	ann := csiNode.GetAnnotations()
	if ann == nil {
		return false, nil
	}

	mpa := ann[v1.MigratedPluginsAnnotationKey]
	tok := strings.Split(mpa, ",")
	mpaSet := sets.NewString(tok...)

	isMigratedOnNode := mpaSet.Has(pluginName)

	return isMigratedOnNode, nil
}
