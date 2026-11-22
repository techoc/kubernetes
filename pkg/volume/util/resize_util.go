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

package util

import (
	"context"
	"encoding/json"
	"fmt"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/strategicpatch"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/pkg/volume"
	volumetypes "k8s.io/kubernetes/pkg/volume/util/types"
	"k8s.io/mount-utils"
	"k8s.io/utils/exec"
)

var (
	// knownResizeConditions 列出已知的扩容相关条件类型，合并时需与原条件区分对待。
	knownResizeConditions = map[v1.PersistentVolumeClaimConditionType]bool{
		v1.PersistentVolumeClaimFileSystemResizePending: true,
		v1.PersistentVolumeClaimResizing:                true,
		v1.PersistentVolumeClaimControllerResizeError:   true,
		v1.PersistentVolumeClaimNodeResizeError:         true,
	}

	// AnnPreResizeCapacity 在卷扩容时添加到 PV 上的注解。
	// 其值为扩容前 PVC 的 status.capacity。
	// 当 external-resizer 判断扩容后需要调整文件系统大小时，会设置该注解。
	// pv_controller 绑定 PVC 与 PV 时，将用它来确定 PVC 的 status.capacity。
	AnnPreResizeCapacity = "volume.alpha.kubernetes.io/pre-resize-capacity"
)

// resizeProcessStatus 记录一个扩容条件及其是否已被处理。
type resizeProcessStatus struct {
	condition v1.PersistentVolumeClaimCondition
	processed bool
}

// UpdatePVSize 在云厂商扩容成功后只更新 PV 的容量。
func UpdatePVSize(
	pv *v1.PersistentVolume,
	newSize resource.Quantity,
	kubeClient clientset.Interface) (*v1.PersistentVolume, error) {
	pvClone := pv.DeepCopy()
	pvClone.Spec.Capacity[v1.ResourceStorage] = newSize

	return PatchPV(pv, pvClone, kubeClient)
}

// AddAnnPreResizeCapacity 给 PV 添加 volume.alpha.kubernetes.io/pre-resize-capacity 注解。
func AddAnnPreResizeCapacity(
	pv *v1.PersistentVolume,
	oldCapacity resource.Quantity,
	kubeClient clientset.Interface) error {
	// 已有扩容注解则跳过，避免覆盖首次扩容前的容量。
	if metav1.HasAnnotation(pv.ObjectMeta, AnnPreResizeCapacity) {
		return nil
	}

	pvClone := pv.DeepCopy()
	if pvClone.ObjectMeta.Annotations == nil {
		pvClone.ObjectMeta.Annotations = make(map[string]string)
	}
	pvClone.ObjectMeta.Annotations[AnnPreResizeCapacity] = oldCapacity.String()

	_, err := PatchPV(pv, pvClone, kubeClient)
	return err
}

// DeleteAnnPreResizeCapacity 删除 PV 上的 volume.alpha.kubernetes.io/pre-resize-capacity 注解。
func DeleteAnnPreResizeCapacity(
	pv *v1.PersistentVolume,
	kubeClient clientset.Interface) error {
	// 没有扩容注解则跳过整个流程。
	if !metav1.HasAnnotation(pv.ObjectMeta, AnnPreResizeCapacity) {
		return nil
	}
	pvClone := pv.DeepCopy()
	delete(pvClone.ObjectMeta.Annotations, AnnPreResizeCapacity)
	_, err := PatchPV(pv, pvClone, kubeClient)
	return err
}

// PatchPV 为 PV 创建并执行 patch。
func PatchPV(
	oldPV *v1.PersistentVolume,
	newPV *v1.PersistentVolume,
	kubeClient clientset.Interface) (*v1.PersistentVolume, error) {
	oldData, err := json.Marshal(oldPV)
	if err != nil {
		return oldPV, fmt.Errorf("unexpected error marshaling old PV %q with error : %v", oldPV.Name, err)
	}

	newData, err := json.Marshal(newPV)
	if err != nil {
		return oldPV, fmt.Errorf("unexpected error marshaling new PV %q with error : %v", newPV.Name, err)
	}

	// 只提交差异部分，避免整对象更新覆盖他人改动。
	patchBytes, err := strategicpatch.CreateTwoWayMergePatch(oldData, newData, oldPV)
	if err != nil {
		return oldPV, fmt.Errorf("error Creating two way merge patch for PV %q with error : %v", oldPV.Name, err)
	}

	updatedPV, err := kubeClient.CoreV1().PersistentVolumes().Patch(context.TODO(), oldPV.Name, types.StrategicMergePatchType, patchBytes, metav1.PatchOptions{})
	if err != nil {
		return oldPV, fmt.Errorf("error Patching PV %q with error : %v", oldPV.Name, err)
	}
	return updatedPV, nil
}

// MarkResizeInProgressWithResizer 将云厂商扩容标记为进行中，并在 PVC 上记录 resizer 名称。
func MarkResizeInProgressWithResizer(
	pvc *v1.PersistentVolumeClaim,
	resizerName string,
	kubeClient clientset.Interface) (*v1.PersistentVolumeClaim, error) {
	// 标记 PVC 扩容已开始。
	progressCondition := v1.PersistentVolumeClaimCondition{
		Type:               v1.PersistentVolumeClaimResizing,
		Status:             v1.ConditionTrue,
		LastTransitionTime: metav1.Now(),
	}
	conditions := []v1.PersistentVolumeClaimCondition{progressCondition}
	newPVC := pvc.DeepCopy()
	newPVC = MergeResizeConditionOnPVC(newPVC, conditions, false /* 不保留旧的扩容条件 */)
	newPVC = setResizer(newPVC, resizerName)
	return PatchPVCStatus(pvc /* 旧 PVC */, newPVC, kubeClient)
}

// MarkControllerReisizeInProgress 将控制器侧扩容标记为进行中，记录 resizer 名称、目标容量与状态。
func MarkControllerReisizeInProgress(pvc *v1.PersistentVolumeClaim, resizerName string, newSize resource.Quantity, kubeClient clientset.Interface) (*v1.PersistentVolumeClaim, error) {
	// 标记 PVC 扩容已开始。
	progressCondition := v1.PersistentVolumeClaimCondition{
		Type:               v1.PersistentVolumeClaimResizing,
		Status:             v1.ConditionTrue,
		LastTransitionTime: metav1.Now(),
	}
	conditions := []v1.PersistentVolumeClaimCondition{progressCondition}
	newPVC := pvc.DeepCopy()
	newPVC = MergeResizeConditionOnPVC(newPVC, conditions, false /* 不保留旧的扩容条件 */)
	newPVC = mergeStorageResourceStatus(newPVC, v1.PersistentVolumeClaimControllerResizeInProgress)
	newPVC = mergeStorageAllocatedResources(newPVC, newSize)
	newPVC = setResizer(newPVC, resizerName)
	return PatchPVCStatus(pvc /* 旧 PVC */, newPVC, kubeClient)
}

// SetClaimResizer 在 PVC 上设置 resizer 注解。
func SetClaimResizer(
	pvc *v1.PersistentVolumeClaim,
	resizerName string,
	kubeClient clientset.Interface) (*v1.PersistentVolumeClaim, error) {
	newPVC := pvc.DeepCopy()
	newPVC = setResizer(newPVC, resizerName)
	return PatchPVCStatus(pvc /* 旧 PVC */, newPVC, kubeClient)
}

// setResizer 设置 PVC 上的 resizer 注解；值相同时直接返回以避免无谓 patch。
func setResizer(pvc *v1.PersistentVolumeClaim, resizerName string) *v1.PersistentVolumeClaim {
	if val, ok := pvc.Annotations[volumetypes.VolumeResizerKey]; ok && val == resizerName {
		return pvc
	}
	metav1.SetMetaDataAnnotation(&pvc.ObjectMeta, volumetypes.VolumeResizerKey, resizerName)
	return pvc
}

// MarkForFSResize 将文件系统扩容标记为 pending。
func MarkForFSResize(
	pvc *v1.PersistentVolumeClaim,
	kubeClient clientset.Interface) (*v1.PersistentVolumeClaim, error) {
	pvcCondition := v1.PersistentVolumeClaimCondition{
		Type:               v1.PersistentVolumeClaimFileSystemResizePending,
		Status:             v1.ConditionTrue,
		LastTransitionTime: metav1.Now(),
		Message:            "Waiting for user to (re-)start a pod to finish file system resize of volume on node.",
	}
	conditions := []v1.PersistentVolumeClaimCondition{pvcCondition}
	newPVC := pvc.DeepCopy()

	if utilfeature.DefaultFeatureGate.Enabled(features.RecoverVolumeExpansionFailure) {
		newPVC = mergeStorageResourceStatus(newPVC, v1.PersistentVolumeClaimNodeResizePending)
	}

	newPVC = MergeResizeConditionOnPVC(newPVC, conditions, true /* 保留旧的扩容条件 */)
	updatedPVC, err := PatchPVCStatus(pvc /* 旧 PVC */, newPVC, kubeClient)
	return updatedPVC, err
}

// MarkResizeFinished 标记所有扩容操作已完成。
func MarkResizeFinished(
	pvc *v1.PersistentVolumeClaim,
	newSize resource.Quantity,
	kubeClient clientset.Interface) (*v1.PersistentVolumeClaim, error) {
	return MarkFSResizeFinished(pvc, newSize, kubeClient)
}

// MarkFSResizeFinished 标记文件系统扩容已完成。
func MarkFSResizeFinished(
	pvc *v1.PersistentVolumeClaim,
	newSize resource.Quantity,
	kubeClient clientset.Interface) (*v1.PersistentVolumeClaim, error) {
	newPVC := pvc.DeepCopy()

	newPVC.Status.Capacity[v1.ResourceStorage] = newSize

	// 启用 RecoverVolumeExpansionFailure 时需将 ResizeStatus 重置为 nil，
	// 否则残留状态会被误判为扩容仍在进行。
	if utilfeature.DefaultFeatureGate.Enabled(features.RecoverVolumeExpansionFailure) {
		allocatedResourceStatusMap := newPVC.Status.AllocatedResourceStatuses
		delete(allocatedResourceStatusMap, v1.ResourceStorage)
		if len(allocatedResourceStatusMap) == 0 {
			newPVC.Status.AllocatedResourceStatuses = nil
		} else {
			newPVC.Status.AllocatedResourceStatuses = allocatedResourceStatusMap
		}
	}

	newPVC = MergeResizeConditionOnPVC(newPVC, []v1.PersistentVolumeClaimCondition{}, false /* 不保留旧的扩容条件 */)
	updatedPVC, err := PatchPVCStatus(pvc /* 旧 PVC */, newPVC, kubeClient)
	return updatedPVC, err
}

// MarkNodeExpansionFinishedWithRecovery 标记节点扩容已完成，并清除分配资源状态以支持扩容失败恢复。
func MarkNodeExpansionFinishedWithRecovery(
	pvc *v1.PersistentVolumeClaim,
	newSize resource.Quantity,
	kubeClient clientset.Interface) (*v1.PersistentVolumeClaim, error) {
	newPVC := pvc.DeepCopy()

	newPVC.Status.Capacity[v1.ResourceStorage] = newSize

	// 扩容已收敛，清掉 storage 的状态；无其他状态时将整个 map 置 nil。
	allocatedResourceStatusMap := newPVC.Status.AllocatedResourceStatuses
	delete(allocatedResourceStatusMap, v1.ResourceStorage)
	if len(allocatedResourceStatusMap) == 0 {
		newPVC.Status.AllocatedResourceStatuses = nil
	} else {
		newPVC.Status.AllocatedResourceStatuses = allocatedResourceStatusMap
	}

	newPVC = MergeResizeConditionOnPVC(newPVC, []v1.PersistentVolumeClaimCondition{}, false /* 不保留旧的扩容条件 */)
	updatedPVC, err := PatchPVCStatus(pvc /* 旧 PVC */, newPVC, kubeClient)
	return updatedPVC, err
}

// MarkNodeExpansionInfeasible 将 PVC 的节点扩容标记为不可行并置为失败。
// Kubelet 不应重试已失败卷的扩容。
func MarkNodeExpansionInfeasible(pvc *v1.PersistentVolumeClaim, kubeClient clientset.Interface, err error) (*v1.PersistentVolumeClaim, error) {
	newPVC := pvc.DeepCopy()
	newPVC = mergeStorageResourceStatus(newPVC, v1.PersistentVolumeClaimNodeResizeInfeasible)
	errorCondition := v1.PersistentVolumeClaimCondition{
		Type:               v1.PersistentVolumeClaimNodeResizeError,
		Status:             v1.ConditionTrue,
		LastTransitionTime: metav1.Now(),
		Message:            fmt.Sprintf("failed to expand pvc with %v", err),
	}
	newPVC = MergeResizeConditionOnPVC(newPVC,
		[]v1.PersistentVolumeClaimCondition{errorCondition},
		true /* 保留旧的扩容条件 */)

	// 错误场景不带 resourceVersion 校验，避免冲突覆盖错误状态。
	patchBytes, err := createPVCPatch(pvc, newPVC, false /* 不校验 resourceVersion */)
	if err != nil {
		return pvc, fmt.Errorf("patchPVCStatus failed to patch PVC %q: %v", pvc.Name, err)
	}

	updatedClaim, updateErr := kubeClient.CoreV1().PersistentVolumeClaims(pvc.Namespace).
		Patch(context.TODO(), pvc.Name, types.StrategicMergePatchType, patchBytes, metav1.PatchOptions{}, "status")
	if updateErr != nil {
		return pvc, fmt.Errorf("patchPVCStatus failed to patch PVC %q: %v", pvc.Name, updateErr)
	}
	return updatedClaim, nil
}

// MarkNodeExpansionFailedCondition 将 PVC 的节点扩容标记为失败条件。
func MarkNodeExpansionFailedCondition(pvc *v1.PersistentVolumeClaim, kubeClient clientset.Interface, err error) (*v1.PersistentVolumeClaim, error) {
	newPVC := pvc.DeepCopy()
	errorCondition := v1.PersistentVolumeClaimCondition{
		Type:               v1.PersistentVolumeClaimNodeResizeError,
		Status:             v1.ConditionTrue,
		LastTransitionTime: metav1.Now(),
		Message:            fmt.Sprintf("failed to expand pvc with %v", err),
	}
	newPVC = MergeResizeConditionOnPVC(newPVC,
		[]v1.PersistentVolumeClaimCondition{errorCondition},
		true /* 保留旧的扩容条件 */)
	patchBytes, err := createPVCPatch(pvc, newPVC, false /* 不校验 resourceVersion */)
	if err != nil {
		return pvc, fmt.Errorf("patchPVCStatus failed to patch PVC %q: %w", pvc.Name, err)
	}

	updatedClaim, updateErr := kubeClient.CoreV1().PersistentVolumeClaims(pvc.Namespace).
		Patch(context.TODO(), pvc.Name, types.StrategicMergePatchType, patchBytes, metav1.PatchOptions{}, "status")
	if updateErr != nil {
		return pvc, fmt.Errorf("patchPVCStatus failed to patch PVC %q: %w", pvc.Name, updateErr)
	}
	return updatedClaim, nil
}

// MarkNodeExpansionInProgress 将 PVC 在节点上的扩容标记为进行中。
func MarkNodeExpansionInProgress(pvc *v1.PersistentVolumeClaim, kubeClient clientset.Interface) (*v1.PersistentVolumeClaim, error) {
	newPVC := pvc.DeepCopy()
	newPVC = mergeStorageResourceStatus(newPVC, v1.PersistentVolumeClaimNodeResizeInProgress)
	updatedPVC, err := PatchPVCStatus(pvc /* 旧 PVC */, newPVC, kubeClient)
	return updatedPVC, err
}

// PatchPVCStatus 通过 PATCH 更新 PVC 状态。
// 不用 Update：该方法可能由 kubelet 调用，旧版 kubelet 的 Update 会覆盖新字段；
// 同时在 patch 中加入 ResourceVersion，使 patch 的 RV 与实际 RV 不符时失败，避免写入过期对象。
func PatchPVCStatus(
	oldPVC *v1.PersistentVolumeClaim,
	newPVC *v1.PersistentVolumeClaim,
	kubeClient clientset.Interface) (*v1.PersistentVolumeClaim, error) {
	patchBytes, err := createPVCPatch(oldPVC, newPVC, true /* 校验 resourceVersion */)
	if err != nil {
		return oldPVC, fmt.Errorf("patchPVCStatus failed to patch PVC %q: %v", oldPVC.Name, err)
	}

	updatedClaim, updateErr := kubeClient.CoreV1().PersistentVolumeClaims(oldPVC.Namespace).
		Patch(context.TODO(), oldPVC.Name, types.StrategicMergePatchType, patchBytes, metav1.PatchOptions{}, "status")
	if updateErr != nil {
		return oldPVC, fmt.Errorf("patchPVCStatus failed to patch PVC %q: %v", oldPVC.Name, updateErr)
	}
	return updatedClaim, nil
}

// createPVCPatch 为新 PVC 与旧 PVC 的差异生成 strategic merge patch，可选加入 resourceVersion 校验。
func createPVCPatch(
	oldPVC *v1.PersistentVolumeClaim,
	newPVC *v1.PersistentVolumeClaim, addResourceVersionCheck bool) ([]byte, error) {
	oldData, err := json.Marshal(oldPVC)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal old data: %v", err)
	}

	newData, err := json.Marshal(newPVC)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal new data: %v", err)
	}

	patchBytes, err := strategicpatch.CreateTwoWayMergePatch(oldData, newData, oldPVC)
	if err != nil {
		return nil, fmt.Errorf("failed to create 2 way merge patch: %v", err)
	}

	if addResourceVersionCheck {
		patchBytes, err = addResourceVersion(patchBytes, oldPVC.ResourceVersion)
		if err != nil {
			return nil, fmt.Errorf("failed to add resource version: %v", err)
		}
	}

	return patchBytes, nil
}

// addResourceVersion 将 resourceVersion 写入 patch，使 patch 具备乐观并发校验能力。
func addResourceVersion(patchBytes []byte, resourceVersion string) ([]byte, error) {
	var patchMap map[string]interface{}
	err := json.Unmarshal(patchBytes, &patchMap)
	if err != nil {
		return nil, fmt.Errorf("error unmarshalling patch: %v", err)
	}
	u := unstructured.Unstructured{Object: patchMap}
	a, err := meta.Accessor(&u)
	if err != nil {
		return nil, fmt.Errorf("error creating accessor: %v", err)
	}
	a.SetResourceVersion(resourceVersion)
	versionBytes, err := json.Marshal(patchMap)
	if err != nil {
		return nil, fmt.Errorf("error marshalling json patch: %v", err)
	}
	return versionBytes, nil
}

// MergeResizeConditionOnPVC 将指定的扩容条件合并进 PVC，其他条件保持不变。
func MergeResizeConditionOnPVC(
	pvc *v1.PersistentVolumeClaim,
	resizeConditions []v1.PersistentVolumeClaimCondition, keepOldResizeConditions bool) *v1.PersistentVolumeClaim {
	resizeConditionMap := map[v1.PersistentVolumeClaimConditionType]*resizeProcessStatus{}

	for _, condition := range resizeConditions {
		resizeConditionMap[condition.Type] = &resizeProcessStatus{condition, false}
	}

	oldConditions := pvc.Status.Conditions
	newConditions := []v1.PersistentVolumeClaimCondition{}
	for _, condition := range oldConditions {
		// 非扩容条件原样保留。
		if _, ok := knownResizeConditions[condition.Type]; !ok {
			newConditions = append(newConditions, condition)
			continue
		}

		if newCondition, ok := resizeConditionMap[condition.Type]; ok {
			if newCondition.condition.Status != condition.Status {
				newConditions = append(newConditions, newCondition.condition)
			} else {
				newConditions = append(newConditions, condition)
			}
			newCondition.processed = true
		} else if keepOldResizeConditions {
			// keepOldResizeConditions 为真时，保留 PVC 原有状态中的扩容条件。
			newConditions = append(newConditions, condition)
		}
	}

	// 追加所有未被处理的新条件。
	for _, newCondition := range resizeConditionMap {
		if !newCondition.processed {
			newConditions = append(newConditions, newCondition.condition)
		}
	}
	pvc.Status.Conditions = newConditions
	return pvc
}

// mergeStorageResourceStatus 更新 PVC 上 storage 资源的分配状态。
func mergeStorageResourceStatus(pvc *v1.PersistentVolumeClaim, status v1.ClaimResourceStatus) *v1.PersistentVolumeClaim {
	allocatedResourceStatusMap := pvc.Status.AllocatedResourceStatuses
	if allocatedResourceStatusMap == nil {
		pvc.Status.AllocatedResourceStatuses = map[v1.ResourceName]v1.ClaimResourceStatus{
			v1.ResourceStorage: status,
		}
		return pvc
	}
	allocatedResourceStatusMap[v1.ResourceStorage] = status
	pvc.Status.AllocatedResourceStatuses = allocatedResourceStatusMap
	return pvc
}

// mergeStorageAllocatedResources 更新 PVC 上 storage 资源的已分配容量。
func mergeStorageAllocatedResources(pvc *v1.PersistentVolumeClaim, size resource.Quantity) *v1.PersistentVolumeClaim {
	allocatedResourcesMap := pvc.Status.AllocatedResources
	if allocatedResourcesMap == nil {
		pvc.Status.AllocatedResources = map[v1.ResourceName]resource.Quantity{
			v1.ResourceStorage: size,
		}
		return pvc
	}
	allocatedResourcesMap[v1.ResourceStorage] = size
	pvc.Status.AllocatedResources = allocatedResourcesMap
	return pvc
}

// GenericResizeFS 调用通用文件系统扩容器，供无特殊文件系统扩容需求的插件使用。
func GenericResizeFS(host volume.VolumeHost, devicePath, deviceMountPath string) (bool, error) {
	resizer := mount.NewResizeFs(exec.New())
	return resizer.Resize(devicePath, deviceMountPath)
}
