/*
Copyright 2024 The Kubernetes Authors.

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

package framework

import (
	"context"
	"fmt"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/client-go/dynamic"
	"k8s.io/kubernetes/test/e2e/framework"
	"k8s.io/kubernetes/test/e2e/storage/utils"
)

// GetVolumeGroupSnapshot 构造一个带标签选择器的 VolumeGroupSnapshot 对象。
// vgsclassName 可选；为空时控制器会使用集群默认的 VolumeGroupSnapshotClass。
func GetVolumeGroupSnapshot(ns string, matchLabels map[string]interface{}, vgsclassName string) *unstructured.Unstructured {
	spec := map[string]interface{}{
		"source": map[string]interface{}{
			"selector": map[string]interface{}{
				"matchLabels": matchLabels,
			},
		},
	}
	if vgsclassName != "" {
		spec["volumeGroupSnapshotClassName"] = vgsclassName
	}
	return &unstructured.Unstructured{
		Object: map[string]interface{}{
			"kind":       "VolumeGroupSnapshot",
			"apiVersion": utils.VolumeGroupSnapshotAPIVersion,
			"metadata": map[string]interface{}{
				"generateName": "group-snapshot-",
				"namespace":    ns,
			},
			"spec": spec,
		},
	}
}

// VolumeGroupSnapshotResource 表示某个测试用例对应的 VolumeGroupSnapshotClass、VolumeGroupSnapshot 及其绑定内容
type VolumeGroupSnapshotResource struct {
	Config  *PerTestConfig
	Pattern TestPattern

	VGS        *unstructured.Unstructured
	VGSContent *unstructured.Unstructured
	VGSClass   *unstructured.Unstructured

	// SnapshotHandleToPVCName 将快照句柄映射到源 PVC 名称。
	// 在转换为预制备快照时填充此映射，以保留预制备资源中丢失的 PVC 名称信息。
	SnapshotHandleToPVCName map[string]string
}

// CreateVolumeGroupSnapshot 使用动态客户端创建具有指定 SnapshotDeletionPolicy 的 VolumeGroupSnapshotClass，
// 并基于该 Class 创建 VolumeGroupSnapshot。
// 返回非结构化的 VolumeGroupSnapshotClass 和 VolumeGroupSnapshot 对象。
func CreateVolumeGroupSnapshot(ctx context.Context, sDriver VolumeGroupSnapshottableTestDriver, config *PerTestConfig, pattern TestPattern, groupName string, pvcNamespace string, timeouts *framework.TimeoutContext, parameters map[string]string) (*unstructured.Unstructured, *unstructured.Unstructured, *unstructured.Unstructured) {
	defer ginkgo.GinkgoRecover()
	var err error
	if pattern.SnapshotType != VolumeGroupSnapshot && pattern.SnapshotType != PreprovisionedCreatedVolumeGroupSnapshot {
		err = fmt.Errorf("SnapshotType must be set to either VolumeGroupSnapshot or PreprovisionedCreatedVolumeGroupSnapshot")
		framework.ExpectNoError(err, "SnapshotType is set to VolumeGroupSnapshot or PreprovisionedCreatedVolumeGroupSnapshot")
	}
	dc := config.Framework.DynamicClient

	ginkgo.By("creating a VolumeGroupSnapshotClass")
	gsclass := sDriver.GetVolumeGroupSnapshotClass(ctx, config, parameters)
	if gsclass == nil {
		framework.Failf("Failed to get volume group snapshot class based on test config")
	}
	gsclass.Object["deletionPolicy"] = pattern.SnapshotDeletionPolicy.String()

	gsclass, err = dc.Resource(utils.VolumeGroupSnapshotClassGVR).Create(ctx, gsclass, metav1.CreateOptions{})
	framework.ExpectNoError(err, "Failed to create volume group snapshot class")
	gsclass, err = dc.Resource(utils.VolumeGroupSnapshotClassGVR).Get(ctx, gsclass.GetName(), metav1.GetOptions{})
	framework.ExpectNoError(err, "Failed to get volume group snapshot class")

	ginkgo.By("creating a dynamic VolumeGroupSnapshot")
	// 准备一个包含特定数据的动态制备组卷快照
	volumeGroupSnapshot := GetVolumeGroupSnapshot(pvcNamespace, map[string]interface{}{
		"group": groupName,
	}, gsclass.GetName())

	volumeGroupSnapshot, err = dc.Resource(utils.VolumeGroupSnapshotGVR).Namespace(volumeGroupSnapshot.GetNamespace()).Create(ctx, volumeGroupSnapshot, metav1.CreateOptions{})
	framework.ExpectNoError(err, "Failed to create volume group snapshot")
	ginkgo.By("Waiting for group snapshot to be ready")
	err = utils.WaitForVolumeGroupSnapshotReady(ctx, dc, volumeGroupSnapshot.GetNamespace(), volumeGroupSnapshot.GetName(), framework.Poll, timeouts.SnapshotCreate*10)
	framework.ExpectNoError(err, "Group snapshot is not ready to use within the timeout")
	ginkgo.By("Getting group snapshot and content")
	volumeGroupSnapshot, err = dc.Resource(utils.VolumeGroupSnapshotGVR).Namespace(volumeGroupSnapshot.GetNamespace()).Get(ctx, volumeGroupSnapshot.GetName(), metav1.GetOptions{})
	framework.ExpectNoError(err, "Failed to get volume group snapshot after creation")
	status := volumeGroupSnapshot.Object["status"]
	err = framework.Gomega().Expect(status).NotTo(gomega.BeNil())
	framework.ExpectNoError(err, "Failed to get status of volume group snapshot")
	vgscName := status.(map[string]interface{})["boundVolumeGroupSnapshotContentName"].(string)
	err = framework.Gomega().Expect(vgscName).NotTo(gomega.BeNil())
	framework.ExpectNoError(err, "Failed to get content name of volume group snapshot")
	vgsc, err := dc.Resource(utils.VolumeGroupSnapshotContentGVR).Get(ctx, vgscName, metav1.GetOptions{})
	framework.ExpectNoError(err, "failed to get content of group snapshot")
	return gsclass, volumeGroupSnapshot, vgsc
}

// patchVGSCDeletionPolicy 修改单个 VolumeGroupSnapshotContent 的删除策略
func patchVGSCDeletionPolicy(ctx context.Context, dc dynamic.Interface, vgscName string, deletionPolicy string) error {
	patchData := fmt.Appendf(nil, `{"spec":{"deletionPolicy":"%s"}}`, deletionPolicy)
	_, err := dc.Resource(utils.VolumeGroupSnapshotContentGVR).Patch(ctx, vgscName, types.MergePatchType, patchData, metav1.PatchOptions{})
	if err != nil {
		return fmt.Errorf("failed to patch VGSC %q deletion policy to %s: %w", vgscName, deletionPolicy, err)
	}
	return nil
}

// patchVSCDeletionPolicy 修改单个 VolumeSnapshotContent 的删除策略
func patchVSCDeletionPolicy(ctx context.Context, dc dynamic.Interface, vscName string, deletionPolicy string) error {
	patchData := fmt.Appendf(nil, `{"spec":{"deletionPolicy":"%s"}}`, deletionPolicy)
	_, err := dc.Resource(utils.SnapshotContentGVR).Patch(ctx, vscName, types.MergePatchType, patchData, metav1.PatchOptions{})
	if err != nil {
		return fmt.Errorf("failed to patch VSC %q deletion policy to %s: %w", vscName, deletionPolicy, err)
	}
	return nil
}

// patchVSCsDeletionPolicy 修改给定 VGS 拥有的所有 VolumeSnapshotContent 的删除策略
func (r *VolumeGroupSnapshotResource) patchVSCsDeletionPolicy(ctx context.Context, dc dynamic.Interface, vgsNamespace string, vgsUID types.UID, deletionPolicy string) error {
	// 获取该 VGS 拥有的所有 VolumeSnapshotContent 名称
	contentNamesSet, err := r.getVolumeSnapshotContentNames(ctx, dc, vgsNamespace, vgsUID)
	if err != nil {
		return fmt.Errorf("failed to get VolumeSnapshotContent names: %w", err)
	}

	// 修改所有 VolumeSnapshotContent 的删除策略
	for _, contentName := range contentNamesSet.UnsortedList() {
		if err := patchVSCDeletionPolicy(ctx, dc, contentName, deletionPolicy); err != nil {
			return err
		}
	}

	return nil
}

// patchVGSAndVSCsDeletionPolicy 修改 VGSC 及其关联的所有 VSC 的删除策略
func (r *VolumeGroupSnapshotResource) patchVGSAndVSCsDeletionPolicy(ctx context.Context, dc dynamic.Interface, vgsc *unstructured.Unstructured, vgsNamespace string, vgsUID types.UID, deletionPolicy string) (*unstructured.Unstructured, error) {
	// 修改 VGSC 的删除策略
	if err := patchVGSCDeletionPolicy(ctx, dc, vgsc.GetName(), deletionPolicy); err != nil {
		return nil, err
	}

	// 获取更新后的 VGSC
	updatedVGSC, err := dc.Resource(utils.VolumeGroupSnapshotContentGVR).Get(ctx, vgsc.GetName(), metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to get updated VGSC %q: %w", vgsc.GetName(), err)
	}

	// 修改所有 VolumeSnapshotContent 的删除策略
	if err := r.patchVSCsDeletionPolicy(ctx, dc, vgsNamespace, vgsUID, deletionPolicy); err != nil {
		return nil, err
	}

	return updatedVGSC, nil
}

// getVolumeSnapshotContentNames 返回绑定到给定 VGS 所拥有的 VolumeSnapshot 的 VolumeSnapshotContent 名称集合
func (r *VolumeGroupSnapshotResource) getVolumeSnapshotContentNames(ctx context.Context, dc dynamic.Interface, vgsNamespace string, vgsUID types.UID) (sets.Set[string], error) {
	vss, err := dc.Resource(utils.SnapshotGVR).Namespace(vgsNamespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}

	// 过滤出该 VGS 拥有的 VolumeSnapshot
	ownedSnapshots := utils.FilterResourcesByOwner(vss.Items, "VolumeGroupSnapshot", vgsUID)

	// 从拥有的快照中提取 VolumeSnapshotContent 名称
	contentNamesSet := sets.New[string]()
	for _, vs := range ownedSnapshots {
		if status, ok := vs.Object["status"].(map[string]interface{}); ok {
			if cName, ok := status["boundVolumeSnapshotContentName"].(string); ok && cName != "" {
				contentNamesSet.Insert(cName)
			}
		}
	}
	return contentNamesSet, nil
}

// cleanupRetainedVolumeSnapshotContents 强制删除采用 Retain 策略的 VolumeSnapshotContent
func (r *VolumeGroupSnapshotResource) cleanupRetainedVolumeSnapshotContents(ctx context.Context, dc dynamic.Interface, contentNames sets.Set[string], timeouts *framework.TimeoutContext) {
	// 更新删除策略并删除每个 VolumeSnapshotContent
	for _, contentName := range contentNames.UnsortedList() {
		// 将删除策略改为 Delete
		if err := patchVSCDeletionPolicy(ctx, dc, contentName, "Delete"); err != nil {
			framework.Failf("Failed to patch VolumeSnapshotContent %q deletion policy: %v", contentName, err)
			return
		}

		// 删除 VolumeSnapshotContent
		err := dc.Resource(utils.SnapshotContentGVR).Delete(ctx, contentName, metav1.DeleteOptions{})
		if err != nil {
			framework.Failf("Failed to delete VolumeSnapshotContent %q: %v", contentName, err)
			return
		}
	}

	// 等待所有 VolumeSnapshotContent 被删除
	framework.Logf("Waiting for VolumeSnapshotContents to be deleted")
	for _, contentName := range contentNames.UnsortedList() {
		if err := utils.WaitForGVRDeletion(ctx, dc, utils.SnapshotContentGVR, contentName, framework.Poll, timeouts.SnapshotDelete); err != nil {
			framework.Failf("VolumeSnapshotContent %q was not fully deleted: %v", contentName, err)
			return
		}
	}
}

// cleanupRetainedVGSContent 强制删除采用 Retain 策略的 VolumeGroupSnapshotContent
func (r *VolumeGroupSnapshotResource) cleanupRetainedVGSContent(ctx context.Context, dc dynamic.Interface, vgscName string, timeouts *framework.TimeoutContext) {
	framework.Logf("Deleting VGSContent %q", vgscName)

	// 将删除策略改为 Delete
	if err := patchVGSCDeletionPolicy(ctx, dc, vgscName, "Delete"); err != nil {
		framework.Failf("Failed to patch VGSContent %q deletion policy: %v", vgscName, err)
		return
	}

	// 删除 VGSContent
	err := dc.Resource(utils.VolumeGroupSnapshotContentGVR).Delete(ctx, vgscName, metav1.DeleteOptions{})
	if err != nil {
		framework.Failf("Failed to delete VGSContent %q: %v", vgscName, err)
		return
	}

	// 等待 VGSContent 被删除
	framework.Logf("Waiting for VGSContent %q to be deleted", vgscName)
	err = utils.WaitForGVRDeletion(ctx, dc, utils.VolumeGroupSnapshotContentGVR, vgscName, framework.Poll, timeouts.SnapshotDelete)
	if err != nil {
		framework.Failf("VGSContent %q was not fully deleted: %v", vgscName, err)
		return
	}
}

// verifyVolumeSnapshotContentsDeletion 验证所有 VolumeSnapshotContent 已被删除
func (r *VolumeGroupSnapshotResource) verifyVolumeSnapshotContentsDeletion(ctx context.Context, dc dynamic.Interface, contentNames sets.Set[string], timeouts *framework.TimeoutContext) {
	ginkgo.By("Verifying all VolumeSnapshotContents have been deleted")
	var verifyErrs []error
	for _, contentName := range contentNames.UnsortedList() {
		err := utils.EnsureGVRDeletion(ctx, dc, utils.SnapshotContentGVR, contentName, framework.Poll, timeouts.SnapshotDelete, "")
		if err != nil {
			verifyErrs = append(verifyErrs, fmt.Errorf("VolumeSnapshotContent %q should be deleted: %w", contentName, err))
		}
	}
	if len(verifyErrs) > 0 {
		framework.ExpectNoError(utilerrors.NewAggregate(verifyErrs), "Failed to verify VolumeSnapshotContents deletion")
	}
}

// CleanupVGS 删除 VolumeGroupSnapshot，并确保绑定的 VolumeGroupSnapshotContent 采用 Delete 策略。
// 它会等待 VolumeGroupSnapshot、其拥有的 VolumeSnapshot 和 VolumeSnapshotContent
// 以及绑定的 VolumeGroupSnapshotContent 全部删除，以防资源泄漏。
func (r *VolumeGroupSnapshotResource) CleanupVGS(ctx context.Context, timeouts *framework.TimeoutContext) error {
	if r.VGS == nil {
		return nil
	}

	dc := r.Config.Framework.DynamicClient
	vgsNamespace := r.VGS.GetNamespace()
	vgsName := r.VGS.GetName()
	vgsUID := r.VGS.GetUID()
	framework.Logf("deleting groupSnapshot %q/%q and ensuring content has Delete policy", vgsNamespace, vgsName)

	// 获取 VGS 以确认其存在
	vgs, err := dc.Resource(utils.VolumeGroupSnapshotGVR).Namespace(vgsNamespace).Get(ctx, vgsName, metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("failed to get VGS %q: %w", vgsName, err)
	}
	r.VGS = vgs

	// 获取该 VGS 拥有的所有 VolumeSnapshotContent 名称
	contentNamesSet, err := r.getVolumeSnapshotContentNames(ctx, dc, vgsNamespace, vgsUID)
	if err != nil {
		return err
	}

	// 获取绑定的 VolumeGroupSnapshotContent
	groupSnapshotStatus := r.VGS.Object["status"].(map[string]interface{})
	groupSnapshotContentName := groupSnapshotStatus["boundVolumeGroupSnapshotContentName"].(string)
	framework.Logf("received groupSnapshotStatus %v", groupSnapshotStatus)
	framework.Logf("groupSnapshotContentName %q", groupSnapshotContentName)

	boundVGSContent, err := dc.Resource(utils.VolumeGroupSnapshotContentGVR).Get(ctx, groupSnapshotContentName, metav1.GetOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("failed to get bound VGSContent %q: %w", groupSnapshotContentName, err)
	}

	// 对于 Delete 策略，确认删除策略已设为 Delete，防止泄漏
	// 对于 Retain 策略，将在验证保留行为后再更新它
	if boundVGSContent != nil && r.Pattern.SnapshotDeletionPolicy == DeleteSnapshot {
		spec := boundVGSContent.Object["spec"].(map[string]interface{})
		if spec["deletionPolicy"] != "Delete" {
			boundVGSContent, err = r.patchVGSAndVSCsDeletionPolicy(ctx, dc, boundVGSContent, vgsNamespace, vgsUID, "Delete")
			if err != nil {
				return fmt.Errorf("failed to update deletion policy: %w", err)
			}
		}
	}
	r.VGSContent = boundVGSContent

	// 删除 VolumeGroupSnapshot
	if err := dc.Resource(utils.VolumeGroupSnapshotGVR).Namespace(vgsNamespace).Delete(ctx, vgsName, metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("failed to delete VGS %q: %w", vgsName, err)
	}

	// 等待 VolumeGroupSnapshot 被删除
	if err := utils.WaitForNamespacedGVRDeletion(ctx, dc, utils.VolumeGroupSnapshotGVR, vgsNamespace, vgsName, framework.Poll, timeouts.SnapshotDelete); err != nil {
		return fmt.Errorf("failed waiting for VGS %q deletion: %w", vgsName, err)
	}

	// 等待该 VGS 拥有的 VolumeSnapshot 被级联删除
	// 无论删除策略如何都会发生，因为 VGS 通过 owner reference 拥有这些 VolumeSnapshot
	if vgsUID != "" {
		if err := utils.WaitForOwnedResourcesDeleted(ctx, dc, utils.SnapshotGVR, vgsNamespace, vgsUID, framework.Poll, timeouts.SnapshotDelete); err != nil {
			return fmt.Errorf("failed waiting for owned snapshots deletion of VGS %q: %w", vgsName, err)
		}
	}

	// 验证 VolumeGroupSnapshotContent 的删除策略行为
	if boundVGSContent != nil {
		vgscName := boundVGSContent.GetName()

		switch r.Pattern.SnapshotDeletionPolicy {
		case DeleteSnapshot:
			ginkgo.By(fmt.Sprintf("Verifying VolumeGroupSnapshotContent %q has been deleted per Delete policy", vgscName))
			err = utils.EnsureGVRDeletion(ctx, dc, utils.VolumeGroupSnapshotContentGVR, vgscName, framework.Poll, timeouts.SnapshotDelete, "")
			framework.ExpectNoError(err, "VolumeGroupSnapshotContent should be deleted with Delete policy")
			r.VGSContent = nil
		case RetainSnapshot:
			ginkgo.By(fmt.Sprintf("Verifying VolumeGroupSnapshotContent %q has been retained per Retain policy", vgscName))
			err = utils.EnsureNoGVRDeletion(ctx, dc, utils.VolumeGroupSnapshotContentGVR, vgscName, framework.Poll, 20*framework.Poll, "")
			framework.ExpectNoError(err, "VolumeGroupSnapshotContent should be retained with Retain policy")

			// 强制清理以防泄漏
			// 清理顺序：
			// 1. VolumeSnapshotContent（更新策略 + 删除 + 等待）
			// 2. VGSContent（更新策略 + 删除 + 等待）
			framework.Logf("Retain policy verified, now forcing cleanup to prevent leaks")
			r.cleanupRetainedVolumeSnapshotContents(ctx, dc, contentNamesSet, timeouts)
			r.cleanupRetainedVGSContent(ctx, dc, vgscName, timeouts)
			r.VGSContent = nil
		}
	}

	// 验证所有 VolumeSnapshotContent 已被删除
	r.verifyVolumeSnapshotContentsDeletion(ctx, dc, contentNamesSet, timeouts)

	return nil
}

// CleanupVGSClass 删除 VolumeGroupSnapshotClass。
func (r *VolumeGroupSnapshotResource) CleanupVGSClass(ctx context.Context, timeouts *framework.TimeoutContext) error {
	if r.VGSClass == nil {
		framework.Logf("VGSClass is nil, skipping cleanup")
		return nil
	}

	dc := r.Config.Framework.DynamicClient
	vgsClassName := r.VGSClass.GetName()
	framework.Logf("deleting VolumeGroupSnapshotClass %q", vgsClassName)

	err := dc.Resource(utils.VolumeGroupSnapshotClassGVR).Delete(ctx, vgsClassName, metav1.DeleteOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("failed to delete groupSnapshotClass %q: %w", vgsClassName, err)
	}

	if err = utils.WaitForGVRDeletion(ctx, dc, utils.VolumeGroupSnapshotClassGVR, vgsClassName, framework.Poll, timeouts.SnapshotDelete); err != nil {
		return fmt.Errorf("failed waiting for VolumeGroupSnapshotClass %q deletion: %w", vgsClassName, err)
	}

	framework.Logf("successfully deleted VolumeGroupSnapshotClass %q", vgsClassName)
	r.VGSClass = nil
	return nil
}

// CleanupResource 使用动态客户端删除 VolumeGroupSnapshotClass 和 VolumeGroupSnapshot 对象，
// 并忽略 not found 错误。
func (r *VolumeGroupSnapshotResource) CleanupResource(ctx context.Context, timeouts *framework.TimeoutContext) error {
	var cleanupErrs []error

	if err := r.CleanupVGS(ctx, timeouts); err != nil {
		cleanupErrs = append(cleanupErrs, err)
	}

	if err := r.CleanupVGSClass(ctx, timeouts); err != nil {
		cleanupErrs = append(cleanupErrs, err)
	}

	return utilerrors.NewAggregate(cleanupErrs)
}

// CreateVolumeGroupSnapshotResource 使用给定参数创建 VolumeGroupSnapshotResource 对象。
func CreateVolumeGroupSnapshotResource(ctx context.Context, sDriver VolumeGroupSnapshottableTestDriver, config *PerTestConfig, pattern TestPattern, pvcName string, pvcNamespace string, timeouts *framework.TimeoutContext, parameters map[string]string) *VolumeGroupSnapshotResource {
	vgsClass, snapshot, vgsc := CreateVolumeGroupSnapshot(ctx, sDriver, config, pattern, pvcName, pvcNamespace, timeouts, parameters)
	r := &VolumeGroupSnapshotResource{
		Config:     config,
		Pattern:    pattern,
		VGS:        snapshot,
		VGSClass:   vgsClass,
		VGSContent: vgsc,
	}

	dc := config.Framework.DynamicClient

	if pattern.SnapshotType == PreprovisionedCreatedVolumeGroupSnapshot {
		r.convertToPreprovisioned(ctx, dc, snapshot, vgsc, vgsClass, pvcNamespace, timeouts)
	}

	return r
}

// convertToPreprovisioned 将动态制备的 VolumeGroupSnapshot 转换为预制备版本。
// 步骤如下：
// 1. 记录动态快照中的快照句柄
// 2. 删除动态快照资源（采用 Retain 策略以保留后端快照）
// 3. 创建引用同一批后端快照的预制备资源
func (r *VolumeGroupSnapshotResource) convertToPreprovisioned(ctx context.Context, dc dynamic.Interface, snapshot, vgsc, vgsClass *unstructured.Unstructured, pvcNamespace string, timeouts *framework.TimeoutContext) {
	var err error
	vgsUID := snapshot.GetUID()

	// 仅当模式指定 Delete 策略时才改为 Retain，
	// 这样删除动态资源时后端快照会被保留
	if r.Pattern.SnapshotDeletionPolicy == DeleteSnapshot {
		ginkgo.By("updating VGSC and VSC deletion policies to Retain")
		vgsc, err = r.patchVGSAndVSCsDeletionPolicy(ctx, dc, vgsc, pvcNamespace, vgsUID, "Retain")
		framework.ExpectNoError(err, "failed to update VGSC and VSC deletion policies to Retain")
	}

	ginkgo.By("recording properties of the pre-provisioned group snapshot")
	vgscStatus := vgsc.Object["status"].(map[string]interface{})
	groupSnapshotHandle := vgscStatus["volumeGroupSnapshotHandle"].(string)
	framework.Logf("Recording group snapshot content handle: %s", groupSnapshotHandle)

	// 从 volumeSnapshotInfoList 中提取各个卷快照句柄
	volumeSnapshotInfoList := vgscStatus["volumeSnapshotInfoList"].([]interface{})
	volumeSnapshotHandles := make([]string, 0, len(volumeSnapshotInfoList))
	for _, info := range volumeSnapshotInfoList {
		infoMap := info.(map[string]interface{})
		snapshotHandle := infoMap["snapshotHandle"].(string)
		volumeSnapshotHandles = append(volumeSnapshotHandles, snapshotHandle)
	}
	framework.Logf("Recording %d volume snapshot handles", len(volumeSnapshotHandles))

	csiDriverName := vgsClass.Object["driver"].(string)
	framework.Logf("Recording snapshot driver: %s", csiDriverName)

	// 获取该 VGS 拥有的所有 VolumeSnapshotContent 名称 before deletion
	contentNamesSet, err := r.getVolumeSnapshotContentNames(ctx, dc, pvcNamespace, vgsUID)
	framework.ExpectNoError(err, "failed to get VolumeSnapshotContent names owned by VGS %s", vgsUID)

	// 删除动态快照前记录快照句柄到源 PVC 名称的映射。
	// 预制备快照没有 spec.source.persistentVolumeClaimName，因此需要这一步。
	ginkgo.By("capturing source PVC names from dynamic VolumeSnapshots")
	vsList, err := dc.Resource(utils.SnapshotGVR).Namespace(pvcNamespace).List(ctx, metav1.ListOptions{})
	framework.ExpectNoError(err, "failed to list VolumeSnapshots")

	// 过滤出该 VGS 拥有的 VolumeSnapshot
	ownedSnapshots := utils.FilterResourcesByOwner(vsList.Items, "VolumeGroupSnapshot", vgsUID)

	// 将 snapshotHandle 映射到源 PVC 名称并存入资源结构体
	r.SnapshotHandleToPVCName = make(map[string]string)
	for _, vs := range ownedSnapshots {
		// 从动态 VolumeSnapshot 获取源 PVC 名称。
		// 由于已等待 VGS 就绪，VS 应已绑定并带有源 PVC 名称。
		spec := vs.Object["spec"].(map[string]interface{})
		source := spec["source"].(map[string]interface{})
		sourcePVCName := source["persistentVolumeClaimName"].(string)

		// 从 status 获取 VSC 名称（VGS 已就绪，因此 VS 已绑定）
		status := vs.Object["status"].(map[string]interface{})
		vscName := status["boundVolumeSnapshotContentName"].(string)

		// 获取 VSC 以找到快照句柄（VS 已绑定，因此它应存在）
		vsc, err := dc.Resource(utils.SnapshotContentGVR).Get(ctx, vscName, metav1.GetOptions{})
		framework.ExpectNoError(err, "failed to get VolumeSnapshotContent %s for bound VolumeSnapshot %s", vscName, vs.GetName())

		vscStatus := vsc.Object["status"].(map[string]interface{})
		snapshotHandle := vscStatus["snapshotHandle"].(string)
		r.SnapshotHandleToPVCName[snapshotHandle] = sourcePVCName
		framework.Logf("Mapped snapshot handle %s to PVC %s", snapshotHandle, sourcePVCName)
	}

	// 当 VGSC 的删除策略为 Retain 时：
	// 删除 VGS 不会删除 VGSC；手动删除 VGSC 也不会删除底层组快照资源。
	// 我们利用这一点创建组快照资源，再基于它创建预制备快照。
	ginkgo.By("deleting the group snapshot")
	err = dc.Resource(utils.VolumeGroupSnapshotGVR).Namespace(snapshot.GetNamespace()).Delete(ctx, snapshot.GetName(), metav1.DeleteOptions{})
	framework.ExpectNoError(err, "failed to delete VolumeGroupSnapshot %s/%s", snapshot.GetNamespace(), snapshot.GetName())

	ginkgo.By("checking the VolumeGroupSnapshot has been deleted")
	err = utils.WaitForNamespacedGVRDeletion(ctx, dc, utils.VolumeGroupSnapshotGVR, snapshot.GetNamespace(), snapshot.GetName(), framework.Poll, timeouts.SnapshotDelete)
	framework.ExpectNoError(err, "VolumeGroupSnapshot %s/%s was not deleted within timeout", snapshot.GetNamespace(), snapshot.GetName())

	// 等待该 VGS 拥有的 VolumeSnapshot 被级联删除
	ginkgo.By("waiting for owned VolumeSnapshots to be deleted")
	err = utils.WaitForOwnedResourcesDeleted(ctx, dc, utils.SnapshotGVR, pvcNamespace, vgsUID, framework.Poll, timeouts.SnapshotDelete)
	framework.ExpectNoError(err, "VolumeSnapshots owned by VGS %s were not deleted within timeout", vgsUID)

	// 删除 VolumeSnapshotContent（采用 Retain 策略，因此物理快照仍保留）
	ginkgo.By("deleting VolumeSnapshotContents")
	for _, contentName := range contentNamesSet.UnsortedList() {
		err := dc.Resource(utils.SnapshotContentGVR).Delete(ctx, contentName, metav1.DeleteOptions{})
		if err != nil && !apierrors.IsNotFound(err) {
			framework.ExpectNoError(err, "failed to delete VolumeSnapshotContent %s", contentName)
		}
	}

	// 等待 VolumeSnapshotContent 被删除
	for _, contentName := range contentNamesSet.UnsortedList() {
		err := utils.WaitForGVRDeletion(ctx, dc, utils.SnapshotContentGVR, contentName, framework.Poll, timeouts.SnapshotDelete)
		framework.ExpectNoError(err, "VolumeSnapshotContent %s was not deleted within timeout", contentName)
	}

	// 删除 VolumeGroupSnapshotContent (with Retain policy, so physical group snapshot remains)
	err = dc.Resource(utils.VolumeGroupSnapshotContentGVR).Delete(ctx, vgsc.GetName(), metav1.DeleteOptions{})
	framework.ExpectNoError(err, "failed to delete VolumeGroupSnapshotContent %s", vgsc.GetName())

	ginkgo.By("checking the VolumeGroupSnapshotContent has been deleted")
	err = utils.WaitForGVRDeletion(ctx, dc, utils.VolumeGroupSnapshotContentGVR, vgsc.GetName(), framework.Poll, timeouts.SnapshotDelete)
	framework.ExpectNoError(err, "VolumeGroupSnapshotContent %s was not deleted within timeout", vgsc.GetName())

	// 使用提取出的组快照句柄创建新的预制备 VGSContent
	ginkgo.By("creating a new pre-provisioned VolumeGroupSnapshotContent with the group snapshot handle")
	vgsUUID := uuid.NewUUID()

	vgsName := getPreProvisionedVolumeGroupSnapshotName(vgsUUID)
	vgscName := getPreProvisionedVolumeGroupSnapshotContentName(vgsUUID)

	r.VGSContent = getPreProvisionedVolumeGroupSnapshotContent(vgscName, vgsName, pvcNamespace, groupSnapshotHandle, volumeSnapshotHandles, r.Pattern.SnapshotDeletionPolicy.String(), csiDriverName)
	r.VGSContent, err = dc.Resource(utils.VolumeGroupSnapshotContentGVR).Create(ctx, r.VGSContent, metav1.CreateOptions{})
	framework.ExpectNoError(err, "failed to create pre-provisioned VolumeGroupSnapshotContent %s", vgscName)

	ginkgo.By("creating a pre-provisioned VGS with that VGSContent")
	r.VGS = getPreProvisionedVolumeGroupSnapshot(vgsName, pvcNamespace, vgscName)
	r.VGS, err = dc.Resource(utils.VolumeGroupSnapshotGVR).Namespace(r.VGS.GetNamespace()).Create(ctx, r.VGS, metav1.CreateOptions{})
	framework.ExpectNoError(err, "failed to create pre-provisioned VolumeGroupSnapshot %s/%s", pvcNamespace, vgsName)

	err = utils.WaitForVolumeGroupSnapshotReady(ctx, dc, r.VGS.GetNamespace(), r.VGS.GetName(), framework.Poll, timeouts.SnapshotCreate*10)
	framework.ExpectNoError(err, "pre-provisioned VolumeGroupSnapshot %s/%s did not become ready within timeout", r.VGS.GetNamespace(), r.VGS.GetName())

	// 获取新 VGS 的 UID 用于 owner reference
	r.VGS, err = dc.Resource(utils.VolumeGroupSnapshotGVR).Namespace(r.VGS.GetNamespace()).Get(ctx, r.VGS.GetName(), metav1.GetOptions{})
	framework.ExpectNoError(err, "failed to get VolumeGroupSnapshot %s/%s for UID", r.VGS.GetNamespace(), r.VGS.GetName())
	newVgsUID := r.VGS.GetUID()
	vgsAPIVersion := r.VGS.GetAPIVersion()
	vgsKind := r.VGS.GetKind()

	// 创建新的预制备 VolumeSnapshotContent 和 VolumeSnapshot
	// VolumeSnapshot 必须由 VGS 拥有，控制器才能发现它们
	ginkgo.By("creating pre-provisioned VolumeSnapshotContents and VolumeSnapshots owned by the VGS")

	for i, snapshotHandle := range volumeSnapshotHandles {
		// 为预制备的 VolumeSnapshotContent 和 VolumeSnapshot 生成唯一名称，
		// 使用 VGS UUID + 索引，使名称可预测且与父 VGS 关联
		snapName := fmt.Sprintf("pre-provisioned-vs-%s-%d", string(vgsUUID), i)
		snapContentName := fmt.Sprintf("pre-provisioned-vsc-%s-%d", string(vgsUUID), i)

		// 先创建预制备的 VolumeSnapshotContent
		// 注意：预制备的 VolumeSnapshotContent 不需要 volumeSnapshotClassName
		vsc := getPreProvisionedVolumeSnapshotContent(snapContentName, snapName, pvcNamespace, snapshotHandle, r.Pattern.SnapshotDeletionPolicy.String(), csiDriverName)
		_, err := dc.Resource(utils.SnapshotContentGVR).Create(ctx, vsc, metav1.CreateOptions{})
		framework.ExpectNoError(err, "failed to create pre-provisioned VolumeSnapshotContent %s", snapContentName)

		// 获取该快照句柄对应的源 PVC 名称映射
		sourcePVCName := r.SnapshotHandleToPVCName[snapshotHandle]
		gomega.Expect(sourcePVCName).NotTo(gomega.BeEmpty(), "no source PVC name mapping found for snapshot handle %s", snapshotHandle)
		framework.Logf("Creating pre-provisioned snapshot %s for PVC %s (handle: %s)", snapName, sourcePVCName, snapshotHandle)

		// 创建预制备的 VolumeSnapshot
		preProvisionedVS := &unstructured.Unstructured{
			Object: map[string]interface{}{
				"kind":       "VolumeSnapshot",
				"apiVersion": utils.SnapshotAPIVersion,
				"metadata": map[string]interface{}{
					"name":      snapName,
					"namespace": pvcNamespace,
					"ownerReferences": []interface{}{
						map[string]interface{}{
							"apiVersion": vgsAPIVersion,
							"kind":       vgsKind,
							"name":       r.VGS.GetName(),
							"uid":        newVgsUID,
						},
					},
				},
				"spec": map[string]interface{}{
					"source": map[string]interface{}{
						"volumeSnapshotContentName": snapContentName,
					},
				},
			},
		}

		_, err = dc.Resource(utils.SnapshotGVR).Namespace(pvcNamespace).Create(ctx, preProvisionedVS, metav1.CreateOptions{})
		framework.ExpectNoError(err, "failed to create pre-provisioned VolumeSnapshot %s/%s", pvcNamespace, snapName)

		// 等待 VolumeSnapshot 就绪
		err = utils.WaitForSnapshotReady(ctx, dc, pvcNamespace, snapName, framework.Poll, timeouts.SnapshotCreate)
		framework.ExpectNoError(err, "pre-provisioned VolumeSnapshot %s/%s did not become ready within timeout", pvcNamespace, snapName)

		framework.Logf("Created pre-provisioned VolumeSnapshotContent %s and VolumeSnapshot %s (%d/%d)", snapContentName, snapName, i+1, len(volumeSnapshotHandles))
	}

	ginkgo.By("getting the group snapshot and group snapshot content")
	r.VGS, err = dc.Resource(utils.VolumeGroupSnapshotGVR).Namespace(r.VGS.GetNamespace()).Get(ctx, r.VGS.GetName(), metav1.GetOptions{})
	framework.ExpectNoError(err, "failed to get final VolumeGroupSnapshot %s/%s", r.VGS.GetNamespace(), r.VGS.GetName())

	r.VGSContent, err = dc.Resource(utils.VolumeGroupSnapshotContentGVR).Get(ctx, r.VGSContent.GetName(), metav1.GetOptions{})
	framework.ExpectNoError(err, "failed to get final VolumeGroupSnapshotContent %s", r.VGSContent.GetName())
}

func getPreProvisionedVolumeGroupSnapshot(vgsName, ns, vgscName string) *unstructured.Unstructured {
	snapshot := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"kind":       "VolumeGroupSnapshot",
			"apiVersion": utils.VolumeGroupSnapshotAPIVersion,
			"metadata": map[string]interface{}{
				"name":      vgsName,
				"namespace": ns,
			},
			"spec": map[string]interface{}{
				"source": map[string]interface{}{
					"volumeGroupSnapshotContentName": vgscName,
				},
			},
		},
	}

	return snapshot
}

func getPreProvisionedVolumeGroupSnapshotName(uuid types.UID) string {
	return fmt.Sprintf("pre-provisioned-vgs-%s", string(uuid))
}

func getPreProvisionedVolumeGroupSnapshotContentName(uuid types.UID) string {
	return fmt.Sprintf("pre-provisioned-vgsc-%s", string(uuid))
}

func getPreProvisionedVolumeGroupSnapshotContent(vgscName, vgsName, vgsNamespace, groupSnapshotHandle string, volumeSnapshotHandles []string, deletionPolicy, csiDriverName string) *unstructured.Unstructured {
	snapshotContent := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"kind":       "VolumeGroupSnapshotContent",
			"apiVersion": utils.VolumeGroupSnapshotAPIVersion,
			"metadata": map[string]interface{}{
				"name": vgscName,
			},
			"spec": map[string]interface{}{
				"source": map[string]interface{}{
					"groupSnapshotHandles": map[string]interface{}{
						"volumeGroupSnapshotHandle": groupSnapshotHandle,
						"volumeSnapshotHandles":     volumeSnapshotHandles,
					},
				},
				"volumeGroupSnapshotRef": map[string]interface{}{
					"name":      vgsName,
					"namespace": vgsNamespace,
				},
				"driver":         csiDriverName,
				"deletionPolicy": deletionPolicy,
			},
		},
	}

	return snapshotContent
}

func getPreProvisionedVolumeSnapshotContent(snapcontentName, snapshotName, snapshotNamespace, snapshotHandle, deletionPolicy, csiDriverName string) *unstructured.Unstructured {
	snapshotContent := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"kind":       "VolumeSnapshotContent",
			"apiVersion": utils.SnapshotAPIVersion,
			"metadata": map[string]interface{}{
				"name": snapcontentName,
			},
			"spec": map[string]interface{}{
				"source": map[string]interface{}{
					"snapshotHandle": snapshotHandle,
				},
				"volumeSnapshotRef": map[string]interface{}{
					"name":      snapshotName,
					"namespace": snapshotNamespace,
				},
				"driver":         csiDriverName,
				"deletionPolicy": deletionPolicy,
			},
		},
	}

	return snapshotContent
}
