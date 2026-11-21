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

package persistentvolume

import (
	"errors"
	"testing"

	v1 "k8s.io/api/core/v1"
	storage "k8s.io/api/storage/v1"
	"k8s.io/component-helpers/storage/volume"
	"k8s.io/klog/v2/ktesting"
	pvtesting "k8s.io/kubernetes/pkg/controller/volume/persistentvolume/testing"
)

// TestDeleteSync 测试单次调用 syncVolume，预期触发删除回收流程。
// 1. 用初始数据填充控制器。
// 2. 只调用一次 syncVolume。
// 3. 将最终得到的卷与预期卷进行比较。
func TestDeleteSync(t *testing.T) {
	const gceDriver = "pd.csi.storage.gke.io"
	_, ctx := ktesting.NewTestContext(t)
	tests := []controllerTest{
		{
			// 删除由控制器绑定的卷
			name:            "8-1 - successful delete",
			initialVolumes:  newVolumeArray("volume8-1", "1Gi", "uid8-1", "claim8-1", v1.VolumeBound, v1.PersistentVolumeReclaimDelete, classEmpty, volume.AnnBoundByController),
			expectedVolumes: novolumes,
			initialClaims:   noclaims,
			expectedClaims:  noclaims,
			expectedEvents:  noevents,
			errors:          noerrors,
			// 向控制器注入删除器并调用 syncVolume；删除器模拟一次成功的 delete() 调用。
			test: wrapTestWithReclaimCalls(operationDelete, []error{nil}, testSyncVolume),
		},
		{
			// 删除由用户预绑定的卷
			name:            "8-2 - successful delete with prebound volume",
			initialVolumes:  newVolumeArray("volume8-2", "1Gi", "uid8-2", "claim8-2", v1.VolumeBound, v1.PersistentVolumeReclaimDelete, classEmpty),
			expectedVolumes: novolumes,
			initialClaims:   noclaims,
			expectedClaims:  noclaims,
			expectedEvents:  noevents,
			errors:          noerrors,
			// 向控制器注入删除器并调用 syncVolume；删除器模拟一次成功的 delete() 调用。
			test: wrapTestWithReclaimCalls(operationDelete, []error{nil}, testSyncVolume),
		},
		{
			// 删除失败：找不到匹配的卷插件
			name:            "8-3 - plugin not found",
			initialVolumes:  newVolumeArray("volume8-3", "1Gi", "uid8-3", "claim8-3", v1.VolumeBound, v1.PersistentVolumeReclaimDelete, classEmpty),
			expectedVolumes: withMessage("error getting deleter volume plugin for volume \"volume8-3\": no volume plugin matched", newVolumeArray("volume8-3", "1Gi", "uid8-3", "claim8-3", v1.VolumeFailed, v1.PersistentVolumeReclaimDelete, classEmpty)),
			initialClaims:   noclaims,
			expectedClaims:  noclaims,
			expectedEvents:  []string{"Warning VolumeFailedDelete"},
			errors:          noerrors,
			test:            testSyncVolume,
		},
		{
			// 删除失败：newDeleter 返回错误
			name:            "8-4 - newDeleter returns error",
			initialVolumes:  newVolumeArray("volume8-4", "1Gi", "uid8-4", "claim8-4", v1.VolumeBound, v1.PersistentVolumeReclaimDelete, classEmpty),
			expectedVolumes: withMessage("failed to create deleter for volume \"volume8-4\": Mock plugin error: no deleteCalls configured", newVolumeArray("volume8-4", "1Gi", "uid8-4", "claim8-4", v1.VolumeFailed, v1.PersistentVolumeReclaimDelete, classEmpty)),
			initialClaims:   noclaims,
			expectedClaims:  noclaims,
			expectedEvents:  []string{"Warning VolumeFailedDelete"},
			errors:          noerrors,
			test:            wrapTestWithReclaimCalls(operationDelete, []error{}, testSyncVolume),
		},
		{
			// 删除失败：delete() 返回错误
			name:            "8-5 - delete returns error",
			initialVolumes:  newVolumeArray("volume8-5", "1Gi", "uid8-5", "claim8-5", v1.VolumeBound, v1.PersistentVolumeReclaimDelete, classEmpty),
			expectedVolumes: withMessage("Mock delete error", newVolumeArray("volume8-5", "1Gi", "uid8-5", "claim8-5", v1.VolumeFailed, v1.PersistentVolumeReclaimDelete, classEmpty)),
			initialClaims:   noclaims,
			expectedClaims:  noclaims,
			expectedEvents:  []string{"Warning VolumeFailedDelete"},
			errors:          noerrors,
			test:            wrapTestWithReclaimCalls(operationDelete, []error{errors.New("Mock delete error")}, testSyncVolume),
		},
		{
			// 删除“成功”：doDelete() 开始前卷已被删除
			name:            "8-6 - volume is deleted before deleting",
			initialVolumes:  newVolumeArray("volume8-6", "1Gi", "uid8-6", "claim8-6", v1.VolumeBound, v1.PersistentVolumeReclaimDelete, classEmpty),
			expectedVolumes: novolumes,
			initialClaims:   noclaims,
			expectedClaims:  noclaims,
			expectedEvents:  noevents,
			errors:          noerrors,
			test: wrapTestWithInjectedOperation(ctx, wrapTestWithReclaimCalls(operationDelete, []error{}, testSyncVolume), func(ctrl *PersistentVolumeController, reactor *pvtesting.VolumeReactor) {
				// 在删除操作开始前先移除卷，验证同步时能处理卷已消失的情况。
				reactor.DeleteVolume("volume8-6")
			}),
		},
		{
			// 删除"成功"：恰在 doDelete() 开始时卷被重新绑定，模拟"卷已不再需要回收，跳过删除"的场景。
			name:            "8-7 - volume is bound before deleting",
			initialVolumes:  newVolumeArray("volume8-7", "1Gi", "uid8-7", "claim8-7", v1.VolumeBound, v1.PersistentVolumeReclaimDelete, classEmpty, volume.AnnBoundByController),
			expectedVolumes: newVolumeArray("volume8-7", "1Gi", "uid8-7", "claim8-7", v1.VolumeBound, v1.PersistentVolumeReclaimDelete, classEmpty, volume.AnnBoundByController),
			initialClaims:   noclaims,
			expectedClaims:  newClaimArray("claim8-7", "uid8-7", "10Gi", "volume8-7", v1.ClaimBound, nil),
			expectedEvents:  noevents,
			errors:          noerrors,
			test: wrapTestWithInjectedOperation(ctx, wrapTestWithReclaimCalls(operationDelete, []error{}, testSyncVolume), func(ctrl *PersistentVolumeController, reactor *pvtesting.VolumeReactor) {
				// 将卷绑定到"复活"的 PVC（现实中不应发生），验证同步时的竞争处理。
				claim := newClaim("claim8-7", "uid8-7", "10Gi", "volume8-7", v1.ClaimBound, nil)
				reactor.AddClaimBoundToVolume(claim)
				ctrl.claims.Add(claim)
			}),
		},
		{
			// 删除成功：用户预绑定的卷被删除，同时存在 UID 不同的新 PVC。
			name:            "8-9 - prebound volume is deleted while the claim exists",
			initialVolumes:  newVolumeArray("volume8-9", "1Gi", "uid8-9", "claim8-9", v1.VolumeBound, v1.PersistentVolumeReclaimDelete, classEmpty),
			expectedVolumes: novolumes,
			initialClaims:   newClaimArray("claim8-9", "uid8-9-x", "10Gi", "", v1.ClaimPending, nil),
			expectedClaims:  newClaimArray("claim8-9", "uid8-9-x", "10Gi", "", v1.ClaimPending, nil),
			expectedEvents:  noevents,
			errors:          noerrors,
			// 向控制器注入删除器并调用 syncVolume；删除器模拟一次成功的 delete() 调用。
			test: wrapTestWithReclaimCalls(operationDelete, []error{nil}, testSyncVolume),
		},
		{
			// PV 需要外部删除器
			name:            "8-10-1 - external deleter when volume is dynamic provisioning",
			initialVolumes:  []*v1.PersistentVolume{newExternalProvisionedVolume("volume8-10-1", "1Gi", "uid10-1-1", "claim10-1-1", v1.VolumeBound, v1.PersistentVolumeReclaimDelete, classEmpty, gceDriver, nil, volume.AnnBoundByController)},
			expectedVolumes: []*v1.PersistentVolume{newExternalProvisionedVolume("volume8-10-1", "1Gi", "uid10-1-1", "claim10-1-1", v1.VolumeReleased, v1.PersistentVolumeReclaimDelete, classEmpty, gceDriver, nil, volume.AnnBoundByController)},
			initialClaims:   noclaims,
			expectedClaims:  noclaims,
			expectedEvents:  noevents,
			errors:          noerrors,
			test: func(ctrl *PersistentVolumeController, reactor *pvtesting.VolumeReactor, test controllerTest) error {
				// 注入外部制备器注解，使控制器走外部删除路径，PV 应被标记为 Released。
				test.initialVolumes[0].Annotations[volume.AnnDynamicallyProvisioned] = "external.io/test"
				test.expectedVolumes[0].Annotations[volume.AnnDynamicallyProvisioned] = "external.io/test"
				return testSyncVolume(ctrl, reactor, test)
			},
		},
		{
			// PV 需要外部删除器
			name:            "8-10-2 - external deleter when volume is static provisioning",
			initialVolumes:  []*v1.PersistentVolume{newExternalProvisionedVolume("volume8-10-2", "1Gi", "uid10-1-2", "claim10-1-2", v1.VolumeBound, v1.PersistentVolumeReclaimDelete, classEmpty, gceDriver, nil, volume.AnnBoundByController)},
			expectedVolumes: []*v1.PersistentVolume{newExternalProvisionedVolume("volume8-10-2", "1Gi", "uid10-1-2", "claim10-1-2", v1.VolumeReleased, v1.PersistentVolumeReclaimDelete, classEmpty, gceDriver, nil, volume.AnnBoundByController)},
			initialClaims:   noclaims,
			expectedClaims:  noclaims,
			expectedEvents:  noevents,
			errors:          noerrors,
			test:            testSyncVolume,
		},
		{
			// PV 需要外部删除器
			name:            "8-10-3 - external deleter when volume is migrated",
			initialVolumes:  []*v1.PersistentVolume{volumeWithAnnotation(volume.AnnMigratedTo, "pd.csi.storage.gke.io", volumeWithAnnotation(volume.AnnDynamicallyProvisioned, "kubernetes.io/gce-pd", newVolume("volume8-10-3", "1Gi", "uid10-1-3", "claim10-1-3", v1.VolumeBound, v1.PersistentVolumeReclaimDelete, classEmpty, volume.AnnDynamicallyProvisioned)))},
			expectedVolumes: []*v1.PersistentVolume{volumeWithAnnotation(volume.AnnMigratedTo, "pd.csi.storage.gke.io", volumeWithAnnotation(volume.AnnDynamicallyProvisioned, "kubernetes.io/gce-pd", newVolume("volume8-10-3", "1Gi", "uid10-1-3", "claim10-1-3", v1.VolumeReleased, v1.PersistentVolumeReclaimDelete, classEmpty, volume.AnnDynamicallyProvisioned)))},
			initialClaims:   noclaims,
			expectedClaims:  noclaims,
			expectedEvents:  noevents,
			errors:          noerrors,
			test:            testSyncVolume,
		},
		{
			// 删除成功：同一个 PVC 被制备了两个 PV，其中一个会被删除。
			name: "8-11 - two PVs provisioned for a single claim",
			initialVolumes: []*v1.PersistentVolume{
				newVolume("volume8-11-1", "1Gi", "uid8-11", "claim8-11", v1.VolumeBound, v1.PersistentVolumeReclaimDelete, classEmpty, volume.AnnDynamicallyProvisioned),
				newVolume("volume8-11-2", "1Gi", "uid8-11", "claim8-11", v1.VolumeBound, v1.PersistentVolumeReclaimDelete, classEmpty, volume.AnnDynamicallyProvisioned),
			},
			expectedVolumes: []*v1.PersistentVolume{
				newVolume("volume8-11-2", "1Gi", "uid8-11", "claim8-11", v1.VolumeBound, v1.PersistentVolumeReclaimDelete, classEmpty, volume.AnnDynamicallyProvisioned),
			},
			// PVC 绑定到了 volume8-11-2，volume8-11-1 在竞争中落败，应被删除
			initialClaims:  newClaimArray("claim8-11", "uid8-11", "10Gi", "volume8-11-2", v1.ClaimBound, nil),
			expectedClaims: newClaimArray("claim8-11", "uid8-11", "10Gi", "volume8-11-2", v1.ClaimBound, nil),
			expectedEvents: noevents,
			errors:         noerrors,
			// 向控制器注入删除器并调用 syncVolume；删除器模拟一次成功的 delete() 调用。
			test: wrapTestWithReclaimCalls(operationDelete, []error{nil}, testSyncVolume),
		},
		{
			// 删除成功：同一个 PVC 被外部制备了两个 PV，其中一个被标记为 Released，交由外部制备器删除。
			name: "8-12 - two PVs externally provisioned for a single claim",
			initialVolumes: []*v1.PersistentVolume{
				newExternalProvisionedVolume("volume8-12-1", "1Gi", "uid8-12", "claim8-12", v1.VolumeBound, v1.PersistentVolumeReclaimDelete, classEmpty, gceDriver, nil, volume.AnnDynamicallyProvisioned),
				newExternalProvisionedVolume("volume8-12-2", "1Gi", "uid8-12", "claim8-12", v1.VolumeBound, v1.PersistentVolumeReclaimDelete, classEmpty, gceDriver, nil, volume.AnnDynamicallyProvisioned),
			},
			expectedVolumes: []*v1.PersistentVolume{
				newExternalProvisionedVolume("volume8-12-1", "1Gi", "uid8-12", "claim8-12", v1.VolumeReleased, v1.PersistentVolumeReclaimDelete, classEmpty, gceDriver, nil, volume.AnnDynamicallyProvisioned),
				newExternalProvisionedVolume("volume8-12-2", "1Gi", "uid8-12", "claim8-12", v1.VolumeBound, v1.PersistentVolumeReclaimDelete, classEmpty, gceDriver, nil, volume.AnnDynamicallyProvisioned),
			},
			// PVC 绑定到了 volume8-12-2，volume8-12-1 在竞争中落败，应被标记为 "Released"
			initialClaims:  newClaimArray("claim8-12", "uid8-12", "10Gi", "volume8-12-2", v1.ClaimBound, nil),
			expectedClaims: newClaimArray("claim8-12", "uid8-12", "10Gi", "volume8-12-2", v1.ClaimBound, nil),
			expectedEvents: noevents,
			errors:         noerrors,
			test: func(ctrl *PersistentVolumeController, reactor *pvtesting.VolumeReactor, test controllerTest) error {
				// 注入外部制备器注解，使控制器走外部删除路径，PV 应被标记为 Released。
				test.initialVolumes[0].Annotations[volume.AnnDynamicallyProvisioned] = "external.io/test"
				test.expectedVolumes[0].Annotations[volume.AnnDynamicallyProvisioned] = "external.io/test"
				return testSyncVolume(ctrl, reactor, test)
			},
		},
		{
			// 删除成功：doDelete() 开始前卷已带有删除时间戳
			name:            "8-13 - volume has deletion timestamp and processed",
			initialVolumes:  volumesWithFinalizers(withVolumeDeletionTimestamp(newVolumeArray("volume8-13", "1Gi", "uid8-13", "claim8-13", v1.VolumeBound, v1.PersistentVolumeReclaimDelete, classEmpty, volume.AnnBoundByController)), []string{volume.PVDeletionInTreeProtectionFinalizer}),
			expectedVolumes: novolumes,
			initialClaims:   noclaims,
			expectedClaims:  noclaims,
			expectedEvents:  noevents,
			errors:          noerrors,
			test:            wrapTestWithReclaimCalls(operationDelete, []error{nil}, testSyncVolume),
		},
	}
	runSyncTests(t, ctx, tests, []*storage.StorageClass{}, []*v1.Pod{})
}

// TestDeleteMultiSync 测试多次调用 syncClaim/syncVolume，以及对全部卷/PVC 的周期性同步。
// 测试遵循如下模式：
//  0. 用初始数据加载控制器。
//  1. 像 TestSync() 一样调用一次 controllerTest.testCall()。
//  2. 对上一次 syncVolume/syncClaim 调用改变过的所有卷/PVC，调用相应的
//     syncVolume/syncClaim（模拟"卷/PVC 发生变化"事件）；若又产生了变化，则回到步骤 2.。
//  3. 当所有变化都处理完且没有新变化时，对所有卷/PVC 调用 syncVolume/syncClaim
//     （模拟"周期性同步"）。
//  4. 若步骤 3. 产生了变化，回到步骤 2.（模拟"卷/PVC 更新"事件，最终再次执行步骤 3.）。
//  5. 当步骤 3. 不再产生任何变化时结束测试，比较最终的卷/PVC 集合与预期值并报告差异。
//
// 强制限制调用次数，防止陷入死循环。
func TestDeleteMultiSync(t *testing.T) {
	tests := []controllerTest{
		{
			// 删除失败：delete() 返回错误，控制器应在下一次同步时重试。
			name:            "9-1 - delete returns error",
			initialVolumes:  volumesWithFinalizers(newVolumeArray("volume9-1", "1Gi", "uid9-1", "claim9-1", v1.VolumeBound, v1.PersistentVolumeReclaimDelete, classEmpty), []string{volume.PVDeletionInTreeProtectionFinalizer}),
			expectedVolumes: novolumes,
			initialClaims:   noclaims,
			expectedClaims:  noclaims,
			expectedEvents:  []string{"Warning VolumeFailedDelete"},
			errors:          noerrors,
			// 第一次删除失败、第二次成功，验证重试后最终能完成删除。
			test: wrapTestWithReclaimCalls(operationDelete, []error{errors.New("Mock delete error"), nil}, testSyncVolume),
		},
	}
	_, ctx := ktesting.NewTestContext(t)
	runMultisyncTests(t, ctx, tests, []*storage.StorageClass{}, "")
}
