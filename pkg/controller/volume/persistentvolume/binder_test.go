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
	"fmt"
	"testing"

	v1 "k8s.io/api/core/v1"
	storage "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/version"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	featuregatetesting "k8s.io/component-base/featuregate/testing"
	"k8s.io/component-helpers/storage/volume"
	"k8s.io/klog/v2/ktesting"
	"k8s.io/kubernetes/pkg/features"
)

// TestSync 单次调用 syncClaim 与 syncVolume 方法的测试。
//  1. 用初始数据填充控制器。
//  2. 通过 controllerTest.testCall 调用被测函数（syncClaim/syncVolume）一次。
//  3. 将生成的卷和声明与期望值比较。
func TestSync(t *testing.T) {
	labels := map[string]string{
		"foo": "true",
		"bar": "false",
	}

	tests := []controllerTest{
		// [单元测试集 1] 用户不关心获得哪个 PV。
		// 测试 claim.Spec.VolumeName 为空时与各种卷的匹配。
		{
			// syncClaim 将声明绑定到匹配的未绑定卷。
			name:            "1-1 - successful bind",
			initialVolumes:  newVolumeArray("volume1-1", "1Gi", "", "", v1.VolumeAvailable, v1.PersistentVolumeReclaimRetain, classEmpty),
			expectedVolumes: newVolumeArray("volume1-1", "1Gi", "uid1-1", "claim1-1", v1.VolumeBound, v1.PersistentVolumeReclaimRetain, classEmpty, volume.AnnBoundByController),
			initialClaims:   newClaimArray("claim1-1", "uid1-1", "1Gi", "", v1.ClaimPending, nil),
			expectedClaims:  newClaimArray("claim1-1", "uid1-1", "1Gi", "volume1-1", v1.ClaimBound, nil, volume.AnnBoundByController, volume.AnnBindCompleted),
			expectedEvents:  noevents,
			errors:          noerrors,
			test:            testSyncClaim,
		},
		{
			// 没有匹配的卷时 syncClaim 不做任何操作。
			name:            "1-2 - noop",
			initialVolumes:  newVolumeArray("volume1-2", "1Gi", "", "", v1.VolumeAvailable, v1.PersistentVolumeReclaimRetain, classEmpty),
			expectedVolumes: newVolumeArray("volume1-2", "1Gi", "", "", v1.VolumeAvailable, v1.PersistentVolumeReclaimRetain, classEmpty),
			initialClaims:   newClaimArray("claim1-2", "uid1-2", "10Gi", "", v1.ClaimPending, nil),
			expectedClaims:  newClaimArray("claim1-2", "uid1-2", "10Gi", "", v1.ClaimPending, nil),
			expectedEvents:  []string{"Normal FailedBinding"},
			errors:          noerrors,
			test:            testSyncClaim,
		},
		{
			// 没有匹配的卷时 syncClaim 将 claim.Status 重置为 Pending。
			name:            "1-3 - reset to Pending",
			initialVolumes:  newVolumeArray("volume1-3", "1Gi", "", "", v1.VolumeAvailable, v1.PersistentVolumeReclaimRetain, classEmpty),
			expectedVolumes: newVolumeArray("volume1-3", "1Gi", "", "", v1.VolumeAvailable, v1.PersistentVolumeReclaimRetain, classEmpty),
			initialClaims:   newClaimArray("claim1-3", "uid1-3", "10Gi", "", v1.ClaimBound, nil),
			expectedClaims:  newClaimArray("claim1-3", "uid1-3", "10Gi", "", v1.ClaimPending, nil),
			expectedEvents:  []string{"Normal FailedBinding"},
			errors:          noerrors,
			test:            testSyncClaim,
		},
		{
			// syncClaim 将声明绑定到最小的匹配卷。
			name: "1-4 - smallest volume",
			initialVolumes: []*v1.PersistentVolume{
				newVolume("volume1-4_1", "10Gi", "", "", v1.VolumeAvailable, v1.PersistentVolumeReclaimRetain, classEmpty),
				newVolume("volume1-4_2", "1Gi", "", "", v1.VolumeAvailable, v1.PersistentVolumeReclaimRetain, classEmpty),
			},
			expectedVolumes: []*v1.PersistentVolume{
				newVolume("volume1-4_1", "10Gi", "", "", v1.VolumeAvailable, v1.PersistentVolumeReclaimRetain, classEmpty),
				newVolume("volume1-4_2", "1Gi", "uid1-4", "claim1-4", v1.VolumeBound, v1.PersistentVolumeReclaimRetain, classEmpty, volume.AnnBoundByController),
			},
			initialClaims:  newClaimArray("claim1-4", "uid1-4", "1Gi", "", v1.ClaimPending, nil),
			expectedClaims: newClaimArray("claim1-4", "uid1-4", "1Gi", "volume1-4_2", v1.ClaimBound, nil, volume.AnnBoundByController, volume.AnnBindCompleted),
			expectedEvents: noevents,
			errors:         noerrors,
			test:           testSyncClaim,
		},
		{
			// 即使存在更小的卷，syncClaim 也只将声明绑定到按名称预先指向它的卷。
			name: "1-5 - prebound volume by name - success",
			initialVolumes: []*v1.PersistentVolume{
				newVolume("volume1-5_1", "10Gi", "", "claim1-5", v1.VolumeAvailable, v1.PersistentVolumeReclaimRetain, classEmpty),
				newVolume("volume1-5_2", "1Gi", "", "", v1.VolumeAvailable, v1.PersistentVolumeReclaimRetain, classEmpty),
			},
			expectedVolumes: []*v1.PersistentVolume{
				newVolume("volume1-5_1", "10Gi", "uid1-5", "claim1-5", v1.VolumeBound, v1.PersistentVolumeReclaimRetain, classEmpty),
				newVolume("volume1-5_2", "1Gi", "", "", v1.VolumeAvailable, v1.PersistentVolumeReclaimRetain, classEmpty),
			},
			initialClaims:  newClaimArray("claim1-5", "uid1-5", "1Gi", "", v1.ClaimPending, nil),
			expectedClaims: withExpectedCapacity("10Gi", newClaimArray("claim1-5", "uid1-5", "1Gi", "volume1-5_1", v1.ClaimBound, nil, volume.AnnBoundByController, volume.AnnBindCompleted)),
			expectedEvents: noevents,
			errors:         noerrors,
			test:           testSyncClaim,
		},
		{
			// 即使存在更小的卷，syncClaim 也只将声明绑定到按名称预先指向它的卷。
			name: "1-6 - prebound volume by UID - success",
			initialVolumes: []*v1.PersistentVolume{
				newVolume("volume1-6_1", "10Gi", "uid1-6", "claim1-6", v1.VolumeAvailable, v1.PersistentVolumeReclaimRetain, classEmpty),
				newVolume("volume1-6_2", "1Gi", "", "", v1.VolumeAvailable, v1.PersistentVolumeReclaimRetain, classEmpty),
			},
			expectedVolumes: []*v1.PersistentVolume{
				newVolume("volume1-6_1", "10Gi", "uid1-6", "claim1-6", v1.VolumeBound, v1.PersistentVolumeReclaimRetain, classEmpty),
				newVolume("volume1-6_2", "1Gi", "", "", v1.VolumeAvailable, v1.PersistentVolumeReclaimRetain, classEmpty),
			},
			initialClaims:  newClaimArray("claim1-6", "uid1-6", "1Gi", "", v1.ClaimPending, nil),
			expectedClaims: withExpectedCapacity("10Gi", newClaimArray("claim1-6", "uid1-6", "1Gi", "volume1-6_1", v1.ClaimBound, nil, volume.AnnBoundByController, volume.AnnBindCompleted)),
			expectedEvents: noevents,
			errors:         noerrors,
			test:           testSyncClaim,
		},
		{
			// 卷预先绑定到同名但 UID 不同的声明时，syncClaim 不进行绑定。
			name:            "1-7 - prebound volume to different claim",
			initialVolumes:  newVolumeArray("volume1-7", "10Gi", "uid1-777", "claim1-7", v1.VolumeAvailable, v1.PersistentVolumeReclaimRetain, classEmpty),
			expectedVolumes: newVolumeArray("volume1-7", "10Gi", "uid1-777", "claim1-7", v1.VolumeAvailable, v1.PersistentVolumeReclaimRetain, classEmpty),
			initialClaims:   newClaimArray("claim1-7", "uid1-7", "1Gi", "", v1.ClaimPending, nil),
			expectedClaims:  newClaimArray("claim1-7", "uid1-7", "1Gi", "", v1.ClaimPending, nil),
			expectedEvents:  []string{"Normal FailedBinding"},
			errors:          noerrors,
			test:            testSyncClaim,
		},
		{
			// syncClaim 完成绑定的后续步骤——模拟控制器在保存 PV.ClaimRef 后崩溃。
			name:            "1-8 - complete bind after crash - PV bound",
			initialVolumes:  newVolumeArray("volume1-8", "1Gi", "uid1-8", "claim1-8", v1.VolumeAvailable, v1.PersistentVolumeReclaimRetain, classEmpty, volume.AnnBoundByController),
			expectedVolumes: newVolumeArray("volume1-8", "1Gi", "uid1-8", "claim1-8", v1.VolumeBound, v1.PersistentVolumeReclaimRetain, classEmpty, volume.AnnBoundByController),
			initialClaims:   newClaimArray("claim1-8", "uid1-8", "1Gi", "", v1.ClaimPending, nil),
			expectedClaims:  newClaimArray("claim1-8", "uid1-8", "1Gi", "volume1-8", v1.ClaimBound, nil, volume.AnnBoundByController, volume.AnnBindCompleted),
			expectedEvents:  noevents,
			errors:          noerrors,
			test:            testSyncClaim,
		},
		{
			// syncClaim 完成绑定的后续步骤——模拟控制器在保存 PV.ClaimRef 后崩溃。
			// 模拟控制器在保存 PV.Status 后崩溃。
			name:            "1-9 - complete bind after crash - PV status saved",
			initialVolumes:  newVolumeArray("volume1-9", "1Gi", "uid1-9", "claim1-9", v1.VolumeBound, v1.PersistentVolumeReclaimRetain, classEmpty, volume.AnnBoundByController),
			expectedVolumes: newVolumeArray("volume1-9", "1Gi", "uid1-9", "claim1-9", v1.VolumeBound, v1.PersistentVolumeReclaimRetain, classEmpty, volume.AnnBoundByController),
			initialClaims:   newClaimArray("claim1-9", "uid1-9", "1Gi", "", v1.ClaimPending, nil),
			expectedClaims:  newClaimArray("claim1-9", "uid1-9", "1Gi", "volume1-9", v1.ClaimBound, nil, volume.AnnBoundByController, volume.AnnBindCompleted),
			expectedEvents:  noevents,
			errors:          noerrors,
			test:            testSyncClaim,
		},
		{
			// syncClaim 完成绑定的后续步骤——模拟控制器在保存 PV.ClaimRef 后崩溃。
			// 模拟控制器在保存 PVC.VolumeName 后崩溃。
			name:            "1-10 - complete bind after crash - PVC bound",
			initialVolumes:  newVolumeArray("volume1-10", "1Gi", "uid1-10", "claim1-10", v1.VolumeBound, v1.PersistentVolumeReclaimRetain, classEmpty, volume.AnnBoundByController),
			expectedVolumes: newVolumeArray("volume1-10", "1Gi", "uid1-10", "claim1-10", v1.VolumeBound, v1.PersistentVolumeReclaimRetain, classEmpty, volume.AnnBoundByController),
			initialClaims:   newClaimArray("claim1-10", "uid1-10", "1Gi", "volume1-10", v1.ClaimPending, nil, volume.AnnBoundByController, volume.AnnBindCompleted),
			expectedClaims:  newClaimArray("claim1-10", "uid1-10", "1Gi", "volume1-10", v1.ClaimBound, nil, volume.AnnBoundByController, volume.AnnBindCompleted),
			expectedEvents:  noevents,
			errors:          noerrors,
			test:            testSyncClaim,
		},
		{
			// 仅当标签选择器与卷匹配时 syncClaim 才绑定声明。
			name:            "1-11 - bind when selector matches",
			initialVolumes:  withLabels(labels, newVolumeArray("volume1-1", "1Gi", "", "", v1.VolumeAvailable, v1.PersistentVolumeReclaimRetain, classEmpty)),
			expectedVolumes: withLabels(labels, newVolumeArray("volume1-1", "1Gi", "uid1-1", "claim1-1", v1.VolumeBound, v1.PersistentVolumeReclaimRetain, classEmpty, volume.AnnBoundByController)),
			initialClaims:   withLabelSelector(labels, newClaimArray("claim1-1", "uid1-1", "1Gi", "", v1.ClaimPending, nil)),
			expectedClaims:  withLabelSelector(labels, newClaimArray("claim1-1", "uid1-1", "1Gi", "volume1-1", v1.ClaimBound, nil, volume.AnnBoundByController, volume.AnnBindCompleted)),
			expectedEvents:  noevents,
			errors:          noerrors,
			test:            testSyncClaim,
		},
		{
			// 标签选择器不匹配时 syncClaim 不绑定声明。
			name:            "1-12 - do not bind when selector does not match",
			initialVolumes:  newVolumeArray("volume1-1", "1Gi", "", "", v1.VolumeAvailable, v1.PersistentVolumeReclaimRetain, classEmpty),
			expectedVolumes: newVolumeArray("volume1-1", "1Gi", "", "", v1.VolumeAvailable, v1.PersistentVolumeReclaimRetain, classEmpty),
			initialClaims:   withLabelSelector(labels, newClaimArray("claim1-1", "uid1-1", "1Gi", "", v1.ClaimPending, nil)),
			expectedClaims:  withLabelSelector(labels, newClaimArray("claim1-1", "uid1-1", "1Gi", "", v1.ClaimPending, nil)),
			expectedEvents:  []string{"Normal FailedBinding"},
			errors:          noerrors,
			test:            testSyncClaim,
		},
		{
			// 绑定被延迟时 syncClaim 不做任何操作。
			name:            "1-13 - delayed binding",
			initialVolumes:  newVolumeArray("volume1-1", "1Gi", "", "", v1.VolumeAvailable, v1.PersistentVolumeReclaimRetain, classWait),
			expectedVolumes: newVolumeArray("volume1-1", "1Gi", "", "", v1.VolumeAvailable, v1.PersistentVolumeReclaimRetain, classWait),
			initialClaims:   newClaimArray("claim1-1", "uid1-1", "1Gi", "", v1.ClaimPending, &classWait),
			expectedClaims:  newClaimArray("claim1-1", "uid1-1", "1Gi", "", v1.ClaimPending, &classWait),
			expectedEvents:  []string{"Normal WaitForFirstConsumer"},
			errors:          noerrors,
			test:            testSyncClaim,
		},
		{
			// 绑定被延迟但 PV 已预绑定到 PVC 时，syncClaim 仍执行绑定。
			name:            "1-14 - successful prebound PV",
			initialVolumes:  newVolumeArray("volume1-1", "1Gi", "", "claim1-1", v1.VolumeAvailable, v1.PersistentVolumeReclaimRetain, classWait),
			expectedVolumes: newVolumeArray("volume1-1", "1Gi", "uid1-1", "claim1-1", v1.VolumeBound, v1.PersistentVolumeReclaimRetain, classWait),
			initialClaims:   newClaimArray("claim1-1", "uid1-1", "1Gi", "", v1.ClaimPending, &classWait),
			expectedClaims:  newClaimArray("claim1-1", "uid1-1", "1Gi", "volume1-1", v1.ClaimBound, &classWait, volume.AnnBoundByController, volume.AnnBindCompleted),
			expectedEvents:  noevents,
			errors:          noerrors,
			test:            testSyncClaim,
		},
		{
			// 预绑定的 PVC 只绑定到其指向的卷，
			// 即使存在更小的卷也是如此。
			name: "1-15 - successful prebound PVC",
			initialVolumes: []*v1.PersistentVolume{
				newVolume("volume1-15_1", "10Gi", "", "", v1.VolumeAvailable, v1.PersistentVolumeReclaimRetain, classEmpty),
				newVolume("volume1-15_2", "1Gi", "", "", v1.VolumeAvailable, v1.PersistentVolumeReclaimRetain, classEmpty),
			},
			expectedVolumes: []*v1.PersistentVolume{
				newVolume("volume1-15_1", "10Gi", "uid1-15", "claim1-15", v1.VolumeBound, v1.PersistentVolumeReclaimRetain, classEmpty, volume.AnnBoundByController),
				newVolume("volume1-15_2", "1Gi", "", "", v1.VolumeAvailable, v1.PersistentVolumeReclaimRetain, classEmpty),
			},
			initialClaims:  newClaimArray("claim1-15", "uid1-15", "1Gi", "volume1-15_1", v1.ClaimPending, nil),
			expectedClaims: withExpectedCapacity("10Gi", newClaimArray("claim1-15", "uid1-15", "1Gi", "volume1-15_1", v1.ClaimBound, nil, volume.AnnBindCompleted)),
			expectedEvents: noevents,
			errors:         noerrors,
			test:           testSyncClaim,
		},
		{
			// syncClaim 不将预绑定 PVC 绑定到访问模式不同的 PV。
			name: "1-16 - successful prebound PVC",
			// PV 的访问模式为 ReadWriteOnce。
			initialVolumes:  newVolumeArray("volume1-16", "1Gi", "", "", v1.VolumeAvailable, v1.PersistentVolumeReclaimRetain, classEmpty),
			expectedVolumes: newVolumeArray("volume1-16", "1Gi", "", "", v1.VolumeAvailable, v1.PersistentVolumeReclaimRetain, classEmpty),
			initialClaims:   claimWithAccessMode([]v1.PersistentVolumeAccessMode{v1.ReadWriteMany}, newClaimArray("claim1-16", "uid1-16", "1Gi", "volume1-16", v1.ClaimPending, nil)),
			expectedClaims:  claimWithAccessMode([]v1.PersistentVolumeAccessMode{v1.ReadWriteMany}, newClaimArray("claim1-16", "uid1-16", "1Gi", "volume1-16", v1.ClaimPending, nil)),
			expectedEvents:  noevents,
			errors:          noerrors,
			test:            testSyncClaim,
		},
		{
			// 非预绑定场景下 syncClaim 不将 PVC 绑定到不可用的 PV。
			name: "1-17 - skip non-available PV if it's not pre-bind",
			initialVolumes: []*v1.PersistentVolume{
				newVolume("volume1-17-pending", "1Gi", "", "", v1.VolumePending, v1.PersistentVolumeReclaimRetain, classEmpty),
				newVolume("volume1-17-failed", "1Gi", "", "", v1.VolumeFailed, v1.PersistentVolumeReclaimRetain, classEmpty),
				newVolume("volume1-17-released", "1Gi", "", "", v1.VolumeReleased, v1.PersistentVolumeReclaimRetain, classEmpty),
				newVolume("volume1-17-empty", "1Gi", "", "", "", v1.PersistentVolumeReclaimRetain, classEmpty),
			},
			expectedVolumes: []*v1.PersistentVolume{
				newVolume("volume1-17-pending", "1Gi", "", "", v1.VolumePending, v1.PersistentVolumeReclaimRetain, classEmpty),
				newVolume("volume1-17-failed", "1Gi", "", "", v1.VolumeFailed, v1.PersistentVolumeReclaimRetain, classEmpty),
				newVolume("volume1-17-released", "1Gi", "", "", v1.VolumeReleased, v1.PersistentVolumeReclaimRetain, classEmpty),
				newVolume("volume1-17-empty", "1Gi", "", "", "", v1.PersistentVolumeReclaimRetain, classEmpty),
			},
			initialClaims: []*v1.PersistentVolumeClaim{
				newClaim("claim1-17", "uid1-17", "1Gi", "", v1.ClaimPending, nil),
			},
			expectedClaims: []*v1.PersistentVolumeClaim{
				newClaim("claim1-17", "uid1-17", "1Gi", "", v1.ClaimPending, nil),
			},
			expectedEvents: noevents,
			errors:         noerrors,
			test:           testSyncClaim,
		},
		{
			// syncClaim 处理已调度到指定节点的声明。
			name:            "1-18 - successful pre-bound PV to PVC provisioning",
			initialVolumes:  newVolumeArray("volume1-18", "1Gi", "uid1-18", "claim1-18", v1.VolumeAvailable, v1.PersistentVolumeReclaimRetain, classWait),
			expectedVolumes: newVolumeArray("volume1-18", "1Gi", "uid1-18", "claim1-18", v1.VolumeBound, v1.PersistentVolumeReclaimRetain, classWait),
			initialClaims: claimWithAnnotation(volume.AnnSelectedNode, "node1",
				newClaimArray("claim1-18", "uid1-18", "1Gi", "", v1.ClaimPending, &classWait)),
			expectedClaims: claimWithAnnotation(volume.AnnSelectedNode, "node1",
				newClaimArray("claim1-18", "uid1-18", "1Gi", "volume1-18", v1.ClaimBound, &classWait, volume.AnnBoundByController, volume.AnnBindCompleted)),
			expectedEvents: noevents,
			errors:         noerrors,
			test:           testSyncClaim,
		},

		// [单元测试集 2] 用户指定了特定的 PV。
		// 测试 pv.ClaimRef 已由控制器或用户设置时的绑定行为。
		{
			// syncClaim 处理声明预绑定到不存在的 PV 的场景。
			name:            "2-1 - claim prebound to non-existing volume - noop",
			initialVolumes:  novolumes,
			expectedVolumes: novolumes,
			initialClaims:   newClaimArray("claim2-1", "uid2-1", "10Gi", "volume2-1", v1.ClaimPending, nil),
			expectedClaims:  newClaimArray("claim2-1", "uid2-1", "10Gi", "volume2-1", v1.ClaimPending, nil),
			expectedEvents:  noevents,
			errors:          noerrors,
			test:            testSyncClaim,
		},
		{
			// syncClaim 处理声明预绑定到不存在的 PV 的场景。
			// 检查声明状态是否被重置为 Pending。
			name:            "2-2 - claim prebound to non-existing volume - reset status",
			initialVolumes:  novolumes,
			expectedVolumes: novolumes,
			initialClaims:   newClaimArray("claim2-2", "uid2-2", "10Gi", "volume2-2", v1.ClaimBound, nil),
			expectedClaims:  newClaimArray("claim2-2", "uid2-2", "10Gi", "volume2-2", v1.ClaimPending, nil),
			expectedEvents:  noevents,
			errors:          noerrors,
			test:            testSyncClaim,
		},
		{
			// syncClaim 处理声明预绑定到已存在且未绑定的 PV，
			// 检查其完成绑定且未设置 volume.AnnBoundByController 注解。
			name:            "2-3 - claim prebound to unbound volume",
			initialVolumes:  newVolumeArray("volume2-3", "1Gi", "", "", v1.VolumeAvailable, v1.PersistentVolumeReclaimRetain, classEmpty),
			expectedVolumes: newVolumeArray("volume2-3", "1Gi", "uid2-3", "claim2-3", v1.VolumeBound, v1.PersistentVolumeReclaimRetain, classEmpty, volume.AnnBoundByController),
			initialClaims:   newClaimArray("claim2-3", "uid2-3", "1Gi", "volume2-3", v1.ClaimPending, nil),
			expectedClaims:  newClaimArray("claim2-3", "uid2-3", "1Gi", "volume2-3", v1.ClaimBound, nil, volume.AnnBindCompleted),
			expectedEvents:  noevents,
			errors:          noerrors,
			test:            testSyncClaim,
		},
		{
			// 声明预绑定到按名称反向预绑定到该声明的 PV，
			// 检查其完成绑定且未设置 volume.AnnBoundByController 注解。
			name:            "2-4 - claim prebound to prebound volume by name",
			initialVolumes:  newVolumeArray("volume2-4", "1Gi", "", "claim2-4", v1.VolumeAvailable, v1.PersistentVolumeReclaimRetain, classEmpty),
			expectedVolumes: newVolumeArray("volume2-4", "1Gi", "uid2-4", "claim2-4", v1.VolumeBound, v1.PersistentVolumeReclaimRetain, classEmpty),
			initialClaims:   newClaimArray("claim2-4", "uid2-4", "1Gi", "volume2-4", v1.ClaimPending, nil),
			expectedClaims:  newClaimArray("claim2-4", "uid2-4", "1Gi", "volume2-4", v1.ClaimBound, nil, volume.AnnBindCompleted),
			expectedEvents:  noevents,
			errors:          noerrors,
			test:            testSyncClaim,
		},
		{
			// syncClaim 处理声明预绑定到按 UID 反向预绑定到该声明的 PV，
			// 检查其完成绑定且未设置 volume.AnnBoundByController 注解。
			name:            "2-5 - claim prebound to prebound volume by UID",
			initialVolumes:  newVolumeArray("volume2-5", "1Gi", "uid2-5", "claim2-5", v1.VolumeAvailable, v1.PersistentVolumeReclaimRetain, classEmpty),
			expectedVolumes: newVolumeArray("volume2-5", "1Gi", "uid2-5", "claim2-5", v1.VolumeBound, v1.PersistentVolumeReclaimRetain, classEmpty),
			initialClaims:   newClaimArray("claim2-5", "uid2-5", "1Gi", "volume2-5", v1.ClaimPending, nil),
			expectedClaims:  newClaimArray("claim2-5", "uid2-5", "1Gi", "volume2-5", v1.ClaimBound, nil, volume.AnnBindCompleted),
			expectedEvents:  noevents,
			errors:          noerrors,
			test:            testSyncClaim,
		},
		{
			// syncClaim 处理声明预绑定到已绑定至其他声明的 PV，
			// 检查声明状态被重置为 Pending。
			name:            "2-6 - claim prebound to already bound volume",
			initialVolumes:  newVolumeArray("volume2-6", "1Gi", "uid2-6_1", "claim2-6_1", v1.VolumeBound, v1.PersistentVolumeReclaimRetain, classEmpty),
			expectedVolumes: newVolumeArray("volume2-6", "1Gi", "uid2-6_1", "claim2-6_1", v1.VolumeBound, v1.PersistentVolumeReclaimRetain, classEmpty),
			initialClaims:   newClaimArray("claim2-6", "uid2-6", "1Gi", "volume2-6", v1.ClaimBound, nil),
			expectedClaims:  newClaimArray("claim2-6", "uid2-6", "1Gi", "volume2-6", v1.ClaimPending, nil),
			expectedEvents:  noevents,
			errors:          noerrors,
			test:            testSyncClaim,
		},
		{
			// syncClaim 处理由控制器绑定到已绑定至其他声明的 PV 的场景，
			// 检查其抛出错误。
			name:            "2-7 - claim bound by controller to already bound volume",
			initialVolumes:  newVolumeArray("volume2-7", "1Gi", "uid2-7_1", "claim2-7_1", v1.VolumeBound, v1.PersistentVolumeReclaimRetain, classEmpty),
			expectedVolumes: newVolumeArray("volume2-7", "1Gi", "uid2-7_1", "claim2-7_1", v1.VolumeBound, v1.PersistentVolumeReclaimRetain, classEmpty),
			initialClaims:   newClaimArray("claim2-7", "uid2-7", "1Gi", "volume2-7", v1.ClaimBound, nil, volume.AnnBoundByController),
			expectedClaims:  newClaimArray("claim2-7", "uid2-7", "1Gi", "volume2-7", v1.ClaimBound, nil, volume.AnnBoundByController),
			expectedEvents:  noevents,
			errors:          noerrors,
			test:            testSyncClaimError,
		},
		{
			// syncClaim 处理声明预绑定到已存在且未绑定的 PV，
			// 标签选择器不匹配但仍应完成绑定，
			// 且未设置 volume.AnnBoundByController 注解。
			name:            "2-8 - claim prebound to unbound volume that does not match the selector",
			initialVolumes:  newVolumeArray("volume2-8", "1Gi", "", "", v1.VolumeAvailable, v1.PersistentVolumeReclaimRetain, classEmpty),
			expectedVolumes: newVolumeArray("volume2-8", "1Gi", "uid2-8", "claim2-8", v1.VolumeBound, v1.PersistentVolumeReclaimRetain, classEmpty, volume.AnnBoundByController),
			initialClaims:   withLabelSelector(labels, newClaimArray("claim2-8", "uid2-8", "1Gi", "volume2-8", v1.ClaimPending, nil)),
			expectedClaims:  withLabelSelector(labels, newClaimArray("claim2-8", "uid2-8", "1Gi", "volume2-8", v1.ClaimBound, nil, volume.AnnBindCompleted)),
			expectedEvents:  noevents,
			errors:          noerrors,
			test:            testSyncClaim,
		},
		{
			// syncClaim 处理声明预绑定到已存在且未绑定的 PV，
			// 卷未绑定但容量小于声明请求值。
			// 检查声明状态被重置为 Pending。
			name:            "2-9 - claim prebound to unbound volume that size is smaller than requested",
			initialVolumes:  newVolumeArray("volume2-9", "1Gi", "", "", v1.VolumeAvailable, v1.PersistentVolumeReclaimRetain, classEmpty),
			expectedVolumes: newVolumeArray("volume2-9", "1Gi", "", "", v1.VolumeAvailable, v1.PersistentVolumeReclaimRetain, classEmpty),
			initialClaims:   newClaimArray("claim2-9", "uid2-9", "2Gi", "volume2-9", v1.ClaimBound, nil),
			expectedClaims:  newClaimArray("claim2-9", "uid2-9", "2Gi", "volume2-9", v1.ClaimPending, nil),
			expectedEvents:  []string{"Warning VolumeMismatch"},
			errors:          noerrors,
			test:            testSyncClaim,
		},
		{
			// syncClaim 处理声明预绑定到已存在且未绑定的 PV，
			// 卷未绑定但 class 不匹配，检查声明状态被重置为 Pending。
			name:            "2-10 - claim prebound to unbound volume that class is different",
			initialVolumes:  newVolumeArray("volume2-10", "1Gi", "1", "", v1.VolumeAvailable, v1.PersistentVolumeReclaimRetain, classGold),
			expectedVolumes: newVolumeArray("volume2-10", "1Gi", "", "", v1.VolumeAvailable, v1.PersistentVolumeReclaimRetain, classGold),
			initialClaims:   newClaimArray("claim2-10", "uid2-10", "1Gi", "volume2-10", v1.ClaimBound, nil),
			expectedClaims:  newClaimArray("claim2-10", "uid2-10", "1Gi", "volume2-10", v1.ClaimPending, nil),
			expectedEvents:  []string{"Warning VolumeMismatch"},
			errors:          noerrors,
			test:            testSyncClaim,
		},

		// [单元测试集 3] 同步已绑定的声明。
		{
			// 已绑定声明的 claim.Spec.VolumeName 被移除，
			// 检查声明被标记为 Lost。
			name:            "3-1 - bound claim with missing VolumeName",
			initialVolumes:  novolumes,
			expectedVolumes: novolumes,
			initialClaims:   newClaimArray("claim3-1", "uid3-1", "10Gi", "", v1.ClaimBound, nil, volume.AnnBoundByController, volume.AnnBindCompleted),
			expectedClaims:  newClaimArray("claim3-1", "uid3-1", "10Gi", "", v1.ClaimLost, nil, volume.AnnBoundByController, volume.AnnBindCompleted),
			expectedEvents:  []string{"Warning ClaimLost"},
			errors:          noerrors,
			test:            testSyncClaim,
		},
		{
			// syncClaim 处理声明绑定到不存在卷的场景，
			// 检查声明被标记为 Lost。
			name:            "3-2 - bound claim with missing volume",
			initialVolumes:  novolumes,
			expectedVolumes: novolumes,
			initialClaims:   newClaimArray("claim3-2", "uid3-2", "10Gi", "volume3-2", v1.ClaimBound, nil, volume.AnnBoundByController, volume.AnnBindCompleted),
			expectedClaims:  newClaimArray("claim3-2", "uid3-2", "10Gi", "volume3-2", v1.ClaimLost, nil, volume.AnnBoundByController, volume.AnnBindCompleted),
			expectedEvents:  []string{"Warning ClaimLost"},
			errors:          noerrors,
			test:            testSyncClaim,
		},
		{
			// syncClaim 处理声明绑定到未绑定卷的场景，检查其完成绑定。
			// 同时检查阶段从 Pending 变为 Bound。
			name:            "3-3 - bound claim with unbound volume",
			initialVolumes:  newVolumeArray("volume3-3", "10Gi", "", "", v1.VolumeAvailable, v1.PersistentVolumeReclaimRetain, classEmpty),
			expectedVolumes: newVolumeArray("volume3-3", "10Gi", "uid3-3", "claim3-3", v1.VolumeBound, v1.PersistentVolumeReclaimRetain, classEmpty, volume.AnnBoundByController),
			initialClaims:   newClaimArray("claim3-3", "uid3-3", "10Gi", "volume3-3", v1.ClaimPending, nil, volume.AnnBoundByController, volume.AnnBindCompleted),
			expectedClaims:  newClaimArray("claim3-3", "uid3-3", "10Gi", "volume3-3", v1.ClaimBound, nil, volume.AnnBoundByController, volume.AnnBindCompleted),
			expectedEvents:  noevents,
			errors:          noerrors,
			test:            testSyncClaim,
		},
		{
			// 声明绑定的卷缺失（或不同）volume.Spec.ClaimRef.UID，
			// 检查声明被标记为 lost。
			name:            "3-4 - bound claim with prebound volume",
			initialVolumes:  newVolumeArray("volume3-4", "10Gi", "claim3-4-x", "claim3-4", v1.VolumeAvailable, v1.PersistentVolumeReclaimRetain, classEmpty),
			expectedVolumes: newVolumeArray("volume3-4", "10Gi", "claim3-4-x", "claim3-4", v1.VolumeAvailable, v1.PersistentVolumeReclaimRetain, classEmpty),
			initialClaims:   newClaimArray("claim3-4", "uid3-4", "10Gi", "volume3-4", v1.ClaimPending, nil, volume.AnnBoundByController, volume.AnnBindCompleted),
			expectedClaims:  newClaimArray("claim3-4", "uid3-4", "10Gi", "volume3-4", v1.ClaimLost, nil, volume.AnnBoundByController, volume.AnnBindCompleted),
			expectedEvents:  []string{"Warning ClaimMisbound"},
			errors:          noerrors,
			test:            testSyncClaim,
		},
		{
			// syncClaim 处理声明绑定到已绑定卷的场景，
			// 检查控制器不做任何操作，同时检查 Pending 阶段被设置为 Bound。
			name:            "3-5 - bound claim with bound volume",
			initialVolumes:  newVolumeArray("volume3-5", "10Gi", "uid3-5", "claim3-5", v1.VolumeAvailable, v1.PersistentVolumeReclaimRetain, classEmpty),
			expectedVolumes: newVolumeArray("volume3-5", "10Gi", "uid3-5", "claim3-5", v1.VolumeBound, v1.PersistentVolumeReclaimRetain, classEmpty),
			initialClaims:   newClaimArray("claim3-5", "uid3-5", "10Gi", "volume3-5", v1.ClaimPending, nil, volume.AnnBindCompleted),
			expectedClaims:  newClaimArray("claim3-5", "uid3-5", "10Gi", "volume3-5", v1.ClaimBound, nil, volume.AnnBindCompleted),
			expectedEvents:  noevents,
			errors:          noerrors,
			test:            testSyncClaim,
		},
		{
			// 声明绑定到已绑定至其他声明的卷，
			// 检查声明被标记为 lost。
			// TODO: 补充事件已发出的断言。
			name:            "3-6 - bound claim with bound volume",
			initialVolumes:  newVolumeArray("volume3-6", "10Gi", "uid3-6-x", "claim3-6-x", v1.VolumeAvailable, v1.PersistentVolumeReclaimRetain, classEmpty),
			expectedVolumes: newVolumeArray("volume3-6", "10Gi", "uid3-6-x", "claim3-6-x", v1.VolumeAvailable, v1.PersistentVolumeReclaimRetain, classEmpty),
			initialClaims:   newClaimArray("claim3-6", "uid3-6", "10Gi", "volume3-6", v1.ClaimPending, nil, volume.AnnBindCompleted),
			expectedClaims:  newClaimArray("claim3-6", "uid3-6", "10Gi", "volume3-6", v1.ClaimLost, nil, volume.AnnBindCompleted),
			expectedEvents:  []string{"Warning ClaimMisbound"},
			errors:          noerrors,
			test:            testSyncClaim,
		},
		{
			// syncClaim 处理声明绑定到未绑定卷的场景，检查其完成绑定，
			// 即使声明的选择器与卷不匹配；同时检查 Pending 阶段被设置为 Bound。
			name:            "3-7 - bound claim with unbound volume where selector doesn't match",
			initialVolumes:  newVolumeArray("volume3-3", "10Gi", "", "", v1.VolumeAvailable, v1.PersistentVolumeReclaimRetain, classEmpty),
			expectedVolumes: newVolumeArray("volume3-3", "10Gi", "uid3-3", "claim3-3", v1.VolumeBound, v1.PersistentVolumeReclaimRetain, classEmpty, volume.AnnBoundByController),
			initialClaims:   withLabelSelector(labels, newClaimArray("claim3-3", "uid3-3", "10Gi", "volume3-3", v1.ClaimPending, nil, volume.AnnBoundByController, volume.AnnBindCompleted)),
			expectedClaims:  withLabelSelector(labels, newClaimArray("claim3-3", "uid3-3", "10Gi", "volume3-3", v1.ClaimBound, nil, volume.AnnBoundByController, volume.AnnBindCompleted)),
			expectedEvents:  noevents,
			errors:          noerrors,
			test:            testSyncClaim,
		},
		// [单元测试集 4] 所有 syncVolume 测试。
		{
			// syncVolume 处理 pending 卷，检查其被标记为 Available。
			name:            "4-1 - pending volume",
			initialVolumes:  newVolumeArray("volume4-1", "10Gi", "", "", v1.VolumeAvailable, v1.PersistentVolumeReclaimRetain, classEmpty),
			expectedVolumes: newVolumeArray("volume4-1", "10Gi", "", "", v1.VolumeAvailable, v1.PersistentVolumeReclaimRetain, classEmpty),
			initialClaims:   noclaims,
			expectedClaims:  noclaims,
			expectedEvents:  noevents,
			errors:          noerrors,
			test:            testSyncVolume,
		},
		{
			// syncVolume 处理预绑定的 pending 卷，
			// 检查其被标记为 Available。
			name:            "4-2 - pending prebound volume",
			initialVolumes:  newVolumeArray("volume4-2", "10Gi", "", "claim4-2", v1.VolumeAvailable, v1.PersistentVolumeReclaimRetain, classEmpty),
			expectedVolumes: newVolumeArray("volume4-2", "10Gi", "", "claim4-2", v1.VolumeAvailable, v1.PersistentVolumeReclaimRetain, classEmpty),
			initialClaims:   noclaims,
			expectedClaims:  noclaims,
			expectedEvents:  noevents,
			errors:          noerrors,
			test:            testSyncVolume,
		},
		{
			// syncVolume 处理绑定到不存在声明的卷，
			// 检查卷被标记为 Released。
			name:            "4-3 - bound volume with missing claim",
			initialVolumes:  newVolumeArray("volume4-3", "10Gi", "uid4-3", "claim4-3", v1.VolumeBound, v1.PersistentVolumeReclaimRetain, classEmpty),
			expectedVolumes: newVolumeArray("volume4-3", "10Gi", "uid4-3", "claim4-3", v1.VolumeReleased, v1.PersistentVolumeReclaimRetain, classEmpty),
			initialClaims:   noclaims,
			expectedClaims:  noclaims,
			expectedEvents:  noevents,
			errors:          noerrors,
			test:            testSyncVolume,
		},
		{
			// syncVolume 处理绑定到 UID 不同声明的卷，
			// 检查卷被标记为 Released。
			name:            "4-4 - volume bound to claim with different UID",
			initialVolumes:  newVolumeArray("volume4-4", "10Gi", "uid4-4", "claim4-4", v1.VolumeBound, v1.PersistentVolumeReclaimRetain, classEmpty),
			expectedVolumes: newVolumeArray("volume4-4", "10Gi", "uid4-4", "claim4-4", v1.VolumeReleased, v1.PersistentVolumeReclaimRetain, classEmpty),
			initialClaims:   newClaimArray("claim4-4", "uid4-4-x", "10Gi", "volume4-4", v1.ClaimBound, nil, volume.AnnBindCompleted),
			expectedClaims:  newClaimArray("claim4-4", "uid4-4-x", "10Gi", "volume4-4", v1.ClaimBound, nil, volume.AnnBindCompleted),
			expectedEvents:  noevents,
			errors:          noerrors,
			test:            testSyncVolume,
		},
		{
			// syncVolume 处理由控制器绑定到未绑定声明的卷，
			// 检查 syncVolume 不做任何操作。
			name:            "4-5 - volume bound by controller to unbound claim",
			initialVolumes:  newVolumeArray("volume4-5", "10Gi", "uid4-5", "claim4-5", v1.VolumeBound, v1.PersistentVolumeReclaimRetain, classEmpty, volume.AnnBoundByController),
			expectedVolumes: newVolumeArray("volume4-5", "10Gi", "uid4-5", "claim4-5", v1.VolumeBound, v1.PersistentVolumeReclaimRetain, classEmpty, volume.AnnBoundByController),
			initialClaims:   newClaimArray("claim4-5", "uid4-5", "10Gi", "", v1.ClaimPending, nil),
			expectedClaims:  newClaimArray("claim4-5", "uid4-5", "10Gi", "", v1.ClaimPending, nil),
			expectedEvents:  noevents,
			errors:          noerrors,
			test:            testSyncVolume,
		},
		{
			// syncVolume 处理由用户绑定到未绑定声明的卷，
			// 检查 syncVolume 不做任何操作。
			name:            "4-5 - volume bound by user to bound claim",
			initialVolumes:  newVolumeArray("volume4-5", "10Gi", "uid4-5", "claim4-5", v1.VolumeBound, v1.PersistentVolumeReclaimRetain, classEmpty),
			expectedVolumes: newVolumeArray("volume4-5", "10Gi", "uid4-5", "claim4-5", v1.VolumeBound, v1.PersistentVolumeReclaimRetain, classEmpty),
			initialClaims:   newClaimArray("claim4-5", "uid4-5", "10Gi", "", v1.ClaimPending, nil),
			expectedClaims:  newClaimArray("claim4-5", "uid4-5", "10Gi", "", v1.ClaimPending, nil),
			expectedEvents:  noevents,
			errors:          noerrors,
			test:            testSyncVolume,
		},
		{
			// syncVolume 处理绑定到已绑定声明的卷。
			// 检查卷被标记为 Bound。
			name:            "4-6 - volume bound by to bound claim",
			initialVolumes:  newVolumeArray("volume4-6", "10Gi", "uid4-6", "claim4-6", v1.VolumeAvailable, v1.PersistentVolumeReclaimRetain, classEmpty),
			expectedVolumes: newVolumeArray("volume4-6", "10Gi", "uid4-6", "claim4-6", v1.VolumeBound, v1.PersistentVolumeReclaimRetain, classEmpty),
			initialClaims:   newClaimArray("claim4-6", "uid4-6", "10Gi", "volume4-6", v1.ClaimBound, nil),
			expectedClaims:  newClaimArray("claim4-6", "uid4-6", "10Gi", "volume4-6", v1.ClaimBound, nil),
			expectedEvents:  noevents,
			errors:          noerrors,
			test:            testSyncVolume,
		},
		{
			// syncVolume 处理由控制器绑定到已绑定至其他卷的声明的卷，
			// 检查卷被回滚。
			name:            "4-7 - volume bound by controller to claim bound somewhere else",
			initialVolumes:  newVolumeArray("volume4-7", "10Gi", "uid4-7", "claim4-7", v1.VolumeBound, v1.PersistentVolumeReclaimRetain, classEmpty, volume.AnnBoundByController),
			expectedVolumes: newVolumeArray("volume4-7", "10Gi", "", "", v1.VolumeAvailable, v1.PersistentVolumeReclaimRetain, classEmpty),
			initialClaims:   newClaimArray("claim4-7", "uid4-7", "10Gi", "volume4-7-x", v1.ClaimBound, nil),
			expectedClaims:  newClaimArray("claim4-7", "uid4-7", "10Gi", "volume4-7-x", v1.ClaimBound, nil),
			expectedEvents:  noevents,
			errors:          noerrors,
			test:            testSyncVolume,
		},
		{
			// syncVolume 处理由用户绑定到已绑定至其他卷的声明的卷，
			// 检查卷被标记为 Available 且 UID 被重置。
			name:            "4-8 - volume bound by user to claim bound somewhere else",
			initialVolumes:  newVolumeArray("volume4-8", "10Gi", "uid4-8", "claim4-8", v1.VolumeBound, v1.PersistentVolumeReclaimRetain, classEmpty),
			expectedVolumes: newVolumeArray("volume4-8", "10Gi", "", "claim4-8", v1.VolumeAvailable, v1.PersistentVolumeReclaimRetain, classEmpty),
			initialClaims:   newClaimArray("claim4-8", "uid4-8", "10Gi", "volume4-8-x", v1.ClaimBound, nil),
			expectedClaims:  newClaimArray("claim4-8", "uid4-8", "10Gi", "volume4-8-x", v1.ClaimBound, nil),
			expectedEvents:  noevents,
			errors:          noerrors,
			test:            testSyncVolume,
		},
		{
			// syncVolume 处理绑定到已绑定声明的卷。
			// 检查卷未被删除。
			name:            "4-9 - volume bound to bound claim, with PersistentVolumeReclaimDelete",
			initialVolumes:  newVolumeArray("volume4-9", "10Gi", "uid4-9", "claim4-9", v1.VolumeAvailable, v1.PersistentVolumeReclaimDelete, classEmpty),
			expectedVolumes: newVolumeArray("volume4-9", "10Gi", "uid4-9", "claim4-9", v1.VolumeBound, v1.PersistentVolumeReclaimDelete, classEmpty),
			initialClaims:   newClaimArray("claim4-9", "uid4-9", "10Gi", "volume4-9", v1.ClaimBound, nil),
			expectedClaims:  newClaimArray("claim4-9", "uid4-9", "10Gi", "volume4-9", v1.ClaimBound, nil),
			expectedEvents:  noevents,
			errors:          noerrors,
			test:            testSyncVolume,
		},
		{
			// syncVolume 处理绑定到不存在声明的卷，
			// 检查尝试删除卷；由于没有对应的 deleter，删除会失败。
			name:           "4-10 - volume bound to missing claim",
			initialVolumes: newVolumeArray("volume4-10", "10Gi", "uid4-10", "claim4-10", v1.VolumeAvailable, v1.PersistentVolumeReclaimDelete, classEmpty),
			expectedVolumes: func() []*v1.PersistentVolume {
				volumes := newVolumeArray("volume4-10", "10Gi", "uid4-10", "claim4-10", v1.VolumeFailed, v1.PersistentVolumeReclaimDelete, classEmpty)
				volumes[0].Status.Message = `error getting deleter volume plugin for volume "volume4-10": no volume plugin matched`
				return volumes
			}(),
			initialClaims:  noclaims,
			expectedClaims: noclaims,
			expectedEvents: noevents,
			errors:         noerrors,
			test:           testSyncVolume,
		},
		{
			// 卷绑定的声明存在于 etcd 但不在本地缓存中。
			// 检查一切保持不变，与上面的用例 4-10 形成对照。
			name:            "4-11 - volume bound to unknown claim",
			initialVolumes:  newVolumeArray("volume4-11", "10Gi", "uid4-11", "claim4-11", v1.VolumeAvailable, v1.PersistentVolumeReclaimDelete, classEmpty),
			expectedVolumes: newVolumeArray("volume4-11", "10Gi", "uid4-11", "claim4-11", v1.VolumeBound, v1.PersistentVolumeReclaimDelete, classEmpty),
			initialClaims:   newClaimArray("claim4-11", "uid4-11", "10Gi", "volume4-11", v1.ClaimBound, nil, annSkipLocalStore),
			expectedClaims:  newClaimArray("claim4-11", "uid4-11", "10Gi", "volume4-11", v1.ClaimBound, nil, annSkipLocalStore),
			expectedEvents:  noevents,
			errors:          noerrors,
			test:            testSyncVolume,
		},

		{
			// 卷绑定的声明存在于 etcd，但本地缓存尚未更新。
			name:            "4-12 - volume bound to newest claim but not updated to local cache",
			initialVolumes:  newVolumeArray("volume4-12", "10Gi", "uid4-12-new", "claim4-12", v1.VolumeAvailable, v1.PersistentVolumeReclaimDelete, classEmpty),
			expectedVolumes: newVolumeArray("volume4-12", "10Gi", "uid4-12-new", "claim4-12", v1.VolumeBound, v1.PersistentVolumeReclaimDelete, classEmpty),
			initialClaims: func() []*v1.PersistentVolumeClaim {
				newClaim := newClaimArray("claim4-12", "uid4-12", "10Gi", "volume4-12", v1.ClaimBound, nil, "")
				// 将 UID 更新为新值且不同步到缓存。
				// annSkipLocalStore 让新 UID 的声明只存在于 etcd，模拟本地缓存滞后。
				newClaim = append(newClaim, newClaimArray("claim4-12", "uid4-12-new", "10Gi", "volume4-12", v1.ClaimBound, nil, annSkipLocalStore)...)
				return newClaim
			}(),
			expectedClaims: newClaimArray("claim4-12", "uid4-12-new", "10Gi", "volume4-12", v1.ClaimBound, nil, annSkipLocalStore),
			expectedEvents: noevents,
			errors:         noerrors,
			test:           testSyncVolume,
		},

		// 带 storage class 的 PVC。
		{
			// syncVolume 将声明绑定到请求的 class，
			// 即使存在更小的 PV 也是如此。
			name: "13-1 - binding to class",
			initialVolumes: []*v1.PersistentVolume{
				newVolume("volume13-1-1", "1Gi", "", "", v1.VolumeAvailable, v1.PersistentVolumeReclaimRetain, classEmpty),
				newVolume("volume13-1-2", "10Gi", "", "", v1.VolumeAvailable, v1.PersistentVolumeReclaimRetain, classGold),
			},
			expectedVolumes: []*v1.PersistentVolume{
				newVolume("volume13-1-1", "1Gi", "", "", v1.VolumeAvailable, v1.PersistentVolumeReclaimRetain, classEmpty),
				newVolume("volume13-1-2", "10Gi", "uid13-1", "claim13-1", v1.VolumeBound, v1.PersistentVolumeReclaimRetain, classGold, volume.AnnBoundByController),
			},
			initialClaims:  newClaimArray("claim13-1", "uid13-1", "1Gi", "", v1.ClaimPending, &classGold),
			expectedClaims: withExpectedCapacity("10Gi", newClaimArray("claim13-1", "uid13-1", "1Gi", "volume13-1-2", v1.ClaimBound, &classGold, volume.AnnBoundByController, volume.AnnBindCompleted)),
			expectedEvents: noevents,
			errors:         noerrors,
			test:           testSyncClaim,
		},
		{
			// syncVolume 绑定没有 class 的声明，
			// 即使存在带 class 的更小 PV 也是如此。
			name: "13-2 - binding without a class",
			initialVolumes: []*v1.PersistentVolume{
				newVolume("volume13-2-1", "1Gi", "", "", v1.VolumeAvailable, v1.PersistentVolumeReclaimRetain, classGold),
				newVolume("volume13-2-2", "10Gi", "", "", v1.VolumeAvailable, v1.PersistentVolumeReclaimRetain, classEmpty),
			},
			expectedVolumes: []*v1.PersistentVolume{
				newVolume("volume13-2-1", "1Gi", "", "", v1.VolumeAvailable, v1.PersistentVolumeReclaimRetain, classGold),
				newVolume("volume13-2-2", "10Gi", "uid13-2", "claim13-2", v1.VolumeBound, v1.PersistentVolumeReclaimRetain, classEmpty, volume.AnnBoundByController),
			},
			initialClaims:  newClaimArray("claim13-2", "uid13-2", "1Gi", "", v1.ClaimPending, nil),
			expectedClaims: withExpectedCapacity("10Gi", newClaimArray("claim13-2", "uid13-2", "1Gi", "volume13-2-2", v1.ClaimBound, nil, volume.AnnBoundByController, volume.AnnBindCompleted)),
			expectedEvents: noevents,
			errors:         noerrors,
			test:           testSyncClaim,
		},
		{
			// syncVolume 将声明绑定到指定 class，
			// 即使存在 class 不同的更小 PV 也是如此。
			name: "13-3 - binding to specific a class",
			initialVolumes: []*v1.PersistentVolume{
				newVolume("volume13-3-1", "1Gi", "", "", v1.VolumeAvailable, v1.PersistentVolumeReclaimRetain, classSilver),
				newVolume("volume13-3-2", "10Gi", "", "", v1.VolumeAvailable, v1.PersistentVolumeReclaimRetain, classGold),
			},
			expectedVolumes: []*v1.PersistentVolume{
				newVolume("volume13-3-1", "1Gi", "", "", v1.VolumeAvailable, v1.PersistentVolumeReclaimRetain, classSilver),
				newVolume("volume13-3-2", "10Gi", "uid13-3", "claim13-3", v1.VolumeBound, v1.PersistentVolumeReclaimRetain, classGold, volume.AnnBoundByController),
			},
			initialClaims:  newClaimArray("claim13-3", "uid13-3", "1Gi", "", v1.ClaimPending, &classGold),
			expectedClaims: withExpectedCapacity("10Gi", newClaimArray("claim13-3", "uid13-3", "1Gi", "volume13-3-2", v1.ClaimBound, &classGold, volume.AnnBoundByController, volume.AnnBindCompleted)),
			expectedEvents: noevents,
			errors:         noerrors,
			test:           testSyncClaim,
		},
		{
			// syncVolume 将请求 class "" 的声明绑定到 class 为 "" 的 PV。
			name:            "13-4 - empty class",
			initialVolumes:  newVolumeArray("volume13-4", "1Gi", "", "", v1.VolumeAvailable, v1.PersistentVolumeReclaimRetain, classEmpty),
			expectedVolumes: newVolumeArray("volume13-4", "1Gi", "uid13-4", "claim13-4", v1.VolumeBound, v1.PersistentVolumeReclaimRetain, classEmpty, volume.AnnBoundByController),
			initialClaims:   newClaimArray("claim13-4", "uid13-4", "1Gi", "", v1.ClaimPending, &classEmpty),
			expectedClaims:  newClaimArray("claim13-4", "uid13-4", "1Gi", "volume13-4", v1.ClaimBound, &classEmpty, volume.AnnBoundByController, volume.AnnBindCompleted),
			expectedEvents:  noevents,
			errors:          noerrors,
			test:            testSyncClaim,
		},
		{
			// syncVolume 将请求 class 为 nil 的声明绑定到 class 为空字符串的 PV。
			name:            "13-5 - nil class",
			initialVolumes:  newVolumeArray("volume13-5", "1Gi", "", "", v1.VolumeAvailable, v1.PersistentVolumeReclaimRetain, classEmpty),
			expectedVolumes: newVolumeArray("volume13-5", "1Gi", "uid13-5", "claim13-5", v1.VolumeBound, v1.PersistentVolumeReclaimRetain, classEmpty, volume.AnnBoundByController),
			initialClaims:   newClaimArray("claim13-5", "uid13-5", "1Gi", "", v1.ClaimPending, nil),
			expectedClaims:  newClaimArray("claim13-5", "uid13-5", "1Gi", "volume13-5", v1.ClaimBound, nil, volume.AnnBoundByController, volume.AnnBindCompleted),
			expectedEvents:  noevents,
			errors:          noerrors,
			test:            testSyncClaim,
		},
	}

	// 待 VolumeAttributesClass 特性门控晋升为 GA 后，将这些用例合并到上面的测试中。
	whenFeatureGateEnabled := []controllerTest{
		{
			// syncClaim 处理声明预绑定到已存在且未绑定的 PV，
			// 未绑定但卷属性类不匹配，检查声明状态被重置为 Pending。
			name:            "2-11 - claim prebound to unbound volume that volume attributes class is different",
			initialVolumes:  volumesWithVAC(classGold, newVolumeArray("volume2-11", "1Gi", "", "", v1.VolumeAvailable, v1.PersistentVolumeReclaimRetain, classEmpty)),
			expectedVolumes: volumesWithVAC(classGold, newVolumeArray("volume2-11", "1Gi", "", "", v1.VolumeAvailable, v1.PersistentVolumeReclaimRetain, classEmpty)),
			initialClaims:   newClaimArray("claim2-11", "uid2-11", "1Gi", "volume2-11", v1.ClaimBound, nil),
			expectedClaims:  newClaimArray("claim2-11", "uid2-11", "1Gi", "volume2-11", v1.ClaimPending, nil),
			expectedEvents:  []string{"Warning VolumeMismatch"},
			errors:          noerrors,
			test:            testSyncClaim,
		},
		{
			// syncClaim 处理声明预绑定到已存在且未绑定的 PV，
			// 未绑定且卷属性类相同，检查其完成绑定且未设置 volume.AnnBoundByController 注解。
			name:            "2-12 - claim prebound to unbound volume that volume attributes class is same",
			initialVolumes:  volumesWithVAC(classGold, newVolumeArray("volume2-12", "1Gi", "", "", v1.VolumeAvailable, v1.PersistentVolumeReclaimRetain, classEmpty)),
			expectedVolumes: volumesWithVAC(classGold, newVolumeArray("volume2-12", "1Gi", "uid2-12", "claim2-12", v1.VolumeBound, v1.PersistentVolumeReclaimRetain, classEmpty, volume.AnnBoundByController)),
			initialClaims:   claimWithVAC(&classGold, newClaimArray("claim2-12", "uid2-12", "1Gi", "volume2-12", v1.ClaimPending, nil)),
			expectedClaims:  withExpectedVAC(&classGold, claimWithVAC(&classGold, newClaimArray("claim2-12", "uid2-12", "1Gi", "volume2-12", v1.ClaimBound, nil, volume.AnnBindCompleted))),
			expectedEvents:  noevents,
			errors:          noerrors,
			test:            testSyncClaim,
		},
	}

	// 待 VolumeAttributesClass 特性门控晋升为 GA 后删除此变量。
	whenFeatureGateDisabled := []controllerTest{
		{
			// syncClaim 处理声明预绑定到已存在且未绑定的 PV，
			// 未绑定且卷属性类相同，但特性门控被禁用，检查声明状态被重置为 Pending。
			name:            "2-13 - claim prebound to unbound volume that volume attributes class is same",
			initialVolumes:  volumesWithVAC(classGold, newVolumeArray("volume2-13", "1Gi", "", "", v1.VolumeAvailable, v1.PersistentVolumeReclaimRetain, classEmpty)),
			expectedVolumes: volumesWithVAC(classGold, newVolumeArray("volume2-13", "1Gi", "", "", v1.VolumeAvailable, v1.PersistentVolumeReclaimRetain, classEmpty)),
			initialClaims:   withExpectedVAC(&classGold, claimWithVAC(&classGold, newClaimArray("claim2-13", "uid2-13", "1Gi", "volume2-13", v1.ClaimBound, nil))),
			expectedClaims:  claimWithVAC(&classGold, newClaimArray("claim2-13", "uid2-13", "1Gi", "volume2-13", v1.ClaimPending, nil)),
			expectedEvents:  []string{"Warning VolumeMismatch"},
			errors:          noerrors,
			test:            testSyncClaim,
		},
	}

	for _, isEnabled := range []bool{true, false} {
		if !isEnabled {
			// 禁用特性时回退到旧仿真版本，确保门控在 1.35 中确实处于关闭状态。
			featuregatetesting.SetFeatureGateEmulationVersionDuringTest(t, utilfeature.DefaultFeatureGate, version.MustParse("1.35"))
		}
		featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.VolumeAttributesClass, isEnabled)

		allTests := tests
		if isEnabled {
			allTests = append(allTests, whenFeatureGateEnabled...)
		} else {
			allTests = append(allTests, whenFeatureGateDisabled...)
		}

		for i := range allTests {
			// 将门控状态写入用例名，避免同一用例在两种状态下难以区分。
			allTests[i].name = fmt.Sprintf("features.VolumeAttributesClass=%v %s", isEnabled, allTests[i].name)
		}

		_, ctx := ktesting.NewTestContext(t)
		runSyncTests(t, ctx, allTests, []*storage.StorageClass{
			{
				ObjectMeta:        metav1.ObjectMeta{Name: classWait},
				VolumeBindingMode: &modeWait,
			},
		}, []*v1.Pod{})
	}
}

// TestSyncBlockVolume 验证 block 与 filesystem 卷模式下的绑定与不绑定场景。
func TestSyncBlockVolume(t *testing.T) {
	modeBlock := v1.PersistentVolumeBlock
	modeFile := v1.PersistentVolumeFilesystem

	// 这些测试依赖默认值填充，因此特性开启时 volumeMode 不会为 nil。
	tests := []controllerTest{
		// 带 VolumeMode 的 PVC。
		{
			// syncVolume 将请求 block 的声明绑定到 block 卷。
			name:            "14-1 - binding to volumeMode block",
			initialVolumes:  withVolumeVolumeMode(&modeBlock, newVolumeArray("volume14-1", "10Gi", "", "", v1.VolumeAvailable, v1.PersistentVolumeReclaimRetain, classEmpty)),
			expectedVolumes: withVolumeVolumeMode(&modeBlock, newVolumeArray("volume14-1", "10Gi", "uid14-1", "claim14-1", v1.VolumeBound, v1.PersistentVolumeReclaimRetain, classEmpty, volume.AnnBoundByController)),
			initialClaims:   withClaimVolumeMode(&modeBlock, newClaimArray("claim14-1", "uid14-1", "10Gi", "", v1.ClaimPending, nil)),
			expectedClaims:  withClaimVolumeMode(&modeBlock, newClaimArray("claim14-1", "uid14-1", "10Gi", "volume14-1", v1.ClaimBound, nil, volume.AnnBoundByController, volume.AnnBindCompleted)),
			expectedEvents:  noevents,
			errors:          noerrors,
			test:            testSyncClaim,
		},
		{
			// syncVolume 将请求 filesystem 的声明绑定到 filesystem 卷。
			name:            "14-2 - binding to volumeMode filesystem",
			initialVolumes:  withVolumeVolumeMode(&modeFile, newVolumeArray("volume14-2", "10Gi", "", "", v1.VolumeAvailable, v1.PersistentVolumeReclaimRetain, classEmpty)),
			expectedVolumes: withVolumeVolumeMode(&modeFile, newVolumeArray("volume14-2", "10Gi", "uid14-2", "claim14-2", v1.VolumeBound, v1.PersistentVolumeReclaimRetain, classEmpty, volume.AnnBoundByController)),
			initialClaims:   withClaimVolumeMode(&modeFile, newClaimArray("claim14-2", "uid14-2", "10Gi", "", v1.ClaimPending, nil)),
			expectedClaims:  withClaimVolumeMode(&modeFile, newClaimArray("claim14-2", "uid14-2", "10Gi", "volume14-2", v1.ClaimBound, nil, volume.AnnBoundByController, volume.AnnBindCompleted)),
			expectedEvents:  noevents,
			errors:          noerrors,
			test:            testSyncClaim,
		},
		{
			// syncVolume 不应将未指定 volumeMode 的声明绑定到指定 filesystem 的卷。
			name:            "14-3 - do not bind pv volumeMode filesystem and pvc volumeMode block",
			initialVolumes:  withVolumeVolumeMode(&modeFile, newVolumeArray("volume14-3", "10Gi", "", "", v1.VolumeAvailable, v1.PersistentVolumeReclaimRetain, classEmpty)),
			expectedVolumes: withVolumeVolumeMode(&modeFile, newVolumeArray("volume14-3", "10Gi", "", "", v1.VolumeAvailable, v1.PersistentVolumeReclaimRetain, classEmpty)),
			initialClaims:   withClaimVolumeMode(&modeBlock, newClaimArray("claim14-3", "uid14-3", "10Gi", "", v1.ClaimPending, nil)),
			expectedClaims:  withClaimVolumeMode(&modeBlock, newClaimArray("claim14-3", "uid14-3", "10Gi", "", v1.ClaimPending, nil)),
			expectedEvents:  []string{"Normal FailedBinding"},
			errors:          noerrors,
			test:            testSyncClaim,
		},
		{
			// syncVolume 不应将请求 filesystem 的声明绑定到未指定 volumeMode 的卷。
			name:            "14-4 - do not bind pv volumeMode block and pvc volumeMode filesystem",
			initialVolumes:  withVolumeVolumeMode(&modeBlock, newVolumeArray("volume14-4", "10Gi", "", "", v1.VolumeAvailable, v1.PersistentVolumeReclaimRetain, classEmpty)),
			expectedVolumes: withVolumeVolumeMode(&modeBlock, newVolumeArray("volume14-4", "10Gi", "", "", v1.VolumeAvailable, v1.PersistentVolumeReclaimRetain, classEmpty)),
			initialClaims:   withClaimVolumeMode(&modeFile, newClaimArray("claim14-4", "uid14-4", "10Gi", "", v1.ClaimPending, nil)),
			expectedClaims:  withClaimVolumeMode(&modeFile, newClaimArray("claim14-4", "uid14-4", "10Gi", "", v1.ClaimPending, nil)),
			expectedEvents:  []string{"Normal FailedBinding"},
			errors:          noerrors,
			test:            testSyncClaim,
		},
		{
			// class 匹配但 volumeMode 不匹配时不应绑定。
			name:            "14-5 - do not bind when matching class but not volumeMode",
			initialVolumes:  withVolumeVolumeMode(&modeFile, newVolumeArray("volume14-5", "10Gi", "", "", v1.VolumeAvailable, v1.PersistentVolumeReclaimRetain, classGold)),
			expectedVolumes: withVolumeVolumeMode(&modeFile, newVolumeArray("volume14-5", "10Gi", "", "", v1.VolumeAvailable, v1.PersistentVolumeReclaimRetain, classGold)),
			initialClaims:   withClaimVolumeMode(&modeBlock, newClaimArray("claim14-5", "uid14-5", "10Gi", "", v1.ClaimPending, &classGold)),
			expectedClaims:  withClaimVolumeMode(&modeBlock, newClaimArray("claim14-5", "uid14-5", "10Gi", "", v1.ClaimPending, &classGold)),
			expectedEvents:  []string{"Warning ProvisioningFailed"},
			errors:          noerrors,
			test:            testSyncClaim,
		},
		{
			// volumeMode 匹配但 class 不匹配时不应绑定。
			name:            "14-5-1 - do not bind when matching volumeModes but class does not match",
			initialVolumes:  withVolumeVolumeMode(&modeFile, newVolumeArray("volume14-5-1", "10Gi", "", "", v1.VolumeAvailable, v1.PersistentVolumeReclaimRetain, classGold)),
			expectedVolumes: withVolumeVolumeMode(&modeFile, newVolumeArray("volume14-5-1", "10Gi", "", "", v1.VolumeAvailable, v1.PersistentVolumeReclaimRetain, classGold)),
			initialClaims:   withClaimVolumeMode(&modeFile, newClaimArray("claim14-5-1", "uid14-5-1", "10Gi", "", v1.ClaimPending, &classSilver)),
			expectedClaims:  withClaimVolumeMode(&modeFile, newClaimArray("claim14-5-1", "uid14-5-1", "10Gi", "", v1.ClaimPending, &classSilver)),
			expectedEvents:  []string{"Warning ProvisioningFailed"},
			errors:          noerrors,
			test:            testSyncClaim,
		},
		{
			// PVC 预绑定到 volumeMode 匹配但 class 不匹配的 PV 时不应绑定。
			name:            "14-5-2 - do not bind when pvc is prebound to pv with matching volumeModes but class does not match",
			initialVolumes:  withVolumeVolumeMode(&modeFile, newVolumeArray("volume14-5-2", "10Gi", "", "", v1.VolumeAvailable, v1.PersistentVolumeReclaimRetain, classGold)),
			expectedVolumes: withVolumeVolumeMode(&modeFile, newVolumeArray("volume14-5-2", "10Gi", "", "", v1.VolumeAvailable, v1.PersistentVolumeReclaimRetain, classGold)),
			initialClaims:   withClaimVolumeMode(&modeFile, newClaimArray("claim14-5-2", "uid14-5-2", "10Gi", "volume14-5-2", v1.ClaimPending, &classSilver)),
			expectedClaims:  withClaimVolumeMode(&modeFile, newClaimArray("claim14-5-2", "uid14-5-2", "10Gi", "volume14-5-2", v1.ClaimPending, &classSilver)),
			expectedEvents:  []string{"Warning VolumeMismatch"},
			errors:          noerrors,
			test:            testSyncClaim,
		},
		{
			// PV 预绑定且 volumeMode 匹配时执行绑定。
			name:            "14-7 - bind when pv volume is prebound and volumeModes match",
			initialVolumes:  withVolumeVolumeMode(&modeBlock, newVolumeArray("volume14-7", "10Gi", "", "claim14-7", v1.VolumeAvailable, v1.PersistentVolumeReclaimRetain, classEmpty)),
			expectedVolumes: withVolumeVolumeMode(&modeBlock, newVolumeArray("volume14-7", "10Gi", "uid14-7", "claim14-7", v1.VolumeBound, v1.PersistentVolumeReclaimRetain, classEmpty)),
			initialClaims:   withClaimVolumeMode(&modeBlock, newClaimArray("claim14-7", "uid14-7", "10Gi", "", v1.ClaimPending, nil)),
			expectedClaims:  withClaimVolumeMode(&modeBlock, newClaimArray("claim14-7", "uid14-7", "10Gi", "volume14-7", v1.ClaimBound, nil, volume.AnnBoundByController, volume.AnnBindCompleted)),
			expectedEvents:  noevents,
			errors:          noerrors,
			test:            testSyncClaim,
		},
		{
			// PVC 预绑定到 volumeMode 不匹配的 PV 时不应绑定。
			name:            "14-8 - do not bind when pvc is prebound to pv with mismatching volumeModes",
			initialVolumes:  withVolumeVolumeMode(&modeBlock, newVolumeArray("volume14-8", "10Gi", "", "", v1.VolumeAvailable, v1.PersistentVolumeReclaimRetain, classEmpty)),
			expectedVolumes: withVolumeVolumeMode(&modeBlock, newVolumeArray("volume14-8", "10Gi", "", "", v1.VolumeAvailable, v1.PersistentVolumeReclaimRetain, classEmpty)),
			initialClaims:   withClaimVolumeMode(&modeFile, newClaimArray("claim14-8", "uid14-8", "10Gi", "volume14-8", v1.ClaimPending, nil)),
			expectedClaims:  withClaimVolumeMode(&modeFile, newClaimArray("claim14-8", "uid14-8", "10Gi", "volume14-8", v1.ClaimPending, nil)),
			expectedEvents:  []string{"Warning VolumeMismatch"},
			errors:          noerrors,
			test:            testSyncClaim,
		},
		{
			// PVC 预绑定到 volumeMode 不匹配的 PV 时不应绑定。
			name:            "14-8-1 - do not bind when pvc is prebound to pv with mismatching volumeModes",
			initialVolumes:  withVolumeVolumeMode(&modeBlock, newVolumeArray("volume14-8-1", "10Gi", "", "claim14-8-1", v1.VolumeAvailable, v1.PersistentVolumeReclaimRetain, classEmpty)),
			expectedVolumes: withVolumeVolumeMode(&modeBlock, newVolumeArray("volume14-8-1", "10Gi", "", "claim14-8-1", v1.VolumeAvailable, v1.PersistentVolumeReclaimRetain, classEmpty)),
			initialClaims:   withClaimVolumeMode(&modeFile, newClaimArray("claim14-8-1", "uid14-8-1", "10Gi", "", v1.ClaimPending, nil)),
			expectedClaims:  withClaimVolumeMode(&modeFile, newClaimArray("claim14-8-1", "uid14-8-1", "10Gi", "", v1.ClaimPending, nil)),
			expectedEvents:  []string{"Normal FailedBinding"},
			errors:          noerrors,
			test:            testSyncClaim,
		},
		{
			// PVC 预绑定到 volumeMode 匹配的 block 卷时执行绑定。
			name:            "14-9 - bind when pvc is prebound to pv with matching volumeModes block",
			initialVolumes:  withVolumeVolumeMode(&modeBlock, newVolumeArray("volume14-9", "10Gi", "", "", v1.VolumeAvailable, v1.PersistentVolumeReclaimRetain, classEmpty)),
			expectedVolumes: withVolumeVolumeMode(&modeBlock, newVolumeArray("volume14-9", "10Gi", "uid14-9", "claim14-9", v1.VolumeBound, v1.PersistentVolumeReclaimRetain, classEmpty, volume.AnnBoundByController)),
			initialClaims:   withClaimVolumeMode(&modeBlock, newClaimArray("claim14-9", "uid14-9", "10Gi", "volume14-9", v1.ClaimPending, nil)),
			expectedClaims:  withClaimVolumeMode(&modeBlock, newClaimArray("claim14-9", "uid14-9", "10Gi", "volume14-9", v1.ClaimBound, nil, volume.AnnBindCompleted)),
			expectedEvents:  noevents,
			errors:          noerrors,
			test:            testSyncClaim,
		},
		{
			// PV 预绑定到 volumeMode 匹配的 block 声明时执行绑定。
			name:            "14-10 - bind when pv is prebound to pvc with matching volumeModes block",
			initialVolumes:  withVolumeVolumeMode(&modeBlock, newVolumeArray("volume14-10", "10Gi", "", "claim14-10", v1.VolumeAvailable, v1.PersistentVolumeReclaimRetain, classEmpty)),
			expectedVolumes: withVolumeVolumeMode(&modeBlock, newVolumeArray("volume14-10", "10Gi", "uid14-10", "claim14-10", v1.VolumeBound, v1.PersistentVolumeReclaimRetain, classEmpty)),
			initialClaims:   withClaimVolumeMode(&modeBlock, newClaimArray("claim14-10", "uid14-10", "10Gi", "", v1.ClaimPending, nil)),
			expectedClaims:  withClaimVolumeMode(&modeBlock, newClaimArray("claim14-10", "uid14-10", "10Gi", "volume14-10", v1.ClaimBound, nil, volume.AnnBoundByController, volume.AnnBindCompleted)),
			expectedEvents:  noevents,
			errors:          noerrors,
			test:            testSyncClaim,
		},
		{
			// PVC 预绑定到 volumeMode 匹配的 filesystem 卷时执行绑定。
			name:            "14-11 - bind when pvc is prebound to pv with matching volumeModes filesystem",
			initialVolumes:  withVolumeVolumeMode(&modeFile, newVolumeArray("volume14-11", "10Gi", "", "", v1.VolumeAvailable, v1.PersistentVolumeReclaimRetain, classEmpty)),
			expectedVolumes: withVolumeVolumeMode(&modeFile, newVolumeArray("volume14-11", "10Gi", "uid14-11", "claim14-11", v1.VolumeBound, v1.PersistentVolumeReclaimRetain, classEmpty, volume.AnnBoundByController)),
			initialClaims:   withClaimVolumeMode(&modeFile, newClaimArray("claim14-11", "uid14-11", "10Gi", "volume14-11", v1.ClaimPending, nil)),
			expectedClaims:  withClaimVolumeMode(&modeFile, newClaimArray("claim14-11", "uid14-11", "10Gi", "volume14-11", v1.ClaimBound, nil, volume.AnnBindCompleted)),
			expectedEvents:  noevents,
			errors:          noerrors,
			test:            testSyncClaim,
		},
		{
			// PV 预绑定到 volumeMode 匹配的 filesystem 声明时执行绑定。
			name:            "14-12 - bind when pv is prebound to pvc with matching volumeModes filesystem",
			initialVolumes:  withVolumeVolumeMode(&modeFile, newVolumeArray("volume14-12", "10Gi", "", "claim14-12", v1.VolumeAvailable, v1.PersistentVolumeReclaimRetain, classEmpty)),
			expectedVolumes: withVolumeVolumeMode(&modeFile, newVolumeArray("volume14-12", "10Gi", "uid14-12", "claim14-12", v1.VolumeBound, v1.PersistentVolumeReclaimRetain, classEmpty)),
			initialClaims:   withClaimVolumeMode(&modeFile, newClaimArray("claim14-12", "uid14-12", "10Gi", "", v1.ClaimPending, nil)),
			expectedClaims:  withClaimVolumeMode(&modeFile, newClaimArray("claim14-12", "uid14-12", "10Gi", "volume14-12", v1.ClaimBound, nil, volume.AnnBoundByController, volume.AnnBindCompleted)),
			expectedEvents:  noevents,
			errors:          noerrors,
			test:            testSyncClaim,
		},
		{
			// PV 预绑定到 volumeMode 不匹配的 PVC 时 syncVolume 输出警告。
			name:            "14-13 - output warning when pv is prebound to pvc with different volumeModes",
			initialVolumes:  withVolumeVolumeMode(&modeFile, newVolumeArray("volume14-13", "10Gi", "uid14-13", "claim14-13", v1.VolumeAvailable, v1.PersistentVolumeReclaimRetain, classEmpty, volume.AnnBoundByController)),
			expectedVolumes: withVolumeVolumeMode(&modeFile, newVolumeArray("volume14-13", "10Gi", "uid14-13", "claim14-13", v1.VolumeAvailable, v1.PersistentVolumeReclaimRetain, classEmpty, volume.AnnBoundByController)),
			initialClaims:   withClaimVolumeMode(&modeBlock, newClaimArray("claim14-13", "uid14-13", "10Gi", "", v1.ClaimPending, nil)),
			expectedClaims:  withClaimVolumeMode(&modeBlock, newClaimArray("claim14-13", "uid14-13", "10Gi", "", v1.ClaimPending, nil)),
			expectedEvents:  []string{"Warning VolumeMismatch"},
			errors:          noerrors,
			test:            testSyncVolume,
		},
		{
			// PV 预绑定到 volumeMode 不匹配的 PVC 时 syncVolume 输出警告。
			name:            "14-13-1 - output warning when pv is prebound to pvc with different volumeModes",
			initialVolumes:  withVolumeVolumeMode(&modeBlock, newVolumeArray("volume14-13-1", "10Gi", "uid14-13-1", "claim14-13-1", v1.VolumeAvailable, v1.PersistentVolumeReclaimRetain, classEmpty, volume.AnnBoundByController)),
			expectedVolumes: withVolumeVolumeMode(&modeBlock, newVolumeArray("volume14-13-1", "10Gi", "uid14-13-1", "claim14-13-1", v1.VolumeAvailable, v1.PersistentVolumeReclaimRetain, classEmpty, volume.AnnBoundByController)),
			initialClaims:   withClaimVolumeMode(&modeFile, newClaimArray("claim14-13-1", "uid14-13-1", "10Gi", "", v1.ClaimPending, nil)),
			expectedClaims:  withClaimVolumeMode(&modeFile, newClaimArray("claim14-13-1", "uid14-13-1", "10Gi", "", v1.ClaimPending, nil)),
			expectedEvents:  []string{"Warning VolumeMismatch"},
			errors:          noerrors,
			test:            testSyncVolume,
		},
		{
			// PV 预绑定到 volumeMode 匹配的 block PVC 时，syncVolume 静默等待 syncClaim。
			name:            "14-14 - wait for synClaim without warning when pv is prebound to pvc with matching volumeModes block",
			initialVolumes:  withVolumeVolumeMode(&modeBlock, newVolumeArray("volume14-14", "10Gi", "uid14-14", "claim14-14", v1.VolumeAvailable, v1.PersistentVolumeReclaimRetain, classEmpty, volume.AnnBoundByController)),
			expectedVolumes: withVolumeVolumeMode(&modeBlock, newVolumeArray("volume14-14", "10Gi", "uid14-14", "claim14-14", v1.VolumeAvailable, v1.PersistentVolumeReclaimRetain, classEmpty, volume.AnnBoundByController)),
			initialClaims:   withClaimVolumeMode(&modeBlock, newClaimArray("claim14-14", "uid14-14", "10Gi", "", v1.ClaimPending, nil)),
			expectedClaims:  withClaimVolumeMode(&modeBlock, newClaimArray("claim14-14", "uid14-14", "10Gi", "", v1.ClaimPending, nil)),
			expectedEvents:  noevents,
			errors:          noerrors,
			test:            testSyncVolume,
		},
		{
			// PV 预绑定到 volumeMode 匹配的 filesystem PVC 时，syncVolume 静默等待 syncClaim。
			name:            "14-14-1 - wait for synClaim without warning when pv is prebound to pvc with matching volumeModes file",
			initialVolumes:  withVolumeVolumeMode(&modeFile, newVolumeArray("volume14-14-1", "10Gi", "uid14-14-1", "claim14-14-1", v1.VolumeAvailable, v1.PersistentVolumeReclaimRetain, classEmpty, volume.AnnBoundByController)),
			expectedVolumes: withVolumeVolumeMode(&modeFile, newVolumeArray("volume14-14-1", "10Gi", "uid14-14-1", "claim14-14-1", v1.VolumeAvailable, v1.PersistentVolumeReclaimRetain, classEmpty, volume.AnnBoundByController)),
			initialClaims:   withClaimVolumeMode(&modeFile, newClaimArray("claim14-14-1", "uid14-14-1", "10Gi", "", v1.ClaimPending, nil)),
			expectedClaims:  withClaimVolumeMode(&modeFile, newClaimArray("claim14-14-1", "uid14-14-1", "10Gi", "", v1.ClaimPending, nil)),
			expectedEvents:  noevents,
			errors:          noerrors,
			test:            testSyncVolume,
		},
	}
	_, ctx := ktesting.NewTestContext(t)
	runSyncTests(t, ctx, tests, []*storage.StorageClass{}, []*v1.Pod{})
}

// TestMultiSync 测试多次调用 syncClaim/syncVolume 以及对所有卷/声明的周期性同步。
// 测试遵循以下流程：
//  0. 用初始数据加载控制器。
//  1. 同 TestSync() 一样调用一次 controllerTest.testCall()。
//  2. 对上一次 syncVolume/syncClaim 改变的所有卷/声明，
//     调用相应的 syncVolume/syncClaim（模拟“卷/声明改变”事件）。
//     若这些调用引发了变化，则重复步骤 2。
//  3. 当所有变化都已处理且不再有新变化时，对所有卷/声明调用
//     syncVolume/syncClaim（模拟“周期性同步”）。
//  4. 若步骤 3. 引发了变化，回到步骤 2.（模拟“卷/声明更新”事件，
//     最终会再次执行步骤 3.）。
//  5. 当步骤 3. 不再产生变化时结束测试，比较最终的卷/声明集合与期望值并报告差异。
//
// 强制限制调用次数，防止陷入死循环。
func TestMultiSync(t *testing.T) {
	tests := []controllerTest{
		// 简单绑定测试。
		{
			// syncClaim 将声明绑定到匹配的未绑定卷。
			name:            "10-1 - successful bind",
			initialVolumes:  newVolumeArray("volume10-1", "1Gi", "", "", v1.VolumePending, v1.PersistentVolumeReclaimRetain, classEmpty),
			expectedVolumes: newVolumeArray("volume10-1", "1Gi", "uid10-1", "claim10-1", v1.VolumeBound, v1.PersistentVolumeReclaimRetain, classEmpty, volume.AnnBoundByController),
			initialClaims:   newClaimArray("claim10-1", "uid10-1", "1Gi", "", v1.ClaimPending, nil),
			expectedClaims:  newClaimArray("claim10-1", "uid10-1", "1Gi", "volume10-1", v1.ClaimBound, nil, volume.AnnBoundByController, volume.AnnBindCompleted),
			expectedEvents:  noevents,
			errors:          noerrors,
			test:            testSyncClaim,
		},
		{
			// 两个控制器将两个 PV 绑定到同一个声明，测试其中一个胜出、
			// 另一个回滚。
			name: "10-2 - bind PV race",
			initialVolumes: []*v1.PersistentVolume{
				newVolume("volume10-2-1", "1Gi", "uid10-2", "claim10-2", v1.VolumeBound, v1.PersistentVolumeReclaimRetain, classEmpty, volume.AnnBoundByController),
				newVolume("volume10-2-2", "1Gi", "uid10-2", "claim10-2", v1.VolumeBound, v1.PersistentVolumeReclaimRetain, classEmpty, volume.AnnBoundByController),
			},
			expectedVolumes: []*v1.PersistentVolume{
				newVolume("volume10-2-1", "1Gi", "uid10-2", "claim10-2", v1.VolumeBound, v1.PersistentVolumeReclaimRetain, classEmpty, volume.AnnBoundByController),
				newVolume("volume10-2-2", "1Gi", "", "", v1.VolumeAvailable, v1.PersistentVolumeReclaimRetain, classEmpty),
			},
			initialClaims:  newClaimArray("claim10-2", "uid10-2", "1Gi", "volume10-2-1", v1.ClaimBound, nil, volume.AnnBoundByController, volume.AnnBindCompleted),
			expectedClaims: newClaimArray("claim10-2", "uid10-2", "1Gi", "volume10-2-1", v1.ClaimBound, nil, volume.AnnBoundByController, volume.AnnBindCompleted),
			expectedEvents: noevents,
			errors:         noerrors,
			test:           testSyncClaim,
		},
	}
	_, ctx := ktesting.NewTestContext(t)
	runMultisyncTests(t, ctx, tests, []*storage.StorageClass{}, "")
}
