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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/component-helpers/storage/volume"
	"k8s.io/klog/v2/ktesting"
	pvtesting "k8s.io/kubernetes/pkg/controller/volume/persistentvolume/testing"
)

// TestRecycleSync 只调用一次 syncVolume，验证回收流程按预期执行：
// 1. 用初始数据填充控制器；
// 2. 调用一次 syncVolume；
// 3. 将最终 PV/PVC 状态与期望值比较。
func TestRecycleSync(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)
	// runningPod 引用 runningClaim 且处于 Running，用于验证被活跃 Pod 使用的卷不能被回收。
	runningPod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "runningPod",
			Namespace: testNamespace,
		},
		Spec: v1.PodSpec{
			Volumes: []v1.Volume{
				{
					Name: "vol1",
					VolumeSource: v1.VolumeSource{
						PersistentVolumeClaim: &v1.PersistentVolumeClaimVolumeSource{
							ClaimName: "runningClaim",
						},
					},
				},
			},
		},
		Status: v1.PodStatus{
			Phase: v1.PodRunning,
		},
	}

	// pendingPod 引用 pendingClaim：Pending 状态的 Pod 同样算作卷的使用者，应阻止回收。
	pendingPod := runningPod.DeepCopy()
	pendingPod.Name = "pendingPod"
	pendingPod.Status.Phase = v1.PodPending
	pendingPod.Spec.Volumes[0].PersistentVolumeClaim.ClaimName = "pendingClaim"

	// completedPod 引用 completedClaim：Pod 已进入 Succeeded 终态，不再算使用者，允许回收。
	completedPod := runningPod.DeepCopy()
	completedPod.Name = "completedPod"
	completedPod.Status.Phase = v1.PodSucceeded
	completedPod.Spec.Volumes[0].PersistentVolumeClaim.ClaimName = "completedClaim"

	// 把三种状态的 Pod 一起交给控制器，覆盖"活跃/未启动/已完成"使用者判定。
	pods := []*v1.Pod{
		runningPod,
		pendingPod,
		completedPod,
	}

	// 表驱动用例覆盖成功回收、各类失败、竞态以及 Pod 使用判定的边界情形。
	tests := []controllerTest{
		{
			// 成功场景：由 controller 绑定的 PV 回收后释放 PVC 引用并回到 Available。
			name:            "6-1 - successful recycle",
			initialVolumes:  newVolumeArray("volume6-1", "1Gi", "uid6-1", "claim6-1", v1.VolumeBound, v1.PersistentVolumeReclaimRecycle, classEmpty, volume.AnnBoundByController),
			expectedVolumes: newVolumeArray("volume6-1", "1Gi", "", "", v1.VolumeAvailable, v1.PersistentVolumeReclaimRecycle, classEmpty),
			initialClaims:   noclaims,
			expectedClaims:  noclaims,
			expectedEvents:  noevents,
			errors:          noerrors,
			// 注入一个模拟成功回收的 recycler，这样测试只走一次 syncVolume 就能观察完整结果。
			test: wrapTestWithReclaimCalls(operationRecycle, []error{nil}, testSyncVolume),
		},
		{
			// 用户预先绑定的 PV：回收后应保留原 claimRef 名称，但 UID 被清空以解除绑定。
			name:            "6-2 - successful recycle with prebound volume",
			initialVolumes:  newVolumeArray("volume6-2", "1Gi", "uid6-2", "claim6-2", v1.VolumeBound, v1.PersistentVolumeReclaimRecycle, classEmpty),
			expectedVolumes: newVolumeArray("volume6-2", "1Gi", "", "claim6-2", v1.VolumeAvailable, v1.PersistentVolumeReclaimRecycle, classEmpty),
			initialClaims:   noclaims,
			expectedClaims:  noclaims,
			expectedEvents:  noevents,
			errors:          noerrors,
			// 同上，注入模拟成功的 recycler 驱动一次回收。
			test: wrapTestWithReclaimCalls(operationRecycle, []error{nil}, testSyncVolume),
		},
		{
			// 找不到对应回收插件时，PV 应进入 Failed 并带明确错误信息与事件。
			name:            "6-3 - plugin not found",
			initialVolumes:  newVolumeArray("volume6-3", "1Gi", "uid6-3", "claim6-3", v1.VolumeBound, v1.PersistentVolumeReclaimRecycle, classEmpty),
			expectedVolumes: withMessage("No recycler plugin found for the volume!", newVolumeArray("volume6-3", "1Gi", "uid6-3", "claim6-3", v1.VolumeFailed, v1.PersistentVolumeReclaimRecycle, classEmpty)),
			initialClaims:   noclaims,
			expectedClaims:  noclaims,
			expectedEvents:  []string{"Warning VolumeFailedRecycle"},
			errors:          noerrors,
			test:            testSyncVolume,
		},
		{
			// recycler 创建阶段就失败（未配置可回收调用），同样应把 PV 标记为 Failed。
			name:            "6-4 - newRecycler returns error",
			initialVolumes:  newVolumeArray("volume6-4", "1Gi", "uid6-4", "claim6-4", v1.VolumeBound, v1.PersistentVolumeReclaimRecycle, classEmpty),
			expectedVolumes: withMessage("Recycle failed: Mock plugin error: no recycleCalls configured", newVolumeArray("volume6-4", "1Gi", "uid6-4", "claim6-4", v1.VolumeFailed, v1.PersistentVolumeReclaimRecycle, classEmpty)),
			initialClaims:   noclaims,
			expectedClaims:  noclaims,
			expectedEvents:  []string{"Warning VolumeFailedRecycle"},
			errors:          noerrors,
			test:            wrapTestWithReclaimCalls(operationRecycle, []error{}, testSyncVolume),
		},
		{
			// recycle() 本身返回错误时，错误信息应写入 PV 的 message 并触发告警事件。
			name:            "6-5 - recycle returns error",
			initialVolumes:  newVolumeArray("volume6-5", "1Gi", "uid6-5", "claim6-5", v1.VolumeBound, v1.PersistentVolumeReclaimRecycle, classEmpty),
			expectedVolumes: withMessage("Recycle failed: Mock recycle error", newVolumeArray("volume6-5", "1Gi", "uid6-5", "claim6-5", v1.VolumeFailed, v1.PersistentVolumeReclaimRecycle, classEmpty)),
			initialClaims:   noclaims,
			expectedClaims:  noclaims,
			expectedEvents:  []string{"Warning VolumeFailedRecycle"},
			errors:          noerrors,
			test:            wrapTestWithReclaimCalls(operationRecycle, []error{errors.New("Mock recycle error")}, testSyncVolume),
		},
		{
			// 竞态场景：回收开始前 PV 已被删除，控制器不应因对象消失而报错。
			name:            "6-6 - volume is deleted before recycling",
			initialVolumes:  newVolumeArray("volume6-6", "1Gi", "uid6-6", "claim6-6", v1.VolumeBound, v1.PersistentVolumeReclaimRecycle, classEmpty),
			expectedVolumes: novolumes,
			initialClaims:   noclaims,
			expectedClaims:  noclaims,
			expectedEvents:  noevents,
			errors:          noerrors,
			test: wrapTestWithInjectedOperation(ctx, wrapTestWithReclaimCalls(operationRecycle, []error{}, testSyncVolume), func(ctrl *PersistentVolumeController, reactor *pvtesting.VolumeReactor) {
				// 在 recycler 执行前删除 PV，模拟删除事件先于回收动作到达。
				reactor.DeleteVolume("volume6-6")
			}),
		},
		{
			// 竞态场景：回收开始前 PV 已被另一个 recycler 标记为 Available，本轮应跳过回收。
			name:            "6-7 - volume is deleted before recycling",
			initialVolumes:  newVolumeArray("volume6-7", "1Gi", "uid6-7", "claim6-7", v1.VolumeBound, v1.PersistentVolumeReclaimRecycle, classEmpty, volume.AnnBoundByController),
			expectedVolumes: newVolumeArray("volume6-7", "1Gi", "", "", v1.VolumeAvailable, v1.PersistentVolumeReclaimRecycle, classEmpty),
			initialClaims:   noclaims,
			expectedClaims:  noclaims,
			expectedEvents:  noevents,
			errors:          noerrors,
			test: wrapTestWithInjectedOperation(ctx, wrapTestWithReclaimCalls(operationRecycle, []error{}, testSyncVolume), func(ctrl *PersistentVolumeController, reactor *pvtesting.VolumeReactor) {
				// 在 recycler 执行前将 PV 标记为 Available，模拟"无需再回收"的跳过路径。
				reactor.MarkVolumeAvailable("volume6-7")
			}),
		},
		{
			// 与 6-7 类似，但针对用户预绑定的 PV：已被标记 Available 时应直接跳过。
			name:            "6-8 - prebound volume is deleted before recycling",
			initialVolumes:  newVolumeArray("volume6-8", "1Gi", "uid6-8", "claim6-8", v1.VolumeBound, v1.PersistentVolumeReclaimRecycle, classEmpty),
			expectedVolumes: newVolumeArray("volume6-8", "1Gi", "", "", v1.VolumeAvailable, v1.PersistentVolumeReclaimRecycle, classEmpty),
			initialClaims:   noclaims,
			expectedClaims:  noclaims,
			expectedEvents:  noevents,
			errors:          noerrors,
			test: wrapTestWithInjectedOperation(ctx, wrapTestWithReclaimCalls(operationRecycle, []error{}, testSyncVolume), func(ctrl *PersistentVolumeController, reactor *pvtesting.VolumeReactor) {
				// 在 recycler 执行前将 PV 标记为 Available，验证预绑定 PV 的跳过路径。
				reactor.MarkVolumeAvailable("volume6-8")
			}),
		},
		{
			// 回收过程中出现了同名但 UID 不同的新 PVC；旧 PV 仍应完成回收，不能被新 claim 牵连。
			name:            "6-9 - prebound volume is recycled while the claim exists",
			initialVolumes:  newVolumeArray("volume6-9", "1Gi", "uid6-9", "claim6-9", v1.VolumeBound, v1.PersistentVolumeReclaimRecycle, classEmpty),
			expectedVolumes: newVolumeArray("volume6-9", "1Gi", "", "claim6-9", v1.VolumeAvailable, v1.PersistentVolumeReclaimRecycle, classEmpty),
			initialClaims:   newClaimArray("claim6-9", "uid6-9-x", "10Gi", "", v1.ClaimPending, nil),
			expectedClaims:  newClaimArray("claim6-9", "uid6-9-x", "10Gi", "", v1.ClaimPending, nil),
			expectedEvents:  noevents,
			errors:          noerrors,
			// 注入模拟成功的 recycler，验证新 claim 的存在不影响回收判定。
			test: wrapTestWithReclaimCalls(operationRecycle, []error{nil}, testSyncVolume),
		},
		{
			// 未知的回收策略无法处理，PV 应进入 Failed 并记录相应事件。
			name:            "6-10 - unknown reclaim policy",
			initialVolumes:  newVolumeArray("volume6-10", "1Gi", "uid6-10", "claim6-10", v1.VolumeBound, "Unknown", classEmpty),
			expectedVolumes: withMessage("Volume has unrecognized PersistentVolumeReclaimPolicy", newVolumeArray("volume6-10", "1Gi", "uid6-10", "claim6-10", v1.VolumeFailed, "Unknown", classEmpty)),
			initialClaims:   noclaims,
			expectedClaims:  noclaims,
			expectedEvents:  []string{"Warning VolumeUnknownReclaimPolicy"},
			errors:          noerrors,
			test:            testSyncVolume,
		},
		{
			// 仍被 Running Pod 使用的卷不能被回收，应停留在 Released 并给出提示事件。
			name:            "6-11 - used by running pod",
			initialVolumes:  newVolumeArray("volume6-11", "1Gi", "uid6-11", "runningClaim", v1.VolumeBound, v1.PersistentVolumeReclaimRecycle, classEmpty, volume.AnnBoundByController),
			expectedVolumes: newVolumeArray("volume6-11", "1Gi", "uid6-11", "runningClaim", v1.VolumeReleased, v1.PersistentVolumeReclaimRecycle, classEmpty, volume.AnnBoundByController),
			initialClaims:   noclaims,
			expectedClaims:  noclaims,
			expectedEvents:  []string{"Normal VolumeFailedRecycle"},
			errors:          noerrors,
			test:            testSyncVolume,
		},
		{
			// Pending Pod 同样算使用者，阻止回收，行为与 Running Pod 一致。
			name:            "6-12 - used by pending pod",
			initialVolumes:  newVolumeArray("volume6-12", "1Gi", "uid6-12", "pendingClaim", v1.VolumeBound, v1.PersistentVolumeReclaimRecycle, classEmpty, volume.AnnBoundByController),
			expectedVolumes: newVolumeArray("volume6-12", "1Gi", "uid6-12", "pendingClaim", v1.VolumeReleased, v1.PersistentVolumeReclaimRecycle, classEmpty, volume.AnnBoundByController),
			initialClaims:   noclaims,
			expectedClaims:  noclaims,
			expectedEvents:  []string{"Normal VolumeFailedRecycle"},
			errors:          noerrors,
			test:            testSyncVolume,
		},
		{
			// Pod 已完成（Succeeded）不再占用卷，回收应成功并清空绑定信息。
			name:            "6-13 - used by completed pod",
			initialVolumes:  newVolumeArray("volume6-13", "1Gi", "uid6-13", "completedClaim", v1.VolumeBound, v1.PersistentVolumeReclaimRecycle, classEmpty, volume.AnnBoundByController),
			expectedVolumes: newVolumeArray("volume6-13", "1Gi", "", "", v1.VolumeAvailable, v1.PersistentVolumeReclaimRecycle, classEmpty),
			initialClaims:   noclaims,
			expectedClaims:  noclaims,
			expectedEvents:  noevents,
			errors:          noerrors,
			// 注入模拟成功的 recycler 验证已完成 Pod 的卷可被回收。
			test: wrapTestWithReclaimCalls(operationRecycle, []error{nil}, testSyncVolume),
		},
		{
			// 同名 completedClaim 已被另一个 UID 的 PVC 绑定到别的 PV：
			// 判断卷是否在使用必须核对 claim UID，而不能仅看名称。
			name:            "6-14 - seemingly used by running pod",
			initialVolumes:  newVolumeArray("volume6-14", "1Gi", "uid6-14", "completedClaim", v1.VolumeBound, v1.PersistentVolumeReclaimRecycle, classEmpty, volume.AnnBoundByController),
			expectedVolumes: newVolumeArray("volume6-14", "1Gi", "", "", v1.VolumeAvailable, v1.PersistentVolumeReclaimRecycle, classEmpty),
			initialClaims:   newClaimArray("completedClaim", "uid6-14-x", "10Gi", "", v1.ClaimBound, nil),
			expectedClaims:  newClaimArray("completedClaim", "uid6-14-x", "10Gi", "", v1.ClaimBound, nil),
			expectedEvents:  noevents,
			errors:          noerrors,
			test:            wrapTestWithReclaimCalls(operationRecycle, []error{nil}, testSyncVolume),
		},
	}
	runSyncTests(t, ctx, tests, []*storage.StorageClass{}, pods)
}

// TestRecycleMultiSync 多次调用 syncClaim/syncVolume 并周期性同步所有卷/PVC，
// 模拟事件驱动的反复协调，验证失败回收最终能被重试成功。
// 测试流程：
//  0. 用初始数据填充控制器；
//  1. 调用一次 controllerTest.testCall()（同 TestSync）；
//  2. 对上一轮发生变化的卷/PVC 调用对应的 sync（模拟"对象变更"事件），
//     若仍有变化则重复此步；
//  3. 所有变化处理完后，对所有对象做一次 sync（模拟"周期性同步"）；
//  4. 若第 3 步产生了变化，回到第 2 步，最终再次执行第 3 步；
//  5. 第 3 步不再产生变化时，比较最终状态与期望值并报告差异。
//
// 调用次数设有上限，防止逻辑缺陷导致测试陷入死循环。
func TestRecycleMultiSync(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)
	tests := []controllerTest{
		{
			// 回收首次返回错误、第二次成功：控制器应在后续 sync 中重试并最终完成回收。
			"7-1 - recycle returns error",
			newVolumeArray("volume7-1", "1Gi", "uid7-1", "claim7-1", v1.VolumeBound, v1.PersistentVolumeReclaimRecycle, classEmpty),
			newVolumeArray("volume7-1", "1Gi", "", "claim7-1", v1.VolumeAvailable, v1.PersistentVolumeReclaimRecycle, classEmpty),
			noclaims,
			noclaims,
			[]string{"Warning VolumeFailedRecycle"}, noerrors,
			wrapTestWithReclaimCalls(operationRecycle, []error{errors.New("Mock recycle error"), nil}, testSyncVolume),
		},
	}

	runMultisyncTests(t, ctx, tests, []*storage.StorageClass{}, "")
}
