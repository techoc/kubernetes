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

	"k8s.io/klog/v2/ktesting"

	v1 "k8s.io/api/core/v1"
	storage "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/component-helpers/storage/volume"
	api "k8s.io/kubernetes/pkg/apis/core"
	pvtesting "k8s.io/kubernetes/pkg/controller/volume/persistentvolume/testing"
)

// class1Parameters 与 class2Parameters 对应两个 StorageClass 的不同参数，
// 用于断言 provision 调用是否拿到了正确的参数。
var class1Parameters = map[string]string{
	"param1": "value1",
}

// class2Parameters 是第二类 StorageClass 的测试参数。
var class2Parameters = map[string]string{
	"param2": "value2",
}

// deleteReclaimPolicy 是测试用 StorageClass 的默认回收策略。
var deleteReclaimPolicy = v1.PersistentVolumeReclaimDelete

// modeImmediate 是测试用 StorageClass 的即时绑定模式。
var modeImmediate = storage.VolumeBindingImmediate

// storageClasses 覆盖内部/外部/未知 provisioner、即时绑定与 WaitForFirstConsumer，
// 以及不支持 mountOptions 的 StorageClass 场景。
var storageClasses = []*storage.StorageClass{
	{
		TypeMeta: metav1.TypeMeta{
			Kind: "StorageClass",
		},

		ObjectMeta: metav1.ObjectMeta{
			Name: "gold",
		},

		Provisioner:       mockPluginName,
		Parameters:        class1Parameters,
		ReclaimPolicy:     &deleteReclaimPolicy,
		VolumeBindingMode: &modeImmediate,
	},
	{
		TypeMeta: metav1.TypeMeta{
			Kind: "StorageClass",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: "silver",
		},
		Provisioner:       mockPluginName,
		Parameters:        class2Parameters,
		ReclaimPolicy:     &deleteReclaimPolicy,
		VolumeBindingMode: &modeImmediate,
	},
	{
		TypeMeta: metav1.TypeMeta{
			Kind: "StorageClass",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: "copper",
		},
		Provisioner:       mockPluginName,
		Parameters:        class1Parameters,
		ReclaimPolicy:     &deleteReclaimPolicy,
		VolumeBindingMode: &modeWait,
	},
	{
		TypeMeta: metav1.TypeMeta{
			Kind: "StorageClass",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: "external",
		},
		Provisioner:       "vendor.com/my-volume",
		Parameters:        class1Parameters,
		ReclaimPolicy:     &deleteReclaimPolicy,
		VolumeBindingMode: &modeImmediate,
	},
	{
		TypeMeta: metav1.TypeMeta{
			Kind: "StorageClass",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: "external-wait",
		},
		Provisioner:       "vendor.com/my-volume-wait",
		Parameters:        class1Parameters,
		ReclaimPolicy:     &deleteReclaimPolicy,
		VolumeBindingMode: &modeWait,
	},
	{
		TypeMeta: metav1.TypeMeta{
			Kind: "StorageClass",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: "unknown-internal",
		},
		Provisioner:       "kubernetes.io/unknown",
		Parameters:        class1Parameters,
		ReclaimPolicy:     &deleteReclaimPolicy,
		VolumeBindingMode: &modeImmediate,
	},
	{
		TypeMeta: metav1.TypeMeta{
			Kind: "StorageClass",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: "unsupported-mountoptions",
		},
		Provisioner:       mockPluginName,
		Parameters:        class1Parameters,
		ReclaimPolicy:     &deleteReclaimPolicy,
		MountOptions:      []string{"foo"},
		VolumeBindingMode: &modeImmediate,
	},
	{
		TypeMeta: metav1.TypeMeta{
			Kind: "StorageClass",
		},

		ObjectMeta: metav1.ObjectMeta{
			Name: "csi",
		},

		Provisioner:       "mydriver.csi.k8s.io",
		Parameters:        class1Parameters,
		ReclaimPolicy:     &deleteReclaimPolicy,
		VolumeBindingMode: &modeImmediate,
	},
}

// 调用 storageClass 1 对应的 provisioner，返回错误。
var provision1Error = provisionCall{
	ret:                errors.New("Mock provisioner error"),
	expectedParameters: class1Parameters,
}

// 调用 storageClass 1 对应的 provisioner，成功返回 PV。
var provision1Success = provisionCall{
	ret:                nil,
	expectedParameters: class1Parameters,
}

// 调用 storageClass 2 对应的 provisioner，成功返回 PV。
var provision2Success = provisionCall{
	ret:                nil,
	expectedParameters: class2Parameters,
}

// TestProvisionSync 只调用一次 syncClaim，验证动态创建行为：
// 1. 用初始数据填充控制器；
// 2. 调用一次 syncVolume/syncClaim；
// 3. 将最终卷状态与期望值比对。
func TestProvisionSync(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)
	tests := []controllerTest{
		{
			// 使用默认 StorageClass 成功创建卷。
			name:            "11-1 - successful provision with storage class 1",
			initialVolumes:  novolumes,
			expectedVolumes: volumesWithFinalizers(newVolumeArray("pvc-uid11-1", "1Gi", "uid11-1", "claim11-1", v1.VolumeBound, v1.PersistentVolumeReclaimDelete, classGold, volume.AnnBoundByController, volume.AnnDynamicallyProvisioned), []string{volume.PVDeletionInTreeProtectionFinalizer}),
			// 绑定将在下一次 syncClaim 中完成。
			initialClaims:  newClaimArray("claim11-1", "uid11-1", "1Gi", "", v1.ClaimPending, &classGold),
			expectedClaims: newClaimArray("claim11-1", "uid11-1", "1Gi", "", v1.ClaimPending, &classGold, volume.AnnStorageProvisioner, volume.AnnBetaStorageProvisioner),
			expectedEvents: []string{"Normal ProvisioningSucceeded"},
			errors:         noerrors,
			test:           wrapTestWithProvisionCalls([]provisionCall{provision1Success}, testSyncClaim),
		},
		{
			// 创建失败：找不到对应的 provisioner 插件。
			name:            "11-2 - plugin not found",
			initialVolumes:  novolumes,
			expectedVolumes: novolumes,
			initialClaims:   newClaimArray("claim11-2", "uid11-2", "1Gi", "", v1.ClaimPending, &classGold),
			expectedClaims:  newClaimArray("claim11-2", "uid11-2", "1Gi", "", v1.ClaimPending, &classGold),
			expectedEvents:  []string{"Warning ProvisioningFailed"},
			errors:          noerrors,
			test:            testSyncClaim,
		},
		{
			// 创建失败：newProvisioner 返回错误。
			name:            "11-3 - newProvisioner failure",
			initialVolumes:  novolumes,
			expectedVolumes: novolumes,
			initialClaims:   newClaimArray("claim11-3", "uid11-3", "1Gi", "", v1.ClaimPending, &classGold),
			expectedClaims:  newClaimArray("claim11-3", "uid11-3", "1Gi", "", v1.ClaimPending, &classGold, volume.AnnStorageProvisioner, volume.AnnBetaStorageProvisioner),
			expectedEvents:  []string{"Warning ProvisioningFailed"},
			errors:          noerrors,
			test:            wrapTestWithProvisionCalls([]provisionCall{}, testSyncClaim),
		},
		{
			// 创建失败：Provision 返回错误。
			name:            "11-4 - provision failure",
			initialVolumes:  novolumes,
			expectedVolumes: novolumes,
			initialClaims:   newClaimArray("claim11-4", "uid11-4", "1Gi", "", v1.ClaimPending, &classGold),
			expectedClaims:  newClaimArray("claim11-4", "uid11-4", "1Gi", "", v1.ClaimPending, &classGold, volume.AnnStorageProvisioner, volume.AnnBetaStorageProvisioner),
			expectedEvents:  []string{"Warning ProvisioningFailed"},
			errors:          noerrors,
			test:            wrapTestWithProvisionCalls([]provisionCall{provision1Error}, testSyncClaim),
		},
		{
			// 已有可用卷时不执行动态创建，而是直接绑定该卷。
			name:            "11-6 - provisioning when there is a volume available",
			initialVolumes:  newVolumeArray("volume11-6", "1Gi", "", "", v1.VolumeAvailable, v1.PersistentVolumeReclaimRetain, classGold),
			expectedVolumes: newVolumeArray("volume11-6", "1Gi", "uid11-6", "claim11-6", v1.VolumeBound, v1.PersistentVolumeReclaimRetain, classGold, volume.AnnBoundByController),
			initialClaims:   newClaimArray("claim11-6", "uid11-6", "1Gi", "", v1.ClaimPending, &classGold),
			expectedClaims:  newClaimArray("claim11-6", "uid11-6", "1Gi", "volume11-6", v1.ClaimBound, &classGold, volume.AnnBoundByController, volume.AnnBindCompleted),
			expectedEvents:  noevents,
			errors:          noerrors,
			// 这里未配置任何创建插件：若控制器错误地尝试创建卷，测试就会失败。
			test: wrapTestWithProvisionCalls([]provisionCall{provision1Success}, testSyncClaim),
		},
		{
			// 模拟竞态：provisioner 创建卷之前，claim 已被其他控制器绑定。
			name:            "11-7 - claim is bound before provisioning",
			initialVolumes:  novolumes,
			expectedVolumes: newVolumeArray("pvc-uid11-7", "1Gi", "uid11-7", "claim11-7", v1.VolumeBound, v1.PersistentVolumeReclaimDelete, classGold, volume.AnnBoundByController, volume.AnnDynamicallyProvisioned),
			initialClaims:   newClaimArray("claim11-7", "uid11-7", "1Gi", "", v1.ClaimPending, &classGold),
			// claim 将在下一次 syncClaim 中完成绑定。
			expectedClaims: newClaimArray("claim11-7", "uid11-7", "1Gi", "", v1.ClaimPending, &classGold, volume.AnnStorageProvisioner, volume.AnnBetaStorageProvisioner),
			expectedEvents: noevents,
			errors:         noerrors,
			test: wrapTestWithInjectedOperation(ctx, wrapTestWithProvisionCalls([]provisionCall{}, testSyncClaim), func(ctrl *PersistentVolumeController, reactor *pvtesting.VolumeReactor) {
				// 在 provisionClaimOperation 开始前创建卷，
				// 模拟另一个控制器并行创建了同一个卷。
				volume := newVolume("pvc-uid11-7", "1Gi", "uid11-7", "claim11-7", v1.VolumeBound, v1.PersistentVolumeReclaimDelete, classGold, volume.AnnBoundByController, volume.AnnDynamicallyProvisioned)
				reactor.AddVolume(volume)
			}),
		},
		{
			// 首次保存 PV 失败、重试成功：验证创建流程具备重试能力。
			name:            "11-8 - cannot save provisioned volume",
			initialVolumes:  novolumes,
			expectedVolumes: volumesWithFinalizers(newVolumeArray("pvc-uid11-8", "1Gi", "uid11-8", "claim11-8", v1.VolumeBound, v1.PersistentVolumeReclaimDelete, classGold, volume.AnnBoundByController, volume.AnnDynamicallyProvisioned), []string{volume.PVDeletionInTreeProtectionFinalizer}),
			initialClaims:   newClaimArray("claim11-8", "uid11-8", "1Gi", "", v1.ClaimPending, &classGold),
			// 绑定将在下一次 syncClaim 中完成。
			expectedClaims: newClaimArray("claim11-8", "uid11-8", "1Gi", "", v1.ClaimPending, &classGold, volume.AnnStorageProvisioner, volume.AnnBetaStorageProvisioner),
			expectedEvents: []string{"Normal ProvisioningSucceeded"},
			errors: []pvtesting.ReactorError{
				// 仅在第一次 PersistentVolumes.Create() 调用时注入错误，
				// 后续调用均成功。
				{Verb: "create", Resource: "persistentvolumes", Error: errors.New("Mock creation error")},
			},
			test: wrapTestWithProvisionCalls([]provisionCall{provision1Success}, testSyncClaim),
		},
		{
			// 五次保存 PV 均失败后，控制器清理已创建的卷，且清理成功。
			name:            "11-9 - cannot save provisioned volume, delete succeeds",
			initialVolumes:  novolumes,
			expectedVolumes: novolumes,
			initialClaims:   newClaimArray("claim11-9", "uid11-9", "1Gi", "", v1.ClaimPending, &classGold),
			expectedClaims:  newClaimArray("claim11-9", "uid11-9", "1Gi", "", v1.ClaimPending, &classGold, volume.AnnStorageProvisioner, volume.AnnBetaStorageProvisioner),
			expectedEvents:  []string{"Warning ProvisioningFailed"},
			errors: []pvtesting.ReactorError{
				// 连续五次在 PersistentVolumes.Create() 调用中注入错误。
				{Verb: "create", Resource: "persistentvolumes", Error: errors.New("Mock creation error1")},
				{Verb: "create", Resource: "persistentvolumes", Error: errors.New("Mock creation error2")},
				{Verb: "create", Resource: "persistentvolumes", Error: errors.New("Mock creation error3")},
				{Verb: "create", Resource: "persistentvolumes", Error: errors.New("Mock creation error4")},
				{Verb: "create", Resource: "persistentvolumes", Error: errors.New("Mock creation error5")},
			},
			test: wrapTestWithPluginCalls(
				nil,                                // 回收调用（本场景无）
				[]error{nil},                       // delete calls
				[]provisionCall{provision1Success}, // 创建调用
				testSyncClaim,
			),
		},
		{
			// 五次保存 PV 均失败，且找不到删除插件：清理失败也应上报事件。
			name:            "11-10 - cannot save provisioned volume, no delete plugin found",
			initialVolumes:  novolumes,
			expectedVolumes: novolumes,
			initialClaims:   newClaimArray("claim11-10", "uid11-10", "1Gi", "", v1.ClaimPending, &classGold),
			expectedClaims:  newClaimArray("claim11-10", "uid11-10", "1Gi", "", v1.ClaimPending, &classGold, volume.AnnStorageProvisioner, volume.AnnBetaStorageProvisioner),
			expectedEvents:  []string{"Warning ProvisioningFailed", "Warning ProvisioningCleanupFailed"},
			errors: []pvtesting.ReactorError{
				// 连续五次在 PersistentVolumes.Create() 调用中注入错误。
				{Verb: "create", Resource: "persistentvolumes", Error: errors.New("Mock creation error1")},
				{Verb: "create", Resource: "persistentvolumes", Error: errors.New("Mock creation error2")},
				{Verb: "create", Resource: "persistentvolumes", Error: errors.New("Mock creation error3")},
				{Verb: "create", Resource: "persistentvolumes", Error: errors.New("Mock creation error4")},
				{Verb: "create", Resource: "persistentvolumes", Error: errors.New("Mock creation error5")},
			},
			// 未配置删除调用，等价于没有可用于该卷的删除插件。
			test: wrapTestWithProvisionCalls([]provisionCall{provision1Success}, testSyncClaim),
		},
		{
			// 五次保存 PV 均失败，删除插件也连续五次返回错误。
			name:            "11-11 - cannot save provisioned volume, deleter fails",
			initialVolumes:  novolumes,
			expectedVolumes: novolumes,
			initialClaims:   newClaimArray("claim11-11", "uid11-11", "1Gi", "", v1.ClaimPending, &classGold),
			expectedClaims:  newClaimArray("claim11-11", "uid11-11", "1Gi", "", v1.ClaimPending, &classGold, volume.AnnStorageProvisioner, volume.AnnBetaStorageProvisioner),
			expectedEvents:  []string{"Warning ProvisioningFailed", "Warning ProvisioningCleanupFailed"},
			errors: []pvtesting.ReactorError{
				// 连续五次在 PersistentVolumes.Create() 调用中注入错误。
				{Verb: "create", Resource: "persistentvolumes", Error: errors.New("Mock creation error1")},
				{Verb: "create", Resource: "persistentvolumes", Error: errors.New("Mock creation error2")},
				{Verb: "create", Resource: "persistentvolumes", Error: errors.New("Mock creation error3")},
				{Verb: "create", Resource: "persistentvolumes", Error: errors.New("Mock creation error4")},
				{Verb: "create", Resource: "persistentvolumes", Error: errors.New("Mock creation error5")},
			},
			test: wrapTestWithPluginCalls(
				nil, // 回收调用（本场景无）
				[]error{ // 删除调用
					errors.New("Mock deletion error1"),
					errors.New("Mock deletion error2"),
					errors.New("Mock deletion error3"),
					errors.New("Mock deletion error4"),
					errors.New("Mock deletion error5"),
				},
				[]provisionCall{provision1Success}, // 创建调用
				testSyncClaim),
		},
		{
			// 五次保存 PV 均失败，删除操作第一次失败、第二次成功。
			name:            "11-12 - cannot save provisioned volume, delete succeeds 2nd time",
			initialVolumes:  novolumes,
			expectedVolumes: novolumes,
			initialClaims:   newClaimArray("claim11-12", "uid11-12", "1Gi", "", v1.ClaimPending, &classGold),
			expectedClaims:  newClaimArray("claim11-12", "uid11-12", "1Gi", "", v1.ClaimPending, &classGold, volume.AnnStorageProvisioner, volume.AnnBetaStorageProvisioner),
			expectedEvents:  []string{"Warning ProvisioningFailed"},
			errors: []pvtesting.ReactorError{
				// 连续五次在 PersistentVolumes.Create() 调用中注入错误。
				{Verb: "create", Resource: "persistentvolumes", Error: errors.New("Mock creation error1")},
				{Verb: "create", Resource: "persistentvolumes", Error: errors.New("Mock creation error2")},
				{Verb: "create", Resource: "persistentvolumes", Error: errors.New("Mock creation error3")},
				{Verb: "create", Resource: "persistentvolumes", Error: errors.New("Mock creation error4")},
				{Verb: "create", Resource: "persistentvolumes", Error: errors.New("Mock creation error5")},
			},
			test: wrapTestWithPluginCalls(
				nil, // 回收调用（本场景无）
				[]error{ // 删除调用
					errors.New("Mock deletion error1"),
					nil,
				}, // 创建调用
				[]provisionCall{provision1Success},
				testSyncClaim,
			),
		},
		{
			// 使用非默认 StorageClass 成功创建卷。
			name:            "11-13 - successful provision with storage class 2",
			initialVolumes:  novolumes,
			expectedVolumes: volumesWithFinalizers(newVolumeArray("pvc-uid11-13", "1Gi", "uid11-13", "claim11-13", v1.VolumeBound, v1.PersistentVolumeReclaimDelete, classSilver, volume.AnnBoundByController, volume.AnnDynamicallyProvisioned), []string{volume.PVDeletionInTreeProtectionFinalizer}),
			initialClaims:   newClaimArray("claim11-13", "uid11-13", "1Gi", "", v1.ClaimPending, &classSilver),
			// 绑定将在下一次 syncClaim 中完成。
			expectedClaims: newClaimArray("claim11-13", "uid11-13", "1Gi", "", v1.ClaimPending, &classSilver, volume.AnnStorageProvisioner, volume.AnnBetaStorageProvisioner),
			expectedEvents: []string{"Normal ProvisioningSucceeded"},
			errors:         noerrors,
			test:           wrapTestWithProvisionCalls([]provisionCall{provision2Success}, testSyncClaim),
		},
		{
			// StorageClass 不存在，无法创建卷。
			name:            "11-14 - fail due to non-existing class",
			initialVolumes:  novolumes,
			expectedVolumes: novolumes,
			initialClaims:   newClaimArray("claim11-14", "uid11-14", "1Gi", "", v1.ClaimPending, &classNonExisting),
			expectedClaims:  newClaimArray("claim11-14", "uid11-14", "1Gi", "", v1.ClaimPending, &classNonExisting),
			expectedEvents:  noevents,
			errors:          noerrors,
			test:            wrapTestWithProvisionCalls([]provisionCall{}, testSyncClaim),
		},
		{
			// class 为空字符串时不执行动态创建。
			name:            "11-15 - no provisioning with class=''",
			initialVolumes:  novolumes,
			expectedVolumes: novolumes,
			initialClaims:   newClaimArray("claim11-15", "uid11-15", "1Gi", "", v1.ClaimPending, &classEmpty),
			expectedClaims:  newClaimArray("claim11-15", "uid11-15", "1Gi", "", v1.ClaimPending, &classEmpty),
			expectedEvents:  noevents,
			errors:          noerrors,
			test:            wrapTestWithProvisionCalls([]provisionCall{}, testSyncClaim),
		},
		{
			// class 为 nil 时不执行动态创建。
			name:            "11-16 - no provisioning with class=nil",
			initialVolumes:  novolumes,
			expectedVolumes: novolumes,
			initialClaims:   newClaimArray("claim11-15", "uid11-15", "1Gi", "", v1.ClaimPending, nil),
			expectedClaims:  newClaimArray("claim11-15", "uid11-15", "1Gi", "", v1.ClaimPending, nil),
			expectedEvents:  noevents,
			errors:          noerrors,
			test:            wrapTestWithProvisionCalls([]provisionCall{}, testSyncClaim),
		},
		{
			// 外部 provisioner 场景下不自行创建，只发出正常事件。
			name:            "11-17 - external provisioner",
			initialVolumes:  novolumes,
			expectedVolumes: novolumes,
			initialClaims:   newClaimArray("claim11-17", "uid11-17", "1Gi", "", v1.ClaimPending, &classExternal),
			expectedClaims: claimWithAnnotation(volume.AnnBetaStorageProvisioner, "vendor.com/my-volume",
				claimWithAnnotation(volume.AnnStorageProvisioner, "vendor.com/my-volume",
					newClaimArray("claim11-17", "uid11-17", "1Gi", "", v1.ClaimPending, &classExternal))),
			expectedEvents: []string{"Normal ExternalProvisioning"},
			errors:         noerrors,
			test:           wrapTestWithProvisionCalls([]provisionCall{}, testSyncClaim),
		},
		{
			// 未知的内部 provisioner 无法处理，发出警告事件。
			name:            "11-18 - unknown internal provisioner",
			initialVolumes:  novolumes,
			expectedVolumes: novolumes,
			initialClaims:   newClaimArray("claim11-18", "uid11-18", "1Gi", "", v1.ClaimPending, &classUnknownInternal),
			expectedClaims:  newClaimArray("claim11-18", "uid11-18", "1Gi", "", v1.ClaimPending, &classUnknownInternal),
			expectedEvents:  []string{"Warning ProvisioningFailed"},
			errors:          noerrors,
			test:            wrapTestWithProvisionCalls([]provisionCall{}, testSyncClaim),
		},
		{
			// 模拟 API server 在第一次保存 PV 时崩溃：对象已写入 etcd，
			// 但还没来得及返回 200 OK。控制器重试时收到 AlreadyExists，
			// 说明 PV 实际上已创建成功。
			//
			"11-19 - provisioned volume saved but API server crashed",
			novolumes,
			// 测试并未真的让 API server 先保存对象再崩溃，Create() 直接返回错误且未保存卷，
			// 因此测试结束时期望的卷集合为空。
			novolumes,
			newClaimArray("claim11-19", "uid11-19", "1Gi", "", v1.ClaimPending, &classGold),
			newClaimArray("claim11-19", "uid11-19", "1Gi", "", v1.ClaimPending, &classGold, volume.AnnStorageProvisioner, volume.AnnBetaStorageProvisioner),
			noevents,
			[]pvtesting.ReactorError{
				// 注入错误以模拟 PersistentVolumes.Create() 期间 API server 崩溃。
				{Verb: "create", Resource: "persistentvolumes", Error: errors.New("Mock creation error1")},
				{Verb: "create", Resource: "persistentvolumes", Error: apierrors.NewAlreadyExists(api.Resource("persistentvolumes"), "")},
			},
			wrapTestWithPluginCalls(
				nil, // 回收调用（本场景无）
				nil, // 删除调用；若发生删除，测试即失败
				[]provisionCall{provision1Success},
				testSyncClaim,
			),
		},
		{
			// StorageClass 的 mountOptions 不受支持，发出警告事件。
			name:            "11-20 - unsupported storageClass.mountOptions",
			initialVolumes:  novolumes,
			expectedVolumes: novolumes,
			initialClaims:   newClaimArray("claim11-20", "uid11-20", "1Gi", "", v1.ClaimPending, &classUnsupportedMountOptions),
			expectedClaims:  newClaimArray("claim11-20", "uid11-20", "1Gi", "", v1.ClaimPending, &classUnsupportedMountOptions, volume.AnnStorageProvisioner, volume.AnnBetaStorageProvisioner),
			// 保存 PV 必然失败，因此事件消息以 "Mount options" 开头。
			expectedEvents: []string{"Warning ProvisioningFailed Mount options"},
			errors:         noerrors,
			test:           wrapTestWithProvisionCalls([]provisionCall{}, testSyncClaim),
		},
		{
			// CSI 迁移场景下不自行创建，交由外部 provisioner 处理并发出正常事件。
			name:            "11-21 - external provisioner for CSI migration",
			initialVolumes:  novolumes,
			expectedVolumes: novolumes,
			initialClaims:   newClaimArray("claim11-21", "uid11-21", "1Gi", "", v1.ClaimPending, &classGold),
			expectedClaims: []*v1.PersistentVolumeClaim{
				annotateClaim(
					newClaim("claim11-21", "uid11-21", "1Gi", "", v1.ClaimPending, &classGold),
					map[string]string{
						volume.AnnStorageProvisioner:     "vendor.com/MockCSIDriver",
						volume.AnnBetaStorageProvisioner: "vendor.com/MockCSIDriver",
						volume.AnnMigratedTo:             "vendor.com/MockCSIDriver",
					}),
			},
			expectedEvents: []string{"Normal ExternalProvisioning"},
			errors:         noerrors,
			test:           wrapTestWithCSIMigrationProvisionCalls(testSyncClaim),
		},
		{
			// 已有可用的已创建卷，此时不应发出外部 provisioner 的正常事件。
			name:            "11-22 - external provisioner with volume available",
			initialVolumes:  newVolumeArray("volume11-22", "1Gi", "", "", v1.VolumeAvailable, v1.PersistentVolumeReclaimRetain, classExternal),
			expectedVolumes: newVolumeArray("volume11-22", "1Gi", "uid11-22", "claim11-22", v1.VolumeBound, v1.PersistentVolumeReclaimRetain, classExternal, volume.AnnBoundByController),
			initialClaims:   newClaimArray("claim11-22", "uid11-22", "1Gi", "", v1.ClaimPending, &classExternal),
			expectedClaims:  newClaimArray("claim11-22", "uid11-22", "1Gi", "volume11-22", v1.ClaimBound, &classExternal, volume.AnnBoundByController, volume.AnnBindCompleted),
			expectedEvents:  noevents,
			errors:          noerrors,
			test:            wrapTestWithProvisionCalls([]provisionCall{}, testSyncClaim),
		},
		{
			// 为已调度到节点的 PVC 创建卷，此时跳过寻找已有 PV。
			"11-23 - skip finding PV and provision for PVC annotated with AnnSelectedNode",
			newVolumeArray("volume11-23", "1Gi", "", "", v1.VolumeAvailable, v1.PersistentVolumeReclaimDelete, classCopper),
			[]*v1.PersistentVolume{
				newVolumeWithFinalizers("volume11-23", "1Gi", "", "", v1.VolumeAvailable, v1.PersistentVolumeReclaimDelete, classCopper, nil /*此处不添加 finalizer：该测试只触发 syncClaim，不触发 syncVolume*/),
				newVolumeWithFinalizers("pvc-uid11-23", "1Gi", "uid11-23", "claim11-23", v1.VolumeBound, v1.PersistentVolumeReclaimDelete, classCopper, []string{volume.PVDeletionInTreeProtectionFinalizer}, volume.AnnDynamicallyProvisioned, volume.AnnBoundByController),
			},
			claimWithAnnotation(volume.AnnSelectedNode, "node1",
				newClaimArray("claim11-23", "uid11-23", "1Gi", "", v1.ClaimPending, &classCopper)),
			claimWithAnnotation(volume.AnnSelectedNode, "node1",
				newClaimArray("claim11-23", "uid11-23", "1Gi", "", v1.ClaimPending, &classCopper, volume.AnnStorageProvisioner, volume.AnnBetaStorageProvisioner)),
			[]string{"Normal ProvisioningSucceeded"},
			noerrors,
			wrapTestWithInjectedOperation(ctx, wrapTestWithProvisionCalls([]provisionCall{provision1Success}, testSyncClaim),
				func(ctrl *PersistentVolumeController, reactor *pvtesting.VolumeReactor) {
					nodesIndexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})
					node := &v1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node1"}}
					nodesIndexer.Add(node)
					ctrl.NodeLister = corelisters.NewNodeLister(nodesIndexer)
				}),
		},
		{
			// volume provision for PVC that scheduled
			name:            "11-24 - skip finding PV and wait external provisioner for PVC annotated with AnnSelectedNode",
			initialVolumes:  newVolumeArray("volume11-24", "1Gi", "", "", v1.VolumeAvailable, v1.PersistentVolumeReclaimDelete, classExternalWait),
			expectedVolumes: newVolumeArray("volume11-24", "1Gi", "", "", v1.VolumeAvailable, v1.PersistentVolumeReclaimDelete, classExternalWait),
			initialClaims: claimWithAnnotation(volume.AnnSelectedNode, "node1",
				newClaimArray("claim11-24", "uid11-24", "1Gi", "", v1.ClaimPending, &classExternalWait)),
			expectedClaims: claimWithAnnotation(volume.AnnBetaStorageProvisioner, "vendor.com/my-volume-wait",
				claimWithAnnotation(volume.AnnStorageProvisioner, "vendor.com/my-volume-wait",
					claimWithAnnotation(volume.AnnSelectedNode, "node1",
						newClaimArray("claim11-24", "uid11-24", "1Gi", "", v1.ClaimPending, &classExternalWait)))),
			expectedEvents: []string{"Normal ExternalProvisioning"},
			errors:         noerrors,
			test:           testSyncClaim,
		},
		{
			// in-tree 插件不支持从数据源创建卷，预期失败。
			name:            "11-25 - failed in-tree provision with data source",
			initialVolumes:  novolumes,
			expectedVolumes: novolumes,
			initialClaims:   claimWithDataSource("test-snap", "VolumeSnapshot", "snapshot.storage.k8s.io", newClaimArray("claim11-25", "uid11-25", "1Gi", "", v1.ClaimPending, &classGold)),
			expectedClaims:  claimWithDataSource("test-snap", "VolumeSnapshot", "snapshot.storage.k8s.io", newClaimArray("claim11-25", "uid11-25", "1Gi", "", v1.ClaimPending, &classGold)),
			expectedEvents:  []string{"Warning ProvisioningFailed"},
			errors:          noerrors,
			test:            testSyncClaim,
		},
		{
			// CSI 插件支持从数据源创建卷，流程继续并交由外部 provisioner 处理。
			"11-26 - csi with data source",
			novolumes,
			novolumes,
			claimWithAnnotation(volume.AnnStorageProvisioner, "mydriver.csi.k8s.io",
				claimWithDataSource("test-snap", "VolumeSnapshot", "snapshot.storage.k8s.io", newClaimArray("claim11-26", "uid11-26", "1Gi", "", v1.ClaimPending, &classCSI))),
			claimWithAnnotation(volume.AnnStorageProvisioner, "mydriver.csi.k8s.io",
				claimWithDataSource("test-snap", "VolumeSnapshot", "snapshot.storage.k8s.io", newClaimArray("claim11-26", "uid11-26", "1Gi", "", v1.ClaimPending, &classCSI))),
			[]string{"Normal ExternalProvisioning"},
			noerrors,
			wrapTestWithProvisionCalls([]provisionCall{}, testSyncClaim),
		},
	}
	runSyncTests(t, ctx, tests, storageClasses, []*v1.Pod{})
}

// TestProvisionMultiSync 多次调用 syncClaim/syncVolume 并执行周期性同步，
// 模拟控制器在事件驱动下的收敛过程：
//  0. 用初始数据填充控制器；
//  1. 先调用一次 controllerTest.testCall()，与 TestSync 一致；
//  2. 对上一步发生变化的卷/claim 调用相应的 syncVolume/syncClaim，
//     模拟 "volume/claim changed" 事件；若仍有变化则重复该步骤；
//  3. 没有新变化时，对所有卷/claim 调用一次同步，模拟 "周期性同步"；
//  4. 若周期性同步产生变化，回到步骤 2 继续处理，最终再次执行步骤 3；
//  5. 周期性同步不再产生变化时，比对最终的卷/claim 集合与期望值。
//
// 调用次数设有上限，防止死循环。
func TestProvisionMultiSync(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)
	tests := []controllerTest{
		{
			// 创建卷并完成绑定。
			name:            "12-1 - successful provision",
			initialVolumes:  novolumes,
			expectedVolumes: volumesWithFinalizers(newVolumeArray("pvc-uid12-1", "1Gi", "uid12-1", "claim12-1", v1.VolumeBound, v1.PersistentVolumeReclaimDelete, classGold, volume.AnnBoundByController, volume.AnnDynamicallyProvisioned), []string{volume.PVDeletionInTreeProtectionFinalizer}),
			initialClaims:   newClaimArray("claim12-1", "uid12-1", "1Gi", "", v1.ClaimPending, &classGold),
			expectedClaims:  newClaimArray("claim12-1", "uid12-1", "1Gi", "pvc-uid12-1", v1.ClaimBound, &classGold, volume.AnnBoundByController, volume.AnnBindCompleted, volume.AnnStorageProvisioner, volume.AnnBetaStorageProvisioner),
			expectedEvents:  noevents,
			errors:          noerrors,
			test:            wrapTestWithProvisionCalls([]provisionCall{provision1Success}, testSyncClaim),
		},
		{
			// 外部 provisioner 创建卷并绑定，同时发出正常事件。
			name:            "12-2 - external provisioner with volume provisioned success",
			initialVolumes:  novolumes,
			expectedVolumes: newVolumeArray("pvc-uid12-2", "1Gi", "uid12-2", "claim12-2", v1.VolumeBound, v1.PersistentVolumeReclaimRetain, classExternal, volume.AnnBoundByController),
			initialClaims:   newClaimArray("claim12-2", "uid12-2", "1Gi", "", v1.ClaimPending, &classExternal),
			expectedClaims: claimWithAnnotation(volume.AnnBetaStorageProvisioner, "vendor.com/my-volume",
				claimWithAnnotation(volume.AnnStorageProvisioner, "vendor.com/my-volume",
					newClaimArray("claim12-2", "uid12-2", "1Gi", "pvc-uid12-2", v1.ClaimBound, &classExternal, volume.AnnBoundByController, volume.AnnBindCompleted))),
			expectedEvents: []string{"Normal ExternalProvisioning"},
			errors:         noerrors,
			test: wrapTestWithInjectedOperation(ctx, wrapTestWithProvisionCalls([]provisionCall{}, testSyncClaim), func(ctrl *PersistentVolumeController, reactor *pvtesting.VolumeReactor) {
				// 在 syncClaim 尝试绑定 PV 之前创建卷，
				// 模拟控制器等待绑定期间外部 provisioner 已创建卷。
				// provisionClaimOperationCSI 会发出 ExternalProvisioning 事件，
				// 并把操作开始时间写入本地缓存 operationTimestamps；
				// 这里以该时间戳是否存在作为创建可绑定 PV 的触发条件。
				if ctrl.operationTimestamps.Has("default/claim12-2") {
					volume := newVolume("pvc-uid12-2", "1Gi", "", "", v1.VolumeAvailable, v1.PersistentVolumeReclaimRetain, classExternal)
					ctrl.volumes.store.Add(volume) // 把卷加入控制器缓存
					reactor.AddVolume(volume)
				}
			}),
		},
		{
			// 外部 provisioner 尚未创建卷，绑定不会发生，仅发出正常事件。
			name:            "12-3 - external provisioner with volume to be provisioned",
			initialVolumes:  novolumes,
			expectedVolumes: novolumes,
			initialClaims:   newClaimArray("claim12-3", "uid12-3", "1Gi", "", v1.ClaimPending, &classExternal),
			expectedClaims: claimWithAnnotation(volume.AnnBetaStorageProvisioner, "vendor.com/my-volume",
				claimWithAnnotation(volume.AnnStorageProvisioner, "vendor.com/my-volume",
					newClaimArray("claim12-3", "uid12-3", "1Gi", "", v1.ClaimPending, &classExternal))),
			expectedEvents: []string{"Normal ExternalProvisioning"},
			errors:         noerrors,
			test:           wrapTestWithProvisionCalls([]provisionCall{provision1Success}, testSyncClaim),
		},
		{
			// 外部 provisioner 创建卷并绑定，同时发出正常事件。
			name:            "12-4 - external provisioner with volume provisioned/bound success",
			initialVolumes:  novolumes,
			expectedVolumes: newVolumeArray("pvc-uid12-4", "1Gi", "uid12-4", "claim12-4", v1.VolumeBound, v1.PersistentVolumeReclaimRetain, classExternal, volume.AnnBoundByController),
			initialClaims:   newClaimArray("claim12-4", "uid12-4", "1Gi", "", v1.ClaimPending, &classExternal),
			expectedClaims: claimWithAnnotation(volume.AnnBetaStorageProvisioner, "vendor.com/my-volume",
				claimWithAnnotation(volume.AnnStorageProvisioner, "vendor.com/my-volume",
					newClaimArray("claim12-4", "uid12-4", "1Gi", "pvc-uid12-4", v1.ClaimBound, &classExternal, volume.AnnBoundByController, volume.AnnBindCompleted))),
			expectedEvents: []string{"Normal ExternalProvisioning"},
			errors:         noerrors,
			test: wrapTestWithInjectedOperation(ctx, wrapTestWithProvisionCalls([]provisionCall{}, testSyncClaim), func(ctrl *PersistentVolumeController, reactor *pvtesting.VolumeReactor) {
				// 在 syncClaim 尝试绑定 PV 之前创建卷，
				// 模拟控制器等待绑定期间外部 provisioner 已创建卷。
				// provisionClaimOperationCSI 会发出 ExternalProvisioning 事件，
				// 并把操作开始时间写入本地缓存 operationTimestamps；
				// 这里以该时间戳是否存在作为创建可绑定 PV 的触发条件。
				if ctrl.operationTimestamps.Has("default/claim12-4") {
					volume := newVolume("pvc-uid12-4", "1Gi", "uid12-4", "claim12-4", v1.VolumeBound, v1.PersistentVolumeReclaimRetain, classExternal, volume.AnnBoundByController)
					ctrl.volumes.store.Add(volume) // 把卷加入控制器缓存
					reactor.AddVolume(volume)
				}
			}),
		},
	}

	runMultisyncTests(t, ctx, tests, storageClasses, storageClasses[0].Name)
}

// TestDisablingDynamicProvisioner 验证关闭动态创建后，处理 claim 会立即返回 nil。
func TestDisablingDynamicProvisioner(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)
	ctrl, err := newTestController(ctx, nil, nil, false)
	if err != nil {
		t.Fatalf("Construct PersistentVolume controller failed: %v", err)
	}
	retVal := ctrl.provisionClaim(ctx, nil)
	if retVal != nil {
		t.Errorf("Expected nil return but got %v", retVal)
	}
}
