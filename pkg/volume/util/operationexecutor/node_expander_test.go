/*
Copyright 2022 The Kubernetes Authors.

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

package operationexecutor

import (
	"sync"
	"testing"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/version"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	featuregatetesting "k8s.io/component-base/featuregate/testing"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/pkg/volume"
	volumetesting "k8s.io/kubernetes/pkg/volume/testing"
	volumetypes "k8s.io/kubernetes/pkg/volume/util/types"
)

// fakeActualStateOfWorld 是 ActualStateOfWorld 的测试替身，记录扩容失败与挂载尝试状态。
type fakeActualStateOfWorld struct {
	volumesWithFinalExpansionErrors sets.Set[v1.UniqueVolumeName]
	attemptedMounts                 map[volumetypes.UniquePodName]sets.Set[v1.UniqueVolumeName]
	sync.RWMutex
}

var _ ActualStateOfWorldMounterUpdater = &fakeActualStateOfWorld{}

// newFakeActualStateOfWorld 构造测试用的 fake ActualStateOfWorld 对象。
func newFakeActualStateOfWorld() *fakeActualStateOfWorld {
	return &fakeActualStateOfWorld{
		volumesWithFinalExpansionErrors: sets.New[v1.UniqueVolumeName](),
		attemptedMounts:                 make(map[volumetypes.UniquePodName]sets.Set[v1.UniqueVolumeName]),
	}
}

// TestNodeExpander 节点扩容逻辑的测试。
func TestNodeExpander(t *testing.T) {
	featuregatetesting.SetFeatureGateEmulationVersionDuringTest(t, utilfeature.DefaultFeatureGate, version.MustParse("1.33"))

	nodeResizeFailed := v1.PersistentVolumeClaimNodeResizeInfeasible

	nodeResizePending := v1.PersistentVolumeClaimNodeResizePending
	var tests = []struct {
		name                          string
		pvc                           *v1.PersistentVolumeClaim
		pv                            *v1.PersistentVolume
		recoverVolumeExpansionFailure bool

		// 期望大小，未指定时默认取 pv.Spec.Capacity
		desiredSize *resource.Quantity
		// 实际大小，未指定时默认取 pvc.Status.Capacity
		actualSize *resource.Quantity

		// 该用例的预期结果
		expectedResizeStatus v1.ClaimResourceStatus
		expectedStatusSize   resource.Quantity
		// 是否向插件发起了扩容调用
		expectResizeCall    bool
		expectFinalErrors   bool
		expectedReturnValue bool

		// 是否假定扩容操作已完成
		assumeResizeOpAsFinished bool
		expectError              bool
	}{
		{
			name:                          "pv.spec.cap > pvc.status.cap, resizeStatus=node_expansion_failed",
			pvc:                           getTestPVC("test-vol0", "2G", "1G", "", &nodeResizeFailed),
			pv:                            getTestPV("test-vol0", "2G"),
			recoverVolumeExpansionFailure: true,

			expectedResizeStatus:     nodeResizeFailed,
			expectResizeCall:         false,
			expectedReturnValue:      false,
			assumeResizeOpAsFinished: true,
			expectFinalErrors:        false,
			expectedStatusSize:       resource.MustParse("1G"),
		},
		{
			name:                          "pv.spec.cap > pvc.status.cap, resizeStatus=node_expansion_pending",
			pvc:                           getTestPVC("test-vol0", "2G", "1G", "2G", &nodeResizePending),
			pv:                            getTestPV("test-vol0", "2G"),
			recoverVolumeExpansionFailure: true,

			expectedResizeStatus:     "",
			expectResizeCall:         true,
			expectedReturnValue:      true,
			assumeResizeOpAsFinished: true,
			expectFinalErrors:        false,
			expectedStatusSize:       resource.MustParse("2G"),
		},
		{
			name:                          "pv.spec.cap > pvc.status.cap, resizeStatus=node_expansion_pending, reize_op=infeasible",
			pvc:                           getTestPVC(volumetesting.InfeasibleNodeExpansion, "2G", "1G", "2G", &nodeResizePending),
			pv:                            getTestPV(volumetesting.InfeasibleNodeExpansion, "2G"),
			recoverVolumeExpansionFailure: false,
			expectError:                   true,
			expectedResizeStatus:          nodeResizeFailed,
			expectResizeCall:              true,
			assumeResizeOpAsFinished:      true,
			expectedReturnValue:           false,
			expectFinalErrors:             true,
			expectedStatusSize:            resource.MustParse("1G"),
		},
		{
			name:                          "pv.spec.cap > pvc.status.cap, resizeStatus=node_expansion_pending, reize_op=failing",
			pvc:                           getTestPVC(volumetesting.OtherFinalNodeExpansionError, "2G", "1G", "2G", &nodeResizePending),
			pv:                            getTestPV(volumetesting.OtherFinalNodeExpansionError, "2G"),
			recoverVolumeExpansionFailure: true,
			expectError:                   true,
			expectedResizeStatus:          v1.PersistentVolumeClaimNodeResizeInProgress,
			expectResizeCall:              true,
			expectedReturnValue:           false,
			assumeResizeOpAsFinished:      true,
			expectFinalErrors:             true,
			expectedStatusSize:            resource.MustParse("1G"),
		},
		{
			name:                          "RWO volumes, pv.spec.cap = pvc.status.cap, resizeStatus='', desiredSize > actualSize",
			pvc:                           getTestPVC("test-vol0", "2G", "2G", "2G", nil),
			pv:                            getTestPV("test-vol0", "2G"),
			recoverVolumeExpansionFailure: false,

			expectedResizeStatus:     "",
			expectResizeCall:         false,
			assumeResizeOpAsFinished: true,
			expectedReturnValue:      true,
			expectFinalErrors:        false,
			expectedStatusSize:       resource.MustParse("2G"),
		},
		{
			name:                          "RWX volumes, pv.spec.cap = pvc.status.cap, resizeStatus='', desiredSize > actualSize",
			pvc:                           addAccessMode(getTestPVC("test-vol0", "2G", "2G", "2G", nil), v1.ReadWriteMany),
			pv:                            getTestPV("test-vol0", "2G"),
			recoverVolumeExpansionFailure: true,

			expectedResizeStatus:     "",
			expectResizeCall:         true,
			assumeResizeOpAsFinished: true,
			expectedReturnValue:      true,
			expectFinalErrors:        false,
			expectedStatusSize:       resource.MustParse("2G"),
		},
		{
			name:                          "RWX pv.spec.cap = pvc.status.cap, resizeStatus='', desiredSize > actualSize, reize_op=unsupported",
			pvc:                           addAccessMode(getTestPVC(volumetesting.FailWithUnSupportedVolumeName, "2G", "2G", "2G", nil), v1.ReadWriteMany),
			pv:                            getTestPV(volumetesting.FailWithUnSupportedVolumeName, "2G"),
			recoverVolumeExpansionFailure: true,
			expectError:                   false,
			expectedResizeStatus:          "",
			expectResizeCall:              false,
			assumeResizeOpAsFinished:      true,
			expectedReturnValue:           true,
			expectFinalErrors:             false,
			expectedStatusSize:            resource.MustParse("2G"),
		},
		{
			name:                          "RWX volumes, pv.spec.cap = pvc.status.cap, resizeStatus='', desiredSize > actualSize, node-expansion-not-required",
			pvc:                           addAnnotation(addAccessMode(getTestPVC("test-vol0", "2G", "2G", "2G", nil), v1.ReadWriteMany), volumetypes.NodeExpansionNotRequired, "true"),
			pv:                            getTestPV("test-vol0", "2G"),
			recoverVolumeExpansionFailure: true,

			expectedResizeStatus:     "",
			expectedReturnValue:      true,
			expectResizeCall:         false,
			assumeResizeOpAsFinished: true,
			expectFinalErrors:        false,
			expectedStatusSize:       resource.MustParse("2G"),
		},
		{
			name:                          "pv.spec.cap > pvc.status.cap, resizeStatus=node_expansion_pending, featuregate=disabled",
			pvc:                           getTestPVC("test-vol0", "2G", "1G", "2G", &nodeResizePending),
			pv:                            getTestPV("test-vol0", "2G"),
			recoverVolumeExpansionFailure: false,

			expectedResizeStatus:     "",
			expectResizeCall:         true,
			assumeResizeOpAsFinished: true,
			expectedReturnValue:      true,
			expectFinalErrors:        false,
			expectedStatusSize:       resource.MustParse("2G"),
		},
	}

	for i := range tests {
		test := tests[i]
		t.Run(test.name, func(t *testing.T) {
			featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.RecoverVolumeExpansionFailure, test.recoverVolumeExpansionFailure)
			volumePluginMgr, fakePlugin := volumetesting.GetTestKubeletVolumePluginMgr(t)

			pvc := test.pvc
			pv := test.pv
			pod := getTestPod("test-pod", pvc.Name)
			og := getTestOperationGenerator(volumePluginMgr, pvc, pv)

			vmt := VolumeToMount{
				Pod:        pod,
				VolumeName: v1.UniqueVolumeName(pv.Name),
				VolumeSpec: volume.NewSpecFromPersistentVolume(pv, false),
			}
			desiredSize := test.desiredSize
			if desiredSize == nil {
				desiredSize = pv.Spec.Capacity.Storage()
			}
			actualSize := test.actualSize
			if actualSize == nil {
				actualSize = pvc.Status.Capacity.Storage()
			}
			asow := newFakeActualStateOfWorld()
			resizeOp := nodeResizeOperationOpts{
				pvc:                pvc,
				pv:                 pv,
				volumePlugin:       fakePlugin,
				vmt:                vmt,
				actualStateOfWorld: asow,
				pluginResizeOpts: volume.NodeResizeOptions{
					VolumeSpec: vmt.VolumeSpec,
					NewSize:    *desiredSize,
					OldSize:    *actualSize,
				},
			}
			ogInstance, _ := og.(*operationGenerator)
			nodeExpander := newNodeExpander(resizeOp, ogInstance.kubeClient, ogInstance.recorder)

			returnValue, _, err := nodeExpander.expandOnPlugin()
			expansionResponse := nodeExpander.testStatus

			pvc = nodeExpander.pvc
			pvcStatusCap := pvc.Status.Capacity[v1.ResourceStorage]

			if !test.expectError && err != nil {
				t.Errorf("For test %s, expected no error got: %v", test.name, err)
			}
			if test.expectError && err == nil {
				t.Errorf("For test %s, expected error but got none", test.name)
			}

			if test.expectedReturnValue != returnValue {
				t.Errorf("For test %s, expected return value %t, got %t", test.name, test.expectedReturnValue, returnValue)
			}

			if test.expectResizeCall != expansionResponse.resizeCalledOnPlugin {
				t.Errorf("For test %s, expected resize called %t, got %t", test.name, test.expectResizeCall, expansionResponse.resizeCalledOnPlugin)
			}
			if test.assumeResizeOpAsFinished != expansionResponse.assumeResizeFinished {
				t.Errorf("For test %s, expected assumeResizeOpAsFinished %t, got %t", test.name, test.assumeResizeOpAsFinished, expansionResponse.assumeResizeFinished)
			}
			allocatedResourceStatus := pvc.Status.AllocatedResourceStatuses
			resizeStatus := allocatedResourceStatus[v1.ResourceStorage]

			if test.expectedResizeStatus != resizeStatus {
				t.Errorf("For test %s, expected resizeStatus %v, got %v", test.name, test.expectedResizeStatus, resizeStatus)
			}

			if test.expectFinalErrors != asow.CheckVolumeInFailedExpansionWithFinalErrors(vmt.VolumeName) {
				t.Errorf("For test %s, expected final errors %t, got %t", test.name, test.expectFinalErrors, !test.expectFinalErrors)
			}
			if pvcStatusCap.Cmp(test.expectedStatusSize) != 0 {
				t.Errorf("For test %s, expected status size %s, got %s", test.name, test.expectedStatusSize.String(), pvcStatusCap.String())
			}
		})
	}
}

// CheckAndMarkDeviceUncertainViaReconstruction 通过重建将设备标记为状态不确定，实现 ActualStateOfWorldMounterUpdater 接口。
func (f *fakeActualStateOfWorld) CheckAndMarkDeviceUncertainViaReconstruction(volumeName v1.UniqueVolumeName, deviceMountPath string) bool {
	panic("unimplemented")
}

// CheckAndMarkVolumeAsUncertainViaReconstruction 通过重建将卷标记为状态不确定，实现 ActualStateOfWorldMounterUpdater 接口。
func (f *fakeActualStateOfWorld) CheckAndMarkVolumeAsUncertainViaReconstruction(opts MarkVolumeOpts) (bool, error) {
	panic("unimplemented")
}

// CheckVolumeInFailedExpansionWithFinalErrors 检查卷是否处于不可恢复的扩容失败状态，实现 ActualStateOfWorldMounterUpdater 接口。
func (f *fakeActualStateOfWorld) CheckVolumeInFailedExpansionWithFinalErrors(volumeName v1.UniqueVolumeName) bool {
	f.RLock()
	defer f.RUnlock()
	return f.volumesWithFinalExpansionErrors.Has(volumeName)
}

// GetDeviceMountState 获取设备挂载状态，实现 ActualStateOfWorldMounterUpdater 接口。
func (f *fakeActualStateOfWorld) GetDeviceMountState(volumeName v1.UniqueVolumeName) DeviceMountState {
	panic("unimplemented")
}

// GetVolumeMountState 获取卷挂载状态，实现 ActualStateOfWorldMounterUpdater 接口。
func (f *fakeActualStateOfWorld) GetVolumeMountState(volumName v1.UniqueVolumeName, podName volumetypes.UniquePodName) VolumeMountState {
	panic("unimplemented")
}

// IsVolumeDeviceReconstructed 判断卷设备是否已重建，实现 ActualStateOfWorldMounterUpdater 接口。
func (f *fakeActualStateOfWorld) IsVolumeDeviceReconstructed(volumeName v1.UniqueVolumeName) bool {
	panic("unimplemented")
}

// IsVolumeMountedElsewhere 判断卷是否挂载在其他 Pod 上，实现 ActualStateOfWorldMounterUpdater 接口。
func (f *fakeActualStateOfWorld) IsVolumeMountedElsewhere(volumeName v1.UniqueVolumeName, podName volumetypes.UniquePodName) bool {
	panic("unimplemented")
}

// IsVolumeReconstructed 判断卷是否已重建，实现 ActualStateOfWorldMounterUpdater 接口。
func (f *fakeActualStateOfWorld) IsVolumeReconstructed(volumeName v1.UniqueVolumeName, podName volumetypes.UniquePodName) bool {
	panic("unimplemented")
}

// MarkDeviceAsMounted 将设备标记为已挂载，实现 ActualStateOfWorldMounterUpdater 接口。
func (f *fakeActualStateOfWorld) MarkDeviceAsMounted(volumeName v1.UniqueVolumeName, devicePath string, deviceMountPath string, seLinuxMountContext string) error {
	panic("unimplemented")
}

// MarkDeviceAsUncertain 将设备标记为状态不确定，实现 ActualStateOfWorldMounterUpdater 接口。
func (f *fakeActualStateOfWorld) MarkDeviceAsUncertain(volumeName v1.UniqueVolumeName, devicePath string, deviceMountPath string, seLinuxMountContext string) error {
	panic("unimplemented")
}

// MarkDeviceAsUnmounted 将设备标记为已卸载，实现 ActualStateOfWorldMounterUpdater 接口。
func (f *fakeActualStateOfWorld) MarkDeviceAsUnmounted(volumeName v1.UniqueVolumeName) error {
	panic("unimplemented")
}

// MarkForInUseExpansionError 标记使用中的扩容错误，实现 ActualStateOfWorldMounterUpdater 接口。
func (f *fakeActualStateOfWorld) MarkForInUseExpansionError(volumeName v1.UniqueVolumeName) {
	panic("unimplemented")
}

// MarkVolumeAsMounted 将卷标记为已挂载，实现 ActualStateOfWorldMounterUpdater 接口。
func (f *fakeActualStateOfWorld) MarkVolumeAsMounted(markVolumeOpts MarkVolumeOpts) error {
	panic("unimplemented")
}

// MarkVolumeAsResized 将卷标记为已扩容，实现 ActualStateOfWorldMounterUpdater 接口。
func (f *fakeActualStateOfWorld) MarkVolumeAsResized(volumeName v1.UniqueVolumeName, claimSize resource.Quantity) bool {
	panic("unimplemented")
}

// MarkVolumeAsUnmounted 将卷标记为已卸载，实现 ActualStateOfWorldMounterUpdater 接口。
func (f *fakeActualStateOfWorld) MarkVolumeAsUnmounted(podName volumetypes.UniquePodName, volumeName v1.UniqueVolumeName) error {
	panic("unimplemented")
}

// MarkVolumeExpansionFailedWithFinalError 将卷扩容标记为不可恢复的失败，实现 ActualStateOfWorldMounterUpdater 接口。
func (f *fakeActualStateOfWorld) MarkVolumeExpansionFailedWithFinalError(volumeName v1.UniqueVolumeName) {
	f.Lock()
	defer f.Unlock()

	f.volumesWithFinalExpansionErrors.Insert(volumeName)
}

// MarkVolumeMountAsUncertain 将卷挂载标记为状态不确定，实现 ActualStateOfWorldMounterUpdater 接口。
func (f *fakeActualStateOfWorld) MarkVolumeMountAsUncertain(markVolumeOpts MarkVolumeOpts) error {
	panic("unimplemented")
}

// RemoveVolumeFromFailedWithFinalErrors 从不可恢复失败集合中移除卷，实现 ActualStateOfWorldMounterUpdater 接口。
func (f *fakeActualStateOfWorld) RemoveVolumeFromFailedWithFinalErrors(volumeName v1.UniqueVolumeName) {
	f.Lock()
	defer f.Unlock()
	f.volumesWithFinalExpansionErrors.Delete(volumeName)
}

// MarkVolumeAsMountAttempted 将卷挂载标记为已尝试，实现 ActualStateOfWorldMounterUpdater 接口。
func (f *fakeActualStateOfWorld) MarkVolumeAsMountAttempted(podName volumetypes.UniquePodName, volumeName v1.UniqueVolumeName) {
	f.Lock()
	defer f.Unlock()
	volumes, ok := f.attemptedMounts[podName]
	if !ok {
		volumes = sets.New[v1.UniqueVolumeName]()
		f.attemptedMounts[podName] = volumes
	}
	volumes.Insert(volumeName)
}

// IsVolumeMountAttempted 判断卷挂载是否已尝试，实现 ActualStateOfWorldMounterUpdater 接口。
func (f *fakeActualStateOfWorld) IsVolumeMountAttempted(podName volumetypes.UniquePodName, volumeName v1.UniqueVolumeName) bool {
	f.RLock()
	defer f.RUnlock()
	volumes, ok := f.attemptedMounts[podName]
	if !ok {
		return false
	}
	return volumes.Has(volumeName)
}
