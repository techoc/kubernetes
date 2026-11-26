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

package testsuites

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/errors"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/util/retry"
	"k8s.io/kubernetes/pkg/features"
	volumeutil "k8s.io/kubernetes/pkg/volume/util"
	"k8s.io/kubernetes/test/e2e/feature"
	"k8s.io/kubernetes/test/e2e/framework"
	e2epod "k8s.io/kubernetes/test/e2e/framework/pod"
	e2epv "k8s.io/kubernetes/test/e2e/framework/pv"
	e2eskipper "k8s.io/kubernetes/test/e2e/framework/skipper"
	e2evolume "k8s.io/kubernetes/test/e2e/framework/volume"
	storageframework "k8s.io/kubernetes/test/e2e/storage/framework"
	admissionapi "k8s.io/pod-security-admission/api"
)

const (
	modifyPollInterval     = 2 * time.Second
	setVACWaitPeriod       = 30 * time.Second
	modifyVolumeWaitPeriod = 10 * time.Minute
	vacCleanupWaitPeriod   = 30 * time.Second
	claimDeletingTimeout   = 3 * time.Minute
)

type volumeModifyTestSuite struct {
	tsInfo storageframework.TestSuiteInfo
}

// InitCustomVolumeModifyTestSuite 返回使用自定义测试模式、实现 TestSuite 接口的 volumeModifyTestSuite

func InitCustomVolumeModifyTestSuite(patterns []storageframework.TestPattern) storageframework.TestSuite {
	return &volumeModifyTestSuite{
		tsInfo: storageframework.TestSuiteInfo{
			Name:         "volume-modify",
			TestPatterns: patterns,
			SupportedSizeRange: e2evolume.SizeRange{
				Min: "1Gi",
			},
			TestTags: []interface{}{framework.WithFeatureGate(features.VolumeAttributesClass), feature.VolumeAttributesClass},
		},
	}
}

// InitVolumeModifyTestSuite 返回使用测试套件默认模式、实现 TestSuite 接口的 volumeModifyTestSuite

func InitVolumeModifyTestSuite() storageframework.TestSuite {
	patterns := []storageframework.TestPattern{
		storageframework.DefaultFsDynamicPV,
		storageframework.BlockVolModeDynamicPV,
		storageframework.NtfsDynamicPV,
	}
	return InitCustomVolumeModifyTestSuite(patterns)
}

func (v *volumeModifyTestSuite) GetTestSuiteInfo() storageframework.TestSuiteInfo {
	return v.tsInfo
}

func (v *volumeModifyTestSuite) SkipUnsupportedTests(driver storageframework.TestDriver, pattern storageframework.TestPattern) string {
	_, ok := driver.(storageframework.VolumeAttributesClassTestDriver)
	if !ok {
		return fmt.Sprintf("Driver %q does not support VolumeAttributesClass tests - skipping", driver.GetDriverInfo().Name)
	}
	// 被测 driver 不支持块卷时跳过块存储测试
	// TODO: 应做成通用逻辑，避免每个使用 BlockVolModeDynamicPV 测试用例的测试都重写一遍
	if !driver.GetDriverInfo().Capabilities[storageframework.CapBlock] && pattern.VolMode == v1.PersistentVolumeBlock {
		return fmt.Sprintf("Driver %q does not support block volume mode - skipping", driver.GetDriverInfo().Name)
	}
	return ""
}

func (v *volumeModifyTestSuite) DefineTests(driver storageframework.TestDriver, pattern storageframework.TestPattern) {
	type local struct {
		config *storageframework.PerTestConfig

		resource *storageframework.VolumeResource
		vac      *storagev1.VolumeAttributesClass
	}
	var l local

	// 注意它还会注册一个 AfterEach，使 f 不可用。任何使用
	// f 的代码都必须在 It 或 Context 回调中运行。
	f := framework.NewFrameworkWithCustomTimeouts("volume-modify", storageframework.GetDriverTimeouts(driver))
	f.NamespacePodSecurityLevel = admissionapi.LevelPrivileged

	init := func(ctx context.Context, createVolumeWithVAC bool) {
		l = local{}

		l.config = driver.PrepareTest(ctx, f)
		vacDriver, _ := driver.(storageframework.VolumeAttributesClassTestDriver)
		l.vac = vacDriver.GetVolumeAttributesClass(ctx, l.config)

		if l.vac == nil {
			e2eskipper.Skipf("Driver %q returned nil VolumeAttributesClass - skipping", driver.GetDriverInfo().Name)
		}

		ginkgo.By("Creating VolumeAttributesClass")
		_, err := f.ClientSet.StorageV1().VolumeAttributesClasses().Create(ctx, l.vac, metav1.CreateOptions{})
		framework.ExpectNoError(err, "While creating VolumeAttributesClass")

		ginkgo.By("Creating volume")
		testVolumeSizeRange := v.GetTestSuiteInfo().SupportedSizeRange
		if createVolumeWithVAC {
			l.resource = storageframework.CreateVolumeResourceWithVAC(ctx, driver, l.config, pattern, testVolumeSizeRange, &l.vac.Name)
		} else {
			l.resource = storageframework.CreateVolumeResource(ctx, driver, l.config, pattern, testVolumeSizeRange)
		}
	}

	cleanup := func(ctx context.Context) {
		var errs []error
		if l.resource != nil {
			ginkgo.By("Deleting VolumeResource")
			errs = append(errs, l.resource.CleanupResource(ctx))
			l.resource = nil
		}

		if l.vac != nil {
			ginkgo.By("Deleting VAC")
			CleanupVAC(ctx, l.vac, f.ClientSet, vacCleanupWaitPeriod)
			l.vac = nil
		}

		framework.ExpectNoError(errors.NewAggregate(errs), "While cleaning up")
	}

	ginkgo.It("should create a volume with VAC", func(ctx context.Context) {
		init(ctx, true /* volume created with VAC */)
		ginkgo.DeferCleanup(cleanup)

		ginkgo.By("Creating a pod with dynamically provisioned volume")
		podConfig := e2epod.Config{
			NS:            f.Namespace.Name,
			PVCs:          []*v1.PersistentVolumeClaim{l.resource.Pvc},
			SeLinuxLabel:  e2epod.GetLinuxLabel(),
			NodeSelection: l.config.ClientNodeSelection,
			ImageID:       e2epod.GetDefaultTestImageID(),
		}
		pod, err := e2epod.CreateSecPodWithNodeSelection(ctx, f.ClientSet, &podConfig, f.Timeouts.PodStart)
		ginkgo.DeferCleanup(e2epod.DeletePodWithWait, f.ClientSet, pod)
		framework.ExpectNoError(err, "While creating test pod with VAC")

		ginkgo.By("Checking PVC status")
		err = e2epv.WaitForPersistentVolumeClaimModified(ctx, f.ClientSet, l.resource.Pvc, modifyVolumeWaitPeriod)
		framework.ExpectNoError(err, "While waiting for PVC to have expected VAC")
	})

	ginkgo.It("should modify volume with no VAC", func(ctx context.Context) {
		init(ctx, false /* volume created without VAC */)
		ginkgo.DeferCleanup(cleanup)

		var err error
		ginkgo.By("Creating a pod with dynamically provisioned volume")
		podConfig := e2epod.Config{
			NS:            f.Namespace.Name,
			PVCs:          []*v1.PersistentVolumeClaim{l.resource.Pvc},
			SeLinuxLabel:  e2epod.GetLinuxLabel(),
			NodeSelection: l.config.ClientNodeSelection,
			ImageID:       e2epod.GetDefaultTestImageID(),
		}
		pod, err := e2epod.CreateSecPodWithNodeSelection(ctx, f.ClientSet, &podConfig, f.Timeouts.PodStart)
		ginkgo.DeferCleanup(e2epod.DeletePodWithWait, f.ClientSet, pod)
		framework.ExpectNoError(err, "While creating pod for modifying")

		ginkgo.By("Modifying PVC via VAC")
		l.resource.Pvc = SetPVCVACName(ctx, l.resource.Pvc, l.vac.Name, f.ClientSet, setVACWaitPeriod)
		gomega.Expect(l.resource.Pvc).NotTo(gomega.BeNil())

		ginkgo.By("Checking PVC status")
		err = e2epv.WaitForPersistentVolumeClaimModified(ctx, f.ClientSet, l.resource.Pvc, modifyVolumeWaitPeriod)
		framework.ExpectNoError(err, "While waiting for PVC to have expected VAC")
	})

	ginkgo.It("should modify volume that already has a VAC", func(ctx context.Context) {
		init(ctx, true /* volume created with VAC */)
		ginkgo.DeferCleanup(cleanup)

		vacDriver, _ := driver.(storageframework.VolumeAttributesClassTestDriver)
		newVAC := vacDriver.GetVolumeAttributesClass(ctx, l.config)
		gomega.Expect(newVAC).NotTo(gomega.BeNil())
		_, err := f.ClientSet.StorageV1().VolumeAttributesClasses().Create(ctx, newVAC, metav1.CreateOptions{})
		framework.ExpectNoError(err, "While creating new VolumeAttributesClass")
		ginkgo.DeferCleanup(CleanupVAC, newVAC, f.ClientSet, vacCleanupWaitPeriod)

		ginkgo.By("Creating a pod with dynamically provisioned volume")
		podConfig := e2epod.Config{
			NS:            f.Namespace.Name,
			PVCs:          []*v1.PersistentVolumeClaim{l.resource.Pvc},
			SeLinuxLabel:  e2epod.GetLinuxLabel(),
			NodeSelection: l.config.ClientNodeSelection,
			ImageID:       e2epod.GetDefaultTestImageID(),
		}
		pod, err := e2epod.CreateSecPodWithNodeSelection(ctx, f.ClientSet, &podConfig, f.Timeouts.PodStart)
		ginkgo.DeferCleanup(e2epod.DeletePodWithWait, f.ClientSet, pod)
		framework.ExpectNoError(err, "While creating pod for modifying")

		ginkgo.By("Modifying PVC via VAC")
		l.resource.Pvc = SetPVCVACName(ctx, l.resource.Pvc, newVAC.Name, f.ClientSet, setVACWaitPeriod)
		gomega.Expect(l.resource.Pvc).NotTo(gomega.BeNil())

		ginkgo.By("Checking PVC status")
		err = e2epv.WaitForPersistentVolumeClaimModified(ctx, f.ClientSet, l.resource.Pvc, modifyVolumeWaitPeriod)
		framework.ExpectNoError(err, "While waiting for PVC to have expected VAC")
	})

	ginkgo.It("should recover from invalid target VAC by updating PVC to new valid VAC", func(ctx context.Context) {
		init(ctx, false /* volume created without VAC */)
		ginkgo.DeferCleanup(cleanup)

		// 用不支持的参数创建 VAC
		invalidVAC := MakeInvalidVAC(l.config)
		_, err := f.ClientSet.StorageV1().VolumeAttributesClasses().Create(ctx, invalidVAC, metav1.CreateOptions{})
		framework.ExpectNoError(err, "While creating new VolumeAttributesClass")
		ginkgo.DeferCleanup(CleanupVAC, invalidVAC, f.ClientSet, vacCleanupWaitPeriod)

		ginkgo.By("Creating a pod with dynamically provisioned volume")
		podConfig := e2epod.Config{
			NS:            f.Namespace.Name,
			PVCs:          []*v1.PersistentVolumeClaim{l.resource.Pvc},
			SeLinuxLabel:  e2epod.GetLinuxLabel(),
			NodeSelection: l.config.ClientNodeSelection,
			ImageID:       e2epod.GetDefaultTestImageID(),
		}
		pod, err := e2epod.CreateSecPodWithNodeSelection(ctx, f.ClientSet, &podConfig, f.Timeouts.PodStart)
		ginkgo.DeferCleanup(e2epod.DeletePodWithWait, f.ClientSet, pod)
		framework.ExpectNoError(err, "While creating pod for modifying")

		ginkgo.By("Attempting to modify PVC via VolumeAttributeClass that contains unsupported parameters")
		newPVC := SetPVCVACName(ctx, l.resource.Pvc, invalidVAC.Name, f.ClientSet, setVACWaitPeriod)
		l.resource.Pvc = newPVC
		gomega.Expect(l.resource.Pvc).NotTo(gomega.BeNil())

		ginkgo.By("Waiting for modification to fail")
		err = e2epv.WaitForPersistentVolumeClaimModificationFailure(ctx, f.ClientSet, l.resource.Pvc, modifyVolumeWaitPeriod)
		framework.ExpectNoError(err, "While waiting for PVC to have conditions")

		ginkgo.By("Modifying PVC to new valid VAC")
		l.resource.Pvc = SetPVCVACName(ctx, l.resource.Pvc, l.vac.Name, f.ClientSet, setVACWaitPeriod)
		gomega.Expect(l.resource.Pvc).NotTo(gomega.BeNil())

		ginkgo.By("Checking PVC status")
		err = e2epv.WaitForPersistentVolumeClaimModified(ctx, f.ClientSet, l.resource.Pvc, modifyVolumeWaitPeriod)
		framework.ExpectNoError(err, "While waiting for PVC to have expected VAC")
	})

	ginkgo.It("should be protected by vac-protection finalizer", func(ctx context.Context) {
		init(ctx, false /* volume created without VAC */)
		ginkgo.DeferCleanup(cleanup)

		vacDriver, _ := driver.(storageframework.VolumeAttributesClassTestDriver)
		newVAC := vacDriver.GetVolumeAttributesClass(ctx, l.config)
		gomega.Expect(newVAC).NotTo(gomega.BeNil())
		createdVAC, err := f.ClientSet.StorageV1().VolumeAttributesClasses().Create(ctx, newVAC, metav1.CreateOptions{})
		framework.ExpectNoError(err, "While creating new VolumeAttributesClass")
		ginkgo.DeferCleanup(CleanupVAC, newVAC, f.ClientSet, vacCleanupWaitPeriod)

		ginkgo.By("Verifying that the vac-protection finalizer is set when it is created")
		gomega.Expect(createdVAC.Finalizers).Should(gomega.ContainElement(volumeutil.VACProtectionFinalizer),
			"vac-protection finalizer was not set for VolumeAttributesClass %q", createdVAC.Name)

		ginkgo.By("Creating a pod with dynamically provisioned volume")
		podConfig := e2epod.Config{
			NS:            f.Namespace.Name,
			PVCs:          []*v1.PersistentVolumeClaim{l.resource.Pvc},
			SeLinuxLabel:  e2epod.GetLinuxLabel(),
			NodeSelection: l.config.ClientNodeSelection,
			ImageID:       e2epod.GetDefaultTestImageID(),
		}
		pod, err := e2epod.CreateSecPodWithNodeSelection(ctx, f.ClientSet, &podConfig, f.Timeouts.PodStart)
		ginkgo.DeferCleanup(e2epod.DeletePodWithWait, f.ClientSet, pod)
		framework.ExpectNoError(err, "While creating pod for modifying")

		ginkgo.By("Modifying PVC via VAC")
		l.resource.Pvc = SetPVCVACName(ctx, l.resource.Pvc, newVAC.Name, f.ClientSet, setVACWaitPeriod)
		gomega.Expect(l.resource.Pvc).NotTo(gomega.BeNil())

		ginkgo.By("Checking PVC status")
		err = e2epv.WaitForPersistentVolumeClaimModified(ctx, f.ClientSet, l.resource.Pvc, modifyVolumeWaitPeriod)
		framework.ExpectNoError(err, "While waiting for PVC to have expected VAC")

		ginkgo.By("Attempting to delete the VolumeAttributesClass should be stuck and the vac-protection finalizer is consistently exists")
		err = f.ClientSet.StorageV1().VolumeAttributesClasses().Delete(ctx, newVAC.Name, metav1.DeleteOptions{})
		framework.ExpectNoError(err, "Failed to delete VolumeAttributesClass %q", newVAC.Name)
		// 检查 finalizer 是否始终存在
		gomega.Consistently(ctx, framework.GetObject(f.ClientSet.StorageV1().VolumeAttributesClasses().Get, createdVAC.Name, metav1.GetOptions{})).
			WithPolling(framework.Poll).WithTimeout(vacCleanupWaitPeriod).Should(gomega.HaveField("Finalizers",
			gomega.ContainElement(volumeutil.VACProtectionFinalizer)), "finalizer %s was unexpectedly removed", volumeutil.VACProtectionFinalizer)

		ginkgo.By("Deleting the pod gracefully")
		err = e2epod.DeletePodWithWait(ctx, f.ClientSet, pod)
		framework.ExpectNoError(err, "failed to delete pod")

		ginkgo.By("Update the PV reclaim policy to retain")
		pvc, err := f.ClientSet.CoreV1().PersistentVolumeClaims(f.Namespace.Name).Get(ctx, l.resource.Pvc.Name, metav1.GetOptions{})
		framework.ExpectNoError(err, "Failed to get PVC %q", l.resource.Pvc.Name)
		pvName := pvc.Spec.VolumeName
		pv, err := f.ClientSet.CoreV1().PersistentVolumes().Get(ctx, pvName, metav1.GetOptions{})
		framework.ExpectNoError(err, "Failed to get PV %q", pvName)
		originPv := pv.DeepCopy()
		pv.Spec.PersistentVolumeReclaimPolicy = v1.PersistentVolumeReclaimRetain
		_, err = f.ClientSet.CoreV1().PersistentVolumes().Update(ctx, pv, metav1.UpdateOptions{})
		ginkgo.DeferCleanup(recoverPvReclaimPolicy, f.ClientSet, originPv)
		framework.ExpectNoError(err, "Failed to update PV %q reclaim policy", pvName)

		// vac_protection_controller 确保存在不被任何 PVC/PV 使用的 VolumeAttributesClass，
		// 且该 VolumeAttributesClass 带有 vac-protection finalizer，因此 PVC 删除后
		// VolumeAttributesClass 不应被删除，因为绑定该 PVC 的 PV（使用同一 vac）被保留。
		ginkgo.By(fmt.Sprintf("Deleting PVC %q to make the vac unused for the PVC", newVAC.Name))
		err = f.ClientSet.CoreV1().PersistentVolumeClaims(f.Namespace.Name).Delete(ctx, l.resource.Pvc.Name, metav1.DeleteOptions{})
		framework.ExpectNoError(err, "Failed to delete PVC %q", l.resource.Pvc.Name)

		ginkgo.By(fmt.Sprintf("Waiting for PVC %q to be fully deleted", l.resource.Pvc.Name))
		gomega.Eventually(ctx, func() bool {
			_, err := f.ClientSet.CoreV1().PersistentVolumeClaims(l.resource.Pvc.Name).Get(ctx, l.resource.Pvc.Name, metav1.GetOptions{})
			return apierrors.IsNotFound(err)
		}).
			WithPolling(framework.Poll).
			WithTimeout(claimDeletingTimeout).
			Should(gomega.BeTrueBecause("pvc unexpectedly still exists"))

		ginkgo.By(fmt.Sprintf("Waiting for PV %q to be released", pvName))
		gomega.Eventually(func() v1.PersistentVolumePhase {
			pv, err := f.ClientSet.CoreV1().PersistentVolumes().Get(ctx, pvName, metav1.GetOptions{})
			if err != nil {
				framework.Logf("Failed to get PV %q: %v", pvName, err)
				return ""
			}
			return pv.Status.Phase
		}).
			WithPolling(framework.Poll).
			WithTimeout(claimDeletingTimeout).
			Should(gomega.Equal(v1.VolumeReleased))

		ginkgo.By(fmt.Sprintf("Checking the vac-protection finalizer is still consistently exists on VolumeAttributesClass %q", newVAC.Name))
		gomega.Consistently(ctx, framework.GetObject(f.ClientSet.StorageV1().VolumeAttributesClasses().Get, createdVAC.Name, metav1.GetOptions{})).
			WithPolling(framework.Poll).
			WithTimeout(vacCleanupWaitPeriod).
			Should(gomega.HaveField("Finalizers",
				gomega.ContainElement(volumeutil.VACProtectionFinalizer)), "finalizer %s was unexpectedly removed", volumeutil.VACProtectionFinalizer)

		ginkgo.By(fmt.Sprintf("Deleting PV %q to make the vac unused for the PV", newVAC.Name))
		pv.Spec.PersistentVolumeReclaimPolicy = v1.PersistentVolumeReclaimDelete
		recoverPvReclaimPolicy(ctx, f.ClientSet, pv)

		ginkgo.By(fmt.Sprintf("Waiting for PV %q to be deleted", pvName))
		gomega.Eventually(func() bool {
			_, err := f.ClientSet.CoreV1().PersistentVolumes().Get(ctx, pvName, metav1.GetOptions{})
			return apierrors.IsNotFound(err)
		}).
			WithPolling(framework.Poll).
			WithTimeout(claimDeletingTimeout).
			Should(gomega.BeTrueBecause("pv unexpectedly still exists"))

		ginkgo.By(fmt.Sprintf("Confirming final deletion of VolumeAttributesClass %q", newVAC.Name))
		gomega.Eventually(ctx, func(ctx context.Context) bool {
			_, err := f.ClientSet.StorageV1().VolumeAttributesClasses().Get(ctx, newVAC.Name, metav1.GetOptions{})
			return apierrors.IsNotFound(err)
		}).
			WithPolling(framework.Poll).
			WithTimeout(vacCleanupWaitPeriod).
			Should(gomega.BeTrueBecause("vac unexpectedly still exists"))

	})

}

// SetPVCVACName 在 PVC 对象上设置 VolumeAttributesClassName
func SetPVCVACName(ctx context.Context, origPVC *v1.PersistentVolumeClaim, name string, c clientset.Interface, timeout time.Duration) *v1.PersistentVolumeClaim {
	pvcName := origPVC.Name
	var patchedPVC *v1.PersistentVolumeClaim

	gomega.Eventually(ctx, func() error {
		var err error
		patch := []map[string]interface{}{{"op": "replace", "path": "/spec/volumeAttributesClassName", "value": name}}
		patchBytes, _ := json.Marshal(patch)

		patchedPVC, err = c.CoreV1().PersistentVolumeClaims(origPVC.Namespace).Patch(ctx, pvcName, types.JSONPatchType, patchBytes, metav1.PatchOptions{})
		return err
	}, timeout, modifyPollInterval).Should(gomega.Succeed())

	return patchedPVC
}

// MakeInvalidVAC 创建带无效参数的 VolumeAttributesClass
func MakeInvalidVAC(config *storageframework.PerTestConfig) *storagev1.VolumeAttributesClass {
	return storageframework.CopyVolumeAttributesClass(&storagev1.VolumeAttributesClass{
		DriverName: config.GetUniqueDriverName(),
		Parameters: map[string]string{
			"xxInvalidParameterKey": "xxInvalidParameterValue",
		},
	}, config.Framework.Namespace.Name, "e2e-vac-invalid")
}

// CleanupVAC 清理测试用 VolumeAttributesClass
func CleanupVAC(ctx context.Context, vac *storagev1.VolumeAttributesClass, c clientset.Interface, timeout time.Duration) {
	gomega.Eventually(ctx, func() error {
		err := c.StorageV1().VolumeAttributesClasses().Delete(ctx, vac.Name, metav1.DeleteOptions{})
		if apierrors.IsNotFound(err) {
			framework.Logf("VolumeAttributesClass %q is already cleaned up", vac.Name)
			return nil
		}
		return err
	}, timeout, modifyPollInterval).Should(gomega.BeNil())
}

// recoverPvReclaimPolicy 把测试 PV 的 reclaim policy 恢复为清理测试 PV 时预期的值
func recoverPvReclaimPolicy(ctx context.Context, c clientset.Interface, expectedPv *v1.PersistentVolume) {
	setPvReclaimPolicyErr := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		pv, err := c.CoreV1().PersistentVolumes().Get(ctx, expectedPv.Name, metav1.GetOptions{})
		if err != nil {
			if apierrors.IsNotFound(err) {
				framework.Logf("PV %q is already cleaned up", expectedPv.Name)
				return nil
			}
			return err
		}
		if pv.Spec.PersistentVolumeReclaimPolicy == expectedPv.Spec.PersistentVolumeReclaimPolicy {
			framework.Logf("PV %q reclaim policy is already recovered to %q", expectedPv.Name, expectedPv.Spec.PersistentVolumeReclaimPolicy)
			return nil
		}
		pv.Spec.PersistentVolumeReclaimPolicy = expectedPv.Spec.PersistentVolumeReclaimPolicy
		_, err = c.CoreV1().PersistentVolumes().Update(ctx, pv, metav1.UpdateOptions{})
		return err
	})
	framework.ExpectNoError(setPvReclaimPolicyErr, "Failed to set PV %q reclaim policy to %q", expectedPv.Name, expectedPv.Spec.PersistentVolumeReclaimPolicy)
}
