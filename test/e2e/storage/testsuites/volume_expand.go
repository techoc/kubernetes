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

package testsuites

import (
	"context"
	"fmt"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"

	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/apimachinery/pkg/util/wait"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
	e2epod "k8s.io/kubernetes/test/e2e/framework/pod"
	e2eskipper "k8s.io/kubernetes/test/e2e/framework/skipper"
	e2evolume "k8s.io/kubernetes/test/e2e/framework/volume"
	storageframework "k8s.io/kubernetes/test/e2e/storage/framework"
	admissionapi "k8s.io/pod-security-admission/api"
)

const (
	resizePollInterval = 2 * time.Second
	// 等待云 provider 或文件系统扩容完成的总时间
	totalResizeWaitPeriod = 10 * time.Minute

	// resizedPodStartupTimeout 定义使用离线扩容卷的 Pod 启动时应等待的时间。
	// 该时间比默认 PodStartTimeout 长，因为分离再挂载卷的耗时
	// 通常包含在这段时间内。
	resizedPodStartupTimeout = 10 * time.Minute

	// 等待 PVC 条件同步的时间
	pvcConditionSyncPeriod = 2 * time.Minute
)

type volumeExpandTestSuite struct {
	tsInfo storageframework.TestSuiteInfo
}

// InitCustomVolumeExpandTestSuite 返回使用自定义测试模式、实现 TestSuite 接口的 volumeExpandTestSuite

func InitCustomVolumeExpandTestSuite(patterns []storageframework.TestPattern) storageframework.TestSuite {
	return &volumeExpandTestSuite{
		tsInfo: storageframework.TestSuiteInfo{
			Name:         "volume-expand",
			TestPatterns: patterns,
			SupportedSizeRange: e2evolume.SizeRange{
				Min: "1Gi",
			},
		},
	}
}

// InitVolumeExpandTestSuite 返回使用测试套件默认模式、实现 TestSuite 接口的 volumeExpandTestSuite

func InitVolumeExpandTestSuite() storageframework.TestSuite {
	patterns := []storageframework.TestPattern{
		storageframework.DefaultFsDynamicPV,
		storageframework.XfsDynamicPV,
		storageframework.BlockVolModeDynamicPV,
		storageframework.DefaultFsDynamicPVAllowExpansion,
		storageframework.BlockVolModeDynamicPVAllowExpansion,
		storageframework.NtfsDynamicPV,
		storageframework.NtfsDynamicPVAllowExpansion,
	}
	return InitCustomVolumeExpandTestSuite(patterns)
}

func (v *volumeExpandTestSuite) GetTestSuiteInfo() storageframework.TestSuiteInfo {
	return v.tsInfo
}

func (v *volumeExpandTestSuite) SkipUnsupportedTests(driver storageframework.TestDriver, pattern storageframework.TestPattern) string {
	// 检查前置条件。
	if !driver.GetDriverInfo().Capabilities[storageframework.CapControllerExpansion] {
		return fmt.Sprintf("Driver %q does not support volume expansion", driver.GetDriverInfo().Name)
	}
	// 检查前置条件。
	if !driver.GetDriverInfo().Capabilities[storageframework.CapBlock] && pattern.VolMode == v1.PersistentVolumeBlock {
		return fmt.Sprintf("Driver %q does not support block volume mode", driver.GetDriverInfo().Name)
	}
	return ""
}

func (v *volumeExpandTestSuite) DefineTests(driver storageframework.TestDriver, pattern storageframework.TestPattern) {
	type local struct {
		config *storageframework.PerTestConfig

		resource *storageframework.VolumeResource
		pod      *v1.Pod
		pod2     *v1.Pod

		migrationCheck *migrationOpCheck
	}
	var l local

	// 注意它还会注册一个 AfterEach，使 f 不可用。任何使用
	// f 的代码都必须在 It 或 Context 回调中运行。
	f := framework.NewFrameworkWithCustomTimeouts("volume-expand", storageframework.GetDriverTimeouts(driver))
	f.NamespacePodSecurityLevel = admissionapi.LevelPrivileged

	driverSizeRange := driver.GetDriverInfo().SupportedSizeRange
	expandSize := resource.MustParse("1Gi")
	if driverSizeRange.Step != "" {
		expandSize = resource.MustParse(driverSizeRange.Step)
	}

	init := func(ctx context.Context) {
		l = local{}

		// 现在做开销更大的测试初始化。
		l.config = driver.PrepareTest(ctx, f)
		l.migrationCheck = newMigrationOpCheck(ctx, f.ClientSet, f.ClientConfig(), driver.GetDriverInfo().InTreePluginName)
		testVolumeSizeRange := v.GetTestSuiteInfo().SupportedSizeRange
		l.resource = storageframework.CreateVolumeResource(ctx, driver, l.config, pattern, testVolumeSizeRange)
	}

	cleanup := func(ctx context.Context) {
		var errs []error
		if l.pod != nil {
			ginkgo.By("Deleting pod")
			err := e2epod.DeletePodWithWait(ctx, f.ClientSet, l.pod)
			errs = append(errs, err)
			l.pod = nil
		}

		if l.pod2 != nil {
			ginkgo.By("Deleting pod2")
			err := e2epod.DeletePodWithWait(ctx, f.ClientSet, l.pod2)
			errs = append(errs, err)
			l.pod2 = nil
		}

		if l.resource != nil {
			errs = append(errs, l.resource.CleanupResource(ctx))
			l.resource = nil
		}

		framework.ExpectNoError(errors.NewAggregate(errs), "while cleaning up resource")
		l.migrationCheck.validateMigrationVolumeOpCounts(ctx)
	}

	if !pattern.AllowExpansion {
		ginkgo.It("should not allow expansion of pvcs without AllowVolumeExpansion property", func(ctx context.Context) {
			init(ctx)
			ginkgo.DeferCleanup(cleanup)

			var err error
			// 用 pvc 创建 Pod
			ginkgo.By("Creating a pod with PVC")
			podConfig := e2epod.Config{
				NS:            f.Namespace.Name,
				PVCs:          []*v1.PersistentVolumeClaim{l.resource.Pvc},
				SeLinuxLabel:  e2epod.GetLinuxLabel(),
				NodeSelection: l.config.ClientNodeSelection,
				ImageID:       e2epod.GetDefaultTestImageID(),
			}
			l.pod, err = e2epod.CreateSecPodWithNodeSelection(ctx, f.ClientSet, &podConfig, f.Timeouts.PodStart)
			ginkgo.DeferCleanup(e2epod.DeletePodWithWait, f.ClientSet, l.pod)
			framework.ExpectNoError(err, "While creating pods for expanding")

			// 等待 Pod 运行
			ginkgo.By("Waiting for pod to run")
			err = e2epod.WaitTimeoutForPodRunningInNamespace(ctx, f.ClientSet, l.pod.Name, l.pod.Namespace, f.Timeouts.PodStart)
			framework.ExpectNoError(err)

			gomega.Expect(l.resource.Sc.AllowVolumeExpansion).NotTo(gomega.BeNil())
			allowVolumeExpansion := *l.resource.Sc.AllowVolumeExpansion
			gomega.Expect(allowVolumeExpansion).To(gomega.BeFalseBecause("expected AllowVolumeExpansion value to be false"))

			// 重新获取 PVC，拿到带更新容量的最新状态
			ginkgo.By("Re-fetching PVC to get latest status")
			l.resource.Pvc, err = f.ClientSet.CoreV1().PersistentVolumeClaims(l.resource.Pvc.Namespace).Get(ctx, l.resource.Pvc.Name, metav1.GetOptions{})
			framework.ExpectNoError(err, "While re-fetching PVC before expansion")

			ginkgo.By("Expanding non-expandable pvc")
			// 用 Status.Capacity 而不是 Spec.Resources.Requests 获取实际分配大小。
			// CSI driver 可能分配比请求更大的卷（例如受最小尺寸约束）。
			currentPvcSize := l.resource.Pvc.Status.Capacity[v1.ResourceStorage]
			newSize := currentPvcSize.DeepCopy()
			newSize.Add(expandSize)
			framework.Logf("currentPvcSize %v, newSize %v", currentPvcSize, newSize)
			_, err = ExpandPVCSizeToError(ctx, l.resource.Pvc, newSize, f.ClientSet)
			gomega.Expect(err).To(gomega.MatchError(apierrors.IsForbidden, "While updating non-expandable PVC"))
		})
	} else {
		ginkgo.It("Verify if offline PVC expansion works", func(ctx context.Context) {
			init(ctx)
			ginkgo.DeferCleanup(cleanup)

			if !driver.GetDriverInfo().Capabilities[storageframework.CapOfflineExpansion] {
				e2eskipper.Skipf("Driver %q does not support offline volume expansion - skipping", driver.GetDriverInfo().Name)
			}

			var err error
			ginkgo.By("Creating a pod with dynamically provisioned volume")
			podConfig := e2epod.Config{
				NS:            f.Namespace.Name,
				PVCs:          []*v1.PersistentVolumeClaim{l.resource.Pvc},
				SeLinuxLabel:  e2epod.GetLinuxLabel(),
				NodeSelection: l.config.ClientNodeSelection,
				ImageID:       e2epod.GetDefaultTestImageID(),
			}
			l.pod, err = e2epod.CreateSecPodWithNodeSelection(ctx, f.ClientSet, &podConfig, f.Timeouts.PodStart)
			ginkgo.DeferCleanup(e2epod.DeletePodWithWait, f.ClientSet, l.pod)
			framework.ExpectNoError(err, "While creating pods for resizing")

			ginkgo.By("Deleting the previously created pod")
			err = e2epod.DeletePodWithWait(ctx, f.ClientSet, l.pod)
			framework.ExpectNoError(err, "while deleting pod for resizing")

			// 重新获取 PVC，拿到带更新容量的最新状态
			ginkgo.By("Re-fetching PVC to get latest status")
			l.resource.Pvc, err = f.ClientSet.CoreV1().PersistentVolumeClaims(l.resource.Pvc.Namespace).Get(ctx, l.resource.Pvc.Name, metav1.GetOptions{})
			framework.ExpectNoError(err, "While re-fetching PVC before expansion")

			// 在没有 Pod 使用 PVC 时扩容，确保是离线扩容
			ginkgo.By("Expanding current pvc")
			// 用 Status.Capacity 而不是 Spec.Resources.Requests 获取实际分配大小。
			// CSI driver 可能分配比请求更大的卷（例如受最小尺寸约束）。
			currentPvcSize := l.resource.Pvc.Status.Capacity[v1.ResourceStorage]
			newSize := currentPvcSize.DeepCopy()
			newSize.Add(expandSize)
			framework.Logf("currentPvcSize %v, newSize %v", currentPvcSize, newSize)
			newPVC, err := ExpandPVCSize(ctx, l.resource.Pvc, newSize, f.ClientSet)
			framework.ExpectNoError(err, "While updating pvc for more size")
			l.resource.Pvc = newPVC
			gomega.Expect(l.resource.Pvc).NotTo(gomega.BeNil())

			pvcSize := l.resource.Pvc.Spec.Resources.Requests[v1.ResourceStorage]
			if pvcSize.Cmp(newSize) != 0 {
				framework.Failf("error updating pvc size %q", l.resource.Pvc.Name)
			}

			ginkgo.By("Waiting for cloudprovider resize to finish")
			err = WaitForControllerVolumeResize(ctx, l.resource.Pvc, f.ClientSet, totalResizeWaitPeriod)
			framework.ExpectNoError(err, "While waiting for pvc resize to finish")

			ginkgo.By("Checking for conditions on pvc")
			npvc, err := WaitForPendingFSResizeCondition(ctx, l.resource.Pvc, f.ClientSet)
			framework.ExpectNoError(err, "While waiting for pvc to have fs resizing condition")
			l.resource.Pvc = npvc

			ginkgo.By("Verifying allocatedResources on PVC")
			err = verifyOfflineAllocatedResources(l.resource.Pvc, pvcSize)
			framework.ExpectNoError(err, "While verifying allocatedResources on PVC")

			ginkgo.By("Creating a new pod with same volume")
			podConfig = e2epod.Config{
				NS:            f.Namespace.Name,
				PVCs:          []*v1.PersistentVolumeClaim{l.resource.Pvc},
				SeLinuxLabel:  e2epod.GetLinuxLabel(),
				NodeSelection: l.config.ClientNodeSelection,
				ImageID:       e2epod.GetDefaultTestImageID(),
			}
			l.pod2, err = e2epod.CreateSecPodWithNodeSelection(ctx, f.ClientSet, &podConfig, resizedPodStartupTimeout)
			ginkgo.DeferCleanup(e2epod.DeletePodWithWait, f.ClientSet, l.pod2)
			framework.ExpectNoError(err, "while recreating pod for resizing")

			ginkgo.By("Waiting for file system resize to finish")
			l.resource.Pvc, err = WaitForFSResize(ctx, l.resource.Pvc, f.ClientSet)
			framework.ExpectNoError(err, "while waiting for fs resize to finish")

			pvcConditions := l.resource.Pvc.Status.Conditions
			ExpectNoResizeConditions(pvcConditions)
			err = VerifyRecoveryRelatedFields(l.resource.Pvc)
			framework.ExpectNoError(err, "while verifying recovery related fields")
		})

		ginkgo.It("should resize volume when PVC is edited while pod is using it", func(ctx context.Context) {
			init(ctx)
			ginkgo.DeferCleanup(cleanup)

			if !driver.GetDriverInfo().Capabilities[storageframework.CapOnlineExpansion] {
				e2eskipper.Skipf("Driver %q does not support online volume expansion - skipping", driver.GetDriverInfo().Name)
			}

			var err error
			ginkgo.By("Creating a pod with dynamically provisioned volume")
			podConfig := e2epod.Config{
				NS:            f.Namespace.Name,
				PVCs:          []*v1.PersistentVolumeClaim{l.resource.Pvc},
				SeLinuxLabel:  e2epod.GetLinuxLabel(),
				NodeSelection: l.config.ClientNodeSelection,
				ImageID:       e2epod.GetDefaultTestImageID(),
			}
			l.pod, err = e2epod.CreateSecPodWithNodeSelection(ctx, f.ClientSet, &podConfig, f.Timeouts.PodStart)
			ginkgo.DeferCleanup(e2epod.DeletePodWithWait, f.ClientSet, l.pod)
			framework.ExpectNoError(err, "While creating pods for resizing")

			// 重新获取 PVC，拿到带更新容量的最新状态
			ginkgo.By("Re-fetching PVC to get latest status")
			l.resource.Pvc, err = f.ClientSet.CoreV1().PersistentVolumeClaims(l.resource.Pvc.Namespace).Get(ctx, l.resource.Pvc.Name, metav1.GetOptions{})
			framework.ExpectNoError(err, "While re-fetching PVC before expansion")

			// l.pod 仍在使用 PVC 时扩容，以测试在线扩容。
			ginkgo.By("Expanding current pvc")
			// 用 Status.Capacity 而不是 Spec.Resources.Requests 获取实际分配大小。
			// CSI driver 可能分配比请求更大的卷（例如受最小尺寸约束）。
			currentPvcSize := l.resource.Pvc.Status.Capacity[v1.ResourceStorage]
			newSize := currentPvcSize.DeepCopy()
			newSize.Add(expandSize)
			framework.Logf("currentPvcSize %v, newSize %v", currentPvcSize, newSize)
			newPVC, err := ExpandPVCSize(ctx, l.resource.Pvc, newSize, f.ClientSet)
			framework.ExpectNoError(err, "While updating pvc for more size")
			l.resource.Pvc = newPVC
			gomega.Expect(l.resource.Pvc).NotTo(gomega.BeNil())

			pvcSize := l.resource.Pvc.Spec.Resources.Requests[v1.ResourceStorage]
			if pvcSize.Cmp(newSize) != 0 {
				framework.Failf("error updating pvc size %q", l.resource.Pvc.Name)
			}

			ginkgo.By("Waiting for cloudprovider resize to finish")
			err = WaitForControllerVolumeResize(ctx, l.resource.Pvc, f.ClientSet, totalResizeWaitPeriod)
			framework.ExpectNoError(err, "While waiting for pvc resize to finish")

			ginkgo.By("Waiting for file system resize to finish")
			l.resource.Pvc, err = WaitForFSResize(ctx, l.resource.Pvc, f.ClientSet)
			framework.ExpectNoError(err, "while waiting for fs resize to finish")

			pvcConditions := l.resource.Pvc.Status.Conditions
			ExpectNoResizeConditions(pvcConditions)

			err = VerifyRecoveryRelatedFields(l.resource.Pvc)
			framework.ExpectNoError(err, "while verifying recovery related fields")
		})

		ginkgo.It("should resize volume when PVC is edited and the pod is re-created on the same node after controller resize is finished", func(ctx context.Context) {
			init(ctx)
			ginkgo.DeferCleanup(cleanup)

			if !driver.GetDriverInfo().Capabilities[storageframework.CapOnlineExpansion] {
				e2eskipper.Skipf("Driver %q does not support online volume expansion - skipping", driver.GetDriverInfo().Name)
			}

			var err error
			ginkgo.By("Creating a pod with dynamically provisioned volume")
			podConfig := e2epod.Config{
				NS:            f.Namespace.Name,
				PVCs:          []*v1.PersistentVolumeClaim{l.resource.Pvc},
				SeLinuxLabel:  e2epod.GetLinuxLabel(),
				NodeSelection: l.config.ClientNodeSelection,
				ImageID:       e2epod.GetDefaultTestImageID(),
			}
			l.pod, err = e2epod.CreateSecPodWithNodeSelection(ctx, f.ClientSet, &podConfig, f.Timeouts.PodStart)
			ginkgo.DeferCleanup(e2epod.DeletePodWithWait, f.ClientSet, l.pod)
			framework.ExpectNoError(err, "While creating pods for resizing")

			// 重新获取 PVC，拿到带更新容量的最新状态
			ginkgo.By("Re-fetching PVC to get latest status")
			l.resource.Pvc, err = f.ClientSet.CoreV1().PersistentVolumeClaims(l.resource.Pvc.Namespace).Get(ctx, l.resource.Pvc.Name, metav1.GetOptions{})
			framework.ExpectNoError(err, "While re-fetching PVC before expansion")

			// l.pod 仍在使用 PVC 时扩容，以测试在线扩容。
			ginkgo.By("Expanding current pvc")
			// 用 Status.Capacity 而不是 Spec.Resources.Requests 获取实际分配大小。
			// CSI driver 可能分配比请求更大的卷（例如受最小尺寸约束）。
			currentPvcSize := l.resource.Pvc.Status.Capacity[v1.ResourceStorage]
			newSize := currentPvcSize.DeepCopy()
			newSize.Add(expandSize)
			framework.Logf("currentPvcSize %v, newSize %v", currentPvcSize, newSize)
			newPVC, err := ExpandPVCSize(ctx, l.resource.Pvc, newSize, f.ClientSet)
			framework.ExpectNoError(err, "While updating pvc for more size")
			l.resource.Pvc = newPVC
			gomega.Expect(l.resource.Pvc).NotTo(gomega.BeNil())

			pvcSize := l.resource.Pvc.Spec.Resources.Requests[v1.ResourceStorage]
			if pvcSize.Cmp(newSize) != 0 {
				framework.Failf("error updating pvc size %q", l.resource.Pvc.Name)
			}

			ginkgo.By("Waiting for cloudprovider resize to finish")
			err = WaitForControllerVolumeResize(ctx, l.resource.Pvc, f.ClientSet, totalResizeWaitPeriod)
			framework.ExpectNoError(err, "While waiting for pvc resize to finish")

			ginkgo.By("Deleting the pod")
			nodeName := l.pod.Spec.NodeName
			err = e2epod.DeletePodWithWait(ctx, f.ClientSet, l.pod)
			framework.ExpectNoError(err, "while deleting pod for resizing")
			l.pod = nil

			ginkgo.By("Creating a new pod with same volume on the same node")
			// 把 Pod 固定在同一节点，是因为不希望它删除后
			// 被移到其他节点。让 Pod 留在原节点才能复现
			// 扩容时卷可能已被挂载的场景。
			// 应考虑添加一个能更好覆盖此场景的单元测试。
			// 从 driver 的 ClientNodeSelection 开始（而不是裸 {Name: nodeName}），
			// 以便保留调度 Pod 所需的 selector/affinity，例如 Windows 上的 OS 节点 selector。
			// 然后加上 affinity，把 Pod 保持在原节点。
			pod2NodeSelection := l.config.ClientNodeSelection
			e2epod.SetAffinity(&pod2NodeSelection, nodeName)
			podConfig = e2epod.Config{
				NS:            f.Namespace.Name,
				PVCs:          []*v1.PersistentVolumeClaim{l.resource.Pvc},
				SeLinuxLabel:  e2epod.GetLinuxLabel(),
				NodeSelection: pod2NodeSelection,
				ImageID:       e2epod.GetDefaultTestImageID(),
			}
			l.pod2, err = e2epod.CreateSecPodWithNodeSelection(ctx, f.ClientSet, &podConfig, f.Timeouts.PodStart)
			ginkgo.DeferCleanup(e2epod.DeletePodWithWait, f.ClientSet, l.pod2)
			framework.ExpectNoError(err, "while creating pod for resizing")

			ginkgo.By("Waiting for file system resize to finish")
			l.resource.Pvc, err = WaitForFSResize(ctx, l.resource.Pvc, f.ClientSet)
			framework.ExpectNoError(err, "while waiting for fs resize to finish")

			pvcConditions := l.resource.Pvc.Status.Conditions
			ExpectNoResizeConditions(pvcConditions)

			err = VerifyRecoveryRelatedFields(l.resource.Pvc)
			framework.ExpectNoError(err, "while verifying recovery related fields")
		})
	}
}

// ExpandPVCSize 扩大 PVC 大小
func ExpandPVCSize(ctx context.Context, origPVC *v1.PersistentVolumeClaim, size resource.Quantity, c clientset.Interface) (*v1.PersistentVolumeClaim, error) {
	pvcName := origPVC.Name
	updatedPVC := origPVC.DeepCopy()

	// 出错时重试更新，直到超时。
	// TODO: 确定这里是否适合"带超时的重试"。也许只应在版本冲突时重试。
	var lastUpdateError error
	waitErr := wait.PollUntilContextTimeout(ctx, resizePollInterval, 30*time.Second, true, func(pollContext context.Context) (bool, error) {
		var err error
		updatedPVC, err = c.CoreV1().PersistentVolumeClaims(origPVC.Namespace).Get(pollContext, pvcName, metav1.GetOptions{})
		if err != nil {
			return false, fmt.Errorf("error fetching pvc %q for resizing: %w", pvcName, err)
		}

		updatedPVC.Spec.Resources.Requests[v1.ResourceStorage] = size
		updatedPVC, err = c.CoreV1().PersistentVolumeClaims(origPVC.Namespace).Update(pollContext, updatedPVC, metav1.UpdateOptions{})
		if err != nil {
			framework.Logf("Error updating pvc %s: %v", pvcName, err)
			lastUpdateError = err
			return false, nil
		}
		return true, nil
	})
	if wait.Interrupted(waitErr) {
		return nil, fmt.Errorf("timed out attempting to update PVC size. last update error: %w", lastUpdateError)
	}
	if waitErr != nil {
		return nil, fmt.Errorf("failed to expand PVC size (check logs for error): %v", waitErr)
	}
	return updatedPVC, nil
}

func ExpandPVCSizeToError(ctx context.Context, origPVC *v1.PersistentVolumeClaim, size resource.Quantity, c clientset.Interface) (*v1.PersistentVolumeClaim, error) {
	pvcName := origPVC.Name
	updatedPVC := origPVC.DeepCopy()

	var lastUpdateError error

	waitErr := wait.PollUntilContextTimeout(ctx, resizePollInterval, 30*time.Second, true, func(pollContext context.Context) (bool, error) {
		var err error
		updatedPVC, err = c.CoreV1().PersistentVolumeClaims(origPVC.Namespace).Get(pollContext, pvcName, metav1.GetOptions{})
		if err != nil {
			return false, fmt.Errorf("error fetching pvc %q for resizing: %w", pvcName, err)
		}

		updatedPVC.Spec.Resources.Requests[v1.ResourceStorage] = size
		updatedPVC, err = c.CoreV1().PersistentVolumeClaims(origPVC.Namespace).Update(pollContext, updatedPVC, metav1.UpdateOptions{})
		if err == nil {
			return false, fmt.Errorf("pvc %s should not be allowed to be updated", pvcName)
		} else {
			lastUpdateError = err
			if apierrors.IsForbidden(err) {
				return true, nil
			}
			framework.Logf("Error updating pvc %s: %v", pvcName, err)
			return false, nil
		}
	})

	if wait.Interrupted(waitErr) {
		return nil, fmt.Errorf("timed out attempting to update PVC size. last update error: %w", lastUpdateError)
	}
	if waitErr != nil {
		return nil, fmt.Errorf("failed to expand PVC size (check logs for error): %w", waitErr)
	}
	return updatedPVC, lastUpdateError
}

// WaitForResizingCondition 等待 PVC 条件变为 PersistentVolumeClaimResizing
func WaitForResizingCondition(ctx context.Context, pvc *v1.PersistentVolumeClaim, c clientset.Interface, duration time.Duration) error {
	waitErr := wait.PollUntilContextTimeout(ctx, resizePollInterval, duration, true, func(ctx context.Context) (bool, error) {
		var err error
		updatedPVC, err := c.CoreV1().PersistentVolumeClaims(pvc.Namespace).Get(ctx, pvc.Name, metav1.GetOptions{})

		if err != nil {
			return false, fmt.Errorf("error fetching pvc %q for checking for resize status: %w", pvc.Name, err)
		}

		pvcConditions := updatedPVC.Status.Conditions
		if len(pvcConditions) > 0 {
			if pvcConditions[0].Type == v1.PersistentVolumeClaimResizing {
				return true, nil
			}
		}
		return false, nil
	})
	if waitErr != nil {
		return fmt.Errorf("error waiting for pvc %q to have resize status: %v", pvc.Name, waitErr)
	}
	return nil
}

// WaitForControllerVolumeResize 等待 controller 扩容完成
func WaitForControllerVolumeResize(ctx context.Context, pvc *v1.PersistentVolumeClaim, c clientset.Interface, timeout time.Duration) error {
	pvName := pvc.Spec.VolumeName
	waitErr := wait.PollUntilContextTimeout(ctx, resizePollInterval, timeout, true, func(ctx context.Context) (bool, error) {
		pvcSize := pvc.Spec.Resources.Requests[v1.ResourceStorage]

		pv, err := c.CoreV1().PersistentVolumes().Get(ctx, pvName, metav1.GetOptions{})
		if err != nil {
			return false, fmt.Errorf("error fetching pv %q for resizing %v", pvName, err)
		}

		pvSize := pv.Spec.Capacity[v1.ResourceStorage]

		// pv 大小大于等于请求大小即表示 controller 扩容完成。
		if pvSize.Cmp(pvcSize) >= 0 {
			return true, nil
		}
		return false, nil
	})
	if waitErr != nil {
		return fmt.Errorf("error while waiting for controller resize to finish: %v", waitErr)
	}
	return nil
}

// WaitForPendingFSResizeCondition 等待 PVC 出现 resize 条件
func WaitForPendingFSResizeCondition(ctx context.Context, pvc *v1.PersistentVolumeClaim, c clientset.Interface) (*v1.PersistentVolumeClaim, error) {
	var updatedPVC *v1.PersistentVolumeClaim
	waitErr := wait.PollUntilContextTimeout(ctx, resizePollInterval, pvcConditionSyncPeriod, true, func(ctx context.Context) (bool, error) {
		var err error
		updatedPVC, err = c.CoreV1().PersistentVolumeClaims(pvc.Namespace).Get(ctx, pvc.Name, metav1.GetOptions{})

		if err != nil {
			return false, fmt.Errorf("error fetching pvc %q for checking for resize status : %w", pvc.Name, err)
		}

		inProgressConditions := updatedPVC.Status.Conditions
		hasResizeCondition := false
		for _, condition := range inProgressConditions {
			if condition.Type == v1.PersistentVolumeClaimFileSystemResizePending {
				return true, nil
			}
			if isResizeConditionType(condition.Type) {
				hasResizeCondition = true
			}
		}
		// 没有 resize 相关的 PVC 条件，说明无需节点侧扩容
		if !hasResizeCondition {
			return true, nil
		}
		return false, nil
	})
	if waitErr != nil {
		return nil, fmt.Errorf("error waiting for pvc %q to have filesystem resize status: %v", pvc.Name, waitErr)
	}
	return updatedPVC, nil
}

// WaitForFSResize 等待 pv 中的文件系统完成扩容
func WaitForFSResize(ctx context.Context, pvc *v1.PersistentVolumeClaim, c clientset.Interface) (*v1.PersistentVolumeClaim, error) {
	var updatedPVC *v1.PersistentVolumeClaim
	waitErr := wait.PollUntilContextTimeout(ctx, resizePollInterval, totalResizeWaitPeriod, true, func(pollContext context.Context) (bool, error) {
		var err error
		updatedPVC, err = c.CoreV1().PersistentVolumeClaims(pvc.Namespace).Get(pollContext, pvc.Name, metav1.GetOptions{})

		if err != nil {
			return false, fmt.Errorf("error fetching pvc %q for checking for resize status : %w", pvc.Name, err)
		}

		pvcSize := updatedPVC.Spec.Resources.Requests[v1.ResourceStorage]
		pvcStatusSize := updatedPVC.Status.Capacity[v1.ResourceStorage]

		//如果 pvc 的 status 字段大小大于等于 pvc 的大小，即完成
		if pvcStatusSize.Cmp(pvcSize) >= 0 {
			return true, nil
		}
		return false, nil
	})
	if waitErr != nil {
		return nil, fmt.Errorf("error waiting for pvc %q filesystem resize to finish: %v", pvc.Name, waitErr)
	}
	return updatedPVC, nil
}

func isResizeConditionType(t v1.PersistentVolumeClaimConditionType) bool {
	return t == v1.PersistentVolumeClaimFileSystemResizePending ||
		t == v1.PersistentVolumeClaimResizing ||
		t == v1.PersistentVolumeClaimControllerResizeError ||
		t == v1.PersistentVolumeClaimNodeResizeError
}

// ExpectNoResizeConditions 校验只剩非 resize 的 PVC 条件。
func ExpectNoResizeConditions(pvcConditions []v1.PersistentVolumeClaimCondition) {
	for _, condition := range pvcConditions {
		gomega.ExpectWithOffset(1, isResizeConditionType(condition.Type)).To(gomega.BeFalseBecause("pvc should not have resize-related condition %q", condition.Type))
	}
}

func verifyOfflineAllocatedResources(pvc *v1.PersistentVolumeClaim, allocatedSize resource.Quantity) error {
	actualResizeStatus := pvc.Status.AllocatedResourceStatuses[v1.ResourceStorage]
	if !checkControllerExpansionCompleted(pvc) {
		return fmt.Errorf("pvc %q had %s resize status, expected %s", pvc.Name, actualResizeStatus, v1.PersistentVolumeClaimNodeResizePending)
	}

	actualAllocatedSize := pvc.Status.AllocatedResources.Storage()
	if actualAllocatedSize.Cmp(allocatedSize) < 0 {
		return fmt.Errorf("pvc %q had %s allocated size, expected %s", pvc.Name, actualAllocatedSize.String(), allocatedSize.String())
	}
	return nil
}

func checkControllerExpansionCompleted(pvc *v1.PersistentVolumeClaim) bool {
	resizeStatus := pvc.Status.AllocatedResourceStatuses[v1.ResourceStorage]
	// resizeStatus 为空表示无需节点侧扩容，但 controller 扩容已完成
	return (resizeStatus == "" || resizeStatus == v1.PersistentVolumeClaimNodeResizePending)
}

func VerifyRecoveryRelatedFields(pvc *v1.PersistentVolumeClaim) error {
	resizeStatus := pvc.Status.AllocatedResourceStatuses[v1.ResourceStorage]
	if resizeStatus != "" {
		return fmt.Errorf("pvc %q had %s resize status, expected none", pvc.Name, resizeStatus)
	}

	allocatedSize := pvc.Status.AllocatedResources[v1.ResourceStorage]
	requestedSize := pvc.Spec.Resources.Requests[v1.ResourceStorage]
	// 此时 allocatedSize 应大于 pvc 的资源请求
	if allocatedSize.Cmp(requestedSize) < 0 {
		return fmt.Errorf("pvc %q had %s allocated size, expected %s", pvc.Name, allocatedSize.String(), requestedSize.String())
	}
	return nil
}
