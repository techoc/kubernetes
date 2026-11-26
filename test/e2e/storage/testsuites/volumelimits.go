/*
Copyright 2019 The Kubernetes Authors.

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
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"

	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/wait"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/component-helpers/storage/ephemeral"
	csitrans "k8s.io/csi-translation-lib"
	"k8s.io/kubernetes/test/e2e/framework"
	e2enode "k8s.io/kubernetes/test/e2e/framework/node"
	e2epod "k8s.io/kubernetes/test/e2e/framework/pod"
	e2epv "k8s.io/kubernetes/test/e2e/framework/pv"
	storageframework "k8s.io/kubernetes/test/e2e/storage/framework"
	storageutils "k8s.io/kubernetes/test/e2e/storage/utils"
	admissionapi "k8s.io/pod-security-admission/api"
)

type volumeLimitsTestSuite struct {
	tsInfo storageframework.TestSuiteInfo
}

const (
	// 测试使用通用的 pod 启动 / PV 删除超时。由于它一次创建
	// 更多 volume，这些超时会乘以这个数字。
	// 使用真实 volume 数（例如 GCE 上的 128）实在太多。
	testSlowMultiplier = 10

	// 等待 CSINode 从已安装 CSI driver 获取 attach 限制的时长。
	csiNodeInfoTimeout = 2 * time.Minute
)

var _ storageframework.TestSuite = &volumeLimitsTestSuite{}

// InitCustomVolumeLimitsTestSuite 返回 volumeLimitsTestSuite，它使用自定义测试模式
// 实现 TestSuite 接口
func InitCustomVolumeLimitsTestSuite(patterns []storageframework.TestPattern) storageframework.TestSuite {
	return &volumeLimitsTestSuite{
		tsInfo: storageframework.TestSuiteInfo{
			Name:         "volumeLimits",
			TestPatterns: patterns,
		},
	}
}

// InitVolumeLimitsTestSuite 返回 volumeLimitsTestSuite，它使用测试套件
// 默认模式实现 TestSuite 接口
func InitVolumeLimitsTestSuite() storageframework.TestSuite {
	patterns := []storageframework.TestPattern{
		storageframework.FsVolModeDynamicPV,
		storageframework.DefaultFsGenericEphemeralVolume,
	}
	return InitCustomVolumeLimitsTestSuite(patterns)
}

func (t *volumeLimitsTestSuite) GetTestSuiteInfo() storageframework.TestSuiteInfo {
	return t.tsInfo
}

func (t *volumeLimitsTestSuite) SkipUnsupportedTests(driver storageframework.TestDriver, pattern storageframework.TestPattern) string {
	if pattern.VolType != storageframework.DynamicPV {
		return fmt.Sprintf("Suite %q does not support %v", t.tsInfo.Name, pattern.VolType)
	}
	dInfo := driver.GetDriverInfo()
	if !dInfo.Capabilities[storageframework.CapVolumeLimits] {
		return fmt.Sprintf("Driver %s does not support volume limits", dInfo.Name)
	}
	return ""
}

func (t *volumeLimitsTestSuite) DefineTests(driver storageframework.TestDriver, pattern storageframework.TestPattern) {
	type local struct {
		config *storageframework.PerTestConfig

		cs clientset.Interface
		ns *v1.Namespace
		// 第一个 pod 创建的 VolumeResource，包含 pv、pvc、sc 等
		resource *storageframework.VolumeResource

		// 所有创建的 PVC
		pvcNames []string

		// 所有创建的 Pod
		podNames []string

		// 所有创建的 PV，包括 resource 中的那个
		pvNames sets.Set[string]
	}
	var (
		l local

		dDriver storageframework.DynamicPVTestDriver
	)

	// 注意它还会注册一个 AfterEach，使 f 不可用。任何使用
	// f 的代码都必须在 It 或 Context 回调中运行。
	f := framework.NewFrameworkWithCustomTimeouts("volumelimits", storageframework.GetDriverTimeouts(driver))
	f.NamespacePodSecurityLevel = admissionapi.LevelPrivileged

	ginkgo.BeforeEach(func() {
		dDriver = driver.(storageframework.DynamicPVTestDriver)
	})

	cleanupTest := func(ctx context.Context, timeout time.Duration) error {
		var cleanupErrors []string
		for _, podName := range l.podNames {
			framework.Logf("Deleting pod %s", podName)
			err := l.cs.CoreV1().Pods(l.ns.Name).Delete(ctx, podName, metav1.DeleteOptions{})
			if err != nil && !apierrors.IsNotFound(err) {
				cleanupErrors = append(cleanupErrors, fmt.Sprintf("failed to delete pod %s: %s", podName, err))
			}
		}
		for _, pvcName := range l.pvcNames {
			framework.Logf("Deleting PVC %s", pvcName)
			err := l.cs.CoreV1().PersistentVolumeClaims(l.ns.Name).Delete(ctx, pvcName, metav1.DeleteOptions{})
			if err != nil && !apierrors.IsNotFound(err) {
				cleanupErrors = append(cleanupErrors, fmt.Sprintf("failed to delete PVC %s: %s", pvcName, err))
			}
		}
		// 等待 PV 被删除。由于 PVC 保护，这也包括 pod 和 PVC 删除。
		// 用 PV 来确保测试不会在 CSI driver 刚被销毁时
		// 留下孤儿 PV。
		err := wait.PollUntilContextTimeout(ctx, 5*time.Second, timeout, false, func(ctx context.Context) (bool, error) {
			existing := 0
			for _, pvName := range l.pvNames.UnsortedList() {
				_, err := l.cs.CoreV1().PersistentVolumes().Get(ctx, pvName, metav1.GetOptions{})
				if err == nil {
					existing++
				} else {
					if apierrors.IsNotFound(err) {
						l.pvNames.Delete(pvName)
					} else {
						framework.Logf("Failed to get PV %s: %s", pvName, err)
					}
				}
			}
			if existing > 0 {
				framework.Logf("Waiting for %d PVs to be deleted", existing)
				return false, nil
			}
			return true, nil
		})
		if err != nil {
			cleanupErrors = append(cleanupErrors, fmt.Sprintf("timed out waiting for PVs to be deleted: %s", err))
		}
		if len(cleanupErrors) != 0 {
			return errors.New("test cleanup failed: " + strings.Join(cleanupErrors, "; "))
		}
		return nil
	}

	// 这检查 CSIMaxVolumeLimitChecker 是否按预期工作。
	// 随机选中的节点应能处理
	// CSINode.Spec.Drivers[x].Allocatable 中声明的 CSI volume 数量。
	// 测试用一个带大量 volume 的 pod 来规避
	// 节点上的最大 pod 数限制。
	// 再加一个带 CSI volume 的 pod，应变为 Pending，condition
	// 说明因 volume 限制而不可调度。
	// 注意：测试可能创建大量 volume，非常慢。
	f.It("should support volume limits", f.WithSerial(), func(ctx context.Context) {
		driverInfo := driver.GetDriverInfo()
		l.ns = f.Namespace
		l.cs = f.ClientSet

		l.config = driver.PrepareTest(ctx, f)

		ginkgo.By("Picking a node")
		// 有些 CSI driver 只部署在单个节点上（例如 csi-hostpath），
		// 所以用那个节点，而不是随机选一个。
		nodeName := l.config.ClientNodeSelection.Name
		if nodeName == "" {
			node, err := e2enode.GetRandomReadySchedulableNode(ctx, f.ClientSet)
			framework.ExpectNoError(err)
			nodeName = node.Name
		}
		framework.Logf("Selected node %s", nodeName)

		ginkgo.By("Checking node limits")
		limit, err := getNodeLimits(ctx, l.cs, l.config, nodeName, dDriver)
		framework.ExpectNoError(err)

		framework.Logf("Node %s can handle %d volumes of driver %s", nodeName, limit, driverInfo.Name)
		// 创建 storage class 并生成一个 PVC。先不实例化 PVC，留给最后一个 pod 用。
		testVolumeSizeRange := t.GetTestSuiteInfo().SupportedSizeRange
		driverVolumeSizeRange := dDriver.GetDriverInfo().SupportedSizeRange
		claimSize, err := storageutils.GetSizeRangesIntersection(testVolumeSizeRange, driverVolumeSizeRange)
		framework.ExpectNoError(err, "determine intersection of test size range %+v and driver size range %+v", testVolumeSizeRange, dDriver)

		l.resource = storageframework.CreateVolumeResource(ctx, driver, l.config, pattern, testVolumeSizeRange)
		ginkgo.DeferCleanup(l.resource.CleanupResource)
		ginkgo.DeferCleanup(cleanupTest, testSlowMultiplier*f.Timeouts.PVDelete)

		selection := e2epod.NodeSelection{Name: nodeName}

		if pattern.VolType == storageframework.GenericEphemeralVolume {
			// 创建 <limit> 个 Pod。
			ginkgo.By(fmt.Sprintf("Creating %d Pod(s) with one volume each", limit))
			for range limit {
				pod := StartInPodWithVolumeSource(ctx, l.cs, *l.resource.VolSource, l.ns.Name, "volume-limits", e2epod.InfiniteSleepCommand, selection)
				l.podNames = append(l.podNames, pod.Name)
				l.pvcNames = append(l.pvcNames, ephemeral.VolumeClaimName(pod, &pod.Spec.Volumes[0]))
			}
		} else {
			// 为一个巨型 pod 创建 <limit> 个 PVC。
			var pvcs []*v1.PersistentVolumeClaim
			ginkgo.By(fmt.Sprintf("Creating %d PVC(s)", limit))
			for range limit {
				pvc := e2epv.MakePersistentVolumeClaim(e2epv.PersistentVolumeClaimConfig{
					ClaimSize:        claimSize,
					StorageClassName: &l.resource.Sc.Name,
				}, l.ns.Name)
				pvc, err = l.cs.CoreV1().PersistentVolumeClaims(l.ns.Name).Create(ctx, pvc, metav1.CreateOptions{})
				framework.ExpectNoError(err)
				l.pvcNames = append(l.pvcNames, pvc.Name)
				pvcs = append(pvcs, pvc)
			}

			ginkgo.By("Creating pod to use all PVC(s)")
			podConfig := e2epod.Config{
				NS:            l.ns.Name,
				PVCs:          pvcs,
				SeLinuxLabel:  e2epv.SELinuxLabel,
				NodeSelection: selection,
			}
			pod, err := e2epod.MakeSecPod(&podConfig)
			framework.ExpectNoError(err)
			pod, err = l.cs.CoreV1().Pods(l.ns.Name).Create(ctx, pod, metav1.CreateOptions{})
			framework.ExpectNoError(err)
			l.podNames = append(l.podNames, pod.Name)
		}

		ginkgo.By("Waiting for all PVCs to get Bound")
		l.pvNames = waitForAllPVCsBound(ctx, l.cs, testSlowMultiplier*f.Timeouts.PVBound, l.ns.Name, l.pvcNames)

		ginkgo.By("Waiting for the pod(s) running")
		for _, podName := range l.podNames {
			err = e2epod.WaitTimeoutForPodRunningInNamespace(ctx, l.cs, podName, l.ns.Name, testSlowMultiplier*f.Timeouts.PodStart)
			framework.ExpectNoError(err)
		}

		ginkgo.By("Creating an extra pod with one volume to exceed the limit")
		pod := StartInPodWithVolumeSource(ctx, l.cs, *l.resource.VolSource, l.ns.Name, "volume-limits-exceeded", e2epod.InfiniteSleepCommand, selection)
		l.podNames = append(l.podNames, pod.Name)

		ginkgo.By("Waiting for the pod to get unschedulable with the right message")
		err = e2epod.WaitForPodCondition(ctx, l.cs, l.ns.Name, pod.Name, "Unschedulable", f.Timeouts.PodStart, func(pod *v1.Pod) (bool, error) {
			if pod.Status.Phase == v1.PodPending {
				reg, err := regexp.Compile(`max.+volume.+count`)
				if err != nil {
					return false, err
				}
				for _, cond := range pod.Status.Conditions {
					matched := reg.MatchString(cond.Message)
					if cond.Type == v1.PodScheduled && cond.Status == v1.ConditionFalse && cond.Reason == "Unschedulable" && matched {
						return true, nil
					}
				}
			}
			if pod.Status.Phase != v1.PodPending {
				return true, fmt.Errorf("expected pod to be in phase Pending, but got phase: %v", pod.Status.Phase)
			}
			return false, nil
		})
		framework.ExpectNoError(err)
	})

	ginkgo.It("should verify that all csinodes have volume limits", func(ctx context.Context) {
		driverInfo := driver.GetDriverInfo()
		if !driverInfo.Capabilities[storageframework.CapVolumeLimits] {
			ginkgo.Skip(fmt.Sprintf("driver %s does not support volume limits", driverInfo.Name))
		}

		l.ns = f.Namespace
		l.cs = f.ClientSet

		l.config = driver.PrepareTest(ctx, f)

		nodeNames := []string{}
		if l.config.ClientNodeSelection.Name != "" {
			// 有些 CSI driver 只部署在单个节点上（例如 csi-hostpath），
			// 所以检查那个节点，而不是全部。
			nodeNames = append(nodeNames, l.config.ClientNodeSelection.Name)
		} else {
			nodeList, err := e2enode.GetReadySchedulableNodes(ctx, f.ClientSet)
			framework.ExpectNoError(err)
			for _, node := range nodeList.Items {
				nodeNames = append(nodeNames, node.Name)
			}
		}

		for _, nodeName := range nodeNames {
			ginkgo.By("Checking csinode limits")
			_, err := getNodeLimits(ctx, l.cs, l.config, nodeName, dDriver)
			if err != nil {
				framework.Failf("Expected volume limits to be set, error: %v", err)
			}
		}
	})
}

// waitForAllPVCsBound 等待给定 PVC 全部绑定，然后以 set 形式返回已绑定的 PVC 名称。
func waitForAllPVCsBound(ctx context.Context, cs clientset.Interface, timeout time.Duration, ns string, pvcNames []string) sets.Set[string] {
	pvNames := sets.New[string]()
	gomega.Eventually(ctx, func() (int, error) {
		unbound := 0
		for _, pvcName := range pvcNames {
			pvc, err := cs.CoreV1().PersistentVolumeClaims(ns).Get(ctx, pvcName, metav1.GetOptions{})
			if err != nil {
				gomega.StopTrying("failed to fetch PVCs").Wrap(err).Now()
			}
			if pvc.Status.Phase != v1.ClaimBound {
				unbound++
			} else {
				pvNames.Insert(pvc.Spec.VolumeName)
			}
		}
		framework.Logf("%d/%d of PVCs are Bound", pvNames.Len(), len(pvcNames))
		return unbound, nil
	}).WithPolling(5*time.Second).WithTimeout(timeout).Should(gomega.BeZero(), "error waiting for all PVCs to be bound")
	return pvNames
}

func getNodeLimits(ctx context.Context, cs clientset.Interface, config *storageframework.PerTestConfig, nodeName string, driver storageframework.DynamicPVTestDriver) (int, error) {
	driverInfo := driver.GetDriverInfo()
	if len(driverInfo.InTreePluginName) > 0 {
		csiTranslator := csitrans.New()
		driverName, err := csiTranslator.GetCSINameFromInTreeName(driverInfo.InTreePluginName)
		if err != nil {
			return 0, err
		}
		return getCSINodeLimits(ctx, cs, config, nodeName, driverName)
	}
	sc := driver.GetDynamicProvisionStorageClass(ctx, config, "")
	return getCSINodeLimits(ctx, cs, config, nodeName, sc.Provisioner)
}

func getCSINodeLimits(ctx context.Context, cs clientset.Interface, config *storageframework.PerTestConfig, nodeName, driverName string) (int, error) {
	// 带超时重试，因为 driver 可能刚装好，kubelet 需要一点时间发布所有内容。
	var limit int
	err := wait.PollUntilContextTimeout(ctx, 2*time.Second, csiNodeInfoTimeout, true, func(ctx context.Context) (bool, error) {
		csiNode, err := cs.StorageV1().CSINodes().Get(ctx, nodeName, metav1.GetOptions{})
		if err != nil {
			framework.Logf("%s", err)
			return false, nil
		}
		var csiDriver *storagev1.CSINodeDriver
		for i, c := range csiNode.Spec.Drivers {
			if c.Name == driverName || c.Name == config.GetUniqueDriverName() {
				csiDriver = &csiNode.Spec.Drivers[i]
				break
			}
		}
		if csiDriver == nil {
			framework.Logf("CSINodeInfo does not have driver %s yet", driverName)
			return false, nil
		}
		if csiDriver.Allocatable == nil {
			return false, fmt.Errorf("CSINodeInfo does not have Allocatable for driver %s", driverName)
		}
		if csiDriver.Allocatable.Count == nil {
			return false, fmt.Errorf("CSINodeInfo does not have Allocatable.Count for driver %s", driverName)
		}
		limit = int(*csiDriver.Allocatable.Count)
		return true, nil
	})
	if err != nil {
		return 0, fmt.Errorf("could not get CSINode limit for driver %s: %w", driverName, err)
	}
	return limit, nil
}
