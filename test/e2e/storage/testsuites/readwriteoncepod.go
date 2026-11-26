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

package testsuites

import (
	"context"
	"fmt"

	"github.com/onsi/ginkgo/v2"

	v1 "k8s.io/api/core/v1"
	schedulingv1 "k8s.io/api/scheduling/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	errors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/apimachinery/pkg/util/uuid"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/pkg/kubelet/events"
	"k8s.io/kubernetes/test/e2e/framework"
	e2eevents "k8s.io/kubernetes/test/e2e/framework/events"
	e2epod "k8s.io/kubernetes/test/e2e/framework/pod"
	e2epv "k8s.io/kubernetes/test/e2e/framework/pv"
	storageframework "k8s.io/kubernetes/test/e2e/storage/framework"
	admissionapi "k8s.io/pod-security-admission/api"
)

type readWriteOncePodTestSuite struct {
	tsInfo storageframework.TestSuiteInfo
}

var _ storageframework.TestSuite = &readWriteOncePodTestSuite{}

type readWriteOncePodTest struct {
	config *storageframework.PerTestConfig

	cs            clientset.Interface
	volume        *storageframework.VolumeResource
	pods          []*v1.Pod
	priorityClass *schedulingv1.PriorityClass

	migrationCheck *migrationOpCheck
}

func InitCustomReadWriteOncePodTestSuite(patterns []storageframework.TestPattern) storageframework.TestSuite {
	return &readWriteOncePodTestSuite{
		tsInfo: storageframework.TestSuiteInfo{
			Name:         "read-write-once-pod",
			TestPatterns: patterns,
			TestTags:     []interface{}{},
		},
	}
}

// InitReadWriteOncePodTestSuite 返回 ReadWriteOncePod PersistentVolume access mode 特性的测试套件。
func InitReadWriteOncePodTestSuite() storageframework.TestSuite {
	// 只覆盖一个测试模式，因为 ReadWriteOncePod 的强制约束由
	// Kubernetes 处理，不同卷类型之间没有差异。
	patterns := []storageframework.TestPattern{storageframework.DefaultFsDynamicPV}
	return InitCustomReadWriteOncePodTestSuite(patterns)
}

func (t *readWriteOncePodTestSuite) GetTestSuiteInfo() storageframework.TestSuiteInfo {
	return t.tsInfo
}

func (t *readWriteOncePodTestSuite) SkipUnsupportedTests(driver storageframework.TestDriver, pattern storageframework.TestPattern) string {
	driverInfo := driver.GetDriverInfo()
	if !driverInfo.Capabilities[storageframework.CapReadWriteOncePod] {
		return fmt.Sprintf("Driver %q doesn't support ReadWriteOncePod", driverInfo.Name)
	}
	return ""
}

func (t *readWriteOncePodTestSuite) DefineTests(driver storageframework.TestDriver, pattern storageframework.TestPattern) {
	var (
		driverInfo = driver.GetDriverInfo()
		l          readWriteOncePodTest
	)

	// 注意它还会注册一个 AfterEach，使 f 不可用。任何使用
	// f 的代码都必须在 It 或 Context 回调中运行。
	f := framework.NewFrameworkWithCustomTimeouts("read-write-once-pod", storageframework.GetDriverTimeouts(driver))
	f.NamespacePodSecurityLevel = admissionapi.LevelPrivileged

	init := func(ctx context.Context) {
		l = readWriteOncePodTest{}
		l.config = driver.PrepareTest(ctx, f)
		l.cs = f.ClientSet
		l.pods = []*v1.Pod{}
		l.migrationCheck = newMigrationOpCheck(ctx, f.ClientSet, f.ClientConfig(), driverInfo.InTreePluginName)
	}

	cleanup := func(ctx context.Context) {
		var errs []error
		for _, pod := range l.pods {
			framework.Logf("Deleting pod %v", pod.Name)
			err := e2epod.DeletePodWithWait(ctx, l.cs, pod)
			errs = append(errs, err)
		}

		framework.Logf("Deleting volume %s", l.volume.Pvc.GetName())
		err := l.volume.CleanupResource(ctx)
		errs = append(errs, err)

		if l.priorityClass != nil {
			framework.Logf("Deleting PriorityClass %v", l.priorityClass.Name)
			err := l.cs.SchedulingV1().PriorityClasses().Delete(ctx, l.priorityClass.Name, metav1.DeleteOptions{})
			errs = append(errs, err)
		}

		framework.ExpectNoError(errors.NewAggregate(errs), "while cleaning up resource")
		l.migrationCheck.validateMigrationVolumeOpCounts(ctx)
	}

	ginkgo.BeforeEach(func(ctx context.Context) {
		init(ctx)
		ginkgo.DeferCleanup(cleanup)
	})

	f.It("should preempt lower priority pods using ReadWriteOncePod volumes", f.WithSerial(), func(ctx context.Context) {
		// 创建 ReadWriteOncePod PVC。
		accessModes := []v1.PersistentVolumeAccessMode{v1.ReadWriteOncePod}
		l.volume = storageframework.CreateVolumeResourceWithAccessModes(ctx, driver, l.config, pattern, t.GetTestSuiteInfo().SupportedSizeRange, accessModes, nil)

		l.priorityClass = &schedulingv1.PriorityClass{
			ObjectMeta: metav1.ObjectMeta{Name: "e2e-test-read-write-once-pod-" + string(uuid.NewUUID())},
			Value:      int32(1000),
		}
		_, err := l.cs.SchedulingV1().PriorityClasses().Create(ctx, l.priorityClass, metav1.CreateOptions{})
		framework.ExpectNoError(err, "failed to create priority class")

		podConfig := e2epod.Config{
			NS:           f.Namespace.Name,
			PVCs:         []*v1.PersistentVolumeClaim{l.volume.Pvc},
			SeLinuxLabel: e2epv.SELinuxLabel,
		}

		// 创建第一个 Pod，它将取得 ReadWriteOncePod PVC 的所有权。
		pod1, err := e2epod.MakeSecPod(&podConfig)
		framework.ExpectNoError(err, "failed to create spec for pod1")
		_, err = l.cs.CoreV1().Pods(pod1.Namespace).Create(ctx, pod1, metav1.CreateOptions{})
		framework.ExpectNoError(err, "failed to create pod1")
		err = e2epod.WaitTimeoutForPodRunningInNamespace(ctx, l.cs, pod1.Name, pod1.Namespace, f.Timeouts.PodStart)
		framework.ExpectNoError(err, "failed to wait for pod1 running status")
		l.pods = append(l.pods, pod1)

		// 创建第二个 Pod，因为它使用 ReadWriteOncePod PVC 且优先级更高，
		// 会抢占第一个 Pod。
		pod2, err := e2epod.MakeSecPod(&podConfig)
		framework.ExpectNoError(err, "failed to create spec for pod2")
		pod2.Spec.PriorityClassName = l.priorityClass.Name
		_, err = l.cs.CoreV1().Pods(pod2.Namespace).Create(ctx, pod2, metav1.CreateOptions{})
		framework.ExpectNoError(err, "failed to create pod2")
		l.pods = append(l.pods, pod2)

		// 等待第一个 Pod 被抢占、第二个 Pod 启动。
		err = e2epod.WaitForPodNotFoundInNamespace(ctx, l.cs, pod1.Name, pod1.Namespace, f.Timeouts.PodStart)
		framework.ExpectNoError(err, "failed to wait for pod1 to be preempted")
		err = e2epod.WaitTimeoutForPodRunningInNamespace(ctx, l.cs, pod2.Name, pod2.Namespace, f.Timeouts.PodStart)
		framework.ExpectNoError(err, "failed to wait for pod2 running status")

		// 重建第一个 Pod，它因为第二个 Pod 正在使用
		// ReadWriteOncePod PVC 且优先级更高而无法调度。
		_, err = l.cs.CoreV1().Pods(pod1.Namespace).Create(ctx, pod1, metav1.CreateOptions{})
		framework.ExpectNoError(err, "failed to create pod1")
		err = e2epod.WaitForPodNameUnschedulableInNamespace(ctx, l.cs, pod1.Name, pod1.Namespace)
		framework.ExpectNoError(err, "failed to wait for pod1 unschedulable status")

		// 删除优先级更高的第二个 Pod，观察第一个 Pod 现在能启动。
		err = e2epod.DeletePodWithWait(ctx, l.cs, pod2)
		framework.ExpectNoError(err, "failed to delete pod2")
		err = e2epod.WaitTimeoutForPodRunningInNamespace(ctx, l.cs, pod1.Name, pod1.Namespace, f.Timeouts.PodStart)
		framework.ExpectNoError(err, "failed to wait for pod1 running status")
	})

	ginkgo.It("should block a second pod from using an in-use ReadWriteOncePod volume on the same node", func(ctx context.Context) {
		// 创建 ReadWriteOncePod PVC。
		accessModes := []v1.PersistentVolumeAccessMode{v1.ReadWriteOncePod}
		l.volume = storageframework.CreateVolumeResourceWithAccessModes(ctx, driver, l.config, pattern, t.GetTestSuiteInfo().SupportedSizeRange, accessModes, nil)

		podConfig := e2epod.Config{
			NS:           f.Namespace.Name,
			PVCs:         []*v1.PersistentVolumeClaim{l.volume.Pvc},
			SeLinuxLabel: e2epv.SELinuxLabel,
		}

		// 创建第一个 Pod，它将取得 ReadWriteOncePod PVC 的所有权。
		pod1, err := e2epod.MakeSecPod(&podConfig)
		framework.ExpectNoError(err, "failed to create spec for pod1")
		_, err = l.cs.CoreV1().Pods(pod1.Namespace).Create(ctx, pod1, metav1.CreateOptions{})
		framework.ExpectNoError(err, "failed to create pod1")
		err = e2epod.WaitTimeoutForPodRunningInNamespace(ctx, l.cs, pod1.Name, pod1.Namespace, f.Timeouts.PodStart)
		framework.ExpectNoError(err, "failed to wait for pod1 running status")
		l.pods = append(l.pods, pod1)

		// 第一个 Pod 已运行，获取其节点名。
		pod1, err = l.cs.CoreV1().Pods(pod1.Namespace).Get(ctx, pod1.Name, metav1.GetOptions{})
		framework.ExpectNoError(err, "failed to get pod1")
		nodeName := pod1.Spec.NodeName

		// 在第一个 Pod 所在节点上创建第二个 Pod。
		pod2, err := e2epod.MakeSecPod(&podConfig)
		framework.ExpectNoError(err, "failed to create spec for pod2")
		// 把节点名设为第一个 Pod 的节点名。
		// 设置节点名是为了绕过调度，否则调度会强制校验 access mode。
		pod2.Spec.NodeName = nodeName
		_, err = l.cs.CoreV1().Pods(pod2.Namespace).Create(ctx, pod2, metav1.CreateOptions{})
		framework.ExpectNoError(err, "failed to create pod2")
		l.pods = append(l.pods, pod2)

		// 等待第二个 Pod 产生 FailedMount 事件。
		eventSelector := fields.Set{
			"involvedObject.kind":      "Pod",
			"involvedObject.name":      pod2.Name,
			"involvedObject.namespace": pod2.Namespace,
			"reason":                   events.FailedMountVolume,
		}.AsSelector().String()
		msg := "volume uses the ReadWriteOncePod access mode and is already in use by another pod"
		err = e2eevents.WaitTimeoutForEvent(ctx, l.cs, pod2.Namespace, eventSelector, msg, f.Timeouts.PodStart)
		framework.ExpectNoError(err, "failed to wait for FailedMount event for pod2")

		// 等待第二个 Pod 因卡在 container creating 而失败。
		reason := "ContainerCreating"
		err = e2epod.WaitForPodContainerToFail(ctx, l.cs, pod2.Namespace, pod2.Name, 0, reason, f.Timeouts.PodStart)
		framework.ExpectNoError(err, "failed to wait for pod2 container to fail")

		// 删除第一个 Pod，观察第二个 Pod 现在能启动。
		err = e2epod.DeletePodWithWait(ctx, l.cs, pod1)
		framework.ExpectNoError(err, "failed to delete pod1")
		err = e2epod.WaitTimeoutForPodRunningInNamespace(ctx, l.cs, pod2.Name, pod2.Namespace, f.Timeouts.PodStart)
		framework.ExpectNoError(err, "failed to wait for pod2 running status")
	})
}
