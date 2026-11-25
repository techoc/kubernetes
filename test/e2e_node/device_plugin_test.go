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

package e2enode

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"github.com/onsi/gomega/gcustom"
	"github.com/onsi/gomega/types"

	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	k8stypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
	kubeletdevicepluginv1beta1 "k8s.io/kubelet/pkg/apis/deviceplugin/v1beta1"
	admissionapi "k8s.io/pod-security-admission/api"
	"k8s.io/utils/ptr"

	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/kubectl/pkg/util/podutils"
	kubeletpodresourcesv1 "k8s.io/kubelet/pkg/apis/podresources/v1"
	kubeletpodresourcesv1alpha1 "k8s.io/kubelet/pkg/apis/podresources/v1alpha1"
	"k8s.io/kubernetes/test/e2e/feature"
	"k8s.io/kubernetes/test/e2e/framework"
	e2enode "k8s.io/kubernetes/test/e2e/framework/node"
	e2epod "k8s.io/kubernetes/test/e2e/framework/pod"
	e2etestfiles "k8s.io/kubernetes/test/e2e/framework/testfiles"
	testutils "k8s.io/kubernetes/test/utils"
)

var (
	appsScheme = runtime.NewScheme()
	appsCodecs = serializer.NewCodecFactory(appsScheme)
)

// 串行运行，因为测试会重启 Kubelet
var _ = SIGDescribe("Device Plugin", framework.WithSerial(), feature.DevicePlugin, func() {
	f := framework.NewDefaultFramework("device-plugin-errors")
	f.NamespacePodSecurityLevel = admissionapi.LevelPrivileged
	testDevicePlugin(f, kubeletdevicepluginv1beta1.DevicePluginPath)
	testDevicePluginNodeReboot(f, kubeletdevicepluginv1beta1.DevicePluginPath)
})

// readDaemonSetV1OrDie 从字节读取 daemonset 对象，出错时 panic。
func readDaemonSetV1OrDie(objBytes []byte) *appsv1.DaemonSet {
	appsv1.AddToScheme(appsScheme)
	requiredObj, err := runtime.Decode(appsCodecs.UniversalDecoder(appsv1.SchemeGroupVersion), objBytes)
	if err != nil {
		panic(err)
	}
	return requiredObj.(*appsv1.DaemonSet)
}

const (
	// 这是 Pod 中执行命令的休眠间隔，用于确保容器在测试时间尺度内"永久"运行
	sleepIntervalForever string = "24h"

	// 这是 Pod 中执行命令的休眠间隔，以便容器在预期的测试运行时间内重启
	sleepIntervalWithRestart string = "60s"

	// 这是 Pod 中执行命令的休眠间隔，以便容器在预期的测试运行时间内重启
	sleepIntervalToCompletion string = "5s"
)

func testDevicePlugin(f *framework.Framework, pluginSockDir string) {
	pluginSockDir = filepath.Clean(pluginSockDir) + "/"

	type ResourceValue struct {
		Allocatable int
		Capacity    int
	}

	devicePluginGracefulTimeout := 5 * time.Minute // 参见 pkg/kubelet/cm/devicemanager/types.go 中的 endpointStopGracePeriod

	var getNodeResourceValues = func(ctx context.Context, resourceName string) ResourceValue {
		ginkgo.GinkgoHelper()
		node := getLocalNode(ctx, f)

		// -1 表示未找到该资源
		result := ResourceValue{
			Allocatable: -1,
			Capacity:    -1,
		}

		for key, val := range node.Status.Capacity {
			resource := string(key)
			if resource == resourceName {
				result.Capacity = int(val.Value())
				break
			}
		}

		for key, val := range node.Status.Allocatable {
			resource := string(key)
			if resource == resourceName {
				result.Allocatable = int(val.Value())
				break
			}
		}

		return result
	}

	f.Context("DevicePlugin", f.WithSerial(), f.WithDisruptive(), func() {
		var devicePluginPod, dptemplate *v1.Pod
		var v1alphaPodResources *kubeletpodresourcesv1alpha1.ListPodResourcesResponse
		var v1PodResources *kubeletpodresourcesv1.ListPodResourcesResponse
		var err error

		ginkgo.BeforeEach(func(ctx context.Context) {
			ginkgo.By("Wait for node to be ready")
			gomega.Eventually(ctx, func(ctx context.Context) bool {
				nodes, err := e2enode.TotalReady(ctx, f.ClientSet)
				framework.ExpectNoError(err)
				return nodes == 1
			}, time.Minute, time.Second).Should(gomega.BeTrueBecause("expected node to be ready"))

			// 运行 device plugin 测试前，需要确保
			// 集群处于干净状态，且该节点上
			// 没有 Pod 在运行。
			// 这里使用 gomega.Eventually 带重试完成，因为其他测试套件中的前一个测试可能刚运行，其资源删除可能仍在进行。
			// 参见：https://issue.k8s.io/115381
			gomega.Eventually(ctx, func(ctx context.Context) error {
				v1alphaPodResources, err = getV1alpha1NodeDevices(ctx)
				if err != nil {
					return fmt.Errorf("failed to get node local podresources by accessing the (v1alpha) podresources API endpoint: %v", err)
				}

				v1PodResources, err = getV1NodeDevices(ctx)
				if err != nil {
					return fmt.Errorf("failed to get node local podresources by accessing the (v1) podresources API endpoint: %v", err)
				}

				if len(v1alphaPodResources.PodResources) > 0 {
					return fmt.Errorf("expected v1alpha pod resources to be empty, but got non-empty resources: %+v", v1alphaPodResources.PodResources)
				}

				if len(v1PodResources.PodResources) > 0 {
					return fmt.Errorf("expected v1 pod resources to be empty, but got non-empty resources: %+v", v1PodResources.PodResources)
				}
				return nil
			}, f.Timeouts.PodDelete, f.Timeouts.Poll).Should(gomega.Succeed())

			ginkgo.By("Scheduling a sample device plugin pod")
			dp := getSampleDevicePluginPod(pluginSockDir, "")
			dptemplate = dp.DeepCopy()
			devicePluginPod = e2epod.NewPodClient(f).CreateSync(ctx, dp)

			ginkgo.By("Waiting for devices to become available on the local node")
			gomega.Eventually(ctx, func(ctx context.Context) bool {
				node, ready := getLocalTestNode(ctx, f)
				return ready && e2enode.CountSampleDeviceCapacity(node) > 0
			}, 5*time.Minute, framework.Poll).Should(gomega.BeTrueBecause("expected devices to be available on local node"))
			framework.Logf("Successfully created device plugin pod")

			ginkgo.By(fmt.Sprintf("Waiting for the resource exported by the sample device plugin to become available on the local node (instances: %d)", e2enode.SampleDevsAmount))
			gomega.Eventually(ctx, func(ctx context.Context) bool {
				node, ready := getLocalTestNode(ctx, f)
				return ready &&
					e2enode.CountSampleDeviceCapacity(node) == e2enode.SampleDevsAmount &&
					e2enode.CountSampleDeviceAllocatable(node) == e2enode.SampleDevsAmount
			}, 30*time.Second, framework.Poll).Should(gomega.BeTrueBecause("expected resource to be available on local node"))
		})

		ginkgo.AfterEach(func(ctx context.Context) {
			// 打印 devicePluginPod 的日志以便排查
			ginkgo.By("Get and print devicePluginPod logs")
			logs, err := e2epod.GetPodLogs(ctx, f.ClientSet, devicePluginPod.Namespace, devicePluginPod.Name, devicePluginPod.Spec.Containers[0].Name)
			if err != nil {
				framework.Logf("failed to get pod logs %v", err)
			} else {
				framework.Logf("logs of %s/%s (error: %v): %s", devicePluginPod.Name, devicePluginPod.Spec.Containers[0].Name, err, logs)
			}
			ginkgo.By("Deleting the device plugin pod")
			e2epod.NewPodClient(f).DeleteSync(ctx, devicePluginPod.Name, metav1.DeleteOptions{}, f.Timeouts.PodDelete)

			ginkgo.By("Deleting any Pods created by the test")
			l, err := e2epod.NewPodClient(f).List(ctx, metav1.ListOptions{})
			framework.ExpectNoError(err)
			for _, p := range l.Items {
				if p.Namespace != f.Namespace.Name {
					continue
				}

				framework.Logf("Deleting pod: %s", p.Name)
				e2epod.NewPodClient(f).DeleteSync(ctx, p.Name, metav1.DeleteOptions{}, f.Timeouts.PodDelete)
			}

			restartKubelet(ctx, true)

			ginkgo.By("Waiting for devices to become unavailable on the local node")
			gomega.Eventually(ctx, func(ctx context.Context) bool {
				node, ready := getLocalTestNode(ctx, f)
				return ready && e2enode.CountSampleDeviceCapacity(node) <= 0
			}, 5*time.Minute, framework.Poll).Should(gomega.BeTrueBecause("expected devices to be unavailable on local node"))

			ginkgo.By("devices now unavailable on the local node")
		})

		ginkgo.It("Can schedule a pod that requires a device", func(ctx context.Context) {
			podRECMD := fmt.Sprintf("devs=$(ls /tmp/ | egrep '^Dev-[0-9]+$') && echo stub devices: $devs && sleep %s", sleepIntervalWithRestart)
			pod1 := e2epod.NewPodClient(f).CreateSync(ctx, makeBusyboxPod(e2enode.SampleDeviceResourceName, podRECMD))
			deviceIDRE := "stub devices: (Dev-[0-9]+)"
			devID1, err := parseLog(ctx, f, pod1.Name, pod1.Name, deviceIDRE)
			framework.ExpectNoError(err, "getting logs for pod %q", pod1.Name)
			gomega.Expect(devID1).To(gomega.Not(gomega.Equal("")), "pod1 requested a device but started successfully without")

			v1alphaPodResources, err = getV1alpha1NodeDevices(ctx)
			framework.ExpectNoError(err)

			v1PodResources, err = getV1NodeDevices(ctx)
			framework.ExpectNoError(err)

			framework.Logf("v1alphaPodResources.PodResources:%+v\n", v1alphaPodResources.PodResources)
			framework.Logf("v1PodResources.PodResources:%+v\n", v1PodResources.PodResources)
			framework.Logf("len(v1alphaPodResources.PodResources):%+v", len(v1alphaPodResources.PodResources))
			framework.Logf("len(v1PodResources.PodResources):%+v", len(v1PodResources.PodResources))

			gomega.Expect(v1alphaPodResources.PodResources).To(gomega.HaveLen(2))
			gomega.Expect(v1PodResources.PodResources).To(gomega.HaveLen(2))

			var v1alphaResourcesForOurPod *kubeletpodresourcesv1alpha1.PodResources
			for _, res := range v1alphaPodResources.GetPodResources() {
				if res.Name == pod1.Name {
					v1alphaResourcesForOurPod = res
				}
			}

			var v1ResourcesForOurPod *kubeletpodresourcesv1.PodResources
			for _, res := range v1PodResources.GetPodResources() {
				if res.Name == pod1.Name {
					v1ResourcesForOurPod = res
				}
			}

			gomega.Expect(v1alphaResourcesForOurPod).NotTo(gomega.BeNil())
			gomega.Expect(v1ResourcesForOurPod).NotTo(gomega.BeNil())

			gomega.Expect(v1alphaResourcesForOurPod.Name).To(gomega.Equal(pod1.Name))
			gomega.Expect(v1ResourcesForOurPod.Name).To(gomega.Equal(pod1.Name))

			gomega.Expect(v1alphaResourcesForOurPod.Namespace).To(gomega.Equal(pod1.Namespace))
			gomega.Expect(v1ResourcesForOurPod.Namespace).To(gomega.Equal(pod1.Namespace))

			gomega.Expect(v1alphaResourcesForOurPod.Containers).To(gomega.HaveLen(1))
			gomega.Expect(v1ResourcesForOurPod.Containers).To(gomega.HaveLen(1))

			gomega.Expect(v1alphaResourcesForOurPod.Containers[0].Name).To(gomega.Equal(pod1.Spec.Containers[0].Name))
			gomega.Expect(v1ResourcesForOurPod.Containers[0].Name).To(gomega.Equal(pod1.Spec.Containers[0].Name))

			gomega.Expect(v1alphaResourcesForOurPod.Containers[0].Devices).To(gomega.HaveLen(1))
			gomega.Expect(v1ResourcesForOurPod.Containers[0].Devices).To(gomega.HaveLen(1))

			gomega.Expect(v1alphaResourcesForOurPod.Containers[0].Devices[0].ResourceName).To(gomega.Equal(e2enode.SampleDeviceResourceName))
			gomega.Expect(v1ResourcesForOurPod.Containers[0].Devices[0].ResourceName).To(gomega.Equal(e2enode.SampleDeviceResourceName))

			gomega.Expect(v1alphaResourcesForOurPod.Containers[0].Devices[0].DeviceIds).To(gomega.HaveLen(1))
			gomega.Expect(v1ResourcesForOurPod.Containers[0].Devices[0].DeviceIds).To(gomega.HaveLen(1))
		})

		f.It("can make a CDI device accessible in a container", func(ctx context.Context) {
			// 检查是否设置了 CDI_DEVICE 环境变量
			// 且容器内只有一个对应的设备节点 /tmp/<CDI_DEVICE> 可用
			podObj := makeBusyboxPod(e2enode.SampleDeviceResourceName, "[ $(ls /tmp/CDI-Dev-[1,2] | wc -l) -eq 1 -a -b /tmp/$CDI_DEVICE ]")
			podObj.Spec.RestartPolicy = v1.RestartPolicyNever
			pod := e2epod.NewPodClient(f).Create(ctx, podObj)
			framework.ExpectNoError(e2epod.WaitForPodSuccessInNamespace(ctx, f.ClientSet, pod.Name, pod.Namespace))
		})

		// 模拟容器重启，同时其他相关组件（kubelet、device plugin）保持稳定。为此，在容器
		// 入口点中短暂休眠。设备分配应保持不变，并在容器
		// 重启期间保持稳定。为简洁起见，这里只检查第一次重启。
		ginkgo.It("Keeps device plugin assignments across pod restarts (no kubelet restart, no device plugin restart)", func(ctx context.Context) {
			podRECMD := fmt.Sprintf("devs=$(ls /tmp/ | egrep '^Dev-[0-9]+$') && echo stub devices: $devs && sleep %s", sleepIntervalWithRestart)
			pod1 := e2epod.NewPodClient(f).CreateSync(ctx, makeBusyboxPod(e2enode.SampleDeviceResourceName, podRECMD))
			deviceIDRE := "stub devices: (Dev-[0-9]+)"
			devID1, err := parseLog(ctx, f, pod1.Name, pod1.Name, deviceIDRE)
			framework.ExpectNoError(err, "getting logs for pod %q", pod1.Name)
			gomega.Expect(devID1).To(gomega.Not(gomega.Equal("")), "pod1 requested a device but started successfully without")

			pod1, err = e2epod.NewPodClient(f).Get(ctx, pod1.Name, metav1.GetOptions{})
			framework.ExpectNoError(err)

			ginkgo.By("Waiting for container to restart")
			ensurePodContainerRestart(ctx, f, pod1.Name, pod1.Name)

			// 从容器角度检查设备分配是否保留且稳定
			ginkgo.By("Confirming that after a container restart, fake-device assignment is kept")
			devIDRestart1, err := parseLog(ctx, f, pod1.Name, pod1.Name, deviceIDRE)
			framework.ExpectNoError(err, "getting logs for pod %q", pod1.Name)
			gomega.Expect(devIDRestart1).To(gomega.Equal(devID1))

			// 从 kubelet 角度交叉检查设备分配是否保留且稳定。
			// 需要与容器角度的结果一致。
			ginkgo.By("Verifying the device assignment after container restart using podresources API")
			v1PodResources, err = getV1NodeDevices(ctx)
			if err != nil {
				framework.ExpectNoError(err, "getting pod resources assignment after pod restart")
			}
			err, _ = checkPodResourcesAssignment(v1PodResources, pod1.Namespace, pod1.Name, pod1.Spec.Containers[0].Name, e2enode.SampleDeviceResourceName, []string{devID1})
			framework.ExpectNoError(err, "inconsistent device assignment after pod restart")

			ginkgo.By("Creating another pod")
			pod2 := e2epod.NewPodClient(f).CreateSync(ctx, makeBusyboxPod(e2enode.SampleDeviceResourceName, podRECMD))
			err = e2epod.WaitTimeoutForPodRunningInNamespace(ctx, f.ClientSet, pod2.Name, f.Namespace.Name, 1*time.Minute)
			framework.ExpectNoError(err)

			ginkgo.By("Checking that pod got a fake device")
			devID2, err := parseLog(ctx, f, pod2.Name, pod2.Name, deviceIDRE)
			framework.ExpectNoError(err, "getting logs for pod %q", pod2.Name)

			gomega.Expect(devID2).To(gomega.Not(gomega.Equal("")), "pod2 requested a device but started successfully without")

			ginkgo.By("Verifying the device assignment after extra container start using podresources API")
			v1PodResources, err = getV1NodeDevices(ctx)
			if err != nil {
				framework.ExpectNoError(err, "getting pod resources assignment after pod restart")
			}
			err, _ = checkPodResourcesAssignment(v1PodResources, pod1.Namespace, pod1.Name, pod1.Spec.Containers[0].Name, e2enode.SampleDeviceResourceName, []string{devID1})
			framework.ExpectNoError(err, "inconsistent device assignment after extra container restart - pod1")
			err, _ = checkPodResourcesAssignment(v1PodResources, pod2.Namespace, pod2.Name, pod2.Spec.Containers[0].Name, e2enode.SampleDeviceResourceName, []string{devID2})
			framework.ExpectNoError(err, "inconsistent device assignment after extra container restart - pod2")
		})

		// 模拟 kubelet 重启。合规的 device plugin 应重新注册，而 Pod 和容器保持运行。
		// 有缺陷或缓慢的 device plugin 的流程留待另一个测试。
		// 设备分配应在 kubelet 重启后保留且稳定，因为设备分配由 kubelet 执行，
		// 且 device plugin 和实际消费者（容器）都保持稳定。
		ginkgo.It("Keeps device plugin assignments across kubelet restarts (no pod restart, no device plugin restart)", func(ctx context.Context) {
			podRECMD := fmt.Sprintf("devs=$(ls /tmp/ | egrep '^Dev-[0-9]+$') && echo stub devices: $devs && sleep %s", sleepIntervalForever)
			pod1 := e2epod.NewPodClient(f).CreateSync(ctx, makeBusyboxPod(e2enode.SampleDeviceResourceName, podRECMD))
			deviceIDRE := "stub devices: (Dev-[0-9]+)"
			devID1, err := parseLog(ctx, f, pod1.Name, pod1.Name, deviceIDRE)
			framework.ExpectNoError(err, "getting logs for pod %q", pod1.Name)
			gomega.Expect(devID1).To(gomega.Not(gomega.Equal("")), "pod1 requested a device but started successfully without")

			pod1, err = e2epod.NewPodClient(f).Get(ctx, pod1.Name, metav1.GetOptions{})
			framework.ExpectNoError(err)
			framework.Logf("testing pod: pre-restart  UID=%s namespace=%s name=%s ready=%v", pod1.UID, pod1.Namespace, pod1.Name, podutils.IsPodReady(pod1))

			ginkgo.By("Restarting Kubelet")
			restartKubelet(ctx, true)

			ginkgo.By("Wait for node to be ready again")
			framework.ExpectNoError(e2enode.WaitForAllNodesSchedulable(ctx, f.ClientSet, 5*time.Minute))

			ginkgo.By("Waiting for resource to become available on the local node after restart")
			gomega.Eventually(ctx, func() bool {
				node, ready := getLocalTestNode(ctx, f)
				return ready &&
					e2enode.CountSampleDeviceCapacity(node) == e2enode.SampleDevsAmount &&
					e2enode.CountSampleDeviceAllocatable(node) == e2enode.SampleDevsAmount
			}, 30*time.Second, framework.Poll).Should(gomega.BeTrueBecause("expected resource to be available after restart"))

			ginkgo.By("Checking the same instance of the pod is still running")
			gomega.Eventually(ctx, getPodByName).
				WithArguments(f, pod1.Name).
				WithTimeout(time.Minute).
				Should(BeTheSamePodStillRunning(pod1),
					"the same pod instance not running across kubelet restarts, workload should not be perturbed by kubelet restarts")

			pod2, err := e2epod.NewPodClient(f).Get(ctx, pod1.Name, metav1.GetOptions{})
			framework.ExpectNoError(err)
			framework.Logf("testing pod: post-restart UID=%s namespace=%s name=%s ready=%v", pod2.UID, pod2.Namespace, pod2.Name, podutils.IsPodReady(pod2))

			// 从 kubelet 角度交叉检查设备分配是否保留且稳定。
			// 注意，不再检查容器日志：该检查在启动时完成，容器
			// 从未重启（在此测试时间尺度上"永久"运行），因此重复此检查
			// 毫无意义。
			ginkgo.By("Verifying the device assignment after kubelet restart using podresources API")
			gomega.Eventually(ctx, func() error {
				v1PodResources, err = getV1NodeDevices(ctx)
				return err
			}, 30*time.Second, framework.Poll).ShouldNot(gomega.HaveOccurred(), "cannot fetch the compute resource assignment after kubelet restart")

			err, _ = checkPodResourcesAssignment(v1PodResources, pod2.Namespace, pod2.Name, pod2.Spec.Containers[0].Name, e2enode.SampleDeviceResourceName, []string{devID1})
			framework.ExpectNoError(err, "inconsistent device assignment after pod restart")
		})

		// 模拟 kubelet 和容器重启，但*不*重启 device plugin。
		// 设备分配应在 kubelet 和容器重启后保留且稳定，因为执行
		// 设备分配的是 kubelet，且 device plugin 保持稳定。
		ginkgo.It("Keeps device plugin assignments across pod and kubelet restarts (no device plugin restart)", func(ctx context.Context) {
			podRECMD := fmt.Sprintf("devs=$(ls /tmp/ | egrep '^Dev-[0-9]+$') && echo stub devices: $devs && sleep %s", sleepIntervalWithRestart)
			pod1 := e2epod.NewPodClient(f).CreateSync(ctx, makeBusyboxPod(e2enode.SampleDeviceResourceName, podRECMD))
			deviceIDRE := "stub devices: (Dev-[0-9]+)"
			devID1, err := parseLog(ctx, f, pod1.Name, pod1.Name, deviceIDRE)
			framework.ExpectNoError(err, "getting logs for pod %q", pod1.Name)

			gomega.Expect(devID1).To(gomega.Not(gomega.Equal("")), "pod1 requested a device but started successfully without")

			pod1, err = e2epod.NewPodClient(f).Get(ctx, pod1.Name, metav1.GetOptions{})
			framework.ExpectNoError(err)

			ginkgo.By("Wait for node to be ready again")
			framework.ExpectNoError(e2enode.WaitForAllNodesSchedulable(ctx, f.ClientSet, 5*time.Minute))

			ginkgo.By("Waiting for container to restart")
			ensurePodContainerRestart(ctx, f, pod1.Name, pod1.Name)

			ginkgo.By("Confirming that after a container restart, fake-device assignment is kept")
			devIDRestart1, err := parseLog(ctx, f, pod1.Name, pod1.Name, deviceIDRE)
			framework.ExpectNoError(err, "getting logs for pod %q", pod1.Name)
			gomega.Expect(devIDRestart1).To(gomega.Equal(devID1))

			ginkgo.By("Restarting Kubelet")
			restartKubelet(ctx, true)

			ginkgo.By("Wait for node to be ready again")
			framework.ExpectNoError(e2enode.WaitForAllNodesSchedulable(ctx, f.ClientSet, 5*time.Minute))

			ginkgo.By("Checking an instance of the pod is running")
			gomega.Eventually(ctx, getPodByName).
				WithArguments(f, pod1.Name).
				// kubelet 以指数退避延迟重启 Pod，最大上限为 5 分钟。
				// 在慢速环境中，为 Pod 启动预留 5 分 10 秒。
				WithTimeout(5*time.Minute+10*time.Second).
				Should(gomega.And(
					BeAPodInPhase(v1.PodRunning),
					BeAPodReady(),
				),
					"the pod should still be running, the workload should not be perturbed by kubelet restarts")

			ginkgo.By("Verifying the device assignment after pod and kubelet restart using container logs")
			var devID1Restarted string
			gomega.Eventually(ctx, func() string {
				devID1Restarted, err = parseLog(ctx, f, pod1.Name, pod1.Name, deviceIDRE)
				if err != nil {
					framework.Logf("error getting logds for pod %q: %v", pod1.Name, err)
					return ""
				}
				return devID1Restarted
			}, 30*time.Second, framework.Poll).Should(gomega.Equal(devID1), "pod %s reports a different device after restarts: %s (expected %s)", pod1.Name, devID1Restarted, devID1)

			ginkgo.By("Verifying the device assignment after pod and kubelet restart using podresources API")
			gomega.Eventually(ctx, func() error {
				v1PodResources, err = getV1NodeDevices(ctx)
				return err
			}, 30*time.Second, framework.Poll).ShouldNot(gomega.HaveOccurred(), "cannot fetch the compute resource assignment after kubelet restart")

			err, _ = checkPodResourcesAssignment(v1PodResources, pod1.Namespace, pod1.Name, pod1.Spec.Containers[0].Name, e2enode.SampleDeviceResourceName, []string{devID1})
			framework.ExpectNoError(err, "inconsistent device assignment after pod restart")
		})

		ginkgo.It("will not attempt to admit the succeeded pod after the kubelet restart and device plugin removed", func(ctx context.Context) {
			podRECMD := fmt.Sprintf("devs=$(ls /tmp/ | egrep '^Dev-[0-9]+$') && echo stub devices: $devs && sleep %s", sleepIntervalToCompletion)
			podSpec := makeBusyboxPod(e2enode.SampleDeviceResourceName, podRECMD)
			podSpec.Spec.RestartPolicy = v1.RestartPolicyNever
			// 确保 Pod 不会被垃圾回收，并在之后的 kubelet 重启中保留
			// 它已进入终止状态。使用 finalizer 使测试更可靠。
			podSpec.ObjectMeta.Finalizers = []string{testFinalizer}
			pod := e2epod.NewPodClient(f).CreateSync(ctx, podSpec)

			deviceIDRE := "stub devices: (Dev-[0-9]+)"
			devID1, err := parseLog(ctx, f, pod.Name, pod.Name, deviceIDRE)
			framework.ExpectNoError(err, "getting logs for pod %q", pod.Name)

			gomega.Expect(devID1).To(gomega.Not(gomega.Equal("")), "pod requested a device but started successfully without")

			pod, err = e2epod.NewPodClient(f).Get(ctx, pod.Name, metav1.GetOptions{})
			framework.ExpectNoError(err)

			ginkgo.By("Wait for node to be ready")
			gomega.Expect(e2enode.WaitForAllNodesSchedulable(ctx, f.ClientSet, 5*time.Minute)).To(gomega.Succeed())

			ginkgo.By("Waiting for pod to succeed")
			gomega.Expect(e2epod.WaitForPodSuccessInNamespace(ctx, f.ClientSet, pod.Name, pod.Namespace)).To(gomega.Succeed())

			ginkgo.By("Deleting the device plugin")
			e2epod.NewPodClient(f).DeleteSync(ctx, devicePluginPod.Name, metav1.DeleteOptions{}, f.Timeouts.PodDelete)
			waitForContainerRemoval(ctx, devicePluginPod.Spec.Containers[0].Name, devicePluginPod.Name, devicePluginPod.Namespace)

			gomega.Eventually(getNodeResourceValues, devicePluginGracefulTimeout, f.Timeouts.Poll).WithContext(ctx).WithArguments(e2enode.SampleDeviceResourceName).Should(gomega.Equal(ResourceValue{Allocatable: 0, Capacity: int(e2enode.SampleDevsAmount)}))

			ginkgo.By("Restarting Kubelet")
			restartKubelet(ctx, true)

			ginkgo.By("Wait for node to be ready again")
			gomega.Expect(e2enode.WaitForAllNodesSchedulable(ctx, f.ClientSet, 5*time.Minute)).To(gomega.Succeed())

			ginkgo.By("Pod should still be in Succeed state")
			// 这确保 Pod 已被成功准入。
			// 过去曾出现过 kubelet 尝试重新准入已终止 Pod 并将其 phase 改为 Failed 的问题。
			// 没有任何迹象表明 Pod 被重新准入，因此节点就绪后只需等待一分钟。
			gomega.Consistently(func() v1.PodPhase {
				pod, err = f.ClientSet.CoreV1().Pods(f.Namespace.Name).Get(ctx, pod.Name, metav1.GetOptions{})
				return pod.Status.Phase
			}, 1*time.Minute, f.Timeouts.Poll).Should(gomega.Equal(v1.PodSucceeded))

			ginkgo.By("Removing the finalizer from the pod so it can be deleted now")
			e2epod.NewPodClient(f).RemoveFinalizer(context.TODO(), podSpec.Name, testFinalizer)
		})

		// 模拟 device plugin 重新注册，但*不*重启容器和 kubelet。
		// device plugin 重新注册后，健康设备列表会根据发现的设备重新填充。
		// Pod2 运行后，我们确定分配给它的设备。只要设备分配成功，
		// 测试就应通过。
		ginkgo.It("Keeps device plugin assignments after the device plugin has restarted (no kubelet restart, pod restart)", func(ctx context.Context) {
			podRECMD := fmt.Sprintf("devs=$(ls /tmp/ | egrep '^Dev-[0-9]+$') && echo stub devices: $devs && sleep %s", sleepIntervalForever)
			pod1 := e2epod.NewPodClient(f).CreateSync(ctx, makeBusyboxPod(e2enode.SampleDeviceResourceName, podRECMD))
			deviceIDRE := "stub devices: (Dev-[0-9]+)"
			devID1, err := parseLog(ctx, f, pod1.Name, pod1.Name, deviceIDRE)
			framework.ExpectNoError(err, "getting logs for pod %q", pod1.Name)
			gomega.Expect(devID1).To(gomega.Not(gomega.Equal("")), "pod1 requested a device but started successfully without")

			pod1, err = e2epod.NewPodClient(f).Get(ctx, pod1.Name, metav1.GetOptions{})
			framework.ExpectNoError(err)

			ginkgo.By("Wait for node to be ready again")
			framework.ExpectNoError(e2enode.WaitForAllNodesSchedulable(ctx, f.ClientSet, 5*time.Minute))

			ginkgo.By("Re-Register resources and delete the plugin pod")
			gp := int64(0)
			deleteOptions := metav1.DeleteOptions{
				GracePeriodSeconds: &gp,
			}
			e2epod.NewPodClient(f).DeleteSync(ctx, devicePluginPod.Name, deleteOptions, f.Timeouts.PodDelete)
			waitForContainerRemoval(ctx, devicePluginPod.Spec.Containers[0].Name, devicePluginPod.Name, devicePluginPod.Namespace)

			ginkgo.By("Recreating the plugin pod")
			devicePluginPod = e2epod.NewPodClient(f).CreateSync(ctx, dptemplate)
			err = e2epod.WaitTimeoutForPodRunningInNamespace(ctx, f.ClientSet, devicePluginPod.Name, devicePluginPod.Namespace, 1*time.Minute)
			framework.ExpectNoError(err)

			ginkgo.By("Waiting for resource to become available on the local node after re-registration")
			gomega.Eventually(ctx, func() bool {
				node, ready := getLocalTestNode(ctx, f)
				return ready &&
					e2enode.CountSampleDeviceCapacity(node) == e2enode.SampleDevsAmount &&
					e2enode.CountSampleDeviceAllocatable(node) == e2enode.SampleDevsAmount
			}, 30*time.Second, framework.Poll).Should(gomega.BeTrueBecause("expected resource to be available after re-registration"))

			// 交叉检查 device plugin 重启后设备分配是否保留，且
			// 从 kubelet 角度看是否稳定。
			// 注意，不再检查容器日志：该检查在启动时完成，容器
			// 从未重启（在此测试时间尺度上"永久"运行），因此重复此检查
			// 毫无意义。
			ginkgo.By("Verifying the device assignment after device plugin restart using podresources API")
			gomega.Eventually(ctx, func() error {
				v1PodResources, err = getV1NodeDevices(ctx)
				return err
			}, 30*time.Second, framework.Poll).ShouldNot(gomega.HaveOccurred(), "cannot fetch the compute resource assignment after kubelet restart")

			err, _ = checkPodResourcesAssignment(v1PodResources, pod1.Namespace, pod1.Name, pod1.Spec.Containers[0].Name, e2enode.SampleDeviceResourceName, []string{devID1})
			framework.ExpectNoError(err, "inconsistent device assignment after pod restart")
		})

		// 模拟 kubelet 重启*和* device plugin 重启，同时 Pod 和容器保持运行。
		// 设备分配应在 kubelet/device plugin 重启后保留且稳定，因为这两个组件
		// 共同负责设备分配，而实际消费者（容器）保持稳定。
		ginkgo.It("Keeps device plugin assignments after kubelet restart and device plugin restart (no pod restart)", func(ctx context.Context) {
			podRECMD := fmt.Sprintf("devs=$(ls /tmp/ | egrep '^Dev-[0-9]+$') && echo stub devices: $devs && sleep %s", sleepIntervalForever) // 在此测试时间尺度内，Pod 必须"永久"运行
			pod1 := e2epod.NewPodClient(f).CreateSync(ctx, makeBusyboxPod(e2enode.SampleDeviceResourceName, podRECMD))
			deviceIDRE := "stub devices: (Dev-[0-9]+)"
			devID1, err := parseLog(ctx, f, pod1.Name, pod1.Name, deviceIDRE)
			framework.ExpectNoError(err, "getting logs for pod %q", pod1.Name)

			gomega.Expect(devID1).To(gomega.Not(gomega.BeEmpty()), "pod1 requested a device but started successfully without")

			pod1, err = e2epod.NewPodClient(f).Get(ctx, pod1.Name, metav1.GetOptions{})
			framework.ExpectNoError(err)

			ginkgo.By("Restarting Kubelet")
			restartKubelet(ctx, true)

			ginkgo.By("Wait for node to be ready again")
			framework.ExpectNoError(e2enode.WaitForAllNodesSchedulable(ctx, f.ClientSet, 5*time.Minute))

			ginkgo.By("Checking the same instance of the pod is still running after kubelet restart")
			gomega.Eventually(ctx, getPodByName).
				WithArguments(f, pod1.Name).
				WithTimeout(time.Minute).
				Should(BeTheSamePodStillRunning(pod1),
					"the same pod instance not running across kubelet restarts, workload should not be perturbed by kubelet restarts")

			// 从 kubelet 角度交叉检查设备分配是否保留且稳定。
			// 注意，不再检查容器日志：该检查在启动时完成，容器
			// 从未重启（在此测试时间尺度上"永久"运行），因此重复此检查
			// 毫无意义。
			ginkgo.By("Verifying the device assignment after kubelet restart using podresources API")
			gomega.Eventually(ctx, func() error {
				v1PodResources, err = getV1NodeDevices(ctx)
				return err
			}, 30*time.Second, framework.Poll).ShouldNot(gomega.HaveOccurred(), "cannot fetch the compute resource assignment after kubelet restart")

			err, _ = checkPodResourcesAssignment(v1PodResources, pod1.Namespace, pod1.Name, pod1.Spec.Containers[0].Name, e2enode.SampleDeviceResourceName, []string{devID1})
			framework.ExpectNoError(err, "inconsistent device assignment after pod restart")

			ginkgo.By("Re-Register resources by deleting the plugin pod")
			gp := int64(0)
			deleteOptions := metav1.DeleteOptions{
				GracePeriodSeconds: &gp,
			}
			e2epod.NewPodClient(f).DeleteSync(ctx, devicePluginPod.Name, deleteOptions, f.Timeouts.PodDelete)
			waitForContainerRemoval(ctx, devicePluginPod.Spec.Containers[0].Name, devicePluginPod.Name, devicePluginPod.Namespace)

			ginkgo.By("Recreating the plugin pod")
			devicePluginPod = e2epod.NewPodClient(f).CreateSync(ctx, dptemplate)

			ginkgo.By("Waiting for resource to become available on the local node after restart")
			gomega.Eventually(ctx, func() bool {
				node, ready := getLocalTestNode(ctx, f)
				return ready &&
					e2enode.CountSampleDeviceCapacity(node) == e2enode.SampleDevsAmount &&
					e2enode.CountSampleDeviceAllocatable(node) == e2enode.SampleDevsAmount
			}, 30*time.Second, framework.Poll).Should(gomega.BeTrueBecause("expected resource to be available after restart"))

			ginkgo.By("Checking the same instance of the pod is still running after the device plugin restart")
			gomega.Eventually(ctx, getPodByName).
				WithArguments(f, pod1.Name).
				WithTimeout(time.Minute).
				Should(BeTheSamePodStillRunning(pod1),
					"the same pod instance not running across kubelet restarts, workload should not be perturbed by device plugins restarts")
		})

		ginkgo.It("[OrphanedPods] Ensures pods consuming devices deleted while kubelet is down are cleaned up correctly", func(ctx context.Context) {
			podRECMD := fmt.Sprintf("devs=$(ls /tmp/ | egrep '^Dev-[0-9]+$') && echo stub devices: $devs && sleep %s", sleepIntervalWithRestart)
			pod := e2epod.NewPodClient(f).CreateSync(ctx, makeBusyboxPod(e2enode.SampleDeviceResourceName, podRECMD))

			deviceIDRE := "stub devices: (Dev-[0-9]+)"
			devID, err := parseLog(ctx, f, pod.Name, pod.Name, deviceIDRE)
			framework.ExpectNoError(err, "getting logs for pod %q", pod.Name)
			gomega.Expect(devID).To(gomega.Not(gomega.BeEmpty()), "pod1 requested a device but started successfully without")

			pod, err = e2epod.NewPodClient(f).Get(ctx, pod.Name, metav1.GetOptions{})
			framework.ExpectNoError(err)

			ginkgo.By("stopping the kubelet")
			restartKubelet := mustStopKubelet(ctx, f)

			// 等待 kubelet 健康检查失败
			gomega.Eventually(ctx, func() bool {
				ok := e2enode.HealthCheck(kubeletHealthCheckURL)
				framework.Logf("kubelet health check at %q value=%v", kubeletHealthCheckURL, ok)
				return ok
			}, f.Timeouts.PodStart, f.Timeouts.Poll).Should(gomega.BeFalseBecause("expected kubelet health check to be failed"))

			framework.Logf("Delete the pod while the kubelet is not running")
			// 按名称同步删除 Pod 将强制删除它，将其从 kubelet 配置中移除
			deletePodSyncByName(ctx, f, pod.Name)

			framework.Logf("Starting the kubelet")
			restartKubelet(ctx)

			framework.Logf("wait for the pod %v to disappear", pod.Name)
			gomega.Eventually(ctx, func(ctx context.Context) error {
				err := checkMirrorPodDisappear(ctx, f.ClientSet, pod.Name, pod.Namespace)
				framework.Logf("pod %s/%s disappear check err=%v", pod.Namespace, pod.Name, err)
				return err
			}, f.Timeouts.PodDelete, f.Timeouts.Poll).Should(gomega.BeNil())

			waitForAllContainerRemoval(ctx, pod.Name, pod.Namespace)

			ginkgo.By("Verifying the device assignment after device plugin restart using podresources API")
			gomega.Eventually(ctx, func() error {
				v1PodResources, err = getV1NodeDevices(ctx)
				return err
			}, 30*time.Second, framework.Poll).ShouldNot(gomega.HaveOccurred(), "cannot fetch the compute resource assignment after kubelet restart")
			err, allocated := checkPodResourcesAssignment(v1PodResources, pod.Namespace, pod.Name, pod.Spec.Containers[0].Name, e2enode.SampleDeviceResourceName, []string{})
			if err == nil || allocated {
				framework.Fail(fmt.Sprintf("stale device assignment after pod deletion while kubelet was down allocated=%v error=%v", allocated, err))
			}
		})

		f.It("Can schedule a pod with a restartable init container", framework.WithNodeConformance(), func(ctx context.Context) {
			podRECMD := "devs=$(ls /tmp/ | egrep '^Dev-[0-9]+$') && echo stub devices: $devs && sleep %s"
			sleepOneSecond := "1s"
			rl := v1.ResourceList{v1.ResourceName(e2enode.SampleDeviceResourceName): *resource.NewQuantity(1, resource.DecimalSI)}
			pod := &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: "device-plugin-test-" + string(uuid.NewUUID())},
				Spec: v1.PodSpec{
					RestartPolicy: v1.RestartPolicyAlways,
					InitContainers: []v1.Container{
						{
							Image:   busyboxImage,
							Name:    "init-1",
							Command: []string{"sh", "-c", fmt.Sprintf(podRECMD, sleepOneSecond)},
							Resources: v1.ResourceRequirements{
								Limits:   rl,
								Requests: rl,
							},
						},
						{
							Image:   busyboxImage,
							Name:    "restartable-init-2",
							Command: []string{"sh", "-c", fmt.Sprintf(podRECMD, sleepIntervalForever)},
							Resources: v1.ResourceRequirements{
								Limits:   rl,
								Requests: rl,
							},
							RestartPolicy: &containerRestartPolicyAlways,
						},
					},
					Containers: []v1.Container{{
						Image:   busyboxImage,
						Name:    "regular-1",
						Command: []string{"sh", "-c", fmt.Sprintf(podRECMD, sleepIntervalForever)},
						Resources: v1.ResourceRequirements{
							Limits:   rl,
							Requests: rl,
						},
					}},
				},
			}

			pod1 := e2epod.NewPodClient(f).CreateSync(ctx, pod)
			deviceIDRE := "stub devices: (Dev-[0-9]+)"

			devID1, err := parseLog(ctx, f, pod1.Name, pod1.Spec.InitContainers[0].Name, deviceIDRE)
			framework.ExpectNoError(err, "getting logs for pod %q/%q", pod1.Name, pod1.Spec.InitContainers[0].Name)
			gomega.Expect(devID1).To(gomega.Not(gomega.Equal("")), "pod1's init container requested a device but started successfully without")

			devID2, err := parseLog(ctx, f, pod1.Name, pod1.Spec.InitContainers[1].Name, deviceIDRE)
			framework.ExpectNoError(err, "getting logs for pod %q/%q", pod1.Name, pod1.Spec.InitContainers[1].Name)
			gomega.Expect(devID2).To(gomega.Not(gomega.Equal("")), "pod1's restartable init container requested a device but started successfully without")

			gomega.Expect(devID2).To(gomega.Equal(devID1), "pod1's init container and restartable init container should share the same device")

			devID3, err := parseLog(ctx, f, pod1.Name, pod1.Spec.Containers[0].Name, deviceIDRE)
			framework.ExpectNoError(err, "getting logs for pod %q/%q", pod1.Name, pod1.Spec.Containers[0].Name)
			gomega.Expect(devID3).To(gomega.Not(gomega.Equal("")), "pod1's regular container requested a device but started successfully without")

			gomega.Expect(devID3).NotTo(gomega.Equal(devID2), "pod1's restartable init container and regular container should not share the same device")

			podResources, err := getV1NodeDevices(ctx)
			framework.ExpectNoError(err)

			framework.Logf("PodResources.PodResources:%+v\n", podResources.PodResources)
			framework.Logf("len(PodResources.PodResources):%+v", len(podResources.PodResources))

			gomega.Expect(podResources.PodResources).To(gomega.HaveLen(2))

			var resourcesForOurPod *kubeletpodresourcesv1.PodResources
			for _, res := range podResources.GetPodResources() {
				if res.Name == pod1.Name {
					resourcesForOurPod = res
				}
			}

			gomega.Expect(resourcesForOurPod).NotTo(gomega.BeNil())

			gomega.Expect(resourcesForOurPod.Name).To(gomega.Equal(pod1.Name))
			gomega.Expect(resourcesForOurPod.Namespace).To(gomega.Equal(pod1.Namespace))

			gomega.Expect(resourcesForOurPod.Containers).To(gomega.HaveLen(2))

			for _, container := range resourcesForOurPod.Containers {
				if container.Name == pod1.Spec.InitContainers[1].Name {
					gomega.Expect(container.Devices).To(gomega.HaveLen(1))
					gomega.Expect(container.Devices[0].ResourceName).To(gomega.Equal(e2enode.SampleDeviceResourceName))
					gomega.Expect(container.Devices[0].DeviceIds).To(gomega.HaveLen(1))
				} else if container.Name == pod1.Spec.Containers[0].Name {
					gomega.Expect(container.Devices).To(gomega.HaveLen(1))
					gomega.Expect(container.Devices[0].ResourceName).To(gomega.Equal(e2enode.SampleDeviceResourceName))
					gomega.Expect(container.Devices[0].DeviceIds).To(gomega.HaveLen(1))
				} else {
					framework.Failf("unexpected container name: %s", container.Name)
				}
			}
		})
	})
}

func testDevicePluginNodeReboot(f *framework.Framework, pluginSockDir string) {
	f.Context("DevicePlugin", f.WithSerial(), f.WithDisruptive(), func() {
		var devicePluginPod *v1.Pod
		var v1PodResources *kubeletpodresourcesv1.ListPodResourcesResponse
		var triggerPathFile, triggerPathDir string
		var err error

		ginkgo.BeforeEach(func(ctx context.Context) {
			ginkgo.By("Wait for node to be ready")
			gomega.Eventually(ctx, e2enode.TotalReady).
				WithArguments(f.ClientSet).
				WithTimeout(time.Minute).
				Should(gomega.BeEquivalentTo(1))

			// 运行 device plugin 测试前，需要确保
			// 集群处于干净状态，且该节点上
			// 没有 Pod 在运行。
			// 这里使用 gomega.Eventually 带重试完成，因为其他测试套件中的前一个测试可能刚运行，其资源删除可能仍在进行。
			// 参见：https://issue.k8s.io/115381
			gomega.Eventually(ctx, func(ctx context.Context) error {
				v1PodResources, err = getV1NodeDevices(ctx)
				if err != nil {
					return fmt.Errorf("failed to get node local podresources by accessing the (v1) podresources API endpoint: %v", err)
				}

				if len(v1PodResources.PodResources) > 0 {
					return fmt.Errorf("expected v1 pod resources to be empty, but got non-empty resources: %+v", v1PodResources.PodResources)
				}
				return nil
			}, f.Timeouts.SystemDaemonsetStartup, f.Timeouts.Poll).Should(gomega.Succeed())

			ginkgo.By("Setting up the directory for controlling registration")
			triggerPathDir = filepath.Join(devicePluginDir, "sample")
			if _, err := os.Stat(triggerPathDir); err != nil {
				if errors.Is(err, os.ErrNotExist) {
					if err := os.Mkdir(triggerPathDir, os.ModePerm); err != nil {
						framework.Fail(fmt.Sprintf("registration control directory %q creation failed: %v ", triggerPathDir, err))
					}
					framework.Logf("registration control directory created successfully")
				} else {
					framework.Fail(fmt.Sprintf("unexpected error checking %q: %v", triggerPathDir, err))
				}
			} else {
				framework.Logf("registration control directory %q already present", triggerPathDir)
			}

			ginkgo.By("Setting up the file trigger for controlling registration")
			triggerPathFile = filepath.Join(triggerPathDir, "registration")
			if _, err := os.Stat(triggerPathFile); err != nil {
				if errors.Is(err, os.ErrNotExist) {
					if _, err = os.Create(triggerPathFile); err != nil {
						framework.Fail(fmt.Sprintf("registration control file %q creation failed: %v", triggerPathFile, err))
					}
					framework.Logf("registration control file created successfully")
				} else {
					framework.Fail(fmt.Sprintf("unexpected error creating %q: %v", triggerPathFile, err))
				}
			} else {
				framework.Logf("registration control file %q already present", triggerPathFile)
			}

			ginkgo.By("Scheduling a sample device plugin pod")
			data, err := e2etestfiles.Read(e2enode.SampleDevicePluginControlRegistrationDSYAML)
			if err != nil {
				framework.Fail(fmt.Sprintf("error reading test data %q: %v", e2enode.SampleDevicePluginControlRegistrationDSYAML, err))
			}
			ds := readDaemonSetV1OrDie(data)

			dp := &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name: e2enode.SampleDevicePluginName,
				},
				Spec: ds.Spec.Template.Spec,
			}

			devicePluginPod = e2epod.NewPodClient(f).CreateSync(ctx, dp)

			framework.Logf("Waiting for device plugin pod to be Running")
			err = e2epod.WaitForPodCondition(ctx, f.ClientSet, devicePluginPod.Namespace, devicePluginPod.Name, "Ready", 2*time.Minute, testutils.PodRunningReady)
			if err != nil {
				framework.Logf("Sample Device Pod %v took too long to enter running/ready: %v", dp.Name, err)
			}
			framework.ExpectNoError(err, "WaitForPodCondition() failed err: %v", err)

			ginkgo.By("making sure all the pods are ready")
			err = e2epod.WaitForPodCondition(ctx, f.ClientSet, devicePluginPod.Namespace, devicePluginPod.Name, "Ready", 120*time.Second, testutils.PodRunningReady)
			framework.ExpectNoError(err, "pod %s/%s did not go running", devicePluginPod.Namespace, devicePluginPod.Name)
			framework.Logf("pod %s/%s running", devicePluginPod.Namespace, devicePluginPod.Name)

			ginkgo.By("Verifying the devicePluginPod handleRegistrationProcess entered twice in the loop before we delete the registration file to trigger manual registration")
			gomega.Eventually(ctx, func() bool {
				logs, err := e2epod.GetPodLogs(ctx, f.ClientSet, devicePluginPod.Namespace, devicePluginPod.Name, devicePluginPod.Spec.Containers[0].Name)
				if err != nil {
					framework.Logf("error getting logs for pod %q: %v", devicePluginPod.Name, err)
					return false // 返回 false 以继续尝试
				}
				framework.Logf("got pod logs: %v", logs)
				regex := regexp.MustCompile("Starting watching routine(.?)")
				matches := regex.Find([]byte(logs))
				if matches == nil {
					framework.Logf("handleRegistrationProcess not started for pod %q", devicePluginPod.Name)
					return false // 返回 false 以继续尝试
				}
				framework.Logf("handleRegistrationProcess started: %s", matches)
				return true
			}, 60*time.Second, framework.Poll).Should(gomega.BeTrueBecause("expected watching routine"))

			go func() {
				// 由于 device plugin 的自动注册已禁用（因为指定了
				// REGISTER_CONTROL_FILE 环境变量），需要手动触发 device plugin 注册
				// manually.
				// 方法是删除以下路径的控制文件：
				// `/var/lib/kubelet/device-plugins/sample/registration`。

				defer ginkgo.GinkgoRecover()
				framework.Logf("Deleting the control file: %q to trigger registration", triggerPathFile)
				err := os.Remove(triggerPathFile)
				framework.ExpectNoError(err)
			}()

			ginkgo.By("Waiting for devices to become available on the local node")
			gomega.Eventually(ctx, func(ctx context.Context) bool {
				node, ready := getLocalTestNode(ctx, f)
				return ready && e2enode.CountSampleDeviceCapacity(node) > 0
			}, 5*time.Minute, framework.Poll).Should(gomega.BeTrueBecause("expected devices to be available on the local node"))
			framework.Logf("Successfully created device plugin pod")

			ginkgo.By(fmt.Sprintf("Waiting for the resource exported by the sample device plugin to become available on the local node (instances: %d)", e2enode.SampleDevsAmount))
			gomega.Eventually(ctx, func(ctx context.Context) bool {
				node, ready := getLocalTestNode(ctx, f)
				return ready &&
					e2enode.CountSampleDeviceCapacity(node) == e2enode.SampleDevsAmount &&
					e2enode.CountSampleDeviceAllocatable(node) == e2enode.SampleDevsAmount
			}, 2*time.Minute, framework.Poll).Should(gomega.BeTrueBecause("expected resource exported by the sample device plugin to be available on local node"))
		})

		ginkgo.AfterEach(func(ctx context.Context) {
			ginkgo.By("Get and print devicePluginPod logs")
			// 打印 Pod 的日志，帮助排查 devicePluginPod 手动注册设备失败的情况
			logs, err := e2epod.GetPodLogs(ctx, f.ClientSet, devicePluginPod.Namespace, devicePluginPod.Name, devicePluginPod.Spec.Containers[0].Name)
			if err != nil {
				framework.Logf("failed to get pod logs %v", err)
			} else {
				framework.Logf("logs of %s/%s (error: %v): %s", devicePluginPod.Name, devicePluginPod.Spec.Containers[0].Name, err, logs)
			}

			ginkgo.By("Deleting the device plugin pod")
			e2epod.NewPodClient(f).DeleteSync(ctx, devicePluginPod.Name, metav1.DeleteOptions{}, f.Timeouts.PodDelete)

			ginkgo.By("Deleting any Pods created by the test")
			l, err := e2epod.NewPodClient(f).List(ctx, metav1.ListOptions{})
			framework.ExpectNoError(err)
			for _, p := range l.Items {
				if p.Namespace != f.Namespace.Name {
					continue
				}

				ginkgo.By("Removing the finalizer from the pod in case it was used")
				e2epod.NewPodClient(f).RemoveFinalizer(context.TODO(), p.Name, testFinalizer)

				framework.Logf("Deleting pod: %s", p.Name)
				e2epod.NewPodClient(f).DeleteSync(ctx, p.Name, metav1.DeleteOptions{}, f.Timeouts.PodDelete)
			}

			err = os.Remove(triggerPathDir)
			framework.ExpectNoError(err)

			ginkgo.By("Waiting for devices to become unavailable on the local node")
			gomega.Eventually(ctx, func(ctx context.Context) bool {
				node, ready := getLocalTestNode(ctx, f)
				return ready && e2enode.CountSampleDeviceCapacity(node) <= 0
			}, 5*time.Minute, framework.Poll).Should(gomega.BeTrueBecause("expected devices to be unavailable on local node"))

			ginkgo.By("devices now unavailable on the local node")
		})

		// 通过在 kubelet 启动前用 CRI 移除 Pod 来模拟节点重启场景。此外，
		// 有意构造一种场景：节点重启后，请求设备的应用 Pod 先于暴露这些设备资源的
		// device plugin Pod 重启出现。预期行为是应用 Pod 在准入时失败。
		framework.It("Does not keep device plugin assignments across node reboots if fails admission (no pod restart, no device plugin re-registration)", func(ctx context.Context) {
			podRECMD := fmt.Sprintf("devs=$(ls /tmp/ | egrep '^Dev-[0-9]+$') && echo stub devices: $devs && sleep %s", sleepIntervalForever)
			testPod := makeBusyboxPod(e2enode.SampleDeviceResourceName, podRECMD)
			// 设置较短的终止宽限期以加快测试。
			// kubelet 重启后，此 Pod 将无法通过准入，需要被终止。
			// 默认的 30 秒宽限期会导致测试在等待 Pod
			// 从活跃 Pod 列表中过滤掉时超时。
			testPod.Spec.TerminationGracePeriodSeconds = ptr.To(int64(1))
			pod1 := e2epod.NewPodClient(f).CreateSync(ctx, testPod)
			deviceIDRE := "stub devices: (Dev-[0-9]+)"
			devID1, err := parseLog(ctx, f, pod1.Name, pod1.Name, deviceIDRE)
			framework.ExpectNoError(err, "getting logs for pod %q", pod1.Name)

			gomega.Expect(devID1).To(gomega.Not(gomega.Equal("")))

			pod1, err = e2epod.NewPodClient(f).Get(ctx, pod1.Name, metav1.GetOptions{})
			framework.ExpectNoError(err)

			ginkgo.By("stopping the kubelet")
			restartKubelet := mustStopKubelet(ctx, f)

			ginkgo.By("stopping all the local containers - using CRI")
			rs, _, err := getCRIClient(ctx)
			framework.ExpectNoError(err)
			sandboxes, err := rs.ListPodSandbox(ctx, &runtimeapi.PodSandboxFilter{})
			framework.ExpectNoError(err)
			for _, sandbox := range sandboxes {
				gomega.Expect(sandbox.Metadata).ToNot(gomega.BeNil())
				ginkgo.By(fmt.Sprintf("deleting pod using CRI: %s/%s -> %s", sandbox.Metadata.Namespace, sandbox.Metadata.Name, sandbox.Id))

				err := rs.RemovePodSandbox(ctx, sandbox.Id)
				framework.ExpectNoError(err)
				waitForAllContainerRemoval(ctx, sandbox.Metadata.Name, sandbox.Metadata.Namespace)
			}

			ginkgo.By("restarting the kubelet")
			restartKubelet(ctx)

			ginkgo.By("Wait for node to be ready again")
			framework.ExpectNoError(e2enode.WaitForAllNodesSchedulable(ctx, f.ClientSet, 5*time.Minute))

			ginkgo.By("Waiting for the pod to fail with admission error as device plugin hasn't re-registered yet")
			gomega.Eventually(ctx, getPod).
				WithArguments(f, pod1.Name).
				WithTimeout(time.Minute).
				Should(HaveFailedWithAdmissionError(),
					"the pod succeeded to start, when it should fail with the admission error")

			// 从 kubelet 角度交叉检查设备分配是否保留且稳定。
			// 注意，不再检查容器日志：该检查在启动时完成，容器
			// 从未重启（在此测试时间尺度上"永久"运行），因此重复此检查
			// 毫无意义。
			ginkgo.By("Verifying the device assignment after kubelet restart using podresources API")

			// 如果执行到这里，podresources API 现在应只报告 sample device plugin Pod，
			// 因为准入失败的 Pod 不再出现在列表中
			//（参见 https://github.com/kubernetes/kubernetes/pull/132028）。
			// 因此验证测试 Pod 不在 podresources 响应中。

			// 需要轮询，因为 Pod 准入失败与从 podresources 列表中移除
			gomega.Eventually(ctx, func(ctx context.Context) error {
				v1PodResources, err = getV1NodeDevices(ctx)
				if err != nil {
					return fmt.Errorf("failed to get node local podresources by accessing the (v1) podresources API endpoint: %w", err)
				} else {
					framework.Logf("Local podresources by accessing the (v1) podresources API enpoint: %+v", v1PodResources.PodResources)
				}
				if len(v1PodResources.PodResources) != 1 {
					return fmt.Errorf("expected v1 pod resources to be one, but got resources: %+v", v1PodResources.PodResources)
				}
				return nil
			}, f.Timeouts.PodDelete, f.Timeouts.Poll).Should(gomega.Succeed())

			// 之间存在延迟。
			gomega.Eventually(ctx, func(ctx context.Context) bool {
				v1PodResources, err = getV1NodeDevices(ctx)
				if err != nil {
					framework.Logf("Failed to get node devices: %v, will retry", err)
					return true // 出错时返回 true 以继续轮询
				}
				_, found := checkPodResourcesAssignment(v1PodResources, pod1.Namespace, pod1.Name, pod1.Spec.Containers[0].Name, e2enode.SampleDeviceResourceName, []string{})
				return found
			}, 30*time.Second, framework.Poll).Should(gomega.BeFalseBecause("%s/%s/%s failed admission, so it must not appear in podresources list", pod1.Namespace, pod1.Name, pod1.Spec.Containers[0].Name))
		})
	})
}

// makeBusyboxPod 返回一个简单的 Pod spec，其中包含一个 busybox 容器，
// 它请求 SampleDeviceResourceName 并运行指定命令。
func makeBusyboxPod(SampleDeviceResourceName, cmd string) *v1.Pod {
	podName := "device-plugin-test-" + string(uuid.NewUUID())
	rl := v1.ResourceList{v1.ResourceName(SampleDeviceResourceName): *resource.NewQuantity(1, resource.DecimalSI)}

	return &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: podName},
		Spec: v1.PodSpec{
			RestartPolicy: v1.RestartPolicyAlways,
			Containers: []v1.Container{{
				Image: busyboxImage,
				Name:  podName,
				// 在测试 Pod 中运行指定命令。
				Command: []string{"sh", "-c", cmd},
				Resources: v1.ResourceRequirements{
					Limits:   rl,
					Requests: rl,
				},
			}},
		},
	}
}

// ensurePodContainerRestart 确认 Pod 容器至少重启过一次
func ensurePodContainerRestart(ctx context.Context, f *framework.Framework, podName string, contName string) {
	var initialCount int32
	var currentCount int32
	p, err := e2epod.NewPodClient(f).Get(ctx, podName, metav1.GetOptions{})
	if err != nil || len(p.Status.ContainerStatuses) < 1 {
		framework.Failf("ensurePodContainerRestart failed for pod %q: %v", podName, err)
	}
	initialCount = p.Status.ContainerStatuses[0].RestartCount
	gomega.Eventually(ctx, func() int {
		p, err = e2epod.NewPodClient(f).Get(ctx, podName, metav1.GetOptions{})
		if err != nil || len(p.Status.ContainerStatuses) < 1 {
			return 0
		}
		currentCount = p.Status.ContainerStatuses[0].RestartCount
		framework.Logf("initial %v, current %v", initialCount, currentCount)
		return int(currentCount)
	}, 5*time.Minute, framework.Poll).Should(gomega.BeNumerically(">", initialCount))
}

// parseLog 从容器日志中解析指定正则的匹配字符串。
func parseLog(ctx context.Context, f *framework.Framework, podName string, contName string, re string) (string, error) {
	logs, err := e2epod.GetPodLogs(ctx, f.ClientSet, f.Namespace.Name, podName, contName)
	if err != nil {
		return "", err
	}

	framework.Logf("got pod logs: %v", logs)
	regex := regexp.MustCompile(re)
	matches := regex.FindStringSubmatch(logs)
	if len(matches) < 2 {
		return "", fmt.Errorf("unexpected match in logs: %q", logs)
	}

	return matches[1], nil
}

func checkPodResourcesAssignment(v1PodRes *kubeletpodresourcesv1.ListPodResourcesResponse, podNamespace, podName, containerName, resourceName string, devs []string) (error, bool) {
	for _, podRes := range v1PodRes.PodResources {
		if podRes.Namespace != podNamespace || podRes.Name != podName {
			continue
		}
		for _, contRes := range podRes.Containers {
			if contRes.Name != containerName {
				continue
			}
			return matchContainerDevices(podNamespace+"/"+podName+"/"+containerName, contRes.Devices, resourceName, devs)
		}
	}
	v1PodResStr := ""
	for _, p := range v1PodRes.PodResources {
		for _, c := range p.Containers {
			v1PodResStr += fmt.Sprintf("%s/%s/%s,", p.Namespace, p.Name, c.Name)
		}
	}
	err := fmt.Errorf("no resources found for %s/%s/%s in listpodresources [%s]", podNamespace, podName, containerName, v1PodResStr)
	framework.Logf("%v", err)
	return err, false
}

func matchContainerDevices(ident string, contDevs []*kubeletpodresourcesv1.ContainerDevices, resourceName string, devs []string) (error, bool) {
	expected := sets.New[string](devs...)
	assigned := sets.New[string]()
	for _, contDev := range contDevs {
		if contDev.ResourceName != resourceName {
			continue
		}
		assigned = assigned.Insert(contDev.DeviceIds...)
	}
	expectedStr := strings.Join(expected.UnsortedList(), ",")
	assignedStr := strings.Join(assigned.UnsortedList(), ",")
	framework.Logf("%s: devices expected %q assigned %q", ident, expectedStr, assignedStr)
	if !assigned.Equal(expected) {
		return fmt.Errorf("device allocation mismatch for %s expected %s assigned %s", ident, expectedStr, assignedStr), true
	}
	return nil, true
}

// getSampleDevicePluginPod 返回用于 e2e 测试的 Sample Device Plugin Pod。
func getSampleDevicePluginPod(pluginSockDir string, version string) *v1.Pod {
	data, err := e2etestfiles.Read(e2enode.SampleDevicePluginDSYAML)
	if err != nil {
		framework.Fail(err.Error())
	}

	ds := readDaemonSetV1OrDie(data)
	podName := e2enode.SampleDevicePluginName
	if version != "" {
		podName = podName + "-" + version
	}
	dp := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: podName,
		},
		Spec: ds.Spec.Template.Spec,
	}
	for i := range dp.Spec.Containers[0].Env {
		if dp.Spec.Containers[0].Env[i].Name == e2enode.SampleDeviceEnvVarNamePluginSockDir {
			dp.Spec.Containers[0].Env[i].Value = pluginSockDir
		}
	}

	dp.Spec.Containers[0].Env = append(dp.Spec.Containers[0].Env, v1.EnvVar{Name: "CDI_ENABLED", Value: "1"})
	if version != "" {
		dp.Spec.Containers[0].Env = append(dp.Spec.Containers[0].Env, v1.EnvVar{Name: "UNIQUE_NAME", Value: version})
	}

	return dp
}

func BeTheSamePodStillRunning(expected *v1.Pod) types.GomegaMatcher {
	return gomega.And(
		BeTheSamePodAs(expected.UID),
		BeAPodInPhase(v1.PodRunning),
		BeAPodReady(),
	)
}

// 如果 Pod 被报告为 ready，BeReady 匹配
func BeAPodReady() types.GomegaMatcher {
	return gcustom.MakeMatcher(func(actual *v1.Pod) (bool, error) {
		return podutils.IsPodReady(actual), nil
	}).WithTemplate("Pod {{.Actual.Namespace}}/{{.Actual.Name}} UID {{.Actual.UID}} not ready yet")
}

// 如果 Pod 正在运行，BeAPodInPhase 匹配
func BeAPodInPhase(phase v1.PodPhase) types.GomegaMatcher {
	return gcustom.MakeMatcher(func(actual *v1.Pod) (bool, error) {
		return actual.Status.Phase == phase, nil
	}).WithTemplate("Pod {{.Actual.Namespace}}/{{.Actual.Name}} failed {{.To}} be in phase {{.Data}} instead is in phase {{.Actual.Status.Phase}}").WithTemplateData(phase)
}

// 如果 Pod 具有给定的 UID，BeTheSamePodAs 匹配
func BeTheSamePodAs(podUID k8stypes.UID) types.GomegaMatcher {
	return gcustom.MakeMatcher(func(actual *v1.Pod) (bool, error) {
		return actual.UID == podUID, nil
	}).WithTemplate("Pod {{.Actual.Namespace}}/{{.Actual.Name}} expected UID {{.Data}} has UID instead {{.Actual.UID}}").WithTemplateData(podUID)
}
