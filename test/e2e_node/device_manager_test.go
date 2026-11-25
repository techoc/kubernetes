/*
Copyright 2021 The Kubernetes Authors.

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

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/uuid"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
	admissionapi "k8s.io/pod-security-admission/api"

	"k8s.io/kubernetes/test/e2e/feature"
	"k8s.io/kubernetes/test/e2e/framework"
	e2enode "k8s.io/kubernetes/test/e2e/framework/node"
	e2epod "k8s.io/kubernetes/test/e2e/framework/pod"
	e2etestfiles "k8s.io/kubernetes/test/e2e/framework/testfiles"
	testutils "k8s.io/kubernetes/test/utils"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"github.com/onsi/gomega/gcustom"
	"github.com/onsi/gomega/types"
)

const (
	devicePluginDir = "/var/lib/kubelet/device-plugins"
)

// 串行执行，因为该测试会更新 kubelet 配置
var _ = SIGDescribe("Device Manager", framework.WithSerial(), feature.DeviceManager, func() {
	f := framework.NewDefaultFramework("devicemanager-test")
	f.NamespacePodSecurityLevel = admissionapi.LevelPrivileged

	/*
		该端到端测试模拟如下场景：kubelet 重启/节点重启后，请求设备的 application pod
		先于暴露设备资源的 device plugin pod 出现。

		正常路径是重启后 device plugin pod 先于 application pod 出现。该测试要覆盖的是
		device plugin 没有先出现、或没来得及重新注册自己的异常场景。

		由于无法控制重启后 pod 出现的先后顺序，无法保证 application pod 在 device plugin pod
		之前恢复（即这里要验证的场景）。即便 device plugin pod 先恢复，也可以不发送注册命令
		来有效复现该场景。为此对 sample device plugin 做了增强，实现细节参见：
		`test/images/sample-device-plugin/sampledeviceplugin.go`。该增强通过环境变量
		REGISTER_CONTROL_FILE 控制插件的自动注册：默认不设置该变量时，插件会自动向 kubelet
		注册。本测试使用带有
		REGISTER_CONTROL_FILE=/var/lib/kubelet/device-plugins/sample/registration 的
		sample device plugin spec，改为手动注册：先让请求设备的 application pod 能在节点上
		正常运行，然后重启 kubelet，此时 device plugin 不再注册，application pod 因准入错误失败。

		   该 e2e 测试的步骤如下：
		   1. 在 `/var/lib/kubelet/device-plugins/sample/` 下创建 `registration` 文件
		   2. 创建带有环境变量
		      `REGISTER_CONTROL_FILE=/var/lib/kubelet/device-plugins/sample/registration` 的
			  sample device plugin，等待客户端删除该控制文件
		   3. 删除上述目录触发插件注册
		   4. 创建请求 device plugin 所暴露设备的测试 pod
		   5. 停止 kubelet
		   6. 通过 CRI 移除 pod，确保 kubelet 重启后会创建新 pod
		   7. 重启 kubelet
		   8. 等待 sample device plugin pod 运行；此时注册不会被触发
		   9. 确认 device plugin 导出的资源 capacity/allocatable 为零
		   10. 测试 pod 应以 `UnexpectedAdmissionError` 失败
		   11. 删除测试 pod
		   12. 删除 sample device plugin pod
		   13. 删除 `/var/lib/kubelet/device-plugins/sample/` 及其内容，即用于控制注册的目录
	*/
	f.Context("With sample device plugin", f.WithSerial(), f.WithDisruptive(), func() {
		var deviceCount int = 2
		var devicePluginPod *v1.Pod
		var triggerPathFile, triggerPathDir string

		// 该测试要复现 https://github.com/kubernetes/kubernetes/issues/109595 中的问题
		ginkgo.BeforeEach(func(ctx context.Context) {
			ginkgo.By("Wait for node to be ready")
			gomega.Eventually(ctx, e2enode.TotalReady).
				WithArguments(f.ClientSet).
				WithTimeout(time.Minute).
				Should(gomega.BeEquivalentTo(1))

			// 运行 device plugin 测试前，需确保集群处于干净状态、该节点上没有运行中的 pod。
			// 这里用 gomega.Eventually 重试，因为其他测试套件中的前一个测试可能刚运行完，
			// 其资源删除可能仍在进行中。
			// xref: https://issue.k8s.io/115381
			gomega.Eventually(ctx, func(ctx context.Context) error {
				v1PodResources, err := getV1NodeDevices(ctx)
				if err != nil {
					return fmt.Errorf("failed to get node local podresources by accessing the (v1) podresources API endpoint: %w", err)
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

			ginkgo.By("making sure all the pods are ready")

			err = e2epod.WaitForPodCondition(ctx, f.ClientSet, devicePluginPod.Namespace, devicePluginPod.Name, "Ready", 120*time.Second, testutils.PodRunningReady)
			framework.ExpectNoError(err, "pod %s/%s did not go running", devicePluginPod.Namespace, devicePluginPod.Name)
			framework.Logf("pod %s/%s running", devicePluginPod.Namespace, devicePluginPod.Name)

			ginkgo.By("Verifying the devicePluginPod handleRegistrationProcess entered twice in the loop before we delete the registration file to trigger manual registration")
			gomega.Eventually(ctx, func() bool {
				logs, err := e2epod.GetPodLogs(ctx, f.ClientSet, devicePluginPod.Namespace, devicePluginPod.Name, devicePluginPod.Spec.Containers[0].Name)
				if err != nil {
					framework.Logf("error getting logs for pod %q: %v", devicePluginPod.Name, err)
					return false // Returning false to continue trying
				}
				framework.Logf("got pod logs: %v", logs)
				regex := regexp.MustCompile("Starting watching routine(.?)")
				matches := regex.Find([]byte(logs))
				if matches == nil {
					framework.Logf("handleRegistrationProcess not started for pod %q", devicePluginPod.Name)
					return false // Returning false to continue trying
				}
				framework.Logf("handleRegistrationProcess started: %s", matches)
				return true
			}, 60*time.Second, framework.Poll).Should(gomega.BeTrueBecause("expected watching routine"))

			go func() {
				// 由于指定了 REGISTER_CONTROL_FILE 环境变量，device plugin 的自动注册被禁用，
				// 需要手动触发注册：删除以下路径的控制文件
				// `/var/lib/kubelet/device-plugins/sample/registration`。

				defer ginkgo.GinkgoRecover()
				framework.Logf("Deleting the control file: %q to trigger registration", triggerPathFile)
				err := os.Remove(triggerPathFile)
				framework.ExpectNoError(err)
			}()

			ginkgo.By("Waiting for devices to become available on the local node")

			gomega.Eventually(ctx, isNodeReadyWithSampleResources).
				WithArguments(f).
				WithTimeout(5 * time.Minute).
				Should(BeReady())

			framework.Logf("Successfully created device plugin pod")

			devsLen := int64(deviceCount) // shortcut
			ginkgo.By("Waiting for the resource exported by the sample device plugin to become available on the local node")

			gomega.Eventually(ctx, isNodeReadyWithAllocatableSampleResources).
				WithArguments(f, devsLen).
				WithTimeout(5 * time.Minute).
				Should(HaveAllocatableDevices())
		})

		framework.It("should deploy pod consuming devices first but fail with admission error after kubelet restart in case device plugin hasn't re-registered", framework.WithFlaky(), func(ctx context.Context) {
			var err error
			podCMD := "while true; do sleep 1000; done;"

			ginkgo.By(fmt.Sprintf("creating a pods requiring %d %q", deviceCount, e2enode.SampleDeviceResourceName))

			pod := makeBusyboxDeviceRequiringPod(e2enode.SampleDeviceResourceName, podCMD)
			testPod := e2epod.NewPodClient(f).CreateSync(ctx, pod)

			ginkgo.By("making sure all the pods are ready")

			err = e2epod.WaitForPodCondition(ctx, f.ClientSet, testPod.Namespace, testPod.Name, "Ready", 120*time.Second, testutils.PodRunningReady)
			framework.ExpectNoError(err, "pod %s/%s did not go running", testPod.Namespace, testPod.Name)
			framework.Logf("pod %s/%s running", testPod.Namespace, testPod.Name)

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
			}

			ginkgo.By("restarting the kubelet")
			restartKubelet(ctx)

			ginkgo.By("waiting for the kubelet to be ready again")
			// 等待 kubelet 就绪

			gomega.Eventually(ctx, e2enode.TotalReady).
				WithArguments(f.ClientSet).
				WithTimeout(2 * time.Minute).
				Should(gomega.BeEquivalentTo(1))

			ginkgo.By("making sure all the pods are ready after the recovery")

			var devicePluginPodAfterRestart *v1.Pod

			devicePluginPodAfterRestart, err = e2epod.NewPodClient(f).Get(ctx, devicePluginPod.Name, metav1.GetOptions{})
			framework.ExpectNoError(err)

			err = e2epod.WaitForPodCondition(ctx, f.ClientSet, devicePluginPodAfterRestart.Namespace, devicePluginPodAfterRestart.Name, "Ready", 120*time.Second, testutils.PodRunningReady)
			framework.ExpectNoError(err, "pod %s/%s did not go running", devicePluginPodAfterRestart.Namespace, devicePluginPodAfterRestart.Name)
			framework.Logf("pod %s/%s running", devicePluginPodAfterRestart.Namespace, devicePluginPodAfterRestart.Name)

			ginkgo.By("Waiting for the resource capacity/allocatable exported by the sample device plugin to become zero")

			// device plugin pod 已重启但未向 kubelet 重新注册（AUTO_REGISTER=false），
			// 也没有通过向 `/var/lib/kubelet/device-plugins/registered` 暴露的 unix socket
			// 写入数据来手动触发注册。因此该插件暴露资源的 capacity 和 allocatable 应为零

			gomega.Eventually(ctx, isNodeReadyWithAllocatableSampleResources).
				WithArguments(f, int64(0)).
				WithTimeout(5 * time.Minute).
				Should(HaveAllocatableDevices())

			ginkgo.By("Checking that pod requesting devices failed to start because of admission error")

			// 注意：device plugin 不会再次注册，这是有意为之。
			// 因此请求设备的测试 pod 应以准入错误失败

			gomega.Eventually(ctx, getPod).
				WithArguments(f, testPod.Name).
				WithTimeout(time.Minute).
				Should(HaveFailedWithAdmissionError(),
					"the pod succeeded to start, when it should fail with the admission error")

			ginkgo.By("removing application pods")
			e2epod.NewPodClient(f).DeleteSync(ctx, testPod.Name, metav1.DeleteOptions{}, f.Timeouts.PodDelete)
		})

		ginkgo.AfterEach(func(ctx context.Context) {

			ginkgo.By("Get and print devicePluginPod logs")
			// 打印 pod 日志，便于排查 devicePluginPod 手动注册设备失败的情况
			logs, err := e2epod.GetPodLogs(ctx, f.ClientSet, devicePluginPod.Namespace, devicePluginPod.Name, devicePluginPod.Spec.Containers[0].Name)
			if err != nil {
				framework.Logf("failed to get pod logs %v", err)
			} else {
				framework.Logf("logs of %s/%s/%s (error: %v): %s", devicePluginPod.Namespace, devicePluginPod.Name, devicePluginPod.Spec.Containers[0].Name, err, logs)
			}

			ginkgo.By("Deleting the device plugin pod")
			e2epod.NewPodClient(f).DeleteSync(ctx, devicePluginPod.Name, metav1.DeleteOptions{}, f.Timeouts.PodDelete)

			ginkgo.By("Deleting the directory and file setup for controlling registration")
			err = os.RemoveAll(triggerPathDir)
			framework.ExpectNoError(err)

			ginkgo.By("Deleting any Pods created by the test")
			l, err := e2epod.NewPodClient(f).List(context.TODO(), metav1.ListOptions{})
			framework.ExpectNoError(err)
			for _, p := range l.Items {
				if p.Namespace != f.Namespace.Name {
					continue
				}

				framework.Logf("Deleting pod: %s", p.Name)
				e2epod.NewPodClient(f).DeleteSync(ctx, p.Name, metav1.DeleteOptions{}, f.Timeouts.PodDelete)
			}

			ginkgo.By("Waiting for devices to become unavailable on the local node")
			gomega.Eventually(ctx, isNodeReadyWithoutSampleResources).
				WithArguments(f).
				WithTimeout(5 * time.Minute).
				Should(BeReady())

			ginkgo.By("devices now unavailable on the local node")
		})

	})

})

func makeBusyboxDeviceRequiringPod(resourceName, cmd string) *v1.Pod {
	podName := "device-manager-test-" + string(uuid.NewUUID())
	rl := v1.ResourceList{
		v1.ResourceName(resourceName): *resource.NewQuantity(2, resource.DecimalSI),
	}
	return &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: podName,
		},
		Spec: v1.PodSpec{
			RestartPolicy: v1.RestartPolicyNever,
			Containers: []v1.Container{{
				Image: busyboxImage,
				Name:  podName,
				// 在测试 pod 中运行指定命令
				Command: []string{"sh", "-c", cmd},
				Resources: v1.ResourceRequirements{
					Limits:   rl,
					Requests: rl,
				},
			}},
		},
	}
}

// BeReady 校验节点已就绪且设备已注册
func BeReady() types.GomegaMatcher {
	return gomega.And(
		// 该额外的 matcher 用于检查最终的错误条件
		gcustom.MakeMatcher(func(ready bool) (bool, error) {
			if !ready {
				return false, fmt.Errorf("expected node to be ready=%t", ready)
			}
			return true, nil
		}),
		BeInReadyPhase(true),
	)
}

// BeInReadyPhase 校验节点处于指定就绪状态
func BeInReadyPhase(isReady bool) types.GomegaMatcher {
	return gcustom.MakeMatcher(func(ready bool) (bool, error) {
		return ready == isReady, nil
	}).WithTemplate("expected Node Ready {{.To}} be in {{format .Data}}\nGot instead:\n{{.FormattedActual}}").WithTemplateData(isReady)
}

func isNodeReadyWithSampleResources(ctx context.Context, f *framework.Framework) (bool, error) {
	node, ready := getLocalTestNode(ctx, f)
	if !ready {
		return false, fmt.Errorf("expected node to be ready=%t", ready)
	}

	if e2enode.CountSampleDeviceCapacity(node) <= 0 {
		return false, fmt.Errorf("expected devices to be advertised")
	}
	return true, nil
}

// HaveAllocatableDevices 校验节点上存在可分配的设备
func HaveAllocatableDevices() types.GomegaMatcher {
	return gomega.And(
		// 该额外的 matcher 用于检查最终的错误条件
		gcustom.MakeMatcher(func(hasAllocatable bool) (bool, error) {
			if !hasAllocatable {
				return false, fmt.Errorf("expected node to be have allocatable devices=%t", hasAllocatable)
			}
			return true, nil
		}),
		hasAllocatable(true),
	)
}

// hasAllocatable 校验节点是否具有可分配设备
func hasAllocatable(hasAllocatable bool) types.GomegaMatcher {
	return gcustom.MakeMatcher(func(hasAllocatableDevices bool) (bool, error) {
		return hasAllocatableDevices == hasAllocatable, nil
	}).WithTemplate("expected Node with allocatable {{.To}} be in {{format .Data}}\nGot instead:\n{{.FormattedActual}}").WithTemplateData(hasAllocatable)
}

func isNodeReadyWithAllocatableSampleResources(ctx context.Context, f *framework.Framework, devCount int64) (bool, error) {
	node, ready := getLocalTestNode(ctx, f)
	if !ready {
		return false, fmt.Errorf("expected node to be ready=%t", ready)
	}

	if e2enode.CountSampleDeviceCapacity(node) != devCount {
		return false, fmt.Errorf("expected devices capacity to be: %d", devCount)
	}

	if e2enode.CountSampleDeviceAllocatable(node) != devCount {
		return false, fmt.Errorf("expected devices allocatable to be: %d", devCount)
	}
	return true, nil
}

func isNodeReadyWithoutSampleResources(ctx context.Context, f *framework.Framework) (bool, error) {
	node, ready := getLocalTestNode(ctx, f)
	if !ready {
		return false, fmt.Errorf("expected node to be ready=%t", ready)
	}

	if e2enode.CountSampleDeviceCapacity(node) > 0 {
		return false, fmt.Errorf("expected devices to be not present")
	}
	return true, nil
}

// HaveFailedWithAdmissionError 校验 pod 在准入阶段失败
func HaveFailedWithAdmissionError() types.GomegaMatcher {
	return gomega.And(
		gcustom.MakeMatcher(func(hasFailed bool) (bool, error) {
			if !hasFailed {
				return false, fmt.Errorf("expected pod to have failed=%t", hasFailed)
			}
			return true, nil
		}),
		hasFailed(true),
	)
}

// hasFailed 校验 pod 是否失败
func hasFailed(hasFailed bool) types.GomegaMatcher {
	return gcustom.MakeMatcher(func(hasPodFailed bool) (bool, error) {
		return hasPodFailed == hasFailed, nil
	}).WithTemplate("expected Pod failed {{.To}} be in {{format .Data}}\nGot instead:\n{{.FormattedActual}}").WithTemplateData(hasFailed)
}

func getPodByName(ctx context.Context, f *framework.Framework, podName string) (*v1.Pod, error) {
	return e2epod.NewPodClient(f).Get(ctx, podName, metav1.GetOptions{})
}

func getPod(ctx context.Context, f *framework.Framework, podName string) (bool, error) {
	pod, err := getPodByName(ctx, f, podName)
	if err != nil {
		return false, err
	}

	expectedStatusReason := "UnexpectedAdmissionError"
	expectedStatusMessage := "Allocate failed due to no healthy devices present; cannot allocate unhealthy devices"

	// 该额外的 matcher 用于检查最终的错误条件
	if pod.Status.Phase != v1.PodFailed {
		return false, fmt.Errorf("expected pod to reach phase %q, got final phase %q instead.", v1.PodFailed, pod.Status.Phase)
	}
	if pod.Status.Reason != expectedStatusReason {
		return false, fmt.Errorf("expected pod status reason to be %q, got %q instead.", expectedStatusReason, pod.Status.Reason)
	}
	if !strings.Contains(pod.Status.Message, expectedStatusMessage) {
		return false, fmt.Errorf("expected pod status reason to contain %q, got %q instead.", expectedStatusMessage, pod.Status.Message)
	}
	return true, nil
}
