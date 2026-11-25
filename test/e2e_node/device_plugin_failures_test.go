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

package e2enode

import (
	"context"
	"fmt"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"

	v1 "k8s.io/api/core/v1"
	kubeletdevicepluginv1beta1 "k8s.io/kubelet/pkg/apis/deviceplugin/v1beta1"
	e2epod "k8s.io/kubernetes/test/e2e/framework/pod"
	admissionapi "k8s.io/pod-security-admission/api"

	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/kubernetes/test/e2e/framework"
	"k8s.io/kubernetes/test/e2e_node/testdeviceplugin"
)

var _ = SIGDescribe("Device Plugin Failures:", framework.WithNodeConformance(), func() {
	f := framework.NewDefaultFramework("device-plugin-failures")
	f.NamespacePodSecurityLevel = admissionapi.LevelPrivileged

	type ResourceValue struct {
		Allocatable int
		Capacity    int
	}

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

	var createPod = func(resourceName string, quantity int) *v1.Pod {
		ginkgo.GinkgoHelper()
		rl := v1.ResourceList{v1.ResourceName(resourceName): *resource.NewQuantity(int64(quantity), resource.DecimalSI)}
		pod := &v1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: "device-plugin-failures-test-" + string(uuid.NewUUID())},
			Spec: v1.PodSpec{
				RestartPolicy: v1.RestartPolicyAlways,
				Containers: []v1.Container{{
					Image:   busyboxImage,
					Name:    "container-1",
					Command: []string{"sh", "-c", fmt.Sprintf("env && sleep %s", sleepIntervalForever)},
					Resources: v1.ResourceRequirements{
						Limits:   rl,
						Requests: rl,
					},
				}},
			},
		}
		return pod
	}

	nodeStatusUpdateTimeout := 1 * time.Minute
	devicePluginUpdateTimeout := 1 * time.Minute
	devicePluginGracefulTimeout := 5 * time.Minute // 参见 pkg/kubelet/cm/devicemanager/types.go 中的 endpointStopGracePeriod

	ginkgo.It("when GetDevicePluginOptions fails, device plugin will not be used", func(ctx context.Context) {
		// 随机化以便测试可并行运行
		resourceName := fmt.Sprintf("test.device/%s", f.UniqueName)

		expectedErr := fmt.Errorf("GetDevicePluginOptions failed")

		plugin := testdeviceplugin.NewDevicePlugin(func(name string) error {
			if name == "GetDevicePluginOptions" {
				return expectedErr
			}
			return nil
		})

		ginkgo.By("Wait enough for unix socket to be open")
		time.Sleep(time.Second)

		err := plugin.RegisterDevicePlugin(ctx, f.UniqueName, resourceName, []*kubeletdevicepluginv1beta1.Device{{ID: "testdevice", Health: kubeletdevicepluginv1beta1.Healthy}})
		defer plugin.Stop() // 即使注册失败也应停止
		gomega.Expect(err).To(gomega.MatchError(gomega.ContainSubstring("failed to get device plugin options")))
		gomega.Expect(err).To(gomega.MatchError(gomega.ContainSubstring(expectedErr.Error())))

		gomega.Expect(plugin.WasCalled("ListAndWatch")).To(gomega.BeFalseBecause("plugin should not be used if GetDevicePluginOptions fails"))
		gomega.Expect(plugin.WasCalled("GetDevicePluginOptions")).To(gomega.BeTrueBecause("get device plugin options should be called exactly once"))
		gomega.Expect(plugin.Calls()).To(gomega.HaveLen(1))

		// kubelet 甚至不会注册该资源
		gomega.Eventually(getNodeResourceValues, nodeStatusUpdateTimeout, f.Timeouts.Poll).WithContext(ctx).WithArguments(resourceName).Should(gomega.Equal(ResourceValue{Allocatable: -1, Capacity: -1}))
	})

	ginkgo.It("will set allocatable to zero when a single device became unhealthy and then back to 1 if it got healthy again", func(ctx context.Context) {
		// 随机化以便测试可并行运行
		resourceName := fmt.Sprintf("test.device/%s", f.UniqueName)
		devices := []*kubeletdevicepluginv1beta1.Device{{ID: "testdevice", Health: kubeletdevicepluginv1beta1.Healthy}}
		plugin := testdeviceplugin.NewDevicePlugin(nil)

		ginkgo.By("Wait enough for unix socket to be open")
		time.Sleep(time.Second)

		err := plugin.RegisterDevicePlugin(ctx, f.UniqueName, resourceName, devices)
		defer plugin.Stop() // 即使注册失败也应停止
		gomega.Expect(err).To(gomega.Succeed())

		// 起初设备是健康的
		gomega.Eventually(getNodeResourceValues, nodeStatusUpdateTimeout, f.Timeouts.Poll).WithContext(ctx).WithArguments(resourceName).Should(gomega.Equal(ResourceValue{Allocatable: 1, Capacity: 1}))

		// 现在让设备变得不健康
		devices[0].Health = kubeletdevicepluginv1beta1.Unhealthy
		plugin.UpdateDevices(devices)

		gomega.Eventually(getNodeResourceValues, nodeStatusUpdateTimeout, f.Timeouts.Poll).WithContext(ctx).WithArguments(resourceName).Should(gomega.Equal(ResourceValue{Allocatable: 0, Capacity: 1}))

		// 现在让设备恢复健康
		devices[0].Health = kubeletdevicepluginv1beta1.Healthy
		plugin.UpdateDevices(devices)

		gomega.Eventually(getNodeResourceValues, nodeStatusUpdateTimeout, f.Timeouts.Poll).WithContext(ctx).WithArguments(resourceName).Should(gomega.Equal(ResourceValue{Allocatable: 1, Capacity: 1}))
	})

	ginkgo.It("will set allocatable to zero when a single device became unhealthy, but capacity will stay at 1", func(ctx context.Context) {
		// 随机化以便测试可并行运行
		resourceName := fmt.Sprintf("test.device/%s", f.UniqueName)
		devices := []*kubeletdevicepluginv1beta1.Device{{ID: "testdevice", Health: kubeletdevicepluginv1beta1.Healthy}}
		plugin := testdeviceplugin.NewDevicePlugin(nil)

		ginkgo.By("Wait enough for unix socket to be open")
		time.Sleep(time.Second)

		err := plugin.RegisterDevicePlugin(ctx, f.UniqueName, resourceName, devices)
		defer plugin.Stop() // 即使注册失败也应停止
		gomega.Expect(err).To(gomega.Succeed())

		ginkgo.By("initial state: capacity and allocatable are set")
		gomega.Eventually(getNodeResourceValues, nodeStatusUpdateTimeout, f.Timeouts.Poll).WithContext(ctx).WithArguments(resourceName).Should(gomega.Equal(ResourceValue{Allocatable: 1, Capacity: 1}))

		// 调度一个请求该设备的 Pod
		client := e2epod.NewPodClient(f)
		pod := client.Create(ctx, createPod(resourceName, 1))

		// 等待 Pod 进入运行态
		gomega.Expect(e2epod.WaitForPodRunningInNamespace(ctx, f.ClientSet, pod)).To(gomega.Succeed())

		ginkgo.By("once pod is running, it does not affect allocatable value")
		gomega.Eventually(getNodeResourceValues, nodeStatusUpdateTimeout, f.Timeouts.Poll).WithContext(ctx).WithArguments(resourceName).Should(gomega.Equal(ResourceValue{Allocatable: 1, Capacity: 1}))

		// 现在让设备变得不健康
		devices[0].Health = kubeletdevicepluginv1beta1.Unhealthy
		plugin.UpdateDevices(devices)

		ginkgo.By("even when device became unhealthy. pod is still running and keeping the capacity")
		// 即使设备已不健康，我们仍保持 allocatable 值不变
		gomega.Eventually(getNodeResourceValues, nodeStatusUpdateTimeout, f.Timeouts.Poll).WithContext(ctx).WithArguments(resourceName).Should(gomega.Equal(ResourceValue{Allocatable: 0, Capacity: 1}))

		// Pod 不受设备变不健康的影响

		gomega.Consistently(func() v1.PodPhase {
			pod, err = f.ClientSet.CoreV1().Pods(f.Namespace.Name).Get(ctx, pod.Name, metav1.GetOptions{})
			return pod.Status.Phase
		}, devicePluginUpdateTimeout, f.Timeouts.Poll).Should(gomega.Equal(v1.PodRunning))

		// 删除 Pod
		err = f.ClientSet.CoreV1().Pods(f.Namespace.Name).Delete(ctx, pod.Name, metav1.DeleteOptions{})
		gomega.Expect(err).To(gomega.Succeed())

		// 等待 Pod 被删除
		gomega.Eventually(func() error {
			_, err := f.ClientSet.CoreV1().Pods(f.Namespace.Name).Get(ctx, pod.Name, metav1.GetOptions{})
			return err
		}, f.Timeouts.PodDelete, f.Timeouts.Poll).Should(gomega.MatchError((gomega.ContainSubstring("not found"))))

		ginkgo.By("when pod is deleted, nothing changes")
		gomega.Eventually(getNodeResourceValues, devicePluginGracefulTimeout+1*time.Minute, f.Timeouts.Poll).WithContext(ctx).WithArguments(resourceName).Should(gomega.Equal(ResourceValue{Allocatable: 0, Capacity: 1}))
	})

	ginkgo.It("will lower allocatable to a number of unhealthy devices and then back if they became healthy again", func(ctx context.Context) {
		// 随机化以便测试可并行运行
		resourceName := fmt.Sprintf("test.device/%s", f.UniqueName)

		devices := []*kubeletdevicepluginv1beta1.Device{
			{ID: "0", Health: kubeletdevicepluginv1beta1.Healthy},
			{ID: "1", Health: kubeletdevicepluginv1beta1.Healthy},
			{ID: "2", Health: kubeletdevicepluginv1beta1.Healthy},
			{ID: "3", Health: kubeletdevicepluginv1beta1.Healthy},
		}
		plugin := testdeviceplugin.NewDevicePlugin(nil)

		ginkgo.By("Wait enough for unix socket to be open")
		time.Sleep(time.Second)

		err := plugin.RegisterDevicePlugin(ctx, f.UniqueName, resourceName, devices)
		defer plugin.Stop() // 即使注册失败也应停止
		gomega.Expect(err).To(gomega.Succeed())

		// 起初所有设备都是健康的
		gomega.Eventually(getNodeResourceValues, nodeStatusUpdateTimeout, f.Timeouts.Poll).WithContext(ctx).WithArguments(resourceName).Should(gomega.Equal(ResourceValue{Allocatable: 4, Capacity: 4}))

		// 现在让一个设备变得不健康
		devices[3].Health = kubeletdevicepluginv1beta1.Unhealthy
		plugin.UpdateDevices(devices)

		gomega.Eventually(getNodeResourceValues, nodeStatusUpdateTimeout, f.Timeouts.Poll).WithContext(ctx).WithArguments(resourceName).Should(gomega.Equal(ResourceValue{Allocatable: 3, Capacity: 4}))

		// 现在让设备恢复健康
		devices[3].Health = kubeletdevicepluginv1beta1.Healthy
		plugin.UpdateDevices(devices)

		gomega.Eventually(getNodeResourceValues, nodeStatusUpdateTimeout, f.Timeouts.Poll).WithContext(ctx).WithArguments(resourceName).Should(gomega.Equal(ResourceValue{Allocatable: 4, Capacity: 4}))

		// 现在让两个设备变得不健康
		devices[1].Health = kubeletdevicepluginv1beta1.Unhealthy
		devices[3].Health = kubeletdevicepluginv1beta1.Unhealthy
		plugin.UpdateDevices(devices)

		gomega.Eventually(getNodeResourceValues, nodeStatusUpdateTimeout, f.Timeouts.Poll).WithContext(ctx).WithArguments(resourceName).Should(gomega.Equal(ResourceValue{Allocatable: 2, Capacity: 4}))

		// 现在让设备恢复健康
		devices[3].Health = kubeletdevicepluginv1beta1.Healthy
		plugin.UpdateDevices(devices)

		gomega.Eventually(getNodeResourceValues, nodeStatusUpdateTimeout, f.Timeouts.Poll).WithContext(ctx).WithArguments(resourceName).Should(gomega.Equal(ResourceValue{Allocatable: 3, Capacity: 4}))

		// 现在让设备恢复健康
		devices[1].Health = kubeletdevicepluginv1beta1.Healthy
		plugin.UpdateDevices(devices)

		gomega.Eventually(getNodeResourceValues, nodeStatusUpdateTimeout, f.Timeouts.Poll).WithContext(ctx).WithArguments(resourceName).Should(gomega.Equal(ResourceValue{Allocatable: 4, Capacity: 4}))
	})

	ginkgo.It("when ListAndWatch fails immediately, node allocatable will be set to zero and kubelet will not retry to list resources", func(ctx context.Context) {
		// 随机化以便测试可并行运行
		resourceName := fmt.Sprintf("test.device/%s", f.UniqueName)
		devices := []*kubeletdevicepluginv1beta1.Device{{ID: "testdevice", Health: kubeletdevicepluginv1beta1.Healthy}}

		// 起初该资源没有 allocatable
		gomega.Eventually(getNodeResourceValues, nodeStatusUpdateTimeout, f.Timeouts.Poll).WithContext(ctx).WithArguments(resourceName).Should(gomega.Equal(ResourceValue{Allocatable: -1, Capacity: -1}))

		plugin := testdeviceplugin.NewDevicePlugin(func(name string) error {
			if name == "ListAndWatch" {
				return fmt.Errorf("ListAndWatch failed")
			}
			return nil
		})

		ginkgo.By("Wait enough for unix socket to be open")
		time.Sleep(time.Second)

		err := plugin.RegisterDevicePlugin(ctx, f.UniqueName, resourceName, devices)
		defer plugin.Stop() // 即使注册失败也应停止
		gomega.Expect(err).To(gomega.Succeed())

		// kubelet 注册了该资源，但不会有任何 allocatable
		gomega.Eventually(getNodeResourceValues, nodeStatusUpdateTimeout, f.Timeouts.Poll).WithContext(ctx).WithArguments(resourceName).Should(gomega.Equal(ResourceValue{Allocatable: 0, Capacity: 0}))

		// kubelet 永远不会重试 ListAndWatch（它会长时间休眠）
		gomega.Consistently(plugin.Calls, devicePluginUpdateTimeout, f.Timeouts.Poll).Should(gomega.HaveLen(2))

		// 但 kubelet 不会删除该资源
		gomega.Eventually(getNodeResourceValues, nodeStatusUpdateTimeout, f.Timeouts.Poll).WithContext(ctx).WithArguments(resourceName).Should(gomega.Equal(ResourceValue{Allocatable: 0, Capacity: 0}))
	})

	ginkgo.It("when ListAndWatch fails after provisioning devices, node allocatable will be set to zero and kubelet will not retry to list resources", func(ctx context.Context) {
		// 随机化以便测试可并行运行
		resourceName := fmt.Sprintf("test.device/%s", f.UniqueName)
		devices := []*kubeletdevicepluginv1beta1.Device{
			{ID: "0", Health: kubeletdevicepluginv1beta1.Healthy},
			{ID: "1", Health: kubeletdevicepluginv1beta1.Healthy},
		}

		failing := false
		plugin := testdeviceplugin.NewDevicePlugin(func(name string) error {
			if name == "ListAndWatch" {
				if failing {
					return fmt.Errorf("ListAndWatch failed")
				}
			}
			return nil
		})

		ginkgo.By("Wait enough for unix socket to be open")
		time.Sleep(time.Second)

		err := plugin.RegisterDevicePlugin(ctx, f.UniqueName, resourceName, devices)
		defer plugin.Stop() // 即使注册失败也应停止
		gomega.Expect(err).To(gomega.Succeed())

		// 起初设备是健康的
		gomega.Eventually(getNodeResourceValues, nodeStatusUpdateTimeout, f.Timeouts.Poll).WithContext(ctx).WithArguments(resourceName).Should(gomega.Equal(ResourceValue{Allocatable: 2, Capacity: 2}))

		// 让 ListAndWatch 失败
		failing = true

		// kubelet 会将所有设备标记为不健康
		gomega.Eventually(getNodeResourceValues, nodeStatusUpdateTimeout, f.Timeouts.Poll).WithContext(ctx).WithArguments(resourceName).Should(gomega.Equal(ResourceValue{Allocatable: 0, Capacity: 2}))

		// kubelet 永远不会重试 ListAndWatch（它会长时间休眠）
		gomega.Consistently(plugin.Calls, devicePluginUpdateTimeout, f.Timeouts.Poll).Should(gomega.HaveLen(2))

		// 但 kubelet 不会删除该资源，且会保留容量
		gomega.Eventually(getNodeResourceValues, nodeStatusUpdateTimeout, f.Timeouts.Poll).WithContext(ctx).WithArguments(resourceName).Should(gomega.Equal(ResourceValue{Allocatable: 0, Capacity: 2}))

		// 宽限期后设备容量将重置为零
		gomega.Eventually(getNodeResourceValues, devicePluginGracefulTimeout+1*time.Minute, f.Timeouts.Poll).WithContext(ctx).WithArguments(resourceName).Should(gomega.Equal(ResourceValue{Allocatable: 0, Capacity: 0}))
	})

	ginkgo.It("when device plugin is stopped after provisioning devices, node allocatable will be set to zero", func(ctx context.Context) {
		// 随机化以便测试可并行运行
		resourceName := fmt.Sprintf("test.device/%s", f.UniqueName)
		devices := []*kubeletdevicepluginv1beta1.Device{
			{ID: "0", Health: kubeletdevicepluginv1beta1.Healthy},
			{ID: "1", Health: kubeletdevicepluginv1beta1.Healthy},
		}

		gomega.Eventually(getNodeResourceValues, nodeStatusUpdateTimeout, f.Timeouts.Poll).WithContext(ctx).WithArguments(resourceName).Should(gomega.Equal(ResourceValue{Allocatable: -1, Capacity: -1}))

		plugin := testdeviceplugin.NewDevicePlugin(nil)

		ginkgo.By("Wait enough for unix socket to be open")
		time.Sleep(time.Second)

		err := plugin.RegisterDevicePlugin(ctx, f.UniqueName, resourceName, devices)
		defer plugin.Stop() // 即使注册失败也应停止
		gomega.Expect(err).To(gomega.Succeed())

		// 起初设备是健康的
		gomega.Eventually(getNodeResourceValues, nodeStatusUpdateTimeout, f.Timeouts.Poll).WithContext(ctx).WithArguments(resourceName).Should(gomega.Equal(ResourceValue{Allocatable: 2, Capacity: 2}))

		// 卸载插件
		plugin.Stop()

		// kubelet 会将所有设备标记为不健康
		gomega.Eventually(getNodeResourceValues, nodeStatusUpdateTimeout, f.Timeouts.Poll).WithContext(ctx).WithArguments(resourceName).Should(gomega.Equal(ResourceValue{Allocatable: 0, Capacity: 2}))

		// 宽限期后设备容量将重置为零
		gomega.Eventually(getNodeResourceValues, devicePluginGracefulTimeout+1*time.Minute, f.Timeouts.Poll).WithContext(ctx).WithArguments(resourceName).Should(gomega.Equal(ResourceValue{Allocatable: 0, Capacity: 0}))
	})
})
