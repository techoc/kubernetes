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
	"k8s.io/kubernetes/pkg/features"
	e2epod "k8s.io/kubernetes/test/e2e/framework/pod"
	e2eskipper "k8s.io/kubernetes/test/e2e/framework/skipper"
	imageutils "k8s.io/kubernetes/test/utils/image"
	admissionapi "k8s.io/pod-security-admission/api"

	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/kubernetes/test/e2e/framework"
	"k8s.io/kubernetes/test/e2e_node/testdeviceplugin"
)

var _ = SIGDescribe("Device Plugin Failures Pod Status", framework.WithSlow(), framework.WithNodeConformance(), framework.WithFeatureGate(features.ResourceHealthStatus), func() {
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

	var createPodWrongImage = func(resourceName string, quantity int) *v1.Pod {
		ginkgo.GinkgoHelper()
		rl := v1.ResourceList{v1.ResourceName(resourceName): *resource.NewQuantity(int64(quantity), resource.DecimalSI)}
		pod := &v1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: "device-plugin-failures-test-" + string(uuid.NewUUID())},
			Spec: v1.PodSpec{
				RestartPolicy: v1.RestartPolicyAlways,
				Containers: []v1.Container{{
					Image:           imageutils.GetE2EImage(imageutils.InvalidRegistryImage),
					ImagePullPolicy: v1.PullAlways, // 避免测试因镜像未预拉取的校验而失败
					Name:            "container-1",
					Command:         []string{"sh", "-c", fmt.Sprintf("env && sleep %s", sleepIntervalForever)},
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

	ginkgo.It("will report a Healthy and then Unhealthy single device in the pod status", func(ctx context.Context) {
		e2eskipper.SkipUnlessFeatureGateEnabled(features.ResourceHealthStatus)

		// 随机化名称，使测试可以并行运行
		resourceName := fmt.Sprintf("test.device/%s", f.UniqueName)
		devices := []*kubeletdevicepluginv1beta1.Device{{ID: "testdevice", Health: kubeletdevicepluginv1beta1.Healthy}}
		plugin := testdeviceplugin.NewDevicePlugin(nil)

		err := plugin.RegisterDevicePlugin(ctx, f.UniqueName, resourceName, devices)
		defer plugin.Stop() // 即使注册失败也要停止
		gomega.Expect(err).To(gomega.Succeed())

		ginkgo.By("initial state: capacity and allocatable are set")
		gomega.Eventually(getNodeResourceValues, nodeStatusUpdateTimeout, f.Timeouts.Poll).WithContext(ctx).WithArguments(resourceName).Should(gomega.Equal(ResourceValue{Allocatable: 1, Capacity: 1}))

		// 创建请求该设备的 pod
		client := e2epod.NewPodClient(f)
		pod := client.Create(ctx, createPod(resourceName, 1))

		// 等待 pod 开始运行
		gomega.Expect(e2epod.WaitForPodRunningInNamespace(ctx, f.ClientSet, pod)).To(gomega.Succeed())

		expectedStatus := []v1.ResourceStatus{
			{
				Name: v1.ResourceName(resourceName),
				Resources: []v1.ResourceHealth{
					{
						ResourceID: "testdevice",
						Health:     v1.ResourceHealthStatusHealthy,
					},
				},
			},
		}

		gomega.Eventually(func() []v1.ResourceStatus {
			pod, err = f.ClientSet.CoreV1().Pods(f.Namespace.Name).Get(ctx, pod.Name, metav1.GetOptions{})
			return pod.Status.ContainerStatuses[0].AllocatedResourcesStatus
		}, devicePluginUpdateTimeout, f.Timeouts.Poll).Should(gomega.Equal(expectedStatus))

		// 将设备标记为不健康
		devices[0].Health = kubeletdevicepluginv1beta1.Unhealthy
		plugin.UpdateDevices(devices)

		expectedStatus[0].Resources[0] = v1.ResourceHealth{
			ResourceID: "testdevice",
			Health:     v1.ResourceHealthStatusUnhealthy,
		}

		gomega.Eventually(func() []v1.ResourceStatus {
			pod, err = f.ClientSet.CoreV1().Pods(f.Namespace.Name).Get(ctx, pod.Name, metav1.GetOptions{})
			return pod.Status.ContainerStatuses[0].AllocatedResourcesStatus
		}, devicePluginUpdateTimeout, f.Timeouts.Poll).Should(gomega.Equal(expectedStatus))

		// 删除 pod
		err = f.ClientSet.CoreV1().Pods(f.Namespace.Name).Delete(ctx, pod.Name, metav1.DeleteOptions{})
		gomega.Expect(err).To(gomega.Succeed())

		waitForContainerRemoval(ctx, pod.Spec.Containers[0].Name, pod.Name, pod.Namespace)
	})

	ginkgo.It("will report a Device Status for the failed pod in the pod status", func(ctx context.Context) {
		e2eskipper.SkipUnlessFeatureGateEnabled(features.ResourceHealthStatus)

		// 随机化名称，使测试可以并行运行
		resourceName := fmt.Sprintf("test.device/%s", f.UniqueName)
		devices := []*kubeletdevicepluginv1beta1.Device{{ID: "testdevice", Health: kubeletdevicepluginv1beta1.Healthy}}
		plugin := testdeviceplugin.NewDevicePlugin(nil)

		err := plugin.RegisterDevicePlugin(ctx, f.UniqueName, resourceName, devices)
		defer plugin.Stop() // 即使注册失败也要停止
		gomega.Expect(err).To(gomega.Succeed())

		ginkgo.By("initial state: capacity and allocatable are set")
		gomega.Eventually(getNodeResourceValues, nodeStatusUpdateTimeout, f.Timeouts.Poll).WithContext(ctx).WithArguments(resourceName).Should(gomega.Equal(ResourceValue{Allocatable: 1, Capacity: 1}))

		// 创建请求该设备的 pod
		client := e2epod.NewPodClient(f)
		pod := client.Create(ctx, createPodWrongImage(resourceName, 1))

		// 等待 pod 开始运行
		gomega.Expect(e2epod.WaitForPodCondition(ctx, f.ClientSet, pod.Namespace, pod.Name, "Back-off pulling image", f.Timeouts.PodStartShort,
			func(pod *v1.Pod) (bool, error) {
				if pod.Status.Phase == v1.PodPending &&
					len(pod.Status.ContainerStatuses) > 0 &&
					pod.Status.ContainerStatuses[0].State.Waiting != nil &&
					pod.Status.ContainerStatuses[0].State.Waiting.Reason == "ImagePullBackOff" {
					return true, nil
				}
				return false, nil
			})).To(gomega.Succeed())

		expectedStatus := []v1.ResourceStatus{
			{
				Name: v1.ResourceName(resourceName),
				Resources: []v1.ResourceHealth{
					{
						ResourceID: "testdevice",
						Health:     v1.ResourceHealthStatusHealthy,
					},
				},
			},
		}

		gomega.Eventually(func() []v1.ResourceStatus {
			pod, err = f.ClientSet.CoreV1().Pods(f.Namespace.Name).Get(ctx, pod.Name, metav1.GetOptions{})
			return pod.Status.ContainerStatuses[0].AllocatedResourcesStatus
		}, devicePluginUpdateTimeout, f.Timeouts.Poll).Should(gomega.Equal(expectedStatus))

		// 将设备标记为不健康
		devices[0].Health = kubeletdevicepluginv1beta1.Unhealthy
		plugin.UpdateDevices(devices)

		expectedStatus[0].Resources[0] = v1.ResourceHealth{
			ResourceID: "testdevice",
			Health:     "Unhealthy",
		}

		gomega.Eventually(func() []v1.ResourceStatus {
			pod, err = f.ClientSet.CoreV1().Pods(f.Namespace.Name).Get(ctx, pod.Name, metav1.GetOptions{})
			return pod.Status.ContainerStatuses[0].AllocatedResourcesStatus
		}, devicePluginUpdateTimeout, f.Timeouts.Poll).Should(gomega.Equal(expectedStatus))

		// 删除 pod
		err = f.ClientSet.CoreV1().Pods(f.Namespace.Name).Delete(ctx, pod.Name, metav1.DeleteOptions{})
		gomega.Expect(err).To(gomega.Succeed())

		waitForContainerRemoval(ctx, pod.Spec.Containers[0].Name, pod.Name, pod.Namespace)
	})
})
