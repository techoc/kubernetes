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

package windows

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	kubeletconfigv1beta1 "k8s.io/kubelet/config/v1beta1"
	kubeletconfig "k8s.io/kubernetes/pkg/kubelet/apis/config"
	kubeletconfigscheme "k8s.io/kubernetes/pkg/kubelet/apis/config/scheme"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/kubernetes/test/e2e/feature"
	"k8s.io/kubernetes/test/e2e/framework"
	e2eskipper "k8s.io/kubernetes/test/e2e/framework/skipper"
	imageutils "k8s.io/kubernetes/test/utils/image"
	admissionapi "k8s.io/pod-security-admission/api"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

var _ = sigDescribe(feature.Windows, "Memory Limits", framework.WithSerial(), framework.WithSlow(), skipUnlessWindows(func() {

	f := framework.NewDefaultFramework("memory-limit-test-windows")
	f.NamespacePodSecurityLevel = admissionapi.LevelPrivileged

	ginkgo.BeforeEach(func() {
		// 注意(vyta)：这些测试是 Windows 特有的
		e2eskipper.SkipUnlessNodeOSDistroIs("windows")
	})

	ginkgo.Context("Allocatable node memory", func() {
		ginkgo.It("should be equal to a calculated allocatable memory value", func(ctx context.Context) {
			checkNodeAllocatableTest(ctx, f)
		})
	})

	ginkgo.Context("attempt to deploy past allocatable memory limits", func() {
		ginkgo.It("should fail deployments of pods once there isn't enough memory", func(ctx context.Context) {
			overrideAllocatableMemoryTest(ctx, f, framework.TestContext.CloudConfig.NumNodes)
		})
	})
}))

type nodeMemory struct {
	// 容量
	capacity resource.Quantity
	// 可分配内存
	allocatable resource.Quantity
	// 为 OS 级进程预留的内存
	systemReserve resource.Quantity
	// 为 kubelet 预留的内存（未实现）
	kubeReserve resource.Quantity
	// grace period 内存限制（未实现）
	softEviction resource.Quantity
	// 无 grace period 内存限制
	hardEviction resource.Quantity
}

// runDensityBatchTest 运行批量创建 pod 的密度测试
// 检查计算出的 NodeAllocatable 值与上报值是否相等
func checkNodeAllocatableTest(ctx context.Context, f *framework.Framework) {

	nodeMem := getFirstNodeMemory(ctx, f)
	framework.Logf("nodeMem says: %+v", nodeMem)

	// 根据容量减去预留量计算可分配内存
	calculatedNodeAlloc := nodeMem.capacity.DeepCopy()
	calculatedNodeAlloc.Sub(nodeMem.systemReserve)
	calculatedNodeAlloc.Sub(nodeMem.kubeReserve)
	calculatedNodeAlloc.Sub(nodeMem.softEviction)
	calculatedNodeAlloc.Sub(nodeMem.hardEviction)

	// 对照上报的可分配值做健全性检查
	gomega.Expect(calculatedNodeAlloc.Cmp(nodeMem.allocatable)).To(gomega.Equal(0), "calculated allocatable memory %+v and stated allocatable memory %+v are same", calculatedNodeAlloc, nodeMem.allocatable)
}

// 部署 `allocatablePods + 1` 个 pod，每个 pod 的内存限制为总可分配
// 内存的 `1/allocatablePods`，然后确认最后一个 pod 因 failedScheduling 失败
func overrideAllocatableMemoryTest(ctx context.Context, f *framework.Framework, allocatablePods int) {
	selector := labels.Set{"kubernetes.io/os": "windows"}.AsSelector()
	nodeList, err := f.ClientSet.CoreV1().Nodes().List(ctx, metav1.ListOptions{
		LabelSelector: selector.String(),
	})
	framework.ExpectNoError(err)

	// 减去集群 admission 链注入的每个 pod 的 overhead
	// （Pod Overhead，KEP-688），使 limit+overhead 适配 Allocatable.Memory。
	overhead, err := detectPodOverheadMemory(ctx, f.ClientSet, f.Namespace.Name)
	framework.ExpectNoError(err, "detecting pod overhead memory")

	framework.Logf("Scheduling 1 pod per node to consume all allocatable memory (detected overhead: %d bytes)", overhead)
	for _, node := range nodeList.Items {
		status := node.Status
		// 减去 overhead（消耗 pod 自身的）、已有量（DaemonSets/系统 pod，
		// 含它们的 overhead）和安全缓冲（kubelet 记账噪声）。
		existing := sumExistingPodMemoryReservation(ctx, f.ClientSet, node.Name)
		podMemLimt := resource.NewQuantity(
			status.Allocatable.Memory().Value()-existing-overhead-windowsTestMemorySafetyBuffer,
			resource.BinarySI,
		)
		podName := "mem-test-" + string(uuid.NewUUID())
		framework.Logf("Scheduling pod %s on node %s (allocatable=%d, existing-reservation=%d, overhead=%d, safety-buffer=%d) with memory limit %v",
			podName, node.Name, status.Allocatable.Memory().Value(), existing, overhead, windowsTestMemorySafetyBuffer, podMemLimt)
		pod := &v1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name: podName,
			},
			Spec: v1.PodSpec{
				Containers: []v1.Container{
					{
						Name:  podName,
						Image: imageutils.GetPauseImageName(),
						Resources: v1.ResourceRequirements{
							Limits: v1.ResourceList{
								v1.ResourceMemory: *podMemLimt,
							},
						},
					},
				},
				NodeSelector: map[string]string{
					"kubernetes.io/os": "windows",
				},
				NodeName: node.Name,
			},
		}
		_, err = f.ClientSet.CoreV1().Pods(f.Namespace.Name).Create(ctx, pod, metav1.CreateOptions{})
		framework.ExpectNoError(err)
	}
	framework.Logf("Schedule additional pod which should not get scheduled")
	podName := "mem-failure-pod"
	failurePod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: podName,
		},
		Spec: v1.PodSpec{
			Containers: []v1.Container{
				{
					Name:  podName,
					Image: imageutils.GetPauseImageName(),
					Resources: v1.ResourceRequirements{
						Limits: v1.ResourceList{
							v1.ResourceMemory: *resource.NewQuantity(1024*1024*1024, resource.BinarySI),
						},
					},
				},
			},
			NodeSelector: map[string]string{
				"kubernetes.io/os": "windows",
			},
		},
	}
	framework.Logf("Ensuring that pod %s fails to schedule", podName)
	failurePod, err = f.ClientSet.CoreV1().Pods(f.Namespace.Name).Create(ctx, failurePod, metav1.CreateOptions{})
	framework.ExpectNoError(err)
	gomega.Eventually(ctx, func() error {
		eventList, err := f.ClientSet.CoreV1().Events(f.Namespace.Name).List(ctx, metav1.ListOptions{})
		if err != nil {
			return fmt.Errorf("error getting events: %w", err)
		}
		for _, e := range eventList.Items {
			// 寻找显示 FailedScheduling 的事件
			if e.Type == "Warning" && e.Reason == "FailedScheduling" && e.InvolvedObject.Name == failurePod.ObjectMeta.Name {
				framework.Logf("Found %+v event with message %+v", e.Reason, e.Message)
				return nil
			}
		}
		return fmt.Errorf("did not find any FailedScheduling event for pod %s", failurePod.ObjectMeta.Name)
	}, 3*time.Minute, 10*time.Second).Should(gomega.Succeed())
}

func getNodeMemory(ctx context.Context, f *framework.Framework, node v1.Node) nodeMemory {
	framework.Logf("Getting memory details for node %s", node.ObjectMeta.Name)
	request := f.ClientSet.CoreV1().RESTClient().Get().Resource("nodes").Name(node.ObjectMeta.Name).SubResource("proxy").Suffix("configz")
	rawbytes, err := request.DoRaw(ctx)
	framework.ExpectNoError(err)
	kubeletConfig, err := decodeConfigz(rawbytes)
	framework.ExpectNoError(err)

	systemReserve, err := resource.ParseQuantity(kubeletConfig.SystemReserved["memory"])
	if err != nil {
		systemReserve = *resource.NewQuantity(0, resource.BinarySI)
	}
	kubeReserve, err := resource.ParseQuantity(kubeletConfig.KubeReserved["memory"])
	if err != nil {
		kubeReserve = *resource.NewQuantity(0, resource.BinarySI)
	}
	hardEviction, err := resource.ParseQuantity(kubeletConfig.EvictionHard["memory.available"])
	if err != nil {
		hardEviction = *resource.NewQuantity(0, resource.BinarySI)
	}
	softEviction, err := resource.ParseQuantity(kubeletConfig.EvictionSoft["memory.available"])
	if err != nil {
		softEviction = *resource.NewQuantity(0, resource.BinarySI)
	}

	nodeMem := nodeMemory{
		capacity:      node.Status.Capacity[v1.ResourceMemory],
		allocatable:   node.Status.Allocatable[v1.ResourceMemory],
		systemReserve: systemReserve,
		hardEviction:  hardEviction,
		kubeReserve:   kubeReserve,
		softEviction:  softEviction,
	}

	return nodeMem
}

// getNodeMemory 用集群中找到的第一个 Windows 节点的信息
// 填充 nodeMemory 结构体。
func getFirstNodeMemory(ctx context.Context, f *framework.Framework) nodeMemory {
	selector := labels.Set{"kubernetes.io/os": "windows"}.AsSelector()
	nodeList, err := f.ClientSet.CoreV1().Nodes().List(ctx, metav1.ListOptions{
		LabelSelector: selector.String(),
	})
	framework.ExpectNoError(err)

	// 假设 agent 节点配置相同
	// 确保有 >0 个 agent 节点，然后用第一个获取信息
	gomega.Expect(nodeList.Items).ToNot(gomega.BeEmpty())

	ginkgo.By("Getting memory details from first Windows")
	return getNodeMemory(ctx, f, nodeList.Items[0])
}

// modified from https://github.com/kubernetes/kubernetes/blob/master/test/e2e/framework/kubelet/config.go#L110
// proxy 版本导致的问题，非 proxy 使用了 e2e 未设置的值
func decodeConfigz(contentsBytes []byte) (*kubeletconfig.KubeletConfiguration, error) {
	// 这个 hack 是因为 /configz 上报如下结构：
	// {"kubeletconfig": {the JSON representation of kubeletconfigv1beta1.KubeletConfiguration}}
	type configzWrapper struct {
		ComponentConfig kubeletconfigv1beta1.KubeletConfiguration `json:"kubeletconfig"`
	}

	configz := configzWrapper{}
	kubeCfg := kubeletconfig.KubeletConfiguration{}

	err := json.Unmarshal(contentsBytes, &configz)
	if err != nil {
		return nil, err
	}

	scheme, _, err := kubeletconfigscheme.NewSchemeAndCodecs()
	if err != nil {
		return nil, err
	}
	err = scheme.Convert(&configz.ComponentConfig, &kubeCfg, nil)
	if err != nil {
		return nil, err
	}

	return &kubeCfg, nil
}
