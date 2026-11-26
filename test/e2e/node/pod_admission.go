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

package node

import (
	"context"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"github.com/onsi/gomega/gstruct"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/kubernetes/test/e2e/framework"
	e2enode "k8s.io/kubernetes/test/e2e/framework/node"
	e2epod "k8s.io/kubernetes/test/e2e/framework/pod"
	imageutils "k8s.io/kubernetes/test/utils/image"
	admissionapi "k8s.io/pod-security-admission/api"
)

var _ = SIGDescribe("PodRejectionStatus", func() {
	f := framework.NewDefaultFramework("pod-rejection-status")
	f.NamespacePodSecurityLevel = admissionapi.LevelBaseline
	ginkgo.Context("Kubelet", func() {
		ginkgo.It("should reject pod when the node didn't have enough resource", func(ctx context.Context) {
			node, err := e2enode.GetRandomReadySchedulableNode(ctx, f.ClientSet)
			framework.ExpectNoError(err, "Failed to get a ready schedulable node")

			// 创建一个请求 CPU 超过节点拥有量的 Pod
			pod := &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "pod-out-of-cpu",
					Namespace: f.Namespace.Name,
				},
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{
							Name:  "pod-out-of-cpu",
							Image: imageutils.GetPauseImageName(),
							Resources: v1.ResourceRequirements{
								Requests: v1.ResourceList{
									v1.ResourceCPU: resource.MustParse("1000000000000"), // 请求的 CPU 超过任何节点拥有量
								},
							},
						},
					},
				},
			}

			pod, err = f.ClientSet.CoreV1().Pods(f.Namespace.Name).Create(ctx, pod, metav1.CreateOptions{})
			framework.ExpectNoError(err)

			// 等待 scheduler 更新 Pod 状态
			err = e2epod.WaitForPodNameUnschedulableInNamespace(ctx, f.ClientSet, pod.Name, pod.Namespace)
			framework.ExpectNoError(err)

			// 获取 Pod，校验 scheduler 已设置 PodScheduled 条件
			// 并带上 observedGeneration。
			pod, err = f.ClientSet.CoreV1().Pods(f.Namespace.Name).Get(ctx, pod.Name, metav1.GetOptions{})
			framework.ExpectNoError(err)
			gomega.Expect(len(pod.Status.Conditions)).To(gomega.BeEquivalentTo(1))
			gomega.Expect(pod.Status.Conditions[0].Type).To(gomega.BeEquivalentTo(v1.PodScheduled))
			gomega.Expect(pod.Status.Conditions[0].ObservedGeneration).To(gomega.BeEquivalentTo(1))

			// 获取 Pod 以拿到 scheduler 拒绝该 Pod 前
			// 最后一次观察到的最新状态
			pod, err = f.ClientSet.CoreV1().Pods(pod.Namespace).Get(ctx, pod.Name, metav1.GetOptions{})
			framework.ExpectNoError(err)

			// 强制把 Pod 分配到节点，以便稍后得到拒绝状态
			binding := &v1.Binding{
				ObjectMeta: metav1.ObjectMeta{
					Name:      pod.Name,
					Namespace: pod.Namespace,
					UID:       pod.UID,
				},
				Target: v1.ObjectReference{
					Kind: "Node",
					Name: node.Name,
				},
			}
			err = f.ClientSet.CoreV1().Pods(pod.Namespace).Bind(ctx, binding, metav1.CreateOptions{})
			framework.ExpectNoError(err)

			// kubelet 已拒绝该 Pod
			err = e2epod.WaitForPodFailedReason(ctx, f.ClientSet, pod, "OutOfcpu", f.Timeouts.PodStartShort)
			framework.ExpectNoError(err)

			// 获取被拒绝的 Pod 并比较状态
			gotPod, err := f.ClientSet.CoreV1().Pods(pod.Namespace).Get(ctx, pod.Name, metav1.GetOptions{})
			framework.ExpectNoError(err)

			// 获取被拒绝的 Pod，校验 generation 和 observedGeneration。
			gomega.Expect(gotPod.Generation).To(gomega.BeEquivalentTo(1))
			gomega.Expect(gotPod.Status.ObservedGeneration).To(gomega.BeEquivalentTo(1))

			// 检测 Status 中是否有被 Pod 拒绝流程丢掉的新字段。
			// 这些新字段要么应由 kubelet 准入保留，要么显式加入值不同或需清空的字段列表。
			gomega.Expect(gotPod.Status).To(gstruct.MatchAllFields(gstruct.Fields{
				"ObservedGeneration":                   gstruct.Ignore(),
				"Phase":                                gstruct.Ignore(),
				"Conditions":                           gstruct.Ignore(),
				"Message":                              gstruct.Ignore(),
				"Reason":                               gstruct.Ignore(),
				"NominatedNodeName":                    gstruct.Ignore(),
				"HostIP":                               gstruct.Ignore(),
				"HostIPs":                              gstruct.Ignore(),
				"PodIP":                                gstruct.Ignore(),
				"PodIPs":                               gstruct.Ignore(),
				"StartTime":                            gstruct.Ignore(),
				"InitContainerStatuses":                gstruct.Ignore(),
				"ContainerStatuses":                    gstruct.Ignore(),
				"QOSClass":                             gomega.Equal(pod.Status.QOSClass), // QOSClass 应保留
				"EphemeralContainerStatuses":           gstruct.Ignore(),
				"Resize":                               gstruct.Ignore(),
				"ResourceClaimStatuses":                gstruct.Ignore(),
				"ExtendedResourceClaimStatus":          gstruct.Ignore(),
				"Resources":                            gstruct.Ignore(),
				"AllocatedResources":                   gstruct.Ignore(),
				"NodeAllocatableResourceClaimStatuses": gstruct.Ignore(),
				"VolumeHealth":                         gstruct.Ignore(),
			}))
		})
	})
})
