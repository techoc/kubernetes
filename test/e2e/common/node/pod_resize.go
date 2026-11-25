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
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"time"

	v1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/test/e2e/common/node/framework/cgroups"
	"k8s.io/kubernetes/test/e2e/common/node/framework/podresize"
	"k8s.io/kubernetes/test/e2e/feature"
	"k8s.io/kubernetes/test/e2e/framework"
	e2enode "k8s.io/kubernetes/test/e2e/framework/node"
	e2epod "k8s.io/kubernetes/test/e2e/framework/pod"
	e2eskipper "k8s.io/kubernetes/test/e2e/framework/skipper"

	"github.com/google/go-cmp/cmp"
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

const (
	fakeExtendedResource = "dummy.com/dummy"

	originalCPU       = "20m"
	originalCPULimit  = "30m"
	reducedCPU        = "15m"
	reducedCPULimit   = "25m"
	increasedCPU      = "25m"
	increasedCPULimit = "35m"

	originalMem       = "35Mi"
	originalMemLimit  = "45Mi"
	reducedMem        = "30Mi"
	reducedMemLimit   = "40Mi"
	increasedMem      = "40Mi"
	increasedMemLimit = "50Mi"
)

func offsetCPU(index int, value string) string {
	if value == "" {
		return ""
	}
	val := resource.MustParse(value)
	ptr := &val
	ptr.Add(resource.MustParse(fmt.Sprintf("%dm", 2*index)))
	return ptr.String()
}

func offsetMemory(index int64, value string) string {
	if value == "" {
		return ""
	}
	val := resource.MustParse(value)
	ptr := &val
	ptr.Add(resource.MustParse(fmt.Sprintf("%dMi", 2*index)))
	return ptr.String()
}

func doGuaranteedPodResizeTests(f *framework.Framework) {
	ginkgo.DescribeTableSubtree("guaranteed qos - 1 container with resize policy", feature.InPlacePodVerticalScaling, func(cpuPolicy, memPolicy v1.ResourceResizeRestartPolicy, resizeInitCtrs bool) {
		ginkgo.DescribeTable("resizing", func(ctx context.Context, desiredCPU, desiredMem string) {

			// guaranteed pod 的测试包含扩展资源。
			nodes, err := e2enode.GetReadySchedulableNodes(context.Background(), f.ClientSet)
			framework.ExpectNoError(err)
			for _, node := range nodes.Items {
				e2enode.AddExtendedResource(ctx, f.ClientSet, node.Name, fakeExtendedResource, resource.MustParse("123"))
			}
			defer func() {
				for _, node := range nodes.Items {
					e2enode.RemoveExtendedResource(ctx, f.ClientSet, node.Name, fakeExtendedResource)
				}
			}()

			originalContainers := makeGuaranteedContainers(1, cpuPolicy, memPolicy, true, true, originalCPU, originalMem)
			expectedContainers := makeGuaranteedContainers(1, cpuPolicy, memPolicy, true, true, desiredCPU, desiredMem)
			for i, c := range expectedContainers {
				// 如果 Pod 有 init container，但不调整它们，保留其原有资源。
				if c.InitCtr && !resizeInitCtrs {
					c.Resources = originalContainers[i].Resources
					expectedContainers[i] = c
					continue
				}
				// 对于 resize 策略为 "restart" 的容器，预期会重启。
				expectRestart := int32(0)
				if cpuPolicy == v1.RestartContainer && desiredCPU != originalCPU {
					expectRestart = 1
				}
				if memPolicy == v1.RestartContainer && desiredMem != originalMem {
					expectRestart = 1
				}
				c.RestartCount = expectRestart
				expectedContainers[i] = c
			}

			doPatchAndRollback(ctx, f, originalContainers, expectedContainers, nil, nil, true, false)
		},
			// 所有测试都会执行请求的 resize，完成后再回滚更改。
			// 这样可覆盖资源增加和减少两种情况。
			ginkgo.Entry("cpu", increasedCPU, originalMem),
			ginkgo.Entry("mem", originalCPU, increasedMem),
			ginkgo.Entry("cpu & mem in the same direction", increasedCPU, increasedMem),
			ginkgo.Entry("cpu & mem in opposite directions", increasedCPU, reducedMem),
		)
	},
		ginkgo.Entry("no restart", v1.NotRequired, v1.NotRequired, false),
		ginkgo.Entry("no restart + resize initContainers", v1.NotRequired, v1.NotRequired, true),
		ginkgo.Entry("mem restart", v1.NotRequired, v1.RestartContainer, false),
		ginkgo.Entry("cpu restart", v1.RestartContainer, v1.NotRequired, false),
		ginkgo.Entry("cpu & mem restart", v1.RestartContainer, v1.RestartContainer, false),
		ginkgo.Entry("cpu & mem restart + resize initContainers", v1.RestartContainer, v1.RestartContainer, true),
	)

	// 所有测试都会执行请求的 resize，完成后再回滚更改。
	// 这样可覆盖所述操作及其逆操作。
	ginkgo.Describe("guaranteed pods with multiple containers", func() {
		ginkgo.It("3 containers - increase cpu & mem on c1, c2, decrease cpu & mem on c3 - net increase [MinimumKubeletVersion:1.34]", func(ctx context.Context) {
			originalContainers := makeGuaranteedContainers(3, v1.NotRequired, v1.NotRequired, false, false, originalCPU, originalMem)
			for i := range originalContainers {
				originalContainers[i].CPUPolicy = nil
				originalContainers[i].MemPolicy = nil
			}

			expectedContainers := []podresize.ResizableContainerInfo{
				{
					Name:      "c1",
					Resources: &cgroups.ContainerResources{CPUReq: offsetCPU(0, increasedCPU), CPULim: offsetCPU(0, increasedCPU), MemReq: offsetMemory(0, increasedMem), MemLim: offsetMemory(0, increasedMem)},
				},
				{
					Name:      "c2",
					Resources: &cgroups.ContainerResources{CPUReq: offsetCPU(1, increasedCPU), CPULim: offsetCPU(1, increasedCPU), MemReq: offsetMemory(1, increasedMem), MemLim: offsetMemory(1, increasedMem)},
				},
				{
					Name:      "c3",
					Resources: &cgroups.ContainerResources{CPUReq: offsetCPU(2, reducedCPU), CPULim: offsetCPU(2, reducedCPU), MemReq: offsetMemory(2, reducedMem), MemLim: offsetMemory(2, reducedMem)},
				},
			}

			doPatchAndRollback(ctx, f, originalContainers, expectedContainers, nil, nil, true, false)
		})

		ginkgo.It("3 containers - increase cpu & mem on c1, decrease cpu & mem on c2, c3 - net decrease [MinimumKubeletVersion:1.34]", func(ctx context.Context) {
			originalContainers := makeGuaranteedContainers(3, v1.NotRequired, v1.NotRequired, false, false, originalCPU, originalMem)
			for i := range originalContainers {
				originalContainers[i].CPUPolicy = nil
				originalContainers[i].MemPolicy = nil
			}

			expectedContainers := []podresize.ResizableContainerInfo{
				{
					Name:      "c1",
					Resources: &cgroups.ContainerResources{CPUReq: offsetCPU(0, increasedCPU), CPULim: offsetCPU(0, increasedCPU), MemReq: offsetMemory(0, increasedMem), MemLim: offsetMemory(0, increasedMem)},
				},
				{
					Name:      "c2",
					Resources: &cgroups.ContainerResources{CPUReq: offsetCPU(1, reducedCPU), CPULim: offsetCPU(1, reducedCPU), MemReq: offsetMemory(1, reducedMem), MemLim: offsetMemory(1, reducedMem)},
				},
				{
					Name:      "c3",
					Resources: &cgroups.ContainerResources{CPUReq: offsetCPU(2, reducedCPU), CPULim: offsetCPU(2, reducedCPU), MemReq: offsetMemory(2, reducedMem), MemLim: offsetMemory(2, reducedMem)},
				},
			}

			doPatchAndRollback(ctx, f, originalContainers, expectedContainers, nil, nil, true, false)
		})

		ginkgo.It("3 containers - increase: CPU (c1,c3), memory (c2, c3) ; decrease: CPU (c2) [MinimumKubeletVersion:1.34]", func(ctx context.Context) {
			originalContainers := makeGuaranteedContainers(3, v1.NotRequired, v1.NotRequired, false, false, originalCPU, originalMem)
			for i := range originalContainers {
				originalContainers[i].CPUPolicy = nil
				originalContainers[i].MemPolicy = nil
			}

			expectedContainers := []podresize.ResizableContainerInfo{
				{
					Name:      "c1",
					Resources: &cgroups.ContainerResources{CPUReq: offsetCPU(0, increasedCPU), CPULim: offsetCPU(0, increasedCPU), MemReq: offsetMemory(0, originalMem), MemLim: offsetMemory(0, originalMem)},
				},
				{
					Name:      "c2",
					Resources: &cgroups.ContainerResources{CPUReq: offsetCPU(1, reducedCPU), CPULim: offsetCPU(1, reducedCPU), MemReq: offsetMemory(1, increasedMem), MemLim: offsetMemory(1, increasedMem)},
				},
				{
					Name:      "c3",
					Resources: &cgroups.ContainerResources{CPUReq: offsetCPU(2, increasedCPU), CPULim: offsetCPU(2, increasedCPU), MemReq: offsetMemory(2, increasedMem), MemLim: offsetMemory(2, increasedMem)},
				},
			}

			doPatchAndRollback(ctx, f, originalContainers, expectedContainers, nil, nil, true, false)
		})
	})
}

type resourceRequestsLimits struct {
	cpuReq string
	cpuLim string
	memReq string
	memLim string
}

func doBurstablePodResizeTests(f *framework.Framework) {
	ginkgo.DescribeTableSubtree("burstable pods - 1 container with all requests & limits set and resize policy", feature.InPlacePodVerticalScaling, func(cpuPolicy, memPolicy v1.ResourceResizeRestartPolicy) {
		ginkgo.DescribeTable("resizing", func(ctx context.Context, desiredContainerResources resourceRequestsLimits) {
			originalContainers := makeBurstableContainers(1, cpuPolicy, memPolicy, originalCPU, originalCPULimit, originalMem, originalMemLimit)
			expectedContainers := makeBurstableContainers(1, cpuPolicy, memPolicy, desiredContainerResources.cpuReq, desiredContainerResources.cpuLim, desiredContainerResources.memReq, desiredContainerResources.memLim)
			for i, c := range expectedContainers {
				// 对于 resize 策略为 "restart" 的容器，预期会重启。
				expectRestart := int32(0)
				if cpuPolicy == v1.RestartContainer && (desiredContainerResources.cpuReq != originalCPU || desiredContainerResources.cpuLim != originalCPULimit) {
					expectRestart = 1
				}
				if memPolicy == v1.RestartContainer && (desiredContainerResources.memReq != originalMem || desiredContainerResources.memLim != originalMemLimit) {
					expectRestart = 1
				}
				c.RestartCount = expectRestart
				expectedContainers[i] = c
			}

			doPatchAndRollback(ctx, f, originalContainers, expectedContainers, nil, nil, true, false)
		},
			// 所有测试都会执行请求的 resize，完成后再回滚更改。
			// 这样可覆盖资源增加和减少两种情况。
			ginkgo.Entry("cpu requests", resourceRequestsLimits{increasedCPU, originalCPULimit, originalMem, originalMemLimit}),
			ginkgo.Entry("cpu limits", resourceRequestsLimits{originalCPU, increasedCPULimit, originalMem, originalMemLimit}),
			ginkgo.Entry("mem requests", resourceRequestsLimits{originalCPU, originalCPULimit, increasedMem, originalMemLimit}),
			ginkgo.Entry("mem limits", resourceRequestsLimits{originalCPU, originalCPULimit, originalMem, increasedMemLimit}),
			ginkgo.Entry("all resources in the same direction", resourceRequestsLimits{increasedCPU, increasedCPULimit, increasedMem, increasedMemLimit}),
			ginkgo.Entry("cpu & mem in opposite directions", resourceRequestsLimits{increasedCPU, increasedCPULimit, reducedMem, reducedMemLimit}),
			ginkgo.Entry("requests & limits in opposite directions", resourceRequestsLimits{reducedCPU, increasedCPULimit, increasedMem, reducedMemLimit}),
		)
	},
		ginkgo.Entry("no restart", v1.NotRequired, v1.NotRequired),
		ginkgo.Entry("cpu restart", v1.RestartContainer, v1.NotRequired),
		ginkgo.Entry("mem restart", v1.NotRequired, v1.RestartContainer),
		ginkgo.Entry("cpu & mem restart", v1.RestartContainer, v1.RestartContainer),
	)

	// 以下测试覆盖其余 burstable pod 场景：
	// - 多容器
	// - 在原先只有 request 的地方添加 limit
	// - 在原先没有任何设置的地方添加 request
	// - 用等价资源 resize（例如 2m -> 1m）
	ginkgo.Describe("burstable pods - extended", func() {
		/*
			Release: v1.35
			Testname: In-place Pod Resize, burtable pod with multiple containers and various operations
			Description: 通过 Pod Resize 子资源的 patch endpoint 发起 Pod Resize 请求，用各种操作修改一个 6 容器 Pod 的 CPU 和内存 request 与 limit，MUST 使 Pod 资源按预期更新。
		*/
		framework.ConformanceIt("6 containers - various operations performed (including adding limits and requests) [MinimumKubeletVersion:1.34]", func(ctx context.Context) {
			originalContainers := []podresize.ResizableContainerInfo{
				{
					// c1 起初只有 CPU request；增加 CPU request 并添加 CPU limit
					Name:      "c1",
					Resources: &cgroups.ContainerResources{CPUReq: originalCPU},
				},
				{
					// c2 起初只有 memory request；增加 memory request 并添加 memory limit
					Name:      "c2",
					Resources: &cgroups.ContainerResources{MemReq: originalMem},
				},
				{
					// c3 起初有 CPU 和 memory request；只减少 memory request
					Name:      "c3",
					Resources: &cgroups.ContainerResources{CPUReq: originalCPU, MemReq: originalMem},
				},
				{
					// c4 起初只有 CPU request；减少 CPU request 并添加 memory request
					Name:      "c4",
					Resources: &cgroups.ContainerResources{CPUReq: originalCPU},
				},
				{
					// c5 起初只有 memory request；增加 memory request 并添加 CPU request
					Name:      "c5",
					Resources: &cgroups.ContainerResources{MemReq: originalMem},
				},
			}
			expectedContainers := []podresize.ResizableContainerInfo{
				{
					// c1 起初只有 CPU request；增加 CPU request 并添加 CPU limit
					Name:      "c1",
					Resources: &cgroups.ContainerResources{CPUReq: increasedCPU, CPULim: increasedCPULimit},
				},
				{
					// c2 起初只有 memory request；减少 memory request 并添加 memory limit
					Name:      "c2",
					Resources: &cgroups.ContainerResources{MemReq: reducedMem, MemLim: increasedMemLimit},
				},
				{
					// c3 起初有 CPU 和 memory request；只减少 memory request
					Name:      "c3",
					Resources: &cgroups.ContainerResources{CPUReq: originalCPU, MemReq: reducedMem},
				},
				{
					// c4 起初只有 CPU request；减少 CPU request 并添加 memory request
					Name:      "c4",
					Resources: &cgroups.ContainerResources{CPUReq: reducedCPU, MemReq: originalMem},
				},
				{
					// c5 起初只有 memory request；增加 memory request 并添加 CPU request
					Name:      "c5",
					Resources: &cgroups.ContainerResources{CPUReq: originalCPU, MemReq: increasedMem},
				},
			}
			doPatchAndRollback(ctx, f, originalContainers, expectedContainers, nil, nil, false, false)
		})

		/*
			Release: v1.35
			Testname: In-place Pod Resize, burstable pod resized with equivalents
			Description: 通过 Pod Resize 子资源的 patch endpoint 发起原地 Pod Resize 请求，用等价的值（例如 2m -> 1m）修改 CPU request 和 limit，MUST 使更新后的 Pod 资源在 status 中正确显示。
		*/
		framework.ConformanceIt("resize with equivalents [MinimumKubeletVersion:1.34]", func(ctx context.Context) {
			originalContainers := []podresize.ResizableContainerInfo{
				{
					Name:      "c1",
					Resources: &cgroups.ContainerResources{CPUReq: "2m", CPULim: "10m"},
				},
			}
			expectedContainers := []podresize.ResizableContainerInfo{
				{
					Name:      "c1",
					Resources: &cgroups.ContainerResources{CPUReq: "1m", CPULim: "5m"},
				},
			}
			doPatchAndRollback(ctx, f, originalContainers, expectedContainers, nil, nil, true, false)
		})
	})
}

func doPodResizePatchErrorTests(f *framework.Framework) {
	ginkgo.DescribeTable("apply invalid resize patch requests", feature.InPlacePodVerticalScaling, func(ctx context.Context, originalContainers, desiredContainers []podresize.ResizableContainerInfo, patchError string, waitForStart bool) {
		podClient := e2epod.NewPodClient(f)
		var newPod *v1.Pod

		ginkgo.By("creating and verifying pod")
		if waitForStart {
			newPod = createAndVerifyPod(ctx, f, podClient, originalContainers, nil, false)
		} else {
			tStamp := strconv.Itoa(time.Now().Nanosecond())
			testPod := podresize.MakePodWithResizableContainers(f.Namespace.Name, "testpod", tStamp, originalContainers, nil)
			testPod = e2epod.MustMixinRestrictedPodSecurity(testPod)
			newPod = podClient.Create(ctx, testPod)
		}

		ginkgo.By("patching pod for resize")
		patch := podresize.MakeResizePatch(originalContainers, desiredContainers, nil, nil)
		_, pErr := f.ClientSet.CoreV1().Pods(newPod.Namespace).Patch(ctx, newPod.Name,
			types.StrategicMergePatchType, patch, metav1.PatchOptions{}, "resize")
		gomega.Expect(pErr).To(gomega.HaveOccurred())
		gomega.Expect(pErr.Error()).To(gomega.ContainSubstring(patchError))

		patchedPod, getErr := f.ClientSet.CoreV1().Pods(newPod.Namespace).Get(ctx, newPod.Name, metav1.GetOptions{})
		framework.ExpectNoError(getErr)

		ginkgo.By("verifying pod resources after patch")
		podresize.VerifyPodResources(patchedPod, originalContainers, nil)

		if waitForStart {
			ginkgo.By("verifying pod status resources after patch")
			framework.ExpectNoError(podresize.VerifyPodStatusResources(patchedPod, originalContainers))
		}

		ginkgo.By("deleting pod")
		podClient.DeleteSync(ctx, newPod.Name, metav1.DeleteOptions{}, f.Timeouts.PodDelete)
	},
		ginkgo.Entry("BestEffort pod - request memory",
			[]podresize.ResizableContainerInfo{
				{
					Name: "c1",
				},
			},
			[]podresize.ResizableContainerInfo{
				{
					Name:      "c1",
					Resources: &cgroups.ContainerResources{MemReq: originalMem},
				},
			},
			"Pod QOS Class may not change as a result of resizing",
			true,
		),
		ginkgo.Entry("BestEffort pod - request cpu",
			[]podresize.ResizableContainerInfo{
				{
					Name: "c1",
				},
			},
			[]podresize.ResizableContainerInfo{
				{
					Name:      "c1",
					Resources: &cgroups.ContainerResources{CPUReq: originalCPU},
				},
			},
			"Pod QOS Class may not change as a result of resizing",
			true,
		),
		ginkgo.Entry("Guaranteed pod - remove cpu & memory limits",
			[]podresize.ResizableContainerInfo{
				{
					Name:      "c1",
					Resources: &cgroups.ContainerResources{CPUReq: originalCPU, CPULim: originalCPU, MemReq: originalMem, MemLim: originalMem},
				},
			},
			[]podresize.ResizableContainerInfo{
				{
					Name:      "c1",
					Resources: &cgroups.ContainerResources{CPUReq: originalCPU, MemReq: originalMem},
				},
			},
			"resource limits cannot be removed",
			true,
		),
		ginkgo.Entry("Burstable pod - remove cpu & memory limits + increase requests",
			[]podresize.ResizableContainerInfo{
				{
					Name:      "c1",
					Resources: &cgroups.ContainerResources{CPUReq: originalCPU, CPULim: increasedCPULimit, MemReq: originalMem, MemLim: increasedMemLimit},
				},
			},
			[]podresize.ResizableContainerInfo{
				{
					Name:      "c1",
					Resources: &cgroups.ContainerResources{CPUReq: increasedCPU, MemReq: increasedMem},
				},
			},
			"resource limits cannot be removed",
			true,
		),
		ginkgo.Entry("Burstable pod - remove memory requests",
			[]podresize.ResizableContainerInfo{
				{
					Name:      "c1",
					Resources: &cgroups.ContainerResources{MemReq: originalMem},
				},
			},
			[]podresize.ResizableContainerInfo{
				{
					Name:      "c1",
					Resources: &cgroups.ContainerResources{},
				},
			},
			"resource requests cannot be removed",
			true,
		),
		ginkgo.Entry("Burstable pod - remove cpu requests",
			[]podresize.ResizableContainerInfo{
				{
					Name:      "c1",
					Resources: &cgroups.ContainerResources{CPUReq: originalCPU},
				},
			},
			[]podresize.ResizableContainerInfo{
				{
					Name:      "c1",
					Resources: &cgroups.ContainerResources{},
				},
			},
			"resource requests cannot be removed",
			true,
		),
		ginkgo.Entry("Burstable pod - reorder containers",
			[]podresize.ResizableContainerInfo{
				{
					Name:      "c1",
					Resources: &cgroups.ContainerResources{CPUReq: originalCPU},
				},
				{
					Name:      "c2",
					Resources: &cgroups.ContainerResources{CPUReq: originalCPU},
				},
			},
			[]podresize.ResizableContainerInfo{
				{
					Name:      "c2",
					Resources: &cgroups.ContainerResources{CPUReq: originalCPU},
				},
				{
					Name:      "c1",
					Resources: &cgroups.ContainerResources{CPUReq: originalCPU},
				},
			},
			"spec.containers[0].name: Forbidden: containers may not be renamed or reordered on resize, spec.containers[1].name: Forbidden: containers may not be renamed or reordered on resize",
			true,
		),
		ginkgo.Entry("Guaranteed pod - rename containers",
			[]podresize.ResizableContainerInfo{
				{
					Name:      "c1-old",
					Resources: &cgroups.ContainerResources{CPUReq: originalCPU, CPULim: originalCPU, MemReq: originalMem, MemLim: originalMem},
				},
				{
					Name:      "c2-old",
					Resources: &cgroups.ContainerResources{CPUReq: originalCPU, CPULim: originalCPU, MemReq: originalMem, MemLim: originalMem},
				},
			},
			[]podresize.ResizableContainerInfo{
				{
					Name:      "c1-new",
					Resources: &cgroups.ContainerResources{CPUReq: originalCPU, CPULim: originalCPU, MemReq: originalMem, MemLim: originalMem},
				},
				{
					Name:      "c2-new",
					Resources: &cgroups.ContainerResources{CPUReq: originalCPU, CPULim: originalCPU, MemReq: originalMem, MemLim: originalMem},
				},
			},
			"spec.containers[0].name: Forbidden: containers may not be renamed or reordered on resize, spec.containers[1].name: Forbidden: containers may not be renamed or reordered on resize",
			true,
		),
		ginkgo.Entry("Burstable pod - set requests == limits",
			[]podresize.ResizableContainerInfo{
				{
					Name:      "c1",
					Resources: &cgroups.ContainerResources{CPUReq: originalCPU, CPULim: increasedCPULimit, MemReq: originalMem, MemLim: increasedMemLimit},
				},
			},
			[]podresize.ResizableContainerInfo{
				{
					Name:      "c1",
					Resources: &cgroups.ContainerResources{CPUReq: increasedCPULimit, CPULim: increasedCPULimit, MemReq: increasedMemLimit, MemLim: increasedMemLimit},
				},
			},
			"Pod QOS Class may not change as a result of resizing",
			true,
		),
		ginkgo.Entry("Burstable pod - resize ephemeral storage",
			[]podresize.ResizableContainerInfo{
				{
					Name:      "c1",
					Resources: &cgroups.ContainerResources{CPUReq: originalCPU, MemReq: originalMem, EphStorReq: "1"},
				},
			},
			[]podresize.ResizableContainerInfo{
				{
					Name:      "c1",
					Resources: &cgroups.ContainerResources{CPUReq: originalCPU, MemReq: originalMem, EphStorReq: "3"},
				},
			},
			"only cpu and memory resources are mutable",
			true,
		),
	)
}

func doPodResizeMemoryLimitDecreaseTest(f *framework.Framework) {
	// 测试减少 memory limit 时的行为：
	// 1. 稍微降低 limit —— 应成功
	// 2. 将 limit 降到极小值 —— 应失败
	// 3. 将 limit 恢复原值 —— 应成功
	ginkgo.It("decrease memory limit below usage", feature.InPlacePodVerticalScaling, func(ctx context.Context) {
		podClient := e2epod.NewPodClient(f)
		original := []podresize.ResizableContainerInfo{{
			Name:      "c1",
			Resources: &cgroups.ContainerResources{MemReq: originalMem, MemLim: originalMem},
		}}

		ginkgo.By("creating and verifying pod")
		testPod := createAndVerifyPod(ctx, f, podClient, original, nil, false)

		// 1. 稍微降低 limit —— 应成功
		ginkgo.By("Patching pod with a slightly lowered memory limit")
		viableLoweredLimit := []podresize.ResizableContainerInfo{{
			Name:      "c1",
			Resources: &cgroups.ContainerResources{MemReq: reducedMem, MemLim: reducedMem},
		}}
		patch := podresize.MakeResizePatch(original, viableLoweredLimit, nil, nil)
		testPod, pErr := f.ClientSet.CoreV1().Pods(testPod.Namespace).Patch(ctx, testPod.Name,
			types.StrategicMergePatchType, patch, metav1.PatchOptions{}, "resize")
		framework.ExpectNoError(pErr, "failed to patch pod for viable lowered limit")

		ginkgo.By("verifying pod patched for viable lowered limit")
		podresize.VerifyPodResources(testPod, viableLoweredLimit, nil)

		ginkgo.By("waiting for viable lowered limit to be actuated")
		resizedPod := podresize.WaitForPodResizeActuation(ctx, f, podClient, testPod, viableLoweredLimit)
		podresize.ExpectPodResized(ctx, f, resizedPod, viableLoweredLimit)

		// 容器启动后，采集 memory 用量前有一段延迟。在 CRI-O 上
		// 这个延迟更大，因此要等待足够时间让 cAdvisor 采集两次指标。
		ginkgo.By("Waiting for stats scraping")
		const scrapingDelay = 30 * time.Second // 2 * maxHousekeepingInterval
		startTime := testPod.Status.StartTime
		time.Sleep(time.Until(startTime.Add(scrapingDelay)))

		// 2. 将 limit 降到极小值 —— 应失败
		const nonViableMemoryLimit = "10Ki"
		ginkgo.By("Patching pod with a greatly lowered memory limit")
		nonViableLoweredLimit := []podresize.ResizableContainerInfo{{
			Name:      "c1",
			Resources: &cgroups.ContainerResources{MemReq: nonViableMemoryLimit, MemLim: nonViableMemoryLimit},
		}}
		patch = podresize.MakeResizePatch(viableLoweredLimit, nonViableLoweredLimit, nil, nil)
		testPod, pErr = f.ClientSet.CoreV1().Pods(testPod.Namespace).Patch(ctx, testPod.Name,
			types.StrategicMergePatchType, patch, metav1.PatchOptions{}, "resize")
		framework.ExpectNoError(pErr, "failed to patch pod for viable lowered limit")

		framework.ExpectNoError(framework.Gomega().
			Eventually(ctx, framework.RetryNotFound(framework.GetObject(f.ClientSet.CoreV1().Pods(testPod.Namespace).Get, testPod.Name, metav1.GetOptions{}))).
			WithTimeout(f.Timeouts.PodStart).
			Should(framework.MakeMatcher(func(pod *v1.Pod) (func() string, error) {
				// 如果 VerifyPodStatusResources 成功，说明 resize 已完成。
				if podresize.VerifyPodStatusResources(pod, nonViableLoweredLimit) == nil {
					return nil, gomega.StopTrying("non-viable resize unexpectedly completed")
				}

				var inProgressCondition *v1.PodCondition
				for i, condition := range pod.Status.Conditions {
					switch condition.Type {
					case v1.PodResizeInProgress:
						inProgressCondition = &pod.Status.Conditions[i]
					case v1.PodResizePending:
						return func() string {
							return fmt.Sprintf("pod should not be pending, got reason=%s, message=%q", condition.Reason, condition.Message)
						}, nil
					}
				}
				if inProgressCondition == nil {
					return func() string { return "resize is not in progress" }, nil
				}

				if inProgressCondition.Reason != v1.PodReasonError {
					return func() string { return "in-progress reason is not error" }, nil
				}

				expectedMsg := regexp.MustCompile(`memory limit \(\d+\) below current usage`)
				if !expectedMsg.MatchString(inProgressCondition.Message) {
					return func() string {
						return fmt.Sprintf("Expected %q to contain %q", inProgressCondition.Message, expectedMsg)
					}, nil
				}
				return nil, nil
			})),
		)
		ginkgo.By("verifying pod status resources still match the viable resize")
		framework.ExpectNoError(podresize.VerifyPodStatusResources(testPod, viableLoweredLimit))

		// 3. 将 limit 恢复原值 —— 应成功
		ginkgo.By("Patching pod to revert to original state")
		patch = podresize.MakeResizePatch(nonViableLoweredLimit, original, nil, nil)
		testPod, pErr = f.ClientSet.CoreV1().Pods(testPod.Namespace).Patch(ctx, testPod.Name,
			types.StrategicMergePatchType, patch, metav1.PatchOptions{}, "resize")
		framework.ExpectNoError(pErr, "failed to patch pod back to original values")

		ginkgo.By("verifying pod patched for original values")
		podresize.VerifyPodResources(testPod, original, nil)

		ginkgo.By("waiting for the original values to be actuated")
		resizedPod = podresize.WaitForPodResizeActuation(ctx, f, podClient, testPod, original)
		podresize.ExpectPodResized(ctx, f, resizedPod, original)

		ginkgo.By("deleting pod")
		podClient.DeleteSync(ctx, testPod.Name, metav1.DeleteOptions{}, f.Timeouts.PodDelete)
	})
}

func doPodResizeInitContainerResizeTest(f *framework.Framework) {
	ginkgo.It("should support resizing a non-sidecar init container", func(ctx context.Context) {
		// 一个永不退出的命令，便于我们在容器运行时给 init container 打补丁。
		cmd := "tail -f /dev/null"

		originalContainers := []podresize.ResizableContainerInfo{
			{
				Name:          "init-c1",
				InitCtr:       true,
				Resources:     &cgroups.ContainerResources{CPUReq: originalCPU, CPULim: originalCPU, MemReq: originalMem, MemLim: originalMem},
				RestartPolicy: v1.ContainerRestartPolicyNever,
			},
			{
				Name:      "main-c1",
				Resources: &cgroups.ContainerResources{CPUReq: originalCPU, CPULim: originalCPU, MemReq: originalMem, MemLim: originalMem},
			},
		}
		resizedContainers := []podresize.ResizableContainerInfo{
			{
				Name:          "init-c1",
				InitCtr:       true,
				Resources:     &cgroups.ContainerResources{CPUReq: increasedCPU, CPULim: increasedCPU, MemReq: increasedMem, MemLim: increasedMem},
				RestartPolicy: v1.ContainerRestartPolicyNever,
			},
			{
				Name:      "main-c1",
				Resources: originalContainers[1].Resources,
			},
		}

		originalInitCtr := cgroups.MakeContainerWithResources(
			originalContainers[0].Name,
			originalContainers[0].Resources,
			cmd,
		)
		resizedInitCtr := cgroups.MakeContainerWithResources(
			resizedContainers[0].Name,
			resizedContainers[0].Resources,
			cmd,
		)

		ginkgo.By("Creating pod with an nonrestartable init container")
		testPod := &v1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				GenerateName: "resize-test-",
			},
			Spec: v1.PodSpec{
				InitContainers: []v1.Container{originalInitCtr},
				Containers: []v1.Container{cgroups.MakeContainerWithResources(
					originalContainers[1].Name,
					originalContainers[1].Resources,
					cmd,
				)},
			},
		}
		testPod = e2epod.MustMixinRestrictedPodSecurity(testPod)
		podClient := e2epod.NewPodClient(f)
		newPod := podClient.Create(ctx, testPod)
		verifyInitContainerResources(ctx, f, newPod, originalContainers, originalInitCtr)

		ginkgo.By("patching and verifying pod for resize")
		patch := podresize.MakeResizePatch(originalContainers, resizedContainers, nil, nil)
		patchedPod, pErr := f.ClientSet.CoreV1().Pods(newPod.Namespace).Patch(ctx, newPod.Name,
			types.StrategicMergePatchType, patch, metav1.PatchOptions{}, "resize")
		framework.ExpectNoError(pErr, "failed to patch pod for resize")
		verifyInitContainerResources(ctx, f, patchedPod, resizedContainers, resizedInitCtr)

		// resize 已生效，测试逆操作。
		ginkgo.By("patching and verifying pod to revert to original resources")
		patch = podresize.MakeResizePatch(resizedContainers, originalContainers, nil, nil)
		patchedPod, pErr = f.ClientSet.CoreV1().Pods(newPod.Namespace).Patch(ctx, newPod.Name,
			types.StrategicMergePatchType, patch, metav1.PatchOptions{}, "resize")
		framework.ExpectNoError(pErr, "failed to patch pod for resize")
		verifyInitContainerResources(ctx, f, patchedPod, originalContainers, originalInitCtr)

		ginkgo.By("deleting pod")
		podClient.DeleteSync(ctx, newPod.Name, metav1.DeleteOptions{}, f.Timeouts.PodDelete)
	})
}

func doPodResizeReadAndReplaceTests(f *framework.Framework) {
	/*
		Release: v1.35
		Testname: In-place Pod Resize, read and replace endpoints
		Description: 通过 Pod Resize 子资源的 replace endpoint 发起 Pod Resize 请求，MUST 使 Pod 资源按预期更新。resize 完成后，从 Pod Resize 子资源获取的 Pod 对象 MUST 与从主 Pod endpoint 获取的对象等价。
	*/
	framework.ConformanceIt("resize pod via the replace endpoint [MinimumKubeletVersion:1.34]", func(ctx context.Context) {
		podClient := e2epod.NewPodClient(f)
		original := []podresize.ResizableContainerInfo{{
			Name:      "c1",
			Resources: &cgroups.ContainerResources{CPUReq: originalCPU, MemReq: originalMem},
		}}
		desiredContainers := []podresize.ResizableContainerInfo{{
			Name:      "c1",
			Resources: &cgroups.ContainerResources{CPUReq: increasedCPU, MemReq: increasedMem},
		}}
		desired := v1.ResourceRequirements{
			Requests: v1.ResourceList{
				v1.ResourceCPU:    resource.MustParse(increasedCPU),
				v1.ResourceMemory: resource.MustParse(increasedMem),
			},
		}

		ginkgo.By("creating and verifying pod")
		pod := createAndVerifyPod(ctx, f, podClient, original, nil, false)
		gomega.Expect(pod.Generation).To(gomega.BeEquivalentTo(1))

		podToUpdate, err := podClient.Get(ctx, pod.Name, metav1.GetOptions{})
		framework.ExpectNoError(err, "failed to get pod")
		podToUpdate.Spec.Containers[0].Resources = desired

		ginkgo.By("updating the pod resources")
		_, err = podClient.UpdateResize(ctx, pod.Name, podToUpdate, metav1.UpdateOptions{})
		framework.ExpectNoError(err, "failed to resize pod")

		ginkgo.By("fetching updated pod")
		updatedPod, err := podClient.Get(ctx, pod.Name, metav1.GetOptions{})
		framework.ExpectNoError(err, "failed to get pod")

		ginkgo.By("verifying pod resources")
		podresize.VerifyPodResources(updatedPod, desiredContainers, nil)
		gomega.Expect(updatedPod.Generation).To(gomega.BeEquivalentTo(2))

		ginkgo.By("verifying pod resources after patch")
		expected := podresize.UpdateExpectedContainerRestarts(ctx, updatedPod, desiredContainers)
		resizedPod := podresize.WaitForPodResizeActuation(ctx, f, podClient, updatedPod, expected)
		podresize.ExpectPodResized(ctx, f, resizedPod, expected)

		ginkgo.By("verifying pod fetched from resize subresource")
		framework.ExpectNoError(framework.Gomega().
			Eventually(ctx, framework.RetryNotFound(framework.GetObject(f.ClientSet.CoreV1().Pods(pod.Namespace).Get, pod.Name, metav1.GetOptions{}))).
			WithTimeout(f.Timeouts.PodStart).
			Should(framework.MakeMatcher(func(pod *v1.Pod) (func() string, error) {
				// 不知为何，客户端返回的 pod 对象没有填充 GVK。
				pod.Kind = "Pod"
				pod.APIVersion = "v1"

				ginkgo.By("verifying pod fetched from resize subresource")
				podResource := schema.GroupVersionResource{Group: "", Version: "v1", Resource: "pods"}
				unstruct, err := f.DynamicClient.Resource(podResource).Namespace(f.Namespace.Name).Get(ctx, pod.Name, metav1.GetOptions{}, "resize")
				if err != nil {
					return func() string {
						return fmt.Sprintf("failed to fetch pod after resize: %v", err.Error())
					}, nil
				}
				updatedPodFromResize, err := unstructuredToPod(unstruct)
				if err != nil {
					return func() string {
						return fmt.Sprintf("couldn't convert result to pod object: %v", err.Error())
					}, nil
				}
				if !apiequality.Semantic.DeepEqual(pod, updatedPodFromResize) {
					return func() string {
						return fmt.Sprintf("pod from resize subresource not equivalent to pod; %s", cmp.Diff(pod, updatedPodFromResize))
					}, nil
				}

				return nil, nil
			})),
		)
	})
}

func doPodResizeMemoryVolumeTests(f *framework.Framework) {
	// 测试调整 memory-backed emptyDir volume limit 时的行为：
	// 1. 创建一个带 origSizeLimit 大小的 memory-backed emptyDir volume 的 Pod。
	// 2. 将 volume size limit 调大到 desiredSizeLimit —— 应成功且 Pod 不重启。
	// 3. 将 volume size limit 回滚到 origSizeLimit —— 应成功且 Pod 不重启。
	// TODO: 当 InPlacePodVerticalScalingMemoryBackedVolumes GA 后，将这些测试移到 InPlacePodVerticalScaling 表中，
	// 作为附加参数，用 memory-backed emptyDir volume 测试。
	ginkgo.DescribeTable("memory-backed emptyDir volume resize",
		func(ctx context.Context, origCPU, desiredCPU, origMem, desiredMem, origSizeLimit, desiredSizeLimit string) {
			podClient := e2epod.NewPodClient(f)

			originalContainers := []podresize.ResizableContainerInfo{{
				Name: "c1",
				Resources: &cgroups.ContainerResources{
					CPUReq: origCPU, CPULim: origCPU,
					MemReq: origMem, MemLim: origMem,
				},
			}}
			desiredContainers := []podresize.ResizableContainerInfo{{
				Name: "c1",
				Resources: &cgroups.ContainerResources{
					CPUReq: desiredCPU, CPULim: desiredCPU,
					MemReq: desiredMem, MemLim: desiredMem,
				},
			}}

			ginkgo.By("creating and verifying pod with memory-backed emptyDir volume")
			tStamp := strconv.Itoa(time.Now().Nanosecond())
			testPod := podresize.MakePodWithResizableContainers(f.Namespace.Name, "", tStamp, originalContainers, nil)
			testPod.GenerateName = "resize-memory-vol-test-"

			origQty := resource.MustParse(origSizeLimit)
			testPod.Spec.Volumes = []v1.Volume{
				{
					Name: "mem-vol",
					VolumeSource: v1.VolumeSource{
						EmptyDir: &v1.EmptyDirVolumeSource{
							Medium:    v1.StorageMediumMemory,
							SizeLimit: &origQty,
						},
					},
				},
			}
			testPod.Spec.Containers[0].VolumeMounts = []v1.VolumeMount{
				{
					Name:      "mem-vol",
					MountPath: "/cache",
				},
			}
			testPod = e2epod.MustMixinRestrictedPodSecurity(testPod)

			newPod := podClient.CreateSync(ctx, testPod)
			podresize.VerifyPodResources(newPod, originalContainers, nil)

			ginkgo.By("verifying initial volume mount size via df inside the container")
			stdout, _, err := e2epod.ExecCommandInContainerWithFullOutput(f, newPod.Name, "c1", "df", "-m", "/cache")
			framework.ExpectNoError(err, "failed to run df inside container")
			origMB := origQty.Value() / (1024 * 1024)
			gomega.Expect(stdout).To(gomega.ContainSubstring(strconv.FormatInt(origMB, 10)))

			ginkgo.By("patching pod spec's emptyDir sizeLimit and container resources")
			containerPatchBytes := podresize.MakeResizePatch(originalContainers, desiredContainers, nil, nil)
			var patchMap map[string]interface{}
			err = json.Unmarshal(containerPatchBytes, &patchMap)
			framework.ExpectNoError(err, "failed to unmarshal container patch")

			if patchMap == nil {
				patchMap = make(map[string]interface{})
			}
			specMap, ok := patchMap["spec"].(map[string]interface{})
			if !ok {
				specMap = make(map[string]interface{})
				patchMap["spec"] = specMap
			}
			specMap["volumes"] = []interface{}{
				map[string]interface{}{
					"name": "mem-vol",
					"emptyDir": map[string]interface{}{
						"sizeLimit": desiredSizeLimit,
					},
				},
			}

			patchBytes, err := json.Marshal(patchMap)
			framework.ExpectNoError(err, "failed to marshal combined patch")

			patchedPod, pErr := f.ClientSet.CoreV1().Pods(newPod.Namespace).Patch(ctx, newPod.Name,
				types.StrategicMergePatchType, patchBytes, metav1.PatchOptions{}, "resize")
			framework.ExpectNoError(pErr, "failed to patch pod spec")

			ginkgo.By("waiting for resize actuation to complete")
			expected := podresize.UpdateExpectedContainerRestarts(ctx, patchedPod, desiredContainers)
			resizedPod := podresize.WaitForPodResizeActuation(ctx, f, podClient, patchedPod, expected)
			podresize.ExpectPodResized(ctx, f, resizedPod, expected)

			desiredQty := resource.MustParse(desiredSizeLimit)

			ginkgo.By("verifying new volume mount size via df inside the container")
			stdout, _, err = e2epod.ExecCommandInContainerWithFullOutput(f, resizedPod.Name, "c1", "df", "-m", "/cache")
			framework.ExpectNoError(err, "failed to run df inside container after resize")
			desiredMB := desiredQty.Value() / (1024 * 1024)
			gomega.Expect(stdout).To(gomega.ContainSubstring(strconv.FormatInt(desiredMB, 10)))

			ginkgo.By("rolling back to original state")
			rollbackContainerPatchBytes := podresize.MakeResizePatch(desiredContainers, originalContainers, nil, nil)
			var rollbackPatchMap map[string]interface{}
			err = json.Unmarshal(rollbackContainerPatchBytes, &rollbackPatchMap)
			framework.ExpectNoError(err, "failed to unmarshal rollback container patch")

			if rollbackPatchMap == nil {
				rollbackPatchMap = make(map[string]interface{})
			}
			rollbackSpecMap, ok := rollbackPatchMap["spec"].(map[string]interface{})
			if !ok {
				rollbackSpecMap = make(map[string]interface{})
				rollbackPatchMap["spec"] = rollbackSpecMap
			}
			rollbackSpecMap["volumes"] = []interface{}{
				map[string]interface{}{
					"name": "mem-vol",
					"emptyDir": map[string]interface{}{
						"sizeLimit": origSizeLimit,
					},
				},
			}

			rollbackPatchBytes, err := json.Marshal(rollbackPatchMap)
			framework.ExpectNoError(err, "failed to marshal rollback patch")

			rolledBackPod, pErr := f.ClientSet.CoreV1().Pods(newPod.Namespace).Patch(ctx, newPod.Name,
				types.StrategicMergePatchType, rollbackPatchBytes, metav1.PatchOptions{}, "resize")
			framework.ExpectNoError(pErr, "failed to patch pod for rollback")

			expectedRollback := podresize.UpdateExpectedContainerRestarts(ctx, rolledBackPod, originalContainers)
			finalPod := podresize.WaitForPodResizeActuation(ctx, f, podClient, rolledBackPod, expectedRollback)
			podresize.ExpectPodResized(ctx, f, finalPod, expectedRollback)

			stdout, _, err = e2epod.ExecCommandInContainerWithFullOutput(f, finalPod.Name, "c1", "df", "-m", "/cache")
			framework.ExpectNoError(err, "failed to run df after rollback")
			gomega.Expect(stdout).To(gomega.ContainSubstring(strconv.FormatInt(origMB, 10)))

			ginkgo.By("deleting pod")
			podClient.DeleteSync(ctx, newPod.Name, metav1.DeleteOptions{}, f.Timeouts.PodDelete)
		},
		ginkgo.Entry("volume resize (upsize then downsize)",
			originalCPU, originalCPU, "200Mi", "200Mi", "64Mi", "128Mi",
		),
		ginkgo.Entry("volume resize + container resize (upsize then downsize)",
			originalCPU, increasedCPU, "200Mi", "250Mi", "64Mi", "128Mi",
		),
	)
}

func doPodResizeMemoryVolumeSizeLimitDecreaseTest(f *framework.Framework) {
	// 测试将 memory-backed emptyDir volume 的 sizeLimit 缩到低于当前用量时的行为：
	// 1. 创建一个带 128Mi memory-backed emptyDir volume 的 Pod。
	// 2. 向该 volume 写入 100Mi 数据。
	// 3. 将 volume size limit 缩到 50Mi —— 应失败，并在 condition 中报告错误。
	// 4. 验证 volume 挂载大小没有缩小，容器没有重启。
	ginkgo.It("should fail to shrink sizeLimit of a memory-backed volume below active usage", func(ctx context.Context) {
		podClient := e2epod.NewPodClient(f)
		original := []podresize.ResizableContainerInfo{{
			Name: "c1",
			Resources: &cgroups.ContainerResources{
				CPUReq: originalCPU, CPULim: originalCPU,
				MemReq: "200Mi", MemLim: "200Mi",
			},
		}}

		ginkgo.By("creating and verifying pod with memory-backed emptyDir volume")
		tStamp := strconv.Itoa(time.Now().Nanosecond())
		qty50Mi := resource.MustParse("50Mi")
		qty100Mi := resource.MustParse("100Mi")
		qty128Mi := resource.MustParse("128Mi")
		testPod := podresize.MakePodWithResizableContainers(f.Namespace.Name, "", tStamp, original, nil)
		testPod.GenerateName = "resize-memory-vol-fail-"
		testPod.Spec.Volumes = []v1.Volume{
			{
				Name: "mem-vol",
				VolumeSource: v1.VolumeSource{
					EmptyDir: &v1.EmptyDirVolumeSource{
						Medium:    v1.StorageMediumMemory,
						SizeLimit: &qty128Mi,
					},
				},
			},
		}
		testPod.Spec.Containers[0].VolumeMounts = []v1.VolumeMount{
			{
				Name:      "mem-vol",
				MountPath: "/cache",
			},
		}
		testPod = e2epod.MustMixinRestrictedPodSecurity(testPod)

		newPod := podClient.CreateSync(ctx, testPod)
		podresize.VerifyPodResources(newPod, original, nil)

		ginkgo.By("verifying new volume mount size via df inside the container")
		stdout, _, err := e2epod.ExecCommandInContainerWithFullOutput(f, newPod.Name, "c1", "df", "-m", "/cache")
		framework.ExpectNoError(err, "failed to run df inside container")
		gomega.Expect(stdout).To(gomega.ContainSubstring(strconv.FormatInt(qty128Mi.Value()/(1024*1024), 10)))

		ginkgo.By(fmt.Sprintf("writing a %s file to the volume to ensure active usage", qty100Mi.String()))
		_, _, err = e2epod.ExecCommandInContainerWithFullOutput(f, newPod.Name, "c1", "dd", "if=/dev/zero", "of=/cache/largefile", "bs=1M", fmt.Sprintf("count=%d", qty100Mi.Value()/(1024*1024)))
		framework.ExpectNoError(err, "failed to write large file inside container")

		ginkgo.By(fmt.Sprintf("patching pod spec's emptyDir sizeLimit down to %s", qty50Mi.String()))
		patchBytes := fmt.Appendf(nil, `{
			"spec": {
				"volumes": [
					{
						"name": "mem-vol",
						"emptyDir": {
							"sizeLimit": "%s"
						}
					}
				]
			}
		}`, qty50Mi.String())
		patchedPod, pErr := f.ClientSet.CoreV1().Pods(newPod.Namespace).Patch(ctx, newPod.Name,
			types.StrategicMergePatchType, patchBytes, metav1.PatchOptions{}, "resize")
		framework.ExpectNoError(pErr, "failed to patch emptyDir sizeLimit")

		ginkgo.By("waiting for resize actuation to fail and report error in condition")
		framework.ExpectNoError(framework.Gomega().
			Eventually(ctx, framework.RetryNotFound(framework.GetObject(f.ClientSet.CoreV1().Pods(patchedPod.Namespace).Get, patchedPod.Name, metav1.GetOptions{}))).
			WithTimeout(f.Timeouts.PodStart).
			Should(framework.MakeMatcher(func(pod *v1.Pod) (func() string, error) {
				var inProgressCondition *v1.PodCondition
				for i, condition := range pod.Status.Conditions {
					if condition.Type == v1.PodResizeInProgress {
						inProgressCondition = &pod.Status.Conditions[i]
						break
					}
				}
				if inProgressCondition == nil {
					return func() string { return "resize is not in progress" }, nil
				}
				if inProgressCondition.Reason != v1.PodReasonError {
					return func() string {
						return fmt.Sprintf("expected reason %s, got %s", v1.PodReasonError, inProgressCondition.Reason)
					}, nil
				}
				if len(inProgressCondition.Message) == 0 {
					return func() string { return "expected a non-empty error message in PodResizeInProgress condition" }, nil
				}
				return nil, nil
			})),
		)

		ginkgo.By("verifying the volume still holds the original size and c1 didn't restart")
		stdout, _, err = e2epod.ExecCommandInContainerWithFullOutput(f, newPod.Name, "c1", "df", "-m", "/cache")
		framework.ExpectNoError(err, "failed to run df inside container")
		gomega.Expect(stdout).To(gomega.ContainSubstring(strconv.FormatInt(qty128Mi.Value()/(1024*1024), 10)))

		gotPod, getErr := podClient.Get(ctx, newPod.Name, metav1.GetOptions{})
		framework.ExpectNoError(getErr)
		expected := podresize.UpdateExpectedContainerRestarts(ctx, gotPod, original)
		gomega.Expect(gotPod.Status.ContainerStatuses[0].RestartCount).To(gomega.Equal(expected[0].RestartCount))

		ginkgo.By("deleting pod")
		podClient.DeleteSync(ctx, newPod.Name, metav1.DeleteOptions{}, f.Timeouts.PodDelete)
	})
}

// 注意：Pod resize scheduler、resource quota、limit ranger、deferred resize 测试不在 e2e_node 测试范围内，
//
// \t因为在 e2e_node 测试中
// \t   a) Node e2e 不会运行 scheduler 和 controller manager
// \t   b) services 中的 api-server 未以 --enable-admission-plugins=ResourceQuota 启动
// \t      且无法通过 TEST_ARGS 启动它
// 上述测试位于 test/e2e/node/pod_resize.go
var _ = SIGDescribe("Pod InPlace Resize Container", func() {
	f := framework.NewDefaultFramework("pod-resize-tests")

	ginkgo.BeforeEach(func(ctx context.Context) {
		_, err := e2enode.GetRandomReadySchedulableNode(ctx, f.ClientSet)
		framework.ExpectNoError(err)
		if framework.NodeOSDistroIs("windows") {
			e2eskipper.Skipf("runtime does not support InPlacePodVerticalScaling -- skipping")
		}
	})

	doGuaranteedPodResizeTests(f)
	doBurstablePodResizeTests(f)
	doPodResizeReadAndReplaceTests(f)
	doPodResizePatchErrorTests(f)
	doPodResizeMemoryLimitDecreaseTest(f)
})

var _ = SIGDescribe("Pod InPlace Resize Init Container", framework.WithSlow(), framework.WithNodeConformance(), framework.WithFeatureGate(features.InPlacePodVerticalScalingInitContainers), func() {
	f := framework.NewDefaultFramework("pod-resize-tests")

	ginkgo.BeforeEach(func(ctx context.Context) {
		_, err := e2enode.GetRandomReadySchedulableNode(ctx, f.ClientSet)
		framework.ExpectNoError(err)
		if framework.NodeOSDistroIs("windows") {
			e2eskipper.Skipf("runtime does not support InPlacePodVerticalScaling -- skipping")
		}
	})

	doPodResizeInitContainerResizeTest(f)
})

var _ = SIGDescribe("Pod InPlace Resize Memory-Backed Volume", framework.WithFeatureGate(features.InPlacePodVerticalScalingMemoryBackedVolumes), func() {
	f := framework.NewDefaultFramework("pod-resize-memory-volumes")

	ginkgo.BeforeEach(func(ctx context.Context) {
		_, err := e2enode.GetRandomReadySchedulableNode(ctx, f.ClientSet)
		framework.ExpectNoError(err)
		if framework.NodeOSDistroIs("windows") {
			e2eskipper.Skipf("runtime does not support InPlacePodVerticalScaling -- skipping")
		}
	})

	doPodResizeMemoryVolumeTests(f)
	doPodResizeMemoryVolumeSizeLimitDecreaseTest(f)
})

func doPatchAndRollback(ctx context.Context, f *framework.Framework, originalContainers, expectedContainers []podresize.ResizableContainerInfo, originalPodResources, expectedPodResources *v1.ResourceRequirements, doRollback bool, mountPodCgroup bool) {
	ginkgo.By("creating and verifying pod")
	podClient := e2epod.NewPodClient(f)
	newPod := createAndVerifyPod(ctx, f, podClient, originalContainers, originalPodResources, mountPodCgroup)

	ginkgo.By("patching and verifying pod for resize")
	patchAndVerify(ctx, f, podClient, newPod, originalContainers, expectedContainers, originalPodResources, expectedPodResources, "resize")
	if doRollback {
		// resize 已生效，测试逆操作。
		rollbackContainers := createRollbackContainers(originalContainers, expectedContainers)
		ginkgo.By("patching and verifying pod for rollback")
		patchAndVerify(ctx, f, podClient, newPod, expectedContainers, rollbackContainers, expectedPodResources, originalPodResources, "rollback")
	}

	ginkgo.By("deleting pod")
	podClient.DeleteSync(ctx, newPod.Name, metav1.DeleteOptions{}, f.Timeouts.PodDelete)
}

func patchAndVerify(ctx context.Context, f *framework.Framework, podClient *e2epod.PodClient, newPod *v1.Pod, originalContainers, expectedContainers []podresize.ResizableContainerInfo, originalPodResources, expectedPodResources *v1.ResourceRequirements, opStr string) {
	onCgroupv2 := cgroups.IsPodOnCgroupv2Node(f, newPod.Name, newPod.Spec.Containers[0].Name)
	oldWeights := make(map[string]int64)
	for _, ci := range originalContainers {
		if ci.Resources != nil {
			req := ci.Resources.ResourceRequirements()
			if req != nil {
				cpuReq := req.Requests.Cpu()
				if cpuReq == nil || !cpuReq.IsZero() || newPod.Spec.Resources == nil {
					w, err := cgroups.RetrieveContainerCPUWeight(ctx, f, newPod, ci.Name, onCgroupv2)
					if err == nil {
						oldWeights[ci.Name] = w
					}
				}
			}
		}
	}
	var oldPodWeight int64
	if originalPodResources != nil {
		w, err := cgroups.RetrievePodCPUWeight(ctx, f, newPod, onCgroupv2)
		if err == nil {
			oldPodWeight = w
		}
	}

	patch := podresize.MakeResizePatch(originalContainers, expectedContainers, originalPodResources, expectedPodResources)
	patchedPod, pErr := f.ClientSet.CoreV1().Pods(newPod.Namespace).Patch(ctx, newPod.Name,
		types.StrategicMergePatchType, patch, metav1.PatchOptions{}, "resize")
	framework.ExpectNoError(pErr, fmt.Sprintf("failed to patch pod for %s", opStr))

	expected := podresize.UpdateExpectedContainerRestarts(ctx, patchedPod, expectedContainers)

	podresize.VerifyPodResources(patchedPod, expected, expectedPodResources)
	resizedPod := podresize.WaitForPodResizeActuation(ctx, f, podClient, newPod, expected)

	podresize.ExpectPodResized(ctx, f, resizedPod, expected)
	if expectedPodResources != nil {
		framework.ExpectNoError(podresize.VerifyPodCgroupValues(ctx, f, resizedPod))
	}

	// 验证 CPU weight 朝正确方向变化
	for i, ci := range expectedContainers {
		oldW, ok := oldWeights[ci.Name]
		if !ok {
			continue
		}
		newW, err := cgroups.RetrieveContainerCPUWeight(ctx, f, resizedPod, ci.Name, onCgroupv2)
		if err != nil {
			continue
		}

		if originalContainers[i].Resources != nil && ci.Resources != nil {
			oldReq := originalContainers[i].Resources.ResourceRequirements()
			newReq := ci.Resources.ResourceRequirements()
			if oldReq != nil && newReq != nil {
				oldMilli := cgroups.GetEffectiveCPUMilli(oldReq.Requests, oldReq.Limits)
				newMilli := cgroups.GetEffectiveCPUMilli(newReq.Requests, newReq.Limits)
				if newMilli > oldMilli {
					gomega.Expect(newW).To(gomega.BeNumerically(">=", oldW), fmt.Sprintf("cpu weight for %s should not decrease when requested CPU increases", ci.Name))
				} else if newMilli < oldMilli {
					gomega.Expect(newW).To(gomega.BeNumerically("<=", oldW), fmt.Sprintf("cpu weight for %s should not increase when requested CPU decreases", ci.Name))
				}
			}
		}
	}

	if originalPodResources != nil && expectedPodResources != nil && oldPodWeight > 0 {
		newPodW, err := cgroups.RetrievePodCPUWeight(ctx, f, resizedPod, onCgroupv2)
		if err == nil {
			oldMilli := cgroups.GetEffectiveCPUMilli(originalPodResources.Requests, originalPodResources.Limits)
			newMilli := cgroups.GetEffectiveCPUMilli(expectedPodResources.Requests, expectedPodResources.Limits)
			if newMilli > oldMilli {
				gomega.Expect(newPodW).To(gomega.BeNumerically(">=", oldPodWeight), "pod cpu weight should not decrease when requested Pod CPU increases")
			} else if newMilli < oldMilli {
				gomega.Expect(newPodW).To(gomega.BeNumerically("<=", oldPodWeight), "pod cpu weight should not increase when requested Pod CPU decreases")
			}
		}
	}
}

func createAndVerifyPod(ctx context.Context, f *framework.Framework, podClient *e2epod.PodClient, originalContainers []podresize.ResizableContainerInfo, podResources *v1.ResourceRequirements, mountPodCgroup bool) *v1.Pod {
	tStamp := strconv.Itoa(time.Now().Nanosecond())
	testPod := podresize.MakePodWithResizableContainers(f.Namespace.Name, "", tStamp, originalContainers, podResources)
	testPod.GenerateName = "resize-test-"
	testPod = e2epod.MustMixinRestrictedPodSecurity(testPod)

	// TODO: 为所有 pod resize 相关测试挂载 pod cgroup 路径，并移除
	// 对 mountPodCgroup 的依赖
	if mountPodCgroup {
		// 在第一个容器中挂载 pod cgroup，用于验证 pod cgroup 值
		cgroups.ConfigureHostPathForPodCgroup(testPod)
	}

	newPod := podClient.CreateSync(ctx, testPod)

	podresize.VerifyPodResources(newPod, originalContainers, podResources)
	podresize.VerifyPodResizePolicy(newPod, originalContainers)
	framework.ExpectNoError(podresize.VerifyPodStatusResources(newPod, originalContainers))
	framework.ExpectNoError(podresize.VerifyPodContainersCgroupValues(ctx, f, newPod, originalContainers))
	if podResources != nil {
		framework.ExpectNoError(podresize.VerifyPodCgroupValues(ctx, f, newPod))
	}
	return newPod
}

func makeGuaranteedContainers(numContainers int,
	cpuPolicy, memPolicy v1.ResourceResizeRestartPolicy,
	initContainers, extendedResources bool,
	cpu, mem string) []podresize.ResizableContainerInfo {

	containers := []podresize.ResizableContainerInfo{}
	for i := range numContainers {
		// 稍微错开资源值，使多容器 Pod 中各容器资源不完全相同。
		resources := &cgroups.ContainerResources{
			CPUReq: offsetCPU(i, cpu),
			CPULim: offsetCPU(i, cpu),
			MemReq: offsetMemory(int64(i), mem),
			MemLim: offsetMemory(int64(i), mem),
		}

		if extendedResources {
			resources.ExtendedResourceReq = "1"
			resources.ExtendedResourceLim = "1"
		}

		containers = append(containers, podresize.ResizableContainerInfo{
			Name:      fmt.Sprintf("c%d", i+1),
			Resources: resources,
			CPUPolicy: &cpuPolicy,
			MemPolicy: &memPolicy,
		})
	}

	if initContainers {
		containers = append(containers, podresize.ResizableContainerInfo{
			Name:          "c1-init",
			Resources:     &cgroups.ContainerResources{CPUReq: cpu, CPULim: cpu, MemReq: mem, MemLim: mem},
			InitCtr:       true,
			RestartPolicy: v1.ContainerRestartPolicyAlways,
			CPUPolicy:     &cpuPolicy,
			MemPolicy:     &memPolicy,
		})
	}

	return containers
}

func makeBurstableContainers(numContainers int,
	cpuPolicy, memPolicy v1.ResourceResizeRestartPolicy,
	cpu, cpuLimit, mem, memLimit string) []podresize.ResizableContainerInfo {

	containers := []podresize.ResizableContainerInfo{}
	for i := range numContainers {
		// 稍微错开资源值，使多容器 Pod 中各容器资源不完全相同。
		resources := &cgroups.ContainerResources{CPUReq: offsetCPU(i, cpu), CPULim: offsetCPU(i, cpuLimit), MemReq: offsetMemory(int64(i), mem), MemLim: offsetMemory(int64(i), memLimit)}
		containers = append(containers, podresize.ResizableContainerInfo{
			Name:      fmt.Sprintf("c%d", i+1),
			Resources: resources,
			CPUPolicy: &cpuPolicy,
			MemPolicy: &memPolicy,
		})
	}
	return containers
}

func createRollbackContainers(originalContainers, expectedContainers []podresize.ResizableContainerInfo) []podresize.ResizableContainerInfo {
	rollbackContainers := make([]podresize.ResizableContainerInfo, len(originalContainers))
	copy(rollbackContainers, originalContainers)
	for i, c := range rollbackContainers {
		gomega.Expect(c.Name).To(gomega.Equal(expectedContainers[i].Name),
			"test case containers & expectations should be in the same order")
		// 触发重启的 resize 在回滚时应再次触发重启。
		rollbackContainers[i].RestartCount = expectedContainers[i].RestartCount
	}
	return rollbackContainers
}

func verifyInitContainerResources(ctx context.Context, f *framework.Framework, pod *v1.Pod, expectedContainers []podresize.ResizableContainerInfo, expectedInitCtr v1.Container) {
	ginkgo.GinkgoHelper()

	podresize.VerifyPodResizePolicy(pod, expectedContainers)
	podresize.VerifyPodResources(pod, expectedContainers, nil)

	// 等待 init container 在 status 中显示预期资源，并且
	// cgroup 符合预期。
	framework.ExpectNoError(framework.Gomega().
		Eventually(ctx, framework.RetryNotFound(framework.GetObject(f.ClientSet.CoreV1().Pods(pod.Namespace).Get, pod.Name, metav1.GetOptions{}))).
		WithTimeout(f.Timeouts.PodStart).
		Should(framework.MakeMatcher(func(pod *v1.Pod) (func() string, error) {
			if pod == nil || len(pod.Status.InitContainerStatuses) == 0 || pod.Status.InitContainerStatuses[0].Resources == nil {
				return func() string { return "pod or init container status is not available yet" }, nil
			}
			if err := podresize.VerifyPodContainersStatusResources(pod.Status.InitContainerStatuses, []v1.Container{expectedInitCtr}); err != nil {
				return func() string {
					return fmt.Sprintf("init container status resources don't match expected: %v", err)
				}, nil
			}
			onCgroupv2 := cgroups.IsPodOnCgroupv2Node(f, pod.Name, pod.Spec.InitContainers[0].Name)
			if err := cgroups.VerifyContainerCgroupValues(ctx, f, pod, &expectedInitCtr, onCgroupv2); err != nil {
				return func() string {
					return fmt.Sprintf("cgroup values for init container don't match expected: %v", err)
				}, nil
			}
			return nil, nil
		})),
	)
}
