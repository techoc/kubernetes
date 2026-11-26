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

package podresize

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/apimachinery/pkg/util/strategicpatch"
	helpers "k8s.io/component-helpers/resource"
	"k8s.io/kubectl/pkg/util/podutils"
	kubeqos "k8s.io/kubernetes/pkg/kubelet/qos"
	"k8s.io/kubernetes/test/e2e/common/node/framework/cgroups"
	"k8s.io/kubernetes/test/e2e/framework"
	e2epod "k8s.io/kubernetes/test/e2e/framework/pod"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

const (
	MinContainerRuntimeVersion string = "1.6.9"
)

type ResizableContainerInfo struct {
	Name          string
	Resources     *cgroups.ContainerResources
	CPUPolicy     *v1.ResourceResizeRestartPolicy
	MemPolicy     *v1.ResourceResizeRestartPolicy
	RestartCount  int32
	RestartPolicy v1.ContainerRestartPolicy
	InitCtr       bool
}

func getTestResizePolicy(tcInfo ResizableContainerInfo) (resizePol []v1.ContainerResizePolicy) {
	if tcInfo.CPUPolicy != nil {
		cpuPol := v1.ContainerResizePolicy{ResourceName: v1.ResourceCPU, RestartPolicy: *tcInfo.CPUPolicy}
		resizePol = append(resizePol, cpuPol)
	}
	if tcInfo.MemPolicy != nil {
		memPol := v1.ContainerResizePolicy{ResourceName: v1.ResourceMemory, RestartPolicy: *tcInfo.MemPolicy}
		resizePol = append(resizePol, memPol)
	}
	return resizePol
}

func makeResizableContainer(tcInfo ResizableContainerInfo) v1.Container {
	cmd := "grep Cpus_allowed_list /proc/self/status | cut -f2 && sleep 1d"
	resizePol := getTestResizePolicy(tcInfo)
	tc := cgroups.MakeContainerWithResources(tcInfo.Name, tcInfo.Resources, cmd)
	tc.ResizePolicy = resizePol
	if tcInfo.RestartPolicy != "" {
		tc.RestartPolicy = &tcInfo.RestartPolicy
	}

	return tc
}

func MakePodWithResizableContainers(ns, name, timeStamp string, tcInfo []ResizableContainerInfo, podResources *v1.ResourceRequirements) *v1.Pod {
	testInitContainers, testContainers := separateContainers(tcInfo)

	minGracePeriodSeconds := int64(0)
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: ns,
			Labels: map[string]string{
				"time": timeStamp,
			},
		},
		Spec: v1.PodSpec{
			OS:                            &v1.PodOS{Name: v1.Linux},
			InitContainers:                testInitContainers,
			Containers:                    testContainers,
			RestartPolicy:                 v1.RestartPolicyOnFailure,
			TerminationGracePeriodSeconds: &minGracePeriodSeconds,
		},
	}

	if podResources != nil {
		pod.Spec.Resources = podResources
	}

	return pod
}

// separateContainers 将输入拆分为 initContainers 和普通容器。
func separateContainers(tcInfo []ResizableContainerInfo) ([]v1.Container, []v1.Container) {
	var initContainers, containers []v1.Container

	for _, ci := range tcInfo {
		tc := makeResizableContainer(ci)
		if ci.InitCtr {
			initContainers = append(initContainers, tc)
		} else {
			containers = append(containers, tc)
		}
	}

	return initContainers, containers
}

// separateContainerStatuses 将输入拆分为 initContainerStatuses 和 containerStatuses。
func separateContainerStatuses(tcInfo []ResizableContainerInfo) ([]v1.ContainerStatus, []v1.ContainerStatus) {
	var containerStatuses, initContainerStatuses []v1.ContainerStatus

	for _, ci := range tcInfo {
		ctrStatus := v1.ContainerStatus{
			Name:         ci.Name,
			RestartCount: ci.RestartCount,
		}
		if ci.InitCtr {
			initContainerStatuses = append(initContainerStatuses, ctrStatus)
		} else {
			containerStatuses = append(containerStatuses, ctrStatus)
		}
	}

	return initContainerStatuses, containerStatuses
}

func VerifyPodResizePolicy(gotPod *v1.Pod, wantInfo []ResizableContainerInfo) {
	ginkgo.GinkgoHelper()

	gotCtrs := append(append([]v1.Container{}, gotPod.Spec.Containers...), gotPod.Spec.InitContainers...)
	var wantCtrs []v1.Container
	for _, ci := range wantInfo {
		wantCtrs = append(wantCtrs, makeResizableContainer(ci))
	}
	gomega.Expect(gotCtrs).To(gomega.HaveLen(len(wantCtrs)), "number of containers in pod spec should match")
	for _, wantCtr := range wantCtrs {
		for _, gotCtr := range gotCtrs {
			if wantCtr.Name != gotCtr.Name {
				continue
			}
			gomega.Expect(v1.Container{Name: gotCtr.Name, ResizePolicy: gotCtr.ResizePolicy}).To(gomega.Equal(v1.Container{Name: wantCtr.Name, ResizePolicy: wantCtr.ResizePolicy}))
		}
	}
}

func VerifyPodResources(gotPod *v1.Pod, wantInfo []ResizableContainerInfo, wantPodResources *v1.ResourceRequirements) {
	ginkgo.GinkgoHelper()

	gotCtrs := append(append([]v1.Container{}, gotPod.Spec.Containers...), gotPod.Spec.InitContainers...)
	var wantCtrs []v1.Container
	for _, ci := range wantInfo {
		wantCtrs = append(wantCtrs, makeResizableContainer(ci))
	}
	gomega.Expect(gotCtrs).To(gomega.HaveLen(len(wantCtrs)), "number of containers in pod spec should match")
	for _, wantCtr := range wantCtrs {
		for _, gotCtr := range gotCtrs {
			if wantCtr.Name != gotCtr.Name {
				continue
			}
			gomega.Expect(gotCtr.Resources.Requests).To(gomega.BeComparableTo(wantCtr.Resources.Requests))
			gomega.Expect(gotCtr.Resources.Limits).To(gomega.BeComparableTo(wantCtr.Resources.Limits))
		}
	}
	gomega.Expect(gotPod.Spec.Resources).To(gomega.BeComparableTo(wantPodResources))

}

func VerifyPodStatusResources(gotPod *v1.Pod, wantInfo []ResizableContainerInfo) error {
	ginkgo.GinkgoHelper()

	wantInitCtrs, wantCtrs := separateContainers(wantInfo)
	var errs []error
	if err := VerifyPodContainersStatusResources(gotPod.Status.InitContainerStatuses, wantInitCtrs); err != nil {
		errs = append(errs, err)
	}
	if err := VerifyPodContainersStatusResources(gotPod.Status.ContainerStatuses, wantCtrs); err != nil {
		errs = append(errs, err)
	}

	return utilerrors.NewAggregate(errs)
}

func VerifyPodLevelStatusResources(gotPod *v1.Pod, wantPodResources *v1.ResourceRequirements) error {
	var errs []error
	if err := framework.Gomega().Expect(gotPod.Status.AllocatedResources).To(gomega.BeComparableTo(wantPodResources.Requests)); err != nil {
		errs = append(errs, fmt.Errorf("pod[%s] status allocatedResources mismatch: %w", gotPod.Name, err))
	}

	if err := framework.Gomega().Expect(gotPod.Status.Resources).To(gomega.BeComparableTo(wantPodResources)); err != nil {
		errs = append(errs, fmt.Errorf("pod[%s] status resources mismatch: %w", gotPod.Name, err))
	}
	return utilerrors.NewAggregate(errs)
}

func VerifyPodContainersStatusResources(gotCtrStatuses []v1.ContainerStatus, wantCtrs []v1.Container) error {
	ginkgo.GinkgoHelper()

	var errs []error
	if len(gotCtrStatuses) != len(wantCtrs) {
		return fmt.Errorf("expectation length mismatch: got %d statuses, want %d",
			len(gotCtrStatuses), len(wantCtrs))
	}
	for i, wantCtr := range wantCtrs {
		gotCtrStatus := gotCtrStatuses[i]
		if gotCtrStatus.Name != wantCtr.Name {
			errs = append(errs, fmt.Errorf("container status %d name %q != expected name %q", i, gotCtrStatus.Name, wantCtr.Name))
			continue
		}

		if err := framework.Gomega().Expect(gotCtrStatus.AllocatedResources).To(gomega.BeComparableTo(wantCtr.Resources.Requests)); err != nil {
			errs = append(errs, fmt.Errorf("container[%s] status allocatedResources mismatch: %w", wantCtr.Name, err))
		}

		if err := framework.Gomega().Expect(*gotCtrStatus.Resources).To(gomega.BeComparableTo(wantCtr.Resources)); err != nil {
			errs = append(errs, fmt.Errorf("container[%s] status resources mismatch: %w", wantCtr.Name, err))
		}
	}

	return utilerrors.NewAggregate(errs)
}

func VerifyPodCgroupValues(ctx context.Context, f *framework.Framework, pod *v1.Pod) error {
	aggregatedReqs, aggregatedLims := AggregateContainerResources(pod.Spec)
	cpuReq := aggregatedReqs[v1.ResourceCPU]
	cpuLim := aggregatedLims[v1.ResourceCPU]
	memLim := aggregatedLims[v1.ResourceMemory]
	if pod.Spec.Resources != nil {
		cpuReq = pod.Spec.Resources.Requests[v1.ResourceCPU]
		if pod.Spec.Resources.Limits != nil {
			if podCPULim, found := pod.Spec.Resources.Limits[v1.ResourceCPU]; found {
				cpuLim = podCPULim
			}
			if podMemLim, found := pod.Spec.Resources.Limits[v1.ResourceMemory]; found {
				memLim = podMemLim
			}
		}
	}

	cgroupResources := &cgroups.ContainerResources{
		CPUReq: cpuReq.String(),
		MemLim: memLim.String(),
		CPULim: cpuLim.String(),
		// cgroup 中未设置 memory request
	}

	return cgroups.VerifyPodCgroups(ctx, f, pod, cgroupResources)
}

func AggregateContainerResources(spec v1.PodSpec) (v1.ResourceList, v1.ResourceList) {
	// 按测试范围预先分配 map 容量，因为只跟踪
	// 'cpu' 和 'memory' 两种资源。
	aggregatedReqs := make(v1.ResourceList, 2)
	aggregatedLims := make(v1.ResourceList, 2)

	for _, container := range spec.Containers {
		addResourceList(aggregatedReqs, container.Resources.Requests)
		addResourceList(aggregatedLims, container.Resources.Limits)
	}

	for _, container := range spec.InitContainers {
		addResourceList(aggregatedReqs, container.Resources.Requests)
		addResourceList(aggregatedLims, container.Resources.Limits)
	}

	return aggregatedReqs, aggregatedLims
}

// TODO: 抽成公共 helper 方法，供 pod 级资源
// 相关测试复用
func addResourceList(des, src v1.ResourceList) {
	for name, quantity := range src {
		if value, found := des[name]; found {
			quantity.Add(value)
		}
		des[name] = quantity.DeepCopy()
	}
}

func VerifyPodContainersCgroupValues(ctx context.Context, f *framework.Framework, pod *v1.Pod, tcInfo []ResizableContainerInfo) error {
	ginkgo.GinkgoHelper()

	onCgroupv2 := cgroups.IsPodOnCgroupv2Node(f, pod.Name, pod.Spec.Containers[0].Name)

	var errs []error
	for _, ci := range tcInfo {
		tc := makeResizableContainer(ci)
		errs = append(errs, cgroups.VerifyContainerCgroupValues(ctx, f, pod, &tc, onCgroupv2))
	}
	return utilerrors.NewAggregate(errs)
}

func verifyPodRestarts(ctx context.Context, f *framework.Framework, pod *v1.Pod, wantInfo []ResizableContainerInfo) error {
	ginkgo.GinkgoHelper()

	initCtrStatuses, ctrStatuses := separateContainerStatuses(wantInfo)
	errs := []error{}
	if err := verifyContainerRestarts(ctx, f, pod, pod.Status.InitContainerStatuses, initCtrStatuses); err != nil {
		errs = append(errs, err)
	}
	if err := verifyContainerRestarts(ctx, f, pod, pod.Status.ContainerStatuses, ctrStatuses); err != nil {
		errs = append(errs, err)
	}

	return utilerrors.NewAggregate(errs)
}

func verifyContainerRestarts(ctx context.Context, f *framework.Framework, pod *v1.Pod, gotStatuses []v1.ContainerStatus, wantStatuses []v1.ContainerStatus) error {
	ginkgo.GinkgoHelper()

	if len(gotStatuses) != len(wantStatuses) {
		return fmt.Errorf("expectation length mismatch: got %d statuses, want %d",
			len(gotStatuses), len(wantStatuses))
	}

	errs := []error{}
	for i, gotStatus := range gotStatuses {
		if gotStatus.RestartCount != wantStatuses[i].RestartCount {
			errs = append(errs, fmt.Errorf("unexpected number of restarts for container %s: got %d, want %d", gotStatus.Name, gotStatus.RestartCount, wantStatuses[i].RestartCount))
		} else if gotStatus.RestartCount > 0 {
			if err := verifyOomScoreAdj(ctx, f, pod, gotStatus.Name); err != nil {
				errs = append(errs, err)
			}
		}
	}
	return utilerrors.NewAggregate(errs)
}

func verifyOomScoreAdj(ctx context.Context, f *framework.Framework, pod *v1.Pod, containerName string) error {
	container := e2epod.FindContainerInPod(pod, containerName)
	if container == nil {
		return fmt.Errorf("failed to find container %s in pod %s", containerName, pod.Name)
	}

	node, err := f.ClientSet.CoreV1().Nodes().Get(context.Background(), pod.Spec.NodeName, metav1.GetOptions{})
	if err != nil {
		return err
	}

	nodeMemoryCapacity := node.Status.Capacity[v1.ResourceMemory]
	oomScoreAdj := kubeqos.GetContainerOOMScoreAdjust(pod, container, int64(nodeMemoryCapacity.Value()))
	expectedOomScoreAdj := strconv.FormatInt(int64(oomScoreAdj), 10)

	return framework.Gomega().Eventually(ctx, framework.HandleRetry(func(ctx context.Context) (error, error) {
		return cgroups.VerifyOomScoreAdjValue(f, pod, container.Name, expectedOomScoreAdj), nil
	})).WithTimeout(framework.PollShortTimeout).Should(gomega.Succeed())
}

func WaitForPodResizeActuation(ctx context.Context, f *framework.Framework, podClient *e2epod.PodClient, pod *v1.Pod, expectedContainers []ResizableContainerInfo) *v1.Pod {
	ginkgo.GinkgoHelper()
	// 等待 resize 完成。

	framework.ExpectNoError(framework.Gomega().
		Eventually(ctx, framework.RetryNotFound(framework.GetObject(f.ClientSet.CoreV1().Pods(pod.Namespace).Get, pod.Name, metav1.GetOptions{}))).
		WithTimeout(f.Timeouts.PodStart).
		Should(framework.MakeMatcher(func(pod *v1.Pod) (func() string, error) {
			if helpers.IsPodResizeInfeasible(pod) {
				// 这是 resize 的终止状态
				return func() string {
					return "resize is infeasible"
				}, nil
			}

			if pod.Status.ObservedGeneration < pod.Generation {
				return func() string {
					return fmt.Sprintf("waiting for observedGeneration (%d) to catch up to generation (%d)",
						pod.Status.ObservedGeneration, pod.Generation)
				}, nil
			}

			// 等待 kubelet 清除 resize 状态条件。
			for _, c := range pod.Status.Conditions {
				if c.Type == v1.PodResizePending || c.Type == v1.PodResizeInProgress {
					return func() string {
						return fmt.Sprintf("resize status %v is still present in the pod status", c)
					}, nil
				}
			}

			// 等待 Pod 就绪。
			if !podutils.IsPodReady(pod) {
				return func() string { return "pod is not ready" }, nil
			}
			return nil, nil
		})),
	)

	resizedPod, err := framework.GetObject(podClient.Get, pod.Name, metav1.GetOptions{})(ctx)
	framework.ExpectNoError(err, "failed to get resized pod")
	return resizedPod
}

func ExpectPodResized(ctx context.Context, f *framework.Framework, resizedPod *v1.Pod, expectedContainers []ResizableContainerInfo) {
	ginkgo.GinkgoHelper()

	// 校验 Pod 容器的 Cgroup 值
	var errs []error

	onlyPLRSet := func(pod *v1.Pod) bool {
		if pod.Spec.Resources == nil {
			return false
		}

		for _, container := range pod.Spec.Containers {
			if container.Resources.Requests != nil && container.Resources.Limits != nil {
				return false
			}
		}
		return true
	}

	if onlyPLRSet(resizedPod) {
		// 等待容器启动。仅使用
		// pod 级资源且正在经历容器
		// 重启的 Pod，在 cgroup 指标可用前
		// 会有一段启动延迟。
		ginkgo.By("Waiting for cgroup reading")
		const cgroupReadDelay = time.Second * 60
		time.Sleep(cgroupReadDelay)
	}

	if cgroupErrs := VerifyPodContainersCgroupValues(ctx, f, resizedPod, expectedContainers); cgroupErrs != nil {
		errs = append(errs, fmt.Errorf("container cgroup values don't match expected: %w", formatErrors(cgroupErrs)))
	}

	if resourceErrs := VerifyPodStatusResources(resizedPod, expectedContainers); resourceErrs != nil {
		errs = append(errs, fmt.Errorf("container status resources don't match expected: %w", formatErrors(resourceErrs)))
	}

	if restartErrs := verifyPodRestarts(ctx, f, resizedPod, expectedContainers); restartErrs != nil {
		errs = append(errs, fmt.Errorf("container restart counts don't match expected: %w", formatErrors(restartErrs)))
	}

	// 校验 Pod 的 Resize 条件为空。
	for _, condition := range resizedPod.Status.Conditions {
		if condition.Type == v1.PodResizeInProgress || condition.Type == v1.PodResizePending {
			errs = append(errs, fmt.Errorf("unexpected resize condition type %s found in pod status", condition.Type))
		}
	}

	if len(errs) > 0 {
		resizedPod.ManagedFields = nil // 避免错误信息中出现 managed fields。
		framework.ExpectNoError(formatErrors(utilerrors.NewAggregate(errs)),
			"Verifying pod resources resize state. Pod: %s", framework.PrettyPrintJSON(resizedPod))
	}
}

func MakeResizePatch(originalContainers, desiredContainers []ResizableContainerInfo, originPodResources, desiredPodResources *v1.ResourceRequirements) []byte {
	original, err := json.Marshal(MakePodWithResizableContainers("", "", "", originalContainers, originPodResources))
	framework.ExpectNoError(err)

	desired, err := json.Marshal(MakePodWithResizableContainers("", "", "", desiredContainers, desiredPodResources))
	framework.ExpectNoError(err)

	patch, err := strategicpatch.CreateTwoWayMergePatch(original, desired, v1.Pod{})
	framework.ExpectNoError(err)

	return patch
}

// UpdateExpectedContainerRestarts 把给定 Pod 的 containerStatuses 中现有 RestartCount
// 加到 expectedContainers 的 RestartCount 上。
// 在 resize 前一刻抓取当前重启次数，只校验预期增量（0 或 1），
// 而非绝对次数，从而降低 RestartCount 断言的 flake。
func UpdateExpectedContainerRestarts(ctx context.Context, pod *v1.Pod, expectedContainers []ResizableContainerInfo) []ResizableContainerInfo {
	initialRestarts := make(map[string]int32)
	newExpectedContainers := []ResizableContainerInfo{}
	for _, ctr := range pod.Status.ContainerStatuses {
		initialRestarts[ctr.Name] = ctr.RestartCount
	}
	for _, ctr := range pod.Status.InitContainerStatuses {
		initialRestarts[ctr.Name] = ctr.RestartCount
	}
	for i, ctr := range expectedContainers {
		newExpectedContainers = append(newExpectedContainers, expectedContainers[i])
		newExpectedContainers[i].RestartCount += initialRestarts[ctr.Name]
	}
	return newExpectedContainers
}

func formatErrors(err error) error {
	// 每个错误单独一行，便于阅读。
	var agg utilerrors.Aggregate
	if !errors.As(err, &agg) {
		return err
	}

	errStrings := make([]string, len(agg.Errors()))
	for i, err := range agg.Errors() {
		errStrings[i] = err.Error()
	}
	return fmt.Errorf("[\n%s\n]", strings.Join(errStrings, ",\n"))
}
