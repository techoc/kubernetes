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
	"fmt"
	"strconv"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/test/e2e/common/node/framework/cgroups"
	"k8s.io/kubernetes/test/e2e/feature"
	"k8s.io/kubernetes/test/e2e/framework"
	e2enode "k8s.io/kubernetes/test/e2e/framework/node"
	e2epod "k8s.io/kubernetes/test/e2e/framework/pod"
	e2eskipper "k8s.io/kubernetes/test/e2e/framework/skipper"
	imageutils "k8s.io/kubernetes/test/utils/image"
	admissionapi "k8s.io/pod-security-admission/api"
)

var (
	cmd = e2epod.InfiniteSleepCommand
)

var _ = SIGDescribe("Pod Level Resources", framework.WithSerial(), feature.PodLevelResources, framework.WithFeatureGate(features.PodLevelResources), func() {
	f := framework.NewDefaultFramework("pod-level-resources-tests")
	f.NamespacePodSecurityLevel = admissionapi.LevelPrivileged

	ginkgo.BeforeEach(func(ctx context.Context) {
		_, err := e2enode.GetRandomReadySchedulableNode(ctx, f.ClientSet)
		framework.ExpectNoError(err)

		if framework.NodeOSDistroIs("windows") {
			e2eskipper.Skipf("not supported on windows -- skipping")
		}

		// 在未启用 cgroupv2 的节点上跳过该测试。
		if !isCgroupv2Node(f, ctx) {
			e2eskipper.Skipf("not supported on cgroupv1 -- skipping")
		}
	})
	podLevelResourcesTests(f)
})

// isCgroupv2Node 创建一个小 Pod，并检查它是否运行在
// 启用了 cgroupv2 的节点上。
// TODO: 重构为用 cgroupv2 标签标记此测试，改为在
// 测试任务中检查标签，以便在启用了 cgroupv2 的节点上运行该测试。
func isCgroupv2Node(f *framework.Framework, ctx context.Context) bool {
	podClient := e2epod.NewPodClient(f)
	containerResources := cgroups.ContainerResources{CPULim: "1m", MemReq: "1Mi"}
	cgroupv2Testpod := &v1.Pod{
		ObjectMeta: makeObjectMetadata("cgroupv2-check", f.Namespace.Name),
		Spec: v1.PodSpec{
			Containers: []v1.Container{
				{
					Name:      "cgroupv2-check",
					Image:     imageutils.GetE2EImage(imageutils.BusyBox),
					Command:   []string{"/bin/sh", "-c", cmd},
					Resources: *containerResources.ResourceRequirements(),
				},
			},
		},
	}

	pod := podClient.CreateSync(ctx, cgroupv2Testpod)
	defer func() {
		framework.Logf("Deleting %q pod", cgroupv2Testpod.Name)
		delErr := e2epod.DeletePodWithWait(ctx, f.ClientSet, pod)
		framework.ExpectNoError(delErr, "failed to delete pod %s", delErr)
	}()

	return cgroups.IsPodOnCgroupv2Node(f, pod.Name, pod.Spec.Containers[0].Name)
}

func makeObjectMetadata(name, namespace string) metav1.ObjectMeta {
	return metav1.ObjectMeta{
		Name: "testpod", Namespace: namespace,
		Labels: map[string]string{"time": strconv.Itoa(time.Now().Nanosecond())},
	}
}

type containerInfo struct {
	Name      string
	Resources *cgroups.ContainerResources
}

func makePod(metadata *metav1.ObjectMeta, podResources *cgroups.ContainerResources, containers []containerInfo) *v1.Pod {
	var testContainers []v1.Container
	for _, container := range containers {
		c := cgroups.MakeContainerWithResources(container.Name, container.Resources, cmd)
		testContainers = append(testContainers, c)
	}

	pod := &v1.Pod{
		ObjectMeta: *metadata,

		Spec: v1.PodSpec{
			Containers: testContainers,
		},
	}
	cgroups.ConfigureHostPathForPodCgroup(pod)

	if podResources != nil {
		res := podResources.ResourceRequirements()
		pod.Spec.Resources = res
	}

	return pod
}

func verifyPodResources(gotPod v1.Pod, inputInfo, expectedInfo *cgroups.ContainerResources) {
	ginkgo.GinkgoHelper()
	var expectedResources *v1.ResourceRequirements
	// 如果测试用例输入中未设置 pod 级资源，expectedResources 将为 nil。
	//
	if inputInfo != nil {
		expectedResources = expectedInfo.ResourceRequirements()
	}
	gomega.Expect(expectedResources).To(gomega.Equal(gotPod.Spec.Resources))
}

func verifyQoS(gotPod v1.Pod, expectedQoS v1.PodQOSClass) {
	ginkgo.GinkgoHelper()
	gomega.Expect(expectedQoS).To(gomega.Equal(gotPod.Status.QOSClass))
}

func podLevelResourcesTests(f *framework.Framework) {
	type expectedPodConfig struct {
		qos v1.PodQOSClass
		// totalPodResources 表示 Pod 聚合的资源 request
		// 和 limit。如果指定了 pod 级资源规格，
		// totalPodResources 就等于 pod 级资源。
		// 否则，通过聚合 Pod 内所有容器的资源 request 和
		// limit 计算得出。
		totalPodResources *cgroups.ContainerResources
	}

	type testCase struct {
		name         string
		podResources *cgroups.ContainerResources
		containers   []containerInfo
		expected     expectedPodConfig
		// 如果未指定 expectedPodLevelResourcesOverride，
		// 我们通过将其与 expected.totalPodResources 比较，检查 PodSpec，
		// 以验证 API server 是否正确注入了默认值。多数情况下这种比较可行。
		// 但是，如果未指定 pod 级 limit 且所有容器都设置了 limit，
		// Pod 的 cgroup 将使用聚合后的容器级 limit 配置。
		// 不过 pod 级 limit 字段本身不会被设置默认值。
		// 为覆盖这种模式，我们允许比较 PodSpec 时覆盖这些值。
		expectedPodLevelResourcesOverride *cgroups.ContainerResources
	}

	tests := []testCase{
		{
			name: "Guaranteed QoS pod with only container resources",
			containers: []containerInfo{
				{Name: "c1", Resources: &cgroups.ContainerResources{CPUReq: "50m", CPULim: "50m", MemReq: "70Mi", MemLim: "70Mi"}},
				{Name: "c2", Resources: &cgroups.ContainerResources{CPUReq: "70m", CPULim: "70m", MemReq: "50Mi", MemLim: "50Mi"}},
			},
			expected: expectedPodConfig{
				qos:               v1.PodQOSGuaranteed,
				totalPodResources: &cgroups.ContainerResources{CPUReq: "120m", CPULim: "120m", MemReq: "120Mi", MemLim: "120Mi"},
			},
		},
		{
			name:         "Guaranteed QoS pod, no container resources",
			podResources: &cgroups.ContainerResources{CPUReq: "100m", CPULim: "100m", MemReq: "100Mi", MemLim: "100Mi"},
			containers:   []containerInfo{{Name: "c1"}, {Name: "c2"}},
			expected: expectedPodConfig{
				qos:               v1.PodQOSGuaranteed,
				totalPodResources: &cgroups.ContainerResources{CPUReq: "100m", CPULim: "100m", MemReq: "100Mi", MemLim: "100Mi"},
			},
		},
		{
			name:         "Guaranteed QoS pod with other container resources",
			podResources: &cgroups.ContainerResources{CPUReq: "100m", CPULim: "100m", MemReq: "100Mi", MemLim: "100Mi"},
			containers: []containerInfo{
				{Name: "c1", Resources: &cgroups.ContainerResources{CPUReq: "50m", CPULim: "100m", MemReq: "50Mi", MemLim: "100Mi"}},
				{Name: "c2", Resources: &cgroups.ContainerResources{CPUReq: "50m", CPULim: "100m", MemReq: "50Mi", MemLim: "100Mi"}},
			},
			expected: expectedPodConfig{
				qos:               v1.PodQOSGuaranteed,
				totalPodResources: &cgroups.ContainerResources{CPUReq: "100m", CPULim: "100m", MemReq: "100Mi", MemLim: "100Mi"},
			},
		},
		{
			name:         "Guaranteed QoS pod, 1 container with resources",
			podResources: &cgroups.ContainerResources{CPUReq: "100m", CPULim: "100m", MemReq: "100Mi", MemLim: "100Mi"},
			containers: []containerInfo{
				{Name: "c1", Resources: &cgroups.ContainerResources{CPUReq: "50m", CPULim: "100m", MemReq: "50Mi", MemLim: "100Mi"}},
				{Name: "c2"},
			},
			expected: expectedPodConfig{
				qos:               v1.PodQOSGuaranteed,
				totalPodResources: &cgroups.ContainerResources{CPUReq: "100m", CPULim: "100m", MemReq: "100Mi", MemLim: "100Mi"},
			},
		},
		{
			name:         "Guaranteed QoS pod, pod resources limits, no container resources",
			podResources: &cgroups.ContainerResources{CPULim: "100m", MemLim: "100Mi"},
			containers: []containerInfo{
				{Name: "c1"},
				{Name: "c2"},
			},
			expected: expectedPodConfig{
				qos:               v1.PodQOSGuaranteed,
				totalPodResources: &cgroups.ContainerResources{CPUReq: "100m", CPULim: "100m", MemReq: "100Mi", MemLim: "100Mi"},
			},
		},
		{
			name:         "Burstable QoS pod, pod resources limits, container resources limits",
			podResources: &cgroups.ContainerResources{CPULim: "100m", MemLim: "100Mi"},
			containers: []containerInfo{
				{Name: "c1", Resources: &cgroups.ContainerResources{CPULim: "50m", MemLim: "50Mi"}},
				{Name: "c2", Resources: &cgroups.ContainerResources{CPULim: "30m", MemLim: "30Mi"}},
			},
			expected: expectedPodConfig{
				qos:               v1.PodQOSBurstable,
				totalPodResources: &cgroups.ContainerResources{CPUReq: "80m", CPULim: "100m", MemReq: "80Mi", MemLim: "100Mi"},
			},
		},
		{
			name:         "Burstable QoS pod, pod resources limits, container resources requests",
			podResources: &cgroups.ContainerResources{CPULim: "100m", MemLim: "100Mi"},
			containers: []containerInfo{
				{Name: "c1", Resources: &cgroups.ContainerResources{CPUReq: "50m", MemReq: "50Mi"}},
				{Name: "c2", Resources: &cgroups.ContainerResources{CPUReq: "30m", MemReq: "30Mi"}},
			},
			expected: expectedPodConfig{
				qos:               v1.PodQOSBurstable,
				totalPodResources: &cgroups.ContainerResources{CPUReq: "80m", CPULim: "100m", MemReq: "80Mi", MemLim: "100Mi"},
			},
		},
		{
			name:         "Burstable QoS pod, pod resources requests, no container resources",
			podResources: &cgroups.ContainerResources{CPUReq: "100m", MemReq: "100Mi"},
			containers:   []containerInfo{{Name: "c1"}, {Name: "c2"}},
			expected: expectedPodConfig{
				qos:               v1.PodQOSBurstable,
				totalPodResources: &cgroups.ContainerResources{CPUReq: "100m", MemReq: "100Mi"},
			},
		},
		{
			name:         "Burstable QoS pod, pod resources requests, container resources requests",
			podResources: &cgroups.ContainerResources{CPUReq: "100m", MemReq: "100Mi"},
			containers: []containerInfo{
				{Name: "c1", Resources: &cgroups.ContainerResources{CPUReq: "50m", MemReq: "50Mi"}},
				{Name: "c2", Resources: &cgroups.ContainerResources{CPUReq: "30m", MemReq: "30Mi"}},
			},
			expected: expectedPodConfig{
				qos:               v1.PodQOSBurstable,
				totalPodResources: &cgroups.ContainerResources{CPUReq: "100m", MemReq: "100Mi"},
			},
		},
		{
			name:         "Burstable QoS pod, pod resources requests, container resources requests and partial limits",
			podResources: &cgroups.ContainerResources{CPUReq: "100m", MemReq: "100Mi"},
			containers: []containerInfo{
				{Name: "c1", Resources: &cgroups.ContainerResources{CPUReq: "50m", CPULim: "50m", MemReq: "50Mi", MemLim: "50Mi"}},
				{Name: "c2", Resources: &cgroups.ContainerResources{CPUReq: "30m", MemReq: "30Mi"}},
			},
			expected: expectedPodConfig{
				qos:               v1.PodQOSBurstable,
				totalPodResources: &cgroups.ContainerResources{CPUReq: "100m", MemReq: "100Mi"},
			},
		},
		{
			name:         "Guaranteed QoS pod, pod resources requests, container resources limits",
			podResources: &cgroups.ContainerResources{CPUReq: "100m", MemReq: "100Mi"},
			containers: []containerInfo{
				{Name: "c1", Resources: &cgroups.ContainerResources{CPULim: "50m", MemLim: "50Mi"}},
				{Name: "c2", Resources: &cgroups.ContainerResources{CPULim: "50m", MemLim: "50Mi"}},
			},
			expected: expectedPodConfig{
				qos:               v1.PodQOSGuaranteed,
				totalPodResources: &cgroups.ContainerResources{CPUReq: "100m", CPULim: "100m", MemReq: "100Mi", MemLim: "100Mi"},
			},
		},
		{
			name:         "Burstable QoS pod, pod resources requests, partial container resources limits",
			podResources: &cgroups.ContainerResources{CPUReq: "100m", MemReq: "100Mi"},
			containers: []containerInfo{
				{Name: "c1", Resources: &cgroups.ContainerResources{CPULim: "50m", MemLim: "50Mi"}},
				{Name: "c2"},
			},
			expected: expectedPodConfig{
				qos: v1.PodQOSBurstable,
				// 如果并非所有容器都指定了容器级 limit，
				// cpu.max 和 memory.max 将不会被设置。
				totalPodResources: &cgroups.ContainerResources{CPUReq: "100m", MemReq: "100Mi"},
			},
			expectedPodLevelResourcesOverride: &cgroups.ContainerResources{CPUReq: "100m", MemReq: "100Mi"},
		},
		{
			name:         "Guaranteed QoS pod, pod resources requests, container resources requests and limits",
			podResources: &cgroups.ContainerResources{CPUReq: "100m", MemReq: "100Mi"},
			containers: []containerInfo{
				{Name: "c1", Resources: &cgroups.ContainerResources{CPUReq: "30m", CPULim: "30m", MemReq: "30Mi", MemLim: "30Mi"}},
				{Name: "c2", Resources: &cgroups.ContainerResources{CPUReq: "30m", CPULim: "30m", MemReq: "30Mi", MemLim: "30Mi"}},
			},
			expected: expectedPodConfig{
				qos:               v1.PodQOSGuaranteed,
				totalPodResources: &cgroups.ContainerResources{CPUReq: "100m", CPULim: "100m", MemReq: "100Mi", MemLim: "100Mi"},
			},
		},
		{
			name:         "Burstable QoS pod, no container resources",
			podResources: &cgroups.ContainerResources{CPUReq: "50m", CPULim: "100m", MemReq: "50Mi", MemLim: "100Mi"},
			containers: []containerInfo{
				{Name: "c1"},
				{Name: "c2"},
			},
			expected: expectedPodConfig{
				qos:               v1.PodQOSBurstable,
				totalPodResources: &cgroups.ContainerResources{CPUReq: "50m", CPULim: "100m", MemReq: "50Mi", MemLim: "100Mi"},
			},
		},
		{
			name:         "Burstable QoS pod with yet some other container resources",
			podResources: &cgroups.ContainerResources{CPUReq: "50m", CPULim: "100m", MemReq: "50Mi", MemLim: "100Mi"},
			containers: []containerInfo{
				{Name: "c1", Resources: &cgroups.ContainerResources{CPUReq: "20m", CPULim: "100m", MemReq: "20Mi", MemLim: "100Mi"}},
				{Name: "c2", Resources: &cgroups.ContainerResources{CPUReq: "30m", CPULim: "100m", MemReq: "30Mi", MemLim: "100Mi"}},
			},
			expected: expectedPodConfig{
				qos:               v1.PodQOSBurstable,
				totalPodResources: &cgroups.ContainerResources{CPUReq: "50m", CPULim: "100m", MemReq: "50Mi", MemLim: "100Mi"},
			},
		},
		{
			name:         "Burstable QoS pod, 1 container with resources",
			podResources: &cgroups.ContainerResources{CPUReq: "50m", CPULim: "100m", MemReq: "50Mi", MemLim: "100Mi"},
			containers: []containerInfo{
				{Name: "c1", Resources: &cgroups.ContainerResources{CPUReq: "20m", CPULim: "100m", MemReq: "50Mi", MemLim: "100Mi"}},
				{Name: "c2"},
			},
			expected: expectedPodConfig{
				qos:               v1.PodQOSBurstable,
				totalPodResources: &cgroups.ContainerResources{CPUReq: "50m", CPULim: "100m", MemReq: "50Mi", MemLim: "100Mi"},
			},
		},
		{
			name:         "Burstable QoS pod, pod resources requests and limits, container resources limits",
			podResources: &cgroups.ContainerResources{CPUReq: "50m", CPULim: "100m", MemReq: "50Mi", MemLim: "100Mi"},
			containers: []containerInfo{
				{Name: "c1", Resources: &cgroups.ContainerResources{CPULim: "30m", MemLim: "30Mi"}},
				{Name: "c2", Resources: &cgroups.ContainerResources{CPULim: "20m", MemLim: "20Mi"}},
			},
			expected: expectedPodConfig{
				qos:               v1.PodQOSBurstable,
				totalPodResources: &cgroups.ContainerResources{CPUReq: "50m", CPULim: "100m", MemReq: "50Mi", MemLim: "100Mi"},
			},
		},
		{
			name:         "Burstable QoS pod, pod resources requests and limits, container resources requests",
			podResources: &cgroups.ContainerResources{CPUReq: "50m", CPULim: "100m", MemReq: "50Mi", MemLim: "100Mi"},
			containers: []containerInfo{
				{Name: "c1", Resources: &cgroups.ContainerResources{CPUReq: "50m", MemReq: "30Mi"}},
				{Name: "c2", Resources: &cgroups.ContainerResources{MemReq: "20Mi"}},
			},
			expected: expectedPodConfig{
				qos:               v1.PodQOSBurstable,
				totalPodResources: &cgroups.ContainerResources{CPUReq: "50m", CPULim: "100m", MemReq: "50Mi", MemLim: "100Mi"},
			},
		},
		{
			name:         "Burstable QoS pod, partial requests in pod level resources, 1 container with guaranteed resources",
			podResources: &cgroups.ContainerResources{CPUReq: "", CPULim: "200m", MemReq: "200Mi", MemLim: "200Mi"},
			containers: []containerInfo{
				{Name: "c1", Resources: &cgroups.ContainerResources{CPUReq: "100m", CPULim: "100m", MemReq: "100Mi", MemLim: "100Mi"}},
				{Name: "c2"},
			},
			expected: expectedPodConfig{
				qos:               v1.PodQOSBurstable,
				totalPodResources: &cgroups.ContainerResources{CPUReq: "100m", CPULim: "200m", MemReq: "200Mi", MemLim: "200Mi"},
			},
		},
		{
			name: "BestEffort QoS no pod resources, no container resources",
			containers: []containerInfo{
				{Name: "c1"},
				{Name: "c2"},
			},
			expected: expectedPodConfig{
				qos:               v1.PodQOSBestEffort,
				totalPodResources: &cgroups.ContainerResources{CPUReq: "", CPULim: "", MemReq: "", MemLim: ""},
			},
		},
	}

	for _, tc := range tests {
		ginkgo.It(tc.name, func(ctx context.Context) {
			podMetadata := makeObjectMetadata("testpod", f.Namespace.Name)
			testPod := makePod(&podMetadata, tc.podResources, tc.containers)

			ginkgo.By("creating pods")
			podClient := e2epod.NewPodClient(f)
			pod := podClient.CreateSync(ctx, testPod)

			ginkgo.By("verifying pod resources are as expected")
			expectedPodResources := tc.expected.totalPodResources
			if tc.expectedPodLevelResourcesOverride != nil {
				expectedPodResources = tc.expectedPodLevelResourcesOverride
			}
			verifyPodResources(*pod, tc.podResources, expectedPodResources)

			ginkgo.By("verifying pod QoS as expected")
			verifyQoS(*pod, tc.expected.qos)

			ginkgo.By("verifying pod cgroup values")
			err := cgroups.VerifyPodCgroups(ctx, f, pod, tc.expected.totalPodResources)
			framework.ExpectNoError(err, "failed to verify pod's cgroup values: %v", err)

			ginkgo.By("verifying containers cgroup limits are same as pod container's cgroup limits")
			err = verifyContainersCgroupLimits(ctx, f, pod)
			framework.ExpectNoError(err, "failed to verify containers cgroup values: %v", err)

			ginkgo.By("deleting pods")
			delErr := e2epod.DeletePodWithWait(ctx, f.ClientSet, pod)
			framework.ExpectNoError(delErr, "failed to delete pod %s", delErr)
		})
	}
}

func verifyContainersCgroupLimits(ctx context.Context, f *framework.Framework, pod *v1.Pod) error {
	var errs []error
	for _, container := range pod.Spec.Containers {
		if pod.Spec.Resources != nil && pod.Spec.Resources.Limits.Memory() != nil &&
			container.Resources.Limits.Memory() == nil {
			err := cgroups.VerifyContainerMemoryLimit(ctx, f, pod, container.Name, &container.Resources, true)
			if err != nil {
				errs = append(errs, fmt.Errorf("failed to verify memory limit cgroup value: %w", err))
			}
		}

		if pod.Spec.Resources != nil && pod.Spec.Resources.Limits.Cpu() != nil &&
			container.Resources.Limits.Cpu() == nil {
			err := cgroups.VerifyContainerCPULimit(ctx, f, pod, container.Name, &container.Resources, true)
			if err != nil {
				errs = append(errs, fmt.Errorf("failed to verify cpu limit cgroup value: %w", err))
			}
		}
	}
	return utilerrors.NewAggregate(errs)
}

// podLevelResourcesFixDefaultingTests 包含同时需要
// PodLevelResources 和 PodLevelResourcesFixDefaulting feature gate 的测试。
func podLevelResourcesFixDefaultingTests(f *framework.Framework) {
	// 当设置了 pod 级 request 且所有容器都定义了 limit 时，pod 级
	// limit 会自动默认为聚合后的容器 limit。由于默认值处理后 req==lim，
	// Pod 会获得 Guaranteed QoS。
	ginkgo.It("Guaranteed QoS pod, pod resources requests, container resources limits auto-defaulted", func(ctx context.Context) {
		containers := []containerInfo{
			{Name: "c1", Resources: &cgroups.ContainerResources{CPULim: "50m", MemLim: "50Mi"}},
			{Name: "c2", Resources: &cgroups.ContainerResources{CPULim: "50m", MemLim: "50Mi"}},
		}
		podResources := &cgroups.ContainerResources{CPUReq: "100m", MemReq: "100Mi"}
		expectedResources := &cgroups.ContainerResources{CPUReq: "100m", CPULim: "100m", MemReq: "100Mi", MemLim: "100Mi"}

		podMetadata := makeObjectMetadata("testpod", f.Namespace.Name)
		testPod := makePod(&podMetadata, podResources, containers)

		ginkgo.By("creating pod")
		podClient := e2epod.NewPodClient(f)
		pod := podClient.CreateSync(ctx, testPod)

		ginkgo.By("verifying pod resources are as expected")
		verifyPodResources(*pod, podResources, expectedResources)

		ginkgo.By("verifying pod QoS as expected")
		verifyQoS(*pod, v1.PodQOSGuaranteed)

		ginkgo.By("verifying pod cgroup values")
		err := cgroups.VerifyPodCgroups(ctx, f, pod, expectedResources)
		framework.ExpectNoError(err, "failed to verify pod's cgroup values: %v", err)

		ginkgo.By("verifying containers cgroup limits are same as pod container's cgroup limits")
		err = verifyContainersCgroupLimits(ctx, f, pod)
		framework.ExpectNoError(err, "failed to verify containers cgroup values: %v", err)

		ginkgo.By("deleting pod")
		delErr := e2epod.DeletePodWithWait(ctx, f.ClientSet, pod)
		framework.ExpectNoError(delErr, "failed to delete pod %s", delErr)
	})

	// 当 pod 级只设置了 CPU request（无 memory request 或 limit）时，默认值处理
	// 会从聚合的容器 request 中补全缺失的 pod 级 request，并从容器 limit 中补全缺失的
	// pod 级 limit，以维持完整的 pod 级资源统计。
	// 此外，后续更新会保留这些默认值。
	ginkgo.It("partial pod-level request: all missing pod requests and limits defaulted from containers, stable across updates", func(ctx context.Context) {
		containers := []containerInfo{
			{Name: "c1", Resources: &cgroups.ContainerResources{CPUReq: "50m", CPULim: "50m", MemReq: "50Mi", MemLim: "50Mi"}},
		}
		// pod 级只设置 CPU request —— 无 memory request 或 limit。
		podResources := &cgroups.ContainerResources{CPUReq: "50m"}
		// 预期：memory request 从容器默认补全，CPU 和 memory limit 从容器 limit 默认补全。
		expectedResources := &cgroups.ContainerResources{CPUReq: "50m", CPULim: "50m", MemReq: "50Mi", MemLim: "50Mi"}

		podMetadata := makeObjectMetadata("testpod-partial-req", f.Namespace.Name)
		testPod := makePod(&podMetadata, podResources, containers)

		ginkgo.By("creating pod")
		podClient := e2epod.NewPodClient(f)
		pod := podClient.CreateSync(ctx, testPod)

		ginkgo.By("verifying pod resources after create: missing memory request and limits defaulted from container")
		verifyPodResources(*pod, podResources, expectedResources)

		ginkgo.By("patching an annotation to trigger a decode+update cycle")
		patch := []byte(`{"metadata":{"annotations":{"test-annotation":"trigger-defaulting"}}}`)
		updatedPod, err := f.ClientSet.CoreV1().Pods(f.Namespace.Name).Patch(
			ctx, pod.Name, types.MergePatchType, patch, metav1.PatchOptions{})
		framework.ExpectNoError(err, "failed to patch pod annotation")

		ginkgo.By("verifying pod resources after annotation update: defaulted memory request and limits remain unchanged")
		verifyPodResources(*updatedPod, podResources, expectedResources)

		ginkgo.By("deleting pod")
		delErr := e2epod.DeletePodWithWait(ctx, f.ClientSet, pod)
		framework.ExpectNoError(delErr, "failed to delete pod %s", delErr)
	})
}

var _ = SIGDescribe("Pod Level Resources Fix Defaulting", framework.WithSerial(), framework.WithFeatureGate(features.PodLevelResources), framework.WithFeatureGate(features.PodLevelResourcesFixDefaulting), func() {
	f := framework.NewDefaultFramework("pod-level-resources-fix-defaulting-tests")
	f.NamespacePodSecurityLevel = admissionapi.LevelPrivileged

	ginkgo.BeforeEach(func(ctx context.Context) {
		_, err := e2enode.GetRandomReadySchedulableNode(ctx, f.ClientSet)
		framework.ExpectNoError(err)

		if framework.NodeOSDistroIs("windows") {
			e2eskipper.Skipf("not supported on windows -- skipping")
		}

		if !isCgroupv2Node(f, ctx) {
			e2eskipper.Skipf("not supported on cgroupv1 -- skipping")
		}
	})

	podLevelResourcesFixDefaultingTests(f)
})
