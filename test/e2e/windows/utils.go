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
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"time"

	"github.com/onsi/ginkgo/v2"
	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/apimachinery/pkg/util/wait"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/pkg/controller/deployment/util"
	"k8s.io/kubernetes/test/e2e/framework"
	e2eskipper "k8s.io/kubernetes/test/e2e/framework/skipper"
	imageutils "k8s.io/kubernetes/test/utils/image"

	semver "github.com/blang/semver/v4"
)

// 等待 deployment 创建完成、期望副本数更新并可用，且不再有旧 pod 运行。
func waitForDeployment(getDeploymentFunc func() (*appsv1.Deployment, error), interval, timeout time.Duration) error {
	return wait.PollImmediate(interval, timeout, func() (bool, error) {
		deployment, err := getDeploymentFunc()
		if err != nil {
			if apierrors.IsNotFound(err) {
				framework.Logf("deployment not found, continue waiting: %s", err)
				return false, nil
			}

			framework.Logf("error while deploying, error %s", err)
			return false, err
		}
		framework.Logf("deployment status %s", &deployment.Status)
		return util.DeploymentComplete(deployment, &deployment.Status), nil
	})
}

// 获取节点的容器运行时及其版本
func getNodeContainerRuntimeAndVersion(n v1.Node) (string, semver.Version, error) {
	containerRuntimeVersionString := n.Status.NodeInfo.DeepCopy().ContainerRuntimeVersion
	parts := strings.Split(containerRuntimeVersionString, "://")

	if len(parts) != 2 {
		return "", semver.Version{}, fmt.Errorf("could not get container runtime and version from '%s'", containerRuntimeVersionString)
	}

	v, err := semver.ParseTolerant(parts[1])
	if err != nil {
		return "", semver.Version{}, err
	}

	return parts[0], v, nil
}

func getRandomUserGrounName() string {
	var letters = []rune("abcdefghijklmnopqrstuvwxya")

	s := make([]rune, 8)
	for i := range s {
		s[i] = letters[rand.Intn(len(letters))]
	}

	return "hpc-" + string(s)
}

func skipUnlessContainerdOneSevenOrGreater(ctx context.Context, f *framework.Framework) {
	ginkgo.By("Ensuring Windows nodes are running containerd v1.7+")
	windowsNode, err := findWindowsNode(ctx, f)
	framework.ExpectNoError(err, "error finding Windows node")
	r, v, err := getNodeContainerRuntimeAndVersion(windowsNode)
	framework.ExpectNoError(err, "error getting node container runtime and version")
	framework.Logf("Got runtime: %s, version %v for node %s", r, v, windowsNode.Name)

	if !strings.EqualFold(r, "containerd") {
		e2eskipper.Skipf("container runtime is not containerd")
	}

	v1dot7 := semver.MustParse("1.7.0-alpha.1")
	if v.LT(v1dot7) {
		e2eskipper.Skipf("container runtime is < 1.7.0")
	}
}

// windowsTestMemorySafetyBuffer 是在节点上为已准入 pod 的 request 之和加
// pod.Spec.Overhead 之外预留的固定余量（字节）。它用于吸收 kubelet/cgroup
// 内存统计的偏差，否则在容量临界点会偶发 OutOfmemory 准入错误。
//
// 256 MiB 是经验值，并非来自任何 kubelet 常量：它足以覆盖实测中观察到的
// 统计抖动（几 MiB 到几十 MiB），同时只占典型 Windows 测试节点
// Allocatable 的一小部分（约 5-10%）。若将来成为瓶颈，可通过测量收紧；
// 若需要在小到无法使用固定余量的节点上运行，也可改用 Allocatable 的比例。
const windowsTestMemorySafetyBuffer int64 = 256 * 1024 * 1024

var (
	podOverheadMemoryOnce  sync.Once
	podOverheadMemoryCache int64
	podOverheadMemoryErr   error
)

// detectPodOverheadMemory 返回集群准入链路注入的单 pod 内存开销
// （Pod Overhead，KEP-688，单位字节）；未注入开销时返回 0。
// 仅当探测本身无法完成（例如 apiserver 拒绝 DryRun 创建）时 error 才非 nil；
// 探测成功但没有开销时返回 (0, nil)。
//
// 结果与错误在测试进程生命周期内缓存，准入链路每次运行最多走一遍。
// 探测细节见 computePodOverheadMemory。
func detectPodOverheadMemory(ctx context.Context, c clientset.Interface, namespace string) (int64, error) {
	podOverheadMemoryOnce.Do(func() {
		podOverheadMemoryCache, podOverheadMemoryErr = computePodOverheadMemory(ctx, c, namespace)
	})
	return podOverheadMemoryCache, podOverheadMemoryErr
}

// computePodOverheadMemory 执行一次 DryRun pod 创建，检查变更后的结果中
// 是否带有 pod.Spec.Overhead。之所以用 DryRun 而不是直接读取 RuntimeClass，
// 是因为准入 webhook 可能根据 namespace、标签或其他请求范围内的数据
// 有条件地注入开销，而这些信息无法从静态 API 读取中获得。
//
// 这里假设开销由 RuntimeClass 决定（或对单个 pod 而言是静态的），不会随
// 容器数量、镜像或单容器 request 变化；若将来有 webhook 按 pod 形态调整
// 开销，本包中的容量计算将失效。调用方必须使用允许创建 pause 镜像 pod 的
// namespace（当前调用方设置 PodSecurity LevelPrivileged）。
func computePodOverheadMemory(ctx context.Context, c clientset.Interface, namespace string) (int64, error) {
	probePod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "overhead-probe-" + string(uuid.NewUUID()),
		},
		Spec: v1.PodSpec{
			Containers: []v1.Container{{
				Name:  "probe",
				Image: imageutils.GetPauseImageName(),
			}},
			NodeSelector: map[string]string{
				"kubernetes.io/os": "windows",
			},
		},
	}
	result, err := c.CoreV1().Pods(namespace).Create(
		ctx, probePod, metav1.CreateOptions{DryRun: []string{metav1.DryRunAll}},
	)
	if err != nil {
		return 0, fmt.Errorf("DryRun pod create for overhead detection failed: %w", err)
	}
	if result.Spec.Overhead == nil {
		return 0, nil
	}
	if mem, ok := result.Spec.Overhead[v1.ResourceMemory]; ok {
		framework.Logf("Detected pod overhead memory: %d bytes (per Pod Overhead admission, KEP-688)", mem.Value())
		return mem.Value(), nil
	}
	return 0, nil
}

// waitForNodeMemoryToSettle 轮询等待节点上至少有 neededBytes 的可准入空闲内存
// （allocatable 减去已准入 pod 的 request 之和），或等满 90 秒超时。
// 适用于 [Serial] 测试：前一个测试遗留的大 pod 处于 Terminating 时，
// 其 request 会一直被计入，直到彻底删除。
//
// 超时不会让测试失败，而是打印带标记的消息，便于把后续的 OutOfmemory
// 失败追溯到真正原因（残留 pod），而不是被误认为莫名其妙的调度错误。
func waitForNodeMemoryToSettle(ctx context.Context, c clientset.Interface, nodeName string, neededBytes int64) {
	node, err := c.CoreV1().Nodes().Get(ctx, nodeName, metav1.GetOptions{})
	if err != nil {
		framework.Logf("waitForNodeMemoryToSettle: cannot get node %s: %v (skipping wait)", nodeName, err)
		return
	}
	allocatable := node.Status.Allocatable.Memory().Value()
	var lastFree int64
	pollErr := wait.PollUntilContextTimeout(ctx, 5*time.Second, 90*time.Second, true, func(ctx context.Context) (bool, error) {
		existing := sumExistingPodMemoryReservation(ctx, c, nodeName)
		lastFree = allocatable - existing
		framework.Logf("Waiting for node %s memory to settle: existing-reservation=%d free=%d (need >= %d)",
			nodeName, existing, lastFree, neededBytes)
		return lastFree >= neededBytes, nil
	})
	if pollErr == nil {
		framework.Logf("Node %s memory settled: free=%d (need >= %d)", nodeName, lastFree, neededBytes)
		return
	}
	framework.Logf("Node %s memory did NOT settle within 90s: free=%d (need >= %d): %v "+
		"(test will likely fail with OutOfmemory; check for leftover Terminating pods)",
		nodeName, lastFree, neededBytes, pollErr)
}

// sumExistingPodMemoryReservation 返回调度到指定节点上所有非终态 pod 的
// 内存预留总量（容器内存 request 之和 + pod overhead）。
// 计算“剩余可调度内存”的测试（如 Memory Limits 测试）必须从 allocatable
// 中减去它，为 DaemonSet 和其他系统 pod 留出空间。
func sumExistingPodMemoryReservation(ctx context.Context, c clientset.Interface, nodeName string) int64 {
	podList, err := c.CoreV1().Pods("").List(ctx, metav1.ListOptions{
		FieldSelector: "spec.nodeName=" + nodeName,
	})
	if err != nil {
		framework.Logf("Could not list pods on node %s: %v (assuming 0 reservation)", nodeName, err)
		return 0
	}
	var total int64
	for _, p := range podList.Items {
		if p.Status.Phase == v1.PodSucceeded || p.Status.Phase == v1.PodFailed {
			continue
		}
		for _, c := range p.Spec.Containers {
			if mem, ok := c.Resources.Requests[v1.ResourceMemory]; ok {
				total += mem.Value()
			}
		}
		if p.Spec.Overhead != nil {
			if oh, ok := p.Spec.Overhead[v1.ResourceMemory]; ok {
				total += oh.Value()
			}
		}
	}
	return total
}
