/*
Copyright 2016 The Kubernetes Authors.

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
	"strconv"
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	internalapi "k8s.io/cri-api/pkg/apis"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
	"k8s.io/kubelet/pkg/types"
	"k8s.io/kubernetes/test/e2e/framework"
	e2epod "k8s.io/kubernetes/test/e2e/framework/pod"
	admissionapi "k8s.io/pod-security-admission/api"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

const (
	//TODO (dashpole): 动态配置可用后，测试 maxPerPodContainer 和 maxContainers 的不同值
	// 目前 maxPerPodContainer 和 maxTotalContainers 使用默认值
	maxPerPodContainer = 1
	maxTotalContainers = -1

	garbageCollectDuration = 3 * time.Minute
	setupDuration          = 10 * time.Minute
	runtimePollInterval    = 10 * time.Second
)

type testPodSpec struct {
	podName string
	// containerPrefix 对每个 pod 必须唯一，且不能以数字结尾。
	// 测试中用它标识哪些容器属于哪个 pod。
	containerPrefix string
	// 每个容器应重启的次数
	restartCount int32
	// 测试 pod 中的容器数量
	numContainers int
	// 一个函数，返回节点上当前的容器数（包括已死亡的容器）。
	getContainerNames func() ([]string, error)
}

func (pod *testPodSpec) getContainerName(containerNumber int) string {
	return fmt.Sprintf("%s%d", pod.containerPrefix, containerNumber)
}

type testRun struct {
	// 用于记录日志的名称
	testName string
	// 测试的 Pod spec
	testPods []*testPodSpec
}

// GarbageCollect 测试 Kubelet 是否符合 Kubelet 垃圾回收策略，见：
// http://kubernetes.io/docs/admin/garbage-collection/
var _ = SIGDescribe("GarbageCollect", framework.WithSerial(), framework.WithNodeConformance(), func() {
	f := framework.NewDefaultFramework("garbage-collect-test")
	f.NamespacePodSecurityLevel = admissionapi.LevelPrivileged
	containerNamePrefix := "gc-test-container-"
	podNamePrefix := "gc-test-pod-"

	// 这些后缀追加到 pod 和容器名称上。
	// 它们区分不同 pod，并允许
	// 按名称过滤，识别哪些容器属于哪些 pod
	// 它们必须唯一，且不能以数字结尾
	firstSuffix := "one-container-no-restarts"
	secondSuffix := "many-containers-many-restarts-one-pod"
	thirdSuffix := "many-containers-many-restarts-"
	tests := []testRun{
		{
			testName: "One Non-restarting Container",
			testPods: []*testPodSpec{
				{
					podName:         podNamePrefix + firstSuffix,
					containerPrefix: containerNamePrefix + firstSuffix,
					restartCount:    0,
					numContainers:   1,
				},
			},
		},
		{
			testName: "Many Restarting Containers",
			testPods: []*testPodSpec{
				{
					podName:         podNamePrefix + secondSuffix,
					containerPrefix: containerNamePrefix + secondSuffix,
					restartCount:    4,
					numContainers:   4,
				},
			},
		},
		{
			testName: "Many Pods with Many Restarting Containers",
			testPods: []*testPodSpec{
				{
					podName:         podNamePrefix + thirdSuffix + "one",
					containerPrefix: containerNamePrefix + thirdSuffix + "one",
					restartCount:    3,
					numContainers:   4,
				},
				{
					podName:         podNamePrefix + thirdSuffix + "two",
					containerPrefix: containerNamePrefix + thirdSuffix + "two",
					restartCount:    2,
					numContainers:   6,
				},
				{
					podName:         podNamePrefix + thirdSuffix + "three",
					containerPrefix: containerNamePrefix + thirdSuffix + "three",
					restartCount:    3,
					numContainers:   5,
				},
			},
		},
	}
	for _, test := range tests {
		containerGCTest(f, test)
	}
})

// 测试以下内容：
//
// 创建 pod，所有容器重启指定次数
// 容器运行期间，单个容器的副本数不超过 maxPerPodContainer
// 容器运行期间，容器总数不超过 maxTotalContainers
// 容器运行期间，如果不受 maxPerPodContainer 或 maxTotalContainers 限制，保留每个容器的额外副本
// pod 被杀死后，所有容器最终被清理
func containerGCTest(f *framework.Framework, test testRun) {
	var runtime internalapi.RuntimeService
	ginkgo.BeforeEach(func(ctx context.Context) {
		var err error
		runtime, _, err = getCRIClient(ctx)
		framework.ExpectNoError(err)
	})
	for _, pod := range test.testPods {
		// 初始化 getContainerNames 函数以使用 CRI runtime client。
		pod.getContainerNames = func() ([]string, error) {
			relevantContainers := []string{}
			containers, err := runtime.ListContainers(context.Background(), &runtimeapi.ContainerFilter{
				LabelSelector: map[string]string{
					types.KubernetesPodNameLabel:      pod.podName,
					types.KubernetesPodNamespaceLabel: f.Namespace.Name,
				},
			})
			if err != nil {
				return relevantContainers, err
			}
			for _, container := range containers {
				relevantContainers = append(relevantContainers, container.Labels[types.KubernetesContainerNameLabel])
			}
			return relevantContainers, nil
		}
	}

	ginkgo.Context(fmt.Sprintf("Garbage Collection Test: %s", test.testName), func() {
		ginkgo.BeforeEach(func(ctx context.Context) {
			realPods := getPods(test.testPods)
			e2epod.NewPodClient(f).CreateBatch(ctx, realPods)
			ginkgo.By("Making sure all containers restart the specified number of times")
			gomega.Eventually(ctx, func(ctx context.Context) error {
				for _, podSpec := range test.testPods {
					err := verifyPodRestartCount(ctx, f, podSpec.podName, podSpec.numContainers, podSpec.restartCount)
					if err != nil {
						return err
					}
				}
				return nil
			}, setupDuration, runtimePollInterval).Should(gomega.BeNil())
		})

		ginkgo.It("Should eventually garbage collect containers when we exceed the number of dead containers per container", func(ctx context.Context) {
			totalContainers := 0
			for _, pod := range test.testPods {
				totalContainers += pod.numContainers*2 + 1
			}
			gomega.Eventually(ctx, func() error {
				total := 0
				for _, pod := range test.testPods {
					containerNames, err := pod.getContainerNames()
					if err != nil {
						return err
					}
					total += len(containerNames)
					// 检查 pod 中每个容器的 maxPerPodContainer
					for i := 0; i < pod.numContainers; i++ {
						containerCount := 0
						for _, containerName := range containerNames {
							if containerName == pod.getContainerName(i) {
								containerCount++
							}
						}
						if containerCount > maxPerPodContainer+1 {
							return fmt.Errorf("expected number of copies of container: %s, to be <= maxPerPodContainer: %d; list of containers: %v",
								pod.getContainerName(i), maxPerPodContainer, containerNames)
						}
					}
				}
				//检查 maxTotalContainers。当前默认为 -1，因此在能配置 maxTotalContainers 前这永远不会发生
				if maxTotalContainers > 0 && totalContainers <= maxTotalContainers && total > maxTotalContainers {
					return fmt.Errorf("expected total number of containers: %v, to be <= maxTotalContainers: %v", total, maxTotalContainers)
				}
				return nil
			}, garbageCollectDuration, runtimePollInterval).Should(gomega.BeNil())

			if maxPerPodContainer >= 2 && maxTotalContainers < 0 { // make sure constraints wouldn't make us gc old containers
				ginkgo.By("Making sure the kubelet consistently keeps around an extra copy of each container.")
				gomega.Consistently(ctx, func() error {
					for _, pod := range test.testPods {
						containerNames, err := pod.getContainerNames()
						if err != nil {
							return err
						}
						for i := 0; i < pod.numContainers; i++ {
							containerCount := 0
							for _, containerName := range containerNames {
								if containerName == pod.getContainerName(i) {
									containerCount++
								}
							}
							if pod.restartCount > 0 && containerCount < maxPerPodContainer+1 {
								return fmt.Errorf("expected pod %v to have extra copies of old containers", pod.podName)
							}
						}
					}
					return nil
				}, garbageCollectDuration, runtimePollInterval).Should(gomega.BeNil())
			}
		})

		ginkgo.AfterEach(func(ctx context.Context) {
			for _, pod := range test.testPods {
				ginkgo.By(fmt.Sprintf("Deleting Pod %v", pod.podName))
				e2epod.NewPodClient(f).DeleteSync(ctx, pod.podName, metav1.DeleteOptions{}, f.Timeouts.PodDelete)
			}

			ginkgo.By("Making sure all containers get cleaned up")
			gomega.Eventually(ctx, func() error {
				for _, pod := range test.testPods {
					containerNames, err := pod.getContainerNames()
					if err != nil {
						return err
					}
					if len(containerNames) > 0 {
						return fmt.Errorf("%v containers still remain", containerNames)
					}
				}
				return nil
			}, garbageCollectDuration, runtimePollInterval).Should(gomega.BeNil())

			if ginkgo.CurrentSpecReport().Failed() && framework.TestContext.DumpLogsOnFailure {
				logNodeEvents(ctx, f)
				logPodEvents(ctx, f)
			}
		})
	})
}

func getPods(specs []*testPodSpec) (pods []*v1.Pod) {
	for _, spec := range specs {
		ginkgo.By(fmt.Sprintf("Creating %v containers with restartCount: %v", spec.numContainers, spec.restartCount))
		containers := []v1.Container{}
		for i := 0; i < spec.numContainers; i++ {
			containers = append(containers, v1.Container{
				Image:   busyboxImage,
				Name:    spec.getContainerName(i),
				Command: getRestartingContainerCommand("/test-empty-dir-mnt", i, spec.restartCount, ""),
				VolumeMounts: []v1.VolumeMount{
					{MountPath: "/test-empty-dir-mnt", Name: "test-empty-dir"},
				},
			})
		}
		pods = append(pods, &v1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: spec.podName},
			Spec: v1.PodSpec{
				RestartPolicy: v1.RestartPolicyAlways,
				Containers:    containers,
				Volumes: []v1.Volume{
					{Name: "test-empty-dir", VolumeSource: v1.VolumeSource{EmptyDir: &v1.EmptyDirVolumeSource{}}},
				},
			},
		})
	}
	return
}

func getRestartingContainerCommand(path string, containerNum int, restarts int32, loopingCommand string) []string {
	return []string{
		"sh",
		"-c",
		fmt.Sprintf(`
			f=%s/countfile%s
			count=$(echo 'hello' >> $f ; wc -l $f | awk {'print $1'})
			if [ $count -lt %d ]; then
				exit 0
			fi
			while true; do %s sleep 1; done`,
			path, strconv.Itoa(containerNum), restarts+1, loopingCommand),
	}
}

func verifyPodRestartCount(ctx context.Context, f *framework.Framework, podName string, expectedNumContainers int, expectedRestartCount int32) error {
	updatedPod, err := f.ClientSet.CoreV1().Pods(f.Namespace.Name).Get(ctx, podName, metav1.GetOptions{})
	if err != nil {
		return err
	}
	if len(updatedPod.Status.ContainerStatuses) != expectedNumContainers {
		return fmt.Errorf("expected pod %s to have %d containers, actual: %d",
			updatedPod.Name, expectedNumContainers, len(updatedPod.Status.ContainerStatuses))
	}
	for _, containerStatus := range updatedPod.Status.ContainerStatuses {
		if containerStatus.RestartCount < expectedRestartCount {
			return fmt.Errorf("pod %s had container with restartcount %d.  Should have been at least %d",
				updatedPod.Name, containerStatus.RestartCount, expectedRestartCount)
		}
	}
	return nil
}
