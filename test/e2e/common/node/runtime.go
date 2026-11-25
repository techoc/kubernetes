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

package node

import (
	"context"
	"fmt"
	"os"
	"path"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/kubernetes/pkg/kubelet/images"
	"k8s.io/kubernetes/test/e2e/framework"
	e2epod "k8s.io/kubernetes/test/e2e/framework/pod"
	e2eregistry "k8s.io/kubernetes/test/e2e/framework/registry"
	imageutils "k8s.io/kubernetes/test/utils/image"
	admissionapi "k8s.io/pod-security-admission/api"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	gomegatypes "github.com/onsi/gomega/types"
)

var _ = SIGDescribe("Container Runtime", func() {
	f := framework.NewDefaultFramework("container-runtime")
	f.NamespacePodSecurityLevel = admissionapi.LevelPrivileged // custom registry pods need HostPorts

	ginkgo.Describe("blackbox test", func() {
		ginkgo.Context("when starting a container that exits", func() {

			/*
				Release: v1.13
				Testname: Container Runtime, Restart Policy, Pod Phases
				Description: restart policy 为 'Always' 时，Pod 终止后 MUST 重启；为 'OnFailure' 时，仅当以非零退出码终止才 MUST 重启；为 'Never' 时 MUST 永不重启。三个测试用例都 MUST 校验重启次数。
			*/
			framework.ConformanceIt("should run with the expected status", f.WithNodeConformance(), func(ctx context.Context) {
				restartCountVolumeName := "restart-count"
				restartCountVolumePath := "/restart-count"
				testContainer := v1.Container{
					Image: imageutils.GetE2EImage(imageutils.BusyBox),
					VolumeMounts: []v1.VolumeMount{
						{
							MountPath: restartCountVolumePath,
							Name:      restartCountVolumeName,
						},
					},
				}
				testVolumes := []v1.Volume{
					{
						Name: restartCountVolumeName,
						VolumeSource: v1.VolumeSource{
							EmptyDir: &v1.EmptyDirVolumeSource{Medium: v1.StorageMediumMemory},
						},
					},
				}
				testCases := []struct {
					Name          string
					RestartPolicy v1.RestartPolicy
					Phase         v1.PodPhase
					State         ContainerState
					RestartCount  int32
					Ready         bool
				}{
					{"terminate-cmd-rpa", v1.RestartPolicyAlways, v1.PodRunning, ContainerStateRunning, 2, true},
					{"terminate-cmd-rpof", v1.RestartPolicyOnFailure, v1.PodSucceeded, ContainerStateTerminated, 1, false},
					{"terminate-cmd-rpn", v1.RestartPolicyNever, v1.PodFailed, ContainerStateTerminated, 0, false},
				}
				for _, testCase := range testCases {

					// 第一次运行失败，第二次成功，之后一直运行
					cmdScripts := `
f=%s
count=$(echo 'hello' >> $f ; wc -l $f | awk {'print $1'})
if [ $count -eq 1 ]; then
	exit 1
fi
if [ $count -eq 2 ]; then
	exit 0
fi
while true; do sleep 1; done
`
					tmpCmd := fmt.Sprintf(cmdScripts, path.Join(restartCountVolumePath, "restartCount"))
					testContainer.Name = testCase.Name
					testContainer.Command = []string{"sh", "-c", tmpCmd}
					terminateContainer := ConformanceContainer{
						PodClient:     e2epod.NewPodClient(f),
						Container:     testContainer,
						RestartPolicy: testCase.RestartPolicy,
						Volumes:       testVolumes,
					}
					terminateContainer.Create(ctx)
					ginkgo.DeferCleanup(framework.IgnoreNotFound(terminateContainer.Delete))

					ginkgo.By(fmt.Sprintf("Container '%s': should get the expected 'RestartCount'", testContainer.Name))
					gomega.Eventually(ctx, func() (int32, error) {
						status, err := terminateContainer.GetStatus(ctx)
						return status.RestartCount, err
					}, ContainerStatusRetryTimeout, ContainerStatusPollInterval).Should(gomega.Equal(testCase.RestartCount))

					ginkgo.By(fmt.Sprintf("Container '%s': should get the expected 'Phase'", testContainer.Name))
					gomega.Eventually(ctx, terminateContainer.GetPhase, ContainerStatusRetryTimeout, ContainerStatusPollInterval).Should(gomega.Equal(testCase.Phase))

					ginkgo.By(fmt.Sprintf("Container '%s': should get the expected 'Ready' condition", testContainer.Name))
					gomega.Eventually(ctx, func() (bool, error) {
						return terminateContainer.IsReady(ctx)
					}, ContainerStatusRetryTimeout, ContainerStatusPollInterval).Should(gomega.Equal(testCase.Ready))

					status, err := terminateContainer.GetStatus(ctx)
					framework.ExpectNoError(err)

					ginkgo.By(fmt.Sprintf("Container '%s': should get the expected 'State'", testContainer.Name))
					gomega.Expect(GetContainerState(status.State)).To(gomega.Equal(testCase.State))

					ginkgo.By(fmt.Sprintf("Container '%s': should be possible to delete", testContainer.Name))
					gomega.Expect(terminateContainer.Delete(ctx)).To(gomega.Succeed())
					gomega.Eventually(ctx, terminateContainer.Present, ContainerStatusRetryTimeout, ContainerStatusPollInterval).Should(gomega.BeFalseBecause("container '%s': should have been terminated by now.", testContainer.Name))
				}
			})
		})

		ginkgo.Context("on terminated container", func() {
			rootUser := int64(0)
			nonRootUser := int64(10000)
			adminUserName := "ContainerAdministrator"
			nonAdminUserName := "ContainerUser"

			// 在指定 PodPhase 下创建并终止容器，验证终止消息是否与预期输出一致，最后删除该容器。
			matchTerminationMessage := func(ctx context.Context, container v1.Container, expectedPhase v1.PodPhase, expectedMsg gomegatypes.GomegaMatcher) {
				container.Name = "termination-message-container"
				c := ConformanceContainer{
					PodClient:     e2epod.NewPodClient(f),
					Container:     container,
					RestartPolicy: v1.RestartPolicyNever,
				}

				ginkgo.By("create the container")
				c.Create(ctx)
				ginkgo.DeferCleanup(framework.IgnoreNotFound(c.Delete))

				ginkgo.By(fmt.Sprintf("wait for the container to reach %s", expectedPhase))
				gomega.Eventually(ctx, c.GetPhase, ContainerStatusRetryTimeout, ContainerStatusPollInterval).Should(gomega.Equal(expectedPhase))

				ginkgo.By("get the container status")
				status, err := c.GetStatus(ctx)
				framework.ExpectNoError(err)

				ginkgo.By("the container should be terminated")
				gomega.Expect(GetContainerState(status.State)).To(gomega.Equal(ContainerStateTerminated))

				ginkgo.By("the termination message should be set")
				framework.Logf("Expected: %v to match Container's Termination Message: %v --", expectedMsg, status.State.Terminated.Message)
				gomega.Expect(status.State.Terminated.Message).Should(expectedMsg)

				ginkgo.By("delete the container")
				gomega.Expect(c.Delete(ctx)).To(gomega.Succeed())
			}

			f.It("should report termination message if TerminationMessagePath is set", f.WithNodeConformance(), func(ctx context.Context) {
				container := v1.Container{
					Image:                  imageutils.GetE2EImage(imageutils.BusyBox),
					Command:                []string{"/bin/sh", "-c"},
					Args:                   []string{"/bin/echo -n DONE > /dev/termination-log"},
					TerminationMessagePath: "/dev/termination-log",
					SecurityContext:        &v1.SecurityContext{},
				}
				if framework.NodeOSDistroIs("windows") {
					container.SecurityContext.WindowsOptions = &v1.WindowsSecurityContextOptions{RunAsUserName: &adminUserName}
				} else {
					container.SecurityContext.RunAsUser = &rootUser
				}
				matchTerminationMessage(ctx, container, v1.PodSucceeded, gomega.Equal("DONE"))
			})

			/*
				Release: v1.15
				Testname: Container Runtime, TerminationMessagePath, non-root user and non-default path
				Description: 创建一个带容器的 Pod，以非 root 用户运行并设置自定义 TerminationMessagePath。Pod 成功将输出重定向到指定路径；容器终止时，终止消息 MUST 与该自定义路径中记录的预期输出一致。
			*/
			framework.ConformanceIt("should report termination message if TerminationMessagePath is set as non-root user and at a non-default path", f.WithNodeConformance(), func(ctx context.Context) {
				container := v1.Container{
					Image:                  imageutils.GetE2EImage(imageutils.BusyBox),
					Command:                []string{"/bin/sh", "-c"},
					Args:                   []string{"/bin/echo -n DONE > /dev/termination-custom-log"},
					TerminationMessagePath: "/dev/termination-custom-log",
					SecurityContext:        &v1.SecurityContext{},
				}
				if framework.NodeOSDistroIs("windows") {
					container.SecurityContext.WindowsOptions = &v1.WindowsSecurityContextOptions{RunAsUserName: &nonAdminUserName}
				} else {
					container.SecurityContext.RunAsUser = &nonRootUser
				}
				matchTerminationMessage(ctx, container, v1.PodSucceeded, gomega.Equal("DONE"))
			})

			/*
				Release: v1.15
				Testname: Container Runtime, TerminationMessage, from container's log output of failing container
				Description: 创建带容器的 Pod，容器输出被记录到日志且以错误退出。容器终止时，终止消息 MUST 与容器日志中记录的预期输出一致。
			*/
			framework.ConformanceIt("should report termination message from log output if TerminationMessagePolicy FallbackToLogsOnError is set", f.WithNodeConformance(), func(ctx context.Context) {
				container := v1.Container{
					Image:                    imageutils.GetE2EImage(imageutils.BusyBox),
					Command:                  []string{"/bin/sh", "-c"},
					Args:                     []string{"/bin/echo -n DONE; /bin/false"},
					TerminationMessagePath:   "/dev/termination-log",
					TerminationMessagePolicy: v1.TerminationMessageFallbackToLogsOnError,
				}
				matchTerminationMessage(ctx, container, v1.PodFailed, gomega.Equal("DONE"))
			})

			/*
				Release: v1.15
				Testname: Container Runtime, TerminationMessage, from log output of succeeding container
				Description: 创建带容器的 Pod，容器输出被记录到日志且成功退出。容器终止时，因容器执行成功，terminationMessage MUST 为空。
			*/
			framework.ConformanceIt("should report termination message as empty when pod succeeds and TerminationMessagePolicy FallbackToLogsOnError is set", f.WithNodeConformance(), func(ctx context.Context) {
				container := v1.Container{
					Image:                    imageutils.GetE2EImage(imageutils.BusyBox),
					Command:                  []string{"/bin/sh", "-c"},
					Args:                     []string{"/bin/echo -n DONE; /bin/true"},
					TerminationMessagePath:   "/dev/termination-log",
					TerminationMessagePolicy: v1.TerminationMessageFallbackToLogsOnError,
				}
				matchTerminationMessage(ctx, container, v1.PodSucceeded, gomega.Equal(""))
			})

			/*
				Release: v1.15
				Testname: Container Runtime, TerminationMessage, from file of succeeding container
				Description: 创建带容器的 Pod，容器输出被记录到文件中且成功退出。容器终止时，terminationMessage MUST 与文件内容一致。
			*/
			framework.ConformanceIt("should report termination message from file when pod succeeds and TerminationMessagePolicy FallbackToLogsOnError is set", f.WithNodeConformance(), func(ctx context.Context) {
				container := v1.Container{
					Image:                    imageutils.GetE2EImage(imageutils.BusyBox),
					Command:                  []string{"/bin/sh", "-c"},
					Args:                     []string{"/bin/echo -n OK > /dev/termination-log; /bin/echo DONE; /bin/true"},
					TerminationMessagePath:   "/dev/termination-log",
					TerminationMessagePolicy: v1.TerminationMessageFallbackToLogsOnError,
				}
				matchTerminationMessage(ctx, container, v1.PodSucceeded, gomega.Equal("OK"))
			})
		})

		framework.Context("when running a container with a new image", framework.WithSerial(), func() {
			var registryAddress string
			var registryNodeNames []string
			var pullSecret *v1.Secret
			ginkgo.BeforeEach(func(ctx context.Context) {
				var err error

				registryAddress, registryNodeNames, err = e2eregistry.SetupRegistry(ctx, f, true)
				framework.ExpectNoError(err)
				gomega.Expect(registryNodeNames).ToNot(gomega.BeEmpty())
				// 需要等待 registry 被移除，因此必须自行删除整个命名空间
				ginkgo.DeferCleanup(func(ctx context.Context) {
					f.DeleteNamespace(ctx, f.Namespace.Name)
				})

				secret := e2eregistry.User1DockerSecret(registryAddress)
				secret.Name = "image-pull-secret-" + string(uuid.NewUUID())
				// 可能会要求使用不同的 docker config JSON。
				if framework.TestContext.DockerConfigFile != "" {
					contents, err := os.ReadFile(framework.TestContext.DockerConfigFile)
					framework.ExpectNoError(err)
					secret.Data[v1.DockerConfigJsonKey] = contents
				}
				ginkgo.By("create image pull secret")
				pullSecret, err = f.ClientSet.CoreV1().Secrets(f.Namespace.Name).Create(ctx, secret, metav1.CreateOptions{})
				framework.ExpectNoError(err)
			})

			f.It("should not be able to pull image from invalid registry", f.WithNodeConformance(), func(ctx context.Context) {
				image := imageutils.GetE2EImage(imageutils.InvalidRegistryImage)
				_ = ImagePullTest(ctx, f, image, v1.PullAlways, nil, registryNodeNames[0], v1.PodPending, true)
			})

			f.It("should be able to pull image", f.WithNodeConformance(), func(ctx context.Context) {
				image := imageutils.GetE2EImage(imageutils.Agnhost)
				_ = ImagePullTest(ctx, f, image, v1.PullAlways, nil, registryNodeNames[0], v1.PodRunning, false)
			})

			f.It("should not be able to pull from private registry without secret", f.WithNodeConformance(), func(ctx context.Context) {
				image := registryAddress + "/pause:testing"
				_ = ImagePullTest(ctx, f, image, v1.PullAlways, nil, registryNodeNames[0], v1.PodPending, true)
			})

			f.It("should be able to pull from private registry with secret", f.WithNodeConformance(), func(ctx context.Context) {
				image := registryAddress + "/pause:testing"
				_ = ImagePullTest(ctx, f, image, v1.PullAlways, pullSecret, registryNodeNames[0], v1.PodRunning, false)
			})
		})
	})
})

// imagePullTest 使用的镜像不加入 NodePrePullImageList，因为该测试专门验证镜像拉取行为。
// 容器使用的 ImagePullPolicy 不是 PullNever，因此框架允许在测试期间拉取这些镜像。
func ImagePullTest(ctx context.Context, f *framework.Framework, image string, imagePullPolicy v1.PullPolicy, pullSecret *v1.Secret, nodeName string, expectedPhase v1.PodPhase, expectedPullStatus bool) *v1.Pod {
	container := ConformanceContainer{
		PodClient: e2epod.NewPodClient(f),
		NodeName:  nodeName,
		Container: v1.Container{
			Name:            "image-pull-test",
			Image:           image,
			ImagePullPolicy: imagePullPolicy,
		},
		RestartPolicy: v1.RestartPolicyNever,
	}
	if pullSecret != nil {
		container.ImagePullSecrets = []string{pullSecret.Name}
	}
	// checkContainerStatus 检查容器状态是否符合预期。
	checkContainerStatus := func(ctx context.Context) error {
		status, err := container.GetStatus(ctx)
		if err != nil {
			return fmt.Errorf("failed to get container status: %w", err)
		}
		// 必须先检查容器状态。Pod 默认状态是 Pending，若先检查 Pod 阶段且期望值也是 Pending，
		// 检查时容器状态可能还没出现。
		// 检查容器状态
		if !expectedPullStatus {
			if status.State.Running == nil {
				return fmt.Errorf("expected container state: Running, got: %q",
					GetContainerState(status.State))
			}
		}
		if expectedPullStatus {
			if status.State.Waiting == nil {
				gomega.Expect(status.State.Running).To(gomega.BeNil())
				return fmt.Errorf("expected container state: Waiting, got: %q",
					GetContainerState(status.State))
			}
			reason := status.State.Waiting.Reason
			if reason != images.ErrImagePull.Error() &&
				reason != images.ErrImagePullBackOff.Error() &&
				reason != images.ErrImageNeverPull.Error() {
				return fmt.Errorf("unexpected waiting reason: %q", reason)
			}
		}
		// 检查 Pod 阶段
		phase, err := container.GetPhase(ctx)
		if err != nil {
			return fmt.Errorf("failed to get pod phase: %w", err)
		}
		if phase != expectedPhase {
			return fmt.Errorf("expected pod phase: %q, got: %q", expectedPhase, phase)
		}
		return nil
	}

	ginkgo.By("create the container")
	testPod := container.Create(ctx)
	ginkgo.DeferCleanup(func(ctx context.Context) {
		ginkgo.By("delete the conformance container")
		if err := container.Delete(ctx); err != nil {
			framework.Logf("error deleting a conformance container: %v", err)
		}
	})

	ginkgo.By("check the container status")
	var latestErr error
	err := wait.PollUntilContextTimeout(ctx, ContainerStatusPollInterval, ContainerStatusRetryTimeout, true, func(ctx context.Context) (bool, error) {
		if latestErr = checkContainerStatus(ctx); latestErr != nil {
			return false, nil
		}
		return true, nil
	})
	if err != nil {
		framework.Failf("Failed to read container status: %v; last observed error from wait loop: %v", err, latestErr)
	}

	return testPod
}
