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
	"os"
	"path/filepath"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/kubernetes/pkg/kubelet/images"
	"k8s.io/kubernetes/test/e2e/common/node"
	"k8s.io/kubernetes/test/e2e/framework"
	e2epod "k8s.io/kubernetes/test/e2e/framework/pod"
	e2eregistry "k8s.io/kubernetes/test/e2e/framework/registry"
	"k8s.io/kubernetes/test/e2e_node/services"
	admissionapi "k8s.io/pod-security-admission/api"

	"github.com/onsi/ginkgo/v2"
)

var _ = SIGDescribe("Container Runtime Conformance Test", func() {
	f := framework.NewDefaultFramework("runtime-conformance")
	f.NamespacePodSecurityLevel = admissionapi.LevelPrivileged // 自定义 registry 的 Pod 需要 HostPort

	ginkgo.Describe("container runtime conformance blackbox test", func() {

		ginkgo.Context("when running a container with a new image", func() {
			// 以下镜像不加入 NodePrePullImageList，因为此测试
			// 测试的是镜像拉取，这些镜像无需预拉取。ImagePullPolicy
			// 为 v1.PullAlways，因此不会被 framework 的镜像预拉取列表检查阻塞。
			for _, testCase := range []struct {
				description  string
				image        string
				setupRegisty bool
				phase        v1.PodPhase
				waiting      bool
			}{
				{
					description:  "should be able to pull from private registry with credential provider",
					image:        "pause:testing",
					setupRegisty: true,
					phase:        v1.PodRunning,
					waiting:      false,
				},
			} {
				var registryAddress string
				var podNodes []string
				ginkgo.BeforeEach(func(ctx context.Context) {
					var err error
					if !testCase.setupRegisty {
						return
					}

					registryAddress, podNodes, err = e2eregistry.SetupRegistry(ctx, f, true)
					framework.ExpectNoError(err)
					// 需要等待 registry 被移除，因此必须自行删除整个命名空间
					ginkgo.DeferCleanup(func(ctx context.Context) {
						f.DeleteNamespace(ctx, f.Namespace.Name)
					})
				})

				f.It(testCase.description+"", f.WithNodeConformance(), f.WithDisruptive(), f.WithSerial(), func(ctx context.Context) {
					name := "image-pull-test"
					container := node.ConformanceContainer{
						PodClient: e2epod.NewPodClient(f),
						Container: v1.Container{
							Name:  name,
							Image: registryAddress + "/" + testCase.image,
							// PullAlways 确保即使测试前镜像已存在，也始终会拉取。
							ImagePullPolicy: v1.PullAlways,
						},
						RestartPolicy: v1.RestartPolicyNever,
					}
					if testCase.setupRegisty {
						container.NodeName = podNodes[0]
					}

					auth := e2eregistry.User1DockerSecret(registryAddress).Data[v1.DockerConfigJsonKey]
					configFile := filepath.Join(services.KubeletRootDirectory, "config.json")

					// kubelet 会缓存配置 5 分钟，改为重启更直接
					restartKubelet(context.Background(), true)

					err := os.WriteFile(configFile, []byte(auth), 0644)
					framework.ExpectNoError(err)
					ginkgo.DeferCleanup(func(ctx context.Context) {
						imageManagerDir := filepath.Join(services.KubeletRootDirectory, "image_manager")

						// 重启 kubelet
						withStoppedKubelet(ctx, f, false, func() {
							framework.ExpectNoError(os.Remove(configFile))
							framework.ExpectNoError(os.RemoveAll(imageManagerDir))
						})
						framework.ExpectNoError(wait.PollUntilContextTimeout(ctx, 5*time.Second, 2*time.Minute, true, func(_ context.Context) (bool, error) {
							if _, err := os.Stat(imageManagerDir); err != nil {
								f.Logf("waiting for %q to appear: %v", imageManagerDir, err)
								return false, nil
							}
							return true, nil
						}),
							"directory %q never reappeared", imageManagerDir,
						)
					})

					// checkContainerStatus 检查容器状态是否符合预期。
					checkContainerStatus := func(ctx context.Context) error {
						status, err := container.GetStatus(ctx)
						if err != nil {
							return fmt.Errorf("failed to get container status: %w", err)
						}
						// 需要先检查容器状态。默认 Pod 状态是 pending，如果先检查
						// Pod phase 且期望的 phase 是 Pending，那么检查时容器状态可能
						// 尚未出现。
						// 检查容器状态
						if !testCase.waiting {
							if status.State.Running == nil {
								return fmt.Errorf("expected container state: Running, got: %q",
									node.GetContainerState(status.State))
							}
						}
						if testCase.waiting {
							if status.State.Waiting == nil {
								return fmt.Errorf("expected container state: Waiting, got: %q",
									node.GetContainerState(status.State))
							}
							reason := status.State.Waiting.Reason
							if reason != images.ErrImagePull.Error() &&
								reason != images.ErrImagePullBackOff.Error() {
								return fmt.Errorf("unexpected waiting reason: %q", reason)
							}
						}
						// 检查 Pod phase
						phase, err := container.GetPhase(ctx)
						if err != nil {
							return fmt.Errorf("failed to get pod phase: %w", err)
						}
						if phase != testCase.phase {
							return fmt.Errorf("expected pod phase: %q, got: %q", testCase.phase, phase)
						}
						return nil
					}

					ginkgo.By("create the container")
					container.Create(ctx)
					ginkgo.DeferCleanup(func(ctx context.Context) {
						ginkgo.By("delete the conformance container")
						if err := container.Delete(ctx); err != nil {
							framework.Logf("error deleting a conformance container: %v", err)
						}
					})

					ginkgo.By("check the container status")
					var latestErr error
					err = wait.PollUntilContextTimeout(ctx, node.ContainerStatusPollInterval, node.ContainerStatusRetryTimeout, true, func(ctx context.Context) (bool, error) {
						if latestErr = checkContainerStatus(ctx); latestErr != nil {
							return false, nil
						}
						return true, nil
					})
					if err != nil {
						credsContent, readErr := os.ReadFile(configFile)
						framework.Logf("credentials read error: %v; credentials used:\n%v", readErr, credsContent)
						framework.Failf("Failed to read container status: %v; last observed error from wait loop: %v", err, latestErr)
					}
				})
			}
		})
	})
})
