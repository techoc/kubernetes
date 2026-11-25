/*
Copyright The Kubernetes Authors.

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
	"strconv"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/test/e2e/common/node/framework/cgroups"
	"k8s.io/kubernetes/test/e2e/common/node/framework/podresize"
	"k8s.io/kubernetes/test/e2e/feature"
	"k8s.io/kubernetes/test/e2e/framework"
	e2enode "k8s.io/kubernetes/test/e2e/framework/node"
	e2epod "k8s.io/kubernetes/test/e2e/framework/pod"
	e2eskipper "k8s.io/kubernetes/test/e2e/framework/skipper"
	"k8s.io/kubernetes/test/e2e_node/criproxy"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

var _ = SIGDescribe("Pod InPlace Resize Memory-Backed Volume with CRI Proxy", framework.WithFeatureGate(features.InPlacePodVerticalScalingMemoryBackedVolumes), func() {
	// 测试多个并发 volume 和容器 resize 操作的协调流水线。
	//
	// 测试创建一个带两个 memory-backed emptyDir volume（vol-1 和 vol-2）的 Pod，并触发一个 patch：
	// 1. 缩小 vol-1（100Mi -> 50Mi）
	// 2. 扩大 vol-2（100Mi -> 250Mi）
	// 3. 扩大容器的 memory limit（220Mi -> 320Mi）
	//
	// 通过在容器的 CRI UpdateContainerResources 调用中注入延迟，测试验证：
	// - vol-1 的缩小立即执行（因为 volume 缩小是缩小序列的第一步）。
	// - 容器扩容待处理期间，vol-2 的扩大被阻塞并保持在 100Mi（因为 volume 扩大必须等待容器 cgroup 内存扩展完成）。
	// - 容器 cgroup 更新解除阻塞后，vol-2 的扩大最终完成。
	f := framework.NewDefaultFramework("pod-resize-memory-vol-criproxy")

	ginkgo.BeforeEach(func(ctx context.Context) {
		_, err := e2enode.GetRandomReadySchedulableNode(ctx, f.ClientSet)
		framework.ExpectNoError(err)
		if framework.NodeOSDistroIs("windows") {
			e2eskipper.Skipf("runtime does not support InPlacePodVerticalScaling -- skipping")
		}
	})

	f.Context("with CRI Proxy delays", feature.CriProxy, func() {
		ginkgo.BeforeEach(func() {
			if e2eCriProxy == nil {
				ginkgo.Skip("Skip the test since the CRI Proxy is undefined. Please run with --cri-proxy-enabled=true")
			}
			if err := resetCRIProxyInjector(e2eCriProxy); err != nil {
				ginkgo.Skip("Skip the test since the CRI Proxy is undefined.")
			}
			ginkgo.DeferCleanup(func() error {
				return resetCRIProxyInjector(e2eCriProxy)
			})
		})

		ginkgo.It("should actuate multiple volume resizing in the correct order, prioritizing downsize before upsize", func(ctx context.Context) {
			podClient := e2epod.NewPodClient(f)
			original := []podresize.ResizableContainerInfo{{
				Name:      "c1",
				Resources: &cgroups.ContainerResources{CPUReq: "100m", CPULim: "100m", MemReq: "220Mi", MemLim: "220Mi"},
			}}
			resizedContainers := []podresize.ResizableContainerInfo{{
				Name:      "c1",
				Resources: &cgroups.ContainerResources{CPUReq: "100m", CPULim: "100m", MemReq: "320Mi", MemLim: "320Mi"},
			}}

			ginkgo.By("creating and verifying pod with two memory-backed emptyDir volumes")
			tStamp := strconv.Itoa(time.Now().Nanosecond())
			testPod := podresize.MakePodWithResizableContainers(f.Namespace.Name, "", tStamp, original, nil)
			testPod.GenerateName = "resize-vol-ordered-"

			limit100 := resource.MustParse("100Mi")
			testPod.Spec.Volumes = []v1.Volume{
				{
					Name: "vol-1", // 将缩小到 50Mi
					VolumeSource: v1.VolumeSource{
						EmptyDir: &v1.EmptyDirVolumeSource{
							Medium:    v1.StorageMediumMemory,
							SizeLimit: &limit100,
						},
					},
				},
				{
					Name: "vol-2", // 将扩大到 250Mi
					VolumeSource: v1.VolumeSource{
						EmptyDir: &v1.EmptyDirVolumeSource{
							Medium:    v1.StorageMediumMemory,
							SizeLimit: &limit100,
						},
					},
				},
			}

			testPod.Spec.Containers[0].VolumeMounts = []v1.VolumeMount{
				{
					Name:      "vol-1",
					MountPath: "/cache1",
				},
				{
					Name:      "vol-2",
					MountPath: "/cache2",
				},
			}
			testPod = e2epod.MustMixinRestrictedPodSecurity(testPod)

			newPod := podClient.CreateSync(ctx, testPod)
			podresize.VerifyPodResources(newPod, original, nil)

			ginkgo.By("Injecting delay into UpdateContainerResources calls")
			const delayTime = 40 * time.Second
			err := addCRIProxyInjector(e2eCriProxy, func(apiName string) error {
				if apiName == criproxy.UpdateContainerResources {
					framework.Logf("Delaying UpdateContainerResources by %v", delayTime)
					time.Sleep(delayTime)
				}
				return nil
			})
			framework.ExpectNoError(err)

			ginkgo.By("Patching pod: downsize vol-1, upsize vol-2, and upsize container")
			patchBytes := []byte(`{
				"spec": {
					"containers": [
						{
							"name": "c1",
							"resources": {
								"limits": {
									"memory": "320Mi"
								},
								"requests": {
									"memory": "320Mi"
								}
							}
						}
					],
					"volumes": [
						{
							"name": "vol-1",
							"emptyDir": {
								"sizeLimit": "50Mi"
							}
						},
						{
							"name": "vol-2",
							"emptyDir": {
								"sizeLimit": "250Mi"
							}
						}
					]
				}
			}`)
			patchedPod, pErr := f.ClientSet.CoreV1().Pods(newPod.Namespace).Patch(ctx, newPod.Name,
				types.StrategicMergePatchType, patchBytes, metav1.PatchOptions{}, "resize")
			framework.ExpectNoError(pErr, "failed to patch volume/container limits")

			// 执行顺序：
			// 1. 将 vol-1 缩小到 50Mi（volume 缩小是缩小的第 1 步）。
			// 2. 调用 UpdateContainerResources 将 cgroup 扩展到 320Mi（cgroup 扩展是扩大的第 1 步）。
			//    --> 此处延迟 40 秒。
			// 3. 将 vol-2 扩大到 250Mi（volume 扩大是扩大的第 2 步）。
			//
			// 因此，在第 2 步阻塞期间（40 秒延迟内）：
			// - vol-1（缩小）MUST 立即缩小到 50Mi。
			// - vol-2（扩大）MUST NOT 此时扩大，必须保持在 100Mi。

			ginkgo.By("Verifying vol-1 is downsized immediately while vol-2 remains unchanged")
			gomega.Eventually(ctx, func(ctx context.Context) (string, error) {
				stdout, _, err := e2epod.ExecCommandInContainerWithFullOutput(f, newPod.Name, "c1", "df", "-m", "/cache1")
				return stdout, err
			}).WithTimeout(30 * time.Second).WithPolling(500 * time.Millisecond).Should(gomega.ContainSubstring("50"))

			// 验证 CRI 阻塞期间 vol-2 保持在 100Mi
			stdout, _, err := e2epod.ExecCommandInContainerWithFullOutput(f, newPod.Name, "c1", "df", "-m", "/cache2")
			framework.ExpectNoError(err)
			gomega.Expect(stdout).To(gomega.ContainSubstring("100"))

			ginkgo.By("Waiting for the entire actuation to complete after CRI unblocks")
			expected := podresize.UpdateExpectedContainerRestarts(ctx, patchedPod, resizedContainers)
			gotPod := podresize.WaitForPodResizeActuation(ctx, f, podClient, patchedPod, expected)
			podresize.ExpectPodResized(ctx, f, gotPod, expected)

			ginkgo.By("Verifying both volume resizes are eventually complete")

			stdout, _, err = e2epod.ExecCommandInContainerWithFullOutput(f, gotPod.Name, "c1", "df", "-m", "/cache1")
			framework.ExpectNoError(err)
			gomega.Expect(stdout).To(gomega.ContainSubstring("50"))

			stdout, _, err = e2epod.ExecCommandInContainerWithFullOutput(f, gotPod.Name, "c1", "df", "-m", "/cache2")
			framework.ExpectNoError(err)
			gomega.Expect(stdout).To(gomega.ContainSubstring("250"))

			ginkgo.By("deleting pod")
			podClient.DeleteSync(ctx, gotPod.Name, metav1.DeleteOptions{}, f.Timeouts.PodDelete)
		})
	})
})
