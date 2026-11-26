/*
Copyright 2023 The Kubernetes Authors.

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
	"os"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/kubernetes/test/e2e/framework"
	e2epod "k8s.io/kubernetes/test/e2e/framework/pod"
	imageutils "k8s.io/kubernetes/test/utils/image"
	admissionapi "k8s.io/pod-security-admission/api"
)

/*
* 未知 pod 是 kubelet 不认识、但仍在容器运行时中
* 运行的 pod。如果 kubelet 检测到一个不在配置中
* （即 API-server 或 static pod 中不存在）、但在容器运行时中
* 发现正在运行的 pod，kubelet 应果断终止它。
*
* 以下情况可能出现：pod 正在运行，然后 kubelet
* 停止，在停止期间 manifest 被删除（强制删除
* API pod 或删除 static pod manifest），然后重启
* kubelet。重启后，kubelet 会通过容器运行时看到 pod 在运行，
* 但配置中不存在它，因此该 pod 成为
* "未知 pod"。kubelet 应继续终止这些未知 pod。
 */
var _ = SIGDescribe("Unknown Pods", framework.WithSerial(), framework.WithDisruptive(), func() {
	f := framework.NewDefaultFramework("unknown-pods")
	f.NamespacePodSecurityLevel = admissionapi.LevelBaseline

	ginkgo.Context("when creating a mirror pod", func() {
		var ns, podPath, staticPodName, mirrorPodName string
		ginkgo.BeforeEach(func(ctx context.Context) {
			ns = f.Namespace.Name
			staticPodName = "unknown-test-pod-" + string(uuid.NewUUID())
			mirrorPodName = staticPodName + "-" + framework.TestContext.NodeName

			podPath = kubeletCfg.StaticPodPath

			framework.Logf("create the static pod %v", staticPodName)
			err := createStaticPodWithGracePeriod(podPath, staticPodName, ns)
			framework.ExpectNoError(err)

			framework.Logf("wait for the mirror pod %v to be running", mirrorPodName)
			gomega.Eventually(ctx, func(ctx context.Context) error {
				return checkMirrorPodRunning(ctx, f.ClientSet, mirrorPodName, ns)
			}, f.Timeouts.PodStart, f.Timeouts.Poll).Should(gomega.BeNil())
		})

		ginkgo.It("the static pod should be terminated and cleaned up due to becoming a unknown pod due to being force deleted while kubelet is not running", func(ctx context.Context) {
			framework.Logf("Stopping the kubelet")
			restartKubelet := mustStopKubelet(ctx, f)

			pod, err := f.ClientSet.CoreV1().Pods(ns).Get(ctx, mirrorPodName, metav1.GetOptions{})
			framework.ExpectNoError(err)

			framework.Logf("Delete the static pod manifest while the kubelet is not running")
			file := staticPodPath(podPath, staticPodName, ns)
			framework.Logf("deleting static pod manifest %q", file)
			err = os.Remove(file)
			framework.ExpectNoError(err)

			framework.Logf("Restarting the kubelet")
			restartKubelet(ctx)

			framework.Logf("wait for the mirror pod %v to disappear", mirrorPodName)
			gomega.Eventually(ctx, func(ctx context.Context) error {
				return checkMirrorPodDisappear(ctx, f.ClientSet, mirrorPodName, ns)
			}, f.Timeouts.PodDelete, f.Timeouts.Poll).Should(gomega.BeNil())

			waitForAllContainerRemoval(ctx, pod.Name, pod.Namespace)
		})

		ginkgo.AfterEach(func(ctx context.Context) {
			framework.Logf("deleting the static pod %v", staticPodName)
			err := deleteStaticPod(podPath, staticPodName, ns)
			if !os.IsNotExist(err) {
				framework.ExpectNoError(err)
			}

			framework.Logf("wait for the mirror pod to disappear")
			gomega.Eventually(ctx, func(ctx context.Context) error {
				return checkMirrorPodDisappear(ctx, f.ClientSet, mirrorPodName, ns)
			}, f.Timeouts.PodDelete, f.Timeouts.Poll).Should(gomega.BeNil())
		})
	})

	ginkgo.Context("when creating a API pod", func() {
		var ns, podName string

		ginkgo.BeforeEach(func(ctx context.Context) {
			ns = f.Namespace.Name
			podName = "unknown-test-pause-pod-" + string(uuid.NewUUID())
			pod := &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name: podName,
				},
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{
							Name:  "pause",
							Image: imageutils.GetPauseImageName(),
						},
					},
				},
			}

			e2epod.NewPodClient(f).CreateSync(ctx, pod)
		})

		ginkgo.It("the api pod should be terminated and cleaned up due to becoming a unknown pod due to being force deleted while kubelet is not running", func(ctx context.Context) {
			framework.Logf("Stopping the kubelet")
			restartKubelet := mustStopKubelet(ctx, f)

			pod, err := f.ClientSet.CoreV1().Pods(ns).Get(ctx, podName, metav1.GetOptions{})
			framework.ExpectNoError(err)

			framework.Logf("Delete the pod while the kubelet is not running")
			// 按名称同步删除 pod 会强制删除它，将其从 kubelet 的配置中移除
			deletePodSyncByName(ctx, f, podName)

			framework.Logf("Restarting the kubelet")
			restartKubelet(ctx)

			framework.Logf("wait for the pod %v to disappear", podName)
			gomega.Eventually(ctx, func(ctx context.Context) error {
				return checkMirrorPodDisappear(ctx, f.ClientSet, podName, ns)
			}, f.Timeouts.PodDelete, f.Timeouts.Poll).Should(gomega.BeNil())

			waitForAllContainerRemoval(ctx, pod.Name, pod.Namespace)
		})
	})
})
