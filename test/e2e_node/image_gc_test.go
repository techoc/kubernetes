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

package e2enode

import (
	"context"
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	internalapi "k8s.io/cri-api/pkg/apis"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
	kubeletconfig "k8s.io/kubernetes/pkg/kubelet/apis/config"
	"k8s.io/kubernetes/test/e2e/framework"
	e2epod "k8s.io/kubernetes/test/e2e/framework/pod"
	admissionapi "k8s.io/pod-security-admission/api"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

const (
	// kubelet 的 GC 每 5 分钟执行一次，这里多留一点余量。
	checkGCUntil time.Duration = 6 * time.Minute
	checkGCFreq  time.Duration = 30 * time.Second
)

var _ = SIGDescribe("ImageGarbageCollect", framework.WithSerial(), framework.WithNodeConformance(), func() {
	f := framework.NewDefaultFramework("image-garbage-collect-test")
	f.NamespacePodSecurityLevel = admissionapi.LevelPrivileged
	var is internalapi.ImageManagerService
	ginkgo.BeforeEach(func(ctx context.Context) {
		var err error
		_, is, err = getCRIClient(ctx)
		framework.ExpectNoError(err)
	})
	ginkgo.AfterEach(func(ctx context.Context) {
		framework.ExpectNoError(PrePullAllImages(ctx))
	})
	ginkgo.Context("when ImageMaximumGCAge is set", func() {
		tempSetCurrentKubeletConfig(f, func(ctx context.Context, initialConfig *kubeletconfig.KubeletConfiguration) {
			initialConfig.ImageMaximumGCAge = metav1.Duration{Duration: time.Duration(time.Minute * 1)}
			initialConfig.ImageMinimumGCAge = metav1.Duration{Duration: time.Duration(time.Second * 1)}
		})
		ginkgo.It("should GC unused images", func(ctx context.Context) {
			pod := innocentPod()
			e2epod.NewPodClient(f).CreateBatch(ctx, []*v1.Pod{pod})

			_, err := is.PullImage(context.Background(), &runtimeapi.ImageSpec{Image: agnhostImage}, nil, nil)
			framework.ExpectNoError(err)

			allImages, err := is.ListImages(context.Background(), &runtimeapi.ImageFilter{})
			framework.ExpectNoError(err)

			e2epod.NewPodClient(f).DeleteSync(ctx, pod.ObjectMeta.Name, metav1.DeleteOptions{}, f.Timeouts.PodDelete)

			// 即使镜像 GC 的最长时限较短，实际仍受 kubelet 的 ImageGCPeriod 限制，
			// 该值硬编码为 5 分钟。
			gomega.Eventually(ctx, func() int {
				gcdImageList, err := is.ListImages(context.Background(), &runtimeapi.ImageFilter{})
				framework.ExpectNoError(err)
				return len(gcdImageList)
			}, checkGCUntil, checkGCFreq).Should(gomega.BeNumerically("<", len(allImages)))
		})
		ginkgo.It("should not GC unused images prematurely", func(ctx context.Context) {
			pod := innocentPod()
			e2epod.NewPodClient(f).CreateBatch(ctx, []*v1.Pod{pod})

			_, err := is.PullImage(context.Background(), &runtimeapi.ImageSpec{Image: agnhostImage}, nil, nil)
			framework.ExpectNoError(err)

			allImages, err := is.ListImages(context.Background(), &runtimeapi.ImageFilter{})
			framework.ExpectNoError(err)

			e2epod.NewPodClient(f).DeleteSync(ctx, pod.ObjectMeta.Name, metav1.DeleteOptions{}, f.Timeouts.PodDelete)

			restartKubelet(ctx, true)

			// kubelet 重启后等待镜像达到 maxAge，确认不会过早 GC。
			gomega.Consistently(ctx, func() int {
				gcdImageList, err := is.ListImages(context.Background(), &runtimeapi.ImageFilter{})
				framework.ExpectNoError(err)
				return len(gcdImageList)
			}, 50*time.Second, 10*time.Second).Should(gomega.Equal(len(allImages)))

			// 即使镜像 GC 的最长时限较短，实际仍受 kubelet 的 ImageGCPeriod 限制，
			// 该值硬编码为 5 分钟。
			gomega.Eventually(ctx, func() int {
				gcdImageList, err := is.ListImages(context.Background(), &runtimeapi.ImageFilter{})
				framework.ExpectNoError(err)
				return len(gcdImageList)
			}, checkGCUntil, checkGCFreq).Should(gomega.BeNumerically("<", len(allImages)))
		})
	})
})
