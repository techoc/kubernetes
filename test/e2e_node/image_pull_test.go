//go:build linux

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
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kubeletconfig "k8s.io/kubernetes/pkg/kubelet/apis/config"
	kubeletevents "k8s.io/kubernetes/pkg/kubelet/events"
	"k8s.io/kubernetes/pkg/kubelet/images"
	"k8s.io/kubernetes/test/e2e/feature"
	"k8s.io/kubernetes/test/e2e/framework"
	e2epod "k8s.io/kubernetes/test/e2e/framework/pod"
	"k8s.io/kubernetes/test/e2e_node/criproxy"
	imageutils "k8s.io/kubernetes/test/utils/image"
	admissionapi "k8s.io/pod-security-admission/api"
	"k8s.io/utils/ptr"
)

// CriProxy injector 用于模拟并验证 image pull 行为。
// 这些测试必须串行运行，避免其他测试缓存 image，也避免其他 image 拉长 image pull 的等待时间。
var _ = SIGDescribe("Pull Image", feature.CriProxy, framework.WithSerial(), func() {

	f := framework.NewDefaultFramework("parallel-pull-image-test")
	f.NamespacePodSecurityLevel = admissionapi.LevelPrivileged
	var testpods []*v1.Pod

	ginkgo.Context("parallel image pull with MaxParallelImagePulls=5", func() {
		tempSetCurrentKubeletConfig(f, func(ctx context.Context, initialConfig *kubeletconfig.KubeletConfiguration) {
			initialConfig.SerializeImagePulls = false
			initialConfig.MaxParallelImagePulls = ptr.To[int32](5)
		})

		ginkgo.BeforeEach(func(ctx context.Context) {
			if err := resetCRIProxyInjector(e2eCriProxy); err != nil {
				ginkgo.Skip("Skip the test since the CRI Proxy is undefined.")
			}
			ginkgo.DeferCleanup(func() error {
				return resetCRIProxyInjector(e2eCriProxy)
			})
			testpods = prepareAndCleanup(ctx, f)
			gomega.Expect(len(testpods)).To(gomega.BeNumerically("<=", 5))
		})

		ginkgo.It("should pull immediately if no more than 5 pods", func(ctx context.Context) {
			var mu sync.Mutex
			timeout := 20 * time.Second
			callCh := make(chan struct{})
			callStatus := make(map[int]chan struct{})
			err := addCRIProxyInjector(e2eCriProxy, func(apiName string) error {
				if apiName == criproxy.PullImage {
					mu.Lock()
					callID := len(callStatus)
					callStatus[callID] = callCh
					mu.Unlock()
					if callID == 0 {
						// 等待下一次调用
						select {
						case <-callCh:
							return nil
						case <-time.After(timeout):
							return fmt.Errorf("no parallel image pull after %s", timeout)
						}
					} else {
						// 通知第一次调用
						callCh <- struct{}{}
					}
				}
				return nil
			})
			framework.ExpectNoError(err)

			for _, testpod := range testpods {
				pod := e2epod.NewPodClient(f).Create(ctx, testpod)
				ginkgo.DeferCleanup(deletePodSyncByName, f, pod.Name)
			}

			imagePulled, podStartTime, podEndTime, err := getPodImagePullDurations(ctx, f, testpods)
			framework.ExpectNoError(err)

			checkPodPullingOverlap(podStartTime, podEndTime, testpods)

			for _, img := range imagePulled {
				framework.Logf("Pod pull duration including waiting is %v, and the pulled duration is %v", img.pulledIncludeWaitingDuration, img.pulledDuration)
				// 如果 pod image pull 的等待时间超过实际拉取时间 50%，说明发生了延迟拉取
				if float32(img.pulledIncludeWaitingDuration.Milliseconds())/float32(img.pulledDuration.Milliseconds()) > 1.5 {
					// 并行拉取时，等待时间应与实际拉取时间接近
					framework.Failf("There is a delayed image pulling, which is not expected for parallel image pulling.")
				}
			}
		})

	})
})

var _ = SIGDescribe("Pull Image", feature.CriProxy, framework.WithSerial(), func() {

	f := framework.NewDefaultFramework("serialize-pull-image-test")
	f.NamespacePodSecurityLevel = admissionapi.LevelPrivileged

	ginkgo.Context("serialize image pull", func() {
		// 当前默认行为
		tempSetCurrentKubeletConfig(f, func(ctx context.Context, initialConfig *kubeletconfig.KubeletConfiguration) {
			initialConfig.SerializeImagePulls = true
			initialConfig.MaxParallelImagePulls = ptr.To[int32](1)
		})

		var testpods []*v1.Pod

		ginkgo.BeforeEach(func(ctx context.Context) {
			if err := resetCRIProxyInjector(e2eCriProxy); err != nil {
				ginkgo.Skip("Skip the test since the CRI Proxy is undefined.")
			}
			ginkgo.DeferCleanup(func() error {
				return resetCRIProxyInjector(e2eCriProxy)
			})
			testpods = prepareAndCleanup(ctx, f)
			gomega.Expect(len(testpods)).To(gomega.BeNumerically("<=", 5))
		})

		ginkgo.It("should be waiting more", func(ctx context.Context) {
			// 串行 image pull 应该全部超时
			timeout := 20 * time.Second
			var mu sync.Mutex
			callCh := make(chan struct{})
			callStatus := make(map[int]chan struct{})
			err := addCRIProxyInjector(e2eCriProxy, func(apiName string) error {
				if apiName == criproxy.PullImage {
					mu.Lock()
					callID := len(callStatus)
					callStatus[callID] = callCh
					mu.Unlock()
					if callID == 0 {
						// 等待下一次调用
						select {
						case <-callCh:
							return errors.New("parallel image pull detected")
						case <-time.After(timeout):
							return nil
						}
					} else {
						// 通知第一次调用
						select {
						case callCh <- struct{}{}:
							return errors.New("parallel image pull detected")
						case <-time.After(timeout):
							return nil
						}
					}
				}
				return nil
			})
			framework.ExpectNoError(err)

			var pods []*v1.Pod
			for _, testpod := range testpods {
				pod := e2epod.NewPodClient(f).Create(ctx, testpod)
				ginkgo.DeferCleanup(deletePodSyncByName, f, pod.Name)
				pods = append(pods, pod)
			}
			for _, pod := range pods {
				err := e2epod.WaitForPodCondition(ctx, f.ClientSet, f.Namespace.Name, pod.Name, "Running", 2*time.Minute, func(pod *v1.Pod) (bool, error) {
					if pod.Status.Phase == v1.PodRunning {
						return true, nil
					}
					return false, nil
				})
				framework.ExpectNoError(err)
			}

			imagePulled, podStartTime, podEndTime, err := getPodImagePullDurations(ctx, f, testpods)
			framework.ExpectNoError(err)
			gomega.Expect(len(testpods)).To(gomega.BeComparableTo(len(imagePulled)))

			checkPodPullingOverlap(podStartTime, podEndTime, testpods)

			// 如果 pod image pull 的等待时间超过实际拉取时间 50%，说明发生了延迟拉取
			var anyDelayedPull bool
			for _, img := range imagePulled {
				framework.Logf("Pod pull duration including waiting is %v, and the pulled duration is %v", img.pulledIncludeWaitingDuration, img.pulledDuration)
				if float32(img.pulledIncludeWaitingDuration.Milliseconds())/float32(img.pulledDuration.Milliseconds()) > 1.5 {
					anyDelayedPull = true
				}
			}
			// 串行拉取时，等待时间应接近实际拉取时间的两倍
			// 用 1.5 作为通用阈值，容忍 pod 创建过程中出现的少量重叠
			if !anyDelayedPull {
				framework.Failf("All image pullings are not delayed, which is not expected for serilized image pull")
			}
		})

	})

	ginkgo.It("Image pull retry backs off on error.", func(ctx context.Context) {
		if err := resetCRIProxyInjector(e2eCriProxy); err != nil {
			ginkgo.Skip("Skip the test since the CRI Proxy is undefined.")
		}

		// 注入 PullImage 失败以触发 backoff
		expectedErr := fmt.Errorf("PullImage failed")
		err := addCRIProxyInjector(e2eCriProxy, func(apiName string) error {
			if apiName == criproxy.PullImage {
				return expectedErr
			}
			return nil
		})
		framework.ExpectNoError(err)

		pod := e2epod.NewPodClient(f).Create(ctx, newPullImageAlwaysPod())
		podErr := e2epod.WaitForPodCondition(ctx, f.ClientSet, f.Namespace.Name, pod.Name, "ImagePullBackOff", 1*time.Minute, func(pod *v1.Pod) (bool, error) {
			if len(pod.Status.ContainerStatuses) > 0 && pod.Status.Reason == images.ErrImagePullBackOff.Error() {
				return true, nil
			}
			return false, nil
		})
		gomega.Expect(podErr).To(gomega.HaveOccurred())

		eventMsg, err := getFailedToPullImageMsg(ctx, f, pod.Name)
		framework.ExpectNoError(err)
		isExpectedErrMsg := strings.Contains(eventMsg, expectedErr.Error())
		gomega.Expect(isExpectedErrMsg).To(gomega.BeTrueBecause("we injected an exception into the PullImage interface of the cri proxy"))

		// 固定等待 30 秒，让 image pull 反复 backoff
		time.Sleep(30 * time.Second)

		e, err := getImagePullAttempts(ctx, f, pod.Name)
		framework.ExpectNoError(err)
		// 理想情况下 3 次约需 10s
		gomega.Expect(e.Count).Should(gomega.BeNumerically(">=", 3))
		// 基础设施较慢时，理想情况下 7 次约需 310s
		gomega.Expect(e.Count).Should(gomega.BeNumerically("<=", 7))

	})

})

func getPodImagePullDurations(ctx context.Context, f *framework.Framework, testpods []*v1.Pod) (map[string]*pulledStruct, map[string]metav1.Time, map[string]metav1.Time, error) {
	events, err := f.ClientSet.CoreV1().Events(f.Namespace.Name).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, nil, nil, err
	}

	imagePulled := map[string]*pulledStruct{}
	podStartTime := map[string]metav1.Time{}
	podEndTime := map[string]metav1.Time{}

	for _, event := range events.Items {
		if event.Reason == kubeletevents.PulledImage {
			podEndTime[event.InvolvedObject.Name] = event.CreationTimestamp
			for _, testpod := range testpods {
				if event.InvolvedObject.Name == testpod.Name {
					pulled, err := getDurationsFromPulledEventMsg(event.Message)
					if err != nil {
						return nil, nil, nil, err
					}
					imagePulled[testpod.Name] = pulled
					break
				}
			}
		} else if event.Reason == kubeletevents.PullingImage {
			podStartTime[event.InvolvedObject.Name] = event.CreationTimestamp
		}
	}

	return imagePulled, podStartTime, podEndTime, nil
}

// pod 同时创建且 image pull 会延迟 15s，因此拉取时间应相互重叠
func checkPodPullingOverlap(podStartTime map[string]metav1.Time, podEndTime map[string]metav1.Time, testpods []*v1.Pod) {
	if podStartTime[testpods[0].Name].Time.Before(podStartTime[testpods[1].Name].Time) && podEndTime[testpods[0].Name].Time.Before(podStartTime[testpods[1].Name].Time) {
		framework.Failf("%v pulling time and %v pulling time are not overlapped", testpods[0].Name, testpods[1].Name)
	} else if podStartTime[testpods[0].Name].Time.After(podStartTime[testpods[1].Name].Time) && podStartTime[testpods[0].Name].Time.After(podEndTime[testpods[1].Name].Time) {
		framework.Failf("%v pulling time and %v pulling time are not overlapped", testpods[0].Name, testpods[1].Name)
	}
}

func prepareAndCleanup(ctx context.Context, f *framework.Framework) (testpods []*v1.Pod) {
	// cuda image 超过 2Gi，可降低 flaky 概率
	image1 := imageutils.GetE2EImage(imageutils.AgnhostPrev)
	image2 := imageutils.GetE2EImage(imageutils.Agnhost)
	node := getNodeName(ctx, f)

	testpod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "testpod",
			Namespace: f.Namespace.Name,
		},
		Spec: v1.PodSpec{
			Containers: []v1.Container{{
				Name:            "testpod",
				Image:           image1,
				ImagePullPolicy: v1.PullAlways,
			}},
			NodeName:      node,
			RestartPolicy: v1.RestartPolicyNever,
		},
	}
	testpod2 := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "testpod2",
			Namespace: f.Namespace.Name,
		},
		Spec: v1.PodSpec{
			Containers: []v1.Container{{
				Name:            "testpod2",
				Image:           image2,
				ImagePullPolicy: v1.PullAlways,
			}},
			NodeName:      node,
			RestartPolicy: v1.RestartPolicyNever,
		},
	}
	testpods = []*v1.Pod{testpod, testpod2}

	ginkgo.By("cleanup images")
	for _, pod := range testpods {
		_ = RemoveImage(ctx, pod.Spec.Containers[0].Image)
	}
	return testpods
}

type pulledStruct struct {
	pulledDuration               time.Duration
	pulledIncludeWaitingDuration time.Duration
}

// getDurationsFromPulledEventMsg 从 pulled 消息中解析两个时长
// 示例消息：`Successfully pulled image \"busybox:1.28\" in 39.356s (49.356s including waiting). Image size: 41901587 bytes.`
func getDurationsFromPulledEventMsg(msg string) (*pulledStruct, error) {
	splits := strings.Split(msg, " ")
	if len(splits) != 13 {
		return nil, fmt.Errorf("pull event message should be spilted to 13: %d", len(splits))
	}
	pulledDuration, err := time.ParseDuration(splits[5])
	if err != nil {
		return nil, err
	}
	// 跳过 '('
	pulledIncludeWaitingDuration, err := time.ParseDuration(splits[6][1:])
	if err != nil {
		return nil, err
	}
	return &pulledStruct{
		pulledDuration:               pulledDuration,
		pulledIncludeWaitingDuration: pulledIncludeWaitingDuration,
	}, nil
}

func getImagePullAttempts(ctx context.Context, f *framework.Framework, podName string) (v1.Event, error) {
	event := v1.Event{}
	e, err := f.ClientSet.CoreV1().Events(f.Namespace.Name).List(ctx, metav1.ListOptions{})
	if err != nil {
		return event, err
	}

	for _, event := range e.Items {
		if event.InvolvedObject.Name == podName && event.Reason == kubeletevents.PullingImage {
			return event, nil
		}
	}
	return event, nil
}
