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

package statefulset

import (
	"context"
	"fmt"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubectl/pkg/util/podutils"
	"k8s.io/kubernetes/test/e2e/framework"
)

// WaitForRunning 等待 ss 中 numPodsRunning 个 Pod 处于 Running，且前
// numPodsReady 个序号的 Pod 处于 Ready。
func WaitForRunning(ctx context.Context, c clientset.Interface, numPodsRunning, numPodsReady int32, ss *appsv1.StatefulSet) {
	pollErr := wait.PollUntilContextTimeout(ctx, StatefulSetPoll, StatefulSetTimeout, true,
		func(ctx context.Context) (bool, error) {
			podList, err := GetPodList(ctx, c, ss)
			if err != nil {
				return false, err
			}

			SortStatefulPods(podList)
			if int32(len(podList.Items)) < numPodsRunning {
				framework.Logf("Found %d stateful pods, waiting for %d", len(podList.Items), numPodsRunning)
				return false, nil
			}
			if int32(len(podList.Items)) > numPodsRunning {
				return false, fmt.Errorf("too many pods scheduled, expected %d got %d", numPodsRunning, len(podList.Items))
			}
			for _, p := range podList.Items {
				shouldBeReady := getStatefulPodOrdinal(&p) < int(numPodsReady)
				isReady := podutils.IsPodReady(&p)
				desiredReadiness := shouldBeReady == isReady
				framework.Logf("Waiting for pod %v to enter %v - Ready=%v, currently %v - Ready=%v", p.Name, v1.PodRunning, shouldBeReady, p.Status.Phase, isReady)
				if p.Status.Phase != v1.PodRunning || !desiredReadiness {
					return false, nil
				}
			}
			return true, nil
		})
	if pollErr != nil {
		framework.Failf("Failed waiting for pods to enter running: %v", pollErr)
	}
}

// WaitForState 定期轮询 ss 及其 Pod，直到 until 函数返回 true 或错误。
func WaitForState(ctx context.Context, c clientset.Interface, ss *appsv1.StatefulSet, pollInterval time.Duration, until func(*appsv1.StatefulSet, *v1.PodList) (bool, error)) {
	pollErr := wait.PollUntilContextTimeout(ctx, pollInterval, StatefulSetTimeout, true,
		func(ctx context.Context) (bool, error) {
			ssGet, err := c.AppsV1().StatefulSets(ss.Namespace).Get(ctx, ss.Name, metav1.GetOptions{})
			if err != nil {
				return false, err
			}
			podList, err := GetPodList(ctx, c, ssGet)
			if err != nil {
				return false, err
			}

			return until(ssGet, podList)
		})
	if pollErr != nil {
		framework.Failf("Failed waiting for state update: %v", pollErr)
	}
}

// WaitForRunningAndReady 等待 ss 中 numStatefulPods 个 Pod 处于 Running 和 Ready。
func WaitForRunningAndReady(ctx context.Context, c clientset.Interface, numStatefulPods int32, ss *appsv1.StatefulSet) {
	WaitForRunning(ctx, c, numStatefulPods, numStatefulPods, ss)
}

// WaitForPodReady 等待 set 中名为 podName 的 Pod 存在且具有 Ready condition。
func WaitForPodReady(ctx context.Context, c clientset.Interface, set *appsv1.StatefulSet, podName string) (*appsv1.StatefulSet, *v1.PodList) {
	var pods *v1.PodList
	WaitForState(ctx, c, set, StatefulSetPoll, func(set2 *appsv1.StatefulSet, pods2 *v1.PodList) (bool, error) {
		set = set2
		pods = pods2
		for i := range pods.Items {
			if pods.Items[i].Name == podName {
				return podutils.IsPodReady(&pods.Items[i]), nil
			}
		}
		return false, nil
	})
	return set, pods
}

// WaitForStatusReadyReplicas 等待 ss.Status.ReadyReplicas 等于 expectedReplicas
func WaitForStatusReadyReplicas(ctx context.Context, c clientset.Interface, ss *appsv1.StatefulSet, expectedReplicas int32) {
	framework.Logf("Waiting for statefulset status.readyReplicas updated to %d", expectedReplicas)

	ns, name := ss.Namespace, ss.Name
	pollErr := wait.PollUntilContextTimeout(ctx, StatefulSetPoll, StatefulSetTimeout, true,
		func(ctx context.Context) (bool, error) {
			ssGet, err := c.AppsV1().StatefulSets(ns).Get(ctx, name, metav1.GetOptions{})
			if err != nil {
				return false, err
			}
			if ssGet.Status.ObservedGeneration < ss.Generation {
				return false, nil
			}
			if ssGet.Status.ReadyReplicas != expectedReplicas {
				framework.Logf("Waiting for statefulset status.readyReplicas to become %d, currently %d", expectedReplicas, ssGet.Status.ReadyReplicas)
				return false, nil
			}
			return true, nil
		})
	if pollErr != nil {
		framework.Failf("Failed waiting for statefulset status.readyReplicas updated to %d: %v", expectedReplicas, pollErr)
	}
}

// WaitForStatusAvailableReplicas 等待 ss.Status.Available 等于 expectedReplicas
func WaitForStatusAvailableReplicas(ctx context.Context, c clientset.Interface, ss *appsv1.StatefulSet, expectedReplicas int32) {
	framework.Logf("Waiting for statefulset status.AvailableReplicas updated to %d", expectedReplicas)

	ns, name := ss.Namespace, ss.Name
	pollErr := wait.PollUntilContextTimeout(ctx, StatefulSetPoll, StatefulSetTimeout, true,
		func(ctx context.Context) (bool, error) {
			ssGet, err := c.AppsV1().StatefulSets(ns).Get(ctx, name, metav1.GetOptions{})
			if err != nil {
				return false, err
			}
			if ssGet.Status.ObservedGeneration < ss.Generation {
				return false, nil
			}
			if ssGet.Status.AvailableReplicas != expectedReplicas {
				framework.Logf("Waiting for statefulset status.AvailableReplicas to become %d, currently %d", expectedReplicas, ssGet.Status.AvailableReplicas)
				return false, nil
			}
			return true, nil
		})
	if pollErr != nil {
		framework.Failf("Failed waiting for statefulset status.AvailableReplicas updated to %d: %v", expectedReplicas, pollErr)
	}
}

// WaitForStatusReplicas 等待 ss.Status.Replicas 等于 expectedReplicas
func WaitForStatusReplicas(ctx context.Context, c clientset.Interface, ss *appsv1.StatefulSet, expectedReplicas int32) {
	framework.Logf("Waiting for statefulset status.replicas updated to %d", expectedReplicas)

	ns, name := ss.Namespace, ss.Name
	pollErr := wait.PollUntilContextTimeout(ctx, StatefulSetPoll, StatefulSetTimeout, true,
		func(ctx context.Context) (bool, error) {
			ssGet, err := c.AppsV1().StatefulSets(ns).Get(ctx, name, metav1.GetOptions{})
			if err != nil {
				return false, err
			}
			if ssGet.Status.ObservedGeneration < ss.Generation {
				return false, nil
			}
			if ssGet.Status.Replicas != expectedReplicas {
				framework.Logf("Waiting for statefulset status.replicas to become %d, currently %d", expectedReplicas, ssGet.Status.Replicas)
				return false, nil
			}
			return true, nil
		})
	if pollErr != nil {
		framework.Failf("Failed waiting for statefulset status.replicas updated to %d: %v", expectedReplicas, pollErr)
	}
}

// Saturate 等待 ss 中所有 Pod 进入 Running 和 Ready。
func Saturate(ctx context.Context, c clientset.Interface, ss *appsv1.StatefulSet) {
	var i int32
	for i = 0; i < *(ss.Spec.Replicas); i++ {
		framework.Logf("Waiting for stateful pod at index %v to enter Running", i)
		WaitForRunning(ctx, c, i+1, i, ss)
		framework.Logf("Resuming stateful pod at index %v", i)
		ResumeNextPod(ctx, c, ss)
	}
}

// WaitForFailedWithImagePullErr 等待 ss 中 numPodsFailed 个 Pod 因 ImagePullBackError 或 ImagePullErr 进入 Failed。
func WaitForFailedWithImagePullErr(ctx context.Context, c clientset.Interface, numPodsFailed int32, ss *appsv1.StatefulSet) {
	pollErr := wait.PollUntilContextTimeout(ctx, StatefulSetPoll, StatefulSetTimeout, true,
		func(ctx context.Context) (bool, error) {
			podList, err := GetPodList(ctx, c, ss)
			if err != nil {
				return false, err
			}

			SortStatefulPods(podList)
			if int32(len(podList.Items)) < numPodsFailed {
				framework.Logf("Found %d stateful pods, waiting for %d", len(podList.Items), numPodsFailed)
				return false, nil
			}
			if int32(len(podList.Items)) > numPodsFailed {
				return false, fmt.Errorf("too many pods scheduled, expected %d got %d", numPodsFailed, len(podList.Items))
			}
			for _, p := range podList.Items {
				isFailingWithImagePullErr := false
				shouldBeFailing := getStatefulPodOrdinal(&p) < int(numPodsFailed)
				for _, containerStatus := range p.Status.ContainerStatuses {
					w := containerStatus.State.Waiting
					if w != nil && (w.Reason == "ImagePullBackOff" || w.Reason == "ErrImagePull") {
						isFailingWithImagePullErr = true
					}
				}
				desiredFailing := shouldBeFailing == isFailingWithImagePullErr
				framework.Logf("Waiting for pod %v to be stuck with image pull error - shouldFail=%v, phase=%v, imagePullErr=%v", p.Name, shouldBeFailing, p.Status.Phase, isFailingWithImagePullErr)
				if p.Status.Phase != v1.PodPending || !desiredFailing {
					return false, nil
				}
			}
			return true, nil
		})
	if pollErr != nil {
		framework.Failf("Failed waiting for %d pods to be stuck with image pull error: %v", numPodsFailed, pollErr)
	}
}
