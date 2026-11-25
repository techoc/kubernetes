/*
Copyright 2017 The Kubernetes Authors.

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

package scheduling

import (
	"context"
	"fmt"
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
)

var (
	timeout  = 10 * time.Minute
	waitTime = 2 * time.Second

	// SIGDescribe 为测试标注 SIG 标签。
	SIGDescribe = framework.SIGDescribe("scheduling")
)

// WaitForStableCluster 等待所有现有 pod 完成调度，并返回其数量。
func WaitForStableCluster(c clientset.Interface, workerNodes sets.Set[string]) int {
	startTime := time.Now()
	// 等待所有 pod 完成调度。
	allScheduledPods, allNotScheduledPods := getScheduledAndUnscheduledPods(c, workerNodes)
	for len(allNotScheduledPods) != 0 {
		time.Sleep(waitTime)
		if startTime.Add(timeout).Before(time.Now()) {
			framework.Logf("Timed out waiting for the following pods to schedule")
			for _, p := range allNotScheduledPods {
				framework.Logf("%v/%v", p.Namespace, p.Name)
			}
			framework.Failf("Timed out after %v waiting for stable cluster.", timeout)
			break
		}
		allScheduledPods, allNotScheduledPods = getScheduledAndUnscheduledPods(c, workerNodes)
	}
	return len(allScheduledPods)
}

// getScheduledAndUnscheduledPods 列出所有 namespace 中已调度和未调度的 pod，并过滤掉成功和失败的 pod。
func getScheduledAndUnscheduledPods(c clientset.Interface, workerNodes sets.Set[string]) (scheduledPods, notScheduledPods []v1.Pod) {
	pods, err := c.CoreV1().Pods(metav1.NamespaceAll).List(context.TODO(), metav1.ListOptions{})
	framework.ExpectNoError(err, fmt.Sprintf("listing all pods in namespace %q while waiting for stable cluster", metav1.NamespaceAll))

	// API server 还会返回已成功的 Pod，需要将其过滤掉。
	filteredPods := make([]v1.Pod, 0, len(pods.Items))
	for _, p := range pods.Items {
		if !podTerminated(p) {
			filteredPods = append(filteredPods, p)
		}
	}
	pods.Items = filteredPods
	return GetPodsScheduled(workerNodes, pods)
}

func podTerminated(p v1.Pod) bool {
	return p.Status.Phase == v1.PodSucceeded || p.Status.Phase == v1.PodFailed
}
