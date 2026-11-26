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

package auth

import (
	"context"
	"fmt"
	"time"

	"github.com/onsi/ginkgo/v2"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	e2eauth "k8s.io/kubernetes/test/e2e/auth"
	"k8s.io/kubernetes/test/e2e/framework"
	e2epod "k8s.io/kubernetes/test/e2e/framework/pod"
	"k8s.io/kubernetes/test/e2e/upgrades"
	imageutils "k8s.io/kubernetes/test/utils/image"
)

const (
	podBeforeMigrationName = "pod-before-migration"
	podAfterMigrationName  = "pod-after-migration"
)

// ServiceAccountAdmissionControllerMigrationTest 测试 pod 在集群升级前后
// 均能正常工作。
type ServiceAccountAdmissionControllerMigrationTest struct {
	pod *v1.Pod
}

// Name 返回测试的跟踪名称。
func (ServiceAccountAdmissionControllerMigrationTest) Name() string {
	return "[sig-auth] serviceaccount-admission-controller-migration"
}

// Setup 创建 pod-before-migration，它带有旧版 service account token。
func (t *ServiceAccountAdmissionControllerMigrationTest) Setup(ctx context.Context, f *framework.Framework) {
	t.pod = createPod(ctx, f, podBeforeMigrationName)
	inClusterClientMustWork(ctx, f, t.pod)
}

// Test 等待升级完成，然后验证 pod-before-migration
// 和 pod-after-migration 能用 in-cluster config 发起请求。
func (t *ServiceAccountAdmissionControllerMigrationTest) Test(ctx context.Context, f *framework.Framework, done <-chan struct{}, upgrade upgrades.UpgradeType) {
	ginkgo.By("Waiting for upgrade to finish")
	<-done

	ginkgo.By("Starting post-upgrade check")
	ginkgo.By("Checking pod-before-migration makes successful requests using in cluster config")
	podBeforeMigration, err := f.ClientSet.CoreV1().Pods(f.Namespace.Name).Get(ctx, podBeforeMigrationName, metav1.GetOptions{})
	framework.ExpectNoError(err)
	if podBeforeMigration.GetUID() != t.pod.GetUID() {
		framework.Failf("Pod %q GetUID() = %q, want %q.", podBeforeMigration.Name, podBeforeMigration.GetUID(), t.pod.GetUID())
	}
	if podBeforeMigration.Status.ContainerStatuses[0].RestartCount != 0 {
		framework.Failf("Pod %q RestartCount = %d, want 0.", podBeforeMigration.Name, podBeforeMigration.Status.ContainerStatuses[0].RestartCount)
	}
	inClusterClientMustWork(ctx, f, podBeforeMigration)

	ginkgo.By("Checking pod-after-migration makes successful requests using in cluster config")
	podAfterMigration := createPod(ctx, f, podAfterMigrationName)
	if len(podAfterMigration.Spec.Volumes) != 1 || podAfterMigration.Spec.Volumes[0].Projected == nil {
		framework.Failf("Pod %q Volumes[0].Projected.Sources = nil, want non-nil.", podAfterMigration.Name)
	}
	inClusterClientMustWork(ctx, f, podAfterMigration)

	ginkgo.By("Finishing post-upgrade check")
}

// Teardown 清理所有剩余资源。
func (t *ServiceAccountAdmissionControllerMigrationTest) Teardown(ctx context.Context, f *framework.Framework) {
	// 靠 namespace 删除来清理所有内容
}

func inClusterClientMustWork(ctx context.Context, f *framework.Framework, pod *v1.Pod) {
	var logs string
	since := time.Now()
	if err := wait.PollImmediate(15*time.Second, 5*time.Minute, func() (done bool, err error) {
		framework.Logf("Polling logs")
		logs, err = e2epod.GetPodLogsSince(ctx, f.ClientSet, pod.Namespace, pod.Name, "inclusterclient", since)
		if err != nil {
			framework.Logf("Error pulling logs: %v", err)
			return false, nil
		}
		numTokens, err := e2eauth.ParseInClusterClientLogs(logs)
		if err != nil {
			framework.Logf("Error parsing inclusterclient logs: %v", err)
			return false, fmt.Errorf("inclusterclient reported an error: %w", err)
		}
		if numTokens == 0 {
			framework.Logf("No authenticated API calls found")
			return false, nil
		}
		return true, nil
	}); err != nil {
		framework.Failf("Unexpected error: %v\n%s", err, logs)
	}
}

// createPod 创建一个 pod。
func createPod(ctx context.Context, f *framework.Framework, podName string) *v1.Pod {
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      podName,
			Namespace: f.Namespace.Name,
		},
		Spec: v1.PodSpec{
			Containers: []v1.Container{{
				Name:  "inclusterclient",
				Image: imageutils.GetE2EImage(imageutils.Agnhost),
				Args:  []string{"inclusterclient", "--poll-interval=5"},
			}},
			RestartPolicy: v1.RestartPolicyNever,
		},
	}

	createdPod, err := f.ClientSet.CoreV1().Pods(f.Namespace.Name).Create(ctx, pod, metav1.CreateOptions{})
	framework.ExpectNoError(err)
	framework.Logf("Created pod %s", podName)

	if !e2epod.CheckPodsRunningReady(ctx, f.ClientSet, f.Namespace.Name, []string{pod.Name}, time.Minute) {
		framework.Failf("Pod %q/%q never became ready", createdPod.Namespace, createdPod.Name)
	}

	return createdPod
}
