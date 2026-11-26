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

package job

import (
	"context"

	batchv1 "k8s.io/api/batch/v1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	clientset "k8s.io/client-go/kubernetes"
)

// GetJob 用 c 获取命名空间 ns 中名为 name 的 Job。若返回 error 为 nil，返回的 Job 有效。
func GetJob(ctx context.Context, c clientset.Interface, ns, name string) (*batchv1.Job, error) {
	return c.BatchV1().Jobs(ns).Get(ctx, name, metav1.GetOptions{})
}

// GetAllRunningJobPods 返回属于某个 Job 的所有运行中 Pod 列表。
func GetAllRunningJobPods(ctx context.Context, c clientset.Interface, ns, jobName string) ([]v1.Pod, error) {
	if podList, err := GetJobPods(ctx, c, ns, jobName); err != nil {
		return nil, err
	} else {
		pods := []v1.Pod{}
		for _, pod := range podList.Items {
			if pod.Status.Phase == v1.PodRunning {
				pods = append(pods, pod)
			}
		}
		return pods, nil
	}
}

// GetJobPods 返回属于某个 Job 的 Pod 列表。
func GetJobPods(ctx context.Context, c clientset.Interface, ns, jobName string) (*v1.PodList, error) {
	label := labels.SelectorFromSet(labels.Set(map[string]string{JobSelectorKey: jobName}))
	options := metav1.ListOptions{LabelSelector: label.String()}
	return c.CoreV1().Pods(ns).List(ctx, options)
}

// CreateJob 用 c 在命名空间 ns 中创建 job。若返回 error 为 nil，返回的 Job 有效且
// 已创建。
func CreateJob(ctx context.Context, c clientset.Interface, ns string, job *batchv1.Job) (*batchv1.Job, error) {
	return c.BatchV1().Jobs(ns).Create(ctx, job, metav1.CreateOptions{})
}

// UpdateJob 用 c 更新命名空间 ns 中的 job。若返回 error 为
// nil，返回的 Job 有效且已更新。
func UpdateJob(ctx context.Context, c clientset.Interface, ns string, job *batchv1.Job) (*batchv1.Job, error) {
	return c.BatchV1().Jobs(ns).Update(ctx, job, metav1.UpdateOptions{})
}
