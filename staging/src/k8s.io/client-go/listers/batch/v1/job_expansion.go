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

package v1

import (
	"fmt"

	batch "k8s.io/api/batch/v1"
	"k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
)

// JobListerExpansion 允许向 JobLister 添加自定义方法。
// 手写扩展接口，属于生成 lister 的一部分。
type JobListerExpansion interface {
	// GetPodJobs returns a list of Jobs that potentially
	// match a Pod. Only the one specified in the Pod's ControllerRef
	// will actually manage it.
	// Returns an error only if no matching Jobs are found.
	GetPodJobs(pod *v1.Pod) (jobs []batch.Job, err error)
}

// GetPodJobs 返回可能与某个 Pod 匹配的 Job 列表，真正管理该 Pod 的只有 Pod 的
// ControllerRef 指定的那一个；只有在找不到匹配的 Job 时才返回错误。
// 返回值为 Job 的副本而非指针，避免调用方意外修改缓存中的对象。
// 与 DaemonSet 版本不同，这里不显式排除空 selector：LabelSelectorAsSelector(nil)
// 会得到 Nothing，天然满足“空 selector 不匹配任何对象”。
func (l *jobLister) GetPodJobs(pod *v1.Pod) (jobs []batch.Job, err error) {
	if len(pod.Labels) == 0 {
		err = fmt.Errorf("no jobs found for pod %v because it has no labels", pod.Name)
		return
	}

	// 中文：在 Pod 所在命名空间内列举 Job，再按 selector 过滤。
	var list []*batch.Job
	list, err = l.Jobs(pod.Namespace).List(labels.Everything())
	if err != nil {
		return
	}
	for _, job := range list {
		// 中文：注意这里用 := 遮蔽了外层命名返回值 err；非法 selector 仅跳过该 Job，
		// 不会污染最终返回的 error。
		selector, err := metav1.LabelSelectorAsSelector(job.Spec.Selector)
		if err != nil {
			// This object has an invalid selector, it does not match the pod
			continue
		}
		if !selector.Matches(labels.Set(pod.Labels)) {
			continue
		}
		jobs = append(jobs, *job)
	}
	if len(jobs) == 0 {
		err = fmt.Errorf("could not find jobs for pod %s in namespace %s with labels: %v", pod.Name, pod.Namespace, pod.Labels)
	}
	return
}

// JobNamespaceListerExpansion 允许向 JobNamespaceLister 添加自定义方法。
// 当前无命名空间级扩展方法，仅作为预留钩子。
type JobNamespaceListerExpansion interface{}
