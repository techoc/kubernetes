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

package drain

import (
	"fmt"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	daemonSetFatal      = "DaemonSet-managed Pods (use --ignore-daemonsets to ignore)"
	daemonSetWarning    = "ignoring DaemonSet-managed Pods"
	localStorageFatal   = "Pods with local storage (use --delete-emptydir-data to override)"
	localStorageWarning = "deleting Pods with local storage"
	unmanagedFatal      = "Pods that declare no controller (use --force to override)"
	unmanagedWarning    = "deleting Pods that declare no controller"
)

// PodDelete 告知过滤逻辑某个 pod 是否应被删除
type PodDelete struct {
	Pod    corev1.Pod
	Status PodDeleteStatus
}

// PodDeleteList 是对 []PodDelete 的包装
type PodDeleteList struct {
	items []PodDelete
}

// Pods 返回过滤后所有被标记为待删除的 pod 列表。
func (l *PodDeleteList) Pods() []corev1.Pod {
	pods := []corev1.Pod{}
	for _, i := range l.items {
		if i.Status.Delete {
			pods = append(pods, i.Pod)
		}
	}
	return pods
}

// Warnings 将所有警告消息拼接为一个字符串并返回。
func (l *PodDeleteList) Warnings() string {
	ps := make(map[string][]string)
	for _, i := range l.items {
		if i.Status.Reason == PodDeleteStatusTypeWarning {
			ps[i.Status.Message] = append(ps[i.Status.Message], fmt.Sprintf("%s/%s", i.Pod.Namespace, i.Pod.Name))
		}
	}

	msgs := []string{}
	for key, pods := range ps {
		msgs = append(msgs, fmt.Sprintf("%s: %s", key, strings.Join(pods, ", ")))
	}
	return strings.Join(msgs, "; ")
}

func (l *PodDeleteList) errors() []error {
	failedPods := make(map[string][]string)
	for _, i := range l.items {
		if i.Status.Reason == PodDeleteStatusTypeError {
			msg := i.Status.Message
			if msg == "" {
				msg = "unexpected error"
			}
			failedPods[msg] = append(failedPods[msg], fmt.Sprintf("%s/%s", i.Pod.Namespace, i.Pod.Name))
		}
	}
	errs := make([]error, 0, len(failedPods))
	for msg, pods := range failedPods {
		errs = append(errs, fmt.Errorf("cannot delete %s: %s", msg, strings.Join(pods, ", ")))
	}
	return errs
}

// PodDeleteStatus 告知过滤器某个 pod 是否应被删除
type PodDeleteStatus struct {
	Delete  bool
	Reason  string
	Message string
}

// PodFilter 接收一个 pod 并返回 PodDeleteStatus
type PodFilter func(corev1.Pod) PodDeleteStatus

const (
	// PodDeleteStatusTypeOkay 为 "Okay"
	PodDeleteStatusTypeOkay = "Okay"
	// PodDeleteStatusTypeSkip 为 "Skip"
	PodDeleteStatusTypeSkip = "Skip"
	// PodDeleteStatusTypeWarning 为 "Warning"
	PodDeleteStatusTypeWarning = "Warning"
	// PodDeleteStatusTypeError 为 "Error"
	PodDeleteStatusTypeError = "Error"
)

// MakePodDeleteStatusOkay 是返回对应 PodDeleteStatus 的辅助方法
func MakePodDeleteStatusOkay() PodDeleteStatus {
	return PodDeleteStatus{
		Delete: true,
		Reason: PodDeleteStatusTypeOkay,
	}
}

// MakePodDeleteStatusSkip 是返回对应 PodDeleteStatus 的辅助方法
func MakePodDeleteStatusSkip() PodDeleteStatus {
	return PodDeleteStatus{
		Delete: false,
		Reason: PodDeleteStatusTypeSkip,
	}
}

// MakePodDeleteStatusWithWarning 是返回对应 PodDeleteStatus 的辅助方法
func MakePodDeleteStatusWithWarning(delete bool, message string) PodDeleteStatus {
	return PodDeleteStatus{
		Delete:  delete,
		Reason:  PodDeleteStatusTypeWarning,
		Message: message,
	}
}

// MakePodDeleteStatusWithError 是返回对应 PodDeleteStatus 的辅助方法
func MakePodDeleteStatusWithError(message string) PodDeleteStatus {
	return PodDeleteStatus{
		Delete:  false,
		Reason:  PodDeleteStatusTypeError,
		Message: message,
	}
}

// 过滤器按特定顺序应用，若存在多条警告，
// 仅保留最后一个过滤器的消息。
func (d *Helper) makeFilters() []PodFilter {
	baseFilters := []PodFilter{
		d.skipDeletedFilter,
		d.daemonSetFilter,
		d.mirrorPodFilter,
		d.localStorageFilter,
		d.unreplicatedFilter,
	}
	return append(baseFilters, d.AdditionalFilters...)
}

func hasLocalStorage(pod corev1.Pod) bool {
	for _, volume := range pod.Spec.Volumes {
		if volume.EmptyDir != nil {
			return true
		}
	}

	return false
}

func isControllerRefDaemonSet(workloadRef *metav1.OwnerReference) bool {
	// 判断 workloadRef 是否为 daemonSet
	daemonSetAPIVersion, daemonSetKind := appsv1.SchemeGroupVersion.WithKind("DaemonSet").ToAPIVersionAndKind()
	return workloadRef.Kind == daemonSetKind && workloadRef.APIVersion == daemonSetAPIVersion
}

func (d *Helper) daemonSetFilter(pod corev1.Pod) PodDeleteStatus {
	// 注意，无论标志如何设置，对于由 DaemonSet 管理的 pod
	// 我们都返回 false。
	//
	// 例外情况是孤立的 pod（其引用的管理资源
	// ——包括 DaemonSet——未找到）。
	// 使用 --force 时，这类 pod 会被删除。
	controllerRef := metav1.GetControllerOf(&pod)
	if controllerRef == nil || !isControllerRefDaemonSet(controllerRef) {
		return MakePodDeleteStatusOkay()
	}
	// 任何已结束的 pod 都可以被移除。
	if pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed {
		return MakePodDeleteStatusOkay()
	}

	if _, err := d.Client.AppsV1().DaemonSets(pod.Namespace).Get(d.getContext(), controllerRef.Name, metav1.GetOptions{}); err != nil {
		// 使用 --force 时，带警告地移除孤立 pod
		if apierrors.IsNotFound(err) && d.Force {
			return MakePodDeleteStatusWithWarning(true, err.Error())
		}

		return MakePodDeleteStatusWithError(err.Error())
	}

	if !d.IgnoreAllDaemonSets {
		return MakePodDeleteStatusWithError(daemonSetFatal)
	}

	return MakePodDeleteStatusWithWarning(false, daemonSetWarning)
}

func (d *Helper) mirrorPodFilter(pod corev1.Pod) PodDeleteStatus {
	if _, found := pod.ObjectMeta.Annotations[corev1.MirrorPodAnnotationKey]; found {
		return MakePodDeleteStatusSkip()
	}
	return MakePodDeleteStatusOkay()
}

func (d *Helper) localStorageFilter(pod corev1.Pod) PodDeleteStatus {
	if !hasLocalStorage(pod) {
		return MakePodDeleteStatusOkay()
	}
	// 任何已结束的 pod 都可以被移除。
	if pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed {
		return MakePodDeleteStatusOkay()
	}
	if !d.DeleteEmptyDirData {
		return MakePodDeleteStatusWithError(localStorageFatal)
	}

	// TODO: 该警告会被后续过滤器丢弃；
	// 应考虑兼容多个警告条件，或至少
	// 保留最后一条警告消息。
	return MakePodDeleteStatusWithWarning(true, localStorageWarning)
}

func (d *Helper) unreplicatedFilter(pod corev1.Pod) PodDeleteStatus {
	// 任何已结束的 pod 都可以被移除
	if pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed {
		return MakePodDeleteStatusOkay()
	}

	controllerRef := metav1.GetControllerOf(&pod)
	if controllerRef != nil {
		return MakePodDeleteStatusOkay()
	}
	if d.Force {
		return MakePodDeleteStatusWithWarning(true, unmanagedWarning)
	}
	return MakePodDeleteStatusWithError(unmanagedFatal)
}

func shouldSkipPod(pod corev1.Pod, skipDeletedTimeoutSeconds int) bool {
	return skipDeletedTimeoutSeconds > 0 &&
		!pod.ObjectMeta.DeletionTimestamp.IsZero() &&
		int(time.Now().Sub(pod.ObjectMeta.GetDeletionTimestamp().Time).Seconds()) > skipDeletedTimeoutSeconds
}

func (d *Helper) skipDeletedFilter(pod corev1.Pod) PodDeleteStatus {
	if shouldSkipPod(pod, d.SkipWaitForDeleteTimeoutSeconds) {
		return MakePodDeleteStatusSkip()
	}
	return MakePodDeleteStatusOkay()
}
