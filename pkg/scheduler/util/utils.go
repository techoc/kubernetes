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

// Package util 提供调度器使用的一组通用工具函数。
package util

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	v1 "k8s.io/api/core/v1"
	resourceapi "k8s.io/api/resource/v1"
	schedulingv1alpha3 "k8s.io/api/scheduling/v1alpha3"
	schedulingv1beta1 "k8s.io/api/scheduling/v1beta1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/net"
	"k8s.io/apimachinery/pkg/util/strategicpatch"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/retry"
	corev1helpers "k8s.io/component-helpers/scheduling/corev1"
	"k8s.io/klog/v2"
	extenderv1 "k8s.io/kube-scheduler/extender/v1"
	v1helper "k8s.io/kubernetes/pkg/apis/core/v1/helper"
)

// GetPodFullName 返回唯一标识一个 Pod 的名称。
func GetPodFullName(pod *v1.Pod) string {
	// 用下划线作分隔符：Pod 名称是 DNS 子域格式，不允许出现下划线，因而不会与名称本身混淆。
	return pod.Name + "_" + pod.Namespace
}

// GetPodStartTime 返回给定 Pod 的启动时间；尚未启动时返回当前时间戳。
func GetPodStartTime(pod *v1.Pod) *metav1.Time {
	if pod.Status.StartTime != nil {
		return pod.Status.StartTime
	}
	// 已被假定（assumed）或已绑定但尚未启动的 Pod 还没有 StartTime。
	return &metav1.Time{Time: time.Now()}
}

// GetEarliestPodStartTime 返回所有受害者中优先级最高的那些 Pod 里最早的启动时间。
func GetEarliestPodStartTime(victims *extenderv1.Victims) *metav1.Time {
	if len(victims.Pods) == 0 {
		// 理论上不会走到这里。
		klog.Background().Error(nil, "victims.Pods is empty. Should not reach here")
		return nil
	}

	earliestPodStartTime := GetPodStartTime(victims.Pods[0])
	maxPriority := corev1helpers.PodPriority(victims.Pods[0])

	for _, pod := range victims.Pods {
		// 遇到更高优先级的 Pod 时，之前记录的启动时间不再有意义，直接以它重置。
		if podPriority := corev1helpers.PodPriority(pod); podPriority == maxPriority {
			if podStartTime := GetPodStartTime(pod); podStartTime.Before(earliestPodStartTime) {
				earliestPodStartTime = podStartTime
			}
		} else if podPriority > maxPriority {
			maxPriority = podPriority
			earliestPodStartTime = GetPodStartTime(pod)
		}
	}

	return earliestPodStartTime
}

// MoreImportantPod 在第一个 Pod 优先级更高时返回 true。
// 优先级相同时比较启动时间，更早启动的 Pod 视为更重要。
func MoreImportantPod(pod1, pod2 *v1.Pod) bool {
	p1 := corev1helpers.PodPriority(pod1)
	p2 := corev1helpers.PodPriority(pod2)
	if p1 != p2 {
		return p1 > p2
	}
	return GetPodStartTime(pod1).Before(GetPodStartTime(pod2))
}

// Retriable 判断错误是否为调度周期内可重试的错误。
func Retriable(err error) bool {
	return apierrors.IsInternalError(err) || apierrors.IsServiceUnavailable(err) ||
		net.IsConnectionRefused(err)
}

// RetriableWithConflict 判断错误是否为调度周期内可重试的错误（包括冲突）。
func RetriableWithConflict(err error) bool {
	return Retriable(err) || apierrors.IsConflict(err)
}

// BindPod 带重试地将 Pod 绑定到节点。
func BindPod(ctx context.Context, cs kubernetes.Interface, binding *v1.Binding) error {
	bindFn := func() error {
		return cs.CoreV1().Pods(binding.Namespace).Bind(ctx, binding, metav1.CreateOptions{})
	}

	return retry.OnError(retry.DefaultBackoff, Retriable, bindFn)
}

// PatchPodStatus 计算从 old.Status 到 newStatus 的差异字节，
// 然后向 API server 提交请求以 patch Pod 的变更。
func PatchPodStatus(ctx context.Context, cs kubernetes.Interface, name string, namespace string, oldStatus *v1.PodStatus, newStatus *v1.PodStatus) error {
	if newStatus == nil {
		return nil
	}

	if oldStatus == nil {
		oldStatus = &v1.PodStatus{}
	}

	oldData, err := json.Marshal(v1.Pod{Status: *oldStatus})
	if err != nil {
		return err
	}

	newData, err := json.Marshal(v1.Pod{Status: *newStatus})
	if err != nil {
		return err
	}
	patchBytes, err := strategicpatch.CreateTwoWayMergePatch(oldData, newData, &v1.Pod{})
	if err != nil {
		return fmt.Errorf("failed to create merge patch for pod %q/%q: %w", namespace, name, err)
	}

	// 差异为空时不发起无意义的 API 请求。
	if "{}" == string(patchBytes) {
		return nil
	}

	patchFn := func() error {
		// 只 patch status 子资源，避免覆盖 Pod 的其他字段。
		_, err := cs.CoreV1().Pods(namespace).Patch(ctx, name, types.StrategicMergePatchType, patchBytes, metav1.PatchOptions{}, "status")
		return err
	}

	return retry.OnError(retry.DefaultBackoff, RetriableWithConflict, patchFn)
}

// PatchPodGroupStatus 计算从 old.Status 到 newStatus 的差异字节，
// 然后向 API server 提交请求以 patch PodGroup 状态的变更。
func PatchPodGroupStatus(ctx context.Context, cs kubernetes.Interface, name string,
	namespace string, oldStatus *schedulingv1beta1.PodGroupStatus,
	newStatus *schedulingv1beta1.PodGroupStatus) error {
	if newStatus == nil {
		return nil
	}

	if oldStatus == nil {
		oldStatus = &schedulingv1beta1.PodGroupStatus{}
	}

	oldData, err := json.Marshal(schedulingv1beta1.PodGroup{Status: *oldStatus})
	if err != nil {
		return err
	}

	newData, err := json.Marshal(schedulingv1beta1.PodGroup{Status: *newStatus})
	if err != nil {
		return err
	}
	patchBytes, err := strategicpatch.CreateTwoWayMergePatch(oldData, newData, &schedulingv1beta1.PodGroup{})
	if err != nil {
		return fmt.Errorf("failed to create merge patch for podgroup %q/%q: %w", namespace, name, err)
	}

	// 差异为空时不发起无意义的 API 请求。
	if string(patchBytes) == "{}" {
		return nil
	}

	patchFn := func() error {
		// 只 patch status 子资源，避免覆盖 PodGroup 的其他字段。
		_, err := cs.SchedulingV1beta1().PodGroups(namespace).Patch(ctx, name, types.StrategicMergePatchType, patchBytes, metav1.PatchOptions{}, "status")
		return err
	}

	return retry.OnError(retry.DefaultBackoff, RetriableWithConflict, patchFn)
}

// DeletePod 从 API server 删除给定的 Pod。
func DeletePod(ctx context.Context, cs kubernetes.Interface, pod *v1.Pod) error {
	return cs.CoreV1().Pods(pod.Namespace).Delete(ctx, pod.Name, metav1.DeleteOptions{})
}

// IsScalarResourceName 校验资源是否为扩展资源、大页、原生或可挂载卷资源。
func IsScalarResourceName(name v1.ResourceName) bool {
	return v1helper.IsExtendedResourceName(name) || v1helper.IsHugePageResourceName(name) ||
		v1helper.IsPrefixedNativeResource(name) || v1helper.IsAttachableVolumeResourceName(name)
}

// IsDRAExtendedResourceName 在名称是扩展资源名，或是由设备类名派生的隐式扩展资源名
// （格式为 deviceclass.resource.kubernetes.io/<device class name>）时返回 true。
func IsDRAExtendedResourceName(name v1.ResourceName) bool {
	return v1helper.IsExtendedResourceName(name) || strings.HasPrefix(string(name), resourceapi.ResourceDeviceClassPrefix)
}

// As 把两个对象转换为给定类型。
// 两个对象必须是同一类型，否则返回错误。
// 允许传入 nil，转换结果仍为 nil。
// 对 oldObj 会特殊处理 cache.DeletedFinalStateUnknown，转换其中保存的对象。
func As[T any](oldObj, newobj interface{}) (T, T, error) {
	var oldTyped T
	var newTyped T
	var ok bool
	if newobj != nil {
		newTyped, ok = newobj.(T)
		if !ok {
			return oldTyped, newTyped, fmt.Errorf("expected %T, but got %T", newTyped, newobj)
		}
	}

	if oldObj != nil {
		// informer 的删除事件可能带上 DeletedFinalStateUnknown 包装，需要拆出真实对象再转换。
		if realOldObj, ok := oldObj.(cache.DeletedFinalStateUnknown); ok {
			oldObj = realOldObj.Obj
		}
		oldTyped, ok = oldObj.(T)
		if !ok {
			return oldTyped, newTyped, fmt.Errorf("expected %T, but got %T", oldTyped, oldObj)
		}
	}
	return oldTyped, newTyped, nil
}

// GetHostPorts 返回 Pod 的容器以及 restartPolicy: Always 的 initContainer 占用的主机端口。
func GetHostPorts(pod *v1.Pod) []v1.ContainerPort {
	var ports []v1.ContainerPort
	if pod == nil {
		return ports
	}

	hostPort := func(p v1.ContainerPort) bool {
		return p.HostPort > 0
	}

	for _, c := range pod.Spec.InitContainers {
		// 只考虑会在 Pod 整个生命周期内持续运行的 initContainer。
		if c.RestartPolicy == nil || *c.RestartPolicy != v1.ContainerRestartPolicyAlways {
			continue
		}
		for _, p := range c.Ports {
			if !hostPort(p) {
				continue
			}
			ports = append(ports, p)
		}
	}
	for _, c := range pod.Spec.Containers {
		for _, p := range c.Ports {
			if !hostPort(p) {
				continue
			}
			ports = append(ports, p)
		}
	}
	return ports
}

// PodGroupPriority 返回给定 PodGroup 的优先级。
func PodGroupPriority(pg *schedulingv1beta1.PodGroup) int32 {
	if pg.Spec.Priority != nil {
		return *pg.Spec.Priority
	}
	// priority 为 nil 说明该 PodGroup 创建时尚无全局默认 PriorityClass，
	// 且 priorityClassName 为空，因此回退到静态默认优先级 0。
	return 0
}

// CompositePodGroupPriority 返回给定 CompositePodGroup 的优先级。
func CompositePodGroupPriority(cpg *schedulingv1alpha3.CompositePodGroup) int32 {
	if cpg.Spec.Priority != nil {
		return *cpg.Spec.Priority
	}
	// priority 为 nil 说明该 CompositePodGroup 创建时尚无全局默认 PriorityClass，
	// 且 priorityClassName 为空，因此回退到静态默认优先级 0。
	return 0
}
