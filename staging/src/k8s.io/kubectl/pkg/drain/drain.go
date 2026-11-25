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
	"context"
	"fmt"
	"io"
	"math"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	policyv1beta1 "k8s.io/api/policy/v1beta1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/cli-runtime/pkg/resource"
	"k8s.io/client-go/kubernetes"
	cmdutil "k8s.io/kubectl/pkg/cmd/util"
)

const (
	// EvictionKind 表示 evictions 对象的 kind
	EvictionKind = "Eviction"
	// EvictionSubresource 表示作为 pod 子资源的 evictions 对象的 kind
	EvictionSubresource = "pods/eviction"
	podSkipMsgTemplate  = "pod %q has DeletionTimestamp older than %v seconds, skipping\n"
)

// Helper 包含控制 drainer 行为的参数
type Helper struct {
	Ctx    context.Context
	Client kubernetes.Interface
	Force  bool

	// GracePeriodSeconds 是等待 pod 终止的时长。
	// 重要：0 表示"立即删除"；设置为负值
	// 则使用 pod 的 terminationGracePeriodSeconds。
	GracePeriodSeconds int

	IgnoreAllDaemonSets bool
	Timeout             time.Duration
	DeleteEmptyDirData  bool
	Selector            string
	PodSelector         string
	ChunkSize           int64

	// DisableEviction 强制 drain 使用 delete 而非 evict
	DisableEviction bool

	// SkipWaitForDeleteTimeoutSeconds 忽略具有较大
	// DeletionTimeStamp（超过 N 秒）的 pod。何时适用该选项
	// 由用户自行判断；例如 Node 未就绪且
	// 这些 pod 无法通过其他方式 drain。
	SkipWaitForDeleteTimeoutSeconds int

	// EvictErrorRetryDelay 用于控制 pod eviction 出错后的重试延迟
	EvictErrorRetryDelay time.Duration

	// AdditionalFilters 在基础 drain 过滤器之后依次应用，
	// 使用自定义逻辑排除 pod。任何返回 Delete == false 的 PodDeleteStatus
	// 的过滤器都会立即中止后续过滤器的执行。
	AdditionalFilters []PodFilter

	Out    io.Writer
	ErrOut io.Writer

	DryRunStrategy cmdutil.DryRunStrategy

	// OnPodDeletedOrEvicted 在 pod 被 evict/delete 时调用，用于输出进度信息
	// Deprecated: use OnPodDeletionOrEvictionFinished instead
	OnPodDeletedOrEvicted func(pod *corev1.Pod, usingEviction bool)

	// OnPodDeletionOrEvictionFinished 在 pod eviction/deletion 失败时调用，用于输出进度信息
	OnPodDeletionOrEvictionFinished func(pod *corev1.Pod, usingEviction bool, err error)

	// OnPodDeletionOrEvictionStarted 在 pod eviction/deletion 开始时调用，用于输出进度信息
	OnPodDeletionOrEvictionStarted func(pod *corev1.Pod, usingEviction bool)
}

type waitForDeleteParams struct {
	ctx                             context.Context
	pods                            []corev1.Pod
	interval                        time.Duration
	timeout                         time.Duration
	usingEviction                   bool
	getPodFn                        func(string, string) (*corev1.Pod, error)
	onDoneFn                        func(pod *corev1.Pod, usingEviction bool)
	onFinishFn                      func(pod *corev1.Pod, usingEviction bool, err error)
	globalTimeout                   time.Duration
	skipWaitForDeleteTimeoutSeconds int
	out                             io.Writer
}

// CheckEvictionSupport 使用 Discovery API 查询服务端是否支持
// eviction 子资源。若支持，则返回其 groupVersion；否则
// 返回空的 GroupVersion。
func CheckEvictionSupport(clientset kubernetes.Interface) (schema.GroupVersion, error) {
	discoveryClient := clientset.Discovery()

	// 自 v1.8.0 起子资源中提供版本信息，参见 https://github.com/kubernetes/kubernetes/pull/49971
	resourceList, err := discoveryClient.ServerResourcesForGroupVersion("v1")
	if err != nil {
		return schema.GroupVersion{}, err
	}
	for _, resource := range resourceList.APIResources {
		if resource.Name == EvictionSubresource && resource.Kind == EvictionKind && len(resource.Group) > 0 && len(resource.Version) > 0 {
			return schema.GroupVersion{Group: resource.Group, Version: resource.Version}, nil
		}
	}
	return schema.GroupVersion{}, nil
}

func (d *Helper) makeDeleteOptions() metav1.DeleteOptions {
	deleteOptions := metav1.DeleteOptions{}
	if d.GracePeriodSeconds >= 0 {
		gracePeriodSeconds := int64(d.GracePeriodSeconds)
		deleteOptions.GracePeriodSeconds = &gracePeriodSeconds
	}
	if d.DryRunStrategy == cmdutil.DryRunServer {
		deleteOptions.DryRun = []string{metav1.DryRunAll}
	}
	return deleteOptions
}

// DeletePod 删除给定的 pod，若删除失败则返回错误
func (d *Helper) DeletePod(pod corev1.Pod) error {
	return d.Client.CoreV1().Pods(pod.Namespace).Delete(d.getContext(), pod.Name, d.makeDeleteOptions())
}

// EvictPod evict 给定的 pod，若失败则返回错误
func (d *Helper) EvictPod(pod corev1.Pod, evictionGroupVersion schema.GroupVersion) error {
	delOpts := d.makeDeleteOptions()

	switch evictionGroupVersion {
	case policyv1.SchemeGroupVersion:
		// 服务端支持时发送 policy/v1
		eviction := &policyv1.Eviction{
			ObjectMeta: metav1.ObjectMeta{
				Name:      pod.Name,
				Namespace: pod.Namespace,
			},
			DeleteOptions: &delOpts,
		}
		return d.Client.PolicyV1().Evictions(eviction.Namespace).Evict(d.getContext(), eviction)

	default:
		// 否则回退到 policy/v1beta1，所有支持 eviction 子资源的服务端都支持该版本
		eviction := &policyv1beta1.Eviction{
			ObjectMeta: metav1.ObjectMeta{
				Name:      pod.Name,
				Namespace: pod.Namespace,
			},
			DeleteOptions: &delOpts,
		}
		return d.Client.PolicyV1beta1().Evictions(eviction.Namespace).Evict(d.getContext(), eviction)
	}
}

// GetPodsForDeletion 接收节点的资源信息，以 PodDeleteList 形式返回该节点上的 pod，
// 若无法列出 pod 则返回错误。可通过 .Pods() 获取所有可删除的 pod，
// 通过 .Warnings() 获取包含全部警告的字符串，
// 通过 .Errors() 获取删除过程中发生的所有错误。
func (d *Helper) GetPodsForDeletion(nodeName string) (*PodDeleteList, []error) {
	labelSelector, err := labels.Parse(d.PodSelector)
	if err != nil {
		return nil, []error{err}
	}

	podList := &corev1.PodList{}
	initialOpts := &metav1.ListOptions{
		LabelSelector: labelSelector.String(),
		FieldSelector: fields.SelectorFromSet(fields.Set{"spec.nodeName": nodeName}).String(),
		Limit:         d.ChunkSize,
	}

	err = resource.FollowContinue(initialOpts, func(options metav1.ListOptions) (runtime.Object, error) {
		newPods, err := d.Client.CoreV1().Pods(metav1.NamespaceAll).List(d.getContext(), options)
		if err != nil {
			podR := corev1.SchemeGroupVersion.WithResource(corev1.ResourcePods.String())
			return nil, resource.EnhanceListError(err, options, podR.String())
		}
		podList.Items = append(podList.Items, newPods.Items...)
		return newPods, nil
	})

	if err != nil {
		return nil, []error{err}
	}

	list := filterPods(podList, d.makeFilters())
	if errs := list.errors(); len(errs) > 0 {
		return list, errs
	}

	return list, nil
}

func filterPods(podList *corev1.PodList, filters []PodFilter) *PodDeleteList {
	pods := []PodDelete{}
	for _, pod := range podList.Items {
		var status PodDeleteStatus
		for _, filter := range filters {
			status = filter(pod)
			if !status.Delete {
				// pod 被过滤掉后立即短路
				// 此时没有必要再让该 pod
				// 经过任何后续过滤器
				break
			}
		}
		// 无论 PodDeleteStatus 是什么，都将 pod 加入 PodDeleteList，
		// 像 DaemonSet 这类 PodDeleteStatus 为 false 的 pod
		// 会由 list.errors() 捕获
		pod.Kind = "Pod"
		pod.APIVersion = "v1"
		pods = append(pods, PodDelete{
			Pod:    pod,
			Status: status,
		})
	}
	list := &PodDeleteList{items: pods}
	return list
}

// DeleteOrEvictPods 删除或 evict API server 上的 pod
func (d *Helper) DeleteOrEvictPods(pods []corev1.Pod) error {
	if len(pods) == 0 {
		return nil
	}

	// TODO(justinsb): 是否多余？
	getPodFn := func(namespace, name string) (*corev1.Pod, error) {
		return d.Client.CoreV1().Pods(namespace).Get(d.getContext(), name, metav1.GetOptions{})
	}

	if !d.DisableEviction {
		evictionGroupVersion, err := CheckEvictionSupport(d.Client)
		if err != nil {
			return err
		}

		if !evictionGroupVersion.Empty() {
			return d.evictPods(pods, evictionGroupVersion, getPodFn)
		}
	}

	return d.deletePods(pods, getPodFn)
}

// syncWriter 使用互斥锁将并发的 Write 调用串行化。
type syncWriter struct {
	mu sync.Mutex
	w  io.Writer
}

func (s *syncWriter) Write(p []byte) (n int, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.w.Write(p)
}

func (d *Helper) evictPods(pods []corev1.Pod, evictionGroupVersion schema.GroupVersion, getPodFn func(namespace, name string) (*corev1.Pod, error)) error {
	returnCh := make(chan error, 1)
	out := io.Writer(&syncWriter{w: d.Out})
	errOut := io.Writer(&syncWriter{w: d.ErrOut})
	// 超时时间为 0 表示无限等待，这里用 MaxInt64 表示。
	var globalTimeout time.Duration
	if d.Timeout == 0 {
		globalTimeout = time.Duration(math.MaxInt64)
	} else {
		globalTimeout = d.Timeout
	}
	ctx, cancel := context.WithTimeout(d.getContext(), globalTimeout)
	defer cancel()
	for _, pod := range pods {
		go func(pod corev1.Pod, returnCh chan error) {
			activePod := pod
			for {
				switch d.DryRunStrategy {
				case cmdutil.DryRunServer:
					//nolint:errcheck
					fmt.Fprintf(out, "evicting pod %s/%s (server dry run)\n", activePod.Namespace, activePod.Name)
				default:
					if d.OnPodDeletionOrEvictionStarted != nil {
						d.OnPodDeletionOrEvictionStarted(&activePod, true)
					}
					//nolint:errcheck
					fmt.Fprintf(out, "evicting pod %s/%s\n", activePod.Namespace, activePod.Name)
				}
				select {
				case <-ctx.Done():
					// 必须在此返回，否则会泄漏 goroutine。
					returnCh <- fmt.Errorf("error when evicting pods/%q -n %q: global timeout reached: %v", activePod.Name, activePod.Namespace, globalTimeout)
					return
				default:
				}

				err := d.EvictPod(activePod, evictionGroupVersion)
				if err == nil {
					break
				} else if apierrors.IsNotFound(err) {
					returnCh <- nil
					return
				} else if apierrors.IsTooManyRequests(err) {
					//nolint:errcheck
					fmt.Fprintf(errOut, "error when evicting pods/%q -n %q (will retry after %v): %v\n", activePod.Name, activePod.Namespace, d.EvictErrorRetryDelay, err)
					time.Sleep(d.EvictErrorRetryDelay)
				} else if !activePod.ObjectMeta.DeletionTimestamp.IsZero() && apierrors.IsForbidden(err) && apierrors.HasStatusCause(err, corev1.NamespaceTerminatingCause) {
					// 在正在删除的命名空间中发起 eviction 请求会返回 forbidden 错误，
					// 如果 pod 已被标记为删除，可以忽略该错误：eviction 请求永远不会成功，
					// 但我们会通过 waitForDelete 等待该 pod。
					break
				} else if apierrors.IsForbidden(err) && apierrors.HasStatusCause(err, corev1.NamespaceTerminatingCause) {
					// 在正在删除的命名空间中发起 eviction 请求会返回 forbidden 错误，
					// 如果 pod 尚未被标记为删除，则持续重试直到它被标记。
					//nolint:errcheck
					fmt.Fprintf(errOut, "error when evicting pod %q from terminating namespace %q (will retry after %v): %v\n", activePod.Name, activePod.Namespace, d.EvictErrorRetryDelay, err)
					time.Sleep(d.EvictErrorRetryDelay)
				} else {
					returnCh <- fmt.Errorf("error when evicting pods/%q -n %q: %v", activePod.Name, activePod.Namespace, err)
					return
				}

				freshPod, err := getPodFn(activePod.Namespace, activePod.Name)
				// 忽略错误，让 eviction 逻辑结合原始 pod 自行处理。
				if err == nil {
					activePod = *freshPod
				}
			}
			if d.DryRunStrategy == cmdutil.DryRunServer {
				returnCh <- nil
				return
			}
			params := waitForDeleteParams{
				ctx:                             ctx,
				pods:                            []corev1.Pod{activePod},
				interval:                        1 * time.Second,
				timeout:                         time.Duration(math.MaxInt64),
				usingEviction:                   true,
				getPodFn:                        getPodFn,
				onDoneFn:                        d.OnPodDeletedOrEvicted,
				onFinishFn:                      d.OnPodDeletionOrEvictionFinished,
				globalTimeout:                   globalTimeout,
				skipWaitForDeleteTimeoutSeconds: d.SkipWaitForDeleteTimeoutSeconds,
				out:                             out,
			}
			_, err := waitForDelete(params)
			if err == nil {
				returnCh <- nil
			} else {
				returnCh <- fmt.Errorf("error when waiting for pod %q in namespace %q to terminate: %v", pod.Name, pod.Namespace, err)
			}
		}(pod, returnCh)
	}

	doneCount := 0
	var errors []error

	numPods := len(pods)
	for doneCount < numPods {
		if err := <-returnCh; err != nil {
			errors = append(errors, err)
		}
		doneCount++
	}

	return utilerrors.NewAggregate(errors)
}

func (d *Helper) deletePods(pods []corev1.Pod, getPodFn func(namespace, name string) (*corev1.Pod, error)) error {
	// 超时时间为 0 表示无限等待，这里用 MaxInt64 表示。
	var globalTimeout time.Duration
	if d.Timeout == 0 {
		globalTimeout = time.Duration(math.MaxInt64)
	} else {
		globalTimeout = d.Timeout
	}
	for _, pod := range pods {
		err := d.DeletePod(pod)
		if err != nil && !apierrors.IsNotFound(err) {
			return err
		}
		if d.DryRunStrategy == cmdutil.DryRunServer {
			continue
		}
		if d.OnPodDeletionOrEvictionStarted != nil {
			d.OnPodDeletionOrEvictionStarted(&pod, false)
		}
	}

	if d.DryRunStrategy == cmdutil.DryRunServer {
		return nil
	}

	ctx := d.getContext()
	params := waitForDeleteParams{
		ctx:                             ctx,
		pods:                            pods,
		interval:                        1 * time.Second,
		timeout:                         globalTimeout,
		usingEviction:                   false,
		getPodFn:                        getPodFn,
		onDoneFn:                        d.OnPodDeletedOrEvicted,
		onFinishFn:                      d.OnPodDeletionOrEvictionFinished,
		globalTimeout:                   globalTimeout,
		skipWaitForDeleteTimeoutSeconds: d.SkipWaitForDeleteTimeoutSeconds,
		out:                             d.Out,
	}
	_, err := waitForDelete(params)
	return err
}

func waitForDelete(params waitForDeleteParams) ([]corev1.Pod, error) {
	pods := params.pods
	if params.ctx == nil {
		params.ctx = context.Background()
	}
	err := wait.PollUntilContextTimeout(params.ctx, params.interval, params.timeout, true, func(ctx context.Context) (done bool, err error) {
		pendingPods := []corev1.Pod{}
		for i, pod := range pods {
			p, err := params.getPodFn(pod.Namespace, pod.Name)
			// 使用 client-go 的 getPodFn 实现在出错时会返回空的 Pod 结构体，
			// 因此需要同时检查 err == nil 和 p != nil，才能确认成功找到了 pod。
			if apierrors.IsNotFound(err) || (err == nil && p != nil && p.ObjectMeta.UID != pod.ObjectMeta.UID) {
				if params.onFinishFn != nil {
					params.onFinishFn(&pod, params.usingEviction, nil)
				} else if params.onDoneFn != nil {
					params.onDoneFn(&pod, params.usingEviction)
				}
				continue
			} else if err != nil {
				if params.onFinishFn != nil {
					params.onFinishFn(&pod, params.usingEviction, err)
				}
				return false, err
			} else {
				if shouldSkipPod(*p, params.skipWaitForDeleteTimeoutSeconds) {
					fmt.Fprintf(params.out, podSkipMsgTemplate, pod.Name, params.skipWaitForDeleteTimeoutSeconds)
					continue
				}
				pendingPods = append(pendingPods, pods[i])
			}
		}
		pods = pendingPods
		return len(pods) == 0, nil
	})
	return pods, err
}

// Helper 没有构造函数，因此无法强制保证 Helper.Ctx != nil。
// 同时存在多个公开方法，同样无法在一个位置
// 统一初始化 context。
func (d *Helper) getContext() context.Context {
	if d.Ctx != nil {
		return d.Ctx
	}
	return context.Background()
}
