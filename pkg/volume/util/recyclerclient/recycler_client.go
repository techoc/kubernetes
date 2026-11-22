/*
Copyright 2018 The Kubernetes Authors.

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

package recyclerclient

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/watch"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"
)

// RecycleEventRecorder 定义如何记录回收事件的回调。
type RecycleEventRecorder func(eventtype, message string)

// RecycleVolumeByWatchingPodUntilCompletion 供卷回收器使用。
// 该函数把给定的 Pod 保存到 API，并持续 watch 直到它完成、失败，
// 或超过 ActiveDeadlineSeconds，以最先发生者为准。返回前总会尝试删除回收 Pod。
//
// 若同名同命名空间下已有 Pod 在运行，本函数会直接删除它——因为无法判断
// 那是旧的回收器，还是用户伪造的回收器来阻止 Kubernetes 回收卷。//
//
//	 pod - 卷插件为回收卷设计的 pod。pod.Name 会被基于 PV.Name 的唯一名称覆盖。
//		client - 用于 API 操作的 kube client。
func RecycleVolumeByWatchingPodUntilCompletion(pvName string, pod *v1.Pod, kubeClient clientset.Interface, recorder RecycleEventRecorder) error {
	return internalRecycleVolumeByWatchingPodUntilCompletion(pvName, pod, newRecyclerClient(kubeClient, recorder))
}

// 与上面的函数注释相同，区别是 recyclerClient 是更窄的 Pod API 接口，便于测试。
func internalRecycleVolumeByWatchingPodUntilCompletion(pvName string, pod *v1.Pod, recyclerClient recyclerClient) error {
	klog.V(5).Infof("creating recycler pod for volume %s\n", pod.Name)

	// 为回收 Pod 生成唯一名称：这样若前一个控制器已开始回收该卷，
	// 会收到“already exists”错误。这里假设 pv.Name 本身已唯一。
	pod.Name = "recycler-for-" + pvName
	pod.GenerateName = ""

	stopChannel := make(chan struct{})
	defer close(stopChannel)
	podCh, err := recyclerClient.WatchPod(pod.Name, pod.Namespace, stopChannel)
	if err != nil {
		klog.V(4).Infof("cannot start watcher for pod %s/%s: %v", pod.Namespace, pod.Name, err)
		return err
	}

	// 启动 Pod
	_, err = recyclerClient.CreatePod(pod)
	if err != nil {
		if apierrors.IsAlreadyExists(err) {
			deleteErr := recyclerClient.DeletePod(pod.Name, pod.Namespace)
			if deleteErr != nil {
				return fmt.Errorf("failed to delete old recycler pod %s/%s: %s", pod.Namespace, pod.Name, deleteErr)
			}
			// 回收器会在下次重试，届时旧 Pod 应该已被删除。
			return fmt.Errorf("old recycler pod found, will retry later")
		}
		return fmt.Errorf("unexpected error creating recycler pod:  %+v", err)
	}
	err = waitForPod(pod, recyclerClient, podCh)

	// 无论结果如何都删除回收 Pod，并记录删除结果。
	klog.V(2).Infof("deleting recycler pod %s/%s", pod.Namespace, pod.Name)
	deleteErr := recyclerClient.DeletePod(pod.Name, pod.Namespace)
	if deleteErr != nil {
		klog.Errorf("failed to delete recycler pod %s/%s: %v", pod.Namespace, pod.Name, deleteErr)
	}

	// 优先返回回收错误；Pod 会在下一次重试时再次删除。
	if err != nil {
		return fmt.Errorf("failed to recycle volume: %s", err)
	}

	// 回收成功但删除回收 Pod 失败：上报该错误，控制器很快会再次重试回收该 PV。
	if deleteErr != nil {
		return fmt.Errorf("failed to delete recycler pod: %s", deleteErr)
	}

	return nil
}

// waitForPod 持续 watch Pod 直到结束，并把 Pod 上的所有事件转发给 PV。
func waitForPod(pod *v1.Pod, recyclerClient recyclerClient, podCh <-chan watch.Event) error {
	for {
		event, ok := <-podCh
		if !ok {
			return fmt.Errorf("recycler pod %q watch channel had been closed", pod.Name)
		}
		switch event.Object.(type) {
		case *v1.Pod:
			// Pod 状态变化
			pod := event.Object.(*v1.Pod)
			klog.V(4).Infof("recycler pod update received: %s %s/%s %s", event.Type, pod.Namespace, pod.Name, pod.Status.Phase)
			switch event.Type {
			case watch.Added, watch.Modified:
				if pod.Status.Phase == v1.PodSucceeded {
					// 回收成功。
					return nil
				}
				if pod.Status.Phase == v1.PodFailed {
					if pod.Status.Message != "" {
						return errors.New(pod.Status.Message)
					}
					return fmt.Errorf("pod failed, pod.Status.Message unknown")
				}

			case watch.Deleted:
				return fmt.Errorf("recycler pod was deleted")

			case watch.Error:
				return fmt.Errorf("recycler pod watcher failed")
			}

		case *v1.Event:
			// 收到事件
			podEvent := event.Object.(*v1.Event)
			klog.V(4).Infof("recycler event received: %s %s/%s %s/%s %s", event.Type, podEvent.Namespace, podEvent.Name, podEvent.InvolvedObject.Namespace, podEvent.InvolvedObject.Name, podEvent.Message)
			if event.Type == watch.Added {
				recyclerClient.Event(podEvent.Type, podEvent.Message)
			}
		}
	}
}

// recyclerClient 通过更窄的接口抽象 Pod 访问，便于在测试中 mock 客户端。
type recyclerClient interface {
	CreatePod(pod *v1.Pod) (*v1.Pod, error)
	GetPod(name, namespace string) (*v1.Pod, error)
	DeletePod(name, namespace string) error
	// WatchPod 返回用于 watch Pod 的 ListWatch。stopChannel 用于关闭 watch 背后的 reflector。
	// 调用方负责 defer 关闭该 channel 以停止 reflector。
	WatchPod(name, namespace string, stopChannel chan struct{}) (<-chan watch.Event, error)
	// Event 把事件发送给正在被回收的卷。
	Event(eventtype, message string)
}

// newRecyclerClient 创建基于真实客户端的 recyclerClient。
func newRecyclerClient(client clientset.Interface, recorder RecycleEventRecorder) recyclerClient {
	return &realRecyclerClient{
		client,
		recorder,
	}
}

// realRecyclerClient 是基于 clientset 的 recyclerClient 实现。
type realRecyclerClient struct {
	client   clientset.Interface
	recorder RecycleEventRecorder
}

// CreatePod 创建回收 Pod。
func (c *realRecyclerClient) CreatePod(pod *v1.Pod) (*v1.Pod, error) {
	return c.client.CoreV1().Pods(pod.Namespace).Create(context.TODO(), pod, metav1.CreateOptions{})
}

// GetPod 获取指定名称和命名空间的 Pod。
func (c *realRecyclerClient) GetPod(name, namespace string) (*v1.Pod, error) {
	return c.client.CoreV1().Pods(namespace).Get(context.TODO(), name, metav1.GetOptions{})
}

// DeletePod 删除指定名称和命名空间的 Pod。
func (c *realRecyclerClient) DeletePod(name, namespace string) error {
	return c.client.CoreV1().Pods(namespace).Delete(context.TODO(), name, metav1.DeleteOptions{})
}

// Event 通过 recorder 记录事件。
func (c *realRecyclerClient) Event(eventtype, message string) {
	c.recorder(eventtype, message)
}

// WatchPod watch 一个 Pod 及其相关事件，把 Pod 更新和事件发送到返回的 channel。
// 持续运行直到 stopChannel 被关闭。
func (c *realRecyclerClient) WatchPod(name, namespace string, stopChannel chan struct{}) (<-chan watch.Event, error) {
	podSelector, err := fields.ParseSelector("metadata.name=" + name)
	if err != nil {
		return nil, err
	}
	options := metav1.ListOptions{
		FieldSelector: podSelector.String(),
		Watch:         true,
	}

	podWatch, err := c.client.CoreV1().Pods(namespace).Watch(context.TODO(), options)
	if err != nil {
		return nil, err
	}

	eventSelector, _ := fields.ParseSelector("involvedObject.name=" + name)
	eventWatch, err := c.client.CoreV1().Events(namespace).Watch(context.TODO(), metav1.ListOptions{
		FieldSelector: eventSelector.String(),
		Watch:         true,
	})
	if err != nil {
		podWatch.Stop()
		return nil, err
	}

	eventCh := make(chan watch.Event, 30)
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer close(eventCh)
		wg.Wait()
	}()

	go func() {
		defer eventWatch.Stop()
		defer wg.Done()
		for {
			select {
			case <-stopChannel:
				return
			case eventEvent, ok := <-eventWatch.ResultChan():
				if !ok {
					return
				}
				eventCh <- eventEvent
			}
		}
	}()

	go func() {
		defer podWatch.Stop()
		defer wg.Done()
		for {
			select {
			case <-stopChannel:
				return

			case podEvent, ok := <-podWatch.ResultChan():
				if !ok {
					return
				}
				eventCh <- podEvent
			}
		}
	}()

	return eventCh, nil
}
