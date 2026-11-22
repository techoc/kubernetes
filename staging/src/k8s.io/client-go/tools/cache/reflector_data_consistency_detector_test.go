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

package cache

import (
	"context"
	"fmt"
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apimachinery/pkg/watch"
	clientfeatures "k8s.io/client-go/features"
	clientfeaturestesting "k8s.io/client-go/features/testing"
	"k8s.io/client-go/util/consistencydetector"
	"k8s.io/klog/v2/ktesting"
)

// TestReflectorDataConsistencyDetector 验证 WatchList 模式下的一致性检测不会误报：
// 检测器会重新 list 并与 watch 期间收到的对象比较，本用例构造的数据必须使二者一致。
// 通过 feature gate 的组合（InOrderInformers × TransformFunc）覆盖不同处理路径。
func TestReflectorDataConsistencyDetector(t *testing.T) {
	// WatchListClient 打开后 reflector 才走 WatchList 语义，一致性检测也只在此模式下启用。
	clientfeaturestesting.SetFeatureDuringTest(t, clientfeatures.WatchListClient, true)
	restore := consistencydetector.SetDataConsistencyDetectionForWatchListEnabledForTest(true)
	defer restore()

	// 变换函数给每个 Pod 打上标签；若检测器比较的是变换前/后的不同视图，就会产生假的不一致。
	markTransformed := func(obj interface{}) (interface{}, error) {
		pod, ok := obj.(*v1.Pod)
		if !ok {
			return obj, nil
		}
		newPod := pod.DeepCopy()
		if newPod.Labels == nil {
			newPod.Labels = make(map[string]string)
		}
		newPod.Labels["transformed"] = "true"
		return newPod, nil
	}

	for _, inOrder := range []bool{false, true} {
		t.Run(fmt.Sprintf("InOrder=%v", inOrder), func(t *testing.T) {
			clientfeaturestesting.SetFeatureDuringTest(t, clientfeatures.InOrderInformers, inOrder)
			for _, transformerEnabled := range []bool{false, true} {
				var transformer TransformFunc
				if transformerEnabled {
					transformer = markTransformed
				}
				t.Run(fmt.Sprintf("Transformer=%v", transformerEnabled), func(t *testing.T) {
					runTestReflectorDataConsistencyDetector(t, transformer)
				})
			}
		})
	}
}

// runTestReflectorDataConsistencyDetector 搭建一个最小 reflector：List 返回一个 Pod，
// Watch 先重复该 Pod 再发送带 InitialEventsAnnotationKey 的 Bookmark 表示初始事件结束。
func runTestReflectorDataConsistencyDetector(t *testing.T, transformer TransformFunc) {
	logger, ctx := ktesting.NewTestContext(t)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	store := NewStore(MetaNamespaceKeyFunc)
	_, fifo := newQueueFIFO(logger, nil, store, transformer, InformerNameAndResource{}, nil)

	lw := &ListWatch{
		ListFunc: func(options metav1.ListOptions) (runtime.Object, error) {
			return &v1.PodList{
				ListMeta: metav1.ListMeta{ResourceVersion: "1"},
				Items: []v1.Pod{
					{ObjectMeta: metav1.ObjectMeta{Name: "pod-1", ResourceVersion: "1"}},
				},
			}, nil
		},
		WatchFunc: func(options metav1.ListOptions) (watch.Interface, error) {
			w := watch.NewFake()
			go func() {
				w.Add(&v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pod-1", ResourceVersion: "1"}})
				w.Action(watch.Bookmark, &v1.Pod{ObjectMeta: metav1.ObjectMeta{
					Name:            "pod-1",
					ResourceVersion: "1",
					Annotations:     map[string]string{metav1.InitialEventsAnnotationKey: "true"},
				}})
			}()
			return w, nil
		},
	}

	r := NewReflector(lw, &v1.Pod{}, fifo, 0)

	// 等到 reflector 记录下 resourceVersion（说明首轮同步已完成）后取消上下文，
	// 否则 ListAndWatchWithContext 会一直阻塞在 watch 上。
	go func() {
		_ = wait.PollUntilContextTimeout(ctx, 10*time.Millisecond, 5*time.Second, true, func(ctx context.Context) (bool, error) {
			return r.LastSyncResourceVersion() != "", nil
		})
		cancel()
	}()

	err := r.ListAndWatchWithContext(ctx)
	if err != nil {
		t.Errorf("ListAndWatchWithContext returned error: %v", err)
	}
}
