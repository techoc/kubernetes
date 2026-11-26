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

package fakeclient

import (
	"context"
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/cache"
)

// TestFakeClient 演示在测试中如何把 fake client 与 SharedInformerFactory 配合使用。
//
// 示例刻意展示 fake client 与 informer 配合时的两个坑：fake client 不维护
// resourceVersion，且需要显式等到 watch 建立后再写入，否则事件会丢失。
func TestFakeClient(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	watcherStarted := make(chan struct{})
	// 创建 fake client。
	client := fake.NewSimpleClientset()
	// 通配 watch reactor，用于注入 watcherStarted 通道。
	//
	// Prepend 确保该 reactor 先于 tracker 默认实现执行：我们自己调用
	// Tracker().Watch 并把结果返回，从而能在 watch 真正建立时 close 通道。
	client.PrependWatchReactor("*", func(action clienttesting.Action) (handled bool, ret watch.Interface, err error) {
		var opts metav1.ListOptions
		if watchAction, ok := action.(clienttesting.WatchActionImpl); ok {
			opts = watchAction.ListOptions
		}
		gvr := action.GetResource()
		ns := action.GetNamespace()
		watch, err := client.Tracker().Watch(gvr, ns, opts)
		if err != nil {
			return false, nil, err
		}
		close(watcherStarted)
		return true, watch, nil
	})

	// 创建一个 informer，把新增的 pod 写入 channel。
	// 通道缓冲为 1，避免 handler 在测试主协程尚未读取时阻塞。
	pods := make(chan *v1.Pod, 1)
	informers := informers.NewSharedInformerFactory(client, 0)
	podInformer := informers.Core().V1().Pods().Informer()
	podInformer.AddEventHandler(&cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			pod := obj.(*v1.Pod)
			t.Logf("pod added: %s/%s", pod.Namespace, pod.Name)
			pods <- pod
		},
	})

	// 确保 Informer 已启动。
	informers.Start(ctx.Done())

	// 测试中并非必需，但可借此确认 informer goroutine 已预热并完成 List，
	// 再向其发送事件。
	cache.WaitForCacheSync(ctx.Done(), podInformer.HasSynced)

	// fake client 不支持 resourceVersion。informer 初次 LIST 之后、watch 建立之前
	// 发生的写入会被漏掉，因此这里等到 watch 启动后再写。
	// fake client 本就不是为 informer 设计的；若需测试 informer/controller 的复杂
	// 行为，建议在集成/E2E 测试中使用真实客户端。
	<-watcherStarted
	// 向 fake client 注入一个事件。
	// 需要注意：Create 的命名空间是 test-ns，而 informer 监听所有命名空间。
	p := &v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "my-pod"}}
	_, err := client.CoreV1().Pods("test-ns").Create(context.TODO(), p, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("error injecting pod add: %v", err)
	}

	select {
	case pod := <-pods:
		t.Logf("Got pod from channel: %s/%s", pod.Namespace, pod.Name)
	case <-time.After(wait.ForeverTestTimeout):
		t.Error("Informer did not get the added pod")
	}
}
