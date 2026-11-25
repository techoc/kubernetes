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

package kubelet

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	kubeapiservertesting "k8s.io/kubernetes/cmd/kube-apiserver/app/testing"
	"k8s.io/kubernetes/pkg/kubelet/util/manager"
	"k8s.io/kubernetes/test/integration/framework"
	testingclock "k8s.io/utils/clock/testing"
)

func TestWatchBasedManager(t *testing.T) {
	testNamespace := "test-watch-based-manager"
	server := kubeapiservertesting.StartTestServerOrDie(t, nil, framework.DefaultTestServerFlags(), framework.SharedEtcd())
	t.Cleanup(server.TearDownFn)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	const n = 10
	server.ClientConfig.QPS = 10000
	server.ClientConfig.Burst = 10000
	client, err := kubernetes.NewForConfig(server.ClientConfig)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, err := client.CoreV1().Namespaces().Create(ctx, (&v1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: testNamespace}}), metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}

	listObj := func(namespace string, options metav1.ListOptions) (runtime.Object, error) {
		return client.CoreV1().Secrets(namespace).List(ctx, options)
	}
	watchObj := func(namespace string, options metav1.ListOptions) (watch.Interface, error) {
		return client.CoreV1().Secrets(namespace).Watch(ctx, options)
	}
	newObj := func() runtime.Object { return &v1.Secret{} }
	// 压测需要所有 watch 都处于运行状态，因此这里不把任何 Secret 视为不可变。
	isImmutable := func(_ runtime.Object) bool { return false }
	listWatcherWithWatchListSemanticsWrapper := func(lw *cache.ListWatch) cache.ListerWatcher {
		return cache.ToListWatcherWithWatchListSemantics(lw, client)
	}
	fakeClock := testingclock.NewFakeClock(time.Now())

	stopCh := make(chan struct{})
	t.Cleanup(func() { close(stopCh) })

	store := manager.NewObjectCache(listObj, watchObj, newObj, isImmutable, listWatcherWithWatchListSemanticsWrapper, schema.GroupResource{Group: "v1", Resource: "secrets"}, fakeClock, time.Minute, stopCh)

	// 并行创建 1000 个 Secret。
	t.Log(time.Now(), "creating 1000 secrets")
	wg := sync.WaitGroup{}
	errCh := make(chan error, n)
	for i := range n {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := range 100 {
				name := fmt.Sprintf("s%d", i*100+j)
				if _, err := client.CoreV1().Secrets(testNamespace).Create(ctx, &v1.Secret{ObjectMeta: metav1.ObjectMeta{Name: name}}, metav1.CreateOptions{}); err != nil {
					select {
					case errCh <- err:
					default:
					}
				}
			}
			fmt.Print(".")
		}(i)
	}

	wg.Wait()
	select {
	case err := <-errCh:
		t.Fatal(err)
	default:
	}
	t.Log(time.Now(), "finished creating 1000 secrets")

	// 并发读取所有 Secret。
	wg = sync.WaitGroup{}
	errCh = make(chan error, n)
	for i := range n {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := range 100 {
				name := fmt.Sprintf("s%d", i*100+j)
				start := time.Now()
				store.AddReference(testNamespace, name, types.UID(name))
				err := wait.PollImmediate(10*time.Millisecond, 10*time.Second, func() (bool, error) {
					obj, err := store.Get(testNamespace, name)
					if err != nil {
						t.Logf("failed on %s, retrying: %v", name, err)
						return false, nil
					}
					if obj.(*v1.Secret).Name != name {
						return false, fmt.Errorf("wrong object: %v", obj.(*v1.Secret).Name)
					}
					return true, nil
				})
				if err != nil {
					select {
					case errCh <- fmt.Errorf("failed on :%s: %v", name, err):
					default:
					}
				}
				if d := time.Since(start); d > time.Second {
					t.Logf("%s took %v", name, d)
				}
			}
		}(i)
	}

	wg.Wait()
	select {
	case err = <-errCh:
		t.Fatal(err)
	default:
	}
}
