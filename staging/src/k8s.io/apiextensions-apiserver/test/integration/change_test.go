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

package integration

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	"k8s.io/apiextensions-apiserver/test/integration/fixtures"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/dynamic"
)

// TestChangeCRD 在后台反复修改 CRD 的同时，持续创建、读取自定义资源并维持多个 watch，
// 验证 CRD 变更不会导致 watch 或请求永久挂起。
func TestChangeCRD(t *testing.T) {
	tearDown, config, _, err := fixtures.StartDefaultServer(t)
	if err != nil {
		t.Fatal(err)
	}
	defer tearDown()
	config.QPS = 1000
	config.Burst = 1000
	apiExtensionsClient, err := clientset.NewForConfig(config)
	if err != nil {
		t.Fatal(err)
	}
	dynamicClient, err := dynamic.NewForConfig(config)
	if err != nil {
		t.Fatal(err)
	}

	noxuDefinition := fixtures.NewNoxuV1CustomResourceDefinition(apiextensionsv1.NamespaceScoped)
	noxuDefinition, err = fixtures.CreateNewV1CustomResourceDefinition(noxuDefinition, apiExtensionsClient, dynamicClient)
	if err != nil {
		t.Fatal(err)
	}

	ns := "default"
	noxuNamespacedResourceClient := newNamespacedCustomResourceVersionedClient(ns, dynamicClient, noxuDefinition, "v1beta1")

	updateCRD := func() {
		noxuDefinitionToUpdate, err := apiExtensionsClient.ApiextensionsV1().CustomResourceDefinitions().Get(context.TODO(), noxuDefinition.Name, metav1.GetOptions{})
		if err != nil {
			t.Error(err)
			return
		}
		if len(noxuDefinitionToUpdate.Spec.Versions) == 1 {
			v2 := noxuDefinitionToUpdate.Spec.Versions[0]
			v2.Name = "v2"
			v2.Served = true
			v2.Storage = false
			noxuDefinitionToUpdate.Spec.Versions = append(noxuDefinitionToUpdate.Spec.Versions, v2)
		} else {
			noxuDefinitionToUpdate.Spec.Versions = noxuDefinitionToUpdate.Spec.Versions[0:1]
		}
		if _, err := apiExtensionsClient.ApiextensionsV1().CustomResourceDefinitions().Update(context.TODO(), noxuDefinitionToUpdate, metav1.UpdateOptions{}); err != nil && !apierrors.IsConflict(err) {
			t.Error(err)
		}
	}

	// 为自定义资源配置 10 个 watcher。
	// 不能像 get 请求那样在循环里反复演练：watchcache 未初始化时会以 429 和
	// Retry-After: 1 拒绝，虽然 429 会自动重试，但频繁的 watchcache 终止与重建
	// 可能让它们被拒绝 N 次后失败，或直到最后一次重建才初始化、从而始终收不到终止。
	// 因此一开始就显式演练它们的终止流程。
	wg := &sync.WaitGroup{}
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()

			w, err := noxuNamespacedResourceClient.Watch(context.TODO(), metav1.ListOptions{})
			if err != nil {
				t.Errorf("unexpected error establishing watch: %v", err)
				return
			}
			for event := range w.ResultChan() {
				switch event.Type {
				case watch.Added, watch.Modified, watch.Deleted:
					// 均为预期事件。
				default:
					t.Errorf("unexpected watch event: %#v", event)
				}
			}
		}(i)
	}

	// 让已建立的 watch 与请求循环预热一段时间。
	time.Sleep(5 * time.Second)

	// 更新 CRD，确认所有 watch 都被正常终止。
	updateCRD()

	drained := make(chan struct{})
	go func() {
		defer close(drained)
		wg.Wait()
	}()

	select {
	case <-drained:
	case <-time.After(wait.ForeverTestTimeout):
		t.Fatal("timed out waiting for watchers to be terminated")
	}

	stopChan := make(chan struct{})

	// 启动后台循环，持续修改 CRD
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stopChan:
				return
			default:
			}

			time.Sleep(10 * time.Millisecond)

			updateCRD()
		}
	}()

	// 启动 10 个循环，持续创建并读取自定义资源
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			noxuInstanceToCreate := fixtures.NewNoxuInstance(ns, fmt.Sprintf("foo-%d", i))
			if _, err := noxuNamespacedResourceClient.Create(context.TODO(), noxuInstanceToCreate, metav1.CreateOptions{}); err != nil {
				t.Error(err)
				return
			}
			for {
				time.Sleep(10 * time.Millisecond)
				select {
				case <-stopChan:
					return
				default:
					if _, err := noxuNamespacedResourceClient.Get(context.TODO(), noxuInstanceToCreate.GetName(), metav1.GetOptions{}); err != nil {
						t.Error(err)
						continue
					}
				}
			}
		}(i)
	}

	// 让已建立的 get 请求循环预热一段时间
	time.Sleep(5 * time.Second)

	// 收尾
	close(stopChan)

	// 等待循环退出
	drained = make(chan struct{})
	go func() {
		defer close(drained)
		wg.Wait()
	}()

	select {
	case <-drained:
	case <-time.After(wait.ForeverTestTimeout):
		t.Fatal("timed out waiting for clients to complete")
	}
}
