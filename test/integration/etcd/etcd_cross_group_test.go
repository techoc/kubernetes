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

package etcd

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/apiserver/pkg/storage"
	"k8s.io/client-go/dynamic"
)

// TestCrossGroupStorage 测试存储在 etcd 预期位置的所有对象是否都能被转换/读取。
func TestCrossGroupStorage(t *testing.T) {
	apiServer := StartRealAPIServerOrDie(t)
	defer apiServer.Cleanup()

	etcdStorageData := GetEtcdStorageData()

	crossGroupResources := map[schema.GroupVersionKind][]Resource{}

	apiServer.Client.CoreV1().Namespaces().Create(context.TODO(), &v1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: testNamespace}}, metav1.CreateOptions{})

	// 按持久化时的 GVK 分组
	for _, resourceToPersist := range apiServer.Resources {
		gvk := resourceToPersist.Mapping.GroupVersionKind
		data, exists := etcdStorageData[resourceToPersist.Mapping.Resource]
		if !exists {
			continue
		}
		storageGVK := gvk
		if data.ExpectedGVK != nil {
			storageGVK = *data.ExpectedGVK
		}
		crossGroupResources[storageGVK] = append(crossGroupResources[storageGVK], resourceToPersist)
	}

	// 清除那些没有跨 group 来源的项
	for gvk, resources := range crossGroupResources {
		groups := sets.NewString()
		for _, resource := range resources {
			groups.Insert(resource.Mapping.GroupVersionKind.Group)
		}
		if len(groups) < 2 {
			delete(crossGroupResources, gvk)
		}
	}

	if len(crossGroupResources) == 0 {
		// 健全性检查
		t.Fatal("no cross-group resources found")
	}

	// 测试所有潜在的跨 group 来源都能被其他来源 watch 和获取
	for gvk, resources := range crossGroupResources {
		t.Run(gvk.String(), func(t *testing.T) {
			// 使用第一个资源创建初始对象
			resource := resources[0]

			// 计算 namespace
			ns := ""
			if resource.Mapping.Scope.Name() == meta.RESTScopeNameNamespace {
				ns = testNamespace
			}

			data := etcdStorageData[resource.Mapping.Resource]
			// 创建对象
			resourceClient, obj, err := JSONToUnstructured(data.Stub, ns, resource.Mapping, apiServer.Dynamic)
			if err != nil {
				t.Fatal(err)
			}
			actual, err := resourceClient.Create(context.TODO(), obj, metav1.CreateOptions{})
			if err != nil {
				t.Fatal(err)
			}
			name := actual.GetName()

			// 为所有版本设置客户端、带版本的数据和 watch
			var (
				clients       = map[schema.GroupVersionResource]dynamic.ResourceInterface{}
				versionedData = map[schema.GroupVersionResource]*unstructured.Unstructured{}
				watches       = map[schema.GroupVersionResource]watch.Interface{}
			)
			for _, resource := range resources {
				clients[resource.Mapping.Resource] = apiServer.Dynamic.Resource(resource.Mapping.Resource).Namespace(ns)
				versionedData[resource.Mapping.Resource], err = clients[resource.Mapping.Resource].Get(context.TODO(), name, metav1.GetOptions{})
				if err != nil {
					t.Fatalf("error finding resource via %s: %v", resource.Mapping.Resource.GroupVersion().String(), err)
				}
				watches[resource.Mapping.Resource], err = clients[resource.Mapping.Resource].Watch(context.TODO(), metav1.ListOptions{ResourceVersion: actual.GetResourceVersion()})
				if err != nil {
					t.Fatalf("error opening watch via %s: %v", resource.Mapping.Resource.GroupVersion().String(), err)
				}
			}

			versioner := storage.APIObjectVersioner{}
			for _, resource := range resources {
				// 清除那些在 etcd 中会被清除的字段
				versioned := versionedData[resource.Mapping.Resource]
				if err := versioner.PrepareObjectForStorage(versioned); err != nil {
					t.Error(err)
					continue
				}
				versionedJSON, err := versioned.MarshalJSON()
				if err != nil {
					t.Error(err)
					continue
				}

				// 在 etcd 中更新
				if _, err := apiServer.KV.Put(context.Background(), data.ExpectedEtcdPath, string(versionedJSON)); err != nil {
					t.Error(err)
					continue
				}
				t.Logf("wrote %s to etcd", resource.Mapping.Resource.GroupVersion().String())

				// 确保每个 watch 都能收到版本正确的事件
				for watchResource, watcher := range watches {
					select {
					case event, ok := <-watcher.ResultChan():
						if !ok {
							t.Fatalf("watch of %s closed in response to persisting %s", watchResource.GroupVersion().String(), resource.Mapping.Resource.GroupVersion().String())
						}
						if event.Type != watch.Modified {
							eventJSON, _ := json.Marshal(event)
							t.Errorf("unexpected watch event sent to watch of %s in response to persisting %s: %s", watchResource.GroupVersion().String(), resource.Mapping.Resource.GroupVersion().String(), string(eventJSON))
							continue
						}
						if event.Object.GetObjectKind().GroupVersionKind().GroupVersion() != watchResource.GroupVersion() {
							t.Errorf("unexpected group version object sent to watch of %s in response to persisting %s: %#v", watchResource.GroupVersion().String(), resource.Mapping.Resource.GroupVersion().String(), event.Object)
							continue
						}
						t.Logf("     received event for %s", watchResource.GroupVersion().String())
					case <-time.After(30 * time.Second):
						t.Errorf("timed out waiting for watch event for %s in response to persisting %s", watchResource.GroupVersion().String(), resource.Mapping.Resource.GroupVersion().String())
						continue
					}
				}

				// 确保每个客户端都能直接 get 并拿到正确的版本
				for clientResource, client := range clients {
					obj, err := client.Get(context.TODO(), name, metav1.GetOptions{})
					if err != nil {
						t.Errorf("error looking up %s after persisting %s", clientResource.GroupVersion().String(), resource.Mapping.Resource.GroupVersion().String())
						continue
					}
					if obj.GetObjectKind().GroupVersionKind().GroupVersion() != clientResource.GroupVersion() {
						t.Errorf("unexpected group version retrieved from %s after persisting %s: %#v", clientResource.GroupVersion().String(), resource.Mapping.Resource.GroupVersion().String(), obj)
						continue
					}
					t.Logf("     fetched object for %s", clientResource.GroupVersion().String())
				}
			}
		})
	}
}
