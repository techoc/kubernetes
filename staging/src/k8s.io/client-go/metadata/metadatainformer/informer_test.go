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

package metadatainformer

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"

	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/json"
	"k8s.io/apimachinery/pkg/watch"
	clientfeatures "k8s.io/client-go/features"
	clientfeaturestesting "k8s.io/client-go/features/testing"
	"k8s.io/client-go/metadata"
	"k8s.io/client-go/metadata/fake"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"
)

func init() {
	// 打开 klog 便于在测试失败时查看 informer/reflector 的行为。
	klog.InitFlags(flag.CommandLine)
	flag.CommandLine.Lookup("v").Value.Set("5")
	flag.CommandLine.Lookup("alsologtostderr").Value.Set("true")
}

// TestWatchListSemanticsSimple 验证开启 WatchListClient 特性后，metadata
// informer 会直接发起 watch 请求，并使用服务端返回的 initial-events
// bookmark 作为初始同步完成的信号（而不是先 list 再 watch）。
func TestWatchListSemanticsSimple(t *testing.T) {
	clientfeaturestesting.SetFeatureDuringTest(t, clientfeatures.WatchListClient, true)

	// 服务端只接受 watch；若客户端先发 list，测试即失败。
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if _, ok := req.URL.Query()["watch"]; !ok {
			t.Errorf("expected a watch request, params: %v", req.URL.Query())
			http.Error(w, fmt.Errorf("unexpected request").Error(), http.StatusInternalServerError)
			return
		}

		// initial-events-end 注解标记初始事件集合结束，informer 据此判定缓存已同步。
		obj := &metav1.PartialObjectMetadata{
			TypeMeta: metav1.TypeMeta{
				APIVersion: "meta.k8s.io/v1",
				Kind:       "PartialObjectMetadata",
			},
			ObjectMeta: metav1.ObjectMeta{
				Annotations: map[string]string{
					metav1.InitialEventsAnnotationKey: "true",
				},
			},
		}
		rawObj, err := json.Marshal(obj)
		if err != nil {
			t.Errorf("failed to marshal rawObj: %v", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		// 以 bookmark 事件下发该注解，客户端不把它当作普通对象处理。
		watchEvent := &metav1.WatchEvent{
			Type:   string(watch.Bookmark),
			Object: runtime.RawExtension{Raw: rawObj},
		}
		rawRsp, err := json.Marshal(watchEvent)
		if err != nil {
			t.Errorf("failed to marshal watchEvent: %v", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, err = w.Write(rawRsp)
		if err != nil {
			t.Fatalf("failed to write response: %v", err)
		}
	}))
	defer server.Close()

	cfg := &rest.Config{Host: server.URL}
	client, err := metadata.NewForConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}

	factory := NewFilteredSharedInformerFactory(client, 0, "ns", nil)
	target := factory.ForResource(schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "deployments"})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	factory.Start(ctx.Done())

	if !cache.WaitForCacheSync(ctx.Done(), target.Informer().HasSynced) {
		t.Fatalf("failed to wait for caches to sync")
	}
}

// TestUnSupportWatchListSemantics 验证当服务端/客户端实现不支持 WatchList
// 语义时，informer 能回退到 list+watch 并正常完成初始同步。
func TestUnSupportWatchListSemantics(t *testing.T) {
	clientfeaturestesting.SetFeatureDuringTest(t, clientfeatures.WatchListClient, true)
	scheme := runtime.NewScheme()
	if err := appsv1.AddToScheme(scheme); err != nil {
		t.Fatalf("failed to add appsv1 to scheme: %v", err)
	}
	// The fake client doesn’t support WatchList semantics,
	// so we don’t need to prepare a response.
	// 这里的关键是验证回退路径，而不是 watch 事件内容。
	fakeClient := fake.NewSimpleMetadataClient(scheme)
	factory := NewFilteredSharedInformerFactory(fakeClient, 0, "ns", nil)
	target := factory.ForResource(schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "deployments"})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	factory.Start(ctx.Done())

	if !cache.WaitForCacheSync(ctx.Done(), target.Informer().HasSynced) {
		t.Fatalf("failed to wait for caches to sync")
	}
}

// TestMetadataSharedInformerFactory 覆盖 metadata SharedInformerFactory 的
// 三种事件回调：新增、更新、删除，验证事件能从 fake 客户端经 informer
// 正确投递到事件处理器。
func TestMetadataSharedInformerFactory(t *testing.T) {
	scenarios := []struct {
		name        string
		existingObj *metav1.PartialObjectMetadata
		gvr         schema.GroupVersionResource
		ns          string
		trigger     func(gvr schema.GroupVersionResource, ns string, fakeClient *fake.FakeMetadataClient, testObject *metav1.PartialObjectMetadata) *metav1.PartialObjectMetadata
		handler     func(rcvCh chan<- *metav1.PartialObjectMetadata) *cache.ResourceEventHandlerFuncs
	}{
		// scenario 1
		{
			// 新建对象应触发 AddFunc。
			name: "scenario 1: test if adding an object triggers AddFunc",
			ns:   "ns-foo",
			gvr:  schema.GroupVersionResource{Group: "extensions", Version: "v1beta1", Resource: "deployments"},
			trigger: func(gvr schema.GroupVersionResource, ns string, fakeClient *fake.FakeMetadataClient, _ *metav1.PartialObjectMetadata) *metav1.PartialObjectMetadata {
				testObject := newPartialObjectMetadata("extensions/v1beta1", "Deployment", "ns-foo", "name-foo")
				createdObj, err := fakeClient.Resource(gvr).Namespace(ns).(fake.MetadataClient).CreateFake(testObject, metav1.CreateOptions{})
				if err != nil {
					t.Error(err)
				}
				return createdObj
			},
			handler: func(rcvCh chan<- *metav1.PartialObjectMetadata) *cache.ResourceEventHandlerFuncs {
				return &cache.ResourceEventHandlerFuncs{
					AddFunc: func(obj interface{}) {
						rcvCh <- obj.(*metav1.PartialObjectMetadata)
					},
				}
			},
		},

		// scenario 2
		{
			// 修改注解应触发 UpdateFunc，且处理器收到的是更新后的对象。
			name:        "scenario 2: tests if updating an object triggers UpdateFunc",
			ns:          "ns-foo",
			gvr:         schema.GroupVersionResource{Group: "extensions", Version: "v1beta1", Resource: "deployments"},
			existingObj: newPartialObjectMetadata("extensions/v1beta1", "Deployment", "ns-foo", "name-foo"),
			trigger: func(gvr schema.GroupVersionResource, ns string, fakeClient *fake.FakeMetadataClient, testObject *metav1.PartialObjectMetadata) *metav1.PartialObjectMetadata {
				if testObject.Annotations == nil {
					testObject.Annotations = make(map[string]string)
				}
				testObject.Annotations["test"] = "updatedName"
				updatedObj, err := fakeClient.Resource(gvr).Namespace(ns).(fake.MetadataClient).UpdateFake(testObject, metav1.UpdateOptions{})
				if err != nil {
					t.Error(err)
				}
				return updatedObj
			},
			handler: func(rcvCh chan<- *metav1.PartialObjectMetadata) *cache.ResourceEventHandlerFuncs {
				return &cache.ResourceEventHandlerFuncs{
					UpdateFunc: func(old, updated interface{}) {
						rcvCh <- updated.(*metav1.PartialObjectMetadata)
					},
				}
			},
		},

		// scenario 3
		{
			// 删除对象应触发 DeleteFunc。
			name:        "scenario 3: test if deleting an object triggers DeleteFunc",
			ns:          "ns-foo",
			gvr:         schema.GroupVersionResource{Group: "extensions", Version: "v1beta1", Resource: "deployments"},
			existingObj: newPartialObjectMetadata("extensions/v1beta1", "Deployment", "ns-foo", "name-foo"),
			trigger: func(gvr schema.GroupVersionResource, ns string, fakeClient *fake.FakeMetadataClient, testObject *metav1.PartialObjectMetadata) *metav1.PartialObjectMetadata {
				err := fakeClient.Resource(gvr).Namespace(ns).Delete(context.TODO(), testObject.GetName(), metav1.DeleteOptions{})
				if err != nil {
					t.Error(err)
				}
				return testObject
			},
			handler: func(rcvCh chan<- *metav1.PartialObjectMetadata) *cache.ResourceEventHandlerFuncs {
				return &cache.ResourceEventHandlerFuncs{
					DeleteFunc: func(obj interface{}) {
						rcvCh <- obj.(*metav1.PartialObjectMetadata)
					},
				}
			},
		},
	}

	for _, ts := range scenarios {
		t.Run(ts.name, func(t *testing.T) {
			// test data
			timeout := time.Duration(3 * time.Second)
			ctx, cancel := context.WithTimeout(context.Background(), timeout)
			defer cancel()
			scheme := fake.NewTestScheme()
			metav1.AddMetaToScheme(scheme)
			// 带缓冲的通道用于在事件处理器与测试主体之间传递对象，避免处理器阻塞。
			informerReciveObjectCh := make(chan *metav1.PartialObjectMetadata, 1)
			objs := []runtime.Object{}
			if ts.existingObj != nil {
				objs = append(objs, ts.existingObj)
			}
			fakeClient := fake.NewSimpleMetadataClient(scheme, objs...)
			target := NewSharedInformerFactory(fakeClient, 0)

			// act
			informerListerForGvr := target.ForResource(ts.gvr)
			informerListerForGvr.Informer().AddEventHandler(ts.handler(informerReciveObjectCh))
			target.Start(ctx.Done())
			if synced := target.WaitForCacheSync(ctx.Done()); !synced[ts.gvr] {
				t.Fatalf("informer for %s hasn't synced", ts.gvr)
			}

			testObject := ts.trigger(ts.gvr, ts.ns, fakeClient, ts.existingObj)
			// 等待处理器收到对象或超时，确保测试不会永久挂起。
			select {
			case objFromInformer := <-informerReciveObjectCh:
				if !equality.Semantic.DeepEqual(testObject, objFromInformer) {
					t.Fatalf("%v", cmp.Diff(testObject, objFromInformer))
				}
			case <-ctx.Done():
				t.Errorf("tested informer haven't received an object, waited %v", timeout)
			}
		})
	}
}

// newPartialObjectMetadata 构造只含 TypeMeta 与基本 ObjectMeta 的元数据对象。
func newPartialObjectMetadata(apiVersion, kind, namespace, name string) *metav1.PartialObjectMetadata {
	return &metav1.PartialObjectMetadata{
		TypeMeta: metav1.TypeMeta{
			APIVersion: apiVersion,
			Kind:       kind,
		},
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      name,
		},
	}
}
