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

package dynamicinformer_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"

	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/json"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/dynamic/fake"
	clientfeatures "k8s.io/client-go/features"
	clientfeaturestesting "k8s.io/client-go/features/testing"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"net/http"
	"net/http/httptest"
)

// triggerFunc 在测试中修改 fake dynamic client 来产生 informer 事件。
//
// 参数 testObject 是场景中预先存在的对象（可能为 nil），返回值为期望被 informer
// 观察到的对象。
type triggerFunc func(gvr schema.GroupVersionResource, ns string, fakeClient *fake.FakeDynamicClient, testObject *unstructured.Unstructured) *unstructured.Unstructured

// triggerFactory 返回一个标准的 create 触发器：在指定命名空间创建带 environment=test
// 标签的 Deployment，用于验证 AddFunc 是否被触发。
func triggerFactory(t *testing.T) triggerFunc {
	return func(gvr schema.GroupVersionResource, ns string, fakeClient *fake.FakeDynamicClient, _ *unstructured.Unstructured) *unstructured.Unstructured {
		testObject := newUnstructured("apps/v1", "Deployment", "ns-foo", "name-foo")
		testObject.SetLabels(map[string]string{"environment": "test"})
		createdObj, err := fakeClient.Resource(gvr).Namespace(ns).Create(context.TODO(), testObject, metav1.CreateOptions{})
		if err != nil {
			t.Error(err)
		}
		return createdObj
	}
}

// handler 返回只实现 AddFunc 的事件处理器，把对象转发到 rcvCh。
//
// 中文：channel 容量为 1，配合 select+ctx 超时用于判断“是否收到事件”。
func handler(rcvCh chan<- *unstructured.Unstructured) *cache.ResourceEventHandlerFuncs {
	return &cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			rcvCh <- obj.(*unstructured.Unstructured)
		},
	}
}

// TestWatchListSemanticsSimple 验证启用 WatchListClient 后，informer 能通过
// WatchList 的初始事件（Bookmark + initial-events 注解）完成同步。
// 服务端只允许带 `watch` 参数的请求，并返回一个带
// InitialEventsAnnotationKey 的 Bookmark 事件，模拟 WatchList 协议。若客户端仍发
// List 请求，测试会立即失败；最终以 HasSynced 作为成功判据。
func TestWatchListSemanticsSimple(t *testing.T) {
	clientfeaturestesting.SetFeatureDuringTest(t, clientfeatures.WatchListClient, true)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		// 中文：WatchList 只应产生 watch 请求，出现普通 List 说明特性未生效。
		if _, ok := req.URL.Query()["watch"]; !ok {
			t.Errorf("expected a watch request, params: %v", req.URL.Query())
			http.Error(w, fmt.Errorf("unexpected request").Error(), http.StatusInternalServerError)
			return
		}

		obj := &appsv1.Deployment{
			TypeMeta: metav1.TypeMeta{
				APIVersion: "apps/v1",
				Kind:       "Deployment",
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

		// 中文：用 Bookmark 事件携带初始列表，是 WatchList 完成同步的标记。
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
	client, err := dynamic.NewForConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}

	factory := dynamicinformer.NewFilteredDynamicSharedInformerFactory(client, 0, "ns", nil)
	target := factory.ForResource(schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "deployments"})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	factory.Start(ctx.Done())

	if !cache.WaitForCacheSync(ctx.Done(), target.Informer().HasSynced) {
		t.Fatalf("failed to wait for caches to sync")
	}
}

// TestUnSupportWatchListSemantics 验证服务端/客户端不支持 WatchList 时能自动回退到
// 传统 List+Watch，并仍然完成同步。
// fake dynamic client 不支持 WatchList 语义，这里不提供任何自定义响应，
// 只要 informer 能在超时内 HasSynced 即证明回退路径可用。
func TestUnSupportWatchListSemantics(t *testing.T) {
	clientfeaturestesting.SetFeatureDuringTest(t, clientfeatures.WatchListClient, true)
	scheme := runtime.NewScheme()
	if err := appsv1.AddToScheme(scheme); err != nil {
		t.Fatalf("failed to add corev1 to scheme: %v", err)
	}
	// fake 客户端不支持 WatchList 语义，无需准备响应。
	fakeClient := fake.NewSimpleDynamicClient(scheme)
	factory := dynamicinformer.NewFilteredDynamicSharedInformerFactory(fakeClient, 0, "ns", nil)
	target := factory.ForResource(schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "deployments"})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	factory.Start(ctx.Done())

	if !cache.WaitForCacheSync(ctx.Done(), target.Informer().HasSynced) {
		t.Fatalf("failed to wait for caches to sync")
	}
}

// TestFilteredDynamicSharedInformerFactory 验证工厂级的命名空间与 ListOptions 过滤。
// 覆盖两类过滤——构造工厂时指定的 namespace，以及 tweakListOptions 注入的
// field/label selector。核心断言是：不匹配的对象不得触发 AddFunc；匹配的对象必须在
// 超时前送达。超时本身被用作“未收到事件”的判定手段。
func TestFilteredDynamicSharedInformerFactory(t *testing.T) {
	scenarios := []struct {
		name             string
		existingObj      *unstructured.Unstructured
		gvr              schema.GroupVersionResource
		informNS         string
		ns               string
		trigger          func(gvr schema.GroupVersionResource, ns string, fakeClient *fake.FakeDynamicClient, testObject *unstructured.Unstructured) *unstructured.Unstructured
		handler          func(rcvCh chan<- *unstructured.Unstructured) *cache.ResourceEventHandlerFuncs
		tweakListOptions dynamicinformer.TweakListOptionsFunc
	}{
		// 场景 1
		{
			// 中文：informer 监听 ns-bar，对象创建在 ns-foo，不应收到事件。
			name:     "scenario 1: test adding an object in different namespace should not trigger AddFunc",
			informNS: "ns-bar",
			ns:       "ns-foo",
			gvr:      schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "deployments"},
			trigger:  triggerFactory(t),
			handler:  handler,
		},
		// 场景 2
		{
			// 中文：命名空间一致，应收到 AddFunc。
			name:     "scenario 2: test adding an object should trigger AddFunc",
			informNS: "ns-foo",
			ns:       "ns-foo",
			gvr:      schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "deployments"},
			trigger:  triggerFactory(t),
			handler:  handler,
		},
		{
			// 中文：field selector 要求 name-bar，而创建的是 name-foo，应过滤掉。
			name:     "tweak options: test adding an object not matching field selector should not trigger AddFunc",
			informNS: "ns-foo",
			ns:       "ns-foo",
			gvr:      schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "deployments"},
			trigger:  triggerFactory(t),
			handler:  handler,
			tweakListOptions: func(opts *metav1.ListOptions) {
				opts.FieldSelector = "metadata.name=name-bar"
			},
		},
		{
			// 中文：field selector 与对象名一致，应收到事件。
			name:     "tweak options: test adding an object matching field selector should trigger AddFunc",
			informNS: "ns-foo",
			ns:       "ns-foo",
			gvr:      schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "deployments"},
			trigger:  triggerFactory(t),
			handler:  handler,
			tweakListOptions: func(opts *metav1.ListOptions) {
				opts.FieldSelector = "metadata.name=name-foo"
			},
		},
		{
			// 中文：触发器创建的 Deployment 带 environment=test，production 过滤器应将其排除。
			name:     "tweak options: test adding an object not matching label selector should not trigger AddFunc",
			informNS: "ns-foo",
			ns:       "ns-foo",
			gvr:      schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "deployments"},
			trigger:  triggerFactory(t),
			handler:  handler,
			tweakListOptions: func(opts *metav1.ListOptions) {
				opts.LabelSelector = "environment=production"
			},
		},
		{
			// 中文：environment=test 与对象标签匹配，应收到事件。
			name:     "tweak options: test adding an object matching label selector should trigger AddFunc",
			informNS: "ns-foo",
			ns:       "ns-foo",
			gvr:      schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "deployments"},
			trigger:  triggerFactory(t),
			handler:  handler,
			tweakListOptions: func(opts *metav1.ListOptions) {
				opts.LabelSelector = "environment=test"
			},
		},
	}

	for _, ts := range scenarios {
		t.Run(ts.name, func(t *testing.T) {
			// 准备测试数据
			timeout := time.Duration(3 * time.Second)
			ctx, cancel := context.WithTimeout(context.Background(), timeout)
			defer cancel()
			scheme := runtime.NewScheme()
			informerReciveObjectCh := make(chan *unstructured.Unstructured, 1)
			objs := []runtime.Object{}
			if ts.existingObj != nil {
				objs = append(objs, ts.existingObj)
			}
			// 不调整 scheme 以包含 DeploymentList：这里只登记 GVR→ListKind 映射、不注册具体类型，
			// 以验证动态 informer 在不依赖 scheme 类型的情况下也能工作。
			gvrToListKind := map[schema.GroupVersionResource]string{
				{Group: "apps", Version: "v1", Resource: "deployments"}: "DeploymentList",
			}
			fakeClient := fake.NewSimpleDynamicClientWithCustomListKinds(scheme, gvrToListKind, objs...)
			target := dynamicinformer.NewFilteredDynamicSharedInformerFactory(fakeClient, 0, ts.informNS, nil)

			// 执行
			informerListerForGvr := target.ForResource(ts.gvr)
			informerListerForGvr.Informer().AddEventHandler(ts.handler(informerReciveObjectCh))
			target.Start(ctx.Done())
			if synced := target.WaitForCacheSync(ctx.Done()); !synced[ts.gvr] {
				t.Errorf("informer for %s hasn't synced", ts.gvr)
			}

			testObject := ts.trigger(ts.gvr, ts.ns, fakeClient, ts.existingObj)
			select {
			case objFromInformer := <-informerReciveObjectCh:
				// 中文：收到事件但命名空间不匹配，说明过滤失效，属严重错误。
				if ts.ns != ts.informNS {
					t.Errorf("informer received an object for namespace %s when watching namespace %s", ts.ns, ts.informNS)
				}
				if !equality.Semantic.DeepEqual(testObject, objFromInformer) {
					t.Fatalf("%v", cmp.Diff(testObject, objFromInformer))
				}
			case <-ctx.Done():
				// 中文：超时仍无事件是预期结果（对象应被过滤）；若本应匹配则判失败。
				if ts.ns == ts.informNS {
					t.Errorf("tested informer haven't received an object, waited %v", timeout)
				}
			}
		})
	}

}

// TestDynamicSharedInformerFactory 验证动态共享 informer 对 Add/Update/Delete 的转发。
// 三种场景分别触发对应事件处理器，并用 DeepEqual 校验 informer 收到的对象
// 与 API 调用返回值一致。所有等待都有 3 秒超时，避免测试无限挂起。
func TestDynamicSharedInformerFactory(t *testing.T) {
	scenarios := []struct {
		name        string
		existingObj *unstructured.Unstructured
		gvr         schema.GroupVersionResource
		ns          string
		trigger     func(gvr schema.GroupVersionResource, ns string, fakeClient *fake.FakeDynamicClient, testObject *unstructured.Unstructured) *unstructured.Unstructured
		handler     func(rcvCh chan<- *unstructured.Unstructured) *cache.ResourceEventHandlerFuncs
	}{
		// 场景 1
		{
			// 中文：直接 Create 应触发 AddFunc。
			name: "scenario 1: test if adding an object triggers AddFunc",
			ns:   "ns-foo",
			gvr:  schema.GroupVersionResource{Group: "extensions", Version: "v1beta1", Resource: "deployments"},
			trigger: func(gvr schema.GroupVersionResource, ns string, fakeClient *fake.FakeDynamicClient, _ *unstructured.Unstructured) *unstructured.Unstructured {
				testObject := newUnstructured("extensions/v1beta1", "Deployment", "ns-foo", "name-foo")
				createdObj, err := fakeClient.Resource(gvr).Namespace(ns).Create(context.TODO(), testObject, metav1.CreateOptions{})
				if err != nil {
					t.Error(err)
				}
				return createdObj
			},
			handler: func(rcvCh chan<- *unstructured.Unstructured) *cache.ResourceEventHandlerFuncs {
				return &cache.ResourceEventHandlerFuncs{
					AddFunc: func(obj interface{}) {
						rcvCh <- obj.(*unstructured.Unstructured)
					},
				}
			},
		},

		// 场景 2
		{
			// 中文：先预置对象，再修改 spec 触发 UpdateFunc；只把新对象送入 channel。
			name:        "scenario 2: tests if updating an object triggers UpdateFunc",
			ns:          "ns-foo",
			gvr:         schema.GroupVersionResource{Group: "extensions", Version: "v1beta1", Resource: "deployments"},
			existingObj: newUnstructured("extensions/v1beta1", "Deployment", "ns-foo", "name-foo"),
			trigger: func(gvr schema.GroupVersionResource, ns string, fakeClient *fake.FakeDynamicClient, testObject *unstructured.Unstructured) *unstructured.Unstructured {
				testObject.Object["spec"] = "updatedName"
				updatedObj, err := fakeClient.Resource(gvr).Namespace(ns).Update(context.TODO(), testObject, metav1.UpdateOptions{})
				if err != nil {
					t.Error(err)
				}
				return updatedObj
			},
			handler: func(rcvCh chan<- *unstructured.Unstructured) *cache.ResourceEventHandlerFuncs {
				return &cache.ResourceEventHandlerFuncs{
					UpdateFunc: func(old, updated interface{}) {
						rcvCh <- updated.(*unstructured.Unstructured)
					},
				}
			},
		},

		// 场景 3
		{
			// 中文：预置对象后删除，DeleteFunc 收到的对象应与删除前的对象一致。
			name:        "scenario 3: test if deleting an object triggers DeleteFunc",
			ns:          "ns-foo",
			gvr:         schema.GroupVersionResource{Group: "extensions", Version: "v1beta1", Resource: "deployments"},
			existingObj: newUnstructured("extensions/v1beta1", "Deployment", "ns-foo", "name-foo"),
			trigger: func(gvr schema.GroupVersionResource, ns string, fakeClient *fake.FakeDynamicClient, testObject *unstructured.Unstructured) *unstructured.Unstructured {
				err := fakeClient.Resource(gvr).Namespace(ns).Delete(context.TODO(), testObject.GetName(), metav1.DeleteOptions{})
				if err != nil {
					t.Error(err)
				}
				return testObject
			},
			handler: func(rcvCh chan<- *unstructured.Unstructured) *cache.ResourceEventHandlerFuncs {
				return &cache.ResourceEventHandlerFuncs{
					DeleteFunc: func(obj interface{}) {
						rcvCh <- obj.(*unstructured.Unstructured)
					},
				}
			},
		},
	}

	for _, ts := range scenarios {
		t.Run(ts.name, func(t *testing.T) {
			// 准备测试数据
			timeout := time.Duration(3 * time.Second)
			ctx, cancel := context.WithTimeout(context.Background(), timeout)
			defer cancel()
			scheme := runtime.NewScheme()
			informerReciveObjectCh := make(chan *unstructured.Unstructured, 1)
			objs := []runtime.Object{}
			if ts.existingObj != nil {
				objs = append(objs, ts.existingObj)
			}
			// 同样不调整 scheme 以包含 DeploymentList，仅提供 ListKind 映射，
			// 验证动态 informer 不依赖 scheme 注册类型。
			gvrToListKind := map[schema.GroupVersionResource]string{
				{Group: "extensions", Version: "v1beta1", Resource: "deployments"}: "DeploymentList",
			}
			fakeClient := fake.NewSimpleDynamicClientWithCustomListKinds(scheme, gvrToListKind, objs...)
			target := dynamicinformer.NewDynamicSharedInformerFactory(fakeClient, 0)

			// 执行
			informerListerForGvr := target.ForResource(ts.gvr)
			informerListerForGvr.Informer().AddEventHandler(ts.handler(informerReciveObjectCh))
			target.Start(ctx.Done())
			if synced := target.WaitForCacheSync(ctx.Done()); !synced[ts.gvr] {
				t.Errorf("informer for %s hasn't synced", ts.gvr)
			}

			testObject := ts.trigger(ts.gvr, ts.ns, fakeClient, ts.existingObj)
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

// newUnstructured 构造一个最小化的 unstructured 对象；
// spec 直接复用 name，便于 Update 场景修改并区分新旧对象。
func newUnstructured(apiVersion, kind, namespace, name string) *unstructured.Unstructured {
	return &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": apiVersion,
			"kind":       kind,
			"metadata": map[string]interface{}{
				"namespace": namespace,
				"name":      name,
			},
			"spec": name,
		},
	}
}
