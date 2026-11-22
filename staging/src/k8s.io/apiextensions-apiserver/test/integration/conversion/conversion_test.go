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

package conversion

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"

	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/apimachinery/pkg/util/wait"
	etcd3watcher "k8s.io/apiserver/pkg/storage/etcd3"
	"k8s.io/client-go/dynamic"
	_ "k8s.io/component-base/logs/testinit" // 启用日志相关 flag

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apiextensionsv1beta1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1beta1"
	"k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	serveroptions "k8s.io/apiextensions-apiserver/pkg/cmd/server/options"
	"k8s.io/apiextensions-apiserver/test/integration/fixtures"
	"k8s.io/apiextensions-apiserver/test/integration/storage"
)

// Checker 是针对转换测试上下文执行断言的检查函数。
type Checker func(t *testing.T, ctc *conversionTestContext)

// checks 将多个检查函数聚合为切片，便于在测试表中声明。
func checks(checkers ...Checker) []Checker {
	return checkers
}

// TestWebhookConverterWithWatchCache 在启用 watch cache 时运行 webhook 转换测试。
func TestWebhookConverterWithWatchCache(t *testing.T) {
	testWebhookConverter(t, true)
}

// TestWebhookConverterWithoutWatchCache 在关闭 watch cache 时运行 webhook 转换测试。
func TestWebhookConverterWithoutWatchCache(t *testing.T) {
	testWebhookConverter(t, false)
}

// TestWebhookNotCalledForUnusedVersions 覆盖一个场景：转换 webhook 可能被要求转换
// 既不 served 也不 storage 的版本。
// 详见 https://github.com/kubernetes/kubernetes/issues/129979
func TestWebhookNotCalledForUnusedVersions(t *testing.T) {
	ctx := context.Background()

	etcd3watcher.TestOnlySetFatalOnDecodeError(t, false)

	tearDown, config, options, err := fixtures.StartDefaultServer(t, "--watch-cache=true")
	if err != nil {
		t.Fatal(err)
	}

	apiExtensionsClient, err := clientset.NewForConfig(config)
	if err != nil {
		tearDown()
		t.Fatal(err)
	}

	dynamicClient, err := dynamic.NewForConfig(config)
	if err != nil {
		tearDown()
		t.Fatal(err)
	}
	defer tearDown()

	crd := multiVersionFixture.DeepCopy()
	crd.Spec.Versions[0].Storage = false
	crd.Spec.Versions[3].Served = true
	crd.Spec.Versions[3].Storage = true

	RESTOptionsGetter := serveroptions.NewCRDRESTOptionsGetter(*options.RecommendedOptions.Etcd, nil, nil)
	restOptions, err := RESTOptionsGetter.GetRESTOptions(schema.GroupResource{Group: crd.Spec.Group, Resource: crd.Spec.Names.Plural}, nil)
	if err != nil {
		t.Fatal(err)
	}
	etcdClient, _, err := storage.GetEtcdClients(restOptions.StorageConfig.Transport)
	if err != nil {
		t.Fatal(err)
	}
	// nolint:errcheck
	defer etcdClient.Close()

	etcdObjectReader := storage.NewEtcdObjectReader(etcdClient, &restOptions, crd)
	ctcTearDown, ctc := newConversionTestContext(t, apiExtensionsClient, dynamicClient, etcdObjectReader, crd)
	defer ctcTearDown()

	marker, err := ctc.versionedClient("marker", "v1alpha2").Create(ctx, newConversionMultiVersionFixture("marker", "marker", "v1alpha2"), metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}

	// 更新 CRD，停止 serve v1alpha2 版本
	ctc.setStorageVersion(t, "v1beta1")
	ctc.setServed(t, "v1alpha2", false)

	// 清空 managed fields，规避已知问题：https://github.com/kubernetes/kubernetes/issues/111937
	v1alpha1marker := newConversionMultiVersionFixture("marker", "marker", "v1alpha1")
	v1alpha1marker.SetResourceVersion(marker.GetResourceVersion())
	v1alpha1marker.SetManagedFields([]metav1.ManagedFieldsEntry{{}})
	marker, err = ctc.versionedClient(marker.GetNamespace(), "v1alpha1").Update(ctx, v1alpha1marker, metav1.UpdateOptions{})
	if err != nil {
		t.Fatal(err)
	}

	// 设置 webhook，校验它绝不会以 v1alpha2 版本被调用
	upCh, handler := closeOnCall(NewObjectConverterWebhookHandler(t, getUnexpectedVersionCheckConverter(t, "v1alpha2")))
	tearDown, webhookClientConfig, err := StartConversionWebhookServer(handler)
	if err != nil {
		t.Fatal(err)
	}
	defer tearDown()

	ctc.setConversionWebhook(t, webhookClientConfig, []string{"v1alpha1", "v1beta1", "v1beta2"})
	defer ctc.removeConversionWebhook(t)

	// 等待新 webhook 被首次调用
	if err := wait.PollUntilContextTimeout(ctx, time.Millisecond*100, wait.ForeverTestTimeout, true, func(ctx context.Context) (done bool, err error) {
		_, getErr := ctc.versionedClient(marker.GetNamespace(), "v1alpha1").Get(ctx, marker.GetName(), metav1.GetOptions{})
		select {
		case <-upCh:
			return true, nil
		default:
			t.Logf("Waiting for webhook to become effective, getting marker object: %v", getErr)
			return false, nil
		}
	}); err != nil {
		t.Fatal(err)
	}

	// 确认 marker 在所有 served 版本下都可读
	for _, v := range servedVersions(ctc.crd.Spec.Versions) {
		if _, err := ctc.versionedClient(marker.GetNamespace(), v.Name).Get(ctx, marker.GetName(), metav1.GetOptions{}); err != nil {
			t.Fatal(err)
		}
	}

	// 确认可通过不同 API 版本用 server-side apply 更新 marker
	for _, v := range servedVersions(ctc.crd.Spec.Versions) {
		if _, err := ctc.versionedClient(marker.GetNamespace(), v.Name).Apply(ctx, marker.GetName(), newConversionMultiVersionFixture("marker", "marker", v.Name), metav1.ApplyOptions{FieldManager: "application/apply-patch"}); err != nil {
			t.Fatal(err)
		}
	}
}

// testWebhookConverter 用共享的 API server 与 webhook server 逐项运行转换用例。
func testWebhookConverter(t *testing.T, watchCache bool) {
	tests := []struct {
		group          string
		handler        http.Handler
		reviewVersions []string
		checks         []Checker
	}{
		{
			group:          "noop-converter-v1",
			handler:        NewObjectConverterWebhookHandler(t, noopConverter),
			reviewVersions: []string{"v1", "v1beta1"},
			checks:         checks(validateStorageVersion, validateServed, validateMixedStorageVersions("v1alpha1", "v1beta1")), // 不含 v1beta2，因为 schema 不同
		},
		{
			group:          "noop-converter-v1beta1",
			handler:        NewObjectConverterWebhookHandler(t, noopConverter),
			reviewVersions: []string{"v1beta1", "v1"},
			checks:         checks(validateStorageVersion, validateServed, validateMixedStorageVersions("v1alpha1", "v1beta1")), // 不含 v1beta2，因为 schema 不同
		},
		{
			group:          "nontrivial-converter-v1",
			handler:        NewObjectConverterWebhookHandler(t, nontrivialConverter),
			reviewVersions: []string{"v1", "v1beta1"},
			checks:         checks(validateStorageVersion, validateServed, validateMixedStorageVersions("v1alpha1", "v1beta1", "v1beta2"), validateNonTrivialConverted, validateNonTrivialConvertedList, validateStoragePruning, validateDefaulting),
		},
		{
			group:          "nontrivial-converter-v1beta1",
			handler:        NewObjectConverterWebhookHandler(t, nontrivialConverter),
			reviewVersions: []string{"v1beta1", "v1"},
			checks:         checks(validateStorageVersion, validateServed, validateMixedStorageVersions("v1alpha1", "v1beta1", "v1beta2"), validateNonTrivialConverted, validateNonTrivialConvertedList, validateStoragePruning, validateDefaulting),
		},
		{
			group:          "metadata-mutating-v1",
			handler:        NewObjectConverterWebhookHandler(t, metadataMutatingConverter),
			reviewVersions: []string{"v1", "v1beta1"},
			checks:         checks(validateObjectMetaMutation),
		},
		{
			group:          "metadata-mutating-v1beta1",
			handler:        NewObjectConverterWebhookHandler(t, metadataMutatingConverter),
			reviewVersions: []string{"v1beta1", "v1"},
			checks:         checks(validateObjectMetaMutation),
		},
		{
			group:          "metadata-uid-mutating-v1",
			handler:        NewObjectConverterWebhookHandler(t, uidMutatingConverter),
			reviewVersions: []string{"v1", "v1beta1"},
			checks:         checks(validateUIDMutation),
		},
		{
			group:          "metadata-uid-mutating-v1beta1",
			handler:        NewObjectConverterWebhookHandler(t, uidMutatingConverter),
			reviewVersions: []string{"v1beta1", "v1"},
			checks:         checks(validateUIDMutation),
		},
		{
			group:          "empty-response-v1",
			handler:        NewReviewWebhookHandler(t, nil, emptyV1ResponseConverter),
			reviewVersions: []string{"v1", "v1beta1"},
			checks:         checks(expectConversionFailureMessage("empty-response", "returned 0 objects, expected 1")),
		},
		{
			group:          "empty-response-v1beta1",
			handler:        NewReviewWebhookHandler(t, emptyV1Beta1ResponseConverter, nil),
			reviewVersions: []string{"v1beta1", "v1"},
			checks:         checks(expectConversionFailureMessage("empty-response", "returned 0 objects, expected 1")),
		},
		{
			group:          "failure-message-v1",
			handler:        NewReviewWebhookHandler(t, nil, failureV1ResponseConverter("custom webhook conversion error")),
			reviewVersions: []string{"v1", "v1beta1"},
			checks:         checks(expectConversionFailureMessage("failure-message", "custom webhook conversion error")),
		},
		{
			group:          "failure-message-v1beta1",
			handler:        NewReviewWebhookHandler(t, failureV1Beta1ResponseConverter("custom webhook conversion error"), nil),
			reviewVersions: []string{"v1beta1", "v1"},
			checks:         checks(expectConversionFailureMessage("failure-message", "custom webhook conversion error")),
		},
		{
			group:          "unhandled-v1",
			handler:        NewReviewWebhookHandler(t, nil, nil),
			reviewVersions: []string{"v1", "v1beta1"},
			checks:         checks(expectConversionFailureMessage("server-error", "the server rejected our request")),
		},
		{
			group:          "unhandled-v1beta1",
			handler:        NewReviewWebhookHandler(t, nil, nil),
			reviewVersions: []string{"v1beta1", "v1"},
			checks:         checks(expectConversionFailureMessage("server-error", "the server rejected our request")),
		},
	}

	// KUBE_APISERVER_SERVE_REMOVED_APIS_FOR_ONE_RELEASE 允许待移除的 API 不阻塞测试
	t.Setenv("KUBE_APISERVER_SERVE_REMOVED_APIS_FOR_ONE_RELEASE", "true")

	// TODO: 为转换 webhook 的集成测试而加，需要测试由 webhook 转换失败引起的解码错误。
	// 也许应在解码时识别转换 webhook 相关错误以避免触发此机制？或者移除生产代码中针对测试用例的这种特判？
	etcd3watcher.TestOnlySetFatalOnDecodeError(t, false)

	// 为避免每个测试用例都重启 API server 的高昂开销，在测试开始时只启动一次
	// 基础设施（API Server + Webhook Server）。
	//
	// 用 'dynamicWebhookHandler' 在每个用例之间替换转换逻辑（handler），
	// 无需重启实际 HTTP server。
	//
	// 这样 webhook server 只需在测试开始时启动一次。
	// 关键在于，这可以强制保证拆解顺序：先停 API Server，再停 Webhook Server。

	// 创建可变的 handler。
	proxyHandler := &dynamicWebhookHandler{}

	// 先启动 Webhook Server。
	// 这样其延迟拆解最后执行（在 API server 停止之后）。
	webhookTearDown, webhookClientConfig, err := StartConversionWebhookServer(proxyHandler)
	if err != nil {
		t.Fatal(err)
	}
	defer webhookTearDown()

	// 再启动 API Server。
	// 这样其延迟拆解最先执行。
	tearDown, config, options, err := fixtures.StartDefaultServer(t, fmt.Sprintf("--watch-cache=%v", watchCache))
	if err != nil {
		t.Fatal(err)
	}

	apiExtensionsClient, err := clientset.NewForConfig(config)
	if err != nil {
		tearDown()
		t.Fatal(err)
	}

	dynamicClient, err := dynamic.NewForConfig(config)
	if err != nil {
		tearDown()
		t.Fatal(err)
	}
	defer tearDown()

	crd := multiVersionFixture.DeepCopy()

	RESTOptionsGetter := serveroptions.NewCRDRESTOptionsGetter(*options.RecommendedOptions.Etcd, nil, nil)
	restOptions, err := RESTOptionsGetter.GetRESTOptions(schema.GroupResource{Group: crd.Spec.Group, Resource: crd.Spec.Names.Plural}, nil)
	if err != nil {
		t.Fatal(err)
	}
	etcdClient, _, err := storage.GetEtcdClients(restOptions.StorageConfig.Transport)
	if err != nil {
		t.Fatal(err)
	}
	defer etcdClient.Close()

	etcdObjectReader := storage.NewEtcdObjectReader(etcdClient, &restOptions, crd)
	ctcTearDown, ctc := newConversionTestContext(t, apiExtensionsClient, dynamicClient, etcdObjectReader, crd)
	defer ctcTearDown()

	// 只读对象：需要强制转换时，用它读取与存储版本不同的版本
	marker, err := ctc.versionedClient("marker", "v1beta1").Create(context.TODO(), newConversionMultiVersionFixture("marker", "marker", "v1beta1"), metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}

	for _, test := range tests {
		t.Run(test.group, func(t *testing.T) {
			upCh, handler := closeOnCall(test.handler)

			// 注入该用例专用的逻辑
			proxyHandler.set(handler)
			defer proxyHandler.set(nil)

			// 配置 CRD 使用共享的 webhook server
			ctc.setConversionWebhook(t, webhookClientConfig, test.reviewVersions)
			defer ctc.removeConversionWebhook(t)

			// 等待新 webhook 被首次调用
			if err := wait.PollUntilContextTimeout(context.Background(), time.Millisecond*100, wait.ForeverTestTimeout, true, func(ctx context.Context) (done bool, err error) {
				_, getErr := ctc.versionedClient(marker.GetNamespace(), "v1alpha1").Get(ctx, marker.GetName(), metav1.GetOptions{})
				select {
				case <-upCh:
					return true, nil
				default:
					t.Logf("Waiting for webhook to become effective, getting marker object: %v", getErr)
					return false, nil
				}
			}); err != nil {
				t.Fatal(err)
			}

			for i, checkFn := range test.checks {
				name := fmt.Sprintf("check-%d", i)
				t.Run(name, func(t *testing.T) {
					defer ctc.setAndWaitStorageVersion(t, "v1beta1")
					ctc.namespace = fmt.Sprintf("webhook-conversion-%s-%s", test.group, name)
					checkFn(t, ctc)
				})
			}
		})
	}
}

// validateStorageVersion 校验存储版本切换后对象仍可读取。
func validateStorageVersion(t *testing.T, ctc *conversionTestContext) {
	ns := ctc.namespace

	for _, version := range servedVersions(ctc.crd.Spec.Versions) {
		t.Run(version.Name, func(t *testing.T) {
			name := "storageversion-" + version.Name
			client := ctc.versionedClient(ns, version.Name)
			obj, err := client.Create(context.TODO(), newConversionMultiVersionFixture(ns, name, version.Name), metav1.CreateOptions{})
			if err != nil {
				t.Fatal(err)
			}
			ctc.setAndWaitStorageVersion(t, "v1beta2")

			if _, err = client.Get(context.TODO(), obj.GetName(), metav1.GetOptions{}); err != nil {
				t.Fatal(err)
			}

			ctc.setAndWaitStorageVersion(t, "v1beta1")
		})
	}
}

// validateMixedStorageVersions 确保以不同存储版本写入的相同自定义资源
// 都可读且内容一致。
func validateMixedStorageVersions(versions ...string) func(t *testing.T, ctc *conversionTestContext) {
	return func(t *testing.T, ctc *conversionTestContext) {
		ns := ctc.namespace
		clients := ctc.versionedClients(ns)

		// 在所有存储版本下分别创建 CR
		objNames := []string{}
		for _, version := range versions {
			ctc.setAndWaitStorageVersion(t, version)

			name := "mixedstorage-stored-as-" + version
			obj, err := clients[version].Create(context.TODO(), newConversionMultiVersionFixture(ns, name, version), metav1.CreateOptions{})
			if err != nil {
				t.Fatal(err)
			}
			objNames = append(objNames, obj.GetName())
		}

		// 确保同一对象的副本在 CRD 的每个版本下字段和取值一致，与存储版本无关
		for clientVersion, client := range clients {
			t.Run(clientVersion, func(t *testing.T) {
				o1, err := client.Get(context.TODO(), objNames[0], metav1.GetOptions{})
				if err != nil {
					t.Fatal(err)
				}
				for _, objName := range objNames[1:] {
					o2, err := client.Get(context.TODO(), objName, metav1.GetOptions{})
					if err != nil {
						t.Fatal(err)
					}

					// 比较时忽略 metadata
					delete(o1.Object, "metadata")
					delete(o2.Object, "metadata")
					if !reflect.DeepEqual(o1.Object, o2.Object) {
						t.Errorf("Expected custom resource to be same regardless of which storage version is used to create, but got: %s", cmp.Diff(o1, o2))
					}
				}
			})
		}
	}
}

// validateServed 校验修改 served 状态后，对象在对应版本下变为不可读/可读。
func validateServed(t *testing.T, ctc *conversionTestContext) {
	ns := ctc.namespace

	for _, version := range servedVersions(ctc.crd.Spec.Versions) {
		t.Run(version.Name, func(t *testing.T) {
			name := "served-" + version.Name
			client := ctc.versionedClient(ns, version.Name)
			obj, err := client.Create(context.TODO(), newConversionMultiVersionFixture(ns, name, version.Name), metav1.CreateOptions{})
			if err != nil {
				t.Fatal(err)
			}
			ctc.setServed(t, version.Name, false)
			ctc.waitForServed(t, version.Name, false, client, obj)
			ctc.setServed(t, version.Name, true)
			ctc.waitForServed(t, version.Name, true, client, obj)
		})
	}
}

// validateNonTrivialConverted 校验非平凡的转换结果在不同版本和存储中保持一致。
func validateNonTrivialConverted(t *testing.T, ctc *conversionTestContext) {
	ns := ctc.namespace

	for _, createVersion := range servedVersions(ctc.crd.Spec.Versions) {
		t.Run(fmt.Sprintf("getting objects created as %s", createVersion.Name), func(t *testing.T) {
			name := "converted-" + createVersion.Name
			client := ctc.versionedClient(ns, createVersion.Name)

			fixture := newConversionMultiVersionFixture(ns, name, createVersion.Name)
			if err := unstructured.SetNestedField(fixture.Object, "foo", "garbage"); err != nil {
				t.Fatal(err)
			}
			if _, err := client.Create(context.TODO(), fixture, metav1.CreateOptions{}); err != nil {
				t.Fatal(err)
			}

			// 确认存储中是正确的、已剪枝的版本
			obj, err := ctc.etcdObjectReader.GetStoredCustomResource(ns, name)
			if err != nil {
				t.Fatal(err)
			}
			verifyMultiVersionObject(t, "v1beta1", obj)

			for _, getVersion := range servedVersions(ctc.crd.Spec.Versions) {
				client := ctc.versionedClient(ns, getVersion.Name)
				obj, err := client.Get(context.TODO(), name, metav1.GetOptions{})
				if err != nil {
					t.Fatal(err)
				}
				verifyMultiVersionObject(t, getVersion.Name, obj)
			}

			// 对主资源发送非平凡 patch，校验 oldObject 处于正确版本
			if _, err := client.Patch(context.TODO(), name, types.MergePatchType, []byte(`{"metadata":{"annotations":{"main":"true"}}}`), metav1.PatchOptions{}); err != nil {
				t.Fatal(err)
			}
			// 确认存储中是正确的、已剪枝的版本
			obj, err = ctc.etcdObjectReader.GetStoredCustomResource(ns, name)
			if err != nil {
				t.Fatal(err)
			}
			verifyMultiVersionObject(t, "v1beta1", obj)

			// 对 status 子资源发送非平凡 patch，校验 oldObject 处于正确版本
			if _, err := client.Patch(context.TODO(), name, types.MergePatchType, []byte(`{"metadata":{"annotations":{"status":"true"}}}`), metav1.PatchOptions{}, "status"); err != nil {
				t.Fatal(err)
			}
			// 确认存储中是正确的、已剪枝的版本
			obj, err = ctc.etcdObjectReader.GetStoredCustomResource(ns, name)
			if err != nil {
				t.Fatal(err)
			}
			verifyMultiVersionObject(t, "v1beta1", obj)
		})
	}
}

// validateNonTrivialConvertedList 校验列表读取时每个条目都按目标版本完成转换。
func validateNonTrivialConvertedList(t *testing.T, ctc *conversionTestContext) {
	ns := ctc.namespace + "-list"

	names := sets.String{}
	versions := servedVersions(ctc.crd.Spec.Versions)
	for _, createVersion := range versions {
		name := "converted-" + createVersion.Name
		client := ctc.versionedClient(ns, createVersion.Name)
		fixture := newConversionMultiVersionFixture(ns, name, createVersion.Name)
		if err := unstructured.SetNestedField(fixture.Object, "foo", "garbage"); err != nil {
			t.Fatal(err)
		}
		_, err := client.Create(context.TODO(), fixture, metav1.CreateOptions{})
		if err != nil {
			t.Fatal(err)
		}
		names.Insert(name)
	}

	for _, listVersion := range versions {
		t.Run(fmt.Sprintf("listing objects as %s", listVersion.Name), func(t *testing.T) {
			client := ctc.versionedClient(ns, listVersion.Name)
			obj, err := client.List(context.TODO(), metav1.ListOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if len(obj.Items) != len(versions) {
				t.Fatal("unexpected number of items")
			}
			foundNames := sets.String{}
			for _, u := range obj.Items {
				foundNames.Insert(u.GetName())
				verifyMultiVersionObject(t, listVersion.Name, &u)
			}
			if !foundNames.Equal(names) {
				t.Errorf("unexpected set of returned items: %s", foundNames.Difference(names))
			}
		})
	}
}

// validateStoragePruning 校验读取时存储中的未知字段会被剪枝。
func validateStoragePruning(t *testing.T, ctc *conversionTestContext) {
	ns := ctc.namespace

	for _, createVersion := range servedVersions(ctc.crd.Spec.Versions) {
		t.Run(fmt.Sprintf("getting objects created as %s", createVersion.Name), func(t *testing.T) {
			name := "storagepruning-" + createVersion.Name
			client := ctc.versionedClient(ns, createVersion.Name)

			fixture := newConversionMultiVersionFixture(ns, name, createVersion.Name)
			if err := unstructured.SetNestedField(fixture.Object, "foo", "garbage"); err != nil {
				t.Fatal(err)
			}
			_, err := client.Create(context.TODO(), fixture, metav1.CreateOptions{})
			if err != nil {
				t.Fatal(err)
			}

			// 确认存储中是正确的、已剪枝的版本
			obj, err := ctc.etcdObjectReader.GetStoredCustomResource(ns, name)
			if err != nil {
				t.Fatal(err)
			}
			verifyMultiVersionObject(t, "v1beta1", obj)

			// 加入垃圾字段并设置一个 label
			if err := unstructured.SetNestedField(obj.Object, "foo", "garbage"); err != nil {
				t.Fatal(err)
			}
			labels := obj.GetLabels()
			if labels == nil {
				labels = map[string]string{}
			}
			labels["mutated"] = "true"
			obj.SetLabels(labels)
			if err := ctc.etcdObjectReader.SetStoredCustomResource(ns, name, obj); err != nil {
				t.Fatal(err)
			}

			for _, getVersion := range servedVersions(ctc.crd.Spec.Versions) {
				client := ctc.versionedClient(ns, getVersion.Name)
				obj, err := client.Get(context.TODO(), name, metav1.GetOptions{})
				if err != nil {
					t.Fatal(err)
				}

				// 确认直接修改 etcd 已生效
				labels := obj.GetLabels()
				if labels["mutated"] != "true" {
					t.Errorf("expected object %s in version %s to have label 'mutated=true'", name, getVersion.Name)
				}

				verifyMultiVersionObject(t, getVersion.Name, obj)
			}
		})
	}
}

// validateObjectMetaMutation 校验 webhook 对 annotations/labels 的修改被保留，其他 metadata 修改被忽略。
func validateObjectMetaMutation(t *testing.T, ctc *conversionTestContext) {
	ns := ctc.namespace

	t.Logf("Creating object in storage version v1beta1")
	storageVersion := "v1beta1"
	ctc.setAndWaitStorageVersion(t, storageVersion)
	name := "objectmeta-mutation-" + storageVersion
	client := ctc.versionedClient(ns, storageVersion)
	obj, err := client.Create(context.TODO(), newConversionMultiVersionFixture(ns, name, storageVersion), metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	validateObjectMetaMutationObject(t, false, false, obj)

	t.Logf("Getting object in other version v1beta2")
	client = ctc.versionedClient(ns, "v1beta2")
	obj, err = client.Get(context.TODO(), name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	validateObjectMetaMutationObject(t, true, true, obj)

	t.Logf("Creating object in non-storage version")
	name = "objectmeta-mutation-v1beta2"
	client = ctc.versionedClient(ns, "v1beta2")
	obj, err = client.Create(context.TODO(), newConversionMultiVersionFixture(ns, name, "v1beta2"), metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	validateObjectMetaMutationObject(t, true, true, obj)

	t.Logf("Listing objects in non-storage version")
	client = ctc.versionedClient(ns, "v1beta2")
	list, err := client.List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		t.Fatal(err)
	}
	for _, obj := range list.Items {
		validateObjectMetaMutationObject(t, true, true, &obj)
	}
}

// validateObjectMetaMutationObject 按预期断言对象上的 annotations/labels 及其他 metadata 字段。
func validateObjectMetaMutationObject(t *testing.T, expectAnnotations, expectLabels bool, obj *unstructured.Unstructured) {
	if expectAnnotations {
		if _, found := obj.GetAnnotations()["from"]; !found {
			t.Errorf("expected 'from=stable.example.com/v1beta1' annotation")
		}
		if _, found := obj.GetAnnotations()["to"]; !found {
			t.Errorf("expected 'to=stable.example.com/v1beta2' annotation")
		}
	} else {
		if v, found := obj.GetAnnotations()["from"]; found {
			t.Errorf("unexpected 'from' annotation: %s", v)
		}
		if v, found := obj.GetAnnotations()["to"]; found {
			t.Errorf("unexpected 'to' annotation: %s", v)
		}
	}
	if expectLabels {
		if _, found := obj.GetLabels()["from"]; !found {
			t.Errorf("expected 'from=stable.example.com.v1beta1' label")
		}
		if _, found := obj.GetLabels()["to"]; !found {
			t.Errorf("expected 'to=stable.example.com.v1beta2' label")
		}
	} else {
		if v, found := obj.GetLabels()["from"]; found {
			t.Errorf("unexpected 'from' label: %s", v)
		}
		if v, found := obj.GetLabels()["to"]; found {
			t.Errorf("unexpected 'to' label: %s", v)
		}
	}
	if sets.NewString(obj.GetFinalizers()...).Has("foo") {
		t.Errorf("unexpected 'foo' finalizer")
	}
	if obj.GetGeneration() == 42 {
		t.Errorf("unexpected generation 42")
	}
	if v, found, err := unstructured.NestedString(obj.Object, "metadata", "garbage"); err != nil {
		t.Errorf("unexpected error accessing 'metadata.garbage': %v", err)
	} else if found {
		t.Errorf("unexpected 'metadata.garbage': %s", v)
	}
}

// validateUIDMutation 校验 webhook 修改 UID 会被拒绝。
func validateUIDMutation(t *testing.T, ctc *conversionTestContext) {
	ns := ctc.namespace

	t.Logf("Creating object in non-storage version v1beta1")
	storageVersion := "v1beta1"
	ctc.setAndWaitStorageVersion(t, storageVersion)
	name := "uid-mutation-" + storageVersion
	client := ctc.versionedClient(ns, "v1beta2")
	obj, err := client.Create(context.TODO(), newConversionMultiVersionFixture(ns, name, "v1beta2"), metav1.CreateOptions{})
	if err == nil {
		t.Fatalf("expected creation error, but got: %v", obj)
	} else if !strings.Contains(err.Error(), "must have the same UID") {
		t.Errorf("expected 'must have the same UID' error message, but got: %v", err)
	}
}

// validateDefaulting 校验默认化只作用于请求版本和（不持久化的）存储版本。
func validateDefaulting(t *testing.T, ctc *conversionTestContext) {
	if _, defaulting := ctc.crd.Spec.Versions[0].Schema.OpenAPIV3Schema.Properties["defaults"]; !defaulting {
		return
	}

	ns := ctc.namespace
	storageVersion := "v1beta1"

	for _, createVersion := range servedVersions(ctc.crd.Spec.Versions) {
		t.Run(fmt.Sprintf("getting objects created as %s", createVersion.Name), func(t *testing.T) {
			name := "defaulting-" + createVersion.Name
			client := ctc.versionedClient(ns, createVersion.Name)

			fixture := newConversionMultiVersionFixture(ns, name, createVersion.Name)
			if err := unstructured.SetNestedField(fixture.Object, map[string]interface{}{}, "defaults"); err != nil {
				t.Fatal(err)
			}
			created, err := client.Create(context.TODO(), fixture, metav1.CreateOptions{})
			if err != nil {
				t.Fatal(err)
			}

			// 确认默认化发生在：
			// - 反序列化时对请求版本做 no-op 转换
			// - 以存储版本从存储读回时
			// 只有第一处会被持久化。
			defaults, found, err := unstructured.NestedMap(created.Object, "defaults")
			if err != nil {
				t.Fatal(err)
			} else if !found {
				t.Fatalf("expected .defaults to exist")
			}
			expectedLen := 1
			if !createVersion.Storage {
				expectedLen++
			}
			if len(defaults) != expectedLen {
				t.Fatalf("after %s create expected .defaults to have %d values, but got: %v", createVersion.Name, expectedLen, defaults)
			}
			if _, found := defaults[createVersion.Name].(bool); !found {
				t.Errorf("after %s create expected .defaults[%s] to be true, but .defaults is: %v", createVersion.Name, createVersion.Name, defaults)
			}
			if _, found := defaults[storageVersion].(bool); !found {
				t.Errorf("after %s create expected .defaults[%s] to be true because it is the storage version, but .defaults is: %v", createVersion.Name, storageVersion, defaults)
			}

			// 确认只有请求版本的默认值被持久化
			persisted, err := ctc.etcdObjectReader.GetStoredCustomResource(ns, name)
			if err != nil {
				t.Fatal(err)
			}
			if _, found, err := unstructured.NestedBool(persisted.Object, "defaults", storageVersion); err != nil {
				t.Fatal(err)
			} else if createVersion.Name != storageVersion && found {
				t.Errorf("after %s create .defaults[storage version %s] not to be persisted, but got in etcd: %v", createVersion.Name, storageVersion, defaults)
			}

			// 确认读取其他版本时不会默认化该版本，只默认化（不持久化的）存储版本
			for _, v := range servedVersions(ctc.crd.Spec.Versions) {
				if v.Name == createVersion.Name {
					// 创建版本本来就会持久化，无需校验
					continue
				}

				got, err := ctc.versionedClient(ns, v.Name).Get(context.TODO(), created.GetName(), metav1.GetOptions{})
				if err != nil {
					t.Fatal(err)
				}

				if _, found, err := unstructured.NestedBool(got.Object, "defaults", v.Name); err != nil {
					t.Fatal(err)
				} else if v.Name != storageVersion && found {
					t.Errorf("after %s GET expected .defaults[%s] not to be true because only storage version %s is defaulted on read, but .defaults is: %v", v.Name, v.Name, storageVersion, defaults)
				}

				if _, found, err := unstructured.NestedBool(got.Object, "defaults", storageVersion); err != nil {
					t.Fatal(err)
				} else if !found {
					t.Errorf("after non-create, non-storage %s GET expected .defaults[storage version %s] to be true, but .defaults is: %v", v.Name, storageVersion, defaults)
				}
			}
		})
	}
}

// expectConversionFailureMessage 返回一个检查函数，断言各 verb 与子资源均返回包含指定信息的错误。
func expectConversionFailureMessage(id, message string) func(t *testing.T, ctc *conversionTestContext) {
	return func(t *testing.T, ctc *conversionTestContext) {
		ns := ctc.namespace
		clients := ctc.versionedClients(ns)
		var err error
		// 存储版本是 v1beta1，因此这里跳过转换
		obj, err := clients["v1beta1"].Create(context.TODO(), newConversionMultiVersionFixture(ns, id, "v1beta1"), metav1.CreateOptions{})
		if err != nil {
			t.Fatal(err)
		}

		// 手工转换
		objv1beta2 := newConversionMultiVersionFixture(ns, id, "v1beta2")
		meta, _, _ := unstructured.NestedFieldCopy(obj.Object, "metadata")
		unstructured.SetNestedField(objv1beta2.Object, meta, "metadata")

		for _, verb := range []string{"get", "list", "create", "update", "patch", "delete", "deletecollection"} {
			t.Run(verb, func(t *testing.T) {
				switch verb {
				case "get":
					_, err = clients["v1beta2"].Get(context.TODO(), obj.GetName(), metav1.GetOptions{})
				case "list":
					_, err = clients["v1beta2"].List(context.TODO(), metav1.ListOptions{})
				case "create":
					_, err = clients["v1beta2"].Create(context.TODO(), newConversionMultiVersionFixture(ns, id, "v1beta2"), metav1.CreateOptions{})
				case "update":
					_, err = clients["v1beta2"].Update(context.TODO(), objv1beta2, metav1.UpdateOptions{})
				case "patch":
					_, err = clients["v1beta2"].Patch(context.TODO(), obj.GetName(), types.MergePatchType, []byte(`{"metadata":{"annotations":{"patch":"true"}}}`), metav1.PatchOptions{})
				case "delete":
					err = clients["v1beta2"].Delete(context.TODO(), obj.GetName(), metav1.DeleteOptions{})
				case "deletecollection":
					err = clients["v1beta2"].DeleteCollection(context.TODO(), metav1.DeleteOptions{}, metav1.ListOptions{})
				default:
					t.Errorf("unknown verb %q", verb)
				}

				if err == nil {
					t.Errorf("expected error with message %s, but got no error", message)
				} else if !strings.Contains(err.Error(), message) {
					t.Errorf("expected error with message %s, but got %v", message, err)
				}
			})
		}
		for _, subresource := range []string{"status", "scale"} {
			for _, verb := range []string{"get", "update", "patch"} {
				t.Run(fmt.Sprintf("%s-%s", subresource, verb), func(t *testing.T) {
					switch verb {
					case "get":
						_, err = clients["v1beta2"].Get(context.TODO(), obj.GetName(), metav1.GetOptions{}, subresource)
					case "update":
						o := objv1beta2
						if subresource == "scale" {
							o = &unstructured.Unstructured{
								Object: map[string]interface{}{
									"apiVersion": "autoscaling/v1",
									"kind":       "Scale",
									"metadata": map[string]interface{}{
										"name": obj.GetName(),
									},
									"spec": map[string]interface{}{
										"replicas": 42,
									},
								},
							}
						}
						_, err = clients["v1beta2"].Update(context.TODO(), o, metav1.UpdateOptions{}, subresource)
					case "patch":
						_, err = clients["v1beta2"].Patch(context.TODO(), obj.GetName(), types.MergePatchType, []byte(`{"metadata":{"annotations":{"patch":"true"}}}`), metav1.PatchOptions{}, subresource)
					default:
						t.Errorf("unknown subresource verb %q", verb)
					}

					if err == nil {
						t.Errorf("expected error with message %s, but got no error", message)
					} else if !strings.Contains(err.Error(), message) {
						t.Errorf("expected error with message %s, but got %v", message, err)
					}
				})
			}
		}
	}
}

// noopConverter 只改写 apiVersion，不做其他转换。
func noopConverter(desiredAPIVersion string, obj runtime.RawExtension) (runtime.RawExtension, error) {
	u := &unstructured.Unstructured{Object: map[string]interface{}{}}
	if err := json.Unmarshal(obj.Raw, u); err != nil {
		return runtime.RawExtension{}, fmt.Errorf("failed to deserialize object: %s with error: %v", string(obj.Raw), err)
	}
	u.Object["apiVersion"] = desiredAPIVersion
	raw, err := json.Marshal(u)
	if err != nil {
		return runtime.RawExtension{}, fmt.Errorf("failed to serialize object: %v with error: %v", u, err)
	}
	return runtime.RawExtension{Raw: raw}, nil
}

// emptyV1ResponseConverter 返回成功但不含任何转换对象的 v1 响应。
func emptyV1ResponseConverter(review *apiextensionsv1.ConversionReview) (*apiextensionsv1.ConversionReview, error) {
	review.Response = &apiextensionsv1.ConversionResponse{
		UID:              review.Request.UID,
		ConvertedObjects: []runtime.RawExtension{},
		Result:           metav1.Status{Status: "Success"},
	}
	return review, nil
}

// emptyV1Beta1ResponseConverter 返回成功但不含任何转换对象的 v1beta1 响应。
func emptyV1Beta1ResponseConverter(review *apiextensionsv1beta1.ConversionReview) (*apiextensionsv1beta1.ConversionReview, error) {
	review.Response = &apiextensionsv1beta1.ConversionResponse{
		UID:              review.Request.UID,
		ConvertedObjects: []runtime.RawExtension{},
		Result:           metav1.Status{Status: "Success"},
	}
	return review, nil
}

// failureV1ResponseConverter 返回带指定错误信息的失败 v1 响应。
func failureV1ResponseConverter(message string) func(review *apiextensionsv1.ConversionReview) (*apiextensionsv1.ConversionReview, error) {
	return func(review *apiextensionsv1.ConversionReview) (*apiextensionsv1.ConversionReview, error) {
		review.Response = &apiextensionsv1.ConversionResponse{
			UID:              review.Request.UID,
			ConvertedObjects: []runtime.RawExtension{},
			Result:           metav1.Status{Message: message, Status: "Failure"},
		}
		return review, nil
	}
}

// failureV1Beta1ResponseConverter 返回带指定错误信息的失败 v1beta1 响应。
func failureV1Beta1ResponseConverter(message string) func(review *apiextensionsv1beta1.ConversionReview) (*apiextensionsv1beta1.ConversionReview, error) {
	return func(review *apiextensionsv1beta1.ConversionReview) (*apiextensionsv1beta1.ConversionReview, error) {
		review.Response = &apiextensionsv1beta1.ConversionResponse{
			UID:              review.Request.UID,
			ConvertedObjects: []runtime.RawExtension{},
			Result:           metav1.Status{Message: message, Status: "Failure"},
		}
		return review, nil
	}
}

// nontrivialConverter 在各版本之间做字段重命名的真实转换。
func nontrivialConverter(desiredAPIVersion string, obj runtime.RawExtension) (runtime.RawExtension, error) {
	u := &unstructured.Unstructured{Object: map[string]interface{}{}}
	if err := json.Unmarshal(obj.Raw, u); err != nil {
		return runtime.RawExtension{}, fmt.Errorf("failed to deserialize object: %s with error: %v", string(obj.Raw), err)
	}

	currentAPIVersion := u.GetAPIVersion()

	if currentAPIVersion == "stable.example.com/v1beta2" && (desiredAPIVersion == "stable.example.com/v1alpha1" || desiredAPIVersion == "stable.example.com/v1beta1") {
		u.Object["num"] = u.Object["numv2"]
		u.Object["content"] = u.Object["contentv2"]
		delete(u.Object, "numv2")
		delete(u.Object, "contentv2")
	} else if (currentAPIVersion == "stable.example.com/v1alpha1" || currentAPIVersion == "stable.example.com/v1beta1") && desiredAPIVersion == "stable.example.com/v1beta2" {
		u.Object["numv2"] = u.Object["num"]
		u.Object["contentv2"] = u.Object["content"]
		delete(u.Object, "num")
		delete(u.Object, "content")
	} else if currentAPIVersion == "stable.example.com/v1alpha1" && desiredAPIVersion == "stable.example.com/v1beta1" {
		// schema 相同
	} else if currentAPIVersion == "stable.example.com/v1beta1" && desiredAPIVersion == "stable.example.com/v1alpha1" {
		// schema 相同
	} else if currentAPIVersion != desiredAPIVersion {
		return runtime.RawExtension{}, fmt.Errorf("cannot convert from %s to %s", currentAPIVersion, desiredAPIVersion)
	}
	u.Object["apiVersion"] = desiredAPIVersion
	raw, err := json.Marshal(u)
	if err != nil {
		return runtime.RawExtension{}, fmt.Errorf("failed to serialize object: %v with error: %v", u, err)
	}
	return runtime.RawExtension{Raw: raw}, nil
}

// metadataMutatingConverter 在转换时修改 metadata，用于校验哪些字段会被保留。
func metadataMutatingConverter(desiredAPIVersion string, obj runtime.RawExtension) (runtime.RawExtension, error) {
	obj, err := nontrivialConverter(desiredAPIVersion, obj)
	if err != nil {
		return runtime.RawExtension{}, err
	}

	u := &unstructured.Unstructured{Object: map[string]interface{}{}}
	if err := json.Unmarshal(obj.Raw, u); err != nil {
		return runtime.RawExtension{}, fmt.Errorf("failed to deserialize object: %s with error: %v", string(obj.Raw), err)
	}

	// 不要修改 marker 和 probe 对象
	if !strings.Contains(u.GetName(), "mutation") {
		return obj, nil
	}

	currentAPIVersion := u.GetAPIVersion()

	// 修改 annotations，这些应被持久化。
	annotations := u.GetAnnotations()
	if annotations == nil {
		annotations = map[string]string{}
	}
	annotations["from"] = currentAPIVersion
	annotations["to"] = desiredAPIVersion
	u.SetAnnotations(annotations)

	// 修改 labels，这些应被持久化。
	labels := u.GetLabels()
	if labels == nil {
		labels = map[string]string{}
	}
	labels["from"] = strings.Replace(currentAPIVersion, "/", ".", 1) // 把 / 换成 .，因为 label 取值不允许 /
	labels["to"] = strings.Replace(desiredAPIVersion, "/", ".", 1)
	u.SetLabels(labels)

	// 修改其他字段，这些应被忽略。
	u.SetGeneration(42)
	u.SetOwnerReferences([]metav1.OwnerReference{{
		APIVersion:         "v1",
		Kind:               "Namespace",
		Name:               "default",
		UID:                "1234",
		Controller:         nil,
		BlockOwnerDeletion: nil,
	}})
	u.SetResourceVersion("42")
	u.SetFinalizers([]string{"foo"})
	if err := unstructured.SetNestedField(u.Object, "foo", "metadata", "garbage"); err != nil {
		return runtime.RawExtension{}, err
	}

	raw, err := json.Marshal(u)
	if err != nil {
		return runtime.RawExtension{}, fmt.Errorf("failed to serialize object: %v with error: %v", u, err)
	}
	return runtime.RawExtension{Raw: raw}, nil
}

// uidMutatingConverter 在转换时修改 UID，用于校验 UID 不可变。
func uidMutatingConverter(desiredAPIVersion string, obj runtime.RawExtension) (runtime.RawExtension, error) {
	u := &unstructured.Unstructured{Object: map[string]interface{}{}}
	if err := json.Unmarshal(obj.Raw, u); err != nil {
		return runtime.RawExtension{}, fmt.Errorf("failed to deserialize object: %s with error: %v", string(obj.Raw), err)
	}

	// 不要修改 marker 和 probe 对象
	if strings.Contains(u.GetName(), "mutation") {
		// 修改其他字段，这些应被忽略。
		if err := unstructured.SetNestedField(u.Object, "42", "metadata", "uid"); err != nil {
			return runtime.RawExtension{}, err
		}
	}

	u.Object["apiVersion"] = desiredAPIVersion
	raw, err := json.Marshal(u)
	if err != nil {
		return runtime.RawExtension{}, fmt.Errorf("failed to serialize object: %v with error: %v", u, err)
	}
	return runtime.RawExtension{Raw: raw}, nil
}

// getUnexpectedVersionCheckConverter 返回的转换器在收到指定版本时直接使测试失败。
func getUnexpectedVersionCheckConverter(t *testing.T, version string) ObjectConverterFunc {
	return func(desiredAPIVersion string, obj runtime.RawExtension) (runtime.RawExtension, error) {
		if desiredAPIVersion == "stable.example.com/"+version {
			t.Fatalf("webhook received unexpected version: %s", version)
		}

		return nontrivialConverter(desiredAPIVersion, obj)
	}
}

// newConversionTestContext 创建 CRD 并构造转换测试上下文，返回拆解函数。
func newConversionTestContext(t *testing.T, apiExtensionsClient clientset.Interface, dynamicClient dynamic.Interface, etcdObjectReader *storage.EtcdObjectReader, v1CRD *apiextensionsv1.CustomResourceDefinition) (func(), *conversionTestContext) {
	v1CRD, err := fixtures.CreateNewV1CustomResourceDefinition(v1CRD, apiExtensionsClient, dynamicClient)
	if err != nil {
		t.Fatal(err)
	}
	crd, err := apiExtensionsClient.ApiextensionsV1().CustomResourceDefinitions().Get(context.TODO(), v1CRD.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}

	tearDown := func() {
		if err := fixtures.DeleteV1CustomResourceDefinition(crd, apiExtensionsClient); err != nil {
			t.Fatal(err)
		}
	}

	return tearDown, &conversionTestContext{apiExtensionsClient: apiExtensionsClient, dynamicClient: dynamicClient, crd: crd, etcdObjectReader: etcdObjectReader}
}

// conversionTestContext 保存转换测试所需的客户端与 CRD 状态。
type conversionTestContext struct {
	namespace           string
	apiExtensionsClient clientset.Interface
	dynamicClient       dynamic.Interface
	crd                 *apiextensionsv1.CustomResourceDefinition
	etcdObjectReader    *storage.EtcdObjectReader
}

// versionedClient 返回指定命名空间与版本的动态资源客户端。
func (c *conversionTestContext) versionedClient(ns string, version string) dynamic.ResourceInterface {
	gvr := schema.GroupVersionResource{Group: c.crd.Spec.Group, Version: version, Resource: c.crd.Spec.Names.Plural}
	if c.crd.Spec.Scope != apiextensionsv1.ClusterScoped {
		return c.dynamicClient.Resource(gvr).Namespace(ns)
	}
	return c.dynamicClient.Resource(gvr)
}

// versionedClients 返回所有 served 版本的动态资源客户端。
func (c *conversionTestContext) versionedClients(ns string) map[string]dynamic.ResourceInterface {
	ret := map[string]dynamic.ResourceInterface{}
	for _, v := range servedVersions(c.crd.Spec.Versions) {
		ret[v.Name] = c.versionedClient(ns, v.Name)
	}
	return ret
}

// setConversionWebhook 将 CRD 的转换策略切换为 webhook。
func (c *conversionTestContext) setConversionWebhook(t *testing.T, webhookClientConfig *apiextensionsv1.WebhookClientConfig, reviewVersions []string) {
	crd, err := c.apiExtensionsClient.ApiextensionsV1().CustomResourceDefinitions().Get(context.TODO(), c.crd.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	crd.Spec.Conversion = &apiextensionsv1.CustomResourceConversion{
		Strategy: apiextensionsv1.WebhookConverter,
		Webhook: &apiextensionsv1.WebhookConversion{
			ClientConfig:             webhookClientConfig,
			ConversionReviewVersions: reviewVersions,
		},
	}
	crd, err = c.apiExtensionsClient.ApiextensionsV1().CustomResourceDefinitions().Update(context.TODO(), crd, metav1.UpdateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	c.crd = crd
}

// removeConversionWebhook 将 CRD 的转换策略恢复为 None。
func (c *conversionTestContext) removeConversionWebhook(t *testing.T) {
	crd, err := c.apiExtensionsClient.ApiextensionsV1().CustomResourceDefinitions().Get(context.TODO(), c.crd.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	crd.Spec.Conversion = &apiextensionsv1.CustomResourceConversion{
		Strategy: apiextensionsv1.NoneConverter,
	}

	crd, err = c.apiExtensionsClient.ApiextensionsV1().CustomResourceDefinitions().Update(context.TODO(), crd, metav1.UpdateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	c.crd = crd
}

// setAndWaitStorageVersion 设置存储版本并等待 etcd 中已有对象完成版本迁移。
func (c *conversionTestContext) setAndWaitStorageVersion(t *testing.T, version string) {
	c.setStorageVersion(t, version)

	// 创建 probe 对象。版本应取默认版本，以免测试准备阶段触发 webhook 调用。
	client := c.versionedClient("probe", "v1beta1")
	name := fmt.Sprintf("probe-%v", uuid.NewUUID())
	storageProbe, err := client.Create(context.TODO(), newConversionMultiVersionFixture("probe", name, "v1beta1"), metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}

	// 持续更新对象，等待 etcd 中的对象变为目标存储版本。
	c.waitForStorageVersion(t, version, c.versionedClient(storageProbe.GetNamespace(), "v1beta1"), storageProbe)

	err = client.Delete(context.TODO(), name, metav1.DeleteOptions{})
	if err != nil {
		t.Fatal(err)
	}
}

// setStorageVersion 将 CRD 的存储版本切换为指定版本。
func (c *conversionTestContext) setStorageVersion(t *testing.T, version string) {
	crd, err := c.apiExtensionsClient.ApiextensionsV1().CustomResourceDefinitions().Get(context.TODO(), c.crd.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	for i, v := range crd.Spec.Versions {
		crd.Spec.Versions[i].Storage = v.Name == version
	}
	crd, err = c.apiExtensionsClient.ApiextensionsV1().CustomResourceDefinitions().Update(context.TODO(), crd, metav1.UpdateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	c.crd = crd
}

// waitForStorageVersion 通过反复 patch 对象，等待其存储版本变为指定版本。
func (c *conversionTestContext) waitForStorageVersion(t *testing.T, version string, versionedClient dynamic.ResourceInterface, obj *unstructured.Unstructured) *unstructured.Unstructured {
	if err := c.etcdObjectReader.WaitForStorageVersion(version, obj.GetNamespace(), obj.GetName(), 30*time.Second, func() {
		if _, err := versionedClient.Patch(context.TODO(), obj.GetName(), types.MergePatchType, []byte(`{}`), metav1.PatchOptions{}); err != nil {
			t.Fatalf("failed to update object: %v", err)
		}
	}); err != nil {
		t.Fatalf("failed waiting for storage version %s: %v", version, err)
	}

	t.Logf("Effective storage version: %s", version)

	return obj
}

// setServed 设置 CRD 中指定版本的 served 状态。
func (c *conversionTestContext) setServed(t *testing.T, version string, served bool) {
	crd, err := c.apiExtensionsClient.ApiextensionsV1().CustomResourceDefinitions().Get(context.TODO(), c.crd.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	for i, v := range crd.Spec.Versions {
		if v.Name == version {
			crd.Spec.Versions[i].Served = served
		}
	}
	crd, err = c.apiExtensionsClient.ApiextensionsV1().CustomResourceDefinitions().Update(context.TODO(), crd, metav1.UpdateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	c.crd = crd
}

// waitForServed 等待 CRD 的 served 变更在客户端读取结果中生效。
func (c *conversionTestContext) waitForServed(t *testing.T, version string, served bool, versionedClient dynamic.ResourceInterface, obj *unstructured.Unstructured) {
	timeout := 30 * time.Second
	waitCh := time.After(timeout)
	for {
		obj, err := versionedClient.Get(context.TODO(), obj.GetName(), metav1.GetOptions{})
		if (err == nil && served) || (errors.IsNotFound(err) && served == false) {
			return
		}
		select {
		case <-waitCh:
			t.Fatalf("Timed out after %v waiting for CRD served=%t for version %s for %v. Last error: %v", timeout, served, version, obj, err)
		case <-time.After(10 * time.Millisecond):
		}
	}
}

// multiVersionFixture 是包含多个版本、不同 schema 与默认值的 CRD 测试夹具。
var multiVersionFixture = &apiextensionsv1.CustomResourceDefinition{
	ObjectMeta: metav1.ObjectMeta{Name: "multiversion.stable.example.com"},
	Spec: apiextensionsv1.CustomResourceDefinitionSpec{
		Group: "stable.example.com",
		Names: apiextensionsv1.CustomResourceDefinitionNames{
			Plural:     "multiversion",
			Singular:   "multiversion",
			Kind:       "MultiVersion",
			ShortNames: []string{"mv"},
			ListKind:   "MultiVersionList",
			Categories: []string{"all"},
		},
		Scope:                 apiextensionsv1.NamespaceScoped,
		PreserveUnknownFields: false,
		Versions: []apiextensionsv1.CustomResourceDefinitionVersion{
			{
				// 存储版本，schema 与 v1alpha1 相同
				Name:    "v1beta1",
				Served:  true,
				Storage: true,
				Subresources: &apiextensionsv1.CustomResourceSubresources{
					Status: &apiextensionsv1.CustomResourceSubresourceStatus{},
					Scale: &apiextensionsv1.CustomResourceSubresourceScale{
						SpecReplicasPath:   ".spec.num.num1",
						StatusReplicasPath: ".status.num.num2",
					},
				},
				Schema: &apiextensionsv1.CustomResourceValidation{
					OpenAPIV3Schema: &apiextensionsv1.JSONSchemaProps{
						Type: "object",
						Properties: map[string]apiextensionsv1.JSONSchemaProps{
							"content": {
								Type: "object",
								Properties: map[string]apiextensionsv1.JSONSchemaProps{
									"key": {Type: "string"},
								},
							},
							"num": {
								Type: "object",
								Properties: map[string]apiextensionsv1.JSONSchemaProps{
									"num1": {Type: "integer"},
									"num2": {Type: "integer"},
								},
							},
							"defaults": {
								Type: "object",
								Properties: map[string]apiextensionsv1.JSONSchemaProps{
									"v1alpha1": {Type: "boolean"},
									"v1alpha2": {Type: "boolean"},
									"v1beta1":  {Type: "boolean", Default: jsonPtr(true)},
									"v1beta2":  {Type: "boolean"},
								},
							},
						},
					},
				},
			},
			{
				// schema 与 v1beta1 相同
				Name:    "v1alpha1",
				Served:  true,
				Storage: false,
				Subresources: &apiextensionsv1.CustomResourceSubresources{
					Status: &apiextensionsv1.CustomResourceSubresourceStatus{},
					Scale: &apiextensionsv1.CustomResourceSubresourceScale{
						SpecReplicasPath:   ".spec.num.num1",
						StatusReplicasPath: ".status.num.num2",
					},
				},
				Schema: &apiextensionsv1.CustomResourceValidation{
					OpenAPIV3Schema: &apiextensionsv1.JSONSchemaProps{
						Type: "object",
						Properties: map[string]apiextensionsv1.JSONSchemaProps{
							"content": {
								Type: "object",
								Properties: map[string]apiextensionsv1.JSONSchemaProps{
									"key": {Type: "string"},
								},
							},
							"num": {
								Type: "object",
								Properties: map[string]apiextensionsv1.JSONSchemaProps{
									"num1": {Type: "integer"},
									"num2": {Type: "integer"},
								},
							},
							"defaults": {
								Type: "object",
								Properties: map[string]apiextensionsv1.JSONSchemaProps{
									"v1alpha1": {Type: "boolean", Default: jsonPtr(true)},
									"v1alpha2": {Type: "boolean"},
									"v1beta1":  {Type: "boolean"},
									"v1beta2":  {Type: "boolean"},
								},
							},
						},
					},
				},
			},
			{
				// schema 与 v1beta1 和 v1alpha1 均不同
				Name:    "v1beta2",
				Served:  true,
				Storage: false,
				Subresources: &apiextensionsv1.CustomResourceSubresources{
					Status: &apiextensionsv1.CustomResourceSubresourceStatus{},
					Scale: &apiextensionsv1.CustomResourceSubresourceScale{
						SpecReplicasPath:   ".spec.num.num1",
						StatusReplicasPath: ".status.num.num2",
					},
				},
				Schema: &apiextensionsv1.CustomResourceValidation{
					OpenAPIV3Schema: &apiextensionsv1.JSONSchemaProps{
						Type: "object",
						Properties: map[string]apiextensionsv1.JSONSchemaProps{
							"contentv2": {
								Type: "object",
								Properties: map[string]apiextensionsv1.JSONSchemaProps{
									"key": {Type: "string"},
								},
							},
							"numv2": {
								Type: "object",
								Properties: map[string]apiextensionsv1.JSONSchemaProps{
									"num1": {Type: "integer"},
									"num2": {Type: "integer"},
								},
							},
							"defaults": {
								Type: "object",
								Properties: map[string]apiextensionsv1.JSONSchemaProps{
									"v1alpha1": {Type: "boolean"},
									"v1alpha2": {Type: "boolean"},
									"v1beta1":  {Type: "boolean"},
									"v1beta2":  {Type: "boolean", Default: jsonPtr(true)},
								},
							},
						},
					},
				},
			},
			{
				// schema 与 v1beta1 相同，但不 served
				Name:    "v1alpha2",
				Served:  false,
				Storage: false,
				Subresources: &apiextensionsv1.CustomResourceSubresources{
					Status: &apiextensionsv1.CustomResourceSubresourceStatus{},
					Scale: &apiextensionsv1.CustomResourceSubresourceScale{
						SpecReplicasPath:   ".spec.num.num1",
						StatusReplicasPath: ".status.num.num2",
					},
				},
				Schema: &apiextensionsv1.CustomResourceValidation{
					OpenAPIV3Schema: &apiextensionsv1.JSONSchemaProps{
						Type: "object",
						Properties: map[string]apiextensionsv1.JSONSchemaProps{
							"content": {
								Type: "object",
								Properties: map[string]apiextensionsv1.JSONSchemaProps{
									"key": {Type: "string"},
								},
							},
							"num": {
								Type: "object",
								Properties: map[string]apiextensionsv1.JSONSchemaProps{
									"num1": {Type: "integer"},
									"num2": {Type: "integer"},
								},
							},
							"defaults": {
								Type: "object",
								Properties: map[string]apiextensionsv1.JSONSchemaProps{
									"v1alpha1": {Type: "boolean"},
									"v1alpha2": {Type: "boolean", Default: jsonPtr(true)},
									"v1beta1":  {Type: "boolean"},
									"v1beta2":  {Type: "boolean"},
								},
							},
						},
					},
				},
			},
		},
	},
}

// newConversionMultiVersionFixture 按版本构造 MultiVersion 自定义资源。
func newConversionMultiVersionFixture(namespace, name, version string) *unstructured.Unstructured {
	u := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "stable.example.com/" + version,
			"kind":       "MultiVersion",
			"metadata": map[string]interface{}{
				"namespace": namespace,
				"name":      name,
			},
		},
	}

	switch version {
	case "v1alpha1":
		u.Object["content"] = map[string]interface{}{
			"key": "value",
		}
		u.Object["num"] = map[string]interface{}{
			"num1": int64(1),
			"num2": int64(1000000),
		}
	case "v1alpha2":
		u.Object["content"] = map[string]interface{}{
			"key": "value",
		}
		u.Object["num"] = map[string]interface{}{
			"num1": int64(1),
			"num2": int64(1000000),
		}
	case "v1beta1":
		u.Object["content"] = map[string]interface{}{
			"key": "value",
		}
		u.Object["num"] = map[string]interface{}{
			"num1": int64(1),
			"num2": int64(1000000),
		}
	case "v1beta2":
		u.Object["contentv2"] = map[string]interface{}{
			"key": "value",
		}
		u.Object["numv2"] = map[string]interface{}{
			"num1": int64(1),
			"num2": int64(1000000),
		}
	default:
		panic(fmt.Sprintf("unknown version %s", version))
	}

	return u
}

// verifyMultiVersionObject 校验对象在指定版本下的字段与预期一致。
func verifyMultiVersionObject(t *testing.T, v string, obj *unstructured.Unstructured) {
	j := runtime.DeepCopyJSON(obj.Object)

	if expected := "stable.example.com/" + v; obj.GetAPIVersion() != expected {
		t.Errorf("unexpected apiVersion %q, expected %q", obj.GetAPIVersion(), expected)
		return
	}

	delete(j, "metadata")

	var expected = map[string]map[string]interface{}{
		"v1alpha1": {
			"apiVersion": "stable.example.com/v1alpha1",
			"kind":       "MultiVersion",
			"content": map[string]interface{}{
				"key": "value",
			},
			"num": map[string]interface{}{
				"num1": int64(1),
				"num2": int64(1000000),
			},
		},
		"v1beta1": {
			"apiVersion": "stable.example.com/v1beta1",
			"kind":       "MultiVersion",
			"content": map[string]interface{}{
				"key": "value",
			},
			"num": map[string]interface{}{
				"num1": int64(1),
				"num2": int64(1000000),
			},
		},
		"v1beta2": {
			"apiVersion": "stable.example.com/v1beta2",
			"kind":       "MultiVersion",
			"contentv2": map[string]interface{}{
				"key": "value",
			},
			"numv2": map[string]interface{}{
				"num1": int64(1),
				"num2": int64(1000000),
			},
		},
	}
	if !reflect.DeepEqual(expected[v], j) {
		t.Errorf("unexpected %s object: %s", v, cmp.Diff(expected[v], j))
	}
}

// closeOnCall 包装 handler，并在首次被调用时关闭返回的 channel。
func closeOnCall(h http.Handler) (chan struct{}, http.Handler) {
	ch := make(chan struct{})
	once := sync.Once{}
	return ch, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		once.Do(func() {
			close(ch)
		})
		h.ServeHTTP(w, r)
	})
}

// jsonPtr 将任意值序列化为 JSON 并包装为指针。
func jsonPtr(x interface{}) *apiextensionsv1.JSON {
	bs, err := json.Marshal(x)
	if err != nil {
		panic(err)
	}
	ret := apiextensionsv1.JSON{Raw: bs}
	return &ret
}

// servedVersions 过滤出所有 served 的版本。
func servedVersions(versions []apiextensionsv1.CustomResourceDefinitionVersion) (served []apiextensionsv1.CustomResourceDefinitionVersion) {
	for _, v := range versions {
		if v.Served {
			served = append(served, v)
		}
	}

	return served
}

// TestWebhookConversion_WhitespaceCABundleEtcdBypass 校验仅含空白的 CABundle 被视为空，
// 读对象时不会因此走转换流程。
func TestWebhookConversion_WhitespaceCABundleEtcdBypass(t *testing.T) {
	// 准备 server 和客户端
	apiServerTearDown, config, options, err := fixtures.StartDefaultServer(t)
	if err != nil {
		t.Fatal(err)
	}

	apiExtensionsClient, err := clientset.NewForConfig(config)
	if err != nil {
		t.Fatal(err)
	}
	dynamicClient, err := dynamic.NewForConfig(config)
	if err != nil {
		t.Fatal(err)
	}

	crd := multiVersionFixture.DeepCopy()

	RESTOptionsGetter := serveroptions.NewCRDRESTOptionsGetter(*options.RecommendedOptions.Etcd, nil, nil)
	restOptions, err := RESTOptionsGetter.GetRESTOptions(schema.GroupResource{Group: crd.Spec.Group, Resource: crd.Spec.Names.Plural}, nil)
	if err != nil {
		t.Fatal(err)
	}
	etcdClient, _, err := storage.GetEtcdClients(restOptions.StorageConfig.Transport)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err = etcdClient.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()
	etcdObjectReader := storage.NewEtcdObjectReader(etcdClient, &restOptions, crd)

	ctcTearDown, ctc := newConversionTestContext(t, apiExtensionsClient, dynamicClient, etcdObjectReader, crd)
	defer ctcTearDown()

	ns := "whitespace-cabundle"
	version := "v1beta1"
	client := ctc.versionedClient(ns, version)

	// 创建 CR 实例
	name := "test"
	obj, err := client.Create(context.TODO(), newConversionMultiVersionFixture(ns, name, version), metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("failed to create CR instance: %v", err)
	}
	verifyMultiVersionObject(t, "v1beta1", obj)

	// 配置 webhook 转换
	webhookTearDown, webhookClientConfig, err := StartConversionWebhookServer(NewObjectConverterWebhookHandler(t, noopConverter))
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		// 保证在 apiserver 及其 etcd watch 完全关闭之前，转换 webhook 一直可用。
		apiServerTearDown()
		webhookTearDown()
	})

	crd.Spec.Conversion = &apiextensionsv1.CustomResourceConversion{
		Strategy: apiextensionsv1.WebhookConverter,
		Webhook: &apiextensionsv1.WebhookConversion{
			ClientConfig:             webhookClientConfig,
			ConversionReviewVersions: []string{"v1beta1"},
		},
	}
	crd.TypeMeta = metav1.TypeMeta{
		APIVersion: "apiextensions.k8s.io/v1",
		Kind:       "CustomResourceDefinition",
	}

	// 从 API server 获取最新 CRD，以拿到当前 resourceVersion
	crdFromAPI, err := ctc.apiExtensionsClient.ApiextensionsV1().CustomResourceDefinitions().Get(
		context.TODO(), crd.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	crd.ResourceVersion = crdFromAPI.ResourceVersion
	_, err = ctc.apiExtensionsClient.ApiextensionsV1().CustomResourceDefinitions().Update(
		context.TODO(), crd, metav1.UpdateOptions{})
	if err != nil {
		t.Fatal(err)
	}

	// 把 CABundle 替换为空白字符
	crd.Spec.Conversion.Webhook.ClientConfig.CABundle = []byte("    \n\t   ")
	err = etcdObjectReader.SetStoredCustomResourceDefinition(crd.Name, crd)
	if err != nil {
		t.Fatal(err)
	}

	// 尝试读取 CR 实例（应成功，因为不需要转换）
	obj, err = ctc.etcdObjectReader.GetStoredCustomResource(ns, name)
	if err != nil {
		t.Fatal(err)
	}
	verifyMultiVersionObject(t, "v1beta1", obj)

}

// dynamicWebhookHandler 是线程安全的 http.Handler，可在运行时替换底层 delegate handler。
// 这样可以共享同一个 server 实例给需要不同行为的多个测试用例。
type dynamicWebhookHandler struct {
	mu       sync.RWMutex
	delegate http.Handler
}

// ServeHTTP 实现 http.Handler，将请求转交给当前配置的 handler。
// 若未设置 handler，则返回 500 错误。
func (h *dynamicWebhookHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	if h.delegate != nil {
		h.delegate.ServeHTTP(w, r)
	} else {
		http.Error(w, "unexpected call", http.StatusInternalServerError)
	}
}

// set 安全地替换底层 delegate handler。
func (h *dynamicWebhookHandler) set(delegate http.Handler) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.delegate = delegate
}
