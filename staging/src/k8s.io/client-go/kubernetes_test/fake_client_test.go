/*
Copyright 2020 The Kubernetes Authors.

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

package kubernetes_test

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/util/watchlist"
)

// TestDoesClientSupportWatchListSemantics 确认 fake clientset 仍使用传统 List+Watch，
// 而非 WatchList 语义；依赖该差异的调用方（如 informer 的 feature gate 分支）才有稳定行为。
func TestDoesClientSupportWatchListSemantics(t *testing.T) {
	target := fake.NewClientset()
	if !watchlist.DoesClientNotSupportWatchListSemantics(target) {
		t.Fatalf("NewSimpleClientset should NOT support WatchList semantics")
	}
}

// 本测试证明 kube fake client 不返回 GVK；这与真实客户端一致（见下方测试），
// 除非解码行为或字面量创建对象（`&corev1.ConfigMap{}`）的行为改变，否则不应修改。
//
// 即：fake 客户端返回的对象 TypeMeta 为空，与真实客户端解码后的行为一致，
// 因此调用方不能依赖 GetObjectKind().GroupVersionKind() 判断类型。
func Test_ConfigMapFakeClient(t *testing.T) {
	fakeKubeClient := fake.NewSimpleClientset(&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: "foo-ns", Name: "foo-name"}})
	cm, err := fakeKubeClient.CoreV1().ConfigMaps("foo-ns").Get(context.TODO(), "foo-name", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if cm.GetObjectKind().GroupVersionKind() != (schema.GroupVersionKind{}) {
		t.Fatal(cm.GetObjectKind().GroupVersionKind())
	}
	cmList, err := fakeKubeClient.CoreV1().ConfigMaps("foo-ns").List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if cmList.GetObjectKind().GroupVersionKind() != (schema.GroupVersionKind{}) {
		t.Fatal(cmList.GetObjectKind().GroupVersionKind())
	}
}

// 本测试检查真实客户端的解码行为，以确保上面测过的 fake client 与之保持一致。
// 复现生成代码中 Get 的解码路径：即使响应体带 apiVersion/kind，解码到目标类型后也不会设置 GVK。
func TestGetDecoding(t *testing.T) {
	// 这里复用真实 ConfigMap Get API 的逻辑，以证明生成的客户端不返回 GVK。
	mediaTypes := scheme.Codecs.WithoutConversion().SupportedMediaTypes()
	info, ok := runtime.SerializerInfoForMediaType(mediaTypes, "application/json")
	if !ok {
		t.Fatal("missing serializer")
	}
	decoder := scheme.Codecs.WithoutConversion().DecoderToVersion(info.Serializer, corev1.SchemeGroupVersion)

	body := []byte(`{"apiVersion": "v1", "kind": "ConfigMap", "metadata":{"Namespace":"foo","Name":"bar"}}`)
	obj := &corev1.ConfigMap{}
	out, _, err := decoder.Decode(body, nil, obj)
	if err != nil || out != obj {
		t.Fatal(err)
	}

	if obj.GetObjectKind().GroupVersionKind() != (schema.GroupVersionKind{}) {
		t.Fatal(obj.GetObjectKind().GroupVersionKind())
	}
}

// 本测试检查真实客户端的解码行为，以确保上面测过的 fake client 与之保持一致。
// 与 Get 同理，List 结果也不会携带 GVK。
func TestListDecoding(t *testing.T) {
	// 这里复用真实 ConfigMap Get API 的逻辑，以证明生成的客户端不返回 GVK。
	mediaTypes := scheme.Codecs.WithoutConversion().SupportedMediaTypes()
	info, ok := runtime.SerializerInfoForMediaType(mediaTypes, "application/json")
	if !ok {
		t.Fatal("missing serializer")
	}
	decoder := scheme.Codecs.WithoutConversion().DecoderToVersion(info.Serializer, corev1.SchemeGroupVersion)

	body := []byte(`{"apiVersion": "v1", "kind": "ConfigMapList", "items":[]}`)
	obj := &corev1.ConfigMapList{}
	out, _, err := decoder.Decode(body, nil, obj)
	if err != nil || out != obj {
		t.Fatal(err)
	}

	if obj.GetObjectKind().GroupVersionKind() != (schema.GroupVersionKind{}) {
		t.Fatal(obj.GetObjectKind().GroupVersionKind())
	}
}
