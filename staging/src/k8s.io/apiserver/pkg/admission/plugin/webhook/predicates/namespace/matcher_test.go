/*
Copyright 2017 The Kubernetes Authors.

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

package namespace_test

import (
	"reflect"
	"testing"

	registrationv1 "k8s.io/api/admissionregistration/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apiserver/pkg/admission"
	"k8s.io/apiserver/pkg/admission/plugin/webhook"
	"k8s.io/apiserver/pkg/admission/plugin/webhook/predicates/namespace"
)

// fakeNamespaceLister 是只支持按名获取的测试替身，模拟 indexer 的 namespace
// lister；List 未实现，因为被测逻辑只按请求的 namespace 名查询。
type fakeNamespaceLister struct {
	namespaces map[string]*corev1.Namespace
}

// List 未使用，返回空以保持接口完整。
func (f fakeNamespaceLister) List(selector labels.Selector) (ret []*corev1.Namespace, err error) {
	return nil, nil
}

// Get 按名返回命名空间，缺失时返回标准 NotFound 错误。
func (f fakeNamespaceLister) Get(name string) (*corev1.Namespace, error) {
	ns, ok := f.namespaces[name]
	if ok {
		return ns, nil
	}
	return nil, errors.NewNotFound(corev1.Resource("namespaces"), name)
}

// TestGetNamespaceLabels 验证获取命名空间标签时的取值来源：
// 创建/更新用请求中的对象，删除与其他资源请求回落到缓存，
// 且 namespace/finalizer 这类子资源按命名空间本身处理。
func TestGetNamespaceLabels(t *testing.T) {
	namespace1Labels := map[string]string{
		"runlevel": "1",
	}
	namespace1 := corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name:   "1",
			Labels: namespace1Labels,
		},
	}
	namespace2Labels := map[string]string{
		"runlevel": "2",
	}
	namespace2 := corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name:   "2",
			Labels: namespace2Labels,
		},
	}
	namespaceLister := fakeNamespaceLister{map[string]*corev1.Namespace{
		"1": &namespace1,
	},
	}

	tests := []struct {
		name           string
		attr           admission.Attributes
		expectedLabels map[string]string
	}{
		{
			// 创建请求尚无缓存中的旧对象，标签必须取自待创建对象。
			name:           "request is for creating namespace, the labels should be from the object itself",
			attr:           admission.NewAttributesRecord(&namespace2, nil, schema.GroupVersionKind{}, "", namespace2.Name, schema.GroupVersionResource{Resource: "namespaces"}, "", admission.Create, &metav1.CreateOptions{}, false, nil),
			expectedLabels: namespace2Labels,
		},
		{
			// 更新请求可能同时带旧对象，但选择器应基于更新后的新对象求值。
			name:           "request is for updating namespace, the labels should be from the new object",
			attr:           admission.NewAttributesRecord(&namespace2, nil, schema.GroupVersionKind{}, namespace2.Name, namespace2.Name, schema.GroupVersionResource{Resource: "namespaces"}, "", admission.Update, &metav1.UpdateOptions{}, false, nil),
			expectedLabels: namespace2Labels,
		},
		{
			// 删除请求的对象已不可用（本例传入的是 namespace2），只能回落到
			// 缓存中同名的 namespace1 取其标签。
			name:           "request is for deleting namespace, the labels should be from the cache",
			attr:           admission.NewAttributesRecord(&namespace2, nil, schema.GroupVersionKind{}, namespace1.Name, namespace1.Name, schema.GroupVersionResource{Resource: "namespaces"}, "", admission.Delete, &metav1.DeleteOptions{}, false, nil),
			expectedLabels: namespace1Labels,
		},
		{
			// 子资源请求（namespaces/finalizers）没有完整对象，仍应按所属
			// 命名空间从缓存取标签。
			name:           "request is for namespace/finalizer",
			attr:           admission.NewAttributesRecord(nil, nil, schema.GroupVersionKind{}, namespace1.Name, "mock-name", schema.GroupVersionResource{Resource: "namespaces"}, "finalizers", admission.Create, &metav1.CreateOptions{}, false, nil),
			expectedLabels: namespace1Labels,
		},
		{
			// 普通命名空间级资源（pod）没有自身对象传入，命名空间标签来自缓存。
			name:           "request is for pod",
			attr:           admission.NewAttributesRecord(nil, nil, schema.GroupVersionKind{}, namespace1.Name, "mock-name", schema.GroupVersionResource{Resource: "pods"}, "", admission.Create, &metav1.CreateOptions{}, false, nil),
			expectedLabels: namespace1Labels,
		},
	}
	matcher := namespace.Matcher{
		NamespaceLister: namespaceLister,
	}
	for _, tt := range tests {
		actualLabels, err := matcher.GetNamespaceLabels(tt.attr)
		if err != nil {
			t.Error(err)
		}
		if !reflect.DeepEqual(actualLabels, tt.expectedLabels) {
			t.Errorf("expected labels to be %#v, got %#v", tt.expectedLabels, actualLabels)
		}
	}
}

// TestNotExemptClusterScopedResource 确保集群级资源（node）不会被当作
// “namespace 对象本身”而豁免于 namespaceSelector：只有 Namespace 资源才享有
// 该特例，否则带 selector 的 webhook 会意外丢失集群级请求。
func TestNotExemptClusterScopedResource(t *testing.T) {
	hook := &registrationv1.ValidatingWebhook{
		NamespaceSelector: &metav1.LabelSelector{},
	}
	attr := admission.NewAttributesRecord(nil, nil, schema.GroupVersionKind{}, "", "mock-name", schema.GroupVersionResource{Version: "v1", Resource: "nodes"}, "", admission.Create, &metav1.CreateOptions{}, false, nil)
	matcher := namespace.Matcher{}
	matches, err := matcher.MatchNamespaceSelector(webhook.NewValidatingWebhookAccessor("mock-hook", "mock-cfg", hook), attr)
	if err != nil {
		t.Fatal(err)
	}
	if !matches {
		t.Errorf("cluster scoped resources (but not a namespace) should not be exempted from webhooks")
	}
}
