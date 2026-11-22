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

package resource

import (
	"context"
	"fmt"
	"reflect"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
)

// CRDGetter is a function that can download the list of GVK for all
// CRDs.
//
// 之所以抽象成函数而不是直接依赖 dynamic client，是为了让发现逻辑
// 可被测试替换，也避免 resource 包耦合具体的 client 实现。
type CRDGetter func() ([]schema.GroupKind, error)

// CRDFromDynamic 返回一个基于 dynamic client 的 CRDGetter。
// 只取 spec.group 与 spec.names.kind：判断“是否已有 CRD”不需要版本信息，
// 而 GroupKind 足以覆盖不同版本间的同一 CRD。
func CRDFromDynamic(client dynamic.Interface) CRDGetter {
	return func() ([]schema.GroupKind, error) {
		list, err := client.Resource(schema.GroupVersionResource{
			Group:    "apiextensions.k8s.io",
			Version:  "v1",
			Resource: "customresourcedefinitions",
		}).List(context.TODO(), metav1.ListOptions{})
		if err != nil {
			return nil, fmt.Errorf("failed to list CRDs: %v", err)
		}
		if list == nil {
			return nil, nil
		}

		gks := []schema.GroupKind{}

		// We need to parse the list to get the gvk, I guess that's fine.
		for _, crd := range (*list).Items {
			// Look for group, version, and kind
			group, _, _ := unstructured.NestedString(crd.Object, "spec", "group")
			kind, _, _ := unstructured.NestedString(crd.Object, "spec", "names", "kind")

			gks = append(gks, schema.GroupKind{
				Group: group,
				Kind:  kind,
			})
		}

		return gks, nil
	}
}

// CRDFinder keeps a cache of known CRDs and finds a given GVK in the
// list.
type CRDFinder interface {
	HasCRD(gvk schema.GroupKind) (bool, error)
}

// NewCRDFinder 返回带惰性缓存的实现：首次查询才真正拉取 CRD 列表。
func NewCRDFinder(getter CRDGetter) CRDFinder {
	return &crdFinder{
		getter: getter,
	}
}

// crdFinder 用 cache 指针区分“尚未加载”(nil) 与“加载结果为空”(指向空切片)，
// 避免空列表时每次都重新请求。
type crdFinder struct {
	getter CRDGetter
	cache  *[]schema.GroupKind
}

func (f *crdFinder) cacheCRDs() error {
	if f.cache != nil {
		return nil
	}

	list, err := f.getter()
	if err != nil {
		return err
	}
	f.cache = &list
	return nil
}

// findCRD 用 DeepEqual 比较 GroupKind，调用前必须确保 cache 已加载。
func (f *crdFinder) findCRD(gvk schema.GroupKind) bool {
	for _, crd := range *f.cache {
		if reflect.DeepEqual(gvk, crd) {
			return true
		}
	}
	return false
}

// HasCRD 报告给定 GroupKind 是否对应集群中的某个 CRD；查询失败时返回错误，
// 调用方据此决定是否回退到 discovery。
func (f *crdFinder) HasCRD(gvk schema.GroupKind) (bool, error) {
	if err := f.cacheCRDs(); err != nil {
		return false, err
	}
	return f.findCRD(gvk), nil
}
