/*
Copyright 2014 The Kubernetes Authors.

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
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
)

// Selector 遍历匹配标签选择器的资源。
//
// Selector 负责按 label/field selector 向服务端发起列表查询，并按 chunk
// 分页遍历；每个分页的结果作为一个 Info 交给 VisitorFunc。
type Selector struct {
	Client        RESTClient
	Mapping       *meta.RESTMapping
	Namespace     string
	LabelSelector string
	FieldSelector string
	// LimitChunks 为 0 表示不分页，一次性取回全部结果。
	LimitChunks int64
}

// 创建资源选择器，隐藏按标签选择器取对象的细节。
func NewSelector(client RESTClient, mapping *meta.RESTMapping, namespace, labelSelector, fieldSelector string, limitChunks int64) *Selector {
	return &Selector{
		Client:        client,
		Mapping:       mapping,
		Namespace:     namespace,
		LabelSelector: labelSelector,
		FieldSelector: fieldSelector,
		LimitChunks:   limitChunks,
	}
}

// Visit 实现 Visitor，默认启用请求分块。
//
// 通过 FollowContinue 自动跟随 continue token，既避免超大列表一次性传输，
// 又保证每个分页都能被处理。
func (r *Selector) Visit(fn VisitorFunc) error {
	helper := NewHelper(r.Client, r.Mapping)
	initialOpts := metav1.ListOptions{
		LabelSelector: r.LabelSelector,
		FieldSelector: r.FieldSelector,
		Limit:         r.LimitChunks,
	}
	return FollowContinue(&initialOpts, func(options metav1.ListOptions) (runtime.Object, error) {
		list, err := helper.List(
			r.Namespace,
			r.ResourceMapping().GroupVersionKind.GroupVersion().String(),
			&options,
		)
		// 列表错误补充分页上下文（selector、continue token），便于定位是哪个查询失败。
		if err != nil {
			return nil, EnhanceListError(err, options, r.Mapping.Resource.String())
		}
		resourceVersion, _ := metadataAccessor.ResourceVersion(list)

		// 分页结果也包装成 Info，语义上仍是“一次逻辑查询”的一个片段。
		info := &Info{
			Client:  r.Client,
			Mapping: r.Mapping,

			Namespace:       r.Namespace,
			ResourceVersion: resourceVersion,

			Object: list,
		}

		if err := fn(info, nil); err != nil {
			return nil, err
		}
		return list, nil
	})
}

// Watch 对 selector 命中的集合发起 watch；复用与列表相同的 label/field selector。
func (r *Selector) Watch(resourceVersion string) (watch.Interface, error) {
	return NewHelper(r.Client, r.Mapping).Watch(r.Namespace, r.ResourceMapping().GroupVersionKind.GroupVersion().String(),
		&metav1.ListOptions{ResourceVersion: resourceVersion, LabelSelector: r.LabelSelector, FieldSelector: r.FieldSelector})
}

// 返回该资源的映射，实现 ResourceMapping 接口。
func (r *Selector) ResourceMapping() *meta.RESTMapping {
	return r.Mapping
}
