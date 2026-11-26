/*
Copyright 2025 The Kubernetes Authors.

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

package proxy

import (
	"fmt"

	discoveryv1 "k8s.io/api/discovery/v1"
	"k8s.io/apimachinery/pkg/labels"
	discoveryv1informer "k8s.io/client-go/informers/discovery/v1"
	discoveryv1lister "k8s.io/client-go/listers/discovery/v1"
	"k8s.io/client-go/tools/cache"
)

// EndpointSliceGetter 是一个辅助接口，用于便捷地获取某个 Service 的全部 EndpointSlice。
// 获取某 Service 的全部 EndpointSlice。
type EndpointSliceGetter interface {
	// GetEndpointSlices 返回与给定 service 关联的全部已知切片。
	// 若没有关联切片，返回空列表而不是错误。
	// 返回空列表而不是错误。
	GetEndpointSlices(namespaceName, serviceName string) ([]*discoveryv1.EndpointSlice, error)
}

// indexKey 是索引名；值与索引函数返回的 "namespace/serviceName" 格式对应。
const indexKey = "namespaceName_serviceName"

// ensureServiceNameIndexer 确保索引器包含 namespace/serviceName 索引。
//
// 先检查再添加，因为同一个 informer 可能被多个组件共享；并发添加时依赖
// AddIndexers 的报错检测竞态，若索引最终存在则视为成功。
func ensureServiceNameIndexer(indexer cache.Indexer) error {
	if _, exists := indexer.GetIndexers()[indexKey]; exists {
		return nil
	}
	err := indexer.AddIndexers(map[string]cache.IndexFunc{indexKey: func(obj any) ([]string, error) {
		ep, ok := obj.(*discoveryv1.EndpointSlice)
		if !ok {
			return nil, fmt.Errorf("expected *discoveryv1.EndpointSlice, got %T", obj)
		}
		serviceName, labelExists := ep.Labels[discoveryv1.LabelServiceName]
		if !labelExists {
			// 不与任何 Service 关联，不加入该索引。
			return nil, nil
		}
		return []string{ep.Namespace + "/" + serviceName}, nil
	}})
	if err != nil {
		// 再次检查索引是否已存在；若存在，说明与另一线程竞争且对方已成功安装索引，
		// 因此可以忽略错误。
		//
		if _, exists := indexer.GetIndexers()[indexKey]; exists {
			err = nil
		}
	}
	return err
}

// NewEndpointSliceIndexerGetter 返回一个包装 informer 的 EndpointSliceGetter，
// 它会更新索引以便后续高效查找与某 Service 关联的 EndpointSlice。
// 注意：只要 sliceInformer 还在运行，索引就会持续维护，即使 EndpointSliceGetter 已被销毁。
// 若不想改动底层缓存，应使用 NewEndpointSliceListerGetter。
//
// 改动底层缓存。）
//
// 该实现以 O(1) 的索引查询换取对共享 informer 缓存的副作用，适合长期运行、
// 查询频繁的场景。
func NewEndpointSliceIndexerGetter(sliceInformer discoveryv1informer.EndpointSliceInformer) (EndpointSliceGetter, error) {
	indexer := sliceInformer.Informer().GetIndexer()
	if err := ensureServiceNameIndexer(indexer); err != nil {
		return nil, err
	}
	return &endpointSliceIndexerGetter{indexer: indexer}, nil
}

type endpointSliceIndexerGetter struct {
	indexer cache.Indexer
}

// GetEndpointSlices 通过索引直接取出某 Service 的所有 EndpointSlice。
func (e *endpointSliceIndexerGetter) GetEndpointSlices(namespaceName, serviceName string) ([]*discoveryv1.EndpointSlice, error) {
	objs, err := e.indexer.ByIndex(indexKey, namespaceName+"/"+serviceName)
	if err != nil {
		return nil, err
	}
	eps := make([]*discoveryv1.EndpointSlice, 0, len(objs))
	for _, obj := range objs {
		ep, ok := obj.(*discoveryv1.EndpointSlice)
		if !ok {
			return nil, fmt.Errorf("expected *discoveryv1.EndpointSlice, got %T", obj)
		}
		eps = append(eps, ep)
	}
	return eps, nil
}

// NewEndpointSliceListerGetter 返回一个使用 lister 在每次查询时做全量选择的 EndpointSliceGetter。
// full selection on every lookup.
//
// 相比索引版本，它不修改共享缓存，但每次查询都要遍历命名空间下的全部切片。
func NewEndpointSliceListerGetter(sliceLister discoveryv1lister.EndpointSliceLister) (EndpointSliceGetter, error) {
	return &endpointSliceListerGetter{lister: sliceLister}, nil
}

type endpointSliceListerGetter struct {
	lister discoveryv1lister.EndpointSliceLister
}

// GetEndpointSlices 用 service-name 标签选择器过滤命名空间内的所有切片。
func (e *endpointSliceListerGetter) GetEndpointSlices(namespaceName, serviceName string) ([]*discoveryv1.EndpointSlice, error) {
	return e.lister.EndpointSlices(namespaceName).List(labels.SelectorFromSet(labels.Set{discoveryv1.LabelServiceName: serviceName}))
}
