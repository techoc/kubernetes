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
	"fmt"
	"reflect"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// Mapper 集中持有为任意对象创建 Info 所需的接口引用。
type mapper struct {
	// localFn 表示当前调用不能访问服务端。
	// 中文：为 true 时只做本地解码，不查 RESTMapping、不取 client，供离线处理文件/流的场景使用。
	localFn func() bool

	// 延迟获取 REST 映射器与客户端，避免在真正需要之前触发发现请求。
	restMapperFn RESTMapperFunc
	clientFn     func(version schema.GroupVersion) (RESTClient, error)
	decoder      runtime.Decoder
}

// InfoForData 为给定数据创建 Info；解码或客户端查找失败时返回错误。
// 若映射的 MetadataAccessor 能取到 name 和 namespace，就写入 Info。
func (m *mapper) infoForData(data []byte, source string) (*Info, error) {
	obj, gvk, err := m.decoder.Decode(data, nil, nil)
	if err != nil {
		return nil, fmt.Errorf("unable to decode %q: %v", source, err)
	}

	name, _ := metadataAccessor.Name(obj)
	namespace, _ := metadataAccessor.Namespace(obj)
	resourceVersion, _ := metadataAccessor.ResourceVersion(obj)

	ret := &Info{
		Source:          source,
		Namespace:       namespace,
		Name:            name,
		ResourceVersion: resourceVersion,

		Object: obj,
	}

	if m.localFn == nil || !m.localFn() {
		// 仅在线模式下才需要映射与客户端；本地模式保持 Info 只有对象本身。
		restMapper, err := m.restMapperFn()
		if err != nil {
			return nil, err
		}
		mapping, err := restMapper.RESTMapping(gvk.GroupKind(), gvk.Version)
		if err != nil {
			if _, ok := err.(*meta.NoKindMatchError); ok {
				// 未知 GVK 最常见的原因是 CRD 未安装，直接给出可操作提示。
				return nil, fmt.Errorf("resource mapping not found for name: %q namespace: %q from %q: %w\nensure CRDs are installed first",
					name, namespace, source, err)
			}
			return nil, fmt.Errorf("unable to recognize %q: %v", source, err)
		}
		ret.Mapping = mapping

		client, err := m.clientFn(gvk.GroupVersion())
		if err != nil {
			return nil, fmt.Errorf("unable to connect to a server to handle %q: %v", mapping.Resource, err)
		}
		ret.Client = client
	}

	return ret, nil
}

// InfoForObject 为给定对象创建 Info；无法解析对象时返回错误。
// 若 MetadataAccessor 能取到 name 和 namespace，就写入 Info。
func (m *mapper) infoForObject(obj runtime.Object, typer runtime.ObjectTyper, preferredGVKs []schema.GroupVersionKind) (*Info, error) {
	groupVersionKinds, _, err := typer.ObjectKinds(obj)
	if err != nil {
		return nil, fmt.Errorf("unable to get type info from the object %q: %v", reflect.TypeOf(obj), err)
	}

	gvk := groupVersionKinds[0]
	// 一个类型可能注册了多个 GVK；调用方可传入偏好列表来挑选合适的版本。
	if len(groupVersionKinds) > 1 && len(preferredGVKs) > 0 {
		gvk = preferredObjectKind(groupVersionKinds, preferredGVKs)
	}

	name, _ := metadataAccessor.Name(obj)
	namespace, _ := metadataAccessor.Namespace(obj)
	resourceVersion, _ := metadataAccessor.ResourceVersion(obj)
	ret := &Info{
		Namespace:       namespace,
		Name:            name,
		ResourceVersion: resourceVersion,

		Object: obj,
	}

	if m.localFn == nil || !m.localFn() {
		restMapper, err := m.restMapperFn()
		if err != nil {
			return nil, err
		}
		mapping, err := restMapper.RESTMapping(gvk.GroupKind(), gvk.Version)
		if err != nil {
			return nil, fmt.Errorf("unable to recognize %v", err)
		}
		ret.Mapping = mapping

		client, err := m.clientFn(gvk.GroupVersion())
		if err != nil {
			return nil, fmt.Errorf("unable to connect to a server to handle %q: %v", mapping.Resource, err)
		}
		ret.Client = client
	}

	return ret, nil
}

// preferredObjectKind 按以下优先级挑选最匹配的可能项：
// GVK 完全匹配、GroupKind 匹配、Group 匹配。
func preferredObjectKind(possibilities []schema.GroupVersionKind, preferences []schema.GroupVersionKind) schema.GroupVersionKind {
	// 从最精确的完全匹配开始放宽，确保偏好列表靠前的条目优先。
	// 完全匹配。
	for _, priority := range preferences {
		for _, possibility := range possibilities {
			if possibility == priority {
				return possibility
			}
		}
	}

	// GroupKind 匹配。
	for _, priority := range preferences {
		for _, possibility := range possibilities {
			if possibility.GroupKind() == priority.GroupKind() {
				return possibility
			}
		}
	}

	// Group 匹配。
	for _, priority := range preferences {
		for _, possibility := range possibilities {
			if possibility.Group == priority.Group {
				return possibility
			}
		}
	}

	// 都没有时取第一个。
	// 全都不匹配时退回注册顺序的第一个，保证行为确定而非随机。
	return possibilities[0]
}
