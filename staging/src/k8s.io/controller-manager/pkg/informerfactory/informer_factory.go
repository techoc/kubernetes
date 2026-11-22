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

package informerfactory

import (
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/metadata/metadatainformer"
)

// InformerFactory creates informers for each group version resource.
// 统一创建 Informer 的工厂接口：同时支持结构化资源与仅元数据资源。
// 先查 typed 工厂，找不到再回退到 metadata 工厂。
type InformerFactory interface {
	ForResource(resource schema.GroupVersionResource) (informers.GenericInformer, error)
	// 按 GVR 返回通用 Informer。
	Start(stopCh <-chan struct{})
	// 启动所有已创建的 Informer，直到 stopCh 关闭。
}

// 默认实现：内部同时持有 typed 与 metadata 两个工厂。
type informerFactory struct {
	typedInformerFactory informers.SharedInformerFactory
	// 用于只关心元信息（metadata）的资源，无需知道完整结构。
	metadataInformerFactory metadatainformer.SharedInformerFactory
}

// 先尝试 typed 工厂；若该资源没有对应的 Go 类型（返回错误），
// 则回退到 metadata 工厂，这样通用控制器也能处理 CRD 等未知资源。
func (i *informerFactory) ForResource(resource schema.GroupVersionResource) (informers.GenericInformer, error) {
	informer, err := i.typedInformerFactory.ForResource(resource)
	// typed 工厂只认已注册的类型。
	if err != nil {
		// 回退：metadata Informer 适用于任何资源（按元数据访问）。
		return i.metadataInformerFactory.ForResource(resource), nil
	}
	return informer, nil
}

// 两个工厂都要启动；它们通常共享同一个 stopCh。
func (i *informerFactory) Start(stopCh <-chan struct{}) {
	i.typedInformerFactory.Start(stopCh)
	i.metadataInformerFactory.Start(stopCh)
}

// NewInformerFactory creates a new InformerFactory which works with both typed
// resources and metadata-only resources
// 用于有 Go 结构体定义的资源。
// 组合两个工厂，返回对外接口。
func NewInformerFactory(typedInformerFactory informers.SharedInformerFactory, metadataInformerFactory metadatainformer.SharedInformerFactory) InformerFactory {
	return &informerFactory{
		typedInformerFactory:    typedInformerFactory,
		metadataInformerFactory: metadataInformerFactory,
	}
}
