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

package storage

import (
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apiserver/pkg/registry/generic"
	genericregistry "k8s.io/apiserver/pkg/registry/generic/registry"
	"k8s.io/kubernetes/pkg/apis/apps"
	"k8s.io/kubernetes/pkg/printers"
	printersinternal "k8s.io/kubernetes/pkg/printers/internalversion"
	printerstorage "k8s.io/kubernetes/pkg/printers/storage"
	"k8s.io/kubernetes/pkg/registry/apps/controllerrevision"
)

// REST 实现 ControllerRevision 的通用 RESTStorage；快照数据按控制器提供的对象内容存入后端。
type REST struct {
	*genericregistry.Store
}

// NewREST 创建并返回用于访问 ControllerRevision 资源的 RESTStorage。
// NewREST 创建 ControllerRevision Store，注册对象类型、资源名、版本策略和表格输出器。
func NewREST(optsGetter generic.RESTOptionsGetter) (*REST, error) {
	store := &genericregistry.Store{
		NewFunc:                   func() runtime.Object { return &apps.ControllerRevision{} },
		NewListFunc:               func() runtime.Object { return &apps.ControllerRevisionList{} },
		DefaultQualifiedResource:  apps.Resource("controllerrevisions"),
		SingularQualifiedResource: apps.Resource("controllerrevision"),

		CreateStrategy: controllerrevision.Strategy,
		UpdateStrategy: controllerrevision.Strategy,
		DeleteStrategy: controllerrevision.Strategy,

		TableConvertor: printerstorage.TableConvertor{TableGenerator: printers.NewTableGenerator().With(printersinternal.AddHandlers)},
	}
	// 将统一 RESTOptions 注入通用 Store，确定 ControllerRevision 的 etcd 前缀和序列化存储行为。
	options := &generic.StoreOptions{RESTOptions: optsGetter}
	if err := store.CompleteWithOptions(options); err != nil {
		return nil, err
	}
	return &REST{store}, nil
}
