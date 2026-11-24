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
	"context"
	"errors"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apiserver/pkg/registry/generic"
	genericregistry "k8s.io/apiserver/pkg/registry/generic/registry"
	"k8s.io/apiserver/pkg/registry/rest"
	"k8s.io/kubernetes/pkg/apis/scheduling"
	schedulingapiv1 "k8s.io/kubernetes/pkg/apis/scheduling/v1"
	"k8s.io/kubernetes/pkg/printers"
	printersinternal "k8s.io/kubernetes/pkg/printers/internalversion"
	printerstorage "k8s.io/kubernetes/pkg/printers/storage"
	"k8s.io/kubernetes/pkg/registry/scheduling/priorityclass"
)

// REST 将 PriorityClass 的 REST 请求映射到底层通用 Store，并通过 etcd 持久化。
type REST struct {
	*genericregistry.Store
}

// NewREST 创建集群级 PriorityClass Store，并配置资源策略和标准表格转换器。
func NewREST(optsGetter generic.RESTOptionsGetter) (*REST, error) {
	store := &genericregistry.Store{
		NewFunc:                   func() runtime.Object { return &scheduling.PriorityClass{} },
		NewListFunc:               func() runtime.Object { return &scheduling.PriorityClassList{} },
		DefaultQualifiedResource:  scheduling.Resource("priorityclasses"),
		SingularQualifiedResource: scheduling.Resource("priorityclass"),

		CreateStrategy: priorityclass.Strategy,
		UpdateStrategy: priorityclass.Strategy,
		DeleteStrategy: priorityclass.Strategy,

		TableConvertor: printerstorage.TableConvertor{TableGenerator: printers.NewTableGenerator().With(printersinternal.AddHandlers)},
	}
	options := &generic.StoreOptions{RESTOptions: optsGetter}
	if err := store.CompleteWithOptions(options); err != nil {
		return nil, err
	}

	return &REST{store}, nil
}

// 声明 REST 实现 ShortNamesProvider 接口。
var _ rest.ShortNamesProvider = &REST{}

// ShortNames 返回 PriorityClass 资源的 kubectl 简称 pc。
func (r *REST) ShortNames() []string {
	return []string{"pc"}
}

// Delete 阻止删除 Kubernetes 内置系统 PriorityClass，其他优先级类交由底层 Store 正常删除。
func (r *REST) Delete(ctx context.Context, name string, deleteValidation rest.ValidateObjectFunc, options *metav1.DeleteOptions) (runtime.Object, bool, error) {
	for _, spcName := range schedulingapiv1.SystemPriorityClassNames() {
		if name == spcName {
			return nil, false, apierrors.NewForbidden(scheduling.Resource("priorityclasses"), spcName, errors.New("this is a system priority class and cannot be deleted"))
		}
	}

	return r.Store.Delete(ctx, name, deleteValidation, options)
}
