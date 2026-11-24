/*
Copyright 2022 The Kubernetes Authors.

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
	"fmt"

	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apiserver/pkg/registry/generic"
	genericregistry "k8s.io/apiserver/pkg/registry/generic/registry"
	"k8s.io/apiserver/pkg/registry/rest"
	api "k8s.io/kubernetes/pkg/apis/certificates"
	"k8s.io/kubernetes/pkg/printers"
	printersinternal "k8s.io/kubernetes/pkg/printers/internalversion"
	printerstorage "k8s.io/kubernetes/pkg/printers/storage"
	"k8s.io/kubernetes/pkg/registry/certificates/clustertrustbundle"
)

// REST 为 ClusterTrustBundle 提供基于通用 Store 的 REST 存储实现。
type REST struct {
	*genericregistry.Store
}

var _ rest.StandardStorage = &REST{}
var _ rest.TableConvertor = &REST{}
var _ genericregistry.GenericStore = &REST{}

// NewREST 创建 ClusterTrustBundle 的 REST 存储，并配置字段筛选和表格输出能力。
func NewREST(optsGetter generic.RESTOptionsGetter) (*REST, error) {
	store := &genericregistry.Store{
		NewFunc:                   func() runtime.Object { return &api.ClusterTrustBundle{} },
		NewListFunc:               func() runtime.Object { return &api.ClusterTrustBundleList{} },
		DefaultQualifiedResource:  api.Resource("clustertrustbundles"),
		SingularQualifiedResource: api.Resource("clustertrustbundle"),

		CreateStrategy: clustertrustbundle.Strategy,
		UpdateStrategy: clustertrustbundle.Strategy,
		DeleteStrategy: clustertrustbundle.Strategy,

		TableConvertor: printerstorage.TableConvertor{TableGenerator: printers.NewTableGenerator().With(printersinternal.AddHandlers)},
	}
	options := &generic.StoreOptions{
		RESTOptions: optsGetter,
		AttrFunc:    getAttrs,
	}
	if err := store.CompleteWithOptions(options); err != nil {
		return nil, err
	}
	return &REST{store}, nil
}

// getAttrs 提取 ClusterTrustBundle 的标签和可筛选字段，供 List、Watch 等请求过滤。
func getAttrs(obj runtime.Object) (labels.Set, fields.Set, error) {
	bundle, ok := obj.(*api.ClusterTrustBundle)
	if !ok {
		return nil, nil, fmt.Errorf("not a clustertrustbundle")
	}

	selectableFields := generic.MergeFieldsSets(generic.ObjectMetaFieldsSet(&bundle.ObjectMeta, false), fields.Set{
		"spec.signerName": bundle.Spec.SignerName,
	})

	return labels.Set(bundle.Labels), selectableFields, nil
}
