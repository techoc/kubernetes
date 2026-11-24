/*
Copyright 2015 The Kubernetes Authors.

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

// 本包创建 MutatingWebhookConfiguration 的 REST Store，并将 Webhook 规则/客户端配置校验接入通用存储流程。
package storage

import (
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apiserver/pkg/registry/generic"
	genericregistry "k8s.io/apiserver/pkg/registry/generic/registry"
	"k8s.io/apiserver/pkg/registry/rest"
	"k8s.io/kubernetes/pkg/apis/admissionregistration"
	"k8s.io/kubernetes/pkg/printers"
	printersinternal "k8s.io/kubernetes/pkg/printers/internalversion"
	printerstorage "k8s.io/kubernetes/pkg/printers/storage"
	"k8s.io/kubernetes/pkg/registry/admissionregistration/mutatingwebhookconfiguration"
)

// REST 将 MutatingWebhookConfiguration 的通用 Store 封装为 REST 存储接口。
type REST struct {
	*genericregistry.Store
}

// NewREST 创建并配置 MutatingWebhookConfiguration 的 REST Store。
func NewREST(optsGetter generic.RESTOptionsGetter) (*REST, error) {
	store := &genericregistry.Store{
		NewFunc:     func() runtime.Object { return &admissionregistration.MutatingWebhookConfiguration{} },
		NewListFunc: func() runtime.Object { return &admissionregistration.MutatingWebhookConfigurationList{} },
		ObjectNameFunc: func(obj runtime.Object) (string, error) {
			return obj.(*admissionregistration.MutatingWebhookConfiguration).Name, nil
		},
		DefaultQualifiedResource:  admissionregistration.Resource("mutatingwebhookconfigurations"),
		SingularQualifiedResource: admissionregistration.Resource("mutatingwebhookconfiguration"),

		CreateStrategy: mutatingwebhookconfiguration.Strategy,
		UpdateStrategy: mutatingwebhookconfiguration.Strategy,
		DeleteStrategy: mutatingwebhookconfiguration.Strategy,

		TableConvertor: printerstorage.TableConvertor{TableGenerator: printers.NewTableGenerator().With(printersinternal.AddHandlers)},
	}
	options := &generic.StoreOptions{RESTOptions: optsGetter}
	if err := store.CompleteWithOptions(options); err != nil {
		return nil, err
	}
	return &REST{store}, nil
}

// 确保 REST 实现资源类别提供者接口。
var _ rest.CategoriesProvider = &REST{}

// Categories 返回该资源所属的 API 资源类别。
func (r *REST) Categories() []string {
	return []string{"api-extensions"}
}
