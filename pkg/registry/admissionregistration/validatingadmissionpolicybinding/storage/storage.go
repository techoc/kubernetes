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

// 本包创建 ValidatingAdmissionPolicyBinding 的 REST Store；Binding 写入前会校验 Policy 关联和 ParamRef。
package storage

import (
	"context"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apiserver/pkg/authorization/authorizer"
	"k8s.io/apiserver/pkg/registry/generic"
	genericregistry "k8s.io/apiserver/pkg/registry/generic/registry"
	"k8s.io/apiserver/pkg/registry/rest"
	"k8s.io/kubernetes/pkg/apis/admissionregistration"
	"k8s.io/kubernetes/pkg/printers"
	printersinternal "k8s.io/kubernetes/pkg/printers/internalversion"
	printerstorage "k8s.io/kubernetes/pkg/printers/storage"
	"k8s.io/kubernetes/pkg/registry/admissionregistration/resolver"
	"k8s.io/kubernetes/pkg/registry/admissionregistration/validatingadmissionpolicybinding"
)

// REST 将 ValidatingAdmissionPolicyBinding 的通用 Store 封装为 REST 存储接口。
type REST struct {
	*genericregistry.Store
}

var groupResource = admissionregistration.Resource("validatingadmissionpolicybindings")

// NewREST 创建并配置 ValidatingAdmissionPolicyBinding 的 REST Store。
func NewREST(optsGetter generic.RESTOptionsGetter, authorizer authorizer.UnconditionalAuthorizer, policyGetter PolicyGetter, resourceResolver resolver.ResourceResolver) (*REST, error) {
	r := &REST{}
	strategy := validatingadmissionpolicybinding.NewStrategy(authorizer, policyGetter, resourceResolver)
	store := &genericregistry.Store{
		NewFunc:     func() runtime.Object { return &admissionregistration.ValidatingAdmissionPolicyBinding{} },
		NewListFunc: func() runtime.Object { return &admissionregistration.ValidatingAdmissionPolicyBindingList{} },
		ObjectNameFunc: func(obj runtime.Object) (string, error) {
			return obj.(*admissionregistration.ValidatingAdmissionPolicyBinding).Name, nil
		},
		DefaultQualifiedResource:  groupResource,
		SingularQualifiedResource: admissionregistration.Resource("validatingadmissionpolicybinding"),

		CreateStrategy: strategy,
		UpdateStrategy: strategy,
		DeleteStrategy: strategy,

		TableConvertor: printerstorage.TableConvertor{TableGenerator: printers.NewTableGenerator().With(printersinternal.AddHandlers)},
	}
	options := &generic.StoreOptions{RESTOptions: optsGetter}
	if err := store.CompleteWithOptions(options); err != nil {
		return nil, err
	}
	r.Store = store
	return r, nil
}

// 确保 REST 实现资源类别提供者接口。
var _ rest.CategoriesProvider = &REST{}

// Categories 返回该资源所属的 API 资源类别。
func (r *REST) Categories() []string {
	return []string{"api-extensions"}
}

type PolicyGetter interface {
	// GetValidatingAdmissionPolicy 根据名称读取集群级 ValidatingAdmissionPolicy。
	// 该资源是集群级资源，因此查询不需要 Namespace 参数。
	GetValidatingAdmissionPolicy(ctx context.Context, name string) (*admissionregistration.ValidatingAdmissionPolicy, error)
}

type DefaultPolicyGetter struct {
	Getter rest.Getter
}

func (g *DefaultPolicyGetter) GetValidatingAdmissionPolicy(ctx context.Context, name string) (*admissionregistration.ValidatingAdmissionPolicy, error) {
	p, err := g.Getter.Get(ctx, name, &metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	return p.(*admissionregistration.ValidatingAdmissionPolicy), err
}
