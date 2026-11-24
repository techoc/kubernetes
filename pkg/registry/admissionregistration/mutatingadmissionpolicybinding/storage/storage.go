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

// 本包创建 MutatingAdmissionPolicyBinding 的 REST Store；策略校验依赖关联 Policy 和参数读取授权。
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
	mutatingadmissionpolicybinding "k8s.io/kubernetes/pkg/registry/admissionregistration/mutatingadmissionpolicybinding"
	"k8s.io/kubernetes/pkg/registry/admissionregistration/resolver"
)

// REST 将 MutatingAdmissionPolicyBinding 的通用 Store 封装为 REST 存储接口。
type REST struct {
	*genericregistry.Store
}

var groupResource = admissionregistration.Resource("mutatingadmissionpolicybindings")

// NewREST 创建并配置 MutatingAdmissionPolicyBinding 的 REST Store。
func NewREST(optsGetter generic.RESTOptionsGetter, authorizer authorizer.UnconditionalAuthorizer, policyGetter PolicyGetter, resourceResolver resolver.ResourceResolver) (*REST, error) {
	r := &REST{}
	strategy := mutatingadmissionpolicybinding.NewStrategy(authorizer, policyGetter, resourceResolver)
	store := &genericregistry.Store{
		NewFunc:     func() runtime.Object { return &admissionregistration.MutatingAdmissionPolicyBinding{} },
		NewListFunc: func() runtime.Object { return &admissionregistration.MutatingAdmissionPolicyBindingList{} },
		ObjectNameFunc: func(obj runtime.Object) (string, error) {
			return obj.(*admissionregistration.MutatingAdmissionPolicyBinding).Name, nil
		},
		DefaultQualifiedResource:  groupResource,
		SingularQualifiedResource: admissionregistration.Resource("mutatingadmissionpolicybinding"),

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

// PolicyGetter 定义 Binding 获取其所引用 MutatingAdmissionPolicy 的查询接口。
type PolicyGetter interface {
	// GetMutatingAdmissionPolicy 按名称读取集群级 MutatingAdmissionPolicy，无需 Namespace 参数。
	GetMutatingAdmissionPolicy(ctx context.Context, name string) (*admissionregistration.MutatingAdmissionPolicy, error)
}

// DefaultPolicyGetter 使用通用 REST Getter 实现 PolicyGetter。
type DefaultPolicyGetter struct {
	Getter rest.Getter
}

// GetMutatingAdmissionPolicy 从底层 REST Getter 读取对象，并转换为内部 MutatingAdmissionPolicy 类型。
func (g *DefaultPolicyGetter) GetMutatingAdmissionPolicy(ctx context.Context, name string) (*admissionregistration.MutatingAdmissionPolicy, error) {
	p, err := g.Getter.Get(ctx, name, &metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	return p.(*admissionregistration.MutatingAdmissionPolicy), err
}
