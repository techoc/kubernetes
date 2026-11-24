/*
Copyright 2016 The Kubernetes Authors.

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

package rolebinding

import (
	"context"

	rbacv1 "k8s.io/api/rbac/v1"
	metainternalversion "k8s.io/apimachinery/pkg/apis/meta/internalversion"
	genericapirequest "k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/apiserver/pkg/registry/rest"
	"k8s.io/kubernetes/pkg/apis/rbac"
	rbacv1helpers "k8s.io/kubernetes/pkg/apis/rbac/v1"
)

// Registry 定义 命名空间级 RoleBinding 的读取接口，供授权规则解析器查询 RBAC 对象。
type Registry interface {
	ListRoleBindings(ctx context.Context, options *metainternalversion.ListOptions) (*rbacv1.RoleBindingList, error)
}

// storage 为通用 REST 存储调用提供强类型封装。
type storage struct {
	rest.Lister
}

// NewRegistry 将传入的 StandardStorage 适配为 命名空间级 RoleBinding 的强类型 Registry。
// 如果底层返回的对象类型与预期不匹配，类型断言会触发 panic。
func NewRegistry(s rest.StandardStorage) Registry {
	return &storage{s}
}

func (s *storage) ListRoleBindings(ctx context.Context, options *metainternalversion.ListOptions) (*rbacv1.RoleBindingList, error) {
	obj, err := s.List(ctx, options)
	if err != nil {
		return nil, err
	}

	ret := &rbacv1.RoleBindingList{}
	if err := rbacv1helpers.Convert_rbac_RoleBindingList_To_v1_RoleBindingList(obj.(*rbac.RoleBindingList), ret, nil); err != nil {
		return nil, err
	}
	return ret, nil
}

// AuthorizerAdapter 将 Registry 适配为授权规则解析器所需的查询接口。
type AuthorizerAdapter struct {
	Registry Registry
}

// ListRoleBindings 列出指定 namespace 内的 RoleBinding，并转换成公开 v1 类型供 Subject 匹配。
func (a AuthorizerAdapter) ListRoleBindings(ctx context.Context, namespace string) ([]*rbacv1.RoleBinding, error) {
	list, err := a.Registry.ListRoleBindings(genericapirequest.WithNamespace(ctx, namespace), &metainternalversion.ListOptions{})
	if err != nil {
		return nil, err
	}

	ret := []*rbacv1.RoleBinding{}
	for i := range list.Items {
		ret = append(ret, &list.Items[i])
	}
	return ret, nil
}
