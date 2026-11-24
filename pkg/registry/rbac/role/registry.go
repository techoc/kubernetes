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

package role

import (
	"context"

	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	genericapirequest "k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/apiserver/pkg/registry/rest"
	"k8s.io/kubernetes/pkg/apis/rbac"
	rbacv1helpers "k8s.io/kubernetes/pkg/apis/rbac/v1"
)

// Registry 定义 命名空间级 Role 的读取接口，供授权规则解析器查询 RBAC 对象。
type Registry interface {
	GetRole(ctx context.Context, name string, options *metav1.GetOptions) (*rbacv1.Role, error)
}

// storage 为通用 REST 存储调用提供强类型封装。
type storage struct {
	rest.Getter
}

// NewRegistry 将传入的 StandardStorage 适配为 命名空间级 Role 的强类型 Registry。
// 如果底层返回的对象类型与预期不匹配，类型断言会触发 panic。
func NewRegistry(s rest.StandardStorage) Registry {
	return &storage{s}
}

func (s *storage) GetRole(ctx context.Context, name string, options *metav1.GetOptions) (*rbacv1.Role, error) {
	obj, err := s.Get(ctx, name, options)
	if err != nil {
		return nil, err
	}

	ret := &rbacv1.Role{}
	if err := rbacv1helpers.Convert_rbac_Role_To_v1_Role(obj.(*rbac.Role), ret, nil); err != nil {
		return nil, err
	}
	return ret, nil
}

// AuthorizerAdapter 将 Registry 适配为授权规则解析器所需的查询接口。
type AuthorizerAdapter struct {
	Registry Registry
}

// GetRole 在指定 namespace 中读取 Role，并转换为公开 RBAC v1 类型。
func (a AuthorizerAdapter) GetRole(ctx context.Context, namespace, name string) (*rbacv1.Role, error) {
	return a.Registry.GetRole(genericapirequest.WithNamespace(ctx, namespace), name, &metav1.GetOptions{})
}
