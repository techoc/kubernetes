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

package handlers

import (
	"fmt"
	"net/http"

	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apiserver/pkg/endpoints/request"
)

// ScopeNamer 负责从请求和对象中读取名称信息
type ScopeNamer interface {
	// Namespace 从请求中返回合适的命名空间值（可能为空），否则返回错误。
	Namespace(req *http.Request) (namespace string, err error)
	// Name 返回请求中的名称；若是命名空间作用域的调用还会返回命名空间值。
	// 名称不可用时返回错误。
	Name(req *http.Request) (namespace, name string, err error)
	// ObjectName 返回对象上的命名空间与名称（若存在）；对象不支持命名时返回错误。
	ObjectName(obj runtime.Object) (namespace, name string, err error)
}

// ContextBasedNaming 从请求上下文（RequestInfo）或对象元数据中提取命名空间与名称，
// 是 ScopeNamer 的默认实现。依赖上下文而不是再次解析 URL，可避免路由与命名
// 逻辑出现分歧。
type ContextBasedNaming struct {
	// Namer 用于从对象本身提取 name/namespace；对于没有元数据的对象会返回错误。
	Namer runtime.Namer
	// ClusterScoped 表示对象不属于任何命名空间，此时 Namespace 始终为空。
	ClusterScoped bool
}

// ContextBasedNaming 实现了 ScopeNamer
var _ ScopeNamer = ContextBasedNaming{}

// Namespace 从请求上下文中的 RequestInfo 取命名空间。
// 不直接解析路径，是因为 RequestInfo 已由前置过滤器完成一次解析，
// 复用它可以保证命名空间判定与路由一致。
func (n ContextBasedNaming) Namespace(req *http.Request) (namespace string, err error) {
	requestInfo, ok := request.RequestInfoFrom(req.Context())
	if !ok {
		return "", fmt.Errorf("missing requestInfo")
	}
	return requestInfo.Namespace, nil
}

// Name 从 RequestInfo 取命名空间与名称；名称为空视为请求非法。
func (n ContextBasedNaming) Name(req *http.Request) (namespace, name string, err error) {
	requestInfo, ok := request.RequestInfoFrom(req.Context())
	if !ok {
		return "", "", fmt.Errorf("missing requestInfo")
	}

	if len(requestInfo.Name) == 0 {
		return "", "", errEmptyName
	}
	return requestInfo.Namespace, requestInfo.Name, nil
}

// ObjectName 从对象元数据中提取 namespace/name。
// 先校验 name 再取 namespace，是为了让无名字对象的错误信息更明确。
func (n ContextBasedNaming) ObjectName(obj runtime.Object) (namespace, name string, err error) {
	name, err = n.Namer.Name(obj)
	if err != nil {
		return "", "", err
	}
	if len(name) == 0 {
		return "", "", errEmptyName
	}
	namespace, err = n.Namer.Namespace(obj)
	if err != nil {
		return "", "", err
	}
	return namespace, name, err
}

// errEmptyName 在 API 请求未填写路径中的名称部分时返回。
var errEmptyName = errors.NewBadRequest("name must be provided")
