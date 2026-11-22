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

// ScopeNamer handles accessing names from requests and objects
type ScopeNamer interface {
	// Namespace returns the appropriate namespace value from the request (may be empty) or an
	// error.
	Namespace(req *http.Request) (namespace string, err error)
	// Name returns the name from the request, and an optional namespace value if this is a namespace
	// scoped call. An error is returned if the name is not available.
	Name(req *http.Request) (namespace, name string, err error)
	// ObjectName returns the namespace and name from an object if they exist, or an error if the object
	// does not support names.
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

// ContextBasedNaming implements ScopeNamer
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

// errEmptyName is returned when API requests do not fill the name section of the path.
var errEmptyName = errors.NewBadRequest("name must be provided")
