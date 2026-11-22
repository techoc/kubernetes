/*
Copyright 2023 The Kubernetes Authors.

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

// Package resolver 定义从 GroupVersionKind 解析 OpenAPI schema 的最小接口。
// 上层 CEL 类型系统只依赖该接口，从而可以分别对接内置类型与 CRD。
package resolver

import (
	"fmt"

	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/kube-openapi/pkg/validation/spec"
)

// SchemaResolver finds the OpenAPI schema for the given GroupVersionKind.
// This interface uses the type defined by k8s.io/kube-openapi
type SchemaResolver interface {
	// ResolveSchema takes a GroupVersionKind (GVK) and returns the OpenAPI schema
	// identified by the GVK.
	// The function returns a non-nil error if the schema cannot be found or fail
	// to resolve. The returned error wraps ErrSchemaNotFound if the resolution is
	// attempted but the corresponding schema cannot be found.
	ResolveSchema(gvk schema.GroupVersionKind) (*spec.Schema, error)
}

// ErrSchemaNotFound is wrapped and returned if the schema cannot be located
// by the resolver.
//
// 调用方应使用 errors.Is(err, ErrSchemaNotFound) 区分“类型确实不存在”
// 与其它解析失败（例如 OpenAPI 服务不可用），前者通常可安全降级。
var ErrSchemaNotFound = fmt.Errorf("schema not found")
