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

package openapi

import (
	"encoding/json"
	"fmt"

	"k8s.io/apimachinery/pkg/util/managedfields"
	"k8s.io/kube-openapi/pkg/spec3"
	"k8s.io/kube-openapi/pkg/validation/spec"
)

// NewTypeConverter 从服务端 OpenAPI 文档构建 managedFields 类型转换器。
// 它把各 GroupVersion schema 的 components 合并成一张全局 schema 表，
// 供 server-side apply 在结构化数据与 typed 对象之间转换。
func NewTypeConverter(client Client, preserveUnknownFields bool) (managedfields.TypeConverter, error) {
	spec := map[string]*spec.Schema{}
	paths, err := client.Paths()
	if err != nil {
		return nil, fmt.Errorf("failed to list paths: %w", err)
	}
	for _, gv := range paths {
		s, err := gv.Schema("application/json")
		if err != nil {
			return nil, fmt.Errorf("failed to download schema: %w", err)
		}
		var openapi spec3.OpenAPI
		if err := json.Unmarshal(s, &openapi); err != nil {
			return nil, fmt.Errorf("failed to parse schema: %w", err)
		}
		// schema 定义以 kube-openapi 的引用名为 key，跨 GroupVersion 合并时依赖该命名约定。
		for k, v := range openapi.Components.Schemas {
			spec[k] = v
		}
	}
	return managedfields.NewTypeConverter(spec, preserveUnknownFields)
}
