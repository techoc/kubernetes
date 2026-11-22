/*
Copyright 2021 The Kubernetes Authors.

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

// Package cel 提供 CEL 表达式求值与校验所需的类型适配与辅助实现。
package cel

import (
	"github.com/google/cel-go/common/types/ref"

	structuralschema "k8s.io/apiextensions-apiserver/pkg/apiserver/schema"
	"k8s.io/apiextensions-apiserver/pkg/apiserver/schema/cel/model"
	celopenapi "k8s.io/apiserver/pkg/cel/common"
)

// UnstructuredToVal 将 Kubernetes 非结构化数据元素转换为 CEL 的 Val。
// 自定义资源 schema 的根 schema 预期包含 type meta 与 object meta 的 schema；
// 若内嵌资源缺少这两部分 schema，会被自动补全。
func UnstructuredToVal(unstructured interface{}, schema *structuralschema.Structural) ref.Val {
	// 包装为 model.Structural，供 CEL 按声明的类型做字段解析。
	return celopenapi.UnstructuredToVal(unstructured, &model.Structural{Structural: schema})
}
