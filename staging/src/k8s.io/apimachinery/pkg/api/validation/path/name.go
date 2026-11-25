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

package path

import (
	"k8s.io/apimachinery/pkg/api/validate/content"
)

// Deprecated: 请直接使用 content.IsPathSegmentName。
//
// 保留此别名是为了不破坏既有调用方；实际实现已迁到 content 包，
// 两处行为必须保持一致。
var IsValidPathSegmentName = content.IsPathSegmentName

// IsValidPathSegmentPrefix 校验名称可作为将编码为路径段的名称前缀；任意后缀可能使其合法，因此不检查禁用名称的精确匹配。
//
// Deprecated: 请直接使用 content.IsPathSegmentPrefix。
var IsValidPathSegmentPrefix = content.IsPathSegmentPrefix

// Deprecated: 改用本地定义的函数。
//
// prefix 为 true 时按前缀规则校验（允许结尾部分尚未确定），否则按完整
// 名称校验。返回值是可直接拼进 field.ErrorList 的错误描述列表。
func ValidatePathSegmentName(name string, prefix bool) []string {
	if prefix {
		return IsValidPathSegmentPrefix(name)
	}
	return IsValidPathSegmentName(name)
}
