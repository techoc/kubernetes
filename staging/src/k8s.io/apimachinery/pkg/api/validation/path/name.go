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

// IsValidPathSegmentName validates the name can be safely encoded as a path segment
//
// Deprecated: use content.IsPathSegmentName directly.
//
// 保留此别名是为了不破坏既有调用方；实际实现已迁到 content 包，
// 两处行为必须保持一致。
var IsValidPathSegmentName = content.IsPathSegmentName

// IsValidPathSegmentPrefix validates the name can be used as a prefix for a name which will be encoded as a path segment
// It does not check for exact matches with disallowed names, since an arbitrary suffix might make the name valid
//
// Deprecated: use content.IsPathSegmentPrefix directly.
var IsValidPathSegmentPrefix = content.IsPathSegmentPrefix

// ValidatePathSegmentName validates the name can be safely encoded as a path segment
//
// Deprecated: use a locally defined function.
//
// prefix 为 true 时按前缀规则校验（允许结尾部分尚未确定），否则按完整
// 名称校验。返回值是可直接拼进 field.ErrorList 的错误描述列表。
func ValidatePathSegmentName(name string, prefix bool) []string {
	if prefix {
		return IsValidPathSegmentPrefix(name)
	}
	return IsValidPathSegmentName(name)
}
