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

package strategicpatch

import (
	"fmt"
)

// LookupPatchMetaError 表示在给定字段路径上查询 patch 元数据失败，
// 包装底层错误并保留路径，便于定位是哪一层 schema 出问题。
type LookupPatchMetaError struct {
	Path string
	Err  error
}

func (e LookupPatchMetaError) Error() string {
	return fmt.Sprintf("LookupPatchMetaError(%s): %v", e.Path, e.Err)
}

// FieldNotFoundError 表示在对象结构中找不到指定的 API 字段。
type FieldNotFoundError struct {
	Path  string
	Field string
}

func (e FieldNotFoundError) Error() string {
	return fmt.Sprintf("unable to find api field %q in %s", e.Field, e.Path)
}

// InvalidTypeError 表示字段的实际类型与 patch 操作期望的类型不符。
type InvalidTypeError struct {
	Path     string
	Expected string
	Actual   string
}

func (e InvalidTypeError) Error() string {
	return fmt.Sprintf("invalid type for %s: got %q, expected %q", e.Path, e.Actual, e.Expected)
}
