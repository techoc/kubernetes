/*
Copyright 2014 The Kubernetes Authors.

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

package validate

import (
	"context"

	"k8s.io/apimachinery/pkg/api/operation"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

// FixedResult asserts a fixed boolean result.  This is mostly useful for
// testing.
//
// 返回固定的校验结果，便于在测试中构造“必定通过/必定失败”的 validator；
// arg 会拼进错误详情，用于区分不同的强制失败点。
func FixedResult[T any](_ context.Context, op operation.Operation, fldPath *field.Path, value, _ T, result bool, arg string) field.ErrorList {
	if result {
		return nil
	}
	return field.ErrorList{
		field.Invalid(fldPath, value, "forced failure: "+arg).WithOrigin("validateFalse"),
	}
}
