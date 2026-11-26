/*
Copyright The Kubernetes Authors.

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

package external

import (
	"context"
	"testing"

	"k8s.io/apimachinery/pkg/api/operation"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/code-generator/cmd/validation-gen/output_tests/multiple_packages/registered"
	"k8s.io/code-generator/cmd/validation-gen/output_tests/multiple_packages/types"
)

// TestSelfContained 通过直接调用校验函数（不经过 scheme）验证本包生成的
// 校验器，并确认其与 registered 副本产生一致的错误。
func TestSelfContained(t *testing.T) {
	ctx := context.Background()
	matcher := field.ErrorMatcher{}.ByField().ByDetailExact()

	// 共享类型必须与注册副本一致。
	// T1/T2 在两个包中都会生成，结果必须逐字段一致。
	obj := &types.T1{List: []types.T2{{}, {}}}
	ext := Validate_T1(ctx, operation.Operation{}, nil, obj, nil)
	if len(ext) == 0 {
		t.Fatalf("expected validation errors from Validate_T1, got none")
	}
	reg := registered.Validate_T1(ctx, operation.Operation{}, nil, obj, nil)
	matcher.Test(t, reg, ext)

	// T3 只被 external 选中，registered 中没有 Validate_T3。
	// 说明 selection（+k8s:validation-gen=*）只影响本包副本。
	got := Validate_T3(ctx, operation.Operation{}, nil, &types.T3{}, nil)
	want := field.ErrorList{
		field.Invalid(field.NewPath("s"), nil, "forced failure: field T3.S"),
	}
	matcher.Test(t, want, got)
}
