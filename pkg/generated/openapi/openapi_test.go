/*
Copyright 2019 The Kubernetes Authors.

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
	"testing"

	"github.com/go-openapi/jsonreference"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"

	"k8s.io/kube-openapi/pkg/common"
	"k8s.io/kube-openapi/pkg/handler"
	"k8s.io/kube-openapi/pkg/validation/spec"
)

// TestOpenAPIRoundtrip 验证所有生成的 OpenAPI schema 经 JSON 序列化/反序列化后保持一致。
func TestOpenAPIRoundtrip(t *testing.T) {
	dummyRef := func(name string) spec.Ref { return spec.MustCreateRef("#/definitions/dummy") }
	for name, value := range GetOpenAPIDefinitions(dummyRef) {
		t.Run(name, func(t *testing.T) {
			// TODO(kubernetes/gengo#193): 目前整数会在 round-trip 中变成浮点数。
			value.Schema = *handler.PruneDefaultsSchema(&value.Schema)
			data, err := json.Marshal(value.Schema)
			if err != nil {
				t.Error(err)
				return
			}

			roundTripped := spec.Schema{}
			if err := json.Unmarshal(data, &roundTripped); err != nil {
				t.Error(err)
				return
			}

			// 删除内嵌的 v2 schema：它要么直接作为 schema（v2 场景），要么被裁剪掉（v3），
			// 本身不参与 round-trip，因此不纳入比较。
			delete(roundTripped.Extensions, common.ExtensionV2Schema)
			delete(value.Schema.Extensions, common.ExtensionV2Schema)

			opts := []cmp.Option{
				cmpopts.EquateEmpty(),
				// jsonreference.Ref 含未导出字段，用字符串形式比较可以得到一致的结果：
				cmp.Comparer(func(x, y jsonreference.Ref) bool {
					return x.String() == y.String()
				}),
			}
			if !cmp.Equal(value.Schema, roundTripped, opts...) {
				t.Errorf("unexpected diff (a=expected,b=roundtripped):\n%s", cmp.Diff(value.Schema, roundTripped, opts...))
				return
			}
		})
	}
}
