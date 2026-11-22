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

package schema

import (
	"math/rand"
	"reflect"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"sigs.k8s.io/randfill"

	"k8s.io/apiextensions-apiserver/pkg/apis/apiextensions"
	apiextensionsv1beta1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1beta1"
	"k8s.io/apimachinery/pkg/util/json"
)

// isJSONEmptyForOmitempty 判断 JSON 值是否会被 kube-openapi v2 JSON 编码器的
// omitempty 标签丢弃。与 Default 使用的 omitzero 不同，Example 字段使用 omitempty，
// 因此空字符串、空切片和空 map 都算作“空”。
func isJSONEmptyForOmitempty(j JSON) bool {
	switch v := j.Object.(type) {
	case string:
		return v == ""
	case []interface{}:
		return len(v) == 0
	case map[string]interface{}:
		return len(v) == 0
	default:
		return j.Object == nil
	}
}

// TestStructuralKubeOpenAPIRoundtrip 用随机数据验证 Structural 经 kube-openapi
// 序列化再反序列化后保持不变，防止 JSON 标签或转换逻辑悄悄丢失字段。
func TestStructuralKubeOpenAPIRoundtrip(t *testing.T) {
	f := randfill.New()
	seed := time.Now().UnixNano()
	t.Logf("seed = %v", seed)
	//seed = int64(1549012506261785182)
	f.RandSource(rand.New(rand.NewSource(seed)))
	// 显式覆盖 JSON 值，确保空字符串、空切片、空 map 等边界值都参与测试。
	f.Funcs(
		func(s *JSON, c randfill.Continue) {
			switch c.Intn(7) {
			case 0:
				s.Object = float64(42.2)
			case 1:
				s.Object = map[string]interface{}{"foo": "bar"}
			case 2:
				s.Object = ""
			case 3:
				s.Object = []interface{}{}
			case 4:
				s.Object = map[string]interface{}{}
			case 5:
				s.Object = nil
			case 6:
				s.Object = int64(42)
			}
		},
	)
	f.MaxDepth(3)
	f.NilChance(0.5)

	for i := 0; i < 10000; i++ {
		orig := &Structural{}
		f.Fill(orig)

		// 把原本为 nil 的 ValueValidation 规范化为零值，
		// 并规范化会被 omitempty 视为空的 Example 值：
		// kube-openapi 的 Example 使用 omitempty 而非 omitzero，
		// JSON 往返时空字符串/切片/map 会被丢弃。
		normalizer := Visitor{
			Structural: func(s *Structural) bool {
				changed := false
				if s.ValueValidation == nil {
					s.ValueValidation = &ValueValidation{}
					changed = true
				}
				if isJSONEmptyForOmitempty(s.Generic.Example) {
					s.Generic.Example = JSON{}
					changed = true
				}
				return changed
			},
		}
		normalizer.Visit(orig)

		kubeOpenAPI := orig.ToKubeOpenAPI()
		bs, err := json.Marshal(kubeOpenAPI)
		if err != nil {
			t.Fatal(err)
		}
		v1beta1Schema := &apiextensionsv1beta1.JSONSchemaProps{}
		err = json.Unmarshal(bs, v1beta1Schema)
		if err != nil {
			t.Fatal(err)
		}
		internalSchema := &apiextensions.JSONSchemaProps{}
		err = apiextensionsv1beta1.Convert_v1beta1_JSONSchemaProps_To_apiextensions_JSONSchemaProps(v1beta1Schema, internalSchema, nil)
		if err != nil {
			t.Fatal(err)
		}
		s, err := NewStructural(internalSchema)
		if err != nil {
			t.Fatal(err)
		}

		if !reflect.DeepEqual(orig, s) {
			t.Fatalf("original and result differ: %v", cmp.Diff(orig, s))
		}
	}
}
