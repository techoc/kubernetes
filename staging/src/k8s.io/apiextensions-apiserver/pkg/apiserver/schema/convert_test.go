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

// TestStructuralRoundtripOrError 对 JSONSchemaProps 的随机字段做 fuzz：
// NewStructural 成功时，结构必须能依次经由 go-openapi、JSON、v1beta1、
// 内部类型往返而不丢失信息；失败则不要求往返。
func TestStructuralRoundtripOrError(t *testing.T) {
	f := randfill.New()
	seed := time.Now().UnixNano()
	t.Logf("seed = %v", seed)
	//seed = int64(1549012506261785182)
	f.RandSource(rand.New(rand.NewSource(seed)))
	f.Funcs(
		func(s *apiextensions.JSON, c randfill.Continue) {
			*s = apiextensions.JSON(map[string]interface{}{"foo": float64(42.2)})
		},
		func(s *apiextensions.JSONSchemaPropsOrArray, c randfill.Continue) {
			c.FillNoCustom(s)
			// Schema 与 JSONSchemas 互斥，随机填充后强制只留一个。
			if s.Schema != nil {
				s.JSONSchemas = nil
			} else if s.JSONSchemas == nil {
				s.Schema = &apiextensions.JSONSchemaProps{}
			}
		},
		func(s *apiextensions.JSONSchemaPropsOrBool, c randfill.Continue) {
			c.FillNoCustom(s)
			if s.Schema != nil {
				s.Allows = false
			}
		},
		func(s **string, c randfill.Continue) {
			c.FillNoCustom(s)
			// nil 与空串在序列化后无法区分，这里统一把空串折叠为 nil。
			if *s != nil && **s == "" {
				*s = nil
			}
		},
	)

	f.MaxDepth(2)
	f.NilChance(0.5)

	for i := 0; i < 10000; i++ {
		// fuzz JSONSchemaProps 中的单个随机字段
		origSchema := &apiextensions.JSONSchemaProps{}
		x := reflect.ValueOf(origSchema).Elem()
		n := rand.Intn(x.NumField())
		f.Fill(x.Field(n).Addr().Interface())

		// 要么往返成功，要么 NewStructural 报错；任何字段都不应被静默丢弃
		orig, err := NewStructural(origSchema)
		if err != nil {
			continue
		}

		// 依次经过 go-openapi、JSON、v1beta1 JSONSchemaProp、内部 JSONSchemaProp 往返
		goOpenAPI := orig.ToKubeOpenAPI()
		bs, err := json.Marshal(goOpenAPI)
		if err != nil {
			t.Fatal(err)
		}
		v1beta1Schema := &apiextensionsv1beta1.JSONSchemaProps{}
		err = json.Unmarshal([]byte(bs), v1beta1Schema)
		if err != nil {
			t.Fatal(err)
		}
		internalSchema := &apiextensions.JSONSchemaProps{}
		err = apiextensionsv1beta1.Convert_v1beta1_JSONSchemaProps_To_apiextensions_JSONSchemaProps(v1beta1Schema, internalSchema, nil)
		if err != nil {
			t.Fatal(err)
		}

		if !reflect.DeepEqual(origSchema, internalSchema) {
			t.Fatalf("original and result differ: %v", cmp.Diff(origSchema, internalSchema))
		}
	}
}
