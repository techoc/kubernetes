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

package resolver

import (
	"sync"
	"testing"

	"github.com/go-openapi/jsonreference"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"

	"k8s.io/kube-openapi/pkg/validation/spec"
)

// schemaRef 构造一个仅含 $ref 的 schema，用于模拟尚未解析的引用。
func schemaRef(ref string) *spec.Schema {
	return &spec.Schema{
		SchemaProps: spec.SchemaProps{
			Ref: spec.MustCreateRef(ref),
		},
	}
}

// newSchema 构造一个带 ID 的空 schema，作为引用解析的目标；
// ID 用于断言"解析到的是正确的那一个"。
func newSchema(ref string) *spec.Schema {
	return &spec.Schema{
		SchemaProps: spec.SchemaProps{
			ID: ref,
		},
	}
}

// rootSchema 返回一个在各处都含引用的 schema，覆盖 TestPopulateRefs
// 解析引用的所有位置。
//
// 覆盖 PopulateRefs 支持的三类引用位置：properties、additionalProperties 和 items。
// 只要解析器漏掉其中一类，测试就会失败。
func rootSchema() *spec.Schema {
	return &spec.Schema{
		SchemaProps: spec.SchemaProps{
			Properties: map[string]spec.Schema{
				"a": *schemaRef("prop"),
			},
			AdditionalProperties: &spec.SchemaOrBool{
				Schema: schemaRef("additional"),
			},
			Items: &spec.SchemaOrArray{
				Schema: schemaRef("item"),
			},
		},
	}
}

// resolvedRootSchema 是 rootSchema 解析后的期望形态：$ref 被替换为对应 ID 的 schema。
func resolvedRootSchema() *spec.Schema {
	return &spec.Schema{
		SchemaProps: spec.SchemaProps{
			Properties: map[string]spec.Schema{
				"a": *newSchema("prop"),
			},
			AdditionalProperties: &spec.SchemaOrBool{
				Schema: newSchema("additional"),
			},
			Items: &spec.SchemaOrArray{
				Schema: newSchema("item"),
			},
		},
	}
}

// TestPopulateRefs 验证引用解析的正确性与只读性。
func TestPopulateRefs(t *testing.T) {
	schemas := map[string]*spec.Schema{
		"root": rootSchema(),
	}

	// 为上述每个引用各添加一个 schema，并用 ID 验证
	// 解析到的是正确的那一个。
	for _, ref := range []string{"prop", "additional", "item"} {
		schemas[ref] = newSchema(ref)
	}

	schemaOf := func(ref string) (*spec.Schema, bool) {
		schema, ok := schemas[ref]
		return schema, ok
	}

	// 比较根 schema 能检测出语义不同的改写，但检测不出“写入相同值”的多余写入
	// （不太可能，但确实可能发生）。因此这里并发执行两次 PopulateRefs，
	// 让数据竞争检测器报告未经协调的并发写入。
	//
	//
	// 并发跑两次是关键：解析器必须为每个调用方构造新对象，不能就地改写共享的输入
	// schema（OpenAPI 定义会被多个 CEL 编译共用）。语义比较查不出"写入相同值"的问题，
	// 只能靠 -race 捕捉。
	var wg sync.WaitGroup
	var actualResolved *spec.Schema
	wg.Go(func() {
		schema, err := PopulateRefs(schemaOf, "root")
		if err != nil {
			t.Errorf("first PopulateRefs failed: %v", err)
		}
		actualResolved = schema
	})
	wg.Go(func() {
		_, err := PopulateRefs(schemaOf, "root")
		if err != nil {
			t.Errorf("second PopulateRefs failed: %v", err)
		}
	})
	wg.Wait()

	if diff := cmp.Diff(resolvedRootSchema(), actualResolved, cmpopts.IgnoreUnexported(jsonreference.Ref{})); diff != "" {
		t.Errorf("unexpected resolved schema (- want, + got):\n%s", diff)
	}

	// 再确认输入 schema 未被就地修改，保护调用方之间的共享数据。
	if diff := cmp.Diff(rootSchema(), schemas["root"], cmpopts.IgnoreUnexported(jsonreference.Ref{})); diff != "" {
		t.Errorf("read-only input schema got modified (- original, + modification):\n%s", diff)
	}
}
