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

package testing

import (
	"testing"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer/json"
	"k8s.io/apimachinery/pkg/runtime/serializer/recognizer"
)

// A 是用于识别器测试的最小 runtime.Object，只有类型元信息，没有实际字段。
type A struct{}

// GetObjectKind 返回空的 ObjectKind，满足 runtime.Object 接口。
func (A) GetObjectKind() schema.ObjectKind { return schema.EmptyObjectKind }

// DeepCopyObject 对无字段类型直接返回自身副本。
func (a A) DeepCopyObject() runtime.Object {
	return a
}

// TestRecognizer 验证 recognizer.Decoder 能在多编码器之间自动选择：
// 同样的 kind/apiVersion，分别以 YAML 和 JSON 输入时都应被正确识别并解码。
func TestRecognizer(t *testing.T) {
	s := runtime.NewScheme()
	s.AddKnownTypes(schema.GroupVersion{Version: "v1"}, &A{})
	d := recognizer.NewDecoder(
		json.NewSerializerWithOptions(json.DefaultMetaFactory, s, s, json.SerializerOptions{}),
		json.NewSerializerWithOptions(json.DefaultMetaFactory, s, s, json.SerializerOptions{Yaml: true}),
	)
	// YAML 输入：识别器应选中 YAML 序列化器。
	out, _, err := d.Decode([]byte(`
kind: A
apiVersion: v1
`), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("%#v", out)

	// JSON 输入：同一识别器应改选 JSON 序列化器。
	out, _, err = d.Decode([]byte(`
{
  "kind":"A",
  "apiVersion":"v1"
}
`), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("%#v", out)
}
