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

package json

import "testing"

// TestSimpleMetaFactoryInterpret 验证 SimpleMetaFactory 能仅凭 apiVersion/kind 两个字段
// 识别出对象的版本与类型，且对缺失字段与非法 JSON 的处理符合预期。
//
// 该工厂用于在完整反序列化之前做类型探测，因此必须容忍只有元数据的输入。
func TestSimpleMetaFactoryInterpret(t *testing.T) {
	factory := SimpleMetaFactory{}
	// 常规输入：两个字段都存在。
	gvk, err := factory.Interpret([]byte(`{"apiVersion":"1","kind":"object"}`))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if gvk.Version != "1" || gvk.Kind != "object" {
		t.Errorf("unexpected interpret: %#v", gvk)
	}

	// no kind or version
	// 空对象不是错误，应返回零值 GVK，交由上层决定是否报错。
	gvk, err = factory.Interpret([]byte(`{}`))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if gvk.Version != "" || gvk.Kind != "" {
		t.Errorf("unexpected interpret: %#v", gvk)
	}

	// unparsable
	// 非法 JSON 必须返回错误，避免把解析失败误判为"无类型信息"。
	_, err = factory.Interpret([]byte(`{`))
	if err == nil {
		t.Errorf("unexpected non-error")
	}
}
