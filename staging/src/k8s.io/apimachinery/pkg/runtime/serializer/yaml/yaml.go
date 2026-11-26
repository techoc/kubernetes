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

// Package yaml 在 JSON 序列化器之上增加 YAML 解码能力：先把 YAML 转成 JSON，
// 再交给内嵌的 JSON 序列化器处理，避免为 YAML 单独实现一套解码逻辑。
package yaml

import (
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/yaml"
)

// yamlSerializer 把传给 Decoder 方法的 YAML 转换为 JSON。
// 中文补充：只覆写 Decode；编码仍沿用内嵌的 JSON 序列化器。
type yamlSerializer struct {
	// 内层的序列化器
	runtime.Serializer
}

// yamlSerializer 实现 Serializer 接口。
var _ runtime.Serializer = yamlSerializer{}

// NewDecodingSerializer 为支持 JSON 的序列化器加上 YAML 解码能力。
// 中文补充：传入的 jsonSerializer 必须能处理 JSON 输入，YAML 只是前置转换层。
func NewDecodingSerializer(jsonSerializer runtime.Serializer) runtime.Serializer {
	return &yamlSerializer{jsonSerializer}
}

// Decode 先将 YAML 数据转换为等价的 JSON，再委托给内嵌序列化器解码；
// 转换失败时直接返回错误，不做任何部分解码。
func (c yamlSerializer) Decode(data []byte, gvk *schema.GroupVersionKind, into runtime.Object) (runtime.Object, *schema.GroupVersionKind, error) {
	out, err := yaml.ToJSON(data)
	if err != nil {
		return nil, nil, err
	}
	data = out
	return c.Serializer.Decode(data, gvk, into)
}
