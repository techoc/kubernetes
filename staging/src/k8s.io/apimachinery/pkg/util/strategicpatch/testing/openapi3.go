/*
Copyright 2023 The Kubernetes Authors.

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
	"os"
	"sync"

	"k8s.io/kube-openapi/pkg/spec3"
)

// OpenAPIV3Getter 按需从磁盘加载 OpenAPI v3 规范文件，用于测试 strategic merge patch。
//
// 读取结果通过 sync.Once 缓存，避免同一测试中重复解析大体积规范文件。
type OpenAPIV3Getter struct {
	Path      string
	once      sync.Once
	bytes     []byte
	openapiv3 spec3.OpenAPI
}

// SchemaBytesOrDie 返回规范文件的原始字节；文件不存在或不可读时 panic。
//
// 测试数据缺失属于测试环境问题，panic 能让失败点直接指向缺失的文件，
// 而不是在后续解析阶段抛出难以定位的错误。
func (f *OpenAPIV3Getter) SchemaBytesOrDie() []byte {
	f.once.Do(func() {
		_, err := os.Stat(f.Path)
		if err != nil {
			panic(err)
		}
		spec, err := os.ReadFile(f.Path)
		if err != nil {
			panic(err)
		}
		f.bytes = spec
	})
	return f.bytes
}

// SchemaOrDie 返回解析后的 OpenAPI v3 规范；文件缺失、不可读或 JSON 非法时 panic。
//
// 注意：它与 SchemaBytesOrDie 共用同一个 once，先调用任一个都会阻止另一个的初始化逻辑执行。
// 在单个测试内应只使用其中一个方法。
func (f *OpenAPIV3Getter) SchemaOrDie() *spec3.OpenAPI {
	f.once.Do(func() {
		_, err := os.Stat(f.Path)
		if err != nil {
			panic(err)
		}
		spec, err := os.ReadFile(f.Path)
		if err != nil {
			panic(err)
		}

		err = f.openapiv3.UnmarshalJSON(spec)
		if err != nil {
			panic(err)
		}
	})
	return &f.openapiv3
}
