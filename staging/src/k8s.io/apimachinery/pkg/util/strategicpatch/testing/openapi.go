/*
Copyright 2017 The Kubernetes Authors.

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

// Package testing 提供测试用的 OpenAPI 文档辅助工具，便于在单元测试中
// 从本地 swagger 文件加载 schema。
package testing

import (
	"os"
	"sync"

	openapi_v2 "github.com/google/gnostic-models/openapiv2"
	openapi "k8s.io/kube-openapi/pkg/util/proto"
)

// Fake 打开并返回文件路径中的 OpenAPI swagger 文档。
// 它只解析一次，之后每次调用都返回同一份副本。
type Fake struct {
	Path string

	once     sync.Once
	document *openapi_v2.Document
	err      error
}

// OpenAPISchema 返回 OpenAPI 文档及可能的错误。
// 首次调用执行文件读取与解析；后续调用复用缓存结果（含错误），
// 因此同一 Fake 只会产生一次 I/O 与解析开销。
func (f *Fake) OpenAPISchema() (*openapi_v2.Document, error) {
	f.once.Do(func() {
		_, err := os.Stat(f.Path)
		if err != nil {
			f.err = err
			return
		}
		spec, err := os.ReadFile(f.Path)
		if err != nil {
			f.err = err
			return
		}
		f.document, f.err = openapi_v2.ParseDocument(spec)
	})
	return f.document, f.err
}

// getSchema 加载文档并按模型名查找对应的 schema。
func getSchema(f *Fake, model string) (openapi.Schema, error) {
	s, err := f.OpenAPISchema()
	if err != nil {
		return nil, err
	}
	m, err := openapi.NewOpenAPIData(s)
	if err != nil {
		return nil, err
	}
	return m.LookupModel(model), nil
}

// GetSchemaOrDie 返回指定模型的 OpenAPI schema；加载或查找失败时 panic，
// 适用于测试中“schema 必然存在”的场景。
func GetSchemaOrDie(f *Fake, model string) openapi.Schema {
	s, err := getSchema(f, model)
	if err != nil {
		panic(err)
	}
	return s
}
