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

package openapi

import (
	"sync"

	openapi_v2 "github.com/google/gnostic-models/openapiv2"
	"k8s.io/client-go/discovery"
)

// 只取一次 openapi schema 并缓存在内存中。
type CachedOpenAPIGetter struct {
	openAPIClient discovery.OpenAPISchemaInterface

	// 缓存结果。
	sync.Once
	openAPISchema *openapi_v2.Document
	err           error
}

var _ discovery.OpenAPISchemaInterface = &CachedOpenAPIGetter{}

// 创建 OpenAPIData 提供者，首次从服务端读取，
// 后续调用直接读内存缓存。
func NewOpenAPIGetter(openAPIClient discovery.OpenAPISchemaInterface) *CachedOpenAPIGetter {
	return &CachedOpenAPIGetter{
		openAPIClient: openAPIClient,
	}
}

// 实现 OpenAPISchemaInterface。
func (g *CachedOpenAPIGetter) OpenAPISchema() (*openapi_v2.Document, error) {
	g.Do(func() {
		g.openAPISchema, g.err = g.openAPIClient.OpenAPISchema()
	})

	// 返回已保存的结果。
	return g.openAPISchema, g.err
}

type CachedOpenAPIParser struct {
	openAPIClient discovery.OpenAPISchemaInterface

	// 缓存结果。
	sync.Once
	openAPIResources Resources
	err              error
}

func NewOpenAPIParser(openAPIClient discovery.OpenAPISchemaInterface) *CachedOpenAPIParser {
	return &CachedOpenAPIParser{
		openAPIClient: openAPIClient,
	}
}

func (p *CachedOpenAPIParser) Parse() (Resources, error) {
	p.Do(func() {
		oapi, err := p.openAPIClient.OpenAPISchema()
		if err != nil {
			p.err = err
			return
		}
		p.openAPIResources, p.err = NewOpenAPIData(oapi)
	})

	return p.openAPIResources, p.err
}
