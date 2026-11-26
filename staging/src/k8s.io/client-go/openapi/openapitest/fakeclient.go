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

// fakeclient.go 提供 openapi.Client/GroupVersion 的可控测试替身；
// 包级说明见 fileclient.go。

package openapitest

import (
	"fmt"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/openapi"
)

// FakeClient 是 openapi.Client 的测试实现，返回硬编码值，并可通过 ForcedErr 强制出错。
type FakeClient struct {
	// PathsMap 是 Paths() 直接返回的映射，key 为 "/apis/<group>/<version>" 形式。
	PathsMap map[string]openapi.GroupVersion
	// ForcedErr 非 nil 时，Paths() 直接返回该错误。
	ForcedErr error
}

// 编译期断言：确保 FakeClient 实现了 openapi.Client。
var _ openapi.Client = &FakeClient{}

// NewFakeClient 返回一个 PathsMap 为空的 fake openapi 客户端。
func NewFakeClient() *FakeClient {
	return &FakeClient{PathsMap: make(map[string]openapi.GroupVersion)}
}

// Paths 返回保存的 PathsMap（为空则先创建）；若设置了 ForcedErr 则返回该错误。
func (f FakeClient) Paths() (map[string]openapi.GroupVersion, error) {
	if f.ForcedErr != nil {
		return nil, f.ForcedErr
	}
	return f.PathsMap, nil
}

// FakeGroupVersion 是 openapi.GroupVersion 的测试实现，返回硬编码的 GroupVersion schema
// 字节；若设置了 ForcedErr，Schema() 返回该错误而非 GVSpec。
type FakeGroupVersion struct {
	// GVSpec 为直接返回的 OpenAPI 文档字节。
	GVSpec []byte
	// ForcedErr 非 nil 时，Schema() 直接返回该错误。
	ForcedErr error
}

// 编译期断言：确保 FakeGroupVersion 实现了 openapi.GroupVersion。
var _ openapi.GroupVersion = &FakeGroupVersion{}

// Schema 返回硬编码字节（未设置时返回空切片）；设置了 ForcedErr 则返回错误。
// 仅支持 application/json，其他 content type 一律报错。
func (f FakeGroupVersion) Schema(contentType string) ([]byte, error) {
	// 测试替身只支持 JSON，遇到其他 content type 立即报错，避免误用。
	if contentType != runtime.ContentTypeJSON {
		return nil, fmt.Errorf("application/json is only content type supported: %s", contentType)
	}
	if f.ForcedErr != nil {
		return nil, f.ForcedErr
	}
	return f.GVSpec, nil
}

// ServerRelativeURL 返回空字符串。
func (f FakeGroupVersion) ServerRelativeURL() string {
	// 未实现：测试替身不提供相对 URL，调用即 panic 以尽早暴露依赖。
	panic("unimplemented")
}
