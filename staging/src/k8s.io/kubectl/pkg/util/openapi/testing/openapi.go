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

package testing

import (
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/kube-openapi/pkg/util/proto"
	"k8s.io/kube-openapi/pkg/util/proto/testing"
	"k8s.io/kubectl/pkg/util/openapi"
)

// 直接从文件加载 openapi schema 并按 GVK 返回，仅用于测试：
// 假设文件一定存在且一切正常。
type FakeResources struct {
	fake testing.Fake
}

var _ openapi.Resources = &FakeResources{}

// 创建 FakeResources。
func NewFakeResources(path string) *FakeResources {
	return &FakeResources{
		fake: testing.Fake{Path: path},
	}
}

// 读取并解析 schema 后返回资源；不返回错误，出错直接 panic。
func (f *FakeResources) LookupResource(gvk schema.GroupVersionKind) proto.Schema {
	s, err := f.fake.OpenAPISchema()
	if err != nil {
		panic(err)
	}
	resources, err := openapi.NewOpenAPIData(s)
	if err != nil {
		panic(err)
	}
	return resources.LookupResource(gvk)
}

func (f *FakeResources) GetConsumes(gvk schema.GroupVersionKind, operation string) []string {
	s, err := f.fake.OpenAPISchema()
	if err != nil {
		panic(err)
	}

	resources, err := openapi.NewOpenAPIData(s)
	if err != nil {
		panic(err)
	}
	return resources.GetConsumes(gvk, operation)
}

// 不含任何资源的 Resources 实现。
type EmptyResources struct{}

var _ openapi.Resources = EmptyResources{}

// 始终返回 nil，因为没有资源。
func (f EmptyResources) LookupResource(gvk schema.GroupVersionKind) proto.Schema {
	return nil
}

func (f EmptyResources) GetConsumes(gvk schema.GroupVersionKind, operation string) []string {
	return nil
}

// 返回供 TestFactory 使用的函数。
func CreateOpenAPISchemaFunc(path string) func() (openapi.Resources, error) {
	return func() (openapi.Resources, error) {
		return NewFakeResources(path), nil
	}
}
