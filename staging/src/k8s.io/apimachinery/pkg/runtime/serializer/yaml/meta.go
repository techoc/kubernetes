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

package yaml

import (
	"fmt"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/yaml"
)

// DefaultMetaFactory 是 JSON/YAML 对象的默认版本信息工厂，内存对象和默认
// 序列化都使用 "kind" 与 "apiVersion" 字段。
var DefaultMetaFactory = SimpleMetaFactory{}

// SimpleMetaFactory 提供默认实现，从 JSON 序列化中的 "apiVersion" 与 "kind"
// 字段读取对象的类型和版本。内存字段名可配置，也可指定一组基础结构体用于
// 在其中查找这些字段。
//
// 当前实现不保存任何配置，仅按标准字段名解析；保留结构体是为了将来支持
// 自定义字段名而不破坏 API。
type SimpleMetaFactory struct{}

// Interpret 返回对象 JSON 在线格式中的 APIVersion 和 Kind，或返回错误。
//
// 用 yaml.Unmarshal 解析（YAML 是 JSON 超集），因此同时支持两种输入格式。
func (SimpleMetaFactory) Interpret(data []byte) (*schema.GroupVersionKind, error) {
	gvk := runtime.TypeMeta{}
	if err := yaml.Unmarshal(data, &gvk); err != nil {
		return nil, fmt.Errorf("could not interpret GroupVersionKind; unmarshal error: %v", err)
	}
	gv, err := schema.ParseGroupVersion(gvk.APIVersion)
	if err != nil {
		return nil, err
	}
	return &schema.GroupVersionKind{Group: gv.Group, Version: gv.Version, Kind: gvk.Kind}, nil
}
