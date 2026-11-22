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

// Package json 实现基于 JSON 的序列化器，支持 YAML 互转与对象版本/类型的元数据提取。
package json

import (
	"encoding/json"
	"fmt"

	"k8s.io/apimachinery/pkg/runtime/schema"
)

// MetaFactory is used to store and retrieve the version and kind
// information for JSON objects in a serializer.
//
// MetaFactory 在序列化器中读取 JSON 对象的版本与类型信息。
type MetaFactory interface {
	// Interpret should return the version and kind of the wire-format of
	// the object.
	//
	// Interpret 返回对象线上格式的版本与类型。
	Interpret(data []byte) (*schema.GroupVersionKind, error)
}

// DefaultMetaFactory is a default factory for versioning objects in JSON. The object
// in memory and in the default JSON serialization will use the "kind" and "apiVersion"
// fields.
//
// DefaultMetaFactory 是 JSON 对象版本信息的默认工厂：
// 内存对象与默认 JSON 序列化都使用 "kind" 与 "apiVersion" 字段。
var DefaultMetaFactory = SimpleMetaFactory{}

// SimpleMetaFactory provides default methods for retrieving the type and version of objects
// that are identified with an "apiVersion" and "kind" fields in their JSON
// serialization. It may be parameterized with the names of the fields in memory, or an
// optional list of base structs to search for those fields in memory.
//
// SimpleMetaFactory 提供默认方法，从 JSON 序列化中带有 "apiVersion" 与 "kind" 字段的对象里
// 读取类型与版本。
type SimpleMetaFactory struct {
}

// Interpret will return the APIVersion and Kind of the JSON wire-format
// encoding of an object, or an error.
//
// Interpret 返回对象 JSON 线上格式中的 APIVersion 与 Kind，解析失败时返回错误。
func (SimpleMetaFactory) Interpret(data []byte) (*schema.GroupVersionKind, error) {
	// 只解析 apiVersion/kind 两个字段，忽略其余内容。
	findKind := struct {
		// +optional
		APIVersion string `json:"apiVersion,omitempty"`
		// +optional
		Kind string `json:"kind,omitempty"`
	}{}
	if err := json.Unmarshal(data, &findKind); err != nil {
		return nil, fmt.Errorf("couldn't get version/kind; json parse error: %v", err)
	}
	gv, err := schema.ParseGroupVersion(findKind.APIVersion)
	if err != nil {
		return nil, err
	}
	return &schema.GroupVersionKind{Group: gv.Group, Version: gv.Version, Kind: findKind.Kind}, nil
}
