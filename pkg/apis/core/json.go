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

package core

import "encoding/json"

// 本文件为那些当前会被序列化进注解（annotation）的内部对象实现 json 编解码接口，
// 目的是阻止任何人直接序列化这些内部结构体：它们只应通过外部版本转换后再落盘。

var _ = json.Marshaler(&AvoidPods{})
var _ = json.Unmarshaler(&AvoidPods{})

// MarshalJSON 故意 panic，防止内部结构体被直接序列化。
// 内部版本与对外 wire 版本不同，直接序列化会暴露不应公开的数据结构。
func (AvoidPods) MarshalJSON() ([]byte, error) { panic("do not marshal internal struct") }

// UnmarshalJSON 故意 panic，防止数据被直接反序列化到内部结构体。
// 数据必须先经过外部版本校验与转换，才能进入内部表示。
func (*AvoidPods) UnmarshalJSON([]byte) error { panic("do not unmarshal to internal struct") }
