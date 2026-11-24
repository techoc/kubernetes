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

package v1

import (
	"encoding/json"
	"time"
)

// Duration 包装 time.Duration，并提供符合 Kubernetes API 约定的 YAML/JSON 序列化。
// 它始终编码为字符串（例如 "1m30s"），因此也可以安全地用作 JSON 对象的键。
type Duration struct {
	time.Duration `protobuf:"varint,1,opt,name=duration,casttype=time.Duration"`
}

// UnmarshalJSON implements the json.Unmarshaller interface.
func (d *Duration) UnmarshalJSON(b []byte) error {
	// JSON 数字无法表达时间单位，因此只接受字符串形式的时长。
	var str string
	err := json.Unmarshal(b, &str)
	if err != nil {
		return err
	}

	pd, err := time.ParseDuration(str)
	if err != nil {
		return err
	}
	d.Duration = pd
	return nil
}

// MarshalJSON 实现 json.Marshaler 接口。时长编码为可读字符串（例如 "1m30s"），
// 既保留单位信息，也允许该值出现在 JSON map key 的位置。
func (d Duration) MarshalJSON() ([]byte, error) {
	return json.Marshal(d.Duration.String())
}

// ToUnstructured implements the value.UnstructuredConverter interface.
func (d Duration) ToUnstructured() interface{} {
	return d.Duration.String()
}

// OpenAPISchemaType 供 kube-openapi 生成器构建该类型的 OpenAPI Schema 时调用。
//
// See: https://github.com/kubernetes/kube-openapi/tree/master/pkg/generators
func (_ Duration) OpenAPISchemaType() []string { return []string{"string"} }

// OpenAPISchemaFormat 供 kube-openapi 生成器构建该类型的 OpenAPI Schema 时调用。
func (_ Duration) OpenAPISchemaFormat() string { return "" }
