/*
Copyright 2018 The Kubernetes Authors.

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
	"encoding/json"

	openapi_v2 "github.com/google/gnostic-models/openapiv2"

	"k8s.io/kube-openapi/pkg/util/proto"
	"k8s.io/kube-openapi/pkg/validation/spec"
)

// ToProtoModels 从 OpenAPI spec 构建 proto 格式的模型
//
// proto.Models 是 kube-openapi 中用于结构化遍历 schema 的中间表示，
// CEL 的类型检查依赖它。这里先经过 JSON 往返再解析，是为了复用 gnostic
// 的 v2 解析器，而不是直接操作 spec 结构体。
func ToProtoModels(openAPISpec *spec.Swagger) (proto.Models, error) {
	// gnostic 解析器只接受字节流，因此先把 Swagger 结构体序列化。
	specBytes, err := json.MarshalIndent(openAPISpec, " ", " ")
	if err != nil {
		return nil, err
	}

	// 解析为 OpenAPI v2 文档模型。
	doc, err := openapi_v2.ParseDocument(specBytes)
	if err != nil {
		return nil, err
	}

	// 转换为 kube-openapi 的 proto.Models 抽象。
	models, err := proto.NewOpenAPIData(doc)
	if err != nil {
		return nil, err
	}

	return models, nil
}
