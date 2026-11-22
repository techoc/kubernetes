/*
Copyright 2021 The Kubernetes Authors.

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

package main

import (
	"encoding/json"
	"fmt"
	"os"

	"k8s.io/apiextensions-apiserver/pkg/generated/openapi"
	"k8s.io/kube-openapi/pkg/common"
	"k8s.io/kube-openapi/pkg/validation/spec"
)

// main 把 zz_generated.openapi.go 中的 schema 定义序列化为 OpenAPI v2 JSON 写到 stdout。
// 生成物供下游工具消费，失败时以非零码退出。
func main() {
	err := output()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed: %v", err) // nolint:errcheck
		os.Exit(1)
	}
}

// output 构造仅含 definitions 的 Swagger 2.0 文档并输出。
func output() error {
	refFunc := func(name string) spec.Ref {
		return spec.MustCreateRef(fmt.Sprintf("#/definitions/%s", name))
	}
	defs := openapi.GetOpenAPIDefinitions(refFunc)
	schemaDefs := make(map[string]spec.Schema, len(defs))
	for k, v := range defs {
		// 若 schema 内嵌了 v2 版本，则用内嵌版本替换顶层 schema，
		// 保证本程序输出始终是 OpenAPI v2。做法是查找标记内嵌 v2 schema 的扩展；
		// 找到后把该 v2 schema 作为此类型的最终 schema。
		if schema, ok := v.Schema.Extensions[common.ExtensionV2Schema]; ok {
			if v2Schema, isOpenAPISchema := schema.(spec.Schema); isOpenAPISchema {
				schemaDefs[k] = v2Schema
				continue
			}
		}

		schemaDefs[k] = v.Schema
	}
	data, err := json.Marshal(&spec.Swagger{
		SwaggerProps: spec.SwaggerProps{
			Definitions: schemaDefs,
			Info: &spec.Info{
				InfoProps: spec.InfoProps{
					Title:   "Kubernetes",
					Version: "unversioned",
				},
			},
			Swagger: "2.0",
		},
	})
	if err != nil {
		return fmt.Errorf("error serializing api definitions: %w", err)
	}
	os.Stdout.Write(data) // nolint:errcheck
	return nil
}
