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

	"k8s.io/kube-openapi/pkg/common"
	"k8s.io/kube-openapi/pkg/validation/spec"
	"k8s.io/kubernetes/pkg/generated/openapi"
)

// main 输出包含 zz_generated.openapi.go 中所有 schema 定义的 OpenAPI JSON。
func main() {
	err := output()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed: %v", err) // nolint:errcheck
		os.Exit(1)
	}
}

// output 构造并输出 OpenAPI v2 格式的完整 schema 文档。
func output() error {
	refFunc := func(name string) spec.Ref {
		return spec.MustCreateRef(fmt.Sprintf("#/definitions/%s", name))
	}
	defs := openapi.GetOpenAPIDefinitions(refFunc)
	schemaDefs := make(map[string]spec.Schema, len(defs))
	for k, v := range defs {
		// 若类型内嵌了 v2 schema，就用它替换顶层 schema，
		// 保证本程序输出的始终是 OpenAPI v2。
		// 做法是通过扩展字段查找内嵌的 v2 schema，找到后将其作为该类型的最终 schema。
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
