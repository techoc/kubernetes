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
	"k8s.io/sample-apiserver/pkg/generated/openapi"
)

// main 输出 zz_generated.openapi.go 中的 schema 定义（OpenAPI JSON）；
// 走 stdout 输出，便于生成脚本直接重定向到文件。
func main() {
	err := output()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed: %v", err) // nolint:errcheck
		os.Exit(1)
	}
}

// 集中生成 schema 而不是逐类型手写，保证定义和生成代码同步。
func output() error {
	refFunc := func(name string) spec.Ref {
		return spec.MustCreateRef(fmt.Sprintf("#/definitions/%s", name))
	}
	defs := openapi.GetOpenAPIDefinitions(refFunc)
	schemaDefs := make(map[string]spec.Schema, len(defs))
	for k, v := range defs {
		// 有的定义同时带 v3 主 schema 与内嵌的 v2；通过标记扩展找到 v2 后取而代之，
		// 保证输出始终是 OpenAPI v2，下游不会拿到混合版本。
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
