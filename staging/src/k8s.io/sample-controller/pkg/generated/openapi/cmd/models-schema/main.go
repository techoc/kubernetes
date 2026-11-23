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
	"k8s.io/sample-controller/pkg/generated/openapi"
)

// Outputs openAPI schema JSON containing the schema definitions in zz_generated.openapi.go.
//
// 本文件位于 generated 目录下的 cmd 子目录，属于手写工具而非生成产物：
// 它读取生成好的定义并输出稳定的 v2 schema，供其他工具或校验流程消费。
func main() {
	err := output()
	if err != nil {
		// 写到 stderr 而不是 stdout，避免错误信息混进本该是纯 JSON 的输出里。
		fmt.Fprintf(os.Stderr, "Failed: %v", err) // nolint:errcheck
		os.Exit(1)
	}
}

// output 把生成的内嵌定义整理成 OpenAPI v2 文档写到标准输出。
func output() error {
	// 引用统一指向本地 definitions，输出的 schema 才能被独立解析、不依赖外部地址。
	refFunc := func(name string) spec.Ref {
		return spec.MustCreateRef(fmt.Sprintf("#/definitions/%s", name))
	}
	defs := openapi.GetOpenAPIDefinitions(refFunc)
	schemaDefs := make(map[string]spec.Schema, len(defs))
	for k, v := range defs {
		// Replace top-level schema with v2 if a v2 schema is embedded
		// so that the output of this program is always in OpenAPI v2.
		// This is done by looking up an extension that marks the embedded v2
		// schema, and, if the v2 schema is found, make it the resulting schema for
		// the type.
		//
		// 生成器可能同时给出 v3 主 schema 和内嵌的 v2 版本；下游约定只接受 v2，
		// 所以这里优先取内嵌版本，拿不到才回退到主 schema。
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
