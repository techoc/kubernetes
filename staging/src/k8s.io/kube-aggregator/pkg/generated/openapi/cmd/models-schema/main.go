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

	"k8s.io/kube-aggregator/pkg/generated/openapi"
	"k8s.io/kube-openapi/pkg/common"
	"k8s.io/kube-openapi/pkg/validation/spec"
)

// 输出包含 zz_generated.openapi.go 中定义的 OpenAPI schema JSON。
// 这个小程序把生成的定义导出为 JSON，供模型生成等下游工具消费；
// main 只做错误到退出码的转换，真正的组装逻辑放在 output 里以便测试。
func main() {
	err := output()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed: %v", err) // nolint:errcheck
		os.Exit(1)
	}
}

func output() error {
	// 生成的定义用局部引用指向 definitions，因此这里给出一个把类型名映射为
	// #/definitions/<name> 的引用函数，让各定义之间的引用能正确解析。
	refFunc := func(name string) spec.Ref {
		return spec.MustCreateRef(fmt.Sprintf("#/definitions/%s", name))
	}
	defs := openapi.GetOpenAPIDefinitions(refFunc)
	schemaDefs := make(map[string]spec.Schema, len(defs))
	for k, v := range defs {
		// 若内嵌了 v2 schema 就用它替换顶层 schema，确保本程序输出始终为 OpenAPI v2。
		// 做法是查找标记内嵌 v2 schema 的扩展；找到后将其作为该类型的最终 schema。
		// 生成器可能同时产出 v3 与 v2 两种 schema，用扩展字段标记其中的 v2 版本；
		// 下游只认 v2，所以存在该扩展时用 v2 替换顶层定义。
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
	// 写到标准输出，让调用方可以用管道把结果重定向到任意文件；
	// 错误在 main 中统一处理，这里沿用 nolint 标注忽略 Write 的返回值。
	os.Stdout.Write(data) // nolint:errcheck
	return nil
}
