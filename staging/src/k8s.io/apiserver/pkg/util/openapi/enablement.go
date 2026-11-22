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

// Package openapi 提供 apiserver 对外暴露 OpenAPI 定义时的辅助逻辑，
// 包括按特性门控状态裁剪 schema（例如关闭 OpenAPIEnums 时移除枚举信息）。
package openapi

import (
	"strings"

	genericfeatures "k8s.io/apiserver/pkg/features"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/kube-openapi/pkg/common"
	"k8s.io/kube-openapi/pkg/schemamutation"
	"k8s.io/kube-openapi/pkg/validation/spec"
)

// enumTypeDescriptionHeader is the header of enum section in schema description.
const enumTypeDescriptionHeader = "Possible enum values:"

// GetOpenAPIDefinitionsWithoutDisabledFeatures wraps a GetOpenAPIDefinitions to revert
// any change to the schema that was made by disabled features.
// 目的：生成的 OpenAPI 定义可能包含默认启用的特性内容，但运行时特性门控关闭时，
// 对外暴露的 schema 必须与门控状态一致，否则客户端会看到实际不可用的字段。
func GetOpenAPIDefinitionsWithoutDisabledFeatures(GetOpenAPIDefinitions common.GetOpenAPIDefinitions) common.GetOpenAPIDefinitions {
	return func(ref common.ReferenceCallback) map[string]common.OpenAPIDefinition {
		defs := GetOpenAPIDefinitions(ref)
		restoreDefinitions(defs)
		return defs
	}
}

// restoreDefinitions restores any changes by disabled features from definition map.
func restoreDefinitions(defs map[string]common.OpenAPIDefinition) {
	// revert changes from OpenAPIEnums
	if !utilfeature.DefaultFeatureGate.Enabled(genericfeatures.OpenAPIEnums) {
		for gvk, def := range defs {
			orig := &def.Schema
			// pruneEnums 未发生修改时返回同一指针，可据此跳过回写，减少 map 更新。
			if ret := pruneEnums(orig); ret != orig {
				def.Schema = *ret
				defs[gvk] = def
			}
		}
	}
}

// pruneEnums 从 schema 中移除枚举值及描述中的枚举段落，返回可能被克隆的新 schema。
// 使用写时复制（copy-on-write）：只有真正需要修改的节点才克隆，
// 避免深拷贝整棵 schema 树。
func pruneEnums(schema *spec.Schema) *spec.Schema {
	walker := schemamutation.Walker{
		SchemaCallback: func(schema *spec.Schema) *spec.Schema {
			orig := schema
			clone := func() {
				if orig == schema { // if schema has not been mutated yet
					schema = new(spec.Schema)
					*schema = *orig // make a clone from orig to schema
				}
			}
			if headerIndex := strings.Index(schema.Description, enumTypeDescriptionHeader); headerIndex != -1 {
				// remove the enum section from description.
				// note that the new lines before the header should be removed too,
				// thus the slice range.
				clone()
				schema.Description = strings.TrimSpace(schema.Description[:headerIndex])
			}
			if len(schema.Enum) != 0 {
				// remove the enum field
				clone()
				schema.Enum = nil
			}
			return schema
		},
		RefCallback: schemamutation.RefCallbackNoop,
	}
	return walker.WalkSchema(schema)
}
