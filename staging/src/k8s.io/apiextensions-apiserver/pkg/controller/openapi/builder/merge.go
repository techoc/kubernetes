/*
Copyright 2019 The Kubernetes Authors.

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

package builder

import (
	"fmt"
	"strings"

	"k8s.io/kube-openapi/pkg/aggregator"
	"k8s.io/kube-openapi/pkg/spec3"
	"k8s.io/kube-openapi/pkg/validation/spec"
)

const metadataGV = "io.k8s.apimachinery.pkg.apis.meta.v1"
const autoscalingGV = "io.k8s.api.autoscaling.v1"

// MergeSpecs 聚合所有 OpenAPI spec，复用第一个静态 spec 的元数据作为基础。
// 静态 spec 优先级最高，其路径和定义不会被用户自定义的 CRD 覆盖。
// 输入均不会被修改，但输入与输出共享底层数据结构。
func MergeSpecs(staticSpec *spec.Swagger, crdSpecs ...*spec.Swagger) (*spec.Swagger, error) {
	// 对 staticSpec 做浅拷贝，但替换 paths 和 definitions，因为后续会修改它们。
	specToReturn := *staticSpec
	if staticSpec.Definitions != nil {
		specToReturn.Definitions = make(spec.Definitions, len(staticSpec.Definitions))
		for k, s := range staticSpec.Definitions {
			specToReturn.Definitions[k] = s
		}
	}
	if staticSpec.Parameters != nil {
		specToReturn.Parameters = make(map[string]spec.Parameter, len(staticSpec.Parameters))
		for k, s := range staticSpec.Parameters {
			specToReturn.Parameters[k] = s
		}
	}
	if staticSpec.Paths != nil {
		specToReturn.Paths = &spec.Paths{
			Paths: make(map[string]spec.PathItem, len(staticSpec.Paths.Paths)),
		}
		for k, p := range staticSpec.Paths.Paths {
			specToReturn.Paths.Paths[k] = p
		}
	}

	crdSpec := &spec.Swagger{}
	for _, s := range crdSpecs {
		// 合并不检查冲突，因为命名控制器已阻止用户自定义 CRD 之间的冲突。
		mergeSpec(crdSpec, s)
	}

	// 静态 spec 优先级最高。解析冲突，避免用户自定义 CRD 可能覆盖内建的 apiextensions API。
	if err := aggregator.MergeSpecsIgnorePathConflictRenamingDefinitionsAndParameters(&specToReturn, crdSpec); err != nil {
		return nil, err
	}
	return &specToReturn, nil
}

// mergeSpec 把 source 的 paths、parameters 和 definitions 复制到 dest，
// 只修改 dest，不修改 source。这里假定冲突无关紧要。
func mergeSpec(dest, source *spec.Swagger) {
	if source == nil || source.Paths == nil {
		return
	}
	if dest.Paths == nil {
		dest.Paths = &spec.Paths{}
	}
	for k, v := range source.Definitions {
		if dest.Definitions == nil {
			dest.Definitions = make(spec.Definitions, len(source.Definitions))
		}
		dest.Definitions[k] = v
	}
	for k, v := range source.Parameters {
		if dest.Parameters == nil {
			dest.Parameters = make(map[string]spec.Parameter, len(source.Parameters))
		}
		dest.Parameters[k] = v
	}
	for k, v := range source.Paths.Paths {
		if dest.Paths.Paths == nil {
			dest.Paths.Paths = make(map[string]spec.PathItem, len(source.Paths.Paths))
		}
		dest.Paths.Paths[k] = v
	}
}

// MergeSpecsV3 合并 CRD 的 OpenAPI v3 spec。
// 属于 meta.v1 或 autoscaling.v1 组版本的冲突会被跳过，因为所有 CRD 都引用这些类型；
// 其他冲突则返回错误。
func MergeSpecsV3(crdSpecs ...*spec3.OpenAPI) (*spec3.OpenAPI, error) {
	crdSpec := &spec3.OpenAPI{}
	if len(crdSpecs) > 0 {
		crdSpec.Version = crdSpecs[0].Version
		crdSpec.Info = crdSpecs[0].Info
	}
	for _, s := range crdSpecs {
		err := mergeSpecV3(crdSpec, s)
		if err != nil {
			return nil, err
		}
	}
	return crdSpec, nil
}

// mergeSpecV3 把 source 的 paths 和 definitions 复制到 dest，
// 只修改 dest，不修改 source。
// 属于 meta.v1 或 autoscaling.v1 组版本的冲突会被跳过，因为所有 CRD 都引用这些类型；
// 其他冲突会返回错误。
func mergeSpecV3(dest, source *spec3.OpenAPI) error {
	if source == nil || source.Paths == nil {
		return nil
	}
	if dest.Paths == nil {
		dest.Paths = &spec3.Paths{}
	}

	for k, v := range source.Components.Schemas {
		if dest.Components == nil {
			dest.Components = &spec3.Components{}
		}
		if dest.Components.Schemas == nil {
			dest.Components.Schemas = map[string]*spec.Schema{}
		}
		if _, exists := dest.Components.Schemas[k]; exists {
			// 这些组版本被所有 CRD 共享，重复出现属于正常现象。
			if strings.HasPrefix(k, metadataGV) || strings.HasPrefix(k, autoscalingGV) {
				continue
			}
			return fmt.Errorf("OpenAPI V3 merge schema conflict on %s", k)
		}
		dest.Components.Schemas[k] = v
	}
	for k, v := range source.Paths.Paths {
		if dest.Paths.Paths == nil {
			dest.Paths.Paths = map[string]*spec3.Path{}
		}
		dest.Paths.Paths[k] = v
	}
	return nil
}
