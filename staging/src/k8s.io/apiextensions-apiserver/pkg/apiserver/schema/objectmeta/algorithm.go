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

package objectmeta

import (
	"sort"

	structuralschema "k8s.io/apiextensions-apiserver/pkg/apiserver/schema"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

// CoerceOptions 提供两种能力：为 schema 不认识的字段返回其路径
// （ReturnUnknownFieldPaths），或丢弃属于 schema 但格式错误的字段（DropInvalidFields）。
type CoerceOptions struct {
	// DropInvalidFields 会丢弃无法成功解码到对应 ObjectMeta 字段的
	// 畸形序列化元数据。它只适用于 schema 认可、但类型非法的字段
	// （即反序列化时报错，而不是被丢弃或触发 strictErr 的情况）。
	DropInvalidFields bool
	// ReturnUnknownFieldPaths 会返回 schema 不认识的字段路径。
	ReturnUnknownFieldPaths bool
}

// Coerce 检查内嵌 ObjectMeta 和 TypeMeta 的类型，并裁剪前者内部的未知字段。
// 当 isResourceRoot 为 true 时，会强制转换根部的 ObjectMeta 和 TypeMeta。
// 若 opts.ReturnUnknownFieldPaths 为 true，会返回反序列化时被丢弃、
// 且不属于 schema 的字段路径。
// 若 opts.DropInvalidFields 为 true，类型错误的字段会被丢弃。
func CoerceWithOptions(pth *field.Path, obj interface{}, s *structuralschema.Structural, isResourceRoot bool, opts CoerceOptions) (*field.Error, []string) {
	if isResourceRoot {
		if s == nil {
			s = &structuralschema.Structural{}
		}
		if !s.XEmbeddedResource {
			// 根资源隐含内嵌了 ObjectMeta/TypeMeta；复制一份，避免修改共享 schema。
			clone := *s
			clone.XEmbeddedResource = true
			s = &clone
		}
	}
	c := coercer{DropInvalidFields: opts.DropInvalidFields, ReturnUnknownFieldPaths: opts.ReturnUnknownFieldPaths}
	schemaOpts := &structuralschema.UnknownFieldPathOptions{
		TrackUnknownFieldPaths: opts.ReturnUnknownFieldPaths,
	}
	fieldErr := c.coerce(pth, obj, s, schemaOpts)
	sort.Strings(schemaOpts.UnknownFieldPaths)
	return fieldErr, schemaOpts.UnknownFieldPaths
}

// Coerce 调用 CoerceWithOptions，但不返回未知字段路径。
func Coerce(pth *field.Path, obj interface{}, s *structuralschema.Structural, isResourceRoot, dropInvalidFields bool) *field.Error {
	fieldErr, _ := CoerceWithOptions(pth, obj, s, isResourceRoot, CoerceOptions{DropInvalidFields: dropInvalidFields})
	return fieldErr
}

type coercer struct {
	DropInvalidFields       bool
	ReturnUnknownFieldPaths bool
}

// coerce 递归遍历对象，按 schema 校正内嵌资源元数据并记录未知字段路径。
func (c *coercer) coerce(pth *field.Path, x interface{}, s *structuralschema.Structural, opts *structuralschema.UnknownFieldPathOptions) *field.Error {
	if s == nil {
		return nil
	}
	origPathLen := len(opts.ParentPath)
	// 递归返回时把路径栈恢复到进入时的长度，避免兄弟字段相互污染。
	defer func() {
		opts.ParentPath = opts.ParentPath[:origPathLen]
	}()
	switch x := x.(type) {
	case map[string]interface{}:
		for k, v := range x {
			if s.XEmbeddedResource {
				switch k {
				case "apiVersion", "kind":
					if _, ok := v.(string); !ok && c.DropInvalidFields {
						delete(x, k)
					} else if !ok {
						return field.Invalid(pth.Child(k), v, "must be a string")
					}
				case "metadata":
					meta, found, unknownFields, err := GetObjectMetaWithOptions(x, ObjectMetaOptions{
						DropMalformedFields:     c.DropInvalidFields,
						ReturnUnknownFieldPaths: c.ReturnUnknownFieldPaths,
						ParentPath:              pth,
					})
					opts.UnknownFieldPaths = append(opts.UnknownFieldPaths, unknownFields...)
					if err != nil {
						if !c.DropInvalidFields {
							return field.Invalid(pth.Child("metadata"), v, err.Error())
						}
						// DropInvalidFields 为 true 时忽略错误，继续后续处理。
					} else if found {
						if err := SetObjectMeta(x, meta); err != nil {
							return field.Invalid(pth.Child("metadata"), v, err.Error())
						}
						// 解码/重建后的 ObjectMeta 会带上零值 creationTimestamp；
						// 零值不应出现在序列化结果中，需显式删除。
						if meta.CreationTimestamp.IsZero() {
							unstructured.RemoveNestedField(x, "metadata", "creationTimestamp")
						}
					}
				}
			}
			prop, ok := s.Properties[k]
			if ok {
				opts.AppendKey(k)
				if err := c.coerce(pth.Child(k), v, &prop, opts); err != nil {
					return err
				}
				opts.ParentPath = opts.ParentPath[:origPathLen]
			} else if s.AdditionalProperties != nil {
				opts.AppendKey(k)
				if err := c.coerce(pth.Key(k), v, s.AdditionalProperties.Structural, opts); err != nil {
					return err
				}
				opts.ParentPath = opts.ParentPath[:origPathLen]
			}
		}
	case []interface{}:
		for i, v := range x {
			opts.AppendIndex(i)
			if err := c.coerce(pth.Index(i), v, s.Items, opts); err != nil {
				return err
			}
			opts.ParentPath = opts.ParentPath[:origPathLen]
		}
	default:
		// 标量无需处理。
	}

	return nil
}
