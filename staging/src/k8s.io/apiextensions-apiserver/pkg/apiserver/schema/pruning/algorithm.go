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

package pruning

import (
	"sort"

	structuralschema "k8s.io/apiextensions-apiserver/pkg/apiserver/schema"
)

// PruneWithOptions 删除 obj 中未在 s 里声明的对象字段。若 XEmbeddedResource 为 true，
// 或是 isResourceRoot=true 的根节点，则跳过 TypeMeta 和 ObjectMeta 字段，
// 即不裁剪未知的 metadata 字段。
// 当 opts.TrackUnknownFieldPaths 为 true 时，返回被裁剪字段的路径集合。
func PruneWithOptions(obj interface{}, s *structuralschema.Structural, isResourceRoot bool, opts structuralschema.UnknownFieldPathOptions) []string {
	if isResourceRoot {
		if s == nil {
			s = &structuralschema.Structural{}
		}
		if !s.XEmbeddedResource {
			clone := *s
			clone.XEmbeddedResource = true
			s = &clone
		}
	}
	prune(obj, s, &opts)
	sort.Strings(opts.UnknownFieldPaths)
	return opts.UnknownFieldPaths
}

// Prune 等价于
// PruneWithOptions(obj, s, isResourceRoot, structuralschema.UnknownFieldPathOptions{})
func Prune(obj interface{}, s *structuralschema.Structural, isResourceRoot bool) {
	PruneWithOptions(obj, s, isResourceRoot, structuralschema.UnknownFieldPathOptions{})
}

// metaFields 是内嵌资源必须保留的 TypeMeta/ObjectMeta 字段。
var metaFields = map[string]bool{
	"apiVersion": true,
	"kind":       true,
	"metadata":   true,
}

// prune 递归裁剪 x，并在进入子节点时维护 ParentPath 以便记录未知字段路径。
func prune(x interface{}, s *structuralschema.Structural, opts *structuralschema.UnknownFieldPathOptions) {
	if s != nil && s.XPreserveUnknownFields {
		skipPrune(x, s, opts)
		return
	}

	origPathLen := len(opts.ParentPath)
	defer func() {
		// 递归返回时恢复路径长度，兄弟字段才不会继承错误的前缀。
		opts.ParentPath = opts.ParentPath[:origPathLen]
	}()
	switch x := x.(type) {
	case map[string]interface{}:
		if s == nil {
			for k := range x {
				opts.RecordUnknownField(k)
				delete(x, k)
			}
			return
		}
		for k, v := range x {
			if s.XEmbeddedResource && metaFields[k] {
				continue
			}
			prop, ok := s.Properties[k]
			if ok {
				opts.AppendKey(k)
				prune(v, &prop, opts)
				opts.ParentPath = opts.ParentPath[:origPathLen]
			} else if s.AdditionalProperties != nil {
				opts.AppendKey(k)
				prune(v, s.AdditionalProperties.Structural, opts)
				opts.ParentPath = opts.ParentPath[:origPathLen]
			} else {
				// 根路径下的 metadata 由 API 服务器统一处理，不视为未知字段。
				if !metaFields[k] || len(opts.ParentPath) > 0 {
					opts.RecordUnknownField(k)
				}
				delete(x, k)
			}
		}
	case []interface{}:
		if s == nil {
			for i, v := range x {
				opts.AppendIndex(i)
				prune(v, nil, opts)
				opts.ParentPath = opts.ParentPath[:origPathLen]
			}
			return
		}
		for i, v := range x {
			opts.AppendIndex(i)
			prune(v, s.Items, opts)
			opts.ParentPath = opts.ParentPath[:origPathLen]
		}
	default:
		// 标量无需处理
	}
}

// skipPrune 在允许保留未知字段时递归下探：只对 schema 中已知的属性继续裁剪，
// 未知字段原样保留。
func skipPrune(x interface{}, s *structuralschema.Structural, opts *structuralschema.UnknownFieldPathOptions) {
	if s == nil {
		return
	}
	origPathLen := len(opts.ParentPath)
	defer func() {
		// 递归返回时恢复路径长度，兄弟字段才不会继承错误的前缀。
		opts.ParentPath = opts.ParentPath[:origPathLen]
	}()

	switch x := x.(type) {
	case map[string]interface{}:
		for k, v := range x {
			if s.XEmbeddedResource && metaFields[k] {
				continue
			}
			if prop, ok := s.Properties[k]; ok {
				opts.AppendKey(k)
				prune(v, &prop, opts)
				opts.ParentPath = opts.ParentPath[:origPathLen]
			} else if s.AdditionalProperties != nil {
				opts.AppendKey(k)
				prune(v, s.AdditionalProperties.Structural, opts)
				opts.ParentPath = opts.ParentPath[:origPathLen]
			}
		}
	case []interface{}:
		for i, v := range x {
			opts.AppendIndex(i)
			skipPrune(v, s.Items, opts)
			opts.ParentPath = opts.ParentPath[:origPathLen]
		}
	default:
		// 标量无需处理
	}
}
