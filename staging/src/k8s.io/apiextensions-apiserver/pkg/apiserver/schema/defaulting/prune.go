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

package defaulting

import (
	"fmt"
	"reflect"

	structuralschema "k8s.io/apiextensions-apiserver/pkg/apiserver/schema"
	structuralobjectmeta "k8s.io/apiextensions-apiserver/pkg/apiserver/schema/objectmeta"
	"k8s.io/apiextensions-apiserver/pkg/apiserver/schema/pruning"
	"k8s.io/apimachinery/pkg/runtime"
)

// PruneDefaults 按 schema 以及当前服务器 ObjectMeta 的定义裁剪默认值。
// 它会原地修改传入的 schema。
func PruneDefaults(s *structuralschema.Structural) error {
	p := pruner{s}
	_, err := p.pruneDefaults(s, NewRootObjectFunc())
	return err
}

// pruner 持有根 schema，供 ObjectMeta 强制转换时查找类型信息。
type pruner struct {
	rootSchema *structuralschema.Structural
}

// pruneDefaults 递归遍历 schema 并裁剪每个默认值；
// changed 表示是否真的改动了对象，调用方据此决定是否需要写回。
func (p *pruner) pruneDefaults(s *structuralschema.Structural, f SurroundingObjectFunc) (changed bool, err error) {
	if s == nil {
		return false, nil
	}

	if s.Default.Object != nil {
		// 先深拷贝，裁剪后再比对，用来判断 schema 是否真的发生变化。
		orig := runtime.DeepCopyJSONValue(s.Default.Object)

		// 默认值本身可能只是叶节点，先还原其所在的外围对象，
		// 这样 ObjectMeta 强制转换才能看到完整的上下文。
		obj, acc, err := f(s.Default.Object)
		if err != nil {
			return false, fmt.Errorf("failed to prune default value: %v", err)
		}
		if err := structuralobjectmeta.Coerce(nil, obj, p.rootSchema, true, true); err != nil {
			return false, fmt.Errorf("failed to prune default value: %v", err)
		}
		pruning.Prune(obj, p.rootSchema, true)
		// acc 是构造函数的逆操作，从裁剪后的外围对象取回叶节点。
		s.Default.Object, _, err = acc(obj)
		if err != nil {
			return false, fmt.Errorf("failed to prune default value: %v", err)
		}

		changed = changed || !reflect.DeepEqual(orig, s.Default.Object)
	}

	if s.AdditionalProperties != nil && s.AdditionalProperties.Structural != nil {
		c, err := p.pruneDefaults(s.AdditionalProperties.Structural, f.Child("*"))
		if err != nil {
			return false, err
		}
		changed = changed || c
	}
	if s.Items != nil {
		c, err := p.pruneDefaults(s.Items, f.Index())
		if err != nil {
			return false, err
		}
		changed = changed || c
	}
	for k, subSchema := range s.Properties {
		c, err := p.pruneDefaults(&subSchema, f.Child(k))
		if err != nil {
			return false, err
		}
		if c {
			s.Properties[k] = subSchema
			changed = true
		}
	}

	return changed, nil
}
