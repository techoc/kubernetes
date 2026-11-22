/*
Copyright 2014 The Kubernetes Authors.

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

package generic

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
)

// ObjectMetaFieldsSet returns a fields that represent the ObjectMeta.
// 字段集合用于字段选择器（fieldSelector）匹配；集群级资源没有命名空间，
// 因此 hasNamespaceField 决定是否暴露 metadata.namespace。
func ObjectMetaFieldsSet(objectMeta *metav1.ObjectMeta, hasNamespaceField bool) fields.Set {
	if !hasNamespaceField {
		return fields.Set{
			"metadata.name": objectMeta.Name,
		}
	}
	return fields.Set{
		"metadata.name":      objectMeta.Name,
		"metadata.namespace": objectMeta.Namespace,
	}
}

// AdObjectMetaField add fields that represent the ObjectMeta to source.
// 与 ObjectMetaFieldsSet 不同，这里原地修改并返回同一个 map，
// 调用方需明确其副作用（通常是在已有字段集合上追加）。
func AddObjectMetaFieldsSet(source fields.Set, objectMeta *metav1.ObjectMeta, hasNamespaceField bool) fields.Set {
	source["metadata.name"] = objectMeta.Name
	if hasNamespaceField {
		source["metadata.namespace"] = objectMeta.Namespace
	}
	return source
}

// MergeFieldsSets merges a fields'set from fragment into the source.
// fragment 中的键覆盖 source 中的同名键，调用方据此实现“特化字段优先”的语义。
func MergeFieldsSets(source fields.Set, fragment fields.Set) fields.Set {
	for k, value := range fragment {
		source[k] = value
	}
	return source
}
