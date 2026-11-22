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
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	utiljson "k8s.io/apimachinery/pkg/util/json"
	"k8s.io/apimachinery/pkg/util/validation/field"
	kjson "sigs.k8s.io/json"
)

// GetObjectMeta 调用 GetObjectMetaWithOptions，但不返回未知字段路径。
func GetObjectMeta(obj map[string]interface{}, dropMalformedFields bool) (*metav1.ObjectMeta, bool, error) {
	meta, found, _, err := GetObjectMetaWithOptions(obj, ObjectMetaOptions{
		DropMalformedFields: dropMalformedFields,
	})
	return meta, found, err
}

// ObjectMetaOptions 控制 GetObjectMeta 如何提取对象元数据。
type ObjectMetaOptions struct {
	// DropMalformedFields 丢弃无法成功解码到对应 ObjectMeta 字段的畸形元数据。
	// 仅适用于 schema 中已识别、但类型非法的字段（即反序列化会报错，
	// 而不是被丢弃或触发 strictErr 的字段）。
	DropMalformedFields bool
	// ReturnUnknownFieldPaths 返回 schema 未识别字段的路径。
	ReturnUnknownFieldPaths bool
	// ParentPath 提供到当前 ObjectMeta 为止的路径。
	// 为 nil 时假定元数据位于对象根节点。
	ParentPath *field.Path
}

// GetObjectMetaWithOptions 把 JSON 转成 ObjectMeta。
// 先尝试用 json.Unmarshal 解码为 metav1.ObjectMeta 类型；
// 失败且 opts.DropMalformedFields 为 true 时，退化为逐字段尽力转换，
// 丢弃导致错误的字段。
// 若 opts.ReturnedUnknownFields 为 true，则改用 UnmarshalStrict，
// 并返回遇到的未知字段路径（即 UnmarshalStrict 返回的 strict err 路径）。
func GetObjectMetaWithOptions(obj map[string]interface{}, opts ObjectMetaOptions) (*metav1.ObjectMeta, bool, []string, error) {
	metadata, found := obj["metadata"]
	if !found {
		return nil, false, nil, nil
	}

	// 先走一遍 JSON 往返，期待反序列化直接成功。
	objectMeta := &metav1.ObjectMeta{}
	metadataBytes, err := utiljson.Marshal(metadata)
	if err != nil {
		return nil, false, nil, err
	}
	var unmarshalErr error
	if opts.ReturnUnknownFieldPaths {
		var strictErrs []error
		strictErrs, unmarshalErr = kjson.UnmarshalStrict(metadataBytes, objectMeta)
		if unmarshalErr == nil {
			if len(strictErrs) > 0 {
				unknownPaths := []string{}
				prefix := opts.ParentPath.Child("metadata").String()
				for _, err := range strictErrs {
					if fieldPathErr, ok := err.(kjson.FieldError); ok {
						unknownPaths = append(unknownPaths, prefix+"."+fieldPathErr.FieldPath())
					}
				}
				return objectMeta, true, unknownPaths, nil
			}
			return objectMeta, true, nil, nil
		}
	} else {
		if unmarshalErr = utiljson.Unmarshal(metadataBytes, objectMeta); unmarshalErr == nil {
			// 成功则直接返回
			return objectMeta, true, nil, nil
		}
	}
	if !opts.DropMalformedFields {
		// 不丢弃畸形字段时，直接把错误抛出去
		return nil, true, nil, unmarshalErr
	}

	metadataMap, ok := metadata.(map[string]interface{})
	if !ok {
		return nil, false, nil, fmt.Errorf("invalid metadata: expected object, got %T", metadata)
	}

	// 逐字段累积到 metadata 对象中。
	// 这利用了这样一个事实：可以把单个字段反复反序列化进同一个结构体，
	// 每次迭代都会保留已有的键值。
	accumulatedObjectMeta := &metav1.ObjectMeta{}
	testObjectMeta := &metav1.ObjectMeta{}
	var unknownFields []string
	for k, v := range metadataMap {
		// 序列化单个字段
		if singleFieldBytes, err := utiljson.Marshal(map[string]interface{}{k: v}); err == nil {
			// 先试一次反序列化
			if utiljson.Unmarshal(singleFieldBytes, testObjectMeta) == nil {
				// 成功才真正写入
				if opts.ReturnUnknownFieldPaths {
					strictErrs, _ := kjson.UnmarshalStrict(singleFieldBytes, accumulatedObjectMeta)
					if len(strictErrs) > 0 {
						prefix := opts.ParentPath.Child("metadata").String()
						for _, err := range strictErrs {
							if fieldPathErr, ok := err.(kjson.FieldError); ok {
								unknownFields = append(unknownFields, prefix+"."+fieldPathErr.FieldPath())
							}
						}
					}
				} else {
					utiljson.Unmarshal(singleFieldBytes, accumulatedObjectMeta)
				}
			}
		}
	}

	return accumulatedObjectMeta, true, unknownFields, nil
}

// SetObjectMeta 把 ObjectMeta 写回 JSON 数据结构。
func SetObjectMeta(obj map[string]interface{}, objectMeta *metav1.ObjectMeta) error {
	if objectMeta == nil {
		// nil 元数据等价于删除 metadata 字段。
		unstructured.RemoveNestedField(obj, "metadata")
		return nil
	}

	metadata, err := runtime.DefaultUnstructuredConverter.ToUnstructured(objectMeta)
	if err != nil {
		return err
	}

	obj["metadata"] = metadata
	return nil
}
