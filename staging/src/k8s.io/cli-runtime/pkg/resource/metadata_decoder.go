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

package resource

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	utiljson "k8s.io/apimachinery/pkg/util/json"
)

// 包装解码器，返回 unstructured 对象前额外校验 metadata 字段能否解码。
//
// 目的：unstructured 解码会推迟类型检查，metadata 里的字段错误可能直到
// meta.Accessor 的 Get/Set 阶段才暴露；这里提前校验，避免错误被静默吞掉。
type metadataValidatingDecoder struct {
	decoder runtime.Decoder
}

// Decode 先走底层解码，仅对 unstructured 结果追加一次 metadata-only 反序列化校验。
func (m *metadataValidatingDecoder) Decode(data []byte, defaults *schema.GroupVersionKind, into runtime.Object) (runtime.Object, *schema.GroupVersionKind, error) {
	obj, gvk, err := m.decoder.Decode(data, defaults, into)

	// 已有错误时直接返回。
	if err != nil {
		return obj, gvk, err
	}

	// 非 unstructured 对象直接返回。
	if _, isUnstructured := obj.(runtime.Unstructured); !isUnstructured {
		return obj, gvk, err
	}

	// 返回前先确认数据能解码为 ObjectMeta，
	// 避免后续通过 accessor 读写时静默丢失 metadata 中的 schema 错误。
	v := &metadataOnlyObject{}
	if typedErr := utiljson.Unmarshal(data, v); typedErr != nil {
		return obj, gvk, typedErr
	}
	return obj, gvk, err
}

// metadataOnlyObject 只声明 TypeMeta + ObjectMeta，用作 metadata 结构的强类型校验目标。
type metadataOnlyObject struct {
	metav1.TypeMeta   `json:""`
	metav1.ObjectMeta `json:"metadata,omitempty"`
}
