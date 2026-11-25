/*
Copyright The Kubernetes Authors.

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

package sharding

import (
	"fmt"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
)

// ResolveFieldValue 根据给定的字段路径，从 runtime.Object 中提取元数据字段值。
//
// 字段路径采用 CEL 风格的以对象为根的语法（"object.metadata.<field>"），
// 这与 fieldSelector 格式（"metadata.<field>"）不同。"object." 前缀
// 将路径锚定在被过滤的资源上。
//
// 支持的字段路径：
//   - "object.metadata.uid"
//   - "object.metadata.namespace"
func ResolveFieldValue(obj runtime.Object, fieldPath string) (string, error) {
	// 所有支持的路径都在 metadata 下，因此预先一次性获取 accessor。
	accessor, err := meta.Accessor(obj)
	if err != nil {
		return "", fmt.Errorf("failed to access object metadata: %w", err)
	}

	// 只接受取值稳定且可用于分片的字段；未知路径会报错，
	// 以免拼写错误悄悄导致分到错误的分区。
	switch fieldPath {
	case "object.metadata.uid":
		return string(accessor.GetUID()), nil
	case "object.metadata.namespace":
		return accessor.GetNamespace(), nil
	default:
		return "", fmt.Errorf("unsupported field path: %q", fieldPath)
	}
}
