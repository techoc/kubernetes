/*
Copyright 2016 The Kubernetes Authors.

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

package testing

import (
	"fmt"

	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
)

// BuildContainerName creates a unique container name string.
//
// BuildContainerName 根据容器元数据构造一个唯一的容器名称字符串。
// 名称格式为：<沙箱ID>_<容器名>_<第几次尝试>。
// 把 sandboxID 拼进去是为了避免不同 Pod 沙箱中同名容器之间发生冲突。
func BuildContainerName(metadata *runtimeapi.ContainerMetadata, sandboxID string) string {
	// include the sandbox ID to make the container ID unique.
	// 拼入沙箱 ID，保证即使容器名和 attempt 相同，不同沙箱下的容器名仍然唯一。
	return fmt.Sprintf("%s_%s_%d", sandboxID, metadata.Name, metadata.Attempt)
}

// BuildSandboxName creates a unique sandbox name string.
//
// BuildSandboxName 根据 Pod 沙箱元数据构造一个唯一的沙箱名称字符串。
// 名称格式为：<沙箱名>_<命名空间>_<UID>_<第几次尝试>。
// 其中 Name 和 Namespace 用于可读性，Uid 保证跨命名空间唯一，Attempt 区分重建次数。
func BuildSandboxName(metadata *runtimeapi.PodSandboxMetadata) string {
	return fmt.Sprintf("%s_%s_%s_%d", metadata.Name, metadata.Namespace, metadata.Uid, metadata.Attempt)
}

// filterInLabels 判断 labels 是否完全匹配 filter 中指定的所有标签。
// 这是一个"包含式"匹配：filter 中的每一个键值对都必须在 labels 中存在且值相等，
// 而 labels 中额外的标签不影响结果。
//
// 匹配流程：
//  1. 遍历 filter 的每个键值对；
//  2. 若 labels 中不存在该键，返回 false；
//  3. 若存在但值不相等，返回 false；
//  4. 所有键值对都匹配通过后返回 true。
//
// 注意：filter 为空时任意 labels 都匹配（返回 true）。
func filterInLabels(filter, labels map[string]string) bool {
	for k, v := range filter {
		// 检查 filter 中的键是否存在于 labels。
		if value, ok := labels[k]; ok {
			// 键存在但值不等，说明不满足过滤条件。
			if value != v {
				return false
			}
		} else {
			// 键不存在，同样不满足过滤条件。
			return false
		}
	}

	return true
}
