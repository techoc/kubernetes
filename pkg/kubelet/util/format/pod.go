/*
Copyright 2015 The Kubernetes Authors.

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

package format

import (
	"fmt"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
)

// Pod 以一致的、便于阅读的格式返回 Pod 字符串表示，其中包含 Pod UID。
// pod 为 nil 时返回 "<nil>"。
func Pod(pod *v1.Pod) string {
	if pod == nil {
		return "<nil>"
	}
	return PodDesc(pod.Name, pod.Namespace, pod.UID)
}

// PodDesc 以一致的、便于阅读的格式返回 Pod 字符串表示，其中包含 Pod UID。
func PodDesc(podName, podNamespace string, podUID types.UID) string {
	// 使用下划线作分隔符：Pod 名（DNS 子域格式）不允许下划线，而容器名格式允许，
	// 因此可以保证结果无歧义。
	return fmt.Sprintf("%s_%s(%s)", podName, podNamespace, podUID)
}
