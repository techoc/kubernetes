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

package v1

import (
	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// +genclient
// +genclient:nonNamespaced
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.14

// PriorityClass 定义从优先级类名到优先级整数值的映射，值可以是任意有效整数。
type PriorityClass struct {
	metav1.TypeMeta `json:""`
	// metadata 是标准对象元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// value 表示此优先级类的整数值。当 Pod 的 spec 中指定该类名时，
	// 这就是 Pod 实际获得的优先级。
	// +optional
	Value int32 `json:"value" protobuf:"bytes,2,opt,name=value"`

	// globalDefault 指定此 PriorityClass 是否作为未指定优先级类的 Pod 的默认优先级。
	// 只能有一个 PriorityClass 被标记为 `globalDefault`。但若存在多个
	// `globalDefault` 为 true 的 PriorityClass，则取其中值最小者作为默认优先级。
	// +optional
	GlobalDefault bool `json:"globalDefault,omitempty" protobuf:"bytes,3,opt,name=globalDefault"`

	// description 是任意字符串，通常说明何时应使用此优先级类。
	// +optional
	Description string `json:"description,omitempty" protobuf:"bytes,4,opt,name=description"`

	// preemptionPolicy 是抢占低优先级 Pod 的策略，取值 Never 或 PreemptLowerPriority。
	// 未设置时默认为 PreemptLowerPriority。
	// +optional
	PreemptionPolicy *apiv1.PreemptionPolicy `json:"preemptionPolicy,omitempty" protobuf:"bytes,5,opt,name=preemptionPolicy"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.14

// PriorityClassList 是优先级类的集合。
type PriorityClassList struct {
	metav1.TypeMeta `json:""`
	// 标准列表元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// items 是 PriorityClass 列表。
	Items []PriorityClass `json:"items" protobuf:"bytes,2,rep,name=items"`
}
