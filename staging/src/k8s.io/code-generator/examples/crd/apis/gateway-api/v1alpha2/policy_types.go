/*
Copyright 2021 The Kubernetes Authors.

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

package v1alpha2

import (
	v1 "k8s.io/code-generator/examples/crd/apis/gateway-api/v1"
)

// 从 Gateway API 拷来，作为 #131533 的最小复现
//
// 该文件从 Gateway API 复制而来，是 #131533 的最小复现用例。
// 手写 DeepCopyInto 用于覆盖“内嵌跨包结构体需要逐层深拷贝”的生成路径。

// PolicyAncestorStatus 描述某个策略祖先的状态。
type PolicyAncestorStatus struct {
	AncestorRef v1.ParentReference `json:"ancestorRef"`
}

// PolicyStatus 汇总策略所有祖先的状态。
type PolicyStatus struct {
	Ancestors []PolicyAncestorStatus `json:"ancestors"`
}

// DeepCopyInto 手写实现，必须深入复制跨包的 AncestorRef，
// 不能仅靠 *out = *in 的结构赋值。
func (in *PolicyAncestorStatus) DeepCopyInto(out *PolicyAncestorStatus) {
	*out = *in
	in.AncestorRef.DeepCopyInto(&out.AncestorRef)
}

// DeepCopyInto 手写实现，为切片分配新底层数组并逐元素深拷贝。
func (in *PolicyStatus) DeepCopyInto(out *PolicyStatus) {
	*out = *in
	if in.Ancestors != nil {
		in, out := &in.Ancestors, &out.Ancestors
		*out = make([]PolicyAncestorStatus, len(*in))
		for i := range *in {
			(*in)[i].DeepCopyInto(&(*out)[i])
		}
	}
}
