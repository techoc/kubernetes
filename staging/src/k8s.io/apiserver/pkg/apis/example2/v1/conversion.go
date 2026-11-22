/*
Copyright 2017 The Kubernetes Authors.

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
	conversion "k8s.io/apimachinery/pkg/conversion"
	example "k8s.io/apiserver/pkg/apis/example"
)

// Convert_example_ReplicaSetSpec_To_v1_ReplicaSetSpec 把手写的转换函数
// 用于 ReplicaSetSpec：内部版本 Replicas 是 int32，外部版本是 *int32。
// 生成器无法自动完成这种“值 -> 指针”的语义转换，所以显式分配。
func Convert_example_ReplicaSetSpec_To_v1_ReplicaSetSpec(in *example.ReplicaSetSpec, out *ReplicaSetSpec, s conversion.Scope) error {
	out.Replicas = new(int32)
	*out.Replicas = int32(in.Replicas)
	return nil
}

// Convert_v1_ReplicaSetSpec_To_example_ReplicaSetSpec 是反向转换。
// 外部版本的 nil 表示“未设置”，这里保持内部版本的零值（0），
// 由默认值填充逻辑决定最终取值。
func Convert_v1_ReplicaSetSpec_To_example_ReplicaSetSpec(in *ReplicaSetSpec, out *example.ReplicaSetSpec, s conversion.Scope) error {
	if in.Replicas != nil {
		out.Replicas = *in.Replicas
	}
	return nil
}
