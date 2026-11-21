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

package v1alpha1

import (
	v1alpha1 "k8s.io/api/node/v1alpha1"
	conversion "k8s.io/apimachinery/pkg/conversion"
	node "k8s.io/kubernetes/pkg/apis/node"
)

// Convert_v1alpha1_RuntimeClass_To_node_RuntimeClass 必须覆盖自动生成的转换逻辑，
// 因为在 v1alpha1 之后我们把 spec 结构体展开了（unnest）：
// v1alpha1 把字段嵌套在 spec 下，而内部类型把它们放到顶层，
// 因此需要手工把这些字段从 in.Spec 搬到 out 的顶层字段上。
func Convert_v1alpha1_RuntimeClass_To_node_RuntimeClass(in *v1alpha1.RuntimeClass, out *node.RuntimeClass, s conversion.Scope) error {
	out.ObjectMeta = in.ObjectMeta
	out.Handler = in.Spec.RuntimeHandler

	// 只有在 overhead 非空时才分配目标对象，避免把 nil 变成非 nil 的空对象；
	// 这样往返转换时字段的有无（presence）能够保持不变。
	if in.Spec.Overhead != nil {
		out.Overhead = &node.Overhead{}
		if err := Convert_v1alpha1_Overhead_To_node_Overhead(in.Spec.Overhead, out.Overhead, s); err != nil {
			return err
		}
	}
	// 与 overhead 同理：仅当源 scheduling 非空时才创建目标对象，
	// 保持 nil 语义，防止凭空生成空的调度约束。
	if in.Spec.Scheduling != nil {
		out.Scheduling = &node.Scheduling{}
		if err := Convert_v1alpha1_Scheduling_To_node_Scheduling(in.Spec.Scheduling, out.Scheduling, s); err != nil {
			return err
		}
	}
	return nil
}

// Convert_node_RuntimeClass_To_v1alpha1_RuntimeClass 必须覆盖自动生成的转换逻辑，
// 因为 v1alpha1 之后 spec 结构体被展开了：
// 内部类型的顶层字段需要写回 v1alpha1 的 in.Spec 中。
func Convert_node_RuntimeClass_To_v1alpha1_RuntimeClass(in *node.RuntimeClass, out *v1alpha1.RuntimeClass, s conversion.Scope) error {
	out.ObjectMeta = in.ObjectMeta
	out.Spec.RuntimeHandler = in.Handler

	// 仅当 overhead 非空时才分配 Spec.Overhead，
	// 否则保持缺省的 nil，确保往返转换不引入空对象。
	if in.Overhead != nil {
		out.Spec.Overhead = &v1alpha1.Overhead{}
		if err := Convert_node_Overhead_To_v1alpha1_Overhead(in.Overhead, out.Spec.Overhead, s); err != nil {
			return err
		}
	}
	// scheduling 同理：nil 与空结构在 API 上语义不同，必须原样保留。
	if in.Scheduling != nil {
		out.Spec.Scheduling = &v1alpha1.Scheduling{}
		if err := Convert_node_Scheduling_To_v1alpha1_Scheduling(in.Scheduling, out.Spec.Scheduling, s); err != nil {
			return err
		}
	}
	return nil
}
