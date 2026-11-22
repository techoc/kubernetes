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

package internalversion

import (
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/conversion"
)

// Convert_v1_ListOptions_To_internalversion_ListOptions 把公开的 metav1.ListOptions 转为内部版本。
// 这里是生成代码的手写包装层：v1 版本的 ListOptions 带自定义转换（如选择器解析），
// 手写函数用来覆盖 autoConvert 的默认行为，当前直接委托给自动生成实现。
func Convert_v1_ListOptions_To_internalversion_ListOptions(in *v1.ListOptions, out *ListOptions, s conversion.Scope) error {
	return autoConvert_v1_ListOptions_To_internalversion_ListOptions(in, out, s)
}

// Convert_internalversion_ListOptions_To_v1_ListOptions 把内部 ListOptions 转回公开的 metav1 版本。
// 同样作为手写转换入口存在，当前直接委托给自动生成实现。
func Convert_internalversion_ListOptions_To_v1_ListOptions(in *ListOptions, out *v1.ListOptions, s conversion.Scope) error {
	return autoConvert_internalversion_ListOptions_To_v1_ListOptions(in, out, s)
}
