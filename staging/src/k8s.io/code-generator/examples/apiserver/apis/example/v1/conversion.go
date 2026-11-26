/*
Copyright 2025 The Kubernetes Authors.

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
	example "k8s.io/code-generator/examples/apiserver/apis/example"
)

// 对象含无法自动转换的私有字段，故需手写最终转换函数
//
// 由于 ConversionPrivate 含私有字段，生成器无法自动完成转换，
// 需手写 Convert_* 并复用生成的 autoConvert_*。
func Convert_v1_ConversionPrivate_To_example_ConversionPrivate(in *ConversionPrivate, out *example.ConversionPrivate, scope conversion.Scope) error {
	return autoConvert_v1_ConversionPrivate_To_example_ConversionPrivate(in, out, scope)
}

// 对象含无法自动转换的私有字段，故需手写最终转换函数
//
// 反向转换同理，必须手写以处理私有字段。
func Convert_example_ConversionPrivate_To_v1_ConversionPrivate(in *example.ConversionPrivate, out *ConversionPrivate, scope conversion.Scope) error {
	return autoConvert_example_ConversionPrivate_To_v1_ConversionPrivate(in, out, scope)
}

// 自定义转换函数，用于验证 slice/map/指针字段能使用自定义函数
//
// 手写 Convert_* 用于验证生成器在 slice/map/pointer 字段中
// 会优先调用这些自定义转换函数。
func Convert_v1_ConversionCustom_To_example_ConversionCustom(in *ConversionCustom, out *example.ConversionCustom, scope conversion.Scope) error {
	return autoConvert_v1_ConversionCustom_To_example_ConversionCustom(in, out, scope)
}

// 自定义转换函数，用于验证 slice/map/指针字段能使用自定义函数
//
// 反向自定义转换，保证双向路径都被覆盖。
func Convert_example_ConversionCustom_To_v1_ConversionCustom(in *example.ConversionCustom, out *ConversionCustom, scope conversion.Scope) error {
	return autoConvert_example_ConversionCustom_To_v1_ConversionCustom(in, out, scope)
}
