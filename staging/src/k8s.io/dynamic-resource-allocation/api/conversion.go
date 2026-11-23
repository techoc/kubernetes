/*
Copyright 2024 The Kubernetes Authors.

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

package api

import (
	"unique"

	conversion "k8s.io/apimachinery/pkg/conversion"
	"k8s.io/apimachinery/pkg/runtime"
)

// localSchemeBuilder 是本包的类型注册构建器，外部通过 AddToScheme 间接触发。
// AddToScheme 把本包的 API 类型注册到给定的 scheme 中，供转换与序列化使用。
var (
	localSchemeBuilder runtime.SchemeBuilder
	AddToScheme        = localSchemeBuilder.AddToScheme
)

// Convert_api_UniqueString_To_string 将内部 UniqueString 转换为普通字符串。
// 空值 NullUniqueString 转为空字符串，非空值取出其保存的字符串。
// s 为转换作用域，此处未使用。
func Convert_api_UniqueString_To_string(in *UniqueString, out *string, s conversion.Scope) error {
	// 空值映射为空字符串。
	if *in == NullUniqueString {
		*out = ""
		// 无需进一步转换，直接返回成功。
		return nil
	}
	// 非空值通过 String 方法取出底层字符串。
	*out = in.String()
	return nil
}

// Convert_string_To_api_UniqueString 将普通字符串转换为内部 UniqueString。
// 空字符串映射为 NullUniqueString，非空字符串通过 unique.Make 构造唯一字符串。
// s 为转换作用域，此处未使用。
func Convert_string_To_api_UniqueString(in *string, out *UniqueString, s conversion.Scope) error {
	// 空字符串映射为空值。
	if *in == "" {
		*out = NullUniqueString
		// 无需进一步转换，直接返回成功。
		return nil
	}
	// 非空字符串先驻留为唯一句柄，再包装成 UniqueString。
	*out = UniqueString(unique.Make(*in))
	return nil
}
