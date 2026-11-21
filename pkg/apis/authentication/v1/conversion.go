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
	v1 "k8s.io/api/authentication/v1"
	conversion "k8s.io/apimachinery/pkg/conversion"
	authentication "k8s.io/kubernetes/pkg/apis/authentication"
)

// Convert_v1_UserInfo_To_authentication_UserInfo 把 v1 版本的 UserInfo 转换为内部版本。
// 该函数刻意放在自动生成文件之外，供其他 API 包直接调用：
// 它被其他 API 引用，但因 build tag 的限制在代码生成时不可见，生成器不会产出可复用的导出版本。
func Convert_v1_UserInfo_To_authentication_UserInfo(in *v1.UserInfo, out *authentication.UserInfo, s conversion.Scope) error {
	return autoConvert_v1_UserInfo_To_authentication_UserInfo(in, out, s)
}

// Convert_authentication_UserInfo_To_v1_UserInfo 把内部版本的 UserInfo 转换为 v1 版本。
// 同样放在自动生成文件之外，原因同上：其他 API 需要引用它，
// 而生成时受 build tag 限制不可见，因此必须手写一个导出包装。
func Convert_authentication_UserInfo_To_v1_UserInfo(in *authentication.UserInfo, out *v1.UserInfo, s conversion.Scope) error {
	return autoConvert_authentication_UserInfo_To_v1_UserInfo(in, out, s)
}
