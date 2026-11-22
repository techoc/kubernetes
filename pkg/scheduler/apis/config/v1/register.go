/*
Copyright 2022 The Kubernetes Authors.

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
	v1 "k8s.io/kube-scheduler/config/v1"
)

// GroupName 是本包使用的 API 组名。
const GroupName = v1.GroupName

// SchemeGroupVersion 是注册这些对象时使用的组版本。
var SchemeGroupVersion = v1.SchemeGroupVersion

var (
	// localSchemeBuilder 在外部类型的基础上扩展 SchemeBuilder 实例。
	// 本包还在此注册默认值与转换的初始化函数。
	localSchemeBuilder = &v1.SchemeBuilder
	// AddToScheme 是将本 API 组与版本注册到 scheme 的全局函数。
	AddToScheme = localSchemeBuilder.AddToScheme
)

func init() {
	// 此处只注册手写的函数，生成函数的注册放在生成文件里。
	// 这样即使生成文件缺失，代码依然可以编译。
	localSchemeBuilder.Register(addDefaultingFuncs)
}
