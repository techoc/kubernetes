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

package v1alpha1

import (
	"k8s.io/apimachinery/pkg/runtime/schema"
	kubeproxyconfigv1alpha1 "k8s.io/kube-proxy/config/v1alpha1"
)

// GroupName 是本包使用的 API 组名。
const GroupName = "kubeproxy.config.k8s.io"

// SchemeGroupVersion 是注册这些对象时使用的 group version。
var SchemeGroupVersion = schema.GroupVersion{Group: GroupName, Version: "v1alpha1"}

var (
	// localSchemeBuilder 在 SchemeBuilder 实例上扩展外部类型；
	// 本包还会在此注册默认值设置和转换相关的 init 函数。
	localSchemeBuilder = &kubeproxyconfigv1alpha1.SchemeBuilder
	// AddToScheme 是全局函数，用于将该 API group 和 version 注册到 scheme。
	AddToScheme = localSchemeBuilder.AddToScheme
)

// init 将手动编写的默认值设置函数注册到 scheme builder。
func init() {
	// 此处只注册手写的函数；自动生成的函数由生成的文件负责注册。
	// 这样即使生成的文件缺失，包也仍然可以编译。
	localSchemeBuilder.Register(addDefaultingFuncs)
}
