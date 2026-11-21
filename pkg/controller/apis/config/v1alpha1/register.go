/*
Copyright 2018 The Kubernetes Authors.

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
	kubectrlmgrconfigv1alpha1 "k8s.io/kube-controller-manager/config/v1alpha1"
)

// GroupName 是本包使用的 API 组名。
const GroupName = "kubecontrollermanager.config.k8s.io"

// SchemeGroupVersion 是注册这些对象时使用的组版本。
var SchemeGroupVersion = schema.GroupVersion{Group: GroupName, Version: "v1alpha1"}

var (
	// localSchemeBuilder 在外部类型的基础上扩展 SchemeBuilder；本包还会在其中注册
	// 默认值与转换相关的 init 函数。
	localSchemeBuilder = &kubectrlmgrconfigv1alpha1.SchemeBuilder
	// AddToScheme 是将本 API 组与版本注册到 scheme 的全局函数。
	AddToScheme = localSchemeBuilder.AddToScheme
)

func init() {
	// 这里只注册手写的函数；生成函数的注册发生在生成文件中。这样拆分后，
	// 即使生成文件缺失，代码也仍然能够编译通过。
	localSchemeBuilder.Register(addDefaultingFuncs)
}
