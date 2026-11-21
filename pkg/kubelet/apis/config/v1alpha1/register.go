/*
Copyright 2020 The Kubernetes Authors.

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
	kubeletconfigv1alpha1 "k8s.io/kubelet/config/v1alpha1"
)

// GroupName 是本包使用的 API 组名。
const GroupName = "kubelet.config.k8s.io"

// SchemeGroupVersion 是注册这些对象所用的组版本。
var SchemeGroupVersion = schema.GroupVersion{Group: GroupName, Version: "v1alpha1"}

var (
	// localSchemeBuilder 在 SchemeBuilder 中扩展了外部类型；本包还会注册
	// 默认值与转换的初始化函数。
	localSchemeBuilder = &kubeletconfigv1alpha1.SchemeBuilder
	// AddToScheme 是把该 API 组版本注册到 scheme 的全局函数。
	AddToScheme = localSchemeBuilder.AddToScheme
)
