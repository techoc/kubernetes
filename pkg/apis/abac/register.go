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

package abac

import (
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
)

// GroupName 是 abac 的 API 组名。
const GroupName = "abac.authorization.kubernetes.io"

// SchemeGroupVersion 是注册 abac 内部类型所用的 API 组版本。
var SchemeGroupVersion = schema.GroupVersion{Group: GroupName, Version: runtime.APIVersionInternal}

// Scheme 是 abac API 组类型注册到的默认 runtime.Scheme 实例。
// TODO: 移除它，abac 不应拥有独立的 scheme。
var Scheme = runtime.NewScheme()

// Codecs 提供基于该 scheme 的编码与解码能力。
var Codecs = serializer.NewCodecFactory(Scheme)

func init() {
	// TODO: delete this, abac should not have its own scheme.
	addKnownTypes(Scheme)
}

var (
	// SchemeBuilder 是本包的 scheme 构造器，保存了需要执行的初始化函数。
	SchemeBuilder = runtime.NewSchemeBuilder(addKnownTypes)
	// AddToScheme 是通用的注册函数，把本包的 group/version 与类型映射到 scheme。
	AddToScheme = SchemeBuilder.AddToScheme
)

func addKnownTypes(scheme *runtime.Scheme) error {
	scheme.AddKnownTypes(SchemeGroupVersion,
		&Policy{},
	)
	return nil
}
