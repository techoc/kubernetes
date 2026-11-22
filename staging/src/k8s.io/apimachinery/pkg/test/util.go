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

// Package test 提供 apimachinery 测试共用的示例 Scheme 与辅助类型。
package test

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/testapigroup"
	"k8s.io/apimachinery/pkg/apis/testapigroup/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	apiserializer "k8s.io/apimachinery/pkg/runtime/serializer"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
)

// List 与 ListV1 应与 k8s.io/kubernetes/pkg/api#List 及 k8s.io/api/core/v1#List 保持同步：
// 它们是序列化测试中使用的简化列表类型。
//
// +k8s:deepcopy-gen=true
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type List struct {
	metav1.TypeMeta
	metav1.ListMeta

	Items []runtime.Object
}

// ListV1 是 List 的 v1 版本，Items 使用 RawExtension 以支持任意内嵌对象。
// +k8s:deepcopy-gen=true
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type ListV1 struct {
	metav1.TypeMeta `json:""`
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	Items []runtime.RawExtension `json:"items" protobuf:"bytes,2,rep,name=items"`
}

// TestScheme 构造包含 internal 与 v1 两组类型的示例 Scheme 及对应的 CodecFactory，
// 供序列化、转换相关测试直接复用。
func TestScheme() (*runtime.Scheme, apiserializer.CodecFactory) {
	internalGV := schema.GroupVersion{Group: "", Version: runtime.APIVersionInternal}
	externalGV := schema.GroupVersion{Group: "", Version: "v1"}
	scheme := runtime.NewScheme()

	scheme.AddKnownTypes(internalGV,
		&testapigroup.Carp{},
		&testapigroup.CarpList{},
		&List{},
	)
	scheme.AddKnownTypes(externalGV,
		&v1.Carp{},
		&v1.CarpList{},
		&List{},
	)
	utilruntime.Must(testapigroup.AddToScheme(scheme))
	utilruntime.Must(v1.AddToScheme(scheme))

	codecs := apiserializer.NewCodecFactory(scheme)
	return scheme, codecs
}
