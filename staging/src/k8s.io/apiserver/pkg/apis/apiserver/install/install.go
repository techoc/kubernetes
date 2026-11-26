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

// Package install 把 apiserver 配置的各版本类型注册到 runtime.Scheme。
package install

import (
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apiserver/pkg/apis/apiserver"
	v1 "k8s.io/apiserver/pkg/apis/apiserver/v1"
	"k8s.io/apiserver/pkg/apis/apiserver/v1alpha1"
	"k8s.io/apiserver/pkg/apis/apiserver/v1beta1"
)

// Install 注册 API 组，并把类型加入 scheme。
//
// 注册顺序与版本优先级决定了解码未指定 apiVersion 的对象时的默认版本，
// 以及编码时的首选版本。internal 版本必须先注册，之后各外部版本才能通过
// 转换器与它互转。
func Install(scheme *runtime.Scheme) {
	utilruntime.Must(apiserver.AddToScheme(scheme))

	// v1alpha 位于以 k8s.io 为后缀的 API 组。
	utilruntime.Must(v1alpha1.AddToScheme(scheme))
	utilruntime.Must(scheme.SetVersionPriority(v1alpha1.SchemeGroupVersion))

	// v1alpha 位于以 k8s.io 为后缀的 API 组。
	utilruntime.Must(v1beta1.AddToScheme(scheme))
	utilruntime.Must(scheme.SetVersionPriority(v1beta1.SchemeGroupVersion))

	// v1 位于以 config.k8s.io 为后缀的 API 组。
	utilruntime.Must(v1.AddToScheme(scheme))
	utilruntime.Must(scheme.SetVersionPriority(v1.SchemeGroupVersion))
}
