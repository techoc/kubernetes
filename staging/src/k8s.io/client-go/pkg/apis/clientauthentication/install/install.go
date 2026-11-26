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

// Package install 安装实验性 API 组，使其可作为已注册 API 使用。
// 作为所有 API 编解码机制的选项。
//
// Package install 把 clientauthentication API 组注册进给定的 scheme，
// 包含内部版本以及 v1、v1beta1 两个外部版本，供 exec 插件认证凭据的
// 序列化与转换使用。
package install

import (
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/pkg/apis/clientauthentication"
	"k8s.io/client-go/pkg/apis/clientauthentication/v1"
	"k8s.io/client-go/pkg/apis/clientauthentication/v1beta1"
)

// Install 注册 API 组并把类型加入 scheme。
//
// 同时注册内部与外部版本是 conversion 的前提：编码走外部版本，
// 内部一致的类型集合则让 scheme 能完成版本间互转。
func Install(scheme *runtime.Scheme) {
	utilruntime.Must(clientauthentication.AddToScheme(scheme))
	utilruntime.Must(v1.AddToScheme(scheme))
	utilruntime.Must(v1beta1.AddToScheme(scheme))
}
