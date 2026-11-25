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

// Package install 安装 example2 API 组，使所有 API 编解码机制都可以选择使用它。
// install 包是 API 组的「注册入口」，被 apiserver 的 scheme 初始化代码引用。
// 它把内部版本（example2）与外部版本（example2/v1）同时加入同一个 scheme，
// 这样编解码器才能在两者之间转换；缺少任一版本都会导致 REST 存储无法序列化。
package install

import (
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apiserver/pkg/apis/example"
	"k8s.io/apiserver/pkg/apis/example2"
	example2v1 "k8s.io/apiserver/pkg/apis/example2/v1"
)

// Install 注册 API 组并把类型加入 scheme。
// example 是 example2 的「前身」组，两者共享内部类型，故一并注册；
// example2/v1 是唯一对外暴露的版本。注册顺序不影响结果，但 SetVersionPriority 决定了
// 无版本偏好时的默认序列化版本，必须指向外部版本而非内部版本。
func Install(scheme *runtime.Scheme) {
	// example 组提供内部类型，example2 的存储/转换依赖它。
	utilruntime.Must(example.AddToScheme(scheme))
	utilruntime.Must(example2.AddToScheme(scheme))
	// 外部版本必须注册，否则客户端请求的 v1 无法解码。
	utilruntime.Must(example2v1.AddToScheme(scheme))
	// 将 v1 设为默认版本，保证未显式指定版本的转换有确定结果。
	utilruntime.Must(scheme.SetVersionPriority(example2v1.SchemeGroupVersion))
}
