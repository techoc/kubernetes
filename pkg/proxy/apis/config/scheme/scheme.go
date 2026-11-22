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

// Package scheme 提供 kube-proxy 配置 API 的 Scheme 与 Codecs，
// 集中完成内部版本与外部版本的注册，供序列化、默认值和校验流程共用。
package scheme

import (
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	kubeproxyconfig "k8s.io/kubernetes/pkg/proxy/apis/config"
	kubeproxyconfigv1alpha1 "k8s.io/kubernetes/pkg/proxy/apis/config/v1alpha1"
)

var (
	// Scheme 定义了 API 对象的序列化与反序列化方法。
	Scheme = runtime.NewScheme()
	// Codecs 提供了获取特定版本和内容类型的 codec 与 serializer 的方法。
	// 启用严格模式可避免解码时静默丢弃未知字段。
	Codecs = serializer.NewCodecFactory(Scheme, serializer.EnableStrict)
)

// init 在包初始化时注册全部类型，确保直接使用 Scheme/Codecs 的调用方无需显式初始化。
func init() {
	// 包初始化时即注册全部类型，确保直接使用 Scheme/Codecs 的调用方无需显式初始化。
	AddToScheme(Scheme)
}

// AddToScheme 将该 API 组的类型注册到给定的 scheme 中。
// 同时注册外部版本（v1alpha1）和内部版本，以支持两者之间的转换、默认值与序列化；
// utilruntime.Must 保证注册失败时立即 panic，避免出现“看似正常但类型缺失”的 scheme。
func AddToScheme(scheme *runtime.Scheme) {
	utilruntime.Must(kubeproxyconfigv1alpha1.AddToScheme(scheme))
	utilruntime.Must(kubeproxyconfig.AddToScheme(scheme))
}
