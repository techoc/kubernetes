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

package scheme

import (
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/kubernetes/pkg/controller/apis/config"
	"k8s.io/kubernetes/pkg/controller/apis/config/v1alpha1"
)

var (
	// Scheme 定义了 API 对象序列化与反序列化所需的方法。
	Scheme = runtime.NewScheme()
	// Codecs 提供按指定版本和内容类型获取 codec 与 serializer 的方法。
	Codecs = serializer.NewCodecFactory(Scheme)
)

// init 在包初始化时注册内部类型与外部版本，
// 确保调用方直接使用 Scheme/Codecs 时无需手动完成注册。
func init() {
	AddToScheme(Scheme)
}

// AddToScheme 注册 API 组并向 scheme 添加类型，同时设定版本优先级。
func AddToScheme(scheme *runtime.Scheme) {
	utilruntime.Must(config.AddToScheme(scheme))
	utilruntime.Must(v1alpha1.AddToScheme(scheme))
	utilruntime.Must(scheme.SetVersionPriority(v1alpha1.SchemeGroupVersion))
}
