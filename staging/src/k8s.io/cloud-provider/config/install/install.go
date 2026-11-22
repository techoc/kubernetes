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

// Package install 把 cloud-controller-manager 的内部与 v1alpha1 外部配置类型
// 注册进 runtime.Scheme，供组件配置的解析、默认化与转换使用。
package install

import (
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/cloud-provider/config"
	"k8s.io/cloud-provider/config/v1alpha1"
)

var (
	// Scheme defines methods for serializing and deserializing API objects.
	// Scheme 只包含 CCM 自身的配置类型；组件配置读写都经由它完成。
	Scheme = runtime.NewScheme()
	// Codecs provides methods for retrieving codecs and serializers for specific
	// versions and content types.
	Codecs = serializer.NewCodecFactory(Scheme)
)

func init() {
	// 包初始化即注册，保证导入本包即可使用 Scheme/Codecs。
	Install(Scheme)
}

// Install adds the types of this group into the given scheme.
// 同时注册内部版本与 v1alpha1，并把 v1alpha1 设为序列化首选版本，
// 这样外部配置文件与 /configz 输出默认使用 v1alpha1。
func Install(scheme *runtime.Scheme) {
	utilruntime.Must(config.AddToScheme(scheme))
	utilruntime.Must(v1alpha1.AddToScheme(scheme))
	utilruntime.Must(scheme.SetVersionPriority(v1alpha1.SchemeGroupVersion))
}
