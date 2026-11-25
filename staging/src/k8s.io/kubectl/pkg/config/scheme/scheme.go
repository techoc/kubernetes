/*
Copyright 2025 The Kubernetes Authors.

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
	"k8s.io/kubectl/pkg/config"
	"k8s.io/kubectl/pkg/config/v1alpha1"
	"k8s.io/kubectl/pkg/config/v1beta1"
)

var (
	// Scheme 定义了序列化和反序列化 API 对象的方法。
	Scheme = runtime.NewScheme()
	// StrictCodecs 提供了针对特定版本和内容类型获取编解码器
	// 与序列化器的方法。
	StrictCodecs = serializer.NewCodecFactory(Scheme, serializer.EnableStrict)
	// LenientCodecs 提供了针对特定版本和内容类型获取编解码器
	// 与序列化器的方法。
	LenientCodecs = serializer.NewCodecFactory(Scheme, serializer.DisableStrict)
)

func init() {
	AddToScheme(Scheme)
}

// AddToScheme 注册 API 组，并将类型添加到 scheme 中。
func AddToScheme(scheme *runtime.Scheme) {
	utilruntime.Must(config.AddToScheme(scheme))
	utilruntime.Must(v1beta1.AddToScheme(scheme))
	utilruntime.Must(v1alpha1.AddToScheme(scheme))
}
