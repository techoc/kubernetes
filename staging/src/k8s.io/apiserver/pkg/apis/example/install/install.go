/*
Copyright 2016 The Kubernetes Authors.

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

// Package install 注册示例 API 组，使其可用于 API 编解码体系。
package install

import (
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apiserver/pkg/apis/example"
	examplev1 "k8s.io/apiserver/pkg/apis/example/v1"
)

// Install 注册 API 组并将类型加入 scheme。
//
// 同时注册内部版本与 v1 外部版本，并把 v1 设为版本优先级最高者。
// 该 example 组只用于 apiserver 的测试与示例，不会真正对外提供服务。
func Install(scheme *runtime.Scheme) {
	utilruntime.Must(example.AddToScheme(scheme))
	utilruntime.Must(examplev1.AddToScheme(scheme))
	utilruntime.Must(scheme.SetVersionPriority(examplev1.SchemeGroupVersion))
}
