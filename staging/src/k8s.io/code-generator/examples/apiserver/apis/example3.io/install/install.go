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

// install 包安装实验性 API group，使其可用于
// 所有 API 编解码设施。
//
// install 包负责把示例 API group（内部版本与 v1 外部版本）注册进 scheme，
// 并把 v1 设为优先版本。
package install

import (
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	example3 "k8s.io/code-generator/examples/apiserver/apis/example3.io"
	"k8s.io/code-generator/examples/apiserver/apis/example3.io/v1"
)

// Install 注册 API group 并把类型加入 scheme
// Install 注册内部版本与 v1，并把 v1 设为该 group 的首选版本。
func Install(scheme *runtime.Scheme) {
	utilruntime.Must(example3.AddToScheme(scheme))
	utilruntime.Must(v1.AddToScheme(scheme))
	utilruntime.Must(scheme.SetVersionPriority(v1.SchemeGroupVersion))
}
