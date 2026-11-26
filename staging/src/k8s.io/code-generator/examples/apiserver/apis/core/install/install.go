/*
Copyright 2024 The Kubernetes Authors.

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

// Package install 注册实验性 API group，使其可用于 API 编解码链路。
// install 包把 core 群组的 internal 与 v1 类型都注册进 scheme，
// 并显式设置版本优先级为 v1，使序列化默认选择 v1。
package install

import (
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/code-generator/examples/apiserver/apis/core"
	"k8s.io/code-generator/examples/apiserver/apis/core/v1"
)

// Install 注册 API group 并把类型加入 scheme。
// Must 会在注册失败时 panic：安装阶段失败属于编程/配置错误，不应被静默忽略。
func Install(scheme *runtime.Scheme) {
	utilruntime.Must(core.AddToScheme(scheme))
	utilruntime.Must(v1.AddToScheme(scheme))
	utilruntime.Must(scheme.SetVersionPriority(v1.SchemeGroupVersion))
}
