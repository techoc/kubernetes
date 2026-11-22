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

package server

// This file exists to force the desired plugin implementations to be linked into genericapi pkg.
//
// 该文件通过空导入（实际是直接引用并调用 Register）把准入插件的实现编译进通用 apiserver 二进制。
// 插件按名字在运行时查找，若只靠配置字符串，编译器无法得知依赖，链接器会把实现裁掉；
// 因此必须在这里显式引用每个插件包。
import (
	"k8s.io/apiserver/pkg/admission"
	"k8s.io/apiserver/pkg/admission/plugin/namespace/lifecycle"
	mutatingadmissionpolicy "k8s.io/apiserver/pkg/admission/plugin/policy/mutating"
	validatingadmissionpolicy "k8s.io/apiserver/pkg/admission/plugin/policy/validating"
	mutatingwebhook "k8s.io/apiserver/pkg/admission/plugin/webhook/mutating"
	validatingwebhook "k8s.io/apiserver/pkg/admission/plugin/webhook/validating"
)

// RegisterAllAdmissionPlugins registers all admission plugins
//
// 把所有通用准入插件注册进 registry。NamespaceLifecycle 必须最先注册/最先执行，
// 因为它负责阻止向正在终止的命名空间写入对象，其他插件都应看到这一前置结论。
// 注意：此函数只做注册，具体启用哪些插件仍由配置中的启用列表决定。
func RegisterAllAdmissionPlugins(plugins *admission.Plugins) {
	lifecycle.Register(plugins)
	validatingwebhook.Register(plugins)
	mutatingwebhook.Register(plugins)
	validatingadmissionpolicy.Register(plugins)
	mutatingadmissionpolicy.Register(plugins)
}
