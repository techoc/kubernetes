/*
Copyright The Kubernetes Authors.

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

// 本包安装 policyconfig API 组，使其对所有 API 编解码机制可用。
//
// policyconfig 是 MutatingAdmissionPolicy 等策略配置的 API 组；
// 本包只负责把它挂到 scheme 上，不做任何业务逻辑。
package install

import (
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apiserver/pkg/admission/plugin/policy/config/apis/policyconfig"
	v1 "k8s.io/apiserver/pkg/admission/plugin/policy/config/apis/policyconfig/v1"
)

// Install 注册 API 组并把类型加入 scheme。
//
// 先注册内部版本与 v1 版本，再把 v1 设为该组的首选版本，
// 保证序列化/解码默认走 v1。
func Install(scheme *runtime.Scheme) {
	utilruntime.Must(policyconfig.AddToScheme(scheme))
	utilruntime.Must(v1.AddToScheme(scheme))
	utilruntime.Must(scheme.SetVersionPriority(v1.SchemeGroupVersion))
}
