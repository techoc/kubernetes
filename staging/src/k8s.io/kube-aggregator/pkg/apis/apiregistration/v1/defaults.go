/*
Copyright 2019 The Kubernetes Authors.

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

package v1

import (
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/ptr"
)

// 这个私有的注册钩子由生成代码调用，把本文件里的默认值函数接进 scheme；
// 对外仍然只暴露 scheme 的 AddToScheme 流程，避免调用方手工逐个登记。
func addDefaultingFuncs(scheme *runtime.Scheme) error {
	return RegisterDefaults(scheme)
}

// SetDefaults_ServiceReference sets defaults for AuditSync Webhook's ServiceReference
// 这里默认 443 是历史约定：聚合层向后端 Service 发请求时默认走 HTTPS，
// 因此"没写端口"要显式落成一个端口值，而不是留 nil 让下游各自猜。
// 用指针字段是为了能区分"用户明确没指定"和"用户指定了一个端口"，
// 默认值只在前者上生效。已弃用的 v1beta1 里也保留同一默认值，以免版本转换时行为漂移。
func SetDefaults_ServiceReference(obj *ServiceReference) {
	if obj.Port == nil {
		obj.Port = ptr.To[int32](443)
	}
}
