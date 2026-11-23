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

package v1beta1

import (
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/ptr"
)

// 通过 scheme 注册默认值函数，使得对象在反序列化进入该版本时自动补齐缺省字段，
// 避免每个调用点都记得手工填一遍。
func addDefaultingFuncs(scheme *runtime.Scheme) error {
	return RegisterDefaults(scheme)
}

// SetDefaults_ServiceReference sets defaults for AuditSync Webhook's ServiceReference
// 端口在 v1beta1 是指针，nil 才代表“未指定”；默认值取 443 是为了兼容
// 早期只支持 HTTPS 标准端口的行为，无需用户显式声明。
func SetDefaults_ServiceReference(obj *ServiceReference) {
	if obj.Port == nil {
		obj.Port = ptr.To[int32](443)
	}
}
