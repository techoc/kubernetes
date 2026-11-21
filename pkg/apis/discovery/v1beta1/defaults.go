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
	v1 "k8s.io/api/core/v1"
	discoveryv1beta1 "k8s.io/api/discovery/v1beta1"
	"k8s.io/apimachinery/pkg/runtime"
)

var (
	// defaultPortName 是 EndpointPort 名称为空时的默认值，用空字符串表示匿名端口。
	defaultPortName = ""
	// defaultProtocol 是 EndpointPort 协议未指定时的默认值，选用 TCP 以保持向后兼容。
	defaultProtocol = v1.ProtocolTCP
)

// addDefaultingFuncs 向 scheme 注册默认值设置函数。
func addDefaultingFuncs(scheme *runtime.Scheme) error {
	return RegisterDefaults(scheme)
}

// SetDefaults_EndpointPort 为 EndpointPort 填充默认值：
// 名称缺省为空字符串，协议缺省为 TCP。
func SetDefaults_EndpointPort(obj *discoveryv1beta1.EndpointPort) {
	if obj.Name == nil {
		obj.Name = &defaultPortName
	}

	if obj.Protocol == nil {
		obj.Protocol = &defaultProtocol
	}
}
