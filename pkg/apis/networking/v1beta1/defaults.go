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
	networkingv1beta1 "k8s.io/api/networking/v1beta1"
	"k8s.io/apimachinery/pkg/runtime"
)

// addDefaultingFuncs 把本包的默认值注册函数挂到 scheme，供版本转换与反序列化使用。
func addDefaultingFuncs(scheme *runtime.Scheme) error {
	return RegisterDefaults(scheme)
}

// SetDefaults_HTTPIngressPath 为未指定 PathType 的路径填充默认匹配类型。
func SetDefaults_HTTPIngressPath(obj *networkingv1beta1.HTTPIngressPath) {
	// v1beta1 为兼容旧行为，缺省路径匹配类型为 ImplementationSpecific。
	var defaultPathType = networkingv1beta1.PathTypeImplementationSpecific
	if obj.PathType == nil {
		obj.PathType = &defaultPathType
	}
}
