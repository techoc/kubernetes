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

// Package install 负责把该 API 组注册到编码/解码机制中，
// 使其可以参与所有 API 对象的编码与解码。
package install

import (
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/kubernetes/pkg/api/legacyscheme"
	"k8s.io/kubernetes/pkg/apis/flowcontrol"
	flowcontrolv1 "k8s.io/kubernetes/pkg/apis/flowcontrol/v1"
	flowcontrolv1beta1 "k8s.io/kubernetes/pkg/apis/flowcontrol/v1beta1"
	flowcontrolv1beta2 "k8s.io/kubernetes/pkg/apis/flowcontrol/v1beta2"
	flowcontrolv1beta3 "k8s.io/kubernetes/pkg/apis/flowcontrol/v1beta3"
)

func init() {
	Install(legacyscheme.Scheme)
}

// Install 注册 API 组并把该组的所有版本类型加入 scheme。
func Install(scheme *runtime.Scheme) {
	utilruntime.Must(flowcontrol.AddToScheme(scheme))
	utilruntime.Must(flowcontrolv1beta1.AddToScheme(scheme))
	utilruntime.Must(flowcontrolv1beta2.AddToScheme(scheme))
	utilruntime.Must(flowcontrolv1beta3.AddToScheme(scheme))
	utilruntime.Must(flowcontrolv1.AddToScheme(scheme))

	utilruntime.Must(scheme.SetVersionPriority(flowcontrolv1.SchemeGroupVersion, flowcontrolv1beta3.SchemeGroupVersion,
		flowcontrolv1beta2.SchemeGroupVersion, flowcontrolv1beta1.SchemeGroupVersion))
}
