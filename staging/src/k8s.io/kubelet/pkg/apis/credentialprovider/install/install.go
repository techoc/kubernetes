/*
Copyright 2020 The Kubernetes Authors.

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

package install

import (
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/kubelet/pkg/apis/credentialprovider"
	v1 "k8s.io/kubelet/pkg/apis/credentialprovider/v1"
	"k8s.io/kubelet/pkg/apis/credentialprovider/v1alpha1"
	"k8s.io/kubelet/pkg/apis/credentialprovider/v1beta1"
)

// Install registers the credentialprovider.kubelet.k8s.io APIs into the given scheme.
// Install 按 v1alpha1、v1beta1、v1 的顺序注册，并把最新注册的版本设为优先级最高，
// 这样 scheme 在解码未指定版本的凭据插件配置时默认使用最新 API。
func Install(scheme *runtime.Scheme) {
	utilruntime.Must(credentialprovider.AddToScheme(scheme))
	utilruntime.Must(v1alpha1.AddToScheme(scheme))
	utilruntime.Must(scheme.SetVersionPriority(v1alpha1.SchemeGroupVersion))
	utilruntime.Must(v1beta1.AddToScheme(scheme))
	utilruntime.Must(scheme.SetVersionPriority(v1beta1.SchemeGroupVersion))
	utilruntime.Must(v1.AddToScheme(scheme))
	utilruntime.Must(scheme.SetVersionPriority(v1.SchemeGroupVersion))
}
