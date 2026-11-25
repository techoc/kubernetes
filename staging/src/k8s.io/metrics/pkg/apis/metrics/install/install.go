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

// Package install 注册实验性 API 组，供 API 编码和解码机制使用。
// 本包只负责注册，不定义类型，让各组件共享同一份 scheme 配置。
package install

import (
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/metrics/pkg/apis/metrics"
	v1 "k8s.io/metrics/pkg/apis/metrics/v1"
	"k8s.io/metrics/pkg/apis/metrics/v1beta1"
)

// Install 注册 API 组，并将相关类型添加到 scheme。
// 将 v1 放在版本优先级首位，编码时默认输出该稳定版本。
func Install(scheme *runtime.Scheme) {
	utilruntime.Must(metrics.AddToScheme(scheme))
	utilruntime.Must(v1beta1.AddToScheme(scheme))
	utilruntime.Must(v1.AddToScheme(scheme))
	utilruntime.Must(scheme.SetVersionPriority(v1.SchemeGroupVersion, v1beta1.SchemeGroupVersion))
}
