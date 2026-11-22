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

// Package install 安装实验性 API 组，使其可用于所有 API 编解码机制。
package install

import (
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	internalapi "k8s.io/kubernetes/plugin/pkg/admission/podtolerationrestriction/apis/podtolerationrestriction"
	versionedapi "k8s.io/kubernetes/plugin/pkg/admission/podtolerationrestriction/apis/podtolerationrestriction/v1alpha1"
)

// Install 注册 API 组并将类型加入 scheme。
func Install(scheme *runtime.Scheme) {
	utilruntime.Must(internalapi.AddToScheme(scheme))
	utilruntime.Must(versionedapi.AddToScheme(scheme))
	utilruntime.Must(scheme.SetVersionPriority(versionedapi.SchemeGroupVersion))
}
