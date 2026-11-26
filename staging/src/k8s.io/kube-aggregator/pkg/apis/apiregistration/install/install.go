/*
Copyright 2016 The Kubernetes Authors.

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
	"k8s.io/kube-aggregator/pkg/apis/apiregistration"
	"k8s.io/kube-aggregator/pkg/apis/apiregistration/v1"
	"k8s.io/kube-aggregator/pkg/apis/apiregistration/v1beta1"
)

// Install 注册 API 组并把类型加入 scheme。
//
// 中文补充：内部版本（apiregistration）是转换的中枢，v1/v1beta1 才是线上传输的线格式，
// 三者都注册进来，scheme 才能在内部对象和外部版本之间来回转换。
// 把 v1 排在 v1beta1 前面，是为了让客户端协商版本时默认拿到更稳定的那一个。
// 注册失败意味着 scheme 从根上不可用，所以用 Must 在启动时直接失败，
// 而不是带着残缺的 scheme 继续对外服务。
func Install(scheme *runtime.Scheme) {
	utilruntime.Must(apiregistration.AddToScheme(scheme))
	utilruntime.Must(v1.AddToScheme(scheme))
	utilruntime.Must(v1beta1.AddToScheme(scheme))
	utilruntime.Must(scheme.SetVersionPriority(v1.SchemeGroupVersion, v1beta1.SchemeGroupVersion))
}
