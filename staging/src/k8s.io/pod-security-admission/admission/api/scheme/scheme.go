/*
Copyright 2021 The Kubernetes Authors.

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

package scheme

import (
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	podsecurityapi "k8s.io/pod-security-admission/admission/api"
	podsecurityv1 "k8s.io/pod-security-admission/admission/api/v1"
	podsecurityv1alpha1 "k8s.io/pod-security-admission/admission/api/v1alpha1"
	podsecurityv1beta1 "k8s.io/pod-security-admission/admission/api/v1beta1"
)

var (
	// Scheme is the runtime.Scheme to which all podsecurity api types are registered.
	Scheme = runtime.NewScheme()

	// Codecs provides access to encoding and decoding for the scheme.
	// 开启严格解码：配置里拼错或多余的字段必须报错，不能被静默忽略后套用默认值。
	Codecs = serializer.NewCodecFactory(Scheme, serializer.EnableStrict)
)

func init() {
	AddToScheme(Scheme)
}

// AddToScheme builds the podsecurity scheme using all known versions of the podsecurity api.
// 版本优先级按 v1 -> v1beta1 -> v1alpha1 排列，旧配置仍可读取，但序列化时一律输出首选版本。
func AddToScheme(scheme *runtime.Scheme) {
	utilruntime.Must(podsecurityapi.AddToScheme(scheme))
	utilruntime.Must(podsecurityv1alpha1.AddToScheme(scheme))
	utilruntime.Must(podsecurityv1beta1.AddToScheme(scheme))
	utilruntime.Must(podsecurityv1.AddToScheme(scheme))
	utilruntime.Must(scheme.SetVersionPriority(podsecurityv1.SchemeGroupVersion, podsecurityv1beta1.SchemeGroupVersion, podsecurityv1alpha1.SchemeGroupVersion))
}
