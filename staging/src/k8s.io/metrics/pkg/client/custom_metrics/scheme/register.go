/*
Copyright 2018 The Kubernetes Authors.

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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	runtime "k8s.io/apimachinery/pkg/runtime"
	schema "k8s.io/apimachinery/pkg/runtime/schema"
	serializer "k8s.io/apimachinery/pkg/runtime/serializer"
	cmint "k8s.io/metrics/pkg/apis/custom_metrics"
	cmv1beta1 "k8s.io/metrics/pkg/apis/custom_metrics/v1beta1"
	cmv1beta2 "k8s.io/metrics/pkg/apis/custom_metrics/v1beta2"
)

// GroupName is the group name use in this package.
// 中文说明：直接复用 API 包里的常量，避免客户端与服务端对 group 名产生分歧。
const GroupName = cmv1beta1.GroupName

// SchemeGroupVersion is group version used to register these objects.
// 中文说明：客户端 scheme 用内部版本做中枢，外部版本在请求时再协商。
var SchemeGroupVersion = schema.GroupVersion{Group: GroupName, Version: runtime.APIVersionInternal}

// Scheme is the runtime.Scheme to which all custom metrics api types are registered.
// 中文说明：这里注册所有版本的类型，使客户端能在版本间做转换。
var Scheme = runtime.NewScheme()

// Codecs provides access to encoding and decoding for the scheme.
// 中文说明：编解码工厂按 scheme 中注册的类型创建序列化器。
var Codecs = serializer.NewCodecFactory(Scheme)

// ParameterCodec handles versioning of objects that are converted to query parameters.
// 中文说明：查询参数里的 LabelSelector 等对象需要按版本编码成字符串。
var ParameterCodec = runtime.NewParameterCodec(Scheme)

// 中文说明：把 metav1 的基础类型和本组各版本类型一起注册，保证解码器认识所有内嵌类型。
func init() {
	metav1.AddToGroupVersion(Scheme, schema.GroupVersion{Version: "v1"})
	AddToScheme(Scheme)
}

// AddToScheme adds all types of this clientset into the given scheme. This allows composition
// of clientsets, like in:
//
//	import (
//	  "k8s.io/client-go/kubernetes"
//	  clientsetscheme "k8s.io/client-go/kubernetes/scheme"
//	  aggregatorclientsetscheme "k8s.io/kube-aggregator/pkg/client/clientset_generated/clientset/scheme"
//	)
//
//	kclientset, _ := kubernetes.NewForConfig(c)
//	aggregatorclientsetscheme.AddToScheme(clientsetscheme.Scheme)
//
// After this, RawExtensions in Kubernetes types will serialize kube-aggregator types
// correctly.
func AddToScheme(scheme *runtime.Scheme) {
	cmint.AddToScheme(scheme)
	cmv1beta1.AddToScheme(scheme)
	cmv1beta2.AddToScheme(scheme)
}
