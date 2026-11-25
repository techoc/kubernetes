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

// GroupName 是本包使用的 API 组名。
// 直接复用 API 包里的常量，避免客户端与服务端对组名产生分歧。
const GroupName = cmv1beta1.GroupName

// SchemeGroupVersion 是注册这些对象时使用的 group/version。
// 客户端 scheme 用内部版本做中枢，外部版本在请求时再协商。
var SchemeGroupVersion = schema.GroupVersion{Group: GroupName, Version: runtime.APIVersionInternal}

// Scheme 是注册了所有自定义指标 API 类型的 runtime.Scheme。
// 这里注册所有版本的类型，使客户端能在版本间做转换。
var Scheme = runtime.NewScheme()

// Codecs 提供基于该 scheme 的编解码器。
// 编解码工厂按 scheme 中注册的类型创建序列化器。
var Codecs = serializer.NewCodecFactory(Scheme)

// ParameterCodec 处理转换为查询参数的对象的版本转换。
// 查询参数里的 LabelSelector 等对象需要按版本编码成字符串。
var ParameterCodec = runtime.NewParameterCodec(Scheme)

// 把 metav1 的基础类型和本组各版本类型一起注册，保证解码器认识所有内嵌类型。
func init() {
	metav1.AddToGroupVersion(Scheme, schema.GroupVersion{Version: "v1"})
	AddToScheme(Scheme)
}

// AddToScheme 将自定义指标 clientset 的所有类型注册到给定 scheme，便于组合多个 clientset，例如：
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
// 完成后，Kubernetes 类型中的 RawExtension 将能正确序列化 kube-aggregator 类型。
func AddToScheme(scheme *runtime.Scheme) {
	cmint.AddToScheme(scheme)
	cmv1beta1.AddToScheme(scheme)
	cmv1beta2.AddToScheme(scheme)
}
