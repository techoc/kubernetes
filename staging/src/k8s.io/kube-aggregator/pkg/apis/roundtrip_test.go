/*
Copyright 2024 The Kubernetes Authors.

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

package apis

import (
	"testing"

	"k8s.io/apimachinery/pkg/api/apitesting/fuzzer"
	"k8s.io/apimachinery/pkg/api/apitesting/roundtrip"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/kube-aggregator/pkg/apis/apiregistration/install"
	apiregistrationv1 "k8s.io/kube-aggregator/pkg/apis/apiregistration/v1"
	apiregistrationv1beta1 "k8s.io/kube-aggregator/pkg/apis/apiregistration/v1beta1"
)

// 聚合 API 的对象既要经 protobuf/JSON 在 apiserver 之间传递，也要经过
// unstructured 形态被动态客户端处理。这里用 fuzz 往返验证两条路径不丢字段，
// 否则客户端读写 scalar 字段时可能悄悄拿不到值。
func TestRoundtripToUnstructured(t *testing.T) {
	scheme := runtime.NewScheme()
	install.Install(scheme)

	// 只跳过元信息里的 options 类型：它们没有对应的内部版本，
	// 走通用的 internal -> external -> unstructured 往返没有意义。
	roundtrip.RoundtripToUnstructured(t, scheme, fuzzer.MergeFuzzerFuncs(), sets.New(
		apiregistrationv1.SchemeGroupVersion.WithKind("CreateOptions"),
		apiregistrationv1.SchemeGroupVersion.WithKind("PatchOptions"),
		apiregistrationv1.SchemeGroupVersion.WithKind("UpdateOptions"),
		apiregistrationv1beta1.SchemeGroupVersion.WithKind("CreateOptions"),
		apiregistrationv1beta1.SchemeGroupVersion.WithKind("PatchOptions"),
		apiregistrationv1beta1.SchemeGroupVersion.WithKind("UpdateOptions"),
	), nil)
}
