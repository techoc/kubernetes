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

package v1beta1_test

import (
	"reflect"
	"testing"

	networkingv1beta1 "k8s.io/api/networking/v1beta1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/kubernetes/pkg/api/legacyscheme"
	_ "k8s.io/kubernetes/pkg/apis/networking/install"
	. "k8s.io/kubernetes/pkg/apis/networking/v1beta1"
)

// TestSetIngressPathDefaults 验证 HTTPIngressPath 的 pathType 默认值：
// 未指定时默认为 ImplementationSpecific，以保持旧版本 Ingress 的行为；
// 已指定的取值（包括 Exact）不会被覆盖。
func TestSetIngressPathDefaults(t *testing.T) {
	pathTypeImplementationSpecific := networkingv1beta1.PathTypeImplementationSpecific
	pathTypeExact := networkingv1beta1.PathTypeExact

	// 用例以名称为键，输入/期望为单个 HTTPIngressPath，需包装成 Ingress 才能走默认化流程。
	testCases := map[string]struct {
		original *networkingv1beta1.HTTPIngressPath
		expected *networkingv1beta1.HTTPIngressPath
	}{
		"empty pathType should default to ImplementationSpecific": {
			original: &networkingv1beta1.HTTPIngressPath{},
			expected: &networkingv1beta1.HTTPIngressPath{PathType: &pathTypeImplementationSpecific},
		},
		"ImplementationSpecific pathType should not change": {
			original: &networkingv1beta1.HTTPIngressPath{PathType: &pathTypeImplementationSpecific},
			expected: &networkingv1beta1.HTTPIngressPath{PathType: &pathTypeImplementationSpecific},
		},
		"Exact pathType should not change": {
			original: &networkingv1beta1.HTTPIngressPath{PathType: &pathTypeExact},
			expected: &networkingv1beta1.HTTPIngressPath{PathType: &pathTypeExact},
		},
	}
	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			ingressOriginal := wrapIngressPath(testCase.original)
			ingressExpected := wrapIngressPath(testCase.expected)
			// 通过编码-解码-转换链路触发默认化，而不是直接调用默认函数，
			// 这样才能同时验证版本转换过程中的默认语义。
			runtimeObj := roundTrip(t, runtime.Object(ingressOriginal))
			ingressActual, ok := runtimeObj.(*networkingv1beta1.Ingress)
			if !ok {
				t.Fatalf("Unexpected object: %v", ingressActual)
			}
			if !apiequality.Semantic.DeepEqual(ingressActual.Spec, ingressExpected.Spec) {
				t.Errorf("Expected: %+v, got: %+v", ingressExpected.Spec, ingressActual.Spec)
			}
		})
	}
}

// wrapIngressPath 把单个路径包装成最小合法的 Ingress 对象，
// 因为 pathType 的默认化发生在 Ingress 的转换链路中，无法对裸路径直接进行。
func wrapIngressPath(path *networkingv1beta1.HTTPIngressPath) *networkingv1beta1.Ingress {
	return &networkingv1beta1.Ingress{
		Spec: networkingv1beta1.IngressSpec{
			Rules: []networkingv1beta1.IngressRule{{
				IngressRuleValue: networkingv1beta1.IngressRuleValue{
					HTTP: &networkingv1beta1.HTTPIngressRuleValue{
						Paths: []networkingv1beta1.HTTPIngressPath{*path},
					},
				},
			}},
		},
	}
}

// roundTrip 将对象编码后解码并转换为目标版本，
// 用于在测试中复现真实 API 请求链路上的默认化行为。
func roundTrip(t *testing.T, obj runtime.Object) runtime.Object {
	t.Helper()
	data, err := runtime.Encode(legacyscheme.Codecs.LegacyCodec(SchemeGroupVersion), obj)
	if err != nil {
		t.Errorf("%v\n %#v", err, obj)
		return nil
	}
	obj2, err := runtime.Decode(legacyscheme.Codecs.UniversalDecoder(), data)
	if err != nil {
		t.Errorf("%v\nData: %s\nSource: %#v", err, string(data), obj)
		return nil
	}
	obj3 := reflect.New(reflect.TypeOf(obj).Elem()).Interface().(runtime.Object)
	err = legacyscheme.Scheme.Convert(obj2, obj3, nil)
	if err != nil {
		t.Errorf("%v\nSource: %#v", err, obj2)
		return nil
	}
	return obj3
}
