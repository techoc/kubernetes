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

package testing

import (
	"fmt"

	"sigs.k8s.io/randfill"

	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	apitesting "k8s.io/apimachinery/pkg/api/apitesting"
	"k8s.io/apimachinery/pkg/api/apitesting/fuzzer"
	metafuzzer "k8s.io/apimachinery/pkg/apis/meta/fuzzer"
	"k8s.io/apimachinery/pkg/runtime"
	runtimeserializer "k8s.io/apimachinery/pkg/runtime/serializer"
	admissionregistrationfuzzer "k8s.io/kubernetes/pkg/apis/admissionregistration/fuzzer"
	"k8s.io/kubernetes/pkg/apis/apps"
	appsfuzzer "k8s.io/kubernetes/pkg/apis/apps/fuzzer"
	autoscalingfuzzer "k8s.io/kubernetes/pkg/apis/autoscaling/fuzzer"
	batchfuzzer "k8s.io/kubernetes/pkg/apis/batch/fuzzer"
	certificatesfuzzer "k8s.io/kubernetes/pkg/apis/certificates/fuzzer"
	api "k8s.io/kubernetes/pkg/apis/core"
	corefuzzer "k8s.io/kubernetes/pkg/apis/core/fuzzer"
	discoveryfuzzer "k8s.io/kubernetes/pkg/apis/discovery/fuzzer"
	extensionsfuzzer "k8s.io/kubernetes/pkg/apis/extensions/fuzzer"
	flowcontrolfuzzer "k8s.io/kubernetes/pkg/apis/flowcontrol/fuzzer"
	networkingfuzzer "k8s.io/kubernetes/pkg/apis/networking/fuzzer"
	policyfuzzer "k8s.io/kubernetes/pkg/apis/policy/fuzzer"
	rbacfuzzer "k8s.io/kubernetes/pkg/apis/rbac/fuzzer"
	resourcefuzzer "k8s.io/kubernetes/pkg/apis/resource/fuzzer"
	schedulingfuzzer "k8s.io/kubernetes/pkg/apis/scheduling/fuzzer"
	storagefuzzer "k8s.io/kubernetes/pkg/apis/storage/fuzzer"
)

// overrideGenericFuncs 覆写 k8s.io/apiserver 中若干通用 fuzzer 函数，
// 让随机生成的值更贴近 Kubernetes 的真实使用场景。
func overrideGenericFuncs(codecs runtimeserializer.CodecFactory) []interface{} {
	return []interface{}{
		func(j *runtime.Object, c randfill.Continue) {
			// TODO: 当 round-trip 从带版本的对象开始时，放开下面的分支。
			if true { // c.Bool() {
				// TypeMeta 不参与 round-trip，因此这里不设置它。
				*j = &runtime.Unknown{
					Raw:         []byte(`{"apiVersion":"unknown.group/unknown","kind":"Something","someKey":"someValue"}`),
					ContentType: runtime.ContentTypeJSON,
				}
			} else {
				types := []runtime.Object{&api.Pod{}, &api.ReplicationController{}}
				t := types[c.Rand.Intn(len(types))]
				c.Fill(t)
				*j = t
			}
		},
		func(r *runtime.RawExtension, c randfill.Continue) {
			// 随机选一种类型做 fuzz，再把对象编码成 RawExtension.Raw。
			types := []runtime.Object{&api.Pod{}, &apps.Deployment{}, &api.Service{}}
			obj := types[c.Rand.Intn(len(types))]
			c.Fill(obj)

			var codec runtime.Codec
			// Deployment 用 apps/v1 编码；其余用 core/v1。
			switch obj.(type) {
			case *apps.Deployment:
				codec = apitesting.TestCodec(codecs, appsv1.SchemeGroupVersion)
			default:
				codec = apitesting.TestCodec(codecs, v1.SchemeGroupVersion)
			}

			bytes, err := runtime.Encode(codec, obj)
			if err != nil {
				panic(fmt.Sprintf("Failed to encode object: %v", err))
			}

			r.Raw = bytes
		},
	}
}

// FuzzerFuncs 是合并后的 fuzzer 函数列表：通用覆写 + 各 API group 的自定义 fuzzer。
// round-trip 测试用它生成覆盖各种字段组合的对象。
var FuzzerFuncs = fuzzer.MergeFuzzerFuncs(
	overrideGenericFuncs,
	corefuzzer.Funcs,
	extensionsfuzzer.Funcs,
	appsfuzzer.Funcs,
	batchfuzzer.Funcs,
	autoscalingfuzzer.Funcs,
	rbacfuzzer.Funcs,
	policyfuzzer.Funcs,
	resourcefuzzer.Funcs,
	certificatesfuzzer.Funcs,
	admissionregistrationfuzzer.Funcs,
	storagefuzzer.Funcs,
	networkingfuzzer.Funcs,
	metafuzzer.Funcs,
	schedulingfuzzer.Funcs,
	discoveryfuzzer.Funcs,
	flowcontrolfuzzer.Funcs,
)
