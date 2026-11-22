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

package fuzzer

import (
	"fmt"

	"sigs.k8s.io/randfill"

	apitesting "k8s.io/apimachinery/pkg/api/apitesting"
	"k8s.io/apimachinery/pkg/api/apitesting/fuzzer"
	"k8s.io/apimachinery/pkg/runtime"
	runtimeserializer "k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/apiserver/pkg/apis/example"
	examplev1 "k8s.io/apiserver/pkg/apis/example/v1"
)

// overrideMetaFuncs 覆盖 apiserver 的通用 fuzzer 函数，以在 Kubernetes 语境下
// 产生更真实的值。
func overrideMetaFuncs(codecs runtimeserializer.CodecFactory) []interface{} {
	return []interface{}{
		func(j *runtime.Object, c randfill.Continue) {
			// TODO: uncomment when round trip starts from a versioned object
			// 当前 round-trip 测试从内部版本出发，无法携带 TypeMeta，
			// 因此统一使用 runtime.Unknown 承载原始 JSON。
			if true { // c.Bool() {
				*j = &runtime.Unknown{
					// We do not set TypeMeta here because it is not carried through a round trip
					Raw:         []byte(`{"apiVersion":"unknown.group/unknown","kind":"Something","someKey":"someValue"}`),
					ContentType: runtime.ContentTypeJSON,
				}
			} else {
				types := []runtime.Object{&example.Pod{}}
				t := types[c.Rand.Intn(len(types))]
				c.Fill(t)
				*j = t
			}
		},
		func(r *runtime.RawExtension, c randfill.Continue) {
			// 随机挑选一个类型并填充
			types := []runtime.Object{&example.Pod{}}
			obj := types[c.Rand.Intn(len(types))]
			c.Fill(obj)

			// 编码为原始字节，模拟从存储/网络读到的未解析对象
			bytes, err := runtime.Encode(apitesting.TestCodec(codecs, examplev1.SchemeGroupVersion), obj)
			if err != nil {
				panic(fmt.Sprintf("Failed to encode object: %v", err))
			}

			// Set the bytes field on the RawExtension
			r.Raw = bytes
		},
	}
}

// exampleFuncs 为 example 组提供自定义 fuzzer，保证字段值落在 API 合法范围内。
func exampleFuncs(codecs runtimeserializer.CodecFactory) []interface{} {
	return []interface{}{
		func(s *example.PodSpec, c randfill.Continue) {
			c.FillNoCustom(s)
			// 该字段有默认值，必须非 nil 且非零才能通过校验
			ttl := int64(30)
			if c.Bool() {
				ttl = int64(c.Uint32())
			}
			s.TerminationGracePeriodSeconds = &ttl

			if s.SchedulerName == "" {
				s.SchedulerName = "default-scheduler"
			}
		},
		func(j *example.PodPhase, c randfill.Continue) {
			// 枚举值必须从已知集合中挑选，随机字符串会被校验拒绝。
			statuses := []example.PodPhase{"Pending", "Running", "Succeeded", "Failed", "Unknown"}
			*j = statuses[c.Rand.Intn(len(statuses))]
		},
		func(rp *example.RestartPolicy, c randfill.Continue) {
			policies := []example.RestartPolicy{"Always", "Never", "OnFailure"}
			*rp = policies[c.Rand.Intn(len(policies))]
		},
	}
}

// Funcs 返回 example API 组使用的 fuzzer 函数集合。
var Funcs = fuzzer.MergeFuzzerFuncs(
	overrideMetaFuncs,
	exampleFuncs,
)
