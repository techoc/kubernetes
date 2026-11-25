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

// Package fuzzer 为 testapigroup 提供 randfill 填充函数。
//
// testapigroup 是 apimachinery 内部的测试 API 组，不对外发布；这里的 fuzz
// 函数只保证生成的值能通过序列化与往返测试。
package fuzzer

import (
	"fmt"

	"sigs.k8s.io/randfill"

	apitesting "k8s.io/apimachinery/pkg/api/apitesting"
	"k8s.io/apimachinery/pkg/api/apitesting/fuzzer"
	"k8s.io/apimachinery/pkg/apis/testapigroup"
	v1 "k8s.io/apimachinery/pkg/apis/testapigroup/v1"
	"k8s.io/apimachinery/pkg/runtime"
	runtimeserializer "k8s.io/apimachinery/pkg/runtime/serializer"
)

// 覆盖 apimachinery 的通用 fuzz 函数，让 runtime.Object / RawExtension 的取值
// 更接近真实 API 场景（而不是随机结构）。
func overrideMetaFuncs(codecs runtimeserializer.CodecFactory) []interface{} {
	return []interface{}{
		func(j *runtime.Object, c randfill.Continue) {
			// TODO: 当往返测试从有版本对象开始时取消注释。
			if true { // c.Bool() {
				// 始终生成 runtime.Unknown：其 Raw 内容在往返中保持不变，
				// 避免随机类型引入无法比较的字段。
				*j = &runtime.Unknown{
					// 此处不设置 TypeMeta，因为它不会经过往返转换。
					Raw:         []byte(`{"apiVersion":"unknown.group/unknown","kind":"Something","someKey":"someValue"}`),
					ContentType: runtime.ContentTypeJSON,
				}
			} else {
				types := []runtime.Object{&testapigroup.Carp{}}
				t := types[c.Rand.Intn(len(types))]
				c.Fill(t)
				*j = t
			}
		},
		func(r *runtime.RawExtension, c randfill.Continue) {
			// 任选一种类型并进行模糊测试。
			types := []runtime.Object{&testapigroup.Carp{}}
			obj := types[c.Rand.Intn(len(types))]
			c.Fill(obj)

			// 用与测试相同的 codec 编码，保证 RawExtension 内容可被解码回同一类型。
			bytes, err := runtime.Encode(apitesting.TestCodec(codecs, v1.SchemeGroupVersion), obj)
			if err != nil {
				panic(fmt.Sprintf("Failed to encode object: %v", err))
			}

			// 设置 RawExtension 的 bytes 字段。
			r.Raw = bytes
		},
	}
}

// testapigroupFuncs 为 Carp 的字段提供合法取值（默认值、枚举集合），
// 避免 fuzz 出 API 校验会拒绝的字符串。
func testapigroupFuncs(codecs runtimeserializer.CodecFactory) []interface{} {
	return []interface{}{
		func(s *testapigroup.CarpSpec, c randfill.Continue) {
			c.FillNoCustom(s)
			// TerminationGracePeriodSeconds 有默认值 30，随机一半概率保留默认。
			ttl := int64(30)
			if c.Bool() {
				ttl = int64(c.Uint32())
			}
			s.TerminationGracePeriodSeconds = &ttl

			// 调度器名称在真实对象中总会被 default 逻辑填充，这里模拟该行为。
			if s.SchedulerName == "" {
				s.SchedulerName = "default-scheduler"
			}
		},
		func(j *testapigroup.CarpPhase, c randfill.Continue) {
			// 只从合法枚举中取值，避免 fuzz 出无法序列化的状态。
			statuses := []testapigroup.CarpPhase{"Pending", "Running", "Succeeded", "Failed", "Unknown"}
			*j = statuses[c.Rand.Intn(len(statuses))]
		},
		func(rp *testapigroup.RestartPolicy, c randfill.Continue) {
			policies := []testapigroup.RestartPolicy{"Always", "Never", "OnFailure"}
			*rp = policies[c.Rand.Intn(len(policies))]
		},
	}
}

// Funcs 返回 testapigroup 的模糊测试函数。
var Funcs = fuzzer.MergeFuzzerFuncs(
	overrideMetaFuncs,
	testapigroupFuncs,
)
