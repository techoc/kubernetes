/*
Copyright 2022 The Kubernetes Authors.

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

package serializer

import (
	"crypto/rand"
	"io/ioutil"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	testapigroupv1 "k8s.io/apimachinery/pkg/apis/testapigroup/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer/protobuf"
)

// BenchmarkProtobufEncoder 衡量常规 protobuf 序列化路径（每次编码自行分配缓冲）。
func BenchmarkProtobufEncoder(b *testing.B) {
	benchmarkEncodeFor(b, protobuf.NewSerializer(nil, nil))
}

// BenchmarkProtobufEncodeWithAllocator 衡量复用 Allocator 的编码路径，
// 用于对比 EncodeWithAllocator 带来的分配收益。
func BenchmarkProtobufEncodeWithAllocator(b *testing.B) {
	benchmarkEncodeWithAllocatorFor(b, protobuf.NewSerializer(nil, nil))
}

// BenchmarkRawProtobufEncoder 衡量不带 runtime.Unknown 包装的裸 protobuf 编码路径。
func BenchmarkRawProtobufEncoder(b *testing.B) {
	benchmarkEncodeFor(b, protobuf.NewRawSerializer(nil, nil))
}

// BenchmarkRawProtobufEncodeWithAllocator 衡量裸 protobuf + Allocator 的编码路径。
func BenchmarkRawProtobufEncodeWithAllocator(b *testing.B) {
	benchmarkEncodeWithAllocatorFor(b, protobuf.NewRawSerializer(nil, nil))
}

// benchmarkEncodeFor 对每个测试用例运行普通 Encode，并输出到 Discard。
func benchmarkEncodeFor(b *testing.B, target runtime.Encoder) {
	for _, tc := range benchTestCases() {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for n := 0; n < b.N; n++ {
				err := target.Encode(tc.obj, ioutil.Discard)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// benchmarkEncodeWithAllocatorFor 对每个测试用例运行 EncodeWithAllocator。
//
// Allocator 在循环外创建并复用，这正是该 API 的使用方式：跨多次编码复用内存。
func benchmarkEncodeWithAllocatorFor(b *testing.B, target runtime.EncoderWithAllocator) {
	for _, tc := range benchTestCases() {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			allocator := &runtime.Allocator{}
			for n := 0; n < b.N; n++ {
				err := target.EncodeWithAllocator(tc.obj, ioutil.Discard, allocator)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// benchTestCase 是一个带名字的编码基准用例。
type benchTestCase struct {
	name string
	obj  runtime.Object
}

// benchTestCases 返回 1kB 到 1MB 四档载荷，覆盖不同缓冲区增长行为。
func benchTestCases() []benchTestCase {
	return []benchTestCase{
		{
			name: "an obj with 1kB payload",
			obj: func() runtime.Object {
				carpPayload := make([]byte, 1000) // 1 kB
				if _, err := rand.Read(carpPayload); err != nil {
					panic(err)
				}
				return carpWithPayload(carpPayload)
			}(),
		},
		{
			name: "an obj with 10kB payload",
			obj: func() runtime.Object {
				carpPayload := make([]byte, 10000) // 10 kB
				if _, err := rand.Read(carpPayload); err != nil {
					panic(err)
				}
				return carpWithPayload(carpPayload)
			}(),
		},
		{
			name: "an obj with 100kB payload",
			obj: func() runtime.Object {
				carpPayload := make([]byte, 100000) // 100 kB
				if _, err := rand.Read(carpPayload); err != nil {
					panic(err)
				}
				return carpWithPayload(carpPayload)
			}(),
		},
		{
			name: "an obj with 1MB payload",
			obj: func() runtime.Object {
				carpPayload := make([]byte, 1000000) // 1 MB
				if _, err := rand.Read(carpPayload); err != nil {
					panic(err)
				}
				return carpWithPayload(carpPayload)
			}(),
		},
	}
}

// carpWithPayload 构造一个包含指定大小随机载荷的 Carp 测试对象。
//
// 载荷放在 NodeSelector 的字符串值中，迫使序列化器处理大字段；
// 随机内容可避免压缩类编码产生失真的基准结果。
func carpWithPayload(carpPayload []byte) *testapigroupv1.Carp {
	gvk := &schema.GroupVersionKind{Group: "group", Version: "version", Kind: "Carp"}
	return &testapigroupv1.Carp{
		TypeMeta: metav1.TypeMeta{APIVersion: gvk.GroupVersion().String(), Kind: gvk.Kind},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "name",
			Namespace: "namespace",
		},
		Spec: testapigroupv1.CarpSpec{
			Subdomain:    "carp.k8s.io",
			NodeSelector: map[string]string{"payload": string(carpPayload)},
		},
	}
}
