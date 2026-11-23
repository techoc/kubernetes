/*
Copyright The Kubernetes Authors.

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

// Package test provides test utilities for DRA API conversion testing.
// Package test 为 DRA API 的转换测试提供公共工具，例如随机填充器与往返转换校验。
package test

import (
	"math/rand"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"

	"k8s.io/apimachinery/pkg/api/apitesting/fuzzer"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	genericfuzzer "k8s.io/apimachinery/pkg/apis/meta/fuzzer"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"sigs.k8s.io/randfill"
)

// FuzzIterations is the number of fuzz iterations to run for each type.
// FuzzIterations 是每个类型执行的模糊测试迭代次数。
const FuzzIterations = 100

// NewFiller creates a new randfill.Filler for fuzzing objects.
// NewFiller 创建一个新的 randfill.Filler，用随机种子与 scheme 构造模糊填充器。
func NewFiller(t *testing.T, scheme *runtime.Scheme, customFuncs fuzzer.FuzzerFuncs) *randfill.Filler {
	// 生成随机种子并写入日志，便于失败时复现。
	seed := rand.Int63()
	t.Logf("Using seed: %d", seed)
	// 基于 scheme 构造编解码工厂。
	codecs := serializer.NewCodecFactory(scheme)
	// 合并通用与自定义填充函数后创建填充器。
	return fuzzer.FuzzerFor(
		fuzzer.MergeFuzzerFuncs(genericfuzzer.Funcs, customFuncs),
		rand.NewSource(seed),
		codecs,
	)
}

// ConversionRoundTrip tests that all non-list types in srcGV can be converted
// to dstGV and back without loss of information.
// ConversionRoundTrip 校验 srcGV 中所有非 List 类型都能转换为 dstGV 再转回且不丢失信息。
func ConversionRoundTrip(t *testing.T, scheme *runtime.Scheme, filler *randfill.Filler, srcGV, dstGV schema.GroupVersion) {
	t.Helper()

	// 统计实际被测的类型数量。
	tested := 0
	for kind := range scheme.KnownTypes(srcGV) {
		// 跳过 List 类型，只测普通资源类型。
		if strings.HasSuffix(kind, "List") {
			continue
		}

		// 构造源与目标的 GroupVersionKind。
		srcGVK := srcGV.WithKind(kind)
		dstGVK := dstGV.WithKind(kind)
		// 目标版本中不存在该 Kind 时跳过。
		if _, err := scheme.New(dstGVK); err != nil {
			// Kind does not exist in the destination version.
			// 目标版本缺少该类型，跳过。
			continue
		}

		// 记录被测类型数并按 kind 建子测试。
		tested++
		t.Run(kind, func(t *testing.T) {
			// 每个类型执行固定次数的模糊测试。
			for i := range FuzzIterations {
				// Create and fuzz source object.
				// 创建源对象并随机填充。
				srcObj, err := scheme.New(srcGVK)
				if err != nil {
					t.Fatal(err)
				}
				filler.Fill(srcObj)

				// Convert source -> destination.
				// 将源对象转换为目标对象。
				dstObj, err := scheme.New(dstGVK)
				if err != nil {
					t.Fatal(err)
				}
				if err := scheme.Convert(srcObj, dstObj, nil); err != nil {
					t.Fatalf("iteration %d: %v -> %v: %v", i, srcGVK, dstGVK, err)
				}

				// Convert destination -> source (round-trip).
				// 再转回源类型，完成往返。
				roundTripped, err := scheme.New(srcGVK)
				if err != nil {
					t.Fatal(err)
				}
				if err := scheme.Convert(dstObj, roundTripped, nil); err != nil {
					t.Fatalf("iteration %d: %v -> %v: %v", i, dstGVK, srcGVK, err)
				}

				// The round-tripped object must be equal to the original.
				// Use Semantic.DeepEqual which treats nil and empty
				// slices/maps as equivalent.
				// 断言往返后的对象与原始对象语义相等；Semantic.DeepEqual 把
				// nil 与空切片/映射视为等价。
				if !apiequality.Semantic.DeepEqual(srcObj, roundTripped) {
					t.Errorf("iteration %d: round-trip %v -> %v -> %v failed\ndiff (-want +got):\n%s",
						i, srcGVK, dstGVK, srcGVK,
						cmp.Diff(srcObj, roundTripped))
				}
			}
		})
	}

	// 没有任何类型被测到说明 scheme 配置有误，直接失败。
	if tested == 0 {
		t.Fatal("no types were tested")
	}
	// 输出被测类型数量，便于了解覆盖范围。
	t.Logf("tested %d types for %v <-> %v", tested, srcGV, dstGV)
}
