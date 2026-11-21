/*
Copyright 2025 The Kubernetes Authors.

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
	"math/rand"
	"sort"
	"testing"

	"k8s.io/apimachinery/pkg/api/apitesting/fuzzer"
	"k8s.io/apimachinery/pkg/api/apitesting/roundtrip"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/kubernetes/pkg/api/legacyscheme"
	nodevalidation "k8s.io/kubernetes/pkg/apis/node/validation"
	resourcevalidation "k8s.io/kubernetes/pkg/apis/resource/validation"
)

// TestVersionedValidationByFuzzing 对所有带声明式校验的 GVK 做随机 fuzz，
// 验证各版本的校验错误是否等价。通过自动扫描 scheme，新增 group/version 无需手工登记即可覆盖。
func TestVersionedValidationByFuzzing(t *testing.T) {
	// 扫描所有对外版本，找出注册了校验函数的 GVK，使新增版本自动纳入测试。
	var gvks []schema.GroupVersionKind
	for gvk := range legacyscheme.Scheme.AllKnownTypes() {
		// 内部版本无声明式校验，跳过。
		if gvk.Version == runtime.APIVersionInternal {
			continue
		}
		obj, err := legacyscheme.Scheme.New(gvk)
		if err != nil {
			continue
		}
		// 只为注册了校验函数的类型做等价性测试。
		if !legacyscheme.Scheme.HasValidationFunc(obj) {
			continue
		}
		gvks = append(gvks, gvk)
	}
	// 排序后子测试执行顺序稳定，便于复现失败。
	sort.Slice(gvks, func(i, j int) bool { return gvks[i].String() < gvks[j].String() })

	// subresourceOnlyKinds 把只有子资源校验的 Kind 映射到对应子资源（如 Scale），
	// 因为这类 Kind 没有根级校验。用 Kind 作键是因为同一个 Kind 会注册在多个组下
	// （autoscaling、apps）。其他 Kind 默认校验根资源（""），其校验已覆盖子资源。
	subresourceOnlyKinds := map[string]string{
		"Scale": "scale",
	}

	// 完整 fuzz 轮次开销较大，这里缩减到十分之一。
	fuzzIters := *roundtrip.FuzzIters / 10 // TODO: Find a better way to manage test running time
	f := fuzzer.FuzzerFor(FuzzerFuncs, rand.NewSource(rand.Int63()), legacyscheme.Codecs)

	for _, gvk := range gvks {
		t.Run(gvk.String(), func(t *testing.T) {
			for range fuzzIters {
				obj, err := legacyscheme.Scheme.New(gvk)
				if err != nil {
					t.Fatalf("could not create a %v: %s", gvk.Kind, err)
				}

				// 特殊 Kind 要走子资源校验路径，其余使用默认的根资源校验。
				subresource := ""
				if specific, ok := subresourceOnlyKinds[gvk.Kind]; ok {
					subresource = specific
				}

				var opts []ValidationTestConfig
				// TODO(API group level configuration): Consider configuring normalization rules at the
				// API group level to avoid potential collisions when multiple rule sets are combined.
				// This would allow each API group to register its own normalization rules independently.
				// 合并各 group 的归一化规则：版本间字段路径可能不同，需先归一化再比较错误。
				allRules := append([]field.NormalizationRule{}, resourcevalidation.ResourceNormalizationRules...)
				allRules = append(allRules, nodevalidation.NodeNormalizationRules...)
				opts = append(opts, WithNormalizationRules(allRules...), WithFuzzer(f))

				if subresource != "" {
					opts = append(opts, WithSubResources(subresource))
				}

				// 创建场景：old 传 nil，只校验新对象。
				VerifyVersionedValidationEquivalence(t, obj, nil, opts...)

				old, err := legacyscheme.Scheme.New(gvk)
				if err != nil {
					t.Fatalf("could not create a %v: %s", gvk.Kind, err)
				}

				// 更新场景：同时校验新旧对象，确保更新路径上的版本校验也等价。
				VerifyVersionedValidationEquivalence(t, obj, old, opts...)
			}
		})
	}
}
