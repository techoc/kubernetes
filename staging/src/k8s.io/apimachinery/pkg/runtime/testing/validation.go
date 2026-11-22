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

package testing

import (
	"context"
	"testing"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

// VersionValidationRunner 是每个版本子测试的回调：t 为子测试，gv 为当前版本，
// versionValidationErrors 为把内部对象转换为该版本后执行声明式校验得到的错误。
type VersionValidationRunner func(t *testing.T, gv string, versionValidationErrors field.ErrorList)

// RunValidationForEachVersion 对给定内部（unversioned）对象的每个版本运行 f 子测试。
// 每个子测试以 GroupVersionKind 命名；每次调用 f 都会传入把内部对象转换为该版本
// 并执行校验后得到的 field.ErrorList。
//
// 只运行自动生成的校验。若需同时测试手写与自动生成校验：
//
//		RunValidationForEachVersion(t, testCase.pod, func(t *testing.T, versionValidationErrors field.ErrorList) {
//		  errs := ValidatePod(testCase.obj) // hand written validation
//	      errs = append(errs, versionValidationErrors...) // generated declarative validation
//		  // Validate that the errors are what was expected for this test case.
//		})
func RunValidationForEachVersion(t *testing.T, scheme *runtime.Scheme, options map[string]bool, unversioned runtime.Object, fn VersionValidationRunner, ignoreConversionErrors bool, subresources ...string) {
	runValidation(t, scheme, options, unversioned, fn, ignoreConversionErrors, subresources...)
}

// RunUpdateValidationForEachVersion 与 RunValidationForEachVersion 类似，但用于更新校验。
func RunUpdateValidationForEachVersion(t *testing.T, scheme *runtime.Scheme, options map[string]bool, unversioned, unversionedOld runtime.Object, fn VersionValidationRunner, ignoreConversionErrors bool, subresources ...string) {
	runUpdateValidation(t, scheme, options, unversioned, unversionedOld, fn, ignoreConversionErrors, subresources...)
}

// runValidation 是 RunValidationForEachVersion 的实现。
func runValidation(t *testing.T, scheme *runtime.Scheme, options map[string]bool, unversioned runtime.Object, fn VersionValidationRunner, ignoreConversionErrors bool, subresources ...string) {
	unversionedGVKs, _, err := scheme.ObjectKinds(unversioned)
	if err != nil {
		t.Fatal(err)
	}
	for _, unversionedGVK := range unversionedGVKs {
		// 跳过非内部版本的对象：调用方应传入内部类型。
		if unversionedGVK.Version != runtime.APIVersionInternal {
			continue
		}
		// 枚举该 GroupKind 已注册的所有版本，逐个转换并校验。
		gvs := scheme.VersionsForGroupKind(unversionedGVK.GroupKind())
		for _, gv := range gvs {
			gvk := gv.WithKind(unversionedGVK.Kind)
			t.Run(gvk.String(), func(t *testing.T) {
				if gvk.Version != runtime.APIVersionInternal { // skip internal
					versioned, err := scheme.New(gvk)
					if err != nil {
						t.Fatal(err)
					}
					err = scheme.Convert(unversioned, versioned, nil)
					// 某些字段在特定版本无法表示，转换失败时可选择跳过而非失败。
					if ignoreConversionErrors && err != nil {
						t.Skipf("Failed to convert object from internal type to %s: %v", gvk.Version, err)
					} else if err != nil {
						t.Fatal(err)
					}
					fn(t, gv.String(), scheme.Validate(context.Background(), options, versioned, subresources...))
				}
			})
		}
	}
}

// runUpdateValidation 是 RunUpdateValidationForEachVersion 的实现，同时转换
// 新旧两个对象以执行更新校验。
func runUpdateValidation(t *testing.T, scheme *runtime.Scheme, options map[string]bool, unversionedNew, unversionedOld runtime.Object, fn VersionValidationRunner, ignoreConversionErrors bool, subresources ...string) {
	unversionedGVKs, _, err := scheme.ObjectKinds(unversionedNew)
	if err != nil {
		t.Fatal(err)
	}
	for _, unversionedGVK := range unversionedGVKs {
		// 跳过非内部版本的对象：调用方应传入内部类型。
		if unversionedGVK.Version != runtime.APIVersionInternal {
			continue
		}
		gvs := scheme.VersionsForGroupKind(unversionedGVK.GroupKind())
		for _, gv := range gvs {
			gvk := gv.WithKind(unversionedGVK.Kind)
			t.Run(gvk.String(), func(t *testing.T) {
				if gvk.Version != runtime.APIVersionInternal { // skip internal
					versionedNew, err := scheme.New(gvk)
					if err != nil {
						t.Fatal(err)
					}
					err = scheme.Convert(unversionedNew, versionedNew, nil)
					if ignoreConversionErrors && err != nil {
						t.Skipf("Failed to convert object from internal type to %s: %v", gvk.Version, err)
					} else if err != nil {
						t.Fatal(err)
					}
					var versionedOld runtime.Object
					// 旧对象允许为 nil（例如模拟创建后的首次更新无旧值）。
					if unversionedOld != nil {
						versionedOld, err = scheme.New(gvk)
						if err != nil {
							t.Fatal(err)
						}

						err = scheme.Convert(unversionedOld, versionedOld, nil)
						if ignoreConversionErrors && err != nil {
							t.Skipf("Failed to convert object from internal type to %s: %v", gvk.Version, err)
						} else if err != nil {
							t.Fatal(err)
						}
					}

					fn(t, gv.String(), scheme.ValidateUpdate(context.Background(), options, versionedNew, versionedOld, subresources...))
				}
			})
		}
	}
}
