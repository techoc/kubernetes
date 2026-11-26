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

// Package testscheme 提供 scheme 实现与测试工具，供 validation-gen 的 output_tests 使用。
//
// 输出测试若要使用该 scheme，应放在独立的 Go 包中：该包需启用 validation-gen
// 与本测试 scheme，并用 'testscheme.New()' 声明 'localSchemeBuilder'。例如：
//
//	// +k8s:validation-gen-scheme-registry=k8s.io/code-generator/cmd/validation-gen/testscheme.Scheme
//	// +k8s:validation-gen
//	package example
//	import "k8s.io/code-generator/cmd/validation-gen/testscheme"
//	var localSchemeBuilder = testscheme.New()
//
// 有这些声明后，validation-gen 就会为该包中可编译的类型生成 `zz_generated.validations.go`。
//
// 启用 scheme 后，输出测试既可手写测试代码，也可使用生成的测试夹具。
//
// 手写测试时，testscheme 提供工具来构造值并断言校验返回预期错误。
// 注意：一旦对 `ValidationTester` 调用 `OldValue()` 或 `OldValueFuzzed()`，
// 后续的校验调用会隐式按 update 语义执行。
//
//	 func Test(t *testing.T) {
//		  st := localSchemeBuilder.Test(t)
//		  st.Value(&T1{
//			E0:  "x",
//			PE0: pointer.To(E0("y")),
//		  }).ExpectInvalid(
//			field.NotSupported(field.NewPath("e0"), "x", []string{EnumValue1, EnumValue2}),
//			field.NotSupported(field.NewPath("pe0"), "y", []string{EnumValue1, EnumValue2}))}
//	 }
//
// 也可以启用测试夹具。例如：
//
//	// ...
//	// +k8s:validation-gen-test-fixture=validateFalse
//	// package example
//
// 启用夹具后，会按指定夹具的行为，为包内所有注册类型生成 `zz_generated.validations_test.go`。
//
// 可用夹具：
//
// `validateFalse`：对每个注册类型执行校验，收集所有 `validateFalse` 校验错误。例如：
//
//	type T1 struct {
//		// +k8s:validateFalse="field T1.S"
//		S string `json:"s"`
//		// +k8s:validateFalse="field T1.T"
//		T T2 `json:"t"`
//	}
//
// 上面的 `example.T1` 有两个受校验字段：`s` 与 't'。字段名取自 Go "json" tag，
// 每个字段通过 `+k8s:validateFalse=<identifier>` 指定错误标识。
//
// `validateFalse` 夹具会用 fuzz 数据构造 `example.T1` 实例并校验，
// 然后按字段名归类错误，JSON 形如：
//
//	{
//	  "*example.T1": {
//	    "s": [
//	      "field T1.S"
//	    ],
//	    "t": [
//	      "field T1.T"
//	    ]
//	  }
//	}
//
// 错误数据按类型名为每个注册类型建立一个对象，其中 `+k8s:validateFalse` 的错误
// 按字段路径归类，错误标识汇总为列表。
//
// 该数据会与同包 `testdata/validate-false.json` 中的预期结果比较，差异即测试失败。
//
// 运行测试时设置环境变量 `UPDATE_VALIDATION_GEN_FIXTURE_DATA=true`，
// 可自动生成 `testdata/validate-false.json`。
//
// 生成 `testdata/validate-false.json` 的测试作者应在提交前确认文件内容正确。
//
// fuzz 数据用固定种子伪随机生成：所有可空字段都会被赋值，
// 每个 map 一个条目、每个 slice 一个元素。
//
// 中文要点：本包提供 validation-gen 的测试脚手架。被测包需启用
// 指令 +k8s:validation-gen-scheme-registry 并定义 testscheme.New() 的 localSchemeBuilder，
// 生成器才会产出 zz_generated.validations.go；测试既可用 ValidationTester 手写，
// 也可通过 +k8s:validation-gen-test-fixture 由生成器产出夹具。
// 同一包内先调用 OldValue()/OldValueFuzzed() 会使其后的校验自动按 update 语义执行。
package testscheme
