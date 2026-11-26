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

// +k8s:validation-gen=*

// 这是一个测试包，用于演示对不在 gengo 参数中的类型的引用。
// 即使该包声称带有校验，它也不在生成 output_tests 所用的参数范围内，
// 因此生成代码不应深入这些类型。
// +k8s:validation-gen-nolint
// 上述三种类型都带有 +k8s:validateFalse，但本包不在生成参数中，
// 因此生成代码不应下探到这里的类型。
package other

// +k8s:validateFalse="you should not see this outside of this pkg"
type StringType string

// +k8s:validateFalse="you should not see this outside of this pkg"
type IntType int

// +k8s:validateFalse="you should not see this outside of this pkg"
type StructType struct {
	// +k8s:validateFalse="you should not see this outside of this pkg"
	StringField string `json:"stringField"`
}
