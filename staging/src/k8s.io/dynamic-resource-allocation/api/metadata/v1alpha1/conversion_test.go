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

package v1alpha1

import (
	"testing"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	apitest "k8s.io/dynamic-resource-allocation/api/internal/test"
	"k8s.io/dynamic-resource-allocation/api/metadata"
)

// TestConversionRoundTrip 验证自动生成的元数据 API 转换代码能在 v1alpha1 与内部版本之间正确往返。
// 测试意图：对每个在 v1alpha1 注册的非列表类型，把 fuzz 生成的对象转换到内部版本再转回来，
// 结果必须与原始对象相等，从而保证转换不丢失信息。
// 输入：包含内部类型和 v1alpha1 类型的 scheme、fuzzer 填充器，以及源/目标组版本。
// 注意：这只覆盖转换顶层 API 类型时被调用的转换代码；内嵌类型虽然也有各自的转换函数，
// 但不一定会在此测试中被调用。
func TestConversionRoundTrip(t *testing.T) {
	// 创建 scheme 并分别注册内部类型和 v1alpha1 版本类型，注册失败则直接终止测试。
	scheme := runtime.NewScheme()
	if err := metadata.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}

	// 基于该 scheme 创建填充器，用于生成随机（fuzzed）测试对象。
	filler := apitest.NewFiller(t, scheme, nil)

	// 在 v1alpha1 组版本与内部组版本之间执行往返转换测试。
	apitest.ConversionRoundTrip(t, scheme, filler,
		SchemeGroupVersion,
		schema.GroupVersion{Group: GroupName, Version: runtime.APIVersionInternal},
	)
}
