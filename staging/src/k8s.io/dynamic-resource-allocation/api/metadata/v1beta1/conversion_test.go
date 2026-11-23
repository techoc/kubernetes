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

package v1beta1

import (
	"testing"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	apitest "k8s.io/dynamic-resource-allocation/api/internal/test"
	"k8s.io/dynamic-resource-allocation/api/metadata"
)

// TestConversionRoundTrip verifies that the automatically generated
// conversion code for the metadata API correctly round-trips between
// v1beta1 and internal. For each non-list type registered in v1beta1,
// a fuzzed object is converted to the internal version and back, and the
// result must be equal to the original.
//
// Note that this only covers conversion code which is called
// while converting the top-level API types. Types embedded
// inside those have their own conversion functions, but those
// are not necessarily called.
//
// TestConversionRoundTrip 验证元数据 API 的生成转换代码能在 v1beta1 与内部版本之间无损往返。
// 测试向同一 scheme 注册两个版本，借助 apitest.ConversionRoundTrip 对每个非 List 类型
// 随机填充对象、正向转换再反向转换，并断言往返结果与原对象语义相等。
func TestConversionRoundTrip(t *testing.T) {
	// 新建 scheme，并分别注册内部版本与 v1beta1 版本的类型。
	scheme := runtime.NewScheme()
	if err := metadata.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}

	// 基于该 scheme 创建随机对象填充器，这里不需要自定义填充函数。
	filler := apitest.NewFiller(t, scheme, nil)

	// 从 v1beta1 转到内部版本再转回，断言语义相等。
	apitest.ConversionRoundTrip(t, scheme, filler,
		SchemeGroupVersion,
		schema.GroupVersion{Group: GroupName, Version: runtime.APIVersionInternal},
	)
}
