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

package api

import (
	"testing"

	"github.com/google/go-cmp/cmp"

	resourceapi "k8s.io/api/resource/v1"
	"k8s.io/apimachinery/pkg/api/apitesting/fuzzer"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	apitest "k8s.io/dynamic-resource-allocation/api/internal/test"
	"sigs.k8s.io/randfill"
)

// v1FillFuncs 返回 v1 类型所需的自定义填充函数：默认随机填充生成的指针字段
// 在内部 api 类型中会经历指针到值的转换，导致往返转换无法还原。
func v1FillFuncs(codecs serializer.CodecFactory) []interface{} {
	return []interface{}{
		// 内部 api 类型用 bool 表示 AllNodes；nil 经 false 再转回 &false 后无法还原，
		// 因此填充后若为 nil 就分配一个非 nil 的指针。
		func(s *resourceapi.ResourceSliceSpec, c randfill.Continue) {
			// 先按默认规则填充，再单独修正 AllNodes。
			c.FillNoCustom(s)
			// 保证 AllNodes 非 nil，避免往返丢失信息。
			if s.AllNodes == nil {
				s.AllNodes = new(bool)
			}
		},
		// 内部 api 类型用 bool 表示 BindsToNode；nil 经 false 再转回 &false 后无法还原，
		// 因此填充后若为 nil 就分配一个非 nil 的指针。
		func(d *resourceapi.Device, c randfill.Continue) {
			// 先按默认规则填充，再单独修正 BindsToNode。
			c.FillNoCustom(d)
			// 保证 BindsToNode 非 nil，避免往返丢失信息。
			if d.BindsToNode == nil {
				d.BindsToNode = new(bool)
			}
		},
	}
}

// TestConversionRoundTrip 验证内部 DRA API 的转换代码能在 v1 与内部 api 类型之间正确往返。
// 测试只覆盖 v1 -> api -> v1 方向，该方向应无损；
// 反方向因内部表示含额外字段会丢失信息，且实际不会使用，故不测试。
func TestConversionRoundTrip(t *testing.T) {
	// 新建一个空 scheme，并分别注册 v1 与外部 api 类型。
	scheme := runtime.NewScheme()
	if err := resourceapi.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}

	// 构造随机填充器，并合并自定义填充函数以规避无法往返的字段。
	filler := apitest.NewFiller(t, scheme, fuzzer.FuzzerFuncs(v1FillFuncs))

	// 定义待测类型清单：v1Type 创建 v1 输入对象，apiNew 创建对应的内部 api 对象。
	testCases := []struct {
		name   string
		v1Type func() runtime.Object
		apiNew func() interface{}
	}{
		// 仅覆盖 ResourceSlice 这一种类型。
		{
			name:   "ResourceSlice",
			v1Type: func() runtime.Object { return &resourceapi.ResourceSlice{} },
			apiNew: func() interface{} { return &ResourceSlice{} },
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// 每个类型执行固定次数的模糊测试。
			for i := range apitest.FuzzIterations {
				// 创建一个 v1 对象并随机填充作为输入。
				v1Obj := tc.v1Type()
				filler.Fill(v1Obj)

				// 将 v1 对象转换为内部 api 对象。
				apiObj := tc.apiNew()
				if err := scheme.Convert(v1Obj, apiObj, nil); err != nil {
					t.Fatalf("iteration %d: v1 -> api: %v", i, err)
				}

				// 再转回 v1，完成往返。
				roundTripped := tc.v1Type()
				if err := scheme.Convert(apiObj, roundTripped, nil); err != nil {
					t.Fatalf("iteration %d: api -> v1: %v", i, err)
				}

				// 断言往返后的对象与原始对象语义相等，失败时输出差异。
				if !apiequality.Semantic.DeepEqual(v1Obj, roundTripped) {
					t.Errorf("iteration %d: round-trip v1 -> api -> v1 failed\ndiff (-want +got):\n%s",
						i, cmp.Diff(v1Obj, roundTripped))
				}
			}
		})
	}
}
