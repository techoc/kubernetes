/*
Copyright 2017 The Kubernetes Authors.

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

// Package fuzzer 提供用于给 Kubernetes API 对象填充随机数据的模糊测试（fuzz）函数。
// 这些函数主要供编解码往返（round-trip）测试使用：在序列化前后生成覆盖多样、可复现的随机对象，
// 从而暴露编解码过程中丢失字段或行为不一致的问题。
package fuzzer

import (
	"encoding/json"
	"fmt"
	"math/rand"

	"sigs.k8s.io/randfill"

	"k8s.io/apimachinery/pkg/runtime"
	runtimeserializer "k8s.io/apimachinery/pkg/runtime/serializer"
	kjson "k8s.io/apimachinery/pkg/util/json"
)

// FuzzerFuncs 返回一组 func(*SomeType, c randfill.Continue) 形式的填充函数，
// 每个函数负责为某一种具体的 API 类型生成随机数据。
type FuzzerFuncs func(codecs runtimeserializer.CodecFactory) []interface{}

// FuzzerFor 构建一个面向指定版本的随机填充器（Filler）：
// 借助传入的随机源对 API 对象做随机填充，用于在测试中产生多样的输入数据。
func FuzzerFor(funcs FuzzerFuncs, src rand.Source, codecs runtimeserializer.CodecFactory) *randfill.Filler {
	// NilChance(.5) 让指针/可选字段有一半概率填充为 nil，NumElements(0, 1) 限制切片长度为 0 或 1，
	// 目的是在尽量少膨胀对象体积的前提下，同时覆盖"字段缺失"与"字段存在"两类边界。
	f := randfill.New().NilChance(.5).NumElements(0, 1)
	// 仅在调用方提供了自定义随机源时才替换默认源，保证测试可用固定序列复现填充结果。
	if src != nil {
		f.RandSource(src)
	}
	f.Funcs(funcs(codecs)...)
	return f
}

// MergeFuzzerFuncs 将多组 FuzzerFuncs 合并为一组：按传入顺序拼接各组函数，
// 目标类型相同的填充函数最终由填充器按顺序注册，靠后注册的会覆盖靠前的，
// 从而让调用方以"基础集 + 覆盖集"的方式叠加多个来源的填充逻辑。
func MergeFuzzerFuncs(funcs ...FuzzerFuncs) FuzzerFuncs {
	return FuzzerFuncs(func(codecs runtimeserializer.CodecFactory) []interface{} {
		result := []interface{}{}
		// 跳过 nil 分组，避免把空函数列表当作有效内容追加到结果中。
		for _, f := range funcs {
			if f != nil {
				result = append(result, f(codecs)...)
			}
		}
		return result
	})
}

// NormalizeJSONRawExtension 将 RawExtension 中已可解析的原始 JSON 规范化为按键名排序的形式，
// 保证 JSON 在 RawExtension -> JSON -> map[string]interface{} -> JSON 的往返过程中逐字节保持一致。
func NormalizeJSONRawExtension(ext *runtime.RawExtension) {
	if json.Valid(ext.Raw) {
		// RawExtension 直接编码时按结构体字段索引顺序输出字段，而 map[string]interface{} 编码 JSON
		// 时按 map 键的字典序输出。这里先反序列化为通用对象再重新序列化，对字段排序，
		// 使上述往返路径产生的 JSON 完全一致，避免测试因键序差异产生假阳性。
		var u any
		err := kjson.Unmarshal(ext.Raw, &u)
		if err != nil {
			// 输入在 json.Valid 检查中已经通过，走到这里说明处理过程中出现意外错误，
			// 属于不可恢复的测试缺陷，直接 panic 而不是静默产出不一致的数据。
			panic(fmt.Sprintf("Failed to encode object: %v", err))
		}
		ext.Raw, err = kjson.Marshal(&u)
		if err != nil {
			panic(fmt.Sprintf("Failed to encode object: %v", err))
		}
	}
}
