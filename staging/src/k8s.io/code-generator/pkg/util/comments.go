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

// Package util 汇集各代码生成器共用的注释解析辅助函数。
package util

import (
	"fmt"

	"k8s.io/gengo/v2"
)

// ExtractCommentTagsWithoutArguments 从注释中解析元数据标签。marker 必须足够独特，
// 只标记需要的标签；否则调用方需自行区分不想要的标签。
//
// tagNames 指定要提取的标签名。为 nil 或空时，所有匹配 marker 的行都会被解析；
// 否则只接受 marker 加指定标签开头的行。这样即使公共 marker 匹配到不符合语法的
// 旧注释，也能安全过滤。
//
// 本函数识别以下形式的输入行：
//   - 'marker' + "key=value"
//   - 'marker' + "key()=value"
//   - 'marker' + "key(arg)=value"
//
// 带参数的标签不受支持：只接受 "key=value" 或 "key()=value"，
// 遇到 "key(arg)=value" 会报错。
//
// 值可省略，省略时结果中的值为空串。
//
// 标签所在行末尾允许带行尾注释。
//
// 返回 map 的键是不带参数的标签名。
//
// 同一标签可出现多次，所有取值都会返回；map 中若存在某个键，其值切片至少含一个元素。
//
// 例如 marker 传 "+"，注释中包含以下行：
//
//	+foo=val1   // foo
//	+bar
//	+foo=val2   // also foo
//	+foo()=val3 // still foo
//	+baz="qux"
//
// 则函数返回：
//
//	map[string][]string{"foo":{"val1", "val2", "val3"}, "bar": {""}, "baz": {`"qux"`}}
func ExtractCommentTagsWithoutArguments(marker string, tagNames []string, lines []string) (map[string][]string, error) {
	functionStyleTags, err := gengo.ExtractFunctionStyleCommentTags(marker, tagNames, lines)
	if err != nil {
		return nil, err
	}

	out := make(map[string][]string)
	for tagName, tags := range functionStyleTags {
		values := make([]string, 0)

		// 本函数只接受无参数标签；"key(arg)=value" 形式属于另一类语义，
		// 混入会改变值解析规则，因此直接报错而不是丢弃参数。
		for _, tag := range tags {
			if tag.Args == nil {
				values = append(values, tag.Value)
			} else {
				return nil, fmt.Errorf(`failed to parse tag %s: expected no arguments, found "%s"`, tag, tag.Args[0])
			}
		}

		// 空值列表不写入 map，调用方可以据此判断“标签是否存在”。
		if len(values) > 0 {
			out[tagName] = values
		}
	}

	return out, nil
}
