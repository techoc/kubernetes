/*
Copyright 2014 The Kubernetes Authors.

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

package recognizer

import (
	"fmt"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// RecognizingDecoder 是能自行判断输入格式的解码器。
type RecognizingDecoder interface {
	runtime.Decoder
	// RecognizesData 在输入属于该解码器时返回 true；读取失败或有歧义时返回错误。
	// 数据不足以判断时返回 unknown=true。解码器可以按需读取调用方提供的 peek
	// 数据；peek 返回 EOF 可能意味着输入结束，也可能只是缓冲区读尽，此时
	// 识别器应给出当前最佳猜测。
	//
	// 返回 ok=true 表示确认匹配；unknown=true 表示数据不足以判定；err 非 nil 表示读取失败。
	RecognizesData(peek []byte) (ok, unknown bool, err error)
}

// NewDecoder 创建一个按以下优先级依次尝试多个解码器的解码器：
//
// 1. 实现 RecognizingDecoder 且能识别数据的解码器
// 2. 其它所有解码器，以及返回 unknown=true 的解码器。
//
// 同一优先级内保持传入构造器的顺序。
//
// 即：先按识别器的确认结果选择，无法确认时再按传入顺序逐个尝试，保证行为可预测。
func NewDecoder(decoders ...runtime.Decoder) runtime.Decoder {
	return &decoder{
		decoders: decoders,
	}
}

// decoder 按优先级组合多个解码器。
type decoder struct {
	decoders []runtime.Decoder
}

// 编译期断言：组合解码器本身也是一个识别器。
var _ RecognizingDecoder = &decoder{}

// RecognizesData 汇总所有子识别器的判断：任一确认匹配即返回 true；
// 否则返回是否出现过 unknown，以及最后一个错误。
func (d *decoder) RecognizesData(data []byte) (bool, bool, error) {
	var (
		lastErr    error
		anyUnknown bool
	)
	for _, r := range d.decoders {
		switch t := r.(type) {
		case RecognizingDecoder:
			ok, unknown, err := t.RecognizesData(data)
			if err != nil {
				lastErr = err
				continue
			}
			anyUnknown = anyUnknown || unknown
			if !ok {
				continue
			}
			return true, false, nil
		}
	}
	return false, anyUnknown, lastErr
}

// Decode 先询问各识别器，命中即解码；未能确认的解码器留待第二轮按顺序尝试。
func (d *decoder) Decode(data []byte, gvk *schema.GroupVersionKind, into runtime.Object) (runtime.Object, *schema.GroupVersionKind, error) {
	var (
		lastErr error
		skipped []runtime.Decoder
	)

	// 先试识别器，记下稍后还需尝试的解码器。
	for _, r := range d.decoders {
		switch t := r.(type) {
		case RecognizingDecoder:
			ok, unknown, err := t.RecognizesData(data)
			if err != nil {
				lastErr = err
				continue
			}
			if unknown {
				// 数据不足，稍后按顺序再试。
				skipped = append(skipped, t)
				continue
			}
			if !ok {
				continue
			}
			return r.Decode(data, gvk, into)
		default:
			// 不支持识别接口的解码器只能进入第二轮。
			skipped = append(skipped, t)
		}
	}

	// 再试返回 unknown 或未识别出数据的识别器。
	for _, r := range skipped {
		out, actual, err := r.Decode(data, gvk, into)
		if err != nil {
			// 解码器返回了对象，即使报错是严格解码错误（未知字段或
			// 重复字段），也视为该识别器理解了对象。
			// 严格解码错误说明格式本身已识别，只是内容有瑕疵，仍返回对象与错误。
			if out == nil || !runtime.IsStrictDecodingError(err) {
				lastErr = err
				continue
			}
		}
		return out, actual, err
	}

	if lastErr == nil {
		lastErr = fmt.Errorf("no serialization format matched the provided data")
	}
	return nil, nil, lastErr
}
