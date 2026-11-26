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

package modes

import (
	"encoding/json"
	"errors"
	"io"

	kjson "sigs.k8s.io/json"
)

// TranscodeFunc 把函数适配为 Transcode 接口，便于在模式表中内联定义转码器。
type TranscodeFunc func(dst io.Writer, src io.Reader) error

// Transcode 调用底层函数完成转码。
func (f TranscodeFunc) Transcode(dst io.Writer, src io.Reader) error {
	return f(dst, src)
}

// TranscodeFromJSON 把 JSON 输入转换为 CBOR 写出。
// 先完整解码为 any，再交给 CBOR 编码器，因此输出是规范的 CBOR 形式。
func TranscodeFromJSON(dst io.Writer, src io.Reader) error {
	var tmp any
	dec := kjson.NewDecoderCaseSensitivePreserveInts(src)
	if err := dec.Decode(&tmp); err != nil {
		return err
	}
	// 第二次 Decode 必须立刻遇到 EOF，否则说明输入里有多余数据（如两个 JSON 值）。
	if err := dec.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return errors.New("extraneous data")
	}

	return encode.MarshalTo(tmp, dst)
}

// TranscodeToJSON 把 CBOR 输入转换为 JSON 写出。
func TranscodeToJSON(dst io.Writer, src io.Reader) error {
	var tmp any
	dec := decode.NewDecoder(src)
	if err := dec.Decode(&tmp); err != nil {
		return err
	}
	// 与 TranscodeFromJSON 相同：拒绝尾随的多余数据。
	if err := dec.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return errors.New("extraneous data")
	}

	// 用 Encoder 代替 Marshal，省去 Marshal 额外分配的 []byte。Encode 与 Marshal 不同，
	// 会在末尾追加换行以分隔不能自定界的连续 JSON 编码值（如数字）；该换行由 suppressor
	// 去掉，以免依赖所有 json.Unmarshaler 实现都容忍尾随空白。
	enc := json.NewEncoder(&trailingLinefeedSuppressor{delegate: dst})
	enc.SetIndent("", "")
	return enc.Encode(tmp)
}

// trailingLinefeedSuppressor 是一个包装其他 io.Writer 的 io.Writer，当最后一次 Write 写入的
// 最后一个字节是换行时，将其抑制。它只抑制“最后一次 Write 的最后一个换行”，若之后还有写入，会把缓存的换行补发出去，
// 从而保证多次 Encode 到同一 writer 时各值仍被正确分隔。
type trailingLinefeedSuppressor struct {
	lf       bool
	delegate io.Writer
}

// Write 实现 io.Writer：延迟写出末尾换行，直到确认它确实是整体输出的最后一个字节。
func (w *trailingLinefeedSuppressor) Write(p []byte) (int, error) {
	if len(p) == 0 {
		// 空写入时不要冲刷缓存的换行符。
		return 0, nil
	}

	if w.lf {
		// 上一次写入末尾的换行符被缓存了下来，而那并不是最后一次 Write，
		// 因此继续之前先把它冲刷出去。
		n, err := w.delegate.Write([]byte{'\n'})
		if n > 0 {
			w.lf = false
		}
		if err != nil {
			return 0, err
		}
	}

	if p[len(p)-1] != '\n' {
		return w.delegate.Write(p)
	}

	p = p[:len(p)-1]

	if len(p) == 0 { // []byte{'\n'}
		// 整个写入只有一个换行，先缓存，等后续写入决定是否真正输出。
		w.lf = true
		return 1, nil
	}

	n, err := w.delegate.Write(p)
	if n == len(p) {
		// 末尾换行符之前的内容都已冲刷完毕，吃掉这个换行符。
		w.lf = true
		n++
	}
	return n, err
}
