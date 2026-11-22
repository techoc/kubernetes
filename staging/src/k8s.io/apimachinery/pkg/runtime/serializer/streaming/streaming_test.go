/*
Copyright 2016 The Kubernetes Authors.

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

package streaming

import (
	"bytes"
	"io"

	"testing"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/framer"
)

// fakeDecoder 记录最近一次收到的帧数据，并按预设返回对象/错误，用于验证流式解码器传帧正确。
type fakeDecoder struct {
	got []byte
	obj runtime.Object
	err error
}

// Decode 保存收到的数据并原样返回预设结果。
func (d *fakeDecoder) Decode(data []byte, gvk *schema.GroupVersionKind, into runtime.Object) (runtime.Object, *schema.GroupVersionKind, error) {
	d.got = data
	return d.obj, nil, d.err
}

// TestEmptyDecoder 验证输入流为空时，解码返回 io.EOF 而不是其他错误。
func TestEmptyDecoder(t *testing.T) {
	buf := bytes.NewBuffer([]byte{})
	d := &fakeDecoder{}
	_, _, err := NewDecoder(io.NopCloser(buf), d).Decode(nil, nil)
	if err != io.EOF {
		t.Fatal(err)
	}
}

// TestDecoder 用长度前缀帧写入四帧（含一帧 17MB 超过默认上限），验证：
// 正常帧逐帧解码、超大帧返回 ErrObjectTooLarge 且不污染解码器状态、流结束返回 io.EOF。
func TestDecoder(t *testing.T) {
	frames := [][]byte{
		make([]byte, 1025),
		make([]byte, 1024*5),
		make([]byte, 1024*1024*17),
		make([]byte, 1025),
	}
	pr, pw := io.Pipe()
	fw := framer.NewLengthDelimitedFrameWriter(pw)
	go func() {
		for i := range frames {
			fw.Write(frames[i])
		}
		pw.Close()
	}()

	r := framer.NewLengthDelimitedFrameReader(pr)
	d := &fakeDecoder{}
	dec := NewDecoder(r, d)
	if _, _, err := dec.Decode(nil, nil); err != nil || !bytes.Equal(d.got, frames[0]) {
		t.Fatalf("unexpected %v %v", err, len(d.got))
	}
	if _, _, err := dec.Decode(nil, nil); err != nil || !bytes.Equal(d.got, frames[1]) {
		t.Fatalf("unexpected %v %v", err, len(d.got))
	}
	if _, _, err := dec.Decode(nil, nil); err != ErrObjectTooLarge || !bytes.Equal(d.got, frames[1]) {
		t.Fatalf("unexpected %v %v", err, len(d.got))
	}
	if _, _, err := dec.Decode(nil, nil); err != nil || !bytes.Equal(d.got, frames[3]) {
		t.Fatalf("unexpected %v %v", err, len(d.got))
	}
	if _, _, err := dec.Decode(nil, nil); err != io.EOF {
		t.Fatalf("unexpected %v %v", err, len(d.got))
	}
}
