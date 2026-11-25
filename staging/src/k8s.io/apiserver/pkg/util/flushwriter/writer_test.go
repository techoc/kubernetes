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

package flushwriter

import (
	"fmt"
	"testing"
)

// writerWithFlush 同时实现 Write 与 Flush，用于验证每次写入后都会刷新。
type writerWithFlush struct {
	writeCount, flushCount int
	err                    error
}

// Flush 记录刷新次数。
func (w *writerWithFlush) Flush() {
	w.flushCount++
}

// Write 记录写入次数并返回预设错误。
func (w *writerWithFlush) Write(p []byte) (n int, err error) {
	w.writeCount++
	return len(p), w.err
}

// writerWithNoFlush 只实现 Write，用于验证没有 Flusher 时仍能正常写入。
type writerWithNoFlush struct {
	writeCount int
}

// Write 记录写入次数。
func (w *writerWithNoFlush) Write(p []byte) (n int, err error) {
	w.writeCount++
	return len(p), nil
}

// TestWriteWithFlush 验证底层实现了 Flush 时，每次 Write 都会触发一次 Flush。
//
// 流式响应依赖这一行为把数据及时推送给客户端。
func TestWriteWithFlush(t *testing.T) {
	w := &writerWithFlush{}
	fw := Wrap(w)
	for i := 0; i < 10; i++ {
		_, err := fw.Write([]byte("Test write"))
		if err != nil {
			t.Errorf("Unexpected error while writing with flush writer: %v", err)
		}
	}
	if w.flushCount != 10 {
		t.Errorf("Flush not called the expected number of times. Actual: %d", w.flushCount)
	}
	if w.writeCount != 10 {
		t.Errorf("Write not called the expected number of times. Actual: %d", w.writeCount)
	}
}

// TestWriteWithoutFlush 验证底层不支持 Flush 时写入照常进行，不 panic。
func TestWriteWithoutFlush(t *testing.T) {
	w := &writerWithNoFlush{}
	fw := Wrap(w)
	for i := 0; i < 10; i++ {
		_, err := fw.Write([]byte("Test write"))
		if err != nil {
			t.Errorf("Unexpected error while writing with flush writer: %v", err)
		}
	}
	if w.writeCount != 10 {
		t.Errorf("Write not called the expected number of times. Actual: %d", w.writeCount)
	}
}

// TestWriteError 验证底层写入错误被原样返回（不被包装），调用方可按需判断。
func TestWriteError(t *testing.T) {
	e := fmt.Errorf("Error")
	w := &writerWithFlush{err: e}
	fw := Wrap(w)
	_, err := fw.Write([]byte("Test write"))
	if err != e {
		t.Errorf("Did not get expected error. Got: %#v", err)
	}
}
