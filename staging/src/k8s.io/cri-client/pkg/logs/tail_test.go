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

package logs

import (
	"bytes"
	"strings"
	"testing"
)

// TestTail 验证 findTailLineStartIndex 在不同 n 值下返回的起始偏移。
// 输入：4 个完整行（每行 blockSize 个字符加换行）加 1 个不完整的半行。
// 断言：n=-1 返回 0；n=0 指向不完整行的起点；n=1 指向倒数第二行起点；n 超过行数时返回 0。
func TestTail(t *testing.T) {
	// 每行长度恰好等于一个块大小，便于验证跨块统计逻辑。
	line := strings.Repeat("a", blockSize)
	// 构造 4 个完整行 + 1 个不完整的半行（半行不计入行数）。
	testBytes := []byte(line + "\n" +
		line + "\n" +
		line + "\n" +
		line + "\n" +
		line[blockSize/2:]) // 不完整的行

	// 表驱动用例：n 为请求跟随的行数，start 为期望的起始字节偏移。
	for c, test := range []struct {
		// n 是请求跟随的行数，start 是期望返回的文件起始偏移。
		n     int64
		start int64
	}{
		// 负数：从头读取。
		{n: -1, start: 0},
		// 0 行：从最后那个不完整行的起点开始。
		{n: 0, start: int64(len(line)+1) * 4},
		// 1 行：从倒数第二行（第 4 个完整行）起点开始。
		{n: 1, start: int64(len(line)+1) * 3},
		// 行数超过文件总行数：返回文件开头。
		{n: 9999, start: 0},
	} {
		t.Logf("TestCase #%d: %+v", c, test)
		// 用 bytes.Reader 作为可读可寻址的输入，调用并断言返回的偏移量。
		r := bytes.NewReader(testBytes)
		s, err := findTailLineStartIndex(r, test.n)
		if err != nil {
			t.Error(err)
		}
		if s != test.start {
			t.Errorf("%d != %d", s, test.start)
		}
	}
}
