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
	"io"
)

// blockSize 是查找尾部行时每次从文件中读取的块大小（字节数）。
const blockSize = 1024

// findTailLineStartIndex 返回倒数第 n 行的起始字节偏移量。
// n < 0 时返回文件开头；n >= 0 时返回倒数第 n 行的起始位置。
// 注意：若最后一行不完整（没有换行符结尾），它不会被计为一行。
func findTailLineStartIndex(f io.ReadSeeker, n int64) (int64, error) {
	if n < 0 {
		// 负数表示读取全部内容，直接从文件开头开始。
		return 0, nil
	}
	// 先定位到文件末尾，以获得文件总字节数。
	size, err := f.Seek(0, io.SeekEnd)
	if err != nil {
		return 0, err
	}
	var left, cnt int64
	buf := make([]byte, blockSize)
	// 从文件末尾开始逐块向前读取，统计块内换行符个数（即完整行数）。
	// 当统计的行数超过 n 时停止，说明目标行已包含在当前块中。
	for right := size; right > 0 && cnt <= n; right -= blockSize {
		left = right - blockSize
		if left < 0 {
			// 剩余内容不足一个块时，起点对齐到文件开头，并按需缩小缓冲区。
			left = 0
			buf = make([]byte, right)
		}
		// 定位到当前块的起始位置并读取该块。
		if _, err := f.Seek(left, io.SeekStart); err != nil {
			return 0, err
		}
		if _, err := f.Read(buf); err != nil {
			return 0, err
		}
		// 每个换行符代表一个完整行的结束，累加得到已扫描到的行数。
		cnt += int64(bytes.Count(buf, eol))
	}
	// 此时 buf 是包含目标行的块，left 是该块在文件中的起始偏移。
	// 从块首开始逐个跳过换行符，直到剩余行数恰好为 n，即为倒数第 n 行的起点。
	for ; cnt > n; cnt-- {
		idx := bytes.Index(buf, eol) + 1
		buf = buf[idx:]
		left += int64(idx)
	}
	return left, nil
}
