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

package term

import (
	"errors"
	"io"
	"os"

	wordwrap "github.com/mitchellh/go-wordwrap"
	"github.com/moby/term"
)

type wordWrapWriter struct {
	limit  uint
	writer io.Writer
}

// 创建能检测终端列宽、按推荐宽度折行以提升可读性的 Writer，
// 自动按词折行。
//
//	终端宽度 >= 120 列时使用 120 列。
//	终端宽度 >= 100 列时使用 100 列。
//	终端宽度 >= 80 列时使用 80 列。
//
// 不在终端中或宽度不足 80 列时
// 不折行。
func NewResponsiveWriter(w io.Writer) io.Writer {
	file, ok := w.(*os.File)
	if !ok {
		return w
	}
	fd := file.Fd()
	if !term.IsTerminal(fd) {
		return w
	}

	terminalSize := GetSize(fd)
	if terminalSize == nil {
		return w
	}
	limit := getTerminalLimitWidth(terminalSize)

	return NewWordWrapWriter(w, limit)
}

// 按每行字符上限自动折行的 Writer。
func NewWordWrapWriter(w io.Writer, limit uint) io.Writer {
	return &wordWrapWriter{
		limit:  limit,
		writer: w,
	}
}

func getTerminalLimitWidth(terminalSize *TerminalSize) uint {
	var limit uint
	switch {
	case terminalSize.Width >= 120:
		limit = 120
	case terminalSize.Width >= 100:
		limit = 100
	case terminalSize.Width >= 80:
		limit = 80
	}
	return limit
}

func GetWordWrapperLimit() (uint, error) {
	stdout := os.Stdout
	fd := stdout.Fd()
	if !term.IsTerminal(fd) {
		return 0, errors.New("file descriptor is not a terminal")
	}
	terminalSize := GetSize(fd)
	if terminalSize == nil {
		return 0, errors.New("terminal size is nil")
	}
	return getTerminalLimitWidth(terminalSize), nil
}

func (w wordWrapWriter) Write(p []byte) (nn int, err error) {
	if w.limit == 0 {
		return w.writer.Write(p)
	}
	original := string(p)
	wrapped := wordwrap.WrapString(original, w.limit)
	return w.writer.Write([]byte(wrapped))
}

// 把行宽限制为 80 列的 NewWordWrapWriter。
func NewPunchCardWriter(w io.Writer) io.Writer {
	return NewWordWrapWriter(w, 80)
}

type maxWidthWriter struct {
	maxWidth     uint
	currentWidth uint
	written      uint
	writer       io.Writer
}

// 限制每行字符数但不自动折行的 Writer。
func NewMaxWidthWriter(w io.Writer, maxWidth uint) io.Writer {
	return &maxWidthWriter{
		maxWidth: maxWidth,
		writer:   w,
	}
}

func (m maxWidthWriter) Write(p []byte) (nn int, err error) {
	for _, b := range p {
		if m.currentWidth == m.maxWidth {
			m.writer.Write([]byte{'\n'})
			m.currentWidth = 0
		}
		if b == '\n' {
			m.currentWidth = 0
		}
		_, err := m.writer.Write([]byte{b})
		if err != nil {
			return int(m.written), err
		}
		m.written++
		m.currentWidth++
	}
	return len(p), nil
}
