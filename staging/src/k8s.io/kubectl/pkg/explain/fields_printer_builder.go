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

package explain

// fieldsPrinterBuilder 根据参数构建 regularFieldsPrinter 或
// recursiveFieldsPrinter。
type fieldsPrinterBuilder struct {
	Recursive bool
	MaxDepth  int
}

// BuildFieldsPrinter 构建合适的 fieldsPrinter。
func (f fieldsPrinterBuilder) BuildFieldsPrinter(writer *Formatter) fieldsPrinter {
	if f.Recursive {
		return &recursiveFieldsPrinter{
			Writer:   writer,
			MaxDepth: f.MaxDepth,
			Depth:    1,
		}
	}

	return &regularFieldsPrinter{
		Writer: writer,
	}
}
