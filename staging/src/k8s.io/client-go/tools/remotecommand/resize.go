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

package remotecommand

// TerminalSize 表示终端的宽和高。
// 它与 staging/src/k8s.io/kubectl/pkg/util/term.TerminalSize 相同，
// 为了包的解耦而刻意复制：终端相关包不应依赖 API 客户端，反之亦然。
type TerminalSize struct {
	Width  uint16
	Height uint16
}

// TerminalSizeQueue 能够在终端尺寸变化发生时返回 resize 事件。
// 它与 staging/src/k8s.io/kubectl/pkg/util/term.TerminalSizeQueue 相同，
// 为了包的解耦而刻意复制：终端相关包不应依赖 API 客户端，反之亦然。
type TerminalSizeQueue interface {
	// Next 在终端被调整后返回新的终端尺寸，没有新尺寸时阻塞；
	// 监控停止时返回 nil，调用方据此结束 resize 循环。
	Next() *TerminalSize
}
