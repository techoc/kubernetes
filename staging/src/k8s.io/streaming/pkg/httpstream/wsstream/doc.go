/*
Copyright 2015 The Kubernetes Authors.

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

// Package wsstream 提供通过 WebSocket 传输流内容的工具。
// Conn 允许调用方在单个 WebSocket 连接上复用多个读写通道。
//
// "channel.k8s.io"
//
// WebSocket RemoteCommand 子协议 "channel.k8s.io" 在每个二进制消息前添加一个字节，
// 表示消息发送到的通道号（从 0 开始）。双向消息都必须带上该通道字节。
// 用于远程执行时，通道号按约定对应 STDIN、STDOUT 和 STDERR 的 POSIX 文件描述符
// （0、1、2）。原始子协议不进行其他转换，服务端按收到的内容原样写出。
//
// 客户端会话示例：
//
//	CONNECT http://server.com with subprotocol "channel.k8s.io"
//	WRITE []byte{0, 102, 111, 111, 10} # send "foo\n" on channel 0 (STDIN)
//	READ  []byte{1, 10}                # receive "\n" on channel 1 (STDOUT)
//	CLOSE
//
// "v2.channel.k8s.io"
//
// 第二版 WebSocket 子协议 "v2.channel.k8s.io" 的行为与第一版相同，但它是第一个显式带版本号的子协议。
//
// "v3.channel.k8s.io"
//
// 第三版 WebSocket RemoteCommand 子协议增加了一个终端尺寸调整通道。该通道的消息以字节 '3' 开头，
// 后续携带两个窗口尺寸值（编码为 TerminalSize 结构体），两个整数的取值范围均为 (0, 65536]。
//
// "v4.channel.k8s.io"
//
// 第四版 WebSocket RemoteCommand 子协议增加了错误通道，用于返回包含进程退出码的结构化错误；
// 错误类型为 "apierrors.StatusError{}"。
//
// "v5.channel.k8s.io"
//
// 第五版 WebSocket RemoteCommand 子协议增加了 CLOSE 信号：消息第一个字节为控制信号，第二个字节为通道 ID。
// WebSocket 服务端收到该信号后关闭对应流；必要时允许其他流继续完成传输，再优雅地关闭连接。
//
// 客户端会话示例：
//
//	CONNECT http://server.com with subprotocol "v5.channel.k8s.io"
//	WRITE []byte{0, 102, 111, 111, 10} # send "foo\n" on channel 0 (STDIN)
//	WRITE []byte{255, 0}               # send CLOSE signal (STDIN)
//	CLOSE
//
// 本包对子协议版本采取“先协商、后解码”的策略：Conn 和 Reader 都把子协议名映射到
// 配置表，握手时只接受表中列出的协议，之后编码方式就固定下来。这样新增协议版本
// 只需往配置表里加一项，而不必在收发循环里堆叠条件判断。
//
// 通道编号按字节上限留出了 255 作为控制字节；文本化的 base64 变体把同样的编号
// 写成字符，所以两者共用一套通道布局，只有载荷编码不同。
package wsstream
