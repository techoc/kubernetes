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

package remotecommand

import "time"

const (
	// DefaultStreamCreationTimeout 是等待客户端创建全部流的默认超时时间。
	DefaultStreamCreationTimeout = 30 * time.Second

	// 以下四个常量是请求中声明所需流的查询参数，值 "1" 表示开启该项。
	ExecStdinParam  = "input"
	ExecStdoutParam = "output"
	ExecStderrParam = "error"
	ExecTTYParam    = "tty"

	// StreamType 是流类型头部的键，下面几个值分别标识 stdin、stdout、stderr、error 和 resize 流。
	StreamType       = "streamType"
	StreamTypeStdin  = "stdin"
	StreamTypeStdout = "stdout"
	StreamTypeStderr = "stderr"
	StreamTypeError  = "error"
	StreamTypeResize = "resize"

	// The SPDY subprotocol "channel.k8s.io" is used for remote command
	// attachment/execution. This represents the initial unversioned subprotocol,
	// which has the known bugs https://issues.k8s.io/13394 and
	// https://issues.k8s.io/13395.
	// StreamProtocolV1Name 是首个未带版本号的 SPDY 子协议，存在已知缺陷（见上方 issue 链接）。
	StreamProtocolV1Name = "channel.k8s.io"

	// The SPDY subprotocol "v2.channel.k8s.io" is used for remote command
	// attachment/execution. It is the second version of the subprotocol and
	// resolves the issues present in the first version.
	// StreamProtocolV2Name 是第二版子协议，修复了第一版中的问题。
	StreamProtocolV2Name = "v2.channel.k8s.io"

	// The SPDY subprotocol "v3.channel.k8s.io" is used for remote command
	// attachment/execution. It is the third version of the subprotocol and
	// adds support for resizing container terminals.
	// StreamProtocolV3Name 是第三版子协议，新增了对容器终端尺寸调整（resize）的支持。
	StreamProtocolV3Name = "v3.channel.k8s.io"

	// The SPDY subprotocol "v4.channel.k8s.io" is used for remote command
	// attachment/execution. It is the 4th version of the subprotocol and
	// adds support for exit codes.
	// StreamProtocolV4Name 是第四版子协议，新增了对退出码（通过状态对象传递）的支持。
	StreamProtocolV4Name = "v4.channel.k8s.io"

	// The subprotocol "v5.channel.k8s.io" is used for remote command
	// attachment/execution. It is the 5th version of the subprotocol and
	// adds support for a CLOSE signal.
	// StreamProtocolV5Name 是第五版子协议，新增了 CLOSE 信号的支持。
	StreamProtocolV5Name = "v5.channel.k8s.io"

	// 这两个常量用于构造非零退出码的状态：Reason 固定为 NonZeroExitCode，cause 的 Type 为 ExitCode。
	NonZeroExitCodeReason = "NonZeroExitCode"
	ExitCodeCauseType     = "ExitCode"

	// RemoteCommand stream identifiers. The first three identifiers (for STDIN,
	// STDOUT, STDERR) are the same as their file descriptors.
	// 以下是远端命令各流的标识符；前三个与文件描述符一致，StreamClose 是关闭信号。
	StreamStdIn  = 0
	StreamStdOut = 1
	StreamStdErr = 2
	StreamErr    = 3
	StreamResize = 4
	StreamClose  = 255
)

// SupportedStreamingProtocols 是服务端可接受的 SPDY 子协议列表，按偏好从高到低排列。
var SupportedStreamingProtocols = []string{StreamProtocolV4Name, StreamProtocolV3Name, StreamProtocolV2Name, StreamProtocolV1Name}
