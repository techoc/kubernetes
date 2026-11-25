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

import (
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// 子协议版本号越高功能越全；服务端与客户端协商时取共同支持的最高版本，
// 因此 v1 必须长期保留以兼容旧客户端。
const (
	// DefaultStreamCreationTimeout 是创建流的默认超时；超时后连接被视为
	// 失败，避免半开连接长期占用。
	DefaultStreamCreationTimeout = 30 * time.Second

	//SPDY 子协议 "channel.k8s.io" 用于远程命令附着和执行。
	//这是初始的未版本化子协议，
	//存在已知缺陷 https://issues.k8s.io/13394 和
	// https://issues.k8s.io/13395.
	StreamProtocolV1Name = "channel.k8s.io"

	//SPDY 子协议 "v2.channel.k8s.io" 用于远程命令附着和执行，
	//是该子协议的第二版，
	//解决了第一版中的问题。
	StreamProtocolV2Name = "v2.channel.k8s.io"

	//SPDY 子协议 "v3.channel.k8s.io" 用于远程命令附着和执行，
	//是该子协议的第三版，
	//新增容器终端大小调整支持。
	StreamProtocolV3Name = "v3.channel.k8s.io"

	//SPDY 子协议 "v4.channel.k8s.io" 用于远程命令附着和执行，
	//是该子协议的第四版，
	//新增退出码支持。
	StreamProtocolV4Name = "v4.channel.k8s.io"

	//子协议 "v5.channel.k8s.io" 用于远程命令附着和执行，
	//是该子协议的第五版，
	//新增 CLOSE 信号支持。
	StreamProtocolV5Name = "v5.channel.k8s.io"

	// NonZeroExitCodeReason/ExitCodeCauseType 用于把容器退出码包装成
	// Status 的 reason 与 cause，客户端据此区分“命令失败”与传输错误。
	NonZeroExitCodeReason = metav1.StatusReason("NonZeroExitCode")
	ExitCodeCauseType     = metav1.CauseType("ExitCode")

	//RemoteCommand 流标识符。前三个标识符（STDIN、
	//STDOUT、STDERR）与相应文件描述符相同。
	//
	// 其余通道：StreamErr 传输错误状态，StreamResize 传输终端尺寸，
	// StreamClose 用于 v5 的半关闭语义（255 故意远离小整数，避免与 fd 冲突）。
	StreamStdIn  = 0
	StreamStdOut = 1
	StreamStdErr = 2
	StreamErr    = 3
	StreamResize = 4
	StreamClose  = 255
)

// SupportedStreamingProtocols 按从新到旧排列，协商时优先选择前面的版本；
// 列表不包含 v5，因此默认协商不会选中 v5，需要 CLOSE 语义的调用方必须
// 显式指定协议版本。
var SupportedStreamingProtocols = []string{StreamProtocolV4Name, StreamProtocolV3Name, StreamProtocolV2Name, StreamProtocolV1Name}
