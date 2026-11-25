/*
Copyright 2023 The Kubernetes Authors.

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

import "k8s.io/klog/v2"

// streamProtocolV5 支持远程命令子协议的 V5 版本。
// 对 streamProtocolHandler 而言，该版本与 V4 相同。
//
// V5 在协议层与 V4 完全一致，引入新版本号只是为了表达语义变化
// （例如对关闭/错误信道的约定），因此实现上直接复用 V4 的处理器。
type streamProtocolV5 struct {
	*streamProtocolV4
}

var _ streamProtocolHandler = &streamProtocolV5{}

func newStreamProtocolV5(options StreamOptions) streamProtocolHandler {
	return &streamProtocolV5{
		streamProtocolV4: newStreamProtocolV4(options).(*streamProtocolV4),
	}
}

// stream 直接委托 V4 实现；保留方法是为了让 V5 成为独立的协议处理器，
// 便于将来在不影响 V4 的前提下修改 V5 行为。
func (p *streamProtocolV5) stream(logger klog.Logger, conn streamCreator, ready chan<- struct{}) error {
	return p.streamProtocolV4.stream(logger, conn, ready)
}
