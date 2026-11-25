/*
Copyright 2018 The Kubernetes Authors.

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
	"io"
)

// readerWrapper 委托给一个 io.Reader，使自身只实现 io.Reader 接口，
// 从而避免 io.Copy 在从该读取器向数据流拷贝时做出我们不希望的行为。
//
// 如果传给 remotecommand 的 Stdin io.Reader 实现了 WriteTo 方法
// （bytes.Buffer 就是如此[1]），io.Copy 会调用该方法[2]，试图一次性把
// 整个缓冲区写入流。这会导致对 spdystream.Stream#Write 的超大调用[3]，
// 进而产生一个过大的单帧数据帧[4]。
//
// [1] https://golang.org/pkg/bytes/#Buffer.WriteTo
// [2] https://golang.org/pkg/io/#Copy
// [3] https://github.com/kubernetes/kubernetes/blob/90295640ef87db9daa0144c5617afe889e7992b2/vendor/github.com/docker/spdystream/stream.go#L66-L73
// [4] https://github.com/kubernetes/kubernetes/blob/90295640ef87db9daa0144c5617afe889e7992b2/vendor/github.com/docker/spdystream/spdy/write.go#L302-L304
type readerWrapper struct {
	reader io.Reader
}

// Read 只暴露 io.Reader，隐藏底层可能存在的 WriteTo，从而避免 io.Copy 生成超大 SPDY 帧。
func (r readerWrapper) Read(p []byte) (int, error) {
	return r.reader.Read(p)
}
