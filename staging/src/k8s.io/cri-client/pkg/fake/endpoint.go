//go:build !windows

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

package fake

import (
	"fmt"
	"math/rand"
)

const (
	// defaultUnixEndpoint 是非 Windows 平台上假 gRPC 服务端使用的 unix socket 地址模板，
	// 其中 %v 会在生成端点时替换为随机整数。
	defaultUnixEndpoint = "unix:///tmp/kubelet_remote_%v.sock"
)

// GenerateEndpoint generates a new unix socket server of grpc server.
// GenerateEndpoint 返回一个新的 unix socket 端点字符串；它只负责拼出路径，并不真正创建 socket，
// 因此第二个返回值始终是 nil。
func GenerateEndpoint() (string, error) {
	// use random int be a part fo file name
	// 用随机整数填充地址模板中的 %v，降低多个测试进程占用同一路径的概率。
	return fmt.Sprintf(defaultUnixEndpoint, rand.Int()), nil
}
