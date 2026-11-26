/*
Copyright The Kubernetes Authors.

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

package criproxy

import (
	"fmt"

	"k8s.io/apimachinery/pkg/util/rand"
)

const (
	// defaultNpipeEndpoint 是 CRI proxy gRPC server 使用的 named-pipe 端点模板。
	// Windows CRI 端点是 named pipe（npipe://），不是 unix socket，
	// 因此 proxy 监听一个唯一命名的 pipe。
	defaultNpipeEndpoint = "npipe://./pipe/kubelet-remote-proxy-%v"
)

// GenerateEndpoint 为 proxy gRPC server 生成新的 named-pipe 端点。
func GenerateEndpoint() (string, error) {
	// 用随机整数作为 pipe 名的一部分，保证唯一
	return fmt.Sprintf(defaultNpipeEndpoint, rand.Int()), nil
}
