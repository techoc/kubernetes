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

package podresources

const (
	// Socket 是 podresources 服务端 socket 的名称。
	Socket = "kubelet"

	// DefaultQPS 基于对已知 API 使用方的经验评估确定。
	// 合法需求不太可能超过每秒 100 次查询，毕竟其他子系统也未必能这么快响应。
	DefaultQPS = 100

	// DefaultBurstTokens 同样基于对已知 API 使用方的经验评估确定，
	// 注意事项参见 DefaultQPS 的说明。
	DefaultBurstTokens = 10
)
