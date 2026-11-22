//go:build go1.27

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

package server

import "net/http"

// 1 MiB MaxHeaderBytes / 64 bytes per value (~16 for header and ~48 for value) bounds tiny-header floods
// while allowing thousands of identity groups sent one per header line.
//
// 中文说明：按「最大头部字节数 ÷ 单个头部值的最小字节数」估算值个数上限，
// 目的有二：一是阻止攻击者用大量极小的头部行耗尽 CPU（Go 1.27 起为头部值数量单独设限），
// 二是仍允许前置代理把成千上万个用户组每组一行地传入（RequestHeader 认证依赖此形态）。
// 上限与 MaxHeaderBytes 保持同量级，避免任一维度成为绕过点。
const maxHeaderValueCount = 16384

// setMaxHeaderValueCount 仅在 go1.27+ 构建时存在；旧版本没有 http.Server.MaxHeaderValueCount 字段，
// 因此调用方需通过同名但空实现的文件（header_value_count_pre_go127.go）保持构建兼容。
func setMaxHeaderValueCount(server *http.Server) {
	server.MaxHeaderValueCount = maxHeaderValueCount
}
