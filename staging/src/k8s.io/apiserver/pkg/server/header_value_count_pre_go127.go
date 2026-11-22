//go:build !go1.27

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

// Go versions before 1.27 do not limit the number of header values.
// 因此这里保持空实现；go1.27 及以上由同名的 go127 构建版本设置上限，
// 两个文件必须保持签名一致，否则不同工具链下编译结果不同。
func setMaxHeaderValueCount(*http.Server) {}
