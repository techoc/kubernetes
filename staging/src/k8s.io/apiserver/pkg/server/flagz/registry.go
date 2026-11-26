/*
Copyright 2024 The Kubernetes Authors.

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

package flagz

import (
	"sync"
)

// registry 保存 flagz 端点运行时需要的状态。
// 其中缓存的明文响应由 Mutex 保护，因为 HTTP 处理是并发的，而 flag 值在进程生命周期内不变，
// 缓存可以避免每次请求都重新序列化全部参数。
type registry struct {
	// reader 是可获取 flag 的 Reader。
	reader Reader
	// deprecatedVersionsMap 是已弃用 flagz 版本的映射。
	deprecatedVersionsMap map[string]bool
	// cachedPlainTextResponse 是 flagz 端点的缓存响应。
	cachedPlainTextResponse []byte
	// cachedPlainTextResponseLock 保护缓存响应。
	cachedPlainTextResponseLock sync.Mutex
}

// Option 是配置 registry 的函数。
// 采用函数式选项，便于在不破坏调用方的前提下扩展 registry 的配置项。
type Option func(reg *registry)

// deprecatedVersions 返回需要标记为已弃用的版本集合。
func (r *registry) deprecatedVersions() map[string]bool {
	return r.deprecatedVersionsMap
}
