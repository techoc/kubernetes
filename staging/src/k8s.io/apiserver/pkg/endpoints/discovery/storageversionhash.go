/*
Copyright 2019 The Kubernetes Authors.

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

package discovery

import (
	"crypto/sha256"
	"encoding/base64"
)

// StorageVersionHash 计算 <group/version/kind> 三元组的存储版本哈希。
// 警告：本函数将来可能变化，客户端不应依赖它。
//
// 该哈希出现在 API 发现文档中，供客户端判断某个资源的存储版本是否变化
// （例如 storage migration 后）。只取 SHA-256 前 8 字节并以 base64 编码，
// 是在“冲突概率足够低”和“发现文档体积/可读性”之间取的折中。
func StorageVersionHash(group, version, kind string) string {
	gvk := group + "/" + version + "/" + kind
	bytes := sha256.Sum256([]byte(gvk))
	// 假设集群中有 N 种 kind，哈希长度为 X 字节，则冲突概率
	// P(N,X) 近似为 1-e^(-(N^2)/2^(8X+1))。P(10,000, 8) ~= 2.7*10^(-12)，足够低。
	// 参见 https://en.wikipedia.org/wiki/Birthday_problem#Approximations。
	return base64.StdEncoding.EncodeToString(bytes[:8])
}
