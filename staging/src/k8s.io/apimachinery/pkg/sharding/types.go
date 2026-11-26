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

// Package sharding 定义基于一致性哈希的请求分片选择规则。
// 哈希空间为 FNV-1a 64 位，按半开区间 [Start, End) 把对象分配到 shard。
package sharding

// ShardRangeRequirement 表示单个分片范围条件：对指定字段路径求哈希，
// 再用十六进制区间 [Start, End) 过滤。哈希空间为 64 位 FNV-1a：
// [0x0000000000000000, 0x10000000000000000)。Start 和 End 都必须填写，
// 不支持留空或开放区间。
//
// 使用半开区间是为了让相邻 shard 共享边界值而不会重复：一个 shard 的 End
// 正好是下一个 shard 的 Start。
type ShardRangeRequirement struct {
	// Key 是字段路径，例如 "object.metadata.uid"。
	//
	// 路径通过 sharding.Accessor 解析，缺失字段视为零值参与哈希。
	Key string
	// Start 是包含下界的 0x 前缀小写十六进制字符串，最小值为 "0x0000000000000000"。
	Start string
	// End 是不含上界的 0x 前缀小写十六进制字符串，最大值为 "0x10000000000000000"（2^64）。
	//
	// 2^64 已超出 uint64 范围，因此解析时需用大整数而非 ParseUint。
	End string
}
