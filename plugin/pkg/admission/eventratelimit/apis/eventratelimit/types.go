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

package eventratelimit

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

// LimitType 表示限流类型（例如按命名空间限流）。
type LimitType string

const (
	// ServerLimitType 表示 API Server 收到的所有事件查询共享一个令牌桶。
	ServerLimitType LimitType = "Server"
	// NamespaceLimitType 表示每个命名空间各使用一个令牌桶。
	NamespaceLimitType LimitType = "Namespace"
	// UserLimitType 表示每个用户各使用一个令牌桶。
	UserLimitType LimitType = "User"
	// SourceAndObjectLimitType 表示事件来源与涉及对象的每种组合各使用一个令牌桶。
	SourceAndObjectLimitType LimitType = "SourceAndObject"
)

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// Configuration 为 EventRateLimit 准入控制器提供配置。
type Configuration struct {
	metav1.TypeMeta `json:""`

	// limits 指定对收到的事件查询施加的限制。
	// 可以按服务器全局、命名空间、用户、来源+对象等维度限流。
	// 至少需要配置一条限制。
	Limits []Limit `json:"limits"`
}

// Limit 是某一限流类型的具体配置。
type Limit struct {
	// type 指定本配置适用的限流类型。
	Type LimitType `json:"type"`

	// qps 是该限流类型每秒允许的事件查询数。
	// qps 与 burst 共同决定一次事件查询是否被接受：burst 配额耗尽后，
	// qps 决定后续每秒放行多少查询。
	QPS int32 `json:"qps"`

	// burst 是该限流类型允许的突发事件查询数。
	// qps 与 burst 共同决定一次事件查询是否被接受；burst 决定单个令牌桶的容量上限。
	// 例如 burst 为 10、qps 为 3 时，准入控制会先放行 10 次查询再开始拒绝，
	// 之后每秒补充 3 次配额。未用完的配额会结转到下一秒，直至达到上限 10。
	Burst int32 `json:"burst"`

	// cacheSize 是该限流类型所用 LRU 缓存的容量。若某个令牌桶被淘汰，
	// 其配额即被重置；后续再有该桶的查询时，它会以全新状态重新进入缓存，
	// 获得完整的 burst 配额。
	//
	// 默认缓存大小为 4096。
	//
	// 若 limitType 为 'server'，则忽略 cacheSize。
	// +optional
	CacheSize int32 `json:"cacheSize,omitempty"`
}
