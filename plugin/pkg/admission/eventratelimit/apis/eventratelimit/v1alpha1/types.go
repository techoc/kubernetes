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

package v1alpha1

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

// LimitType 表示限流类型（例如按命名空间限流）。
type LimitType string

const (
	// ServerLimitType 类型的限流只有一个桶，由 API Server 收到的所有事件查询共享。
	ServerLimitType LimitType = "Server"
	// NamespaceLimitType 类型的限流为每个命名空间使用一个桶。
	NamespaceLimitType LimitType = "Namespace"
	// UserLimitType 类型的限流为每个用户使用一个桶。
	UserLimitType LimitType = "User"
	// SourceAndObjectLimitType 类型的限流为事件的来源与涉及对象的每种组合使用一个桶。
	SourceAndObjectLimitType LimitType = "SourceAndObject"
)

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// Configuration 为 EventRateLimit 准入控制器提供配置。
type Configuration struct {
	metav1.TypeMeta `json:""`

	// limits 是对收到的事件查询施加的限流规则。
	// 可以针对服务器范围、按命名空间、按用户以及按来源+对象收到的事件设置限流。
	// 至少需要配置一条限流规则。
	Limits []Limit `json:"limits"`
}

// Limit 是某一种限流类型的配置。
type Limit struct {
	// type 是本配置适用的限流类型。
	Type LimitType `json:"type"`

	// qps 是该限流类型每秒允许的事件查询数。qps 与 burst 字段共同决定某个事件查询是否被接受。
	// 当 burst 额度用尽后，qps 决定每秒允许多少查询通过。
	QPS int32 `json:"qps"`

	// burst 是该限流类型允许的突发事件查询数。qps 与 burst 共同决定某个事件查询是否被接受。
	// burst 决定单个桶可获得的最大额度。例如 burst 为 10、qps 为 3 时，
	// 准入控制会先接受 10 个查询，之后才开始阻塞；每秒再放行 3 个查询。
	// 未用完的额度会结转到下一秒，直到达到 10 的最大额度为止。
	Burst int32 `json:"burst"`

	// cacheSize 是该限流类型 LRU 缓存的大小。桶一旦被缓存淘汰，其额度即被重置；
	// 之后若再收到该桶的查询，它会以全新状态重新进入缓存，获得完整的 burst 额度。
	//
	// 默认缓存大小为 4096。
	//
	// 若 limitType 为 'server'，则 cacheSize 被忽略。
	// +optional
	CacheSize int32 `json:"cacheSize,omitempty"`
}
