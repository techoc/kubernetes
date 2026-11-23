/*
Copyright 2025 The Kubernetes Authors.

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

package client

import (
	"errors"
	"sync/atomic"

	"k8s.io/client-go/kubernetes"
	cgoresource "k8s.io/client-go/kubernetes/typed/resource/v1"
	rest "k8s.io/client-go/rest"
	"k8s.io/client-go/util/watchlist"
)

// Enumerate all supported APIs, most preferred first.
// 按优先级从高到低枚举受支持的 API 版本，useAPI 保存的就是这些编号之一。
const (
	// useLatestAPI 表示最新且最优先的 API 版本（当前为 v1）。
	useLatestAPI = int32(iota)
	// useV1beta2API 表示可回退使用的 v1beta2 版本。
	useV1beta2API
	// useV1beta1API 表示可回退使用的 v1beta1 版本。
	useV1beta1API
	// numAPIs 为版本总数，用于轮换时取模。
	numAPIs
)

// apiName 返回 API 编号对应的可读名称，未知编号返回 "???"。
func apiName(apiNumber int32) string {
	// 按编号匹配受支持的版本，结果主要用于诊断。
	switch apiNumber {
	case useLatestAPI:
		// 最新稳定版。
		return "V1"
	case useV1beta2API:
		// v1beta2 回退版本。
		return "V1beta2"
	case useV1beta1API:
		// v1beta1 回退版本。
		return "V1beta1"
	default:
		// 未知编号。
		return "???"
	}
}

var (
	// ErrNotImplemented 表示当前包装客户端不支持该操作，
	// 例如 Patch 和 Server-Side Apply。
	ErrNotImplemented = errors.New("not implemented in k8s.io/dynamic-resource-allocation/client")
)

// New 基于给定的 client-go 客户端集合创建包装客户端。
func New(clientSet kubernetes.Interface) *Client {
	// 只保存底层客户端集合；当前 API 版本由首次调用探测得出。
	return &Client{
		clientSet: clientSet,
	}
}

// Client 对外实现资源 v1 客户端接口，内部记录当前可用的 API 版本，
// 以便在当前版本不支持某类资源时自动回退到其他版本。
//
// 字段说明：clientSet 是底层 client-go 客户端集合；
// useAPI 原子保存当前使用的 API 版本编号，可被并发读写。
type Client struct {
	clientSet kubernetes.Interface
	useAPI    atomic.Int32
}

// 编译期断言：Client 满足资源 v1 客户端接口。
var _ cgoresource.ResourceV1Interface = &Client{}

// CurrentAPI 返回当前使用的 API 版本名称，主要用于诊断。
func (c *Client) CurrentAPI() string {
	// 把原子保存的编号转换为可读名称。
	return apiName(c.useAPI.Load())
}

// RESTClient 返回底层 v1 资源的 REST 客户端。
func (c *Client) RESTClient() rest.Interface {
	// REST 客户端直接透传底层实现。
	return c.clientSet.ResourceV1().RESTClient()
}

// DeviceClasses 返回支持跨版本转换的 DeviceClass 客户端。
func (c *Client) DeviceClasses() cgoresource.DeviceClassInterface {
	// 把三个版本的接口交给 newConvertingClient，由它按当前 API 选择并转换。
	return newConvertingClient(c,
		c.clientSet.ResourceV1().DeviceClasses(),
		c.clientSet.ResourceV1beta1().DeviceClasses(),
		c.clientSet.ResourceV1beta2().DeviceClasses(),
	)
}

// DeviceTaintRules 返回支持跨版本转换的 DeviceTaintRule 客户端。
func (c *Client) DeviceTaintRules() cgoresource.DeviceTaintRuleInterface {
	// 把三个版本的接口交给 newConvertingClient。
	return newConvertingClient(c,
		c.clientSet.ResourceV1().DeviceTaintRules(),
		// DeviceTaintRules isn't in v1beta1 but newConvertingClient expects this.
		// We can simply treat it like v1beta2.
		// v1beta1 中不存在该资源，用 v1beta2 的接口占位，避免传入空接口。
		c.clientSet.ResourceV1beta2().DeviceTaintRules(),
		c.clientSet.ResourceV1beta2().DeviceTaintRules(),
	)
}

// ResourceClaims 返回指定命名空间下支持跨版本转换的 ResourceClaim 客户端。
func (c *Client) ResourceClaims(namespace string) cgoresource.ResourceClaimInterface {
	// 命名空间由调用方指定，三个版本的接口共享同一命名空间。
	return newConvertingClient(c,
		c.clientSet.ResourceV1().ResourceClaims(namespace),
		c.clientSet.ResourceV1beta1().ResourceClaims(namespace),
		c.clientSet.ResourceV1beta2().ResourceClaims(namespace),
	)
}

// ResourceClaimTemplates 返回指定命名空间下支持跨版本转换的 ResourceClaimTemplate 客户端。
func (c *Client) ResourceClaimTemplates(namespace string) cgoresource.ResourceClaimTemplateInterface {
	// 命名空间由调用方指定，三个版本的接口共享同一命名空间。
	return newConvertingClient(c,
		c.clientSet.ResourceV1().ResourceClaimTemplates(namespace),
		c.clientSet.ResourceV1beta1().ResourceClaimTemplates(namespace),
		c.clientSet.ResourceV1beta2().ResourceClaimTemplates(namespace),
	)
}

// ResourceSlices 返回支持跨版本转换的 ResourceSlice 客户端。
func (c *Client) ResourceSlices() cgoresource.ResourceSliceInterface {
	// 把三个版本的接口交给 newConvertingClient，由它按当前 API 选择并转换。
	return newConvertingClient(c,
		c.clientSet.ResourceV1().ResourceSlices(),
		c.clientSet.ResourceV1beta1().ResourceSlices(),
		c.clientSet.ResourceV1beta2().ResourceSlices(),
	)
}

// IsWatchListSemanticsUnSupported informs the reflector that this client
// doesn't support WatchList semantics.
//
// This is a synthetic method whose sole purpose is to satisfy the optional
// interface check performed by the reflector.
// Returning true signals that WatchList can NOT be used.
// No additional logic is implemented here.
//
// IsWatchListSemanticsUnSupported 告知 Reflector 本客户端是否不支持 WatchList 语义；
// 该方法只为满足 Reflector 的可选接口检查，实际判断委托给 client-go。
func (c *Client) IsWatchListSemanticsUnSupported() bool {
	// 直接复用工具函数判断底层客户端集合的能力。
	return watchlist.DoesClientNotSupportWatchListSemantics(c.clientSet)
}
