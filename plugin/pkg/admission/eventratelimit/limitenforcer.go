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

import (
	"fmt"
	"strings"

	"k8s.io/apiserver/pkg/admission"
	"k8s.io/client-go/util/flowcontrol"
	api "k8s.io/kubernetes/pkg/apis/core"
	eventratelimitapi "k8s.io/kubernetes/plugin/pkg/admission/eventratelimit/apis/eventratelimit"
	"k8s.io/utils/lru"
)

const (
	// 用户未指定缓存大小时使用的默认值。
	defaultCacheSize = 4096
)

// limitEnforcer 执行单一类型的事件限流，例如服务器、命名空间或来源+对象。
type limitEnforcer struct {
	// 本执行器负责的限流类型。
	limitType eventratelimitapi.LimitType
	// 存放各令牌桶的缓存。
	cache cache
	// keyFunc 根据请求属性计算令牌桶对应的键。
	keyFunc func(admission.Attributes) string
}

// newLimitEnforcer 按配置创建限流执行器。
func newLimitEnforcer(config eventratelimitapi.Limit, clock flowcontrol.Clock) (*limitEnforcer, error) {
	rateLimiterFactory := func() flowcontrol.RateLimiter {
		return flowcontrol.NewTokenBucketRateLimiterWithClock(float32(config.QPS), int(config.Burst), clock)
	}

	if config.Type == eventratelimitapi.ServerLimitType {
		// 服务器级限流只有一个共享桶，无需 LRU 缓存。
		return &limitEnforcer{
			limitType: config.Type,
			cache: &singleCache{
				rateLimiter: rateLimiterFactory(),
			},
			keyFunc: getServerKey,
		}, nil
	}

	cacheSize := int(config.CacheSize)
	if cacheSize == 0 {
		cacheSize = defaultCacheSize
	}
	underlyingCache := lru.New(cacheSize)
	cache := &lruCache{
		rateLimiterFactory: rateLimiterFactory,
		cache:              underlyingCache,
	}

	var keyFunc func(admission.Attributes) string
	switch t := config.Type; t {
	case eventratelimitapi.NamespaceLimitType:
		keyFunc = getNamespaceKey
	case eventratelimitapi.UserLimitType:
		keyFunc = getUserKey
	case eventratelimitapi.SourceAndObjectLimitType:
		keyFunc = getSourceAndObjectKey
	default:
		return nil, fmt.Errorf("unknown event rate limit type: %v", t)
	}

	return &limitEnforcer{
		limitType: config.Type,
		cache:     cache,
		keyFunc:   keyFunc,
	}, nil
}

// accept 判断当前请求在对应令牌桶中是否还有可用配额。
func (enforcer *limitEnforcer) accept(attr admission.Attributes) error {
	key := enforcer.keyFunc(attr)
	rateLimiter := enforcer.cache.get(key)

	// 尝试取一个令牌，配额不足即拒绝请求。
	allow := rateLimiter.TryAccept()

	if !allow {
		return fmt.Errorf("limit reached on type %v for key %v", enforcer.limitType, key)
	}

	return nil
}

// getServerKey 返回服务器级限流的缓存键；所有请求共用同一个桶，故键恒为空。
func getServerKey(attr admission.Attributes) string {
	return ""
}

// getNamespaceKey 根据事件请求所属命名空间返回缓存键。
func getNamespaceKey(attr admission.Attributes) string {
	return attr.GetNamespace()
}

// getUserKey 根据事件请求的用户返回缓存键。
func getUserKey(attr admission.Attributes) string {
	userInfo := attr.GetUserInfo()
	// 无用户信息时归入默认桶，避免空指针。
	if userInfo == nil {
		return ""
	}
	return userInfo.GetName()
}

// getSourceAndObjectKey 根据事件的来源与涉及对象返回缓存键。
func getSourceAndObjectKey(attr admission.Attributes) string {
	object := attr.GetObject()
	if object == nil {
		return ""
	}
	event, ok := object.(*api.Event)
	// 非 Event 对象无法提取来源与涉及对象，归入默认桶。
	if !ok {
		return ""
	}
	return strings.Join([]string{
		event.Source.Component,
		event.Source.Host,
		event.InvolvedObject.Kind,
		event.InvolvedObject.Namespace,
		event.InvolvedObject.Name,
		string(event.InvolvedObject.UID),
		event.InvolvedObject.APIVersion,
	}, "")
}
