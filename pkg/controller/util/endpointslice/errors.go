/*
Copyright 2021 The Kubernetes Authors.

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

package endpointslice

// StaleInformerCache 表示 informer 缓存中包含过期资源，需要重试或刷新缓存。
type StaleInformerCache struct {
	msg string
}

// NewStaleInformerCache 构造一个带错误消息的 StaleInformerCache。
func NewStaleInformerCache(msg string) *StaleInformerCache {
	return &StaleInformerCache{msg}
}

// Error 返回缓存过期的原因描述，实现 error 接口。
func (e *StaleInformerCache) Error() string { return e.msg }

// IsStaleInformerCacheErr 判断错误是否由 informer 缓存过期引起，调用方可据此决定重新入队。
func IsStaleInformerCacheErr(err error) bool {
	// 只匹配本包的类型；包装后的错误需由调用方先用 errors.As 解包。
	_, ok := err.(*StaleInformerCache)
	return ok
}
