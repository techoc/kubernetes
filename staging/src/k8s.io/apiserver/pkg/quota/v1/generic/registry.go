/*
Copyright 2016 The Kubernetes Authors.

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

package generic

import (
	"sync"

	"k8s.io/apimachinery/pkg/runtime/schema"
	quota "k8s.io/apiserver/pkg/quota/v1"
)

// 实现基础评估器注册表。
//
// simpleRegistry 是 Evaluator 的进程内注册表，按 GroupResource 唯一索引；
// 读写锁允许高频的 Get 与低频的 Add/Remove 并发。
type simpleRegistry struct {
	lock sync.RWMutex
	// 注册表跟踪的评估器。
	evaluators map[schema.GroupResource]quota.Evaluator
}

// NewRegistry 根据初始评估器列表创建简单注册表。
// 初始列表按 GroupResource 去重，后出现的条目覆盖先出现的。
func NewRegistry(evaluators []quota.Evaluator) quota.Registry {
	return &simpleRegistry{
		evaluators: evaluatorsByGroupResource(evaluators),
	}
}

// Add 注册或替换某个 GroupResource 的 evaluator。
func (r *simpleRegistry) Add(e quota.Evaluator) {
	r.lock.Lock()
	defer r.lock.Unlock()
	r.evaluators[e.GroupResource()] = e
}

// Remove 删除某个 evaluator；若不存在则为空操作。
func (r *simpleRegistry) Remove(e quota.Evaluator) {
	r.lock.Lock()
	defer r.lock.Unlock()
	delete(r.evaluators, e.GroupResource())
}

// Get 返回对应 GroupResource 的 evaluator，不存在时返回 nil。
func (r *simpleRegistry) Get(gr schema.GroupResource) quota.Evaluator {
	r.lock.RLock()
	defer r.lock.RUnlock()
	return r.evaluators[gr]
}

// List 返回当前所有 evaluator 的快照；顺序不确定，调用方不应依赖。
func (r *simpleRegistry) List() []quota.Evaluator {
	r.lock.RLock()
	defer r.lock.RUnlock()

	return evaluatorsList(r.evaluators)
}

// evaluatorsByGroupResource 把评估器列表转换为按组资源索引的映射。
// 重复条目按“后者胜出”处理，与 Add 的覆盖语义保持一致。
func evaluatorsByGroupResource(items []quota.Evaluator) map[schema.GroupResource]quota.Evaluator {
	result := map[schema.GroupResource]quota.Evaluator{}
	for _, item := range items {
		result[item.GroupResource()] = item
	}
	return result
}

// evaluatorsList 把评估器映射转换为列表。
func evaluatorsList(input map[schema.GroupResource]quota.Evaluator) []quota.Evaluator {
	var result []quota.Evaluator
	for _, item := range input {
		result = append(result, item)
	}
	return result
}
