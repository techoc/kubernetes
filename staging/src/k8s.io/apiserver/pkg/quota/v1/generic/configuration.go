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

// Package generic 提供 quota.Configuration 的通用实现，
// 把 evaluator 列表与忽略资源集合组合成配额控制器可用的配置。
package generic

import (
	"k8s.io/apimachinery/pkg/runtime/schema"
	quota "k8s.io/apiserver/pkg/quota/v1"
)

// 实现基础配额配置。
//
// 该实现不可变：构造后只读，因此无需加锁即可并发访问。
type simpleConfiguration struct {
	evaluators       []quota.Evaluator
	ignoredResources map[schema.GroupResource]struct{}
}

// NewConfiguration 根据评估器列表和忽略资源集合创建配额配置。
func NewConfiguration(evaluators []quota.Evaluator, ignoredResources map[schema.GroupResource]struct{}) quota.Configuration {
	return &simpleConfiguration{
		evaluators:       evaluators,
		ignoredResources: ignoredResources,
	}
}

// IgnoredResources 返回不参与配额计算的资源集合。
func (c *simpleConfiguration) IgnoredResources() map[schema.GroupResource]struct{} {
	return c.ignoredResources
}

// Evaluators 返回按顺序执行的配额 evaluator 列表。
func (c *simpleConfiguration) Evaluators() []quota.Evaluator {
	return c.evaluators
}
