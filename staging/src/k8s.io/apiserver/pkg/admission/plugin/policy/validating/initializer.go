/*
Copyright 2022 The Kubernetes Authors.

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

package validating

import (
	"context"

	"k8s.io/apiserver/pkg/admission"
)

// CELPolicyEvaluator 是校验策略插件暴露给准入链的接口。它内嵌
// InitializationValidator，使插件可以在开始服务流量前一次性完成初始化
// （类型检查、informer 同步）；Run/HasSynced 则让 apiserver 生命周期
// 能够管理其后台循环。
type CELPolicyEvaluator interface {
	admission.InitializationValidator

	// Validate 对给定属性评估所有命中的策略，
	// 任一策略拒绝请求时返回相应的准入错误。
	Validate(ctx context.Context, a admission.Attributes, o admission.ObjectInterfaces) error
	// HasSynced 报告策略与 param 缓存是否已同步。
	HasSynced() bool
	// Run 启动后台工作，stopCh 关闭时返回。
	Run(stopCh <-chan struct{})
}
